#!/usr/bin/env python3
"""The msg/s ramp's verdicts, its next step, and its report (ADR 0090 D1, D3, D4; T3).

    verdict.py judge  <step-dir>                 judge one step; writes <step-dir>/verdict.json
    verdict.py next   <ladder-dir>               print the next step's msg/s, or nothing when done
    verdict.py report <run-dir>                  write <run-dir>/report.md and report.json
    verdict.py idle   [--floor N] <scrape.prom>...  exit 0 when the cluster is idle (between steps)
    verdict.py cost   --tier n3|n10 [...]        hourly price and expected duration of a run

Everything is judged from the broker's side (D1):

- **FAILED**: something was lost. A generator's identity ledger did not close (missing,
  unexpected, unacked) — the primary judge — or a broker dropped a message by a reason
  DROP_IS_LOSS classifies as loss (`mqttd_publish_dropped_total`; an unclassified
  reason is loss), or a broker restarted. A drop that is not loss (an already-acked
  entry leaving the settle-replay window, a withheld ack) is a note. Loss stops the
  ladder.
- **VOID**: the evidence is the harness's, not the broker's. A generator saturated in its
  measured window (mqtt-load `generator.saturated`), a generator produced no summary, the
  generators' measured windows do not overlap, a broker of the run has no scrapes of the
  step (its fetch failed) or its scrapes do not cover the window, or the cluster was not
  idle when the step started (unreset: VOID even over a loss finding, which stays
  listed, since residue of the step before can cause it). The brokers' rates are a cluster's only when every broker
  has a usable window; a partial sum is never judged against the offer. A VOID step is rerun; it
  never counts against the broker.
- **DISCONNECTED**: connections were lost during the step (mqtt-load outcome `error`).
  mqtt-load does not reconnect, so its ledger cannot be read after that: not carried,
  and not by itself a loss finding (a broker drop still makes it FAILED).
- **NOT DRAINED**: the ledger misses deliveries, no broker dropped any as loss, and the
  drain ended (quiet or timeout) while the brokers' own gauges still held at least as
  many messages for the generator's clients (`mqttd_inflight_messages` +
  `mqttd_pending_publishes` at the first scrape after the drain's end). The missing
  messages were still queued, not lost: not carried, not loss. With broker loss drops
  the step is FAILED, and its missing count is reconciled as loss drops + still queued
  at the cut-off.
- **OFFER NOT TAKEN**: the brokers' received counters took less than OFFER_TAKEN of the
  offered msg/s over the window (the constant and its meaning are
  bench/scale/summarize-curve.py's, ADR 0048 amendment of 2026-10-25).
- **CARRIED**: none of the above. Latency is then the merge of every generator's HDR
  histogram (bucket-wise, exact), never an average of percentiles.

The headline is the **maximum lossless** msg/s: the highest CARRIED step, whatever its
latency, reported with its ack and end-to-end p99. Below it, the highest CARRIED step
whose ack p99 is at most 1 s, 250 ms and 100 ms. A QoS 0 workload has no acks; its bands
apply to the end-to-end p99 (the workload's LATENCY_GATE says which).

The ramp (D4) climbs by a factor on every CARRIED step — a lossless step at ack p99 2 s
still climbs — until the first FAILED, OFFER NOT TAKEN, DISCONNECTED or NOT DRAINED step,
then bisects between the last CARRIED and that step until their gap is at most GAP_PCT
percent of the carried one. VOID steps are rerun. It stops at a FAILED step (unless
bisect_on_fail), after a step stays VOID through its retries, or at the ladder's ceiling.

A bad step is retried once, after a full settle, when the carried step just below it
showed no sign of the knee: gated p99 (ack; QoS 0: end-to-end) under retry_p99_ms (50 ms).
Only a retry that is bad too counts; a retry that carries makes the ladder climb on, and
the first attempt is reported "retried: transient". At most retry_max (2) per ladder, so a
real cliff cannot loop (retry_marks).
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import re
import sys
import time
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCALE = HERE.parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# The broker-counter parsing is extract-lane-e.py's, imported rather than re-implemented:
# a truncated, duplicated or non-finite scrape is refused the same way on both rigs.
E = _load("extract_lane_e", SCALE / "extract-lane-e.py")
# One constant, one meaning across the rigs: the share of the offer the brokers must take.
OFFER_TAKEN = _load("summarize_curve", SCALE / "summarize-curve.py").OFFER_TAKEN

SCHEMA_REPORT = "mqtt-rig2.report.v1"
SCHEMAS_ACCEPTED = ("mqtt-load.summary.v2",)
BANDS_MS = (1000, 250, 100)
QUANTILES = {"p50": 0.5, "p99": 0.99, "p999": 0.999}

# Hetzner hourly prices (EUR, excl. VAT) for the server types the tiers use. Unknown
# types have no price here; pass --price TYPE=EUR to cost one.
PRICES_EUR_H = {"ccx23": 0.17225, "ccx33": 0.277375}
TIERS = {
    "n3": {"brokers": 3, "broker_type": "ccx23", "gens": 1, "gen_type": "ccx33"},
    "n10": {"brokers": 10, "broker_type": "ccx23", "gens": 3, "gen_type": "ccx33"},
}

CARRIED, FAILED, VOID, NOT_TAKEN, DISCONNECTED = "CARRIED", "FAILED", "VOID", "OFFER NOT TAKEN", "DISCONNECTED"
NOT_DRAINED = "NOT DRAINED"
# Not carried and not the harness's fault: above the knee for the ramp.
BAD = (FAILED, NOT_TAKEN, DISCONNECTED, NOT_DRAINED)
# The ramp's transient-retry rule (see the module docstring): the carried step below a bad
# step must have had its gated p99 under this, and a ladder retries at most this often.
RETRY_P99_MS = 50.0
RETRY_MAX = 2

# Every `mqttd_publish_dropped_total{reason}` the broker can report, classified ONCE: is
# it the loss of a message the broker took and still owed to a subscriber that exists
# (LOSS, fails the step), or not (counted, reported as a note, never a failure)? Read
# from the reason table in crates/mqtt-observability/src/metrics.rs (`publish_dropped`)
# and each increment site in crates/mqttd/src. A reason missing here is LOSS: a new
# drop reason fails steps until someone reads its site and classifies it. The identity
# ledger stays the primary loss judge either way; this table only decides whether a
# broker counter fails a step whose ledger closed.
DROP_IS_LOSS = {
    # A QoS 0 or ungated message shed before every subscriber got it.
    "outbound-full": True,           # hub/delivery.rs: QoS 0 shed at a stalled subscriber's outbound cap
    "hub-ingress": True,             # conn.rs, rules.rs, hub/mod.rs: QoS 0 shed under shed-qos0 overload
    "peer-backlog": True,            # hub/forwarding.rs, hub/mod.rs: QoS 0 forward shed at a backlogged peer link
    "peer-unreachable": True,        # hub/forwarding.rs: ungated forward not sent to an interested peer with no link
    "no-shared-member": True,        # hub/delivery.rs, hub/mod.rs: a $share group none of whose members was reachable
    "too-large": True,               # conn.rs: over that subscriber's Maximum Packet Size (per spec, still not delivered)
    # A message the broker had stored or acknowledged, gone for its subscriber.
    "queue-overflow": True,          # hub/lanes.rs: durable session queue at its cap (evict-oldest / reject-newest)
    "backlog-overflow": True,        # hub/delivery.rs, hub/lanes.rs: already-acked backlog entries truncated
    "append-backlog-full": True,     # hub/lanes.rs, hub/delivery.rs: append lane full; withheld when answerable, a genuine drop otherwise
    "brownout": True,                # hub/lanes.rs: a durable copy lost above the watermark that nobody was told about
    "relaxed-post-verdict-append": True,  # hub/lanes.rs: a relaxed forward's durable copy did not land after its early verdict
    "retained-replay-read-failed": True,  # hub/mod.rs: a new subscriber's retained replay skipped on a store error
    # Not the loss of an owed message for an existing subscriber.
    "pending-cap-replay": False,     # hub/forwarding.rs: an ALREADY-ACKED entry evicted from the settle-replay window; only a
                                     # session materialised during that window misses it (issue #613 item 2.1)
    "settle-replay": False,          # hub/forwarding.rs: the same replay abandoned for an already-acked publish
    "settle-replay-refused": False,  # hub/forwarding.rs: the same replay refused (brownout) for an already-acked publish
    "pending-cap": False,            # hub/forwarding.rs: an UNACKED publish evicted, ack withheld; the publisher still owns
                                     # it (the ledger counts it unacked if it is never answered)
    "outbound-id-write-failed": False,  # hub/lanes.rs: a QoS 2 delivery deferred, already durable, back to the backlog front
    "retained-replay-client-offline": False,  # hub/mod.rs: retained replay for an offline subscriber; the value stays stored
    "reserved": False,               # hub/mod.rs: a publish into $SYS/ reaching the hub (a bug guard); never acked
    "no-subscriber": False,          # documented in metrics.rs; nothing matched, so no subscriber exists
}


def drop_is_loss(reason: str) -> bool:
    """Whether a drop reason fails a step: DROP_IS_LOSS, and LOSS for any reason not in it."""
    return DROP_IS_LOSS.get(reason, True)


# ── histograms ──────────────────────────────────────────────────────────────────────


def merge_histograms(exports: list[dict | None]) -> list[list[int]]:
    """Bucket-wise sum of mqtt-load histogram exports (identical bounds by construction)."""
    acc: Counter[int] = Counter()
    for e in exports:
        if not e:
            continue
        if e.get("unit") != "us" or e.get("significant_figures") != 3:
            raise ValueError(f"histogram export of another shape: {e.get('unit')}, {e.get('significant_figures')}")
        for v, c in e["buckets"]:
            acc[int(v)] += int(c)
    return [[v, acc[v]] for v in sorted(acc)]


def quantile_us(buckets: list[list[int]], q: float) -> int | None:
    """hdrhistogram's value_at_quantile over exported buckets: the first bucket at which
    the cumulative count reaches max(1, ceil(q * total)); None for an empty histogram."""
    total = sum(c for _, c in buckets)
    if total == 0:
        return None
    target = max(1, math.ceil(min(q, 1.0) * total))
    cum = 0
    for v, c in buckets:
        cum += c
        if cum >= target:
            return v
    return buckets[-1][0]


def latency_summary(buckets: list[list[int]]) -> dict:
    out = {"count": sum(c for _, c in buckets)}
    for k, q in QUANTILES.items():
        out[f"{k}_us"] = quantile_us(buckets, q)
    out["max_us"] = buckets[-1][0] if buckets else None
    return out


# ── broker scrapes ──────────────────────────────────────────────────────────────────


def load_samples(d: Path) -> tuple[list[tuple[int, dict]], int]:
    """(unix_ms, parsed) for every complete scrape in d, sorted; and how many were refused."""
    out, bad = [], 0
    for p in sorted(d.glob("*.prom")):
        try:
            out.append((int(p.stem), E.parse_prom(p)))
        except ValueError:
            bad += 1
    failed = d / "failed"
    if failed.is_file():
        bad += int(failed.read_text().strip() or 0)
    out.sort(key=lambda s: s[0])
    return out, bad


HELD_GAUGES = ("mqttd_inflight_messages", "mqttd_pending_publishes", "mqttd_backlog_bytes")


def expected_brokers(step: Path, meta: dict) -> int | None:
    """How many brokers the step's run has: step.json `brokers`, else the run's run.json
    (<run>/<arm>/<workload>/steps/<step>), else None (judge whatever scrapes are there)."""
    if meta.get("brokers") is not None:
        return int(meta["brokers"])
    run_json = step.resolve().parents[3] / "run.json"
    try:
        n = json.loads(run_json.read_text()).get("brokers")
    except (OSError, ValueError):
        return None
    return None if n is None else int(n)


def missing_brokers(step: Path, expected: int | None) -> list[int]:
    """The brokers of the run with no scrapes at all in the step (no directory, or not
    one .prom file in it): their fetch failed, so any cluster sum would be partial."""
    if expected is None:
        return []
    root = step / "scrapes"
    return [i for i in range(expected) if not any((root / f"broker-{i}").glob("*.prom"))]


def broker_figures(step: Path, lo_ms: float, hi_ms: float, margin_ms: float, min_cover: float,
                   cutoff_ms: float | None = None, expected: int | None = None) -> dict:
    """The brokers' own counters over [lo, hi] (wall ms), and their drops over the step.

    Per broker: the first and last scrape inside the window shrunk by `margin_ms` at each
    edge (absorbs scrape spacing and any small offset between host clocks; the rates use
    only the broker host's own timestamps). Drops and restarts are read across EVERY
    scrape of the step, warm-up and drain included: a message dropped at any point is loss.

    `held_at_cutoff`: what the brokers still held for clients (HELD_GAUGES, summed) at
    each broker's first scrape at or after `cutoff_ms`, the generators' drain end.

    `expected`: the run's broker count. The figures are a cluster's only when EVERY
    broker has a usable window: `complete` is false, a problem names the brokers whose
    scrapes are missing, and the rates are None otherwise. On the aborted 10-node run
    (2026-10-27) brokers 5-9's scrapes could not be fetched and the sum over the other
    five read as half the offer: OFFER NOT TAKEN, for a network outage on the operator's
    side.
    """
    root = step / "scrapes"
    brokers = sorted((p for p in root.glob("broker-*") if p.is_dir()), key=lambda p: int(p.name.split("-")[1]))
    res = {"brokers": [], "received_rate": 0.0, "delivered_rate": 0.0, "dropped": 0.0, "dropped_by_reason": {},
           "lost": 0.0, "lost_by_reason": {}, "not_lost_by_reason": {}, "restarted": [], "problems": [],
           "held_at_cutoff": None, "expected": expected, "complete": False}
    absent = missing_brokers(step, expected)
    if absent:
        res["problems"].append(f"no scrapes from broker(s) {' '.join(map(str, absent))}")
    if not brokers:
        if not absent:
            res["problems"].append("no broker scrapes")
        res["received_rate"] = res["delivered_rate"] = None
        return res
    rated = 0
    want = (hi_ms - lo_ms - 2 * margin_ms) * min_cover
    for b in brokers:
        idx = int(b.name.split("-")[1])
        samples, refused = load_samples(b)
        row = {"broker": idx, "scrapes": len(samples), "refused": refused}
        res["brokers"].append(row)
        if idx in absent:
            continue  # named above
        if len(samples) < 2:
            res["problems"].append(f"broker {idx}: {len(samples)} usable scrapes")
            continue
        first, last = samples[0][1], samples[-1][1]
        if first.get(("process_start_time_seconds", "")) != last.get(("process_start_time_seconds", "")):
            res["restarted"].append(idx)
        drops = E.positive_deltas(first, last, E.DROPPED, "reason")
        for k, v in drops.items():
            res["dropped_by_reason"][k] = res["dropped_by_reason"].get(k, 0.0) + v
            side = "lost_by_reason" if drop_is_loss(k) else "not_lost_by_reason"
            res[side][k] = res[side].get(k, 0.0) + v
        res["dropped"] += sum(drops.values())
        res["lost"] += sum(v for k, v in drops.items() if drop_is_loss(k))
        row["dropped"] = sum(drops.values())
        if cutoff_ms is not None:
            after = [p for t, p in samples if t >= cutoff_ms]
            if after:
                held = res["held_at_cutoff"] or {g: 0.0 for g in HELD_GAUGES}
                for g in HELD_GAUGES:
                    held[g] += E.sum_family(after[0], g)
                res["held_at_cutoff"] = held
        inside = [s for s in samples if lo_ms + margin_ms <= s[0] <= hi_ms - margin_ms]
        if len(inside) < 2 or inside[-1][0] - inside[0][0] < want:
            span = (inside[-1][0] - inside[0][0]) / 1000 if len(inside) >= 2 else 0.0
            res["problems"].append(f"broker {idx}: scrapes cover {span:.1f} s of the window (need {want / 1000:.1f} s)")
            continue
        (t0, a), (t1, z) = inside[0], inside[-1]
        try:
            E.validate_deltas({idx: a}, {idx: z}, "window open", "window close")
        except ValueError as e:
            res["problems"].append(str(e))
            continue
        dt = (t1 - t0) / 1000
        rx, dl = E.delta(a, z, E.RECEIVED) / dt, E.delta(a, z, E.DELIVERED) / dt
        row.update(window_s=dt, received_rate=rx, delivered_rate=dl)
        res["received_rate"] += rx
        res["delivered_rate"] += dl
        rated += 1
    res["complete"] = not absent and rated == len(brokers) and (expected is None or rated >= expected)
    if not res["complete"]:
        # A sum over some of the brokers is not the cluster's rate: never report it as one.
        res["received_rate"] = res["delivered_rate"] = None
    return res


IDLE_GAUGES = ("mqttd_connections_active", "mqttd_sessions", "mqttd_pending_publishes")
# Work still in the brokers' pipes (cluster sums; every one is 0 on an idle cluster):
# durable appends admitted and not completed (the warning before append-backlog-full),
# acks still gated on durability or the settle window or an unreachable peer, unacked
# deliveries and flow-control backlog owed to clients, and frames queued on peer links.
BUSY_GAUGES = ("mqttd_append_lane_jobs", "mqttd_pending_publishes_awaiting_settle",
               "mqttd_publish_held_for_unreachable", "mqttd_inflight_messages", "mqttd_backlog_bytes",
               "mqttd_peer_forwards_in_flight")
# The hub's command queue: one or two commands at idle (the /livez ping, a sweep).
HUB_QUEUE_IDLE = 8
# Counters that move only with work: unchanged across the quiet period means nothing is
# still being received, delivered, forwarded or written (session cleanup and replication
# catch-up write through the durable writer).
QUIET_COUNTERS = ("mqttd_publish_received_total", "mqttd_publish_delivered_total",
                  "mqttd_publish_forwarded_total", "mqttd_durable_writer_ops_total")
# ...but a counter ticking at a background rate does not hold the settle: at most
# QUIET_FRACTION of the coming step's msg/s, and never less than QUIET_FLOOR per second,
# summed over the cluster, between two complete passes. 0.1 % of the step is 30x below
# the ramp's resolution (GAP_PCT, 3 %) and far below the rig's run-to-run noise (~15 %).
# The floor covers an idle cluster's own housekeeping on a small step: on the 10-node run
# of 2026-10-27 the durable writer did 0 to 110 ops in five minutes of settle (at most
# 22/s, had they all fallen into one 5 s pass). A real drain is far above both: the
# writer's tail after a durable step ran at 15,000/s, then 4,000, 520, 360 and 220/s in
# its last 2 s scrapes, and a drain of deliveries runs at the step's own rate. Work still
# in the pipes (lanes, pending publishes, inflight, backlog, peer frames) is held by the
# BUSY_GAUGES whatever the counters do.
QUIET_FRACTION = 0.001
QUIET_FLOOR = 20.0
# mqttd_routing_unsettled{reason="scan"}: a session scan is running. Under a durable
# plane every broker runs one every ~30 s, for under a second; ten brokers make a pass
# see one of them about 30 % of the time (the 2026-10-27 run: 215 of 755 cluster-wide
# scrapes), so treating it as unsettled held a 30 s quiet period off for minutes (six
# clean passes in a row: 13 %). A scan is background, not settling: it holds the settle
# only when the same broker is still scanning on the next pass (a stuck scan). The
# other terms (takeover, scan-incomplete, interest, mesh) hold it on any pass.
SCAN_REASON = "scan"


def quiet_allowance(step_rate: float | None) -> float:
    """Ops/s a quiet counter may move at between two passes without holding the settle."""
    return max(QUIET_FLOOR, QUIET_FRACTION * (step_rate or 0.0))


def idle_state(paths: list[Path]) -> dict:
    """Cluster sums of the residue and busy gauges and of the quiet counters, the hub
    queue, and per broker: placement groups holding fewer copies than desired
    (mqttd_replication_min_actual < _desired), replication lag in groups
    (mqttd_replica_groups_tracked - _current), a routing view still settling (any
    mqttd_routing_unsettled term but `scan` at 1) and a session scan running (`scan`
    at 1, in `scanning`). A cluster still re-replicating, catching up or settling after
    formation or a previous step is not ready for the next one."""
    tot: dict = {g: 0.0 for g in IDLE_GAUGES + BUSY_GAUGES + QUIET_COUNTERS}
    tot["mqttd_hub_queue_depth"] = 0.0
    tot["under_replicated"], tot["routing_unsettled"], tot["replication_lag"] = [], [], {}
    tot["scanning"] = []
    for p in paths:
        parsed = E.parse_prom(p)
        for g in (*IDLE_GAUGES, *BUSY_GAUGES, *QUIET_COUNTERS, "mqttd_hub_queue_depth"):
            tot[g] += E.sum_family(parsed, g)
        want, have = parsed.get(("mqttd_replication_desired", "")), parsed.get(("mqttd_replication_min_actual", ""))
        if want is not None and have is not None and have < want:
            tot["under_replicated"].append(p.stem)
        tracked, current = parsed.get(("mqttd_replica_groups_tracked", "")), parsed.get(("mqttd_replica_groups_current", ""))
        if tracked is not None and current is not None and current < tracked:
            tot["replication_lag"][p.stem] = tracked - current
        terms = E.by_label(parsed, "mqttd_routing_unsettled", "reason")
        if any(v > 0 for r, v in terms.items() if r != SCAN_REASON):
            tot["routing_unsettled"].append(p.stem)
        if terms.get(SCAN_REASON, 0.0) > 0:
            tot["scanning"].append(p.stem)
    return tot


def not_idle(state: dict, floor: float, baseline_sessions: float = 0.0, prev: dict | None = None,
             step_rate: float | None = None) -> list[str]:
    """Why the cluster is not settled (empty when it is). `baseline_sessions` is the
    session count the run started with; `prev`, the state of the previous complete
    scrape pass, makes movement of the QUIET_COUNTERS since then a reason, unless it was
    at a background rate (`quiet_allowance(step_rate)` per second, when both states
    carry their time `at_unix_s`; without times any movement is a reason). Movement
    below the allowance is listed in `state["background"]`.

    Replication lag (groups tracked but not caught up) holds the settle while it moves:
    without `prev`, or when it differs from `prev`'s. A lag that stood still since the
    previous scrape is not catching up with anything and is left to the record (a local
    3-node cluster held 72 groups behind on one node, at idle, for minutes). A session
    scan (`scanning`) holds it only when the same broker was scanning in `prev` too."""
    why = []
    if state["mqttd_connections_active"] > floor:
        why.append(f"connections {state['mqttd_connections_active']:.0f} > {floor:g}")
    if state["mqttd_sessions"] > max(floor, baseline_sessions):
        why.append(f"sessions {state['mqttd_sessions']:.0f} > baseline {max(floor, baseline_sessions):g}")
    if state["mqttd_pending_publishes"] != 0:
        why.append(f"pending publishes {state['mqttd_pending_publishes']:.0f}")
    for g in BUSY_GAUGES:
        if state.get(g, 0.0) != 0:
            why.append(f"{g.removeprefix('mqttd_')} {state[g]:.0f}")
    if state.get("mqttd_hub_queue_depth", 0.0) > HUB_QUEUE_IDLE:
        why.append(f"hub queue {state['mqttd_hub_queue_depth']:.0f}")
    for k, txt in (("under_replicated", "under-replicated"), ("routing_unsettled", "routing unsettled")):
        if state.get(k):
            why.append(f"{txt}: {', '.join(state[k])}")
    lag = state.get("replication_lag") or {}
    if lag and (prev is None or (prev.get("replication_lag") or {}) != lag):
        why.append("replication catching up: " + ", ".join(f"{b} {n:.0f} groups behind" for b, n in sorted(lag.items())))
    if prev is not None:
        stuck = sorted(set(state.get("scanning") or []) & set(prev.get("scanning") or []))
        if stuck:
            why.append(f"session scan still running on {', '.join(stuck)} (two passes in a row)")
        t0, t1 = prev.get("at_unix_s"), state.get("at_unix_s")
        dt = t1 - t0 if (t0 is not None and t1 is not None and t1 > t0) else None
        allowed = quiet_allowance(step_rate)
        moved, background = [], []
        for g in QUIET_COUNTERS:
            d = state.get(g, 0.0) - prev.get(g, 0.0)
            if d == 0:
                continue
            name = g.removeprefix("mqttd_")
            if dt is None:
                moved.append(f"{name} moved by {d:.0f}")
            elif abs(d) / dt > allowed:
                moved.append(f"{name} moved by {d:.0f} in {dt:.1f} s ({abs(d) / dt:.0f}/s > {allowed:.0f}/s allowed)")
            else:
                background.append(f"{name} +{d:.0f} in {dt:.1f} s ({abs(d) / dt:.1f}/s, background)")
        if moved:
            why.append("not quiet: " + "; ".join(moved))
        if background:
            state["background"] = background
    return why


def is_idle(state: dict, floor: float, baseline_sessions: float = 0.0, prev: dict | None = None,
            step_rate: float | None = None) -> bool:
    return not not_idle(state, floor, baseline_sessions, prev, step_rate)


# ── one step ────────────────────────────────────────────────────────────────────────


def measured_span(s: dict) -> tuple[float, float]:
    """A generator summary's measured span, wall ms."""
    ms = [w for w in s["windows"] if w["kind"] == "measure"]
    base = s["schedule_start_unix_ms"]
    return base + min(w["start_s"] for w in ms) * 1000, base + max(w["end_s"] for w in ms) * 1000


def generator_health(s: dict) -> dict:
    ms = [w for w in s["windows"] if w["kind"] == "measure"]
    h = [w["health"] for w in ms]
    bw = [x["busiest_worker"] for x in h if x.get("busiest_worker") is not None]
    return {
        "outcome": s["outcome"],
        "saturated": s["generator"]["saturated"],
        "reasons": sorted({r for x in h for r in x.get("reasons", [])}),
        "cpu_fraction_max": max((x["cpu_fraction"] for x in h), default=0.0),
        "busiest_worker_max": max(bw) if bw else None,
        "sched_lag_p99_us_max": max((w["sched_lag"]["p99_us"] for w in ms), default=0),
        "credit_stall_max": max((x["credit_stall_fraction"] for x in h), default=0.0),
        "backlog_max": max((x["backlog"] for x in h), default=0),
        "sent_per_cpu_second": s["generator"]["sent_per_cpu_second"],
    }


def drain_end_ms(s: dict) -> float:
    """A generator summary's drain end, wall ms (its last window's end)."""
    return s["schedule_start_unix_ms"] + max(w["end_s"] for w in s["windows"]) * 1000


def drain_figures(gens: list, bf: dict | None) -> dict:
    """How the generators' drains ended, and the ledger's missing count reconciled with
    the brokers' own figures when a drain ended before it was complete:

        missing = broker loss drops + still queued at the cut-off + unexplained

    `queued_at_cutoff` is what the brokers' gauges account for (unacked deliveries and
    pending publishes at their first scrape after the drain's end, at most the missing
    beyond the drops); `unexplained` is the rest: missing that no drop counter and no
    broker queue accounts for. `still_queued` is true when the gauges account for every
    message missing beyond the drops (and every unacked publish) and nothing unexpected
    arrived: the drain gave up on messages the brokers still held."""
    per = []
    for g, s, _ in gens:
        d = s.get("drain", {})
        per.append({"generator": g, "ended_by": d.get("ended_by"), "duration_s": d.get("duration_s"),
                    "quiet_s": d.get("quiet_s"), "outstanding_deliveries": d.get("outstanding_deliveries"),
                    "outstanding_acks": d.get("outstanding_acks")})
    missing = sum(s["ledger"]["missing"] for _, s, _ in gens)
    # Of the missing, those whose publish was positively acknowledged: the broker took
    # responsibility and the message never arrived, which is loss. The rest were never
    # acknowledged, which QoS 1 lets the publisher resend (None: an older generator).
    acked_parts = [s["ledger"].get("missing_acked") for _, s, _ in gens]
    missing_acked = None if any(a is None for a in acked_parts) else sum(acked_parts)
    unacked = sum(s["ledger"]["unacked"] for _, s, _ in gens)
    unexpected = sum(s["ledger"]["unexpected"] for _, s, _ in gens)
    cut = any(p["ended_by"] not in (None, "complete") for p in per)
    lost = bf["lost"] if bf else 0.0
    held = bf.get("held_at_cutoff") if bf else None
    held_msgs = 0.0 if held is None else held["mqttd_inflight_messages"] + held["mqttd_pending_publishes"]
    residual = max(0.0, missing - lost)
    queued = min(residual, held_msgs) if cut else 0.0
    out = {"generators": per, "cut_off": cut, "missing": missing, "loss_drops": lost,
           "queued_at_cutoff": queued, "unexplained": residual - queued if cut else 0.0,
           "held_at_cutoff": held, "still_queued": False, "reconciled": None,
           "missing_acked": missing_acked,
           "missing_unacked": None if missing_acked is None else missing - missing_acked,
           # Acknowledged loss that neither a drop counter nor a broker queue explains.
           "unexplained_acked": None if missing_acked is None else max(0.0, missing_acked - lost - queued)}
    if not cut or (missing == 0 and unacked == 0):
        return out
    how = ", ".join(sorted({f"{p['ended_by']} after {p['duration_s']:.1f} s" for p in per
                            if p["ended_by"] not in (None, "complete") and p["duration_s"] is not None}))
    parts = [f"broker loss drops {lost:,.0f}"]
    if missing_acked is not None:
        parts.insert(0, f"(acknowledged {missing_acked:,} + never acknowledged {missing - missing_acked:,})")
    if queued:
        parts.append(f"{queued:,.0f} still queued on the brokers at the cut-off")
    if out["unexplained"]:
        parts.append(f"{out['unexplained']:,.0f} that no broker drop counter or queue accounts for")
    held_txt = ("; the brokers' gauges were not read after it" if held is None else
                f"; the brokers then held {held['mqttd_inflight_messages']:,.0f} unacked deliveries, "
                f"{held['mqttd_pending_publishes']:,.0f} pending publishes and "
                f"{held['mqttd_backlog_bytes'] / 1e6:,.1f} MB of backlog")
    out["reconciled"] = f"drain cut off ({how}): missing {missing:,} = " + " + ".join(parts) + held_txt
    out["still_queued"] = bool(held is not None and unexpected == 0 and residual > 0
                               and held_msgs >= residual + unacked)
    return out


def judge(step: Path, cfg: dict | None = None) -> dict:
    """The verdict of one step directory (see the module docstring for the layout)."""
    meta = json.loads((step / "step.json").read_text())
    cfg = {**meta.get("judge", {}), **(cfg or {})}
    margin_ms = float(cfg.get("margin_s", 1.0)) * 1000
    min_cover = float(cfg.get("min_cover", 0.6))
    min_overlap = float(cfg.get("min_overlap", 0.9))
    floor = float(cfg.get("idle_floor", 0))
    gate = cfg.get("latency_gate", "ack")
    out = {"step": meta["index"], "rate": meta["rate"], "attempt": meta.get("attempt", 1), "voids": [],
           "failures": [], "notes": [], "ramp_reason": meta.get("ramp_reason")}
    gens = []
    for g in range(int(meta["generators"])):
        p = step / f"gen-{g}.json"
        code = (step / f"gen-{g}.exit").read_text().strip() if (step / f"gen-{g}.exit").is_file() else "?"
        try:
            s = json.loads(p.read_text())
        except (OSError, ValueError):
            out["voids"].append(f"generator {g} produced no summary (exit {code})")
            continue
        if s.get("schema") not in SCHEMAS_ACCEPTED:
            out["voids"].append(f"generator {g}: summary schema {s.get('schema')!r}, need one of {SCHEMAS_ACCEPTED}")
            continue
        gens.append((g, s, code))
    idle = meta.get("idle_before")
    unreset = idle is not None and not idle.get("idle", False)
    if unreset:
        st_ = idle.get("state") or {}
        why = "; ".join(st_.get("why_not") or []) if isinstance(st_, dict) else ""
        out["voids"].append(f"the cluster was not settled when the step started, after {idle.get('waited_s', '?')} s "
                            f"({why or 'no reason recorded'})")

    out["generators"] = []
    for g, s, code in gens:
        st_ = s.get("start") or {}
        if st_.get("by") in ("late", "timeout"):
            out["notes"].append(f"generator {g} missed the shared start: "
                                + (f"it named a time {st_.get('late_ms')} ms past" if st_["by"] == "late"
                                   else f"none came in {st_.get('waited_s', 0):.1f} s")
                                + ", so it started on its own")
        h = generator_health(s)
        h.update(generator=g, exit=code, ledger=s["ledger"], errors=s["errors"],
                 offered_rate=s["phases"][0]["offered_rate"], sent_rate=s["phases"][0]["sent_rate"],
                 received_rate=s["phases"][0]["received_rate"],
                 deliveries_per_message=s["workload"]["deliveries_per_message"])
        out["generators"].append(h)
    offered = sum(x["offered_rate"] for x in out["generators"])
    out["offered_rate"] = offered if gens else meta["rate"]

    # The brokers' window: the overlap of every generator's measured span.
    bf = None
    n_brokers = expected_brokers(step, meta)
    if gens:
        spans = [measured_span(s) for _, s, _ in gens]
        lo, hi = max(a for a, _ in spans), min(b for _, b in spans)
        shortest = min(b - a for a, b in spans)
        out["window"] = {"start_unix_ms": lo, "end_unix_ms": hi, "seconds": max(0.0, (hi - lo) / 1000)}
        if hi - lo < min_overlap * shortest:
            out["voids"].append(f"generator measured windows overlap {max(0.0, hi - lo) / 1000:.1f} s of {shortest / 1000:.1f} s")
        else:
            bf = broker_figures(step, lo, hi, margin_ms, min_cover, cutoff_ms=max(drain_end_ms(s) for _, s, _ in gens),
                                expected=n_brokers)
            out["brokers"] = bf
            out["voids"].extend(f"broker evidence: {p}" for p in bf["problems"])
            if bf["lost"] > 0:
                why = ", ".join(f"{k or '?'}={int(v)}" for k, v in sorted(bf["lost_by_reason"].items()))
                out["failures"].append(f"brokers dropped {int(bf['lost'])} messages ({why})")
            if bf["not_lost_by_reason"]:
                why = ", ".join(f"{k}={int(v)}" for k, v in sorted(bf["not_lost_by_reason"].items()))
                out["notes"].append(f"broker drops that are not loss: {why}")
            if bf["restarted"]:
                out["failures"].append(f"broker(s) {bf['restarted']} restarted during the step")
    if bf is None:
        absent = missing_brokers(step, n_brokers)
        if absent:
            out["voids"].append(f"broker evidence: no scrapes from broker(s) {' '.join(map(str, absent))}")
    for x in out["generators"]:
        if x["saturated"]:
            out["voids"].append(f"generator {x['generator']} saturated ({', '.join(x['reasons']) or 'see summary'})")
    disconnected = [x for x in out["generators"] if x["outcome"] == "error"]
    ledger_open = [x for x in out["generators"] if not x["ledger"]["closed"]]
    out["drain"] = drain = drain_figures(gens, bf)
    if drain.get("reconciled"):
        out["notes"].append(drain["reconciled"])

    acks = merge_histograms([s["phases"][0].get("ack_histogram") for _, s, _ in gens])
    e2e = merge_histograms([s["phases"][0].get("e2e_histogram") for _, s, _ in gens])
    out["ack_latency"], out["e2e_latency"] = latency_summary(acks), latency_summary(e2e)
    gate_p99 = (out["ack_latency"] if gate == "ack" else out["e2e_latency"])["p99_us"]
    out["latency_gate"], out["gate_p99_us"] = gate, gate_p99

    received = bf["received_rate"] if bf else None
    out["received_rate"] = received
    out["delivered_rate"] = bf.get("delivered_rate") if bf else None
    out["received_share"] = received / offered if (received is not None and offered) else None

    if out["failures"]:
        verdict = FAILED
    elif out["voids"]:
        verdict = VOID
    elif disconnected:
        verdict = DISCONNECTED
        out["notes"].append("connections lost: " + "; ".join(f"generator {x['generator']}: {x['errors']}" for x in disconnected))
    elif ledger_open and drain["still_queued"]:
        # Nothing the broker counts as loss, and its own gauges hold at least what the
        # ledger misses: the drain gave up while those messages were still queued.
        verdict = NOT_DRAINED
    elif ledger_open:
        verdict = FAILED
        out["failures"].append("ledger did not close: " + "; ".join(
            f"generator {x['generator']}: missing={x['ledger']['missing']} unexpected={x['ledger']['unexpected']} "
            f"unacked={x['ledger']['unacked']} unexpected_acks={x['ledger']['unexpected_acks']}"
            # Deliveries whose packet id skipped on their connection: as many as are
            # missing places the loss on the broker, after it allocated the id.
            + (f" pkid_gaps={x['ledger']['pkid_gaps']}" if x['ledger'].get('pkid_gaps') else "")
            for x in ledger_open))
    elif out["received_share"] is None or out["received_share"] < OFFER_TAKEN:
        verdict = NOT_TAKEN
    elif gate_p99 is None:
        verdict = VOID
        out["voids"].append(f"no {gate} latency samples")
    else:
        verdict = CARRIED
    if unreset and verdict != VOID:
        # A step that started on an unsettled cluster is VOID whatever it measured, loss
        # included: residue of the step before can cause what it shows. Its findings
        # stay listed; the rerun after a full settle decides.
        out["notes"].append(f"would have been {verdict} on a settled cluster; VOID because it started unsettled")
        verdict = VOID
    out["verdict"] = verdict
    # Good for the ramp: carried, whatever its latency. A lossless step above 1 s is
    # still a step the cluster carried; the latency bands are the report's business.
    out["good"] = verdict == CARRIED
    return out


# ── the ramp ────────────────────────────────────────────────────────────────────────


def nice(rate: float, granularity: float) -> float:
    return max(granularity, math.floor(rate / granularity + 0.5) * granularity)


def retry_marks(steps: list[dict], cfg: dict) -> list[dict | None]:
    """The transient-retry rule, per step in run order: None, or a dict whose `mark` is

    - `pending`: a bad step eligible for a retry that has not run yet;
    - `transient`: a bad step whose retry carried: it does not count;
    - `confirmed`: a bad step whose retry was bad too: it counts;
    - `retry`: this step is the retry of step `of`;
    - `not-retried`: eligible, but the ladder went on without a retry (a run judged
      before the rule existed).

    Eligible: a FAILED, OFFER NOT TAKEN, DISCONNECTED or NOT DRAINED step that is not
    itself a retry, while the ladder has used fewer than retry_max retries, and whose
    nearest carried step below it (the highest carried rate under it, run before it) had
    a gated p99 under retry_p99_ms. VOID steps are transparent: the retry is the next
    judged step, and it must be at the same rate."""
    thr_us = float(cfg.get("retry_p99_ms", RETRY_P99_MS)) * 1000
    cap = int(cfg.get("retry_max", RETRY_MAX))
    marks: list[dict | None] = [None] * len(steps)
    used, waiting = 0, None
    for i, s in enumerate(steps):
        if s["verdict"] == VOID:
            continue
        if waiting is not None:
            j, waiting = waiting, None
            if s["rate"] == steps[j]["rate"]:
                marks[j]["mark"] = "transient" if s["verdict"] == CARRIED else "confirmed"
                marks[j]["retry_step"] = s.get("step", i + 1)
                marks[i] = {"mark": "retry", "of": steps[j].get("step", j + 1)}
                continue  # a retry is never retried
            marks[j]["mark"] = "not-retried"
        if s["verdict"] not in BAD or used >= cap:
            continue
        below = [p for p in steps[:i] if p["verdict"] == CARRIED and p["rate"] < s["rate"]
                 and p.get("gate_p99_us") is not None]
        if not below:
            continue
        ref = max(below, key=lambda p: p["rate"])
        if ref["gate_p99_us"] < thr_us:
            used += 1
            marks[i] = {"mark": "pending", "n": used, "of_max": cap, "ref_rate": ref["rate"],
                        "ref_step": ref.get("step"), "ref_p99_us": ref["gate_p99_us"], "threshold_ms": thr_us / 1000}
            waiting = i
    return marks


def next_rate(steps: list[dict], cfg: dict) -> tuple[float | None, str]:
    """The next step's msg/s, or (None, why the ladder is done). `steps` in run order,
    each a verdict (rate, verdict, gate_p99_us)."""
    start, factor = float(cfg["start_rate"]), float(cfg.get("factor", 1.5))
    gap = float(cfg.get("gap_pct", 3.0)) / 100
    gran = float(cfg.get("granularity", 100))
    max_rate, min_rate = float(cfg.get("max_rate", 1e12)), float(cfg.get("min_rate", gran))
    retries = int(cfg.get("void_retries", 1))
    if not steps:
        return nice(start, gran), "first step"
    last = steps[-1]
    marks = retry_marks(steps, cfg)
    if marks[-1] and marks[-1]["mark"] == "pending":
        m = marks[-1]
        return last["rate"], (f"retry of the {last['verdict']} step at {last['rate']:g} msg/s after a full settle: the "
                              f"carried step below it ({m['ref_rate']:g} msg/s) had p99 {m['ref_p99_us'] / 1000:.1f} ms "
                              f"< {m['threshold_ms']:g} ms, no sign of the knee (retry {m['n']} of at most {m['of_max']})")
    if last["verdict"] == FAILED and not cfg.get("bisect_on_fail", False):
        return None, f"FAILED at {last['rate']:g} msg/s (loss stops the ladder)"
    if last["verdict"] == VOID:
        same = 0
        for s in reversed(steps):
            if s["rate"] != last["rate"] or s["verdict"] != VOID:
                break
            same += 1
        if same <= retries:
            return last["rate"], f"rerun of a VOID step ({same} of {retries + 1} attempts)"
        why = "; ".join(last.get("voids") or []) or "more generator capacity needed"
        return None, f"VOID {same} times at {last['rate']:g} msg/s (last: {why})"
    # Carried is good, whatever its latency (a lossless step at ack p99 2 s still climbs);
    # FAILED, OFFER NOT TAKEN and DISCONNECTED are bad. The verdict decides, not the
    # stored `good`, so a ladder judged before 2026-10-26 replays the same way.
    # A bad step whose retry carried was transient: it does not bracket the knee.
    judged = [s for s, m in zip(steps, marks) if s["verdict"] != VOID and not (m and m["mark"] == "transient")]
    goods = [s["rate"] for s in judged if s["verdict"] == CARRIED]
    lo = max(goods) if goods else None
    bads = [s["rate"] for s in judged if s["verdict"] != CARRIED and (lo is None or s["rate"] > lo)]
    hi = min(bads) if bads else None
    measured = {s["rate"] for s in judged}
    if lo is None and hi is None:
        return nice(start, gran), "first judged step"
    if hi is None:
        nxt = nice(lo * factor, gran)
        if lo >= max_rate:
            return None, f"carried at the ladder's ceiling ({max_rate:g} msg/s)"
        return min(nxt, nice(max_rate, gran)), "coarse step up"
    if lo is None:
        nxt = nice(hi / factor, gran)
        if nxt < min_rate or nxt in measured:
            return None, f"not carried at the lowest step ({min(bads):g} msg/s)"
        return nxt, "coarse step down (nothing carried yet)"
    if (hi - lo) / lo <= gap:
        return None, f"knee bracketed: good at {lo:g}, not at {hi:g} msg/s ({(hi - lo) / lo * 100:.1f}% gap)"
    mid = nice((lo + hi) / 2, gran)
    if mid in measured or mid <= lo or mid >= hi:
        return None, f"knee bracketed at the step granularity: good at {lo:g}, not at {hi:g} msg/s"
    return mid, f"bisect between {lo:g} and {hi:g}"


def ladder_steps(ladder: Path) -> list[dict]:
    out = []
    for d in sorted((p for p in (ladder / "steps").glob("*") if p.is_dir()), key=lambda p: p.name):
        v = d / "verdict.json"
        if v.is_file():
            out.append(json.loads(v.read_text()))
    return out


# ── the report ──────────────────────────────────────────────────────────────────────


def capacity(steps: list[dict]) -> dict:
    """`lossless`: the highest CARRIED step, whatever its latency, with its ack and
    end-to-end p99. `p99_le_<band>ms`: the highest CARRIED step whose gated p99 (ack;
    QoS 0: end-to-end) is within the band."""
    carried = [s for s in steps if s["verdict"] == CARRIED and s["gate_p99_us"] is not None]
    top = max(carried, key=lambda s: s["rate"]) if carried else None
    out: dict = {"lossless": None if top is None else {
        "offered_rate": top["offered_rate"], "received_rate": top["received_rate"],
        "gate_p99_us": top["gate_p99_us"], "ack_p99_us": top["ack_latency"]["p99_us"],
        "e2e_p99_us": top["e2e_latency"]["p99_us"], "step": top["step"]}}
    for band in BANDS_MS:
        ok = [s for s in carried if s["gate_p99_us"] <= band * 1000]
        best = max(ok, key=lambda s: s["rate"]) if ok else None
        out[f"p99_le_{band}ms"] = None if best is None else {
            "offered_rate": best["offered_rate"], "received_rate": best["received_rate"],
            "gate_p99_us": best["gate_p99_us"], "step": best["step"]}
    return out


def fmt_rate(x) -> str:
    return "—" if x is None else f"{x:,.0f}"


def fmt_us(x) -> str:
    if x is None:
        return "—"
    return f"{x / 1000:.1f} ms" if x < 10_000_000 else f"{x / 1e6:.2f} s"


def fmt_ledger(s: dict) -> str:
    gs = s.get("generators", [])
    if not gs:
        return "—"
    keys = ("missing", "unexpected", "unacked")
    tot = {k: sum(g["ledger"][k] for g in gs) for k in keys}
    if all(g["ledger"]["closed"] for g in gs):
        return "closed"
    return " ".join(f"{k}={v}" for k, v in tot.items() if v) or "open"


def fmt_health(s: dict) -> str:
    gs = s.get("generators", [])
    if not gs:
        return "—"
    cpu = max(g["cpu_fraction_max"] for g in gs)
    lag = max(g["sched_lag_p99_us_max"] for g in gs)
    sat = [str(g["generator"]) for g in gs if g["saturated"]]
    txt = f"cpu {cpu:.0%}, lag p99 {lag / 1000:.1f} ms"
    return txt + (f", SATURATED g{','.join(sat)}" if sat else "")


def ladder_report(ladder: Path) -> dict:
    meta = json.loads((ladder / "ladder.json").read_text())
    steps = ladder_steps(ladder)
    for st_, m in zip(steps, retry_marks(steps, meta["ramp"])):
        st_["retry"] = m
    nxt, why = next_rate(steps, meta["ramp"]) if steps else (None, "no steps")
    return {"workload": meta["workload"], "arm": meta.get("arm", "A"), "latency_gate": meta["ramp"].get("latency_gate", "ack"),
            "ramp": meta["ramp"], "mqtt_load": meta.get("mqtt_load", {}), "steps": steps, "capacity": capacity(steps),
            "ended": why if nxt is None else f"incomplete (next would be {nxt:g} msg/s: {why})"}


def render_ladder_md(r: dict) -> list[str]:
    gate = r["latency_gate"]
    lines = [f"## {r['workload']} (arm {r['arm']})", ""]
    cap = r["capacity"]
    label = "ack" if gate == "ack" else "end-to-end"
    top = cap.get("lossless")
    if top:
        lat = (f"ack p99 {fmt_us(top['ack_p99_us'])}, " if gate == "ack" else "") + f"e2e p99 {fmt_us(top['e2e_p99_us'])}"
        lines.append(f"**Maximum lossless: {fmt_rate(top['offered_rate'])} msg/s** ({lat}).")
    else:
        lines.append("**Maximum lossless: none** — no step was carried.")
    lines.append("")
    for band in BANDS_MS:
        c = cap[f"p99_le_{band}ms"]
        band_txt = "1 s" if band == 1000 else f"{band} ms"
        lines.append(f"- {label} p99 ≤ {band_txt}: " + (f"{fmt_rate(c['offered_rate'])} msg/s ({fmt_us(c['gate_p99_us'])})" if c else "none"))
    lines += [f"- ladder ended: {r['ended']}", ""]
    if r.get("chart"):
        lines += [f"![{r['workload']}: {label} p99 by offered msg/s]({r['chart']})", ""]
    lines.append("| step | offered msg/s | brokers received | delivered | share | loss | ack p50 / p99 / p999 | "
                 "e2e p50 / p99 / p999 | generator health | verdict |")
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for s in r["steps"]:
        a, e = s["ack_latency"], s["e2e_latency"]
        share = "—" if s["received_share"] is None else f"{s['received_share']:.1%}"
        b = s.get("brokers", {})
        drops = b.get("lost", b.get("dropped"))
        loss = fmt_ledger(s) + ("" if not drops else f", drops {int(drops)}")
        dr = s.get("drain") or {}
        if dr.get("queued_at_cutoff"):
            loss += f", queued at cut-off {int(dr['queued_at_cutoff'])}"
        if dr.get("unexplained"):
            loss += f", unexplained {int(dr['unexplained'])}"
        if dr.get("missing_acked") is not None and (dr.get("missing") or 0):
            loss += (f" (of the missing: acknowledged {int(dr['missing_acked'])}, "
                     f"never acknowledged {int(dr['missing_unacked'])})")
        why = "; ".join(s["failures"] + s["voids"] + s["notes"])
        m = s.get("retry")
        tag = ""
        if m:
            tag = {"pending": " (retry pending)",
                   "transient": f" (retried: transient, step {m.get('retry_step')} carried)",
                   "confirmed": f" (retried: bad again at step {m.get('retry_step')}, counts)",
                   "retry": f" (retry of step {m.get('of')})",
                   "not-retried": " (retry-eligible, not retried in this run)"}[m["mark"]]
        lines.append(
            f"| {s['step']} | {fmt_rate(s['offered_rate'])} | {fmt_rate(s['received_rate'])} | "
            f"{fmt_rate(s['delivered_rate'])} | {share} | {loss} | "
            f"{fmt_us(a['p50_us'])} / {fmt_us(a['p99_us'])} / {fmt_us(a['p999_us'])} | "
            f"{fmt_us(e['p50_us'])} / {fmt_us(e['p99_us'])} / {fmt_us(e['p999_us'])} | {fmt_health(s)} | "
            f"**{s['verdict']}**{tag}{(' — ' + why) if why else ''} |")
    lines.append("")
    return lines


def report(run: Path) -> dict:
    meta = json.loads((run / "run.json").read_text()) if (run / "run.json").is_file() else {}
    order = {w: i for i, w in enumerate(meta.get("workloads", []))}
    ladders = sorted((p.parent for p in run.glob("**/ladder.json")),
                     key=lambda d: (d.parent.name, order.get(d.name, len(order)), d.name))
    data = {"schema": SCHEMA_REPORT, "run": meta, "offer_taken": OFFER_TAKEN, "ladders": [ladder_report(p) for p in ladders]}
    try:
        _load("chart_ramp", HERE / "chart-ramp.py").write_charts(run, data)
    except Exception as e:  # a chart must never cost the report
        print(f"verdict.py: charts not drawn: {e!r}", file=sys.stderr)
    (run / "report.json").write_text(json.dumps(data, indent=2) + "\n")
    md = [f"# mqttd msg/s ramp — {meta.get('tier', '?')}", ""]
    if meta:
        md.append(f"- tier `{meta.get('tier')}`: {meta.get('brokers')} brokers ({meta.get('broker_type')}), "
                  f"{meta.get('generators')} generator host(s) ({meta.get('gen_type')})")
        md.append(f"- broker: {meta.get('binary', '?')}")
        md.append(f"- started {meta.get('started_utc', '?')}; steps: {meta.get('warmup_s')} s warm-up, "
                  f"{meta.get('measure_s')} s measured")
        if meta.get("unreleased"):
            md.append("- **UNRELEASED binary**: these figures are attributable to that binary only")
        md.append("")
    md += ["Judged on the broker only (ADR 0090 D1): loss FAILS a step, a saturated generator VOIDS it, the brokers "
           f"must receive ≥ {OFFER_TAKEN:.0%} of the offer, and latency is the exact merge of every generator's "
           "histogram. The ramp climbs on every carried step, whatever its latency; a bad step right above a carried "
           f"step with p99 < {RETRY_P99_MS:g} ms is retried once after a full settle, and counts only if the retry "
           "is bad too (\"retried: transient\" otherwise). **Maximum lossless** is the "
           "highest carried step; the lines below it are the highest carried step within each ack p99 band "
           "(QoS 0: end-to-end p99).", ""]
    for r in data["ladders"]:
        md += render_ladder_md(r)
    (run / "report.md").write_text("\n".join(md))
    return data


# ── cost ────────────────────────────────────────────────────────────────────────────


def cost(tier: str, brokers: int | None = None, gens: int | None = None, broker_type: str | None = None,
         gen_type: str | None = None, workloads: int = 1, steps: int = 9, step_secs: float = 120.0,
         bringup_min: float | None = None, teardown_min: float = 3.0, prices: dict | None = None) -> dict:
    t = TIERS[tier]
    brokers, gens = brokers or t["brokers"], gens or t["gens"]
    bt, gt = broker_type or t["broker_type"], gen_type or t["gen_type"]
    p = {**PRICES_EUR_H, **(prices or {})}
    if bt not in p or gt not in p:
        raise ValueError(f"no price for {bt if bt not in p else gt}; pass --price TYPE=EUR")
    hourly = brokers * p[bt] + gens * p[gt]
    bring = bringup_min if bringup_min is not None else (10.0 if brokers <= 3 else 14.0)
    ramp_min = workloads * steps * step_secs / 60
    total = bring + ramp_min + teardown_min
    return {"tier": tier, "brokers": brokers, "broker_type": bt, "generators": gens, "gen_type": gt,
            "eur_per_hour": round(hourly, 6), "minutes": round(total), "bringup_min": bring, "ramp_min": round(ramp_min, 1),
            "eur_prorated": round(hourly * total / 60, 2), "eur_hours_rounded_up": round(hourly * math.ceil(total / 60), 2)}


# ── CLI ─────────────────────────────────────────────────────────────────────────────


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="ADR 0090 ramp verdicts")
    sub = ap.add_subparsers(dest="cmd", required=True)
    j = sub.add_parser("judge")
    j.add_argument("step")
    n = sub.add_parser("next")
    n.add_argument("ladder")
    r = sub.add_parser("report")
    r.add_argument("run")
    i = sub.add_parser("idle")
    i.add_argument("--floor", type=float, default=0)
    i.add_argument("--baseline-sessions", type=float, default=0)
    i.add_argument("--prev", help="the previous scrape's state (JSON, as this prints): the quiet counters must not "
                   "have moved faster than the background allowance")
    i.add_argument("--step-rate", type=float, help="msg/s of the step to come: sets the background allowance")
    i.add_argument("scrapes", nargs="+")
    c = sub.add_parser("cost")
    c.add_argument("--tier", required=True, choices=sorted(TIERS))
    c.add_argument("--brokers", type=int)
    c.add_argument("--generators", type=int)
    c.add_argument("--broker-type")
    c.add_argument("--gen-type")
    c.add_argument("--workloads", type=int, default=1)
    c.add_argument("--steps", type=int, default=9)
    c.add_argument("--step-secs", type=float, default=120)
    c.add_argument("--price", action="append", default=[], help="TYPE=EUR per hour")
    c.add_argument("--json", action="store_true")
    a = ap.parse_args(argv)
    if a.cmd == "judge":
        step = Path(a.step)
        v = judge(step)
        (step / "verdict.json").write_text(json.dumps(v, indent=2) + "\n")
        print(f"{v['verdict']}{' (good)' if v['good'] else ''}: offered {fmt_rate(v['offered_rate'])} msg/s, brokers "
              f"received {fmt_rate(v['received_rate'])}, ack p99 {fmt_us(v['ack_latency']['p99_us'])}, e2e p99 "
              f"{fmt_us(v['e2e_latency']['p99_us'])}" + "".join(f"\n  - {x}" for x in v["failures"] + v["voids"] + v["notes"]))
        return 0
    if a.cmd == "next":
        ladder = Path(a.ladder)
        meta = json.loads((ladder / "ladder.json").read_text())
        rate, why = next_rate(ladder_steps(ladder), meta["ramp"])
        print(why, file=sys.stderr)
        if rate is not None:
            print(f"{rate:.0f}" if rate == int(rate) else f"{rate:g}")
        return 0
    if a.cmd == "report":
        report(Path(a.run))
        print((Path(a.run) / "report.md").read_text())
        return 0
    if a.cmd == "idle":
        st = idle_state([Path(p) for p in a.scrapes])
        st["at_unix_s"] = round(time.time(), 3)
        prev = None
        if a.prev:
            try:
                prev = json.loads(Path(a.prev).read_text())
            except (OSError, ValueError):
                prev = None
        st["why_not"] = not_idle(st, a.floor, a.baseline_sessions, prev, a.step_rate)
        print(json.dumps(st))
        return 0 if not st["why_not"] else 1
    if a.cmd == "cost":
        prices = {}
        for kv in a.price:
            k, v = kv.split("=", 1)
            prices[k] = float(v)
        c = cost(a.tier, a.brokers, a.generators, a.broker_type, a.gen_type, a.workloads, a.steps, a.step_secs,
                 prices=prices)
        if a.json:
            print(json.dumps(c))
        else:
            print(f"tier {c['tier']}: {c['brokers']} x {c['broker_type']} + {c['generators']} x {c['gen_type']} = "
                  f"EUR {c['eur_per_hour']:.4f}/h; expected ~{c['minutes']} min ({c['bringup_min']:.0f} bring-up + "
                  f"{c['ramp_min']:.0f} ramp for {a.workloads} workload(s) x ~{a.steps} steps of ~{a.step_secs:.0f} s + "
                  f"teardown) = EUR {c['eur_prorated']:.2f} prorated, EUR {c['eur_hours_rounded_up']:.2f} if every "
                  "started hour is billed")
        return 0
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
