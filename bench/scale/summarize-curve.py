#!/usr/bin/env python3
"""Render the scale curve (ADR 0048 T3) from a bench/scale run's raw results.

    ./summarize-curve.py .runs/<stamp>/results

Emits markdown to stdout for hand-transcription into docs/benchmarks/SCALE-CURVE.md —
the raw results stay untracked scratch (the DURABLE-PATH precedent: the doc is the
record, and hand-transcription forces a human read).

Honesty mechanics, enforced here rather than remembered:
  - Curve 1 (durable) REFUSES to render for any size missing its per-host barrier
    probes (DURABLE-PATH.md's prerequisite for interpreting fsync-bound numbers).
  - Lane B latency percentiles are histogram BUCKET UPPER BOUNDS, labeled as such.
    Histograms are merged across drivers, and DIFFERENCED against a post-ramp
    baseline so they describe the measured window rather than the container's
    lifetime (a rung's connect ramp otherwise lands in the published tail).
  - A rung that did not reach its offered rate is flagged OFFER NOT MET and
    excluded from knee detection. It is NOT called driver-limited: lane B's
    publishers are windowed QoS 1, so the shortfall may be the drivers' publish
    timer OR the brokers throttling the window, and this measurement cannot
    tell them apart.
  - Delivery is judged on TOTALS, not on the windowed rates: the publisher and
    subscriber containers do not share a measurement window.
  - Broker-side counter deltas cross-check driver-reported totals; a mismatch
    beyond ±2% is flagged on the row.
  - durable_bench's own verdicts (violations/caveats) are carried verbatim.
"""

from __future__ import annotations

import importlib.util
import json
import re
import statistics
import sys
from pathlib import Path

TOLERANCE = 0.02  # counter cross-check band
DRIVER_OK = 0.97  # a rung counts only if the offered rate was actually reached
LATE_OK = 0.05  # share of publishes behind their own schedule before a rung is flagged
KNEE_OK = 0.99  # delivered/sent ratio a sustained rung must reach
# How far the closing CONTROL rung may drift from the same rung at the start of
# the ladder before the whole trial is inconclusive. Deliberately tight: within
# ONE provisioning this rig is repeatable to <0.2% (0077-T4, PR #489, each rung
# run twice), so 5% is already twenty times the measured noise — a control that
# misses it is telling you the cluster changed under the ladder, not that the rig
# is imprecise.
CONTROL_OK = 0.05
# Seconds at the END of a rung's driver series that count as the measurement.
# The rung's opening seconds are ramp (subscribers first, then publishers), and
# averaging them in is what turned a healthy ladder into a phantom knee.
STEADY_WINDOW = 60


def sizes(root: Path) -> list[tuple[int, Path]]:
    out = []
    for d in root.glob("nodes=*"):
        m = re.match(r"nodes=(\d+)$", d.name)
        if m and d.is_dir():
            out.append((int(m.group(1)), d))
    return sorted(out)


# ── lane A: durable_bench RESULT lines ───────────────────────────────────────


def lane_a_results(size_dir: Path, name: str) -> list[dict]:
    path = size_dir / "laneA" / f"{name}.txt"
    if not path.exists():
        return []
    out = []
    for line in path.read_text(errors="replace").splitlines():
        if line.startswith("RESULT "):
            try:
                out.append(json.loads(line[len("RESULT ") :]))
            except json.JSONDecodeError:
                pass
    return out


def median_over_valid(reps: list[dict], key: str) -> tuple[str, list[str]]:
    """median [min..max] over reps without violations; all verdicts returned."""
    verdicts = []
    vals = []
    for r in reps:
        if r.get("violations"):
            verdicts.append("INVALID: " + "; ".join(r["violations"]))
        else:
            vals.append(float(r[key]))
            if r.get("caveats"):
                verdicts.append("valid — " + "; ".join(r["caveats"]))
    if not vals:
        return "—", verdicts
    med = statistics.median(vals)
    return f"{med:.2f} [{min(vals):.2f}..{max(vals):.2f}]", verdicts


# ── barrier probes ───────────────────────────────────────────────────────────


def probe_floor(size_dir: Path, n: int) -> list[str] | None:
    """Per-broker single-writer barriers/s, or None if any broker lacks a probe."""
    rows = []
    for i in range(n):
        p = size_dir / "probes" / f"broker{i}-device_barrier_floor.txt"
        if not p.exists():
            return None
        rate = None
        for line in p.read_text(errors="replace").splitlines():
            m = re.match(r"\|\s*1\s*\|\s*(\d+)\s*\|", line)
            if m:
                rate = m.group(1)
        if rate is None:
            return None
        rows.append(rate)
    return rows


# ── lane B parsing ───────────────────────────────────────────────────────────


def driver_rate(log: Path, counter: str, window_secs: float = STEADY_WINDOW) -> tuple[int, float]:
    """Diagnostic log rate; all counters use the same clock, including sparse ones.

    New QoS1 runs use aligned REST snapshots for verdicts, not this fallback.
    """
    if not log.exists(): return 0, 0.0
    series = {}; end = 0
    for line in log.read_text(errors="replace").splitlines():
        m = re.match(r"^(?:(\d+)m)?(\d+)s (\w+) total=(\d+) rate=", line)
        if m:
            t = int(m[1] or 0) * 60 + int(m[2]); end = max(end, t)
            series.setdefault(m[3], []).append((t, int(m[4])))
    points = series.get(counter, [])
    if not points: return 0, 0.0
    start = max(0, end - window_secs)
    def at(t): return next((v for ts, v in reversed(points) if ts <= t), 0)
    return points[-1][1], (at(end) - at(start)) / max(end - start, 1)


def qos1_evidence(rdir: Path) -> dict:
    spec = importlib.util.spec_from_file_location("lane_e_evidence", Path(__file__).with_name("lane-e-evidence.py"))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module.validate(rdir)


def driver_span(log: Path, counter: str) -> float:
    """Seconds the container was actually publishing, from its own progress lines.

    The eight counts (#534) are LIFETIME totals — the driver logs are cumulative
    and the broker deltas run before -> after — so the "offered" figure has to be
    over the same span. Multiplying the offered RATE by the measurement window
    instead compares a 15-second number against a 40-second one and renders a
    healthy rung as having sent 2.4x what was asked. Measured on the first real
    run of this table (2026-09-07), which is what it was for.
    """
    if not log.exists():
        return 0.0
    last = 0.0
    for line in log.read_text(errors="replace").splitlines():
        m = re.search(rf"^(?:(\d+)m)?(\d+)s {counter} total=", line)
        if m:
            last = max(last, int(m.group(1) or 0) * 60 + int(m.group(2)))
    return last


def _histogram_of(prom: Path) -> tuple[dict[float, int], int]:
    """One scrape's cumulative buckets and count."""
    buckets: dict[float, int] = {}
    count = 0
    if not prom.exists():
        return buckets, count
    for line in prom.read_text(errors="replace").splitlines():
        m = re.match(r'e2e_latency_bucket\{le="([\d.+eInf]+)"\}\s+(\d+)', line)
        if m:
            le = float("inf") if m.group(1) == "+Inf" else float(m.group(1))
            buckets[le] = buckets.get(le, 0) + int(m.group(2))
        m = re.match(r"e2e_latency_count\s+(\d+)", line)
        if m:
            count += int(m.group(1))
    return buckets, count


def merged_histogram(proms: list[Path]) -> tuple[dict[float, int], int]:
    """Merged latency histogram over the MEASURED window, across drivers.

    emqtt-bench's histogram is cumulative over a container's whole life, so a
    single end-of-rung scrape reports the rung's *lifetime* latency — every
    message delivered while the publishers were still connecting included.
    That reads as a healthy median with a heavy tail, for reasons that have
    nothing to do with the broker's steady state: measured at the 300k rung,
    3000 publishers gave p50 ≤10ms / p99 ≤25ms while 4000 and 6000 — whose
    ramps are longer and busier — both gave p50 ≤25ms / p99 ≤500ms.

    When a rung carries a `-base.prom` baseline (scraped once the ramp has
    settled), the buckets are DIFFERENCED against it, so the percentiles
    describe the same window the throughput numbers do. Rungs recorded before
    the baseline existed fall back to the lifetime histogram, which is all
    they have.
    """
    buckets: dict[float, int] = {}
    count = 0
    for prom in proms:
        # A baseline is not a scrape. `sub-*.prom` also matches `sub-*-base.prom`,
        # so every caller's glob hands this function each rung's baselines as if
        # they were consumer scrapes; each one has no baseline of its own, so its
        # ramp-period buckets were added back whole and half-undid the correction
        # this function exists to make. Measured on the 2026-09-15 pair: the
        # 720k rung read p99 <=2000ms with the baselines counted and <=1000ms
        # without, which is the difference between failing and passing its budget.
        if prom.name.endswith("-base.prom"):
            continue
        after, after_count = _histogram_of(prom)
        base, base_count = _histogram_of(prom.with_name(f"{prom.stem}-base.prom"))
        for le, v in after.items():
            delta = v - base.get(le, 0)
            if delta > 0:
                buckets[le] = buckets.get(le, 0) + delta
        delta_count = after_count - base_count
        if delta_count > 0:
            count += delta_count
    return buckets, count


def bucket_pct(buckets: dict[float, int], count: int, q: float) -> str:
    if not buckets or not count:
        return "—"
    need = q * count
    finite = [le for le in sorted(buckets) if le != float("inf")]
    for le in sorted(buckets):
        if buckets[le] >= need:
            if le == float("inf"):
                # beyond the histogram's resolution — still an upper-bound truth
                return f">{finite[-1]:g}ms" if finite else "inf"
            return f"<={le:g}ms"
    return f">{finite[-1]:g}ms" if finite else "inf"


def counter_delta(rdir: Path, label_from: str, label_to: str, metric: str) -> float:
    """Sum of a counter's delta across every broker's before/after snapshot."""
    total = 0.0
    for before in rdir.glob(f"metrics-{label_from}-broker*.prom"):
        after = rdir / before.name.replace(label_from, label_to)
        if not after.exists():
            continue

        def val(p: Path) -> float:
            v = 0.0
            for line in p.read_text(errors="replace").splitlines():
                if line.startswith(metric):
                    try:
                        v += float(line.split()[-1])
                    except ValueError:
                        pass
            return v

        total += val(after) - val(before)
    return total


def _prom_value(path: Path, metric: str, label: str | None = None) -> dict[str, float]:
    """Values of one metric in one scrape, keyed by `label`'s value ("" if none).

    EXACT name or name-plus-labels, never a prefix. `mqttd_publish_received` and
    `mqttd_publish_received_total` are both exported, so a prefix match on the
    former silently sums two different metrics — the same class of mistake lane D
    documents for `mqttd_backlog_bytes` / `_max`.
    """
    out: dict[str, float] = {}
    if not path.exists():
        return out
    pat = re.compile(rf"^{re.escape(metric)}(?:\{{(?P<labels>[^}}]*)\}})?\s+(?P<v>[-\d.eE+]+)\s*$")
    for line in path.read_text(errors="replace").splitlines():
        m = pat.match(line)
        if not m:
            continue
        key = ""
        if label and m.group("labels"):
            lm = re.search(rf'{re.escape(label)}="([^"]*)"', m.group("labels"))
            key = lm.group(1) if lm else ""
        try:
            out[key] = out.get(key, 0.0) + float(m.group("v"))
        except ValueError:
            pass
    return out


def broker_delta(rdir: Path, a: str, b: str, metric: str, label: str | None = None) -> dict[str, float]:
    """Counter delta between two snapshot labels, summed over brokers, by label value."""
    out: dict[str, float] = {}
    for before in rdir.glob(f"metrics-{a}-broker*.prom"):
        after = rdir / before.name.replace(f"metrics-{a}-", f"metrics-{b}-", 1)
        if not after.exists():
            continue
        va, vb = _prom_value(before, metric, label), _prom_value(after, metric, label)
        for k in set(va) | set(vb):
            out[k] = out.get(k, 0.0) + vb.get(k, 0.0) - va.get(k, 0.0)
    return out


def broker_at(rdir: Path, snap: str, metric: str) -> float:
    """One gauge's value across the cluster at one snapshot — final state, not a delta."""
    return sum(sum(_prom_value(f, metric).values()) for f in rdir.glob(f"metrics-{snap}-broker*.prom"))


def lane_b_rung(rdir: Path, offered: int) -> dict:
    sent = sent_rate = recv = recv_rate = 0.0
    late_rate = 0.0
    for log in rdir.glob("pub-*.log"):
        # emqtt-bench 0.6.3 counts a QoS 0 publish TWICE. `publish/2` increments
        # `pub` when `emqtt:publish` returns a bare `ok` (emqtt_bench.erl:913),
        # then its caller in `loop/5` matches that same `ok` and increments `pub`
        # again (:713). At QoS > 0 the return is `{ok, #{reason_code := ...}}`,
        # so the callee increments `pub_succ` instead and only the caller touches
        # `pub` — one count each.
        #
        # So `pub` alone is 2x the truth at QoS 0 and 1x at QoS 1, which is
        # exactly what the rig measured (2.00x on every QoS 0 rung of the T7
        # probe AND of market-data; 1.00x on every QoS 1 rung). `pub + pub_succ`
        # is therefore 2x in BOTH cases, and half of it is the real rate at
        # either QoS — verified against both arms, 1.00x at every rung.
        #
        # The blast radius is narrow but it hit exactly one real shape.
        # `sustained` below is recv >= KNEE_OK * sent, so a doubled `sent` only
        # breaks the test where recv is CLOSE to sent:
        #
        #   market-data  QoS 0, fan-out 240 subs   recv/sent 240/2 = 120  fine
        #   telematics   QoS 1, $share            recv/sent   1/1 =   1  fine
        #   T7 probe     QoS 0, $share            recv/sent   1/2 = 0.5  BROKEN
        #
        # So no published curve was wrong — fan-out shapes cleared 0.99 even at
        # 2x, and QoS 1 never doubled. But QoS 0 + $share is precisely the SCADA
        # telemetry shape, and on it this module reported "NO sustained rung" for
        # a run whose first three rungs delivered their offer exactly.
        t_pub, r_pub = driver_rate(log, "pub")
        t_succ, r_succ = driver_rate(log, "pub_succ")
        sent += (t_pub + t_succ) // 2
        sent_rate += (r_pub + r_succ) / 2
        # emqtt-bench counts a publish that ran behind its own schedule as
        # pub_overrun; the share of them in the steady window is the direct
        # symptom of the round-trip floor described below.
        _, late = driver_rate(log, "pub_overrun")
        late_rate += late
    for log in rdir.glob("sub-*.log"):
        t, r = driver_rate(log, "recv")
        recv += t
        recv_rate += r
    buckets, count = merged_histogram(sorted(rdir.glob("sub-*.prom")))
    broker_recv = counter_delta(rdir, "before", "after", 'mqttd_publish_received_total{qos="1"}')
    flags = []
    # The rung did not reach its offered rate. Deliberately NOT called
    # "driver-limited": emqtt-bench's TCP publish is SYNCHRONOUS per client
    # (it returns on the PUBACK; `-F` never engages), so every publisher is a
    # window-1 closed loop capped at 1/RTT, and a shortfall has two possible
    # causes — the drivers could not generate the rate (an integer-millisecond
    # timer at a high rung, or too few publishers for the round trip), or the
    # BROKERS acked slowly enough that 1/RTT fell below the per-client rate.
    # The late-publish share below is the symptom either way; the per-rung
    # mpstat samples and the population size are what separate the causes.
    offer_not_met = sent_rate < DRIVER_OK * offered
    if offer_not_met:
        flags.append(f"OFFER NOT MET ({sent_rate / offered * 100:.0f}% of offer)")
    late_share = late_rate / sent_rate if sent_rate else 0.0
    if late_share > LATE_OK:
        flags.append(
            f"PUBLISHERS LATE ({late_share * 100:.0f}% of publishes behind schedule — "
            "synchronous QoS 1 publish caps each client at 1/PUBACK-RTT; more publishers, or a slower broker)"
        )
    if sent and broker_recv and abs(broker_recv - sent) / sent > TOLERANCE:
        flags.append(f"counter mismatch: broker received {broker_recv:.0f} vs driver sent {sent:.0f}")
    return {
        "offered": offered,
        "sent_rate": sent_rate,
        "recv_rate": recv_rate,
        "late_share": late_share,
        # Delivery is judged on TOTALS, never on the windowed rates. The
        # publisher and subscriber containers do not live on the same clock —
        # subscribers start first and are stopped last — so a subscriber's
        # measurement window carries a tail in which the publishers had already
        # stopped. That drags its RATE ~1.5% below the publishers' while the
        # totals match to within 0.1%, which was enough to fail a rung that
        # delivered every message it was sent. Totals have no window to
        # misalign.
        "sustained": (not offer_not_met) and sent > 0 and recv >= KNEE_OK * sent,
        "p50": bucket_pct(buckets, count, 0.50),
        "p99": bucket_pct(buckets, count, 0.99),
        "p999": bucket_pct(buckets, count, 0.999),
        "flags": flags,
    }


# ── lane C parsing ───────────────────────────────────────────────────────────


def rss_kib(path: Path) -> float | None:
    if not path.exists():
        return None
    m = re.search(r"VmRSS:\s*(\d+)\s*kB", path.read_text(errors="replace"))
    return float(m.group(1)) if m else None


def lane_c(size_dir: Path, n: int) -> dict | None:
    cdirs = sorted((size_dir / "laneC").glob("plain-*")) if (size_dir / "laneC").is_dir() else []
    if not cdirs:
        return None
    cdir = cdirs[-1]
    conns = 0
    for log in cdir.glob("conn-*.log"):
        t, _ = driver_rate(log, "connect_succ")
        conns += t
    delta = 0.0
    ok = True
    for i in range(n):
        b = rss_kib(cdir / f"rss-before-broker{i}.txt")
        a = rss_kib(cdir / f"rss-after-broker{i}.txt")
        if b is None or a is None:
            ok = False
            break
        delta += a - b
    return {
        "target": cdir.name.split("-")[1],
        "connected": conns,
        "rss_delta_mib": delta / 1024 if ok else None,
        "kib_per_conn": (delta / conns) if ok and conns else None,
    }


# ── rendering ────────────────────────────────────────────────────────────────


def xychart(title: str, xs: list[int], ys: list[float], ylabel: str) -> str:
    pts = ", ".join(f"{y:.0f}" for y in ys)
    xcat = ", ".join(str(x) for x in xs)
    return (
        "```mermaid\nxychart-beta\n"
        f'  title "{title}"\n'
        f"  x-axis \"broker nodes\" [{xcat}]\n"
        f'  y-axis "{ylabel}"\n'
        f"  line [{pts}]\n```"
    )


def self_test() -> None:
    """Pin the emqtt-bench publish double-count correction (issue: lane B T9).

    Runs against synthesized driver logs — no cluster, no cost — so CI catches a
    regression in the one arithmetic step that decides whether a rung counts as
    sustained. See the long comment in `lane_b_rung` for the upstream cause.
    """
    import tempfile

    def log_for(qos: int, rate: int, secs: int = 70) -> str:
        """Reproduce what emqtt-bench 0.6.3 actually writes.

        QoS 0: `pub` is incremented by BOTH publish/2 and its caller, and
        `pub_succ` is never touched. QoS 1: each increments a different counter,
        so both land on `rate` exactly.
        """
        lines = []
        for t in range(secs + 1):
            stamp = f"{t // 60}m{t % 60}s" if t >= 60 else f"{t}s"
            pub = rate * t * (2 if qos == 0 else 1)
            succ = 0 if qos == 0 else rate * t
            lines.append(f"{stamp} pub total={pub} rate={rate}/sec")
            lines.append(f"{stamp} pub_succ total={succ} rate={rate}/sec")
        return "\n".join(lines) + "\n"

    failures = []
    with tempfile.TemporaryDirectory() as td:
        for qos in (0, 1):
            for rate in (20_000, 50_000, 100_000):
                f = Path(td) / f"pub-q{qos}-{rate}.log"
                f.write_text(log_for(qos, rate))
                t_pub, r_pub = driver_rate(f, "pub")
                t_succ, r_succ = driver_rate(f, "pub_succ")
                got = (r_pub + r_succ) / 2
                if abs(got - rate) > 1:
                    failures.append(f"QoS {qos} @ {rate}/s: corrected rate {got} != {rate}")
                # and the uncorrected read is wrong in exactly the way we claim
                if qos == 0 and abs(r_pub - 2 * rate) > 1:
                    failures.append(f"QoS {qos} @ {rate}/s: expected raw pub to be 2x, got {r_pub}")

    # ── #534: the validity rules, on synthesized rungs ───────────────────────
    #
    # Each case is a rung that MUST NOT be reported as a usable measurement.
    # They are cheap to get wrong in the direction that flatters the broker,
    # which is why they are pinned here rather than left to a reviewer's eye.
    def lane_e_fixture(td: Path, name: str, *, offered: int, sent: int,
                       recv: int, late: int, secs: int = 70,
                       drained: str | None = None, settled: int | None = None,
                       qos: int = 0, sub_qos: int | None = None,
                       settled_state: str = "yes", reset_state: str = "yes",
                       control: bool = False, broker: dict | None = None) -> Path:
        """A lane E rung directory whose driver logs say exactly this.

        `drained` and `settled` describe the DRAIN (#534): `drained` is what the
        harness recorded in rung.txt ("yes" converged, "no" hit the deadline,
        None = a run directory from before the drain existed), and `settled` is
        the consumers' post-drain total in `sub-*.drain`. Leaving both out
        reproduces a pre-drain rung exactly, which is what the older cases below
        rely on.
        """
        d = td / name
        d.mkdir(parents=True)
        drain_meta = "" if drained is None else f" drained={drained} drain_secs=10 drain_deadline_s=60"
        (d / "rung.txt").write_text(
            f"sites=4 offered={offered} per_consumer=100 p99_budget_ms=1000{drain_meta} "
            f"qos={qos} sub_qos={sub_qos if sub_qos is not None else qos} window_secs=60 "
            f"settled={settled_state} settled_conns={9 if settled_state == 'no' else 100} "
            f"expected_conns=100 reset={reset_state} reset_conns={900 if reset_state == 'no' else 4} "
            f"control={'yes' if control else 'no'}\n"
        )
        # Broker-side counters, when a case is about what the CLUSTER saw rather
        # than what a driver did. Absent for the older cases, which is also the
        # shape of a run directory recorded before this accounting existed.
        if broker is not None:
            for snap, mul in (("before", 0), ("drain", 1), ("after", 1)):
                lines = []
                for q, v in broker.get("recv", {}).items():
                    lines.append(f'mqttd_publish_received_total{{qos="{q}"}} {v * mul}')
                for q, v in broker.get("deliv", {}).items():
                    lines.append(f'mqttd_publish_delivered_total{{qos="{q}"}} {v * mul}')
                # `dropped` takes an int (one anonymous reason, as the older cases
                # pass it) or a {reason: count} map, because at QoS 1 WHICH reason
                # moved is the whole point: a fixed broker table is not shedding.
                drops = broker.get("dropped", 0)
                for reason, v in (drops if isinstance(drops, dict) else {"pending-cap": drops}).items():
                    lines.append(f'mqttd_publish_dropped_total{{reason="{reason}"}} {v * mul}')
                lines.append(f"mqttd_sessions {broker.get('sessions', 0) * mul}")
                lines.append(f"mqttd_connections_active {broker.get('conns', 0) * mul}")
                (d / f"metrics-{snap}-broker0.prom").write_text("\n".join(lines) + "\n")
        def counter_log(path: Path, counters: dict[str, int], final: int | None = None) -> None:
            lines = []
            for s in range(secs + 1):
                stamp = f"{s // 60}m{s % 60}s" if s >= 60 else f"{s}s"
                for cname, rate in counters.items():
                    lines.append(f"{stamp} {cname} total={rate * s} rate={rate}/sec")
            if final is not None:
                # One trailing line pins the FINAL TOTAL exactly, which is all
                # `driver_rate` takes from a `.drain` log — its rate is deliberately
                # discarded, since a draining tail is not a rung's rate.
                stamp = f"{(secs + 1) // 60}m{(secs + 1) % 60}s"
                for cname in counters:
                    lines.append(f"{stamp} {cname} total={final} rate=0/sec")
            path.write_text("\n".join(lines) + "\n")
        # `pub` and `pub_succ` are halved by the double-count correction, so a
        # rung that really sent N/s writes N to each.
        counter_log(d / "pub-0.log", {"pub": sent, "pub_succ": sent, "pub_overrun": late})
        counter_log(d / "sub-0.log", {"recv": recv})
        if settled is not None:
            counter_log(d / "sub-0.drain", {"recv": recv}, final=settled)
        # A latency histogram, or every rung reads p99 "—" and fails the budget
        # for want of data rather than for being slow. The names are the ones the
        # harness writes — `sub-<name>-base.prom` at window open, `sub-<name>.prom`
        # at window close — because the callers glob `sub-*.prom`, which matches
        # BOTH, and a fixture that invents its own names cannot catch what that
        # glob does to the baseline (it did not: the ramp counts were added back
        # as a scrape of their own for as long as this fixture used
        # `sub-0-before.prom`). The ramp is slow (<=100ms) and the window is fast
        # (<=10ms), so any rung that counts the baseline reads p99 <=100ms.
        ramp, window = 1_000, 9_000
        (d / "sub-0-base.prom").write_text(
            "\n".join([f'e2e_latency_bucket{{le="10.0"}} 0',
                       f'e2e_latency_bucket{{le="100.0"}} {ramp}',
                       f'e2e_latency_bucket{{le="+Inf"}} {ramp}',
                       f"e2e_latency_count {ramp}"]) + "\n")
        (d / "sub-0.prom").write_text(
            "\n".join([f'e2e_latency_bucket{{le="10.0"}} {window}',
                       f'e2e_latency_bucket{{le="100.0"}} {ramp + window}',
                       f'e2e_latency_bucket{{le="+Inf"}} {ramp + window}',
                       f"e2e_latency_count {ramp + window}"]) + "\n")
        return d

    with tempfile.TemporaryDirectory() as td:
        root = Path(td)

        # 1. UNDER-OFFER: the drivers never reached the rate the rung claims.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-under", offered=30_000,
                                       sent=20_000, recv=20_000, late=0))
        if r["pass"] or not any("OFFER NOT MET" in f for f in r["flags"]):
            failures.append(f"under-offer rung was not rejected: {r['flags']}")

        # 2. LATE PUBLISHERS while MEETING the offer on average. This is the one
        #    a rate check cannot see: the average is fine, but a third of the
        #    publishes missed their own schedule, so the rung measures the
        #    drivers. Lane B has flagged this since it was written; lane E did
        #    not until #534, and lane E is the SCADA ladder.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-late", offered=30_000,
                                       sent=30_000, recv=30_000, late=10_000))
        if r["pass"] or not any("PUBLISHERS LATE" in f for f in r["flags"]):
            failures.append(
                f"rung with 33% late publishes was accepted: flags={r['flags']}"
            )

        # 3. LOSS on a PRE-DRAIN run directory: rejected, but the flag has to say
        #    that pending and dropped were never separable there rather than
        #    assert a broker defect the run cannot support.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-loss", offered=30_000,
                                       sent=30_000, recv=15_000, late=0))
        if r["pass"] or not any("LOSS" in f for f in r["flags"]):
            failures.append(f"lossy rung was not rejected: {r['flags']}")
        if not any("predates" in f for f in r["flags"]):
            failures.append(
                f"a pre-drain rung claimed loss without disclosing it could not tell "
                f"pending from dropped: {r['flags']}"
            )
        if r.get("unresolved"):
            failures.append("a pre-drain rung was reported UNRESOLVED rather than loss")

        # 3a. PENDING IS NOT LOSS. The steady window ends 10% short, the drain
        #     then converges and every message arrives. Under the teardown this
        #     replaces — publishers and consumers killed in the same batch — this
        #     rung read as 10% LOSS and was rejected. It is a clean rung.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-pending", offered=30_000,
                                       sent=30_000, recv=27_000, late=0,
                                       drained="yes", settled=30_000 * 70))
        if not r["pass"] or r["flags"]:
            failures.append(
                f"a rung whose backlog DRAINED was still rejected: {r['flags']}"
            )

        # 3b. UNRESOLVED: the deadline expired with traffic still outstanding.
        #     Not a pass, and specifically NOT a loss finding — the rung stopped
        #     watching, which settles nothing about the broker either way.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-unresolved", offered=30_000,
                                       sent=30_000, recv=20_000, late=0,
                                       drained="no", settled=24_000 * 70))
        if r["pass"] or not r.get("unresolved"):
            failures.append(f"a rung that hit the drain deadline was not UNRESOLVED: {r}")
        if not any("UNRESOLVED" in f for f in r["flags"]):
            failures.append(f"deadline-expired rung carried no UNRESOLVED flag: {r['flags']}")
        if any("LOSS" in f for f in r["flags"]):
            failures.append(
                f"traffic still pending at the deadline was reported as broker LOSS: {r['flags']}"
            )
        # The shortfall must be COUNTED as pending, not merely described as
        # unresolved in prose: "still-pending" is one of the eight counts #534
        # asks for, and a table that renders it as 0 is the same lie the LOSS
        # flag used to tell, told quietly.
        if r["counts"]["pending"] != r["counts"]["sent"] - r["counts"]["aggregate_delivered"]:
            failures.append(f"pending was not counted at the drain deadline: {r['counts']}")
        # ... and a rung that DID drain owes nothing, so its pending is zero.
        rd = lane_e_rung(lane_e_fixture(
            root, "sites-4-pending-zero", offered=30_000, sent=30_000, recv=27_000, late=0,
            drained="yes", settled=30_000 * 70))
        if rd["counts"]["pending"] != 0:
            failures.append(f"a drained rung still reported pending traffic: {rd['counts']}")

        # 3c. REAL LOSS: the drain converged — the consumers went a whole poll
        #     interval receiving nothing — and the broker still owed 20%. That
        #     one IS a broker finding, and must not be softened to UNRESOLVED.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-dropped", offered=30_000,
                                       sent=30_000, recv=20_000, late=0,
                                       drained="yes", settled=24_000 * 70))
        if r["pass"] or r.get("unresolved"):
            failures.append(f"a drained rung short 20% was not reported as loss: {r}")
        # "LOSS" alone is too weak an assertion here: the pre-drain branch says
        # LOSS too, and says it while disclaiming that it could tell pending from
        # dropped. A rung that DID drain has to be described as one — otherwise a
        # regression that stops reading `drained` looks identical to this case.
        if not any("LOSS" in f and "DRAINED" in f for f in r["flags"]):
            failures.append(
                f"a drained rung short 20% was not reported as loss AFTER a drain: {r['flags']}"
            )

        # 4. The CONTROL: a clean rung must still pass, or the rules above are
        #    just a way of never reporting anything.
        r = lane_e_rung(lane_e_fixture(root, "sites-4-clean", offered=30_000,
                                       sent=30_000, recv=30_000, late=0))
        if not r["pass"] or r["flags"]:
            failures.append(f"a clean rung was rejected: {r['flags']}")
        # The percentiles describe the WINDOW, not the ramp. The fixture's ramp
        # sits in <=100ms and its window in <=10ms, so p50/p99 must both read
        # <=10ms: reading <=100ms means the baseline was counted as a scrape of
        # its own, which is what `sub-*.prom` globbing the baselines did until
        # merged_histogram skipped them (the 2026-09-15 720k rung read <=2000ms
        # that way and <=1000ms once corrected — a failed budget turned into a
        # passed one).
        if (r["p99"], r["p50"]) != ("<=10ms", "<=10ms"):
            failures.append(
                f"window percentiles include the ramp: p50 {r['p50']} p99 {r['p99']} (expected <=10ms)")
        # A rung that met its offer exactly must ACCOUNT for it exactly. `offered`
        # has to be taken over the publishers' own run span, not the measurement
        # window: the first real run of this table (2026-09-07) rendered a healthy
        # rung as 2.4x over-sent because a 15s window was compared against 40s of
        # cumulative driver counters.
        if r["counts"]["offered"] != r["counts"]["sent"]:
            failures.append(
                f"a rung that met its offer did not balance: offered "
                f"{r['counts']['offered']} vs sent {r['counts']['sent']}"
            )

        # ── the remaining acceptance cases (#534) ────────────────────────────
        # "Local test fixtures expose under-offer, duplicate delivery, invalid
        # QoS downgrade, stale backlog and failed controls as invalid
        # measurements." Under-offer and stale backlog are cases 1-3 above.

        # 6. DUPLICATE DELIVERY at QoS 0. The broker put more on the wire than a
        #    consumer received, which at-most-once must never do. Visible only by
        #    reading the broker's delivered counter against the driver's recv.
        clean_broker = {"recv": {"0": 30_000 * 70}, "deliv": {"0": 30_000 * 70}, "sessions": 100, "conns": 100}
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-dup", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70,
            broker={"recv": {"0": 30_000 * 70}, "deliv": {"0": 33_000 * 70}, "sessions": 100, "conns": 100}))
        if not any("DUPLICATE DELIVERY" in f for f in r["flags"]):
            failures.append(f"duplicate delivery at QoS 0 was not flagged: {r['flags']}")
        if r["counts"]["duplicate"] is not None:
            failures.append(f"unverified duplicate count was reported: {r['counts']}")

        # 5b. NOT STEADY is a capacity verdict. Lane E gated "everyone connected"
        #     and "everything drained" and nothing between, so the window could
        #     open while the broker was still repaying what the ramp owed — at
        #     N=3/4 sites on 2026-09-18 that was 372,025 messages, and the rung's
        #     p99 (<=7500ms) was the age of traffic published before the window.
        #     A rung that never settles around its own offer is not a measurement
        #     of that rung, whichever side of the band it sat on.
        for reason, expect in (("behind", "below offer"), ("repaying", "above offer")):
            d = root / ("sites-4-notsteady-" + reason)
            lane_e_fixture(d.parent, d.name, offered=30_000, sent=30_000, recv=30_000, late=0,
                           drained="yes", settled=30_000 * 70)
            txt = (d / "rung.txt").read_text()
            (d / "rung.txt").write_text(txt.rstrip() + " steady=no steady_reason=%s steady_s=180\n" % reason)
            r = lane_e_rung(d)
            if r["pass"]:
                failures.append("a rung that never reached steady state passed: %s" % r["flags"])
            if not any("NOT STEADY" in f and expect in f for f in r["flags"]):
                failures.append("NOT STEADY (%s) was not explained: %s" % (reason, r["flags"]))
        # and a rung that DID settle carries the evidence, which is what a reader
        # needs to believe the latency beside it.
        d = root / "sites-4-steady"
        lane_e_fixture(d.parent, d.name, offered=30_000, sent=30_000, recv=30_000, late=0,
                       drained="yes", settled=30_000 * 70)
        txt = (d / "rung.txt").read_text()
        (d / "rung.txt").write_text(txt.rstrip() + " steady=yes steady_reason=none steady_s=25\n")
        r = lane_e_rung(d)
        if not r["pass"]:
            failures.append("a steady rung was failed: %s" % r["flags"])
        if (r.get("steady"), r.get("steady_s")) != ("yes", "25"):
            failures.append("the catch-up evidence did not reach the report: %s" % r)

        # Lateness alone must not assign a cause at either QoS.
        for level, expect, forbid in (("0", "not isolated", "BROKER's ack"),
                                      ("1", "not isolated", "BROKER's ack")):
            r = lane_e_rung(lane_e_fixture(
                root, "sites-4-late-q%s" % level, offered=30_000, sent=30_000, recv=30_000,
                late=9_000, drained="yes", settled=30_000 * 70, qos=level, sub_qos=level))
            late = [f for f in r["flags"] if "PUBLISHERS LATE" in f]
            if not late:
                failures.append("a late-publisher rung at QoS %s was not flagged: %s" % (level, r["flags"]))
            elif expect not in late[0] or forbid in late[0]:
                failures.append("QoS %s lateness blamed the wrong component: %s" % (level, late[0]))

        # 6b. REDELIVERY at QoS 1 is reported, never gated. At-least-once may
        #     legitimately put a message on the wire twice, so the QoS 0 defect
        #     check is correctly skipped — but skipping it left the QoS 1 arm with
        #     no signal at all, and a rung delivering 10% more than the
        #     application saw read as perfectly clean.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-redeliv", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, qos="1", sub_qos="1",
            broker={"recv": {"1": 30_000 * 70}, "deliv": {"1": 33_000 * 70}, "sessions": 100, "conns": 100}))
        if not any("REDELIVERY at QoS 1" in f for f in r["flags"]):
            failures.append(f"redelivery at QoS 1 was not reported: {r['flags']}")
        if r["pass"]:
            failures.append("legacy QoS1 evidence without a ledger passed")

        # 6c. A BROKER BOUND is not load shedding. `dropped` is summed for the
        #     accounting table, so a rung that hit PENDING_PUBLISH_CAP looked
        #     exactly like one that shed under pressure — and at QoS 1 that cap is
        #     the bound lane E's own shape check predicts.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-cap", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, qos="1", sub_qos="1",
            broker={"recv": {"1": 30_000 * 70}, "deliv": {"1": 30_000 * 70},
                    "dropped": {"pending-cap": 4_096}, "sessions": 100, "conns": 100}))
        if not any("BROKER BOUND REACHED" in f and "pending-cap" in f for f in r["flags"]):
            failures.append(f"a rung that reached PENDING_PUBLISH_CAP was not flagged: {r['flags']}")
        # and ordinary shedding must NOT be dressed up as a fixed bound
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-shed", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, qos="1", sub_qos="1",
            broker={"recv": {"1": 30_000 * 70}, "deliv": {"1": 30_000 * 70},
                    "dropped": {"outbound-full": 4_096}, "sessions": 100, "conns": 100}))
        if any("BROKER BOUND REACHED" in f for f in r["flags"]):
            failures.append(f"load shedding was reported as a fixed broker bound: {r['flags']}")

        # 7. INVALID QoS DOWNGRADE. The subscriber asked for QoS 2; every
        #    delivery went out labelled QoS 1. The rung is measuring a different
        #    protocol than its own directory claims.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-downgrade", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, qos=2, sub_qos=2,
            broker={"recv": {"2": 30_000 * 70}, "deliv": {"1": 30_000 * 70}, "sessions": 100, "conns": 100}))
        if r["pass"] or not any("QOS DOWNGRADE" in f for f in r["flags"]):
            failures.append(f"a QoS 2 subscription served at QoS 1 was not flagged: {r['flags']}")

        # 8. QoS 2 COMPLETION IS NOT MEASURABLE, and the rung must say so rather
        #    than report `sent` as completion — the driver counts at PUBREC and
        #    the broker exports no PUBCOMP counter.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-qos2", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, qos=2, sub_qos=2,
            broker={"recv": {"2": 30_000 * 70}, "deliv": {"2": 30_000 * 70}, "sessions": 100, "conns": 100}))
        if r["counts"]["protocol_completed"] is not None:
            failures.append(
                f"QoS 2 reported a protocol-completion count that nothing can measure: {r['counts']}"
            )
        if r["pass"] or not any("QOS2 COMPLETION UNVERIFIABLE" in f for f in r["flags"]):
            failures.append(f"a QoS 2 rung passed without certifiable completion: {r['flags']}")

        # 9. QoS 1 completion IS measurable — `pub_succ` fires on PUBACK — so the
        #    rule above must not simply refuse every acknowledged QoS.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-qos1", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, qos=1, sub_qos=1,
            broker={"recv": {"1": 30_000 * 70}, "deliv": {"1": 30_000 * 70}, "sessions": 100, "conns": 100}))
        if r["pass"] or r["counts"]["protocol_completed"] != r["counts"]["sent"]:
            failures.append(f"legacy QoS 1 completion or fail-closed behavior incorrect: {r['flags']} {r['counts']}")

        # 10. UNSETTLED: the measurement window opened before the clients
        #     arrived, so the rung measures a cluster still filling up.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-unsettled", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, settled_state="no", broker=clean_broker))
        if r["pass"] or not any("UNSETTLED" in f for f in r["flags"]):
            failures.append(f"a rung measured mid-ramp was accepted: {r['flags']}")

        # 11. UNRESET: the previous rung's connections were still on the cluster,
        #     so this one measures residual overload as well as its own load.
        r = lane_e_rung(lane_e_fixture(
            root, "sites-4-unreset", offered=30_000, sent=30_000, recv=30_000, late=0,
            drained="yes", settled=30_000 * 70, reset_state="no", broker=clean_broker))
        if r["pass"] or not any("UNRESET" in f for f in r["flags"]):
            failures.append(f"a rung on an un-reset cluster was accepted: {r['flags']}")

        # 12. FAILED CONTROLS make the whole TRIAL inconclusive — not the control
        #     rung alone. These are ladder-level rules, so they run against
        #     `lane_e_ladder` directly.
        def rung(sites, *, ok=True, rate=30_000.0, control=False, unresolved=False):
            return {"sites": sites, "rep": 2 if control else 1, "pass": ok, "flags": [] if ok else ["LOSS"],
                    "recv_rate": rate, "offered": 30_000.0, "budget_ms": 1000.0,
                    "control": control, "unresolved": unresolved}

        v = lane_e_ladder([rung(2), rung(4), rung(2, ok=False, control=True)])
        if v["claim"] or "did not pass" not in v["inconclusive"]:
            failures.append(f"a FAILED control still produced a capacity claim: {v}")
        v = lane_e_ladder([rung(2), rung(4), rung(2, rate=24_000.0, control=True)])
        if v["claim"] or "drift" not in v["inconclusive"]:
            failures.append(f"a control that drifted 20% still produced a capacity claim: {v}")
        v = lane_e_ladder([rung(2), rung(4)])
        if v["claim"] or "no control rung" not in v["inconclusive"]:
            failures.append(f"a ladder with no control at all still claimed capacity: {v}")
        # And the control that PASSES and matches must let the claim through, or
        # the rule is just a way of never reporting a site count.
        v = lane_e_ladder([rung(2), rung(4), rung(2, control=True)])
        if not v["claim"] or v["claim"]["sites"] != 4 or v["inconclusive"]:
            failures.append(f"a healthy ladder with a matching control claimed nothing: {v}")

        # 5. A rung still in flight is INCOMPLETE, never a failed one.
        d = root / "sites-4-live"
        d.mkdir()
        r = lane_e_rung(d)
        if not r.get("incomplete") or r["pass"]:
            failures.append(f"an in-flight rung was not reported as incomplete: {r}")

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        sys.exit(1)
    print(
        "summarize-curve self-test: publish double-count correction OK (6 cases); "
        "lane E validity OK (15 rungs + 4 ladders — under-offer, late publishers, "
        "loss on a pre-drain directory, a backlog that DRAINED and must pass, an "
        "expired drain deadline that must read UNRESOLVED rather than loss, real "
        "loss after a converged drain, duplicate delivery at QoS 0, a QoS 2 "
        "subscription served at QoS 1, QoS 2 completion that nothing can certify, "
        "a QoS 1 rung whose completion IS measurable, a rung measured mid-ramp, a "
        "rung on an un-reset cluster, a clean control rung, an in-flight rung; and "
        "at ladder level a failed control, a drifted control, a missing control "
        "and a healthy ladder that must still claim its site count)"
    )


def p99_ms(label: str) -> float:
    """The numeric upper bound behind a bucket_pct label ('<=1ms' -> 1.0).

    Returns inf for '—' and for a '>N' label: both mean "past the last finite
    bucket", and a budget verdict must treat that as over, never as N.
    """
    if not label or label == "—" or label.startswith(">"):
        return float("inf")
    return float(label.lstrip("<=").rstrip("ms"))


def lane_e_ladder(rungs: list[dict]) -> dict:
    """Whether a lane E ladder establishes a site count, and why not when it doesn't.

    Pure, and separate from the printing, because these are the rules #534 asks
    to be provable: a ladder can fail to be a measurement in four different ways
    and each has to be distinguishable from "the cluster ran out of capacity".

      flaky         a site count that passed once and failed on a repeat
      unresolved    a rung whose drain deadline expired with traffic outstanding
      inconclusive  the closing CONTROL disagrees with the ladder's own start
      claim         the highest site count every run of which passed, or None
    """
    by_count: dict[int, list[dict]] = {}
    for r in rungs:
        by_count.setdefault(r["sites"], []).append(r)
    flaky = sorted(c for c, rs in by_count.items() if any(x["pass"] for x in rs) and not all(x["pass"] for x in rs))
    passed = [rs[0] for c, rs in by_count.items() if all(x["pass"] for x in rs)]
    unresolved = sorted(c for c, rs in by_count.items() if any(x.get("unresolved") for x in rs))

    # ── the CONTROL rung (#534, acceptance 6) ───────────────────────────────
    # A ladder is a sequence of trials on ONE cluster, so every rung is
    # confounded by the ones before it: a broker that degraded, or never
    # recovered from a rung that overloaded it, makes the ladder report residual
    # overload as capacity running out at a site count. The control is the bottom
    # rung run again at the end — the lowest load offered, so the one a healthy
    # cluster must still carry. If it does not, nothing above it is a capacity
    # finding, and the issue is explicit that such a trial is reported as
    # INCONCLUSIVE rather than as a limit.
    controls = [r for r in rungs if r.get("control")]
    inconclusive = ""
    if controls:
        ctl = controls[-1]
        if not ctl["pass"]:
            inconclusive = (
                f"the closing control at {ctl['sites']} site(s) did not pass "
                f"({'; '.join(ctl['flags']) or 'no reason recorded'})"
            )
        else:
            base = [r for r in rungs if r["sites"] == ctl["sites"] and not r.get("control")]
            if base and base[0]["recv_rate"] > 0:
                drift = abs(ctl["recv_rate"] - base[0]["recv_rate"]) / base[0]["recv_rate"]
                if drift > CONTROL_OK:
                    inconclusive = (
                        f"the control at {ctl['sites']} site(s) delivered {ctl['recv_rate']:,.0f}/s "
                        f"against {base[0]['recv_rate']:,.0f}/s for the same rung before the ladder — "
                        f"{drift * 100:.1f}% drift, past the {CONTROL_OK * 100:.0f}% bound"
                    )
    elif len(by_count) > 1:
        inconclusive = (
            "no control rung was run, so nothing distinguishes this cluster's capacity "
            "from residual overload accumulated across the ladder (set LANE_E_CONTROL=1)"
        )

    claim = None if inconclusive else (max(passed, key=lambda r: r["sites"]) if passed else None)
    # A capacity claim above an inconsistent rung is not supportable: the cluster
    # demonstrably failed at a LOWER count, so a higher one cannot be its
    # capacity. An UNRESOLVED rung below it blocks the claim for the weaker but
    # sufficient reason that the ladder has a hole there.
    blocked_by = ""
    if claim and any(c < claim["sites"] for c in unresolved):
        blocked_by = (
            f"{', '.join(str(c) for c in unresolved if c < claim['sites'])} site(s) below it went "
            "UNRESOLVED, so the ladder has a hole under the number. Repeat those rungs with a "
            "longer drain."
        )
    elif claim and any(c <= claim["sites"] for c in flaky):
        blocked_by = (
            f"{', '.join(str(c) for c in flaky if c <= claim['sites'])} did not pass consistently "
            "below it — a cluster that fails at a lower count has not established a higher one. "
            "Repeat the rungs until the spread is inside the budget, or widen the budget to "
            "something the spread fits."
        )
    if blocked_by:
        claim = None
    return {
        "by_count": by_count,
        "flaky": flaky,
        "unresolved": unresolved,
        "inconclusive": inconclusive,
        "blocked_by": blocked_by,
        "claim": claim,
        "passed_any": bool(passed),
    }


def lane_e_rung(rdir: Path) -> dict:
    """One site-ladder rung. Same counters as lane B, keyed by tenant count.

    The rung's own metadata is read from rung.txt rather than re-derived: the
    lane knows its site rate and consumer count, and a summarizer that guesses
    them would drift from the harness the first time a knob changes.
    """
    meta = {}
    rt = rdir / "rung.txt"
    if rt.exists():
        for tok in rt.read_text().split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                meta[k] = v
    else:
        # rung.txt is the LAST thing the lane writes, so its absence means the
        # rung is still in flight (or died). Without this, a rung being watched
        # live reads as offered=0 and p99="—", which the budget check below then
        # reports as OVER BUDGET — a running rung looking like a failed one.
        return {
            # `sites-<n>` or `sites-<n>-rep<k>`: the count is the SECOND token, not
            # the last one, since a repeated rung carries a suffix.
            "sites": int(rdir.name.split("-")[1]),
            # The repeat index belongs here too: without it an in-flight REPEAT
            # renders as run 1 and appears to contradict the completed run 1
            # sitting beside it in the table.
            "rep": (
                int(rdir.name.split("-")[2][3:])
                if len(rdir.name.split("-")) > 2 and rdir.name.split("-")[2].startswith("rep")
                else 1
            ),
            "offered": 0.0,
            "sent_rate": 0.0,
            "recv_rate": 0.0,
            "per_consumer": 0.0,
            "p50": "—",
            "p99": "—",
            "budget_ms": 0.0,
            "pass": False,
            "incomplete": True,
            "flags": ["INCOMPLETE (no rung.txt — still running, or the rung died)"],
        }
    sites = int(meta.get("sites", rdir.name.split("-")[1]))
    parts = rdir.name.split("-")
    rep = int(parts[2][3:]) if len(parts) > 2 and parts[2].startswith("rep") else 1
    offered = float(meta.get("offered", 0))
    budget = float(meta.get("p99_budget_ms", 1000))

    sent_rate = recv_rate = 0.0
    sent = recv = 0.0
    late_rate = 0.0
    for log in rdir.glob("pub-*.log"):
        # Same emqtt-bench double-count correction as lane B — see lane_b_rung.
        t_pub, r_pub = driver_rate(log, "pub", float(meta.get("window_secs", 60)))
        t_succ, r_succ = driver_rate(log, "pub_succ", float(meta.get("window_secs", 60)))
        sent += (t_pub + t_succ) // 2
        sent_rate += (r_pub + r_succ) / 2
        # #534: lane B has flagged late publishers since it was written; this
        # lane never did, and this is the lane whose runs the scaling review
        # took apart. `pub_overrun` is the driver saying, itself, that it could
        # not keep its own schedule — a direct signal where OFFER NOT MET is an
        # inference from a rate against a 0.97 threshold. A rung can even MEET
        # its offer on average while a third of its publishes ran late, and only
        # this counter says so.
        _, late = driver_rate(log, "pub_overrun", float(meta.get("window_secs", 60)))
        late_rate += late
    for log in rdir.glob("sub-*.log"):
        t, r = driver_rate(log, "recv", float(meta.get("window_secs", 60)))
        recv += t
        recv_rate += r
    # #534: the delivery question and the rate question need DIFFERENT reads of
    # the same containers. The RATE comes from the steady window (`sub-*.log`,
    # dumped while the publishers were still running); the DELIVERY TOTAL comes
    # from after the drain (`sub-*.drain`, dumped once the publishers stopped and
    # the consumers were given a bounded deadline to finish). Reading the rate
    # from the drained log would average a decaying tail into the rung and
    # understate every one of them.
    #
    # A run directory recorded before the drain existed has no `.drain` files at
    # all, so fall back to the steady-window total: those runs stay readable, and
    # `drained` below is "" for them, which is its own verdict. `max` rather than
    # a plain substitution because `recv` is CUMULATIVE and read twice — the later
    # read can only be larger, so max is a no-op on any real pair and a floor
    # under a drain dump that came back partial.
    settled = max(recv, sum(driver_rate(log, "recv", float(meta.get("window_secs", 60)))[0] for log in rdir.glob("sub-*.drain")))
    buckets, count = merged_histogram(sorted(rdir.glob("sub-*.prom")))
    evidence = None; evidence_error = None
    if meta.get("qos", "0") == "1":
        try:
            evidence = qos1_evidence(rdir)
            sent_rate = evidence["sent_rate"]; recv_rate = evidence["recv_rate"]
            late_rate = evidence["late_share"] * sent_rate
            sent = evidence["counts"]["sent"]; settled = evidence["counts"]["aggregate_delivered"]
            buckets = evidence["buckets"]; count = evidence["histogram_count"]
        except (ValueError, OSError, KeyError, IndexError) as exc:
            evidence_error = str(exc)
    p99 = bucket_pct(buckets, count, 0.99)

    # Protocol metadata determines validation and accounting semantics.
    qos = meta.get("qos", "0")
    sub_qos = meta.get("sub_qos", qos)

    flags = []
    if evidence_error: flags.append("INVALID EVIDENCE (" + evidence_error + ")")
    if evidence and evidence.get("telemetry") and not evidence["telemetry"]["queues_bounded"]:
        flags.append("QUEUE GROWTH OR BACKLOG BUDGET EXCEEDED")
    # A failed gate excludes a capacity point; it does not identify the cause.
    steady = meta.get("steady", "")
    if steady == "no":
        direction = {"behind": "below offer", "repaying": "above offer"}.get(meta.get("steady_reason"), "unclassified")
        flags.append(
            f"NOT STEADY (last aggregate sample {direction}; per-site delivery did not remain within the offer band; "
            "publisher pacing, scrape timing and broker service are not isolated)"
        )

    offer_met = offered and sent_rate >= DRIVER_OK * offered
    if offered and not offer_met:
        flags.append(f"OFFER NOT MET ({sent_rate / offered * 100:.0f}% of offer)")
    late_share = late_rate / sent_rate if sent_rate else 0.0
    if late_share > LATE_OK:
        why = "end-to-end publisher timing; scheduling, network and broker service are not isolated"
        flags.append(
            f"PUBLISHERS LATE ({late_share * 100:.0f}% of publishes behind schedule — {why})"
        )
    # WINDOW CONTAMINATED. Delivering MORE than was offered during the window is
    # not capacity: the surplus was published before the window opened and
    # arrives carrying its full age, which lands in this rung's tail.
    #
    # This is a backstop for GROSS leakage, and it would not have caught the case
    # that motivated it: at 4 sites on 2026-09-18 the surplus was 210,264
    # messages repaid in six seconds, which is only 1.4% spread across a 61s
    # window — under any sane tolerance. No aggregate test sees a short burst in
    # a long window. What prevents that case is the steady-state gate, which
    # refuses to OPEN the window until delivery sits inside a band around the
    # offer; `steady=yes steady_s=N` in rung.txt is the evidence that it did.
    if offered and recv_rate > (1 + TOLERANCE) * offered:
        flags.append(
            f"WINDOW CONTAMINATED (delivered {recv_rate / offered * 100:.0f}% of the offer — the "
            "surplus was published before the window and carries its age into this rung's latency)"
        )

    # PENDING IS NOT LOSS (#534, acceptance 7). A shortfall means one of three
    # things — the broker dropped it, the broker still holds it, or it was in
    # flight — and only the first is a finding about the broker. Before the drain
    # existed the publishers and consumers were killed in the same batch, so
    # every rung's in-flight tail was booked as loss by construction.
    #
    # `drained` from rung.txt is what separates them: "yes" means the consumers
    # went a whole poll interval receiving nothing, so the shortfall is real
    # loss; "no" means the deadline expired with traffic still moving, which is
    # UNRESOLVED and settles nothing either way; "" is a pre-drain run directory,
    # where the two are simply not distinguishable.
    drained = meta.get("drained", "")
    deadline = meta.get("drain_deadline_s", "?")
    delivered = sent > 0 and settled >= KNEE_OK * sent
    unresolved = False
    if not delivered and sent > 0:
        short = (sent - settled) / sent * 100
        if drained == "yes":
            flags.append(
                f"LOSS ({short:.1f}% of what was published never arrived, and the rung "
                f"DRAINED — the consumers stopped receiving while the broker still owed it)"
            )
        elif drained == "no":
            unresolved = True
            flags.append(
                f"UNRESOLVED ({short:.1f}% still undelivered when the {deadline}s drain "
                "deadline expired — pending or dropped, and this rung cannot tell which)"
            )
        else:
            flags.append(
                f"LOSS ({short:.1f}% of what was published never arrived; run directory "
                "predates the drain deadline, so pending traffic here is indistinguishable "
                "from dropped)"
            )
    within = p99_ms(p99) <= budget
    if not within:
        flags.append(f"OVER P99 BUDGET ({p99} > {budget:g}ms)")

    # ── the eight counts (#534, acceptance 3) ────────────────────────────────
    #
    # "Distinguish client completion from durable acceptance and application
    # delivery." They are three different numbers and the rig reported one. Each
    # comes from the side that can actually see it:
    #
    #   offered              what the rung ASKED for       rung.txt x window
    #   sent                 what the drivers got away     driver pub counters
    #   broker_received      what the cluster ACCEPTED     broker, by QoS
    #   protocol_completed   what finished its handshake   see below
    #   uniquely_delivered   exact identities across the whole shared group
    #   duplicate            aggregate receipts minus unique receipt identities
    #   dropped              what the broker refused       broker, by reason
    #   pending              what was still owed at the deadline
    # The span every count below is taken over: how long the PUBLISHERS actually
    # ran, read from their own logs rather than from the configured window, which
    # covers only the middle of it. See `driver_span`.
    span = max((driver_span(log, "pub") for log in rdir.glob("pub-*.log")), default=0.0)
    recv_by_qos = broker_delta(rdir, "before", "after", "mqttd_publish_received_total", "qos")
    deliv_by_qos = broker_delta(rdir, "before", "after", "mqttd_publish_delivered_total", "qos")
    dropped_by_reason = broker_delta(rdir, "before", "after", "mqttd_publish_dropped_total", "reason")
    broker_recv = sum(recv_by_qos.values())
    broker_deliv = sum(deliv_by_qos.values())
    dropped = sum(dropped_by_reason.values())

    # PROTOCOL COMPLETION is measurable at QoS 1 and NOT at QoS 2.
    #
    # At QoS 1 the driver's `pub_succ` fires on PUBACK, which IS completion. At
    # QoS 2 `emqtt` fires the publish callback in `ack_inflight(?PUBREC_PACKET)`
    # and evaluates no callback at all on PUBCOMP, so the driver's counters stop
    # one round trip short of the exactly-once handshake — and the broker exports
    # no PUBCOMP counter either (`mqttd_publish_received_total`,
    # `_delivered_total`, `_dropped_total`, `mqttd_sessions`,
    # `mqttd_connections_active` are the whole relevant surface). Neither end can
    # see it, so it is reported as unknown rather than approximated by `sent`,
    # which would overcount exactly where #534 exists to prevent overcounting.
    completed: float | None
    if qos == "0":
        completed = None  # QoS 0 has no acknowledgement to complete
    elif qos == "1":
        completed = sent
    else:
        completed = None
        flags.append(
            "QOS2 COMPLETION UNVERIFIABLE (the driver counts a QoS 2 publish at "
            "PUBREC, not PUBCOMP, and the broker exports no completion counter — "
            "so nothing on either side can certify the exactly-once handshake finished)"
        )

    # Legacy aggregate counter differences cannot identify duplicates. Only
    # the group-wide identity ledger below can report unique receipt/redelivery.
    duplicate = max(0.0, broker_deliv - settled) if broker_deliv else 0.0
    if duplicate > TOLERANCE * broker_deliv and qos == "0":
        flags.append(
            f"DUPLICATE DELIVERY at QoS 0 ({duplicate:,.0f} more delivered than received; "
            "at-most-once must not redeliver)"
        )
    elif duplicate > TOLERANCE * broker_deliv and qos in ("1", "2"):
        # At least-once MAY redeliver, so this is not the defect it is at QoS 0
        # and it does not gate. Reporting it anyway is the point: the QoS 0 arm
        # had a check here and the QoS 1 arm used to have NOTHING, so a rung that
        # put 30% more on the wire than the application saw read as clean.
        flags.append(
            f"REDELIVERY at QoS {qos} ({duplicate:,.0f} more delivered than received, "
            f"{duplicate / broker_deliv * 100:.1f}% — legal at least-once, reported not gated)"
        )

    # BROKER BOUND REACHED. `dropped` is summed for the accounting table, so a
    # rung that hit one of the broker's own fixed tables is otherwise indistinguishable
    # from one that shed under load. These two reasons are not load: they are the
    # broker saying a bound was reached, and at QoS >= 1 `pending-cap` is the one
    # lane E's shape check predicts (PENDING_PUBLISH_CAP, hub/mod.rs).
    bounds = {r: v for r, v in dropped_by_reason.items()
              if r in ("pending-cap", "backlog-overflow") and v > 0}
    if bounds:
        flags.append(
            "BROKER BOUND REACHED ("
            + ", ".join(f"{r} {v:,.0f}" for r, v in sorted(bounds.items()))
            + ") — a fixed internal table, not load shedding; this rung is not a capacity figure"
        )

    # QoS DOWNGRADE. `mqttd_publish_delivered_total` is labelled by QoS, so the
    # QoS a delivery actually went out at is a measured fact rather than the one
    # the rung asked for. A rung that requested QoS 2 and was granted 1 measures
    # a different protocol than its directory name claims.
    granted = max((int(q) for q, v in deliv_by_qos.items() if q.isdigit() and v > 0), default=None)
    if any(int(q) < min(int(qos), int(sub_qos)) for q,v in deliv_by_qos.items() if q.isdigit() and v > 0):
        flags.append(
            f"QOS DOWNGRADE (subscriber asked for QoS {sub_qos}, deliveries went out at "
            f"QoS {granted} — this rung measures QoS {granted})"
        )

    pending = max(0.0, sent - settled) if drained != "yes" else 0.0
    counts = {
        "offered": offered * span,
        "sent": sent,
        "broker_received": broker_recv,
        "protocol_completed": completed,
        "uniquely_delivered": evidence["counts"]["unique_delivered"] if evidence else None,
        "aggregate_delivered": settled,
        "duplicate": evidence["counts"]["duplicates"] if evidence else None,
        "dropped": dropped,
        "pending": pending,
    }

    # Broker gauges expose partial queue evidence; oldest message age is absent.
    final_state = {
        "sessions": broker_at(rdir, "drain", "mqttd_sessions") or broker_at(rdir, "after", "mqttd_sessions"),
        "connections": broker_at(rdir, "drain", "mqttd_connections_active")
        or broker_at(rdir, "after", "mqttd_connections_active"),
        "queue_depth": None,
        "queue_bytes": broker_at(rdir, "drain", "mqttd_backlog_bytes"),
        "outbound_inflight": broker_at(rdir, "drain", "mqttd_inflight_messages"),
        "oldest_age_s": None,
        "dropped_by_reason": dropped_by_reason,
    }

    # ── the population must have ARRIVED, and the cluster must have been CLEAN ─
    settled_ok = meta.get("settled", "yes") != "no"
    if not settled_ok:
        flags.append(
            f"UNSETTLED (only {meta.get('settled_conns', '?')} of "
            f"{meta.get('expected_conns', '?')} clients had connected when the measurement "
            "window opened — this rung measures a cluster still filling up)"
        )
    reset_ok = meta.get("reset", "yes") != "no"
    if not reset_ok:
        flags.append(
            f"UNRESET ({meta.get('reset_conns', '?')} connections from the previous rung were "
            "still on the cluster when this one started — it measures residual load too)"
        )
    return {
        "sites": sites,
        "rep": rep,
        "offered": offered,
        "sent_rate": sent_rate,
        "recv_rate": recv_rate,
        "per_consumer": float(meta.get("per_consumer", 0)),
        "late_share": late_share,
        "evidence": evidence,
        "settled": settled,
        "drained": drained,
        # The steady-state gate's own record: whether the window was allowed to
        # open on a caught-up broker, and how long that took. Published beside
        # the latency because it is what makes the latency the rung's.
        "steady": steady,
        "steady_s": meta.get("steady_s", ""),
        "qos": qos,
        "sub_qos": sub_qos,
        "counts": counts,
        "final_state": final_state,
        "control": meta.get("control", "no") == "yes",
        # An UNRESOLVED rung is not a pass, and it is not a broker finding either.
        # It rides beside `pass` so a reader — and report_html — can tell "the
        # broker lost traffic" from "the rig stopped watching too early".
        "unresolved": unresolved,
        "p50": bucket_pct(buckets, count, 0.50),
        "p99": p99,
        "budget_ms": budget,
        # A rung PASSES only on all FOUR: the drivers offered the load, they held
        # its schedule, the broker delivered it, and it stayed inside the latency
        # budget. Any one failing makes the site count above it meaningless.
        #
        # `late_share` joined this in #534. Without it a rung where the drivers
        # fell behind could still PASS on a rate average, and the ladder would
        # report a broker limit that was really a generator limit — the exact
        # class of claim the scaling review rejected.
        # A rung PASSES only if every one of these held. `settled_ok` and
        # `reset_ok` joined in #534: a rung measured before its clients arrived,
        # or on a cluster still carrying the previous rung, is not a measurement
        # of this site count at all — and neither shows up in a rate or a p99.
        # `completed is not None or qos == "0"` keeps a QoS 2 rung from passing
        # while its handshake completion is uncertifiable.
        "pass": bool(
            offer_met
            and delivered
            and within
            and late_share <= LATE_OK
            and settled_ok
            and reset_ok
            # A window opened on a broker that never caught up is not a rung this
            # ladder may claim, however good the numbers inside it look.
            and steady != "no"
            and (qos != "2" or completed is not None)
            and not bounds and dropped == 0
            and not any(int(q) < min(int(qos), int(sub_qos)) for q,v in deliv_by_qos.items() if q.isdigit() and v > 0)
            and (qos != "1" or (evidence is not None and drained == "yes" and steady == "yes"
                 and (not evidence.get("telemetry") or evidence["telemetry"]["queues_bounded"])
                 and recv_rate >= DRIVER_OK * offered
                 and all(rate >= DRIVER_OK * offered / sites for rate in evidence["site_rates"].values())))
        ),
        "flags": flags,
    }


def main() -> None:
    if len(sys.argv) == 2 and sys.argv[1] == "--self-test":
        self_test()
        return
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    root = Path(sys.argv[1])
    found = sizes(root)
    if not found:
        sys.exit(f"no nodes=<N> directories under {root}")

    print("# Scale-curve summary (transcribe into docs/benchmarks/SCALE-CURVE.md)\n")
    print(f"sizes found: {[n for n, _ in found]}\n")

    # barrier floors gate Curve 1
    print("## Per-host durability barrier floors (single writer, barriers/s)\n")
    floors: dict[int, list[str] | None] = {}
    print("| nodes | per-broker floor |")
    print("|---|---|")
    for n, d in found:
        floors[n] = probe_floor(d, n)
        cell = " / ".join(floors[n]) if floors[n] else "MISSING — Curve 1 for this size is UNINTERPRETABLE"
        print(f"| {n} | {cell} |")

    # Curve 1 — durable
    print("\n## Curve 1 — durable QoS 1, closed loop (spread ownership)\n")
    print("| nodes | acked msg/s (sat) | p99 ms (sat) | p99 ms (low-contention) | verdicts |")
    print("|---|---|---|---|---|")
    c1_x, c1_y = [], []
    for n, d in found:
        if floors[n] is None:
            print(f"| {n} | REFUSED — no barrier probe | | | |")
            continue
        sat = [r for r in lane_a_results(d, "sat") if r.get("arm") == "qos1-durable-owner"]
        lat = [r for r in lane_a_results(d, "lat") if r.get("arm") == "qos1-durable-owner"]
        rate, verdicts = median_over_valid(sat, "msgs_per_s")
        p99, _ = median_over_valid(sat, "p99_ms")
        p99lat, _ = median_over_valid(lat, "p99_ms")
        vcell = "; ".join(sorted(set(verdicts))) or "all reps valid"
        print(f"| {n} | {rate} | {p99} | {p99lat} | {vcell} |")
        if rate != "—":
            c1_x.append(n)
            c1_y.append(float(rate.split()[0]))
    if len(c1_x) > 1:
        print()
        print(xychart("durable QoS1 acked msg/s vs nodes", c1_x, c1_y, "acked msg/s"))

    # Durability tiers (ADR 0072): same saturating workload, publisher-selected ack meaning
    tier_rows = []
    for n, d in found:
        for name, label in (("sat", "quorum"), ("tier-local", "local"), ("tier-relaxed", "relaxed")):
            reps = [
                r
                for r in lane_a_results(d, name)
                if r.get("arm") == "qos1-durable-owner"
            ]
            if not reps:
                continue
            rate, verdicts = median_over_valid(reps, "msgs_per_s")
            p99, _ = median_over_valid(reps, "p99_ms")
            tier_rows.append((n, label, rate, p99, "; ".join(sorted(set(verdicts))) or "all reps valid"))
    if any(label != "quorum" for _, label, *_ in tier_rows):
        print("\n## Durability tiers (ADR 0072) — same workload, publisher-selected ack meaning\n")
        print(
            "Saturating throughput converges across tiers by design — the session"
        )
        print(
            "lanes flow-control every tier to the durable pipeline's rate — so the"
        )
        print(
            "tier's real face is the UNCONTENDED ack latency (window 1), shown last.\n"
        )
        print("| nodes | tier (what the ack means) | acked msg/s (sat) | p99 ms (sat) | p99 ms (uncontended) |")
        print("|---|---|---|---|---|")
        meaning = {
            "quorum": "fsync'd on a majority, cluster-wide",
            "local": "fsync'd on the owner (single-copy)",
            "relaxed": "accepted + submitted",
        }
        lat_name = {"quorum": "lat", "local": "tier-local-lat", "relaxed": "tier-relaxed-lat"}
        for n, label, rate, p99, _v in tier_rows:
            d = dict(found)[n]
            lat_reps = [
                r
                for r in lane_a_results(d, lat_name[label])
                if r.get("arm") == "qos1-durable-owner"
            ]
            lat_p99 = median_over_valid(lat_reps, "p99_ms")[0] if lat_reps else "—"
            print(f"| {n} | `{label}` — {meaning[label]} | {rate} | {p99} | {lat_p99} |")

    # Curve 2 — $share ladder
    print("\n## Curve 2 — non-durable $share fan-out (latency = bucket upper bounds)\n")
    c2 = {}
    for n, d in found:
        rungs = []
        for rdir in sorted(
            (d / "laneB").glob("rung-*-plain") if (d / "laneB").is_dir() else [],
            key=lambda p: int(p.name.split("-")[1]),
        ):
            rungs.append(lane_b_rung(rdir, int(rdir.name.split("-")[1])))
        c2[n] = rungs
    all_offers = sorted({r["offered"] for rungs in c2.values() for r in rungs})
    print("| offered msg/s | " + " | ".join(f"{n} node(s)" for n, _ in found) + " |")
    print("|---|" + "---|" * len(found))
    for offer in all_offers:
        cells = []
        for n, _ in found:
            r = next((x for x in c2[n] if x["offered"] == offer), None)
            if r is None:
                cells.append("—")
                continue
            cell = f"recv {r['recv_rate']:.0f}/s, p99 {r['p99']}"
            if r["flags"]:
                cell += " ⚠ " + "; ".join(r["flags"])
            cells.append(cell)
        print(f"| {offer} | " + " | ".join(cells) + " |")
    print("\nknee (highest sustained rung; rungs whose offer was not met excluded):\n")
    knee_x, knee_y = [], []
    for n, _ in found:
        sustained = [r for r in c2[n] if r["sustained"]]
        if sustained:
            k = max(sustained, key=lambda r: r["offered"])
            print(f"- {n} node(s): {k['offered']} msg/s offered (recv {k['recv_rate']:.0f}/s, p99 {k['p99']})")
            knee_x.append(n)
            knee_y.append(k["recv_rate"])
        else:
            print(f"- {n} node(s): NO sustained rung")
    if len(knee_x) > 1:
        print()
        print(xychart("$share sustained throughput vs nodes", knee_x, knee_y, "msg/s at knee"))

    # Connections
    print("\n## Idle connections (lane C, plaintext)\n")
    print("| nodes | connected | broker RSS growth | KiB/conn |")
    print("|---|---|---|---|")
    for n, d in found:
        c = lane_c(d, n)
        if c is None:
            print(f"| {n} | — | — | — |")
            continue
        rss = f"{c['rss_delta_mib']:.0f} MiB" if c["rss_delta_mib"] is not None else "—"
        kib = f"{c['kib_per_conn']:.1f}" if c["kib_per_conn"] else "—"
        print(f"| {n} | {c['connected']} | {rss} | {kib} |")

    # Curve 4 — lane E, the tenancy ladder
    e_sizes = [(n, d) for n, d in found if (d / "laneE").is_dir()]
    if e_sizes:
        print("\n## Curve 4 — scale-out by tenant (the rung is a SITE)\n")
        for n, d in e_sizes:
            # Sort by site count, then by repeat index, so a repeated rung sits
            # beside its twin rather than at the end of the table.
            rungs = sorted(
                (lane_e_rung(r) for r in (d / "laneE").glob("sites-*")),
                key=lambda r: (r["sites"], r.get("rep", 1)),
            )
            if not rungs:
                continue
            print(f"### {n} node(s)\n")
            repeated = len({r["sites"] for r in rungs}) < len(rungs)
            # `caught up in` is evidence, not decoration: it is the seconds the
            # broker needed to reach steady state at this offer before the window
            # was allowed to open, so a reader can see that the latency beside it
            # describes the rung rather than a backlog being repaid into it.
            head = "| sites | offered msg/s | delivered/s | per consumer | caught up in | p99 | verdict |"
            if repeated:
                head = "| sites | run | offered msg/s | delivered/s | per consumer | caught up in | p99 | verdict |"
            print(head)
            print("|---|---|---|---|---|---|---|" + ("---|" if repeated else ""))
            for r in rungs:
                verdict = "pass" if r["pass"] else "; ".join(r["flags"]) or "fail"
                run_col = f" {r.get('rep', 1)} |" if repeated else ""
                if r.get("steady") == "yes":
                    caught = f"{r.get('steady_s', '?')}s"
                elif r.get("steady") == "no":
                    caught = "NEVER"
                else:
                    caught = "—"  # a run directory from before the gate existed
                print(
                    f"| {r['sites']} |{run_col} {r['offered']:,.0f} | {r['recv_rate']:,.0f} | "
                    f"{r['per_consumer']:,.0f} | {caught} | {r['p99']} | {verdict} |"
                )
            # ── message accounting (#534, acceptance 3) ──────────────────
            # One row per rung, so a shortfall can be attributed instead of
            # guessed at. "n/a" is a real answer here and appears deliberately:
            # QoS 0 has no completion to report, QoS 2's cannot be seen from
            # either end, and the broker exports no queue-depth gauge at all.
            if any(r.get("counts") for r in rungs):
                print("\nMessage accounting — offered vs what each stage of the path saw:\n")
                print(
                    "| sites | run | QoS pub/sub | offered | sent | broker recv | "
                    "completed | unique delivered | dup | dropped | pending | sessions | conns |"
                )
                print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
                for r in rungs:
                    c = r.get("counts")
                    if not c:
                        continue
                    fs = r.get("final_state", {})

                    def num(v):
                        return "n/a" if v is None else f"{v:,.0f}"

                    run = f"{r.get('rep', 1)}{' (control)' if r.get('control') else ''}"
                    print(
                        f"| {r['sites']} | {run} | {r.get('qos', '?')}/{r.get('sub_qos', '?')} | "
                        f"{num(c['offered'])} | {num(c['sent'])} | {num(c['broker_received'])} | "
                        f"{num(c['protocol_completed'])} | {num(c['uniquely_delivered'])} | "
                        f"{num(c['duplicate'])} | {num(c['dropped'])} | {num(c['pending'])} | "
                        f"{num(fs.get('sessions'))} | {num(fs.get('connections'))} |"
                    )
                reasons = {}
                for r in rungs:
                    for k, v in (r.get("final_state", {}).get("dropped_by_reason") or {}).items():
                        if v:
                            reasons[k] = reasons.get(k, 0.0) + v
                if reasons:
                    print(
                        "\nDropped by reason: "
                        + ", ".join(f"{k or 'unlabelled'} {v:,.0f}" for k, v in sorted(reasons.items()))
                    )
                print(
                    "\n> Counts are LIFETIME totals over the publishers' own run span, not "
                    "over the measurement window — the driver logs are cumulative and the "
                    "broker deltas are before->after, so `offered` is the offered rate across "
                    "that same span. The rate columns above are the steady window; these are not."
                )
                print(
                    "\n> `completed` is n/a at QoS 0 (nothing to acknowledge) and at QoS 2 "
                    "(the driver counts at PUBREC, and the broker exports no PUBCOMP counter). "
                    "Unique delivery and duplicates are n/a without an identity ledger. "
                    "Available backlog bytes and outbound inflight gauges are retained in final_state; oldest age is unavailable."
                )

            v = lane_e_ladder(rungs)
            by_count, flaky, inconclusive = v["by_count"], v["flaky"], v["inconclusive"]
            if inconclusive:
                print(
                    f"\n**INCONCLUSIVE TRIAL — no capacity is claimed.** Because {inconclusive}. "
                    "A ladder run on a cluster that did not end as it started measures the "
                    "ladder, not the cluster."
                )
            if flaky:
                print(
                    f"\n> **{', '.join(str(c) for c in flaky)} site(s): PASSED ON ONE RUN AND FAILED ON ANOTHER.** "
                    "The spread at that count crosses the budget, so no capacity is "
                    "established there and nothing above it can be claimed."
                )
            if v["unresolved"]:
                print(
                    f"\n> **{', '.join(str(c) for c in v['unresolved'])} site(s): UNRESOLVED, not failed.** "
                    "The drain deadline expired with traffic still outstanding, so whether "
                    "the broker dropped it or still held it is unknown. Raise "
                    "LANE_E_DRAIN_SECS and repeat before reading these as a limit."
                )
            if v["blocked_by"]:
                print(f"\n**No capacity is claimed.** {v['blocked_by']}")
            if v["claim"]:
                best = v["claim"]
                print(
                    f"\n**{best['sites']} site(s) per {n}-node cluster** at p99 "
                    f"<= {best['budget_ms']:g}ms — {best['offered']:,.0f} msg/s, "
                    f"{best['sites'] / n:.1f} sites per node."
                )
            elif not v["passed_any"] and not inconclusive:
                print("\n**No rung passed.** Inspect evidence validity and load generation before attributing a broker limit.")
            # A ladder whose TOP rung passed has not found a ceiling; saying so
            # is the difference between a measurement and an advertisement.
            done_rungs = [r for r in rungs if not r.get("incomplete")]
            top = max((r["sites"] for r in done_rungs), default=0)
            if done_rungs and not flaky and not inconclusive and all(r["pass"] for r in by_count.get(top, [])):
                print(
                    f"\n> The top rung passed, so this is a FLOOR, not a ceiling — "
                    f"{top} sites is where the ladder stopped, not where "
                    f"the cluster did. Extend LANE_E_SITES to find the knee."
                )

    print("\n> Raw results are untracked scratch; cite only tracked paths in the doc.")


if __name__ == "__main__":
    main()
