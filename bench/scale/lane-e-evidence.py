#!/usr/bin/env python3
"""Fail-closed validation and exact shared-group QoS1 ledger reconciliation."""

from __future__ import annotations

import base64
import importlib.util
import json
import math
import re
import statistics
import sys
import time
from pathlib import Path


# A negative sample is a delivery faster than the clocks' disagreement. Tolerated
# as a tail; above this share the latency distribution is not resolvable at all.
NEGATIVE_LATENCY_SHARE = 0.001


def metric(path):
    if not path.is_file():
        raise ValueError(f"missing {path.name}")
    return metric_text(path.read_text(), path.name)


def metric_text(text, name):
    result = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        m = re.fullmatch(r"([\w:]+)(\{.*\})?\s+([-+\d.eE]+|[+-]?Inf|NaN)", line)
        if not m:
            raise ValueError(f"malformed metric in {name}: {line[:80]}")
        key = (m[1], m[2] or "")
        if key in result:
            raise ValueError(f"duplicate metric {key} in {name}")
        v = float(m[3])
        if not math.isfinite(v) or v < 0:
            raise ValueError(f"invalid metric {key} in {name}")
        result[key] = v
    return result


def value(m, name):
    if (name, "") not in m:
        raise ValueError(f"missing metric {name}")
    return m[name, ""]


def endpoint_stamps(raw):
    stamps = [int(v) for v in re.findall(r"^# ENDPOINT_STAMP_MS (\d+)$", raw, re.M)]
    if len(stamps) != 2 or stamps[1] < stamps[0]:
        raise ValueError("missing/invalid endpoint timestamps")
    return stamps


def endpoint_seconds(before, after):
    shortest = (after[0] - before[1]) / 1000
    longest = (after[1] - before[0]) / 1000
    dt = (shortest + longest) / 2
    if shortest <= 0 or (longest - shortest) / (2 * dt) > 0.02:
        raise ValueError("endpoint scrape window uncertainty exceeds 2%")
    return dt


def check_negative(m, manifest, name):
    """The share of deliveries the clocks could not resolve. Reported, not fatal.

    A negative cross-host latency means the publisher's stamp was later than the
    subscriber's, i.e. the delivery was FASTER than the two clocks' disagreement
    (~2 ms on this fleet). That is a resolution floor, not a fault: a publish
    routed to a local subscriber can genuinely complete in well under a
    millisecond, and no cross-host stamp pair can express it.

    It cannot corrupt a capacity claim, because these samples are EXCLUDED from
    the histogram (qos1_audit:latency/1 returns false, so the observe never
    happens). Dropping the fastest samples can only move a percentile UP, so the
    reported p99 is an upper bound on the true p99 — conservative in the only
    direction that matters. Measured 2026-09-19 at 60,000 msg/s: 40,160 of
    1,687,670 receipts, 2.38%, on a fleet whose clocks agreed to 1.9 ms.

    What IS still fatal is an image too old to carry the counter — then the
    negatives are invisible rather than accounted for."""
    if not manifest.get("negative_latency_counter"):
        return 0.0
    negatives = value(m, "audit_negative_latency")
    if not negatives:
        return 0.0
    # Presence, not truthiness: a subscriber that received NOTHING must not fall
    # through to the publisher counter and be excused by it.
    denominator = next((k for k in ("recv", "audit_sent") if (k, "") in m), None)
    received = value(m, denominator) if denominator else 0.0
    if not received:
        raise ValueError(f"{name}: {negatives:.0f} negative latency observations against no receipts")
    return negatives / received


def clock_evidence(rdir, manifest):
    if not manifest.get("clock_required"):
        return None
    spec = importlib.util.spec_from_file_location("clock_check", Path(__file__).with_name("clock-check.py"))
    clock = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(clock)
    hosts = [f"broker{i}" for i in range(manifest["nodes"])] + [f"driver{i}" for i in range(manifest["drivers"])]
    phases = {"preflight", "open", "close", "final"}
    phases.update(p.name.rsplit("-broker", 1)[0] for p in (rdir / ".batch").glob("sample-*-broker0"))
    if len(phases) < 7:
        raise ValueError("missing periodic clock evidence")
    reports = {phase: clock.validate_phase(rdir / "clock" / phase, hosts, manifest["clock_max_error_ms"]) for phase in phases}
    bound = max(row["error_ms"] for report in reports.values() for row in report.values())
    # Millisecond timestamp quantization adds <1 ms to a timestamp difference.
    return {"host_error_bound_ms": bound, "latency_uncertainty_ms": 2 * bound + 1,
            "phases": sorted(phases)}


def delta(a, b, name):
    d = value(b, name) - value(a, name)
    if d < 0:
        raise ValueError(f"reset counter {name}")
    return d


def histogram(a, b, name):
    keys = {k for k in a if k[0] == name + "_bucket"}
    if not keys or keys != {k for k in b if k[0] == name + "_bucket"}:
        raise ValueError(f"incomplete {name} buckets")
    count = delta(a, b, name + "_count")
    buckets = {}
    for k in keys:
        m = re.fullmatch(r'\{le="([^"]+)"\}', k[1])
        if not m:
            raise ValueError("unexpected histogram labels")
        v = b[k] - a[k]
        if v < 0:
            raise ValueError("reset histogram bucket")
        buckets[float(m[1])] = v
    vals = [buckets[k] for k in sorted(buckets)]
    if vals != sorted(vals) or buckets.get(math.inf) != count:
        raise ValueError("inconsistent histogram count/buckets")
    return buckets, count


def ledger(paths, kind):
    combined = {}
    total = 0
    for path in paths:
        if not path.is_file():
            raise ValueError(f"missing ledger {path.name}")
        lines = path.read_text().rstrip().splitlines()
        if not lines or lines[-1] != "# EOF":
            raise ValueError(f"truncated ledger {path.name}")
        seen = set()
        for line in lines[:-1]:
            k, t, p, c, b = line.split("\t")
            if k not in ("sent", "acked", "received"):
                raise ValueError("unknown ledger kind")
            topic = base64.b64decode(t, validate=True).decode()
            bits = int(b, 16)
            count = int(c)
            if bits < 0 or count < bits.bit_count():
                raise ValueError("impossible ledger count")
            key = (k, topic, p)
            if key in seen:
                raise ValueError("duplicate ledger row")
            seen.add(key)
            if k != kind:
                continue
            total += count
            combined[topic] = combined.get(topic, 0) | bits
    return combined, total


def reconcile(sent, acked, received):
    topics = set(sent) | set(acked) | set(received)
    missing = sum((sent.get(t, 0) & ~received.get(t, 0)).bit_count() for t in topics)
    unexpected = sum((received.get(t, 0) & ~sent.get(t, 0)).bit_count() for t in topics)
    unacked = sum((sent.get(t, 0) & ~acked.get(t, 0)).bit_count() for t in topics)
    badack = sum((acked.get(t, 0) & ~sent.get(t, 0)).bit_count() for t in topics)
    return {
        "missing": missing,
        "unexpected": unexpected,
        "unacked": unacked,
        "unexpected_acks": badack,
        "unique_delivered": sum(v.bit_count() for v in received.values()),
    }


def load_extractor():
    spec = importlib.util.spec_from_file_location(
        "lane_e_extract", Path(__file__).with_name("extract-lane-e.py")
    )
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def telemetry(rdir, manifest, meta, negative_shares):
    phases = (
        ["open"]
        + sorted(
            {
                p.name.rsplit("-broker", 1)[0]
                for p in (rdir / ".batch").glob("sample-*-broker0")
            },
            key=lambda s: int(s.split("-")[1]),
        )
        + ["close"]
    )
    if len(phases) < 5:
        raise ValueError("missing periodic telemetry")
    prev = {}
    rows = []
    for phase in phases:
        row = {
            "phase": phase,
            "sent_rate": 0.0,
            "ack_rate": 0.0,
            "recv_rate": 0.0,
            "backlog_bytes": 0.0,
            "inflight": 0.0,
            "site_recv_rate": {},
        }
        for i in range(manifest["nodes"]):
            raw = (rdir / ".batch" / f"{phase}-broker{i}").read_text()
            if "# EOF" not in raw:
                raise ValueError("truncated periodic broker sample")
            m = metric_text(
                "\n".join(l for l in raw.splitlines() if not l.startswith("WINDOW_")),
                "periodic broker",
            )
            row["backlog_bytes"] += value(m, "mqttd_backlog_bytes")
            row["inflight"] += value(m, "mqttd_inflight_messages")
        min_dt = math.inf
        for i in range(manifest["drivers"]):
            raw = (rdir / ".batch" / f"{phase}-driver{i}").read_text()
            stamps = []
            parts = {}
            current = None
            for line in raw.splitlines():
                if line.startswith("WINDOW_STAMP_MS "):
                    stamps.append(int(line.split()[1]))
                    continue
                if line.startswith("@@@ "):
                    current = line[4:].strip()
                    if current in parts:
                        raise ValueError("duplicate periodic endpoint")
                    parts[current] = []
                elif current:
                    parts[current].append(line)
            if len(stamps) != 2:
                raise ValueError("missing periodic driver timestamps")
            at = sum(stamps) / 2000
            for ep in manifest["endpoints"]:
                if ep["driver"] != i:
                    continue
                n = ep["name"]
                if n not in parts:
                    raise ValueError(f"missing periodic endpoint {n}")
                raw_endpoint = "\n".join(parts[n])
                m = metric_text(raw_endpoint, n)
                negative_shares.append(check_negative(m, manifest, n))
                edge = endpoint_stamps(raw_endpoint) if manifest.get("endpoint_timestamps") else None
                endpoint_at = sum(edge) / 2000 if edge else at
                if n in prev:
                    t, a, old_edge = prev[n]
                    dt = endpoint_seconds(old_edge, edge) if edge else endpoint_at - t
                    if dt <= 0:
                        raise ValueError("periodic clock moved backwards")
                    min_dt = min(min_dt, dt)
                    if ep["role"] == "pub":
                        row["sent_rate"] += delta(a, m, "audit_sent") / dt
                        row["ack_rate"] += delta(a, m, "audit_acked") / dt
                    else:
                        rate = delta(a, m, "recv") / dt
                        row["recv_rate"] += rate
                        row["site_recv_rate"][ep["site"]] = (
                            row["site_recv_rate"].get(ep["site"], 0) + rate
                        )
                prev[n] = (endpoint_at, m, edge)
        if phase == "open" or min_dt >= 1:
            rows.append(row)
    q = [r["backlog_bytes"] for r in rows]
    quarter = max(1, len(q) // 4)
    growth = statistics.mean(q[-quarter:]) - statistics.mean(q[:quarter])
    # One latency-budget worth of offered bytes bounds queued work. A final
    # quartile rise exceeding 10% of that budget rejects sustained growth.
    bound = (
        float(meta["offered"])
        * manifest.get("payload_bytes", 216)
        * float(meta.get("p99_budget_ms", 1000))
        / 1000
    )
    return {
        "samples": rows,
        "backlog_growth_bytes": growth,
        "backlog_bound_bytes": bound,
        "queues_bounded": max(q) <= bound and growth <= bound * 0.1,
    }


def validate(rdir):
    # Deliveries the clocks could not resolve, per endpoint (see check_negative).
    negative_shares: list[float] = []
    manifest = json.loads((rdir / "manifest.json").read_text())
    x = load_extractor()
    meta = x.read_meta(rdir)
    endpoints = manifest["endpoints"]
    if len({e["name"] for e in endpoints}) != len(endpoints):
        raise ValueError("duplicate endpoint")
    for role, key in [("pub", "publishers"), ("sub", "consumers")]:
        if sum(e["clients"] for e in endpoints if e["role"] == role) != int(meta[key]):
            raise ValueError("manifest population mismatch")
    if {e["site"] for e in endpoints} != {str(i) for i in range(int(meta["sites"]))}:
        raise ValueError("manifest sites mismatch")
    for i in range(manifest["nodes"]):
        if not (rdir / f"metrics-drain-broker{i}.prom").is_file():
            raise ValueError("missing drain snapshot")
    extracted = x.extract_rung(rdir)
    if extracted["cert"] not in ("canary", "structural"):
        raise ValueError("unbound crossing certificate")
    if extracted["bracket_wide"] and not manifest.get("endpoint_timestamps"):
        raise ValueError("scrape window uncertainty exceeds 2%")
    window = x.load_window(rdir, manifest["nodes"])
    if manifest.get("endpoint_timestamps"):
        # Driver host brackets enclose multiple sequential endpoint requests;
        # validate each endpoint below, while keeping broker brackets strict.
        for host, edges in window["hosts"].items():
            if host.startswith("broker"):
                for phase in ("open", "close"):
                    edge = edges[phase]
                    if edge[1] - edge[0] > 0.02 * x.window_seconds(edges) * 1000:
                        raise ValueError("broker scrape window uncertainty exceeds 2%")
    clocks = clock_evidence(rdir, manifest)
    buckets = {}
    count = 0
    sent_rate = ack_rate = recv_rate = late_rate = 0.0
    pubpaths = []
    subpaths = []
    site_rates = {}
    puback_buckets = {}
    puback_count = 0
    for ep in manifest["endpoints"]:
        name = ep["name"]
        host = f"driver{ep['driver']}"
        if host not in window["hosts"]:
            raise ValueError(f"missing window for {host}")
        secs = x.window_seconds(window["hosts"][host])
        if secs < manifest["window_secs"] * 0.95:
            raise ValueError("short window")
        for suffix in ("-base.prom", ".prom", "-terminal.prom"):
            path = rdir / (name + suffix)
            if not path.is_file() or not "\n".join(l for l in path.read_text().splitlines() if not l.startswith("# ENDPOINT_STAMP_MS ")).rstrip().endswith("# EOF"):
                raise ValueError(f"truncated endpoint {path.name}")
        if manifest.get("endpoint_timestamps"):
            secs = endpoint_seconds(endpoint_stamps((rdir / f"{name}-base.prom").read_text()), endpoint_stamps((rdir / f"{name}.prom").read_text()))
            if secs < manifest["window_secs"] * 0.95:
                raise ValueError("short endpoint window")
        a = metric(rdir / f"{name}-base.prom")
        b = metric(rdir / f"{name}.prom")
        negative_shares.append(check_negative(a, manifest, name))
        negative_shares.append(check_negative(b, manifest, name))
        for k, v in a.items():
            if (
                k[0]
                in (
                    "pub",
                    "pub_succ",
                    "pub_overrun",
                    "recv",
                    "audit_sent",
                    "audit_acked",
                    "audit_received",
                )
                and b.get(k, -1) < v
            ):
                raise ValueError(f"reset/missing {name} {k}")
        final = metric(rdir / f"{name}-terminal.prom")
        negative_shares.append(check_negative(final, manifest, name))
        if ep["role"] == "pub":
            sent_rate += delta(a, b, "audit_sent") / secs
            ack_rate += delta(a, b, "audit_acked") / secs
            late_rate += delta(a, b, "pub_overrun") / secs
            if value(final, "audit_paused_workers") != ep["clients"]:
                raise ValueError(f"{name} not fully paused")
            if value(final, "audit_sent") != value(final, "audit_acked"):
                raise ValueError(f"{name} unacknowledged sends")
            h, n = histogram(a, b, "puback_latency")
            puback_count += n
            for k, v in h.items():
                puback_buckets[k] = puback_buckets.get(k, 0) + v
            pubpaths.append(rdir / f"{name}.ledger")
        else:
            rate = delta(a, b, "recv") / secs
            recv_rate += rate
            site_rates[ep["site"]] = site_rates.get(ep["site"], 0) + rate
            h, n = histogram(a, b, "e2e_latency")
            count += n
            if buckets and set(buckets) != set(h):
                raise ValueError("subscriber histogram schemas differ")
            # Every receipt is either IN the histogram or was excluded for being
            # negative — a delivery faster than the cross-host clocks can express
            # (see check_negative). So the identity is histogram + negatives ==
            # recv, not histogram == recv: comparing against receipts alone made a
            # subscriber with many sub-millisecond, locally-routed deliveries look
            # like it had lost them. Measured 2026-09-19 on sub-s1-1, which is
            # where prefer-local put most of its own site's publishers: the gap
            # was 87,911 / 130,141 / 28,735 and the negative counter was 87,911 /
            # 130,141 / 28,738 — the same number, three rungs running, and three
            # 120,000 msg/s measurements were discarded for it.
            excluded = delta(a, b, "audit_negative_latency") if manifest.get("negative_latency_counter") else 0
            if abs(n + excluded - delta(a, b, "recv")) > max(1, n * 0.02):
                raise ValueError(
                    f"{name} histogram+negatives/received mismatch beyond scrape uncertainty"
                )
            for k, v in h.items():
                buckets[k] = buckets.get(k, 0) + v
            subpaths.append(rdir / f"{name}.ledger")
    s, st = ledger(pubpaths, "sent")
    a, at = ledger(pubpaths, "acked")
    r, rt = ledger(subpaths, "received")
    accounting = reconcile(s, a, r)
    accounting.update(
        sent=st,
        protocol_completed=at,
        aggregate_delivered=rt,
        duplicates=rt - accounting["unique_delivered"],
    )
    # Terminal counters must describe exactly the same traffic as the ledgers.
    for role, paths, kind, total in [
        ("pub", pubpaths, "audit_sent", st),
        ("pub", pubpaths, "audit_acked", at),
        ("sub", subpaths, "recv", rt),
    ]:
        observed = sum(
            value(metric(rdir / (p.stem + "-terminal.prom")), kind) for p in paths
        )
        if observed != total:
            raise ValueError(f"{role} ledger/counter mismatch {observed} != {total}")
    if any(
        accounting[k] for k in ("missing", "unexpected", "unacked", "unexpected_acks")
    ):
        raise ValueError(f"ledger does not close: {accounting}")
    if st != sum(v.bit_count() for v in s.values()):
        raise ValueError("publisher identities overlap")
    if not st:
        raise ValueError("empty ledger")
    if extracted["lifetime_received"] != st:
        raise ValueError("broker receive/terminal sent mismatch")
    trace = (
        telemetry(rdir, manifest, meta, negative_shares) if manifest.get("telemetry_required") else None
    )
    return {
        "clock": clocks,
        "telemetry": trace,
        "sent_rate": sent_rate,
        "ack_rate": ack_rate,
        "recv_rate": recv_rate,
        "late_share": late_rate / sent_rate if sent_rate else 0,
        "buckets": buckets,
        "histogram_count": count,
        "puback_buckets": puback_buckets,
        "puback_count": puback_count,
        "counts": accounting,
        "site_rates": site_rates,
        "negative_share": max(negative_shares, default=0.0),
        "crossing": extracted["crossing"],
        "bracket_ms": extracted["bracket_ms"],
    }


def timed_poll(rdir, manifest, offer):
    """Bound each endpoint's rate by its own remote scrape timestamps.

    SSH fan-out completion time is not an endpoint sample time. Summing rate
    bounds also avoids assuming subscriber containers scrape simultaneously.
    """
    endpoints = {}
    totals = {}
    for ep in manifest["endpoints"]:
        if ep["role"] != "sub":
            continue
        path = rdir / ".poll" / f"{ep['name']}.prom"
        raw = path.read_text()
        stamps = re.findall(r"^# POLL_STAMP_MS (\d+)$", raw, re.MULTILINE)
        if len(stamps) != 2:
            raise ValueError(f"missing poll timestamps for {ep['name']}")
        start, end = map(int, stamps)
        if end < start or "# EOF" not in raw:
            raise ValueError("invalid/truncated timed poll")
        metrics = metric_text(raw, ep["name"])
        check_negative(metrics, manifest, ep["name"])
        count = value(metrics, "recv")
        endpoints[ep["name"]] = {
            "start": start,
            "end": end,
            "count": count,
            "site": ep["site"],
        }
        totals[ep["site"]] = totals.get(ep["site"], 0) + count
    state = rdir / "poll-state.json"
    bounds = {}
    good = False
    if state.exists():
        previous = json.loads(state.read_text())["endpoints"]
        if set(previous) != set(endpoints):
            raise ValueError("poll population changed")
        for name, v in endpoints.items():
            old = previous[name]
            shortest = (v["start"] - old["end"]) / 1000
            longest = (v["end"] - old["start"]) / 1000
            count = v["count"] - old["count"]
            if shortest <= 0 or count < 0 or old["site"] != v["site"]:
                raise ValueError("poll clock/counter/population reset")
            lo, hi = bounds.get(v["site"], (0, 0))
            bounds[v["site"]] = (lo + count / longest, hi + count / shortest)
        good = bool(bounds) and all(
            lo >= offer * 0.95 and hi <= offer * 1.05 for lo, hi in bounds.values()
        )
    row = {
        "at": time.monotonic(),
        "endpoints": endpoints,
        "totals": totals,
        "rate_bounds": bounds,
        "rates": {s: (lo + hi) / 2 for s, (lo, hi) in bounds.items()},
        "steady": good,
    }
    state.write_text(json.dumps(row))
    (rdir / "poll-steady").write_text("yes" if good else "no")
    with (rdir / "poll-history.jsonl").open("a") as f:
        f.write(json.dumps(row) + "\n")
    print(int(sum(totals.values())))


def poll(rdir, offer):
    manifest = json.loads((rdir / "manifest.json").read_text())
    if manifest.get("poll_timestamps"):
        return timed_poll(rdir, manifest, offer)
    totals = {}
    # batch_split output is replaced on each poll; every expected endpoint is required.
    for ep in manifest["endpoints"]:
        if ep["role"] == "sub":
            v = value(metric(rdir / ".poll" / f"{ep['name']}.prom"), "recv")
            totals[ep["site"]] = totals.get(ep["site"], 0) + v
    now = time.monotonic()
    state = rdir / "poll-state.json"
    good = False
    if state.exists():
        prev = json.loads(state.read_text())
        elapsed = now - prev["at"]
        if set(prev["totals"]) != set(totals):
            raise ValueError("poll population changed")
        rates = {s: (v - prev["totals"][s]) / elapsed for s, v in totals.items()}
        if any(v < 0 for v in rates.values()):
            raise ValueError("poll counter reset")
        good = all(abs(v - offer) <= offer * 0.05 for v in rates.values())
    else:
        rates = {}
    row = {"at": now, "totals": totals, "rates": rates, "steady": good}
    state.write_text(json.dumps(row))
    (rdir / "poll-steady").write_text("yes" if good else "no")
    with (rdir / "poll-history.jsonl").open("a") as f:
        f.write(json.dumps(row) + "\n")
    print(int(sum(totals.values())))


if __name__ == "__main__":
    try:
        if sys.argv[1] == "poll":
            poll(Path(sys.argv[2]), float(sys.argv[3]))
        else:
            print(json.dumps(validate(Path(sys.argv[2])), indent=2))
    except (ValueError, OSError, KeyError, IndexError) as e:
        print(f"INVALID EVIDENCE: {e}", file=sys.stderr)
        sys.exit(1)
