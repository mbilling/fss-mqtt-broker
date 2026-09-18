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


def telemetry(rdir, manifest, meta):
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
                m = metric_text("\n".join(parts[n]), n)
                if n in prev:
                    t, a = prev[n]
                    dt = at - t
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
                prev[n] = (at, m)
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
    if extracted["bracket_wide"]:
        raise ValueError("scrape window uncertainty exceeds 2%")
    window = x.load_window(rdir, manifest["nodes"])
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
            if not path.is_file() or not path.read_text().rstrip().endswith("# EOF"):
                raise ValueError(f"truncated endpoint {path.name}")
        a = metric(rdir / f"{name}-base.prom")
        b = metric(rdir / f"{name}.prom")
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
            if abs(n - delta(a, b, "recv")) > max(1, n * 0.02):
                raise ValueError(
                    f"{name} histogram/received mismatch beyond scrape uncertainty"
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
        telemetry(rdir, manifest, meta) if manifest.get("telemetry_required") else None
    )
    return {
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
        "crossing": extracted["crossing"],
        "bracket_ms": extracted["bracket_ms"],
    }


def poll(rdir, offer):
    manifest = json.loads((rdir / "manifest.json").read_text())
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
