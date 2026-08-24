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
    Histograms are merged across drivers by summing per-`le` cumulative counts.
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

import json
import re
import statistics
import sys
from pathlib import Path

TOLERANCE = 0.02  # counter cross-check band
DRIVER_OK = 0.97  # a rung counts only if the offered rate was actually reached
KNEE_OK = 0.99  # delivered/sent ratio a sustained rung must reach
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


def driver_rate(log: Path, counter: str) -> tuple[int, float]:
    """(final total, achieved rate) from emqtt-bench progress lines.

    Two things this has to get right, both of which it once got wrong and both
    of which manufactured a broker limit that did not exist:

    1. **The timestamp format changes at one minute.** emqtt-bench prints `59s`
       and then `1m0s`. A `^(\\d+)s` pattern silently stops matching at the
       minute mark, so every rung was read from its first 59 seconds only.
    2. **Those first seconds are the RAMP**, not the measurement. Publishers
       start staggered behind the subscribers, so a rate averaged from the
       start understates the steady state — and understates it by more at
       higher rungs, which is exactly the shape of a knee. The reported
       "~140k plateau with idle CPU everywhere" was this artifact: recomputed
       over the steady window the same run sustained 199k at the 200k rung and
       220k at the 300k rung, with received tracking sent to within 0.5%.

    So: parse `[Nm]Ns`, and measure over the LAST [`STEADY_WINDOW`] seconds of
    the series — the part of the rung that is actually the rung.
    """
    if not log.exists():
        return 0, 0.0
    points = []
    for line in log.read_text(errors="replace").splitlines():
        m = re.search(rf"^(?:(\d+)m)?(\d+)s {counter} total=(\d+) rate=", line)
        if m:
            secs = int(m.group(1) or 0) * 60 + int(m.group(2))
            points.append((secs, int(m.group(3))))
    if len(points) < 2:
        return (points[0][1], 0.0) if points else (0, 0.0)
    end = points[-1][0]
    window = [p for p in points if p[0] >= max(end - STEADY_WINDOW, points[0][0])]
    if len(window) < 2:
        window = points
    (t0, c0), (t1, c1) = window[0], window[-1]
    return points[-1][1], (c1 - c0) / max(t1 - t0, 1)


def merged_histogram(proms: list[Path]) -> tuple[dict[float, int], int]:
    buckets: dict[float, int] = {}
    count = 0
    for prom in proms:
        if not prom.exists():
            continue
        for line in prom.read_text(errors="replace").splitlines():
            m = re.match(r'e2e_latency_bucket\{le="([\d.+eInf]+)"\}\s+(\d+)', line)
            if m:
                le = float("inf") if m.group(1) == "+Inf" else float(m.group(1))
                buckets[le] = buckets.get(le, 0) + int(m.group(2))
            m = re.match(r"e2e_latency_count\s+(\d+)", line)
            if m:
                count += int(m.group(1))
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


def lane_b_rung(rdir: Path, offered: int) -> dict:
    sent = sent_rate = recv = recv_rate = 0.0
    for log in rdir.glob("pub-*.log"):
        t, r = driver_rate(log, "pub")
        sent += t
        sent_rate += r
    for log in rdir.glob("sub-*.log"):
        t, r = driver_rate(log, "recv")
        recv += t
        recv_rate += r
    buckets, count = merged_histogram(sorted(rdir.glob("sub-*.prom")))
    broker_recv = counter_delta(rdir, "before", "after", 'mqttd_publish_received_total{qos="1"}')
    flags = []
    # The rung did not reach its offered rate. Deliberately NOT called
    # "driver-limited": lane B's publishers are windowed QoS 1 (`-F 100`), so a
    # shortfall is a closed loop with two possible causes — the drivers could
    # not generate the rate (an integer-millisecond publish timer at a high
    # rung), or the BROKERS did not ack fast enough and the window throttled the
    # publishers. This measurement cannot tell those apart, and asserting the
    # first is how a broker limit hides for a whole campaign. The per-rung
    # mpstat samples are the evidence that separates them.
    offer_not_met = sent_rate < DRIVER_OK * offered
    if offer_not_met:
        flags.append(f"OFFER NOT MET ({sent_rate / offered * 100:.0f}% of offer)")
    if sent and broker_recv and abs(broker_recv - sent) / sent > TOLERANCE:
        flags.append(f"counter mismatch: broker received {broker_recv:.0f} vs driver sent {sent:.0f}")
    return {
        "offered": offered,
        "sent_rate": sent_rate,
        "recv_rate": recv_rate,
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


def main() -> None:
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

    print("\n> Raw results are untracked scratch; cite only tracked paths in the doc.")


if __name__ == "__main__":
    main()
