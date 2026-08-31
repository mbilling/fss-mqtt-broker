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

import json
import re
import statistics
import sys
from pathlib import Path

TOLERANCE = 0.02  # counter cross-check band
DRIVER_OK = 0.97  # a rung counts only if the offered rate was actually reached
LATE_OK = 0.05  # share of publishes behind their own schedule before a rung is flagged
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

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        sys.exit(1)
    print("summarize-curve self-test: publish double-count correction OK (6 cases)")


def p99_ms(label: str) -> float:
    """The numeric upper bound behind a bucket_pct label ('<=1ms' -> 1.0).

    Returns inf for '—' and for a '>N' label: both mean "past the last finite
    bucket", and a budget verdict must treat that as over, never as N.
    """
    if not label or label == "—" or label.startswith(">"):
        return float("inf")
    return float(label.lstrip("<=").rstrip("ms"))


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
    for log in rdir.glob("pub-*.log"):
        # Same emqtt-bench double-count correction as lane B — see lane_b_rung.
        t_pub, r_pub = driver_rate(log, "pub")
        t_succ, r_succ = driver_rate(log, "pub_succ")
        sent += (t_pub + t_succ) // 2
        sent_rate += (r_pub + r_succ) / 2
    for log in rdir.glob("sub-*.log"):
        t, r = driver_rate(log, "recv")
        recv += t
        recv_rate += r
    buckets, count = merged_histogram(sorted(rdir.glob("sub-*.prom")))
    p99 = bucket_pct(buckets, count, 0.99)

    flags = []
    offer_met = offered and sent_rate >= DRIVER_OK * offered
    if offered and not offer_met:
        flags.append(f"OFFER NOT MET ({sent_rate / offered * 100:.0f}% of offer)")
    delivered = sent > 0 and recv >= KNEE_OK * sent
    if not delivered and sent > 0:
        flags.append(f"LOSS (delivered {recv / sent * 100:.1f}% of what was published)")
    within = p99_ms(p99) <= budget
    if not within:
        flags.append(f"OVER P99 BUDGET ({p99} > {budget:g}ms)")
    return {
        "sites": sites,
        "rep": rep,
        "offered": offered,
        "sent_rate": sent_rate,
        "recv_rate": recv_rate,
        "per_consumer": float(meta.get("per_consumer", 0)),
        "p50": bucket_pct(buckets, count, 0.50),
        "p99": p99,
        "budget_ms": budget,
        # A rung PASSES only on all three: the drivers offered the load, the
        # broker delivered it, and it stayed inside the latency budget. Any one
        # of those failing makes the site count above it meaningless.
        "pass": bool(offer_met and delivered and within),
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
            head = "| sites | offered msg/s | delivered/s | per consumer | p99 | verdict |"
            if repeated:
                head = "| sites | run | offered msg/s | delivered/s | per consumer | p99 | verdict |"
            print(head)
            print("|---|---|---|---|---|---|" + ("---|" if repeated else ""))
            for r in rungs:
                verdict = "pass" if r["pass"] else "; ".join(r["flags"]) or "fail"
                run_col = f" {r.get('rep', 1)} |" if repeated else ""
                print(
                    f"| {r['sites']} |{run_col} {r['offered']:,.0f} | {r['recv_rate']:,.0f} | "
                    f"{r['per_consumer']:,.0f} | {r['p99']} | {verdict} |"
                )
            # A site count counts as passing only if EVERY run of it passed. A rung
            # that passes once and fails on a repeat has not established capacity
            # at that count — it has established that this rig's variance spans
            # the budget there, which is the opposite of a result. Measured
            # 2026-08-31: the same binary at the same shape delivered 210,217
            # msg/s on one provisioning and saturated at 148,080 on another.
            by_count: dict[int, list[dict]] = {}
            for r in rungs:
                by_count.setdefault(r["sites"], []).append(r)
            flaky = sorted(c for c, rs in by_count.items() if any(x["pass"] for x in rs) and not all(x["pass"] for x in rs))
            passed = [rs[0] for c, rs in by_count.items() if all(x["pass"] for x in rs)]
            if flaky:
                print(
                    f"\n> **{', '.join(str(c) for c in flaky)} site(s): PASSED ON ONE RUN AND FAILED ON ANOTHER.** "
                    "The spread at that count crosses the budget, so no capacity is "
                    "established there and nothing above it can be claimed."
                )
            # A capacity claim above an inconsistent rung is not supportable: the
            # cluster demonstrably failed at a LOWER count, so a higher one cannot
            # be its capacity. Suppress the headline rather than print a number
            # the table above it contradicts.
            candidate = max(passed, key=lambda r: r["sites"]) if passed else None
            if candidate and any(c <= candidate["sites"] for c in flaky):
                print(
                    f"\n**No capacity is claimed.** {candidate['sites']} site(s) passed, but "
                    f"{', '.join(str(c) for c in flaky if c <= candidate['sites'])} did not pass "
                    "consistently below it — a cluster that fails at a lower count has not "
                    "established a higher one. Repeat the rungs until the spread is inside the "
                    "budget, or widen the budget to something the spread fits."
                )
                candidate = None
            if candidate:
                best = candidate
                print(
                    f"\n**{best['sites']} site(s) per {n}-node cluster** at p99 "
                    f"<= {best['budget_ms']:g}ms — {best['offered']:,.0f} msg/s, "
                    f"{best['sites'] / n:.1f} sites per node."
                )
            elif not passed:
                print("\n**No rung passed.** The ladder starts above this cluster's capacity.")
            # A ladder whose TOP rung passed has not found a ceiling; saying so
            # is the difference between a measurement and an advertisement.
            done_rungs = [r for r in rungs if not r.get("incomplete")]
            top = max((r["sites"] for r in done_rungs), default=0)
            if done_rungs and not flaky and all(r["pass"] for r in by_count.get(top, [])):
                print(
                    f"\n> The top rung passed, so this is a FLOOR, not a ceiling — "
                    f"{top} sites is where the ladder stopped, not where "
                    f"the cluster did. Extend LANE_E_SITES to find the knee."
                )

    print("\n> Raw results are untracked scratch; cite only tracked paths in the doc.")


if __name__ == "__main__":
    main()
