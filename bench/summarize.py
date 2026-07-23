#!/usr/bin/env python3
"""Summarize a bench run (ADR 0048 T2): one markdown table from the raw logs.

    ./summarize.py results/<stamp>

The raw logs remain the record; this only extracts and links. Latency percentiles are
computed from emqtt-bench's e2e_latency Prometheus HISTOGRAM by bucket upper bound —
i.e. "p99 <= Xms" at the histogram's resolution (1/5/10/25/50/100/500/1000ms), which is
coarse but cannot flatter: the true percentile is at most the reported bound.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

BROKERS = ["mqttd", "mosquitto", "emqx"]
PCTS = [("p50", 0.50), ("p99", 0.99), ("p999", 0.999)]


def last_rate(path: Path, counter: str) -> str:
    """The final cumulative 'total=N rate=R/sec' line for `counter`; returns 'N (peak R/s)'."""
    if not path.exists():
        return "—"
    total, peak = None, 0.0
    for line in path.read_text(errors="replace").splitlines():
        m = re.search(rf"{counter} total=(\d+) rate=([\d.]+)/sec", line)
        if m:
            total = int(m.group(1))
            peak = max(peak, float(m.group(2)))
    return f"{total} ({peak:.0f}/s peak)" if total is not None else "—"


def recv_throughput(sub_log: Path, duration_hint: float | None = None) -> str:
    """Aggregate subscriber-side receive throughput: total msgs / active seconds."""
    if not sub_log.exists():
        return "—"
    totals = []
    for line in sub_log.read_text(errors="replace").splitlines():
        m = re.search(r"^(\d+)s recv total=(\d+) rate=([\d.]+)/sec", line)
        if m:
            totals.append((int(m.group(1)), int(m.group(2))))
    if not totals:
        return "—"
    t0, first = totals[0]
    t1, last = totals[-1]
    span = max(t1 - t0, 1)
    return f"{last} msgs, ~{(last - first) / span:.0f} msg/s"


def latency_percentiles(prom: Path) -> str:
    """p50/p99/p999 upper bounds from the e2e_latency histogram buckets."""
    if not prom.exists():
        return "—"
    buckets: list[tuple[float, int]] = []
    count = 0
    for line in prom.read_text(errors="replace").splitlines():
        m = re.match(r'e2e_latency_bucket\{le="([\d.+eInf]+)"\}\s+(\d+)', line)
        if m:
            le = float("inf") if m.group(1) == "+Inf" else float(m.group(1))
            buckets.append((le, int(m.group(2))))
        m = re.match(r"e2e_latency_count\s+(\d+)", line)
        if m:
            count = int(m.group(1))
    if not buckets:
        return "—"
    buckets.sort()
    if not count:
        count = buckets[-1][1]
    if not count:
        return "—"
    out = []
    for name, p in PCTS:
        need = p * count
        bound = next((le for le, c in buckets if c >= need), float("inf"))
        label = "inf" if bound == float("inf") else f"{bound:g}"
        out.append(f"{name}<={label}ms")
    return " ".join(out)


def mem_delta(before: Path, after: Path, conns: int) -> str:
    """Broker RSS growth across the idle-connection ramp, per connection."""

    def mib(p: Path) -> float | None:
        if not p.exists():
            return None
        m = re.search(r"([\d.]+)(KiB|MiB|GiB)", p.read_text(errors="replace"))
        if not m:
            return None
        v = float(m.group(1))
        return {"KiB": v / 1024, "MiB": v, "GiB": v * 1024}[m.group(2)]

    b, a = mib(before), mib(after)
    if b is None or a is None or conns == 0:
        return "—"
    return f"{a - b:+.1f} MiB (~{(a - b) * 1024 / conns:.1f} KiB/conn)"


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run = Path(sys.argv[1])
    env = (run / "env.txt").read_text() if (run / "env.txt").exists() else ""
    conns = int(re.search(r"conns=(\d+)", env).group(1)) if "conns=" in env else 0

    print(f"# Bench summary — {run.name}\n")
    print("> DEV-GRADE unless env.txt names a dedicated bench host. Raw logs are the")
    print(f"> record — see {run}/<broker>/. Latency = histogram upper bounds (see header).\n")
    print("```")
    print(env.strip())
    print("```\n")

    rows = [
        ("connects (plaintext)", lambda d: last_rate(d / "conn.log", "connect_succ")),
        ("connects (mTLS)", lambda d: last_rate(d / "tls-conn.log", "connect_succ")),
        ("mem / idle conn", lambda d: mem_delta(d / "conn.rss-before", d / "conn.rss-after", conns)),
        ("qos0 recv", lambda d: recv_throughput(d / "pubsub-qos0.sub.log")),
        ("qos1 recv", lambda d: recv_throughput(d / "pubsub-qos1.sub.log")),
        ("qos2 recv", lambda d: recv_throughput(d / "pubsub-qos2.sub.log")),
        ("qos1 e2e latency", lambda d: latency_percentiles(d / "pubsub-qos1.prom")),
        ("mTLS qos1 recv", lambda d: recv_throughput(d / "tls-pubsub-qos1.sub.log")),
        ("mTLS qos1 e2e latency", lambda d: latency_percentiles(d / "tls-pubsub-qos1.prom")),
    ]
    present = [b for b in BROKERS if (run / b).is_dir()]
    print("| metric | " + " | ".join(present) + " |")
    print("|---|" + "---|" * len(present))
    for name, fn in rows:
        print(f"| {name} | " + " | ".join(fn(run / b) for b in present) + " |")


if __name__ == "__main__":
    main()
