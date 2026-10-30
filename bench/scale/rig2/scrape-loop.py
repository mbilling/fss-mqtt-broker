#!/usr/bin/env python3
"""Scrape one broker's /metrics on a fixed tick, each scrape to its own file.

    scrape-loop.py --url http://localhost:8080/metrics --out DIR [--interval 2] [--max-secs 3600]
    scrape-loop.py --url ... --out DIR --once

Runs on the broker's own host (bench/scale/rig2/run.sh starts it before a step's
generators and stops it after they exit), so every timestamp is the broker host's own
clock: rates come from differences of one host's readings, never across hosts.

Each complete exposition (mqttd ends every one with `# EOF`) is written to
`DIR/<unix_ms>.prom`, where `unix_ms` is the midpoint of the request. A scrape that
failed or came back truncated is not written; it is counted in `DIR/failed`. Writes go
to a temporary name first, so a reader never sees half a file. Stops on SIGTERM or
after --max-secs (a loop whose orchestrator vanished must not run forever).

With `--net PORT` each tick also writes `DIR/<unix_ms>.net.json`: the host's network
path, which the broker's own metrics cannot see. TCP round-trip times and retransmits
of the established sockets on PORT (`ss -ti`), softnet's per-CPU processed, dropped and
squeezed counts, per-CPU softirq time, and every interface's packet counters, queue
count and RPS mask. Counters are cumulative; a reader takes differences.
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request

RTT = re.compile(rb"\brtt:([0-9.]+)/")
RETRANS = re.compile(rb"\bretrans:\d+/(\d+)")
NOTSENT = re.compile(rb"\bnotsent:(\d+)")


def quantiles(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    xs = sorted(xs)
    at = lambda q: xs[min(len(xs) - 1, int(q * len(xs)))]  # noqa: E731
    return {"n": len(xs), "p50": at(0.5), "p90": at(0.9), "p99": at(0.99), "max": xs[-1]}


def read(path: str) -> str:
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def net_sample(port: int) -> dict:
    """One reading of the host's network path (see the module docstring)."""
    t0 = time.time()
    try:
        out = subprocess.run(["ss", "-tinH", "state", "established", f"( sport = :{port} )"],
                             capture_output=True, timeout=10).stdout
    except Exception:  # noqa: BLE001 — a missing reading, not a failed run
        out = b""
    rtts = [float(m.group(1)) for m in RTT.finditer(out)]
    retrans = sum(int(m.group(1)) for m in RETRANS.finditer(out))
    notsent = [int(m.group(1)) for m in NOTSENT.finditer(out)]
    softnet = [[int(x, 16) for x in line.split()[:3]] for line in read("/proc/net/softnet_stat").splitlines()]
    softirq_jiffies = [int(line.split()[7]) for line in read("/proc/stat").splitlines()
                       if re.match(r"cpu\d+ ", line)]
    ifaces = {}
    for i in sorted(os.listdir("/sys/class/net")) if os.path.isdir("/sys/class/net") else []:
        base = f"/sys/class/net/{i}"
        if i == "lo" or not os.path.isdir(f"{base}/statistics"):
            continue
        stat = {k: int(read(f"{base}/statistics/{k}") or 0)
                for k in ("rx_packets", "tx_packets", "rx_dropped", "tx_dropped", "rx_bytes", "tx_bytes")}
        qs = os.listdir(f"{base}/queues") if os.path.isdir(f"{base}/queues") else []
        stat["rx_queues"] = sum(q.startswith("rx-") for q in qs)
        stat["tx_queues"] = sum(q.startswith("tx-") for q in qs)
        stat["rps_cpus"] = read(f"{base}/queues/rx-0/rps_cpus").strip()
        ifaces[i] = stat
    return {"sockets": len(rtts), "rtt_ms": quantiles(rtts), "retrans_total": retrans,
            "notsent_bytes": quantiles([float(n) for n in notsent]),
            "softnet": softnet, "softirq_jiffies": softirq_jiffies, "ifaces": ifaces,
            "sample_s": round(time.time() - t0, 3)}


def scrape(url: str, timeout: float) -> tuple[int, bytes | None]:
    t0 = time.time()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body = r.read()
    except Exception:  # noqa: BLE001 — any failure is a missing sample, counted
        body = None
    t1 = time.time()
    if body is not None and not body.rstrip().endswith(b"# EOF"):
        body = None
    return int((t0 + t1) / 2 * 1000), body


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--max-secs", type=float, default=3600.0)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--once", action="store_true", help="one scrape, then exit (1 if it failed)")
    ap.add_argument("--net", type=int, metavar="PORT", help="also sample the host's network path for PORT")
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    stop = False

    def on_term(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    failed = 0
    start = time.monotonic()
    k = 0
    while not stop:
        t_ms, body = scrape(a.url, a.timeout)
        if body is None:
            failed += 1
            with open(os.path.join(a.out, "failed"), "w") as f:
                f.write(f"{failed}\n")
        else:
            tmp = os.path.join(a.out, f".{t_ms}.tmp")
            with open(tmp, "wb") as f:
                f.write(body)
            os.replace(tmp, os.path.join(a.out, f"{t_ms}.prom"))
        if a.net:
            tmp = os.path.join(a.out, f".{t_ms}.net.tmp")
            with open(tmp, "w") as f:
                json.dump(net_sample(a.net), f)
            os.replace(tmp, os.path.join(a.out, f"{t_ms}.net.json"))
        if a.once:
            return 0 if body is not None else 1
        k += 1
        nxt = start + k * a.interval
        if nxt - start > a.max_secs:
            break
        delay = nxt - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
