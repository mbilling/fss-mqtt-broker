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
"""
import argparse
import os
import signal
import sys
import time
import urllib.request


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
