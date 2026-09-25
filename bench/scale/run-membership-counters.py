#!/usr/bin/env python3
"""Local-only perf counters around one receipt-checked paced TCP window.

Build shared_membership first. Requires Linux perf and its permission to count the
child's userspace events; never changes perf permissions, CPU policy or hardware.
Every invocation gets a new directory. Failed/unsupported counters remain errors.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import subprocess
import time

EVENTS = {"cycles:u", "instructions:u"}


def counters(path):
    result = {}
    for line in path.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        event = row["event"]
        if event not in EVENTS:
            continue
        value = float(row["counter-value"])
        running = float(row["pcnt-running"])
        if event in result or not math.isfinite(value) or value <= 0 or not 99 <= running <= 100:
            raise ValueError(f"unusable/multiplexed counter: {row}")
        result[event] = value
    if set(result) != EVENTS:
        raise ValueError("required hardware counter missing")
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--perf", default="perf")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--case", choices=["opening", "closing", "C-miss", "D-hit"], required=True)
    p.add_argument("--peers", type=int, default=9)
    p.add_argument("--rate", type=int, default=20_000)
    p.add_argument("--seconds", type=int, default=10)
    p.add_argument("--warmup", type=int, default=2)
    p.add_argument("--hub-cpus", default="2")
    p.add_argument("--endpoint-cpus", default="3,4")
    args = p.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    control, ack = out / "control.fifo", out / "ack.fifo"
    for fifo in (control, ack):
        os.mkfifo(fifo, 0o600)
    binary = args.binary.resolve()
    env = os.environ.copy()
    env.update(MEMBERSHIP_PACED="1", MEMBERSHIP_TRANSPORT="tcp",
               MEMBERSHIP_RATE=str(args.rate), MEMBERSHIP_SECONDS=str(args.seconds),
               MEMBERSHIP_WARMUP=str(args.warmup), MEMBERSHIP_CASE=args.case,
               MEMBERSHIP_PEERS=str(args.peers), MEMBERSHIP_HUB_CPUS=args.hub_cpus,
               MEMBERSHIP_DRAIN_CPUS=args.endpoint_cpus, MEMBERSHIP_DRAIN_THREADS="2",
               MEMBERSHIP_PERF_CONTROL=str(control), MEMBERSHIP_PERF_ACK=str(ack))
    command = [args.perf, "stat", "--no-inherit", "-j", "-e", ",".join(sorted(EVENTS)),
               "--delay=-1", f"--control=fifo:{control},{ack}",
               "-o", str(out / "counters.jsonl"), "--", str(binary)]
    manifest = dict(command=command, settings={k: v for k, v in env.items() if k.startswith("MEMBERSHIP_")},
                    revision=subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
                    binary_sha256=hashlib.sha256(binary.read_bytes()).hexdigest(),
                    host=platform.uname()._asdict(), start_unix_ns=time.time_ns(),
                    perf_version=subprocess.check_output([args.perf, "version"], env=env, text=True).strip(),
                    loader_environment={k: env[k] for k in ("LD_LIBRARY_PATH", "LD_PRELOAD") if k in env},
                    source_capture_scope="current worktree; an older binary is identified by its SHA and original manifest",
                    scope="userspace hot-thread counters; synthetic peers; not a capacity result")
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
    (out / "diff.patch").write_bytes(subprocess.check_output(["git", "diff"]))
    for source in [Path("crates/mqttd/benches/shared_membership.rs"), *Path("crates/mqttd/benches/shared_membership").glob("*.rs")]:
        dest = out / "source" / source
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(source.read_bytes())
    with (out / "benchmark.log").open("w") as log:
        run = subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=False)
    (out / "perf-exit-status.txt").write_text(f"{run.returncode}\n")
    if run.returncode:
        raise SystemExit(run.returncode)
    rows = [json.loads(line) for line in (out / "benchmark.log").read_text().splitlines() if line.startswith("{")]
    windows = [r for r in rows if r.get("event") == "window_end"]
    if len(windows) != 1 or windows[0]["arm"] != args.case:
        raise ValueError("exactly one requested, receipt-verified window required")
    window = windows[0]
    events = counters(out / "counters.jsonl")
    received = window["data_receipts"]
    if received != args.rate * args.seconds:
        raise ValueError("unexpected receipt total")
    summary = dict(events=events, per_receipt={k: v / received for k, v in events.items()},
                   actual_received_rate=received / window["elapsed_seconds"],
                   offer_valid=abs(received / window["elapsed_seconds"] / args.rate - 1) <= .01,
                   window=window)
    (out / "summary.json").write_text(json.dumps(summary, indent=2))
    print(out, {k: round(v, 2) for k, v in summary["per_receipt"].items()},
          "offer_valid=", summary["offer_valid"])
    if not summary["offer_valid"]:
        raise SystemExit("under-offer: preserved, not an equal-work comparison")


if __name__ == "__main__":
    main()
