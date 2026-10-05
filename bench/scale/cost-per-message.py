#!/usr/bin/env python3
"""Broker CPU cost per delivered message, per arm of a knee campaign (#662).

A knee is a pass/fail threshold, and on this rig the same binary on the same
hosts moved its knee by a full rung between arms (2026-10-05), so effects under
~10% cannot be read from it. CPU per message at a FIXED offered load below the
knee is a smooth quantity that averages over the whole window. Alternate two
binaries A/B/A/B on one provisioning (482-per-node-knee.sh, RIG_BINARY) and
compare this number.

    cost-per-message.py <campaign-dir> <sites>

For each arm: broker busy CPU-seconds over the rung's window (every mpstat
"all" row: (100 - %idle) x CPUs x 1 s, split into usr/sys/soft) divided by the
messages the brokers delivered in that window (mqttd_publish_delivered_total,
window-open to window-close), as CPU-ms per 1k messages. Arms are grouped by
the binary they ran (arm-binary.txt; arm 1 is the provisioned "main").
"""
import pathlib
import re
import statistics
import sys


def prom(path):
    out = {}
    for line in path.read_text().splitlines():
        if line.startswith("#") or " " not in line:
            continue
        k, _, v = line.rpartition(" ")
        try:
            out[k] = float(v)
        except ValueError:
            pass
    return out


def delivered(snapshot):
    return sum(v for k, v in snapshot.items() if k.startswith("mqttd_publish_delivered_total"))


def cpu_seconds(path):
    """Busy CPU-seconds and its usr/sys/soft parts from one host's mpstat stream."""
    ncpu, busy, parts = 1, 0.0, {"usr": 0.0, "sys": 0.0, "soft": 0.0}
    cols = None
    for line in path.read_text().splitlines():
        m = re.search(r"\((\d+) CPU\)", line)
        if m:
            ncpu = int(m.group(1))
        f = line.split()
        if len(f) > 2 and f[1] == "CPU":
            cols = f[2:]
            continue
        if cols and len(f) == len(cols) + 2 and f[1] == "all":
            row = dict(zip(cols, map(float, f[2:])))
            busy += (100 - row["%idle"]) / 100 * ncpu
            parts["usr"] += row["%usr"] / 100 * ncpu
            parts["sys"] += row["%sys"] / 100 * ncpu
            parts["soft"] += row["%soft"] / 100 * ncpu
    return busy, parts


def arm_cost(arm, sites):
    res = next(arm.glob("results/nodes=*/laneE"), None)
    rung = res / f"sites-{sites}" if res else None
    if not rung or not rung.is_dir():
        return None
    msgs = busy = 0.0
    parts = {"usr": 0.0, "sys": 0.0, "soft": 0.0}
    for close in sorted(rung.glob("metrics-window-close-broker*.prom")):
        b = close.name.removeprefix("metrics-window-close-").removesuffix(".prom")
        opened = rung / f"metrics-window-open-{b}.prom"
        cpu = rung / "cpu" / f"cpu-{b}.txt"
        if not (opened.exists() and cpu.exists()):
            return None
        msgs += delivered(prom(close)) - delivered(prom(opened))
        s, p = cpu_seconds(cpu)
        busy += s
        for k in parts:
            parts[k] += p[k]
    if msgs <= 0:
        return None
    verdict = ""
    v = res / "ladder-verdicts.txt"
    if v.exists():
        for line in v.read_text().splitlines():
            if line.startswith(f"sites-{sites} "):
                verdict = line.split(" ", 1)[1][:40]
    per_k = lambda x: x / msgs * 1000 * 1000  # CPU-ms per 1k messages
    return {"msgs": msgs, "ms_per_1k": per_k(busy), **{k: per_k(x) for k, x in parts.items()}, "verdict": verdict}


def binary_of(arm):
    f = arm / "arm-binary.txt"
    return f.read_text().split("\n", 1)[0].split("=", 1)[1] if f.exists() else "main"


def main():
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    campaign, sites = pathlib.Path(sys.argv[1]), sys.argv[2]
    groups = {}
    print(f"{'arm':10} {'binary':6} {'delivered':>11} {'CPU-ms/1k':>10} {'usr':>7} {'sys':>7} {'soft':>7}  verdict")
    for arm in sorted(p for p in campaign.iterdir() if p.is_dir() and re.match(r"\d+-n\d+", p.name)):
        c = arm_cost(arm, sites)
        if not c:
            continue
        b = binary_of(arm)
        groups.setdefault(b, []).append(c["ms_per_1k"])
        print(f"{arm.name:10} {b:6} {c['msgs']:11.0f} {c['ms_per_1k']:10.1f} {c['usr']:7.1f} {c['sys']:7.1f} {c['soft']:7.1f}  {c['verdict']}")
    print()
    for b, xs in groups.items():
        spread = f"[{min(xs):.1f}..{max(xs):.1f}]" if len(xs) > 1 else ""
        print(f"{b:6} n={len(xs)}  mean {statistics.mean(xs):7.1f} CPU-ms/1k msgs {spread}")
    if {"main", "alt"} <= groups.keys():
        a, b = statistics.mean(groups["main"]), statistics.mean(groups["alt"])
        print(f"alt vs main: {(b - a) / a * 100:+.1f}% CPU per message")


if __name__ == "__main__":
    main()
