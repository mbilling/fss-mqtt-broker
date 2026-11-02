#!/usr/bin/env python3
"""The broker hosts' CPU and disks per step, read from a rig run's host samples.

    host-load.py <run dir | ladder dir | step dir>          # a markdown table per ladder
    host-load.py <dir> --json                               # the same figures as JSON

scrape-loop.py --host writes `<unix_ms>.host.json` beside every broker scrape: the
machine's CPU jiffies, the broker process's CPU time and the disks' I/O counters. For
every broker, over each round's measured window (the generators' overlap, the one
verdict.py judges the round on, read as hub-model.py reads it), the first and last host
sample inside give:

  broker cores  the broker process's CPU seconds a second (utime + stime), and the
                kernel's part of them (`system`: syscalls and the socket path they run)
  µs per msg    broker CPU µs per message the broker received (its own scrapes)
  host busy     the machine's busy CPUs (every column but idle and iowait) of the CPUs
                it has, as the busy share of every jiffy counted (the tick counts of a
                32-CPU host add up to 29–36 CPUs a second); softirq (mostly the network's
                receive path) and steal beside it, and the busiest single CPU's busy share
  ctxt/s        context switches a second, machine-wide
  disk          the busiest whole disk: writes/s, MB written/s, flushes/s (what an fsync
                sends a disk with a write cache; 0 on one without) and its busy share
                (io_ms a second: 1.0 is an I/O in flight all the time, not a full disk)

Figures are per broker from that broker's own clock, then across a step's rounds weighted
by the messages each round received. A step's row is its busiest broker host's, with µs
per message as the cluster's (every broker's CPU over every broker's messages). A broker
with no host samples in a round (a run from before --host) is left out of it, and a step
none of whose brokers has any prints no figures.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


HM = _load("hub_model", HERE / "hub-model.py")
E = HM.E
SECTOR = 512  # /proc/diskstats counts 512-byte sectors whatever the disk's own size


def load_host(d: Path) -> list[tuple[int, dict]]:
    """(unix_ms, sample) for every readable host sample in d, sorted; a sample written
    as an error, or with no CPU line, is skipped."""
    out = []
    for p in sorted(d.glob("*.host.json")):
        try:
            s = json.loads(p.read_text())
        except ValueError:
            continue
        if not isinstance(s, dict) or "error" in s or not s.get("cpu"):
            continue
        out.append((int(s.get("unix_ms") or p.name.split(".")[0]), s))
    out.sort(key=lambda x: x[0])
    return out


def _inside(samples: list, lo_ms: float, hi_ms: float, margin_ms: float) -> list:
    return [s for s in samples if lo_ms + margin_ms <= s[0] <= hi_ms - margin_ms]


def broker_host(host: list[tuple[int, dict]], scrapes: list[tuple[int, dict]],
                lo_ms: float, hi_ms: float, margin_ms: float) -> dict | None:
    """One broker host's figures over [lo + margin, hi - margin], from its first and last
    host sample inside; None with fewer than two inside."""
    inside = _inside(host, lo_ms, hi_ms, margin_ms)
    if len(inside) < 2:
        return None
    (t0, a), (t1, z) = inside[0], inside[-1]
    dt = (t1 - t0) / 1000
    hz = float(z.get("clk_tck") or 100)
    fields = z.get("cpu_fields") or ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"]
    col = {n: i for i, n in enumerate(fields)}
    busy_cols = [i for n, i in col.items() if n not in ("idle", "iowait")]
    tot = [y - x for x, y in zip(a["cpu"], z["cpu"])]
    if any(v < 0 for v in tot):
        raise ValueError("the machine's CPU counters went backwards inside the window (a reboot?)")

    ncpu = len(z.get("per_cpu", [])) or None
    whole = sum(tot)

    def cores(name_or_cols) -> float:
        """CPUs' worth of the named columns: their share of every jiffy the machine
        counted, times its CPUs. The tick counts of an idle CPU lag and lead, so the
        columns of a 32-CPU host add up to 29–36 CPUs a second; their shares do not."""
        cols = [col[n] for n in name_or_cols if n in col] if isinstance(name_or_cols, list) \
            else ([col[name_or_cols]] if name_or_cols in col else [])
        part = sum(tot[i] for i in cols if i < len(tot))
        if ncpu and whole > 0:
            return part / whole * ncpu
        return part / hz / dt

    per_cpu = []
    for x, y in zip(a.get("per_cpu", []), z.get("per_cpu", [])):
        d = [q - p for p, q in zip(x, y)]
        # Each CPU's own total, not hz × dt: the tick accounting of an idle CPU lags.
        own = sum(d)
        per_cpu.append(sum(d[i] for i in busy_cols if i < len(d)) / own if own > 0 else 0.0)
    out = {"window_s": dt, "cpus": ncpu,
           "host_busy": cores([n for n in fields if n not in ("idle", "iowait")]),
           "softirq": cores("softirq"), "steal": cores("steal"), "iowait": cores("iowait"),
           "busiest_cpu": max(per_cpu) if per_cpu else None,
           "ctxt_s": (z["ctxt"] - a["ctxt"]) / dt if "ctxt" in a and "ctxt" in z else None,
           "process": None, "received_rate": None, "us_per_msg": None, "disk": None, "samples": len(inside)}
    pa, pz = a.get("process"), z.get("process")
    # The same process at both ends (pid and start time): a restarted broker's CPU is not
    # a difference of the two readings.
    if pa and pz and pa.get("pid") == pz.get("pid") and pa.get("starttime") == pz.get("starttime"):
        user = (pz["utime"] - pa["utime"]) / hz / dt
        system = (pz["stime"] - pa["stime"]) / hz / dt
        out["process"] = {"cores": user + system, "user": user, "system": system, "threads": pz.get("threads")}
    rx_in = _inside(scrapes, lo_ms, hi_ms, margin_ms)
    if len(rx_in) >= 2:
        (s0, sa), (s1, sz) = rx_in[0], rx_in[-1]
        rx = E.delta(sa, sz, E.RECEIVED)
        if rx >= 0 and s1 > s0:
            out["received_rate"] = rx / ((s1 - s0) / 1000)
            if out["process"] and out["received_rate"] > 0:
                out["us_per_msg"] = out["process"]["cores"] / out["received_rate"] * 1e6
    disks = {}
    for name, y in (z.get("disks") or {}).items():
        x = (a.get("disks") or {}).get(name)
        if not x:
            continue
        d = {k: y[k] - x[k] for k in y if k in x}
        writes = d.get("writes", 0)
        disks[name] = {"writes_s": writes / dt, "reads_s": d.get("reads", 0) / dt,
                       "write_mb_s": d.get("write_sectors", 0) * SECTOR / dt / 1e6,
                       "ms_per_write": d["write_ms"] / writes if writes > 0 and "write_ms" in d else None,
                       "flushes_s": d["flushes"] / dt if "flushes" in d else None,
                       "busy": d.get("io_ms", 0) / 1000 / dt}
    if disks:
        name, d = max(disks.items(), key=lambda kv: (kv[1]["busy"], kv[1]["writes_s"]))
        out["disk"] = {"name": name, **d}
    return out


def round_host(rd: Path, margin_ms: float = 1000.0) -> dict:
    """Every broker host of a round over the round's measured window."""
    out = {"dir": rd.name, "brokers": {}, "problems": []}
    win = HM.round_window(rd)
    if win is None:
        out["problems"].append("no measured window (no verdict.json and no generator summary)")
        return out
    lo, hi = win
    root = rd / "scrapes"
    for d in sorted((p for p in root.glob("broker-*") if p.is_dir()), key=lambda p: int(p.name.split("-")[1])):
        idx = int(d.name.split("-")[1])
        host = load_host(d)
        if not host:
            continue  # a run from before --host, or HOST_PROC= turned it off
        try:
            m = broker_host(host, HM.load_samples(d), lo, hi, margin_ms)
        except ValueError as e:
            out["problems"].append(f"broker {idx}: {e}")
            continue
        if m is None:
            out["problems"].append(f"broker {idx}: fewer than two host samples inside the window")
            continue
        out["brokers"][idx] = m
    return out


def _wmean(pairs: list) -> float | None:
    return HM._wmean(pairs)


def merge_rounds(rounds: list[dict]) -> dict[int, dict]:
    """Per broker across a step's rounds, every figure weighted by the round's received
    messages (by its window when the broker's scrapes gave no received rate)."""
    per: dict[int, list[dict]] = {}
    for r in rounds:
        for idx, m in r["brokers"].items():
            per.setdefault(idx, []).append(m)
    out = {}
    for idx, ms in per.items():
        w = [(m, (m["received_rate"] or 0.0) * m["window_s"] or m["window_s"]) for m in ms]

        def mean(key, sub=None):
            return _wmean([((m[sub] or {}).get(key) if sub else m.get(key), wt) for m, wt in w])

        procs = [m for m, _ in w if m["process"]]
        disks = [m for m, _ in w if m["disk"]]
        out[idx] = {"rounds": len(ms), "cpus": next((m["cpus"] for m in ms if m["cpus"]), None),
                    "host_busy": mean("host_busy"), "softirq": mean("softirq"), "steal": mean("steal"),
                    "iowait": mean("iowait"), "busiest_cpu": mean("busiest_cpu"), "ctxt_s": mean("ctxt_s"),
                    "received_rate": mean("received_rate"),
                    "process": {k: mean(k, "process") for k in ("cores", "user", "system")} if procs else None,
                    "disk": {"name": disks[-1]["disk"]["name"],
                             **{k: mean(k, "disk") for k in ("writes_s", "reads_s", "write_mb_s", "ms_per_write",
                                                             "flushes_s", "busy")}} if disks else None}
    return out


def step_host(step: Path) -> dict:
    """A step's rounds (round-K[-aN] directories, else the step itself), merged."""
    meta = json.loads((step / "step.json").read_text()) if (step / "step.json").is_file() else {}
    margin_ms = float((meta.get("judge") or {}).get("margin_s", 1.0)) * 1000
    rounds = sorted((p for p in step.iterdir() if p.is_dir() and HM.ROUND_DIR.match(p.name)),
                    key=lambda p: (int(HM.ROUND_DIR.match(p.name).group(1)),
                                   int(HM.ROUND_DIR.match(p.name).group(2) or 0)))
    rms = [round_host(r, margin_ms) for r in (rounds or [step])]
    verdict = None
    if (step / "verdict.json").is_file():
        try:
            verdict = json.loads((step / "verdict.json").read_text()).get("verdict")
        except ValueError:
            verdict = None
    brokers = merge_rounds(rms)
    busiest = None
    if brokers:
        i, m = max(brokers.items(), key=lambda kv: kv[1]["host_busy"] or 0.0)
        cores = sum((b["process"] or {}).get("cores") or 0.0 for b in brokers.values())
        rx = sum(b["received_rate"] or 0.0 for b in brokers.values() if b["process"])
        busiest = {"node": i, **m, "cluster_us_per_msg": cores / rx * 1e6 if rx > 0 and cores > 0 else None}
    return {"step": meta.get("index"), "id": meta.get("id", step.name), "rate": meta.get("rate"),
            "verdict": verdict, "brokers": brokers, "busiest": busiest,
            "problems": [f"{r['dir']}: {p}" for r in rms for p in r["problems"]]}


def ladder_host(ladder: Path) -> dict:
    steps_dir = ladder / "steps"
    steps = sorted((p for p in steps_dir.iterdir() if p.is_dir() and (p / "step.json").is_file()),
                   key=lambda p: json.loads((p / "step.json").read_text()).get("index", 0)) if steps_dir.is_dir() else []
    return {"ladder": ladder.name, "steps": [step_host(s) for s in steps]}


def find_models(path: Path) -> list[dict]:
    """A run, a ladder or a step directory → its ladders' host figures."""
    if (path / "ladder.json").is_file():
        return [ladder_host(path)]
    if (path / "step.json").is_file():
        return [{"ladder": path.parent.parent.name, "steps": [step_host(path)]}]
    return [ladder_host(p.parent) for p in sorted(path.glob("**/ladder.json"))]


def _f(x: float | None, spec: str = ".1f") -> str:
    return "—" if x is None else format(x, spec)


def render_ladder_md(lm: dict) -> list[str]:
    lines = [f"### Host load: {lm['ladder']}", "",
             "Per step, the busiest broker host over the rounds' measured windows: the broker process's CPU "
             "cores (the kernel's part in brackets), its CPU µs per received message (the cluster's), the "
             "machine's busy CPUs of the CPUs it has with softirq and steal, the busiest single CPU, context "
             "switches a second, and the busiest disk's writes, MB written and busy share a second.", "",
             "| step | offered msg/s | verdict | broker cores (system) | µs CPU per msg | host busy CPUs | "
             "softirq | steal | busiest CPU | ctxt/s | disk writes/s | disk MB/s | flushes/s | disk busy |",
             "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for s in lm["steps"]:
        b = s["busiest"]
        if not b:
            why = "; ".join(s["problems"]) or "no host samples"
            lines.append(f"| {s['step']} | {_f(s['rate'], ',.0f')} | {s['verdict'] or '—'} | {why} |"
                         + " — |" * 10)
            continue
        p, d = b["process"] or {}, b["disk"] or {}
        cpus = f" of {b['cpus']}" if b["cpus"] else ""
        lines.append(f"| {s['step']} | {_f(s['rate'], ',.0f')} | {s['verdict'] or '—'} | "
                     f"{_f(p.get('cores'), '.2f')} ({_f(p.get('system'), '.2f')}) | {_f(b['cluster_us_per_msg'])} | "
                     f"{_f(b['host_busy'], '.2f')}{cpus} | {_f(b['softirq'], '.2f')} | {_f(b['steal'], '.2f')} | "
                     f"{_f(b['busiest_cpu'], '.2f')} | {_f(b['ctxt_s'], ',.0f')} | {_f(d.get('writes_s'), ',.0f')} | "
                     f"{_f(d.get('write_mb_s'), '.2f')} | {_f(d.get('flushes_s'), ',.0f')} | {_f(d.get('busy'), '.2f')} |")
    lines.append("")
    multi = [s for s in lm["steps"] if len(s["brokers"]) > 1]
    if multi:
        lines.append("- the busiest host per step: " + " · ".join(f"{s['id']} node {s['busiest']['node']}" for s in multi))
        lines.append("")
    return lines


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("path", type=Path, help="a run, ladder or step directory")
    ap.add_argument("--json", action="store_true", help="print the figures as JSON")
    args = ap.parse_args(argv)
    models = find_models(args.path)
    if not models:
        print(f"host-load.py: no ladder, step or run under {args.path}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(models, indent=2, default=str))
        return 0
    for lm in models:
        print("\n".join(render_ladder_md(lm)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
