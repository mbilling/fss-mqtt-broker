#!/usr/bin/env python3
"""Recompute every local calibration interval from retained counters and times."""

import argparse
import importlib.util
import json
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("run", type=Path)
a = p.parse_args()
sp = importlib.util.spec_from_file_location(
    "e", Path(__file__).resolve().parent.parent / "lane-e-evidence.py"
)
e = importlib.util.module_from_spec(sp)
sp.loader.exec_module(e)
times = json.loads((a.run / "sample-times.json").read_text())
endpoints = json.loads((a.run / "endpoints.json").read_text())
config = json.loads((a.run / "config.json").read_text())
rows = []
for index in range(1, len(times)):
    rates = {"sent": 0.0, "acked": 0.0, "received": 0.0, "late": 0.0}
    bounds = {}
    count = 0
    uncertainty = 0.0
    for ep in endpoints:
        name = ep["name"]
        first, last = times[index - 1][name], times[index][name]
        dt = last["at"] - first["at"]
        if dt <= 0:
            raise ValueError("nonpositive sample interval")
        uncertainty = max(
            uncertainty,
            ((first["end"] - first["start"]) + (last["end"] - last["start"])) / dt,
        )
        before = e.metric(a.run / f"{name}-sample{index - 1}.prom")
        after = e.metric(a.run / f"{name}-sample{index}.prom")
        pairs = (
            [("received", "recv")]
            if ep["role"] == "sub"
            else [
                ("sent", "audit_sent"),
                ("acked", "audit_acked"),
                ("late", "pub_overrun"),
            ]
        )
        for label, metric in pairs:
            rates[label] += e.delta(before, after, metric) / dt
        if ep["role"] == "sub":
            h, n = e.histogram(before, after, "e2e_latency")
            count += n
            for bound, v in h.items():
                bounds[bound] = bounds.get(bound, 0) + v
    p99 = next(
        (bound for bound in sorted(bounds) if bounds[bound] >= count * 0.99),
        float("inf"),
    )
    late = rates["late"] / rates["sent"] if rates["sent"] else 1.0
    flags = []
    if not all(
        config["publishers"] * 10 * 0.97 <= rates[k] <= config["publishers"] * 10 * 1.03
        for k in ["sent", "acked", "received"]
    ):
        flags.append("rate outside 3% band")
    if late > 0.05:
        flags.append("lateness exceeds 5%")
    if p99 > 1000:
        flags.append("p99 exceeds 1000ms")
    if uncertainty > 0.02:
        flags.append("scrape uncertainty exceeds 2%")
    rows.append(
        {
            "interval": index,
            "rates": rates,
            "late_share": late,
            "p99_upper_ms": p99,
            "scrape_uncertainty": uncertainty,
            "pass": not flags,
            "flags": flags,
        }
    )
result = {
    "intervals": rows,
    "all_intervals_pass": bool(rows) and all(r["pass"] for r in rows),
    "min_received_rate": min((r["rates"]["received"] for r in rows), default=0),
    "max_received_rate": max((r["rates"]["received"] for r in rows), default=0),
    "max_p99_upper_ms": max((r["p99_upper_ms"] for r in rows), default=None),
}
(a.run / "intervals.json").write_text(json.dumps(result, indent=2))
print(json.dumps({k: v for k, v in result.items() if k != "intervals"}, indent=2))
raise SystemExit(0 if result["all_intervals_pass"] else 1)
