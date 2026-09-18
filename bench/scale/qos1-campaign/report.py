#!/usr/bin/env python3
"""Recompute every rung. Invalid evidence never becomes a capacity point."""

import argparse
import importlib.util
import json
import statistics
from pathlib import Path

sp = importlib.util.spec_from_file_location(
    "curve", Path(__file__).resolve().parent.parent / "summarize-curve.py"
)
c = importlib.util.module_from_spec(sp)
sp.loader.exec_module(c)
p = argparse.ArgumentParser()
p.add_argument("runs", nargs="+", type=Path)
p.add_argument("--output", type=Path, required=True)
a = p.parse_args()
rows = []
for root in a.runs:
    results = root / "results" if (root / "results").is_dir() else root
    for n, d in c.sizes(results):
        for rung in sorted((d / "laneE").glob("sites-*")):
            r = c.lane_e_rung(rung)
            r.update(
                nodes=n,
                path=str(rung),
                calibration_only=(root / "driver-transition").exists(),
            )
            r["measurements_valid"] = not any(
                f.startswith(("INVALID EVIDENCE", "INCOMPLETE")) for f in r["flags"]
            )
            rows.append(r)
a.output.mkdir(parents=True, exist_ok=True)
(a.output / "rungs.json").write_text(json.dumps(rows, indent=2))
lines = [
    "# QoS 1 measurement report",
    "",
    "Scope: clean-session, local/shared MQTT5 delivery. This is not a persistent-session durability curve.",
    "",
    "| nodes | sites | repetition | requested/s | emitted/s | received/s | late % | p99 upper bound | result |",
    "|---|---|---|---|---|---|---|---|---|",
]
if any(r.get("calibration_only") for r in rows):
    lines[4:4] = [
        "**Calibration only:** the driver image changed during this provisioning. Image installation also perturbed the active rung. These observations cannot be combined into a capacity curve; consult driver-transition and per-rung driver-images.json.",
        "",
    ]
for r in rows:
    verdict = "PASS" if r["pass"] else "; ".join(r["flags"]) or "FAIL"
    valid = r["measurements_valid"]
    if not valid:
        verdict = "; ".join(
            f
            for f in r["flags"]
            if f.startswith(("INVALID EVIDENCE", "INCOMPLETE", "NOT STEADY"))
        )
    measurements = (
        f"{r['sent_rate']:.1f} | {r['recv_rate']:.1f} | {100 * r.get('late_share', 0):.3f} | {r['p99']}"
        if valid
        else "unvalidated | unvalidated | unvalidated | unvalidated"
    )
    offer_label = (
        f"{r['offered']:.0f}"
        if not any(f.startswith("INCOMPLETE") for f in r["flags"])
        else "unknown"
    )
    lines.append(
        f"| {r['nodes']} | {r['sites']} | {r['rep']}{' control' if r.get('control') else ''} | {offer_label} | {measurements} | {verdict} |"
    )
lines += [
    "",
    "## Scaling qualification",
    "",
    "A capacity point requires at least three passing repetitions at the same load, a passing closing control, and no failing repetition at or below that load. A passing top rung is a lower bound, not a maximum. Repetitions within one provisioning do not establish host-to-host variability.",
    "",
]
capacities = {}
for n in sorted({r["nodes"] for r in rows}):
    rs = [r for r in rows if r["nodes"] == n]
    controls = [r for r in rs if r.get("control")]
    candidates = []
    for offer in sorted({r["offered"] for r in rs if not r.get("control")}):
        reps = [r for r in rs if r["offered"] == offer and not r.get("control")]
        lower = [r for r in rs if r["offered"] <= offer]
        if (
            len(reps) >= 3
            and not any(r.get("calibration_only") for r in rs)
            and all(r["pass"] for r in lower)
            and controls
            and all(r["pass"] for r in controls)
        ):
            candidates.append((offer, reps))
    if candidates:
        offer, reps = candidates[-1]
        vals = [r["recv_rate"] for r in reps]
        capacities[n] = statistics.median(vals)
        lines.append(
            f"- {n} nodes: repeated operating point {offer:,.0f}/s; received median {statistics.median(vals):,.1f}/s, range {min(vals):,.1f}–{max(vals):,.1f}/s. Boundary not established by this statistic."
        )
    else:
        lines.append(
            f"- {n} nodes: insufficient valid repetitions/control for a qualified scaling point."
        )
if all(n in capacities for n in [3, 5, 7, 10]):
    for n, v in capacities.items():
        lines.append(
            f"- {n}-node efficiency relative to 3 nodes: {v / (capacities[3] * n / 3):.3f}. Compare routing and driver calibration before interpreting."
        )
else:
    lines += [
        "",
        "**Cross-size scaling remains unproven:** valid 3-, 5-, 7-, and 10-node points are not all available.",
    ]
(a.output / "REPORT.md").write_text("\n".join(lines) + "\n")
print(a.output / "REPORT.md")
