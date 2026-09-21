#!/usr/bin/env python3
"""Check driver allocation repeats separately from broker capacity qualification."""

import argparse
import itertools
import json
import statistics
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("analysis", type=Path, help="directory produced by report.py")
args = parser.parse_args()
rows = json.loads((args.analysis / "rungs.json").read_text())
complete = [r for r in rows if not any(f.startswith("INCOMPLETE") for f in r["flags"])]
load = max((r["offered"] for r in complete), default=0)
groups = {}
for r in complete:
    if r["offered"] == load and not r.get("control"):
        groups.setdefault(str(r.get("pub_containers_per_site")), []).append(r)
summary = {}
for cells, rs in groups.items():
    passed = len(rs) >= 2 and all(r["pass"] and r["measurements_valid"] for r in rs)
    values = [r["recv_rate"] for r in rs]
    summary[cells] = {
        "repetitions": len(rs),
        "all_pass": passed,
        "received_median": statistics.median(values),
        "spread_fraction": (max(values) - min(values)) / statistics.median(values)
        if min(values) > 0
        else None,
        "rungs": [r["path"] for r in rs],
    }
passing = sorted(
    int(k)
    for k, v in summary.items()
    if k.isdigit() and v["all_pass"] and v["spread_fraction"] <= 0.03
)
controls = [r for r in complete if r.get("control")]
control_ok = bool(controls) and all(
    r["pass"] and r["measurements_valid"] for r in controls
)
selected = None
for smaller, larger in itertools.pairwise(passing):
    a, b = (
        summary[str(smaller)]["received_median"],
        summary[str(larger)]["received_median"],
    )
    if abs(a - b) / max(a, b) <= 0.03:
        selected = larger
comparable = (
    bool(rows)
    and len(
        {
            (
                r.get("nodes"),
                r.get("placement"),
                r.get("sub_containers_per_site"),
                r.get("qos"),
                r.get("sub_qos"),
                r.get("placement_version"),
            )
            for r in rows
        }
    )
    == 1
)
comparable = (
    comparable
    and len({str(Path(r["path"]).parent) for r in rows}) == 1
    and not any(r.get("image_transition") for r in rows)
)
# Compare the actual subscriber-to-host map at the target load, not just the
# policy name. Older placement moved subscribers when publishers were resized.
target = [r for r in complete if r["offered"] == load and not r.get("control")]
comparable = comparable and bool(target) and all(r.get("subscriber_placement") for r in target) and len({json.dumps(r["subscriber_placement"], sort_keys=True) for r in target}) == 1
result = {
    "scope": "driver calibration at this cluster/load only; not a node-scaling result",
    "requested_rate": load,
    "groups": summary,
    "closing_control_pass": control_ok,
    "same_cluster_and_subscriber_policy": comparable,
    "pass": selected is not None
    and control_ok
    and comparable
    and len(complete) == len(rows),
    "candidate_pub_containers_per_site": selected,
    "criteria": "Two passing repeats at each of two allocations; received repeat spread and allocation median difference <=3%; passing closing control; no incomplete rung. Use the larger allocation for margin.",
}
(args.analysis / "CALIBRATION.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
raise SystemExit(0 if result["pass"] else 1)
