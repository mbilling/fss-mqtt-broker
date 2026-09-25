#!/usr/bin/env python3
"""Write future campaign configs only; never contacts a provider."""

import argparse
from pathlib import Path

p = argparse.ArgumentParser()
p.add_argument("output", type=Path)
a = p.parse_args()
a.output.mkdir(parents=True, exist_ok=True)
for n, top in [(3, 6), (5, 10), (7, 14), (10, 20)]:
    # Steps are intentionally modest; refine around the first VALID failed rung.
    step = max(1, n // 3)
    up = list(range(step, top + 1, step))
    down = up[-2::-1]
    sweep = up + down
    sites = " ".join(map(str, sweep * 3 + [up[-2]]))
    drivers = min(12, max(5, (top * 3 + 3) // 4))
    text = f'''# Prepared only. This file authorizes no run.
export MQTTD_VERSION=1.0.17 LANES=E CLOUD=hcloud
export BROKER_TYPE=ccx23 DRIVER_TYPE=ccx33 DRIVER_COUNT={drivers}
export TF_VAR_build_bench=false
export LANE_E_QOS=1 LANE_E_SUB_QOS=1 LANE_E_PUBS_PER_SITE=3000 LANE_E_SUBS_PER_SITE=10
export LANE_E_SITES_OVERRIDE="{sites}"
export LANE_E_PLACEMENT=container LANE_E_PIN_SITES=0
export LANE_E_SECS=300 LANE_E_HOLD_LAST_SECS=1800
export LANE_E_CONTROL=1 LANE_E_FORWARD_CANARY=1 LANE_E_CALIBRATE=0
export OBSERVE=0 KEEP_INFRA=0
# Preflight: PREFLIGHT_ONLY=1 ../run.sh full {n}
# Paid execution requires separate authorization. Calibrate this actual shape first.
'''
    (a.output / f"nodes-{n}.env").write_text(text)
print(a.output)
