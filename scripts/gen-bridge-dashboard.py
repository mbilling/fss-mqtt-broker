#!/usr/bin/env python3
"""Generate the bridge Grafana dashboard.

Written as a generator rather than hand-edited JSON for the same reason the
delivery dashboard is generated: 30 panels of duplicated datasource/target
boilerplate is where copy-paste drift lives. The panel *intent* is the source
here; the JSON is output.

Run: python3 scripts/gen-bridge-dashboard.py
Then: demo/grafana/dashboards/mqttd-bridge.json is rewritten in place.
"""

from __future__ import annotations

import json
from pathlib import Path

OUT = Path(__file__).resolve().parent.parent / "demo/grafana/dashboards/mqttd-bridge.json"
DS = {"type": "prometheus", "uid": "prometheus"}

# The instance selector is applied to every query, matching the broker dashboard.
SEL = '{instance=~"$instance"}'


def target(expr: str, legend: str, ref: str = "A") -> dict:
    return {"refId": ref, "datasource": DS, "expr": expr, "legendFormat": legend}


def stat(title: str, expr: str, legend: str, x: int, y: int, *, w: int = 6, h: int = 4,
         unit: str = "short", thresholds: list | None = None, mappings: list | None = None,
         desc: str = "") -> dict:
    field: dict = {"unit": unit}
    if thresholds:
        field["thresholds"] = {"mode": "absolute", "steps": thresholds}
    if mappings:
        field["mappings"] = mappings
    return {
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "targets": [target(expr, legend)],
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": field, "overrides": []},
        "options": {"colorMode": "background", "graphMode": "area",
                    "reduceOptions": {"calcs": ["lastNonNull"], "fields": "", "values": False}},
    }


def timeseries(title: str, targets: list[dict], x: int, y: int, *, w: int = 12, h: int = 8,
               unit: str = "short", desc: str = "", stack: bool = False) -> dict:
    custom = {"fillOpacity": 10, "lineWidth": 2, "showPoints": "never"}
    if stack:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    return {
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "targets": targets,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "custom": custom}, "overrides": []},
        "options": {"legend": {"displayMode": "table", "placement": "bottom",
                               "calcs": ["lastNonNull", "max"]},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def row(title: str, y: int) -> dict:
    return {"type": "row", "title": title, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
            "collapsed": False, "panels": []}


UP_DOWN = [
    {"type": "value", "options": {"0": {"text": "DOWN", "color": "red", "index": 0},
                                  "1": {"text": "UP", "color": "green", "index": 1}}}
]
GREEN_ONLY = [{"color": "green", "value": None}]
ZERO_IS_GOOD = [{"color": "green", "value": None}, {"color": "red", "value": 1}]
SPOOL_STEPS = [{"color": "green", "value": None},
               {"color": "yellow", "value": 1},
               {"color": "red", "value": 1000}]

panels: list[dict] = []
y = 0

# ---------------------------------------------------------------------------
panels.append(row("Health — is the boundary up, and is anything stuck?", y)); y += 1

panels.append(stat(
    "Sides connected", f"sum(fss_bridge_connected{SEL})", "connected", 0, y,
    thresholds=GREEN_ONLY,
    desc="Sides with a live MQTT connection. The bridge is a client to BOTH sides, so a "
         "healthy two-broker bridge shows 2: the local cluster and one upstream."))
panels.append(stat(
    "Sides DOWN", f"count(fss_bridge_connected{SEL} == 0) or vector(0)", "down", 6, y,
    thresholds=ZERO_IS_GOOD,
    desc="Any side whose connection is currently down. While a side is down its traffic is "
         "spooled, not dropped — until the spool reaches its bound."))
panels.append(stat(
    "Buffered messages", f"sum(fss_bridge_spool_depth{SEL})", "buffered", 12, y,
    thresholds=SPOOL_STEPS,
    desc="Messages currently held in store-and-forward spools, across all sides. Non-zero "
         "means a side is down or slower than the traffic offered to it."))
panels.append(stat(
    "Shed (spool full)", f"sum(fss_bridge_dropped_total{{reason=\"spool-full\",instance=~\"$instance\"}}) or vector(0)",
    "shed", 18, y, thresholds=ZERO_IS_GOOD,
    desc="Messages THROWN AWAY because a spool hit its bound. Drop-oldest is the intended "
         "policy for a bounded buffer, but any non-zero value means the bridge has lost "
         "messages — raise the spool cap, or fix why the side is down."))
y += 4

panels.append(timeseries(
    "Connection state per side", [target(f"fss_bridge_connected{SEL}", "{{side}}")],
    0, y, w=12, unit="short",
    desc="1 = connected, 0 = down. `local` is the connection to your own cluster; the others "
         "are the configured upstreams by name."))
panels.append(timeseries(
    "Reconnects /s per side",
    [target(f"rate(fss_bridge_reconnects_total{SEL}[5m])", "{{side}}")],
    12, y, w=12,
    desc="A steady non-zero rate means a side is flapping — each reconnect re-subscribes and "
         "replays that side's spool."))
y += 8

# ---------------------------------------------------------------------------
panels.append(row("Throughput — rolling 1m / 5m / 15m", y)); y += 1

for i, win in enumerate(("1m", "5m", "15m")):
    panels.append(stat(
        f"Forwarded /s ({win})",
        f"sum(rate(fss_bridge_forwarded_total{SEL}[{win}]))", f"{win}",
        i * 6, y, unit="reqps", thresholds=GREEN_ONLY,
        desc=f"Messages crossing the boundary per second, averaged over {win}. Computed by "
             "Prometheus from the monotonic counter — the bridge exports no windowed metric "
             "of its own, so these stay correct across restarts and multiple replicas."))
panels.append(stat(
    "Bytes /s (5m)", f"sum(rate(fss_bridge_forwarded_bytes_total{SEL}[5m]))", "5m",
    18, y, unit="Bps", thresholds=GREEN_ONLY,
    desc="Payload throughput. Message rate alone hides a change in message size."))
y += 4

for label, win in (("1 minute", "1m"), ("5 minutes", "5m"), ("15 minutes", "15m")):
    panels.append(timeseries(
        f"Forwarded /s by upstream and direction — {label} window",
        [target(f"sum by (upstream, direction) (rate(fss_bridge_forwarded_total{SEL}[{win}]))",
                "{{upstream}} {{direction}}")],
        0 if win != "5m" else 12, y if win != "15m" else y + 8, w=12,
        unit="reqps",
        desc="`out` is local→upstream, `in` is upstream→local. A one-way rule shows traffic in "
             "one direction only — if you see both for a rule you meant to be unidirectional, "
             "the policy is not what you think it is."))
y += 8

panels.append(timeseries(
    "Bytes /s by upstream and direction (5m)",
    [target(f"sum by (upstream, direction) (rate(fss_bridge_forwarded_bytes_total{SEL}[5m]))",
            "{{upstream}} {{direction}}")],
    12, y, w=12, unit="Bps"))
y += 8

# ---------------------------------------------------------------------------
panels.append(row("Buffering — store-and-forward depth and loss", y)); y += 1

panels.append(timeseries(
    "Spool depth per side", [target(f"fss_bridge_spool_depth{SEL}", "{{side}}")],
    0, y, w=12,
    desc="Messages queued for a side that is down or behind. This is the bridge's durability "
         "buffer: it fills while a side is unreachable and drains on reconnect."))
panels.append(timeseries(
    "Spool used vs its bound (%)",
    [target(f"100 * fss_bridge_spool_depth{SEL} / fss_bridge_spool_capacity{SEL}", "{{side}}")],
    12, y, w=12, unit="percent",
    desc="Approaching 100% means the spool is about to start dropping the OLDEST queued "
         "messages. The cap is per side (`spool.max_messages`)."))
y += 8

panels.append(timeseries(
    "Messages shed /s (spool full)",
    [target(f"rate(fss_bridge_dropped_total{{reason=\"spool-full\",instance=~\"$instance\"}}[5m])",
            "{{side}}")],
    0, y, w=12,
    desc="ANY non-zero value here is message loss. Alert on it."))
panels.append(timeseries(
    "Messages dropped /s (hop limit)",
    [target(f"rate(fss_bridge_dropped_total{{reason=\"hop-limit\",instance=~\"$instance\"}}[5m])",
            "hop-limit")],
    12, y, w=12,
    desc="Loop protection working as designed: a message whose hop count reached the limit is "
         "dropped rather than circulated. A steady rate means two bridges are forwarding each "
         "other's traffic — check the rules and topic remaps."))
y += 8

dashboard = {
    "uid": "mqttd-bridge",
    "title": "mqttd — boundary bridge",
    "tags": ["mqttd", "bridge"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-30m", "to": "now"},
    "annotations": {"list": []},
    "templating": {
        "list": [
            {
                "name": "instance",
                "label": "Bridge",
                "type": "query",
                "datasource": DS,
                "query": {"query": "label_values(fss_bridge_connected, instance)", "refId": "A"},
                "includeAll": True,
                "multi": True,
                "refresh": 2,
                "current": {"text": "All", "value": "$__all"},
                "sort": 1,
            }
        ]
    },
    "panels": panels,
}

OUT.write_text(json.dumps(dashboard, indent=2) + "\n", encoding="utf-8")
print(f"wrote {OUT.relative_to(Path.cwd())} ({len(panels)} panels)")
