#!/usr/bin/env python3
"""Derive the bench stack's dashboards from the demo ones, adding a phase strip.

The demo dashboards ship to users; `bench_run_phase` is pushed by bench/scale's
run.sh and would sit permanently at "no data" there, so the strip cannot simply
be added to the originals. Instead this writes a DERIVED copy for the bench
stack, with a new uid/title so both can coexist in one Grafana.

Why a strip on the broker overview at all: during provisioning and teardown there
is legitimately nothing to scrape, and a blank panel is indistinguishable from a
dead collector. The strip makes the blank state say what it is. Whoever is
watching the broker panels is exactly who needs that answer, which is why it is
not enough to have it only on the dedicated run-status dashboard.

Regenerate after changing demo/grafana/dashboards/*.json:
    python3 bench/scale/observe/build-bench-dashboards.py
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[3]
SRC = ROOT / "demo/grafana/dashboards"
DST = pathlib.Path(__file__).resolve().parent / "dashboards"
DS = {"type": "prometheus", "uid": "prometheus"}
STRIP_H = 3  # rows of grid the strip occupies; everything else shifts down by it


def strip_panels():
    """The phase strip: what the rig is doing, and whether anything is reporting."""
    return [
        {
            "type": "stat", "title": "Run phase", "datasource": DS,
            "gridPos": {"h": STRIP_H, "w": 8, "x": 0, "y": 0},
            "description": (
                "What bench/scale is doing right now. Blank broker panels during "
                "provisioning or teardown are CORRECT — the brokers do not exist "
                "yet — and this is how you tell that from a dead collector."
            ),
            "targets": [{"datasource": DS, "expr": "bench_run_phase",
                         "legendFormat": "{{phase}}", "instant": True,
                         "range": False, "refId": "A"}],
            "options": {"textMode": "name", "colorMode": "background",
                        "graphMode": "none", "justifyMode": "center",
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                                          "values": False}},
            "fieldConfig": {"defaults": {"noValue": "no run attached",
                                         "color": {"mode": "fixed",
                                                   "fixedColor": "blue"},
                                         "mappings": []}, "overrides": []},
        },
        {
            "type": "stat", "title": "Phase detail", "datasource": DS,
            "gridPos": {"h": STRIP_H, "w": 8, "x": 8, "y": 0},
            "targets": [{"datasource": DS, "expr": "bench_run_phase",
                         "legendFormat": "{{detail}}", "instant": True,
                         "range": False, "refId": "A"}],
            "options": {"textMode": "name", "colorMode": "none",
                        "graphMode": "none", "justifyMode": "center",
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                                          "values": False}},
            "fieldConfig": {"defaults": {"noValue": "—", "mappings": []},
                            "overrides": []},
        },
        {
            "type": "stat", "title": "Brokers reporting", "datasource": DS,
            "gridPos": {"h": STRIP_H, "w": 8, "x": 16, "y": 0},
            "description": (
                "Zero here beside a phase of provisioning or teardown is the "
                "expected blank window. Zero beside 'running' is a fault."
            ),
            "targets": [{"datasource": DS,
                         "expr": "count(count by (instance) (mqttd_publish_received_total))",
                         "legendFormat": "brokers", "refId": "A"}],
            "options": {"textMode": "value", "colorMode": "value",
                        "graphMode": "none",
                        "reduceOptions": {"calcs": ["lastNotNull"], "fields": "",
                                          "values": False}},
            "fieldConfig": {"defaults": {"noValue": "0", "decimals": 0},
                            "overrides": []},
        },
    ]


def derive(src: pathlib.Path, dst: pathlib.Path) -> None:
    d = json.loads(src.read_text())
    # Push every existing panel down so the strip owns the top rows. Rows carry
    # their children's positions in `panels`, so those shift too.
    def shift(panels):
        for p in panels:
            if "gridPos" in p:
                p["gridPos"]["y"] = p["gridPos"].get("y", 0) + STRIP_H
            if p.get("panels"):
                shift(p["panels"])
    shift(d.get("panels", []))
    d["panels"] = strip_panels() + d.get("panels", [])
    d["uid"] = f"{d['uid']}-bench"
    d["title"] = f"{d['title']} (bench)"
    d["description"] = (
        "Derived from the demo dashboard by bench/scale/observe/"
        "build-bench-dashboards.py — do not edit by hand. Adds the run-phase "
        "strip, which only the bench stack can populate."
    )
    d.setdefault("tags", []).append("bench")
    dst.write_text(json.dumps(d, indent=2) + "\n")
    print(f"  {src.name} -> {dst.name}  (uid {d['uid']}, {len(d['panels'])} panels)")


def main() -> None:
    DST.mkdir(parents=True, exist_ok=True)
    for src in sorted(SRC.glob("*.json")):
        derive(src, DST / f"{src.stem}-bench.json")


if __name__ == "__main__":
    main()
