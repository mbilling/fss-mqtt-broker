#!/usr/bin/env python3
"""Derive the bench stack's dashboards from the production ones, adding a phase strip.

The production dashboards ship under `deploy/observability/grafana/`;
`bench_run_phase` is pushed by bench/scale's run.sh and would sit permanently
at "no data" there, so the strip cannot simply be added to the originals.
Instead this writes a DERIVED copy for the bench stack, with a new uid/title
so both can coexist in one Grafana.

Why a strip on the broker overview at all: during provisioning and teardown there
is legitimately nothing to scrape, and a blank panel is indistinguishable from a
dead collector. The strip makes the blank state say what it is. Whoever is
watching the broker panels is exactly who needs that answer, which is why it is
not enough to have it only on the dedicated run-status dashboard.

It also writes the mqtt-load dashboard (mqtt-load-bench.json) for rig2's generators,
which has no production original.

Regenerate after changing deploy/observability/grafana/*.json:
    python3 bench/scale/observe/build-bench-dashboards.py
"""
import json
import pathlib

ROOT = pathlib.Path(__file__).resolve().parents[3]
SRC = ROOT / "deploy/observability/grafana"
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


def _ts(title, exprs, x, y, w=12, h=8, unit="short", desc="", log=False, stack=False):
    """A timeseries panel: exprs is [(promql, legend), ...]."""
    custom = {"lineWidth": 2, "fillOpacity": 0, "showPoints": "never"}
    if log:
        custom["scaleDistribution"] = {"type": "log", "log": 10}
    if stack:
        custom["stacking"] = {"mode": "normal", "group": "A"}
    return {
        "type": "timeseries", "title": title, "datasource": DS, "description": desc,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "targets": [{"datasource": DS, "expr": e, "legendFormat": leg, "refId": chr(65 + i)}
                    for i, (e, leg) in enumerate(exprs)],
        "fieldConfig": {"defaults": {"unit": unit, "custom": custom}, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    }


def _quantiles(metric):
    return [(f"histogram_quantile({q}, sum by (le) (rate({metric}_bucket[30s])))", name)
            for q, name in ((0.5, "p50"), (0.99, "p99"), (0.999, "p99.9"))]


def mqtt_load_dashboard() -> dict:
    """mqtt-load's live view (rig2, ADR 0090): what the generators offered, sent, got
    acked and received, their latency quantiles, and whether they stayed healthy. Each
    rig2 step is a fresh mqtt-load process, so counters restart per step (rate() takes
    resets in stride) and the panels are empty between steps by design."""
    rate = "sum(rate({m}[10s]))"
    strip = strip_panels()
    strip[2] = {
        **strip[2], "title": "Generators reporting",
        "description": ("mqtt-load processes being scraped. 0 between rig2 steps is CORRECT: each step is "
                        "its own process, and the endpoint exists only while it runs."),
        "targets": [{"datasource": DS, "expr": 'count(up{job="mqtt-load"} == 1)', "legendFormat": "generators",
                     "refId": "A"}],
    }
    y = STRIP_H
    panels = strip + [
        _ts("Offered, sent, acked, received (msg/s, all generators)", [
            ("sum(mqload_target_rate)", "target"),
            (rate.format(m="mqload_offered_total"), "offered"),
            (rate.format(m="mqload_sent_total"), "sent"),
            (rate.format(m="mqload_acked_total"), "acked"),
            (rate.format(m="mqload_received_total"), "received"),
        ], 0, y, unit="short",
            desc="Offered is what the schedule made due; sent what was written. Sent below offered is the "
                 "generator falling behind (see backlog). Received counts deliveries, so it is the offer times "
                 "the deliveries per message."),
        _ts("Brokers took the offer? (msg/s)", [
            ("sum(rate(mqttd_publish_received_total[10s]))", "brokers received"),
            ("sum(rate(mqttd_publish_delivered_total[10s]))", "brokers delivered"),
            (rate.format(m="mqload_sent_total"), "generators sent"),
        ], 12, y,
            desc="The broker's own counters beside the generators'. The rig2 verdict needs the brokers to "
                 "receive at least 99% of the offer over the measured window."),
        _ts("Ack latency (PUBLISH to PUBACK)", _quantiles("mqload_ack_latency_seconds"), 0, y + 8, unit="s",
            log=True,
            desc="Live and coarse: quantiles from mqtt-load's 16 Prometheus buckets over 30 s. The report's "
                 "figures are the exact merge of every generator's HDR histogram over the measured window."),
        _ts("End-to-end latency (PUBLISH to delivery)", _quantiles("mqload_e2e_latency_seconds"), 12, y + 8,
            unit="s", log=True, desc="The QoS 0 workload is gated on this. Same caveat as the ack panel."),
        _ts("Generator CPU (cores) and saturation", [
            ("mqload_cpu_cores", "{{instance}} cpu cores"),
            ("mqload_saturated", "{{instance}} saturated"),
        ], 0, y + 16, w=8,
            desc="saturated = 1 when the current window crossed a generator threshold: such a step is VOID, "
                 "never counted against the broker."),
        _ts("Generator scheduling lag", _quantiles("mqload_sched_lag_seconds"), 8, y + 16, w=8, unit="s",
            log=True, desc="Due to written: sends the generator alone delayed."),
        _ts("In flight and backlog", [
            ("sum by (instance) (mqload_inflight)", "{{instance}} in flight"),
            ("sum by (instance) (mqload_backlog)", "{{instance}} backlog"),
        ], 16, y + 16, w=8,
            desc="In flight: QoS 1 publishes awaiting PUBACK. Backlog: messages due and not yet written."),
        _ts("Errors and ledger anomalies (per s)", [
            ("sum by (kind) (rate(mqload_errors_total[30s]))", "error {{kind}}"),
            ("sum(rate(mqload_duplicates_total[30s]))", "duplicates"),
            ("sum(rate(mqload_unexpected_total[30s]))", "unexpected"),
            ("sum(rate(mqload_negative_acks_total[30s]))", "negative acks"),
            ("sum(rate(mqload_unexpected_acks_total[30s]))", "unexpected acks"),
            ("sum(rate(mqload_pkid_gaps_total[30s]))", "packet-id gaps"),
        ], 0, y + 24,
            desc="All zero in a clean step. Missing messages are only known at the drain: see the step's verdict."),
        _ts("Broker drops by reason (per s)", [
            ("sum by (reason) (rate(mqttd_publish_dropped_total[30s]))", "{{reason}}"),
        ], 12, y + 24,
            desc="verdict.py's DROP_IS_LOSS decides which reasons fail a step; pending-cap-replay and the "
                 "settle-replay family are not loss."),
    ]
    for i, p in enumerate(panels, start=1):
        p["id"] = i
    return {
        "uid": "mqtt-load-bench", "title": "mqtt-load — generators (bench)", "tags": ["bench", "mqtt-load"],
        "description": ("Generated by bench/scale/observe/build-bench-dashboards.py — do not edit by hand. "
                        "mqtt-load's --metrics endpoint (mqload_*) beside the brokers' counters, for rig2."),
        "schemaVersion": 39, "time": {"from": "now-15m", "to": "now"}, "refresh": "5s",
        "editable": True, "panels": panels,
    }


def main() -> None:
    DST.mkdir(parents=True, exist_ok=True)
    for src in sorted(SRC.glob("*.json")):
        derive(src, DST / f"{src.stem}-bench.json")
    d = mqtt_load_dashboard()
    (DST / "mqtt-load-bench.json").write_text(json.dumps(d, indent=2) + "\n")
    print(f"  (generated) -> mqtt-load-bench.json  (uid {d['uid']}, {len(d['panels'])} panels)")


if __name__ == "__main__":
    main()
