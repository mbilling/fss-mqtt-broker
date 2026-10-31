#!/usr/bin/env python3
"""The hub model: what one message costs the hub loop, read from a rig run's scrapes.

    hub-model.py <run dir | ladder dir | step dir>          # a markdown table per ladder
    hub-model.py <dir> --json                               # the same figures as JSON

ADR 0092 T1. The hub is one task per node, and its time per message sets the node's
ceiling: a node is at its knee when the loop's busy share stops growing while its queue
does. For every broker, over each round's measured window (the generators' overlap, the
one verdict.py judges the round on), the 2-second scrapes give:

  busy       the share of wall-clock the loop spent dispatching commands
             (mqttd_hub_dispatch_seconds, all classes)
  us/msg     hub-seconds per message the node received, all classes in; the node's
             ceiling is about busy_at_saturation / (us/msg)
  per class  mean µs per dispatch and dispatches per received message, for `publish`,
             `ack` (consumer PUBACKs), `control`, `attach`, `subscribe`, `cluster`, `sweep`
  wake µs    the runtime wake probe's mean lateness (mqttd_runtime_wake_seconds{probe="yield"}):
             the loop yields every 64 commands, and each yield costs one such wait
  queue      the hub's queue depth at the window's end, and its maximum in the window
  saturated  the queue ended above HUB_SATURATED; then 1 - busy is the share of the loop's
             time that went to something other than dispatching: ingest, yields, wake-ups
             and migrations. On the AWS runs of 2026-10-30 that share was 17% on Ice Lake
             (busy 0.83) and 30% on EPYC Genoa (busy 0.70), read on the saturated node.

Figures are per broker from that broker's own clock (differences of its scrapes), then
summed or averaged over the cluster weighted by the messages each broker received.
A round's window comes from its verdict.json; without one, from the generators' summaries
the way verdict.py computes it. The scrapes are parsed by extract-lane-e.py's parser, so
a truncated or duplicated scrape is refused here the same way.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCALE = HERE.parent


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


E = _load("extract_lane_e", SCALE / "extract-lane-e.py")

DISPATCH_SUM = "mqttd_hub_dispatch_seconds_sum"
DISPATCH_COUNT = "mqttd_hub_dispatch_seconds_count"
WAKE_SUM = "mqttd_runtime_wake_seconds_sum"
WAKE_COUNT = "mqttd_runtime_wake_seconds_count"
QUEUE = "mqttd_hub_queue_depth"
# The hub's queue at a window's end above this is a loop that did not keep up: its busy
# share is then its saturated share, whatever the load. At idle the queue holds one or
# two commands; a hub near its ceiling carries a standing queue in the low thousands
# (7,000 at 379,700 msg/s on six 16-vCPU nodes, carried); one that fell behind ends the
# window above 100,000 (213,000 at 400,800 there, 240,000 on one m6i.8xlarge at 101,300).
HUB_SATURATED = 10_000
ROUND_DIR = re.compile(r"^round-(\d+)(?:-a(\d+))?$")
CLASSES = ("publish", "ack", "control", "attach", "subscribe", "cluster", "sweep")


# ── scrapes ─────────────────────────────────────────────────────────────────────────


def load_samples(d: Path) -> list[tuple[int, dict]]:
    """(unix_ms, parsed) for every complete scrape in d, sorted; a refused scrape is skipped."""
    out = []
    for p in sorted(d.glob("*.prom")):
        try:
            out.append((int(p.stem), E.parse_prom(p)))
        except ValueError:
            continue
    out.sort(key=lambda s: s[0])
    return out


def measured_span(s: dict) -> tuple[float, float]:
    """A generator summary's measured span, wall ms (as verdict.py computes it)."""
    ms = [w for w in s["windows"] if w["kind"] == "measure"]
    base = s["schedule_start_unix_ms"]
    return base + min(w["start_s"] for w in ms) * 1000, base + max(w["end_s"] for w in ms) * 1000


def round_window(rd: Path) -> tuple[float, float] | None:
    """The round's measured window (wall ms): its verdict's, else the overlap of its
    generators' measured spans, else None."""
    v = rd / "verdict.json"
    if v.is_file():
        try:
            w = json.loads(v.read_text()).get("window") or {}
            if w.get("start_unix_ms") is not None and w.get("end_unix_ms") is not None:
                return float(w["start_unix_ms"]), float(w["end_unix_ms"])
        except ValueError:
            pass
    spans = []
    for p in sorted(rd.glob("gen-*.json")):
        try:
            spans.append(measured_span(json.loads(p.read_text())))
        except (ValueError, KeyError):
            continue
    if not spans:
        return None
    return max(a for a, _ in spans), min(b for _, b in spans)


def _probe(parsed: dict, name: str) -> float:
    return E.by_label(parsed, name, "probe").get("yield", 0.0)


# ── the model ───────────────────────────────────────────────────────────────────────


def broker_model(samples: list[tuple[int, dict]], lo_ms: float, hi_ms: float, margin_ms: float) -> dict | None:
    """One broker's figures over [lo + margin, hi - margin], from its first and last
    scrape inside; None with fewer than two scrapes inside."""
    inside = [s for s in samples if lo_ms + margin_ms <= s[0] <= hi_ms - margin_ms]
    if len(inside) < 2:
        return None
    (t0, a), (t1, z) = inside[0], inside[-1]
    dt = (t1 - t0) / 1000
    rx = E.delta(a, z, E.RECEIVED)
    if rx < 0:
        raise ValueError("the received counter went backwards inside the window (a restart?)")
    sum_a, sum_z = E.by_label(a, DISPATCH_SUM, "command"), E.by_label(z, DISPATCH_SUM, "command")
    cnt_a, cnt_z = E.by_label(a, DISPATCH_COUNT, "command"), E.by_label(z, DISPATCH_COUNT, "command")
    classes: dict[str, dict] = {}
    for cls in sorted(set(sum_z) | set(cnt_z)):
        ds = sum_z.get(cls, 0.0) - sum_a.get(cls, 0.0)
        dn = cnt_z.get(cls, 0.0) - cnt_a.get(cls, 0.0)
        if ds < 0 or dn < 0:
            raise ValueError(f"the {cls} dispatch counters went backwards inside the window (a restart?)")
        classes[cls] = {"seconds": ds, "dispatches": dn,
                        "us": ds / dn * 1e6 if dn > 0 else None,
                        "per_msg": dn / rx if rx > 0 else None,
                        "share": ds / dt}
    seconds = sum(c["seconds"] for c in classes.values())
    busy = seconds / dt
    ws, wc = _probe(z, WAKE_SUM) - _probe(a, WAKE_SUM), _probe(z, WAKE_COUNT) - _probe(a, WAKE_COUNT)
    queues = [p.get((QUEUE, ""), 0.0) for _, p in inside]
    return {"window_s": dt, "received": rx, "received_rate": rx / dt, "busy": busy,
            "us_per_msg": seconds / rx * 1e6 if rx > 0 else None, "classes": classes,
            "wake_us": ws / wc * 1e6 if wc > 0 else None,
            "queue_end": queues[-1], "queue_max": max(queues), "saturated": queues[-1] > HUB_SATURATED,
            "scrapes": len(inside)}


def round_model(rd: Path, margin_ms: float = 1000.0) -> dict:
    """Every broker of a round over the round's measured window."""
    out = {"dir": rd.name, "window": None, "brokers": {}, "problems": []}
    win = round_window(rd)
    if win is None:
        out["problems"].append("no measured window (no verdict.json and no generator summary)")
        return out
    lo, hi = win
    out["window"] = {"start_unix_ms": lo, "end_unix_ms": hi, "seconds": (hi - lo) / 1000}
    root = rd / "scrapes"
    dirs = sorted((p for p in root.glob("broker-*") if p.is_dir()), key=lambda p: int(p.name.split("-")[1]))
    if not dirs:
        out["problems"].append("no broker scrapes")
    for d in dirs:
        idx = int(d.name.split("-")[1])
        try:
            m = broker_model(load_samples(d), lo, hi, margin_ms)
        except ValueError as e:
            out["problems"].append(f"broker {idx}: {e}")
            continue
        if m is None:
            out["problems"].append(f"broker {idx}: fewer than two scrapes inside the window")
            continue
        out["brokers"][idx] = m
    return out


def _wmean(pairs: list[tuple[float | None, float]]) -> float | None:
    """Mean of values weighted, None when no value has weight."""
    num = sum(v * w for v, w in pairs if v is not None and w > 0)
    den = sum(w for v, w in pairs if v is not None and w > 0)
    return num / den if den > 0 else None


def merge_brokers(rounds: list[dict]) -> dict[int, dict]:
    """Per broker across a step's rounds: busy and the per-class figures weighted by the
    messages received in each round; the queue's maximum; saturated in any round."""
    per: dict[int, list[dict]] = {}
    for r in rounds:
        for idx, m in r["brokers"].items():
            per.setdefault(idx, []).append(m)
    out = {}
    for idx, ms in per.items():
        w = [(m, m["received"]) for m in ms]
        classes = {}
        for cls in sorted({c for m in ms for c in m["classes"]}):
            cs = [(m["classes"].get(cls), m["received"]) for m in ms if cls in m["classes"]]
            classes[cls] = {"dispatches": sum(c["dispatches"] for c, _ in cs),
                            "us": _wmean([(c["us"], c["dispatches"]) for c, _ in cs]),
                            "per_msg": _wmean([(c["per_msg"], rx) for c, rx in cs]),
                            "share": _wmean([(c["share"], rx) for c, rx in cs])}
        out[idx] = {"rounds": len(ms), "received_rate": _wmean([(m["received_rate"], rx) for m, rx in w]),
                    "busy": _wmean([(m["busy"], rx) for m, rx in w]),
                    "us_per_msg": _wmean([(m["us_per_msg"], rx) for m, rx in w]),
                    "wake_us": _wmean([(m["wake_us"], rx) for m, rx in w]), "classes": classes,
                    "queue_max": max(m["queue_max"] for m in ms),
                    "saturated": any(m["saturated"] for m in ms)}
    return out


def cluster_of(brokers: dict[int, dict]) -> dict | None:
    """The cluster's figures: received-weighted means, the busiest node's busy, the
    saturated nodes."""
    if not brokers:
        return None
    w = [(m, m["received_rate"] or 0.0) for m in brokers.values()]
    classes = {}
    for cls in sorted({c for m in brokers.values() for c in m["classes"]}):
        cs = [(m["classes"][cls], rx) for m, rx in w if cls in m["classes"]]
        # A class's mean µs is weighted by its own dispatches, as merge_brokers weights it;
        # dispatches per message by the node's received messages.
        classes[cls] = {"us": _wmean([(c["us"], c.get("dispatches", rx)) for c, rx in cs]),
                        "per_msg": _wmean([(c["per_msg"], rx) for c, rx in cs])}
    busiest = max(brokers.items(), key=lambda kv: kv[1]["busy"] or 0.0)
    return {"nodes": len(brokers), "received_rate": sum(rx for _, rx in w),
            "us_per_msg": _wmean([(m["us_per_msg"], rx) for m, rx in w]),
            "busy_mean": _wmean([(m["busy"], rx) for m, rx in w]),
            "busy_max": busiest[1]["busy"], "busiest": busiest[0],
            "wake_us_max": max((m["wake_us"] or 0.0) for m in brokers.values()),
            "queue_max": max(m["queue_max"] for m in brokers.values()),
            "saturated": sorted(i for i, m in brokers.items() if m["saturated"]), "classes": classes}


def step_model(step: Path, margin_ms: float | None = None) -> dict:
    """A step's rounds (round-K[-aN] directories, else the step itself), merged."""
    meta = json.loads((step / "step.json").read_text()) if (step / "step.json").is_file() else {}
    if margin_ms is None:
        margin_ms = float((meta.get("judge") or {}).get("margin_s", 1.0)) * 1000
    rounds = sorted((p for p in step.iterdir() if p.is_dir() and ROUND_DIR.match(p.name)),
                    key=lambda p: (int(ROUND_DIR.match(p.name).group(1)), int(ROUND_DIR.match(p.name).group(2) or 0)))
    if not rounds:
        rounds = [step]
    rms = [round_model(r, margin_ms) for r in rounds]
    verdict = None
    if (step / "verdict.json").is_file():
        try:
            verdict = json.loads((step / "verdict.json").read_text()).get("verdict")
        except ValueError:
            verdict = None
    brokers = merge_brokers(rms)
    return {"step": meta.get("index"), "id": meta.get("id", step.name), "rate": meta.get("rate"), "verdict": verdict,
            "rounds": rms, "brokers": brokers, "cluster": cluster_of(brokers),
            "problems": [f"{r['dir']}: {p}" for r in rms for p in r["problems"]]}


def ladder_model(ladder: Path) -> dict:
    """Every step of a ladder (<ladder>/steps/<NN-rate>), in step order."""
    steps_dir = ladder / "steps"
    steps = sorted((p for p in steps_dir.iterdir() if p.is_dir() and (p / "step.json").is_file()),
                   key=lambda p: json.loads((p / "step.json").read_text()).get("index", 0)) if steps_dir.is_dir() else []
    out = {"ladder": ladder.name, "steps": [step_model(s) for s in steps]}
    carried = [s for s in out["steps"] if s["verdict"] == "CARRIED" and s["cluster"]]
    out["highest_carried"] = max(carried, key=lambda s: s["rate"] or 0)["id"] if carried else None
    sat = [s for s in out["steps"] if s["cluster"] and s["cluster"]["saturated"]]
    out["first_saturated"] = min(sat, key=lambda s: s["rate"] or 0)["id"] if sat else None
    # The loop's ceiling: the highest busy share reached by a node whose OWN queue did not
    # keep up, over every step. A node that kept up at a higher busy share in the same step
    # is not at its ceiling, so it does not count; the wake lateness is that node's too.
    # What is left of wall-clock at the ceiling is the loop's own cost: ingest, the yield
    # every 64 commands and the wake it waits for, migrations.
    best = None
    for s in out["steps"]:
        if not s["cluster"]:
            continue
        for i in s["cluster"]["saturated"]:
            m = s["brokers"][i]
            if m["busy"] is not None and (best is None or m["busy"] > best["busy"]):
                best = {"step": s["id"], "busy": m["busy"], "node": i, "wake_us": m["wake_us"]}
    out["ceiling"] = best
    return out


def run_model(run: Path) -> list[dict]:
    return [ladder_model(p.parent) for p in sorted(run.glob("**/ladder.json"))]


def find_models(path: Path) -> list[dict]:
    """A run, a ladder or a step directory → its ladders' models."""
    if (path / "ladder.json").is_file():
        return [ladder_model(path)]
    if (path / "step.json").is_file():
        return [{"ladder": path.parent.parent.name, "steps": [step_model(path)], "highest_carried": None,
                 "first_saturated": None}]
    return run_model(path)


# ── rendering ───────────────────────────────────────────────────────────────────────


def _us(x: float | None) -> str:
    return "—" if x is None else f"{x:.1f}"


def _n(x: float | None) -> str:
    return "—" if x is None else f"{x:,.0f}"


def _busy(c: dict) -> str:
    """The busiest node's busy share, or — for a step where no node received anything."""
    return "—" if c["busy_max"] is None else f"{c['busy_max']:.2f} (node {c['busiest']})"


def render_ladder_md(lm: dict) -> list[str]:
    lines = [f"### Hub model (ADR 0092): {lm['ladder']}", "",
             "Per step, the busiest node's share of wall-clock spent dispatching, the hub-seconds per received "
             "message (every class) as the cluster's received-weighted mean, the mean µs of a `publish` and of an "
             "`ack` (a consumer PUBACK) dispatch with how many of each a message costs, the wake probe's mean "
             "lateness, and the nodes whose queue ended the window above "
             f"{HUB_SATURATED:,}: there 1 − busy is the loop's time not spent dispatching.", "",
             "| step | offered msg/s | verdict | busy, busiest node | µs per message | publish µs × per msg | "
             "ack µs × per msg | wake µs | queue max | saturated nodes |",
             "|---|---|---|---|---|---|---|---|---|---|"]
    for s in lm["steps"]:
        c = s["cluster"]
        if not c:
            lines.append(f"| {s['step']} | {_n(s['rate'])} | {s['verdict'] or '—'} | — | — | — | — | — | — | "
                         f"{'; '.join(s['problems']) or 'no figures'} |")
            continue
        pub, ack = c["classes"].get("publish", {}), c["classes"].get("ack", {})
        sat = ", ".join(str(i) for i in c["saturated"]) or "none"
        lines.append(f"| {s['step']} | {_n(s['rate'])} | {s['verdict'] or '—'} | {_busy(c)} | "
                     f"{_us(c['us_per_msg'])} | {_us(pub.get('us'))} × {_us(pub.get('per_msg'))} | "
                     f"{_us(ack.get('us'))} × {_us(ack.get('per_msg'))} | {_n(c['wake_us_max'])} | "
                     f"{_n(c['queue_max'])} | {sat} |")
    lines.append("")
    if lm.get("highest_carried"):
        lines.append(f"- highest carried step: {lm['highest_carried']}")
    if lm.get("first_saturated"):
        lines.append(f"- first saturated step: {lm['first_saturated']}")
    if lm.get("ceiling"):
        c = lm["ceiling"]
        lines.append(f"- busy ceiling {c['busy']:.2f} on node {c['node']} at {c['step']}: {1 - c['busy']:.0%} of the "
                     f"loop's time not dispatching (ingest, the yield every 64 commands and its wake, "
                     f"{_n(c['wake_us'])} µs mean lateness there)")
    lines.append("")
    return lines


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("path", type=Path, help="a run, ladder or step directory")
    ap.add_argument("--json", action="store_true", help="print the figures as JSON")
    args = ap.parse_args(argv)
    models = find_models(args.path)
    if not models:
        print(f"hub-model.py: no ladder, step or run under {args.path}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(models, indent=2, default=str))
        return 0
    for lm in models:
        print("\n".join(render_ladder_md(lm)))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
