#!/usr/bin/env python3
"""Draw the plant-shape scale-out chart: durable QoS 1 msg/s against nodes on AWS, by node type.

    python3 bench/scale/chart-plant.py            # docs/benchmarks/img/rig2/plant-scale-out.svg
                                                  # and plant-latency.svg
    python3 bench/scale/chart-plant.py --self-test

The workload is rig2's `plant-qos1` (bench/scale/rig2/workloads/plant-qos1.env): every
site is one bridge connection carrying its own 250 topics at up to 10,000 msg/s a site, with
50-200 B payloads, and one durable `$share` historian group over every topic takes it all.
One column per measured cluster, at its maximum lossless step: 3 rounds, no message lost
in any, the mean of the rounds' ack p99 printed. Columns are coloured by AWS node type, so the one
cluster measured on two types at 12 nodes reads as what it is: on m6i.4xlarge the network
card exceeded its packet allowance (ENA pps_allowance_exceeded) from about 110k msg/s a
node and the ladder ended on a VOID round there; on c6in.4xlarge, the network-optimized
type with the same CPU, the same 12 nodes carried the rig's 2,000,000 msg/s ceiling at
1.15M packets/s a node with no exceedance. The gap is the network card, not the broker.

The points are written here, with the run each is read from (named in the SVG's <desc>),
rather than re-read from `.runs/` (untracked scratch), so the chart can be regenerated
from the tree and checked against those reports line by line.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import chart_style as cs  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs/benchmarks/img/rig2/plant-scale-out.svg"

VCPU_EACH = 16
# One row per column. `taken`: the most the brokers received at a step above the carried
# one, when that step was not carried (drawn as a dashed extension). `floor`: the ladder
# ended above the step without bracketing the knee, and `why` says what ended it. `fast`:
# the highest step carried at ack p99 <= 100 ms, with that p99.
POINTS = [
    {"nodes": 3, "host": "m6i.4xlarge", "msg_per_s": 495_800, "p99_ms": 486, "fast": (421_900, 55),
     "floor": False, "why": "", "taken": None, "run": "h3-20261106T145432Z", "date": "2026-11-06",
     "note": "506,300 not taken"},
    {"nodes": 5, "host": "m6i.4xlarge", "msg_per_s": 826_300, "p99_ms": 389, "fast": (661_000, 46),
     "floor": False, "why": "", "taken": None, "run": "h3-20261106T170406Z", "date": "2026-11-06",
     "note": "after a 3 → 5 join under load; 847,000 not taken"},
    {"nodes": 8, "host": "m6i.4xlarge", "msg_per_s": 1_322_000, "p99_ms": 494, "fast": (1_057_600, 48),
     "floor": False, "why": "", "taken": None, "run": "h3-20261106T182931Z", "date": "2026-11-06",
     "note": "after a 5 → 8 join under load; 1,355,100 not taken"},
    {"nodes": 12, "host": "m6i.4xlarge", "msg_per_s": 1_322_000, "p99_ms": 39, "fast": (1_322_000, 39),
     "floor": True, "why": "network card throttled", "taken": (1_568_049, 1_586_400),
     "run": "h3-20261106T194402Z", "date": "2026-11-06",
     "note": "after an 8 → 12 join under load; 1,454,200 VOID twice (one generator's tail) while the brokers' "
             "packet allowance was exceeded; 1,586,400 offered, 1,568,049 taken (98.8%)"},
    {"nodes": 12, "host": "c6in.4xlarge", "msg_per_s": 2_000_000, "p99_ms": 657, "fast": (1_322_000, 12),
     "floor": True, "why": "the rig's rate ceiling", "taken": None, "run": "h3-20261106T233606Z",
     "date": "2026-11-07", "note": "the rig's MAX_RATE ceiling; 1,983,000 at 572 ms"},
]
BASE = POINTS[0]   # the per-node rate the dotted line extends
# Every step of the five ladders, for the latency chart, by run: (offered msg/s, verdict,
# ack p50 ms, ack p99 ms, p99 min, p99 max), the latencies the mean of the step's rounds and
# the rounds' extremes; read from each run's report.json (the card's table has the same).
CARRIED, NOT_TAKEN, VOID = "CARRIED", "OFFER NOT TAKEN", "VOID"
STEPS = {
    "h3-20261106T145432Z": [
        (150_000, CARRIED, 0.5, 0.8, 0.8, 0.8), (225_000, CARRIED, 0.5, 0.8, 0.8, 0.9),
        (337_500, CARRIED, 0.8, 15.9, 14.7, 17.4), (421_900, CARRIED, 1.5, 55.3, 50.1, 60.1),
        (464_100, CARRIED, 7.1, 179.7, 121.2, 285.4), (485_200, CARRIED, 12.3, 326.2, 206.1, 519.4),
        (495_800, CARRIED, 24.2, 486.3, 459.3, 507.4), (506_300, NOT_TAKEN, 436.2, 509.4, 509.4, 509.4)],
    "h3-20261106T170406Z": [
        (661_000, CARRIED, 0.9, 46.2, 44.0, 49.5), (826_300, CARRIED, 7.3, 389.2, 191.4, 496.1),
        (847_000, NOT_TAKEN, 121.4, 494.1, 483.8, 504.3), (867_600, NOT_TAKEN, 372.0, 484.6, 484.6, 484.6),
        (908_900, NOT_TAKEN, 561.2, 665.6, 665.6, 665.6), (991_500, NOT_TAKEN, 564.2, 643.1, 643.1, 643.1)],
    "h3-20261106T182931Z": [
        (1_057_600, CARRIED, 1.0, 48.4, 46.9, 50.4), (1_322_000, CARRIED, 125.9, 493.5, 479.2, 501.2),
        (1_355_100, NOT_TAKEN, 404.5, 500.2, 500.2, 500.2), (1_388_100, NOT_TAKEN, 430.8, 514.6, 514.6, 514.6),
        (1_454_200, NOT_TAKEN, 574.5, 677.4, 677.4, 677.4), (1_586_400, NOT_TAKEN, 582.1, 679.9, 679.9, 679.9)],
    "h3-20261106T194402Z": [
        (1_057_600, CARRIED, 0.8, 3.3, 2.6, 4.6), (1_322_000, CARRIED, 8.8, 39.3, 18.0, 62.1),
        (1_454_200, VOID, 22.2, 462.8, 462.8, 462.8), (1_586_400, NOT_TAKEN, 24.3, 647.2, 647.2, 647.2)],
    "h3-20261106T233606Z": [
        (1_322_000, CARRIED, 0.8, 11.7, 11.2, 12.5), (1_983_000, CARRIED, 21.1, 571.6, 532.0, 626.7),
        (2_000_000, CARRIED, 134.5, 656.9, 654.8, 660.0)],
}
# Where each ladder's label stands on the latency chart: beside the step at `at` msg/s,
# `dx`/`dy` from that point, anchored as said. Placed by hand, since the data is fixed.
LABELS = {
    "h3-20261106T145432Z": ("3 nodes", 150_000, -10, 4, "end"),
    "h3-20261106T170406Z": ("5 and 8 nodes", 661_000, -10, -9, "end"),
    "h3-20261106T182931Z": None,
    "h3-20261106T194402Z": ("12 × m6i.4xlarge: network card throttled", 1_454_200, -11, 4, "end"),
    "h3-20261106T233606Z": ("12 × c6in.4xlarge", 1_322_000, 10, 17, "start"),
}
# One colour per AWS node type, in the order the types were measured: the categorical
# slots carry identity here (one broker, two hosts), as they carry brokers elsewhere.
SLOT = {"m6i.4xlarge": "s1", "c6in.4xlarge": "s2"}
HOST_LABEL = {"m6i.4xlarge": "m6i.4xlarge", "c6in.4xlarge": "c6in.4xlarge: the same CPU, network-optimized"}
TITLE = "Add a node, add 165k msg/s: durable QoS 1 scales linearly to 2 million msg/s on 12 nodes"
TITLE_LATENCY = "Latency follows the load per node, not the cluster size: ack p99 from 3 to 12 nodes"
OUT_LATENCY = ROOT / "docs/benchmarks/img/rig2/plant-latency.svg"
HEADLINE = ROOT / "docs/benchmarks/data/headline.csv"

W, H = 1080, 560
X0, X1 = 96, W - 48          # plot box
YT, YB = 150, 480            # plot top and base
BAR = 44
X_MAX = 13.4
Y_MAX = 2_500_000
Y_STEP = 500_000
# The latency chart: msg/s per node across, ack p99 up on a log scale.
LX_MAX = 205_000
LY_LO_MS, LY_HI_MS = 0.5, 1000.0


def per_node(v: float) -> str:
    return f"{v / 1000:.0f}k"


def total(v: float) -> str:
    """496k, 1.32M, 2.0M: a million always carries a decimal, so 2.0M reads as measured."""
    if v >= 1_000_000:
        s = f"{v / 1e6:.2f}"
        return (s[:-1] if s.endswith("0") else s) + "M"
    return cs.compact(v)


def X(nodes: float) -> float:
    return X0 + nodes / X_MAX * (X1 - X0)


def Y(v: float) -> float:
    return YB - v / Y_MAX * (YB - YT)


def column_x(p: dict) -> float:
    """The left edge: a lone column is centred on its node count; a pair straddles it."""
    pair = [q for q in POINTS if q["nodes"] == p["nodes"]]
    if len(pair) == 1:
        return X(p["nodes"]) - BAR / 2
    i = pair.index(p)
    return X(p["nodes"]) + (i - len(pair) / 2) * (BAR + 2) + 1


def figure(p: dict) -> str:
    return ("≥ " if p["floor"] else "") + total(p["msg_per_s"])


def draw() -> str:
    per = BASE["msg_per_s"] / BASE["nodes"]
    hosts = sorted({p["host"] for p in POINTS}, key=lambda h: SLOT[h])
    o = cs.open_svg(
        W, H, TITLE,
        "Plant shape: a site is one bridge connection, 250 topics, 10,000 msg/s → one durable $share historian "
        f"group · mqttd v1.3.0 · AWS, {VCPU_EACH}-vCPU nodes · Nov 2026",
        "Columns of the cluster's maximum lossless msg/s at 3, 5, 8 and 12 nodes on AWS, coloured by node type, "
        "the type named under each column. " + "; ".join(
            f"{p['nodes']} × {p['host']}: {figure(p)} msg/s, {per_node(p['msg_per_s'] / p['nodes'])} a node, "
            f"ack p99 {p['p99_ms']} ms" for p in POINTS)
        + f". A dotted line extends the 3-node rate, {per_node(per)} a node. At 12 nodes the m6i.4xlarge column "
        "stops at 1.32M with a dashed extension to the 1.57M the brokers received but did not carry, its network "
        "card throttled; the c6in.4xlarge column, the same CPU with a network-optimized card, reaches 2.0M, the "
        "rig's ceiling. Source: bench/scale/rig2 runs "
        + ", ".join(f"{p['run']} ({p['nodes']} × {p['host']})" for p in POINTS) + ", each run's report.md.")
    o.append(cs.text(32, 86, "each column: the cluster's maximum lossless step (3 rounds, no message lost) and "
                     "its ack p99 · ≥ where the ladder ended above the step", size=13, role="ink2"))
    # Legend: the node types by colour, then the two line marks.
    lx = 32
    keys = [(SLOT[h], HOST_LABEL[h], "box") for h in hosts]
    keys += [("ghost", f"{per_node(per)} a node, the 3-node rate, extended", "dot"),
             ("ghost", "received, not carried: network card throttled", "dash")]
    for role, label, kind in keys:
        o += cs.swatch(lx, 117, role, label, kind=kind)
        lx += cs.legend_width(label, kind) - 10
    # Axes and grid.
    for v in range(0, Y_MAX + 1, Y_STEP):
        o.append(cs.hline(X0, X1, Y(v), "axis" if v == 0 else "grid"))
        o.append(cs.text(X0 - 8, Y(v) + 4, total(v) if v else "0", size=11, role="muted", anchor="end", num=True))
    o.append(cs.text(X0 - 64, YT - 12, "msg/s, cluster total", size=11, role="muted"))
    sizes = sorted({p["nodes"] for p in POINTS})
    for n in sizes:
        o.append(cs.vline(X(n), YT, YB, "grid"))
        o.append(cs.text(X(n), YB + 18, n, size=13, role="ink", anchor="middle", weight=650, num=True))
        here = [p for p in POINTS if p["nodes"] == n]
        o.append(cs.text(X(n), YB + 33, " · ".join(p["host"] for p in here), size=10, role="muted",
                         anchor="middle", num=True))
    o.append(cs.text((X0 + X1) / 2, YB + 52, f"nodes, {VCPU_EACH} vCPU each · at 12 the same cluster on both "
                     "node types", size=11, role="muted", anchor="middle"))
    # The dotted line of the 3-node rate, drawn before the columns so they sit on it.
    x_end = 12.9
    o.append(f'<line x1="{X(BASE["nodes"]):.1f}" y1="{Y(per * BASE["nodes"]):.1f}" x2="{X(x_end):.1f}" '
             f'y2="{Y(per * x_end):.1f}" class="k-ghost" stroke-width="2" stroke-dasharray="0.1 5" '
             'stroke-linecap="round"/>')
    # Columns and their figures.
    for p in POINTS:
        bx = column_x(p)
        cx = bx + BAR / 2
        top = Y(p["msg_per_s"])
        o.append(cs.column(bx, top, YB, BAR, SLOT[p["host"]]))
        if p["taken"]:
            got, offered = p["taken"]
            o.append(f'<rect x="{bx + 0.75:.1f}" y="{Y(got) + 0.75:.1f}" width="{BAR - 1.5:.1f}" '
                     f'height="{max(top - Y(got) - 1.5, 1):.1f}" rx="4" fill="none" class="k-ghost" '
                     'stroke-width="1.5" stroke-dasharray="4 3"/>')
        lines = [(figure(p), 13, "ink", 650),
                 (f"{per_node(p['msg_per_s'] / p['nodes'])}/node · p99 {p['p99_ms']} ms", 11, "ink2", 400)]
        fast, fast_ms = p["fast"]
        if fast != p["msg_per_s"]:
            lines.append((f"≤ 100 ms: {total(fast)} ({fast_ms} ms)", 11, "muted", 400))
        if p["why"]:
            lines.append((p["why"], 11, "muted", 400))
        if p["taken"]:
            got, offered = p["taken"]
            lines.append((f"received {total(got)} of {total(offered)} offered", 11, "muted", 400))
        # A pair's left column labels to the left of the pair, clear of the taller column.
        pair = [q for q in POINTS if q["nodes"] == p["nodes"]]
        if len(pair) > 1 and pair.index(p) == 0:
            anchor, lx_, ly = "end", bx - 6, Y(p["taken"][0] if p["taken"] else p["msg_per_s"]) + 4
            for s_, size, role_, wt in lines:
                o.append(cs.text(lx_, ly, s_, size=size, role=role_, anchor=anchor, weight=wt, num=True, halo=True))
                ly += size + 3
        else:
            ly = top - 7
            for s_, size, role_, wt in reversed(lines):
                o.append(cs.text(cx, ly, s_, size=size, role=role_, anchor="middle", weight=wt, num=True, halo=True))
                ly -= size + 3
    o.append("</svg>")
    return "\n".join(o) + "\n"


def LX(per_node_rate: float) -> float:
    return X0 + per_node_rate / LX_MAX * (X1 - X0)


def LY(ms: float) -> float:
    lo, hi = math.log10(LY_LO_MS), math.log10(LY_HI_MS)
    return YB - (math.log10(ms) - lo) / (hi - lo) * (YB - YT)


def mark(x: float, y: float, role: str, verdict: str) -> str:
    """A step: carried filled, with a surface ring; not carried hollow; void a dashed ring."""
    if verdict == CARRIED:
        return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" class="f-{role} k-surface" stroke-width="2"/>'
    dash = ' stroke-dasharray="2.5 2"' if verdict == VOID else ""
    ring = "ghost" if verdict == VOID else role
    return f'<circle cx="{x:.1f}" cy="{y:.1f}" r="4.5" class="f-surface k-{ring}" stroke-width="2"{dash}/>'


def fmt_ms(ms: float) -> str:
    return "1 s" if ms >= 1000 else f"{ms:g} ms"


def draw_latency() -> str:
    hosts = sorted({p["host"] for p in POINTS}, key=lambda h: SLOT[h])
    o = cs.open_svg(
        W, H, TITLE_LATENCY,
        "every step of the five ladders above: ack p99, the mean of its rounds, by msg/s offered per node · "
        "ack p50 about 1 ms to 132k a node on an unthrottled card",
        "Ack p99 on a log scale against msg/s offered per node for every step of the five plant-shape ladders, "
        "coloured by node type, one line per ladder through its carried steps. The 3-, 5- and 8-node m6i.4xlarge "
        "ladders and the 12-node c6in.4xlarge ladder lie on one curve: about 1 ms to 75k a node, 12-16 ms at 110k, "
        "46-55 ms at 132-141k, 180 ms at 155k, 390-570 ms at 165k and 657 ms at 167k, the 12-node ladder's end. "
        "The 12-node m6i.4xlarge ladder leaves that curve early: 39 ms at 110k a node, a voided round at 463 ms "
        "at 121k, the offer not taken at 132k, its network card throttled. Steps not carried are hollow marks. "
        "Source: the runs " + ", ".join(f"{p['run']} ({p['nodes']} × {p['host']})" for p in POINTS) + ".")
    o.append(cs.text(32, 86, "filled: a carried step (3 rounds, no message lost) · hollow: the brokers did not "
                     "take the offer · dashed ring: a round the rig voided · bar: the rounds' p99 range",
                     size=13, role="ink2"))
    lx = 32
    for h in hosts:
        o += cs.swatch(lx, 117, SLOT[h], HOST_LABEL[h], kind="box")
        lx += cs.legend_width(HOST_LABEL[h], "box") - 10
    for v, label in ((CARRIED, "carried"), (NOT_TAKEN, "not carried"), (VOID, "void")):
        o.append(mark(lx + 9, 113, "ink2", v))
        o.append(cs.text(lx + 19, 117, label, size=12, role="ink2"))
        lx += cs.legend_width(label, "box") - 12
    # Grid: decades of latency up, msg/s per node across.
    ms = 1.0
    while ms <= LY_HI_MS:
        o.append(cs.hline(X0, X1, LY(ms), "grid"))
        o.append(cs.text(X0 - 8, LY(ms) + 4, fmt_ms(ms), size=11, role="muted", anchor="end", num=True))
        ms *= 10
    o.append(cs.hline(X0, X1, YB, "axis"))
    o.append(cs.text(X0 - 64, YT - 12, "ack p99, log", size=11, role="muted"))
    for v in range(0, LX_MAX, 50_000):
        o.append(cs.vline(LX(v), YT, YB, "grid"))
        o.append(cs.text(LX(v), YB + 18, per_node(v) if v else "0", size=12, role="ink", anchor="middle",
                         weight=650, num=True))
    o.append(cs.text((X0 + X1) / 2, YB + 40, "msg/s offered per node", size=11, role="muted", anchor="middle"))
    # Each ladder: a line through its carried steps, then every step's mark and spread.
    for p in POINTS:
        role, n = SLOT[p["host"]], p["nodes"]
        steps = sorted(STEPS[p["run"]])
        carried = [s for s in steps if s[1] == CARRIED]
        dash = ' stroke-dasharray="6 4"' if p["why"] == "network card throttled" else ""
        pts = " ".join(f"{LX(s[0] / n):.1f},{LY(s[3]):.1f}" for s in carried)
        o.append(f'<polyline points="{pts}" fill="none" class="k-{role}" stroke-width="2" '
                 f'stroke-linejoin="round"{dash}/>')
        for s in steps:
            x = LX(s[0] / n)
            if s[5] > s[4]:
                o.append(f'<line x1="{x:.1f}" y1="{LY(s[4]):.1f}" x2="{x:.1f}" y2="{LY(s[5]):.1f}" '
                         f'class="k-{role}" stroke-width="1.5" stroke-opacity="0.6"/>')
            o.append(mark(x, LY(s[3]), role, s[1]))
        if LABELS[p["run"]]:
            text, at, dx, dy, anchor = LABELS[p["run"]]
            s = next(s for s in steps if s[0] == at)
            o.append(cs.text(LX(at / n) + dx, LY(s[3]) + dy, text, size=12, role="ink", anchor=anchor,
                             weight=650, halo=True))
    o.append("</svg>")
    return "\n".join(o) + "\n"


def headline_rows() -> list[dict]:
    import csv
    with HEADLINE.open(encoding="utf-8") as f:
        return [r for r in csv.DictReader(line for line in f if not line.startswith("#")) if r["chart"] == "plant"]


def same_curve(a: dict, b: dict, within: float = 0.01) -> list[tuple[float, float]]:
    """Pairs of carried p99 (ms) from two ladders at per-node rates within `within` of each other."""
    out = []
    for s in STEPS[a["run"]]:
        for t in STEPS[b["run"]]:
            if s[1] == t[1] == CARRIED and abs(s[0] / a["nodes"] / (t[0] / b["nodes"]) - 1) < within:
                out.append((s[3], t[3]))
    return out


def self_test() -> None:
    svg = draw()
    desc = svg.split("</desc>")[0]
    body = svg.split("</desc>")[1]
    lat = draw_latency()
    lat_body = lat.split("</desc>")[1]
    per = BASE["msg_per_s"] / BASE["nodes"]
    linear = [p for p in POINTS if p["host"] == "c6in.4xlarge" or p["nodes"] < 12]
    throttled = next(p for p in POINTS if p["why"] == "network card throttled")
    rows = headline_rows()
    on_curve = [pair for a in linear for b in linear if a is not b for pair in same_curve(a, b)]
    off_curve = [pair for b in linear for pair in same_curve(throttled, b)]
    checks = [
        ("every column wears its node type's colour, and each colour has one legend key",
         all(body.count(f'class="f-{slot}"') == sum(1 for p in POINTS if p["host"] == host) + 1
             for host, slot in SLOT.items())),
        ("nothing else is coloured: no column is green, amber or red",
         all(f'class="f-{role}"' not in body for role in ("good", "warn", "crit"))),
        ("every column's figure, per-node rate and p99 are printed",
         all(f">{figure(p)}<" in body and f">{per_node(p['msg_per_s'] / p['nodes'])}/node · p99 {p['p99_ms']} ms<"
             in body for p in POINTS)),
        ("every node type stands under its column", all(p["host"] in body for p in POINTS)),
        ("the received-not-carried extension is drawn once, plus its legend key",
         body.count('stroke-dasharray="4 3"') == sum(1 for p in POINTS if p["taken"]) + 1),
        ("the per-node rate holds within 2% wherever the network was not throttled",
         all(abs(p["msg_per_s"] / p["nodes"] / per - 1) < 0.02 for p in linear)),
        ("a floor is marked with ≥ and says what ended the ladder",
         all(f">≥ {total(p['msg_per_s'])}<" in body and f">{p['why']}<" in body for p in POINTS if p["floor"])),
        ("the 12-node pair straddles its tick",
         abs((column_x(POINTS[3]) + column_x(POINTS[4]) + BAR) / 2 - X(12)) < 1),
        ("no footer: nothing is drawn below the axis caption", svg.count(f'y="{YB + 52:.1f}"') == 1
         and not any(f'y="{y:.1f}"' in body for y in range(YB + 60, H))),
        ("every run is named in the description", all(p["run"] in desc for p in POINTS)),
        ("the headline carries the figures it claims",
         f"{POINTS[-1]['msg_per_s'] / 1e6:.0f} million" in TITLE and f"{per_node(per)} msg/s" in TITLE
         and f"{POINTS[-1]['nodes']} nodes" in TITLE),
        # The README's fact check reads the columns from headline.csv (chart = plant); the two
        # must be one record, column for column.
        ("every column is a headline.csv plant row, and every plant row a column",
         len(rows) == len(POINTS) and all(
             any(int(r["nodes"]) == p["nodes"] and r["host"].endswith(p["host"]) and int(r["msg_per_s"]) == p["msg_per_s"]
                 and int(r["floor"]) == int(p["floor"]) for r in rows) for p in POINTS)),
        # The latency chart.
        ("the latency chart draws every step of every ladder, carried filled and the rest hollow",
         lat_body.count('r="4.5"') == sum(len(STEPS[p["run"]]) for p in POINTS) + 3
         and lat_body.count('class="f-surface k-ghost" stroke-width="2" stroke-dasharray')
         == sum(1 for p in POINTS for s in STEPS[p["run"]] if s[1] == VOID) + 1),
        ("the maximum lossless column is its ladder's highest carried step",
         all(max(s[0] for s in STEPS[p["run"]] if s[1] == CARRIED) == p["msg_per_s"] for p in POINTS)),
        ("the per-node rate and ack p99 of the column is its ladder's",
         all(any(s[0] == p["msg_per_s"] and round(s[3]) == p["p99_ms"] for s in STEPS[p["run"]]) for p in POINTS)),
        ("the ≤ 100 ms step of the column is its ladder's highest carried step within 100 ms",
         all(max((s for s in STEPS[p["run"]] if s[1] == CARRIED and s[3] <= 100), key=lambda s: s[0])[0]
             == p["fast"][0] for p in POINTS)),
        # The knee is steep and its rounds spread (191-496 ms within one 5-node step), so the
        # curve is one to within 2x there; the throttled ladder is 3x off it at 110k a node.
        ("one curve: where two unthrottled ladders share a per-node rate (±1%), their p99 agree within 2×",
         len(on_curve) >= 6 and all(max(a, b) / min(a, b) < 2 for a, b in on_curve)),
        ("the throttled ladder leaves that curve: at a shared per-node rate its p99 is 3× or more",
         len(off_curve) >= 1 and all(a / b >= 3 for a, b in off_curve)),
        ("every ladder with a label is labelled, and the dashed line is the throttled ladder's",
         all(lab[0] in lat_body for lab in LABELS.values() if lab) and lat_body.count('stroke-dasharray="6 4"') == 1),
        ("both committed charts are what this script draws",
         OUT.read_text() == svg and OUT_LATENCY.read_text() == lat),
    ]
    bad = [name for name, ok in checks if not ok]
    if bad:
        raise SystemExit("chart-plant self-test FAILED:\n  " + "\n  ".join(bad))
    print(f"chart-plant self-test: ok ({len(checks)} checks)")


def main(argv: list[str]) -> None:
    if argv == ["--self-test"]:
        self_test()
        return
    if argv:
        raise SystemExit(__doc__)
    for out, draw_it in ((OUT, draw), (OUT_LATENCY, draw_latency)):
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(draw_it())
        print(f"wrote {out}")


if __name__ == "__main__":
    main(sys.argv[1:])
