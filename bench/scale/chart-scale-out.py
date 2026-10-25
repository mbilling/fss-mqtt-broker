#!/usr/bin/env python3
"""Draw the scale-out charts in docs/benchmarks/EVIDENCE.md: QoS 0 and QoS 1 msg/s vs nodes, and the
per-rung chart of a campaign whose rungs are not all certified.

    python3 bench/scale/chart-scale-out.py            # all, into docs/benchmarks/img/
    python3 bench/scale/chart-scale-out.py qos1 [out]  # one
    python3 bench/scale/chart-scale-out.py --self-test

The points are the ones recorded in the run cards and curve documents named in
each curve's `source`; they are written here rather than re-read from `.runs/`
(untracked scratch), so the charts can be regenerated from the tree and checked
against those documents line by line.

One column per cluster size, in the latency bands of ADR 0048 (2026-09-27):
  GREEN segment   the certified knee — p99 <= 1 s, zero loss
  YELLOW cap      the rung above it, carried with zero loss at p99 <= 5 s
  dashed cap      the rung above it was NOT CARRIED (e.g. publishers late —
                  backpressure; nothing lost)
Under each size, what kind of number the green figure is:
  certified   — every repetition the rule asks for
  partial     — not every repetition certified (the others were INVALID on
                driver-side evidence while the brokers received it all)
  floor       — the highest rung measured; the knee lies above it
  uncertified — the arm's own gate failed

The per-rung chart (RUNG_CHARTS) draws one column per rung, msg/s per node, at the
rate the BROKERS' own counters delivered over the window (summarize-curve.py's
`delivered/s (brokers)`), in three states that are never drawn alike:
  filled GREEN    certified: the rig graded the rung GREEN — p99 <= 1 s, zero loss
  GREEN outline   holds by the reading rule (no message lost, p99 <= 1 s, broker
                  counters) but NOT certified: the rig's verdict was NOT CARRIED
                  (the reasons are in the card named in `source`)
  dashed grey     does not hold: a message was lost or p99 is above 1 s
  tick above      the offered rate, where the brokers delivered more than 1% less
A rung the rig did not certify is never filled.

A rung chart with `"unit": "total"` draws the same marks at the CLUSTER's msg/s
instead of the per-node rate, and prints the per-node rate under each column: the
form for "does the total grow when nodes are added".
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import chart_style as cs  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
IMG = ROOT / "docs/benchmarks/img"

NOTE = {
    "certified": "certified",
    "partial": "{reps} certified",
    "floor": "floor — knee above",
    "uncertified": "mesh gate failed",
}

CURVES = {
    "qos0": {
        "file": "scale-out-qos0.svg",
        "title": "QoS 0 scale-out: 1.14M msg/s certified on 10 nodes",
        "subtitle": "$share 1:1, 200 B · 4-vCPU nodes, one provisioning, ladders matched per node · "
                    "0% crossing",
        "source": "bench/scale/knee-3-5-7-10.md · run 2026-09-26 · mqttd main 0a08187 · emqtt-bench 0.6.3 · Hetzner fsn1",
        "y_max": 1_400_000, "y_step": 200_000,
        "per_node": 114_000,
        # (nodes, green msg/s, kind, reps, (next rung, its verdict) or None) — knee-3-5-7-10.md
        "points": [
            (3, 360_000, "floor", "", None),
            (5, 570_000, "certified", "", (600_000, "yellow")),
            (7, 810_000, "certified", "", (840_000, "yellow")),
            (10, 1_140_000, "uncertified", "", (1_200_000, "yellow")),
        ],
    },
    "qos1": {
        "file": "scale-out-qos1.svg",
        "title": "QoS 1 scale-out: 390k msg/s on 10 nodes, every rung GREEN",
        "subtitle": "$share 1:1, QoS 1 both ways, 200 B · 4-vCPU nodes · the knee is backpressure "
                    "(slow acks), never latency",
        "source": "docs/benchmarks/QOS1-SCALE-CURVE.md · 3, 5: v1.0.17 · 7, 10: main 0a08187, 10-node knee v1.0.18 · Hetzner fsn1",
        "y_max": 500_000, "y_step": 100_000,
        "per_node": 39_000,
        # QOS1-SCALE-CURVE.md, "The curve" and "7 and 10 nodes — one provisioning"
        "points": [
            (3, 120_000, "certified", "", None),
            (5, 180_000, "certified", "", None),
            (7, 270_000, "partial", "2 of 3", (300_000, "not carried")),
            # 2026-09-27 knee run: 420k (42k/node) x2 with no failure signal, 450k publishers late
            (10, 390_000, "partial", "1 of 3", (450_000, "not carried")),
        ],
    },
}

# One column per rung, in msg/s PER NODE, for a campaign whose rungs are not all
# certified. A rung is (sites, offered msg/s, delivered msg/s, state, control):
# delivered is summarize-curve.py's `delivered/s (brokers)`, the brokers' own
# counters over the window. state is
#   "green"  the summarizer's verdict is GREEN (the rig certified the rung)
#   "holds"  its broker-counter reading says HOLDS and its verdict is not GREEN
#   "fails"  its reading says DOES NOT HOLD
# 1-site rungs are left out.
# Optional per chart: "unit" ("per-node", the default, or "total": columns at the
# cluster's msg/s) and "slot" / "col_w" / "group_gap" for a chart with few columns.
RUNG_STATES = ("green", "holds", "fails")
RUNG_CHARTS = {
    "qos1-candidate": {
        "file": "scale-out-qos1-candidate.svg",
        "title": "QoS 1 per node on the 1.2.0 candidate: every rung held, nothing lost",
        "subtitle": "msg/s per node from the brokers' own counters, one column per rung · UNRELEASED "
                    "candidate 7d4eced · 4-vCPU nodes",
        "source": "bench/scale/qos1-knees-2026-10-12.md · bench/scale/durable-knees-2026-10-12.md · "
                  "Hetzner fsn1, 2026-10-12",
        "y_max": 60_000, "y_step": 10_000,
        "panels": [
            {"title": "QoS 1, clean sessions — the routing path",
             "groups": [
                 (3, "0.00% forwarded", [
                     (4, 120_000, 120_004, "green", False),
                     (4, 120_000, 120_001, "green", False),
                     (5, 150_000, 149_982, "green", False),
                 ]),
                 (10, "0.00% forwarded", [
                     (13, 390_000, 389_816, "holds", False),
                     (14, 420_000, 420_004, "holds", False),
                     (13, 390_000, 390_290, "holds", True),
                 ]),
             ]},
            {"title": "Durable QoS 1 — default store, 2 copies, fsync + quorum ack",
             "groups": [
                 (3, "0.00% forwarded", [
                     (12, 72_000, 62_243, "holds", False),
                     (15, 90_000, 62_611, "holds", False),
                 ]),
                 (10, "13–17% of publishes forwarded", [
                     (25, 150_000, 150_023, "holds", False),
                     (35, 210_000, 192_217, "holds", False),
                     (40, 240_000, 210_547, "holds", False),
                     (25, 150_000, 149_979, "green", True),
                 ]),
             ]},
        ],
    },
    # bench/scale/durable-scaleout-10-12-14.md, "Result": one panel per per-node offer,
    # one column per cluster size, at the cluster's total. No rung here is certified.
    "durable-scaleout-candidate": {
        "file": "scale-out-durable-candidate.svg",
        "title": "Durable QoS 1 on the 1.2.0 candidate: 10 → 12 → 14 nodes, the total grows, nothing lost",
        "subtitle": "total msg/s from the brokers' own counters · default store (redb), 2 copies, fsync + "
                    "quorum ack · UNRELEASED candidate 7d4eced",
        "source": "bench/scale/durable-scaleout-10-12-14.md · 4-vCPU nodes, one provisioning, Hetzner fsn1, "
                  "2026-10-12 · one 60 s window per rung",
        "unit": "total",
        "y_max": 300_000, "y_step": 50_000,
        "slot": 100, "col_w": 44, "group_gap": 18,
        "panels": [
            {"title": "Offered 15k msg/s per node",
             "groups": [
                 (10, "4.78% forwarded", [(25, 150_000, 150_419, "holds", False)]),
                 (12, "13.41% forwarded", [(30, 180_000, 179_039, "holds", False)]),
                 (14, "15.79% forwarded", [(35, 210_000, 208_739, "holds", False)]),
             ]},
            {"title": "Offered 18k msg/s per node",
             "groups": [
                 (10, "8.67% forwarded", [(30, 180_000, 173_100, "holds", False)]),
                 (12, "12.15% forwarded", [(36, 216_000, 205_750, "holds", False)]),
                 (14, "13.36% forwarded", [(42, 252_000, 240_497, "holds", False)]),
             ]},
        ],
    },
}
# The offered rate is drawn where the brokers delivered more than this share below it.
OFFER_TICK_BELOW = 0.99

W, H = 960, 572
L, R, T, B = 96, 48, 140, 110
PW, PH = W - L - R, H - T - B
# The x-axis reaches 11.2 nodes, or 1.2 past the largest size drawn: a curve that
# gains a 12- or 14-node point must not put it off the plot.
X_MAX = 11.2
COL_W = 24


def x_max_for(points) -> float:
    return max(X_MAX, max(n for n, *_ in points) + 1.2)


def draw(c: dict) -> str:
    y_max = c["y_max"]
    x_max = x_max_for(c["points"])

    def x(n: float) -> float:
        return L + n / x_max * PW

    def y(v: float) -> float:
        return T + PH - v / y_max * PH

    o = cs.open_svg(W, H, c["title"], c["subtitle"],
                    "Columns per cluster size: green is the certified knee (p99 ≤ 1 s, zero loss); "
                    "a yellow cap is the next rung carried with zero loss at p99 ≤ 5 s; a dashed cap "
                    "is a rung not carried. " + "; ".join(
                        f"{n} nodes {cs.compact(v)}" + (f", next rung {cs.compact(a[0])} {a[1]}" if a else "")
                        for n, v, _, _, a in c["points"]))

    # Legend: only the keys this chart uses.
    verdicts = {a[1] for *_, a in c["points"] if a}
    keys = [("good", "GREEN certified · p99 ≤ 1 s, zero loss", "box")]
    if "yellow" in verdicts:
        keys.append(("warn", "YELLOW · carried at p99 ≤ 5 s, zero loss", "box"))
    if "not carried" in verdicts:
        keys.append(("ghost", "not carried · slow acks, zero loss", "ghost"))
    keys.append(("muted", f"linear: {c['per_node'] / 1000:.0f}k msg/s × nodes", "dash"))
    lx = 32
    for role, label, kind in keys:
        o += cs.swatch(lx, 100, role, label, kind=kind)
        lx += cs.legend_width(label, kind)

    # Grid and y-axis: hairline, solid, recessive.
    for v in range(0, y_max + 1, c["y_step"]):
        o.append(cs.hline(L, L + PW, y(v), "axis" if v == 0 else "grid"))
        o.append(cs.text(L - 12, y(v) + 4, cs.compact(v) if v else "0", size=12, role="muted",
                         anchor="end", num=True))
    o.append(cs.text(L - 12, T - 14, "msg/s", size=11, role="muted", anchor="end"))

    # Linear scaling reference: a projection, so it is the one dashed line.
    pn = c["per_node"]
    xe = min(x_max, y_max / pn)
    o.append(f'<line x1="{x(0):.1f}" y1="{y(0):.1f}" x2="{x(xe):.1f}" y2="{y(pn * xe):.1f}" '
             'class="k-muted" stroke-width="1.25" stroke-dasharray="5 5"/>')

    for n, v, kind, reps, above in c["points"]:
        cx = x(n) - COL_W / 2
        top = y(v)
        if above:
            nxt, verdict = above
            yt = y(nxt)
            if verdict == "yellow":
                # 2px surface gap between the green body and its yellow cap.
                o.append(cs.column(cx, yt, top - 2, COL_W, "warn"))
            else:
                o.append(f'<rect x="{cx + 0.75:.1f}" y="{yt + 0.75:.1f}" width="{COL_W - 1.5:.1f}" '
                         f'height="{max(top - 2 - yt - 1.5, 1):.1f}" rx="4" fill="none" class="k-ghost" '
                         'stroke-width="1.5" stroke-dasharray="3 2"/>')
        o.append(cs.column(cx, top, y(0), COL_W, "good", round_top=not above))
        if kind == "floor":
            # The knee lies above: an arrow off the top of the column.
            ax = x(n)
            o.append(f'<path d="M{ax:.1f},{top - 8:.1f} V{top - 34:.1f} M{ax - 6:.1f},{top - 27:.1f} '
                     f'L{ax:.1f},{top - 35:.1f} L{ax + 6:.1f},{top - 27:.1f}" fill="none" class="k-good" '
                     'stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>')

        # Labels above the column: the certified figure, then what sits above it.
        head = y(above[0]) if above else top - (36 if kind == "floor" else 0)
        big = ("≥ " if kind == "floor" else "") + cs.compact(v)
        if above:
            nxt, verdict = above
            sub = (f"{cs.compact(nxt)} YELLOW" if verdict == "yellow" else f"{cs.compact(nxt)} not carried")
            o.append(cs.text(x(n), head - 30, big, size=16, role="ink", anchor="middle", weight=650))
            o.append(cs.text(x(n), head - 12, sub, size=11, role="ink2", anchor="middle"))
        else:
            o.append(cs.text(x(n), head - 12, big, size=16, role="ink", anchor="middle", weight=650))

        # Under the axis: size, per-node rate, and what kind of number it is.
        o.append(cs.text(x(n), y(0) + 24, f"{n} nodes", size=13, role="ink", anchor="middle", weight=600))
        o.append(cs.text(x(n), y(0) + 42, f"{v / n / 1000:.1f}k / node", size=12, role="ink2",
                         anchor="middle", num=True))
        o.append(cs.text(x(n), y(0) + 59, NOTE[kind].format(reps=reps), size=11, role="muted",
                         anchor="middle"))

    o.append(cs.text(32, H - 18, "source: " + c["source"], size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


RW, RH = 960, 600
RL, RR, RT, RB = 96, 48, 156, 118
SLOT, RCOL_W, GROUP_GAP, PANEL_GAP = 56, 26, 30, 56


def per_node(v: float, n: int) -> str:
    return f"{v / n / 1000:.1f}k"


STATE_WORDS = {"green": "certified GREEN", "holds": "holds, not certified", "fails": "does not hold"}


def offer_tick(offered: float, delivered: float) -> bool:
    return delivered < OFFER_TICK_BELOW * offered


def draw_rungs(c: dict) -> str:
    y_max = c["y_max"]
    ph = RH - RT - RB
    y0 = RT + ph
    total = c.get("unit", "per-node") == "total"
    slot, col_w, group_gap = c.get("slot", SLOT), c.get("col_w", RCOL_W), c.get("group_gap", GROUP_GAP)

    def height(v: float, n: int) -> float:
        return v if total else v / n

    def rate(v: float, n: int) -> str:
        return cs.compact(v) if total else per_node(v, n)

    def y(v: float) -> float:
        return RT + ph - v / y_max * ph

    rungs = [(n, r) for p in c["panels"] for n, _, rs in p["groups"] for r in rs]
    for _, r in rungs:
        if r[3] not in RUNG_STATES:
            raise SystemExit(f"unknown rung state {r[3]!r}; one of {', '.join(RUNG_STATES)}")
    o = cs.open_svg(RW, RH, c["title"], c["subtitle"],
                    f"One column per rung, {'total msg/s' if total else 'msg/s per node'}, as the brokers' own "
                    "counters delivered it. A filled "
                    "green column is a rung the rig certified GREEN (p99 ≤ 1 s, zero loss); a green outline "
                    "holds by the reading rule (no message lost, p99 ≤ 1 s) and is NOT certified by the rig; "
                    "a dashed grey column does not hold; a tick above a column is the offered rate where the "
                    "brokers delivered less. " + "; ".join(
                        f"{p['title']}, {n} nodes ({note}): " + ", ".join(
                            f"{sites} sites{' control' if ctl else ''} {rate(dlv, n)} of "
                            f"{rate(off, n)} offered, {STATE_WORDS[v]}"
                            for sites, off, dlv, v, ctl in rs)
                        for p in c["panels"] for n, note, rs in p["groups"]))

    # Only the keys this chart uses: a chart with no certified rung shows no filled key.
    keys = []
    if any(r[3] == "green" for _, r in rungs):
        keys.append(("good", "certified GREEN by the rig", "box"))
    if any(r[3] == "holds" for _, r in rungs):
        keys.append(("good", "holds: no loss, p99 ≤ 1 s · NOT certified", "outline"))
    if any(r[3] == "fails" for _, r in rungs):
        keys.append(("ghost", "does not hold", "ghost"))
    if any(offer_tick(r[1], r[2]) for _, r in rungs):
        keys.append(("ink2", "offered, where the brokers delivered less", "line"))
    lx = 32
    for role, label, kind in keys:
        o += cs.swatch(lx, 100, role, label, kind=kind)
        lx += cs.legend_width(label, kind)

    for v in range(0, y_max + 1, c["y_step"]):
        o.append(cs.hline(RL, RW - RR, y(v), "axis" if v == 0 else "grid"))
        o.append(cs.text(RL - 12, y(v) + 4, cs.compact(v) if v else "0", size=12, role="muted",
                         anchor="end", num=True))
    o.append(cs.text(RL - 12, RT - 14, "msg/s" if total else "msg/s per node", size=11, role="muted",
                     anchor="end"))

    x = RL + 16.0
    for pi, p in enumerate(c["panels"]):
        if pi:
            x += PANEL_GAP
            o.append(cs.vline(x - PANEL_GAP / 2, RT - 30, y0 + 78, "axis"))
        p_start = x
        for gi, (n, note, rs) in enumerate(p["groups"]):
            if gi:
                x += group_gap
            g_start = x
            for sites, off, dlv, verdict, ctl in rs:
                mid = x + slot / 2
                cx = mid - col_w / 2
                top = y(height(dlv, n))
                half = col_w / 2 - 2
                if verdict == "green":
                    o.append(cs.column(cx, top, y0, col_w, "good"))
                elif verdict == "holds":
                    # Open at the baseline, so the outline reads as a column and
                    # not as a box floating on the axis.
                    o.append(f'<path d="M{cx + 1:.1f},{y0:.1f} V{top + 5:.1f} Q{cx + 1:.1f},{top + 1:.1f} '
                             f'{cx + 5:.1f},{top + 1:.1f} H{cx + col_w - 5:.1f} Q{cx + col_w - 1:.1f},{top + 1:.1f} '
                             f'{cx + col_w - 1:.1f},{top + 5:.1f} V{y0:.1f}" fill="none" class="k-good" '
                             'stroke-width="2" data-rung="holds"/>')
                else:
                    o.append(f'<rect x="{cx + 0.75:.1f}" y="{top + 0.75:.1f}" width="{col_w - 1.5:.1f}" '
                             f'height="{max(y0 - top - 0.75, 1):.1f}" rx="4" fill="none" class="k-ghost" '
                             'stroke-width="1.5" stroke-dasharray="3 2" data-rung="fails"/>')
                if offer_tick(off, dlv):
                    yo = y(height(off, n))
                    if top - yo > 6:
                        o.append(cs.vline(mid, yo + 1, top - 2, "ghost", 1, "2 2"))
                    o.append(f'<line x1="{mid - half:.1f}" y1="{yo:.1f}" x2="{mid + half:.1f}" y2="{yo:.1f}" '
                             'class="k-ink2" stroke-width="2.5" stroke-linecap="round" data-rung="offered"/>')
                    o.append(cs.text(mid, yo - 21, rate(dlv, n), size=13, role="ink", anchor="middle",
                                     weight=650, num=True))
                    o.append(cs.text(mid, yo - 8, f"of {rate(off, n)}", size=10, role="ink2",
                                     anchor="middle", num=True))
                else:
                    o.append(cs.text(mid, top - 8, rate(dlv, n), size=13, role="ink", anchor="middle",
                                     weight=650, num=True))
                o.append(cs.text(mid, y0 + 18, f"{sites} sites", size=11, role="ink2", anchor="middle",
                                 num=True))
                if total:
                    # The per-node rate the total stands for, and the control mark beside it.
                    o.append(cs.text(mid, y0 + 31, f"{per_node(dlv, n)} / node" + (" · control" if ctl else ""),
                                     size=10, role="muted", anchor="middle", num=True))
                elif ctl:
                    o.append(cs.text(mid, y0 + 31, "control", size=10, role="muted", anchor="middle"))
                x += slot
            gm = (g_start + x) / 2
            o.append(cs.text(gm, y0 + 54, f"{n} nodes", size=13, role="ink", anchor="middle", weight=600))
            o.append(cs.text(gm, y0 + 71, note, size=11, role="muted", anchor="middle"))
        o.append(cs.text((p_start + x) / 2, RT - 28, p["title"], size=12, role="ink", anchor="middle",
                         weight=600))

    o.append(cs.text(32, RH - 18, "source: " + c["source"], size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


def self_test() -> None:
    """Certified, holds-but-not-certified and does-not-hold are three different marks,
    and only a rung the rig certified is ever filled."""
    c = {
        "title": "t", "subtitle": "s", "source": "x", "y_max": 60_000, "y_step": 10_000,
        "panels": [{"title": "p", "groups": [
            (3, "note-3", [(4, 120_000, 120_000, "green", False), (5, 150_000, 120_000, "holds", False),
                           (6, 180_000, 90_000, "fails", False)]),
            (10, "note-10", [(13, 390_000, 389_000, "holds", True)]),
        ]}],
    }
    svg = draw_rungs(c)
    body = svg.split("</desc>")[1]
    marks = {state: [line for line in svg.splitlines() if f'data-rung="{state}"' in line] for state in ("holds", "fails")}
    checks = [
        ("one certified column plus its legend key", svg.count('class="f-good"') == 2),
        ("two holds-not-certified columns", len(marks["holds"]) == 2),
        ("one does-not-hold column", len(marks["fails"]) == 1),
        ("an uncertified column has no fill", all('fill="none"' in line for lines in marks.values() for line in lines)),
        ("holds is a solid green outline", all('class="k-good"' in line and "dasharray" not in line for line in marks["holds"])),
        ("does-not-hold is dashed grey, never green", all('class="k-ghost"' in line and "dasharray" in line
                                                          for line in marks["fails"])),
        ("the offered rate is drawn only where the brokers delivered less", svg.count('data-rung="offered"') == 2),
        ("the unmet offer is labelled", "of 50.0k" in body and "of 39.0k" not in body),
        ("the legend names all three states", "certified GREEN by the rig" in body and "NOT certified" in body
         and "does not hold" in body),
        ("the description says which rungs are not certified", "holds, not certified" in svg.split("</desc>")[0]),
        ("the control rung is labelled", ">control<" in svg),
        ("both group notes are printed", "note-3" in svg and "note-10" in svg),
    ]
    all_green = {**c, "panels": [{"title": "p", "groups": [(3, "n", [(4, 120_000, 120_000, "green", False)])]}]}
    only = draw_rungs(all_green).split("</desc>")[1]
    checks.append(("no holds or does-not-hold legend key without such a rung",
                   "NOT certified" not in only and "does not hold" not in only))
    try:
        draw_rungs({**c, "panels": [{"title": "p", "groups": [(3, "n", [(4, 1, 1, "not carried", False)])]}]})
        checks.append(("an unknown rung state is refused", False))
    except SystemExit:
        checks.append(("an unknown rung state is refused", True))
    for chart in RUNG_CHARTS.values():
        out = draw_rungs(chart)
        states = [r[3] for p in chart["panels"] for _, _, rs in p["groups"] for r in rs]
        checks.append((f"{chart['file']}: {states.count('green')} certified, {states.count('holds')} holds, "
                       f"{states.count('fails')} does not hold",
                       out.count('class="f-good"') == states.count("green") + (1 if "green" in states else 0)
                       and out.count('data-rung="holds"') == states.count("holds")
                       and out.count('data-rung="fails"') == states.count("fails")))
        ticks = sum(offer_tick(r[1], r[2]) for p in chart["panels"] for _, _, rs in p["groups"] for r in rs)
        checks.append((f"{chart['file']}: {ticks} offered ticks", out.count('data-rung="offered"') == ticks))
    # A total chart: the cluster's msg/s on the columns, the per-node rate beneath, and
    # no filled mark anywhere when the rig certified none of its rungs.
    t = {**c, "unit": "total", "y_max": 300_000, "y_step": 50_000, "slot": 100, "col_w": 44,
         "panels": [{"title": "p", "groups": [
             (10, "n10", [(30, 180_000, 173_100, "holds", False)]),
             (14, "n14", [(35, 210_000, 208_739, "holds", False), (42, 252_000, 240_497, "holds", True)]),
         ]}]}
    tsvg = draw_rungs(t)
    tbody = tsvg.split("</desc>")[1]
    checks += [
        ("a total chart labels the cluster's rate, not the per-node one",
         ">173k<" in tbody and ">209k<" in tbody and ">240k<" in tbody and ">17.3k<" not in tbody),
        ("a total chart prints the per-node rate under each column",
         ">17.3k / node<" in tbody and ">14.9k / node<" in tbody and ">17.2k / node · control<" in tbody),
        ("a total chart marks the unmet offers at the cluster's rate",
         tsvg.count('data-rung="offered"') == 2 and ">of 180k<" in tbody and ">of 252k<" in tbody
         and ">of 210k<" not in tbody),
        ("a total chart's axis is msg/s, not msg/s per node",
         ">msg/s<" in tbody and "msg/s per node" not in tbody and "total msg/s" in tsvg.split("</desc>")[0]),
        ("a chart with no certified rung has no filled mark and no certified key",
         'class="f-good"' not in tsvg and "certified GREEN by the rig" not in tbody
         and tsvg.count('data-rung="holds"') == 3),
        ("the per-node chart keeps its axis", "msg/s per node" in body),
    ]
    scale = RUNG_CHARTS["durable-scaleout-candidate"]
    firsts = [[rs[0] for _, _, rs in p["groups"]] for p in scale["panels"]]
    checks.append(("the scale-out chart is 10, 12 and 14 nodes at each offer, its totals rising, none certified",
                   all([n for n, _, _ in p["groups"]] == [10, 12, 14] for p in scale["panels"])
                   and all(a[2] < b[2] for rungs in firsts for a, b in zip(rungs, rungs[1:]))
                   and all(r[3] == "holds" for rungs in firsts for r in rungs)))
    # A 14-node point stays inside the plot, and the published curves keep their axis.
    wide = {**CURVES["qos1"], "points": CURVES["qos1"]["points"] + [(14, 400_000, "certified", "", None)]}
    checks.append(("a 14-node column is drawn inside the plot",
                   14 / x_max_for(wide["points"]) * PW + COL_W / 2 <= PW and "<svg" in draw(wide)))
    checks.append(("curves up to 10 nodes keep the 11.2-node axis",
                   all(x_max_for(c["points"]) == X_MAX for c in CURVES.values())))
    bad = [name for name, ok in checks if not ok]
    if bad:
        raise SystemExit("chart-scale-out self-test FAILED: " + "; ".join(bad))
    print(f"chart-scale-out self-test: {len(checks)} checks OK (only a rung the rig certified is filled; one that "
          "holds by the reading rule is a green outline, one that does not is dashed grey; the offered rate "
          "appears only where the brokers delivered less)")


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    if which == "--self-test":
        return self_test()
    charts = {**{k: (draw, v) for k, v in CURVES.items()}, **{k: (draw_rungs, v) for k, v in RUNG_CHARTS.items()}}
    names = list(charts) if which == "all" else [which]
    for name in names:
        if name not in charts:
            raise SystemExit(f"unknown chart {name!r}; one of {', '.join(charts)} or all")
        fn, c = charts[name]
        out = Path(sys.argv[2]) if len(sys.argv) > 2 and which != "all" else IMG / c["file"]
        out.write_text(fn(c))
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
