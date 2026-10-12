#!/usr/bin/env python3
"""Draw the README's scale-out charts: QoS 0 and QoS 1 msg/s vs nodes, and the
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

The per-rung chart (RUNG_CHARTS) draws one column per rung, msg/s per node:
  GREEN column    the rig graded the rung GREEN — p99 <= 1 s, zero loss
  hollow column   measured, NOT certified: the rig's verdict was NOT CARRIED
                  (the reasons are in the card named in `source`)
  tick above      the offered rate, where delivery read more than 1% below it
A rung the rig did not certify is never drawn in the certified colour.
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
# certified. A rung is (sites, offered msg/s, delivered msg/s, verdict, control):
# delivered is summarize-curve.py's `delivered/s`, verdict "green" only where the
# summarizer printed GREEN, anything else is drawn hollow. 1-site rungs are left out.
RUNG_CHARTS = {
    "qos1-candidate": {
        "file": "scale-out-qos1-candidate.svg",
        "title": "QoS 1 per node, 1.2.0 candidate: 40–50k GREEN clean, 15k GREEN durable",
        "subtitle": "msg/s per node, one column per rung · UNRELEASED candidate 7d4eced, not the signed "
                    "release · 4-vCPU nodes · zero loss",
        "source": "bench/scale/qos1-knees-2026-10-12.md · bench/scale/durable-knees-2026-10-12.md · "
                  "Hetzner fsn1, 2026-10-12 · delivered = driver-side rate",
        "y_max": 60_000, "y_step": 10_000,
        "panels": [
            {"title": "QoS 1, clean sessions — the routing path",
             "groups": [
                 (3, "crossing 0.00%", [
                     (4, 120_000, 120_001, "green", False),
                     (4, 120_000, 120_000, "green", False),
                     (5, 150_000, 149_988, "green", False),
                 ]),
                 (10, "crossing 0.00%", [
                     (13, 390_000, 374_784, "not carried", False),
                     (14, 420_000, 407_008, "not carried", False),
                     (13, 390_000, 376_125, "not carried", True),
                 ]),
             ]},
            {"title": "Durable QoS 1 — redb store, 2 copies, fsync + replication",
             "groups": [
                 (3, "crossing 0.00%", [
                     (12, 72_000, 62_806, "not carried", False),
                     (15, 90_000, 63_429, "not carried", False),
                 ]),
                 (10, "crossing gate failed: 14–17%", [
                     (25, 150_000, 149_071, "not carried", False),
                     (35, 210_000, 199_072, "not carried", False),
                     (40, 240_000, 219_004, "not carried", False),
                     (25, 150_000, 149_976, "green", True),
                 ]),
             ]},
        ],
    },
}
# The offered rate is drawn where delivery read more than this share below it.
OFFER_TICK_BELOW = 0.99

W, H = 960, 572
L, R, T, B = 96, 48, 140, 110
PW, PH = W - L - R, H - T - B
X_MAX = 11.2
COL_W = 24


def draw(c: dict) -> str:
    y_max = c["y_max"]

    def x(n: float) -> float:
        return L + n / X_MAX * PW

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
    xe = min(X_MAX, y_max / pn)
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


def offer_tick(offered: float, delivered: float) -> bool:
    return delivered < OFFER_TICK_BELOW * offered


def draw_rungs(c: dict) -> str:
    y_max = c["y_max"]
    ph = RH - RT - RB
    y0 = RT + ph

    def y(v: float) -> float:
        return RT + ph - v / y_max * ph

    rungs = [(n, r) for p in c["panels"] for n, _, rs in p["groups"] for r in rs]
    o = cs.open_svg(RW, RH, c["title"], c["subtitle"],
                    "One column per rung, msg/s per node. A green column is a rung the rig graded GREEN "
                    "(p99 ≤ 1 s, zero loss); a hollow dashed column was measured but NOT certified; a tick "
                    "above a column is the offered rate where delivery read below it. " + "; ".join(
                        f"{p['title']}, {n} nodes ({note}): " + ", ".join(
                            f"{sites} sites{' control' if ctl else ''} {per_node(dlv, n)} of "
                            f"{per_node(off, n)} offered, {'GREEN' if v == 'green' else 'not certified'}"
                            for sites, off, dlv, v, ctl in rs)
                        for p in c["panels"] for n, note, rs in p["groups"]))

    keys = [("good", "GREEN · certified, p99 ≤ 1 s, zero loss", "box")]
    if any(r[3] != "green" for _, r in rungs):
        keys.append(("ghost", "measured, NOT certified · zero loss", "ghost"))
    if any(offer_tick(r[1], r[2]) for _, r in rungs):
        keys.append(("ink2", "offered, where delivery read below it", "line"))
    lx = 32
    for role, label, kind in keys:
        o += cs.swatch(lx, 100, role, label, kind=kind)
        lx += cs.legend_width(label, kind)

    for v in range(0, y_max + 1, c["y_step"]):
        o.append(cs.hline(RL, RW - RR, y(v), "axis" if v == 0 else "grid"))
        o.append(cs.text(RL - 12, y(v) + 4, cs.compact(v) if v else "0", size=12, role="muted",
                         anchor="end", num=True))
    o.append(cs.text(RL - 12, RT - 14, "msg/s per node", size=11, role="muted", anchor="end"))

    x = RL + 16.0
    for pi, p in enumerate(c["panels"]):
        if pi:
            x += PANEL_GAP
            o.append(cs.vline(x - PANEL_GAP / 2, RT - 30, y0 + 78, "axis"))
        p_start = x
        for gi, (n, note, rs) in enumerate(p["groups"]):
            if gi:
                x += GROUP_GAP
            g_start = x
            for sites, off, dlv, verdict, ctl in rs:
                mid = x + SLOT / 2
                cx = mid - RCOL_W / 2
                top = y(dlv / n)
                if verdict == "green":
                    o.append(cs.column(cx, top, y0, RCOL_W, "good"))
                else:
                    o.append(f'<rect x="{cx + 0.75:.1f}" y="{top + 0.75:.1f}" width="{RCOL_W - 1.5:.1f}" '
                             f'height="{max(y0 - top - 0.75, 1):.1f}" rx="4" fill="none" class="k-ghost" '
                             'stroke-width="1.5" stroke-dasharray="3 2" data-rung="uncertified"/>')
                if offer_tick(off, dlv):
                    yo = y(off / n)
                    if top - yo > 6:
                        o.append(cs.vline(mid, yo + 1, top - 2, "ghost", 1, "2 2"))
                    o.append(f'<line x1="{mid - 11:.1f}" y1="{yo:.1f}" x2="{mid + 11:.1f}" y2="{yo:.1f}" '
                             'class="k-ink2" stroke-width="2.5" stroke-linecap="round" data-rung="offered"/>')
                    o.append(cs.text(mid, yo - 21, per_node(dlv, n), size=13, role="ink", anchor="middle",
                                     weight=650, num=True))
                    o.append(cs.text(mid, yo - 8, f"of {per_node(off, n)}", size=10, role="ink2",
                                     anchor="middle", num=True))
                else:
                    o.append(cs.text(mid, top - 8, per_node(dlv, n), size=13, role="ink", anchor="middle",
                                     weight=650, num=True))
                o.append(cs.text(mid, y0 + 18, f"{sites} sites", size=11, role="ink2", anchor="middle",
                                 num=True))
                if ctl:
                    o.append(cs.text(mid, y0 + 31, "control", size=10, role="muted", anchor="middle"))
                x += SLOT
            gm = (g_start + x) / 2
            o.append(cs.text(gm, y0 + 54, f"{n} nodes", size=13, role="ink", anchor="middle", weight=600))
            o.append(cs.text(gm, y0 + 71, note, size=11, role="muted", anchor="middle"))
        o.append(cs.text((p_start + x) / 2, RT - 28, p["title"], size=12, role="ink", anchor="middle",
                         weight=600))

    o.append(cs.text(32, RH - 18, "source: " + c["source"], size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


def self_test() -> None:
    """A rung the rig did not certify must never be drawn as a certified one."""
    c = {
        "title": "t", "subtitle": "s", "source": "x", "y_max": 60_000, "y_step": 10_000,
        "panels": [{"title": "p", "groups": [
            (3, "note-3", [(4, 120_000, 120_000, "green", False), (5, 150_000, 120_000, "not carried", False)]),
            (10, "note-10", [(13, 390_000, 389_000, "not carried", True)]),
        ]}],
    }
    svg = draw_rungs(c)
    checks = [
        ("one certified column plus its legend key", svg.count('class="f-good"') == 2),
        ("two hollow columns", svg.count('data-rung="uncertified"') == 2),
        ("a hollow column has no fill", all('fill="none"' in line for line in svg.splitlines()
                                            if 'data-rung="uncertified"' in line)),
        ("the offered rate is drawn only where delivery read below it", svg.count('data-rung="offered"') == 1),
        ("the unmet offer is labelled", "of 50.0k" in svg and "of 39.0k" not in svg.split("</desc>")[1]),
        ("the legend names the uncertified mark", "measured, NOT certified" in svg),
        ("the control rung is labelled", ">control<" in svg),
        ("both group notes are printed", "note-3" in svg and "note-10" in svg),
    ]
    all_green = {**c, "panels": [{"title": "p", "groups": [(3, "n", [(4, 120_000, 120_000, "green", False)])]}]}
    checks.append(("no uncertified legend key without an uncertified rung",
                   "NOT certified" not in draw_rungs(all_green).split("</desc>")[1]))
    for chart in RUNG_CHARTS.values():
        out = draw_rungs(chart)
        want = sum(r[3] == "green" for p in chart["panels"] for _, _, rs in p["groups"] for r in rs)
        total = sum(len(rs) for p in chart["panels"] for _, _, rs in p["groups"])
        checks.append((f"{chart['file']}: {want} certified, {total - want} hollow",
                       out.count('class="f-good"') == want + 1
                       and out.count('data-rung="uncertified"') == total - want))
    bad = [name for name, ok in checks if not ok]
    if bad:
        raise SystemExit("chart-scale-out self-test FAILED: " + "; ".join(bad))
    print(f"chart-scale-out self-test: {len(checks)} checks OK (an uncertified rung is hollow, never green; "
          "the offered rate appears only where delivery read below it)")


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
