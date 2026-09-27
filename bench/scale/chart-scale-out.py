#!/usr/bin/env python3
"""Draw the README's two scale-out charts: QoS 0 and QoS 1, msg/s vs nodes.

    python3 bench/scale/chart-scale-out.py            # both, into docs/benchmarks/img/
    python3 bench/scale/chart-scale-out.py qos1 [out]  # one

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


def main() -> None:
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    names = list(CURVES) if which == "all" else [which]
    for name in names:
        if name not in CURVES:
            raise SystemExit(f"unknown curve {name!r}; one of {', '.join(CURVES)} or all")
        out = Path(sys.argv[2]) if len(sys.argv) > 2 and which != "all" else IMG / CURVES[name]["file"]
        out.write_text(draw(CURVES[name]))
        print(f"wrote {out}")


if __name__ == "__main__":
    main()
