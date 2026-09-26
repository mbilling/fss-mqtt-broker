#!/usr/bin/env python3
"""Draw the README's two scale-out charts: QoS 0 and QoS 1, msg/s vs nodes.

    python3 bench/scale/chart-scale-out.py            # both, into docs/benchmarks/img/
    python3 bench/scale/chart-scale-out.py qos1 [out]  # one

The points are the ones recorded in the run cards and curve documents named in
each curve's `source`; they are written here rather than re-read from `.runs/`
(untracked scratch), so the charts can be regenerated from the tree and checked
against those documents line by line.

Every point says what kind of number it is, because they are not all the same:
  certified   — passed with every repetition the rule asks for
  partial     — passed, but not every repetition is certified (the others were
                INVALID on driver-side evidence while the brokers received it all)
  floor       — the highest rung measured; the knee lies above it
  uncertified — highest passing rung of an arm whose own gate failed
A red cross marks the first rung that FAILED above a point: the knee lies
between the two.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
IMG = ROOT / "docs/benchmarks/img"

CURVES = {
    "qos0": {
        "file": "scale-out-qos0.svg",
        "title": "QoS 0 scale-out: msg/s at the knee vs nodes — $share 1:1, 200 B, 4-vCPU nodes",
        "subtitle": "one provisioning (10 × CCX23 + 20 × CCX33), drivers 2 per broker, ladders matched per node; "
                    "knee = p99 ≤ 1 s, all delivered, 0% crossing",
        "source": "source: bench/scale/knee-3-5-7-10.md, run 2026-09-26 · mqttd main 0a08187 · emqtt-bench 0.6.3 · Hetzner fsn1",
        "y_max": 1_300_000, "y_step": 200_000,
        "per_node": 114_000,
        # (nodes, msg/s, kind, label, first failing rung above or None) — knee-3-5-7-10.md
        "points": [
            (3, 360_000, "floor", "≥ 360k — every rung passed", None),
            (5, 570_000, "certified", "570k — 114k/node", 600_000),
            (7, 810_000, "certified", "810k — 116k/node", 840_000),
            (10, 1_140_000, "uncertified", "1.14M — 114k/node; gate failed (membership flap)", 1_200_000),
        ],
    },
    "qos1": {
        "file": "scale-out-qos1.svg",
        "title": "QoS 1 scale-out: msg/s vs nodes — $share 1:1, QoS 1 both ways, 200 B, 4-vCPU nodes",
        "subtitle": "3 and 5 nodes: separate provisionings, v1.0.17 · 7 and 10 nodes: one provisioning, main 0a08187, "
                    "20 consumers per site · p99 ≤ 1 s, 0% crossing",
        "source": "source: docs/benchmarks/QOS1-SCALE-CURVE.md · audited emqtt-bench 0.6.3 (67bb4194…) · Hetzner fsn1",
        "y_max": 450_000, "y_step": 100_000,
        "per_node": 39_000,
        # QOS1-SCALE-CURVE.md, "The curve" and "7 and 10 nodes — one provisioning"
        "points": [
            (3, 120_000, "certified", "120k — 40.0k/node (3 + control)", None),
            (5, 180_000, "certified", "180k — 36.0k/node (3 + control)", None),
            (7, 270_000, "partial", "270k — 38.6k/node (2 of 3 certified)", 300_000),
            (10, 390_000, "partial", "390k — 39.0k/node (1 of 3); knee not reached", None),
        ],
    },
}

W, H = 900, 460
L, R, T, B = 90, 60, 60, 80
PW, PH = W - L - R, H - T - B
X_MAX = 11
COLORS = {"certified": "#2563eb", "partial": "#0891b2", "floor": "#d97706", "uncertified": "#64748b"}
LEGEND = {
    "certified": "certified",
    "partial": "passed; not every repetition certified",
    "floor": "floor: never reached its knee",
    "uncertified": "uncertified: highest passing rung, own gate failed",
}
FAIL = "#dc2626"


def draw(c: dict) -> str:
    y_max = c["y_max"]

    def x(n: float) -> float:
        return L + n / X_MAX * PW

    def y(v: float) -> float:
        return T + PH - v / y_max * PH

    def fmt(v: float) -> str:
        return f"{v / 1e6:.1f}M" if y_max >= 1_000_000 else f"{v / 1e3:.0f}k"

    o = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
        'font-family="system-ui,-apple-system,Segoe UI,Roboto,sans-serif" role="img" '
        f'aria-label="{c["title"]}">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        f'<text x="{L}" y="26" font-size="16" font-weight="600" fill="#0f172a">{c["title"]}</text>',
        f'<text x="{L}" y="44" font-size="12" fill="#475569">{c["subtitle"]}</text>',
        f'<rect x="{L}" y="{T}" width="{PW}" height="{PH}" fill="none" stroke="#cbd5e1"/>',
    ]
    for v in range(0, y_max + 1, c["y_step"]):
        o.append(f'<line x1="{L}" y1="{y(v):.1f}" x2="{L + PW}" y2="{y(v):.1f}" stroke="#e2e8f0"/>')
        o.append(f'<text x="{L - 8}" y="{y(v) + 4:.1f}" font-size="11" fill="#475569" text-anchor="end">{fmt(v)}</text>')
    for n in (1, 3, 5, 7, 10):
        o.append(f'<line x1="{x(n):.1f}" y1="{T}" x2="{x(n):.1f}" y2="{T + PH}" stroke="#f1f5f9"/>')
        o.append(f'<text x="{x(n):.1f}" y="{T + PH + 18}" font-size="11" fill="#475569" text-anchor="middle">{n}</text>')
    o.append(f'<text x="{L + PW / 2}" y="{T + PH + 38}" font-size="12" fill="#334155" text-anchor="middle">brokers</text>')
    pn = c["per_node"]
    o.append(f'<line x1="{x(0):.1f}" y1="{y(0):.1f}" x2="{x(X_MAX):.1f}" y2="{y(pn * X_MAX):.1f}" '
             'stroke="#94a3b8" stroke-dasharray="6 5"/>')
    # Below the line between the 7- and 10-node points, clear of both labels.
    o.append(f'<text x="{x(8.9):.1f}" y="{y(pn * 8.9) + 22:.1f}" font-size="11" fill="#64748b" '
             f'text-anchor="end">linear: {pn / 1000:.0f}k msg/s × nodes</text>')
    kinds = []
    for n, v, kind, label, fail in c["points"]:
        kinds.append(kind)
        cx, cy = x(n), y(v)
        stroke = COLORS[kind]
        fill = "#ffffff" if kind == "floor" else stroke
        if fail:
            fy = y(fail)
            o.append(f'<line x1="{cx:.1f}" y1="{cy:.1f}" x2="{cx:.1f}" y2="{fy:.1f}" stroke="{FAIL}" '
                     'stroke-width="1.5" stroke-dasharray="3 3"/>')
            o.append(f'<path d="M{cx - 5:.1f},{fy - 5:.1f} L{cx + 5:.1f},{fy + 5:.1f} M{cx - 5:.1f},{fy + 5:.1f} '
                     f'L{cx + 5:.1f},{fy - 5:.1f}" stroke="{FAIL}" stroke-width="2.5"/>')
        if kind == "floor" or (kind == "partial" and not fail and n == 10):
            o.append(f'<line x1="{cx:.1f}" y1="{cy - 8:.1f}" x2="{cx:.1f}" y2="{cy - 38:.1f}" stroke="{stroke}" stroke-width="2"/>')
            o.append(f'<path d="M{cx - 5:.1f},{cy - 32:.1f} L{cx:.1f},{cy - 42:.1f} L{cx + 5:.1f},{cy - 32:.1f}" '
                     f'fill="none" stroke="{stroke}" stroke-width="2"/>')
        o.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="7" fill="{fill}" stroke="{stroke}" stroke-width="2.5"/>')
        anchor, dx = ("end", -14) if n == 10 else ("start", 14)
        o.append(f'<text x="{cx + dx:.1f}" y="{cy + 4:.1f}" font-size="12" fill="#0f172a" text-anchor="{anchor}">{label}</text>')
    lx, ly = L + 14, T + 18
    rows = [k for k in COLORS if k in kinds]
    for i, kind in enumerate(rows):
        yy = ly + i * 18
        fill = "#ffffff" if kind == "floor" else COLORS[kind]
        o.append(f'<circle cx="{lx}" cy="{yy - 4}" r="5" fill="{fill}" stroke="{COLORS[kind]}" stroke-width="2"/>')
        o.append(f'<text x="{lx + 12}" y="{yy}" font-size="11" fill="#334155">{LEGEND[kind]}</text>')
    if any(p[4] for p in c["points"]):
        yy = ly + len(rows) * 18
        o.append(f'<path d="M{lx - 4},{yy - 8} L{lx + 4},{yy} M{lx - 4},{yy} L{lx + 4},{yy - 8}" stroke="{FAIL}" stroke-width="2.5"/>')
        o.append(f'<text x="{lx + 12}" y="{yy}" font-size="11" fill="#334155">first failing rung — the knee lies between</text>')
    o.append(f'<text x="{L}" y="{H - 14}" font-size="11" fill="#64748b">{c["source"]}</text>')
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
