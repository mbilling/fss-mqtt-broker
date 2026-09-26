#!/usr/bin/env python3
"""Draw the QoS 0 per-node scale-out chart the README shows.

    python3 bench/scale/chart-scale-out.py [out.svg]   # default docs/benchmarks/img/scale-out-qos0.svg

The points are the knees recorded in bench/scale/knee-3-5-7-10.md ("2026-09-26
result"): one provisioning, drivers 2N, ladders matched per node. They are
written here rather than re-read from `.runs/` (untracked scratch), so the chart
can be regenerated from the tree and checked against the card line by line.

Every point says what kind of number it is, because they are not all the same:
  certified  — C_N by the card's rule: highest rung passing with every rung below it
  floor      — the whole ladder passed; the knee is above the top rung
  uncertified — highest passing rung of an arm whose crossing gate failed
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "docs/benchmarks/img/scale-out-qos0.svg"

# (nodes, msg/s, kind, label) — knee-3-5-7-10.md, "2026-09-26 result".
POINTS = [
    (3, 360_000, "floor", "≥ 360k — every rung passed, knee above 120k/node"),
    (5, 570_000, "certified", "570k — 114k/node"),
    (7, 810_000, "certified", "810k — 116k/node"),
    (10, 1_140_000, "uncertified", "1.14M — 114k/node; gate failed (membership flap)"),
]
PER_NODE = 114_000  # the per-node knee 5, 7 and 10 nodes share (pass 114k, fail 120k)

W, H = 900, 460
L, R, T, B = 90, 60, 60, 80
PW, PH = W - L - R, H - T - B
X_MAX, Y_MAX = 11, 1_300_000
COLORS = {"certified": "#2563eb", "floor": "#d97706", "uncertified": "#64748b"}


def x(n: float) -> float:
    return L + n / X_MAX * PW


def y(v: float) -> float:
    return T + PH - v / Y_MAX * PH


def main() -> None:
    o = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
        'font-family="system-ui,-apple-system,Segoe UI,Roboto,sans-serif" role="img" '
        'aria-label="QoS 0 shared-subscription throughput at the knee versus cluster size">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        f'<text x="{L}" y="26" font-size="16" font-weight="600" fill="#0f172a">'
        'Scale-out: msg/s at the knee vs nodes — QoS 0 $share, 200 B, 4-vCPU nodes</text>',
        f'<text x="{L}" y="44" font-size="12" fill="#475569">'
        'one provisioning (10 × CCX23 + 20 × CCX33), drivers 2 per broker, ladders matched per node; '
        'knee = p99 ≤ 1 s, all delivered, 0% crossing</text>',
        f'<rect x="{L}" y="{T}" width="{PW}" height="{PH}" fill="none" stroke="#cbd5e1"/>',
    ]
    for v in range(0, Y_MAX + 1, 200_000):
        o.append(f'<line x1="{L}" y1="{y(v):.1f}" x2="{L + PW}" y2="{y(v):.1f}" stroke="#e2e8f0"/>')
        o.append(f'<text x="{L - 8}" y="{y(v) + 4:.1f}" font-size="11" fill="#475569" text-anchor="end">'
                 f'{v / 1e6:.1f}M</text>')
    for n in (1, 3, 5, 7, 10):
        o.append(f'<line x1="{x(n):.1f}" y1="{T}" x2="{x(n):.1f}" y2="{T + PH}" stroke="#f1f5f9"/>')
        o.append(f'<text x="{x(n):.1f}" y="{T + PH + 18}" font-size="11" fill="#475569" '
                 f'text-anchor="middle">{n}</text>')
    o.append(f'<text x="{L + PW / 2}" y="{T + PH + 38}" font-size="12" fill="#334155" '
             'text-anchor="middle">brokers</text>')
    # Ideal linear scale-out from the shared per-node knee.
    o.append(f'<line x1="{x(0):.1f}" y1="{y(0):.1f}" x2="{x(X_MAX):.1f}" y2="{y(PER_NODE * X_MAX):.1f}" '
             'stroke="#94a3b8" stroke-dasharray="6 5"/>')
    o.append(f'<text x="{x(10.9) - 4:.1f}" y="{y(PER_NODE * 10.9) - 10:.1f}" font-size="11" fill="#64748b" '
             f'text-anchor="end">linear: {PER_NODE // 1000}k msg/s × nodes</text>')
    for n, v, kind, label in POINTS:
        cx, cy = x(n), y(v)
        fill = COLORS[kind] if kind != "floor" else "#ffffff"
        stroke = COLORS[kind]
        o.append(f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="7" fill="{fill}" stroke="{stroke}" stroke-width="2.5"/>')
        if kind == "floor":  # the knee lies above: draw the open-ended arrow
            o.append(f'<line x1="{cx:.1f}" y1="{cy - 8:.1f}" x2="{cx:.1f}" y2="{cy - 38:.1f}" '
                     f'stroke="{stroke}" stroke-width="2"/>')
            o.append(f'<path d="M{cx - 5:.1f},{cy - 32:.1f} L{cx:.1f},{cy - 42:.1f} L{cx + 5:.1f},{cy - 32:.1f}" '
                     f'fill="none" stroke="{stroke}" stroke-width="2"/>')
        anchor, dx = ("end", -14) if n == 10 else ("start", 14)
        o.append(f'<text x="{cx + dx:.1f}" y="{cy + 4:.1f}" font-size="12" fill="#0f172a" '
                 f'text-anchor="{anchor}">{label}</text>')
    lx, ly = L + 14, T + 18
    for i, (kind, text) in enumerate((
        ("certified", "certified knee"),
        ("floor", "floor: never reached its knee"),
        ("uncertified", "uncertified: highest passing rung, own gate failed"),
    )):
        yy = ly + i * 18
        fill = COLORS[kind] if kind != "floor" else "#ffffff"
        o.append(f'<circle cx="{lx}" cy="{yy - 4}" r="5" fill="{fill}" stroke="{COLORS[kind]}" stroke-width="2"/>')
        o.append(f'<text x="{lx + 12}" y="{yy}" font-size="11" fill="#334155">{text}</text>')
    o.append(f'<text x="{L}" y="{H - 14}" font-size="11" fill="#64748b">'
             'source: bench/scale/knee-3-5-7-10.md, run 2026-09-26 · mqttd main 0a08187 · '
             'emqtt-bench 0.6.3 · Hetzner fsn1</text>')
    o.append("</svg>")
    OUT.write_text("\n".join(o) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
