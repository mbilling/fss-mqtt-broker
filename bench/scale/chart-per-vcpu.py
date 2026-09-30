#!/usr/bin/env python3
"""Draw the README's per-vCPU comparison from docs/benchmarks/data/per-vcpu.csv.

One bar group per workload class, each on its own scale, so a bar is only ever
read against bars of the same class: the classes differ by 2-10x in what a
message costs, and a shared axis would invite exactly the comparison the data
does not support. A solid bar is our measurement; an outlined one is a vendor's
published figure. Every bar is labelled with msg/s per vCPU, msg/s per physical
core, and what a vCPU is on that hardware (an SMT thread or a whole core).

    python3 bench/scale/chart-per-vcpu.py [out.svg]

The output is committed; a diff after running means the chart and its data disagree.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import chart_style as cs  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "docs/benchmarks/data/per-vcpu.csv"
OUT = ROOT / "docs/benchmarks/img/per-vcpu.svg"

CLASSES = [
    ("single-node-qos0", "Single node, QoS 0 · same 4-vCPU host, measured by us"),
    ("cluster-qos0", "Cluster, QoS 0"),
    ("cluster-qos1", "Cluster, QoS 1, not durable"),
    ("cluster-durable-qos1", "Cluster, durable QoS 1, 2 copies of every message"),
]
# A class where a vendor publishes nothing comparable says so, rather than showing a lone bar.
MISSING: dict[str, str] = {}
VCPU_IS = {"smt-thread": "vCPU = SMT thread", "core": "vCPU = whole core", "unknown": "SMT not published"}

W = 880
L = 250          # bar origin
BAR_W = 330      # longest bar in a group
ROW = 42
BAR_H = 18


def load() -> list[dict]:
    with DATA.open() as f:
        rows = list(csv.DictReader(line for line in f if not line.startswith("#")))
    for r in rows:
        r["per_vcpu"] = int(r["msg_per_s"]) / int(r["vcpu"])
        r["per_core"] = int(r["msg_per_s"]) / int(r["physical_cores"]) if r["physical_cores"] else None
    return rows


def label(r: dict) -> str:
    return f"{r['broker']} {r['version']}"


def draw(rows: list[dict]) -> str:
    groups = [(key, title, [r for r in rows if r["class"] == key]) for key, title in CLASSES]
    groups = [g for g in groups if g[2]]
    h = 132 + sum(40 + len(g[2]) * ROW + (ROW if g[0] in MISSING else 0) for g in groups) + 30
    o = cs.open_svg(
        W, h, "Throughput per vCPU, like for like",
        "msg/s per vCPU at each figure's rate · each group on its own scale · compare within a group only",
        "; ".join(f"{label(r)} ({r['origin']}, {r['class']}): {r['per_vcpu']:.0f} msg/s per vCPU"
                  for r in rows),
    )
    lx = 32
    for role, text, kind in (("s1", "measured by us", "box"), ("ghost", "vendor-published", "ghost")):
        o += cs.swatch(lx, 100, role, text, kind=kind)
        lx += cs.legend_width(text, kind)

    y = 132
    for key, title, members in groups:
        o.append(cs.text(32, y + 16, title, size=13, role="ink", weight=650))
        y += 30
        top = max(r["per_vcpu"] for r in members)
        for r in members:
            role = cs.slot(r["broker"].split()[0])
            w = max(r["per_vcpu"] / top * BAR_W, 2.0)
            yb = y + (ROW - BAR_H) / 2
            o.append(cs.text(L - 12, yb + 9, label(r), size=12, role="ink", anchor="end", weight=600))
            o.append(cs.text(L - 12, yb + 24, f"{r['instance']} · {VCPU_IS[r['vcpu_is']]}", size=10,
                             role="muted", anchor="end"))
            if r["origin"] == "measured":
                o.append(f'<rect x="{L}" y="{yb:.1f}" width="{w:.1f}" height="{BAR_H}" rx="4" class="f-{role}"/>')
            else:
                o.append(f'<rect x="{L + 0.75}" y="{yb + 0.75:.1f}" width="{max(w - 1.5, 1):.1f}" '
                         f'height="{BAR_H - 1.5}" rx="4" fill="none" class="k-{role}" '
                         'stroke-width="1.5" stroke-dasharray="4 3"/>')
            core = f" · {r['per_core']:,.0f}/core" if r["per_core"] else ""
            o.append(cs.text(L + w + 8, yb + 13, f"{r['per_vcpu']:,.0f}/vCPU{core}", size=12,
                             role="ink", num=True, weight=600))
            y += ROW
        if key in MISSING:
            o.append(cs.text(L, y + ROW / 2 + 4, MISSING[key], size=12, role="muted"))
            y += ROW
        y += 10

    o.append(cs.text(32, h - 18, "source: docs/benchmarks/data/per-vcpu.csv · every row names its report",
                     size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


def main() -> None:
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else OUT
    out.write_text(draw(load()))
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
