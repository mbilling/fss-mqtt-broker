#!/usr/bin/env python3
"""Draw the README's three evidence charts from docs/benchmarks/data/headline.csv.

    python3 bench/scale/chart-readme.py              # all three, into docs/benchmarks/img/
    python3 bench/scale/chart-readme.py --self-test

  readme-scale-out.svg         msg/s per node at 3 and 10 nodes: durable QoS 1 (both
                               stores), QoS 1 clean sessions, QoS 0; cluster totals labelled
  readme-durable-per-vcpu.svg  durable QoS 1 per vCPU against HiveMQ's published run
  readme-single-node.svg       the single-node knee of four brokers on one host

Every figure is a row of headline.csv, and every row names the bench card (and the
section of it) the figure is read from; scripts/check-readme-facts.py holds the CSV to
those cards and the README's prose to the CSV.

The marks keep the meaning they have in chart-scale-out.py (ADR 0048, 2026-09-27):
  filled GREEN   the rig certified the rung GREEN: p99 <= 1 s, zero loss
  GREEN outline  holds by the reading rule (zero loss, p99 <= 1 s, the brokers' counters)
                 but NOT certified by the rig
  YELLOW cap     carried with zero loss at p99 <= 5 s
  arrow          the top of the ladder that was run: the knee lies above
A bar the rig did not certify is never filled.
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import chart_style as cs  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "docs/benchmarks/data/headline.csv"
IMG = ROOT / "docs/benchmarks/img"
STATES = ("green", "holds", "yellow", "published")


def load(path: Path = DATA) -> list[dict]:
    with path.open() as f:
        rows = list(csv.DictReader(line for line in f if not line.startswith("#")))
    for r in rows:
        if r["state"] not in STATES:
            raise SystemExit(f"headline.csv: unknown state {r['state']!r}; one of {', '.join(STATES)}")
        for k in ("nodes", "vcpu", "msg_per_s", "floor"):
            r[k] = int(r[k])
    return rows


def per_node(v: float) -> str:
    """A per-node rate the way the cards print it: 30.0k, 18.5k, 114k."""
    return f"{v / 1000:.0f}k" if v >= 100_000 else f"{v / 1000:.1f}k"


# --------------------------------------------------------------------------- scale-out

W, H = 900, 606
T0, PH = 196, 270          # plot top, plot height
Y0 = T0 + PH               # baseline
BAR, IN_GAP = 54, 14       # bar width, gap between the 3- and 10-node bars of a class

PANELS = [
    {"title": "Durable QoS 1 · every ack after fsync + 2-copy quorum replication",
     "x0": 84, "x1": 486, "y_max": 40_000, "y_step": 10_000, "backdrop": True,
     "groups": [("durable-redb", "default store (redb)", "the 1.2.0 default"),
                ("durable-log", "log store", "experimental in 1.2.0")]},
    {"title": "Not durable",
     "x0": 568, "x1": 868, "y_max": 160_000, "y_step": 40_000, "backdrop": False,
     "groups": [("qos1-clean", "QoS 1, clean sessions", "routing path, no fsync"),
                ("qos0", "QoS 0", "at most once")]},
]


def scale_bars(rows: list[dict]) -> dict:
    """series -> nodes -> {"main": row, "yellow": row or None}."""
    out: dict = {}
    for r in rows:
        if r["chart"] != "scale-out":
            continue
        slot = out.setdefault(r["series"], {}).setdefault(r["nodes"], {"main": None, "yellow": None})
        slot["yellow" if r["state"] == "yellow" else "main"] = r
    return out


def draw_scale_out(rows: list[dict]) -> str:
    bars = scale_bars(rows)
    desc = []
    for p in PANELS:
        for series, name, _ in p["groups"]:
            for n in (3, 10):
                b = bars[series][n]
                m = b["main"]
                s = (f"{m['label']}, {n} nodes: {per_node(m['msg_per_s'] / n)} msg/s per node, "
                     f"{m['msg_per_s']:,} msg/s in total, "
                     + ("certified GREEN" if m["state"] == "green" else "holds, not certified")
                     + (", the top of the ladder run" if m["floor"] else ""))
                if b["yellow"]:
                    s += f"; YELLOW at {b['yellow']['msg_per_s']:,} msg/s (p99 ≤ 5 s, zero loss)"
                desc.append(s)
    o = cs.open_svg(
        W, H, "Per-node throughput holds from 3 to 10 nodes, durable QoS 1 included",
        "msg/s per 4-vCPU node from the brokers' own counters · no message lost on any bar · "
        "measured on 1.2.0, 2026-10-21",
        "Columns are msg/s per node at 3 and 10 nodes; the cluster total is printed above each column. "
        "A filled green column was certified GREEN by the rig (p99 ≤ 1 s, zero loss); a green outline "
        "holds by the reading rule (zero loss, p99 ≤ 1 s) and was not certified; a yellow cap was carried "
        "with zero loss at p99 ≤ 5 s; an arrow marks the top of the ladder that was run. " + "; ".join(desc)
        + ". Durable QoS 1 and QoS 1 clean sessions on the 1.2.0 release candidate ec407218, 2026-10-21; "
        "QoS 0 on main 0a08187, 2026-09-26.")
    o.append(cs.text(32, 84, "Cluster totals above each column. QoS 0 was measured on main 0a08187 "
                     "(2026-09-26), before 1.2.0.", size=13, role="ink2"))

    keys = [("good", "certified GREEN · p99 ≤ 1 s, zero loss", "box"),
            ("good", "holds: zero loss, p99 ≤ 1 s · not certified", "outline"),
            ("warn", "YELLOW · zero loss, p99 ≤ 5 s", "box")]
    lx = 32
    for role, label, kind in keys:
        o += cs.swatch(lx, 120, role, label, kind=kind)
        lx += cs.legend_width(label, kind) - 8

    for p in PANELS:
        x0, x1, y_max = p["x0"], p["x1"], p["y_max"]

        def y(v: float, y_max=y_max) -> float:
            return Y0 - v / y_max * PH

        if p["backdrop"]:
            o.append(f'<rect x="{x0 - 64}" y="{T0 - 52}" width="{x1 - x0 + 76}" height="{PH + 136}" '
                     'rx="10" class="f-grid" fill-opacity="0.55"/>')
        o.append(cs.text(x0 - 52, T0 - 30, p["title"], size=13, role="ink", weight=650))
        for v in range(0, y_max + 1, p["y_step"]):
            o.append(cs.hline(x0, x1, y(v), "axis" if v == 0 else "grid"))
            o.append(cs.text(x0 - 8, y(v) + 4, cs.compact(v) if v else "0", size=11, role="muted",
                             anchor="end", num=True))
        o.append(cs.text(x0 - 40, T0 - 12, "msg/s per node", size=10, role="muted"))

        gw = 2 * BAR + IN_GAP
        gap = (x1 - x0 - 2 * gw) / 3
        for gi, (series, name, sub) in enumerate(p["groups"]):
            gx = x0 + gap + gi * (gw + gap)
            for bi, n in enumerate((3, 10)):
                b = bars[series][n]
                m = b["main"]
                v = m["msg_per_s"] / n
                bx = gx + bi * (BAR + IN_GAP)
                mid = bx + BAR / 2
                top = y(v)
                cap = b["yellow"]
                if m["state"] == "green":
                    o.append(cs.column(bx, top, Y0, BAR, "good", round_top=not cap))
                else:
                    o.append(f'<path d="M{bx + 1:.1f},{Y0:.1f} V{top + 5:.1f} Q{bx + 1:.1f},{top + 1:.1f} '
                             f'{bx + 5:.1f},{top + 1:.1f} H{bx + BAR - 5:.1f} Q{bx + BAR - 1:.1f},{top + 1:.1f} '
                             f'{bx + BAR - 1:.1f},{top + 5:.1f} V{Y0:.1f}" fill="none" class="k-good" '
                             'stroke-width="2" data-bar="holds"/>')
                head = top
                if cap:
                    yt = y(cap["msg_per_s"] / n)
                    o.append(cs.column(bx, yt, top - 2, BAR, "warn"))
                    head = yt
                if m["floor"]:
                    o.append(f'<path d="M{mid:.1f},{head - 6:.1f} V{head - 26:.1f} M{mid - 5:.1f},{head - 20:.1f} '
                             f'L{mid:.1f},{head - 27:.1f} L{mid + 5:.1f},{head - 20:.1f}" fill="none" '
                             'class="k-good" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>')
                    head -= 28
                lines = [(("≥ " if m["floor"] else "") + per_node(m["msg_per_s"] / n), 15, "ink", 650),
                         (f"{cs.compact(m['msg_per_s'])} total", 11, "ink2", 400)]
                if cap:
                    lines.append((f"YELLOW {cs.compact(cap['msg_per_s'])}", 11, "ink2", 400))
                ly = head - 8
                for s, size, role, wt in reversed(lines):
                    o.append(cs.text(mid, ly, s, size=size, role=role, anchor="middle", weight=wt, num=True))
                    ly -= size + 4
                o.append(cs.text(mid, Y0 + 18, f"{n} nodes", size=12, role="ink", anchor="middle", weight=600))
            gm = gx + gw / 2
            o.append(cs.text(gm, Y0 + 42, name, size=13, role="ink", anchor="middle", weight=650))
            o.append(cs.text(gm, Y0 + 59, sub, size=11, role="muted", anchor="middle"))

    o.append(cs.text(32, H - 36, "1.2.0 = the release candidate ec407218, built with the release recipe "
                     "(v1.2.0 adds a TLS-library patch) · Hetzner CCX23 brokers · $share 1:1, 200 B · "
                     "60 s windows", size=11, role="muted"))
    o.append(cs.text(32, H - 18, "source: docs/benchmarks/data/headline.csv → bench/scale/durable-knees-2026-10-21.md "
                     "· qos1-knees-2026-10-21.md · knee-3-5-7-10.md", size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


# --------------------------------------------------------------------------- horizontal bars

HL, HBAR_W, HROW, HBAR_H = 270, 360, 54, 22


def draw_hbars(rows: list[dict], *, title: str, subtitle: str, value, detail, sub, footer: list[str],
               desc: str) -> str:
    states = {r["state"] for r in rows}
    legend = len(states) > 1
    h = (150 if legend else 120) + len(rows) * HROW + 18 * len(footer) + 12
    o = cs.open_svg(W, h, title, subtitle, desc)
    keys = []
    if "green" in states:
        keys.append(("s1", "measured by us, certified", "box"))
    if "holds" in states:
        keys.append(("s1", "measured by us, holds, not certified", "outline"))
    if "published" in states:
        keys.append(("ghost", "vendor-published, not reproduced", "ghost"))
    if legend:
        lx = 32
        for role, label, kind in keys:
            o += cs.swatch(lx, 100, role, label, kind=kind)
            lx += cs.legend_width(label, kind)
    top = max(value(r) for r in rows)
    y = 126 if legend else 92
    for r in rows:
        role = cs.slot(r["series"])
        w = max(value(r) / top * HBAR_W, 2.0)
        yb = y + (HROW - HBAR_H) / 2
        o.append(cs.text(HL - 14, yb + 10, r["label"], size=13, role="ink", anchor="end", weight=600))
        o.append(cs.text(HL - 14, yb + 26, sub(r), size=11, role="muted", anchor="end"))
        if r["state"] == "green":
            o.append(f'<rect x="{HL}" y="{yb:.1f}" width="{w:.1f}" height="{HBAR_H}" rx="4" class="f-{role}"/>')
        elif r["state"] == "holds":
            o.append(f'<rect x="{HL + 1}" y="{yb + 1:.1f}" width="{max(w - 2, 1):.1f}" height="{HBAR_H - 2}" '
                     f'rx="4" fill="none" class="k-{role}" stroke-width="2"/>')
        else:
            o.append(f'<rect x="{HL + 0.75}" y="{yb + 0.75:.1f}" width="{max(w - 1.5, 1):.1f}" '
                     f'height="{HBAR_H - 1.5}" rx="4" fill="none" class="k-{role}" stroke-width="1.5" '
                     'stroke-dasharray="4 3"/>')
        o.append(cs.text(HL + w + 10, yb + 11, detail(r)[0], size=14, role="ink", weight=650, num=True))
        o.append(cs.text(HL + w + 10, yb + 26, detail(r)[1], size=11, role="ink2", num=True))
        y += HROW
    fy = h - 18 * len(footer)
    for line in footer:
        o.append(cs.text(32, fy, line, size=11, role="muted"))
        fy += 18
    o.append("</svg>")
    return "\n".join(o) + "\n"


def ratio(a: float, b: float) -> str:
    return f"{a / b:.1f}×"


def draw_per_vcpu(rows: list[dict]) -> str:
    rows = [r for r in rows if r["chart"] == "durable-per-vcpu"]
    pv = {r["series"]: r["msg_per_s"] / r["vcpu"] for r in rows}
    hive = pv["hivemq"]
    words = {"green": "certified GREEN", "holds": "holds, not certified", "published": "vendor-published"}
    return draw_hbars(
        rows,
        title=f"Durable QoS 1 per vCPU: {ratio(pv['mqttd-log'], hive)} HiveMQ's published figure",
        subtitle=(f"3-node clusters, 2 copies of every message · a vCPU is an SMT thread on AMD EPYC Milan "
                  f"in every row · {ratio(pv['mqttd-redb'], hive)} on 1.2.0's default store"),
        value=lambda r: r["msg_per_s"] / r["vcpu"],
        detail=lambda r: (f"{r['msg_per_s'] / r['vcpu']:,.0f} msg/s per vCPU",
                          f"{r['msg_per_s']:,} msg/s on {r['vcpu']} vCPU · {words[r['state']]}"),
        sub=lambda r: {"mqttd-log": "experimental in 1.2.0 · 3 × CCX23, 4 vCPU",
                       "mqttd-redb": "redb · 3 × Hetzner CCX23, 4 vCPU",
                       "hivemq": "3 × AWS m6a.8xlarge, 32 vCPU"}[r["series"]],
        footer=["mqttd: the 1.2.0 release candidate ec407218, 2026-10-21, $share 1:1, 200 B, persistent consumers",
                "HiveMQ: its default configuration (persistence on, replica-count 2), published 2023, payload not "
                "published, not reproduced by us",
                "source: docs/benchmarks/data/headline.csv → bench/scale/durable-knees-2026-10-21.md · "
                "docs/benchmarks/data/per-vcpu.csv (HiveMQ's report)"],
        desc="; ".join(f"{r['label']}: {r['msg_per_s'] / r['vcpu']:,.0f} msg/s per vCPU "
                       f"({r['msg_per_s']:,} msg/s on {r['vcpu']} vCPU, {words[r['state']]})" for r in rows),
    )


def draw_single_node(rows: list[dict]) -> str:
    rows = [r for r in rows if r["chart"] == "single-node"]
    v = {r["series"]: r["msg_per_s"] for r in rows}
    p99 = {"mqttd": "≤ 500 ms", "mosquitto": "≤ 100 ms", "emqx": "≤ 500 ms", "hivemq": "≤ 100 ms"}
    return draw_hbars(
        rows,
        title=(f"One 4-vCPU node: {v['mqttd']:,} msg/s, {ratio(v['mqttd'], v['emqx'])} Mosquitto and EMQX, "
               f"{ratio(v['mqttd'], v['hivemq'])} HiveMQ CE"),
        subtitle="highest GREEN rung (p99 ≤ 1 s) · 1:1 QoS 0, 200 B · one Hetzner CCX23, brokers in sequence, "
                 "untuned · 2026-09-16",
        value=lambda r: r["msg_per_s"],
        detail=lambda r: (f"{r['msg_per_s']:,} msg/s", f"p99 {p99[r['series']]} at the knee"),
        sub=lambda r: "durable sessions off" if r["series"] == "mqttd" else
        ("single-threaded" if r["series"] == "mosquitto" else "documented minimum config"),
        footer=["emqtt-bench 0.6.3 (EMQX's load tool), driver-side measurement · ladder steps of 15,000 msg/s · "
                "knees qualified at ≥ 99% delivered, before the zero-loss rule",
                "source: docs/benchmarks/data/headline.csv → docs/benchmarks/SINGLE-NODE-COMPARISON.md"],
        desc="; ".join(f"{r['label']}: {r['msg_per_s']:,} msg/s, p99 {p99[r['series']]}" for r in rows),
    )


CHARTS = {
    "readme-scale-out.svg": draw_scale_out,
    "readme-durable-per-vcpu.svg": draw_per_vcpu,
    "readme-single-node.svg": draw_single_node,
}


def self_test() -> None:
    rows = load()
    svg = draw_scale_out(rows)
    body = svg.split("</desc>")[1]
    scale = [r for r in rows if r["chart"] == "scale-out"]
    green = sum(r["state"] == "green" for r in scale)
    holds = sum(r["state"] == "holds" for r in scale)
    checks = [
        ("every certified bar is filled, plus its legend key", body.count('class="f-good"') == green + 1),
        ("every uncertified bar is an outline, never filled", body.count('data-bar="holds"') == holds),
        ("every YELLOW cap is drawn, plus its legend key",
         body.count('class="f-warn"') == sum(r["state"] == "yellow" for r in scale) + 1),
        ("the subtitle says what build was measured", "measured on 1.2.0" in body and "0a08187" in body),
        ("every bar's total is printed", all(f">{cs.compact(r['msg_per_s'])} total<" in body
                                             for r in scale if r["state"] != "yellow")),
        ("the description names every figure", all(f"{r['msg_per_s']:,}" in svg.split("</desc>")[0] for r in scale)),
    ]
    for name, fn in CHARTS.items():
        out = fn(rows)
        checks.append((f"{name} has a title and a description", "<title" in out and "<desc" in out))
    bad = [n for n, ok in checks if not ok]
    if bad:
        raise SystemExit("chart-readme self-test FAILED: " + "; ".join(bad))
    print(f"chart-readme self-test: {len(checks)} checks OK")


def main() -> None:
    if sys.argv[1:] == ["--self-test"]:
        return self_test()
    out_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else IMG
    rows = load()
    for name, fn in CHARTS.items():
        (out_dir / name).write_text(fn(rows))
        print(f"wrote {out_dir / name}")


if __name__ == "__main__":
    main()
