#!/usr/bin/env python3
"""Draw the README's evidence charts from docs/benchmarks/data/headline.csv.

    python3 bench/scale/chart-readme.py              # both, into docs/benchmarks/img/
    python3 bench/scale/chart-readme.py --self-test

  readme-scale-out.svg         msg/s per node at 3 and 10 nodes: durable QoS 1, QoS 1
                               clean sessions, QoS 0; cluster totals labelled
  readme-durable-per-vcpu.svg  durable QoS 1 per vCPU against HiveMQ's published run

Every figure is a row of headline.csv, and every row names the results card (and the
section of it) the figure is read from; scripts/check-readme-facts.py holds the CSV to
those cards and the README's prose to the CSV.

The figures are the benchmark rig's (ADR 0090): msg/s from the brokers' own counters,
latency on the generator's one clock, 3 rounds a step, a step carried only when every
round lost nothing. A column has up to two rows:
  GREEN          the highest step carried at p99 <= 1 s (the mean of its rounds)
  YELLOW cap     the maximum lossless step, carried at p99 <= 5 s
  RED cap        the maximum lossless step, carried at p99 above 5 s
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
STATES = ("green", "yellow", "red", "published")
CAP_ROLE = {"yellow": "warn", "red": "crit"}


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
    {"title": "Durable QoS 1",
     "x0": 84, "x1": 360, "y_step": 10_000, "backdrop": True,
     "groups": [("durable", "persistent sessions", "ack after fsync + quorum copy")]},
    {"title": "Not durable",
     "x0": 452, "x1": 868, "y_step": 40_000, "backdrop": False,
     "groups": [("qos1-clean", "QoS 1, clean sessions", "routing path, no fsync"),
                ("qos0", "QoS 0", "at most once")]},
]


def scale_bars(rows: list[dict]) -> dict:
    """series -> nodes -> {"green": row or None, "cap": row or None}: the highest step at
    p99 <= 1 s, and the maximum lossless step when it is above that."""
    out: dict = {}
    for r in rows:
        if r["chart"] != "scale-out":
            continue
        slot = out.setdefault(r["series"], {}).setdefault(r["nodes"], {"green": None, "cap": None})
        key = "green" if r["state"] == "green" else "cap"
        if slot[key] is not None:
            raise SystemExit(f"headline.csv: two scale-out rows for {r['series']} at {r['nodes']} nodes "
                             f"({slot[key]['state']} and {r['state']}): one would hide the other")
        slot[key] = r
    return out


def top_of(b: dict) -> dict:
    return b["cap"] or b["green"]


def describe(b: dict, n: int) -> str:
    t = top_of(b)
    s = (f"{t['label']}, {n} nodes: {per_node(t['msg_per_s'] / n)} msg/s per node lossless, "
         f"{t['msg_per_s']:,} msg/s in total ({t['note']})")
    if b["cap"] and b["green"]:
        s += f"; {b['green']['msg_per_s']:,} msg/s at p99 ≤ 1 s ({b['green']['note']})"
    return s


def draw_scale_out(rows: list[dict]) -> str:
    bars = scale_bars(rows)
    builds = sorted({r["build"] for r in rows if r["chart"] == "scale-out"})
    dates = sorted({r["date"] for r in rows if r["chart"] == "scale-out"})
    desc = [describe(bars[series][n], n) for p in PANELS for series, _, _ in p["groups"] for n in (3, 10)]
    o = cs.open_svg(
        W, H, "Throughput per node from 3 to 10 nodes, no message lost on any column",
        f"msg/s per 4-vCPU node from the brokers' own counters · 3 rounds a step · measured on {', '.join(builds)}, "
        f"{' to '.join([dates[0], dates[-1]] if len(dates) > 1 else dates)}",
        "Columns are msg/s per node at 3 and 10 nodes; the cluster total is printed above each column. "
        "The green part is the highest step carried at p99 ≤ 1 s; a yellow cap reaches the maximum lossless step "
        "when it ran at p99 ≤ 5 s, a red cap when it ran above. " + "; ".join(desc) + ".")
    o.append(cs.text(32, 84, "Cluster totals above each column · p99 is the ack latency for QoS 1 and the "
                     "end-to-end latency for QoS 0, the mean of the rounds", size=13, role="ink2"))
    present = {r["state"] for r in rows if r["chart"] == "scale-out"}
    keys = [k for k in (("good", "GREEN · carried at p99 ≤ 1 s", "box", "green"),
                        ("warn", "YELLOW · lossless, p99 ≤ 5 s", "box", "yellow"),
                        ("crit", "RED · lossless, p99 > 5 s", "box", "red")) if k[3] in present]
    keys = [k[:3] for k in keys]
    lx = 32
    for role, label, kind in keys:
        o += cs.swatch(lx, 120, role, label, kind=kind)
        lx += cs.legend_width(label, kind) - 8

    for p in PANELS:
        x0, x1 = p["x0"], p["x1"]
        peak = max(top_of(bars[s][n])["msg_per_s"] / n for s, _, _ in p["groups"] for n in (3, 10))
        step = p["y_step"]
        y_max = int(-(-peak * 1.25 // step) * step)

        def y(v: float, y_max=y_max) -> float:
            return Y0 - v / y_max * PH

        if p["backdrop"]:
            o.append(f'<rect x="{x0 - 64}" y="{T0 - 52}" width="{x1 - x0 + 76}" height="{PH + 136}" '
                     'rx="10" class="f-grid" fill-opacity="0.55"/>')
        o.append(cs.text(x0 - 52, T0 - 30, p["title"], size=13, role="ink", weight=650))
        for v in range(0, y_max + 1, step):
            o.append(cs.hline(x0, x1, y(v), "axis" if v == 0 else "grid"))
            o.append(cs.text(x0 - 8, y(v) + 4, cs.compact(v) if v else "0", size=11, role="muted",
                             anchor="end", num=True))
        o.append(cs.text(x0 - 40, T0 - 12, "msg/s per node", size=10, role="muted"))

        gw = 2 * BAR + IN_GAP
        k = len(p["groups"])
        gap = (x1 - x0 - k * gw) / (k + 1)
        for gi, (series, name, sub) in enumerate(p["groups"]):
            gx = x0 + gap + gi * (gw + gap)
            for bi, n in enumerate((3, 10)):
                b = bars[series][n]
                t = top_of(b)
                bx = gx + bi * (BAR + IN_GAP)
                mid = bx + BAR / 2
                head = y(t["msg_per_s"] / n)
                if b["green"]:
                    gy = y(b["green"]["msg_per_s"] / n)
                    o.append(cs.column(bx, gy, Y0, BAR, "good", round_top=not b["cap"]))
                else:
                    gy = Y0
                if b["cap"]:
                    o.append(cs.column(bx, head, gy - (2 if b["green"] else 0), BAR, CAP_ROLE[b["cap"]["state"]]))
                lines = [(per_node(t["msg_per_s"] / n), 15, "ink", 650),
                         (f"{cs.compact(t['msg_per_s'])} total", 11, "ink2", 400)]
                if b["cap"] and b["green"]:
                    lines.append((f"≤ 1 s: {cs.compact(b['green']['msg_per_s'])}", 11, "ink2", 400))
                ly = head - 8
                for s_, size, role, wt in reversed(lines):
                    o.append(cs.text(mid, ly, s_, size=size, role=role, anchor="middle", weight=wt, num=True))
                    ly -= size + 4
                o.append(cs.text(mid, Y0 + 18, f"{n} nodes", size=12, role="ink", anchor="middle", weight=600))
            gm = gx + gw / 2
            o.append(cs.text(gm, Y0 + 42, name, size=13, role="ink", anchor="middle", weight=650))
            o.append(cs.text(gm, Y0 + 59, sub, size=11, role="muted", anchor="middle"))

    o.append(cs.text(32, H - 36, "Hetzner CCX23 brokers (4 vCPU) · CCX33 load generators, calibrated before the "
                     "ladder · $share, one member per broker, 200 B · 60 s windows", size=11, role="muted"))
    o.append(cs.text(32, H - 18, "source: docs/benchmarks/data/headline.csv → docs/benchmarks/RIG2-RESULTS.md "
                     "(bench/scale/rig2, ADR 0090)", size=11, role="muted"))
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
        keys.append(("s1", "measured by us", "box"))
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
    words = {"green": "carried at ack p99 ≤ 1 s", "published": "vendor-published"}
    me = next(r for r in rows if r["series"] == "mqttd")
    return draw_hbars(
        rows,
        title=f"Durable QoS 1 per vCPU: {ratio(pv['mqttd'], hive)} HiveMQ's published figure",
        subtitle=("3-node clusters, 2 copies of every message · a vCPU is an SMT thread on AMD EPYC Milan "
                  "in every row"),
        value=lambda r: r["msg_per_s"] / r["vcpu"],
        detail=lambda r: (f"{r['msg_per_s'] / r['vcpu']:,.0f} msg/s per vCPU",
                          f"{r['msg_per_s']:,} msg/s on {r['vcpu']} vCPU · {words[r['state']]}"),
        sub=lambda r: {"mqttd": "3 × Hetzner CCX23, 4 vCPU",
                       "hivemq": "3 × AWS m6a.8xlarge, 32 vCPU"}[r["series"]],
        footer=[f"mqttd: {me['build']}, {me['date']}, $share, 200 B, persistent consumers, 3 rounds a step",
                "HiveMQ: its default configuration (persistence on, replica-count 2), published 2023, payload not "
                "published, not reproduced by us",
                "source: docs/benchmarks/data/headline.csv → docs/benchmarks/RIG2-RESULTS.md · "
                "docs/benchmarks/data/per-vcpu.csv (HiveMQ's report)"],
        desc="; ".join(f"{r['label']}: {r['msg_per_s'] / r['vcpu']:,.0f} msg/s per vCPU "
                       f"({r['msg_per_s']:,} msg/s on {r['vcpu']} vCPU, {words[r['state']]})" for r in rows),
    )


# --------------------------------------------------------------------------- cost

# AWS EC2 on-demand, us-east-1, Linux, m6a.8xlarge (32 vCPU, AMD EPYC Milan): the instance
# HiveMQ 4.18's published run used. Both brokers are priced at it, per vCPU, so the gap is
# the software's; Hetzner CCX23 (EUR 0.17225 an hour for 4 vCPU) is EUR 0.0431 a vCPU-hour.
M6A_8XL_USD_H = 1.3824
M6A_8XL_VCPU = 32
MONTH_H = 730
USD_PER_VCPU_H = M6A_8XL_USD_H / M6A_8XL_VCPU


def month_usd(vcpu: float) -> float:
    return vcpu * USD_PER_VCPU_H * MONTH_H


def cost_points(rows: list[dict]) -> dict:
    """mqttd's measured durable QoS 1 points (the highest step at ack p99 <= 1 s, from
    the scale-out rows) and HiveMQ's one published point."""
    mq = sorted(((r["msg_per_s"], r["vcpu"], r["nodes"]) for r in rows
                 if r["chart"] == "scale-out" and r["series"] == "durable" and r["state"] == "green"))
    hive = next(r for r in rows if r["chart"] == "durable-per-vcpu" and r["series"] == "hivemq")
    return {"mqttd": mq, "hivemq": (hive["msg_per_s"], hive["vcpu"])}


def draw_cost(rows: list[dict]) -> str:
    pts = cost_points(rows)
    hv_rate, hv_vcpu = pts["hivemq"]
    hv_per_vcpu = hv_rate / hv_vcpu

    def hive_month(rate: float) -> float:
        return month_usd(rate / hv_per_vcpu)

    w, h = 900, 560
    x0, x1, y0, y1 = 96, 860, 470, 150      # plot box: left, right, baseline, top
    x_max, y_max = 300_000, 4_000

    def X(v: float) -> float:
        return x0 + v / x_max * (x1 - x0)

    def Y(v: float) -> float:
        return y0 - v / y_max * (y0 - y1)

    ratios = [hive_month(r) / month_usd(v) for r, v, _ in pts["mqttd"]]
    desc = ("Monthly infrastructure cost against durable QoS 1 throughput, both priced at AWS m6a.8xlarge "
            f"on-demand (${USD_PER_VCPU_H:.4f} per vCPU-hour, {MONTH_H} hours a month), no licence. "
            + "; ".join(f"mqttd, {n} nodes: {r:,} msg/s on {v} vCPU, ${month_usd(v):,.0f} a month; HiveMQ "
                        f"4.18 at the same rate ${hive_month(r):,.0f} ({hive_month(r) / month_usd(v):.1f}×)"
                        for r, v, n in pts["mqttd"])
            + f"; HiveMQ 4.18 published: {hv_rate:,} msg/s on {hv_vcpu} vCPU, ${month_usd(hv_vcpu):,.0f} a month, "
            "drawn as a straight line from that one point. HiveMQ's clustering needs its commercial "
            "edition, whose price is not published and is not included.")
    o = cs.open_svg(w, h, f"Durable QoS 1 costs {min(ratios):.1f}–{max(ratios):.1f}× less to run than HiveMQ, "
                    "before its licence",
                    f"monthly infrastructure at AWS m6a.8xlarge on-demand prices for both, ${USD_PER_VCPU_H:.4f} "
                    "per vCPU-hour · no licence included", desc)
    lx = 32
    for role, label, kind in (("s1", "mqttd, measured (ack p99 ≤ 1 s)", "line"),
                              ("s4", "HiveMQ 4.18, from its published run", "dash"),
                              ("s4", "HiveMQ licence on top (not published)", "box")):
        o += cs.swatch(lx, 100, role, label, kind=kind)
        lx += cs.legend_width(label, kind)
    # The licence: everything above HiveMQ's infrastructure line, unknown but not zero.
    top_hive = hive_month(x_max)
    o.append(f'<path d="M{X(0):.1f},{Y(0):.1f} L{X(x_max):.1f},{Y(min(top_hive, y_max)):.1f} '
             f'L{X(x_max):.1f},{Y(y_max):.1f} L{X(0):.1f},{Y(y_max):.1f} Z" class="f-s4" fill-opacity="0.07"/>')
    for v in range(0, y_max + 1, 1000):
        o.append(cs.hline(x0, x1, Y(v), "axis" if v == 0 else "grid"))
        o.append(cs.text(x0 - 10, Y(v) + 4, f"${v:,}" if v else "$0", size=11, role="muted", anchor="end", num=True))
    for v in range(0, x_max + 1, 50_000):
        o.append(cs.text(X(v), y0 + 18, cs.compact(v) if v else "0", size=11, role="muted", anchor="middle", num=True))
    o.append(cs.text((x0 + x1) / 2, y0 + 40, "durable QoS 1 msg/s carried", size=12, role="ink2", anchor="middle"))
    o.append(cs.text(x0 - 64, y1 - 18, "per month", size=11, role="muted"))
    o.append(cs.text(X(20_000), Y(3_700), "HiveMQ's clustering needs its commercial edition: that licence comes on "
                     "top of its line, at a price not published", size=12, role="ink2"))
    # HiveMQ: a straight line through its one published point.
    o.append(f'<line x1="{X(0):.1f}" y1="{Y(0):.1f}" x2="{X(x_max):.1f}" y2="{Y(top_hive):.1f}" class="k-s4" '
             'stroke-width="2" stroke-dasharray="6 4"/>')
    hx, hy = X(hv_rate), Y(month_usd(hv_vcpu))
    o.append(f'<circle cx="{hx:.1f}" cy="{hy:.1f}" r="5" class="f-surface k-s4" stroke-width="2.5"/>')
    o.append(cs.text(hx - 10, hy - 12, f"published: {cs.compact(hv_rate)} msg/s on 3 × m6a.8xlarge, "
                     f"${month_usd(hv_vcpu):,.0f}", size=11, role="ink2", anchor="end", num=True, halo=True))
    # mqttd: its measured points, joined.
    pp = " ".join(f"{X(r):.1f},{Y(month_usd(v)):.1f}" for r, v, _ in pts["mqttd"])
    o.append(f'<polyline points="{pp}" fill="none" class="k-s1" stroke-width="2.5" stroke-linejoin="round"/>')
    for r, v, n in pts["mqttd"]:
        mx, my, hyv = X(r), Y(month_usd(v)), Y(hive_month(r))
        o.append(cs.vline(mx, hyv + 6, my - 8, "ink2", 1.2, "2 3"))
        o.append(cs.text(mx + 8, (hyv + my) / 2 + 4, f"{hive_month(r) / month_usd(v):.1f}× less",
                         size=13, role="ink", weight=650, num=True, halo=True))
        o.append(f'<circle cx="{mx:.1f}" cy="{my:.1f}" r="5.5" class="f-s1 k-surface" stroke-width="2"/>')
        o.append(cs.text(mx + 12, my + 18, f"{n} nodes: {cs.compact(r)} msg/s, ${month_usd(v):,.0f}", size=11,
                         role="ink2", anchor="start", num=True, halo=True))
    o.append(cs.text(32, h - 36, "mqttd: Hetzner CCX23 brokers (4 vCPU, EPYC Milan), the 1.3.0 candidate, 2 copies · the same "
                     "per-vCPU price as Hetzner's own (EUR 0.0431) · load generators not counted", size=11, role="muted"))
    o.append(cs.text(32, h - 18, "HiveMQ: its published 4.18 run, default configuration (persistent, 2 copies), not "
                     "reproduced, payload not published · source: docs/benchmarks/data/headline.csv", size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


CHARTS = {
    "readme-scale-out.svg": draw_scale_out,
    "readme-durable-per-vcpu.svg": draw_per_vcpu,
    "readme-durable-cost.svg": draw_cost,
}


def self_test() -> None:
    rows = load()
    svg = draw_scale_out(rows)
    body = svg.split("</desc>")[1]
    head = svg.split("</desc>")[0]
    scale = [r for r in rows if r["chart"] == "scale-out"]
    bars = scale_bars(rows)
    checks = [
        ("every GREEN column is drawn, plus its legend key",
         body.count('class="f-good"') == sum(r["state"] == "green" for r in scale) + 1),
        ("every YELLOW cap is drawn, plus its legend key when there is one",
         body.count('class="f-warn"') == sum(r["state"] == "yellow" for r in scale)
         + (1 if any(r["state"] == "yellow" for r in scale) else 0)),
        ("every RED cap is drawn, plus its legend key when there is one",
         body.count('class="f-crit"') == sum(r["state"] == "red" for r in scale)
         + (1 if any(r["state"] == "red" for r in scale) else 0)),
        ("every column has both node counts", all(set(v) == {3, 10} for v in bars.values())),
        ("every column's lossless total is printed",
         all(f">{cs.compact(top_of(b)['msg_per_s'])} total<" in body for v in bars.values() for b in v.values())),
        ("the subtitle says what build was measured", all(r["build"] in body for r in scale)),
        ("the description names every figure", all(f"{r['msg_per_s']:,}" in head for r in scale)),
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
