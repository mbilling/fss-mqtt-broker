#!/usr/bin/env python3
"""Draw the README's evidence charts from docs/benchmarks/data/headline.csv.

    python3 bench/scale/chart-readme.py              # both, into docs/benchmarks/img/
    python3 bench/scale/chart-readme.py --self-test

  readme-scale-out.svg         cluster msg/s against nodes, one panel per workload: QoS 1
                               (durable, clean sessions) above, QoS 0 below; per-node rates labelled
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
    """A rate the way the cards print it: 30.0k, 18.5k, 114k."""
    return f"{v / 1000:.0f}k" if v >= 100_000 else f"{v / 1000:.1f}k"


# --------------------------------------------------------------------------- scale-out

W = 1000
PH = 220                   # plot height
BAR = 26                   # column width
X_NODES = 10.6             # the x axis runs a little past the largest cluster
ROW_T0, ROW_STEP = 192, 352     # first plot top, plot top to the next row's
COLS = [(96, 462), (596, 962)]  # plot left and right, per column

# Two rows: QoS 1 on top (durable, clean sessions), QoS 0 below; the cell beside QoS 0 reads
# how close each line comes to linear.
PANELS = [
    {"series": "durable", "title": "Durable QoS 1",
     "sub": "ack after fsync + a quorum copy · 1 node keeps one copy"},
    {"series": "qos1-clean", "title": "QoS 1, clean sessions", "sub": "routing path, no fsync"},
    {"series": "qos0", "title": "QoS 0", "sub": "at most once"},
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


def node_counts(rows: list[dict]) -> list[int]:
    return sorted({r["nodes"] for r in rows if r["chart"] == "scale-out"})


def y_step(peak: float) -> int:
    return next(st for st in (50_000, 100_000, 200_000, 250_000, 500_000) if peak / st <= 5)


def linearity(b: dict, ref: int, n: int) -> float:
    """The n-node lossless rate as a share of the ref-node rate per node times n."""
    return top_of(b[n])["msg_per_s"] / (top_of(b[ref])["msg_per_s"] / ref * n)


def draw_scale_out(rows: list[dict]) -> str:
    bars = scale_bars(rows)
    sizes = node_counts(rows)
    ref = min(n for n in sizes if n > 1)
    big = sizes[-1]
    builds = sorted({r["build"].split()[-1] for r in rows if r["chart"] == "scale-out"})
    dates = sorted({r["date"] for r in rows if r["chart"] == "scale-out"})
    desc = [describe(bars[p["series"]][n], n) for p in PANELS for n in sizes]
    h = ROW_T0 + 2 * ROW_STEP - 40
    o = cs.open_svg(
        W, h, f"Cluster throughput from {sizes[0]} to {big} nodes, no message lost at any point",
        f"cluster msg/s from the brokers' own counters · 4-vCPU nodes · 3 rounds a step · main {' and '.join(builds)}, "
        f"{' to '.join([dates[0], dates[-1]] if len(dates) > 1 else dates)}",
        f"Columns of the cluster's msg/s at their number of nodes, on a linear axis, {', '.join(map(str, sizes))}, one panel per "
        "workload. The green part is the highest step carried at p99 ≤ 1 s; a yellow cap reaches the maximum "
        "lossless step when it ran at p99 ≤ 5 s, a red cap when it ran above; the dotted line is the "
        f"{ref}-node rate per node times the number of nodes. "
        + "; ".join(desc) + ".")
    o.append(cs.text(32, 84, "p99 is the ack latency for QoS 1 and the end-to-end latency for QoS 0, the mean "
                     "of the rounds", size=13, role="ink2"))
    present = {r["state"] for r in rows if r["chart"] == "scale-out"}
    lx = 32
    for role, label, state in (("good", "GREEN · carried at p99 ≤ 1 s", "green"),
                               ("warn", "YELLOW · lossless, p99 ≤ 5 s", "yellow"),
                               ("crit", "RED · lossless, p99 > 5 s", "red")):
        if state in present:
            o += cs.swatch(lx, 117, role, label)
            lx += cs.legend_width(label) - 8
    o.append(f'<line x1="{lx}" y1="113" x2="{lx + 20}" y2="113" class="k-ink2" stroke-width="1.5" '
             'stroke-dasharray="2 4" stroke-linecap="round"/>')
    o.append(cs.text(lx + 28, 117, f"linear: the {ref}-node rate per node × nodes", size=12, role="ink2"))

    for i, p in enumerate(PANELS):
        x0, x1 = COLS[i % 2]
        yt = ROW_T0 + (i // 2) * ROW_STEP
        yb = yt + PH
        b = bars[p["series"]]
        rate = top_of(b[ref])["msg_per_s"] / ref
        peak = max(max(top_of(b[n])["msg_per_s"] for n in sizes), rate * big)
        step = y_step(peak)
        y_max = int(-(-peak * 1.25 // step) * step)   # room above the tallest column for its figures

        def X(n: float, x0=x0, x1=x1) -> float:
            return x0 + n / X_NODES * (x1 - x0)

        def Y(v: float, yb=yb, y_max=y_max) -> float:
            return yb - v / y_max * PH

        o.append(cs.text(x0 - 64, yt - 34, p["title"], size=14, role="ink", weight=650))
        o.append(cs.text(x0 - 64, yt - 17, p["sub"], size=11, role="muted"))
        for v in range(0, y_max + 1, step):
            o.append(cs.hline(x0, X(big), Y(v), "axis" if v == 0 else "grid"))
            o.append(cs.text(x0 - 8, Y(v) + 4, cs.compact(v) if v else "0", size=11, role="muted",
                             anchor="end", num=True))
        for n in range(0, big + 1):
            measured = n in sizes
            if measured:
                o.append(cs.vline(X(n), yt, yb, "grid"))
            o.append(cs.text(X(n), yb + 16, n, size=12 if measured else 10, role="ink" if measured else "muted",
                             anchor="middle", weight=650 if measured else 400, num=True))
        o.append(cs.text(X(big / 2), yb + 36, "nodes in the cluster", size=11, role="muted", anchor="middle"))
        o.append(f'<line x1="{X(0):.1f}" y1="{Y(0):.1f}" x2="{X(big):.1f}" y2="{Y(rate * big):.1f}" '
                 'class="k-ink2" stroke-width="1.5" stroke-dasharray="2 4" stroke-linecap="round"/>')
        for n in sizes:
            v = top_of(b[n])["msg_per_s"]
            bx = X(n) - BAR / 2
            if b[n]["green"]:
                gy = Y(b[n]["green"]["msg_per_s"])
                o.append(cs.column(bx, gy, yb, BAR, "good", round_top=not b[n]["cap"]))
            else:
                gy = yb
            if b[n]["cap"]:
                base = gy - (2 if b[n]["green"] else 0)
                # A cap is never thinner than 2 px, so a lossless step just above p99 <= 1 s still shows.
                o.append(cs.column(bx, min(Y(v), base - 2), base, BAR, CAP_ROLE[b[n]["cap"]["state"]]))
            lines = [(per_node(v), 13, "ink", 650), (f"{per_node(v / n)}/node", 11, "ink2", 400)]
            if b[n]["cap"] and b[n]["green"]:
                lines.append((f"≤ 1 s: {per_node(b[n]['green']['msg_per_s'])}", 11, "muted", 400))
            ly = Y(v) - 7
            for s_, size, role_, wt in reversed(lines):
                o.append(cs.text(X(n), ly, s_, size=size, role=role_, anchor="middle", weight=wt, num=True,
                                 halo=True))
                ly -= size + 3

    # The free cell: how close each workload comes to linear.
    x0 = COLS[1][0] - 64
    yt = ROW_T0 + ROW_STEP - 34
    o.append(cs.text(x0, yt, f"From {ref} to {big} nodes, against linear", size=14, role="ink", weight=650))
    o.append(cs.text(x0, yt + 17, f"the {big}-node maximum lossless as a share of the {ref}-node rate × {big // 1}",
                     size=11, role="muted"))
    ly = yt + 52
    for p in PANELS:
        share = linearity(bars[p["series"]], ref, big)
        o.append(cs.text(x0, ly, p["title"], size=13, role="ink2"))
        o.append(cs.text(x0 + 300, ly, f"{share:.0%}", size=15, role="ink", weight=650, anchor="end", num=True))
        ly += 30
    o.append(cs.text(x0, ly + 6, "Durable QoS 1 falls furthest: at 10 nodes more of each message's", size=11,
                     role="muted"))
    o.append(cs.text(x0, ly + 21, "replication and forwarding crosses between nodes.", size=11, role="muted"))

    o.append(cs.text(32, h - 36, "Hetzner CCX23 brokers (4 vCPU) · CCX33 load generators, calibrated before the "
                     "ladder · $share, one member per broker, 200 B · 60 s windows", size=11, role="muted"))
    o.append(cs.text(32, h - 18, "source: docs/benchmarks/data/headline.csv → docs/benchmarks/RIG2-RESULTS.md "
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
    # A cluster only: one node keeps one copy, and HiveMQ's run keeps two.
    mq = sorted(((r["msg_per_s"], r["vcpu"], r["nodes"]) for r in rows
                 if r["chart"] == "scale-out" and r["series"] == "durable" and r["state"] == "green"
                 and r["nodes"] > 1))
    hive = next(r for r in rows if r["chart"] == "durable-per-vcpu" and r["series"] == "hivemq")
    return {"mqttd": mq, "hivemq": (hive["msg_per_s"], hive["vcpu"])}


def mqttd_vcpu(pts: list, rate: float) -> float:
    """mqttd's vCPU for a durable rate outside the measured points: below the first, at the
    first's msg/s per vCPU; above the last, along the slope between the last two."""
    (r0, v0, _), (r1, v1, _) = pts[0], pts[-1]
    if rate <= r0:
        return rate * v0 / r0
    (rp, vp, _) = pts[-2]
    return v1 + (rate - r1) * (v1 - vp) / (r1 - rp)


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
            "drawn as a straight line from that one point. mqttd's line is extended dotted: below its first "
            "point at that point's msg/s per vCPU, above its last along the slope between its last two; at "
            f"HiveMQ's published rate it needs about {-(-mqttd_vcpu(pts['mqttd'], hv_rate) // 4):.0f} nodes, "
            f"${month_usd(-(-mqttd_vcpu(pts['mqttd'], hv_rate) // 4) * 4):,.0f} a month. "
            "HiveMQ's clustering needs its commercial "
            "edition, whose price is not published and is not included.")
    o = cs.open_svg(w, h, f"Durable QoS 1 costs {min(ratios):.1f}–{max(ratios):.1f}× less to run than HiveMQ, "
                    "before its licence",
                    f"monthly infrastructure at AWS m6a.8xlarge on-demand prices for both, ${USD_PER_VCPU_H:.4f} "
                    "per vCPU-hour · no licence included", desc)
    lx = 32
    for role, label, kind in (("s1", "mqttd, measured (p99 ≤ 1 s)", "line"),
                              ("s1", "mqttd, extended", "dot"),
                              ("s4", "HiveMQ 4.18, its published run", "dash"),
                              ("s4", "HiveMQ licence (not published)", "box")):
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
    # mqttd beyond what was measured: dotted, back to zero and on past HiveMQ's point.
    mq = pts["mqttd"]
    (r0, v0, _), (r1, v1, _) = mq[0], mq[-1]
    for a_, b_ in ((0, r0), (r1, x_max)):
        o.append(f'<line x1="{X(a_):.1f}" y1="{Y(month_usd(mqttd_vcpu(mq, a_))):.1f}" x2="{X(b_):.1f}" '
                 f'y2="{Y(month_usd(mqttd_vcpu(mq, b_))):.1f}" class="k-s1" stroke-width="2.5" '
                 'stroke-dasharray="0.1 6" stroke-linecap="round"/>')
    # What mqttd would cost at HiveMQ's published rate, in whole 4-vCPU nodes.
    est_nodes = -(-mqttd_vcpu(mq, hv_rate) // 4)
    est = month_usd(est_nodes * 4)
    ex, ey = X(hv_rate), Y(est)
    o.append(cs.vline(ex, hy + 7, ey - 7, "ink2", 1.2, "2 3"))
    o.append(f'<circle cx="{ex:.1f}" cy="{ey:.1f}" r="5" class="f-surface k-s1" stroke-width="2.5"/>')
    o.append(cs.text(ex + 8, (hy + ey) / 2 + 4, f"{month_usd(hv_vcpu) / est:.1f}× less", size=13, role="ink",
                     weight=650, num=True, halo=True))
    o.append(cs.text(ex, ey + 30, f"≈ {est_nodes:.0f} nodes: ${est:,.0f}", size=11, role="ink2", anchor="middle",
                     num=True, halo=True))
    # mqttd: its measured points, joined.
    pp = " ".join(f"{X(r):.1f},{Y(month_usd(v)):.1f}" for r, v, _ in pts["mqttd"])
    o.append(f'<polyline points="{pp}" fill="none" class="k-s1" stroke-width="2.5" stroke-linejoin="round"/>')
    for r, v, n in pts["mqttd"]:
        mx, my, hyv = X(r), Y(month_usd(v)), Y(hive_month(r))
        o.append(cs.vline(mx, hyv + 6, my - 8, "ink2", 1.2, "2 3"))
        o.append(cs.text(mx + 8, (hyv + my) / 2 + 4, f"{hive_month(r) / month_usd(v):.1f}× less",
                         size=13, role="ink", weight=650, num=True, halo=True))
        o.append(f'<circle cx="{mx:.1f}" cy="{my:.1f}" r="5.5" class="f-s1 k-surface" stroke-width="2"/>')
        # The last point's label sits above-left, clear of the extended line to its right.
        last = (r, v, n) == pts["mqttd"][-1]
        o.append(cs.text(mx - 12 if last else mx + 12, my - 10 if last else my + 18,
                         f"{n} nodes: {cs.compact(r)} msg/s, ${month_usd(v):,.0f}", size=11,
                         role="ink2", anchor="end" if last else "start", num=True, halo=True))
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
        ("every class has every cluster size", all(set(v) == set(node_counts(rows)) for v in bars.values())),
        ("every point's lossless total and per-node rate are printed",
         all(f">{per_node(top_of(b)['msg_per_s'])}<" in body and f">{per_node(top_of(b)['msg_per_s'] / n)}/node<" in body
             for v in bars.values() for n, b in v.items())),
        ("the subtitle says what build was measured", all(r["build"].split()[-1] in body for r in scale)),
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
