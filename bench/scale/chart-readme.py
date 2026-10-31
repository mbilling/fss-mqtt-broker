#!/usr/bin/env python3
"""Draw the README's evidence charts from docs/benchmarks/data/headline.csv.

    python3 bench/scale/chart-readme.py              # all three, into docs/benchmarks/img/
    python3 bench/scale/chart-readme.py --self-test

  readme-scale-out.svg         cluster msg/s against the cluster's vCPU, one panel per workload:
                               durable QoS 1 across the top, with HiveMQ's published point beside
                               mqttd's at 96 vCPU; QoS 1 with clean sessions and QoS 0 below
  readme-durable-per-vcpu.svg  durable QoS 1 per vCPU against HiveMQ's published run, by node size
  readme-durable-cost.svg      what the hardware behind a million msg/s of durable QoS 1 costs a
                               month: mqttd's measured range across node sizes against HiveMQ's
                               published run, both at one price per vCPU

Every figure is a row of headline.csv, and every row names the results card (and the
section of it) the figure is read from; scripts/check-readme-facts.py holds the CSV to
those cards and the README's prose to the CSV, and takes the figures the charts derive
(ratios, costs, rates per vCPU) from derived_figures() here, so the prose cannot drift
from what the charts draw.

The figures are the benchmark rig's (ADR 0090): msg/s from the brokers' own counters,
latency on the generator's one clock, 3 rounds a step, a step carried only when every
round lost nothing. The scale-out chart has one column per vCPU count: where two cluster
shapes were measured at one count, the CSV keeps the higher and the card has both. A
column has up to two parts:
  GREEN          the highest step carried at p99 <= 1 s (the mean of its rounds)
  YELLOW cap     the maximum lossless step, carried at p99 <= 5 s
  RED cap        the maximum lossless step, carried at p99 above 5 s
A dashed outline is a vendor's published figure, not reproduced here. A figure marked >=
is a floor: the ladder ended above it without bracketing the knee.
"""
from __future__ import annotations

import csv
import re
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
        if r["vcpu"] % r["nodes"]:
            raise SystemExit(f"headline.csv: {r['label']!r} has {r['vcpu']} vCPU on {r['nodes']} nodes, not a whole "
                             "number each")
    return rows


def per_node(v: float) -> str:
    """A rate the way the cards print it: 30.0k, 18.5k, 114k."""
    return f"{v / 1000:.0f}k" if v >= 100_000 else f"{v / 1000:.1f}k"


def each(r: dict) -> int:
    """vCPU per node."""
    return r["vcpu"] // r["nodes"]


def shape(r: dict) -> str:
    """A cluster as nodes × vCPU each: 10 × 4, 6 × 16."""
    return f"{r['nodes']} × {each(r)}"


def ratio_num(x: float) -> str:
    """1.25, 1.2, 2.6, 3.5: two decimals below 2× so 1.25 does not round to 1.2."""
    s = f"{x:.2f}" if x < 2 else f"{x:.1f}"
    if "." in s and len(s.split(".")[1]) == 2 and s.endswith("0"):
        s = s[:-1]
    return s


def ratio_text(x: float) -> str:
    return ratio_num(x) + "×"


def p99_phrase(r: dict) -> str:
    """The latency clause of a row's note: 'ack p99 1.35 s', or '' when it has none."""
    m = re.search(r"(?:ack|end-to-end) p99 [^;]+", r["note"])
    return m.group(0).strip() if m else ""


# --------------------------------------------------------------------------- price basis

# AWS EC2 on-demand, us-east-1, Linux, m6a.8xlarge (32 vCPU, AMD EPYC Milan): the instance
# HiveMQ 4.18's published run used. Both brokers are priced at it, per vCPU, so the gap is
# the software's; Hetzner CCX23 (EUR 0.17225 an hour for 4 vCPU) is EUR 0.0431 a vCPU-hour.
M6A_8XL_USD_H = 1.3824
M6A_8XL_VCPU = 32
MONTH_H = 730
USD_PER_VCPU_H = M6A_8XL_USD_H / M6A_8XL_VCPU


def month_usd(vcpu: float) -> float:
    return vcpu * USD_PER_VCPU_H * MONTH_H


def usd_per_million(vcpu: float, rate: float) -> float:
    """What the hardware behind 1M msg/s costs a month: the cluster's month over its rate."""
    return month_usd(vcpu) * 1_000_000 / rate


def usd_per_million_text(r: dict) -> str:
    """$3,212; ≤ $8,970 for a floor, whose true rate is higher and so its cost lower."""
    return ("≤ " if r["floor"] else "") + f"${usd_per_million(r['vcpu'], r['msg_per_s']):,.0f}"


# --------------------------------------------------------------------------- scale-out

W = 1000
PH = 220                   # plot height
BAR = 26                   # column width
ROW_T0, ROW_STEP = 192, 352     # first plot top, plot top to the next row's
FULL = (96, 962)                # plot left and right across the card
COLS = [(96, 462), (596, 962)]  # the same, per column of a two-panel row

# Durable QoS 1 takes the top row by itself: it has the most cluster shapes (and HiveMQ's
# point); QoS 1 with clean sessions and QoS 0 share the row below.
PANELS = [
    {"series": "durable", "title": "Durable QoS 1", "box": FULL, "row": 0,
     "sub": "ack after fsync + a quorum copy · 1 node keeps one copy"},
    {"series": "qos1-clean", "title": "QoS 1, clean sessions", "box": COLS[0], "row": 1,
     "sub": "routing path, no fsync"},
    {"series": "qos0", "title": "QoS 0", "box": COLS[1], "row": 1, "sub": "at most once"},
]


def scale_bars(rows: list[dict]) -> dict:
    """series -> vcpu -> {"green", "cap", "published"}: the highest step at p99 <= 1 s, the
    maximum lossless step when it is above that, and a vendor's published figure."""
    out: dict = {}
    for r in rows:
        if r["chart"] != "scale-out":
            continue
        slot = out.setdefault(r["series"], {}).setdefault(r["vcpu"], {"green": None, "cap": None, "published": None})
        key = "green" if r["state"] == "green" else "published" if r["state"] == "published" else "cap"
        if slot[key] is not None:
            raise SystemExit(f"headline.csv: two scale-out rows for {r['series']} at {r['vcpu']} vCPU "
                             f"({slot[key]['state']} and {r['state']}): one would hide the other; keep the higher")
        slot[key] = r
    return out


def measured(b: dict) -> bool:
    return bool(b["green"] or b["cap"])


def top_of(b: dict) -> dict:
    return b["cap"] or b["green"]


def describe(b: dict) -> str:
    t = top_of(b)
    s = (f"{t['label']}, {shape(t)} vCPU ({t['host']}): {per_node(t['msg_per_s'] / t['vcpu'])} msg/s per vCPU, "
         f"{t['msg_per_s']:,} msg/s in total with no message lost ({t['note']}), "
         f"{usd_per_million_text(t)} of hardware a month per 1M msg/s")
    if b["cap"] and b["green"]:
        s += f"; {b['green']['msg_per_s']:,} msg/s at p99 ≤ 1 s ({b['green']['note']})"
    if t["floor"]:
        s += "; a floor: the ladder ended above it without bracketing the knee"
    if b["published"]:
        p = b["published"]
        s += (f"; {p['label']}, {shape(p)} vCPU ({p['host']}): {p['msg_per_s']:,} msg/s, "
              f"{per_node(p['msg_per_s'] / p['vcpu'])} per vCPU, published, not reproduced")
    return s


def y_step(peak: float) -> int:
    return next(st for st in (50_000, 100_000, 200_000, 250_000, 500_000) if peak / st <= 5)


def ghost_column(x: float, y_top: float, y_base: float, w: float) -> str:
    """A vendor's published figure: a dashed outline, never a fill."""
    return (f'<rect x="{x + 0.75:.1f}" y="{y_top + 0.75:.1f}" width="{w - 1.5:.1f}" '
            f'height="{max(y_base - y_top - 1.5, 1):.1f}" rx="4" fill="none" class="k-ghost" stroke-width="1.5" '
            'stroke-dasharray="4 3"/>')


def draw_panel(o: list[str], p: dict, b: dict) -> None:
    x0, x1 = p["box"]
    yt = ROW_T0 + p["row"] * ROW_STEP
    yb = yt + PH
    sizes = sorted(v for v in b if measured(b[v]))
    big = sizes[-1]
    x_max = big * 1.08       # the x axis runs a little past the largest cluster
    peak = max([top_of(b[v])["msg_per_s"] for v in sizes]
               + [b[v]["published"]["msg_per_s"] for v in b if b[v]["published"]])
    step = y_step(peak)
    y_max = int(-(-peak * 1.25 // step) * step)   # room above the tallest column for its figures

    def X(v: float) -> float:
        return x0 + v / x_max * (x1 - x0)

    def Y(v: float) -> float:
        return yb - v / y_max * PH

    o.append(cs.text(x0 - 64, yt - 34, p["title"], size=14, role="ink", weight=650))
    o.append(cs.text(x0 - 64, yt - 17, p["sub"], size=11, role="muted"))
    for v in range(0, y_max + 1, step):
        o.append(cs.hline(x0, x1, Y(v), "axis" if v == 0 else "grid"))
        o.append(cs.text(x0 - 8, Y(v) + 4, cs.compact(v) if v else "0", size=11, role="muted", anchor="end", num=True))
    # Regular ticks, except where a measured one would overprint them.
    tick = 20 if x_max > 60 else 10
    for v in range(0, int(x_max) + 1, tick):
        if v in sizes or any(abs(X(v) - X(m)) < 26 for m in sizes):
            continue
        o.append(cs.text(X(v), yb + 16, v, size=10, role="muted", anchor="middle", num=True))
    # Under each measured tick: the cluster's shape, then what its hardware costs a month per
    # 1M msg/s at the maximum lossless step (the price basis is in the footer).
    for v in sizes:
        t = top_of(b[v])
        o.append(cs.vline(X(v), yt, yb, "grid"))
        o.append(cs.text(X(v), yb + 16, v, size=12, role="ink", anchor="middle", weight=650, num=True))
        o.append(cs.text(X(v), yb + 30, shape(t), size=10, role="muted", anchor="middle", num=True))
        o.append(cs.text(X(v), yb + 45, usd_per_million_text(t), size=10, role="ink2", anchor="middle", num=True))
    o.append(cs.text((x0 + x1) / 2, yb + 63, "vCPU in the cluster · nodes × vCPU each · $ a month per 1M msg/s",
                     size=11, role="muted", anchor="middle"))
    for v in sizes:
        s = b[v]
        t = top_of(s)
        total = t["msg_per_s"]
        # Beside a published figure, mqttd's column stands to the right of the pair's centre
        # and the dashed one to the left, 2 px apart.
        cx = X(v) + (BAR / 2 + 1 if s["published"] else 0)
        bx = cx - BAR / 2
        if s["green"]:
            gy = Y(s["green"]["msg_per_s"])
            o.append(cs.column(bx, gy, yb, BAR, "good", round_top=not s["cap"]))
        else:
            gy = yb
        if s["cap"]:
            base = gy - (2 if s["green"] else 0)
            # A cap is never thinner than 2 px, so a lossless step just above p99 <= 1 s still shows.
            o.append(cs.column(bx, min(Y(total), base - 2), base, BAR, CAP_ROLE[s["cap"]["state"]]))
        lines = [(("≥ " if t["floor"] else "") + per_node(total), 13, "ink", 650),
                 (f"{per_node(total / v)}/vCPU", 11, "ink2", 400)]
        if s["cap"] and s["green"]:
            lines.append((f"≤ 1 s: {per_node(s['green']['msg_per_s'])}", 11, "muted", 400))
        ly = Y(total) - 7
        for s_, size, role_, wt in reversed(lines):
            o.append(cs.text(cx, ly, s_, size=size, role=role_, anchor="middle", weight=wt, num=True, halo=True))
            ly -= size + 3
        if s["published"]:
            pub = s["published"]
            pv = pub["msg_per_s"]
            px = X(v) - BAR - 1
            o.append(ghost_column(px, Y(pv), yb, BAR))
            ly = Y(pv) + 12
            for s_, size, role_, wt in ((pub["label"].replace(" (published)", ""), 12, "ink", 600),
                                        (f"{per_node(pv)} published", 11, "ink2", 400),
                                        (f"{shape(pub)} vCPU · {per_node(pv / v)}/vCPU", 11, "muted", 400),
                                        (f"{usd_per_million_text(pub)} a month per 1M msg/s", 11, "muted", 400)):
                o.append(cs.text(px - 8, ly, s_, size=size, role=role_, anchor="end", weight=wt, num=True, halo=True))
                ly += size + 3


def draw_scale_out(rows: list[dict]) -> str:
    bars = scale_bars(rows)
    scale = [r for r in rows if r["chart"] == "scale-out"]
    mine = [r for r in scale if r["state"] != "published"]
    builds = [b for _, b in sorted({(min(r["date"] for r in mine if r["build"] == b), b.split()[-1])
                                    for b in {r["build"] for r in mine}})]
    dates = sorted({r["date"] for r in mine})
    lo, hi = min(r["vcpu"] for r in mine), max(r["vcpu"] for r in mine)
    hosts = sorted({(each(r), r["host"]) for r in mine})
    present = {r["state"] for r in scale}
    floors = [r for r in mine if r["floor"]]
    desc = [describe(bars[p["series"]][v]) for p in PANELS for v in sorted(bars[p["series"]])
            if measured(bars[p["series"]][v])]
    named = [f"{host} ({n} vCPU)" for n, host in hosts]
    footer = [f"brokers: {', '.join(named[:-1])} and {named[-1]} · load generators calibrated before the ladder · "
              "$share, 200 B · 60 s windows",
              f"$ a month per 1M msg/s: hardware only, priced at AWS m6a.8xlarge's on-demand rate, "
              f"${USD_PER_VCPU_H:.4f} a vCPU-hour × {MONTH_H} h (Hetzner's is about the same) · no licence"]
    if floors:
        footer.append("≥ a floor, ≤ its cost: the next step lost messages and the gap was not bisected, so the knee "
                      "lies above the figure")
    footer.append("source: docs/benchmarks/data/headline.csv → docs/benchmarks/RIG2-RESULTS.md (bench/scale/rig2, "
                  "ADR 0090)")
    fy = ROW_T0 + 2 * ROW_STEP - 42
    h = fy + 18 * len(footer)
    o = cs.open_svg(
        W, h, f"Cluster throughput from {lo} to {hi} vCPU, no message lost at any point",
        f"cluster msg/s from the brokers' own counters · 3 rounds a step · main {', '.join(builds[:-1])} and "
        f"{builds[-1]}, {' to '.join([dates[0], dates[-1]] if len(dates) > 1 else dates)}",
        "Columns of the cluster's msg/s at its number of vCPU, on a linear axis, one panel per workload; under "
        "each column its shape as nodes × vCPU each and what its hardware costs a month per 1M msg/s, every host "
        f"priced at AWS m6a.8xlarge's on-demand rate (${USD_PER_VCPU_H:.4f} a vCPU-hour, {MONTH_H} hours a month), "
        "no licence. The green part is the highest step carried at p99 ≤ 1 s; a yellow cap reaches the maximum "
        "lossless step when it ran at p99 ≤ 5 s, a red cap when it ran above; a dashed outline is a vendor's "
        "published figure; ≥ marks a floor, whose cost is then at most the one printed. "
        + "; ".join(desc) + ".")
    o.append(cs.text(32, 84, "p99 is the ack latency for QoS 1 and the end-to-end latency for QoS 0, the mean "
                     "of the rounds", size=13, role="ink2"))
    lx = 32
    for role, label, state, kind in (("good", "GREEN · carried at p99 ≤ 1 s", "green", "box"),
                                     ("warn", "YELLOW · lossless, p99 ≤ 5 s", "yellow", "box"),
                                     ("crit", "RED · lossless, p99 > 5 s", "red", "box"),
                                     ("ghost", "vendor-published, not reproduced", "published", "ghost")):
        if state in present:
            o += cs.swatch(lx, 117, role, label, kind=kind)
            lx += cs.legend_width(label, kind) - 8
    for p in PANELS:
        draw_panel(o, p, bars[p["series"]])
    for line in footer:
        o.append(cs.text(32, fy, line, size=11, role="muted"))
        fy += 18
    o.append("</svg>")
    return "\n".join(o) + "\n"


# --------------------------------------------------------------------------- horizontal bars

HL, HBAR_W, HROW, HBAR_H = 270, 360, 54, 22


def draw_hbars(rows: list[dict], *, title: str, subtitle: str, value, detail, sub, footer: list[str],
               desc: str) -> str:
    states = {r["state"] for r in rows}
    ours = states - {"published"}
    legend = bool(ours) and "published" in states
    h = (150 if legend else 120) + len(rows) * HROW + 18 * len(footer) + 12
    o = cs.open_svg(W, h, title, subtitle, desc)
    if legend:
        lx = 32
        for role, label, kind in (("s1", "measured by us, no message lost", "box"),
                                  ("ghost", "vendor-published, not reproduced", "ghost")):
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
        if r["state"] == "published":
            o.append(f'<rect x="{HL + 0.75}" y="{yb + 0.75:.1f}" width="{max(w - 1.5, 1):.1f}" '
                     f'height="{HBAR_H - 1.5}" rx="4" fill="none" class="k-{role}" stroke-width="1.5" '
                     'stroke-dasharray="4 3"/>')
        else:
            o.append(f'<rect x="{HL}" y="{yb:.1f}" width="{w:.1f}" height="{HBAR_H}" rx="4" class="f-{role}"/>')
        o.append(cs.text(HL + w + 10, yb + 11, detail(r)[0], size=14, role="ink", weight=650, num=True))
        o.append(cs.text(HL + w + 10, yb + 26, detail(r)[1], size=11, role="ink2", num=True))
        y += HROW
    fy = h - 18 * len(footer)
    for line in footer:
        o.append(cs.text(32, fy, line, size=11, role="muted"))
        fy += 18
    o.append("</svg>")
    return "\n".join(o) + "\n"


def per_vcpu_rows(rows: list[dict]) -> tuple[list[dict], dict]:
    """The per-vCPU chart's rows in their order, and HiveMQ's published row."""
    rows = [r for r in rows if r["chart"] == "durable-per-vcpu"]
    hive = next(r for r in rows if r["series"] == "hivemq")
    return rows, hive


def draw_per_vcpu(rows: list[dict]) -> str:
    rows, hive = per_vcpu_rows(rows)
    hv = hive["msg_per_s"] / hive["vcpu"]
    ratios = [r["msg_per_s"] / r["vcpu"] / hv for r in rows if r["series"] == "mqttd"]

    def words(r: dict) -> str:
        if r["state"] == "published":
            return "published, not reproduced"
        return "no message lost, " + p99_phrase(r)

    def sub(r: dict) -> str:
        return f"{r['nodes']} × {r['host']}, {each(r)} vCPU each"

    builds = ", ".join(sorted({r["build"] for r in rows if r["series"] == "mqttd"}))
    return draw_hbars(
        rows,
        title=f"Durable QoS 1 per vCPU: {ratio_num(min(ratios))}–{ratio_num(max(ratios))}× HiveMQ's published "
              "figure, by node size",
        subtitle=("clusters with 2 copies of every message · a vCPU is an SMT thread in every row: AMD EPYC Milan "
                  "on CCX23 and m6a, Intel Ice Lake on m6i"),
        value=lambda r: r["msg_per_s"] / r["vcpu"],
        detail=lambda r: (f"{r['msg_per_s'] / r['vcpu']:,.0f} msg/s per vCPU"
                          + ("" if r["series"] == "hivemq" else f" · {ratio_text(r['msg_per_s'] / r['vcpu'] / hv)}"),
                          f"{r['msg_per_s']:,} msg/s on {r['vcpu']} vCPU · {words(r)}"),
        sub=sub,
        footer=[f"mqttd: {builds} · the maximum lossless step of each run · $share, one member per broker, "
                "200 B, persistent consumers, 3 rounds a step",
                "HiveMQ: its default configuration (persistence on, replica-count 2), published 2023, payload not "
                "published, not reproduced by us",
                "source: docs/benchmarks/data/headline.csv → docs/benchmarks/RIG2-RESULTS.md · "
                "docs/benchmarks/data/per-vcpu.csv (HiveMQ's report)"],
        desc="; ".join(f"{r['label']}, {sub(r)}, {r['build']}: {r['msg_per_s'] / r['vcpu']:,.0f} msg/s per vCPU "
                       f"({r['msg_per_s']:,} msg/s on {r['vcpu']} vCPU, {words(r)})" for r in rows),
    )


# --------------------------------------------------------------------------- cost


def cost_points(rows: list[dict]) -> dict:
    """mqttd's measured durable QoS 1 clusters (the maximum lossless step at every vCPU
    count, from the scale-out rows) and HiveMQ's one published point."""
    # A cluster only: one node keeps one copy, and HiveMQ's run keeps two.
    mq = sorted((top_of(b)["msg_per_s"], v, top_of(b)) for v, b in scale_bars(rows)["durable"].items()
                if measured(b) and top_of(b)["nodes"] > 1)
    hive = next(r for r in rows if r["chart"] == "durable-per-vcpu" and r["series"] == "hivemq")
    return {"mqttd": mq, "hivemq": (hive["msg_per_s"], hive["vcpu"])}


def hive_month(pts: dict, rate: float) -> float:
    """What HiveMQ's published rate per vCPU costs a month at a rate."""
    hv_rate, hv_vcpu = pts["hivemq"]
    return month_usd(rate / (hv_rate / hv_vcpu))


def cost_ratios(rows: list[dict]) -> list[tuple[dict, float]]:
    """HiveMQ's monthly cost at each mqttd point's rate over mqttd's: '3.5× less'."""
    pts = cost_points(rows)
    return [(r, hive_month(pts, rate) / month_usd(v)) for rate, v, r in pts["mqttd"]]


def same_money(pts: dict):
    """mqttd's measured cluster at HiveMQ's vCPU count (rate, vcpu, row), or None."""
    _, hv_vcpu = pts["hivemq"]
    return next(((rate, v, r) for rate, v, r in pts["mqttd"] if v == hv_vcpu), None)


def same_traffic(pts: dict):
    """The cheapest measured cluster that carries at least HiveMQ's published rate, or None."""
    hv_rate, _ = pts["hivemq"]
    fits = sorted((month_usd(v), rate, v, r) for rate, v, r in pts["mqttd"] if rate >= hv_rate)
    return fits[0][1:] if fits else None


def draw_cost(rows: list[dict]) -> str:
    """Two bars: what the hardware behind a million msg/s costs a month, mqttd's measured
    clusters as a range across node sizes, HiveMQ's published run as a dashed outline."""
    pts = cost_points(rows)
    hv_rate, hv_vcpu = pts["hivemq"]
    hive = next(r for r in rows if r["chart"] == "durable-per-vcpu" and r["series"] == "hivemq")
    mine = sorted(pts["mqttd"], key=lambda p: usd_per_million(p[1], p[0]))   # cheapest shape first
    costs = [usd_per_million(v, rate) for rate, v, _ in mine]
    hv_cost = usd_per_million(hv_vcpu, hv_rate)
    ratios = [hv_cost / c for c in costs]
    lo, hi = min(costs), max(costs)
    rng = f"{ratio_num(min(ratios))}–{ratio_num(max(ratios))}×"
    shape_hive = next((r for r in rows if r["chart"] == "durable-per-vcpu" and r["series"] == "mqttd"
                       and r["vcpu"] == hv_vcpu and r["nodes"] == hive["nodes"]), None)
    money, traffic = same_money(pts), same_traffic(pts)
    w, h = W, 466
    bx, bw = 250, 480                       # bars start here; bw is the width of the axis
    x_max = -(-max(hv_cost, hi) * 1.02 // 1000) * 1000

    def X(v: float) -> float:
        return bx + v / x_max * bw

    shapes = " · ".join(f"{shape(r)} vCPU ${c:,.0f}" for (rate, v, r), c in zip(mine, costs))
    hive_words = (f"{hv_rate:,} msg/s on {shape(hive)} vCPU, ${month_usd(hv_vcpu):,.0f} a month · published 2023, "
                  "default configuration (persistent, 2 copies), payload not published")
    words = {2: "two", 3: "three", 4: "four", 5: "five", 6: "six", 7: "seven", 8: "eight", 9: "nine", 10: "ten"}

    def nodes_of(r: dict) -> str:
        return f"{words.get(r['nodes'], r['nodes'])} {each(r)}-vCPU nodes"

    # Two like-for-like comparisons as a small table: what is held fixed, mqttd's figure,
    # HiveMQ's, the difference. Each cell is (figure, what it is).
    table = []
    if money:
        rate, v, r = money
        table.append((f"At HiveMQ's spend, ${month_usd(v):,.0f} a month ({v} vCPU)",
                      (f"{rate:,} msg/s", nodes_of(r)), (f"{hv_rate:,} msg/s", f"{shape(hive)} vCPU"),
                      f"mqttd {rate / hv_rate - 1:.0%} more"))
    if traffic:
        rate, v, r = traffic
        table.append((f"For HiveMQ's traffic, {hv_rate:,} msg/s",
                      (f"${month_usd(v):,.0f} a month", f"{nodes_of(r)}, carrying {rate:,}"),
                      (f"${month_usd(hv_vcpu):,.0f} a month", f"{shape(hive)} vCPU"),
                      f"mqttd {ratio_text(month_usd(hv_vcpu) / month_usd(v))} less"))
    sentences = [f"{fixed}: mqttd {a[0]} ({a[1]}), HiveMQ {b[0]} ({b[1]}), {diff}." for fixed, a, b, diff in table]
    desc = (f"Two horizontal bars of what the hardware behind a million msg/s of durable QoS 1 costs a month, both "
            f"brokers priced at AWS m6a.8xlarge on-demand (${USD_PER_VCPU_H:.4f} per vCPU-hour, {MONTH_H} hours a "
            f"month), no licence. mqttd, measured, as a range across node sizes: {shapes} (the maximum lossless step "
            f"of each run), {rng} less than HiveMQ"
            + (f"; on HiveMQ's own shape, {shape(shape_hive)} vCPU, ${usd_per_million(shape_hive['vcpu'], shape_hive['msg_per_s']):,.0f}, level"
               if shape_hive else "")
            + f". HiveMQ 4.18, published: ${hv_cost:,.0f} ({hive_words}). " + " ".join(sentences))
    o = cs.open_svg(w, h, f"Durable QoS 1: the hardware behind a million msg/s costs {rng} less than HiveMQ's",
                    f"$ a month of hardware per million msg/s · both priced per vCPU at AWS m6a.8xlarge on-demand, "
                    f"${USD_PER_VCPU_H:.4f} a vCPU-hour · 2 copies · no licence", desc)
    lx = 32
    for role, label, kind in (("s1", "mqttd, measured, no message lost", "box"),
                              ("ghost", "HiveMQ, vendor-published, not reproduced", "ghost")):
        o += cs.swatch(lx, 100, role, label, kind=kind)
        lx += cs.legend_width(label, kind)
    bh = 24
    # mqttd: a solid bar to the cheapest shape, a light one on to the dearest, a tick per shape.
    y = 150
    o.append(cs.text(bx - 14, y + 10, "mqttd, by node size", size=13, role="ink", anchor="end", weight=600))
    o.append(cs.text(bx - 14, y + 26, f"{len(mine)} measured clusters, 2 copies", size=11, role="muted", anchor="end"))
    o.append(f'<rect x="{bx}" y="{y}" width="{X(hi) - bx:.1f}" height="{bh}" rx="4" class="f-s1" fill-opacity="0.3"/>')
    o.append(f'<rect x="{bx}" y="{y}" width="{X(lo) - bx:.1f}" height="{bh}" rx="4" class="f-s1"/>')
    for c in costs:
        o.append(cs.vline(X(c), y + bh + 2, y + bh + 8, "s1", 1.5))
    o.append(cs.text(X(hi) + 10, y + 11, f"${lo:,.0f}–{hi:,.0f} a month per million msg/s", size=14, role="ink",
                     weight=650, num=True))
    o.append(cs.text(X(hi) + 10, y + 27, f"{rng} less than HiveMQ, by node size", size=11, role="ink2", num=True))
    o.append(cs.text(bx, y + 44, shapes
                     + (f" · on HiveMQ's shape, {shape(shape_hive)} vCPU, "
                        f"${usd_per_million(shape_hive['vcpu'], shape_hive['msg_per_s']):,.0f}: level"
                        if shape_hive else ""), size=11, role="muted", num=True))
    # HiveMQ: a dashed outline.
    y = 236
    o.append(cs.text(bx - 14, y + 10, "HiveMQ 4.18, published", size=13, role="ink", anchor="end", weight=600))
    o.append(cs.text(bx - 14, y + 26, f"{hive['nodes']} × {hive['host']}, {each(hive)} vCPU each", size=11,
                     role="muted", anchor="end"))
    o.append(f'<rect x="{bx + 0.75}" y="{y + 0.75}" width="{X(hv_cost) - bx - 1.5:.1f}" height="{bh - 1.5}" rx="4" '
             'fill="none" class="k-ghost" stroke-width="1.5" stroke-dasharray="4 3"/>')
    o.append(cs.text(X(hv_cost) + 10, y + 11, f"${hv_cost:,.0f} a month per million msg/s", size=14, role="ink",
                     weight=650, num=True))
    o.append(cs.text(X(hv_cost) + 10, y + 27, "its licence comes on top", size=11, role="ink2"))
    o.append(cs.text(bx, y + 44, hive_words, size=11, role="muted", num=True))
    # The comparisons as a table: no line drawn through anything, nothing to misread.
    if table:
        ty = 314
        cols = (32, 330, 580, 830)
        for x, head in zip(cols, ("", "mqttd, measured", "HiveMQ 4.18, published", "")):
            if head:
                o.append(cs.text(x, ty, head, size=11, role="muted"))
        o.append(cs.hline(32, w - 32, ty + 6, "grid"))
        ty += 24
        for fixed, a, b, diff in table:
            o.append(cs.text(cols[0], ty, fixed, size=12, role="ink2", num=True))
            for x, (val, what) in zip(cols[1:3], (a, b)):
                o.append(cs.text(x, ty, val, size=13, role="ink", weight=650, num=True))
                o.append(cs.text(x, ty + 15, what, size=11, role="muted", num=True))
            o.append(cs.text(cols[3], ty, diff, size=13, role="ink", weight=650, num=True))
            ty += 38
    hosts: dict[tuple[int, str], list[int]] = {}
    for _, _, r in pts["mqttd"]:
        hosts.setdefault((each(r), r["host"]), []).append(r["nodes"])
    o.append(cs.text(32, h - 54, f"both priced per vCPU at m6a.8xlarge's on-demand rate, ${USD_PER_VCPU_H:.4f} a "
                     f"vCPU-hour × {MONTH_H} h (Hetzner's own is about the same, EUR 0.0431) · load generators not "
                     "counted", size=11, role="muted"))
    o.append(cs.text(32, h - 36, "HiveMQ's clustering needs its commercial edition: that licence comes on top of its "
                     "bar, at a price not published", size=11, role="muted"))
    o.append(cs.text(32, h - 18, "mqttd: " + " · ".join(f"{host} ({n} vCPU) at {' and '.join(map(str, sorted(nodes)))} nodes"
                                                        for (n, host), nodes in sorted(hosts.items()))
                     + " · the maximum lossless step of each run · source: docs/benchmarks/data/headline.csv",
                     size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


CHARTS = {
    "readme-scale-out.svg": draw_scale_out,
    "readme-durable-per-vcpu.svg": draw_per_vcpu,
    "readme-durable-cost.svg": draw_cost,
}


def derived_figures(rows: list[dict]) -> set[str]:
    """Every figure the charts derive from the CSV, as the README may print it: the cost
    ratios and monthly costs, the per-vCPU ratios and rates. check-readme-facts.py holds
    the README's prose to these, so a ratio in the text is always one a chart draws."""
    out: set[str] = set()
    pts = cost_points(rows)
    hv_rate, hv_vcpu = pts["hivemq"]
    out.add(f"{month_usd(hv_vcpu):,.0f}")
    for r, x in cost_ratios(rows):
        out |= {ratio_text(x), f"{month_usd(r['vcpu']):,.0f}", f"{hive_month(pts, r['msg_per_s']):,.0f}"}
        if r["vcpu"] == hv_vcpu:
            out.add(f"{r['msg_per_s'] / hv_rate - 1:.0%}")
    traffic = same_traffic(pts)
    if traffic:
        out.add(ratio_text(month_usd(hv_vcpu) / month_usd(traffic[1])))
    per, hive = per_vcpu_rows(rows)
    hv = hive["msg_per_s"] / hive["vcpu"]
    for r in per:
        out |= {f"{r['msg_per_s'] / r['vcpu']:,.0f}", ratio_text(r["msg_per_s"] / r["vcpu"] / hv)}
    for r in rows:
        out |= {f"{r['msg_per_s'] / r['vcpu'] / 1000:.1f}k", f"{usd_per_million(r['vcpu'], r['msg_per_s']):,.0f}"}
    return out


def self_test() -> None:
    rows = load()
    svg = draw_scale_out(rows)
    body = svg.split("</desc>")[1]
    head = svg.split("</desc>")[0]
    scale = [r for r in rows if r["chart"] == "scale-out"]
    bars = scale_bars(rows)
    tops = [top_of(b) for v in bars.values() for b in v.values() if measured(b)]
    pts = cost_points(rows)
    hv_rate, hv_vcpu = pts["hivemq"]
    per_million = sorted(usd_per_million(v, rate) for rate, v, _ in pts["mqttd"])
    cost = draw_cost(rows).split("</desc>")[1]
    checks = [
        ("the cost chart prints mqttd's range and HiveMQ's figure per million msg/s",
         f"${per_million[0]:,.0f}–{per_million[-1]:,.0f}" in cost
         and f"${usd_per_million(hv_vcpu, hv_rate):,.0f}" in cost),
        ("the cost chart states the same-money and same-traffic comparisons the data allows",
         ("At HiveMQ's spend" in cost) == bool(same_money(pts))
         and ("For HiveMQ's traffic" in cost) == bool(same_traffic(pts))),
        ("every GREEN column is drawn, plus its legend key",
         body.count('class="f-good"') == sum(r["state"] == "green" for r in scale) + 1),
        ("every YELLOW cap is drawn, plus its legend key when there is one",
         body.count('class="f-warn"') == sum(r["state"] == "yellow" for r in scale)
         + (1 if any(r["state"] == "yellow" for r in scale) else 0)),
        ("every RED cap is drawn, plus its legend key when there is one",
         body.count('class="f-crit"') == sum(r["state"] == "red" for r in scale)
         + (1 if any(r["state"] == "red" for r in scale) else 0)),
        ("every published figure is a ghost outline, plus its legend key when there is one",
         body.count('class="k-ghost"') == sum(r["state"] == "published" for r in scale)
         + (1 if any(r["state"] == "published" for r in scale) else 0)),
        ("every panel has a measured column", all(any(measured(b) for b in v.values()) for v in bars.values())),
        ("every column's lossless total, its rate per vCPU and its shape are printed",
         all(f">{'≥ ' if t['floor'] else ''}{per_node(t['msg_per_s'])}<" in body
             and f">{per_node(t['msg_per_s'] / t['vcpu'])}/vCPU<" in body and f">{shape(t)}<" in body for t in tops)),
        ("every column's hardware cost a month per 1M msg/s is printed, and the published point's",
         all(f">{usd_per_million_text(t)}<" in body for t in tops)
         and all(f">{usd_per_million_text(r)} a month per 1M msg/s<" in body
                 for r in scale if r["state"] == "published")),
        ("the price basis is stated", f"${USD_PER_VCPU_H:.4f} a vCPU-hour" in body),
        ("a floor is explained when one is drawn",
         ("≥ a floor" in body) == any(t["floor"] for t in tops)),
        ("the subtitle says what builds were measured",
         all(r["build"].split()[-1] in body for r in scale if r["state"] != "published")),
        ("the description names every figure", all(f"{r['msg_per_s']:,}" in head for r in scale)),
        ("the cost chart's points are the scale-out chart's durable cluster tops",
         [r["msg_per_s"] for _, _, r in cost_points(rows)["mqttd"]]
         == sorted(t["msg_per_s"] for t in tops if t["series"] == "durable" and t["nodes"] > 1)),
        ("ratios print as 1.25, 1.2, 2.0 and 3.5",
         [ratio_num(x) for x in (1.25, 1.2, 2.0, 3.49)] == ["1.25", "1.2", "2.0", "3.5"]),
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
