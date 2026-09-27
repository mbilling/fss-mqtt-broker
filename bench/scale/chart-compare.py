#!/usr/bin/env python3
"""Charts for the cross-broker comparison lane — plain SVG, no dependencies.

Two pictures the tables in docs/benchmarks/SINGLE-NODE-COMPARISON.md can only
describe:

  --latency RATE   what fraction of messages arrived within X ms, per broker,
                   at one offered rate. Drawn from the same baseline-differenced
                   histogram the p99 column comes from, so the curve and the
                   table cannot disagree.

  --timeline ARM   one rung's delivered rate and the broker's RSS on one time
                   axis, from the rung's start until the backlog has drained.
                   This is the picture of "queues vs sheds": a broker that
                   absorbed a backlog shows a burst above its offered rate once
                   the publishers stop, and a memory line that climbs and then
                   falls. One that shed shows neither.

SVG is hand-written rather than pulled from a plotting library: this repository
publishes charts into docs/, and a committed artifact whose provenance is one
readable file beats a binary produced by a dependency nobody pinned.
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

SCALE_DIR = Path(__file__).resolve().parent
SUMMARIZE = SCALE_DIR / "summarize-compare.py"


def _load_summarize():
    if not SUMMARIZE.is_file():
        sys.exit(f"{SUMMARIZE} is missing: the comparison loader cannot be imported")
    spec = importlib.util.spec_from_file_location("summarize_compare", SUMMARIZE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_S = _load_summarize()

sys.path.insert(0, str(SCALE_DIR))
import chart_style as cs  # noqa: E402

W = 960
LEGEND_W = 250  # the right-hand key: identity, p99 and band per broker


# ── latency distribution ─────────────────────────────────────────────────────
def cdf_points(rung_dir: Path) -> list[tuple[float, float]]:
    """(latency_ms, cumulative fraction) from the rung's own histogram.

    The same `merged_histogram` the p99 column uses, so the curve and the table
    are the same measurement drawn two ways: baselines differenced, per-driver
    scrapes merged, `-base.prom` files excluded as scrapes.
    """
    scrapes = [p for p in sorted(rung_dir.glob("sub-*.prom")) if not p.name.endswith("-base.prom")]
    if not scrapes:
        return []
    buckets, total = _S.merged_histogram(scrapes)
    if not total:
        return []
    return [(le, min(count / total, 1.0)) for le, count in sorted(buckets.items()) if le != float("inf")]


def latency_series(arms: list[dict], rate: int) -> list[dict]:
    """[{broker, control, cdf: [[ms, frac], ...]}] for every arm with a rung at RATE."""
    out = []
    for arm in sorted(arms, key=lambda a: a["index"]):
        for rung in arm["rungs"]:
            if rung["offered"] == rate:
                pts = cdf_points(rung["dir"])
                if pts:
                    out.append({"broker": arm["broker"], "control": bool(arm.get("control")),
                                "cdf": [list(p) for p in pts]})
    return out


def p99_of(cdf: list) -> float:
    """The first bucket bound holding 99% — the same upper bound the p99 column prints."""
    return next((ms for ms, frac in cdf if frac >= 0.99), float("inf"))


def render_latency(series: list[dict], rate: int, note: str = "") -> str:
    """Share delivered within X, per broker, over the GREEN / YELLOW / RED latency zones.

    The zones are the p99 bands of ADR 0048: where a curve crosses the p99 line
    is the band that rung earns. A step curve, because a bucket says "at most
    this", never "exactly" — the curve can only overstate latency.
    """
    import math

    if not series:
        sys.exit(f"no rung at {rate:,} msg/s carries a latency histogram")
    H = 560
    x0, x1, y0, y1 = 80, W - LEGEND_W - 36, 132, H - 104
    lo, hi = 1.0, 30000.0

    def sx(ms: float) -> float:
        ms = min(max(ms, lo), hi)
        return x0 + (math.log10(ms) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * (x1 - x0)

    def sy(frac: float) -> float:
        return y1 - frac * (y1 - y0)

    main = [s for s in series if not s["control"]]
    fastest = min(main, key=lambda s: p99_of(s["cdf"]))
    o = cs.open_svg(
        W, H, f"Latency at {rate:,} msg/s offered, one 4-vCPU node",
        "share of messages delivered within X · shaded by the p99 band it would earn · "
        "histogram bucket bounds, never flattering",
        "; ".join(f"{cs.name(s['broker'])}{' control' if s['control'] else ''}: p99 ≤ "
                  f"{_ms_label(p99_of(s['cdf']))}" for s in series))

    # Zones first, under everything: a wash, never a block.
    for a, b, band in ((lo, 1000, "green"), (1000, 5000, "yellow"), (5000, hi, "red")):
        role = cs.BAND_SLOT[band]
        o.append(f'<rect x="{sx(a):.1f}" y="{y0}" width="{sx(b) - sx(a):.1f}" height="{y1 - y0}" '
                 f'class="f-{role}" fill-opacity="{0.14 if band == "yellow" else 0.08}"/>')
        o.append(f'<rect x="{sx(a) + 8:.1f}" y="{y0 - 22:.1f}" width="9" height="9" rx="2" class="f-{role}"/>')
        o.append(cs.text(sx(a) + 22, y0 - 13, {"green": "GREEN · p99 ≤ 1 s, certified", "yellow": "YELLOW",
                                               "red": "RED"}[band], size=11, role="ink2", weight=600))
    for pct in (0, 25, 50, 75, 100):
        o.append(cs.hline(x0, x1, sy(pct / 100), "axis" if pct == 0 else "grid"))
        o.append(cs.text(x0 - 10, sy(pct / 100) + 4, f"{pct}%", size=12, role="muted", anchor="end", num=True))
    for ms in (1, 10, 100, 1000, 10000, 30000):
        o.append(cs.text(sx(ms), y1 + 22, _ms_label(ms), size=12, role="muted", anchor="middle"))
    o.append(cs.hline(x0, x1, sy(0.99), "ink2", 1, "2 3"))
    o.append(cs.text(x0 + 6, sy(0.99) + 16, "p99", size=11, role="ink2", weight=600, halo=True))

    # Curves, controls first so the arm they check sits on top.
    for s in sorted(series, key=lambda s: not s["control"]):
        role = cs.slot(s["broker"])
        d, prev = [f"M{sx(lo):.1f},{sy(0):.1f}"], 0.0
        for ms, frac in s["cdf"]:
            d.append(f"L{sx(ms):.1f},{sy(prev):.1f} L{sx(ms):.1f},{sy(frac):.1f}")
            prev = frac
        d.append(f"L{sx(hi):.1f},{sy(prev):.1f}")
        dash = ' stroke-dasharray="5 4" stroke-opacity="0.75"' if s["control"] else ""
        o.append(f'<path d="{" ".join(d)}" fill="none" class="k-{role}" stroke-width="2" '
                 f'stroke-linejoin="round"{dash}/>')
    # The p99 crossing of each arm: a dot with a surface ring, where its band is decided.
    for s in main:
        p = p99_of(s["cdf"])
        if p != float("inf"):
            o.append(f'<circle cx="{sx(p):.1f}" cy="{sy(0.99):.1f}" r="5.5" class="f-{cs.slot(s["broker"])} '
                     'k-surface" stroke-width="2"/>')

    # The key: identity, p99 and band, one row per broker — every value readable
    # without hovering, and identity never carried by colour alone.
    kx, ky = W - LEGEND_W - 4, y0 + 6
    o.append(cs.text(kx, ky - 16, "p99 at this rate", size=11, role="muted", weight=600))
    for s in series:
        p = p99_of(s["cdf"])
        band = cs.band_of(p)
        role = cs.slot(s["broker"])
        kind = "dash" if s["control"] else "line"
        o += cs.swatch(kx, ky + 12, role, cs.name(s["broker"]) + (" · control" if s["control"] else ""), kind=kind)
        o.append(cs.text(kx + 26, ky + 30, f"≤ {_ms_label(p)}" if p != float("inf") else "> 30 s",
                         size=13, role="ink", weight=650, num=True))
        o.append(f'<rect x="{kx + 104:.1f}" y="{ky + 19:.1f}" width="9" height="9" rx="2" '
                 f'class="f-{cs.BAND_SLOT[band]}"/>')
        o.append(cs.text(kx + 118, ky + 28, cs.BAND_LABEL[band], size=11, role="ink2", weight=600))
        ky += 50
    foot = (f"{cs.name(fastest['broker'])} reaches 99% first. "
            "Band shown is the p99 zone only; FAILED means lost messages, judged from the ledger, not from latency.")
    o.append(cs.text(x0, H - 50, foot, size=11, role="ink2"))
    if note:
        o.append(cs.text(x0, H - 32, note, size=11, role="muted"))
    o.append(cs.text(x0, H - 14, "source: docs/benchmarks/SINGLE-NODE-COMPARISON.md · one Hetzner CCX23, "
                     "brokers in sequence · emqtt-bench 0.6.3", size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


def _ms_label(ms: float) -> str:
    if ms == float("inf"):
        return "> 30 s"
    if ms < 1000:
        return f"{ms:g} ms"
    return f"{ms / 1000:g} s"


# ── one rung's timeline: delivered rate + broker memory ──────────────────────
ELAPSED = re.compile(r"(?:(\d+)m)?(\d+)s recv total=(\d+) rate=([\d.]+)/sec")
CLOCK = re.compile(r"^(\d{2}):(\d{2}):(\d{2})$")


def _secs(stamp: str) -> int | None:
    m = CLOCK.match(stamp.strip())
    if not m:
        return None
    h, mi, se = (int(x) for x in m.groups())
    return h * 3600 + mi * 60 + se


def memory_series(rung_dir: Path) -> list[tuple[int, float]]:
    """(seconds-of-day, MiB) from the broker-side cgroup stream."""
    path = rung_dir / "mem-broker.series"
    if not path.is_file():
        return []
    out = []
    for line in path.read_text(errors="replace").splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        at, value = _secs(parts[0]), parts[1]
        if at is None or not value.isdigit():
            continue
        out.append((at, int(value) / (1024 * 1024)))
    return out


def throughput_series(rung_dir: Path) -> list[tuple[int, float]]:
    """(seconds-of-day, delivered msg/s) summed across subscriber containers.

    emqtt-bench stamps each line with elapsed-since-its-own-start and containers
    do not start together, so every container needs an anchor. The one that
    works is the WINDOW CLOSE: `sub-N.log` is dumped at that instant while
    traffic is still flowing, so its last line is that moment, and
    `window-close.utc` is stamped on the broker's own clock — the same clock the
    memory series uses, which removes host-to-host skew from the chart entirely.

    The obvious anchor — the drain dump's clock — is wrong, and quietly: a
    container's log ENDS when its traffic stops, not when we read it. At 150k
    msg/s the log ran out 96 s before the dump, so anchoring on the dump shifted
    the whole curve that far right and drew a broker still delivering 100k msg/s
    long after its queue had emptied.
    """
    close = None
    try:
        close = _secs((rung_dir / "window-close.utc").read_text())
    except OSError:
        close = None
    per_second: dict[int, float] = {}
    for drain in sorted(rung_dir.glob("sub-*.drain")):
        idx = drain.name[len("sub-"):-len(".drain")]
        rows = _elapsed_rows(drain)
        if not rows:
            continue
        anchor = None
        window_log = rung_dir / f"sub-{idx}.log"
        if close is not None and window_log.is_file():
            live = _elapsed_rows(window_log)
            if live:
                anchor = close - live[-1][0]  # this container's start, on the broker's clock
        if anchor is None:
            continue
        for elapsed, rate in rows:
            at = anchor + elapsed
            per_second[at] = per_second.get(at, 0.0) + rate
    return sorted(per_second.items())


def _elapsed_rows(path: Path) -> list[tuple[int, float]]:
    rows = []
    for line in path.read_text(errors="replace").splitlines():
        m = ELAPSED.match(line.strip())
        if m:
            rows.append((int(m.group(1) or 0) * 60 + int(m.group(2)), float(m.group(4))))
    return rows


def timeline_data(arm: dict, rate: int) -> dict:
    """{broker, rate, span_s, delivered: [[s, msg/s]], memory_mib: [[s, MiB]]} for one rung."""
    rung = next((r for r in arm["rungs"] if r["offered"] == rate), None)
    if rung is None:
        sys.exit(f"arm {arm['index']} ({arm['broker']}) has no rung at {rate:,} msg/s")
    thr = throughput_series(rung["dir"])
    mem = memory_series(rung["dir"])
    if not thr and not mem:
        sys.exit(f"{rung['dir']} has neither a subscriber series nor a memory series to plot")
    stamps = [t for t, _ in thr] + [t for t, _ in mem]
    t_lo = min(stamps)
    return {"broker": arm["broker"], "rate": rate, "span_s": max(max(stamps) - t_lo, 1),
            "delivered": [[t - t_lo, v] for t, v in thr], "memory_mib": [[t - t_lo, v] for t, v in mem]}


def render_timeline(data: dict, note: str = "") -> str:
    """Two panels on ONE time axis: delivered msg/s above, broker memory below.

    Two measures, two scales, so two panels — never one plot with two y-axes,
    whose alignment would invent a correlation. The shared x-axis is what lets
    "memory climbs while delivery falls behind, then both unwind" be read.
    A broker that absorbed a backlog shows a burst above the offered line once
    the publishers stop and a memory curve that climbs and falls; one that shed
    shows neither. A rung where no client connected still has a story — the
    broker's memory — and is drawn rather than refused.
    """
    broker, rate, span = data["broker"], data["rate"], max(float(data["span_s"]), 1.0)
    thr, mem = data["delivered"], data["memory_mib"]
    role = cs.slot(broker)
    H = 600
    x0, x1 = 96, W - 40
    a0, a1 = 132, 312  # delivered panel
    b0, b1 = 368, 500  # memory panel

    def sx(t: float) -> float:
        return x0 + t / span * (x1 - x0)

    peak = max((v for _, v in thr), default=0.0)
    mem_peak = max((v for _, v in mem), default=0.0)
    thr_top = _nice(max(peak, rate) * 1.08)
    # Round the memory axis in the unit it is printed in, or its top reads "4.9 GiB".
    mem_top = (_nice(mem_peak * 1.08 / 1024) * 1024 if mem_peak >= 1024 else _nice(max(mem_peak, 1.0) * 1.08))
    facts = [f"offered {cs.compact(rate)}/s"]
    if thr:
        facts.append(f"peak delivered {cs.compact(peak)}/s")
    if mem:
        facts.append(f"peak memory {_mib(mem_peak)}, {_mib(mem[-1][1])} at the end")
    o = cs.open_svg(W, H, f"{cs.name(broker)} under {rate:,} msg/s offered",
                    " · ".join(facts), f"{cs.name(broker)}: delivered rate and broker memory over one rung, "
                    "from start until the backlog drained. " + " · ".join(facts))

    def panel(top: float, bottom: float, vmax: float, fmt, title: str, pts: list, ref: float | None):
        def sy(v: float) -> float:
            return bottom - v / vmax * (bottom - top)
        out = [cs.text(x0, top - 14, title, size=13, role="ink", weight=600)]
        for frac in (0, 0.5, 1.0):
            out.append(cs.hline(x0, x1, sy(vmax * frac), "axis" if frac == 0 else "grid"))
            out.append(cs.text(x0 - 10, sy(vmax * frac) + 4, fmt(vmax * frac), size=12, role="muted",
                               anchor="end", num=True))
        if ref is not None:
            out.append(cs.hline(x0, x1, sy(ref), "ink2", 1, "5 4"))
            out.append(cs.text(x1, sy(ref) - 6, f"offered {cs.compact(ref)}/s", size=11, role="ink2",
                               anchor="end", weight=600, halo=True))
        if pts:
            line = " ".join(f"{'M' if i == 0 else 'L'}{sx(t):.1f},{sy(v):.1f}" for i, (t, v) in enumerate(pts))
            area = line + f" L{sx(pts[-1][0]):.1f},{sy(0):.1f} L{sx(pts[0][0]):.1f},{sy(0):.1f} Z"
            out.append(f'<path d="{area}" class="f-{role}" fill-opacity="0.10"/>')
            out.append(f'<path d="{line}" fill="none" class="k-{role}" stroke-width="2" '
                       'stroke-linejoin="round" stroke-linecap="round"/>')
            t_pk, v_pk = max(pts, key=lambda p: p[1])
            out.append(f'<circle cx="{sx(t_pk):.1f}" cy="{sy(v_pk):.1f}" r="4.5" class="f-{role} k-surface" '
                       'stroke-width="2"/>')
            anchor = "end" if sx(t_pk) > x1 - 140 else "start"
            dx = -10 if anchor == "end" else 10
            out.append(cs.text(sx(t_pk) + dx, sy(v_pk) + 4, "peak " + fmt(v_pk), size=12, role="ink",
                               anchor=anchor, weight=650, halo=True))
        return out

    o += panel(a0, a1, thr_top, lambda v: cs.compact(v) if v else "0", "Delivered msg/s", thr, rate)
    if not thr:
        o.append(cs.text((x0 + x1) / 2, (a0 + a1) / 2 + 6, f"not one client connected at {rate:,} msg/s — "
                         "nothing was delivered", size=14, role="ink", anchor="middle", weight=600))
    o += panel(b0, b1, mem_top, lambda v: _mib(v) if v else "0", "Broker memory (container RSS)", mem, None)
    if not mem:
        o.append(cs.text((x0 + x1) / 2, (b0 + b1) / 2 + 6, "memory not sampled in this run", size=13,
                         role="muted", anchor="middle"))
    step = max(15, (int(span) // 8 + 14) // 15 * 15)
    for sec in range(0, int(span) + 1, step):
        o.append(cs.text(sx(sec), b1 + 22, f"{sec}s", size=12, role="muted", anchor="middle", num=True))
    if note:
        o.append(cs.text(32, H - 36, note, size=11, role="muted"))
    o.append(cs.text(32, H - 18, "source: docs/benchmarks/SINGLE-NODE-COMPARISON.md · one Hetzner CCX23 · "
                     "delivered rate driver-side, memory from the broker's cgroup", size=11, role="muted"))
    o.append("</svg>")
    return "\n".join(o) + "\n"


def _nice(v: float) -> float:
    """The next 'round' axis top at or above v: 1, 2, 2.5 or 5 times a power of ten."""
    import math
    if v <= 0:
        return 1.0
    e = 10 ** math.floor(math.log10(v))
    return next(m * e for m in (1, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10) if m * e >= v)


def _mib(v: float) -> str:
    return f"{v / 1024:.1f} GiB" if v >= 1024 else f"{v:.0f} MiB"


def self_test() -> None:
    import tempfile

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)

    with tempfile.TemporaryDirectory() as td:
        rdir = Path(td)
        # A histogram whose mass is under 10 ms must read as ~100% by 10 ms.
        (rdir / "sub-0.prom").write_text(
            'e2e_latency_bucket{le="1"} 10\ne2e_latency_bucket{le="10"} 90\n'
            'e2e_latency_bucket{le="100"} 100\ne2e_latency_bucket{le="+Inf"} 100\n'
            "e2e_latency_count 100\n"
        )
        pts = dict(cdf_points(rdir))
        check(abs(pts.get(10.0, 0) - 0.9) < 1e-6, f"CDF lost the 10ms bucket: {pts}")
        check(abs(pts.get(100.0, 0) - 1.0) < 1e-6, f"CDF does not reach 1.0: {pts}")

        # A baseline is subtracted, exactly as the p99 column does it.
        (rdir / "sub-0-base.prom").write_text(
            'e2e_latency_bucket{le="1"} 10\ne2e_latency_bucket{le="10"} 10\n'
            'e2e_latency_bucket{le="100"} 10\ne2e_latency_bucket{le="+Inf"} 10\n'
            "e2e_latency_count 10\n"
        )
        based = dict(cdf_points(rdir))
        # 90 of 100 arrived under 10 ms in the window, of which 10 belong to the
        # ramp: 80/90, not the 90/100 an unsubtracted curve would draw.
        check(abs(based.get(10.0, 0) - 80 / 90) < 1e-6,
              f"the ramp baseline was counted into the published curve: {based}")

        # Memory: bytes in, MiB out; a malformed line is skipped, not fatal.
        (rdir / "mem-broker.series").write_text(
            "MEM_STREAM_START_UTC 2026-09-17T00:00:00Z\n"
            "00:00:01 104857600\ngarbage\n00:00:02 209715200\n"
        )
        mem = memory_series(rdir)
        check(mem == [(1, 100.0), (2, 200.0)], f"memory series misparsed: {mem}")

        # Two containers started 90 s apart, anchored by the window close. Their
        # rates must land on the SAME seconds, or a drain burst smears.
        (rdir / "window-close.utc").write_text("00:02:00\n")
        (rdir / "sub-0.log").write_text("1m58s recv total=1 rate=1.0/sec\n")   # started at 00:00:02
        (rdir / "sub-1.log").write_text("28s recv total=1 rate=1.0/sec\n")     # started at 00:01:32
        (rdir / "sub-0.drain").write_text("1m58s recv total=1 rate=100.0/sec\n1m59s recv total=2 rate=200.0/sec\n")
        (rdir / "sub-1.drain").write_text("28s recv total=1 rate=10.0/sec\n29s recv total=2 rate=20.0/sec\n")
        thr = dict(throughput_series(rdir))
        check(thr.get(120) == 110.0 and thr.get(121) == 220.0,
              f"containers with different start times were not aligned: {thr}")
        # And a log that ran out BEFORE the drain dump must not stretch the curve:
        # the series ends where the traffic ended, not where the dump happened.
        (rdir / "sub-1.drain").write_text(
            "28s recv total=1 rate=10.0/sec\n29s recv total=2 rate=20.0/sec\n30s recv total=2 rate=0.0/sec\n")
        check(max(dict(throughput_series(rdir))) == 122,
              "the curve was stretched past the last line the containers logged")

        arms = [{"broker": "mqttd", "index": 1, "rungs": [{"offered": 30000, "dir": rdir, "pass": True}]}]
        svg = render_latency(latency_series(arms, 30000), 30000)
        check(svg.startswith("<svg") and svg.rstrip().endswith("</svg>"), "latency SVG is malformed")
        check("mqttd" in svg, "latency SVG lost its legend")
        # The bands are drawn, and the p99 key names the band the curve earns.
        check("GREEN · p99 ≤ 1 s" in svg and "YELLOW" in svg and "RED" in svg, "latency SVG lost its bands")
        check(p99_of([[1, 0.5], [10, 0.99], [100, 1.0]]) == 10 and cs.band_of(2000) == "yellow"
              and cs.band_of(5000) == "yellow" and cs.band_of(7500) == "red",
              "p99 or band lines moved away from ADR 0048's 1 s / 5 s")
        # The chart is theme-aware: one file, light and dark.
        check("prefers-color-scheme:dark" in svg, "latency SVG has no dark theme")

        svg2 = render_timeline(timeline_data(
            {"broker": "mqttd", "index": 1,
             "rungs": [{"offered": 30000, "dir": rdir, "pass": True}]}, 30000))
        check(svg2.startswith("<svg") and svg2.rstrip().endswith("</svg>"), "timeline SVG is malformed")
        check("Broker memory" in svg2 and "not sampled" not in svg2,
              "timeline SVG dropped the memory panel it has data for")
        # Two panels, never two y-scales on one plot.
        check("(left)" not in svg2 and "(right)" not in svg2, "timeline SVG is a dual-axis chart again")

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        sys.exit(1)
    print(
        "chart-compare self-test: 14 checks OK (the CDF reads its buckets and reaches 1.0; the "
        "ramp baseline is subtracted; memory parses bytes to MiB and survives a bad line; "
        "containers with different start times align on the window-close clock and the curve stops where the traffic did; both SVGs render with their "
        "legends, the latency one with its GREEN / YELLOW / RED zones at 1 s and 5 s, both theme-aware, and the "
        "timeline as two panels rather than two y-axes)"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="?", type=Path, help="a run's results/ directory")
    ap.add_argument("--latency", type=int, metavar="RATE", help="latency CDF across brokers at RATE msg/s")
    ap.add_argument("--timeline", metavar="BROKER", help="one broker's rate+memory timeline")
    ap.add_argument("--rate", type=int, help="offered rate for --timeline")
    ap.add_argument("--out", type=Path, required=False, help="write the SVG here (default: stdout)")
    ap.add_argument("--budget", type=float, default=1000.0, help="p99 GREEN line in ms used to load the arms")
    ap.add_argument("--data", type=Path, help="render from a recovered data file (docs/benchmarks/img/data/*.json)")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return
    if args.data:
        import json
        d = json.loads(args.data.read_text())
        note = "data recovered from the chart rendered from the raw runs, which were not retained"
        svg = (render_latency(d["series"], d["rate"], note) if d["kind"] == "latency"
               else render_timeline(d, note))
        if args.out:
            args.out.write_text(svg)
            print(f"wrote {args.out} ({len(svg):,} bytes)")
        else:
            print(svg)
        return
    if not args.results:
        ap.error("a results directory, or --data, is required")
    arms = _S.load_arms(args.results, args.budget)
    if args.latency:
        svg = render_latency(latency_series(arms, args.latency), args.latency)
    elif args.timeline:
        if not args.rate:
            ap.error("--timeline needs --rate")
        arm = next((a for a in arms if a["broker"] == args.timeline and not a.get("control")), None)
        if arm is None:
            arm = next((a for a in arms if a["broker"] == args.timeline), None)
        if arm is None:
            sys.exit(f"no arm for broker {args.timeline!r}")
        svg = render_timeline(timeline_data(arm, args.rate))
    else:
        ap.error("choose --latency or --timeline")
    if args.out:
        args.out.write_text(svg)
        print(f"wrote {args.out} ({len(svg):,} bytes)")
    else:
        print(svg)


if __name__ == "__main__":
    main()
