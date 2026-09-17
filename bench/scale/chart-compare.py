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

# One colour per broker, stable across every chart so a reader who has seen one
# figure can read the next without consulting its legend again.
COLOURS = {
    "mqttd": "#2563eb",
    "mosquitto": "#059669",
    "emqx": "#d97706",
    "hivemq": "#dc2626",
}
FALLBACK = "#64748b"

W, H = 900, 460
PAD_L, PAD_R, PAD_T, PAD_B = 70, 78, 54, 86


def colour(broker: str) -> str:
    return COLOURS.get(broker.split("-")[0].lower(), FALLBACK)


def esc(text: str) -> str:
    return (
        str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    )


def svg_open(title: str, subtitle: str) -> list[str]:
    return [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
        f'font-family="system-ui,-apple-system,Segoe UI,Roboto,sans-serif" role="img" '
        f'aria-label="{esc(title)}">',
        f'<rect width="{W}" height="{H}" fill="#ffffff"/>',
        f'<text x="{PAD_L}" y="26" font-size="16" font-weight="600" fill="#0f172a">{esc(title)}</text>',
        f'<text x="{PAD_L}" y="44" font-size="12" fill="#475569">{esc(subtitle)}</text>',
    ]


def frame() -> list[str]:
    x0, y0, x1, y1 = PAD_L, PAD_T, W - PAD_R, H - PAD_B
    return [
        f'<rect x="{x0}" y="{y0}" width="{x1 - x0}" height="{y1 - y0}" fill="none" stroke="#cbd5e1"/>',
    ]


def legend(entries: list[tuple[str, str]], y: int) -> list[str]:
    out, x = [], PAD_L
    for label, col in entries:
        out.append(f'<rect x="{x}" y="{y - 8}" width="18" height="3" fill="{col}"/>')
        out.append(f'<text x="{x + 24}" y="{y - 2}" font-size="11" fill="#334155">{esc(label)}</text>')
        x += 34 + 7 * len(label)
    return out


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


def render_latency(arms: list[dict], rate: int) -> str:
    import math

    series = []
    for arm in arms:
        for rung in arm["rungs"]:
            if rung["offered"] != rate:
                continue
            pts = cdf_points(rung["dir"])
            if pts:
                series.append((arm["broker"], arm["index"], pts, rung, arm.get("control", False)))
    if not series:
        sys.exit(f"no rung at {rate:,} msg/s carries a latency histogram")

    lo, hi = 1.0, 30000.0
    x0, y0, x1, y1 = PAD_L, PAD_T, W - PAD_R, H - PAD_B

    def sx(ms: float) -> float:
        ms = min(max(ms, lo), hi)
        return x0 + (math.log10(ms) - math.log10(lo)) / (math.log10(hi) - math.log10(lo)) * (x1 - x0)

    def sy(frac: float) -> float:
        return y1 - frac * (y1 - y0)

    out = svg_open(
        f"Latency distribution at {rate:,} msg/s offered",
        "share of messages delivered within X — driver-side, histogram bucket bounds, "
        "measured window only",
    )
    out += frame()
    for ms in (1, 10, 100, 1000, 10000, 30000):
        x = sx(ms)
        out.append(f'<line x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y1}" stroke="#e2e8f0"/>')
        label = f"{ms} ms" if ms < 1000 else f"{ms // 1000} s"
        out.append(
            f'<text x="{x:.1f}" y="{y1 + 18}" font-size="11" fill="#475569" text-anchor="middle">{label}</text>'
        )
    for pct in (0, 50, 90, 99):
        y = sy(pct / 100)
        out.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" stroke="#e2e8f0"/>')
        out.append(
            f'<text x="{x0 - 8}" y="{y + 4:.1f}" font-size="11" fill="#475569" text-anchor="end">{pct}%</text>'
        )

    entries = []
    for broker, _idx, pts, rung, is_control in sorted(series, key=lambda s: s[1]):
        col = colour(broker)
        # A step curve, because a bucket says "at most this", never "exactly".
        d = []
        prev = 0.0
        for ms, frac in pts:
            d.append(f"{'M' if not d else 'L'}{sx(ms):.1f},{sy(prev):.1f}")
            d.append(f"L{sx(ms):.1f},{sy(frac):.1f}")
            prev = frac
        # The control arm is drawn, not hidden: two lines of the same colour that
        # sit on each other are the reproducibility claim, visible.
        dash = ' stroke-dasharray="5 4"' if is_control else ""
        out.append(f'<path d="{" ".join(d)}" fill="none" stroke="{col}" stroke-width="2"{dash}/>')
        verdict = "pass" if rung["pass"] else "failed"
        label = f"{broker} (control)" if is_control else f"{broker} ({verdict})"
        entries.append((label, col))
    out += legend(entries, H - 14)
    out.append(
        f'<text x="{x0}" y="{y1 + 40}" font-size="11" fill="#64748b">'
        "each step is a histogram bucket bound: the curve can only overstate latency, never flatter it</text>"
    )
    out.append("</svg>")
    return "\n".join(out)


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

    emqtt-bench stamps each line with elapsed-since-its-own-start, and containers
    do not start together, so each container's series is anchored by the driver's
    wall clock captured with the drain dump: the last line's elapsed time IS the
    dump moment, and every earlier line counts back from it. Without that anchor
    the containers float by an unknown offset and a drain burst smears into a
    gentle slope.
    """
    per_second: dict[int, float] = {}
    for drain in sorted(rung_dir.glob("sub-*.drain")):
        di = drain.name.split("-")[1].split(".")[0]
        clock = rung_dir / f"dumpclock-{di}.drain"
        anchor = None
        for candidate in (clock, *sorted(rung_dir.glob("dumpclock-*.drain"))):
            if candidate.is_file():
                anchor = _secs(candidate.read_text().strip().splitlines()[-1] if candidate.read_text().strip() else "")
                if anchor is not None:
                    break
        rows = []
        for line in drain.read_text(errors="replace").splitlines():
            m = ELAPSED.match(line.strip())
            if m:
                rows.append((int(m.group(1) or 0) * 60 + int(m.group(2)), float(m.group(4))))
        if not rows or anchor is None:
            continue
        last_elapsed = rows[-1][0]
        for elapsed, rate in rows:
            at = anchor - (last_elapsed - elapsed)
            per_second[at] = per_second.get(at, 0.0) + rate
    return sorted(per_second.items())


def render_timeline(arm: dict, rate: int) -> str:
    rung = next((r for r in arm["rungs"] if r["offered"] == rate), None)
    if rung is None:
        sys.exit(f"arm {arm['index']} ({arm['broker']}) has no rung at {rate:,} msg/s")
    thr = throughput_series(rung["dir"])
    mem = memory_series(rung["dir"])
    if not thr:
        sys.exit(f"{rung['dir']} has no per-second subscriber series to plot")

    t_lo = min(t for t, _ in thr + (mem or [(thr[0][0], 0)]))
    t_hi = max(t for t, _ in thr + (mem or [(thr[-1][0], 0)]))
    span = max(t_hi - t_lo, 1)
    thr_max = max(v for _, v in thr) or 1
    mem_max = max((v for _, v in mem), default=0) or 1
    x0, y0, x1, y1 = PAD_L, PAD_T, W - PAD_R, H - PAD_B

    def sx(t: int) -> float:
        return x0 + (t - t_lo) / span * (x1 - x0)

    def sy_thr(v: float) -> float:
        return y1 - v / thr_max * (y1 - y0)

    def sy_mem(v: float) -> float:
        return y1 - v / mem_max * (y1 - y0)

    out = svg_open(
        f"{arm['broker']} at {rate:,} msg/s offered — delivered rate and broker memory",
        "from the rung's start until the backlog drained; memory is the container's cgroup RSS",
    )
    out += frame()
    for frac in (0, 0.25, 0.5, 0.75, 1.0):
        y = y1 - frac * (y1 - y0)
        out.append(f'<line x1="{x0}" y1="{y:.1f}" x2="{x1}" y2="{y:.1f}" stroke="#e2e8f0"/>')
        out.append(
            f'<text x="{x0 - 8}" y="{y + 4:.1f}" font-size="11" fill="#2563eb" text-anchor="end">'
            f"{thr_max * frac / 1000:.0f}k</text>"
        )
        if mem:
            out.append(
                f'<text x="{x1 + 8}" y="{y + 4:.1f}" font-size="11" fill="#7c3aed">'
                f"{mem_max * frac:.0f}M</text>"
            )
    for sec in range(0, span + 1, max(15, (span // 8 + 14) // 15 * 15)):
        x = sx(t_lo + sec)
        out.append(
            f'<text x="{x:.1f}" y="{y1 + 18}" font-size="11" fill="#475569" text-anchor="middle">{sec}s</text>'
        )

    # The offered rate, so "above the line" is visible as what it is: a backlog
    # leaving faster than it arrived.
    y_off = sy_thr(rate)
    if y0 <= y_off <= y1:
        out.append(
            f'<line x1="{x0}" y1="{y_off:.1f}" x2="{x1}" y2="{y_off:.1f}" stroke="#94a3b8" '
            'stroke-dasharray="4 4"/>'
        )
        out.append(
            f'<text x="{x1 - 4}" y="{y_off - 6:.1f}" font-size="10" fill="#64748b" text-anchor="end">'
            "offered</text>"
        )

    d = " ".join(f"{'M' if i == 0 else 'L'}{sx(t):.1f},{sy_thr(v):.1f}" for i, (t, v) in enumerate(thr))
    out.append(f'<path d="{d}" fill="none" stroke="#2563eb" stroke-width="2"/>')
    if mem:
        dm = " ".join(f"{'M' if i == 0 else 'L'}{sx(t):.1f},{sy_mem(v):.1f}" for i, (t, v) in enumerate(mem))
        out.append(f'<path d="{dm}" fill="none" stroke="#7c3aed" stroke-width="2" stroke-dasharray="6 3"/>')
    entries = [("delivered msg/s (left)", "#2563eb")]
    entries.append(("broker RSS (right)", "#7c3aed") if mem else ("broker RSS — not sampled in this run", FALLBACK))
    out += legend(entries, H - 18)
    out.append("</svg>")
    return "\n".join(out)


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

        # Throughput: two containers, one started 30s before the other, both
        # anchored by the driver's dump clock — their rates must land on the SAME
        # seconds, or a burst smears.
        (rdir / "dumpclock-0.drain").write_text("00:02:00\n")
        (rdir / "sub-0.drain").write_text("1m58s recv total=1 rate=100.0/sec\n1m59s recv total=2 rate=200.0/sec\n")
        (rdir / "sub-1.drain").write_text("28s recv total=1 rate=10.0/sec\n29s recv total=2 rate=20.0/sec\n")
        thr = dict(throughput_series(rdir))
        # 00:02:00 is second 120 of the day; each container's last line IS the
        # dump moment, so both land on 119 and 120 despite starting 90s apart.
        check(thr.get(119) == 110.0 and thr.get(120) == 220.0,
              f"containers with different start times were not aligned: {thr}")

        svg = render_latency(
            [{"broker": "mqttd", "index": 1,
              "rungs": [{"offered": 30000, "dir": rdir, "pass": True}]}], 30000)
        check(svg.startswith("<svg") and svg.rstrip().endswith("</svg>"), "latency SVG is malformed")
        check("mqttd" in svg, "latency SVG lost its legend")

        svg2 = render_timeline(
            {"broker": "mqttd", "index": 1,
             "rungs": [{"offered": 30000, "dir": rdir, "pass": True}]}, 30000)
        check(svg2.startswith("<svg") and svg2.rstrip().endswith("</svg>"), "timeline SVG is malformed")
        check("broker RSS (right)" in svg2, "timeline SVG dropped the memory axis it has data for")

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        sys.exit(1)
    print(
        "chart-compare self-test: 9 checks OK (the CDF reads its buckets and reaches 1.0; the "
        "ramp baseline is subtracted; memory parses bytes to MiB and survives a bad line; "
        "containers with different start times align on one clock; both SVGs render with their "
        "legends)"
    )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("results", nargs="?", type=Path, help="a run's results/ directory")
    ap.add_argument("--latency", type=int, metavar="RATE", help="latency CDF across brokers at RATE msg/s")
    ap.add_argument("--timeline", metavar="BROKER", help="one broker's rate+memory timeline")
    ap.add_argument("--rate", type=int, help="offered rate for --timeline")
    ap.add_argument("--out", type=Path, required=False, help="write the SVG here (default: stdout)")
    ap.add_argument("--budget", type=float, default=1000.0, help="p99 budget in ms used for pass/fail labels")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()

    if args.self_test:
        self_test()
        return
    if not args.results:
        ap.error("a results directory is required")
    arms = _S.load_arms(args.results, args.budget)
    if args.latency:
        svg = render_latency(arms, args.latency)
    elif args.timeline:
        if not args.rate:
            ap.error("--timeline needs --rate")
        arm = next((a for a in arms if a["broker"] == args.timeline and not a.get("control")), None)
        if arm is None:
            arm = next((a for a in arms if a["broker"] == args.timeline), None)
        if arm is None:
            sys.exit(f"no arm for broker {args.timeline!r}")
        svg = render_timeline(arm, args.rate)
    else:
        ap.error("choose --latency or --timeline")
    if args.out:
        args.out.write_text(svg)
        print(f"wrote {args.out} ({len(svg):,} bytes)")
    else:
        print(svg)


if __name__ == "__main__":
    main()
