#!/usr/bin/env python3
"""One latency chart per ladder of a rig2 run, from its report.json.

    python3 bench/scale/rig2/chart-ramp.py <run-dir>     # writes <run-dir>/report-<workload>.svg
    python3 bench/scale/rig2/chart-ramp.py --self-test

verdict.py report calls this too, so a re-rendered report always has its charts.

x is the offered msg/s of every step, sorted; y is the gated p99 (ack; QoS 0:
end-to-end) on a log scale, with the p50 lighter below it. For a step measured in
rounds, the point is the MEAN of its rounds' percentile and the error bar spans the
rounds' min to max (the legend says so). The background carries the
latency bands of ADR 0048 (2026-09-27): GREEN p99 <= 1 s, YELLOW <= 5 s, RED above.
A CARRIED step (no loss, offer taken) is a filled point; a FAILED, OFFER NOT TAKEN,
DISCONNECTED or NOT DRAINED step a hollow red ring; a VOID step (the harness's fault) and
a bad step whose retry carried ("retried: transient") a dashed grey ring. The maximum
lossless step is labelled. The palette and card are
bench/scale/chart_style.py's, shared with the README charts.
"""
from __future__ import annotations

import json
import math
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))
import chart_style as cs  # noqa: E402

W, H = 960, 560
X0, X1 = 92.0, 928.0     # plot area
Y0, Y1 = 104.0, 462.0
CARRIED, VOID = "CARRIED", "VOID"
WORKLOAD_NAME = {"durable-qos1": "Durable QoS 1", "clean-qos1": "Clean QoS 1", "qos0": "QoS 0"}


def transient(s: dict) -> bool:
    """A bad step whose retry carried (verdict.py retry_marks): it does not count."""
    return (s.get("retry") or {}).get("mark") == "transient"


def fmt_lat(us: float | None) -> str:
    if us is None:
        return "—"
    ms = us / 1000
    if ms >= 1000:
        return f"{ms / 1000:.1f} s"
    if ms >= 10:
        return f"{ms:.0f} ms"
    return f"{ms:.1f} ms"


def chart_name(ladder: dict) -> str:
    arm = ladder.get("arm", "A")
    return f"report-{ladder['workload']}.svg" if arm == "A" else f"report-{arm}-{ladder['workload']}.svg"


def gated(step: dict, gate: str) -> dict:
    return step["ack_latency"] if gate == "ack" else step["e2e_latency"]


def rounds_n(step: dict) -> int:
    return len(step.get("rounds") or []) or 1


def spread_us(step: dict, key: str) -> tuple[float, float] | None:
    """(min, max) of the step's rounds for `key` (gate_p50_us, gate_p99_us), when it has
    two or more rounds with that figure."""
    st = (step.get("stats") or {}).get(key) or {}
    if (st.get("n") or 0) < 2 or st.get("min") is None:
        return None
    return st["min"], st["max"]


def y_range(values_us: list[float]) -> tuple[float, float]:
    """Log decades (in ms) that hold every value and the whole band ladder (to 10 s)."""
    ms = [v / 1000 for v in values_us if v]
    lo = min(ms + [1.0])
    hi = max(ms + [10_000.0])
    return 10 ** math.floor(math.log10(lo) + 1e-9), 10 ** math.ceil(math.log10(hi) - 1e-9)


def x_ticks(hi: float) -> tuple[float, list[float]]:
    """A nice linear axis from 0 that holds `hi` with headroom."""
    raw = hi * 1.08 / 5
    mag = 10 ** math.floor(math.log10(raw))
    step = next(m * mag for m in (1, 2, 2.5, 5, 10) if m * mag >= raw)
    top = math.ceil(hi * 1.08 / step) * step
    return top, [i * step for i in range(int(round(top / step)) + 1)]


def render(ladder: dict, run: dict) -> str:
    gate = ladder.get("latency_gate", "ack")
    label = "ack" if gate == "ack" else "end-to-end"
    steps = sorted((s for s in ladder["steps"] if gated(s, gate)["p99_us"] is not None),
                   key=lambda s: (s["offered_rate"], s["step"]))
    name = WORKLOAD_NAME.get(ladder["workload"], ladder["workload"])
    nodes = run.get("brokers", "?")
    top = (ladder.get("capacity") or {}).get("lossless")
    multi = max((rounds_n(s) for s in steps), default=1)
    if top:
        mean = " mean" if (top.get("rounds") or 1) > 1 else ""
        title = (f"{name}, {nodes} nodes: {top['offered_rate']:,.0f} msg/s lossless, "
                 f"{'ack' if gate == 'ack' else 'e2e'} p99{mean} {fmt_lat(top['gate_p99_us'])}")
    else:
        title = f"{name}, {nodes} nodes: no step carried without loss"
    started = str(run.get("started_utc", ""))
    date = f"{started[:4]}-{started[4:6]}-{started[6:8]}" if len(started) >= 8 else "?"
    payload = (ladder.get("mqtt_load") or {}).get("payload")
    sub = [f"tier {run.get('tier', '?')}: {nodes} × {run.get('broker_type', '?')} brokers, "
           f"{run.get('generators', '?')} × {run.get('gen_type', '?')} generator",
           ladder["workload"]]
    if payload:
        sub.append(f"{payload} B payload")
    if multi > 1:
        sub.append(f"{multi} rounds a step: means, bars min–max")
    sub.append(date)
    if run.get("unreleased"):
        sub.append("UNRELEASED binary")
    out = cs.open_svg(W, H, title, " · ".join(sub),
                      desc=f"{label} p99 and p50 by offered msg/s for every step of the ladder")

    extremes = [v for s in steps for k in ("gate_p50_us", "gate_p99_us") for v in (spread_us(s, k) or ())]
    lo_ms, hi_ms = y_range([v for s in steps for v in (gated(s, gate)["p99_us"], gated(s, gate)["p50_us"])] + extremes)
    l_lo, l_hi = math.log10(lo_ms), math.log10(hi_ms)

    def y(us: float) -> float:
        ms = max(us / 1000, lo_ms)
        return Y1 - (math.log10(ms) - l_lo) / (l_hi - l_lo) * (Y1 - Y0)

    x_top, xt = x_ticks(max([s["offered_rate"] for s in steps] + [1.0]))

    def x(rate: float) -> float:
        return X0 + rate / x_top * (X1 - X0)

    # Bands, each with its label at the top left inside it.
    for role, lo_b, hi_b, txt in (("good", lo_ms, 1000.0, "GREEN · p99 ≤ 1 s"),
                                  ("warn", 1000.0, 5000.0, "YELLOW · ≤ 5 s"),
                                  ("crit", 5000.0, hi_ms, "RED · > 5 s")):
        yt, yb = y(hi_b * 1000), y(lo_b * 1000)
        op = "0.14" if role == "warn" else "0.08"
        out.append(f'<rect x="{X0:.1f}" y="{yt:.1f}" width="{X1 - X0:.1f}" height="{yb - yt:.1f}" '
                   f'class="f-{role}" fill-opacity="{op}"/>')
        out.append(f'<rect x="{X1 - 8 - 9:.1f}" y="{yt + 7:.1f}" width="9" height="9" rx="2" class="f-{role}"/>')
        out.append(cs.text(X1 - 22, yt + 15.5, txt, size=11, weight=600, anchor="end"))

    # Grid and axes.
    d = 10 ** math.floor(l_lo)
    while d <= hi_ms * 1.0001:
        yy = y(d * 1000)
        out.append(cs.hline(X0, X1, yy, "grid"))
        out.append(cs.text(X0 - 10, yy + 4, fmt_lat(d * 1000).replace(".0 ", " "), size=11, role="muted",
                           anchor="end", num=True))
        d *= 10
    for t in xt:
        out.append(cs.vline(x(t), Y0, Y1, "grid"))
        out.append(cs.text(x(t), Y1 + 18, cs.compact(t), size=11, role="muted", anchor="middle", num=True))
    out.append(cs.hline(X0, X1, Y1, "axis"))
    out.append(cs.text((X0 + X1) / 2, Y1 + 40, "offered msg/s", size=12, anchor="middle"))
    out.append(f'<text x="28" y="{(Y0 + Y1) / 2:.1f}" font-size="12" text-anchor="middle" class="f-ink2" '
               f'transform="rotate(-90 28 {(Y0 + Y1) / 2:.1f})">{label} latency (log)</text>')

    # The carried line, p50 then p99, so p99 is on top.
    carried = [s for s in steps if s["verdict"] == CARRIED]
    for key, op, width in (("p50_us", "0.4", 1.5), ("p99_us", "1", 2)):
        if len(carried) >= 2:
            pts = " ".join(f"{x(s['offered_rate']):.1f},{y(gated(s, gate)[key]):.1f}" for s in carried)
            out.append(f'<polyline points="{pts}" fill="none" class="k-s1" stroke-width="{width}" '
                       f'stroke-opacity="{op}" stroke-linejoin="round"/>')
    # Error bars under the points: the rounds' min to max, p50 lighter.
    for s in steps:
        cx = x(s["offered_rate"])
        role = ("s1" if s["verdict"] == CARRIED else "ghost" if (s["verdict"] == VOID or transient(s)) else "crit")
        for key, op, cap in (("gate_p50_us", "0.45", 3.0), ("gate_p99_us", "0.85", 4.5)):
            sp = spread_us(s, key)
            if sp is None:
                continue
            y_lo, y_hi = y(sp[0]), y(sp[1])
            out.append(f'<line x1="{cx:.1f}" y1="{y_hi:.1f}" x2="{cx:.1f}" y2="{y_lo:.1f}" class="k-{role}" '
                       f'stroke-width="1.5" stroke-opacity="{op}"/>')
            for yy in (y_lo, y_hi):
                out.append(f'<line x1="{cx - cap:.1f}" y1="{yy:.1f}" x2="{cx + cap:.1f}" y2="{yy:.1f}" '
                           f'class="k-{role}" stroke-width="1.5" stroke-opacity="{op}"/>')
    for s in steps:
        g = gated(s, gate)
        cx = x(s["offered_rate"])
        n = rounds_n(s)
        sp = spread_us(s, "gate_p99_us")
        tip = (f"step {s['step']}: {s['offered_rate']:,.0f} msg/s offered, {s['verdict']}"
               f"{' (retried: transient)' if transient(s) else ''}; "
               f"{label} p50 {fmt_lat(g['p50_us'])}, p99 {fmt_lat(g['p99_us'])}"
               + (f" (means of {n} rounds; p99 {fmt_lat(sp[0])} to {fmt_lat(sp[1])})" if sp else ""))
        for key, r, op in (("p50_us", 3.5, 0.45), ("p99_us", 5, 1.0)):
            cy = y(g[key]) if g[key] is not None else None
            if cy is None:
                continue
            if s["verdict"] == CARRIED:
                mark = (f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r}" class="f-s1 k-surface" stroke-width="2"'
                        f'{"" if op == 1 else f" fill-opacity={chr(34)}{op}{chr(34)}"}>')
            elif s["verdict"] == VOID or transient(s):
                mark = (f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r}" fill="none" class="k-ghost" '
                        f'stroke-width="1.5" stroke-dasharray="2 2">')
            else:
                mark = (f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{r}" fill="none" class="k-crit" '
                        f'stroke-width="2"{"" if op == 1 else f" stroke-opacity={chr(34)}0.6{chr(34)}"}>')
            out.append(mark + f"<title>{cs.esc(tip)}</title></circle>")

    # The maximum lossless step, labelled above and to the LEFT of its point: the steps
    # above it in rate (failed, bisected) crowd its right, the climb below it its left
    # only lower down. Right of it only near the left edge.
    if top:
        s = next((s for s in carried if s["step"] == top["step"]), None)
        if s is not None:
            cx, cy = x(s["offered_rate"]), y(gated(s, gate)["p99_us"])
            l1 = f"{s['offered_rate']:,.0f} msg/s lossless"
            mean = " mean" if rounds_n(s) > 1 else ""
            l2 = f"{'ack' if gate == 'ack' else 'e2e'} p99{mean} {fmt_lat(gated(s, gate)['p99_us'])}"
            if gate == "ack":
                l2 += f" · e2e p99 {fmt_lat(s['e2e_latency']['p99_us'])}"
            left = cx > X0 + 260
            anchor, lx = ("end", cx - 8) if left else ("start", cx + 8)
            sp = spread_us(s, "gate_p99_us")
            top_y = min(cy, y(sp[1])) if sp else cy  # above the error bar's top too
            ly = max(top_y - 30, Y0 + 40)
            if top_y - 7 > ly + 6:
                out.append(f'<line x1="{cx:.1f}" y1="{top_y - 7:.1f}" x2="{cx:.1f}" y2="{ly + 6:.1f}" class="k-ink2" '
                           'stroke-width="1"/>')
            out.append(cs.text(lx, ly - 14, l1, size=13, role="ink", weight=650, anchor=anchor, num=True, halo=True))
            out.append(cs.text(lx, ly + 2, l2, size=12, role="ink2", anchor=anchor, num=True, halo=True))

    # Legend.
    ly, lx = H - 26, 92.0
    mean = " mean" if multi > 1 else ""
    entries = [
        (f'<circle cx="{lx + 6:.1f}" cy="{ly - 4:.1f}" r="5" class="f-s1"/>', f"carried: {label} p99{mean}"),
        (f'<circle cx="{0:.1f}" cy="{ly - 4:.1f}" r="3.5" class="f-s1" fill-opacity="0.45"/>', f"{label} p50{mean}"),
        (f'<circle cx="{0:.1f}" cy="{ly - 4:.1f}" r="5" fill="none" class="k-crit" stroke-width="2"/>',
         "not carried"),
    ]
    if any(spread_us(s, "gate_p99_us") for s in steps):
        entries.append((f'<line x1="{0:.1f}" y1="{ly - 11:.1f}" x2="{0:.1f}" y2="{ly + 3:.1f}" class="k-s1" '
                        f'stroke-width="1.5"/><line x1="{-4:.1f}" y1="{ly - 11:.1f}" x2="{4:.1f}" y2="{ly - 11:.1f}" '
                        f'class="k-s1" stroke-width="1.5"/><line x1="{-4:.1f}" y1="{ly + 3:.1f}" x2="{4:.1f}" '
                        f'y2="{ly + 3:.1f}" class="k-s1" stroke-width="1.5"/>', "min–max of the rounds"))
    if any(s["verdict"] == VOID or transient(s) for s in steps):
        what = " / ".join(w for w, on in (("void (rerun)", any(s["verdict"] == VOID for s in steps)),
                                          ("retried: transient", any(transient(s) for s in steps))) if on)
        entries.append((f'<circle cx="{0:.1f}" cy="{ly - 4:.1f}" r="5" fill="none" class="k-ghost" '
                        'stroke-width="1.5" stroke-dasharray="2 2"/>', what))
    for mark, txt in entries:
        mark = mark.replace(f'cx="{0:.1f}"', f'cx="{lx + 6:.1f}"')
        for a in ("x1", "x2"):  # the error-bar glyph, drawn around x = 0
            for off in (-4, 0, 4):
                mark = mark.replace(f'{a}="{off:.1f}"', f'{a}="{lx + 6 + off:.1f}"')
        out.append(mark)
        out.append(cs.text(lx + 17, ly, txt, size=12))
        lx += cs.legend_width(txt) + 6
    out.append("</svg>")
    return "\n".join(out) + "\n"


def write_charts(run_dir: Path, data: dict) -> list[Path]:
    """Render every ladder of `data` (a report.json) into run_dir; set each ladder's `chart`."""
    paths = []
    for ladder in data["ladders"]:
        p = run_dir / chart_name(ladder)
        p.write_text(render(ladder, data.get("run", {})))
        ladder["chart"] = p.name
        paths.append(p)
    return paths


# ── self-test ───────────────────────────────────────────────────────────────────────


def _step(i, rate, verdict, p99_ms, p50_ms=None):
    lat = {"p50_us": int((p50_ms or p99_ms / 4) * 1000), "p99_us": int(p99_ms * 1000), "p999_us": None}
    return {"step": i, "rate": rate, "offered_rate": rate, "verdict": verdict, "ack_latency": lat, "e2e_latency": lat}


def self_test() -> None:
    import xml.etree.ElementTree as ET
    steps = [_step(1, 10000, CARRIED, 2), _step(2, 15000, CARRIED, 40), _step(3, 22500, "FAILED", 3000),
             _step(4, 18800, CARRIED, 2200), _step(5, 20600, "OFFER NOT TAKEN", 8000), _step(6, 19700, VOID, 900)]
    ladder = {"workload": "durable-qos1", "arm": "A", "latency_gate": "ack", "steps": steps,
              "mqtt_load": {"payload": 200},
              "capacity": {"lossless": {"offered_rate": 18800, "gate_p99_us": 2_200_000, "step": 4}}}
    run = {"tier": "n3", "brokers": 3, "broker_type": "ccx23", "generators": 1, "gen_type": "ccx33",
           "started_utc": "20261026T063123Z"}
    svg = render(ladder, run)
    root = ET.fromstring(svg)  # well-formed
    ns = "{http://www.w3.org/2000/svg}"
    assert root.find(f"{ns}title").text == "Durable QoS 1, 3 nodes: 18,800 msg/s lossless, ack p99 2.2 s", \
        root.find(f"{ns}title").text
    circles = [c for c in root.iter(f"{ns}circle") if c.find(f"{ns}title") is not None]
    assert len(circles) == 2 * len(steps), len(circles)
    filled = [c for c in circles if "f-s1" in c.get("class", "")]
    hollow_red = [c for c in circles if "k-crit" in c.get("class", "")]
    assert len(filled) == 2 * 3 and len(hollow_red) == 2 * 2, (len(filled), len(hollow_red))
    assert "18,800 msg/s lossless" in svg and "2026-10-26" in svg and "200 B payload" in svg
    assert "GREEN · p99 ≤ 1 s" in svg and "YELLOW" in svg and "RED" in svg
    assert "site" not in svg.lower()
    # Points sit on the log scale: the 2.2 s point is between the 1 s and 5 s band edges.
    ys = {c.find(f"{ns}title").text.split(":")[0]: float(c.get("cy")) for c in filled if c.get("r") == "5"}
    assert ys["step 1"] > ys["step 2"] > ys["step 4"], ys
    # QoS 0 gates on end-to-end; nothing carried reads as such.
    q0 = {**ladder, "workload": "qos0", "latency_gate": "e2e", "capacity": {"lossless": None},
          "steps": [_step(1, 50000, "FAILED", 30)]}
    t = ET.fromstring(render(q0, run)).find(f"{ns}title").text
    assert t == "QoS 0, 3 nodes: no step carried without loss", t
    # Rounds: the point is the mean, the bar the rounds' min to max, and the chart says so.
    def with_rounds(step, p99s_ms):
        n = len(p99s_ms)
        mean = sum(p99s_ms) / n * 1000
        step["ack_latency"] = step["e2e_latency"] = {"p50_us": mean / 4, "p99_us": mean, "p999_us": None}
        step["rounds"] = [{} for _ in p99s_ms]
        step["stats"] = {"gate_p99_us": {"n": n, "mean": mean, "min": min(p99s_ms) * 1000, "max": max(p99s_ms) * 1000},
                         "gate_p50_us": {"n": n, "mean": mean / 4, "min": min(p99s_ms) * 250, "max": max(p99s_ms) * 250}}
        return step
    r_steps = [with_rounds(_step(1, 10000, CARRIED, 2), [1.5, 2, 2.5]),
               with_rounds(_step(2, 15000, CARRIED, 40), [20, 40, 60]),
               with_rounds(_step(3, 22500, "FAILED", 3000), [3000])]
    r_ladder = {**ladder, "steps": r_steps,
                "capacity": {"lossless": {"offered_rate": 15000, "gate_p99_us": 40_000, "step": 2, "rounds": 3}}}
    r_svg = render(r_ladder, run)
    r_root = ET.fromstring(r_svg)
    assert r_root.find(f"{ns}title").text == "Durable QoS 1, 3 nodes: 15,000 msg/s lossless, ack p99 mean 40 ms", \
        r_root.find(f"{ns}title").text
    assert "min–max of the rounds" in r_svg and "3 rounds a step: means, bars min–max" in r_svg
    assert "ack p99 mean" in r_svg and "means of 3 rounds; p99 20 ms to 60 ms" in r_svg
    # Two carried steps with spread, each a p99 and a p50 bar of three lines (stem, two caps);
    # the one-round FAILED step has none.
    bars = [ln for ln in r_root.iter(f"{ns}line") if ln.get("stroke-opacity") in ("0.85", "0.45")]
    assert len(bars) == 2 * 2 * 3, len(bars)
    stems = sorted((float(ln.get("y1")), float(ln.get("y2"))) for ln in bars if ln.get("x1") == ln.get("x2"))
    assert all(a < b for a, b in stems), stems  # top above bottom: max above min on the log scale
    assert y_range([500, 18_940_000]) == (0.1, 100_000)
    assert y_range([800, 140_000]) == (0.1, 10_000)
    assert x_ticks(114200)[0] >= 114200 * 1.08
    print("chart-ramp self-test: ok")


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        self_test()
        return 0
    if len(argv) != 1:
        print(__doc__, file=sys.stderr)
        return 2
    run_dir = Path(argv[0])
    data = json.loads((run_dir / "report.json").read_text())
    for p in write_charts(run_dir, data):
        print(p)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
