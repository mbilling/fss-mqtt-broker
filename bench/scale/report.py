#!/usr/bin/env python3
"""Build the cross-run scale report — one self-contained HTML page.

    ./report.py --out /tmp/report.html .runs/<stamp> [.runs/<stamp> ...]
    ./report.py --out /tmp/report.html --all          # every run under .runs/

Where `summarize-curve.py` renders ONE run for hand-transcription into the
published doc, this aggregates MANY: every run it is given, keyed by the mqttd
version each recorded, so a new cluster size or a new release is added by
pointing it at another run directory and re-running. Nothing is hand-edited.

It reuses summarize-curve.py's parsers rather than reimplementing them — in
particular the lane B latency histogram, which must be DIFFERENCED against a
post-ramp baseline and merged across drivers, and is the single easiest thing
to get wrong (an end-of-rung scrape reports the container's lifetime, so a
rung's connect ramp lands in the published tail).

Every number here is `sustained` per that module's rule, and a rung whose offer
was not met is carried through with its flags rather than dropped, because
"the harness could not offer this" is the finding often enough to matter.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load_summarizer():
    """Import summarize-curve.py despite the hyphen; it guards its own main()."""
    spec = importlib.util.spec_from_file_location("summarize_curve", HERE / "summarize-curve.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


SC = _load_summarizer()


def run_version(run: Path) -> str:
    """The mqttd release a run recorded, from its own log. Unknown is explicit."""
    log = run / "run.log"
    if log.exists():
        m = re.search(r"mqttd=(\d+\.\d+\.\d+)", log.read_text(errors="replace"))
        if m:
            return m.group(1)
    for env in run.glob("results/nodes=*/env/broker0.txt"):
        m = re.search(r"mqttd_version\s*=\s*\"?(\d+\.\d+\.\d+)", env.read_text(errors="replace"))
        if m:
            return m.group(1)
    return "unknown"


def workload_of(run: Path) -> str:
    log = run / "run.log"
    if log.exists():
        m = re.search(r"workload=([a-z-]+)", log.read_text(errors="replace"))
        if m:
            return m.group(1)
    return "ad-hoc"


def collect(run: Path) -> dict:
    """One run -> {version, workload, sizes: {n: {laneA, laneB, laneC, laneD}}}."""
    root = run / "results"
    out = {"run": run.name, "version": run_version(run), "workload": workload_of(run), "sizes": {}}
    if not root.is_dir():
        return out
    for n, size_dir in SC.sizes(root):
        entry: dict = {}

        # lane B: throughput AND latency, per rung, with flags preserved
        rungs = []
        for rdir in sorted((size_dir / "laneB").glob("rung-*-plain")) if (size_dir / "laneB").is_dir() else []:
            m = re.search(r"rung-(\d+)-plain$", rdir.name)
            if not m:
                continue
            try:
                r = SC.lane_b_rung(rdir, int(m.group(1)))
            except Exception as e:  # a partial rung must not sink the report
                r = {"offered": int(m.group(1)), "flags": [f"unparsed: {e}"]}
            r["offered"] = int(m.group(1))
            rungs.append(r)
        if rungs:
            entry["laneB"] = rungs

        # lane A: the arms, medians over reps, with the reps kept
        arms = {}
        for phase in ("sat", "tier-local", "tier-relaxed"):
            for rep in SC.lane_a_results(size_dir, phase):
                arms.setdefault(f"{phase}|{rep['arm']}", []).append(
                    {"rate": rep.get("msgs_per_s"), "p99": rep.get("p99_ms")})
        if arms:
            entry["laneA"] = arms

        # lane C: idle-connection cost
        try:
            c = SC.lane_c(size_dir, n)
            if c:
                entry["laneC"] = c
        except Exception:
            pass

        # lane D: the store-and-forward cycle, parsed from its own summary
        s = size_dir / "laneD" / "summary.txt"
        if s.exists():
            txt = s.read_text(errors="replace")
            def grab(pat, cast=int):
                m = re.search(pat, txt)
                return cast(m.group(1)) if m else None
            entry["laneD"] = {
                "accepted": grab(r"accepted while offline\s+(\d+)"),
                "drained": grab(r"drained after resume\s+(\d+)"),
                "dropped": grab(r"dropped while offline\s+(\d+)"),
                "drain_s": grab(r"drain time\s+(\d+)s"),
                "session_bytes": grab(r"per session, OFFLINE\s+(\d+)"),
                "logs": (lambda m: m.group(1) if m else None)(re.search(r"from (\d+/\d+) container logs", txt)),
            }
        if entry:
            out["sizes"][str(n)] = entry
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("runs", nargs="*", type=Path)
    ap.add_argument("--all", action="store_true", help="every directory under .runs/")
    ap.add_argument("--out", type=Path, required=True, help="HTML file to write")
    ap.add_argument("--json", type=Path, help="also write the extracted dataset")
    a = ap.parse_args()

    runs = list(a.runs)
    if a.all:
        runs += sorted(p for p in (HERE / ".runs").glob("*") if p.is_dir())
    if not runs:
        ap.error("name at least one run directory, or pass --all")

    data = []
    for r in runs:
        d = collect(r)
        if d["sizes"]:
            data.append(d)
            print(f"  {d['run']}  v{d['version']:<8} {d['workload']:<18} sizes={sorted(d['sizes'], key=int)}", file=sys.stderr)
        else:
            print(f"  {r.name}  (no results — skipped)", file=sys.stderr)

    if a.json:
        a.json.write_text(json.dumps(data, indent=1))
    a.out.write_text(render(data))
    print(f"\nwrote {a.out} from {len(data)} run(s)", file=sys.stderr)


def render(data: list[dict]) -> str:
    from report_html import page  # split out so the HTML is editable on its own
    return page(data)


if __name__ == "__main__":
    main()
