#!/usr/bin/env python3
"""Walk a scale-curve results tree and print Lane E crossing / hub / idle hints.

Usage:
  python3 extract-lane-e.py .runs/<stamp>/results
  python3 extract-lane-e.py --self-test

Reads broker Prometheus snapshots under results/nodes=*/laneE/sites-*/.
Does not sum emqtt-bench pub rate= log lines.

Crossing = Δ mqttd_publish_forwarded_total / Δ mqttd_publish_received_total.
A missing forwarded family with large received is 0 forwards (Prometheus omits
zero series) — the 2026-09-14 N=7 Option B scrape shape (#482).
"""
from __future__ import annotations

import argparse
import re
import sys
import tempfile
import unittest
from pathlib import Path

PROM_LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<v>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*$"
)
IDLE_LINE = re.compile(r"\ball\b")


def parse_prom(path: Path) -> dict[tuple[str, str], float]:
    """Exact name+labels → value. Prefix matches are refused (received vs received_total)."""
    out: dict[tuple[str, str], float] = {}
    if not path.exists():
        return out
    for line in path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE.match(line)
        if not m:
            continue
        key = (m.group("name"), m.group("labels") or "")
        out[key] = out.get(key, 0.0) + float(m.group("v"))
    return out


def sum_family(parsed: dict[tuple[str, str], float], name: str) -> float:
    return sum(v for (n, _), v in parsed.items() if n == name)


def by_label(parsed: dict[tuple[str, str], float], name: str, label: str) -> dict[str, float]:
    out: dict[str, float] = {}
    pat = re.compile(rf'{re.escape(label)}="([^"]*)"')
    for (n, labels), v in parsed.items():
        if n != name:
            continue
        m = pat.search(labels)
        key = m.group(1) if m else ""
        out[key] = out.get(key, 0.0) + v
    return out


def load_snap(rdir: Path, label: str) -> dict[tuple[str, str], float]:
    merged: dict[tuple[str, str], float] = {}
    for path in sorted(rdir.glob(f"metrics-{label}-broker*.prom")):
        for k, v in parse_prom(path).items():
            merged[k] = merged.get(k, 0.0) + v
    return merged


def delta(a: dict[tuple[str, str], float], b: dict[tuple[str, str], float], name: str) -> float:
    return sum_family(b, name) - sum_family(a, name)


def mpstat_idle_mean(path: Path) -> float | None:
    """Mean %idle from mpstat `all` rows. Hint only — not a saturation proof."""
    if not path.exists():
        return None
    idles: list[float] = []
    for line in path.read_text(errors="replace").splitlines():
        if not IDLE_LINE.search(line):
            continue
        parts = line.split()
        if len(parts) < 3:
            continue
        try:
            idles.append(float(parts[-1]))
        except ValueError:
            continue
    if not idles:
        return None
    return sum(idles) / len(idles)


def mean_idle(rdir: Path, role: str) -> str:
    files = sorted((rdir / "cpu").glob(f"cpu-{role}*.txt"))
    if not files:
        return "—"
    vals = [mpstat_idle_mean(p) for p in files]
    vals = [v for v in vals if v is not None]
    if not vals:
        return "—"
    return f"{sum(vals) / len(vals):.0f}%"


def extract_rung(rdir: Path) -> dict:
    before, after = load_snap(rdir, "before"), load_snap(rdir, "after")
    drain = load_snap(rdir, "drain")
    end = drain or after
    received = delta(before, after, "mqttd_publish_received_total")
    forwarded = delta(before, after, "mqttd_publish_forwarded_total")
    if forwarded < 0:
        forwarded = 0.0
    crossing = (forwarded / received) if received else None
    hub_sum = by_label(after, "mqttd_hub_dispatch_seconds_sum", "command")
    hub_sum_b = by_label(before, "mqttd_hub_dispatch_seconds_sum", "command")
    hub_n = by_label(after, "mqttd_hub_dispatch_seconds_count", "command")
    hub_n_b = by_label(before, "mqttd_hub_dispatch_seconds_count", "command")
    hub_us: dict[str, float] = {}
    for cmd in sorted(set(hub_sum) | set(hub_sum_b) | set(hub_n) | set(hub_n_b)):
        ds = hub_sum.get(cmd, 0.0) - hub_sum_b.get(cmd, 0.0)
        dn = hub_n.get(cmd, 0.0) - hub_n_b.get(cmd, 0.0)
        if dn > 0:
            hub_us[cmd] = ds / dn * 1e6
    inflight = sum_family(end, "mqttd_peer_forwards_in_flight")
    sessions = sum_family(end, "mqttd_sessions")
    drops = by_label(after, "mqttd_publish_dropped_total", "reason")
    drops_b = by_label(before, "mqttd_publish_dropped_total", "reason")
    drop_delta = {
        k: drops.get(k, 0.0) - drops_b.get(k, 0.0)
        for k in set(drops) | set(drops_b)
        if drops.get(k, 0.0) - drops_b.get(k, 0.0) > 0
    }
    meta = {}
    rt = rdir / "rung.txt"
    if rt.exists():
        for tok in rt.read_text().split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                meta[k] = v
    return {
        "path": rdir,
        "sites": meta.get("sites", rdir.name.split("-")[1] if "-" in rdir.name else rdir.name),
        "offered": meta.get("offered", "—"),
        "received": received,
        "forwarded": forwarded,
        "crossing": crossing,
        "forwarded_series_absent": sum_family(after, "mqttd_publish_forwarded_total") == 0
        and sum_family(before, "mqttd_publish_forwarded_total") == 0,
        "hub_us": hub_us,
        "inflight": inflight,
        "sessions": sessions,
        "drops": drop_delta,
        "broker_idle": mean_idle(rdir, "broker"),
        "driver_idle": mean_idle(rdir, "driver"),
        "settled": meta.get("settled", "—"),
        "drained": meta.get("drained", "—"),
    }


def find_rungs(root: Path) -> list[Path]:
    results = root / "results" if (root / "results").is_dir() else root
    return sorted(results.glob("nodes=*/laneE/sites-*"))


def format_crossing(r: dict) -> str:
    if r["crossing"] is None:
        return "n/a (received=0)"
    note = ""
    if r["forwarded_series_absent"] and r["received"] > 0:
        note = " [forwarded series absent → 0]"
    return f"{r['crossing'] * 100:.2f}%{note}"


def format_hub(r: dict) -> str:
    if not r["hub_us"]:
        return "—"
    return " ".join(f"{k}={v:.1f}µs" for k, v in sorted(r["hub_us"].items()))


def print_report(rungs: list[dict]) -> None:
    print(
        "nodes  sites  offered  received  forwarded  crossing  "
        "hub_dispatch_mean  peer_inflight  sessions  broker_idle  driver_idle  settled  drained"
    )
    for r in rungs:
        nodes = "—"
        for p in r["path"].parts:
            if p.startswith("nodes="):
                nodes = p.split("=", 1)[1]
        drops = ""
        if r["drops"]:
            drops = " drops=" + ",".join(f"{k}:{v:.0f}" for k, v in sorted(r["drops"].items()))
        print(
            f"{nodes:>5}  {str(r['sites']):>5}  {str(r['offered']):>7}  "
            f"{r['received']:.0f}  {r['forwarded']:.0f}  {format_crossing(r):<28}  "
            f"{format_hub(r):<40}  {r['inflight']:.0f}  {r['sessions']:.0f}  "
            f"{r['broker_idle']:>11}  {r['driver_idle']:>11}  {r['settled']}  {r['drained']}{drops}"
        )


class ExtractTests(unittest.TestCase):
    def test_absent_forwarded_with_received_is_zero_crossing(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "results/nodes=7/laneE/sites-10"
            rdir.mkdir(parents=True)
            (rdir / "rung.txt").write_text(
                "sites=10 offered=300000 settled=yes drained=yes\n"
            )
            (rdir / "metrics-before-broker0.prom").write_text(
                'mqttd_publish_received_total{qos="0"} 100\n'
                'mqttd_hub_dispatch_seconds_sum{command="publish"} 0.001\n'
                'mqttd_hub_dispatch_seconds_count{command="publish"} 100\n'
                "mqttd_sessions 0\n"
            )
            (rdir / "metrics-after-broker0.prom").write_text(
                'mqttd_publish_received_total{qos="0"} 30100\n'
                'mqttd_hub_dispatch_seconds_sum{command="publish"} 0.0013\n'
                'mqttd_hub_dispatch_seconds_count{command="publish"} 200\n'
                "mqttd_sessions 12\n"
                "mqttd_peer_forwards_in_flight 0\n"
            )
            cpu = rdir / "cpu"
            cpu.mkdir()
            (cpu / "cpu-driver0.txt").write_text(
                "Average:     all   20.00    0.00    10.00    0.00    0.00    0.00    0.00    0.00    0.00   70.00\n"
            )
            r = extract_rung(rdir)
            self.assertEqual(r["received"], 30000)
            self.assertEqual(r["forwarded"], 0)
            self.assertEqual(r["crossing"], 0.0)
            self.assertTrue(r["forwarded_series_absent"])
            self.assertAlmostEqual(r["hub_us"]["publish"], 3.0, places=6)
            self.assertEqual(r["driver_idle"], "70%")

    def test_does_not_prefix_match_received_gauge(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "r"
            rdir.mkdir()
            (rdir / "metrics-before-broker0.prom").write_text(
                "mqttd_publish_received 999999\n"
                'mqttd_publish_received_total{qos="0"} 1\n'
            )
            (rdir / "metrics-after-broker0.prom").write_text(
                "mqttd_publish_received 999999\n"
                'mqttd_publish_received_total{qos="0"} 11\n'
            )
            r = extract_rung(rdir)
            self.assertEqual(r["received"], 10)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("results", nargs="?", type=Path, help="run dir or results/ tree")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(ExtractTests)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if args.results is None:
        parser.error("results path required (or --self-test)")
    rungs = [extract_rung(p) for p in find_rungs(args.results)]
    if not rungs:
        print(f"no laneE sites-* under {args.results}", file=sys.stderr)
        return 2
    print_report(rungs)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
