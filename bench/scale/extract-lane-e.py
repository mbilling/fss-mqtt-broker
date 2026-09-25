#!/usr/bin/env python3
"""Walk a scale-curve results tree and print Lane E crossing / hub / idle hints.

Usage:
  python3 extract-lane-e.py .runs/<stamp>/results
  python3 extract-lane-e.py --self-test

Reads broker Prometheus snapshots under results/nodes=*/laneE/sites-*/.
Does not sum emqtt-bench pub rate= log lines.

Crossing = Δ mqttd_publish_forwarded_total / Δ mqttd_publish_received_total.
Lazily absent forwarded samples mean zero only in complete, validated snapshots
that declare the counter. Missing scrapes, unsupported counters and resets are
INVALID, never zero crossing. Totals include ramp/drain, not steady throughput.

Ingress skew = busiest broker's Δ received / mean broker Δ received (#613).
With shared prefer-local, delivery work follows the PUBLISHER's broker, and the
only spillover is a full subscriber socket, never a saturated hub. So once the
busiest broker saturates, the cluster delivers capacity × N / skew: the
`eff_nodes = N / skew` column is how many brokers' worth of work the rung could
ever use. N=7 at skew 1.4 is five nodes — a flat 5→7 with no broker defect.
Per-broker crossing is printed beside it: ingress skew only equals WORK skew
while forwarding stays near zero on every broker, not merely on average.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import math
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
    if not path.is_file() or not path.stat().st_size:
        raise ValueError(f"missing/empty scrape: {path}")
    for line in path.read_text(errors="replace").splitlines():
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE.match(line)
        if not m:
            raise ValueError(f"malformed scrape sample in {path}: {line}")
        key = (m.group("name"), m.group("labels") or "")
        if key in out:
            raise ValueError(f"duplicate scrape sample in {path}: {key}")
        value = float(m.group("v"))
        if not math.isfinite(value):
            raise ValueError(f"non-finite scrape sample in {path}: {key}")
        out[key] = value
    if not out:
        raise ValueError(f"scrape has no samples: {path}")
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


def load_snap(rdir: Path, label: str) -> dict[str, dict]:
    nodes = next((int(p.split("=", 1)[1]) for p in rdir.parts if p.startswith("nodes=")), None)
    if not nodes:
        raise ValueError(f"expected nodes=N in results path: {rdir}")
    expected = {f"metrics-{label}-broker{i}.prom" for i in range(nodes)}
    actual = {p.name for p in rdir.glob(f"metrics-{label}-broker*.prom")}
    if actual != expected:
        raise ValueError(f"incomplete {label} broker coverage: expected {sorted(expected)}, got {sorted(actual)}")
    snapshots = {}
    for name in sorted(expected):
        path = rdir / name
        parsed = parse_prom(path)
        text = path.read_text()
        for family in ("mqttd_publish_received", "mqttd_publish_forwarded"):
            if not any(n == family + "_total" for n, _ in parsed) and not re.search(
                rf"(?m)^# TYPE {family} counter\s*$", text
            ):
                raise ValueError(f"unsupported/missing {family} counter: {path}")
        snapshots[name.split("broker", 1)[1]] = parsed
    return snapshots


def merge_snap(snapshots: dict[str, dict]) -> dict:
    merged = {}
    for parsed in snapshots.values():
        for k, v in parsed.items():
            merged[k] = merged.get(k, 0.0) + v
    return merged


def validate_deltas(before: dict[str, dict], after: dict[str, dict]) -> None:
    for broker, start in before.items():
        end = after[broker]
        for key, value in start.items():
            name, _ = key
            if name.endswith(("_total", "_count", "_sum")) and end.get(key, 0) < value:
                raise ValueError(f"counter reset/disappeared on broker {broker}: {key}")
            if name == "process_start_time_seconds" and end.get(key) != value:
                raise ValueError(f"process changed on broker {broker}")


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
    starts, ends = load_snap(rdir, "before"), load_snap(rdir, "after")
    validate_deltas(starts, ends)
    before, after = merge_snap(starts), merge_snap(ends)
    end = after
    if list(rdir.glob("metrics-drain-broker*.prom")):
        drains = load_snap(rdir, "drain")
        validate_deltas(starts, drains)
        validate_deltas(drains, ends)
        end = merge_snap(drains)
    received = delta(before, after, "mqttd_publish_received_total")
    forwarded = delta(before, after, "mqttd_publish_forwarded_total")
    if received <= 0:
        raise ValueError("no positive received delta; crossing is unknown")
    crossing = forwarded / received
    per_node = per_broker(starts, ends)
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
        "forwarded_series_absent": not any(
            n == "mqttd_publish_forwarded_total" for n, _ in set(before) | set(after)
        ),
        "hub_us": hub_us,
        "inflight": inflight,
        "sessions": sessions,
        "drops": drop_delta,
        "broker_idle": mean_idle(rdir, "broker"),
        "driver_idle": mean_idle(rdir, "driver"),
        "settled": meta.get("settled", "—"),
        "drained": meta.get("drained", "—"),
        "per_node": per_node,
        **skew(per_node),
    }


def per_broker(starts: dict[str, dict], ends: dict[str, dict]) -> dict[str, dict]:
    """Δ received and Δ forwarded for EACH broker, before any merge.

    The merged totals answer "how much did the cluster take"; they cannot say
    which broker took it, and under prefer-local that is the question.
    """
    # `load_snap` keys brokers by the tail of the file name ("0.prom"); strip it
    # here rather than change a key other callers already rely on.
    def index(key: str) -> str:
        return key.removesuffix(".prom")

    out = {}
    for broker in sorted(starts, key=lambda k: int(index(k))):
        rx = delta(starts[broker], ends[broker], "mqttd_publish_received_total")
        fx = delta(starts[broker], ends[broker], "mqttd_publish_forwarded_total")
        out[index(broker)] = {"received": rx, "forwarded": fx, "crossing": fx / rx if rx > 0 else None}
    return out


def skew(per_node: dict[str, dict]) -> dict:
    """Busiest-over-mean ingress, and the broker count that implies.

    `eff_nodes` is N / skew: the number of brokers' worth of work the rung can
    use once its busiest broker saturates. It is a CEILING on useful scale-out
    under prefer-local, not a measurement of saturation — a rung whose busiest
    broker still has headroom is not limited by it yet.
    """
    rx = [n["received"] for n in per_node.values()]
    mean = sum(rx) / len(rx) if rx else 0.0
    if mean <= 0:
        return {"rx_skew": None, "eff_nodes": None}
    s = max(rx) / mean
    return {"rx_skew": s, "eff_nodes": len(rx) / s}


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
        "hub_dispatch_mean  peer_inflight  sessions  broker_idle  driver_idle  settled  drained  "
        "rx_skew  eff_nodes"
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
            f"{r['broker_idle']:>11}  {r['driver_idle']:>11}  {r['settled']}  {r['drained']}  "
            f"{format_skew(r)}{drops}"
        )
        print("       per-broker " + format_per_node(r))


def format_skew(r: dict) -> str:
    if r["rx_skew"] is None:
        return "—  —"
    return f"{r['rx_skew']:.2f}  {r['eff_nodes']:.1f}"


def format_per_node(r: dict) -> str:
    """`b<i>=<Δreceived>/<crossing%>` per broker, in broker order."""
    cells = []
    for broker, n in r["per_node"].items():
        x = "—" if n["crossing"] is None else f"{n['crossing'] * 100:.1f}%"
        cells.append(f"b{broker}={n['received']:.0f}/{x}")
    return " ".join(cells)


class ExtractTests(unittest.TestCase):
    def test_incomplete_reset_and_unsupported_scrapes_are_invalid(self):
        for defect in ("missing", "empty", "reset", "unsupported", "partial-drain", "malformed", "duplicate", "nonfinite"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                rdir = root / "nodes=2/laneE/sites-1"
                rdir.mkdir(parents=True)
                for node in range(2):
                    for label, recv, fwd in (("before", 100, 100), ("after", 1000, 300)):
                        (rdir / f"metrics-{label}-broker{node}.prom").write_text(
                            f"mqttd_publish_received_total {recv}\nmqttd_publish_forwarded_total {fwd}\n"
                        )
                bad = rdir / "metrics-after-broker1.prom"
                if defect == "missing":
                    bad.unlink()
                elif defect == "empty":
                    bad.write_text("")
                elif defect == "reset":
                    # Aggregate forwards still rise; only per-node validation sees it.
                    bad.write_text("mqttd_publish_received_total 1000\nmqttd_publish_forwarded_total 50\n")
                elif defect == "unsupported":
                    bad.write_text("mqttd_publish_received_total 1000\n")
                elif defect == "partial-drain":
                    (rdir / "metrics-drain-broker0.prom").write_text(bad.read_text())
                elif defect == "malformed":
                    bad.write_text("HTTP scrape failed\n")
                elif defect == "duplicate":
                    bad.write_text(bad.read_text() + "mqttd_publish_forwarded_total 300\n")
                else:
                    bad.write_text("mqttd_publish_received_total 1e999\nmqttd_publish_forwarded_total 300\n")
                with self.assertRaises(ValueError):
                    extract_rung(rdir)
                stderr, stdout = io.StringIO(), io.StringIO()
                with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(stdout):
                    self.assertEqual(main([str(root)]), 1)
                self.assertIn("INVALID", stderr.getvalue())
                self.assertNotIn("0.00%", stdout.getvalue())

    def test_explicit_zero_is_not_an_absent_series(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "nodes=1/laneE/sites-1"
            rdir.mkdir(parents=True)
            for label, recv in (("before", 0), ("after", 100)):
                (rdir / f"metrics-{label}-broker0.prom").write_text(
                    f"mqttd_publish_received_total {recv}\nmqttd_publish_forwarded_total 0\n"
                )
            result = extract_rung(rdir)
            self.assertEqual(result["crossing"], 0)
            self.assertFalse(result["forwarded_series_absent"])

    def test_absent_forwarded_with_received_is_zero_crossing(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "results/nodes=1/laneE/sites-10"
            rdir.mkdir(parents=True)
            (rdir / "rung.txt").write_text(
                "sites=10 offered=300000 settled=yes drained=yes\n"
            )
            (rdir / "metrics-before-broker0.prom").write_text(
                '# TYPE mqttd_publish_forwarded counter\n'
                'mqttd_publish_received_total{qos="0"} 100\n'
                'mqttd_hub_dispatch_seconds_sum{command="publish"} 0.001\n'
                'mqttd_hub_dispatch_seconds_count{command="publish"} 100\n'
                "mqttd_sessions 0\n"
            )
            (rdir / "metrics-after-broker0.prom").write_text(
                '# TYPE mqttd_publish_forwarded counter\n'
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
            rdir = Path(td) / "nodes=1/laneE/sites-1"
            rdir.mkdir(parents=True)
            (rdir / "metrics-before-broker0.prom").write_text(
                "mqttd_publish_received 999999\n"
                '# TYPE mqttd_publish_forwarded counter\n'
                'mqttd_publish_received_total{qos="0"} 1\n'
            )
            (rdir / "metrics-after-broker0.prom").write_text(
                "mqttd_publish_received 999999\n"
                '# TYPE mqttd_publish_forwarded counter\n'
                'mqttd_publish_received_total{qos="0"} 11\n'
            )
            r = extract_rung(rdir)
            self.assertEqual(r["received"], 10)


class SkewTests(unittest.TestCase):
    """Ingress skew (#613 candidate 3): the busiest broker bounds prefer-local."""

    @staticmethod
    def _rung(root: Path, nodes: int, per_node_rx: list[int], per_node_fx: list[int] | None = None) -> Path:
        rdir = root / f"nodes={nodes}/laneE/sites-10"
        rdir.mkdir(parents=True)
        fx = per_node_fx or [0] * nodes
        for i in range(nodes):
            (rdir / f"metrics-before-broker{i}.prom").write_text(
                "mqttd_publish_received_total 0\nmqttd_publish_forwarded_total 0\n"
            )
            (rdir / f"metrics-after-broker{i}.prom").write_text(
                f"mqttd_publish_received_total {per_node_rx[i]}\n"
                f"mqttd_publish_forwarded_total {fx[i]}\n"
            )
        return rdir

    def test_an_even_split_uses_every_broker(self):
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self._rung(Path(td), 5, [60_000] * 5))
            self.assertAlmostEqual(r["rx_skew"], 1.0)
            self.assertAlmostEqual(r["eff_nodes"], 5.0)

    def test_five_drivers_over_seven_brokers_cap_below_seven(self):
        # The shape that voided the old ladders: five equal publisher pools
        # landing on seven brokers, two of which take a double share. The
        # merged total looks healthy; the busiest broker says the rung can use
        # at most 4.5 brokers' worth of prefer-local work.
        with tempfile.TemporaryDirectory() as td:
            rx = [2, 2, 1, 1, 1, 1, 1]
            r = extract_rung(self._rung(Path(td), 7, [v * 30_000 for v in rx]))
            self.assertAlmostEqual(r["rx_skew"], 2 / (9 / 7))
            self.assertAlmostEqual(r["eff_nodes"], 4.5)
            self.assertLess(r["eff_nodes"], 5.0)

    def test_crossing_is_judged_per_broker_not_on_the_aggregate(self):
        # 1% crossing overall hides one broker forwarding a fifth of its
        # ingress. Ingress skew equals WORK skew only when every broker is
        # near zero, so the per-broker figure is the one that decides it.
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self._rung(Path(td), 5, [100_000] * 5, [0, 0, 0, 0, 5_000]))
            self.assertAlmostEqual(r["crossing"], 0.01)
            self.assertAlmostEqual(r["per_node"]["4"]["crossing"], 0.05)
            self.assertEqual(r["per_node"]["0"]["crossing"], 0.0)
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                print_report([r])
            self.assertIn("b4=100000/5.0%", out.getvalue())

    def test_the_busiest_broker_is_found_whatever_its_index(self):
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self._rung(Path(td), 3, [10, 10, 40]))
            self.assertAlmostEqual(r["rx_skew"], 2.0)
            self.assertAlmostEqual(r["eff_nodes"], 1.5)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("results", nargs="?", type=Path, help="run dir or results/ tree")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        suite = unittest.TestSuite(
            unittest.defaultTestLoader.loadTestsFromTestCase(case) for case in (ExtractTests, SkewTests)
        )
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if args.results is None:
        parser.error("results path required (or --self-test)")
    paths = find_rungs(args.results)
    if not paths:
        print(f"no laneE sites-* under {args.results}", file=sys.stderr)
        return 2
    rungs, invalid = [], False
    for path in paths:
        try:
            rungs.append(extract_rung(path))
        except (ValueError, OSError) as exc:
            print(f"INVALID {path}: {exc}", file=sys.stderr)
            invalid = True
    print_report(rungs)
    return 1 if invalid else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
