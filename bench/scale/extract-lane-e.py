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
INVALID, never zero crossing.

A rung recorded by the aligned harness carries `metrics-window-{open,close}-*`
snapshots and `window.tsv`, taken by the same batch that baselines and closes the
consumer histograms. Rates, crossing, hub dispatch, drops and CPU idle then come
from that steady window only; each host's mpstat samples are kept only between
that host's own scrape stamps. An older rung has only before/after: it is
reported as UNALIGNED, with lifetime totals that include ramp and drain.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
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
# `mpstat -P ALL 1` under LC_ALL=C TZ=UTC (cpu.sh): one `all` row per second,
# stamped with the END of its one-second interval.
MPSTAT_ALL = re.compile(r"^(\d\d):(\d\d):(\d\d)\s+all\s")
STREAM_START = re.compile(r"(?m)^CPU_STREAM_START_UTC (\S+)\s*$")


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
        snapshots[name.split("broker", 1)[1].removesuffix(".prom")] = parsed
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


def windowed_idle(path: Path, lo_s: float, hi_s: float) -> float | None:
    """Mean %idle over the `all` rows whose whole interval lies inside [lo_s, hi_s].

    The row's printed second is truncated, so its real end is somewhere in
    [t, t+1): requiring t-1 >= lo and t+1 <= hi keeps only rows that cannot have
    sampled the ramp before the window or the teardown after it.
    """
    if not path.exists():
        return None
    text = path.read_text(errors="replace")
    m = STREAM_START.search(text)
    if not m:
        return None
    start = datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
    midnight = start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    idles: list[float] = []
    prev = None
    offset = 0.0
    for line in text.splitlines():
        row = MPSTAT_ALL.match(line)
        if not row:
            continue
        secs = int(row.group(1)) * 3600 + int(row.group(2)) * 60 + int(row.group(3))
        if prev is None and midnight + secs < start.timestamp() - 1:
            offset += 86400  # the stream started just before midnight
        elif prev is not None and secs < prev:
            offset += 86400
        prev = secs
        t = midnight + offset + secs
        if t - 1 >= lo_s and t + 1 <= hi_s:
            idles.append(float(line.split()[-1]))
    return sum(idles) / len(idles) if idles else None


def mean_idle(rdir: Path, role: str, window: dict | None = None) -> str:
    files = sorted((rdir / "cpu").glob(f"cpu-{role}*.txt"))
    if not files:
        return "—"
    if window is None:
        vals = [mpstat_idle_mean(p) for p in files]
    else:
        vals = []
        for p in files:
            host = p.stem.split("-", 1)[1]
            if host in window:
                lo, hi = window[host]["open"][1] / 1000, window[host]["close"][0] / 1000
                vals.append(windowed_idle(p, lo, hi))
    vals = [v for v in vals if v is not None]
    if not vals:
        return "—"
    return f"{sum(vals) / len(vals):.0f}%"


def load_window(rdir: Path, nodes: int) -> dict[str, dict[str, tuple[int, int]]]:
    """Per-host scrape stamps (ms, that host's clock). Every broker must have both edges."""
    path = rdir / "window.tsv"
    if not path.is_file():
        raise ValueError(f"window snapshots without window.tsv: {rdir}")
    hosts: dict[str, dict[str, tuple[int, int]]] = {}
    for line in path.read_text().splitlines()[1:]:
        parts = line.split("\t")
        if len(parts) != 4 or parts[1] not in ("open", "close"):
            raise ValueError(f"malformed window.tsv row: {line!r}")
        host, phase, start, end = parts
        if not (start.isdigit() and end.isdigit()) or int(end) < int(start):
            # An empty stamp is a scrape that never reached the host.
            if host.startswith("broker"):
                raise ValueError(f"no usable {phase} stamp for {host}: {line!r}")
            continue
        hosts.setdefault(host, {})[phase] = (int(start), int(end))
    for i in range(nodes):
        edges = hosts.get(f"broker{i}", {})
        if set(edges) != {"open", "close"}:
            raise ValueError(f"window.tsv lacks both edges for broker{i}")
        if edges["close"][0] <= edges["open"][1]:
            raise ValueError(f"window for broker{i} closes before it opens")
    return {h: e for h, e in hosts.items() if set(e) == {"open", "close"}}


def window_seconds(edges: dict[str, tuple[int, int]]) -> float:
    mid = lambda e: (e[0] + e[1]) / 2  # noqa: E731
    return (mid(edges["close"]) - mid(edges["open"])) / 1000


def rate(starts: dict[str, dict], ends: dict[str, dict], window: dict, name: str) -> float:
    """Σ over brokers of Δcounter / that broker's own window length."""
    return sum(
        (sum_family(ends[b], name) - sum_family(starts[b], name)) / window_seconds(window[f"broker{b}"])
        for b in starts
    )


def extract_rung(rdir: Path) -> dict:
    starts, ends = load_snap(rdir, "before"), load_snap(rdir, "after")
    validate_deltas(starts, ends)
    lifetime_before, lifetime_after = merge_snap(starts), merge_snap(ends)
    end = lifetime_after
    if list(rdir.glob("metrics-drain-broker*.prom")):
        drains = load_snap(rdir, "drain")
        validate_deltas(starts, drains)
        validate_deltas(drains, ends)
        end = merge_snap(drains)
    meta = {}
    rt = rdir / "rung.txt"
    if rt.exists():
        for tok in rt.read_text().split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                meta[k] = v
    nodes = len(starts)
    aligned = (
        meta.get("window") == "aligned"
        or (rdir / "window.tsv").exists()
        or any(rdir.glob("metrics-window-*-broker*.prom"))
    )
    window = None
    window_s = recv_rate = deliv_rate = None
    if aligned:
        # Any trace of the aligned harness makes the whole window mandatory: a
        # half-present window silently falling back to lifetime totals would
        # report ramp and drain as steady work.
        window = load_window(rdir, nodes)
        opens, closes = load_snap(rdir, "window-open"), load_snap(rdir, "window-close")
        validate_deltas(starts, opens)
        validate_deltas(opens, closes)
        validate_deltas(closes, ends)
        before, after = merge_snap(opens), merge_snap(closes)
        window_s = sum(window_seconds(window[f"broker{b}"]) for b in opens) / nodes
        recv_rate = rate(opens, closes, window, "mqttd_publish_received_total")
        deliv_rate = rate(opens, closes, window, "mqttd_publish_delivered_total")
    else:
        before, after = lifetime_before, lifetime_after
    received = delta(before, after, "mqttd_publish_received_total")
    forwarded = delta(before, after, "mqttd_publish_forwarded_total")
    if received <= 0:
        raise ValueError("no positive received delta; crossing is unknown")
    crossing = forwarded / received
    lifetime_received = delta(lifetime_before, lifetime_after, "mqttd_publish_received_total")
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
    return {
        "path": rdir,
        "aligned": aligned,
        "window_s": window_s,
        "recv_rate": recv_rate,
        "deliv_rate": deliv_rate,
        "per_node_deliv": deliv_rate / nodes if deliv_rate is not None else None,
        "lifetime_received": lifetime_received,
        "cpu_window": meta.get("cpu_window", "—"),
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
        "broker_idle": mean_idle(rdir, "broker", window),
        "driver_idle": mean_idle(rdir, "driver", window),
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
        "nodes  sites  offered  window  window_s  recv/s  deliv/s  deliv/s/node  received  forwarded  crossing  "
        "hub_dispatch_mean  peer_inflight  sessions  broker_idle  driver_idle  cpu_window  settled  drained"
    )
    num = lambda v: "—" if v is None else f"{v:.0f}"  # noqa: E731
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
            f"{'aligned' if r['aligned'] else 'UNALIGNED':<9}  {num(r['window_s']):>8}  "
            f"{num(r['recv_rate']):>6}  {num(r['deliv_rate']):>7}  {num(r['per_node_deliv']):>12}  "
            f"{r['received']:.0f}  {r['forwarded']:.0f}  {format_crossing(r):<28}  "
            f"{format_hub(r):<40}  {r['inflight']:.0f}  {r['sessions']:.0f}  "
            f"{r['broker_idle']:>11}  {r['driver_idle']:>11}  {r['cpu_window']:>10}  "
            f"{r['settled']}  {r['drained']}{drops}"
        )


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


class WindowTests(unittest.TestCase):
    OPEN_MS, CLOSE_MS = 1_789_000_010_000, 1_789_000_070_000  # 60 s apart

    def aligned_rung(self, root: Path, nodes: int = 2) -> Path:
        rdir = root / f"results/nodes={nodes}/laneE/sites-4"
        (rdir / "cpu").mkdir(parents=True)
        (rdir / "rung.txt").write_text("sites=4 offered=120000 window=aligned cpu_window=aligned settled=yes drained=yes\n")
        # Lifetime counters carry ramp and drain; the window must not.
        snaps = {"before": (0, 0, 0), "window-open": (50_000, 500, 100_000),
                 "window-close": (1_850_000, 500, 3_700_000), "after": (2_000_000, 600, 4_000_000)}
        for node in range(nodes):
            for label, (recv, fwd, deliv) in snaps.items():
                (rdir / f"metrics-{label}-broker{node}.prom").write_text(
                    "# TYPE mqttd_publish_forwarded counter\n"
                    f'mqttd_publish_received_total{{qos="0"}} {recv}\n'
                    f"mqttd_publish_forwarded_total {fwd}\n"
                    f'mqttd_publish_delivered_total{{qos="0"}} {deliv}\n'
                    f'mqttd_hub_dispatch_seconds_sum{{command="publish"}} {recv * 4e-6}\n'
                    f'mqttd_hub_dispatch_seconds_count{{command="publish"}} {recv}\n'
                )
        rows = ["host\tphase\tstart_ms\tend_ms"]
        for host in [f"broker{n}" for n in range(nodes)] + ["driver0"]:
            rows.append(f"{host}\topen\t{self.OPEN_MS - 200}\t{self.OPEN_MS + 200}")
            rows.append(f"{host}\tclose\t{self.CLOSE_MS - 200}\t{self.CLOSE_MS + 200}")
        (rdir / "window.tsv").write_text("\n".join(rows) + "\n")
        start = datetime.fromtimestamp(self.OPEN_MS / 1000 - 30, timezone.utc)
        lines = [f"CPU_STREAM_START_UTC {start:%Y-%m-%dT%H:%M:%SZ}"]
        for t in range(int(self.OPEN_MS / 1000) - 29, int(self.CLOSE_MS / 1000) + 30):
            # 0% idle during the ramp, 10% during teardown, 80% inside the window.
            idle = 0.0 if t * 1000 <= self.OPEN_MS else 10.0 if t * 1000 >= self.CLOSE_MS else 80.0
            lines.append(f"{datetime.fromtimestamp(t, timezone.utc):%H:%M:%S}     all    1.00    0.00    1.00    0.00    0.00    0.00    0.00    0.00    0.00   {idle:.2f}")
        for host in ["broker0", "driver0"]:
            (rdir / "cpu" / f"cpu-{host}.txt").write_text("\n".join(lines) + "\n")
        return rdir

    def test_aligned_rung_reads_the_window_not_the_lifetime(self):
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self.aligned_rung(Path(td)))
            self.assertTrue(r["aligned"])
            self.assertAlmostEqual(r["window_s"], 60.0)
            self.assertEqual(r["received"], 3_600_000)
            self.assertEqual(r["lifetime_received"], 4_000_000)
            self.assertAlmostEqual(r["recv_rate"], 60_000.0)
            self.assertAlmostEqual(r["deliv_rate"], 120_000.0)
            self.assertAlmostEqual(r["per_node_deliv"], 60_000.0)
            self.assertEqual(r["forwarded"], 0)
            self.assertEqual(r["crossing"], 0.0)
            self.assertAlmostEqual(r["hub_us"]["publish"], 4.0, places=6)
            # Ramp (0%) and teardown (10%) samples are outside the window.
            self.assertEqual(r["broker_idle"], "80%")
            self.assertEqual(r["driver_idle"], "80%")

    def test_a_partial_window_is_invalid_not_a_lifetime_fallback(self):
        for defect in ("close-missing", "tsv-missing", "edge-missing", "inverted", "window-reset"):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as td:
                rdir = self.aligned_rung(Path(td))
                if defect == "close-missing":
                    (rdir / "metrics-window-close-broker1.prom").unlink()
                elif defect == "tsv-missing":
                    (rdir / "window.tsv").unlink()
                elif defect == "edge-missing":
                    rows = (rdir / "window.tsv").read_text().splitlines()
                    (rdir / "window.tsv").write_text("\n".join(r for r in rows if not r.startswith("broker1\tclose")) + "\n")
                elif defect == "inverted":
                    text = (rdir / "window.tsv").read_text()
                    (rdir / "window.tsv").write_text(text.replace(f"broker0\tclose\t{self.CLOSE_MS - 200}", f"broker0\tclose\t{self.OPEN_MS - 500}"))
                else:
                    path = rdir / "metrics-window-close-broker0.prom"
                    path.write_text(path.read_text().replace("mqttd_publish_received_total{qos=\"0\"} 1850000", "mqttd_publish_received_total{qos=\"0\"} 10"))
                with self.assertRaises(ValueError):
                    extract_rung(rdir)

    def test_pre_window_rung_is_reported_unaligned(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "nodes=1/laneE/sites-1"
            rdir.mkdir(parents=True)
            for label, recv in (("before", 0), ("after", 100)):
                (rdir / f"metrics-{label}-broker0.prom").write_text(
                    f"mqttd_publish_received_total {recv}\nmqttd_publish_forwarded_total 0\n"
                )
            r = extract_rung(rdir)
            self.assertFalse(r["aligned"])
            self.assertIsNone(r["recv_rate"])
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                self.assertEqual(main([str(Path(td))]), 0)
            self.assertIn("UNALIGNED", stdout.getvalue())

    def test_mpstat_rows_across_midnight_stay_in_order(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "cpu-broker0.txt"
            base = datetime(2026, 9, 14, 23, 59, 50, tzinfo=timezone.utc).timestamp()
            lines = ["CPU_STREAM_START_UTC 2026-09-14T23:59:50Z"]
            for t in range(int(base) + 1, int(base) + 21):
                lines.append(f"{datetime.fromtimestamp(t, timezone.utc):%H:%M:%S}  all  0 0 0 0 0 0 0 0 0 {50.0 if t > base + 10 else 0.0}")
            path.write_text("\n".join(lines) + "\n")
            # 00:00:01 .. 00:00:10 after midnight only.
            self.assertEqual(windowed_idle(path, base + 10, base + 21), 50.0)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("results", nargs="?", type=Path, help="run dir or results/ tree")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        loader = unittest.defaultTestLoader
        suite = unittest.TestSuite([loader.loadTestsFromTestCase(ExtractTests), loader.loadTestsFromTestCase(WindowTests)])
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
