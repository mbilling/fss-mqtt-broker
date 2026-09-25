#!/usr/bin/env python3
"""Walk a scale-curve results tree and print Lane E crossing / hub / idle hints.

Usage:
  python3 extract-lane-e.py .runs/<stamp>/results
  python3 extract-lane-e.py --crossing-gate 0.5 .runs/<stamp>/results
  python3 extract-lane-e.py --self-test

Reads broker Prometheus snapshots under results/nodes=*/laneE/sites-*/.
Does not sum emqtt-bench pub rate= log lines.

Crossing = Δ mqttd_publish_forwarded_total (all reasons) / Δ
mqttd_publish_received_total, for the cluster AND for each broker on its own: an
aggregate near zero can hide one broker forwarding a large share of its traffic.

Ingress skew (#613) = the busiest broker's Δ received over the mean broker's.
With shared prefer-local, delivery work follows the PUBLISHER's broker, and the
only spillover is a full subscriber socket, never a saturated hub. So once the
busiest broker saturates, the cluster delivers capacity × N / skew, and
`eff_nodes = N / rx_skew` is the most brokers' worth of work the rung can use.
Five driver pools over seven brokers at skew 1.4 is five nodes: a flat 5→7 with
no broker defect. It equals WORK skew only while crossing stays near zero on
every broker, which is why it sits beside max_broker.

A zero is only a measurement if forwarding was observable (#482). mqttd's
prometheus-client omits a labelled family that has no children — no HELP, no
TYPE, no sample — so on a healthy prefer-local run the forwarded family never
appears, and "absent" reads the same as "never exported" or "counter lost to a
restart". Every rung therefore carries a crossing certificate or is INVALID:

  structural  nodes=1 and every broker snapshot reports mqttd_peer_links 0:
              there is no peer to forward to.
  canary      nodes>=2 and the size's forwarding positive control
              (laneE/forward-canary/, run by run-curve.sh before calibration)
              re-derives as a pass through forward-canary.py's own ledger — the
              status line alone is never trusted — AND every snapshot of every
              broker still carries at least that broker's canary
              forwarded{reason="shared-remote"} and received totals (the same
              process, with the series present), AND the broker MainPID that
              window.tsv records at both window edges is the one read right after
              the canary (an aligned rung without that column is not certified).
              A passing control binds nodes=1 rungs the same way; a failed one
              makes every rung of its size INVALID.

Missing, empty, truncated (no trailing '# EOF'), malformed, duplicate or
non-finite scrapes and per-broker counter resets are INVALID as well, never zero.

A rung recorded by the aligned harness carries `metrics-window-{open,close}-*`
snapshots and `window.tsv`, taken by the same batch that baselines and closes the
consumer histograms. Rates, crossing, hub dispatch, window drops and CPU idle
then come from that steady window only; each host's mpstat samples are kept only
between that host's own scrape stamps. Lifetime received/delivered/drops
(before -> drain, or after) are printed beside them for the drain-vs-broker
delivery check. An older rung has only before/after: it is reported as
UNALIGNED, with lifetime totals that include ramp and drain.

--crossing-gate PCT prints one `GATE nodes=N PASS|FAIL <reasons>` line per size
and exits 1 on any FAIL. PASS needs every sites-* rung of the size valid,
aligned and certified, peer_links == N-1 on every broker at both window edges,
no idle broker, and every broker's own crossing <= PCT percent.
"""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
import importlib.util
import io
import math
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The ledger is imported, not re-implemented: run-curve.sh's verify step and this
# extractor must certify from the same code, or one day they will disagree.
CANARY_SCRIPT = HERE / "forward-canary.py"
FIXTURES = HERE / "testdata" / "lane-e"

PROM_LINE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<v>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)\s*$"
)
# `mpstat -P ALL 1` under LC_ALL=C TZ=UTC (cpu.sh): one `all` row per second,
# stamped with the END of its one-second interval.
MPSTAT_ALL = re.compile(r"^(\d\d):(\d\d):(\d\d)\s+all\s")
MPSTAT_AVERAGE = re.compile(r"^Average:\s+all\s")
STREAM_START = re.compile(r"(?m)^CPU_STREAM_START_UTC (\S+)\s*$")
NODES_DIR = re.compile(r"^nodes=(\d+)$")
RUNG_DIR = re.compile(r"^sites-(\d+)(?:-rep(\d+))?$")
HOST = re.compile(r"^(broker|driver)(\d+)$")

RECEIVED = "mqttd_publish_received_total"
FORWARDED = "mqttd_publish_forwarded_total"
DELIVERED = "mqttd_publish_delivered_total"
DROPPED = "mqttd_publish_dropped_total"
PEER_LINKS = "mqttd_peer_links"
# A scrape takes time. Once the widest one is more than this share of the window,
# "the window" has soft edges and its rates deserve a second look.
BRACKET_SHARE = 0.02


def parse_prom(path: Path) -> dict[tuple[str, str], float]:
    """Exact name+labels → value. Prefix matches are refused (received vs received_total)."""
    out: dict[tuple[str, str], float] = {}
    if not path.is_file() or not path.stat().st_size:
        raise ValueError(f"missing/empty scrape: {path}")
    lines = path.read_text(errors="replace").splitlines()
    # mqttd ends every exposition with `# EOF`. A scrape cut short by a timeout or
    # a dropped ssh still parses line by line — and reads as low counters.
    tail = next((line.strip() for line in reversed(lines) if line.strip()), "")
    if tail != "# EOF":
        raise ValueError(f"truncated scrape (last non-empty line is not '# EOF'): {path}")
    for line in lines:
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


def peer_links(parsed: dict[tuple[str, str], float]) -> float | None:
    return parsed.get((PEER_LINKS, ""))


def nodes_of(rdir: Path) -> int:
    for part in reversed(Path(rdir).parts):
        m = NODES_DIR.match(part)
        if m and int(m.group(1)) > 0:
            return int(m.group(1))
    raise ValueError(f"expected nodes=N in results path: {rdir}")


def load_snap(rdir: Path, label: str, nodes: int) -> dict[int, dict]:
    """Every broker's scrape for one label, or ValueError. Which families a scrape
    carries is not judged here: an absent forwarded family is the certificate's
    question, not a parse error."""
    expected = {f"metrics-{label}-broker{i}.prom" for i in range(nodes)}
    actual = {p.name for p in rdir.glob(f"metrics-{label}-broker*.prom")}
    if actual != expected:
        raise ValueError(f"incomplete {label} broker coverage: expected {sorted(expected)}, got {sorted(actual)}")
    return {i: parse_prom(rdir / f"metrics-{label}-broker{i}.prom") for i in range(nodes)}


def merge_snap(snapshots: dict[int, dict]) -> dict:
    merged: dict[tuple[str, str], float] = {}
    for parsed in snapshots.values():
        for k, v in parsed.items():
            merged[k] = merged.get(k, 0.0) + v
    return merged


def validate_deltas(before: dict[int, dict], after: dict[int, dict], first: str = "before", then: str = "after") -> None:
    for broker, start in before.items():
        end = after[broker]
        for key, value in start.items():
            name, _ = key
            if name.endswith(("_total", "_count", "_sum", "_bucket")) and end.get(key, 0) < value:
                raise ValueError(f"counter reset/disappeared on broker{broker} between {first} and {then}: {key}")
            if name == "process_start_time_seconds" and end.get(key) != value:
                raise ValueError(f"process changed on broker{broker} between {first} and {then}")


def delta(a: dict[tuple[str, str], float], b: dict[tuple[str, str], float], name: str) -> float:
    return sum_family(b, name) - sum_family(a, name)


def positive_deltas(a: dict, b: dict, name: str, label: str) -> dict[str, float]:
    after, before = by_label(b, name, label), by_label(a, name, label)
    out = {k: after.get(k, 0.0) - before.get(k, 0.0) for k in set(after) | set(before)}
    return {k: v for k, v in out.items() if v > 0}


def read_meta(rdir: Path) -> dict[str, str]:
    meta: dict[str, str] = {}
    rt = rdir / "rung.txt"
    if rt.exists():
        for tok in rt.read_text().split():
            if "=" in tok:
                k, v = tok.split("=", 1)
                meta[k] = v
    return meta


# ── CPU idle ────────────────────────────────────────────────────────────────


def mpstat_rows(path: Path) -> tuple[list[tuple[float | None, float]], list[float]]:
    """((interval-end epoch seconds or None, %idle) per `all` row, `Average:` idles)."""
    text = path.read_text(errors="replace")
    m = STREAM_START.search(text)
    start_s = midnight = None
    if m:
        # A garbled marker leaves the rows unplaceable, so the host reads as
        # missing: CPU is a hint and must not invalidate the rung's broker evidence.
        with contextlib.suppress(ValueError):
            start = datetime.fromisoformat(m.group(1).replace("Z", "+00:00")).astimezone(timezone.utc)
            start_s = start.timestamp()
            midnight = start.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    rows: list[tuple[float | None, float]] = []
    averages: list[float] = []
    prev = None
    offset = 0.0
    for line in text.splitlines():
        parts = line.split()
        try:
            idle = float(parts[-1])
        except (IndexError, ValueError):
            continue
        if MPSTAT_AVERAGE.match(line):
            averages.append(idle)
            continue
        row = MPSTAT_ALL.match(line)
        if not row:
            continue
        if midnight is None or start_s is None:
            rows.append((None, idle))
            continue
        secs = int(row.group(1)) * 3600 + int(row.group(2)) * 60 + int(row.group(3))
        if prev is None and midnight + secs < start_s - 1:
            offset += 86400  # the stream started just before midnight
        elif prev is not None and secs < prev:
            offset += 86400
        prev = secs
        rows.append((midnight + offset + secs, idle))
    return rows, averages


def windowed_idle(path: Path, lo_s: float, hi_s: float) -> list[float]:
    """%idle of the `all` rows whose whole interval lies inside [lo_s, hi_s].

    The row's printed second is truncated, so its real end is somewhere in
    [t, t+1): requiring t-1 >= lo and t+1 <= hi keeps only rows that cannot have
    sampled the ramp before the window or the teardown after it.
    """
    if not path.exists():
        return []
    rows, _ = mpstat_rows(path)
    return [idle for t, idle in rows if t is not None and t - 1 >= lo_s and t + 1 <= hi_s]


MPSTAT_CORE = re.compile(r"^(\d\d):(\d\d):(\d\d)\s+(\d+)\s")
CORE_BUSY_PCT = 95.0  # a core this busy has no headroom left for the scheduler pinned to it


def core_saturation(rdir: Path, role: str, window: dict | None) -> dict | None:
    """The worst single core on any `role` host, inside the measurement window.

    The `all` row is a mean over every core, so one emqtt-bench scheduler pinned
    at 100% on an 8-vCPU driver reads as "87% idle" — and that scheduler is then
    the rung's real rate limit while the host looks unloaded. This reports, per
    host and core, the SHARE of in-window samples in which that core was at least
    CORE_BUSY_PCT busy, and returns the worst. A per-core row carries the same
    timestamp as the `all` row printed just above it, so each is placed by that
    row's already-resolved epoch second (midnight rollover and all).
    """
    worst = None
    for path in sorted((rdir / "cpu").glob(f"cpu-{role}*.txt")):
        host = path.stem.split("-", 1)[1]
        if not (HOST.match(host) and host.startswith(role)):
            continue
        edges = None if window is None else window["hosts"].get(host)
        if window is not None and edges is None:
            continue
        times = [t for t, _ in mpstat_rows(path)[0]]
        k = -1
        busy: dict[int, list[float]] = {}
        for line in path.read_text(errors="replace").splitlines():
            if MPSTAT_ALL.match(line):
                k += 1
                continue
            m = MPSTAT_CORE.match(line)
            if not m or not 0 <= k < len(times):
                continue
            t = times[k]
            if edges is not None and (t is None or t - 1 < edges["open"][1] / 1000 or t + 1 > edges["close"][0] / 1000):
                continue
            with contextlib.suppress(ValueError):
                busy.setdefault(int(m.group(4)), []).append(100.0 - float(line.split()[-1]))
        for core, samples in busy.items():
            share = sum(b >= CORE_BUSY_PCT for b in samples) / len(samples)
            mean = sum(samples) / len(samples)
            if worst is None or (share, mean) > (worst["share"], worst["mean_busy"]):
                worst = {"host": host, "core": core, "share": share, "samples": len(samples), "mean_busy": mean}
    return worst


def host_sort_key(host: str) -> tuple[str, int]:
    m = HOST.match(host)
    return (m.group(1), int(m.group(2))) if m else (host, -1)


def idle_summary(rdir: Path, role: str, nodes: int, window: dict | None, cpu_window: str) -> dict:
    """Per role: the mean and the minimum of the per-host means, the lowest single
    row, and how many of the hosts that should have been sampled were. A mean
    over hosts on its own hides one pinned host behind idle ones."""
    files = {}
    for p in (rdir / "cpu").glob(f"cpu-{role}*.txt"):
        host = p.stem.split("-", 1)[1]
        if HOST.match(host) and host.startswith(role):
            files[host] = p
    expected = set(files)
    if role == "broker":
        expected |= {f"broker{i}" for i in range(nodes)}
    if window is not None:
        expected |= {h for h in window["listed"] if h.startswith(role)}
    samplers = rdir / "cpu" / "samplers.tsv"
    if samplers.is_file():
        for line in samplers.read_text(errors="replace").splitlines():
            parts = line.split("\t")
            if len(parts) == 2 and HOST.match(parts[1]) and parts[1].startswith(role):
                expected.add(parts[1])
    means: list[float] = []
    lows: list[float] = []
    unusable: list[str] = []
    for host in sorted(expected, key=host_sort_key):
        path = files.get(host)
        if path is None:
            unusable.append(host)
            continue
        if window is None:
            rows, averages = mpstat_rows(path)
            idles = [idle for _, idle in rows] or averages
        else:
            edges = window["hosts"].get(host)
            # Without a usable window row this host's steady seconds cannot be
            # told from its ramp: it is reported missing, never silently dropped.
            idles = [] if edges is None else windowed_idle(path, edges["open"][1] / 1000, edges["close"][0] / 1000)
        if not idles:
            unusable.append(host)
            continue
        means.append(sum(idles) / len(idles))
        lows.append(min(idles))
    return {
        "mean": sum(means) / len(means) if means else None,
        "min_host": min(means) if means else None,
        "min_1s": min(lows) if lows else None,
        "hosts": len(means),
        "expected": len(expected),
        "unusable": unusable,
        "marked": cpu_window != "aligned",
    }


def format_idle(s: dict) -> str:
    text = "—" if s["mean"] is None else f"{s['mean']:.0f}/{s['min_host']:.0f}/{s['min_1s']:.0f}%"
    if s["hosts"] < s["expected"]:
        text += f"[{s['hosts']}/{s['expected']}hosts]"
    return text + ("*" if s["marked"] else "")


# ── the aligned window ──────────────────────────────────────────────────────


def load_window(rdir: Path, nodes: int) -> dict:
    """Per-host scrape stamps (ms, that host's clock) and each broker's MainPID.

    Every broker must have both edges; a driver whose row is unusable stays in
    `listed` so its CPU is reported missing rather than vanishing. A 5th column
    (main_pid) must name the same process at both edges of every broker.
    """
    path = rdir / "window.tsv"
    if not path.is_file():
        raise ValueError(f"window snapshots without window.tsv: {rdir}")
    lines = path.read_text().splitlines()
    header = lines[0].split("\t") if lines else []
    if header not in (["host", "phase", "start_ms", "end_ms"], ["host", "phase", "start_ms", "end_ms", "main_pid"]):
        raise ValueError(f"unexpected window.tsv header in {rdir}: {header!r}")
    has_pid = len(header) == 5
    edges: dict[str, dict[str, tuple[int, int]]] = {}
    pids: dict[str, dict[str, str]] = {}
    seen: set[tuple[str, str]] = set()
    listed: set[str] = set()
    for line in lines[1:]:
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) not in (4, 5) or (len(parts) == 5 and not has_pid) or parts[1] not in ("open", "close"):
            raise ValueError(f"malformed window.tsv row: {line!r}")
        host, phase, start, end = parts[:4]
        m = HOST.match(host)
        if not m:
            raise ValueError(f"malformed window.tsv host: {line!r}")
        if (host, phase) in seen:
            raise ValueError(f"duplicate window.tsv row: {line!r}")
        seen.add((host, phase))
        listed.add(host)
        if m.group(1) == "broker" and int(m.group(2)) >= nodes:
            raise ValueError(f"window.tsv lists {host} but the size is nodes={nodes}")
        if has_pid:
            pids.setdefault(host, {})[phase] = parts[4].strip() if len(parts) == 5 else ""
        if not (start.isdigit() and end.isdigit()) or int(end) < int(start):
            # An empty stamp is a scrape that never reached the host.
            if host.startswith("broker"):
                raise ValueError(f"no usable {phase} stamp for {host}: {line!r}")
            continue
        edges.setdefault(host, {})[phase] = (int(start), int(end))
    main_pid: dict[int, int] = {}
    for i in range(nodes):
        host = f"broker{i}"
        e = edges.get(host, {})
        if set(e) != {"open", "close"}:
            raise ValueError(f"window.tsv lacks both edges for {host}")
        if e["close"][0] <= e["open"][1]:
            raise ValueError(f"window for {host} closes before it opens")
        if has_pid:
            p = pids.get(host, {})
            for phase in ("open", "close"):
                if not p.get(phase, "").isdigit() or int(p[phase]) == 0:
                    raise ValueError(f"window.tsv carries main_pid but none usable for {host} at {phase}: {p.get(phase, '')!r}")
            if int(p["open"]) != int(p["close"]):
                raise ValueError(
                    f"{host} MainPID changed inside the window ({p['open']} at open, {p['close']} at close): the broker restarted"
                )
            main_pid[i] = int(p["open"])
    usable = {h: e for h, e in edges.items() if set(e) == {"open", "close"} and e["close"][0] > e["open"][1]}
    brackets = [end - start for e in usable.values() for start, end in e.values()]
    return {"hosts": usable, "listed": listed, "main_pid": main_pid, "max_bracket_ms": max(brackets)}


def window_seconds(edges: dict[str, tuple[int, int]]) -> float:
    mid = lambda e: (e[0] + e[1]) / 2  # noqa: E731
    return (mid(edges["close"]) - mid(edges["open"])) / 1000


# ── crossing certificate ────────────────────────────────────────────────────

_CANARY_MODULE = None


def canary_ledger():
    global _CANARY_MODULE
    if _CANARY_MODULE is None:
        if not CANARY_SCRIPT.is_file():
            raise ValueError(
                f"{CANARY_SCRIPT} is missing: the forwarding positive control cannot be re-derived, so no crossing is certified"
            )
        spec = importlib.util.spec_from_file_location("forward_canary", CANARY_SCRIPT)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)  # type: ignore[union-attr]
        _CANARY_MODULE = module
    return _CANARY_MODULE.ledger


def load_canary(lane: Path, nodes: int) -> dict | None:
    """None when the size has no canary evidence at all; {"status": "skipped"}
    when the run opted out; floors and MainPIDs when the ledger re-derives a pass.
    Anything else raises, and every rung of the size is INVALID with it."""
    txt, cdir = lane / "forward-canary.txt", lane / "forward-canary"
    if not txt.exists() and not cdir.exists():
        return None
    if not txt.is_file():
        raise ValueError(f"{cdir} has no forward-canary.txt beside it: the positive control never reached a verdict")
    lines = txt.read_text(errors="replace").splitlines()
    head = dict(tok.split("=", 1) for tok in (lines[0].split() if lines else []) if "=" in tok)
    status = head.get("status", "")
    if status == "skipped":
        return {"status": "skipped"}
    want = "pass" if nodes > 1 else "pass-local"
    if status != want:
        errors = "; ".join(line for line in lines[1:] if line.startswith("error"))
        raise ValueError(
            f"forwarding positive control status={status or 'missing'} (want {want}) in {txt}" + (f": {errors}" if errors else "")
        )
    if head.get("nodes") != str(nodes):
        raise ValueError(f"{txt} verified nodes={head.get('nodes')} but this size is nodes={nodes}")
    count = head.get("count")
    timeline = cdir / "timeline.tsv"
    if timeline.is_file():
        for line in timeline.read_text(errors="replace").splitlines():
            parts = line.split("\t")
            if len(parts) == 2 and parts[0] == "count":
                if count is None:
                    count = parts[1].strip()
                elif parts[1].strip() != count:
                    raise ValueError(f"{txt} says count={count} but {timeline} says {parts[1].strip()}")
    if count is None or not count.isdigit() or int(count) <= 0:
        raise ValueError(f"no usable canary count in {txt} or {timeline}")
    try:
        result = canary_ledger()(cdir, nodes, int(count))
    except ValueError as exc:
        raise ValueError(f"forwarding positive control does not re-derive from {cdir}: {exc}") from None
    except Exception as exc:  # noqa: BLE001 — a ledger that crashes certifies nothing
        raise ValueError(f"forwarding positive control ledger crashed on {cdir}: {exc!r}") from None
    if not isinstance(result, dict) or result.get("status") != want:
        raise ValueError(f"forwarding positive control ledger returned {result!r} for {cdir} (want status {want})")
    floors: dict[int, tuple[float, float]] = {}
    raw = result.get("floors") or {}
    for i in range(nodes):
        f = raw.get(i, raw.get(str(i)))
        if not isinstance(f, dict) or "forwarded" not in f or "received" not in f:
            raise ValueError(f"forwarding positive control ledger gave no floor for broker{i}")
        floors[i] = (float(f["forwarded"]), float(f["received"]))
    mainpid: dict[int, int | None] = {}
    for i in range(nodes):
        p = cdir / f"mainpid-broker{i}.txt"
        v = p.read_text(errors="replace").strip() if p.is_file() else ""
        if v.startswith("MainPID="):
            v = v[len("MainPID="):]
        mainpid[i] = int(v) if v.isdigit() and int(v) > 0 else None
    return {"status": want, "count": int(count), "floors": floors, "mainpid": mainpid}


def ingress_skew(per_broker: list[dict]) -> tuple[float | None, float | None]:
    """`(rx_skew, eff_nodes)`: busiest broker's Δ received over the mean, and
    N / that. A CEILING on useful scale-out under prefer-local, not a
    saturation measurement — a rung whose busiest broker has headroom is not
    limited by it yet. `(None, None)` when nothing was received."""
    rx = [p["received"] for p in per_broker]
    mean = sum(rx) / len(rx) if rx else 0.0
    if mean <= 0:
        return None, None
    skew = max(rx) / mean
    return skew, len(rx) / skew


def certify(nodes: int, snaps: dict[str, dict[int, dict]], window: dict | None, canary: dict | None) -> str:
    errors: list[str] = []
    if nodes == 1:
        cert = "structural"
        for label, per in snaps.items():
            links = peer_links(per[0])
            if links != 0:
                shown = "absent" if links is None else f"{links:g}"
                errors.append(f"broker0 {label}: mqttd_peer_links={shown}, so nodes=1 crossing is not structurally zero")
    else:
        cert = "canary"
        if canary is None:
            raise ValueError(
                f"crossing not certified: no forwarding positive control for nodes={nodes} (laneE/forward-canary.txt "
                "missing) — an absent or flat forwarded counter cannot be told from one that is not exported"
            )
        if canary["status"] == "skipped":
            raise ValueError(
                f"crossing not certified: the forwarding positive control was skipped for nodes={nodes} (LANE_E_FORWARD_CANARY=0)"
            )
        if window is None:
            # No window.tsv, so no MainPID to compare: floors alone cannot tell the
            # process that passed the control from a restarted one.
            cert = "canary-unbound"
        elif not window["main_pid"]:
            # Floors alone miss a restart whose new process already forwarded past
            # them; only the PID ties the window to the process that passed.
            errors.append(
                "window.tsv has no main_pid column, so the brokers that answered the window cannot be tied to the "
                "processes that passed the positive control"
            )
    # A passing control binds every rung of its size, nodes=1 included: a floor
    # that goes backwards is a restart whichever certificate the rung carries.
    if canary is not None and canary["status"] != "skipped":
        for b in range(nodes):
            fwd_floor, recv_floor = canary["floors"][b]
            for label, per in snaps.items():
                fwd = by_label(per[b], FORWARDED, "reason").get("shared-remote", 0.0)
                recv = sum_family(per[b], RECEIVED)
                if fwd < fwd_floor:
                    errors.append(
                        f"broker{b} {label}: forwarded{{shared-remote}}={fwd:.0f} below its canary floor {fwd_floor:.0f} "
                        "(restarted, or the series is gone, since the positive control)"
                    )
                if recv < recv_floor:
                    errors.append(f"broker{b} {label}: received={recv:.0f} below its canary floor {recv_floor:.0f}")
            if window is not None and b in window["main_pid"]:
                seen, passed = window["main_pid"][b], canary["mainpid"].get(b)
                if passed is None:
                    errors.append(f"broker{b}: window.tsv main_pid={seen} but forward-canary/mainpid-broker{b}.txt is missing or unusable")
                elif seen != passed:
                    errors.append(f"broker{b}: MainPID {seen} in the window is not {passed}, the process that passed the canary")
    if errors:
        raise ValueError("crossing not certified: " + "; ".join(errors))
    return cert


# ── one rung ────────────────────────────────────────────────────────────────


def extract_rung(rdir: Path, canaries: dict | None = None) -> dict:
    rdir = Path(rdir)
    nodes = nodes_of(rdir)
    meta = read_meta(rdir)
    aligned = (
        meta.get("window") == "aligned"
        or (rdir / "window.tsv").exists()
        or any(rdir.glob("metrics-window-*-broker*.prom"))
    )
    # In the order they were taken, so every consecutive pair must be monotonic.
    snaps: dict[str, dict[int, dict]] = {"before": load_snap(rdir, "before", nodes)}
    window = None
    if aligned:
        # Any trace of the aligned harness makes the whole window mandatory: a
        # half-present window silently falling back to lifetime totals would
        # report ramp and drain as steady work.
        window = load_window(rdir, nodes)
        snaps["window-open"] = load_snap(rdir, "window-open", nodes)
        snaps["window-close"] = load_snap(rdir, "window-close", nodes)
    if any(rdir.glob("metrics-drain-broker*.prom")):
        snaps["drain"] = load_snap(rdir, "drain", nodes)
    snaps["after"] = load_snap(rdir, "after", nodes)
    labels = list(snaps)
    for first, then in zip(labels, labels[1:]):
        validate_deltas(snaps[first], snaps[then], first, then)

    # One control per size, re-derived once and shared by every rung of the size.
    if canaries is None:
        canaries = {}
    key = (str(rdir.parent.resolve()), nodes)
    if key not in canaries:
        try:
            canaries[key] = (None, load_canary(rdir.parent, nodes))
        except ValueError as exc:
            canaries[key] = (str(exc), None)
    canary_error, canary = canaries[key]
    if canary_error:
        raise ValueError(canary_error)
    cert = certify(nodes, snaps, window, canary)

    lo_label, hi_label = ("window-open", "window-close") if aligned else ("before", "after")
    lo, hi = snaps[lo_label], snaps[hi_label]
    per_broker = []
    for b in range(nodes):
        recv = delta(lo[b], hi[b], RECEIVED)
        fwd = delta(lo[b], hi[b], FORWARDED)
        per_broker.append({
            "broker": b,
            "received": recv,
            "forwarded": fwd,
            "delivered": delta(lo[b], hi[b], DELIVERED),
            "crossing": fwd / recv if recv > 0 else None,
            "peer_links": {lo_label: peer_links(lo[b]), hi_label: peer_links(hi[b])},
            "window_s": window_seconds(window["hosts"][f"broker{b}"]) if window else None,
        })
    received = sum(p["received"] for p in per_broker)
    forwarded = sum(p["forwarded"] for p in per_broker)
    if received <= 0:
        raise ValueError("no positive received delta; crossing is unknown")
    window_s = recv_rate = deliv_rate = bracket_ms = None
    bracket_wide = False
    if window is not None:
        # Each broker over its OWN window: the edges are stamped per host.
        window_s = sum(p["window_s"] for p in per_broker) / nodes
        recv_rate = sum(p["received"] / p["window_s"] for p in per_broker)
        deliv_rate = sum(p["delivered"] / p["window_s"] for p in per_broker)
        bracket_ms = window["max_bracket_ms"]
        bracket_wide = bracket_ms > BRACKET_SHARE * window_s * 1000
    worst = max((p for p in per_broker if p["crossing"] is not None), key=lambda p: p["crossing"])
    rx_skew, eff_nodes = ingress_skew(per_broker)

    before, after = merge_snap(lo), merge_snap(hi)
    lifetime_start = merge_snap(snaps["before"])
    end_label = "drain" if "drain" in snaps else "after"
    end = merge_snap(snaps[end_label])
    hub_us: dict[str, float] = {}
    hub_sum = by_label(after, "mqttd_hub_dispatch_seconds_sum", "command")
    hub_sum_b = by_label(before, "mqttd_hub_dispatch_seconds_sum", "command")
    hub_n = by_label(after, "mqttd_hub_dispatch_seconds_count", "command")
    hub_n_b = by_label(before, "mqttd_hub_dispatch_seconds_count", "command")
    for cmd in sorted(set(hub_sum) | set(hub_sum_b) | set(hub_n) | set(hub_n_b)):
        ds = hub_sum.get(cmd, 0.0) - hub_sum_b.get(cmd, 0.0)
        dn = hub_n.get(cmd, 0.0) - hub_n_b.get(cmd, 0.0)
        if dn > 0:
            hub_us[cmd] = ds / dn * 1e6
    m = RUNG_DIR.match(rdir.name)
    cpu_window = meta.get("cpu_window", "—")
    return {
        "path": rdir,
        "nodes": nodes,
        "sites": meta.get("sites", m.group(1) if m else rdir.name),
        "rep": int(m.group(2)) if m and m.group(2) else 1,
        "control": meta.get("control", "—"),
        "aligned": aligned,
        "window_s": window_s,
        "bracket_ms": bracket_ms,
        "bracket_wide": bracket_wide,
        "recv_rate": recv_rate,
        "deliv_rate": deliv_rate,
        "per_node_deliv": deliv_rate / nodes if deliv_rate is not None else None,
        "offered": meta.get("offered", "—"),
        "received": received,
        "forwarded": forwarded,
        "crossing": forwarded / received,
        "max_crossing": worst["crossing"],
        "max_crossing_broker": worst["broker"],
        "rx_skew": rx_skew,
        "eff_nodes": eff_nodes,
        "idle_brokers": [p["broker"] for p in per_broker if p["crossing"] is None],
        "per_broker": per_broker,
        "cert": cert,
        "lifetime_end": end_label,
        "lifetime_received": delta(lifetime_start, end, RECEIVED),
        "lifetime_delivered": delta(lifetime_start, end, DELIVERED),
        "hub_us": hub_us,
        "inflight": sum_family(end, "mqttd_peer_forwards_in_flight"),
        "sessions": sum_family(end, "mqttd_sessions"),
        "drops": positive_deltas(before, after, DROPPED, "reason"),
        "lifetime_drops": positive_deltas(lifetime_start, merge_snap(snaps["after"]), DROPPED, "reason"),
        "broker_idle": idle_summary(rdir, "broker", nodes, window, cpu_window),
        "driver_idle": idle_summary(rdir, "driver", nodes, window, cpu_window),
        "cpu_window": cpu_window,
        "settled": meta.get("settled", "—"),
        "drained": meta.get("drained", "—"),
    }


def results_root(root: Path) -> Path:
    return root / "results" if (root / "results").is_dir() else root


def rung_sort_key(path: Path) -> tuple:
    m = RUNG_DIR.match(path.name)
    try:
        nodes = nodes_of(path)
    except ValueError:
        nodes = 0
    if not m:
        return (nodes, math.inf, 0, path.name)
    return (nodes, int(m.group(1)), int(m.group(2) or 1), path.name)


def find_rungs(root: Path) -> list[Path]:
    return sorted(results_root(root).glob("nodes=*/laneE/sites-*"), key=rung_sort_key)


# ── output ──────────────────────────────────────────────────────────────────


def format_hub(r: dict) -> str:
    if not r["hub_us"]:
        return "—"
    return ",".join(f"{k}={v:.1f}µs" for k, v in sorted(r["hub_us"].items()))


def format_drops(drops: dict[str, float]) -> str:
    return ",".join(f"{k}:{v:.0f}" for k, v in sorted(drops.items())) or "0"


def report_rows(rungs: list[dict]) -> list[list[str]]:
    num = lambda v: "—" if v is None else f"{v:.0f}"  # noqa: E731
    rows = [[
        "nodes", "sites", "rep", "control", "offered", "window", "window_s", "bracket_ms", "recv/s", "deliv/s",
        "deliv/s/node", "win_recv", "life_recv", "life_deliv", "fwd", "crossing", "max_broker", "rx_skew", "eff_nodes", "cert",
        "hub_dispatch_mean", "peer_inflight", "sessions", "broker_idle", "driver_idle", "cpu_window", "settled",
        "drained", "win_drops", "life_drops",
    ]]
    for r in rungs:
        worst = f"{r['max_crossing'] * 100:.2f}%@broker{r['max_crossing_broker']}"
        if r["idle_brokers"]:
            worst += "+idle:" + ",".join(f"broker{b}" for b in r["idle_brokers"])
        rows.append([
            str(r["nodes"]), str(r["sites"]), str(r["rep"]), r["control"], str(r["offered"]),
            "aligned" if r["aligned"] else "UNALIGNED",
            "—" if r["window_s"] is None else f"{r['window_s']:.1f}",
            "—" if r["bracket_ms"] is None else f"{r['bracket_ms']}" + ("!" if r["bracket_wide"] else ""),
            num(r["recv_rate"]), num(r["deliv_rate"]), num(r["per_node_deliv"]),
            f"{r['received']:.0f}", f"{r['lifetime_received']:.0f}", f"{r['lifetime_delivered']:.0f}",
            f"{r['forwarded']:.0f}", f"{r['crossing'] * 100:.2f}%", worst,
            "—" if r["rx_skew"] is None else f"{r['rx_skew']:.2f}",
            "—" if r["eff_nodes"] is None else f"{r['eff_nodes']:.1f}",
            r["cert"], format_hub(r),
            f"{r['inflight']:.0f}", f"{r['sessions']:.0f}", format_idle(r["broker_idle"]), format_idle(r["driver_idle"]),
            r["cpu_window"], r["settled"], r["drained"], format_drops(r["drops"]), format_drops(r["lifetime_drops"]),
        ])
    return rows


def print_report(rungs: list[dict]) -> None:
    rows = report_rows(rungs)
    widths = [max(len(row[c]) for row in rows) for c in range(len(rows[0]))]
    for row in rows:
        print("  ".join(cell.ljust(w) for cell, w in zip(row, widths)).rstrip())
    print(
        "# crossing = Σ forwarded (all reasons) / Σ received over the window; max_broker = the highest single broker's own ratio\n"
        "# rx_skew = busiest broker's received / the mean broker's; eff_nodes = nodes / rx_skew, the prefer-local ceiling on usable brokers\n"
        "# cert: structural = nodes=1 with no peer links; canary = the size's forwarding positive control re-derived and still in force;\n"
        "#   canary-unbound = floors held but no window.tsv MainPID ties the rung to the process that passed (not certified)\n"
        "# win_* = steady window; life_* = before -> drain (or after), for the drain-vs-broker delivery check\n"
        f"# bracket_ms = widest window scrape; ! = wider than {BRACKET_SHARE:.0%} of window_s\n"
        "# idle = mean of per-host means / lowest host mean / lowest 1 s row; [n/m hosts] = hosts with a usable\n"
        "#   window row and CPU samples inside it; * = cpu_window not aligned, so the figure may not cover the window"
    )
    for r in rungs:
        missing = r["broker_idle"]["unusable"] + r["driver_idle"]["unusable"]
        if missing:
            print(f"# nodes={r['nodes']} {r['path'].name}: no usable window CPU for {', '.join(missing)}")


SHAPE_ROW = re.compile(r"^\s*(\d+) \|")
SHAPE_CONTROL = re.compile(r"^control rung: ON — the (\d+)-site rung")


def declared_rungs(lane: Path) -> list[str] | None:
    """The rung directories laneE/shape.txt says the size would run, in order;
    None when there is no shape.txt to read (a hand-built or local-proof tree)."""
    shape = lane / "shape.txt"
    if not shape.is_file():
        return None
    ladder: list[int] = []
    control = None
    for line in shape.read_text(errors="replace").splitlines():
        if m := SHAPE_ROW.match(line):
            ladder.append(int(m.group(1)))
        elif m := SHAPE_CONTROL.match(line):
            control = int(m.group(1))
    names, seen = [], []
    for sites in ladder + ([control] if control is not None else []):
        rep = seen.count(sites) + 1
        seen.append(sites)
        names.append(f"sites-{sites}" if rep == 1 else f"sites-{sites}-rep{rep}")
    return names


def gate_lines(root: Path, outcomes: list[tuple[Path, dict | None]], pct: float) -> list[tuple[bool, str]]:
    sizes: dict[int, list[tuple[Path, dict | None]]] = {}
    lanes: dict[int, Path] = {}
    for lane in results_root(root).glob("nodes=*/laneE"):
        with contextlib.suppress(ValueError):
            sizes.setdefault(nodes_of(lane), [])
            lanes[nodes_of(lane)] = lane
    for path, r in outcomes:
        with contextlib.suppress(ValueError):
            sizes.setdefault(nodes_of(path), []).append((path, r))
    out = []
    for n in sorted(sizes):
        reasons: list[str] = []
        rungs = sizes[n]
        if not rungs:
            reasons.append("no sites-* rungs")
        declared = declared_rungs(lanes[n]) if n in lanes else None
        if declared is not None:
            present = {path.name for path, _ in rungs}
            missing = [name for name in declared if name not in present]
            if missing:
                reasons.append(f"{', '.join(missing)} declared in shape.txt but not run")
        worst = 0.0
        for path, r in rungs:
            name = path.name
            if r is None:
                reasons.append(f"{name} INVALID")
                continue
            if not r["aligned"]:
                reasons.append(f"{name} UNALIGNED")
            if r["cert"] not in ("canary", "structural"):
                reasons.append(f"{name} cert={r['cert']}")
            for p in r["per_broker"]:
                b = p["broker"]
                for label, links in p["peer_links"].items():
                    if links != n - 1:
                        shown = "absent" if links is None else f"{links:g}"
                        reasons.append(f"{name} broker{b} peer_links={shown} at {label} (want {n - 1})")
                if p["crossing"] is None:
                    reasons.append(f"{name} broker{b} idle (window received 0)")
                    continue
                worst = max(worst, p["crossing"] * 100)
                if p["crossing"] * 100 > pct:
                    reasons.append(f"{name} broker{b} crossing {p['crossing'] * 100:.2f}% > {pct:g}%")
        if reasons:
            out.append((False, f"GATE nodes={n} FAIL " + "; ".join(reasons)))
        else:
            certs = ",".join(sorted({r["cert"] for _, r in rungs if r}))
            out.append((True, f"GATE nodes={n} PASS {len(rungs)} rungs, cert={certs}, max broker crossing {worst:.2f}% <= {pct:g}%"))
    return out


# ── self-test ───────────────────────────────────────────────────────────────


def fixture(name: str) -> Path:
    path = FIXTURES / name
    if not path.is_dir():
        raise AssertionError(
            f"fixture {path} is missing: --self-test needs testdata/lane-e and forward-canary.py beside the extractor"
        )
    return path


def stage(root: Path, *names: str) -> Path:
    """Copy real fixture trees into root/results, so tests only ever mutate copies."""
    results = root / "results"
    for name in names:
        shutil.copytree(fixture(name), results, dirs_exist_ok=True)
    return results


def exposition(recv: float | None, fwd: float | None = None, deliv: float | None = None, links: int = 0,
               drops: dict[str, float] | None = None, hub: tuple[float, float] | None = None,
               type_only_forwarded: bool = False) -> str:
    lines = []
    if recv is not None:
        lines.append(f'mqttd_publish_received_total{{qos="0"}} {recv}')
    if type_only_forwarded:
        lines.append("# TYPE mqttd_publish_forwarded counter")
    if fwd is not None:
        lines.append(f'mqttd_publish_forwarded_total{{reason="shared-remote"}} {fwd}')
    if deliv is not None:
        lines.append(f'mqttd_publish_delivered_total{{qos="0"}} {deliv}')
    for reason, v in sorted((drops or {}).items()):
        lines.append(f'mqttd_publish_dropped_total{{reason="{reason}"}} {v}')
    if hub is not None:
        lines.append(f'mqttd_hub_dispatch_seconds_sum{{command="publish"}} {hub[0]}')
        lines.append(f'mqttd_hub_dispatch_seconds_count{{command="publish"}} {hub[1]}')
    lines.append(f"mqttd_peer_links {links}")
    lines.append("# EOF")
    return "\n".join(lines) + "\n"


def run_main(argv: list[str]) -> tuple[int, str, str]:
    stdout, stderr = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        rc = main(argv)
    return rc, stdout.getvalue(), stderr.getvalue()


SMOKE = "nodes=1/laneE/sites-1"
# One continuous 3-process lifetime: canary, then rung, then a SIGKILL restart.
PROOF = "local-proof-n3"
N3 = "nodes=3/laneE"
RESTART = "restart/metrics-restart-broker2.prom"


class ScrapeTests(unittest.TestCase):
    def test_incomplete_reset_and_unsupported_scrapes_are_invalid(self):
        defects = ("missing", "empty", "reset", "partial-drain", "malformed", "duplicate", "nonfinite", "truncated",
                   "sample-after-eof", "extra-broker")
        for defect in defects:
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as td:
                results = stage(Path(td), PROOF)
                rdir = results / N3 / "sites-1"
                extract_rung(rdir)  # the unmutated real rung is valid, so the defect is what bites
                bad = rdir / "metrics-after-broker1.prom"
                text = bad.read_text()
                if defect == "missing":
                    bad.unlink()
                elif defect == "empty":
                    bad.write_text("")
                elif defect == "reset":
                    # The cluster's aggregate still rises; only per-broker validation sees it.
                    shutil.copy(rdir / "metrics-before-broker1.prom", bad)
                elif defect == "partial-drain":
                    shutil.copy(rdir / "metrics-after-broker0.prom", rdir / "metrics-drain-broker0.prom")
                elif defect == "malformed":
                    bad.write_text("HTTP scrape failed\n# EOF\n")
                elif defect == "duplicate":
                    bad.write_text('mqttd_publish_received_total{qos="0"} 704\n' + text)
                elif defect == "nonfinite":
                    bad.write_text(text.replace('mqttd_publish_received_total{qos="0"} 704', 'mqttd_publish_received_total{qos="0"} 1e999'))
                elif defect == "truncated":
                    # Every sample intact, only the terminator lost: nothing else would notice.
                    bad.write_text(text.rstrip()[: -len("# EOF")])
                elif defect == "sample-after-eof":
                    bad.write_text(text + "mqttd_after_eof 1\n")
                else:
                    shutil.copy(bad, rdir / "metrics-after-broker3.prom")
                with self.assertRaises(ValueError):
                    extract_rung(rdir)
                rc, stdout, stderr = run_main([str(Path(td))])
                self.assertEqual(rc, 1)
                self.assertIn("INVALID", stderr)
                self.assertNotIn("0.00%", stdout)

    def test_does_not_prefix_match_received_gauge(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "nodes=1/laneE/sites-1"
            rdir.mkdir(parents=True)
            for label, recv in (("before", 1), ("after", 11)):
                (rdir / f"metrics-{label}-broker0.prom").write_text("mqttd_publish_received 999999\n" + exposition(recv))
            self.assertEqual(extract_rung(rdir)["received"], 10)


class CertificationTests(unittest.TestCase):
    def test_real_n1_smoke_rung_is_structural_zero_crossing(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            r = extract_rung(results / SMOKE)
            self.assertEqual(r["cert"], "structural")
            self.assertTrue(r["aligned"])
            self.assertEqual((r["forwarded"], r["crossing"], r["max_crossing"]), (0, 0.0, 0.0))
            # Window, not lifetime: 62614-47040 in the window, 64013-23839 over the rung.
            self.assertEqual(r["received"], 15574)
            self.assertEqual(r["lifetime_received"], 40174)
            self.assertEqual(r["lifetime_end"], "drain")
            self.assertAlmostEqual(r["window_s"], 15.572)
            self.assertAlmostEqual(r["recv_rate"], 15574 / 15.572)
            # The window's hub mean is 13.49 µs; the lifetime, ramp included, 13.81 µs.
            self.assertAlmostEqual(r["hub_us"]["publish"], (0.6582009460000011 - 0.4480509740000026) / 15574 * 1e6, places=6)
            self.assertLess(r["hub_us"]["publish"], 13.6)
            self.assertEqual((r["bracket_ms"], r["bracket_wide"]), (24, False))
            # Rows 20:35:30..20:35:43 only: the stamps put open at :28.63 and close at :44.19.
            lines = (results / SMOKE / "cpu/cpu-broker0.txt").read_text().splitlines()
            idles = [float(ln.split()[-1]) for ln in lines if "20:35:30" <= ln[:8] <= "20:35:43" and " all " in ln]
            self.assertEqual(len(idles), 14)
            self.assertAlmostEqual(r["broker_idle"]["mean"], sum(idles) / 14)
            self.assertEqual(r["broker_idle"]["min_1s"], min(idles))
            self.assertEqual((r["broker_idle"]["hosts"], r["broker_idle"]["expected"]), (1, 1))
            rc, stdout, stderr = run_main([str(Path(td)), "--crossing-gate", "0.5"])
            self.assertEqual(rc, 0, stderr)
            self.assertIn("structural", stdout)
            self.assertIn("GATE nodes=1 PASS", stdout)

    def test_n1_with_a_peer_link_is_invalid(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            path = results / SMOKE / "metrics-window-close-broker0.prom"
            path.write_text(path.read_text().replace("\nmqttd_peer_links 0\n", "\nmqttd_peer_links 1\n"))
            with self.assertRaisesRegex(ValueError, "broker0 window-close: mqttd_peer_links=1"):
                extract_rung(results / SMOKE)
            rc, _, stderr = run_main([str(Path(td))])
            self.assertEqual(rc, 1)
            self.assertIn("INVALID", stderr)

    def test_type_only_forwarded_without_a_canary_is_invalid_at_n2(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "results/nodes=2/laneE/sites-1"
            rdir.mkdir(parents=True)
            for b in range(2):
                for label, recv in (("before", 0), ("window-open", 1000), ("window-close", 31000), ("after", 32000)):
                    (rdir / f"metrics-{label}-broker{b}.prom").write_text(
                        exposition(recv, deliv=recv, links=1, type_only_forwarded=True)
                    )
            rows = ["host\tphase\tstart_ms\tend_ms"]
            for b in range(2):
                rows += [f"broker{b}\topen\t1000\t1010", f"broker{b}\tclose\t31000\t31010"]
            (rdir / "window.tsv").write_text("\n".join(rows) + "\n")
            with self.assertRaisesRegex(ValueError, "no forwarding positive control"):
                extract_rung(rdir)
            rc, stdout, stderr = run_main([str(Path(td)), "--crossing-gate", "0.5"])
            self.assertEqual(rc, 1)
            self.assertIn("INVALID", stderr)
            self.assertIn("GATE nodes=2 FAIL sites-1 INVALID", stdout)
            (rdir.parent / "forward-canary.txt").write_text("status=skipped nodes=2\n")
            with self.assertRaisesRegex(ValueError, "skipped"):
                extract_rung(rdir)

    def test_real_post_canary_zero_crossing_rung_is_canary_certified(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF)
            rdir = results / N3 / "sites-1"
            r = extract_rung(rdir)
            self.assertEqual(r["cert"], "canary")
            self.assertEqual((r["forwarded"], r["crossing"], r["max_crossing"]), (0, 0.0, 0.0))
            self.assertEqual([p["received"] for p in r["per_broker"]], [500, 500, 500])
            self.assertEqual([p["peer_links"]["window-close"] for p in r["per_broker"]], [2, 2, 2])
            # The same processes answered the canary and both window edges: real PIDs, not stand-ins.
            window = load_window(rdir, 3)
            canary = load_canary(results / N3, 3)
            self.assertEqual(window["main_pid"], canary["mainpid"])
            self.assertEqual(len(set(window["main_pid"].values())), 3)
            # The family is present from the canary on, at exactly the canary's floor.
            for b in range(3):
                for label in ("before", "window-open", "window-close", "after"):
                    parsed = parse_prom(rdir / f"metrics-{label}-broker{b}.prom")
                    self.assertEqual(by_label(parsed, FORWARDED, "reason"), {"shared-remote": canary["floors"][b][0]})
            rc, stdout, stderr = run_main([str(Path(td)), "--crossing-gate", "0.5"])
            self.assertEqual(rc, 0, stderr)
            self.assertIn("GATE nodes=3 PASS 1 rungs, cert=canary", stdout)

    def test_a_snapshot_below_the_canary_floor_is_invalid_and_names_the_broker(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF)
            # A real scrape of broker1 taken between the canary's pilot and its burst:
            # every counter still rises into the rung, but it carries 4 forwards
            # where the canary left 204.
            shutil.copy(results / N3 / "forward-canary/metrics-pre-broker1.prom", results / N3 / "sites-1/metrics-before-broker1.prom")
            with self.assertRaises(ValueError) as caught:
                extract_rung(results / N3 / "sites-1")
            self.assertIn("broker1 before: forwarded{shared-remote}=4 below its canary floor 204", str(caught.exception))
            self.assertIn("broker1 before: received=4 below its canary floor 204", str(caught.exception))
            self.assertNotIn("broker0", str(caught.exception))

    def test_a_real_restart_scrape_is_invalid(self):
        # broker2's first scrape after the proof SIGKILLed and respawned it: every
        # counter family gone, peer links back — a healthy-looking new process.
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF)
            rdir = results / N3 / "sites-1"
            shutil.copy(fixture(PROOF) / RESTART, rdir / "metrics-after-broker2.prom")
            with self.assertRaisesRegex(ValueError, "counter reset/disappeared on broker2 between window-close and after"):
                extract_rung(rdir)
        # Restarted before the rung began: every snapshot of broker2 is the new
        # process, so nothing resets inside the rung — only the canary floor and the
        # MainPID the harness recorded (here the respawn's real PID) can see it.
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF)
            rdir = results / N3 / "sites-1"
            for label in ("before", "window-open", "window-close", "after"):
                shutil.copy(fixture(PROOF) / RESTART, rdir / f"metrics-{label}-broker2.prom")
            with self.assertRaisesRegex(ValueError, r"broker2 before: forwarded\{shared-remote\}=0 below its canary floor 204"):
                extract_rung(rdir)
            new_pid = (fixture(PROOF) / "restart/mainpid-restart-broker2.txt").read_text().strip()
            old_pid = (results / N3 / "forward-canary/mainpid-broker2.txt").read_text().strip()
            self.assertNotEqual(new_pid, old_pid)
            tsv = rdir / "window.tsv"
            tsv.write_text(re.sub(r"(?m)^(broker2\t\w+\t\d+\t\d+\t)\d+$", rf"\g<1>{new_pid}", tsv.read_text()))
            with self.assertRaises(ValueError) as caught:
                extract_rung(rdir)
            self.assertIn(f"broker2: MainPID {new_pid} in the window is not {old_pid}, the process that passed the canary", str(caught.exception))
            self.assertIn("broker2 window-close: received=0 below its canary floor 204", str(caught.exception))
        # A restart the counters alone might not show: the process identity moved.
        for mutation in ("inside-window", "since-canary"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                results = stage(Path(td), PROOF)
                rdir = results / N3 / "sites-1"
                tsv = rdir / "window.tsv"
                if mutation == "inside-window":
                    tsv.write_text(re.sub(r"(?m)^(broker1\tclose\t\d+\t\d+\t)\d+$", r"\g<1>7777", tsv.read_text()))
                    pattern = "broker1 MainPID changed inside the window"
                else:
                    tsv.write_text(re.sub(r"(?m)^(broker1\t\w+\t\d+\t\d+\t)\d+$", r"\g<1>7777", tsv.read_text()))
                    pattern = "broker1: MainPID 7777 in the window is not"
                with self.assertRaisesRegex(ValueError, pattern):
                    extract_rung(rdir)

    def test_a_failed_canary_ledger_invalidates_every_rung_of_that_size(self):
        for mutation, why in (("pair-missing", "does not re-derive"), ("no-burst-on-broker2", "does not re-derive"),
                              ("status-fail", "status=fail")):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                results = stage(Path(td), PROOF, "smoke-n1")
                shutil.copytree(results / N3 / "sites-1", results / N3 / "sites-1-rep2")
                cdir = results / N3 / "forward-canary"
                if mutation == "pair-missing":
                    # forward-canary.txt still says pass: the ledger, not the line, decides.
                    clients = cdir / "clients.tsv"
                    rows = [line for line in clients.read_text().splitlines() if line.strip()]
                    clients.write_text("\n".join(rows[:-1]) + "\n")
                elif mutation == "no-burst-on-broker2":
                    shutil.copy(cdir / "metrics-pre-broker2.prom", cdir / "metrics-post-broker2.prom")
                else:
                    (results / N3 / "forward-canary.txt").write_text("status=fail nodes=3 count=100\nerror synthetic\n")
                rc, stdout, stderr = run_main([str(Path(td)), "--crossing-gate", "0.5"])
                self.assertEqual(rc, 1)
                self.assertIn(f"INVALID {results / N3 / 'sites-1'}:", stderr)
                self.assertIn(f"INVALID {results / N3 / 'sites-1-rep2'}:", stderr)
                self.assertIn(why, stderr)
                self.assertIn("GATE nodes=3 FAIL sites-1 INVALID; sites-1-rep2 INVALID", stdout)
                self.assertIn("GATE nodes=1 PASS", stdout)
        # A size is bound by its own control, nodes=1 included: a failed local
        # canary says the broker could not deliver to itself.
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            (results / "nodes=1/laneE/forward-canary.txt").write_text("status=fail nodes=1 count=100\nerror synthetic\n")
            with self.assertRaisesRegex(ValueError, "status=fail"):
                extract_rung(results / SMOKE)


class WindowTests(unittest.TestCase):
    T0 = 1_789_000_010_000
    # host -> (open offset ms, close offset ms): every host has its own stamps and
    # every broker its own window length, so reading broker0's edges for every
    # host fails the rates and the idle figures below.
    STAMPS = {"broker0": (0, 60_000), "broker1": (10_000, 50_000), "broker2": (5_000, 35_000), "driver0": (2_000, 58_000)}
    RECV = {0: 600_000, 1: 400_000, 2: 300_000}  # 10k/s over each broker's own window
    IDLE = {"broker0": 80.0, "broker1": 20.0, "broker2": 60.0, "driver0": 50.0, "driver1": 90.0}

    def synthetic_rung(self, root: Path, cpu_window: str = "aligned", fwd_in_window: dict[int, float] | None = None,
                       pid_column: bool = True) -> Path:
        """A synthetic nodes=3 rung certified by the REAL canary-n3 positive control."""
        results = stage(root, PROOF)
        rdir = results / N3 / "sites-4"
        (rdir / "cpu").mkdir(parents=True)
        (rdir / "rung.txt").write_text(f"sites=4 offered=120000 window=aligned cpu_window={cpu_window} settled=yes drained=yes control=no\n")
        for b in range(3):
            recv = self.RECV[b]
            fwd = (fwd_in_window or {}).get(b, 0.0)
            window_drops = 7 if b == 1 else 0
            # Lifetime carries a 50 µs ramp, 500 ramp drops and 100 teardown drops
            # per broker; the window carries 4 µs and 7 drops, on broker1 only.
            snaps = {
                "before": (1000, 300, {"queue-full": 0}, (1000 * 50e-6, 1000)),
                "window-open": (10_000, 300, {"queue-full": 500}, (10_000 * 50e-6, 10_000)),
                "window-close": (10_000 + recv, 300 + fwd, {"queue-full": 500 + window_drops},
                                 (10_000 * 50e-6 + recv * 4e-6, 10_000 + recv)),
                "after": (15_000 + recv, 300 + fwd, {"queue-full": 500 + window_drops, "session-gone": 100},
                          (15_000 * 50e-6 + recv * 4e-6, 15_000 + recv)),
            }
            for label, (r, f, drops, hub) in snaps.items():
                (rdir / f"metrics-{label}-broker{b}.prom").write_text(
                    exposition(r, fwd=f, deliv=r - 1000, links=2, drops=drops, hub=hub)
                )
        pids = {f"broker{b}": (rdir.parent / f"forward-canary/mainpid-broker{b}.txt").read_text().strip() for b in range(3)}
        rows = ["host\tphase\tstart_ms\tend_ms" + ("\tmain_pid" if pid_column else "")]
        for phase, idx in (("open", 0), ("close", 1)):
            for host, stamps in self.STAMPS.items():
                at = self.T0 + stamps[idx]
                rows.append(f"{host}\t{phase}\t{at - 100}\t{at + 100}" + (f"\t{pids.get(host, '')}" if pid_column else ""))
            # driver1's scrape never reached it: no stamps, yet its sampler ran.
            rows.append(f"driver1\t{phase}\t\t" + ("\t" if pid_column else ""))
        (rdir / "window.tsv").write_text("\n".join(rows) + "\n")
        start = datetime.fromtimestamp(self.T0 / 1000 - 30, timezone.utc)
        for host, idle in self.IDLE.items():
            open_ms, close_ms = self.STAMPS.get(host, (0, 60_000))
            lines = [f"CPU_STREAM_START_UTC {start:%Y-%m-%dT%H:%M:%SZ}"]
            for t in range(self.T0 // 1000 - 29, self.T0 // 1000 + 90):
                value = idle if self.T0 + open_ms < t * 1000 < self.T0 + close_ms else 0.0
                if host == "broker1" and t == self.T0 // 1000 + 30:
                    value = 5.0  # one pinned second inside broker1's window
                lines.append(f"{datetime.fromtimestamp(t, timezone.utc):%H:%M:%S}     all    1.00    0.00    1.00    0.00    0.00    0.00    0.00    0.00    0.00   {value:.2f}")
            (rdir / "cpu" / f"cpu-{host}.txt").write_text("\n".join(lines) + "\n")
        return rdir

    def test_ingress_skew_reads_the_busiest_brokers_window(self):
        # End to end through a CERTIFIED rung: RECV is 600k/400k/300k inside the
        # window, so broker0 is 600k over a 433,333 mean — and the rung can use
        # at most ~2.2 of its 3 brokers under prefer-local (#613).
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self.synthetic_rung(Path(td)))
            mean = sum(self.RECV.values()) / 3
            self.assertAlmostEqual(r["rx_skew"], 600_000 / mean)
            self.assertAlmostEqual(r["eff_nodes"], 3 * mean / 600_000)
            header, row = report_rows([r])
            self.assertEqual(row[header.index("rx_skew")], f"{600_000 / mean:.2f}")
            self.assertEqual(row[header.index("eff_nodes")], f"{3 * mean / 600_000:.1f}")

    def test_aligned_rung_reads_each_hosts_window_not_the_lifetime(self):
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self.synthetic_rung(Path(td)))
            self.assertEqual(r["cert"], "canary")
            self.assertAlmostEqual(r["window_s"], (60 + 40 + 30) / 3)
            self.assertEqual(r["received"], 1_300_000)
            self.assertEqual(r["lifetime_received"], 1_300_000 + 3 * 14_000)
            self.assertAlmostEqual(r["recv_rate"], 30_000.0)
            self.assertAlmostEqual(r["deliv_rate"], 30_000.0)
            self.assertAlmostEqual(r["per_node_deliv"], 10_000.0)
            self.assertAlmostEqual(r["hub_us"]["publish"], 4.0, places=6)
            self.assertEqual(r["drops"], {"queue-full": 7})
            self.assertEqual(r["lifetime_drops"], {"queue-full": 1507, "session-gone": 300})
            self.assertEqual((r["crossing"], r["max_crossing"]), (0.0, 0.0))
            # broker1 keeps 37 rows (10 s + 2 .. 50 s - 2), one of them at 5 %.
            b1 = (20.0 * 36 + 5.0) / 37
            idle = r["broker_idle"]
            self.assertAlmostEqual(idle["mean"], (80.0 + b1 + 60.0) / 3)
            self.assertAlmostEqual(idle["min_host"], b1)
            self.assertEqual(idle["min_1s"], 5.0)
            self.assertEqual((idle["hosts"], idle["expected"], idle["marked"]), (3, 3, False))
            driver = r["driver_idle"]
            self.assertEqual((driver["mean"], driver["hosts"], driver["expected"], driver["unusable"]), (50.0, 1, 2, ["driver1"]))
            self.assertEqual(format_idle(driver), "50/50/50%[1/2hosts]")
            rc, stdout, stderr = run_main([str(Path(td))])
            self.assertEqual(rc, 0, stderr)
            self.assertIn("50/50/50%[1/2hosts]", stdout)
            self.assertIn("# nodes=3 sites-4: no usable window CPU for driver1", stdout)
            self.assertIn("queue-full:1507,session-gone:300", stdout)

    def test_idle_is_marked_when_cpu_window_is_not_aligned(self):
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self.synthetic_rung(Path(td), cpu_window="incomplete"))
            self.assertTrue(format_idle(r["broker_idle"]).endswith("%*"))
            self.assertTrue(format_idle(r["driver_idle"]).endswith("[1/2hosts]*"))
            _, stdout, _ = run_main([str(Path(td))])
            self.assertRegex(stdout, r"\d+/\d+/5%\*")

    def test_a_wide_scrape_bracket_is_flagged(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = self.synthetic_rung(Path(td))
            self.assertFalse(extract_rung(rdir)["bracket_wide"])  # 200 ms of a 43 s window
            tsv = rdir / "window.tsv"
            tsv.write_text(re.sub(r"(?m)^(broker2\topen\t)(\d+)\t", lambda m: f"{m.group(1)}{int(m.group(2)) - 1800}\t", tsv.read_text()))
            r = extract_rung(rdir)
            self.assertEqual((r["bracket_ms"], r["bracket_wide"]), (2000, True))
            _, stdout, _ = run_main([str(Path(td))])
            self.assertIn("2000!", stdout)

    def test_four_column_window_tsv_is_structural_only(self):
        # The pre-MainPID harness's window.tsv still reads (the real N=1 smoke rung is
        # one), but at nodes>=2 nothing would tie its window to the canary's processes.
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            self.assertEqual((results / SMOKE / "window.tsv").read_text().splitlines()[0].count("\t"), 3)
            self.assertEqual(extract_rung(results / SMOKE)["cert"], "structural")
        with tempfile.TemporaryDirectory() as td:
            rdir = self.synthetic_rung(Path(td), pid_column=False)
            self.assertEqual(load_window(rdir, 3)["main_pid"], {})
            with self.assertRaisesRegex(ValueError, "window.tsv has no main_pid column"):
                extract_rung(rdir)

    def test_every_snapshot_is_held_to_the_canary_floor(self):
        # Consecutive-snapshot validation already refuses a counter that goes down, so
        # the floor is pinned here directly: each label is checked, not just `before`.
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF)
            rdir = results / N3 / "sites-1"
            canary = load_canary(results / N3, 3)
            window = load_window(rdir, 3)
            labels = ("before", "window-open", "window-close", "after")
            real = {label: load_snap(rdir, label, 3) for label in labels}
            self.assertEqual(certify(3, real, window, canary), "canary")
            below = parse_prom(results / N3 / "forward-canary/metrics-pre-broker0.prom")
            for label in labels[1:]:
                with self.subTest(label=label):
                    snaps = {k: dict(v) for k, v in real.items()}
                    snaps[label][0] = below
                    with self.assertRaises(ValueError) as caught:
                        certify(3, snaps, window, canary)
                    self.assertIn(f"broker0 {label}: forwarded{{shared-remote}}=4 below its canary floor 204", str(caught.exception))
                    self.assertIn(f"broker0 {label}: received=4 below its canary floor 204", str(caught.exception))
                    self.assertNotIn("broker0 before", str(caught.exception))

    def test_a_partial_window_is_invalid_not_a_lifetime_fallback(self):
        defects = {
            "close-missing": "incomplete window-close broker coverage",
            "tsv-missing": "window snapshots without window.tsv",
            "edge-missing": "window.tsv lacks both edges for broker1",
            "inverted": "window for broker0 closes before it opens",
            "window-reset": "counter reset/disappeared on broker0 between window-open and window-close",
            "broker-pid-missing": "carries main_pid but none usable for broker2 at open",
            "unknown-broker": "window.tsv lists broker3 but the size is nodes=3",
            "bad-header": "unexpected window.tsv header",
            "canary-pid-missing": "broker2: window.tsv main_pid=\\d+ but forward-canary/mainpid-broker2.txt is missing or unusable",
        }
        for defect, pattern in defects.items():
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as td:
                rdir = self.synthetic_rung(Path(td))
                extract_rung(rdir)
                tsv = rdir / "window.tsv"
                text = tsv.read_text()
                if defect == "close-missing":
                    (rdir / "metrics-window-close-broker1.prom").unlink()
                elif defect == "tsv-missing":
                    tsv.unlink()
                elif defect == "edge-missing":
                    tsv.write_text("\n".join(r for r in text.splitlines() if not r.startswith("broker1\tclose")) + "\n")
                elif defect == "inverted":
                    tsv.write_text(re.sub(r"(?m)^(broker0\tclose\t)\d+\t\d+", rf"\g<1>{self.T0 - 500}\t{self.T0 - 400}", text))
                elif defect == "window-reset":
                    shutil.copy(rdir / "metrics-before-broker0.prom", rdir / "metrics-window-close-broker0.prom")
                elif defect == "broker-pid-missing":
                    tsv.write_text(re.sub(r"(?m)^(broker2\topen\t\d+\t\d+\t)\d+$", r"\g<1>", text))
                elif defect == "unknown-broker":
                    tsv.write_text(text + "broker3\topen\t1\t2\t9\n")
                elif defect == "bad-header":
                    tsv.write_text(text.replace("start_ms\tend_ms", "end_ms\tstart_ms", 1))
                else:
                    (rdir.parent / "forward-canary/mainpid-broker2.txt").write_text("\n")
                with self.assertRaisesRegex(ValueError, pattern):
                    extract_rung(rdir)

    def test_pre_window_rung_is_reported_unaligned(self):
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "nodes=1/laneE/sites-1"
            rdir.mkdir(parents=True)
            for label, recv in (("before", 0), ("after", 100)):
                (rdir / f"metrics-{label}-broker0.prom").write_text(exposition(recv, fwd=0))
            r = extract_rung(rdir)
            self.assertFalse(r["aligned"])
            self.assertIsNone(r["recv_rate"])
            self.assertEqual(r["cert"], "structural")
            rc, stdout, _ = run_main([str(Path(td))])
            self.assertEqual(rc, 0)
            self.assertIn("UNALIGNED", stdout)
            rc, stdout, _ = run_main([str(Path(td)), "--crossing-gate", "0.5"])
            self.assertEqual(rc, 1)
            self.assertIn("GATE nodes=1 FAIL sites-1 UNALIGNED", stdout)

    def test_find_rungs_sorts_numerically_and_reports_rep_and_control(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            order = ["nodes=5/laneE/sites-1", "nodes=5/laneE/sites-1-rep2", "nodes=5/laneE/sites-2",
                     "nodes=5/laneE/sites-10", "nodes=7/laneE/sites-2", "nodes=10/laneE/sites-1"]
            for name in reversed(order):
                (root / "results" / name).mkdir(parents=True)
            self.assertEqual([p.relative_to(root / "results").as_posix() for p in find_rungs(root)], order)
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            rep = results / "nodes=1/laneE/sites-1-rep2"
            shutil.copytree(results / SMOKE, rep)
            (rep / "rung.txt").write_text((rep / "rung.txt").read_text().replace("control=no", "control=yes"))
            r = extract_rung(rep)
            self.assertEqual((r["rep"], r["control"]), (2, "yes"))
            rows = report_rows([extract_rung(results / SMOKE), r])
            self.assertEqual([row[2:4] for row in rows], [["rep", "control"], ["1", "no"], ["2", "yes"]])

    def test_crossing_gate_judges_every_broker_not_the_aggregate(self):
        with tempfile.TemporaryDirectory() as td:
            stage(Path(td), "smoke-n1")
            self.synthetic_rung(Path(td))
            rc, stdout, stderr = run_main(["--crossing-gate", "0.5", str(Path(td))])
            self.assertEqual(rc, 0, stderr)
            self.assertIn("GATE nodes=1 PASS", stdout)
            self.assertIn("GATE nodes=3 PASS 2 rungs, cert=canary", stdout)
        # broker0 forwards 1% of its own traffic; the cluster's aggregate is 0.46%.
        with tempfile.TemporaryDirectory() as td:
            r = extract_rung(self.synthetic_rung(Path(td), fwd_in_window={0: 6000}))
            self.assertLess(r["crossing"] * 100, 0.5)
            self.assertAlmostEqual(r["max_crossing"], 0.01)
            rc, stdout, _ = run_main(["--crossing-gate", "0.5", str(Path(td))])
            self.assertEqual(rc, 1)
            self.assertIn("GATE nodes=3 FAIL sites-4 broker0 crossing 1.00% > 0.5%", stdout)
            self.assertNotIn("broker1 crossing", stdout)
        for defect, reason in (("idle-broker", "sites-4 broker2 idle (window received 0)"),
                               ("peer-link-lost", "sites-4 broker1 peer_links=1 at window-close (want 2)"),
                               ("peer-link-late", "sites-4 broker1 peer_links=1 at window-open (want 2)")):
            with self.subTest(defect=defect), tempfile.TemporaryDirectory() as td:
                rdir = self.synthetic_rung(Path(td))
                if defect == "idle-broker":
                    shutil.copy(rdir / "metrics-window-open-broker2.prom", rdir / "metrics-window-close-broker2.prom")
                else:
                    edge = "close" if defect == "peer-link-lost" else "open"
                    path = rdir / f"metrics-window-{edge}-broker1.prom"
                    path.write_text(path.read_text().replace("mqttd_peer_links 2", "mqttd_peer_links 1"))
                rc, stdout, _ = run_main(["--crossing-gate", "0.5", str(Path(td))])
                self.assertEqual(rc, 1)
                self.assertIn(reason, stdout)


    def test_crossing_gate_fails_a_size_without_rungs(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF, "smoke-n1")
            shutil.rmtree(results / N3 / "sites-1")
            rc, stdout, _ = run_main(["--crossing-gate", "0.5", str(Path(td))])
            self.assertEqual(rc, 1)
            self.assertIn("GATE nodes=3 FAIL no sites-* rungs", stdout)

    def test_crossing_gate_fails_a_ladder_cut_short_of_its_shape(self):
        shape = (
            "brokers=1 drivers=1\n"
            "control rung: {control}\n\n"
            "  sites | publishers | consumers |   offered/s | containers | per driver | verdict\n"
            "      1 |        100 |         7 |        1000 |          2 |          2 | ok\n"
            "      2 |        200 |        14 |        2000 |          4 |          4 | ok\n"
        )
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            lane = results / "nodes=1/laneE"
            (lane / "shape.txt").write_text(shape.format(control="ON — the 1-site rung repeats after the ladder"))
            self.assertEqual(declared_rungs(lane), ["sites-1", "sites-2", "sites-1-rep2"])
            rc, stdout, _ = run_main(["--crossing-gate", "0.5", str(Path(td))])
            self.assertEqual(rc, 1)
            self.assertIn("GATE nodes=1 FAIL sites-2, sites-1-rep2 declared in shape.txt but not run", stdout)
            (lane / "shape.txt").write_text(shape.format(control="OFF").replace("      2 |", "   nope |"))
            rc, stdout, _ = run_main(["--crossing-gate", "0.5", str(Path(td))])
            self.assertEqual(rc, 0, stdout)
            self.assertIn("GATE nodes=1 PASS", stdout)

    def test_a_passing_local_canary_holds_nodes1_rungs_to_its_floors(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), "smoke-n1")
            lane = results / "nodes=1/laneE"
            shutil.copytree(HERE / "testdata/forward-canary/n1", lane / "forward-canary")
            (lane / "forward-canary.txt").write_text("status=pass-local nodes=1 count=100\n")
            canary = load_canary(lane, 1)
            rdir = results / SMOKE
            window = load_window(rdir, 1)
            labels = ("before", "window-open", "window-close", "drain", "after")
            snaps = {label: load_snap(rdir, label, 1) for label in labels}
            self.assertEqual(certify(1, snaps, window, canary), "structural")
            # A restart after the control: a fresh process's counters sit below its floors.
            snaps["before"] = {0: parse_prom(lane / "forward-canary/metrics-pre-broker0.prom")}
            with self.assertRaisesRegex(ValueError, "broker0 before: received=2 below its canary floor 102"):
                certify(1, snaps, window, canary)

    def test_an_unaligned_nodes3_rung_is_not_bound_to_the_canary_process(self):
        with tempfile.TemporaryDirectory() as td:
            results = stage(Path(td), PROOF)
            rdir = results / N3 / "sites-1"
            (rdir / "window.tsv").unlink()
            for path in rdir.glob("metrics-window-*.prom"):
                path.unlink()
            (rdir / "rung.txt").write_text((rdir / "rung.txt").read_text().replace("window=aligned", "window=lifetime"))
            r = extract_rung(rdir)
            self.assertFalse(r["aligned"])
            self.assertEqual(r["cert"], "canary-unbound")
            rc, stdout, _ = run_main(["--crossing-gate", "0.5", str(Path(td))])
            self.assertEqual(rc, 1)
            self.assertIn("sites-1 UNALIGNED; sites-1 cert=canary-unbound", stdout)

class SkewTests(unittest.TestCase):
    """Ingress skew (#613): under prefer-local the busiest broker bounds the rung."""

    @staticmethod
    def brokers(rx: list[float]) -> list[dict]:
        return [{"broker": b, "received": v} for b, v in enumerate(rx)]

    def test_an_even_split_uses_every_broker(self):
        skew, eff = ingress_skew(self.brokers([60_000] * 5))
        self.assertAlmostEqual(skew, 1.0)
        self.assertAlmostEqual(eff, 5.0)

    def test_five_pools_over_seven_brokers_cap_below_seven(self):
        # Five equal publisher pools over seven brokers, two taking a double
        # share: the merged total looks healthy, the busiest broker says the
        # rung can use at most 4.5 brokers' worth of prefer-local work.
        skew, eff = ingress_skew(self.brokers([v * 30_000 for v in (2, 2, 1, 1, 1, 1, 1)]))
        self.assertAlmostEqual(skew, 2 / (9 / 7))
        self.assertAlmostEqual(eff, 4.5)

    def test_the_busiest_broker_is_found_whatever_its_index(self):
        skew, eff = ingress_skew(self.brokers([10, 10, 40]))
        self.assertAlmostEqual(skew, 2.0)
        self.assertAlmostEqual(eff, 1.5)

    def test_no_ingress_is_unknown_not_one(self):
        self.assertEqual(ingress_skew(self.brokers([0, 0, 0])), (None, None))
        self.assertEqual(ingress_skew([]), (None, None))


class MpstatTests(unittest.TestCase):
    def test_mpstat_rows_across_midnight_stay_in_order(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "cpu-broker0.txt"
            base = datetime(2026, 9, 14, 23, 59, 50, tzinfo=timezone.utc).timestamp()
            lines = ["CPU_STREAM_START_UTC 2026-09-14T23:59:50Z"]
            for t in range(int(base) + 1, int(base) + 21):
                lines.append(f"{datetime.fromtimestamp(t, timezone.utc):%H:%M:%S}  all  0 0 0 0 0 0 0 0 0 {50.0 if t > base + 10 else 0.0}")
            path.write_text("\n".join(lines) + "\n")
            # 00:00:01 .. 00:00:10 after midnight only.
            self.assertEqual(windowed_idle(path, base + 10, base + 21), [50.0] * 10)

    def test_one_pinned_core_is_found_behind_an_idle_host_mean(self):
        """One scheduler pinned on an 8-core driver: the `all` row says 87% idle,
        and that is exactly the reading the per-core measure exists to see past.
        Samples outside the window must not count either way."""
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td)
            (rdir / "cpu").mkdir()
            base = int(datetime(2026, 9, 19, 2, 0, 0, tzinfo=timezone.utc).timestamp())
            lines = ["CPU_STREAM_START_UTC 2026-09-19T02:00:00Z"]
            for t in range(base + 1, base + 41):
                stamp = f"{datetime.fromtimestamp(t, timezone.utc):%H:%M:%S}"
                pinned = base + 10 < t <= base + 30  # busy only inside the window
                lines.append(f"{stamp}  all  0 0 0 0 0 0 0 0 0 {87.5 if pinned else 100.0}")
                for core in range(8):
                    idle = 0.0 if pinned and core == 3 else 100.0
                    lines.append(f"{stamp}    {core}  0 0 0 0 0 0 0 0 0 {idle}")
            (rdir / "cpu" / "cpu-driver0.txt").write_text("\n".join(lines) + "\n")
            (rdir / "cpu" / "cpu-broker0.txt").write_text("\n".join(lines) + "\n")
            edge = lambda t: (t * 1000, t * 1000)
            window = {"hosts": {"driver0": {"open": edge(base + 12), "close": edge(base + 28)}}}
            worst = core_saturation(rdir, "driver", window)
            self.assertEqual((worst["host"], worst["core"], worst["share"]), ("driver0", 3, 1.0))
            self.assertEqual(worst["samples"], 15)  # t-1 >= open and t+1 <= close
            # The whole stream, ramp included: 20 pinned seconds of 40.
            self.assertEqual(core_saturation(rdir, "driver", None)["share"], 0.5)
            # A host with no usable window row is skipped, never guessed at.
            self.assertIsNone(core_saturation(rdir, "broker", window))


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("results", nargs="?", type=Path, help="run dir or results/ tree")
    parser.add_argument("--crossing-gate", type=float, metavar="PCT",
                        help="per size, PASS only if every rung is valid, aligned and certified, every broker is "
                             "linked to every peer at both window edges, none is idle, and each one's crossing <= PCT percent")
    parser.add_argument("--self-test", action="store_true")
    args = parser.parse_args(argv)
    if args.self_test:
        loader = unittest.defaultTestLoader
        suite = unittest.TestSuite(
            loader.loadTestsFromTestCase(case) for case in (ScrapeTests, CertificationTests, WindowTests, SkewTests, MpstatTests)
        )
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        return 0 if result.wasSuccessful() else 1
    if args.results is None:
        parser.error("results path required (or --self-test)")
    if args.crossing_gate is not None and not (math.isfinite(args.crossing_gate) and args.crossing_gate >= 0):
        parser.error("--crossing-gate needs a finite, non-negative percentage")
    paths = find_rungs(args.results)
    if not paths:
        print(f"no laneE sites-* under {args.results}", file=sys.stderr)
        return 2
    rungs: list[dict] = []
    outcomes: list[tuple[Path, dict | None]] = []
    canaries: dict = {}
    invalid = False
    for path in paths:
        try:
            r = extract_rung(path, canaries)
        except (ValueError, OSError) as exc:
            print(f"INVALID {path}: {exc}", file=sys.stderr)
            outcomes.append((path, None))
            invalid = True
            continue
        rungs.append(r)
        outcomes.append((path, r))
    if rungs:
        print_report(rungs)
    failed = False
    if args.crossing_gate is not None:
        for passed, line in gate_lines(args.results, outcomes, args.crossing_gate):
            print(line)
            failed = failed or not passed
    return 1 if invalid or failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
