#!/usr/bin/env python3
"""Render the cross-broker single-node comparison (ADR 0048 T4) from raw results.

    ./summarize-compare.py .runs/<stamp>/results [--p99-budget-ms 1000]

One broker at a time on ONE provisioned broker host, driven by emqtt-bench from
separate driver hosts. Emits markdown to stdout for hand-transcription into
docs/benchmarks/ — the raw results stay untracked scratch (the DURABLE-PATH
precedent: the doc is the record, and hand-transcription forces a human read).

What this module is for: per broker, the KNEE — the highest offered rate whose
rung passed every gate — plus the per-rung detail that shows why the next rung
did not. Nothing here reads a broker's own counters (see the "what this is not"
block it prints): three of the four brokers in this lane export different things
under different names, and a comparison built on four vendors' self-reports is a
comparison of their instrumentation. The drivers are the only instrument all
four arms share, so the drivers are the only instrument used.

Honesty mechanics, enforced here rather than remembered:
  - The same four gates as the cluster ladders (offer met, delivered, p99
    budget, settled+drained), at the same thresholds, imported from
    summarize-curve.py rather than re-typed — a second copy of DRIVER_OK is a
    second thing to forget to update.
  - Late publishers (`pub_overrun`) are REPORTED, never gated. On this rig the
    windowed figure is noisy: it has read 7% on a rung whose lifetime share was
    0.8%. Both are printed so a reader can see the disagreement instead of
    inheriting whichever one the summarizer picked.
  - A run is a SEQUENCE on one host, so every arm is confounded by the arms
    before it. A closing `-control` arm re-runs the first broker; if its knee or
    its delivered rate has moved, the sequence measured drift and the whole
    comparison is void. That verdict is printed, not left to the reader.
"""

from __future__ import annotations

import argparse
import importlib.util
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CURVE_SCRIPT = HERE / "summarize-curve.py"


def _load_curve():
    """Import summarize-curve.py by path — its filename has a dash.

    Precedent: extract-lane-e.py loads forward-canary.py the same way. The
    alternative is a third copy of the emqtt-bench counter arithmetic, and the
    QoS 0 double-count correction is exactly the kind of fix that lands in one
    copy and not the others.
    """
    if not CURVE_SCRIPT.is_file():
        sys.exit(f"{CURVE_SCRIPT} is missing: the shared counter parsing cannot be imported")
    spec = importlib.util.spec_from_file_location("summarize_curve", CURVE_SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


_CURVE = _load_curve()

# Reused verbatim from summarize-curve.py — see its docstrings for WHY each is
# shaped the way it is (the minute-mark timestamp bug, the ramp-vs-steady window,
# the QoS 0 (pub + pub_succ)/2 correction, and the `-base.prom` subtraction that
# keeps a rung's connect ramp out of its published tail).
driver_rate = _CURVE.driver_rate
merged_histogram = _CURVE.merged_histogram
bucket_pct = _CURVE.bucket_pct
p99_ms = _CURVE.p99_ms
DRIVER_OK = _CURVE.DRIVER_OK  # 0.97 — a rung counts only if the offer was reached
KNEE_OK = _CURVE.KNEE_OK  # 0.99 — delivered/sent a passing rung must reach
LATE_OK = _CURVE.LATE_OK  # 0.05 — reported here, deliberately not gated on
CONTROL_OK = _CURVE.CONTROL_OK  # 0.05 — drift the closing control may show

ARM_DIR = re.compile(r"^(\d+)-(.+?)(-control)?$")
RUNG_DIR = re.compile(r"^rung-(\d+)$")
# `mpstat -P ALL 1` under LC_ALL=C TZ=UTC (cpu.sh): one `all` row per second,
# stamped with the END of its one-second interval, %idle last.
MPSTAT_ALL = re.compile(r"^\d\d:\d\d:\d\d\s+all\s")
MPSTAT_AVERAGE = re.compile(r"^Average:\s+all\s")


# ── the run's flat files ─────────────────────────────────────────────────────


def load_kv(path: Path) -> dict[str, str]:
    """key=value tokens from broker.txt / rung.txt, whitespace- or line-separated.

    Tolerant on purpose: broker.txt is written one pair per line and rung.txt as
    a single line of pairs, and a summarizer that cares which is a summarizer
    that breaks the next time the runner reflows a heredoc.
    """
    out: dict[str, str] = {}
    if not path.is_file():
        return out
    for tok in path.read_text(errors="replace").split():
        if "=" in tok:
            k, v = tok.split("=", 1)
            out[k] = v
    return out


def _secs(stamp: str) -> int | None:
    """HH:MM:SS -> seconds since midnight UTC."""
    m = re.match(r"^(\d{2}):(\d{2}):(\d{2})$", stamp.strip())
    if not m:
        return None
    h, mi, se = (int(x) for x in m.groups())
    return h * 3600 + mi * 60 + se


def window_bounds(rdir: Path) -> tuple[int, int] | None:
    """The rung's measured window, stamped on the broker's own clock.

    Returns None for a rung recorded before the lane stamped its window; those
    fall back to the whole stream and the report says so.
    """
    try:
        open_s = _secs((rdir / "window-open.utc").read_text())
        close_s = _secs((rdir / "window-close.utc").read_text())
    except OSError:
        return None
    if open_s is None or close_s is None:
        return None
    if close_s < open_s:  # the window crossed midnight
        close_s += 86400
    return open_s, close_s


def idle_rows(path: Path, bounds: tuple[int, int] | None = None) -> list[float]:
    """%idle of every `all` row in one mpstat stream, or its Average: rows.

    Adapted from extract-lane-e.py's `mpstat_rows`. With `bounds`, only rows
    inside the rung's measured window count: a rung's stream also covers settle
    and drain, and a broker that drains slowly collects more near-idle rows than
    a fast one, which would read as the slow broker using less CPU. Per-CPU rows
    are skipped because a mean over them hides a single pinned core behind idle
    ones.
    """
    rows: list[float] = []
    averages: list[float] = []
    if not path.is_file():
        return rows
    for line in path.read_text(errors="replace").splitlines():
        try:
            idle = float(line.split()[-1])
        except (IndexError, ValueError):
            continue
        if MPSTAT_AVERAGE.match(line):
            averages.append(idle)
        elif MPSTAT_ALL.match(line):
            if bounds is not None:
                at = _secs(line.split()[0])
                if at is None:
                    continue
                lo, hi = bounds
                if at < lo:  # the stream may have crossed midnight before the window did
                    at += 86400
                if not lo <= at <= hi:
                    continue
            rows.append(idle)
    # An Average: line describes the whole stream, so it cannot answer a windowed
    # question; only an unwindowed call may fall back to it.
    return rows or ([] if bounds is not None else averages)


def cpu_idle(rdir: Path, role: str) -> dict:
    """Mean of the per-host means and the single lowest 1 s row, for one role.

    The lowest row is carried beside the mean because a broker that saturates
    for one second in ten still reports a comfortable mean, and "CPU was not the
    limit" is the single most load-bearing claim a knee makes.
    """
    means: list[float] = []
    lows: list[float] = []
    bounds = window_bounds(rdir) if role == "broker" else None
    for path in sorted((rdir / "cpu").glob(f"cpu-{role}*.txt")):
        rows = idle_rows(path, bounds)
        if not rows and bounds is not None:
            # Stamped, but no sample landed inside the window: report the stream
            # rather than nothing, and mark it, because an unmarked whole-stream
            # mean is the bias this alignment exists to remove.
            rows = idle_rows(path)
            if rows:
                bounds = None
        if not rows:
            continue
        means.append(sum(rows) / len(rows))
        lows.append(min(rows))
    return {
        "mean": sum(means) / len(means) if means else None,
        "min_1s": min(lows) if lows else None,
        "hosts": len(means),
        "windowed": bounds is not None,
    }


def format_idle(s: dict) -> str:
    if s["mean"] is None:
        return "—"
    # A star is not decoration: it says the mean covers the rung, not the window.
    star = "" if s.get("windowed", True) else "*"
    return f"{s['mean']:.0f}/{s['min_1s']:.0f}%{star}"


def container_mem(rdir: Path) -> str:
    """The broker container's resident memory from `docker stats --no-stream`.

    The MemUsage column is itself three tokens ("1.2GiB / 30.5GiB"), so the line
    is matched rather than split: a naive split by whitespace reads the limit as
    the usage on every row, which makes every broker look identical.
    """
    path = rdir / "mem-broker.txt"
    if not path.is_file():
        return "—"
    for line in path.read_text(errors="replace").splitlines():
        m = re.search(r"(\S+)\s*/\s*(\S+)", line)
        if m:
            return m.group(1)
    return "—"


# ── one rung ─────────────────────────────────────────────────────────────────


def driver_totals(rdir: Path) -> dict:
    """What the drivers say they sent and received, over the rung's steady window."""
    sent = sent_rate = 0.0
    late_rate = late_total = 0.0
    for log in sorted(rdir.glob("pub-*.log")):
        # emqtt-bench 0.6.3 counts a QoS 0 publish TWICE; (pub + pub_succ)/2 is
        # the real figure at either QoS. Full derivation in summarize-curve.py's
        # `lane_b_rung`. This lane is QoS 0 throughout, so it is ALWAYS the
        # doubled case — an uncorrected read would halve every delivered ratio
        # here and report loss on four healthy brokers.
        t_pub, r_pub = driver_rate(log, "pub")
        t_succ, r_succ = driver_rate(log, "pub_succ")
        sent += (t_pub + t_succ) // 2
        sent_rate += (r_pub + r_succ) / 2
        t_late, r_late = driver_rate(log, "pub_overrun")
        late_rate += r_late
        late_total += t_late
    recv = recv_rate = 0.0
    for log in sorted(rdir.glob("sub-*.log")):
        t, r = driver_rate(log, "recv")
        recv += t
        recv_rate += r
    # The RATE comes from the steady window (`sub-*.log`, dumped while the
    # publishers were still running); the DELIVERY TOTAL comes from after the
    # drain (`sub-*.drain`). Reading the rate from the drained log would average
    # a decaying tail into the rung and understate every one of them. `max`
    # because `recv` is cumulative and read twice: the later read can only be
    # larger, so this is a no-op on any real pair and a floor under a partial
    # drain dump.
    settled = max(recv, sum(driver_rate(log, "recv")[0] for log in sorted(rdir.glob("sub-*.drain"))))
    return {
        "sent": sent,
        "sent_rate": sent_rate,
        "recv_rate": recv_rate,
        "settled": settled,
        "late_rate": late_rate,
        "late_total": late_total,
    }


def rung_stats(rdir: Path, budget: float) -> dict:
    """One rung, its four gates, and everything the tables print about it."""
    meta = load_kv(rdir / "rung.txt")
    m = RUNG_DIR.match(rdir.name)
    dir_offered = float(m.group(1)) if m else 0.0
    if not meta:
        # rung.txt is the LAST thing a rung writes, so its absence means the rung
        # is still in flight (or died). Without this a running rung reads as
        # offered=0 with no histogram, which every gate below then reports as a
        # failure — a live rung looking like a broken broker.
        return {
            "offered": dir_offered,
            "sent_rate": 0.0,
            "recv_rate": 0.0,
            "delivered": None,
            "p99": "—",
            "late_window": None,
            "late_lifetime": None,
            "cpu": {"mean": None, "min_1s": None, "hosts": 0},
            "mem": "—",
            "pass": False,
            "incomplete": True,
            "flags": ["INCOMPLETE (no rung.txt — still running, or the rung died)"],
        }

    offered = float(meta.get("offered", dir_offered))
    d = driver_totals(rdir)
    # The BASELINES are excluded from the scrape list. `sub-*.prom` matches
    # `sub-d0-base.prom` too, and `merged_histogram` then adds each baseline in
    # as a scrape of its own (it has no baseline of its own to subtract) at the
    # same moment it subtracts it from the real scrape — so the ramp's tail comes
    # back at its own buckets and the correction is only half applied. On the
    # fixture in `self_test` that alone moves p99 from <=10ms to <=5000ms.
    scrapes = [p for p in sorted(rdir.glob("sub-*.prom")) if not p.name.endswith("-base.prom")]
    buckets, count = merged_histogram(scrapes)
    p99 = bucket_pct(buckets, count, 0.99)

    flags: list[str] = []
    driver_cpu = cpu_idle(rdir, "driver")
    if offered and d["sent_rate"] < DRIVER_OK * offered:
        # The drivers' own idle goes INTO this flag rather than into a column of
        # its own, because it is the first question anyone asks of an unmet
        # offer and the one this measurement cannot otherwise answer: a
        # shortfall is either the drivers failing to offer or the broker
        # refusing to take it, and pinned drivers make the rung a statement
        # about the drivers.
        flags.append(
            f"OFFER NOT MET ({d['sent_rate'] / offered * 100:.0f}% of offer; "
            f"driver CPU idle {format_idle(driver_cpu)})"
        )
    delivered = d["settled"] / d["sent"] if d["sent"] else None
    if not d["sent"]:
        flags.append("NO TRAFFIC (the publishers logged nothing)")
    elif delivered < KNEE_OK:
        flags.append(f"UNDER-DELIVERED ({(1 - delivered) * 100:.1f}% of what was published never arrived)")
    if p99_ms(p99) > budget:
        flags.append(f"OVER P99 BUDGET ({p99} > {budget:g}ms)")
    # settled/drained are the runner's own statements about the rung's edges, and
    # both are gates rather than notes: an unsettled rung measured a broker still
    # filling up, and an undrained one cannot tell pending traffic from loss, so
    # neither can support a knee no matter how good its numbers look.
    if meta.get("settled") != "yes":
        flags.append("UNSETTLED (the window opened before every client had connected)")
    if meta.get("drained") != "yes":
        flags.append("NOT DRAINED (the drain deadline expired with traffic still moving)")

    return {
        "offered": offered,
        "sent_rate": d["sent_rate"],
        "recv_rate": d["recv_rate"],
        "delivered": delivered,
        "p99": p99,
        # Two readings of the same counter, neither of which gates. The windowed
        # share is the slope over the steady window; the lifetime share is the
        # final total over everything the drivers sent. They disagree whenever
        # the rung is much longer than the steady window, which is most of them.
        "late_window": d["late_rate"] / d["sent_rate"] if d["sent_rate"] else None,
        "late_lifetime": d["late_total"] / d["sent"] if d["sent"] else None,
        "cpu": cpu_idle(rdir, "broker"),
        "driver_cpu": driver_cpu,
        "mem": container_mem(rdir),
        "meta": meta,
        "pass": not flags,
        "incomplete": False,
        "flags": flags,
    }


# ── one arm (one broker, one run of the ladder) ──────────────────────────────


def arm_rungs(arm_dir: Path, budget: float) -> list[dict]:
    rungs = []
    for d in sorted(arm_dir.iterdir() if arm_dir.is_dir() else []):
        if d.is_dir() and RUNG_DIR.match(d.name):
            rungs.append(rung_stats(d, budget))
    return sorted(rungs, key=lambda r: r["offered"])


def load_arms(root: Path, budget: float) -> list[dict]:
    """Every arm under <results>/compare, in the order the runner ran them.

    The directory INDEX orders the sequence, not the name and not the mtime: the
    control arm exists precisely because order matters, and sorting these
    alphabetically would put `10-mqttd-control` second.
    """
    base = root / "compare"
    if not base.is_dir():
        return []
    arms = []
    for d in sorted(base.iterdir()):
        m = ARM_DIR.match(d.name)
        if not d.is_dir() or not m:
            continue
        meta = load_kv(d / "broker.txt")
        arms.append(
            {
                "dir": d,
                "index": int(meta.get("arm", m.group(1))),
                # broker.txt is authoritative; the directory name is the fallback
                # for an arm whose broker.txt never landed (a container that died
                # at startup still leaves its rung dirs behind).
                "broker": meta.get("broker", m.group(2)),
                "control": meta.get("control", "yes" if m.group(3) else "no") == "yes",
                "meta": meta,
                "host": (d / "host.txt").read_text(errors="replace") if (d / "host.txt").is_file() else "",
                "rungs": arm_rungs(d, budget),
            }
        )
    return sorted(arms, key=lambda a: a["index"])


def knee(rungs: list[dict]) -> dict | None:
    """The highest offered rate whose rung passed every gate.

    Highest PASSING, not "highest below the first failure": a broker that fails
    one rung and recovers above it has not established the higher rate as a
    sustained figure, but it has established the lower one, and silently
    dropping the recovered rung would hide the inconsistency rather than report
    it. The first failing rate is printed beside the knee for exactly that
    reason.
    """
    passed = [r for r in rungs if r["pass"]]
    return max(passed, key=lambda r: r["offered"]) if passed else None


def first_failure(rungs: list[dict]) -> dict | None:
    failed = [r for r in rungs if not r["pass"]]
    return min(failed, key=lambda r: r["offered"]) if failed else None


def control_verdict(control: dict, base: dict | None) -> tuple[bool, str]:
    """Whether the closing control arm re-derives the arm it repeats.

    A run is a sequence of brokers on ONE host: kernel state, page cache, a NIC
    queue that never recovered, a noisy neighbour on the hypervisor. If the same
    broker run last does not reproduce the same knee at the same delivered rate,
    every difference the table reports could be drift between arm 1 and arm N
    rather than a difference between brokers — so the verdict is on the RUN, not
    on the control arm.
    """
    if base is None:
        return False, f"no non-control arm for {control['broker']} to compare against"
    ck, bk = knee(control["rungs"]), knee(base["rungs"])
    if ck is None or bk is None:
        which = "control" if ck is None else f"arm {base['index']}"
        return False, f"the {which} arm established no knee at all, so the pair cannot be compared"
    if ck["offered"] != bk["offered"]:
        return False, (
            f"the control knee is {ck['offered']:,.0f}/s against {bk['offered']:,.0f}/s "
            f"for arm {base['index']}"
        )
    # Below here the two arms are known to share a knee, so the wording never
    # says "knee is" again: the self-test distinguishes the two verdicts by their
    # text, and two messages that share a phrase make a mutated rule look tested.
    if bk["recv_rate"] <= 0:
        return False, "the opening arm's knee delivered nothing measurable"
    drift = abs(ck["recv_rate"] - bk["recv_rate"]) / bk["recv_rate"]
    if drift > CONTROL_OK:
        return False, (
            f"the control delivered {ck['recv_rate']:,.0f}/s at that rate against "
            f"{bk['recv_rate']:,.0f}/s for arm {base['index']} — {drift * 100:.1f}% drift, "
            f"past the {CONTROL_OK * 100:.0f}% bound"
        )
    return True, (
        f"both arms knee at {ck['offered']:,.0f}/s, delivered {ck['recv_rate']:,.0f}/s "
        f"against {bk['recv_rate']:,.0f}/s ({drift * 100:.1f}% drift)"
    )


# ── rendering ────────────────────────────────────────────────────────────────


def run_stamp(root: Path) -> str:
    """The run's stamp from its results path (.runs/<stamp>/results)."""
    return root.parent.name if root.name == "results" else root.name


def host_line(arms: list[dict]) -> str:
    """The broker host, from the first arm that recorded one.

    host.txt is free text (uname, lscpu, server_type) and only two facts from it
    belong in a header: what the kernel said it was and what was paid for.
    """
    for arm in arms:
        if not arm["host"]:
            continue
        first = arm["host"].splitlines()[0].strip()
        m = re.search(r"server_type=(\S+)", arm["host"])
        model = re.search(r"Model name:\s*(.+)", arm["host"])
        bits = [first]
        if model:
            bits.append(model.group(1).strip())
        if m and m.group(0) not in first:
            bits.append(f"server_type={m.group(1)}")
        return " — ".join(bits)
    return "UNRECORDED (no host.txt in any arm)"


def driver_fleet(arms: list[dict]) -> str:
    hosts: set[str] = set()
    containers = ""
    for arm in arms:
        for rdir in sorted(arm["dir"].glob("rung-*")):
            for p in (rdir / "cpu").glob("cpu-driver*.txt"):
                hosts.add(p.stem.split("-", 1)[1])
            containers = containers or load_kv(rdir / "rung.txt").get("containers", "")
    fleet = ", ".join(sorted(hosts)) if hosts else "UNRECORDED (no driver CPU streams)"
    return f"{fleet}" + (f" — containers={containers}" if containers else "")


def num(value: float | None, fmt: str = ",.0f") -> str:
    return "—" if value is None else format(value, fmt)


def pct(value: float | None) -> str:
    return "—" if value is None else f"{value * 100:.1f}%"


def workload_line(arm: dict) -> str:
    """The workload every rung of this arm ran, from the first rung's rung.txt."""
    for r in arm["rungs"]:
        meta = r.get("meta") or {}
        if meta:
            return (
                f"publishers={meta.get('publishers', '?')} subscribers={meta.get('subscribers', '?')} "
                f"payload={meta.get('payload', '?')}B qos={meta.get('qos', '?')} "
                f"window={meta.get('window_secs', '?')}s settle={meta.get('settle_s', '?')}s"
            )
    return "no completed rung"


def render_header(root: Path, arms: list[dict]) -> list[str]:
    out = [
        "# Cross-broker single-node comparison (ADR 0048 T4)",
        "",
        f"run: `{run_stamp(root)}` — `{root}`",
        f"broker host: {host_line(arms)}",
        f"drivers: {driver_fleet(arms)}",
        "",
        "| arm | broker | version | image | digest | config sha256 | rungs |",
        "|---|---|---|---|---|---|---|",
    ]
    for arm in arms:
        meta = arm["meta"]
        label = arm["broker"] + (" (control)" if arm["control"] else "")
        # A missing digest prints as an em dash rather than being quietly
        # omitted: "which build was this" is the first question asked of any
        # published comparison, and a blank cell is a visible hole where an
        # absent column would be an invisible one.
        out.append(
            f"| {arm['index']} | {label} | {meta.get('version', '—')} | {meta.get('image', '—')} | "
            f"`{meta.get('digest', '—')}` | `{meta.get('config_sha256', '—')}` | {len(arm['rungs'])} |"
        )
    return out


def render_knees(arms: list[dict]) -> list[str]:
    out = [
        "",
        "## Knee per broker",
        "",
        "| arm | broker | knee offered | delivered/s | p99 | broker CPU idle (mean/min 1 s) | container mem | first failing rate |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for arm in arms:
        k = knee(arm["rungs"])
        f = first_failure(arm["rungs"])
        label = arm["broker"] + (" (control)" if arm["control"] else "")
        if k is None:
            cells = ["NONE — no rung passed", "—", "—", "—", "—"]
        else:
            cells = [
                f"{k['offered']:,.0f}/s",
                f"{k['recv_rate']:,.0f}",
                k["p99"],
                format_idle(k["cpu"]),
                k["mem"],
            ]
        fail = "none — the ladder stopped before this broker did" if f is None else (
            f"{f['offered']:,.0f}/s — " + "; ".join(f["flags"])
        )
        out.append(f"| {arm['index']} | {label} | " + " | ".join(cells) + f" | {fail} |")
    return out


def render_rungs(arm: dict) -> list[str]:
    label = arm["broker"] + (" (control)" if arm["control"] else "")
    out = [
        "",
        f"### Arm {arm['index']} — {label}",
        "",
        workload_line(arm),
        "",
        "| offered | sent/s | recv/s | delivered | p99 | late (window/lifetime) | broker CPU idle | mem | verdict |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for r in arm["rungs"]:
        verdict = "pass" if r["pass"] else "; ".join(r["flags"])
        out.append(
            f"| {r['offered']:,.0f} | {num(r['sent_rate'])} | {num(r['recv_rate'])} | "
            f"{pct(r['delivered'])} | {r['p99']} | {pct(r['late_window'])} / {pct(r['late_lifetime'])} | "
            f"{format_idle(r['cpu'])} | {r['mem']} | {verdict} |"
        )
    return out


def render_control(arms: list[dict]) -> list[str]:
    out = ["", "## Control check", ""]
    controls = [a for a in arms if a["control"]]
    if not controls:
        out.append(
            "No control arm ran, so nothing separates a difference between brokers from drift "
            "on the host across the sequence. Re-run with a closing control arm."
        )
        return out
    for arm in controls:
        base = next((a for a in arms if a["broker"] == arm["broker"] and not a["control"]), None)
        ok, why = control_verdict(arm, base)
        if ok:
            out.append(f"- **PASS** — arm {arm['index']} ({arm['broker']}) reproduces arm "
                       f"{base['index']}: {why}.")
        else:
            out.append(
                f"- **SEQUENCE VOID** — arm {arm['index']} ({arm['broker']}) does not reproduce "
                f"the arm it repeats: {why}. This sequence measures drift on the host, not the "
                f"brokers; no comparison above is a finding about any broker."
            )
    return out


def render_caveats() -> list[str]:
    return [
        "",
        "## What this is not",
        "",
        "- **One node.** No cluster, no replication, no failover — this measures a single broker",
        "  process on a single host, and says nothing about how any of them scale out.",
        "- **QoS 0 only**, so nothing here is an acknowledged or durable delivery figure. A broker",
        "  that is fast here may be slow where the guarantee is real.",
        "- **Plaintext and anonymous.** No TLS, no authentication, no authorization: the security",
        "  cost is measured by a different posture (ADR 0048 §3), and it is not free.",
        "- **No persistence anywhere**, including mqttd, whose durable-by-default is explicitly",
        "  turned OFF for this lane so the competitors' in-memory posture is the like-for-like one.",
        "  Fast because a guarantee was disabled is only honest when it says so; this says so.",
        "- **One config per broker, as committed** under `bench/scale/compare/` — a documented",
        "  reasonable minimum, not a tuned config, and not a vendor's best-effort tuning either.",
        "- **Driver-side measurement only.** Broker-internal counters are deliberately not used:",
        "  the four brokers export different things under different names, so a table built from",
        "  them would compare their instrumentation. emqtt-bench is the one instrument all arms",
        "  share — and it is EMQX's own tool, not ours.",
        "- **Latency figures are histogram bucket UPPER BOUNDS**, differenced against a baseline",
        "  scraped once the rung settled, so they describe the measured window and not the ramp.",
    ]


def render(root: Path, budget: float) -> str:
    arms = load_arms(root, budget)
    if not arms:
        return f"no arm directories under {root / 'compare'}"
    out = render_header(root, arms)
    out += render_knees(arms)
    out += ["", "## Per-rung detail"]
    for arm in arms:
        out += render_rungs(arm)
    out += render_control(arms)
    out += render_caveats()
    out += ["", "> Raw results are untracked scratch; cite only tracked paths in the doc."]
    return "\n".join(out)


# ── self-test ────────────────────────────────────────────────────────────────


def _write_counter_log(path: Path, counters: dict[str, int], secs: int, final: dict[str, int] | None = None) -> None:
    """emqtt-bench progress lines: cumulative totals, `Ns` then `NmNs`."""
    lines = []
    for s in range(secs + 1):
        stamp = f"{s // 60}m{s % 60}s" if s >= 60 else f"{s}s"
        for name, rate in counters.items():
            lines.append(f"{stamp} {name} total={rate * s} rate={rate}/sec")
    if final is not None:
        stamp = f"{(secs + 1) // 60}m{(secs + 1) % 60}s"
        for name, total in final.items():
            lines.append(f"{stamp} {name} total={total} rate=0/sec")
    path.write_text("\n".join(lines) + "\n")


def _write_overrun_log(path: Path, sent_rate: int, late_rate: int, late_total: int, secs: int) -> None:
    """A publisher log whose WINDOWED late slope differs from its lifetime share.

    The last 60 s carry `late_rate`/s; everything before them carries whatever is
    left of `late_total`. That gap is the whole reason both figures are printed.
    """
    ramp = max(secs - _CURVE.STEADY_WINDOW, 1)
    early = max(late_total - late_rate * _CURVE.STEADY_WINDOW, 0)
    lines = []
    for s in range(secs + 1):
        stamp = f"{s // 60}m{s % 60}s" if s >= 60 else f"{s}s"
        overrun = early * min(s, ramp) // ramp + late_rate * max(s - ramp, 0)
        lines.append(f"{stamp} pub total={2 * sent_rate * s} rate={2 * sent_rate}/sec")
        lines.append(f"{stamp} pub_succ total=0 rate=0/sec")
        lines.append(f"{stamp} pub_overrun total={overrun} rate={late_rate}/sec")
    path.write_text("\n".join(lines) + "\n")


def _write_prom(path: Path, fast: int, slow: int, fast_le: float = 10.0, slow_le: float = 5000.0) -> None:
    """A cumulative e2e_latency histogram: `fast` under fast_le, `slow` under slow_le."""
    lines = []
    for le in (1, 5, 10, 50, 100, 500, 1000, 5000):
        below = (fast if le >= fast_le else 0) + (slow if le >= slow_le else 0)
        lines.append(f'e2e_latency_bucket{{le="{le}"}} {below}')
    lines.append(f'e2e_latency_bucket{{le="+Inf"}} {fast + slow}')
    lines.append(f"e2e_latency_count {fast + slow}")
    path.write_text("\n".join(lines) + "\n")


def _write_cpu(path: Path, idles: list[float]) -> None:
    """An `mpstat -P ALL 1` stream, per-CPU rows included.

    The per-CPU rows carry a deliberately uneven spread — one core far busier
    than the `all` row — because they are what a row filter that stopped
    matching would pull in, and a mean over them is a different (flattering or
    alarming, never correct) number than the `all` series.
    """
    lines = ["CPU_STREAM_START_UTC 2026-09-16T10:00:00Z", "Linux 6.8.0 (compare) 09/16/26 _x86_64_ (8 CPU)", ""]
    for i, idle in enumerate(idles):
        stamp = f"10:00:{i + 1:02d}"
        lines.append(f"{stamp}     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle")
        lines.append(f"{stamp}     all    1.00    0.00    1.00    0.00    0.00    0.50    0.00    0.00    0.00   {idle:.2f}")
        for cpu, delta in enumerate((-40.0, 8.0, 8.0, 8.0)):
            lines.append(
                f"{stamp}       {cpu}    1.00    0.00    1.00    0.00    0.00    0.50    0.00    0.00    0.00   "
                f"{min(max(idle + delta, 0.0), 100.0):.2f}"
            )
        lines.append("")
    path.write_text("\n".join(lines) + "\n")


def _fixture_rung(
    arm: Path,
    offered: int,
    *,
    sent: int,
    recv: int,
    secs: int = 300,
    settled_total: int | None = None,
    p99_fast_le: float = 10.0,
    late_rate: int = 0,
    late_total: int = 0,
    ramp_slow: int = 0,
    settled: str = "yes",
    drained: str = "yes",
    idles: tuple[float, ...] = (92.0, 88.0, 90.0),
) -> None:
    """One rung directory exactly as the runner writes it."""
    d = arm / f"rung-{offered}"
    (d / "cpu").mkdir(parents=True)
    # `recv` is the subscribers' AGGREGATE rate, so recv < sent is loss and
    # recv > sent is fan-out; `settled_total` overrides only what the post-drain
    # dump reports, which is how a rung whose tail arrived after the window is
    # told from one that lost it.
    (d / "rung.txt").write_text(
        f"broker={arm.name.split('-', 1)[1]} arm={arm.name.split('-')[0]} offered={offered} "
        f"publishers=2000 subscribers=200 payload=64 qos=0 window_secs={secs} settle_s=30 "
        f"settled={settled} drained={drained} drain_secs=15 containers=8\n"
    )
    if late_rate or late_total:
        _write_overrun_log(d / "pub-d0.log", sent, late_rate, late_total, secs)
    else:
        # QoS 0 doubles `pub` and never touches `pub_succ`; the corrected read is
        # (pub + pub_succ)/2, so a rung that really sent N/s writes 2N to `pub`.
        _write_counter_log(d / "pub-d0.log", {"pub": 2 * sent, "pub_succ": 0, "pub_overrun": 0}, secs)
    _write_counter_log(d / "sub-d1.log", {"recv": recv}, secs)
    total_recv = settled_total if settled_total is not None else recv * secs
    _write_counter_log(d / "sub-d1.drain", {"recv": recv}, secs, final={"recv": total_recv})
    # The baseline carries the ramp's slow tail; the after-scrape carries the
    # ramp AND the window. Subtracting one from the other is what leaves the
    # window alone.
    _write_prom(d / "sub-d1-base.prom", 0, ramp_slow)
    _write_prom(d / "sub-d1.prom", recv * secs, ramp_slow, fast_le=p99_fast_le)
    _write_cpu(d / "cpu" / "cpu-broker0.txt", list(idles))
    _write_cpu(d / "cpu" / "cpu-driver0.txt", [95.0, 94.0])
    (d / "mem-broker.txt").write_text("compare-broker  812.4MiB / 30.51GiB  431.20%\n")


def _fixture_arm(root: Path, index: int, broker: str, *, control: bool = False) -> Path:
    name = f"{index}-{broker}" + ("-control" if control else "")
    d = root / "compare" / name
    d.mkdir(parents=True)
    (d / "broker.txt").write_text(
        f"broker={broker}\nimage=ghcr.io/example/{broker}:1.0\ndigest=sha256:{'ab' * 16}\n"
        f"config_sha256={'cd' * 16}\nstarted_unix=1758000000\nstopped_unix=1758003600\n"
        f"arm={index}\ncontrol={'yes' if control else 'no'}\n"
    )
    (d / "host.txt").write_text(
        "Linux compare-broker 6.8.0-138-generic #1 SMP x86_64 GNU/Linux\n"
        "Model name:  AMD EPYC 9454\nserver_type=ccx23\n"
    )
    return d


def self_test() -> None:
    """Pin every rule this summarizer applies, on synthesized rungs.

    No cloud, no cost. Each case is written so that INVERTING the rule it covers
    makes it fail: a gate that stopped firing, a gate that fired on a clean rung,
    a control that accepted drift, a late share that started gating.
    """
    import tempfile

    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if not cond:
            failures.append(msg)

    with tempfile.TemporaryDirectory() as td:
        root = Path(td) / "2026-09-16T10-00-00" / "results"

        # Arm 1 — a broker that carries 200k and fails at 300k on delivery.
        a1 = _fixture_arm(root, 1, "mqttd")
        _fixture_rung(a1, 100_000, sent=100_000, recv=100_000)
        _fixture_rung(a1, 200_000, sent=200_000, recv=200_000)
        # The ramp tail on this rung is deliberately large enough to move an
        # unsubtracted p99 into the 5000ms bucket — see check 9.
        _fixture_rung(a1, 300_000, sent=300_000, recv=270_000, ramp_slow=2_000_000)

        # Arm 2 — a broker whose ladder stops lower, each higher rung failing a
        # DIFFERENT gate, so no single gate can be carrying the whole result.
        a2 = _fixture_arm(root, 2, "mosquitto")
        _fixture_rung(a2, 100_000, sent=100_000, recv=100_000)
        _fixture_rung(a2, 200_000, sent=180_000, recv=180_000)  # offer not met (90%)
        _fixture_rung(a2, 300_000, sent=300_000, recv=300_000, p99_fast_le=5000.0)  # over budget
        _fixture_rung(a2, 400_000, sent=400_000, recv=400_000, settled="no")
        _fixture_rung(a2, 500_000, sent=500_000, recv=500_000, drained="no")

        # Arm 10 — a rung whose tail lands during the drain (check 14). It is
        # created HERE, with a two-digit index, so that check 1 below can tell an
        # index sort from a name sort: `10-latecomer` sorts before `2-mosquitto`
        # as a string, and an arm order taken from the name would run the control
        # arm out of place.
        a10 = _fixture_arm(root, 10, "latecomer")
        _fixture_rung(a10, 100_000, sent=100_000, recv=95_000, settled_total=100_000 * 300)

        # Arm 3 — the control that reproduces arm 1.
        a3 = _fixture_arm(root, 3, "mqttd", control=True)
        _fixture_rung(a3, 100_000, sent=100_000, recv=100_000)
        _fixture_rung(a3, 200_000, sent=200_000, recv=201_000)
        _fixture_rung(a3, 300_000, sent=300_000, recv=270_000)

        arms = load_arms(root, 1000.0)
        by_index = {a["index"]: a for a in arms}

        # 1. The sequence is read in RUN ORDER, from the directory index.
        check([a["index"] for a in arms] == [1, 2, 3, 10], f"arms out of sequence: {[a['index'] for a in arms]}")

        # 2. The knee is the highest rate that passed every gate — 200k here, not
        #    the 300k rung that under-delivered.
        k1 = knee(by_index[1]["rungs"])
        check(k1 is not None and k1["offered"] == 200_000, f"arm 1 knee: {k1 and k1['offered']}")

        # 3. A broker whose ladder stops lower must REPORT a lower knee; if this
        #    ever matches arm 1 the comparison has stopped discriminating.
        k2 = knee(by_index[2]["rungs"])
        check(k2 is not None and k2["offered"] == 100_000, f"arm 2 knee: {k2 and k2['offered']}")
        check(k1["offered"] > k2["offered"], "two brokers with different ladders produced the same knee")

        # 4. Under-delivery is a failure, and is named as one.
        r = next(r for r in by_index[1]["rungs"] if r["offered"] == 300_000)
        check(not r["pass"] and any("UNDER-DELIVERED" in f for f in r["flags"]),
              f"a rung that lost 10% of its traffic was accepted: {r['flags']}")

        # 5. An offer the drivers never reached is a failure of the RUNG, not a
        #    finding about the broker — and it must not be silently rounded up.
        r = next(r for r in by_index[2]["rungs"] if r["offered"] == 200_000)
        check(not r["pass"] and any("OFFER NOT MET" in f for f in r["flags"]),
              f"a rung at 90% of its offer was accepted: {r['flags']}")

        # 6. The p99 budget gates, and the budget is the CLI's, not a constant.
        r = next(r for r in by_index[2]["rungs"] if r["offered"] == 300_000)
        check(not r["pass"] and any("OVER P99 BUDGET" in f for f in r["flags"]),
              f"a rung at p99 <=5000ms passed a 1000ms budget: {r['flags']}")
        relaxed = rung_stats(by_index[2]["dir"] / "rung-300000", 10_000.0)
        check(relaxed["pass"], f"the same rung failed a 10s budget it fits inside: {relaxed['flags']}")

        # 7. settled=no and drained=no each fail on their own, whatever the
        #    throughput looked like.
        r = next(r for r in by_index[2]["rungs"] if r["offered"] == 400_000)
        check(not r["pass"] and any("UNSETTLED" in f for f in r["flags"]),
              f"a rung measured mid-ramp was accepted: {r['flags']}")
        r = next(r for r in by_index[2]["rungs"] if r["offered"] == 500_000)
        check(not r["pass"] and any("NOT DRAINED" in f for f in r["flags"]),
              f"an undrained rung was accepted: {r['flags']}")

        # 8. The FIRST failing rate is the lowest one that failed, so a reader
        #    sees where the ladder broke rather than where it ended.
        f2 = first_failure(by_index[2]["rungs"])
        check(f2 is not None and f2["offered"] == 200_000, f"arm 2 first failure: {f2 and f2['offered']}")

        # 9. The histogram baseline is SUBTRACTED. The 300k rung of arm 1 carries
        #    a ramp tail big enough that an unsubtracted read would put its p99
        #    in the 5000ms bucket; the window itself is 10ms.
        ramped = rung_stats(by_index[1]["dir"] / "rung-300000", 1000.0)
        check(ramped["p99"] == "<=10ms", f"the ramp's tail leaked into the published p99: {ramped['p99']}")

        # 10. Late publishers are REPORTED and NOT gated: this rung's windowed
        #     share is far past LATE_OK and it still passes, because on this rig
        #     the windowed figure has been wrong by an order of magnitude.
        a4 = _fixture_arm(root, 4, "emqx")
        _fixture_rung(a4, 100_000, sent=100_000, recv=100_000, late_rate=7_000, late_total=420_000)
        late = rung_stats(a4 / "rung-100000", 1000.0)
        check(late["late_window"] > LATE_OK, f"the late-share fixture is not actually late: {late['late_window']}")
        check(late["pass"], f"a late-publisher share gated a rung it must only annotate: {late['flags']}")
        check(late["late_lifetime"] < late["late_window"] / 2,
              f"windowed and lifetime late shares collapsed to one number: {late['late_window']} "
              f"{late['late_lifetime']}")

        # 11. A matching control passes and says so.
        ok, why = control_verdict(by_index[3], by_index[1])
        check(ok, f"a control that reproduces its arm was rejected: {why}")

        # 12. A control whose KNEE moved voids the sequence.
        a5 = _fixture_arm(root, 5, "mqttd-drifted", control=True)
        _fixture_rung(a5, 100_000, sent=100_000, recv=100_000)
        _fixture_rung(a5, 200_000, sent=200_000, recv=180_000)  # loses the rung arm 1 carried
        a5_arm = next(a for a in load_arms(root, 1000.0) if a["index"] == 5)
        ok, why = control_verdict(a5_arm, by_index[1])
        check(not ok and "control knee is" in why, f"a control that lost a whole rung still passed: {why}")

        # 13. A control at the SAME knee but delivering 12% less also voids it:
        #     the knee is a coarse number and can hide real degradation. The pair
        #     is fan-out 2, because that is the shape where delivered/s is free to
        #     move while every per-rung gate still passes — which is exactly why
        #     the control compares the RATE and does not just re-check the gates.
        a6 = _fixture_arm(root, 6, "hivemq")
        _fixture_rung(a6, 100_000, sent=100_000, recv=200_000)
        _fixture_rung(a6, 200_000, sent=200_000, recv=400_000)
        _fixture_rung(a6, 300_000, sent=300_000, recv=600_000, drained="no")
        a7 = _fixture_arm(root, 7, "hivemq", control=True)
        _fixture_rung(a7, 100_000, sent=100_000, recv=200_000)
        _fixture_rung(a7, 200_000, sent=200_000, recv=352_000)
        _fixture_rung(a7, 300_000, sent=300_000, recv=600_000, drained="no")
        reloaded = {a["index"]: a for a in load_arms(root, 1000.0)}
        check(knee(reloaded[7]["rungs"])["offered"] == knee(reloaded[6]["rungs"])["offered"] == 200_000,
              "the fan-out control pair does not share a knee, so it tests nothing")
        ok, why = control_verdict(reloaded[7], reloaded[6])
        check(not ok and "drift" in why, f"a control delivering 12% less at the same knee passed: {why}")

        # 14. The post-drain total is what decides delivery — arm 10's window log
        #     stops at 95% while its drain dump reaches 100%: the tail arrived
        #     late, which is not loss.
        drained_late = rung_stats(a10 / "rung-100000", 1000.0)
        check(drained_late["pass"], f"a tail that arrived during the drain was booked as loss: {drained_late['flags']}")

        # 15. The CPU and memory cells decide whether a reader believes the knee
        #     was the BROKER rather than the host. The mean hides a pinned second,
        #     so the lowest 1 s row is carried beside it; and the memory column is
        #     the container's usage, never the host limit printed next to it.
        check(abs(k1["cpu"]["mean"] - 90.0) < 0.01 and abs(k1["cpu"]["min_1s"] - 88.0) < 0.01,
              f"the knee rung's CPU idle is not mean/min of its stream: {k1['cpu']}")
        check(k1["mem"] == "812.4MiB", f"the memory cell is not the container's usage: {k1['mem']}")

        # 15b. A rung's stream also covers settle and drain. The CPU mean must
        #      describe the WINDOW, or a broker that drains slowly banks extra
        #      near-idle rows and reads as the one using less CPU — the bias runs
        #      in favour of the loser, so it cannot be dismissed as noise.
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td)
            (rdir / "cpu").mkdir()
            # Busy inside 10:00:04..10:00:06, near-idle either side.
            _write_cpu(rdir / "cpu" / "cpu-broker0.txt", [95.0, 95.0, 95.0, 20.0, 20.0, 20.0, 95.0, 95.0])
            unwindowed = cpu_idle(rdir, "broker")
            check(not unwindowed["windowed"] and unwindowed["mean"] > 60.0,
                  f"an unstamped rung claimed a windowed mean: {unwindowed}")
            check(format_idle(unwindowed).endswith("*"),
                  f"a whole-stream mean is not marked as one: {format_idle(unwindowed)}")
            (rdir / "window-open.utc").write_text("10:00:04\n")
            (rdir / "window-close.utc").write_text("10:00:06\n")
            windowed = cpu_idle(rdir, "broker")
            check(windowed["windowed"] and abs(windowed["mean"] - 20.0) < 0.01,
                  f"the window stamps did not select the window's rows: {windowed}")
            check(not format_idle(windowed).endswith("*"), "a windowed mean was marked as a whole-stream one")
            # A window whose stamps land outside the stream falls back, marked.
            (rdir / "window-open.utc").write_text("23:59:58\n")
            (rdir / "window-close.utc").write_text("00:00:02\n")
            midnight = cpu_idle(rdir, "broker")
            check(midnight["mean"] is not None and not midnight["windowed"],
                  f"a window with no samples inside it reported nothing: {midnight}")

        # 16. The report renders, and carries the verdicts the tables promise.
        text = render(root, 1000.0)
        for needle in ("## Knee per broker", "SEQUENCE VOID", "## What this is not", "ccx23", "driver0"):
            check(needle in text, f"the rendered report is missing {needle!r}")
        check("| 2 | mosquitto |" in text, "the knee table lost an arm")

    if failures:
        for f in failures:
            print(f"FAIL {f}", file=sys.stderr)
        sys.exit(1)
    print(
        "summarize-compare self-test: 17 checks OK (run order from the arm index; the knee is "
        "the highest PASSING rung; two brokers with different ladders get different knees; "
        "each of the four gates fails on its own — under-delivery, offer not met, p99 budget "
        "(and the same rung passing a wider budget), unsettled, undrained; the first failing "
        "rate is the lowest one; the histogram baseline is subtracted and not added back; late "
        "publishers are reported with both shares and gate nothing; a matching control passes; "
        "a control whose knee moved and one delivering 12% less at the same knee both void the "
        "sequence; a tail that arrived during the drain is not loss; the CPU idle and memory "
        "cells read the right columns; the CPU mean covers the window and not the settle and "
        "drain around it, falling back marked when it cannot; the report renders)"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("results", nargs="?", help="the run's results directory (.runs/<stamp>/results)")
    parser.add_argument("--p99-budget-ms", type=float, default=1000.0,
                        help="p99 a rung must fit inside to pass (default: 1000)")
    parser.add_argument("--self-test", action="store_true", help="run the built-in fixtures and exit")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return
    if not args.results:
        parser.error("a results directory is required")
    print(render(Path(args.results), args.p99_budget_ms))


if __name__ == "__main__":
    main()
