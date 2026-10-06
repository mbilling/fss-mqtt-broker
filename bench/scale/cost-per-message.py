#!/usr/bin/env python3
"""Broker CPU cost per delivered message, per arm of a knee campaign (#662).

A knee is a pass/fail threshold, and on this rig the same binary on the same hosts
moved its knee by a full rung between arms (2026-10-05), so effects under ~10%
cannot be read from it. CPU per message at a FIXED offered load below the knee is
a smooth quantity that averages over the whole window. Alternate two binaries
A/B/A/B on one provisioning (482-per-node-knee.sh, RIG_BINARY) and compare this.

    cost-per-message.py <campaign-dir> <sites> [--green-only]
    cost-per-message.py --self-test

For one rung of every arm, per broker: the mpstat `all` rows whose whole one-second
interval lies inside THAT broker's window (open scrape end .. close scrape start,
its own clock, the extractor's rule), as work done = %usr + %nice + %sys + %irq +
%soft (iowait and steal are not broker work; steal is printed so a noisy arm is
visible). That rate times the broker's window length, summed over brokers, divided
by the messages delivered between the window scrapes, is CPU-ms per 1k messages.

Scrapes and window stamps go through extract-lane-e.py's own loaders, so a
truncated or duplicated scrape, a counter reset or a broker restart inside the
window makes the arm INVALID rather than a number. A failed arm that was re-run
(`N-nM-r2`) is replaced by its re-run. Arm 1 ran the provisioned binary ("main");
a later arm runs what its arm-binary.txt says, or, without one, whatever the
previous arm left installed.
"""
import importlib.util
import pathlib
import re
import statistics
import sys
import tempfile
import unittest

HERE = pathlib.Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("extract_lane_e", HERE / "extract-lane-e.py")
E = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(E)

WORK = ("%usr", "%nice", "%sys", "%irq", "%soft")
ARM = re.compile(r"^(\d+)-n(\d+)(-r2)?$")


def windowed_cpu(path, lo_s, hi_s):
    """(work, steal) CPU-seconds per second over the rows inside [lo_s, hi_s]."""
    stamped, _ = E.mpstat_rows(path)
    text = path.read_text(errors="replace")
    ncpu = int(m.group(1)) if (m := re.search(r"\((\d+) CPU\)", text)) else 1
    cols, full = None, []
    for line in text.splitlines():
        f = line.split()
        if len(f) > 2 and f[1] == "CPU" and "%idle" in f:
            cols = f[2:]
        elif E.MPSTAT_ALL.match(line) and cols:
            full.append(dict(zip(cols, map(float, f[2:]))))
    if len(full) != len(stamped):
        raise ValueError(f"mpstat rows and columns disagree in {path}")
    inside = [row for (t, _), row in zip(stamped, full) if t is not None and t - 1 >= lo_s and t + 1 <= hi_s]
    if not inside:
        raise ValueError(f"no mpstat row inside the window: {path}")
    work = statistics.mean(sum(r.get(c, 0.0) for c in WORK) for r in inside) / 100 * ncpu
    steal = statistics.mean(r.get("%steal", 0.0) for r in inside) / 100 * ncpu
    return work, steal


def arm_cost(rung):
    """CPU-ms per 1k delivered messages for one rung directory, or ValueError."""
    nodes = E.nodes_of(rung)
    window = E.load_window(rung, nodes)
    opened = E.load_snap(rung, "window-open", nodes)
    closed = E.load_snap(rung, "window-close", nodes)
    E.validate_deltas(opened, closed, "window-open", "window-close")
    msgs = work = steal = 0.0
    for i in range(nodes):
        edges = window["hosts"][f"broker{i}"]
        lo, hi = edges["open"][1] / 1000, edges["close"][0] / 1000
        secs = E.window_seconds(edges)
        w, s = windowed_cpu(rung / "cpu" / f"cpu-broker{i}.txt", lo, hi)
        work += w * secs
        steal += s * secs
        msgs += E.delta(opened[i], closed[i], E.DELIVERED)
    if msgs <= 0:
        raise ValueError("no messages delivered in the window")
    return {"msgs": msgs, "ms_per_1k": work / msgs * 1e6, "steal_per_1k": steal / msgs * 1e6}


def arms(campaign):
    """(arm dir, binary) in campaign order; a re-run replaces the arm it re-ran."""
    found = {}
    for p in campaign.iterdir():
        m = ARM.match(p.name)
        if p.is_dir() and m:
            k = int(m.group(1))
            if m.group(3) or k not in found:
                found[k] = p
    out, binary = [], "main"
    for k in sorted(found):
        f = found[k] / "arm-binary.txt"
        if k == 1:
            binary = "main"
        elif f.exists():
            binary = f.read_text().split("\n", 1)[0].split("=", 1)[1]
        out.append((found[k], binary))
    return out


def verdict_of(lane, sites):
    v = lane / "ladder-verdicts.txt"
    if v.exists():
        for line in v.read_text().splitlines():
            if line.startswith(f"sites-{sites} "):
                return line.split(" ", 1)[1]
    return ""


def main(argv):
    if argv[1:] == ["--self-test"]:
        return unittest.main(argv=[argv[0]], exit=False).result.wasSuccessful() and 0 or 1
    if len(argv) < 3:
        sys.exit(__doc__)
    campaign, sites, green_only = pathlib.Path(argv[1]), argv[2], "--green-only" in argv[3:]
    groups = {}
    print(f"{'arm':10} {'binary':6} {'delivered':>11} {'CPU-ms/1k':>10} {'steal/1k':>9}  verdict")
    for arm, binary in arms(campaign):
        lane = next(arm.glob("results/nodes=*/laneE"), None)
        rung = lane / f"sites-{sites}" if lane else None
        if not rung or not rung.is_dir():
            continue
        verdict = verdict_of(lane, sites)
        try:
            c = arm_cost(rung)
        except ValueError as e:
            print(f"{arm.name:10} {binary:6} {'INVALID':>11}  {e}")
            continue
        counted = not green_only or verdict == "GREEN"
        if counted:
            groups.setdefault(binary, []).append(c["ms_per_1k"])
        mark = "" if counted else "  (excluded: --green-only)"
        print(f"{arm.name:10} {binary:6} {c['msgs']:11.0f} {c['ms_per_1k']:10.1f} {c['steal_per_1k']:9.1f}  {verdict[:40]}{mark}")
    print()
    for b, xs in groups.items():
        spread = f"[{min(xs):.1f}..{max(xs):.1f}]" if len(xs) > 1 else ""
        print(f"{b:6} n={len(xs)}  mean {statistics.mean(xs):7.1f} CPU-ms/1k msgs {spread}")
    if {"main", "alt"} <= groups.keys():
        a, b = statistics.mean(groups["main"]), statistics.mean(groups["alt"])
        print(f"alt vs main: {(b - a) / a * 100:+.1f}% CPU per message")
    return 0


class SelfTest(unittest.TestCase):
    T0 = 1_791_200_000  # a fixed epoch second

    def rung(self, root, delivered=60_000, close_s=60, reset=False):
        rung = root / "1-n1" / "results" / "nodes=1" / "laneE" / "sites-4"
        (rung / "cpu").mkdir(parents=True)
        (rung / "window.tsv").write_text(
            "host\tphase\tstart_ms\tend_ms\n"
            f"broker0\topen\t{self.T0 * 1000 - 100}\t{self.T0 * 1000}\n"
            f"broker0\tclose\t{(self.T0 + close_s) * 1000}\t{(self.T0 + close_s) * 1000 + 100}\n"
        )
        prom = lambda n: f'mqttd_publish_delivered_total{{qos="1"}} {n}\n# EOF\n'  # noqa: E731
        (rung / "metrics-window-open-broker0.prom").write_text(prom(1000))
        (rung / "metrics-window-close-broker0.prom").write_text(prom(500 if reset else 1000 + delivered))
        from datetime import datetime, timezone

        start = datetime.fromtimestamp(self.T0 - 30, timezone.utc)
        lines = [f"CPU_STREAM_START_UTC {start:%Y-%m-%dT%H:%M:%SZ}", "Linux 6.8 (b0)  10/05/26  _x86_64_  (2 CPU)", ""]
        lines.append("00:00:00     CPU    %usr   %nice    %sys %iowait    %irq   %soft  %steal  %guest  %gnice   %idle")
        for t in range(self.T0 - 29, self.T0 + close_s + 30):
            inside = self.T0 <= t - 1 and t + 1 <= self.T0 + close_s
            # Ramp and drain: fully busy, with iowait and steal. Window: 30% usr +
            # 10% sys + 10% soft = half of 2 CPUs, plus 20% iowait and 5% steal.
            usr, sys_, soft, iow, steal = (30, 10, 10, 20, 5) if inside else (90, 0, 0, 5, 5)
            idle = 100 - usr - sys_ - soft - iow - steal
            lines.append(f"{datetime.fromtimestamp(t, timezone.utc):%H:%M:%S}     all  {usr:6.2f}  0.00  {sys_:6.2f}  {iow:6.2f}  0.00  {soft:6.2f}  {steal:6.2f}  0.00  0.00  {idle:6.2f}")
        (rung / "cpu" / "cpu-broker0.txt").write_text("\n".join(lines) + "\n")
        return rung

    def test_only_rows_inside_the_window_count_and_iowait_is_not_work(self):
        with tempfile.TemporaryDirectory() as d:
            c = arm_cost(self.rung(pathlib.Path(d)))
            # 0.5 x 2 CPUs = 1.0 CPU-s/s over the 60.1 s between scrape midpoints, for
            # 60,000 messages: ~1,002 CPU-ms per 1k. Counting the 90%-busy ramp and
            # drain rows, as the first version did, would read far higher.
            self.assertAlmostEqual(c["ms_per_1k"], 1001.7, delta=1.0)
            self.assertAlmostEqual(c["steal_per_1k"], 100.2, delta=1.0)

    def test_a_counter_reset_is_invalid(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(ValueError):
                arm_cost(self.rung(pathlib.Path(d), reset=True))

    def test_a_rerun_replaces_its_arm_and_unswapped_arms_inherit_the_binary(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            for name in ("1-n3", "2-n3", "2-n3-r2", "3-n3", "10-n3"):
                (root / name).mkdir()
            (root / "2-n3-r2" / "arm-binary.txt").write_text("binary=alt\nurl=x\n")
            got = [(p.name, b) for p, b in arms(root)]
            self.assertEqual(got, [("1-n3", "main"), ("2-n3-r2", "alt"), ("3-n3", "alt"), ("10-n3", "alt")])


if __name__ == "__main__":
    sys.exit(main(sys.argv))
