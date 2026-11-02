#!/usr/bin/env python3
"""Offline tests for the broker hosts' CPU and disk samples: scrape-loop.py --host's
reading of /proc (a fake tree in the kernel's formats) and bench/scale/rig2/host-load.py's
figures over a round's window, a restarted broker, rounds merged, and the markdown.
    python3 bench/scale/rig2/test-host-load.py
Rounds are synthesized as run.sh writes them: round-K/verdict.json with the measured
window, round-K/scrapes/broker-<i>/<unix_ms>.prom and <unix_ms>.host.json every 2 s.
"""
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


L = _load("host_load", HERE / "host-load.py")
S = _load("scrape_loop", HERE / "scrape-loop.py")

T0 = 1_800_000_000_000  # unix ms
FIELDS = ["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"]


def host(i, cpus=4, pid=4242, start=777, utime=150, stime=50, per_cpu=(60, 0, 40, 90, 0, 0, 10, 0)):
    """The i-th host sample of a steady host: every 2 s each CPU spends `per_cpu`
    jiffies (100 a second), the broker utime + stime jiffies, the disk 200 writes of
    4,096 sectors in 100 ms, 400 ms busy and 20 flushes, and the machine 10,000 switches."""
    row = [v * i for v in per_cpu]
    return {"unix_ms": T0 + i * 2000 + 5, "clk_tck": 100, "cpu_fields": FIELDS,
            "cpu": [v * cpus for v in row], "per_cpu": [row] * cpus, "ctxt": 10_000 * i, "procs_running": 3,
            "process": {"pid": pid, "utime": utime * i, "stime": stime * i, "threads": 33, "starttime": start},
            "disks": {"nvme0n1": {"reads": 0, "read_sectors": 0, "writes": 200 * i, "write_sectors": 4096 * i,
                                  "write_ms": 100 * i, "io_ms": 400 * i, "flushes": 20 * i, "flush_ms": 0}}}


def prom(received):
    return f'mqttd_publish_received_total{{qos="1"}} {received}\nprocess_start_time_seconds 1700000000\n# EOF\n'


def write_round(step, k, rate=50_000, samples=None, n=6):
    rd = step / f"round-{k}"
    d = rd / "scrapes" / "broker-0"
    d.mkdir(parents=True)
    rd.joinpath("verdict.json").write_text(json.dumps(
        {"verdict": "CARRIED", "window": {"start_unix_ms": T0, "end_unix_ms": T0 + 10_000}}))
    for i in range(n):
        (d / f"{T0 + i * 2000}.prom").write_text(prom(rate * 2 * i))
        s = samples(i) if samples else host(i)
        if s is not None:
            (d / f"{T0 + i * 2000}.host.json").write_text(json.dumps(s))
    return rd


def write_step(root, index=1, rate=50_000, verdict="CARRIED"):
    step = root / f"{index:02d}-{rate}"
    step.mkdir(parents=True)
    step.joinpath("step.json").write_text(json.dumps({"index": index, "id": step.name, "rate": rate,
                                                      "judge": {"margin_s": 1.0}}))
    step.joinpath("verdict.json").write_text(json.dumps({"verdict": verdict}))
    return step


class ScrapeLoopHostSample(unittest.TestCase):
    """scrape-loop.py's reading of a fake /proc in the kernel's own formats."""

    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        p, b = self.tmp / "proc", self.tmp / "sys_block"
        (p / "4242").mkdir(parents=True)
        (p / "77").mkdir()
        (b / "nvme0n1").mkdir(parents=True)
        p.joinpath("stat").write_text(
            "cpu  42848 208 43935 3213323 9052 0 14278 213 0 0\n"
            "cpu0 1410 0 1424 100736 182 0 0 2 0 0\n"
            "cpu1 1300 1 1500 100700 100 0 9 3 0 0\n"
            "intr 1 2 3\nctxt 96112919\nbtime 1793640000\nprocesses 3100\nprocs_running 11\nprocs_blocked 0\n")
        p.joinpath("diskstats").write_text(
            "   7       0 loop0 162 0 9198 53 0 0 0 0 0 58 53 0 0 0 0 0 0\n"
            " 259       0 nvme0n1 12887 2953 789380 8546 107798 7100 9188004 141024 0 88726 149570 0 0 0 0 31 7\n"
            " 259       1 nvme0n1p1 12111 2307 752014 8045 107768 7068 9187592 140999 0 89061 149044 0 0 0 0 0 0\n")
        # A comm with a space and parentheses: the fields are counted from the last ')'.
        p.joinpath("4242", "comm").write_text("mqttd\n")
        p.joinpath("4242", "stat").write_text(
            "4242 (mqttd) S 1 4242 4242 0 -1 4194560 166980 0 0 0 41787 48494 0 0 20 0 33 0 72707 2255069184\n")
        p.joinpath("77", "comm").write_text("my (odd) proc\n")
        p.joinpath("77", "stat").write_text(
            "77 (my (odd) proc) S 1 77 77 0 -1 0 0 0 0 0 5 6 0 0 20 0 1 0 900 1000\n")
        self.proc, self.block = str(p), str(b)

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_machine_process_and_whole_disks(self):
        pids = {}
        s = S.host_sample("mqttd", pids, proc=self.proc, sys_block=self.block)
        self.assertEqual(s["cpu"], [42848, 208, 43935, 3213323, 9052, 0, 14278, 213])
        self.assertEqual(len(s["per_cpu"]), 2)
        self.assertEqual(s["per_cpu"][1], [1300, 1, 1500, 100700, 100, 0, 9, 3])
        self.assertEqual((s["ctxt"], s["procs_running"], s["procs_blocked"]), (96112919, 11, 0))
        self.assertEqual(s["process"], {"pid": 4242, "utime": 41787, "stime": 48494, "threads": 33,
                                        "starttime": 72707})
        self.assertEqual(pids, {"mqttd": 4242})
        # The partition and the loop device are left out; the disk's flushes are read.
        self.assertEqual(list(s["disks"]), ["nvme0n1"])
        self.assertEqual(s["disks"]["nvme0n1"], {"reads": 12887, "read_sectors": 789380, "writes": 107798,
                                                 "write_sectors": 9188004, "write_ms": 141024, "io_ms": 88726,
                                                 "flushes": 31, "flush_ms": 7})
        self.assertEqual(s["cpu_fields"], FIELDS)

    def test_a_comm_with_spaces_and_parentheses(self):
        s = S.host_sample("my (odd) proc", {}, proc=self.proc, sys_block=self.block)
        self.assertEqual(s["process"], {"pid": 77, "utime": 5, "stime": 6, "threads": 1, "starttime": 900})

    def test_a_restarted_process_is_looked_up_again(self):
        pids = {"mqttd": 77}  # a pid reused by another process since the last tick
        s = S.host_sample("mqttd", pids, proc=self.proc, sys_block=self.block)
        self.assertEqual(s["process"]["pid"], 4242)
        self.assertEqual(pids, {"mqttd": 4242})

    def test_no_process_and_no_proc(self):
        self.assertIsNone(S.host_sample("absent", {}, proc=self.proc, sys_block=self.block)["process"])
        s = S.host_sample("mqttd", {}, proc=str(self.tmp / "none"), sys_block=str(self.tmp / "none"))
        self.assertEqual((s["cpu"], s["per_cpu"], s["process"], s["disks"]), ([], [], None, {}))


class HostLoad(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_one_broker_over_the_window(self):
        step = write_step(self.tmp)
        write_round(step, 1)
        r = L.round_host(step / "round-1")
        self.assertEqual(r["problems"], [])
        m = r["brokers"][0]
        # Inside [T0 + 1 s, T0 + 9 s]: the samples of T0 + 2 s .. T0 + 8 s, 6 s apart.
        self.assertAlmostEqual(m["window_s"], 6.0)
        self.assertEqual(m["cpus"], 4)
        self.assertAlmostEqual(m["host_busy"], 4 * (60 + 40 + 10) / 200)
        self.assertAlmostEqual(m["softirq"], 4 * 10 / 200)
        self.assertAlmostEqual(m["busiest_cpu"], 0.55)
        self.assertAlmostEqual(m["ctxt_s"], 5000)
        self.assertAlmostEqual(m["process"]["cores"], 1.0)
        self.assertAlmostEqual(m["process"]["system"], 0.25)
        self.assertAlmostEqual(m["received_rate"], 50_000)
        self.assertAlmostEqual(m["us_per_msg"], 20.0)
        d = m["disk"]
        self.assertEqual(d["name"], "nvme0n1")
        self.assertAlmostEqual(d["writes_s"], 100)
        self.assertAlmostEqual(d["write_mb_s"], 4096 * 512 / 2 / 1e6)
        self.assertAlmostEqual(d["ms_per_write"], 0.5)
        self.assertAlmostEqual(d["flushes_s"], 10)
        self.assertAlmostEqual(d["busy"], 0.2)

    def test_a_restarted_broker_has_no_cpu_figure(self):
        step = write_step(self.tmp)
        write_round(step, 1, samples=lambda i: host(i, pid=4242 if i < 3 else 5151, start=777 if i < 3 else 999))
        m = L.round_host(step / "round-1")["brokers"][0]
        self.assertIsNone(m["process"])
        self.assertIsNone(m["us_per_msg"])
        self.assertAlmostEqual(m["host_busy"], 2.2)

    def test_failed_and_missing_samples(self):
        before = write_step(self.tmp, 1)
        write_round(before, 1, samples=lambda i: None)  # a run from before --host
        failed = write_step(self.tmp, 2, rate=60_000)
        write_round(failed, 1, samples=lambda i: {"error": "OSError()"})
        sparse = write_step(self.tmp, 3, rate=70_000)
        write_round(sparse, 1, samples=lambda i: host(i) if i == 2 else None)
        self.assertIsNone(L.step_host(before)["busiest"])
        self.assertIsNone(L.step_host(failed)["busiest"])
        s = L.step_host(sparse)
        self.assertIsNone(s["busiest"])
        self.assertEqual(s["problems"], ["round-1: broker 0: fewer than two host samples inside the window"])

    def test_rounds_merged_by_received_messages(self):
        step = write_step(self.tmp)
        write_round(step, 1, rate=50_000)                                    # 1.0 core
        write_round(step, 2, rate=150_000, samples=lambda i: host(i, utime=450, stime=150))  # 3.0 cores
        s = L.step_host(step)
        b = s["busiest"]
        self.assertAlmostEqual(b["process"]["cores"], (1.0 * 1 + 3.0 * 3) / 4)
        self.assertAlmostEqual(b["received_rate"], (50_000 * 1 + 150_000 * 3) / 4)
        self.assertAlmostEqual(b["cluster_us_per_msg"], b["process"]["cores"] / b["received_rate"] * 1e6)

    def test_the_markdown(self):
        run = self.tmp / "run"
        ladder = run / "A" / "durable-qos1"
        (ladder / "steps").mkdir(parents=True)
        ladder.joinpath("ladder.json").write_text("{}")
        write_round(write_step(ladder / "steps", 1), 1)
        write_round(write_step(ladder / "steps", 2, rate=60_000, verdict="OFFER NOT TAKEN"), 1,
                    samples=lambda i: None)
        models = L.find_models(run)
        self.assertEqual([m["ladder"] for m in models], ["durable-qos1"])
        md = "\n".join(L.render_ladder_md(models[0]))
        self.assertIn("| 1 | 50,000 | CARRIED | 1.00 (0.25) | 20.0 | 2.20 of 4 | 0.20 | 0.00 | 0.55 | 5,000 | "
                      "100 | 1.05 | 10 | 0.20 |", md)
        self.assertIn("| 2 | 60,000 | OFFER NOT TAKEN | no host samples |" + " — |" * 10, md)
        header = next(line for line in md.splitlines() if line.startswith("| step"))
        row = next(line for line in md.splitlines() if line.startswith("| 1 "))
        self.assertEqual(header.count("|"), row.count("|"))


if __name__ == "__main__":
    unittest.main()
