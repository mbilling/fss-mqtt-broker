#!/usr/bin/env python3
"""Offline tests for bench/scale/rig2/hub-model.py (ADR 0092 T1): one broker's figures
over a round's window, a step's rounds merged, a ladder's highest carried and first
saturated steps, and the markdown the report cites.
    python3 bench/scale/rig2/test-hub-model.py
Rounds are synthesized as run.sh writes them: round-K/verdict.json with the measured
window, round-K/scrapes/broker-<i>/<unix_ms>.prom every 2 s.
"""
import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("hub_model", HERE / "hub-model.py")
H = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(H)

T0 = 1_800_000_000_000  # unix ms


def prom(received, dispatch, wake=(0.0, 0), queue=0.0):
    """A scrape: the received counter, per-class dispatch (seconds, count), the wake
    probe (seconds, count) and the hub queue gauge."""
    lines = [f'mqttd_publish_received_total{{qos="1"}} {received}', "process_start_time_seconds 1700000000"]
    for cls, (sec, n) in dispatch.items():
        lines.append(f'mqttd_hub_dispatch_seconds_sum{{command="{cls}"}} {sec}')
        lines.append(f'mqttd_hub_dispatch_seconds_count{{command="{cls}"}} {n}')
    lines.append(f'mqttd_runtime_wake_seconds_sum{{probe="yield"}} {wake[0]}')
    lines.append(f'mqttd_runtime_wake_seconds_count{{probe="yield"}} {wake[1]}')
    lines.append(f"mqttd_hub_queue_depth {queue}")
    return "\n".join(lines) + "\n# EOF\n"


def write_round(step: Path, k: int, brokers: dict, window=(T0, T0 + 10_000)):
    """round-k with one scrape per 2 s from T0 to T0 + 10 s for every broker in
    `brokers`: {idx: [(received, dispatch, wake, queue), ...]} (6 scrapes each)."""
    rd = step / f"round-{k}"
    rd.mkdir(parents=True)
    rd.joinpath("verdict.json").write_text(json.dumps(
        {"verdict": "CARRIED", "window": {"start_unix_ms": window[0], "end_unix_ms": window[1]}}))
    for idx, scrapes in brokers.items():
        d = rd / "scrapes" / f"broker-{idx}"
        d.mkdir(parents=True)
        for i, (rx, disp, wake, q) in enumerate(scrapes):
            (d / f"{T0 + i * 2000}.prom").write_text(prom(rx, disp, wake, q))
    return rd


def linear(rate_per_s, us_publish, us_ack, acks_per_msg=1.0, wake_ms=0.05, queue=10.0, n=6, extra=None):
    """Scrapes of a broker running steadily: every 2 s it receives rate*2 messages,
    dispatches one publish per message at us_publish and acks_per_msg acks at us_ack."""
    out = []
    for i in range(n):
        rx = rate_per_s * 2 * i
        acks = rx * acks_per_msg
        disp = {"publish": (rx * us_publish / 1e6, rx), "ack": (acks * us_ack / 1e6, acks)}
        if extra:
            disp.update({k: (v[0] * i, v[1] * i) for k, v in extra.items()})
        out.append((rx, disp, (i * 100 * wake_ms / 1e3, i * 100), queue))
    return out


class BrokerModel(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def test_one_broker_over_the_window(self):
        step = self.tmp / "01-50000"
        step.mkdir()
        step.joinpath("step.json").write_text(json.dumps({"index": 1, "rate": 50000, "judge": {"margin_s": 1.0}}))
        write_round(step, 1, {0: linear(50_000, 6.0, 2.0, queue=10.0)})
        m = H.round_model(step / "round-1")
        self.assertEqual(m["problems"], [])
        b = m["brokers"][0]
        # The window is [T0 + 1 s, T0 + 9 s]: scrapes at 2, 4, 6, 8 s are inside, 6 s apart.
        self.assertEqual(b["scrapes"], 4)
        self.assertAlmostEqual(b["window_s"], 6.0)
        self.assertAlmostEqual(b["received_rate"], 50_000.0)
        self.assertAlmostEqual(b["classes"]["publish"]["us"], 6.0)
        self.assertAlmostEqual(b["classes"]["ack"]["us"], 2.0)
        self.assertAlmostEqual(b["classes"]["publish"]["per_msg"], 1.0)
        self.assertAlmostEqual(b["classes"]["ack"]["per_msg"], 1.0)
        self.assertAlmostEqual(b["us_per_msg"], 8.0)
        self.assertAlmostEqual(b["busy"], 50_000 * 8e-6)        # 0.4 of wall-clock
        self.assertAlmostEqual(b["wake_us"], 50.0)
        self.assertEqual(b["queue_max"], 10.0)
        self.assertFalse(b["saturated"])

    def test_a_saturated_loop_is_named_and_a_short_window_is_a_problem(self):
        step = self.tmp / "02-90000"
        step.mkdir()
        step.joinpath("step.json").write_text(json.dumps({"index": 2, "rate": 90000, "judge": {"margin_s": 1.0}}))
        sat = linear(90_000, 7.0, 2.1, queue=150_000.0)
        write_round(step, 1, {0: sat, 1: linear(90_000, 7.0, 2.1)[:2]})   # broker 1: one scrape inside
        m = H.round_model(step / "round-1")
        self.assertTrue(m["brokers"][0]["saturated"])
        self.assertAlmostEqual(m["brokers"][0]["busy"], 90_000 * 9.1e-6)
        self.assertNotIn(1, m["brokers"])
        self.assertEqual(m["problems"], ["broker 1: fewer than two scrapes inside the window"])

    def test_a_window_without_a_verdict_comes_from_the_generators(self):
        step = self.tmp / "01-50000"
        step.mkdir()
        rd = write_round(step, 1, {0: linear(50_000, 6.0, 2.0)})
        (rd / "verdict.json").unlink()
        s = {"schedule_start_unix_ms": T0, "windows": [{"kind": "warmup", "start_s": 0, "end_s": 1},
                                                       {"kind": "measure", "start_s": 1, "end_s": 9}]}
        (rd / "gen-0.json").write_text(json.dumps(s))
        self.assertEqual(H.round_window(rd), (T0 + 1000, T0 + 9000))
        m = H.round_model(rd)
        self.assertEqual(m["window"]["seconds"], 8.0)
        self.assertIn(0, m["brokers"])


class StepAndLadder(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp())
        self.ladder = self.tmp / "durable-qos1"
        (self.ladder / "steps").mkdir(parents=True)
        (self.ladder / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "ramp": {}}))

    def tearDown(self):
        shutil.rmtree(self.tmp)

    def make_step(self, idx, rate, verdict, rounds):
        step = self.ladder / "steps" / f"{idx:02d}-{rate}"
        step.mkdir()
        step.joinpath("step.json").write_text(json.dumps({"index": idx, "id": step.name, "rate": rate,
                                                          "judge": {"margin_s": 1.0}}))
        step.joinpath("verdict.json").write_text(json.dumps({"verdict": verdict}))
        for k, brokers in enumerate(rounds, 1):
            write_round(step, k, brokers)
        return step

    def test_rounds_merge_weighted_by_received_and_the_cluster_sums(self):
        step = self.make_step(1, 100_000, "CARRIED", [
            {0: linear(50_000, 6.0, 2.0), 1: linear(50_000, 8.0, 2.0)},
            {0: linear(50_000, 6.0, 2.0), 1: linear(50_000, 8.0, 2.0)}])
        sm = H.step_model(step)
        self.assertEqual(sm["verdict"], "CARRIED")
        self.assertEqual(sm["brokers"][0]["rounds"], 2)
        self.assertAlmostEqual(sm["brokers"][1]["classes"]["publish"]["us"], 8.0)
        c = sm["cluster"]
        self.assertEqual(c["nodes"], 2)
        self.assertAlmostEqual(c["received_rate"], 100_000.0)
        self.assertAlmostEqual(c["us_per_msg"], 9.0)           # (8 + 10) / 2, equal weights
        self.assertEqual(c["busiest"], 1)
        self.assertAlmostEqual(c["busy_max"], 50_000 * 10e-6)
        self.assertEqual(c["saturated"], [])

    def test_the_ladder_names_the_highest_carried_and_first_saturated_steps_and_renders(self):
        self.make_step(1, 50_000, "CARRIED", [{0: linear(50_000, 6.0, 2.0)}])
        self.make_step(2, 75_000, "CARRIED", [{0: linear(75_000, 6.0, 2.0)}])
        self.make_step(3, 112_500, "FAILED", [{0: linear(100_000, 6.5, 2.0, queue=200_000.0)}])
        lm = H.ladder_model(self.ladder)
        self.assertEqual([s["step"] for s in lm["steps"]], [1, 2, 3])
        self.assertEqual(lm["highest_carried"], "02-75000")
        self.assertEqual(lm["first_saturated"], "03-112500")
        md = "\n".join(H.render_ladder_md(lm))
        self.assertIn("### Hub model (ADR 0092): durable-qos1", md)
        self.assertIn("| 2 | 75,000 | CARRIED | 0.60 (node 0) | 8.0 | 6.0 × 1.0 | 2.0 × 1.0 |", md)
        self.assertIn("first saturated step: 03-112500", md)
        self.assertIn("busy ceiling 0.85 on node 0 at 03-112500: 15% of the loop's time not dispatching", md)
        self.assertAlmostEqual(lm["ceiling"]["wake_us"], 50.0)
        self.assertEqual(H.find_models(self.ladder)[0]["ladder"], "durable-qos1")
        self.assertEqual(len(H.find_models(self.ladder / "steps" / "02-75000")[0]["steps"]), 1)

    def test_a_step_without_scrapes_is_a_row_with_its_problem(self):
        step = self.make_step(1, 50_000, "VOID", [])
        (step / "round-1").mkdir()
        (step / "round-1" / "verdict.json").write_text(json.dumps({"window": {"start_unix_ms": T0, "end_unix_ms": T0 + 10_000}}))
        lm = H.ladder_model(self.ladder)
        self.assertIsNone(lm["steps"][0]["cluster"])
        self.assertIn("no broker scrapes", "\n".join(H.render_ladder_md(lm)))


if __name__ == "__main__":
    unittest.main()
