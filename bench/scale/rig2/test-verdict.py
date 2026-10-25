#!/usr/bin/env python3
"""Offline tests for bench/scale/rig2/verdict.py: the step verdicts, the capacity lines,
the ramp's next step, the histogram merge, the idle check and the price quote.

    python3 bench/scale/rig2/test-verdict.py

Steps are synthesized as directories exactly as run.sh writes them (step.json, gen-<g>.json,
scrapes/broker-<i>/<unix_ms>.prom). The histogram merge is also checked against a REAL
mqtt-load summary (testdata/mqtt-load-summary.json, from a local run): the Python quantile
of the exported buckets must equal the percentile mqtt-load itself computed.
"""
import importlib.util
import json
import math
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
_spec = importlib.util.spec_from_file_location("verdict", HERE / "verdict.py")
V = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(V)

T0 = 1_800_000_000_000  # schedule start, unix ms


def hist(samples_us: dict[int, int]) -> dict:
    return {"unit": "us", "significant_figures": 3, "highest_trackable_us": 600_000_000,
            "buckets": [[v, c] for v, c in sorted(samples_us.items())]}


def summary(rate, *, p99_us=50_000, saturated=False, missing=0, outcome=None, start_shift_s=0.0,
            measure_s=60.0, warmup_s=20.0, errors=None):
    """A minimal mqtt-load.summary.v2 with one phase: `rate` msg/s offered; ack and e2e
    histograms whose p99 is `p99_us` (98 % at 1 ms, 1.5 % at p99, 0.5 % at 2 x p99)."""
    n = int(rate * measure_s)
    lo, mid, hi = round(n * 0.98), round(n * 0.015), n - round(n * 0.98) - round(n * 0.015)
    h = hist({1000: lo, p99_us: mid, 2 * p99_us: hi})
    windows, t = [], warmup_s
    windows.append({"index": 0, "kind": "warmup", "start_s": 0.0, "end_s": warmup_s, "sched_lag": {"p99_us": 1000},
                    "health": {"cpu_fraction": 0.2, "busiest_worker": 0.3, "credit_stall_fraction": 0.0, "backlog": 0,
                               "saturated": False, "reasons": []}})
    while t < warmup_s + measure_s - 1e-9:
        windows.append({"index": len(windows), "kind": "measure", "start_s": t, "end_s": t + 10,
                        "sched_lag": {"p99_us": 2000},
                        "health": {"cpu_fraction": 0.95 if saturated else 0.3, "busiest_worker": 0.4,
                                   "credit_stall_fraction": 0.0, "backlog": 0, "saturated": saturated,
                                   "reasons": ["cpu"] if saturated else []}})
        t += 10
    closed = missing == 0
    return {
        "schema": "mqtt-load.summary.v2",
        "schedule_start_unix_ms": T0 + int(start_shift_s * 1000),
        "workload": {"deliveries_per_message": 1},
        "windows": windows,
        "phases": [{"index": 0, "target_rate": rate, "offered_rate": rate, "sent_rate": rate, "received_rate": rate,
                    "ack_histogram": h, "e2e_histogram": h, "saturated": saturated}],
        "ledger": {"sent": n, "acked": n, "received": n - missing, "missing": missing, "unexpected": 0, "unacked": 0,
                   "unexpected_acks": 0, "closed": closed},
        "errors": errors or {"connect": 0, "disconnects": 0, "server_disconnects": 0, "protocol": 0, "write": 0},
        "generator": {"saturated": saturated, "sent_per_cpu_second": 40000.0},
        "outcome": outcome or ("void" if saturated else ("ok" if closed else "loss")),
    }


def prom(received, delivered, dropped=None, start=1700000000.0):
    lines = [f'mqttd_publish_received_total{{qos="1"}} {received}',
             f'mqttd_publish_delivered_total{{qos="1"}} {delivered}',
             f"process_start_time_seconds {start}",
             "mqttd_connections_active 0", "mqttd_sessions 0", "mqttd_pending_publishes 0"]
    for reason, v in (dropped or {}).items():
        lines.append(f'mqttd_publish_dropped_total{{reason="{reason}"}} {v}')
    return "\n".join(lines) + "\n# EOF\n"


def make_step(root: Path, rate, gens, *, brokers=3, received_share=1.0, drops=None, idle=True, gate="ack",
              restart=False, coverage_s=None):
    """A step directory: `gens` summaries, and brokers whose received counters grew at
    received_share x rate in total over the measured window (scrapes every 2 s)."""
    step = root / f"{len(list(root.iterdir())) + 1:02d}-{rate}"
    (step / "scrapes").mkdir(parents=True)
    (step / "step.json").write_text(json.dumps({
        "index": len(list(root.iterdir())), "id": step.name, "rate": rate, "attempt": 1, "generators": len(gens),
        "idle_before": {"idle": idle, "state": {}}, "judge": {"margin_s": 1.0, "latency_gate": gate}}))
    for g, s in enumerate(gens):
        (step / f"gen-{g}.json").write_text(json.dumps(s))
        (step / f"gen-{g}.exit").write_text(str({"ok": 0, "loss": 2, "void": 3, "error": 4}[s["outcome"]]))
    per_broker = rate * received_share / brokers
    first, last = T0 - 5000, T0 + 20_000 + 60_000 + 30_000
    if coverage_s is not None:
        last = T0 + 20_000 + int(coverage_s * 1000)
    for b in range(brokers):
        d = step / "scrapes" / f"broker-{b}"
        d.mkdir()
        stamps = list(range(first, last + 1, 2000))
        for t in stamps:
            secs = max(0.0, (t - first) / 1000)
            dropped = {k: (v if t == stamps[-1] else 0) for k, v in (drops or {}).items()} if b == 0 else None
            st = 1700000001.0 if (restart and b == 1 and t == stamps[-1]) else 1700000000.0
            (d / f"{t}.prom").write_text(prom(per_broker * secs, per_broker * secs, dropped, st))
    return step


def verdict_of(step: Path) -> dict:
    v = V.judge(step)
    (step / "verdict.json").write_text(json.dumps(v))
    return v


class Verdicts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ladder = Path(self.tmp.name)
        self.root = self.ladder / "steps"
        self.root.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def test_a_clean_step_is_carried_with_the_brokers_rate(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)]))
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertTrue(v["good"])
        self.assertAlmostEqual(v["received_rate"], 30000, delta=1)
        self.assertEqual(v["ack_latency"]["p99_us"], 50_000)

    def test_ledger_loss_fails(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=3)]))
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertIn("missing=3", v["failures"][0])
        self.assertFalse(v["good"])

    def test_a_broker_drop_fails_even_with_a_closed_ledger(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], drops={"append-backlog-full": 7}))
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertIn("append-backlog-full=7", v["failures"][0])

    def test_a_broker_restart_fails(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], restart=True))
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertIn("restarted", v["failures"][0])

    def test_a_saturated_generator_voids_the_step(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000), summary(30000, saturated=True)]))
        self.assertEqual(v["verdict"], V.VOID)
        self.assertTrue(any("generator 1 saturated (cpu)" in x for x in v["voids"]), v["voids"])

    def test_a_saturated_generator_voids_even_its_ledger_loss(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000, saturated=True, missing=5)]))
        self.assertEqual(v["verdict"], V.VOID)

    def test_98_percent_received_is_offer_not_taken(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], received_share=0.98))
        self.assertEqual(v["verdict"], V.NOT_TAKEN)
        self.assertAlmostEqual(v["received_share"], 0.98, places=3)
        # The boundary is summarize-curve.py's OFFER_TAKEN, not a copy of it.
        self.assertEqual(V.OFFER_TAKEN, 0.99)
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], received_share=0.995))
        self.assertEqual(v["verdict"], V.CARRIED)

    def test_lost_connections_are_disconnected_not_loss(self):
        errs = {"connect": 0, "disconnects": 0, "server_disconnects": 4, "protocol": 0, "write": 0}
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=40, outcome="error", errors=errs)]))
        self.assertEqual(v["verdict"], V.DISCONNECTED)

    def test_an_unidle_cluster_voids_the_step(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], idle=False))
        self.assertEqual(v["verdict"], V.VOID)

    def test_misaligned_generators_void_the_step(self):
        v = verdict_of(make_step(self.root, 30000, [summary(15000), summary(15000, start_shift_s=30)]))
        self.assertEqual(v["verdict"], V.VOID)
        self.assertTrue(any("overlap" in x for x in v["voids"]), v["voids"])

    def test_scrapes_that_miss_the_window_void_the_step(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], coverage_s=10))
        self.assertEqual(v["verdict"], V.VOID)
        self.assertTrue(any("broker evidence" in x for x in v["voids"]), v["voids"])

    def test_a_missing_summary_voids_the_step(self):
        step = make_step(self.root, 30000, [summary(30000)])
        (step / "gen-0.json").unlink()
        v = verdict_of(step)
        self.assertEqual(v["verdict"], V.VOID)

    def test_two_generators_sum_offer_and_merge_latency(self):
        v = verdict_of(make_step(self.root, 30000, [summary(15000, p99_us=20_000), summary(15000, p99_us=400_000)]))
        self.assertEqual(v["verdict"], V.CARRIED)
        self.assertEqual(v["offered_rate"], 30000)
        # Each puts 0.5 % of its samples at twice its p99, so the merged top 0.25 % is the
        # slower generator's alone: the merged p999 is its bucket, not an average.
        self.assertEqual(v["ack_latency"]["p999_us"], 800_000)
        self.assertEqual(v["ack_latency"]["p50_us"], 1000)
        self.assertEqual(v["ack_latency"]["count"], 2 * 15000 * 60)

    def test_qos0_gates_on_end_to_end_latency(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000, p99_us=2_000_000)], gate="e2e"))
        self.assertEqual(v["latency_gate"], "e2e")
        self.assertEqual(v["gate_p99_us"], 2_000_000)
        self.assertFalse(v["good"])

    def test_capacity_lines_by_p99_band(self):
        steps = []
        for rate, p99 in ((10000, 80_000), (15000, 200_000), (22000, 900_000), (33000, 1_500_000)):
            steps.append(verdict_of(make_step(self.root, rate, [summary(rate, p99_us=p99)])))
        steps.append(verdict_of(make_step(self.root, 50000, [summary(50000)], received_share=0.9)))
        cap = V.capacity(steps)
        self.assertEqual(cap["p99_le_1000ms"]["offered_rate"], 22000)
        self.assertEqual(cap["p99_le_250ms"]["offered_rate"], 15000)
        self.assertEqual(cap["p99_le_100ms"]["offered_rate"], 10000)
        (self.ladder / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "ramp": {"start_rate": 10000}}))
        lines = "\n".join(V.render_ladder_md(V.ladder_report(self.ladder)))
        self.assertIn("**Capacity: 22,000 msg/s** carried with ack p99 900.0 ms", lines)
        self.assertIn("ack p99 ≤ 250 ms: 15,000 msg/s (200.0 ms)", lines)
        self.assertIn("ack p99 ≤ 100 ms: 10,000 msg/s (80.0 ms)", lines)
        self.assertIn("**OFFER NOT TAKEN**", lines)
        self.assertNotIn("site", lines.lower())

    def test_no_carried_step_means_no_capacity(self):
        steps = [verdict_of(make_step(self.root, 10000, [summary(10000, missing=1)]))]
        self.assertIsNone(V.capacity(steps)["p99_le_1000ms"])


class Histograms(unittest.TestCase):
    def test_quantile_follows_hdrhistogram(self):
        b = [[1000, 99], [5000, 1]]
        self.assertEqual(V.quantile_us(b, 0.5), 1000)
        self.assertEqual(V.quantile_us(b, 0.99), 1000)  # ceil(0.99 * 100) = 99
        self.assertEqual(V.quantile_us(b, 0.999), 5000)
        self.assertIsNone(V.quantile_us([], 0.99))

    def test_merge_adds_equal_buckets(self):
        a, b = hist({1000: 50, 2000: 50}), hist({2000: 50, 3000: 50})
        m = V.merge_histograms([a, b, None])
        self.assertEqual(m, [[1000, 50], [2000, 100], [3000, 50]])
        self.assertEqual(V.quantile_us(m, 0.5), 2000)
        self.assertEqual(V.latency_summary(m)["count"], 200)

    def test_merging_a_histogram_with_itself_keeps_its_quantiles(self):
        a = hist({i * 10: i for i in range(1, 200)})
        m = V.merge_histograms([a, a])
        for q in (0.5, 0.99, 0.999):
            self.assertEqual(V.quantile_us(m, q), V.quantile_us(a["buckets"], q))

    def test_another_shape_is_refused(self):
        with self.assertRaises(ValueError):
            V.merge_histograms([{"unit": "ms", "significant_figures": 3, "buckets": []}])

    def test_python_quantiles_match_mqtt_load_on_a_real_summary(self):
        s = json.loads((HERE / "testdata" / "mqtt-load-summary.json").read_text())
        self.assertEqual(s["schema"], "mqtt-load.summary.v2")
        for agg in [*s["phases"], s["totals"]]:
            for kind in ("ack", "e2e"):
                ref = agg[f"{kind}_latency"]
                b = V.merge_histograms([agg[f"{kind}_histogram"]])
                self.assertGreater(ref["count"], 100)
                self.assertEqual(sum(c for _, c in b), ref["count"])
                for k in ("p50", "p99", "p999"):
                    self.assertEqual(V.quantile_us(b, V.QUANTILES[k]), ref[f"{k}_us"], (kind, k))
                self.assertEqual(b[-1][0], ref["max_us"])


def st(rate, verdict, good=None):
    return {"rate": float(rate), "verdict": verdict, "good": verdict == V.CARRIED if good is None else good}


class Ramp(unittest.TestCase):
    CFG = {"start_rate": 10000, "factor": 1.5, "gap_pct": 3, "granularity": 100, "void_retries": 1}

    def nxt(self, steps, **kw):
        return V.next_rate(steps, {**self.CFG, **kw})

    def test_climbs_by_the_factor(self):
        self.assertEqual(self.nxt([])[0], 10000)
        self.assertEqual(self.nxt([st(10000, V.CARRIED)])[0], 15000)
        self.assertEqual(self.nxt([st(10000, V.CARRIED), st(15000, V.CARRIED)])[0], 22500)

    def test_bisects_after_the_first_bad_step_until_the_gap_is_small(self):
        steps = [st(10000, V.CARRIED), st(15000, V.CARRIED), st(22500, V.NOT_TAKEN)]
        rate, why = self.nxt(steps)
        self.assertEqual(rate, 18800)  # (15000 + 22500) / 2 to the 100 msg/s granularity
        self.assertIn("bisect", why)
        steps.append(st(18800, V.CARRIED, good=False))  # carried, but ack p99 > 1 s
        self.assertEqual(self.nxt(steps)[0], 16900)
        steps.append(st(16900, V.CARRIED))
        self.assertEqual(self.nxt(steps)[0], 17900)  # 17850 rounds half up
        steps.append(st(17900, V.CARRIED))
        self.assertEqual(self.nxt(steps)[0], 18400)
        steps.append(st(18400, V.DISCONNECTED))
        rate, why = self.nxt(steps)
        self.assertIsNone(rate)
        self.assertIn("knee bracketed: good at 17900, not at 18400", why)
        self.assertIn("2.8% gap", why)

    def test_a_failed_step_stops_the_ladder(self):
        rate, why = self.nxt([st(10000, V.CARRIED), st(15000, V.FAILED)])
        self.assertIsNone(rate)
        self.assertIn("FAILED", why)
        # unless asked to bisect below it
        self.assertEqual(self.nxt([st(10000, V.CARRIED), st(15000, V.FAILED)], bisect_on_fail=True)[0], 12500)

    def test_a_void_step_is_rerun_then_stops(self):
        rate, why = self.nxt([st(10000, V.CARRIED), st(15000, V.VOID)])
        self.assertEqual(rate, 15000)
        self.assertIn("rerun", why)
        self.assertEqual(self.nxt([st(10000, V.CARRIED), st(15000, V.VOID), st(15000, V.CARRIED)])[0], 22500)
        rate, why = self.nxt([st(10000, V.CARRIED), st(15000, V.VOID), st(15000, V.VOID)])
        self.assertIsNone(rate)
        self.assertIn("VOID 2 times", why)

    def test_a_bad_first_step_steps_down(self):
        self.assertEqual(self.nxt([st(10000, V.NOT_TAKEN)])[0], 6700)
        rate, why = self.nxt([st(100, V.NOT_TAKEN)])
        self.assertIsNone(rate)

    def test_the_ceiling_stops_the_climb(self):
        self.assertEqual(self.nxt([st(10000, V.CARRIED)], max_rate=12000)[0], 12000)
        rate, why = self.nxt([st(10000, V.CARRIED), st(12000, V.CARRIED)], max_rate=12000)
        self.assertIsNone(rate)
        self.assertIn("ceiling", why)


class IdleAndCost(unittest.TestCase):
    def test_idle_needs_no_sessions_connections_or_pending_publishes(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "b.prom"
            p.write_text(prom(1, 1))
            st_ = V.idle_state([p])
            self.assertTrue(V.is_idle(st_, 0))
            p.write_text(prom(1, 1).replace("mqttd_sessions 0", "mqttd_sessions 12"))
            self.assertFalse(V.is_idle(V.idle_state([p]), 0))
            self.assertTrue(V.is_idle(V.idle_state([p]), 32))
            p.write_text(prom(1, 1).replace("mqttd_pending_publishes 0", "mqttd_pending_publishes 3"))
            self.assertFalse(V.is_idle(V.idle_state([p]), 32))
            # Still re-replicating: not ready for a step.
            p.write_text(prom(1, 1).replace("# EOF", "mqttd_replication_desired 3\nmqttd_replication_min_actual 1\n# EOF"))
            self.assertEqual(V.idle_state([p])["under_replicated"], ["b"])
            self.assertFalse(V.is_idle(V.idle_state([p]), 32))
            p.write_text(prom(1, 1).replace("# EOF", "mqttd_replication_desired 3\nmqttd_replication_min_actual 3\n# EOF"))
            self.assertTrue(V.is_idle(V.idle_state([p]), 0))

    def test_tier_prices(self):
        n3 = V.cost("n3")
        self.assertAlmostEqual(n3["eur_per_hour"], 3 * 0.17225 + 0.277375, places=6)
        n10 = V.cost("n10")
        self.assertAlmostEqual(n10["eur_per_hour"], 10 * 0.17225 + 3 * 0.277375, places=6)
        one = V.cost("n3", workloads=1, steps=9, step_secs=120)
        self.assertEqual(one["minutes"], round(10 + 18 + 3))
        self.assertEqual(one["eur_hours_rounded_up"], round(n3["eur_per_hour"], 2))
        with self.assertRaises(ValueError):
            V.cost("n3", broker_type="ccx99")
        self.assertTrue(math.isclose(V.cost("n3", broker_type="ccx99", prices={"ccx99": 1.0})["eur_per_hour"],
                                     3 + 0.277375))


if __name__ == "__main__":
    unittest.main()
