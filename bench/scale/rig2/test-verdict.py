#!/usr/bin/env python3
"""Offline tests for bench/scale/rig2/verdict.py: the step verdicts, the drop-reason
classification, the capacity lines, the ramp's next step, a step's rounds (their plan,
the step verdict and the statistics across them), the histogram merge, the idle check,
the price quote, and the report's charts (chart-ramp.py's self-test).

    python3 bench/scale/rig2/test-verdict.py

Steps are synthesized as directories exactly as run.sh writes them (step.json, gen-<g>.json,
scrapes/broker-<i>/<unix_ms>.prom). The histogram merge is also checked against a REAL
mqtt-load summary (testdata/mqtt-load-summary.json, from a local run): the Python quantile
of the exported buckets must equal the percentile mqtt-load itself computed.
"""
import importlib.util
import json
import math
import re
import shutil
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
            measure_s=60.0, warmup_s=20.0, errors=None, drain="complete", unexpected=0,
            missing_acked=None, unacked=0, session_skew=1.0, spread=None, stalled_windows=0):
    """A minimal mqtt-load.summary.v2 with one phase: `rate` msg/s offered; ack and e2e
    histograms whose p99 is `p99_us` (98 % at 1 ms, 1.5 % at p99, 0.5 % at 2 x p99)."""
    n = int(rate * measure_s)
    lo, mid, hi = round(n * 0.98), round(n * 0.015), n - round(n * 0.98) - round(n * 0.015)
    h = hist({1000: lo, p99_us: mid, 2 * p99_us: hi})
    windows, t = [], warmup_s
    windows.append({"index": 0, "kind": "warmup", "start_s": 0.0, "end_s": warmup_s, "sched_lag": {"p99_us": 1000},
                    "health": {"cpu_fraction": 0.2, "busiest_worker": 0.3, "credit_stall_fraction": 0.0, "backlog": 0,
                               "saturated": False, "reasons": []}})
    measured = 0
    while t < warmup_s + measure_s - 1e-9:
        # `stalled_windows`: the first that many measured windows paused (scheduling lag).
        stall = not saturated and measured < stalled_windows
        windows.append({"index": len(windows), "kind": "measure", "start_s": t, "end_s": t + 10,
                        "sched_lag": {"p99_us": 41_000 if stall else 2000},
                        "health": {"cpu_fraction": 0.95 if saturated else 0.3, "busiest_worker": 0.4,
                                   "credit_stall_fraction": 0.0, "backlog": 0, "saturated": saturated or stall,
                                   "reasons": ["cpu"] if saturated else (["sched_lag"] if stall else [])}})
        t += 10
        measured += 1
    closed = missing == 0
    return {
        "schema": "mqtt-load.summary.v2",
        "schedule_start_unix_ms": T0 + int(start_shift_s * 1000),
        "workload": {"deliveries_per_message": 1, "session_skew": session_skew},
        "windows": windows,
        "phases": [{"index": 0, "target_rate": rate, "offered_rate": rate, "sent_rate": rate, "received_rate": rate,
                    "ack_histogram": h, "e2e_histogram": h, "saturated": saturated}],
        "ledger": {"sent": n, "acked": n, "received": n - missing, "missing": missing, "unexpected": unexpected,
                   "unacked": unacked, "unexpected_acks": 0,
                   "closed": closed and not unexpected and not unacked,
                   **({} if missing_acked is None else {"missing_acked": missing_acked})},
        "drain": {"duration_s": 0.05 if drain == "complete" else 10.0, "ended_by": drain, "quiet_s": 10.0,
                  "outstanding_deliveries": missing, "outstanding_acks": 0, "since_progress_s": 10.0},
        "errors": errors or {"connect": 0, "disconnects": 0, "server_disconnects": 0, "protocol": 0, "write": 0},
        "generator": {"saturated": saturated or stalled_windows > 0, "sent_per_cpu_second": 40000.0},
        "outcome": outcome or ("void" if saturated else ("ok" if closed else "loss")),
        **({} if spread is None else {"spread": spread}),
    }


def prom(received, delivered, dropped=None, start=1700000000.0, gauges=None):
    lines = [f'mqttd_publish_received_total{{qos="1"}} {received}',
             f'mqttd_publish_delivered_total{{qos="1"}} {delivered}',
             f"process_start_time_seconds {start}",
             "mqttd_connections_active 0", "mqttd_sessions 0", "mqttd_pending_publishes 0"]
    lines += [f"{k} {v}" for k, v in (gauges or {}).items()]
    for reason, v in (dropped or {}).items():
        lines.append(f'mqttd_publish_dropped_total{{reason="{reason}"}} {v}')
    return "\n".join(lines) + "\n# EOF\n"


def make_step(root: Path, rate, gens, *, brokers=3, received_share=1.0, drops=None, idle=True, gate="ack",
              restart=False, coverage_s=None, held=None, unfetched=(), short=()):
    """A step directory: `gens` summaries, and brokers whose received counters grew at
    received_share x rate in total over the measured window (scrapes every 2 s).
    `unfetched`: brokers whose scrapes never came back (no directory); `short`: brokers
    whose scrapes stop 10 s into the measured window."""
    step = root / f"{len(list(root.iterdir())) + 1:02d}-{rate}"
    (step / "scrapes").mkdir(parents=True)
    (step / "step.json").write_text(json.dumps({
        "index": len(list(root.iterdir())), "id": step.name, "rate": rate, "attempt": 1, "generators": len(gens),
        "brokers": brokers,
        "idle_before": {"idle": idle, "state": {}}, "judge": {"margin_s": 1.0, "latency_gate": gate}}))
    for g, s in enumerate(gens):
        (step / f"gen-{g}.json").write_text(json.dumps(s))
        (step / f"gen-{g}.exit").write_text(str({"ok": 0, "loss": 2, "void": 3, "error": 4}[s["outcome"]]))
    per_broker = rate * received_share / brokers
    first, last = T0 - 5000, T0 + 20_000 + 60_000 + 30_000
    if coverage_s is not None:
        last = T0 + 20_000 + int(coverage_s * 1000)
    for b in range(brokers):
        if b in unfetched:
            continue
        d = step / "scrapes" / f"broker-{b}"
        d.mkdir()
        stamps = list(range(first, (T0 + 30_000 if b in short else last) + 1, 2000))
        for t in stamps:
            secs = max(0.0, (t - first) / 1000)
            dropped = {k: (v if t == stamps[-1] else 0) for k, v in (drops or {}).items()} if b == 0 else None
            st = 1700000001.0 if (restart and b == 1 and t == stamps[-1]) else 1700000000.0
            # `held`: what broker 0's gauges hold over the generators' drain (its last 10 s,
            # to the drain end at 80 s). After it they read 0, as on a real run: mqtt-load
            # disconnects its clients at the drain end, durable consumers with session
            # expiry 0, and the broker discards their sessions' unacked deliveries.
            g = held if (held and b == 0 and T0 + 70_000 <= t <= T0 + 80_000) else None
            (d / f"{t}.prom").write_text(prom(per_broker * secs, per_broker * secs, dropped, st, g))
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

    def test_a_generator_whose_p99_diverges_from_the_others_voids_the_step(self):
        # 0.8, 1.6 and 291 ms on one step: one host's receive path, not the brokers.
        gens = [summary(30000, p99_us=800), summary(30000, p99_us=1600), summary(30000, p99_us=291_000)]
        v = verdict_of(make_step(self.root, 30000, gens))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertEqual(v["harness_generators"], [2])
        self.assertTrue(any(x.startswith("generator 2's ack p99 291.0 ms is far above the other generators' (median")
                            for x in v["voids"]), v["voids"])

    def test_a_few_ms_between_healthy_hosts_is_not_divergence(self):
        # 225k QoS 0 on 2026-10-27: 2.1, 2.3, 12.8 and 20.9 ms. No band is within reach.
        gens = [summary(7500, p99_us=p) for p in (2_100, 2_300, 12_800, 20_900)]
        v = verdict_of(make_step(self.root, 30000, gens))
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual(v["harness_generators"], [])

    def test_generators_that_agree_do_not_diverge_even_at_a_high_p99(self):
        # The cluster at its knee slows every generator alike.
        gens = [summary(10000, p99_us=400_000), summary(10000, p99_us=900_000), summary(10000, p99_us=600_000)]
        v = verdict_of(make_step(self.root, 30000, gens))
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual(v["harness_generators"], [])
        self.assertEqual(V.diverging({0: 1000, 1: None}), [])

    def test_slow_consumer_drops_under_a_saturated_generator_are_void_not_failed(self):
        # The 10-node QoS 0 step at 150k: generator 2 stalled, and the brokers dropped 40
        # messages for its consumers (outbound-full).
        v = verdict_of(make_step(self.root, 30000, [summary(30000), summary(30000, saturated=True)],
                                 drops={"outbound-full": 40}))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertEqual(v["failures"], [])
        self.assertTrue(any(x.startswith("would have been FAILED (brokers dropped 40 messages (outbound-full=40))")
                            for x in v["notes"]), v["notes"])

    def test_publisher_side_drops_still_fail_under_a_saturated_generator(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000), summary(30000, saturated=True)],
                                 drops={"append-backlog-full": 20}))
        self.assertEqual(v["verdict"], V.FAILED, v)

    def test_slow_consumer_drops_fail_when_every_generator_kept_up(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], drops={"outbound-full": 40}))
        self.assertEqual(v["verdict"], V.FAILED, v)

    def test_calibration_names_the_slow_generator_host(self):
        d = self.ladder / "calibration"
        d.mkdir()
        for g, p99 in enumerate([800, 1600, 291_000, 900]):
            (d / f"gen-{g}.json").write_text(json.dumps(summary(30000, p99_us=p99)))
        (d / "gen-3.json").write_text(json.dumps(summary(30000, p99_us=900, saturated=True)))
        c = V.calibrate(d)
        self.assertEqual(c["bad"], [2, 3], c)
        self.assertIn("against the others' median", c["why"][2])
        self.assertIn("saturated (cpu)", c["why"][3])
        for g in (2, 3):
            (d / f"gen-{g}.json").write_text(json.dumps(summary(30000, p99_us=1000)))
        self.assertEqual(V.calibrate(d)["bad"], [])

    def test_one_paused_window_is_a_note_not_a_void(self):
        # 225k QoS 0 on 2026-10-27: one window at 37-45 ms scheduling lag, five at 2.1 ms.
        v = verdict_of(make_step(self.root, 30000, [summary(15000), summary(15000, stalled_windows=1)]))
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual(v["harness_generators"], [])
        self.assertTrue(any(n.startswith("generator 1 stalled in 1 of its measured windows (sched_lag; "
                                         "scheduling lag p99 41.0 ms there)") for n in v["notes"]), v["notes"])

    def test_two_paused_windows_void_the_round(self):
        v = verdict_of(make_step(self.root, 30000, [summary(15000), summary(15000, stalled_windows=2)]))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertEqual(v["harness_generators"], [1])

    def test_calibration_rejects_a_host_for_a_single_paused_window(self):
        d = self.ladder / "calibration"
        d.mkdir()
        (d / "gen-0.json").write_text(json.dumps(summary(30000, p99_us=1000)))
        (d / "gen-1.json").write_text(json.dumps(summary(30000, p99_us=1000, stalled_windows=1)))
        self.assertEqual(V.calibrate(d)["bad"], [1])

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

    def test_an_unidle_cluster_voids_a_step_that_lost_messages_too(self):
        # The 10-node run's 05-152000 started unsettled and dropped 467 messages: it was
        # judged FAILED although the settle had warned "the step will be VOID".
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], idle=False, drops={"append-backlog-full": 7}))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertTrue(any("append-backlog-full" in x for x in v["failures"]), v)
        self.assertIn("would have been FAILED on a settled cluster; VOID because it started unsettled", v["notes"])

    def test_misaligned_generators_void_the_step(self):
        v = verdict_of(make_step(self.root, 30000, [summary(15000), summary(15000, start_shift_s=30)]))
        self.assertEqual(v["verdict"], V.VOID)
        self.assertTrue(any("overlap" in x for x in v["voids"]), v["voids"])

    def test_a_generator_that_missed_the_shared_start_is_noted(self):
        late = summary(15000)
        late["start"] = {"by": "timeout", "requested_unix_ms": None, "waited_s": 45.0, "late_ms": 0}
        on_time = summary(15000)
        on_time["start"] = {"by": "shared", "requested_unix_ms": 1, "waited_s": 2.0, "late_ms": 0}
        v = verdict_of(make_step(self.root, 30000, [on_time, late]))
        self.assertIn("generator 1 missed the shared start: none came in 45.0 s, so it started on its own", v["notes"])
        self.assertFalse(any("generator 0" in x for x in v["notes"]), v["notes"])

    def test_scrapes_that_miss_the_window_void_the_step(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], coverage_s=10))
        self.assertEqual(v["verdict"], V.VOID)
        self.assertTrue(any("broker evidence" in x for x in v["voids"]), v["voids"])

    def test_brokers_whose_scrapes_were_not_fetched_void_the_step(self):
        # The aborted 10-node run's 01-30000: brokers 5-9's scrapes never came back, and
        # the sum over the other five read as OFFER NOT TAKEN at half the offer.
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], brokers=10, unfetched=(5, 6, 7, 8, 9)))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertIn("broker evidence: no scrapes from broker(s) 5 6 7 8 9", v["voids"])
        self.assertIsNone(v["received_rate"], "a sum over half the brokers is not the cluster's rate")
        self.assertFalse(v["brokers"]["complete"])
        self.assertEqual(V.next_rate([v], {"start_rate": 30000})[0], 30000, "a VOID step is rerun")

    def test_the_broker_count_comes_from_run_json_when_step_json_lacks_it(self):
        run = Path(self.tmp.name) / "run"
        steps = run / "A" / "durable-qos1" / "steps"
        steps.mkdir(parents=True)
        (run / "run.json").write_text(json.dumps({"brokers": 3}))
        step = make_step(steps, 30000, [summary(30000)], unfetched=(2,))
        meta = json.loads((step / "step.json").read_text())
        del meta["brokers"]
        (step / "step.json").write_text(json.dumps(meta))
        v = verdict_of(step)
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertIn("broker evidence: no scrapes from broker(s) 2", v["voids"])

    def test_one_broker_short_of_the_window_voids_not_offer_not_taken(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], short=(1,)))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertTrue(any(x.startswith("broker evidence: broker 1: scrapes cover") for x in v["voids"]), v["voids"])
        self.assertIsNone(v["received_rate"])

    def test_missing_brokers_void_a_step_without_summaries_too(self):
        step = make_step(self.root, 30000, [summary(30000)], unfetched=(0,))
        (step / "gen-0.json").unlink()
        v = verdict_of(step)
        self.assertEqual(v["verdict"], V.VOID)
        self.assertIn("broker evidence: no scrapes from broker(s) 0", v["voids"])

    def test_loss_on_the_brokers_that_were_fetched_still_fails(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], drops={"queue-overflow": 3}, unfetched=(2,)))
        self.assertEqual(v["verdict"], V.FAILED, v)

    def test_a_missing_summary_voids_the_step(self):
        step = make_step(self.root, 30000, [summary(30000)])
        (step / "gen-0.json").unlink()
        v = verdict_of(step)
        self.assertEqual(v["verdict"], V.VOID)

    def test_two_generators_sum_offer_and_merge_latency(self):
        # 100 and 400 ms: apart, but within DIVERGE_* of each other.
        v = verdict_of(make_step(self.root, 30000, [summary(15000, p99_us=100_000), summary(15000, p99_us=400_000)]))
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
        # Lossless and taken: carried, and good for the ramp, at any latency.
        self.assertEqual(v["verdict"], V.CARRIED)
        self.assertTrue(v["good"])

    def test_a_workload_the_harness_concentrated_voids_the_step(self):
        # All of a topic's publishers on one broker (the 10-node deal before the fix):
        # one session per group carried the topic, so the knee was the harness's.
        v = verdict_of(make_step(self.root, 30000, [summary(30000), summary(30000, session_skew=9.6)]))
        self.assertEqual(v["verdict"], V.VOID, v)
        self.assertTrue(any("generator 1's workload was not spread evenly" in x for x in v["voids"]), v["voids"])

    def test_an_uneven_broker_spread_is_noted_not_judged(self):
        spread = {"consumer_skew": 3.4, "busiest_consumer": 66, "busiest_broker": 6, "idle_consumers": 9}
        v = verdict_of(make_step(self.root, 30000, [summary(30000, spread=spread)]))
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertTrue(any("busiest consumer (c66, on broker 7) received 3.40x" in x and "9 consumer(s) none" in x
                            for x in v["notes"]), v["notes"])
        even = {"consumer_skew": 1.1, "busiest_consumer": 3, "busiest_broker": 0, "idle_consumers": 0}
        v = verdict_of(make_step(self.root, 30000, [summary(30000, spread=even)]))
        self.assertFalse(any("uneven delivery" in x for x in v["notes"]), v["notes"])

    def test_a_drop_that_is_not_loss_is_a_note_not_a_failure(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)],
                                 drops={"pending-cap-replay": 40, "settle-replay": 2}))
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual(v["brokers"]["lost"], 0)
        self.assertEqual(v["brokers"]["dropped"], 42)
        self.assertIn("broker drops that are not loss: pending-cap-replay=40, settle-replay=2", v["notes"])

    def test_loss_and_non_loss_drops_together_fail_on_the_loss_only(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)],
                                 drops={"pending-cap-replay": 40, "outbound-full": 3}))
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertEqual(v["failures"], ["brokers dropped 3 messages (outbound-full=3)"])

    def test_an_unknown_drop_reason_is_loss(self):
        v = verdict_of(make_step(self.root, 30000, [summary(30000)], drops={"a-reason-from-the-future": 1}))
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertIn("a-reason-from-the-future=1", v["failures"][0])

    def test_a_drain_cut_off_with_the_missing_still_queued_is_not_drained_not_loss(self):
        # No broker drop, the drain went quiet, and broker 0 still holds more unacked
        # deliveries than the ledger misses: the messages were queued, not lost.
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=500, drain="quiet")],
                                 held={"mqttd_inflight_messages": 700}))
        self.assertEqual(v["verdict"], V.NOT_DRAINED, v)
        self.assertFalse(v["good"])
        self.assertEqual(v["failures"], [])
        self.assertTrue(v["drain"]["still_queued"])
        self.assertEqual(v["drain"]["queued_at_cutoff"], 500)
        self.assertIn("missing 500 = broker loss drops 0 + 500 still queued on the brokers at the cut-off", v["notes"][0])
        # For the ramp it is above the knee, like OFFER NOT TAKEN.
        self.assertIn(V.NOT_DRAINED, V.BAD)

    def test_what_the_brokers_held_is_read_before_the_generator_ended_its_sessions(self):
        # h3 durable-qos1 45,000 round 2 (2026-10-29): the broker held 297 unacked
        # deliveries until mqtt-load ended its durable sessions at the drain end, and 0
        # at its first scrape after. Read there, the 297 were "unexplained" loss.
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=297, missing_acked=297,
                                                            drain="quiet")],
                                 brokers=1, held={"mqttd_inflight_messages": 297}))
        self.assertEqual(v["drain"]["held_at_cutoff"]["mqttd_inflight_messages"], 297)
        self.assertEqual(v["drain"]["queued_at_cutoff"], 297)
        self.assertEqual(v["drain"]["unexplained"], 0)
        self.assertEqual(v["drain"]["unexplained_acked"], 0)
        self.assertEqual(v["verdict"], V.NOT_DRAINED, v)

    def test_a_drain_cut_off_with_drops_fails_and_reconciles_missing(self):
        # Run 2's clean-qos1 152,000: missing = loss drops + what the brokers still held.
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=1000, drain="quiet")],
                                 drops={"backlog-overflow": 700}, held={"mqttd_inflight_messages": 900,
                                                                         "mqttd_backlog_bytes": 2_000_000}))
        self.assertEqual(v["verdict"], V.FAILED, v)
        self.assertEqual(v["drain"]["loss_drops"], 700)
        self.assertEqual(v["drain"]["queued_at_cutoff"], 300)
        self.assertEqual(v["drain"]["unexplained"], 0)
        self.assertTrue(any("missing 1,000 = broker loss drops 700 + 300 still queued on the brokers at the cut-off; "
                            "the brokers then held 900 unacked deliveries, 0 pending publishes and 2.0 MB" in n
                            for n in v["notes"]), v["notes"])

    def test_missing_is_split_into_acknowledged_loss_and_never_acknowledged(self):
        # The repeatability run's durable 114,200 step: the brokers closed publishers under
        # overload (append-backlog-full); what they never acknowledged is not loss.
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=1228, missing_acked=0, unacked=3298,
                                                            drain="quiet")],
                                 drops={"append-backlog-full": 723}))
        self.assertEqual(v["verdict"], V.FAILED, v)
        self.assertEqual(v["drain"]["missing_acked"], 0)
        self.assertEqual(v["drain"]["missing_unacked"], 1228)
        self.assertEqual(v["drain"]["unexplained_acked"], 0)
        self.assertTrue(any("(acknowledged 0 + never acknowledged 1,228)" in n for n in v["notes"]), v["notes"])
        # Acknowledged loss beyond what the brokers counted is reported as such.
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=1228, missing_acked=900, unacked=10,
                                                            drain="quiet")],
                                 drops={"append-backlog-full": 723}))
        self.assertEqual(v["drain"]["unexplained_acked"], 177)

    def test_a_drain_cut_off_with_nothing_held_is_loss(self):
        # Nothing on the brokers' gauges accounts for the missing: the ledger's loss stands.
        v = verdict_of(make_step(self.root, 30000, [summary(30000, missing=40, drain="quiet")],
                                 held={"mqttd_inflight_messages": 10}))
        self.assertEqual(v["verdict"], V.FAILED, v)
        self.assertEqual(v["drain"]["queued_at_cutoff"], 10)
        self.assertEqual(v["drain"]["unexplained"], 30)
        self.assertIn("missing=40", v["failures"][0])
        # A complete drain has nothing to reconcile.
        v = verdict_of(make_step(self.root, 30000, [summary(30000)]))
        self.assertFalse(v["drain"]["cut_off"])
        self.assertIsNone(v["drain"]["reconciled"])

    def test_capacity_lines_by_p99_band(self):
        steps = []
        for rate, p99 in ((10000, 80_000), (15000, 200_000), (22000, 900_000), (33000, 2_000_000)):
            steps.append(verdict_of(make_step(self.root, rate, [summary(rate, p99_us=p99)])))
        steps.append(verdict_of(make_step(self.root, 50000, [summary(50000)], received_share=0.9)))
        cap = V.capacity(steps)
        # The lossless maximum ignores latency: the 2 s step was carried.
        self.assertEqual(cap["lossless"]["offered_rate"], 33000)
        self.assertEqual(cap["lossless"]["ack_p99_us"], 2_000_000)
        self.assertEqual(cap["lossless"]["e2e_p99_us"], 2_000_000)
        self.assertEqual(cap["p99_le_1000ms"]["offered_rate"], 22000)
        self.assertEqual(cap["p99_le_250ms"]["offered_rate"], 15000)
        self.assertEqual(cap["p99_le_100ms"]["offered_rate"], 10000)
        (self.ladder / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "ramp": {"start_rate": 10000}}))
        lines = "\n".join(V.render_ladder_md(V.ladder_report(self.ladder)))
        self.assertIn("**Maximum lossless: 33,000 msg/s** (ack p99 2000.0 ms, e2e p99 2000.0 ms)", lines)
        self.assertIn("ack p99 ≤ 1 s: 22,000 msg/s (900.0 ms)", lines)
        self.assertIn("ack p99 ≤ 250 ms: 15,000 msg/s (200.0 ms)", lines)
        self.assertIn("ack p99 ≤ 100 ms: 10,000 msg/s (80.0 ms)", lines)
        self.assertIn("**OFFER NOT TAKEN**", lines)
        self.assertNotIn("site", lines.lower())

    def test_no_carried_step_means_no_capacity(self):
        steps = [verdict_of(make_step(self.root, 10000, [summary(10000, missing=1)]))]
        self.assertIsNone(V.capacity(steps)["p99_le_1000ms"])
        self.assertIsNone(V.capacity(steps)["lossless"])

    def test_the_report_draws_and_links_a_chart_per_ladder(self):
        run = self.ladder
        lad = run / "A" / "durable-qos1"
        (lad / "steps").mkdir(parents=True)
        for rate, p99 in ((10000, 5_000), (15000, 300_000)):
            verdict_of(make_step(lad / "steps", rate, [summary(rate, p99_us=p99)]))
        (lad / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "arm": "A",
                                                     "ramp": {"start_rate": 10000}, "mqtt_load": {"payload": 200}}))
        (run / "run.json").write_text(json.dumps({"tier": "n3", "brokers": 3, "workloads": ["durable-qos1"],
                                                  "started_utc": "20261026T063123Z"}))
        data = V.report(run)
        self.assertEqual(data["ladders"][0]["chart"], "report-durable-qos1.svg")
        self.assertIn("15,000 msg/s lossless", (run / "report-durable-qos1.svg").read_text())
        self.assertIn("](report-durable-qos1.svg)", (run / "report.md").read_text())
        self.assertEqual(json.loads((run / "report.json").read_text())["ladders"][0]["capacity"]["lossless"]["offered_rate"], 15000)


class RetryReport(unittest.TestCase):
    def test_both_attempts_are_in_the_report_and_the_first_is_transient(self):
        with tempfile.TemporaryDirectory() as d:
            run = Path(d)
            lad = run / "A" / "durable-qos1"
            (lad / "steps").mkdir(parents=True)
            for rate, missing in ((22500, 0), (33800, 9), (33800, 0)):
                verdict_of(make_step(lad / "steps", rate, [summary(rate, p99_us=1_500, missing=missing)]))
            (lad / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "arm": "A",
                                                         "ramp": {"start_rate": 22500}}))
            (run / "run.json").write_text(json.dumps({"tier": "n3", "workloads": ["durable-qos1"]}))
            data = V.report(run)
            md = (run / "report.md").read_text()
            self.assertIn("**FAILED** (retried: transient, step 3 carried)", md)
            self.assertIn("**CARRIED** (retry of step 2)", md)
            lad_r = data["ladders"][0]
            self.assertEqual(lad_r["capacity"]["lossless"]["offered_rate"], 33800)
            self.assertIn("incomplete (next would be 50700", lad_r["ended"])
            self.assertIn("retried: transient", (run / "report-durable-qos1.svg").read_text())


def new_step(steps_root: Path, rate, rounds=3, after="CARRIED") -> Path:
    """A step directory measured in rounds, as run.sh's run_step writes it (no round yet)."""
    idx = len([p for p in steps_root.iterdir() if p.is_dir()]) + 1
    step = steps_root / f"{idx:02d}-{rate}"
    step.mkdir()
    (step / "step.json").write_text(json.dumps({"index": idx, "id": step.name, "rate": rate, "attempt": 1,
                                                "rounds": rounds, "generators": 1, "brokers": 3, "after": after}))
    return step


def add_round(step: Path, name: str, rate, gens, **kw) -> dict:
    """One judged round, `name` (round-K or round-K-aN), as run.sh's run_round leaves it."""
    with tempfile.TemporaryDirectory() as d:
        shutil.move(str(make_step(Path(d), rate, gens, **kw)), str(step / name))
    return verdict_of(step / name)


RAMP = {"start_rate": 10000, "factor": 1.5, "gap_pct": 3, "granularity": 100, "void_retries": 1,
        "retry_p99_ms": 50, "retry_max": 2}


class Rounds(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ladder = Path(self.tmp.name)
        self.root = self.ladder / "steps"
        self.root.mkdir()
        (self.ladder / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "arm": "A", "ramp": RAMP}))

    def tearDown(self):
        self.tmp.cleanup()

    def plan(self, step, prior=()):
        return V.plan_rounds(step, list(prior), RAMP)

    def finish(self, step, prior=()):
        v = V.aggregate(step, list(prior), RAMP)
        (step / "verdict.json").write_text(json.dumps(v))
        return v

    def flat_step(self, rate=20000, p99_us=1_500):
        """A carried step below, its rounds flat (p99 under the 50 ms retry threshold)."""
        step = new_step(self.root, rate)
        for k in (1, 2, 3):
            add_round(step, f"round-{k}", rate, [summary(rate, p99_us=p99_us)])
        return self.finish(step)

    def test_three_carried_rounds_carry_the_step_with_their_statistics(self):
        step = new_step(self.root, 30000)
        self.assertEqual(self.plan(step)["next"], {"dir": "round-1", "round": 1, "attempt": 1, "settle": "short",
                                                   "reason": "round 1 of 3"})
        for k, (p99, share) in enumerate(((10_000, 1.0), (100_000, 1.02), (100_000, 0.995)), 1):
            add_round(step, f"round-{k}", 30000, [summary(30000, p99_us=p99)], received_share=share)
            nxt = self.plan(step)["next"]
            if k < 3:
                # Between the rounds of a carried step: the short settle.
                self.assertEqual((nxt["dir"], nxt["settle"]), (f"round-{k + 1}", "short"))
        self.assertTrue(self.plan(step)["done"])
        v = self.finish(step)
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual([r["dir"] for r in v["rounds"]], ["round-1", "round-2", "round-3"])
        p = v["stats"]["ack_p99_us"]
        self.assertEqual((p["n"], p["mean"], p["min"], p["max"]), (3, 70_000, 10_000, 100_000))
        self.assertAlmostEqual(p["sd"], math.sqrt(2700) * 1000, places=3)   # sample sd, n - 1
        self.assertAlmostEqual(p["cv"], math.sqrt(2700) / 70, places=6)
        # The headline is the MEAN of the rounds' p99; the merged histogram is beside it.
        self.assertEqual(v["ack_latency"]["p99_us"], 70_000)
        self.assertEqual(v["gate_p99_us"], 70_000)
        # Merged: 98 % at 1 ms, 0.5 % at 10 ms, 0.17 % at 20 ms, 1 % at 100 ms: its p99 is 100 ms.
        self.assertEqual(v["gate_p99_merged_us"], 100_000)
        self.assertEqual(v["ack_latency_merged"]["count"], 3 * 30000 * 60)
        rx = v["stats"]["received_rate"]
        self.assertAlmostEqual(rx["mean"], 30000 * (1.0 + 1.02 + 0.995) / 3, delta=1)
        self.assertAlmostEqual(v["received_rate"], rx["mean"])
        self.assertAlmostEqual(v["spread"]["received"], 0.025 / 0.995, places=3)
        # 10x in p99 is flagged, 2.5 % in received msg/s is not.
        self.assertEqual(len(v["spread"]["flags"]), 1, v["spread"])
        self.assertIn("p99 differs 10.0x across rounds", v["spread"]["flags"][0])
        self.assertIsNone(v["decided_by"])

    def test_one_round_that_lost_messages_fails_the_step_by_name(self):
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000)])
        add_round(step, "round-2", 30000, [summary(30000, missing=3)])
        # Nothing carried below it: no transient retry, and round 3 cannot carry the step.
        plan = self.plan(step)
        self.assertTrue(plan["done"], plan)
        v = self.finish(step)
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertEqual(v["decided_by"], "round-2")
        self.assertTrue(v["failures"][0].startswith("round 2: ledger did not close"), v["failures"])
        self.assertEqual(len(v["rounds"]), 2)
        self.assertEqual(V.next_rate([v], RAMP)[0], None)

    def test_a_void_round_is_rerun_alone(self):
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000)])
        add_round(step, "round-2", 30000, [summary(30000, saturated=True)])
        nxt = self.plan(step)["next"]
        self.assertEqual((nxt["dir"], nxt["settle"]), ("round-2-a2", "long"))
        self.assertIn("rerun of VOID round 2 (attempt 2 of 2): generator 0 saturated", nxt["reason"])
        add_round(step, "round-2-a2", 30000, [summary(30000)])
        self.assertEqual(self.plan(step)["next"]["dir"], "round-3")
        add_round(step, "round-3", 30000, [summary(30000)])
        v = self.finish(step)
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual([r["dir"] for r in v["rounds"]], ["round-1", "round-2-a2", "round-3"])
        self.assertEqual(v["rounds"][1]["attempts"], ["round-2", "round-2-a2"])
        self.assertTrue(any(n.startswith("round-2 was VOID (generator 0 saturated") for n in v["notes"]), v["notes"])
        self.assertEqual(v["stats"]["received_rate"]["n"], 3)

    def test_a_round_void_through_its_reruns_voids_the_step_and_stops_the_ladder(self):
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000, saturated=True)])
        add_round(step, "round-1-a2", 30000, [summary(30000, saturated=True)])
        self.assertTrue(self.plan(step)["done"])
        v = self.finish(step)
        self.assertEqual(v["verdict"], V.VOID)
        self.assertIn("round 1 stayed VOID through 2 attempt(s)", v["void_reason"])
        rate, why = V.next_rate([v], RAMP)
        self.assertIsNone(rate)
        self.assertIn("round 1 stayed VOID", why)

    def test_a_host_that_voids_two_attempts_is_due_for_replacement_and_gets_another_attempt(self):
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000, saturated=True)])
        self.assertIsNone(V.replace_due(step))
        add_round(step, "round-1-a2", 30000, [summary(30000, saturated=True)])
        self.assertEqual(V.replace_due(step), {"generator": 0, "after": "round-1-a2", "strikes": 2})
        # run.sh replaces the host and records it: no longer due, and the round reruns.
        (step / "replaced.json").write_text(json.dumps([{"generator": 0, "after": "round-1-a2"}]))
        self.assertIsNone(V.replace_due(step))
        plan = self.plan(step)
        self.assertFalse(plan["done"], plan)
        self.assertEqual(plan["next"]["dir"], "round-1-a3")
        add_round(step, "round-1-a3", 30000, [summary(30000)])
        add_round(step, "round-2", 30000, [summary(30000)])
        add_round(step, "round-3", 30000, [summary(30000)])
        v = self.finish(step)
        self.assertEqual(v["verdict"], V.CARRIED, v)

    def test_a_transient_bad_round_is_retried_and_the_step_carries(self):
        below = self.flat_step()
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000, p99_us=1_500)])
        add_round(step, "round-2", 30000, [summary(30000)], received_share=0.9)  # OFFER NOT TAKEN
        nxt = self.plan(step, [below])["next"]
        self.assertEqual((nxt["dir"], nxt["settle"]), ("round-2-a2", "long"))
        self.assertIn("retry of round 2 (OFFER NOT TAKEN) after a full settle: the carried step below it "
                      "(20000 msg/s) had p99 1.5 ms < 50 ms", nxt["reason"])
        self.assertIn("retry 1 of at most 2", nxt["reason"])
        add_round(step, "round-2-a2", 30000, [summary(30000, p99_us=1_500)])
        add_round(step, "round-3", 30000, [summary(30000, p99_us=1_500)])
        v = self.finish(step, [below])
        self.assertEqual(v["verdict"], V.CARRIED, v)
        self.assertEqual(v["round_retries"][0]["outcome"], "transient")
        self.assertTrue(any(n.startswith("round 2 retried: transient (round-2 was OFFER NOT TAKEN") for n in v["notes"]))
        # The retried round's first attempt does not count in the statistics.
        self.assertEqual([r["dir"] for r in v["rounds"]], ["round-1", "round-2-a2", "round-3"])
        self.assertAlmostEqual(v["stats"]["received_rate"]["min"], 30000, delta=1)
        # A ladder's retries are capped across steps: this one used one of two.
        self.assertEqual(V.retry_eligible([below, v], 40000, RAMP)["n"], 2)
        self.assertIsNone(V.retry_eligible([below, v, v], 40000, RAMP))
        # The step's retry is its own: the ladder never reruns the whole step.
        self.assertEqual(V.next_rate([below, v], RAMP), (45000, "coarse step up"))

    def test_a_retry_that_is_bad_again_counts_and_one_retry_a_step(self):
        below = self.flat_step()
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000, missing=2)])
        add_round(step, "round-1-a2", 30000, [summary(30000, missing=2)])
        self.assertTrue(self.plan(step, [below])["done"])
        v = self.finish(step, [below])
        self.assertEqual(v["verdict"], V.FAILED)
        self.assertEqual(v["decided_by"], "round-1-a2")
        self.assertIn("round 1 retried: bad again in round-1-a2", "; ".join(v["notes"]))
        # A second bad round after a transient one is not retried.
        step2 = new_step(self.root, 25000)
        add_round(step2, "round-1", 25000, [summary(25000, missing=2)])
        add_round(step2, "round-1-a2", 25000, [summary(25000)])
        add_round(step2, "round-2", 25000, [summary(25000, missing=1)])
        self.assertTrue(self.plan(step2, [below])["done"])
        self.assertEqual(self.finish(step2, [below])["decided_by"], "round-2")

    def test_no_retry_above_a_step_that_already_showed_the_knee(self):
        below = self.flat_step(p99_us=80_000)
        step = new_step(self.root, 30000)
        add_round(step, "round-1", 30000, [summary(30000, missing=2)])
        self.assertTrue(self.plan(step, [below])["done"])

    def test_capacity_uses_the_step_verdicts_and_their_means(self):
        # 60 ms below: past the retry threshold, so the bad round counts at once.
        low = self.flat_step(rate=20000, p99_us=60_000)
        high = new_step(self.root, 30000)
        add_round(high, "round-1", 30000, [summary(30000, p99_us=5_000)])
        add_round(high, "round-2", 30000, [summary(30000, p99_us=5_000)], drops={"queue-overflow": 4})
        high_v = self.finish(high, [low])
        self.assertEqual(high_v["verdict"], V.FAILED)
        cap = V.capacity([low, high_v])
        # The FAILED step's carried round does not count: the step is the unit.
        self.assertEqual(cap["lossless"]["offered_rate"], 20000)
        self.assertEqual(cap["lossless"]["gate_p99_us"], 60_000)
        self.assertEqual(cap["lossless"]["rounds"], 3)
        self.assertEqual(cap["p99_le_100ms"]["offered_rate"], 20000)
        self.assertEqual(cap["p99_le_100ms"]["gate_p99_sd_us"], 0.0)  # flat rounds

    def test_the_report_has_rounds_variance_and_merged_p99(self):
        run = Path(self.tmp.name) / "run"
        lad = run / "A" / "durable-qos1"
        (lad / "steps").mkdir(parents=True)
        (lad / "ladder.json").write_text(json.dumps({"workload": "durable-qos1", "arm": "A", "ramp": RAMP}))
        (run / "run.json").write_text(json.dumps({"tier": "n3", "brokers": 3, "workloads": ["durable-qos1"],
                                                  "rounds": 3, "started_utc": "20261027T063123Z"}))
        prior = []
        for rate, p99s in ((10000, (2_000, 2_100, 1_900)), (15000, (3_000, 9_000, 3_100))):
            step = new_step(lad / "steps", rate)
            for k, p99 in enumerate(p99s, 1):
                add_round(step, f"round-{k}", rate, [summary(rate, p99_us=p99)])
            v = V.aggregate(step, prior, RAMP)
            (step / "verdict.json").write_text(json.dumps(v))
            prior.append(v)
        V.report(run)
        md = (run / "report.md").read_text()
        self.assertIn("| step | rounds | offered msg/s |", md)
        self.assertIn("ack p99 mean ± sd | CV |", md)
        self.assertIn("| 3 of 3 |", md)
        self.assertIn("2.0 ± 0.1 ms (merged 2.0 ms)", md)
        self.assertIn("5.0 ± 3.4 ms (merged 3.1 ms) | rx 0.0%, p99 68.3% ⚑ |", md)
        self.assertIn("### Variance across rounds: durable-qos1", md)
        self.assertIn("largest CV of the ack p99: 68.3% at step 2 (15,000 msg/s, CARRIED)", md)
        self.assertIn("step 2 (15,000 msg/s, CARRIED): p99 differs 3.0x across rounds", md)
        self.assertIn("means of 3 rounds, all carried", md)
        self.assertIn("steps: 3 round(s) of", md)
        svg = (run / "report-durable-qos1.svg").read_text()
        self.assertIn("min–max of the rounds", svg)
        self.assertIn("ack p99 mean", svg)

    def test_a_round_dir_is_judged_with_the_run_s_broker_count(self):
        run = Path(self.tmp.name) / "run"
        steps = run / "A" / "durable-qos1" / "steps"
        steps.mkdir(parents=True)
        (run / "run.json").write_text(json.dumps({"brokers": 3}))
        step = new_step(steps, 30000)
        add_round(step, "round-1", 30000, [summary(30000)], unfetched=(2,))
        meta = json.loads((step / "round-1" / "step.json").read_text())
        del meta["brokers"]
        (step / "round-1" / "step.json").write_text(json.dumps(meta))
        self.assertIn("broker evidence: no scrapes from broker(s) 2", verdict_of(step / "round-1")["voids"])

    def test_statistics(self):
        st_ = V.stat([1, 2, 3, None])
        self.assertEqual((st_["n"], st_["mean"], st_["sd"], st_["min"], st_["max"], st_["cv"]), (3, 2, 1, 1, 3, 0.5))
        one = V.stat([5])
        self.assertEqual((one["mean"], one["sd"], one["cv"]), (5, None, None))
        self.assertEqual(V.stat([])["mean"], None)
        self.assertEqual(V.fmt_mean_sd(V.stat([10_000, 12_000])), "11.0 ± 1.4 ms")
        self.assertEqual(V.fmt_mean_sd(V.stat([10_000])), "10.0 ms")


class DropReasons(unittest.TestCase):
    def test_every_reason_the_broker_documents_or_increments_is_classified(self):
        root = HERE.parents[2]
        doc = (root / "crates/mqtt-observability/src/metrics.rs").read_text()
        m = re.search(r'"Messages dropped, by reason \(([^)]*)\)"', doc)
        self.assertIsNotNone(m, "the publish_dropped help text moved")
        reasons = {r.strip() for r in m.group(1).split(",")}
        for p in (root / "crates/mqttd/src").rglob("*.rs"):
            reasons |= set(re.findall(r'publish_dropped(?:_by)?\(\s*"([a-z0-9-]+)"', p.read_text()))
        missing = sorted(reasons - set(V.DROP_IS_LOSS))
        self.assertEqual(missing, [], "classify these in verdict.py's DROP_IS_LOSS")

    def test_the_settle_replay_family_and_withheld_acks_are_not_loss(self):
        for r in ("pending-cap-replay", "settle-replay", "settle-replay-refused", "pending-cap",
                  "outbound-id-write-failed"):
            self.assertFalse(V.drop_is_loss(r), r)
        for r in ("outbound-full", "append-backlog-full", "queue-overflow", "backlog-overflow", "no-shared-member",
                  "peer-unreachable", "brownout", "hub-ingress"):
            self.assertTrue(V.drop_is_loss(r), r)
        self.assertTrue(V.drop_is_loss("never-heard-of-it"))


class Chart(unittest.TestCase):
    def test_chart_self_test(self):
        V._load("chart_ramp", HERE / "chart-ramp.py").self_test()


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


def st(rate, verdict, good=None, p99_us=None):
    """A judged step for the ramp; p99_us None (no latency) never makes a retry eligible."""
    return {"rate": float(rate), "verdict": verdict, "good": verdict == V.CARRIED if good is None else good,
            "gate_p99_us": p99_us}


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
        steps.append(st(18800, V.FAILED))
        self.assertEqual(self.nxt(steps, bisect_on_fail=True)[0], 16900)
        steps.append(st(16900, V.CARRIED))
        self.assertEqual(self.nxt(steps, bisect_on_fail=True)[0], 17900)  # 17850 rounds half up
        steps.append(st(17900, V.CARRIED))
        self.assertEqual(self.nxt(steps)[0], 18400)
        steps.append(st(18400, V.DISCONNECTED))
        rate, why = self.nxt(steps)
        self.assertIsNone(rate)
        self.assertIn("knee bracketed: good at 17900, not at 18400", why)
        self.assertIn("2.8% gap", why)

    def test_a_lossless_step_above_1_s_keeps_climbing(self):
        # Carried at ack p99 2 s: no loss, offer taken, so the ramp climbs past it. A
        # verdict stored with good=False (the pre-2026-10-26 rule) climbs too.
        steps = [st(10000, V.CARRIED), st(15000, V.CARRIED, good=False)]
        rate, why = self.nxt(steps)
        self.assertEqual(rate, 22500)
        self.assertEqual(why, "coarse step up")
        steps.append(st(22500, V.NOT_TAKEN))
        self.assertEqual(self.nxt(steps)[0], 18800)  # bisect from the last CARRIED, 15000

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

    def test_a_failure_right_above_a_flat_step_is_retried_and_a_carried_retry_climbs_on(self):
        # Run 2's durable-qos1: 22,500 carried at ack p99 1.5 ms, 33,800 FAILED.
        steps = [st(10000, V.CARRIED), st(15000, V.CARRIED), st(22500, V.CARRIED, p99_us=1_500),
                 st(33800, V.FAILED, p99_us=15_000)]
        rate, why = self.nxt(steps)
        self.assertEqual(rate, 33800)
        self.assertIn("retry of the FAILED step at 33800 msg/s after a full settle", why)
        self.assertIn("1.5 ms < 50 ms", why)
        self.assertIn("retry 1 of at most 2", why)
        steps.append(st(33800, V.CARRIED))
        self.assertEqual(self.nxt(steps), (50700, "coarse step up"))
        marks = V.retry_marks(steps, self.CFG)
        self.assertEqual(marks[3]["mark"], "transient")
        self.assertEqual(marks[4]["mark"], "retry")

    def test_a_retry_that_fails_again_counts(self):
        steps = [st(15000, V.CARRIED), st(22500, V.CARRIED, p99_us=1_500), st(33800, V.FAILED),
                 st(33800, V.FAILED)]
        rate, why = self.nxt(steps)
        self.assertIsNone(rate)
        self.assertIn("FAILED at 33800", why)
        self.assertEqual(V.retry_marks(steps, self.CFG)[2]["mark"], "confirmed")
        # With bisect_on_fail, it brackets the knee as any bad step does; never a second retry.
        self.assertEqual(self.nxt(steps, bisect_on_fail=True)[0], 28200)

    def test_no_retry_when_the_step_below_already_showed_the_knee(self):
        # Run 1's durable-qos1: 76,100 carried at ack p99 66 ms, then 114,200 failed.
        steps = [st(50700, V.CARRIED, p99_us=4_000), st(76100, V.CARRIED, p99_us=65_800), st(114200, V.FAILED)]
        rate, why = self.nxt(steps, bisect_on_fail=True)
        self.assertEqual(rate, 95200)
        self.assertIn("bisect", why)
        self.assertIsNone(V.retry_marks(steps, self.CFG)[2])
        self.assertIsNone(self.nxt(steps)[0])
        # The threshold is a ramp setting.
        self.assertEqual(self.nxt(steps, retry_p99_ms=100)[0], 114200)

    def test_offer_not_taken_and_qos0_gates_are_retried_too(self):
        # QoS 0: gate_p99_us is the end-to-end p99 (run 2's qos0: 112,500 at e2e p99 30.1 ms).
        steps = [st(75000, V.CARRIED, p99_us=3_500), st(112500, V.CARRIED, p99_us=30_100), st(168800, V.NOT_TAKEN)]
        self.assertEqual(self.nxt(steps)[0], 168800)
        steps.append(st(168800, V.NOT_TAKEN))
        self.assertEqual(self.nxt(steps)[0], 140700)  # confirmed: bisect

    def test_retries_are_capped_per_ladder(self):
        cfg = {"bisect_on_fail": True}
        flat = 2_000
        steps = [st(10000, V.CARRIED, p99_us=flat), st(15000, V.FAILED), st(15000, V.CARRIED, p99_us=flat),  # retry 1
                 st(22500, V.FAILED), st(22500, V.CARRIED, p99_us=flat),                                     # retry 2
                 st(33800, V.FAILED)]                                                   # cap reached: counts
        rate, why = self.nxt(steps, **cfg)
        self.assertEqual(rate, 28200)
        self.assertIn("bisect between 22500 and 33800", why)
        self.assertIsNone(V.retry_marks(steps, self.CFG)[5])
        self.assertEqual(self.nxt(steps, retry_max=3, **cfg)[0], 33800)

    def test_a_void_retry_is_rerun_and_still_settles_the_first_attempt(self):
        steps = [st(22500, V.CARRIED, p99_us=2_000), st(33800, V.DISCONNECTED), st(33800, V.VOID)]
        rate, why = self.nxt(steps)
        self.assertEqual(rate, 33800)
        self.assertIn("rerun of a VOID step", why)
        steps.append(st(33800, V.CARRIED))
        self.assertEqual(self.nxt(steps)[0], 50700)
        self.assertEqual(V.retry_marks(steps, self.CFG)[1]["mark"], "transient")

    def test_a_ladder_judged_before_the_rule_marks_what_would_have_been_retried(self):
        steps = [st(22500, V.CARRIED, p99_us=1_500), st(33800, V.FAILED), st(28200, V.CARRIED)]
        self.assertEqual(V.retry_marks(steps, {"bisect_on_fail": True})[1]["mark"], "not-retried")

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

    def test_the_settle_waits_for_lanes_replication_routing_sessions_and_quiet(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "b.prom"

            def state(**gauges):
                p.write_text(prom(1, 1, gauges=gauges))
                return V.idle_state([p])

            self.assertEqual(V.not_idle(state(), 0), [])
            for g, v, why in (("mqttd_append_lane_jobs", 245, "append_lane_jobs 245"),
                              ("mqttd_inflight_messages", 3679, "inflight_messages 3679"),
                              ("mqttd_backlog_bytes", 68390000, "backlog_bytes 68390000"),
                              ("mqttd_peer_forwards_in_flight", 4, "peer_forwards_in_flight 4"),
                              ("mqttd_publish_held_for_unreachable", 1, "publish_held_for_unreachable 1"),
                              ("mqttd_hub_queue_depth", 300, "hub queue 300")):
                self.assertEqual(V.not_idle(state(**{g: v}), 0), [why])
            # A command or two in the hub queue is idle.
            self.assertEqual(V.not_idle(state(mqttd_hub_queue_depth=1), 0), [])
            # Replication lag (groups tracked but not caught up) holds the settle while it moves.
            behind = state(mqttd_replica_groups_tracked=179, mqttd_replica_groups_current=170)
            self.assertEqual(V.not_idle(behind, 0), ["replication catching up: b 9 groups behind"])
            closer = state(mqttd_replica_groups_tracked=179, mqttd_replica_groups_current=175)
            self.assertEqual(V.not_idle(closer, 0, prev=behind), ["replication catching up: b 4 groups behind"])
            # A lag that stood still since the last scrape is not catching up: recorded, not waited for.
            self.assertEqual(V.not_idle(closer, 0, prev=closer), [])
            self.assertEqual(closer["replication_lag"], {"b": 4})
            self.assertEqual(V.not_idle(state(mqttd_replica_groups_tracked=179, mqttd_replica_groups_current=179), 0), [])
            p.write_text(prom(1, 1).replace("# EOF", 'mqttd_routing_unsettled{reason="scan"} 0\n'
                                                      'mqttd_routing_unsettled{reason="mesh"} 1\n# EOF'))
            self.assertEqual(V.not_idle(V.idle_state([p]), 0), ["routing unsettled: b"])
            # A session scan running is background (every broker runs one every ~30 s):
            # recorded, and it holds the settle only when still running on the next pass.
            p.write_text(prom(1, 1).replace("# EOF", 'mqttd_routing_unsettled{reason="scan"} 1\n'
                                                      'mqttd_routing_unsettled{reason="mesh"} 0\n# EOF'))
            scanning = V.idle_state([p])
            self.assertEqual(scanning["scanning"], ["b"])
            self.assertEqual(V.not_idle(scanning, 0), [])
            self.assertEqual(V.not_idle(scanning, 0, prev=state()), [])
            self.assertEqual(V.not_idle(scanning, 0, prev=scanning),
                             ["session scan still running on b (two passes in a row)"])
            # Sessions back to the run's baseline, not below it.
            p.write_text(prom(1, 1).replace("mqttd_sessions 0", "mqttd_sessions 3"))
            self.assertTrue(V.not_idle(V.idle_state([p]), 0))
            self.assertEqual(V.not_idle(V.idle_state([p]), 0, baseline_sessions=3), [])
            # Quiet: the traffic and durable-writer counters stood still since the last scrape.
            before = state(mqttd_durable_writer_ops_total=340254)
            self.assertEqual(V.not_idle(state(mqttd_durable_writer_ops_total=340254), 0, prev=before), [])
            self.assertEqual(V.not_idle(state(mqttd_durable_writer_ops_total=340910), 0, prev=before),
                             ["not quiet: durable_writer_ops_total moved by 656"])

    def test_a_background_tick_does_not_hold_the_settle_and_a_drain_does(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "b.prom"

            def state(at, **gauges):
                p.write_text(prom(1, 1, gauges=gauges))
                st_ = V.idle_state([p])
                st_["at_unix_s"] = at
                return st_

            before = state(1000.0, mqttd_durable_writer_ops_total=340254)
            # 50 writer ops in 5 s (10/s): under the floor of 20/s, background.
            tick = state(1005.0, mqttd_durable_writer_ops_total=340304)
            self.assertEqual(V.not_idle(tick, 0, prev=before, step_rate=1000), [])
            self.assertEqual(tick["background"], ["durable_writer_ops_total +50 in 5.0 s (10.0/s, background)"])
            # 500 in 5 s (100/s): background beside a 152,000 msg/s step (0.1 %: 152/s),
            # not beside a 30,000 one (30/s).
            more = state(1005.0, mqttd_durable_writer_ops_total=340754)
            self.assertEqual(V.not_idle(more, 0, prev=before, step_rate=152000), [])
            self.assertEqual(V.not_idle(more, 0, prev=before, step_rate=30000),
                             ["not quiet: durable_writer_ops_total moved by 500 in 5.0 s (100/s > 30/s allowed)"])
            # A drain: deliveries at thousands per second, named with their size.
            p.write_text(prom(1, 26001, gauges={"mqttd_durable_writer_ops_total": 340254}))
            drain = V.idle_state([p])
            drain["at_unix_s"] = 1005.0
            self.assertEqual(V.not_idle(drain, 0, prev=before, step_rate=152000),
                             ["not quiet: publish_delivered_total moved by 26000 in 5.0 s (5200/s > 152/s allowed)"])
            # Work in the pipes holds it whatever the counters do.
            lanes = state(1005.0, mqttd_durable_writer_ops_total=340254, mqttd_append_lane_jobs=3)
            self.assertEqual(V.not_idle(lanes, 0, prev=before, step_rate=152000), ["append_lane_jobs 3"])

    def test_the_idle_command_reports_why_not(self):
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "broker-0.prom"
            p.write_text(prom(1, 1, gauges={"mqttd_append_lane_jobs": 3}))
            import contextlib
            import io
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                rc = V.main(["idle", str(p)])
            self.assertEqual(rc, 1)
            self.assertEqual(json.loads(buf.getvalue())["why_not"], ["append_lane_jobs 3"])

    def test_tier_prices(self):
        n3 = V.cost("n3")
        self.assertAlmostEqual(n3["eur_per_hour"], 3 * 0.17225 + 0.277375, places=6)
        n10 = V.cost("n10")
        self.assertAlmostEqual(n10["eur_per_hour"], 10 * 0.17225 + 4 * 0.277375, places=6)
        one = V.cost("n3", workloads=1, steps=9, step_secs=120)
        self.assertEqual(one["minutes"], round(10 + 18 + 3))
        # Rounds multiply the measuring time; a ladder's prime and long settles add to it.
        three = V.cost("n10", workloads=3, steps=9, step_secs=135, rounds=3, ladder_secs=300)
        self.assertEqual(three["ramp_min"], round(3 * (300 + 9 * 3 * 135) / 60, 1))
        self.assertEqual(three["minutes"], round(14 + 3 * (300 + 9 * 3 * 135) / 60 + 3))
        self.assertEqual(one["eur_hours_rounded_up"], round(n3["eur_per_hour"], 2))
        with self.assertRaises(ValueError):
            V.cost("n3", broker_type="ccx99")
        self.assertTrue(math.isclose(V.cost("n3", broker_type="ccx99", prices={"ccx99": 1.0})["eur_per_hour"],
                                     3 + 0.277375))



class PerVcpu(unittest.TestCase):
    def test_capacity_is_stated_per_vcpu_of_the_brokers_server_type(self):
        self.assertEqual(V.cluster_vcpu({"brokers": 3, "broker_type": "ccx53"}), 96)
        self.assertIsNone(V.cluster_vcpu({"brokers": 3, "broker_type": "local"}))
        self.assertEqual(V.per_vcpu(288_000, 96), " · 3,000 msg/s per vCPU")
        self.assertEqual(V.per_vcpu(288_000, None), "")

    def test_the_h3_tier_is_priced(self):
        c = V.cost("h3", workloads=1, steps=10, rounds=3)
        self.assertEqual((c["brokers"], c["broker_type"], c["generators"], c["gen_type"]), (3, "ccx53", 6, "ccx33"))
        self.assertAlmostEqual(c["eur_per_hour"], 3 * 1.06875 + 6 * 0.277375)
    def test_aws_types_have_vcpu_counts_and_a_usd_quote(self):
        self.assertEqual(V.cluster_vcpu({"brokers": 3, "broker_type": "m6i.8xlarge"}), 96)
        self.assertEqual(V.VCPU["m6i.4xlarge"], 16)
        c = V.cost("h3", broker_type="m6i.8xlarge", gen_type="m6i.2xlarge", currency="USD",
                   prices={"m6i.8xlarge": 1.632, "m6i.2xlarge": 0.408})
        self.assertEqual(c["currency"], "USD")
        self.assertAlmostEqual(c["eur_per_hour"], 3 * 1.632 + 6 * 0.408)

    def test_the_cloud_is_named_in_the_report(self):
        with tempfile.TemporaryDirectory() as d:
            run = Path(d)
            (run / "run.json").write_text(json.dumps({"tier": "h3", "cloud": "aws", "region": "eu-north-1",
                                                      "location": "eu-north-1a", "brokers": 1,
                                                      "broker_type": "m6i.8xlarge", "workloads": []}))
            V.report(run)
            md = (run / "report.md").read_text()
            self.assertIn("- cloud: aws, eu-north-1 (eu-north-1a)", md)
            self.assertIn("32 vCPU in all", md)

    def test_added_broker_env_is_disclosed_in_the_report(self):
        with tempfile.TemporaryDirectory() as d:
            run = Path(d)
            (run / "run.json").write_text(json.dumps({"tier": "h3", "brokers": 1, "broker_type": "ccx53",
                                                      "workloads": [], "extra_broker_env": ["MQTTD_STORE_SHARDS=8"]}))
            V.report(run)
            self.assertIn("broker env added to the shipped defaults: `MQTTD_STORE_SHARDS=8`",
                          (run / "report.md").read_text())

if __name__ == "__main__":
    unittest.main()
