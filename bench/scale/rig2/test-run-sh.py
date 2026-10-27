#!/usr/bin/env python3
"""Offline tests for bench/scale/rig2/run.sh's settle and its signal handling.

    python3 bench/scale/rig2/test-run-sh.py

The REAL run.sh runs at TIER=local in a scratch tree, against fake brokers: one HTTP
server per broker in this process, serving a canned /metrics that can fail, stall or
report a busy cluster on demand. local-cluster.sh, mqttd and mqtt-load are stubs; lib.sh,
verdict.py and the workloads are the real ones (symlinked). Checked: a broker whose
scrapes fail now and then does not hold the settle back (the first 10-node run never
settled because one failed scrape restarted the quiet period); a counter ticking at a
background rate does not hold it either, a drain does, and a settle that runs out names
what held it, even when its last pass was idle; the settle budget is
wall-clock time, not a count of sleeps; a broker that never answers voids the step with
its name; a broker whose step scrapes cannot be fetched voids that step by name, and the
ladder reruns it; and a TERM sent mid-step runs the trap at once (it used to wait for the ladder
to finish), stopping the generator and the local cluster.
"""
import json
import os
import signal
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCALE = HERE.parent

IDLE = ("# TYPE mqttd_connections_active gauge\nmqttd_connections_active {conns}\n"
        "mqttd_durable_writer_ops_total {writer}\nmqttd_publish_delivered_total {delivered}\n# EOF\n")


class FakeBroker:
    """One broker's health port. `pattern` is a cycle of 'ok' / 'fail' (HTTP 500) per
    request; `delay` stalls every answer; `conns` is the connection gauge served, or
    `conns_cycle` a cycle of them per answer; `writer_tick` and `delivered_tick` grow
    the durable-writer and delivered counters by that much on every answer."""

    def __init__(self):
        self.pattern, self.delay, self.conns, self.requests = ["ok"], 0.0, 0, 0
        self.conns_cycle, self.writer_tick, self.delivered_tick = None, 0, 0
        self.writer = self.delivered = self.answers = 0
        self.lock = threading.Lock()
        fb = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 — http.server's name
                with fb.lock:
                    kind = fb.pattern[fb.requests % len(fb.pattern)]
                    fb.requests += 1
                    if kind != "fail":
                        fb.writer += fb.writer_tick
                        fb.delivered += fb.delivered_tick
                        conns = fb.conns_cycle[fb.answers % len(fb.conns_cycle)] if fb.conns_cycle else fb.conns
                        fb.answers += 1
                        body = IDLE.format(conns=conns, writer=fb.writer, delivered=fb.delivered).encode()
                if fb.delay:
                    time.sleep(fb.delay)  # the fake broker's stall, not a wait in the test
                if kind == "fail":
                    self.send_response(500)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.port = self.server.server_address[1]
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def close(self):
        self.server.shutdown()
        self.server.server_close()


LOCAL_CLUSTER_STUB = """#!/usr/bin/env bash
# Stub of rig2/local-cluster.sh: the brokers are the test's HTTP servers.
set -euo pipefail
STATE="$2"
case "$1" in
start)
	mkdir -p "$STATE"
	: >"$STATE/brokers.tsv"
	i=0
	for p in $FAKE_BROKER_PORTS; do
		printf '%s\\t127.0.0.1:%s\\t127.0.0.1:%s\\n' "$i" "$((21000 + i))" "$p" >>"$STATE/brokers.tsv"
		i=$((i + 1))
	done
	;;
stop) touch "$STATE/stopped" ;;
esac
"""

LOAD_STUB = """#!/usr/bin/env bash
# Stub of mqtt-load: no summary (the step is VOID), or, with FAKE_LOAD_PIDFILE, a
# generator that runs until it is killed.
[ "${1:-}" != --version ] || { echo "mqtt-load stub"; exit 0; }
if [ -n "${FAKE_LOAD_PIDFILE:-}" ]; then
	echo $$ >"$FAKE_LOAD_PIDFILE"
	exec sleep 300
fi
exit 0
"""

MQTTD_STUB = "#!/usr/bin/env bash\necho 'mqttd stub'\n"


def poll(cond, secs, what):
    """A bounded poll: returns as soon as cond() holds, fails loudly when it never does."""
    deadline = time.monotonic() + secs
    while time.monotonic() < deadline:
        if cond():
            return
        time.sleep(0.1)
    raise AssertionError(f"timed out after {secs} s waiting for {what}")


class RunShTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        scale = root / "bench" / "scale"
        rig2 = scale / "rig2"
        rig2.mkdir(parents=True)
        for f in ("lib.sh", "cloud.sh"):
            (scale / f).symlink_to(SCALE / f)
        for f in ("run.sh", "verdict.py", "chart-ramp.py", "scrape-loop.py", "workloads"):
            (rig2 / f).symlink_to(HERE / f)
        for path, text in ((rig2 / "local-cluster.sh", LOCAL_CLUSTER_STUB), (root / "mqttd", MQTTD_STUB),
                           (root / "mqtt-load", LOAD_STUB)):
            path.write_text(text)
            path.chmod(0o755)
        # run.sh records the repository's commit.
        git = ["git", "-C", str(root), "-c", "user.name=t", "-c", "user.email=t@t", "-c", "commit.gpgsign=false"]
        subprocess.run(git + ["init", "-q"], check=True)
        subprocess.run(git + ["commit", "-q", "--allow-empty", "-m", "t"], check=True)
        self.root, self.run_sh, self.run_dir = root, rig2 / "run.sh", root / "run"
        self.brokers = [FakeBroker() for _ in range(3)]

    def tearDown(self):
        for b in self.brokers:
            b.close()
        self.tmp.cleanup()

    def env(self, **extra):
        env = dict(os.environ)
        env.update(TIER="local", OBSERVE="0", SKIP_BUILD="1", WORKLOADS="durable-qos1",
                   MQTTD_BIN=str(self.root / "mqttd"), MQTT_LOAD_BIN=str(self.root / "mqtt-load"),
                   RUN_DIR=str(self.run_dir), MAX_STEPS="1", PRIME_SECS="0", WARMUP="1", MEASURE="1",
                   WINDOW="1", SCRAPE_INTERVAL="1", SETTLE_POLL="1", CHILD_GRACE="5",
                   BENCH_KEEP_AWAKE_PID="none",  # no caffeinate
                   FAKE_BROKER_PORTS=" ".join(str(b.port) for b in self.brokers))
        env.update({k: str(v) for k, v in extra.items()})
        return env

    def run_to_end(self, timeout=120, **extra):
        r = subprocess.run(["bash", str(self.run_sh)], env=self.env(**extra), capture_output=True, text=True,
                           timeout=timeout, start_new_session=True)
        self.assertEqual(r.returncode, 0, r.stderr[-3000:])
        return r

    def idle_before(self):
        steps = sorted((self.run_dir / "A" / "durable-qos1" / "steps").glob("*/step.json"))
        self.assertTrue(steps, "no step was started")
        return json.loads(steps[0].read_text())["idle_before"]

    def test_a_flaky_broker_does_not_hold_the_settle_back(self):
        # Broker 1 fails two requests out of three: the retry inside a pass cannot always
        # save it, so every other pass misses it. Such a pass is unknown; it must neither
        # restart nor confirm the quiet period.
        self.brokers[1].pattern = ["fail", "fail", "ok"]
        self.run_to_end(SETTLE_QUIET_LONG=4, SETTLE_BUDGET_LONG=60)
        rec = self.idle_before()
        self.assertTrue(rec["idle"], rec)
        self.assertGreaterEqual(rec["unknown_passes"], 1, rec)
        self.assertIn("broker-1", rec["scrape_misses"], rec)
        self.assertGreaterEqual(rec["waited_s"], 4, rec)
        self.assertLess(rec["waited_s"], 30, rec)

    def test_the_settle_budget_is_wall_clock_and_the_brokers_are_scraped_together(self):
        # Every scrape stalls 2 s and the cluster never goes idle. The budget is 8 s of
        # wall clock: three passes (at 0, ~3 and ~6 s) if the brokers are scraped
        # together, two if one after another (6 s a pass).
        for b in self.brokers:
            b.delay, b.conns = 2.0, 1
        self.run_to_end(SETTLE_QUIET_LONG=4, SETTLE_BUDGET_LONG=8)
        rec = self.idle_before()
        self.assertFalse(rec["idle"], rec)
        self.assertGreaterEqual(rec["waited_s"], 8, rec)
        self.assertLessEqual(rec["waited_s"], 14, rec)
        self.assertGreaterEqual(rec["passes"], 3, rec)
        self.assertTrue(any("connections" in w for w in rec["state"]["why_not"]), rec)

    def test_a_background_tick_does_not_hold_the_settle(self):
        # Every broker's durable writer ticks by 2 an answer, one answer a pass of ~1 s:
        # 6/s over the cluster, under the background floor (20/s). Before, any movement
        # restarted the quiet period and this cluster never settled.
        for b in self.brokers:
            b.writer_tick = 2
        self.run_to_end(SETTLE_QUIET_LONG=3, SETTLE_BUDGET_LONG=30)
        rec = self.idle_before()
        self.assertTrue(rec["idle"], rec)
        self.assertLess(rec["waited_s"], 15, rec)
        self.assertTrue(any("durable_writer_ops_total" in x for x in rec["state"].get("background", [])), rec)

    def test_a_drain_holds_the_settle_and_is_named(self):
        # Deliveries still flowing at ~30,000/s: not background, and the warning says so.
        for b in self.brokers:
            b.delivered_tick = 10000
        r = self.run_to_end(SETTLE_QUIET_LONG=3, SETTLE_BUDGET_LONG=6)
        rec = self.idle_before()
        self.assertFalse(rec["idle"], rec)
        self.assertTrue(any("publish_delivered_total moved by 30000" in w for w in rec["state"]["why_not"]), rec)
        self.assertIn("cluster not settled", r.stderr)
        self.assertRegex(r.stderr, r"publish_delivered_total moved by 30000 in [0-9.]+ s .*VOID \(unreset\)")

    def test_a_settle_that_ends_on_an_idle_pass_names_what_restarted_it(self):
        # Broker 0 has a connection on its second answer only (the first is the run's
        # baseline scrape): the settle's first pass is busy, every later one idle, and
        # the budget (4 s) runs out before the quiet period (6 s). The last pass is idle;
        # the record and the warning still name what restarted the quiet period.
        self.brokers[0].conns_cycle = [0, 1] + [0] * 1000
        r = self.run_to_end(SETTLE_QUIET_LONG=6, SETTLE_BUDGET_LONG=4)
        rec = self.idle_before()
        self.assertFalse(rec["idle"], rec)
        self.assertEqual(rec["blocked_passes"], 1, rec)
        self.assertEqual(rec["last_blocked"]["why_not"], ["connections 1 > 0"], rec)
        self.assertEqual(len(rec["state"]["why_not"]), 1, rec)
        self.assertRegex(rec["state"]["why_not"][0], r"^the 6 s quiet period last restarted at 0 s of \d+, on: "
                                                      r"connections 1 > 0 \(1 of \d+ passes were not idle\)$")
        self.assertIn("on: connections 1 > 0", r.stderr)
        log = [json.loads(x) for x in next((self.run_dir / "A" / "durable-qos1" / "steps").glob("*/pre/passes.jsonl"))
               .read_text().splitlines()]
        self.assertEqual(len(log), rec["passes"], rec)
        self.assertEqual([x["idle"] for x in log], [False] + [True] * (len(log) - 1), log)

    def test_a_broker_that_never_answers_voids_the_step_by_name(self):
        self.brokers[2].pattern = ["fail"]
        self.run_to_end(SETTLE_QUIET_LONG=2, SETTLE_BUDGET_LONG=4)
        rec = self.idle_before()
        self.assertFalse(rec["idle"], rec)
        self.assertEqual(rec["unknown_passes"], rec["passes"], rec)
        self.assertTrue(any("no scrape of broker-2 answered" in w for w in rec["state"]["why_not"]), rec)
        verdict = json.loads(next((self.run_dir / "A" / "durable-qos1" / "steps").glob("*/verdict.json")).read_text())
        self.assertEqual(verdict["verdict"], "VOID", verdict)
        self.assertTrue(any("broker-2" in v for v in verdict.get("voids", [])), verdict)

    def test_a_broker_whose_step_scrapes_cannot_be_fetched_voids_the_step_and_it_is_rerun(self):
        # Broker 1's host has no room for the step (its steps/ is a file): its scrape loop
        # does not start and its scrapes cannot be fetched. The step is VOID by that
        # broker's name, never judged on the others' sum, and the ladder reruns it.
        host = self.run_dir / "hosts" / "broker-1"
        host.mkdir(parents=True)
        (host / "steps").write_text("not a directory\n")
        r = self.run_to_end(MAX_STEPS=2, SETTLE_QUIET_LONG=1, SETTLE_BUDGET_LONG=20)
        self.assertIn("could not fetch the scrapes of broker(s) 1", r.stderr)
        steps = sorted(p for p in (self.run_dir / "A" / "durable-qos1" / "steps").iterdir() if p.is_dir())
        self.assertEqual(len(steps), 2, [p.name for p in steps])
        first = json.loads((steps[0] / "verdict.json").read_text())
        self.assertEqual(first["verdict"], "VOID", first)
        self.assertIn("broker evidence: no scrapes from broker(s) 1", first["voids"])
        # The other brokers' scrapes came back whole, through the tar stream.
        for b in (0, 2):
            self.assertTrue(list((steps[0] / "scrapes" / f"broker-{b}").glob("*.prom")), f"broker {b}'s scrapes")
        self.assertFalse(list(steps[0].glob(".fetch-*")), "a fetch left its staging directory")
        self.assertEqual(json.loads((steps[1] / "step.json").read_text())["rate"], first["rate"])
        self.assertIn("rerun of a VOID step", json.loads((steps[1] / "step.json").read_text())["ramp_reason"])

    def test_term_mid_step_runs_the_trap_at_once(self):
        pidfile = self.root / "load.pid"
        log = open(self.root / "run.log", "w")
        p = subprocess.Popen(["bash", str(self.run_sh)], env=self.env(FAKE_LOAD_PIDFILE=pidfile, MEASURE=300,
                                                                     SETTLE_QUIET_LONG=1),
                             stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            poll(lambda: pidfile.is_file() and pidfile.read_text().strip(), 60, "the generator to start")
            load_pid = int(pidfile.read_text())
            t0 = time.monotonic()
            p.send_signal(signal.SIGTERM)
            p.wait(timeout=20)
            took = time.monotonic() - t0
        finally:
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGKILL)
                p.wait()
            log.close()
        out = (self.root / "run.log").read_text()
        self.assertEqual(p.returncode, 143, out[-3000:])
        self.assertLess(took, 15, out[-3000:])
        self.assertIn("stopping (trap; exit code was 143)", out)
        self.assertTrue((self.run_dir / "local" / "stopped").is_file(), "the trap did not stop the local cluster")

        def gone():
            try:
                os.kill(load_pid, 0)
            except ProcessLookupError:
                return True
            return False
        poll(gone, 5, "the generator to be gone (the trap must stop it)")


if __name__ == "__main__":
    unittest.main()
