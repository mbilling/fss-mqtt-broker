#!/usr/bin/env python3
"""Offline tests for bench/scale/rig2/run.sh's settle and its signal handling.

    python3 bench/scale/rig2/test-run-sh.py

The REAL run.sh runs at TIER=local in a scratch tree, against fake brokers: one HTTP
server per broker in this process, serving a canned /metrics that can fail, stall or
report a busy cluster on demand. local-cluster.sh, mqttd and mqtt-load are stubs; lib.sh,
verdict.py and the workloads are the real ones (symlinked). Checked: a broker whose
scrapes fail now and then does not hold the settle back (the first 10-node run never
settled because one failed scrape restarted the quiet period); the settle budget is
wall-clock time, not a count of sleeps; a broker that never answers voids the step with
its name; and a TERM sent mid-step runs the trap at once (it used to wait for the ladder
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

IDLE = "# TYPE mqttd_connections_active gauge\nmqttd_connections_active {conns}\n# EOF\n"


class FakeBroker:
    """One broker's health port. `pattern` is a cycle of 'ok' / 'fail' (HTTP 500) per
    request; `delay` stalls every answer; `conns` is the connection gauge served."""

    def __init__(self):
        self.pattern, self.delay, self.conns, self.requests = ["ok"], 0.0, 0, 0
        self.lock = threading.Lock()
        fb = self

        class H(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 — http.server's name
                with fb.lock:
                    kind = fb.pattern[fb.requests % len(fb.pattern)]
                    fb.requests += 1
                if fb.delay:
                    time.sleep(fb.delay)  # the fake broker's stall, not a wait in the test
                if kind == "fail":
                    self.send_response(500)
                    self.end_headers()
                    return
                body = IDLE.format(conns=fb.conns).encode()
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
