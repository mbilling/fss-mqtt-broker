#!/usr/bin/env python3
"""Offline provider selection/recovery tests: all cloud CLIs are stubs."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCALE = Path(__file__).resolve().parent


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.rig = self.root / "bench/scale"
        self.rig.mkdir(parents=True)
        for name in ("lib.sh", "cloud.sh", "run.sh", "teardown.sh", "run-curve.sh", "cpu.sh", "test-upcloud-quota.py", "482-smoke.sh", "482-constant-driver-N5-N7-optionB.env", "extract-lane-e.py"):
            shutil.copy2(SCALE / name, self.rig / name)
        for name in ("terraform", "terraform-upcloud"):
            (self.rig / name).mkdir()
        bin_dir = self.root / "bin"
        bin_dir.mkdir()
        self.bin_dir = bin_dir
        self.log = self.root / "calls.jsonl"
        stub = '''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps([os.path.basename(sys.argv[0]), os.getcwd(), sys.argv[1:]]) + "\\n")
# Optional failed state destroy plus a real (stubbed) label-recovery path.
if "destroy" in sys.argv and os.getenv("FAIL_DESTROY") == "1": sys.exit(77)
if os.path.basename(sys.argv[0]) == "hcloud" and os.getenv("LEAK_SERVER") == "1":
    if sys.argv[1:3] == ["server", "list"]: print("123 leaked-server")
    sys.exit(0)
# Stop before output/bootstrap, but let the EXIT trap exercise destroy.
sys.exit(77 if "apply" in sys.argv or os.path.basename(sys.argv[0]) in ("hcloud", "ssh") else 0)
'''
        for name in ("terraform", "tofu", "hcloud", "ssh"):
            path = bin_dir / name
            path.write_text(stub)
            path.chmod(0o755)
        # Do not inherit real cloud tokens, TF_VAR_* settings or workload knobs.
        self.env = {
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "HOME": str(self.root), "CALL_LOG": str(self.log),
            "MQTTD_VERSION": "test", "OBSERVE": "0",
            "RUN_DIR": str(self.root / "run"),
        }

    def run_script(self, name, *args, **env):
        return subprocess.run(
            ["bash", str(self.rig / name), *args], env=self.env | env,
            capture_output=True, text=True, timeout=15,
        )

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def cpu_session(self, **extra):
        # Phase handshakes, not a guessed sampling duration: the fake driver
        # advances only after both streams observed preflight and measurement.
        ssh = self.bin_dir / "ssh"
        ssh.write_text('''#!/usr/bin/env python3
import os, pathlib, time
print("CPU_STREAM_START_UTC test", flush=True)
while True:
    phase = pathlib.Path(os.environ["CPU_PHASE"])
    print("SAMPLE " + (phase.read_text() if phase.exists() else "preflight"), flush=True)
    if os.getenv("FAIL_SAMPLER") == "1": break
    time.sleep(0.01)
''')
        driver = self.bin_dir / "driver"
        driver.write_text('''#!/usr/bin/env python3
import os, pathlib, time
root = pathlib.Path(os.environ["CPU_DIR"])
for phase in ("preflight", "measurement"):
    pathlib.Path(os.environ["CPU_PHASE"]).write_text(phase)
    deadline = time.monotonic() + 5
    while not all("SAMPLE " + phase in (root / name).read_text() for name in ("cpu-broker0.txt", "cpu-driver0.txt")):
        if time.monotonic() >= deadline: raise SystemExit("sampler failed to follow driver phase")
        time.sleep(0.01)
    if os.getenv("FAIL_DRIVER") == "1": raise SystemExit(77)
    if os.getenv("FAIL_SAMPLER") == "1":
        def alive(pid):
            try: os.kill(pid, 0)
            except ProcessLookupError: return False
            return True
        pids = [int(line.split()[0]) for line in (root / "samplers.tsv").read_text().splitlines()]
        while any(alive(pid) for pid in pids):
            if time.monotonic() >= deadline: raise SystemExit("sampler did not exit")
            time.sleep(0.01)
        raise SystemExit(0)  # A successful driver must not hide dead samplers.
''')
        driver.chmod(0o755)
        out = self.root / "cpu"
        env = self.env | {"CPU_DIR": str(out), "CPU_PHASE": str(self.root / "phase"), **extra}
        result = subprocess.run(["bash", "-c", '''
source "$1/lib.sh"
source "$1/cpu.sh"
RUN="$2"; N=1; D=1
broker_pub_ip() { echo broker; }
driver_pub_ip() { echo driver; }
with_cpu_sampling "$CPU_DIR" driver
''', "test", str(self.rig), str(self.root)], env=env, capture_output=True, text=True, timeout=15)
        for line in (out / "samplers.tsv").read_text().splitlines():
            with self.assertRaises(ProcessLookupError, msg="sampler must be reaped"):
                os.kill(int(line.split()[0]), 0)
        return result, out

    def test_cpu_streams_cover_the_driver_and_stop_without_a_timer_tail(self):
        result, out = self.cpu_session()
        self.assertEqual(result.returncode, 0, result.stderr)
        for name in ("cpu-broker0.txt", "cpu-driver0.txt"):
            text = (out / name).read_text()
            self.assertIn("SAMPLE preflight", text)
            self.assertIn("SAMPLE measurement", text)

    def test_cpu_streams_are_reaped_when_the_driver_fails(self):
        result, _ = self.cpu_session(FAIL_DRIVER="1")
        self.assertEqual(result.returncode, 77, result.stderr)

    def test_early_sampler_exit_is_not_success(self):
        result, _ = self.cpu_session(FAIL_SAMPLER="1")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("coverage is incomplete", result.stderr)

    def test_shape_only_alias_and_malformed_modes_cannot_provision(self):
        result = self.run_script("run.sh", "smoke", SHAPE_ONLY="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        for args, env in (
            (("smoke", "3"), {}),
            (("smoke",), {"PREFLIGHT_ONLY": "true"}),
            (("smoke",), {"SHAPE_ONLY": "true"}),
        ):
            result = self.run_script("run.sh", *args, HCLOUD_TOKEN="dummy", **env)
            self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertEqual(self.calls(), [])

    def test_all_shapes_fail_before_any_cloud_call(self):
        for sizes, env, diagnostic in (
            (("10",), {"DRIVER_COUNT": "4"}, "spreads unevenly"),
            (("1", "10"), {"DRIVER_COUNT": "4"}, "spreads unevenly"),
            (("1", "2"), {}, "sizes must be"),
            (("1",), {"DRIVER_COUNT": "2.5"}, "DRIVER_COUNT"),
            (("1",), {"DRIVER_COUNT": "0"}, "DRIVER_COUNT"),
            (("1",), {"DRIVER_TYPE": "unknown"}, "unknown CPU count"),
            (("1",), {"TF_CLI_ARGS_apply": "-var driver_count=4"}, "opaque variable"),
        ):
            with self.subTest(sizes=sizes, env=env):
                result = self.run_script("run.sh", "full", *sizes, HCLOUD_TOKEN="dummy", **env)
                self.assertNotEqual(result.returncode, 0, result.stdout)
                self.assertIn(diagnostic, result.stderr)
                self.assertEqual(self.calls(), [])

    def test_offline_matrix_positive_controls_need_no_token_or_cloud(self):
        for args, env in (
            (("smoke",), {}),
            (("standard",), {}),
            (("full", "1", "3", "5"), {}),
            (("standard", "10"), {"DRIVER_COUNT": "4", "LANE_B_PUB_CONTAINERS": "3",
                                    "LANE_B_SUB_CONTAINERS": "5"}),
            (("smoke",), {"CLOUD": "upcloud"}),
            (("smoke",), {"DRIVER_TYPE": "custom", "DRIVER_VCPUS": "8"}),
        ):
            with self.subTest(args=args, env=env):
                result = self.run_script("run.sh", *args, PREFLIGHT_ONLY="1", **env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn("no cloud calls made", result.stderr)
                self.assertEqual(self.calls(), [])

    def test_variable_file_shape_overrides_fail_closed(self):
        path = self.rig / "terraform/terraform.tfvars.json"
        path.write_text('{"driver_count":4}')
        result = self.run_script("run.sh", "standard", HCLOUD_TOKEN="dummy")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("overrides workload shape", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_tf_var_driver_count_is_validated_not_silently_ignored(self):
        result = self.run_script("run.sh", "standard", HCLOUD_TOKEN="dummy", TF_VAR_driver_count="4")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("spreads unevenly", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_upcloud_smoke_defaults_and_trap(self):
        result = self.run_script("run.sh", "smoke", CLOUD="upcloud", UPCLOUD_TOKEN="dummy")
        self.assertNotEqual(result.returncode, 0)
        calls = self.calls()
        self.assertEqual([c[2][0] for c in calls], ["init", "apply", "destroy"])
        self.assertTrue(all(c[0] == "tofu" for c in calls), calls)
        self.assertTrue(all(c[1] == str(self.rig / "terraform-upcloud") for c in calls))
        self.assertNotIn("broker_server_type=cpx32", calls[1][2])
        self.assertNotIn("driver_server_type=cpx42", calls[1][2])
        self.assertIn("driver_count=1", calls[1][2])

    def test_hcloud_smoke_unchanged(self):
        self.run_script("run.sh", "smoke", HCLOUD_TOKEN="dummy")
        calls = self.calls()
        self.assertEqual(len(calls), 3)
        self.assertTrue(all(c[0] == "tofu" for c in calls), calls)
        self.assertTrue(all(c[1] == str(self.rig / "terraform") for c in calls))
        self.assertIn("broker_server_type=cpx32", calls[1][2])
        self.assertIn("driver_server_type=cpx42", calls[1][2])

    def test_explicit_upcloud_plans_preserved(self):
        self.run_script("run.sh", "smoke", CLOUD="upcloud", UPCLOUD_TOKEN="dummy",
                        BROKER_TYPE="PREMIUM-48xCPU-96GB", DRIVER_TYPE="4xCPU-8GB")
        args = self.calls()[1][2]
        self.assertIn("broker_server_type=PREMIUM-48xCPU-96GB", args)
        self.assertIn("driver_server_type=4xCPU-8GB", args)

    def test_preflight_errors_touch_no_cloud(self):
        for env, expected in (
            ({"CLOUD": "upcloud", "HCLOUD_TOKEN": "dummy"}, "UPCLOUD_TOKEN is not set"),
            ({"CLOUD": "hcloud", "UPCLOUD_TOKEN": "dummy"}, "HCLOUD_TOKEN is not set"),
            ({"CLOUD": "typo"}, "unknown CLOUD"),
            ({"CLOUD": "upcloud", "UPCLOUD_TOKEN": "dummy", "BROKER_NIC_SPREAD": "1"}, "not supported"),
        ):
            with self.subTest(env=env):
                result = self.run_script("run.sh", "smoke", **env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)
                self.assertEqual(self.calls(), [])

    def test_upcloud_teardown_uses_only_its_state(self):
        (self.rig / "terraform-upcloud/terraform.tfstate").write_text("{}")
        result = self.run_script("teardown.sh", CLOUD="upcloud", UPCLOUD_TOKEN="dummy")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("No account-wide leak audit", result.stderr)
        self.assertEqual([c[2][0] for c in self.calls()], ["init", "destroy"])
        self.assertTrue(all(c[0] == "tofu" for c in self.calls()), self.calls())
        self.assertTrue(all(c[1] == str(self.rig / "terraform-upcloud") for c in self.calls()))

    def test_hcloud_teardown_never_selects_terraform(self):
        (self.rig / "terraform/terraform.tfstate").write_text("{}")
        result = self.run_script("teardown.sh", HCLOUD_TOKEN="dummy")
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        self.assertEqual(calls[0], ["tofu", str(self.rig / "terraform"),
                                   ["destroy", "-auto-approve", "-var", "node_count=1"]])
        self.assertTrue(all(c[0] == "hcloud" for c in calls[1:]), calls)

    def test_non_default_ssh_key_is_passed_on_apply_and_every_destroy(self):
        key = self.root / "hetzner"
        key.write_text("private")
        key.with_suffix(".pub").write_text("ssh-ed25519 AAAA test")
        pub = f"ssh_public_key_path={key}.pub"
        self.run_script("run.sh", "smoke", HCLOUD_TOKEN="dummy", SSH_KEY=str(key))
        calls = self.calls()
        self.assertEqual([c[2][0] for c in calls], ["init", "apply", "destroy"])
        self.assertIn(pub, calls[1][2], calls[1])
        self.assertIn(pub, calls[2][2], calls[2])
        self.log.write_text("")
        (self.rig / "terraform/terraform.tfstate").write_text("{}")
        result = self.run_script("teardown.sh", HCLOUD_TOKEN="dummy", SSH_KEY=str(key))
        self.assertEqual(result.returncode, 0, result.stderr)
        destroy = self.calls()[0][2]
        self.assertIn(pub, destroy, destroy)

    def test_ssh_key_without_pub_fails_before_tofu(self):
        key = self.root / "hetzner"
        key.write_text("private")
        result = self.run_script("run.sh", "smoke", HCLOUD_TOKEN="dummy", SSH_KEY=str(key))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("SSH_KEY.pub", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_missing_pub_does_not_block_force_recovery_after_failed_destroy(self):
        key = self.root / "hetzner"
        key.write_text("private")
        (self.rig / "terraform/terraform.tfstate").write_text("{}")
        result = self.run_script("teardown.sh", "--force", HCLOUD_TOKEN="dummy",
                                 SSH_KEY=str(key), FAIL_DESTROY="1", LEAK_SERVER="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("destroy failed", result.stderr)
        calls = self.calls()
        self.assertEqual(calls[0][0], "tofu")
        self.assertIn(f"ssh_public_key_path={key}.pub", calls[0][2])
        self.assertTrue(any(c[0] == "hcloud" and c[2] == ["server", "delete", "123"] for c in calls), calls)

    def test_extractor_controls_run_in_ci(self):
        result = subprocess.run(["python3", str(self.rig / "extract-lane-e.py"), "--self-test"],
                                env=self.env, capture_output=True, text=True, timeout=15)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("test_incomplete_reset_and_unsupported_scrapes_are_invalid", result.stderr)

    def test_campaign_smoke_does_not_inherit_full_fleet_or_ladder(self):
        key = self.root / "campaign-key"
        key.write_text("private")
        key.with_suffix(".pub").write_text("ssh-ed25519 AAAA test")
        result = subprocess.run(
            ["bash", "-c", 'source "$1"; exec bash "$2"', "smoke-test",
             str(self.rig / "482-constant-driver-N5-N7-optionB.env"),
             str(self.rig / "482-smoke.sh")],
            env=self.env | {"HCLOUD_TOKEN": "dummy", "SSH_KEY": str(key),
                            "MQTTD_URL": "https://example.invalid/pinned-main", "MQTTD_SHA256": "a" * 64,
                            "BENCH_GIT_REF": "29e43854fd699fa910cf383db273dee191f47d87"},
            capture_output=True, text=True, timeout=15,
        )
        # The apply stub deliberately fails, then the EXIT trap destroys.
        self.assertNotEqual(result.returncode, 0)
        calls = self.calls()
        self.assertEqual([c[2][0] for c in calls], ["init", "apply", "destroy"])
        args = calls[1][2]
        for value in ("driver_count=1", "broker_server_type=cpx32", "driver_server_type=cpx42",
                      f"ssh_public_key_path={key}.pub", "mqttd_url=https://example.invalid/pinned-main",
                      "mqttd_sha256=" + "a" * 64,
                      "bench_git_ref=29e43854fd699fa910cf383db273dee191f47d87"):
            self.assertIn(value, args)
        self.assertIn(f"ssh_public_key_path={key}.pub", calls[2][2])
        shapes = list((self.rig / ".runs").glob("*/preflight-1/results/nodes=1/laneE/shape.txt"))
        self.assertEqual(len(shapes), 1)
        shape = shapes[0].read_text()
        self.assertIn("brokers=1 drivers=1", shape)
        self.assertIn("= 1000 msg/s", shape)
        self.assertNotIn("300000", shape)
        self.assertEqual(len([line for line in shape.splitlines() if line.rstrip().endswith("| ok")]), 1)

    def test_lane_e_unpinned_shape_describes_prefer_local_not_round_robin(self):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({
            "brokers": [{}] * 7,
            "drivers": [{"vcpus": 8}] * 5,
        }))
        out = self.root / "shape"
        for subs, expect in (("7", "predicted crossing ≈ 0%"), ("5", "predicted crossing ≈ 2/7")):
            with self.subTest(subs=subs):
                result = self.run_script(
                    "run-curve.sh", str(out), str(inventory),
                    LANES="E", SHAPE_ONLY="1", LANE_E_PIN_SITES="0",
                    LANE_E_SUBS_PER_SITE=subs, LANE_E_SITES_OVERRIDE="1",
                )
                self.assertEqual(result.returncode, 0, result.stderr)
                shape = (out / "results/nodes=7/laneE/shape.txt").read_text()
                self.assertNotIn("round-robin $share selection roughly (N-1)/N", shape)
                self.assertIn("prefer-local", shape)
                self.assertIn(expect, shape)
                self.assertIn("mqttd_publish_forwarded_total", shape)

    def test_multiple_subscriber_containers_do_not_imply_full_local_coverage(self):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({"brokers": [{}] * 5, "drivers": [{"vcpus": 8}] * 5}))
        out = self.root / "multi-sub-shape"
        result = self.run_script("run-curve.sh", str(out), str(inventory), LANES="E", SHAPE_ONLY="1",
                                 LANE_E_PIN_SITES="0", LANE_E_SUBS_PER_SITE="6",
                                 LANE_E_SUB_CONTAINERS_PER_SITE="3", LANE_E_SITES_OVERRIDE="1")
        self.assertEqual(result.returncode, 0, result.stderr)
        shape = (out / "results/nodes=5/laneE/shape.txt").read_text()
        self.assertIn("verify actual broker coverage", shape)
        self.assertNotIn("predicted crossing ≈ 0%", shape)
        self.assertEqual(self.calls(), [])

    def test_lane_e_pinned_shape_wording_unchanged(self):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({"brokers": [{}], "drivers": [{"vcpus": 8}, {"vcpus": 8}]}))
        result = self.run_script(
            "run-curve.sh", str(self.root / "shape-pin"), str(inventory),
            LANES="E", SHAPE_ONLY="1", LANE_E_PIN_SITES="1", LANE_E_SITES_OVERRIDE="1",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        shape = (self.root / "shape-pin/results/nodes=1/laneE/shape.txt").read_text()
        self.assertIn("site affinity: ON", shape)
        self.assertNotIn("prefer-local", shape)

    def test_lane_e_brokers_consumers_and_cpu_share_one_window(self):
        # Every remote command really runs, locally, against fake curl/docker/
        # mpstat; `sleep` returns at once. The log records what each host was
        # asked to do and when, so the ORDER of the rung is what is asserted.
        fake = self.bin_dir
        (fake / "ssh").write_text('''#!/usr/bin/env python3
import json, os, sys, time
args = sys.argv[1:]
i = next(k for k, a in enumerate(args) if a.startswith("root@"))
host, cmd = args[i][5:], " ".join(args[i + 1:])
if cmd == "bash -s":
    cmd = sys.stdin.read()
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"t": time.time_ns(), "host": host, "cmd": cmd}) + "\\n")
os.environ["FAKE_HOST"] = host
os.execvp("bash", ["bash", "-c", cmd])
''')
        tools = {
            "sleep": '''#!/usr/bin/env python3
import json, os, sys, time
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"t": time.time_ns(), "sleep": sys.argv[1]}) + "\\n")
''',
            "mpstat": '''#!/usr/bin/env python3
import json, os, signal, sys, time
def stop(*_):
    with open(os.environ["CALL_LOG"], "a") as f:
        f.write(json.dumps({"t": time.time_ns(), "host": os.environ["FAKE_HOST"], "cpu": "stop"}) + "\\n")
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
# `mpstat -P ALL <interval> <count>` is a timer that ends on its own.
count = int(sys.argv[4]) if len(sys.argv) > 4 else None
while count is None or count > 0:
    print(time.strftime("%H:%M:%S", time.gmtime()) + "  all  0 0 0 0 0 0 0 0 0 50.00", flush=True)
    count = None if count is None else count - 1
    time.sleep(0.05)
''',
            "curl": '''#!/usr/bin/env python3
import os, sys
url = sys.argv[-1]
state = os.path.join(os.environ["FAKE_STATE"], "pubs")
if url.endswith(":8080/metrics"):
    print("mqttd_connections_active %d" % (10**6 if os.path.exists(state) else 0))
    print("mqttd_publish_received_total 100")
elif "/metrics" in url:
    print("recv 100")
''',
            "docker": '''#!/usr/bin/env python3
import os, sys
state = os.path.join(os.environ["FAKE_STATE"], "pubs")
a = sys.argv[1:]
if a[:1] == ["run"] and "pub" in a:
    open(state, "w").close()
if a[:1] == ["rm"] and any(x.startswith("pub-") for x in a):
    if os.path.exists(state): os.unlink(state)
''',
        }
        for name, body in tools.items():
            (fake / name).write_text(body)
            (fake / name).chmod(0o755)
        (self.root / "state").mkdir()
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({
            "brokers": [{"public_ip": f"broker-{i}", "private_ip": f"10.0.0.{i + 1}"} for i in range(2)],
            "drivers": [{"public_ip": "driver-0", "vcpus": 8}],
        }))
        out = self.root / "e2e"
        result = subprocess.run(
            ["bash", str(self.rig / "run-curve.sh"), str(out), str(inventory)],
            env=self.env | {"LANES": "E", "LANE_E_SITES_OVERRIDE": "1", "LANE_E_CONTROL": "0",
                            "LANE_E_CALIBRATE": "0", "FAKE_STATE": str(self.root / "state")},
            capture_output=True, text=True, timeout=120,
        )
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        events = [json.loads(line) for line in self.log.read_text().splitlines()]
        events.sort(key=lambda e: e["t"])

        def when(pred, what, last=False):
            hits = [e["t"] for e in events if pred(e)]
            self.assertTrue(hits, what)
            return hits[-1] if last else hits[0]

        cmd = lambda e: e.get("cmd", "")  # noqa: E731
        pubs_up = when(lambda e: " pub -h " in cmd(e), "publishers started")
        settle = when(lambda e: e.get("sleep") == "20", "settle sleep")
        cpu_up = when(lambda e: "mpstat" in cmd(e), "samplers started", last=True)
        opened = [e for e in events if "WINDOW_STAMP_MS" in cmd(e)]
        self.assertEqual({e["host"] for e in opened}, {"broker-0", "broker-1", "driver-0"})
        window = when(lambda e: e.get("sleep") == "60", "window sleep")
        first_open = min(e["t"] for e in opened)
        last_open = max(e["t"] for e in opened if e["t"] < window)
        first_close = min(e["t"] for e in opened if e["t"] > window)
        cpu_down = [e for e in events if e.get("cpu") == "stop"]
        self.assertEqual(len(cpu_down), 3, "every host's sampler is stopped by the window, not a timer")
        stop_logs = when(lambda e: "docker logs pub-" in cmd(e), "steady logs dumped")
        before = [e["t"] for e in events if cmd(e) == "curl -s http://localhost:8080/metrics"]
        self.assertLess(pubs_up, settle)
        self.assertLess(settle, cpu_up)
        self.assertLess(cpu_up, first_open, "samplers cover the whole window")
        self.assertLess(last_open, window)
        self.assertEqual(sum(1 for e in opened if e["t"] < window), 3, "one open scrape per host, one batch")
        self.assertEqual(sum(1 for e in opened if e["t"] > window), 3, "one close scrape per host, one batch")
        self.assertLess(max(e["t"] for e in opened), min(e["t"] for e in cpu_down))
        self.assertLess(max(e["t"] for e in cpu_down), stop_logs)
        self.assertTrue(any(t < pubs_up for t in before), "lifetime before snapshot kept")
        self.assertTrue(any(t > first_close for t in before), "lifetime after snapshot kept")

        rdir = out / "results/nodes=2/laneE/sites-1"
        for label in ("window-open", "window-close", "before", "after"):
            for i in range(2):
                text = (rdir / f"metrics-{label}-broker{i}.prom").read_text()
                self.assertIn("mqttd_publish_received_total 100", text)
                self.assertNotIn("WINDOW_STAMP_MS", text)
        self.assertNotIn("WINDOW_STAMP_MS", (rdir / "sub-s0-0-base.prom").read_text())
        self.assertNotIn("WINDOW_STAMP_MS", (rdir / "sub-s0-0.prom").read_text())
        rows = [r.split("\t") for r in (rdir / "window.tsv").read_text().splitlines()[1:]]
        self.assertEqual(sorted((r[0], r[1]) for r in rows), sorted(
            (h, p) for h in ("broker0", "broker1", "driver0") for p in ("open", "close")))
        for host, phase, start, end in rows:
            self.assertTrue(start.isdigit() and end.isdigit() and int(start) <= int(end), (host, phase))
        rung = (rdir / "rung.txt").read_text()
        self.assertIn("window=aligned", rung)
        self.assertIn("cpu_window=aligned", rung)
        for host in ("broker0", "broker1", "driver0"):
            self.assertIn("CPU_STREAM_START_UTC", (rdir / f"cpu/cpu-{host}.txt").read_text())

    def test_missing_tofu_fails_even_when_terraform_exists(self):
        # Hermetic PATH: removing the stub must not reveal the host's real tofu.
        (self.bin_dir / "tofu").unlink()
        for name in ("bash", "dirname", "git", "sed", "python3"):
            target = shutil.which(name)
            self.assertIsNotNone(target, name)
            (self.bin_dir / name).symlink_to(target)
        self.env["PATH"] = str(self.bin_dir)
        for cloud in ("hcloud", "upcloud"):
            for script, args in (("run.sh", ("smoke",)), ("teardown.sh", ())):
                with self.subTest(cloud=cloud, script=script):
                    result = self.run_script(script, *args, CLOUD=cloud,
                                             HCLOUD_TOKEN="dummy", UPCLOUD_TOKEN="dummy")
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("OpenTofu (tofu) is required", result.stderr)
                    self.assertEqual(self.calls(), [])
        result = subprocess.run([str(self.bin_dir / "python3"),
                                 str(self.rig / "test-upcloud-quota.py")],
                                env=self.env, capture_output=True, text=True, timeout=15)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("OpenTofu (tofu) is required", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_upcloud_missing_state_and_force_fail_closed(self):
        for args, expected in (((), "state missing"), (("--force",), "does not support --force")):
            with self.subTest(args=args):
                result = self.run_script("teardown.sh", *args, CLOUD="upcloud", UPCLOUD_TOKEN="dummy")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)
                self.assertEqual(self.calls(), [])

    def test_lane_e_respects_inventory_cpu_budget(self):
        inventory = self.root / "inventory.json"
        for cores, success in ((None, True), (8, True), (4, False)):
            with self.subTest(cores=cores):
                driver = {} if cores is None else {"vcpus": cores}
                inventory.write_text(json.dumps({"brokers": [{}], "drivers": [driver, driver]}))
                result = self.run_script("run-curve.sh", str(self.root / "shape"), str(inventory),
                                         LANES="E", SHAPE_ONLY="1", LANE_E_SITES_OVERRIDE="4")
                self.assertEqual(result.returncode == 0, success, result.stderr)
                if not success:
                    self.assertIn("6 containers on the busiest driver > 4", result.stderr)
                self.assertEqual(self.calls(), [])


if __name__ == "__main__":
    unittest.main()
