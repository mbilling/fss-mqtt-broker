#!/usr/bin/env python3
"""Offline provider selection/recovery tests: all cloud CLIs are stubs."""
import contextlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import tempfile
import time
import unittest

SCALE = Path(__file__).resolve().parent


class CloudTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.rig = self.root / "bench/scale"
        self.rig.mkdir(parents=True)
        for name in ("lib.sh", "cloud.sh", "run.sh", "teardown.sh", "run-curve.sh", "cpu.sh", "test-upcloud-quota.py", "482-smoke.sh", "482-constant-driver-N5-N7-optionB.env", "extract-lane-e.py", "compare-brokers.sh", "forward-canary.py"):
            shutil.copy2(SCALE / name, self.rig / name)
        # The real mqttd captures the ledgers and the extractor are tested against.
        shutil.copytree(SCALE / "testdata", self.rig / "testdata")
        for name in ("terraform", "terraform-upcloud"):
            (self.rig / name).mkdir()
        shutil.copytree(SCALE / "compare", self.rig / "compare")
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
            capture_output=True, text=True, timeout=60,
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
            capture_output=True, text=True, timeout=60,
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

    # ── Lane E end to end, against fakes that behave like the fleet ──────────
    # Every remote command really runs, locally, against fake curl/docker/
    # mpstat/systemctl; `sleep` returns at once. The log records what each host
    # was asked to do and when, so the ORDER of a size is what is asserted.
    FAKE_SSH = r'''#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys, time
args = sys.argv[1:]
i = next(k for k, a in enumerate(args) if a.startswith("root@"))
host, cmd = args[i][5:], " ".join(args[i + 1:])
if cmd == "bash -s":
    cmd = sys.stdin.read()
start = time.time_ns()
state = pathlib.Path(os.environ["FAKE_STATE"])
os.environ["FAKE_HOST"] = host
def log(**extra):
    with open(os.environ["CALL_LOG"], "a") as f:
        f.write(json.dumps({"t": start, "host": host, "cmd": cmd, **extra}) + "\n")
if "python3 - run" in cmd:
    # The canary arrives on stdin and answers with its '@@@' stream: replay a
    # real capture instead of dialling brokers that do not exist.
    sys.stdin.read()
    (state / "canary").touch()
    sys.stdout.write(pathlib.Path(os.environ["FAKE_CANARY_STREAM"]).read_text())
    log(end=time.time_ns())
    sys.exit(int(os.environ.get("FAKE_CANARY_RC", "0")))
if "mqttd_connections_active[ {]" in cmd:
    (state / "rung").touch()  # a rung's reset wait: everything after it is the rung's
if "mpstat" in cmd:
    # A sampler must BE the process cpu.sh kills, exactly as `exec ssh` is.
    log()
    os.execvp("bash", ["bash", "-c", cmd])
if "WINDOW_STAMP_MS" in cmd:
    with open(state / f"window-{host}", "ab") as f:
        f.write(b"x")
        edge = f.tell()
    if os.environ.get("FAKE_WINDOW_FAILS") == f"{host}:{edge}":
        sys.stderr.write(f"ssh: connect to host {host} port 22: Connection timed out\n")
        log(end=time.time_ns(), failed=True)
        sys.exit(255)
    # Long enough that scrapes taken one after another could never overlap.
    time.sleep(0.3)
rc = subprocess.run(["bash", "-c", cmd]).returncode
log(end=time.time_ns())
sys.exit(rc)
'''
    FAKE_TOOLS = {
        "sleep": r'''#!/usr/bin/env python3
import json, os, pathlib, signal, sys, time
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"t": time.time_ns(), "sleep": sys.argv[1]}) + "\n")
blocked = pathlib.Path(os.environ["FAKE_STATE"]) / "blocked"
if sys.argv[1] == os.environ.get("FAKE_BLOCK_SLEEP") and not blocked.exists():
    # Behave like coreutils sleep under Ctrl-C: die of the signal.
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    blocked.touch()
    time.sleep(120)
''',
        "mpstat": r'''#!/usr/bin/env python3
import json, os, signal, sys, time
def stop(*_):
    with open(os.environ["CALL_LOG"], "a") as f:
        f.write(json.dumps({"t": time.time_ns(), "host": os.environ["FAKE_HOST"], "cpu": "stop"}) + "\n")
    sys.exit(0)
signal.signal(signal.SIGTERM, stop)
dies = os.environ.get("FAKE_MPSTAT_DIES") == os.environ["FAKE_HOST"]
while True:
    print(time.strftime("%H:%M:%S", time.gmtime()) + "  all  0 0 0 0 0 0 0 0 0 50.00", flush=True)
    if dies:
        sys.exit(0)
    time.sleep(0.05)
''',
        "curl": r'''#!/usr/bin/env python3
import os, pathlib, sys
url = sys.argv[-1]
state = pathlib.Path(os.environ["FAKE_STATE"])
host = os.environ.get("FAKE_HOST", "")
if url.endswith(":8080/metrics"):
    # A live broker: every scrape sees its counters advance, a labelled family
    # exists only once it has a sample, and the exposition ends in '# EOF'.
    with open(state / f"scrapes-{host}", "ab") as f:
        f.write(b"x")
        n = f.tell()
    if (state / "canary").exists() and host == os.environ.get("FAKE_LOSE_CONTROL"):
        # Restarted since the control: serve the real first scrape of a respawned
        # mqttd — a whole exposition ending in '# EOF', with no publish family at all.
        sys.stdout.write(pathlib.Path(os.environ["FAKE_RESTART_SCRAPE"]).read_text())
        sys.exit(0)
    cut = False
    if (state / "canary").exists() and host == os.environ.get("FAKE_CUT_SCRAPES"):
        cut = True  # every scrape since the control ends mid-body
    if (state / "rung").exists() and host == os.environ.get("FAKE_LOSE_THEN_CUT") and "-m" in sys.argv:
        with open(state / f"lose-cut-{host}", "ab") as f:
            f.write(b"x")
            first = f.tell() == 1
        if first:
            sys.stdout.write(pathlib.Path(os.environ["FAKE_RESTART_SCRAPE"]).read_text())
            sys.exit(0)
        cut = True
    conns = 10**6 if (state / "pubs").exists() else 0
    lingering = 3 if (state / "canary").exists() and host == os.environ.get("FAKE_LINGER") else 0
    out = [
        "# HELP mqttd_connections_active Currently open client connections.",
        "# TYPE mqttd_connections_active gauge",
        f"mqttd_connections_active {conns}",
        "# TYPE mqttd_publish_received counter",
        f'mqttd_publish_received_total{{qos="0"}} {10**7 + 1000 * n}',
        "# TYPE mqttd_publish_delivered counter",
        f'mqttd_publish_delivered_total{{qos="0"}} {10**7 + 1000 * n}',
    ]
    if (state / "canary").exists():
        out += ["# TYPE mqttd_publish_forwarded counter",
                f'mqttd_publish_forwarded_total{{reason="shared-remote"}} {10**6 + n}']
    out += [
        "# TYPE mqttd_sessions gauge", f"mqttd_sessions {lingering}",
        "# TYPE mqttd_subscriptions gauge", "mqttd_subscriptions 0",
        "# TYPE mqttd_peer_links gauge", f"mqttd_peer_links {int(os.environ['FAKE_BROKERS']) - 1}",
        "# EOF",
    ]
    if cut:
        out = out[: len(out) // 2]
    print("\n".join(out))
elif url.endswith("/readyz"):
    print('{"ready":true}')
elif "/metrics" in url:
    print("recv 100")
''',
        "docker": r'''#!/usr/bin/env python3
import os, pathlib, sys
state = pathlib.Path(os.environ["FAKE_STATE"]) / "pubs"
a = sys.argv[1:]
if a[:1] == ["run"] and any(x.startswith("pub-") for x in a):
    state.touch()
if a[:1] == ["rm"] and any(x.startswith("pub-") for x in a) and state.exists():
    state.unlink()
if a[:2] == ["logs", "cal-pub"]:
    print("1s pub total=300000 rate=15000/sec")
    print("1s pub_succ total=300000 rate=15000/sec")
    print("1s pub_overrun total=0 rate=0/sec")
''',
        "systemctl": r'''#!/usr/bin/env python3
import os, sys
if sys.argv[1:] == ["show", "-p", "MainPID", "--value", "mqttd"]:
    host = os.environ.get("FAKE_HOST", "")
    fails = os.environ.get("FAKE_MAINPID_FAILS", "")  # "<host>:<times>"
    if fails.startswith(host + ":"):
        with open(os.path.join(os.environ["FAKE_STATE"], "mainpid-" + host), "ab") as f:
            f.write(b"x")
            n = f.tell()
        if n <= int(fails.rsplit(":", 1)[1]):
            sys.stderr.write(f"ssh: connect to host {host} port 22: Connection timed out\n")
            sys.exit(255)
    respawned = host == os.environ.get("FAKE_RESPAWN") and os.path.exists(os.path.join(os.environ["FAKE_STATE"], "rung"))
    print((5000 if respawned else 4000) + int(host.rsplit("-", 1)[1]) if host.startswith("broker-") else 0)
''',
    }
    CANARY_CHUNK = re.compile(r"metrics-(pre|post)-broker\d+\.prom|clients\.tsv|timeline\.tsv")

    def canary_fixture(self):
        """A real mqttd forward-canary capture that passes the rig's own verify.

        The canary module's N=2 capture is preferred; any other passing N>=2
        capture under testdata is accepted, and the fleet is sized to match it.
        """
        root = self.rig / "testdata"
        found = []
        for timeline in sorted(root.rglob("timeline.tsv")):
            d = timeline.parent
            fields = dict(line.split("\t", 1) for line in timeline.read_text().splitlines() if "\t" in line)
            if fields.get("status") != "complete" or not {"nodes", "count"} <= set(fields):
                continue
            nodes, count = int(fields["nodes"]), int(fields["count"])
            if nodes < 2:
                continue
            verdict = subprocess.run(
                ["python3", str(self.rig / "forward-canary.py"), "verify", str(d), "--nodes", str(nodes), "--count", str(count)],
                capture_output=True, text=True, timeout=30)
            if verdict.returncode == 0:
                preferred = d.relative_to(root).parts[0] == "forward-canary"
                found.append((not preferred, nodes != 2, str(d), nodes, count))
        self.assertTrue(found, "no passing real N>=2 forward-canary capture under bench/scale/testdata")
        _, _, d, nodes, count = sorted(found)[0]
        return Path(d), nodes, count

    def lane_e_fleet(self, mutate=None, local=False, **env):
        """Install the fakes; return (env, nodes, count) for a fleet matching the capture.

        local=True replays the real N=1 capture (a local pair, status=pass-local)."""
        for name, body in {"ssh": self.FAKE_SSH, **self.FAKE_TOOLS}.items():
            (self.bin_dir / name).write_text(body)
            (self.bin_dir / name).chmod(0o755)
        if local:
            fixture, nodes, count = self.rig / "testdata/forward-canary/n1", 1, 100
        else:
            fixture, nodes, count = self.canary_fixture()
        chunks = {p.name: p.read_text().rstrip("\n") + "\n"
                  for p in sorted(fixture.iterdir()) if self.CANARY_CHUNK.fullmatch(p.name)}
        if mutate:
            mutate(chunks)
        stream = self.root / "canary-stream.txt"
        stream.write_text("".join(f"\n@@@ {name}\n{text}" for name, text in chunks.items()))
        state = self.root / "state"
        state.mkdir()
        self.inventory = self.root / "inventory.json"
        self.inventory.write_text(json.dumps({
            "brokers": [{"public_ip": f"broker-{i}", "private_ip": f"10.0.0.{i + 1}"} for i in range(nodes)],
            "drivers": [{"public_ip": "driver-0", "vcpus": 8}],
        }))
        base = {"LANES": "E", "LANE_E_SITES_OVERRIDE": "1", "LANE_E_CONTROL": "0", "LANE_E_CALIBRATE": "0",
                "LANE_E_FORWARD_CANARY_COUNT": str(count), "FAKE_STATE": str(state),
                "FAKE_BROKERS": str(nodes), "FAKE_CANARY_STREAM": str(stream),
                "FAKE_RESTART_SCRAPE": str(self.rig / "testdata/lane-e/local-proof-n3/restart/metrics-restart-broker2.prom")}
        return self.env | base | env, nodes, count

    def run_lane_e(self, env):
        out = self.root / "e2e"
        result = subprocess.run(["bash", str(self.rig / "run-curve.sh"), str(out), str(self.inventory)],
                                env=env, capture_output=True, text=True, timeout=180)
        return result, out

    def events(self):
        return sorted((json.loads(line) for line in self.log.read_text().splitlines()), key=lambda e: e["t"])

    def test_lane_e_forward_canary_precedes_calibration_and_every_rung(self):
        env, nodes, count = self.lane_e_fleet(LANE_E_SITES_OVERRIDE="1 2", LANE_E_CONTROL="1", LANE_E_CALIBRATE="1")
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        events = self.events()
        canary = [e for e in events if "python3 - run" in e.get("cmd", "")]
        self.assertEqual(len(canary), 1, "one control per size")
        self.assertEqual(canary[0]["host"], "driver-0")
        for i in range(nodes):
            self.assertIn(f"--broker 10.0.0.{i + 1}:1883:8080", canary[0]["cmd"])
        self.assertIn(f"--count {count} --timeout 90", canary[0]["cmd"])
        calibration = [e["t"] for e in events if "--name cal-pub" in e.get("cmd", "")]
        rungs = [e["t"] for e in events if "--name sub-s0-0" in e.get("cmd", "") and "docker run" in e["cmd"]]
        self.assertTrue(calibration, "calibration ran")
        self.assertEqual(len(rungs), 3, "sites-1, sites-2 and the control each started consumers")
        self.assertLess(canary[0]["end"], min(calibration), "the control precedes calibration")
        self.assertLess(max(calibration), min(rungs), "and therefore every rung")
        laneE = out / f"results/nodes={nodes}/laneE"
        verdict = (laneE / "forward-canary.txt").read_text()
        self.assertTrue(verdict.startswith(f"status=pass nodes={nodes} count={count}"), verdict)
        self.assertIn("residue_status=clean", verdict)
        for i in range(nodes):
            self.assertEqual((laneE / f"forward-canary/mainpid-broker{i}.txt").read_text().strip(), str(4000 + i))
            self.assertTrue((laneE / f"forward-canary/metrics-post-broker{i}.prom").is_file())
        for rung in ("sites-1", "sites-2", "sites-1-rep2"):
            self.assertTrue((laneE / rung / "rung.txt").is_file(), rung)
        self.assertIn("forward canary: ON", (laneE / "shape.txt").read_text())

    def test_lane_e_failed_forward_canary_starts_no_load(self):
        def break_ledger(chunks):
            # One forward short on one broker: the canary still says "complete".
            post = chunks["metrics-post-broker0.prom"]
            value = re.search(r'(?m)^mqttd_publish_forwarded_total\{reason="shared-remote"\} (\S+)$', post)
            chunks["metrics-post-broker0.prom"] = post.replace(
                value.group(0), value.group(0).rsplit(" ", 1)[0] + f" {float(value.group(1)) - 1:g}")
        env, nodes, _ = self.lane_e_fleet(mutate=break_ledger, LANE_E_CALIBRATE="1")
        result, out = self.run_lane_e(env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("forwarding positive control FAILED", result.stderr)
        for e in self.events():
            for name in ("--name sub-", "--name pub-", "--name cal-pub"):
                self.assertNotIn(name, e.get("cmd", ""), "no load after a failed control")
        laneE = out / f"results/nodes={nodes}/laneE"
        self.assertTrue((laneE / "forward-canary.txt").read_text().startswith("status=fail"))
        for name in ("clients.tsv", "timeline.tsv", "metrics-pre-broker0.prom", "metrics-post-broker0.prom",
                     "run.stdout", "run.stderr", "mainpid-broker0.txt"):
            self.assertTrue((laneE / "forward-canary" / name).is_file(), name)
        self.assertFalse(list(laneE.glob("sites-*")), "no rung directory")

    def rung_snapshots(self, events, host):
        """A broker's snapshot_broker scrapes taken inside a rung (after its reset wait began)."""
        rung = min(e["t"] for e in events if "mqttd_connections_active[ {]" in e.get("cmd", ""))
        return [e for e in events if e.get("host") == host and e["t"] > rung
                and e.get("cmd") == "curl -s -m 10 http://localhost:8080/metrics"]

    def test_lane_e_transient_mainpid_failure_is_retried(self):
        # Mutation named: the old one-shot `rssh ... || true` left this broker's
        # mainpid file empty, and every rung of the size was then uncertifiable.
        env, nodes, _ = self.lane_e_fleet(FAKE_MAINPID_FAILS="broker-1:1")
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        laneE = out / f"results/nodes={nodes}/laneE"
        for i in range(nodes):
            self.assertEqual((laneE / f"forward-canary/mainpid-broker{i}.txt").read_text().strip(), str(4000 + i))
        asks = [e for e in self.events() if e.get("host") == "broker-1" and "MainPID" in e.get("cmd", "")]
        self.assertGreaterEqual(len(asks), 2, "the failed read was retried")
        self.assertTrue((laneE / "sites-1/rung.txt").is_file())

    def test_lane_e_unreadable_mainpid_starts_no_load(self):
        env, nodes, _ = self.lane_e_fleet(FAKE_MAINPID_FAILS="broker-1:99", LANE_E_CALIBRATE="1")
        result, out = self.run_lane_e(env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("could not read the MainPID of: broker1 (4 tries)", result.stderr)
        for e in self.events():
            for name in ("--name sub-", "--name pub-", "--name cal-pub"):
                self.assertNotIn(name, e.get("cmd", ""), "no load without a bound control")
        laneE = out / f"results/nodes={nodes}/laneE"
        self.assertFalse(list(laneE.glob("sites-*")), "no rung directory")
        self.assertIn("mainpid=unreadable broker1", (laneE / "forward-canary.txt").read_text())

    def test_lane_e_canary_verdict_needs_both_the_exit_code_and_the_status_line(self):
        # Two independent checks on `forward-canary.py verify`: a zero exit with the
        # wrong verdict line, and a pass line with a non-zero exit, must each stop the size.
        real = (self.rig / "forward-canary.py").read_text()
        for line, rc in (("status=pass nodes=9 count=100", 0), ("status=pass-local nodes={nodes} count=100", 0),
                         ("status=pass nodes={nodes} count=100", 1)):
            with self.subTest(line=line, rc=rc):
                for leftover in (self.root / "state", self.root / "e2e"):
                    shutil.rmtree(leftover, ignore_errors=True)
                with contextlib.suppress(FileNotFoundError):
                    self.log.unlink()
                (self.rig / "forward-canary.py").write_text(real)
                env, nodes, _ = self.lane_e_fleet()
                (self.rig / "forward-canary.py").write_text(
                    "import sys\nprint(%r)\nsys.exit(%d)\n" % (line.format(nodes=nodes), rc))
                result, out = self.run_lane_e(env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(f"forwarding positive control FAILED at N={nodes}", result.stderr)
                for e in self.events():
                    self.assertNotIn("--name sub-", e.get("cmd", ""), "no load after a refused verdict")
        (self.rig / "forward-canary.py").write_text(real)

    def test_lane_e_rung_dies_when_a_broker_lost_the_control(self):
        # Mutation named: judging "complete" by the received family as well as
        # '# EOF' reads this real restarted-broker scrape as incomplete, warns, and
        # starts the rung's containers.
        env, nodes, _ = self.lane_e_fleet(FAKE_LOSE_CONTROL="broker-1")
        result, out = self.run_lane_e(env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("broker1 no longer exposes the forwarding positive control", result.stderr)
        self.assertNotIn("still incomplete", result.stderr)
        events = self.events()
        for e in events:
            self.assertNotIn("--name sub-", e.get("cmd", ""), "no consumer started")
            self.assertNotIn("--name pub-", e.get("cmd", ""), "no publisher started")
        self.assertEqual(len(self.rung_snapshots(events, "broker-1")), 4, "one before-scrape and three retries")
        retries = [e for e in events if e.get("sleep") == "2" and e["t"] > self.rung_snapshots(events, "broker-1")[0]["t"]]
        self.assertGreaterEqual(len(retries), 3, "the retries are spaced")
        before = (out / f"results/nodes={nodes}/laneE/sites-1/metrics-before-broker1.prom").read_text()
        self.assertEqual(before, (self.rig / "testdata/lane-e/local-proof-n3/restart/metrics-restart-broker2.prom").read_text())
        self.assertNotIn("mqttd_publish_received_total", before)

    def test_lane_e_rung_guard_keeps_a_loss_that_later_scrapes_cannot_read(self):
        # Mutation named: keeping only the last try's state turns this complete
        # scrape without the series into "still incomplete", and the rung runs.
        env, nodes, _ = self.lane_e_fleet(FAKE_LOSE_THEN_CUT="broker-1")
        result, out = self.run_lane_e(env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("broker1 no longer exposes the forwarding positive control", result.stderr)
        self.assertNotIn("still incomplete", result.stderr)
        for e in self.events():
            self.assertNotIn("--name sub-", e.get("cmd", ""), "no consumer started")
        rdir = out / f"results/nodes={nodes}/laneE/sites-1"
        restart = (self.rig / "testdata/lane-e/local-proof-n3/restart/metrics-restart-broker2.prom").read_text()
        self.assertEqual((rdir / "metrics-before-broker1.prom").read_text(), restart, "the scrape that proved the loss is kept")
        self.assertFalse(list(rdir.glob(".guard-lost-*")))

    def test_lane_e_rung_guard_only_warns_on_an_incomplete_scrape(self):
        # Mutation named: `die` for the incomplete branch stops a healthy ladder here.
        env, nodes, _ = self.lane_e_fleet(FAKE_CUT_SCRAPES="broker-1")
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        self.assertIn("broker1's before-scrape for rung 1 is still incomplete after 3 retries — the rung runs", result.stderr)
        events = self.events()
        self.assertTrue(any("--name sub-" in e.get("cmd", "") for e in events), "the rung's consumers started")
        self.assertGreaterEqual(len(self.rung_snapshots(events, "broker-1")), 4, "one before-scrape and three retries")
        rdir = out / f"results/nodes={nodes}/laneE/sites-1"
        self.assertTrue((rdir / "rung.txt").is_file())
        # Lifetime snapshots are retried while incomplete, then kept as they are.
        after = [e for e in events if e.get("host") == "broker-1" and e.get("cmd") == "curl -s -m 10 http://localhost:8080/metrics"
                 and e["t"] > max(x["t"] for x in events if "--name sub-" in x.get("cmd", ""))]
        self.assertGreaterEqual(len(after), 3)

    def test_lane_e_rung_dies_when_a_broker_was_respawned_since_the_control(self):
        # The series is still there (the new process forwarded again); only the PID tells.
        env, nodes, _ = self.lane_e_fleet(FAKE_RESPAWN="broker-1")
        result, out = self.run_lane_e(env)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("broker1's mqttd is MainPID 5001 before rung 1, not 4001", result.stderr)
        for e in self.events():
            self.assertNotIn("--name sub-", e.get("cmd", ""), "no consumer started")
            self.assertNotIn("--name pub-", e.get("cmd", ""), "no publisher started")

    def test_lane_e_single_broker_canary_is_the_local_pass_and_has_no_guard(self):
        # Mutations named: demanding status=pass at N=1 kills every single-broker run
        # at the control; dropping the N>1 condition on the guard kills it at the rung.
        env, nodes, count = self.lane_e_fleet(local=True)
        self.assertEqual(nodes, 1)
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        laneE = out / "results/nodes=1/laneE"
        verdict = (laneE / "forward-canary.txt").read_text()
        self.assertTrue(verdict.startswith(f"status=pass-local nodes=1 count={count}"), verdict)
        self.assertEqual((laneE / "forward-canary/mainpid-broker0.txt").read_text().strip(), "4000")
        self.assertIn("forward canary: ON (local)", (laneE / "shape.txt").read_text())
        events = self.events()
        rung = min(e["t"] for e in events if "mqttd_connections_active[ {]" in e.get("cmd", ""))
        starts = min(e["t"] for e in events if "--name sub-" in e.get("cmd", ""))
        guard = [e for e in events if rung < e["t"] < starts and "MainPID" in e.get("cmd", "")]
        self.assertEqual(guard, [], "no rung guard at N=1")
        self.assertTrue((laneE / "sites-1/rung.txt").is_file())

    def test_lane_e_lingering_canary_residue_is_recorded_and_warned(self):
        env, nodes, _ = self.lane_e_fleet(FAKE_LINGER="broker-1")
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        verdict = (out / f"results/nodes={nodes}/laneE/forward-canary.txt").read_text()
        self.assertTrue(verdict.startswith(f"status=pass nodes={nodes} "), verdict)
        self.assertIn("residue_status=lingering", verdict)
        self.assertIn("residue_waited_s=60", verdict)
        self.assertRegex(verdict, r"(?m)^residue_elapsed_s=\d+$")
        self.assertIn("residue_broker1=connections_active:0/0,sessions:3/0,subscriptions:0/0", verdict)
        self.assertIn("residue_broker0=connections_active:0/0,sessions:0/0,subscriptions:0/0", verdict)
        self.assertIn("still reports more connections/sessions/subscriptions than before the canary", result.stderr)

    def test_lane_e_sigint_during_window_stops_the_run(self):
        env, nodes, _ = self.lane_e_fleet(LANE_E_SITES_OVERRIDE="1 2", FAKE_BLOCK_SLEEP="60")
        out = self.root / "e2e"
        proc = subprocess.Popen(["bash", str(self.rig / "run-curve.sh"), str(out), str(self.inventory)],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                start_new_session=True)
        try:
            blocked = Path(env["FAKE_STATE"]) / "blocked"
            deadline = time.monotonic() + 120
            while not blocked.exists():
                self.assertIsNone(proc.poll(), "run ended before its window")
                self.assertLess(time.monotonic(), deadline, "window never opened")
                time.sleep(0.05)
            os.killpg(proc.pid, signal.SIGINT)  # what Ctrl-C delivers: the whole foreground group
            _, stderr = proc.communicate(timeout=60)
            # Before the finally's SIGKILL, which would reap a sampler the harness left behind.
            samplers = Path(out / f"results/nodes={nodes}/laneE/sites-1/cpu/samplers.tsv").read_text().splitlines()
            def alive(pid):
                try:
                    os.kill(pid, 0)
                except ProcessLookupError:
                    return False
                return True
            pids = [int(line.split()[0]) for line in samplers]
            deadline = time.monotonic() + 5  # an exited sampler may await its reaper briefly
            while any(alive(pid) for pid in pids) and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertFalse([pid for pid in pids if alive(pid)], "sampler must be stopped by the harness")
        finally:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, signal.SIGKILL)
        self.assertNotEqual(proc.returncode, 0, stderr[-3000:])
        self.assertIn("rung 1 interrupted", stderr)
        self.assertNotIn("CPU samplers failed to start", stderr)
        self.assertNotIn("partial CPU coverage", stderr)
        self.assertEqual(sum(1 for e in self.events() if e.get("sleep") == "60"), 1, "the window is not re-run")
        laneE = out / f"results/nodes={nodes}/laneE"
        self.assertFalse((laneE / "sites-2").exists(), "the ladder stops")

    def test_lane_e_degraded_window_is_recorded_and_says_why(self):
        # Mutation named: without the `.batch/window-ran` marker (or with every
        # sampler failure treated as a start failure) this rung is re-measured as
        # cpu_window=missing — two open and two close batches.
        env, nodes, _ = self.lane_e_fleet(FAKE_MPSTAT_DIES="broker-1", FAKE_WINDOW_FAILS="broker-0:2")
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        rdir = out / f"results/nodes={nodes}/laneE/sites-1"
        self.assertIn("cpu_window=incomplete", (rdir / "rung.txt").read_text())
        self.assertIn("continues", result.stderr)
        windows = [e for e in self.events() if "WINDOW_STAMP_MS" in e.get("cmd", "")]
        self.assertEqual(len(windows), 2 * (nodes + 1), "the window ran once")
        self.assertIn("close broker0: ssh: connect to host broker-0", (rdir / "window-ssh.log").read_text())
        self.assertIn("window close scrape on broker0 never started", result.stderr)
        self.assertIn("window close scrape of broker0 is incomplete", result.stderr)
        self.assertNotIn("open broker", (rdir / "window-ssh.log").read_text())

    def test_lane_e_brokers_consumers_and_cpu_share_one_window(self):
        env, nodes, _ = self.lane_e_fleet()
        result, out = self.run_lane_e(env)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        events = self.events()

        def when(pred, what, last=False):
            hits = [e["t"] for e in events if pred(e)]
            self.assertTrue(hits, what)
            return hits[-1] if last else hits[0]

        cmd = lambda e: e.get("cmd", "")  # noqa: E731
        brokers = {f"broker-{i}" for i in range(nodes)}
        pubs_up = when(lambda e: " pub -h " in cmd(e), "publishers started")
        settle = when(lambda e: e.get("sleep") == "20", "settle sleep")
        cpu_up = when(lambda e: "mpstat" in cmd(e), "samplers started", last=True)
        window = when(lambda e: e.get("sleep") == "60", "window sleep")
        scrapes = [e for e in events if "WINDOW_STAMP_MS" in cmd(e)]
        opens = [e for e in scrapes if e["t"] < window]
        closes = [e for e in scrapes if e["t"] > window]
        self.assertEqual(len(opens) + len(closes), len(scrapes))
        self.assertEqual({e["host"] for e in opens}, brokers | {"driver-0"}, "one open scrape per host")
        self.assertEqual({e["host"] for e in closes}, brokers | {"driver-0"}, "one close scrape per host")
        self.assertEqual(len(opens), nodes + 1)
        self.assertEqual(len(closes), nodes + 1)
        # The edges are one parallel batch each: every broker scrape and every
        # consumer scrape of an edge is in flight at the same instant, and the
        # window sleeps only after the whole open batch has come back.
        for edge in (opens, closes):
            self.assertLess(max(e["t"] for e in edge), min(e["end"] for e in edge), "scrapes overlap")
        self.assertLess(max(e["end"] for e in opens), window)
        for e in opens:
            if e["host"] in brokers:
                self.assertIn("WINDOW_MAINPID", cmd(e))
                self.assertIn("curl -s -m 10 http://localhost:8080/metrics", cmd(e))
        cpu_down = [e for e in events if e.get("cpu") == "stop"]
        self.assertEqual(len(cpu_down), nodes + 1, "every host's sampler is stopped by the window, not a timer")
        stop_logs = when(lambda e: "docker logs pub-" in cmd(e), "steady logs dumped")
        before = [e["t"] for i in range(nodes) for e in self.rung_snapshots(events, f"broker-{i}")]
        canary = when(lambda e: "python3 - run" in cmd(e), "forwarding control ran")
        self.assertLess(canary, min(before), "the control's floors predate every rung snapshot")
        self.assertEqual(len(before), 3 * nodes, "before, drain and after, once per broker")
        self.assertLess(pubs_up, settle)
        self.assertLess(settle, cpu_up)
        self.assertLess(cpu_up, min(e["t"] for e in opens), "samplers cover the whole window")
        self.assertLess(max(e["end"] for e in scrapes), min(e["t"] for e in cpu_down))
        self.assertLess(max(e["t"] for e in cpu_down), stop_logs)
        self.assertTrue(any(t < pubs_up for t in before), "lifetime before snapshot kept")
        self.assertTrue(any(t > min(e["t"] for e in closes) for t in before), "lifetime after snapshot kept")

        rdir = out / f"results/nodes={nodes}/laneE/sites-1"
        for label in ("window-open", "window-close", "before", "drain", "after"):
            for i in range(nodes):
                text = (rdir / f"metrics-{label}-broker{i}.prom").read_text()
                self.assertTrue(text.rstrip().endswith("# EOF"), (label, i))
                self.assertIn('mqttd_publish_forwarded_total{reason="shared-remote"}', text)
                self.assertNotIn("WINDOW_", text)
        self.assertNotIn("WINDOW_", (rdir / "sub-s0-0-base.prom").read_text())
        self.assertNotIn("WINDOW_", (rdir / "sub-s0-0.prom").read_text())
        lines = (rdir / "window.tsv").read_text().splitlines()
        self.assertEqual(lines[0].split("\t"), ["host", "phase", "start_ms", "end_ms", "main_pid"])
        rows = {(r[0], r[1]): r[2:] for r in (line.split("\t") for line in lines[1:])}
        hosts = [f"broker{i}" for i in range(nodes)] + ["driver0"]
        self.assertEqual(set(rows), {(h, p) for h in hosts for p in ("open", "close")})
        for host in hosts:
            for phase in ("open", "close"):
                start, end, _ = rows[host, phase]
                self.assertTrue(start.isdigit() and end.isdigit() and int(start) <= int(end), (host, phase))
            self.assertGreater(int(rows[host, "close"][0]), int(rows[host, "open"][1]), host)
            want = str(4000 + int(host[len("broker"):])) if host.startswith("broker") else ""
            self.assertEqual((rows[host, "open"][2], rows[host, "close"][2]), (want, want), host)
        self.assertFalse((rdir / "window-ssh.log").exists(), "a clean window logs no ssh errors")
        rung = (rdir / "rung.txt").read_text()
        self.assertIn("window=aligned", rung)
        self.assertIn("cpu_window=aligned", rung)
        for host in hosts:
            self.assertIn("CPU_STREAM_START_UTC", (rdir / f"cpu/cpu-{host}.txt").read_text())
        # The extractor certifies this rung from what the harness left behind.
        extracted = subprocess.run(["python3", str(self.rig / "extract-lane-e.py"), str(out)],
                                   env=self.env, capture_output=True, text=True, timeout=60)
        self.assertEqual(extracted.returncode, 0, extracted.stderr + extracted.stdout)
        header, row = extracted.stdout.splitlines()[:2]
        starts = [m.start() for m in re.finditer(r"\S+", header)] + [None]
        report = {header[a:b].strip(): row[a:b].strip() for a, b in zip(starts, starts[1:])}
        self.assertEqual((report["nodes"], report["sites"]), (str(nodes), "1"), extracted.stdout)
        self.assertEqual(report["window"], "aligned", extracted.stdout)
        self.assertEqual(report["cert"], "canary", extracted.stdout)

    def test_lane_e_forward_canary_knobs_refused_before_any_ssh(self):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({"brokers": [{}] * 5, "drivers": [{"vcpus": 8}] * 5}))
        for env, diagnostic in (
            ({"LANE_E_FORWARD_CANARY": "2"}, "LANE_E_FORWARD_CANARY must be 0 or 1"),
            ({"LANE_E_FORWARD_CANARY_COUNT": "0"}, "LANE_E_FORWARD_CANARY_COUNT must be a positive integer"),
            ({"LANE_E_FORWARD_CANARY_TIMEOUT": "90s"}, "LANE_E_FORWARD_CANARY_TIMEOUT must be a positive integer"),
        ):
            with self.subTest(env=env):
                result = self.run_script("run-curve.sh", str(self.root / "bad"), str(inventory),
                                         LANES="E", SHAPE_ONLY="1", LANE_E_SITES_OVERRIDE="1", **env)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(diagnostic, result.stderr)
                self.assertEqual(self.calls(), [])
        for brokers, env, expect in (
            (5, {}, "forward canary: ON — before calibration, 100 QoS 0 msgs over each of 20 directed broker pairs"),
            (5, {"LANE_E_FORWARD_CANARY": "0"}, "forward canary: OFF"),
            (1, {"LANE_E_FORWARD_CANARY_COUNT": "7"}, "forward canary: ON (local) — 7 QoS 0 msgs over one local pair (timeout 90s)"),
        ):
            with self.subTest(brokers=brokers, env=env):
                inventory.write_text(json.dumps({"brokers": [{}] * brokers, "drivers": [{"vcpus": 8}] * 5}))
                out = self.root / "shape-canary"
                shutil.rmtree(out, ignore_errors=True)
                result = self.run_script("run-curve.sh", str(out), str(inventory), LANES="E", SHAPE_ONLY="1",
                                         LANE_E_SITES_OVERRIDE="1", **env)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertIn(expect, (out / f"results/nodes={brokers}/laneE/shape.txt").read_text())
                self.assertEqual(self.calls(), [])

    def test_broken_local_checks_stop_run_before_any_cloud_call(self):
        for name in ("forward-canary.py", "extract-lane-e.py"):
            with self.subTest(name=name):
                original = (SCALE / name).read_text()
                (self.rig / name).write_text("import sys\nsys.exit(1 if '--self-test' in sys.argv else 0)\n")
                try:
                    for env in ({"PREFLIGHT_ONLY": "1"}, {"HCLOUD_TOKEN": "dummy"}):
                        result = self.run_script("run.sh", "smoke", **env)
                        self.assertNotEqual(result.returncode, 0)
                        self.assertIn(f"{name} --self-test failed", result.stderr)
                        self.assertEqual(self.calls(), [])
                finally:
                    (self.rig / name).write_text(original)

    def test_compare_runs_every_broker_on_one_host_in_order(self):
        # The comparison's value is that the hardware never changes, so the
        # ordering is the thing to pin: each broker starts, ladders, stops, and
        # the host reboots before the next one; the first broker repeats last as
        # the control that says whether the sequence drifted.
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
state = os.environ["FAKE_STATE"]
if "random/boot_id" in cmd:
    boot = os.path.join(state, "boot")
    print(open(boot).read().strip() if os.path.exists(boot) else "boot-0")
    sys.exit(0)
if cmd.strip() == "reboot":
    boot = os.path.join(state, "boot")
    n = int(open(boot).read().strip().split("-")[1]) + 1 if os.path.exists(boot) else 1
    open(boot, "w").write("boot-%d" % n)
    sys.exit(0)
if "/dev/tcp/" in cmd:
    sys.exit(0)
if "docker stats" in cmd:
    print("compare-broker 512MiB / 16GiB 42.00%")
    sys.exit(0)
if "curl -s http://localhost:94" in cmd or "@@@" in cmd:
    # subscriber scrape batch: one chunk per named container
    import re
    for name in re.findall(r"@@@ (\\S+)", cmd):
        print("\\n@@@ %s" % name)
        print("recv 1000")
        print("connect_succ 600")
        print('e2e_latency_bucket{le="5"} 1000')
        print('e2e_latency_bucket{le="+Inf"} 1000')
    sys.exit(0)
if "docker logs" in cmd:
    import re
    for name in re.findall(r"@@@ (\\S+)", cmd):
        print("\\n@@@ %s" % name)
        print("pub total=100000 rate=15000/sec" if name.startswith("pub") else "recv total=100000 rate=15000/sec")
    sys.exit(0)
if "mpstat" in cmd:
    print("CPU_STREAM_START_UTC 2026-09-16T00:00:00Z", flush=True)
    while True:
        print("00:00:01  all  1 0 1 0 0 0 0 0 0 98", flush=True)
        time.sleep(0.05)
sys.exit(0)
''')
        (fake / "scp").write_text("#!/usr/bin/env python3\nimport sys\nsys.exit(0)\n")
        for t in ("ssh", "scp"):
            (fake / t).chmod(0o755)
        (fake / "sleep").write_text('''#!/usr/bin/env python3
import json, os, sys, time
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"t": time.time_ns(), "sleep": sys.argv[1]}) + "\\n")
''')
        (fake / "sleep").chmod(0o755)
        (self.root / "cstate").mkdir()
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({
            "brokers": [{"name": "mqttd-1", "public_ip": "broker-0", "private_ip": "10.99.1.11", "server_type": "ccx23"}],
            "drivers": [{"name": f"bench-driver-{i}", "public_ip": f"driver-{i}", "private_ip": f"10.99.1.2{i}",
                         "server_type": "ccx43"} for i in range(1, 3)],
        }))
        out = self.root / "cmp"
        result = subprocess.run(
            ["bash", str(self.rig / "compare-brokers.sh"), str(out), str(inventory)],
            env=self.env | {"FAKE_STATE": str(self.root / "cstate"), "RUN": str(out),
                            "COMPARE_BROKERS": "mqttd mosquitto", "COMPARE_RATES": "30000",
                            "COMPARE_SECS": "1", "COMPARE_SETTLE": "1", "COMPARE_DRAIN_SECS": "5",
                            "COMPARE_DRAIN_POLL": "1", "COMPARE_FLAT_POLLS": "1"},
            capture_output=True, text=True, timeout=180)
        self.assertEqual(result.returncode, 0, result.stderr[-3000:])
        events = sorted((json.loads(l) for l in self.log.read_text().splitlines()), key=lambda e: e["t"])
        cmd = lambda e: e.get("cmd", "")  # noqa: E731
        starts = [e["t"] for e in events if "--name compare-broker" in cmd(e)]
        images = [cmd(e) for e in events if "--name compare-broker" in cmd(e)]
        reboots = [e["t"] for e in events if cmd(e).strip() == "reboot"]
        subs = [e["t"] for e in events if " sub -h " in cmd(e)]
        pubs = [e["t"] for e in events if " pub -h " in cmd(e)]
        self.assertEqual(len(starts), 3, "two brokers plus the control arm")
        self.assertIn("fss-mqtt-broker", images[0])
        self.assertIn("eclipse-mosquitto", images[1])
        self.assertIn("fss-mqtt-broker", images[2], "the control repeats the first broker")
        self.assertEqual(len(reboots), 2, "the host reboots between arms, not after the last one")
        for k in range(2):
            self.assertLess(starts[k], reboots[k])
            self.assertLess(reboots[k], starts[k + 1])
        self.assertTrue(all(subs[k] < pubs[k] for k in range(min(len(subs), len(pubs)))),
                        "subscribers connect before publishers")
        arms = sorted(d.name for d in (out / "results/compare").iterdir() if d.is_dir())
        self.assertEqual(arms, ["1-mqttd", "2-mosquitto", "3-mqttd-control"])
        rung = (out / "results/compare/1-mqttd/rung-30000/rung.txt").read_text()
        self.assertIn("broker=mqttd offered=30000", rung)
        self.assertIn("drained=yes", rung)
        broker_txt = (out / "results/compare/3-mqttd-control/broker.txt").read_text()
        self.assertIn("control=yes", broker_txt)
        self.assertIn("image=ghcr.io/mbilling/fss-mqtt-broker@sha256:", broker_txt)

    def test_compare_refuses_a_ladder_that_outruns_the_drivers(self):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({
            "brokers": [{"public_ip": "b", "private_ip": "10.99.1.11", "server_type": "ccx23"}],
            "drivers": [{"public_ip": "d", "private_ip": "10.99.1.21", "server_type": "ccx33"}],
        }))
        result = subprocess.run(
            ["bash", str(self.rig / "compare-brokers.sh"), str(self.root / "cmp2"), str(inventory)],
            env=self.env | {"COMPARE_SHAPE_ONLY": "1", "COMPARE_RATES": "30000 240000"},
            capture_output=True, text=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("more containers per driver than 8", result.stderr)
        self.assertEqual(self.calls(), [])

    def test_compare_refuses_a_rate_that_is_not_a_whole_number_of_containers(self):
        inventory = self.root / "inventory.json"
        inventory.write_text(json.dumps({
            "brokers": [{"public_ip": "b", "private_ip": "10.99.1.11", "server_type": "ccx23"}],
            "drivers": [{"public_ip": "d", "private_ip": "10.99.1.21", "server_type": "ccx43"}],
        }))
        result = subprocess.run(
            ["bash", str(self.rig / "compare-brokers.sh"), str(self.root / "cmp3"), str(inventory)],
            env=self.env | {"COMPARE_SHAPE_ONLY": "1", "COMPARE_RATES": "31000"},
            capture_output=True, text=True, timeout=60)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not a multiple of 15000", result.stderr)
        self.assertEqual(self.calls(), [])

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

    def test_lane_e_driver_budget_follows_the_live_hetzner_inventory(self):
        # The live Hetzner inventory names server_type and omits vcpus. 8 x CCX43 at
        # 28 sites is 12 containers on the busiest 16-vCPU driver: allowed. The
        # same shape on 8-vCPU CCX33s, or with no type at all (legacy 8), is not.
        inventory = self.root / "inventory.json"
        for server_type, sites, success, refusal in (
            ("ccx43", "28", True, None),
            ("ccx33", "28", False, "12 containers on the busiest driver > 8"),
            (None, "28", False, "12 containers on the busiest driver > 8"),
            ("cpx32", "4", False, "12 containers on the busiest driver > 4"),
        ):
            with self.subTest(server_type=server_type, sites=sites):
                driver = {"name": "bench-driver", "public_ip": "192.0.2.1", "private_ip": "10.99.1.21"}
                if server_type:
                    driver["server_type"] = server_type
                brokers = [{"name": f"mqttd-{i}", "server_type": "ccx23"} for i in range(10)]
                drivers = [dict(driver) for _ in range(8 if sites == "28" else 1)]
                inventory.write_text(json.dumps({"brokers": brokers, "drivers": drivers}))
                result = self.run_script("run-curve.sh", str(self.root / "budget"), str(inventory),
                                         LANES="E", SHAPE_ONLY="1", LANE_E_SITES_OVERRIDE=sites,
                                         LANE_E_SUBS_PER_SITE="10")
                self.assertEqual(result.returncode == 0, success, result.stderr)
                if refusal:
                    self.assertIn(refusal, result.stderr)
                self.assertEqual(self.calls(), [])

    def test_preflight_shape_inventory_uses_the_live_driver_schema(self):
        # run.sh's offline shape check must take the same budget path as the paid
        # run: for Hetzner that is server_type, not a precomputed vcpus.
        for extra, expected in (({}, {"server_type": "ccx43"}), ({"DRIVER_VCPUS": "12"}, {"vcpus": 12})):
            with self.subTest(extra=extra):
                run_dir = self.root / f"preflight-{len(extra)}"
                result = self.run_script("run.sh", "full", "10", CLOUD="hcloud", PREFLIGHT_ONLY="1",
                                         RUN_DIR=str(run_dir), DRIVER_COUNT="8", DRIVER_TYPE="ccx43",
                                         LANES="E", LANE_E_SITES_OVERRIDE="28", LANE_E_SUBS_PER_SITE="10",
                                         **extra)
                shape_inventory = json.loads((run_dir / "shape-inventory-10.json").read_text())
                self.assertEqual(shape_inventory["drivers"][0], expected)
                self.assertEqual(result.returncode, 0, result.stderr[-2000:])
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
