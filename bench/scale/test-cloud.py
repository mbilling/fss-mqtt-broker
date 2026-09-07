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
        for name in ("lib.sh", "cloud.sh", "run.sh", "teardown.sh", "run-curve.sh", "test-upcloud-quota.py"):
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
