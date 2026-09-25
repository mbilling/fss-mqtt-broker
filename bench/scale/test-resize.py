#!/usr/bin/env python3
"""Offline tests for the one-provisioning size A/B: resize-cluster.sh and the
482-per-node-knee.sh campaign that drives it. ssh and every rig step are stubs;
no cloud call is possible."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCALE = Path(__file__).resolve().parent

# Logs every call, and fails on request: FAIL_ON=<substring of the joined argv>.
STUB = '''#!/usr/bin/env python3
import json, os, sys
argv = [os.path.basename(sys.argv[0])] + sys.argv[1:]
env = {k: os.environ.get(k, "") for k in ("LANE_E_SITES_OVERRIDE", "KEEP_INFRA", "RUN_DIR", "PREFLIGHT_ONLY")}
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"argv": argv, "env": env}) + "\\n")
fail_on = os.environ.get("FAIL_ON")
if fail_on and fail_on in " ".join(argv):
    sys.exit(3)
# run.sh's paid path leaves the provisioning's inventory behind, as the real one does.
if argv[0] == "run.sh" and env["PREFLIGHT_ONLY"] != "1":
    run = env["RUN_DIR"]
    os.makedirs(run, exist_ok=True)
    with open(os.environ["FULL_INVENTORY"]) as src, open(os.path.join(run, f"inventory-{argv[-1]}.json"), "w") as dst:
        dst.write(src.read())
'''


def inventory(brokers=7, drivers=10):
    return {
        "brokers": [{"node_id": f"mqttd-{i + 1}", "name": f"mqttd-{i + 1}",
                     "public_ip": f"198.51.100.{i + 1}", "private_ip": f"10.99.1.{11 + i}",
                     "server_type": "ccx23"} for i in range(brokers)],
        "drivers": [{"name": f"driver-{i + 1}", "public_ip": f"203.0.113.{i + 1}",
                     "private_ip": f"10.99.2.{11 + i}", "server_type": "ccx33"} for i in range(drivers)],
        "location": "fsn1", "mqttd_version": "test", "node_count": brokers, "run_label": "x",
    }


class Rig(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.rig = self.root / "bench/scale"
        self.rig.mkdir(parents=True)
        for name in ("lib.sh", "resize-cluster.sh", "482-per-node-knee.sh", "482-per-node-knee.env"):
            shutil.copy2(SCALE / name, self.rig / name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.log = self.root / "calls.jsonl"
        stub = self.bin / "ssh"
        stub.write_text(STUB)
        stub.chmod(0o755)
        self.provision = self.root / "prov"
        self.provision.mkdir()
        self.full = self.provision / "inventory-7.json"
        self.full.write_text(json.dumps(inventory()))
        (self.provision / "known_hosts").write_text("198.51.100.1 ssh-ed25519 AAAA\n")
        self.env = {"PATH": f"{self.bin}:{os.environ['PATH']}", "HOME": str(self.root),
                    "CALL_LOG": str(self.log), "FULL_INVENTORY": str(self.full)}

    def calls(self):
        if not self.log.exists():
            return []
        return [json.loads(line) for line in self.log.read_text().splitlines()]

    def run_script(self, *args, env=None):
        return subprocess.run(["bash", *map(str, args)], cwd=self.rig, text=True,
                              capture_output=True, env=self.env | (env or {}))


class ResizeTests(Rig):
    def resize(self, size, new_run, inv=None, env=None):
        return self.run_script(self.rig / "resize-cluster.sh", inv or self.full, size, new_run, env=env)

    def test_shrinks_to_a_prefix_and_stops_every_broker(self):
        out = self.root / "arm2"
        r = self.resize(5, out)
        self.assertEqual(r.returncode, 0, r.stderr)
        got = json.loads((out / "inventory-5.json").read_text())
        full = inventory()
        self.assertEqual(got["brokers"], full["brokers"][:5])
        self.assertEqual(got["drivers"], full["drivers"])
        self.assertEqual((got["node_count"], got["resized_from"]), (5, 7))
        self.assertEqual(got["resized_inventory"], str(self.full))
        # Every broker of the provisioning, left-out ones included, is stopped and
        # cleared — and only brokers: drivers are never touched.
        hosts = [c["argv"][-2] for c in self.calls()]
        self.assertEqual(hosts, [f"root@198.51.100.{i + 1}" for i in range(7)])
        for c in self.calls():
            self.assertIn("systemctl stop mqttd", c["argv"][-1])
            self.assertIn("rm -rf /var/lib/mqttd/*", c["argv"][-1])
            self.assertIn(f"UserKnownHostsFile={self.provision}/known_hosts", c["argv"])
        stamp = (out / "RESIZED.txt").read_text()
        self.assertIn("size=5\nprovisioned=7\n", stamp)
        self.assertIn("left_out=mqttd-6,mqttd-7", stamp)
        self.assertEqual((out / "known_hosts").read_text(), (self.provision / "known_hosts").read_text())

    def test_the_full_size_is_the_way_back(self):
        r = self.resize(7, self.root / "arm3")
        self.assertEqual(r.returncode, 0, r.stderr)
        got = json.loads((self.root / "arm3/inventory-7.json").read_text())
        self.assertEqual(got["brokers"], inventory()["brokers"])
        self.assertIn("left_out=\n", (self.root / "arm3/RESIZED.txt").read_text())

    def test_refuses_what_it_cannot_honour(self):
        cases = [
            ((8, self.root / "a"), "exceeds the 7 brokers"),
            ((0, self.root / "a"), "positive integer"),
            (("05", self.root / "a"), "positive integer"),
            (("five", self.root / "a"), "positive integer"),
            ((5, self.provision), "must not be the provisioning run dir"),
        ]
        for (size, out), message in cases:
            with self.subTest(size=size, out=out):
                r = self.resize(size, out)
                self.assertNotEqual(r.returncode, 0)
                self.assertIn(message, r.stderr)
        self.assertEqual(self.calls(), [], "a refused resize must not touch a host")

    def test_a_resized_inventory_is_not_a_provisioning(self):
        self.assertEqual(self.resize(5, self.root / "arm2").returncode, 0)
        before = len(self.calls())
        r = self.resize(3, self.root / "arm3", inv=self.root / "arm2/inventory-5.json")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("already a resized inventory", r.stderr)
        self.assertEqual(len(self.calls()), before)

    def test_one_arm_per_run_dir(self):
        self.assertEqual(self.resize(5, self.root / "arm2").returncode, 0)
        r = self.resize(5, self.root / "arm2")
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("one arm per run dir", r.stderr)

    def test_fails_closed_when_a_left_out_broker_cannot_be_stopped(self):
        r = self.resize(5, self.root / "arm2", env={"FAIL_ON": "root@198.51.100.7"})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("broker 6 (198.51.100.7)", r.stderr)
        self.assertFalse((self.root / "arm2/inventory-5.json").exists(),
                         "no inventory may be written for a provisioning in an unknown state")


class CampaignTests(Rig):
    def setUp(self):
        super().setUp()
        for name in ("run.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh",
                     "teardown.sh", "observe.sh", "resize-cluster.sh"):
            path = self.rig / name
            path.write_text(STUB)
            path.chmod(0o755)
        env = subprocess.run(["bash", "-c", f"set -a; . {self.rig}/482-per-node-knee.env; env -0"],
                             capture_output=True, text=True, check=True, env={"PATH": os.environ["PATH"]})
        knobs = dict(kv.split("=", 1) for kv in env.stdout.split("\0") if kv and "=" in kv)
        self.env |= {k: v for k, v in knobs.items() if k.startswith(("KNEE_", "LANE", "DRIVER", "OBSERVE"))}
        self.env["OBSERVE"] = "0"
        # The stubs log under their own names; ssh is not expected here.
        for name in ("run.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh", "teardown.sh",
                     "observe.sh", "resize-cluster.sh"):
            text = (self.rig / name).read_text().replace(
                "os.path.basename(sys.argv[0])", repr(name))
            (self.rig / name).write_text(text)

    def campaign(self, env=None):
        return self.run_script(self.rig / "482-per-node-knee.sh", env=env)

    def steps(self):
        return [c["argv"][0] for c in self.calls()]

    def test_preflight_checks_all_three_arms_offline(self):
        r = self.campaign(env={"PREFLIGHT_ONLY": "1"})
        self.assertEqual(r.returncode, 0, r.stderr)
        calls = self.calls()
        self.assertEqual([c["argv"] for c in calls], [["run.sh", "full", "7"], ["run.sh", "full", "5"],
                                                     ["run.sh", "full", "7"]])
        self.assertEqual([c["env"]["LANE_E_SITES_OVERRIDE"] for c in calls],
                         [self.env["KNEE_LADDER_FULL"], self.env["KNEE_LADDER_SMALL"], self.env["KNEE_LADDER_CLOSE"]])
        self.assertEqual(len({c["env"]["RUN_DIR"] for c in calls}), 3, "each preflight needs its own scratch dir")
        self.assertNotIn("teardown.sh", self.steps())

    def test_the_three_arms_share_one_provisioning_and_it_is_destroyed(self):
        r = self.campaign()
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.steps(), [
            "run.sh",
            "resize-cluster.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh",
            "resize-cluster.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh",
            "teardown.sh",
        ])
        calls = self.calls()
        first = calls[0]
        self.assertEqual(first["argv"], ["run.sh", "full", "7"])
        self.assertEqual(first["env"]["KEEP_INFRA"], "1")
        self.assertEqual(first["env"]["LANE_E_SITES_OVERRIDE"], self.env["KNEE_LADDER_FULL"])
        full = Path(first["env"]["RUN_DIR"]) / "inventory-7.json"
        resizes = [c["argv"] for c in calls if c["argv"][0] == "resize-cluster.sh"]
        # Both resizes start from the provisioning's own inventory, never a prefix.
        self.assertEqual([(a[1], a[2]) for a in resizes], [(str(full), "5"), (str(full), "7")])
        ladders = [c["env"]["LANE_E_SITES_OVERRIDE"] for c in calls if c["argv"][0] == "run-curve.sh"]
        self.assertEqual(ladders, [self.env["KNEE_LADDER_SMALL"], self.env["KNEE_LADDER_CLOSE"]])
        boots = [c["argv"] for c in calls if c["argv"][0] == "bootstrap-cluster.sh"]
        self.assertEqual([b[-1] for b in boots], ["durable", "durable"], "same mode run.sh gives arm 1")
        self.assertTrue(boots[0][2].endswith("2-n5/inventory-5.json"))
        self.assertTrue(boots[1][2].endswith("3-n7-close/inventory-7.json"))

    def test_a_failed_arm_keeps_its_evidence_and_still_destroys(self):
        r = self.campaign(env={"FAIL_ON": "run-curve.sh"})
        self.assertNotEqual(r.returncode, 0)
        steps = self.steps()
        self.assertEqual(steps[-1], "teardown.sh")
        self.assertEqual(steps[-2], "collect.sh", "evidence before destroy")
        self.assertEqual(steps.count("run-curve.sh"), 1, "nothing runs after the failed arm")

    def test_a_failed_resize_is_collected_against_the_whole_provisioning(self):
        r = self.campaign(env={"FAIL_ON": "resize-cluster.sh"})
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.steps(), ["run.sh", "resize-cluster.sh", "collect.sh", "teardown.sh"])
        collect = self.calls()[2]["argv"]
        self.assertTrue(collect[1].endswith("/2-n5"))
        self.assertTrue(collect[2].endswith("/1-n7/inventory-7.json"))

    def test_a_failed_provisioning_is_destroyed(self):
        r = self.campaign(env={"FAIL_ON": "run.sh"})
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.steps(), ["run.sh", "teardown.sh"])

    def test_refuses_to_be_told_how_to_manage_the_cluster(self):
        for env, message in (({"KEEP_INFRA": "1"}, "do not set KEEP_INFRA"),
                             ({"RUN_DIR": "/x"}, "do not set RUN_DIR"),
                             ({"LANES": "AE"}, "LANES must be E"),
                             ({"KNEE_SMALL": "7"}, "must be below KNEE_FULL")):
            with self.subTest(env=env):
                r = self.campaign(env=env)
                self.assertNotEqual(r.returncode, 0)
                self.assertIn(message, r.stderr)
        self.assertEqual(self.calls(), [])


# A broker's /metrics, scripted per host: MESH_PLAN maps a host to the
# (links, members) it reports on successive scrapes; the last entry repeats.
MESH_STUB = '''#!/usr/bin/env python3
import json, os, sys
host = sys.argv[-2].split("@")[-1]
plan = json.load(open(os.environ["MESH_PLAN"]))[host]
counter = os.path.join(os.environ["MESH_STATE"], host)
n = int(open(counter).read()) if os.path.exists(counter) else 0
open(counter, "w").write(str(n + 1))
links, members = plan[min(n, len(plan) - 1)]
if links is None:
    sys.exit(255)
print(f"# TYPE mqttd_peer_links gauge\\nmqttd_peer_links {links}\\nmqttd_cluster_members {members}")
'''


class MeshSettleTests(Rig):
    def setUp(self):
        super().setUp()
        (self.bin / "ssh").write_text(MESH_STUB)
        self.inv = self.root / "inv3.json"
        self.inv.write_text(json.dumps(inventory(brokers=3, drivers=1)))
        self.state = self.root / "state"
        self.state.mkdir()
        self.evidence = self.root / "mesh.txt"

    def settle(self, plan, budget=30, stable=3):
        (self.root / "plan.json").write_text(json.dumps(
            {f"198.51.100.{i + 1}": steps for i, steps in enumerate(plan)}))
        script = (f". {self.rig}/lib.sh; INVENTORY={self.inv}; RUN={self.provision}; "
                  f"await_full_mesh {budget} {stable} {self.evidence}")
        return subprocess.run(["bash", "-c", script], text=True, capture_output=True,
                              env=self.env | {"MESH_PLAN": str(self.root / "plan.json"),
                                              "MESH_STATE": str(self.state), "MESH_POLL_SECS": "0"})

    def rounds(self):
        return self.evidence.read_text().splitlines()

    def test_a_healthy_mesh_passes_after_the_stable_rounds(self):
        r = self.settle([[[2, 3]]] * 3)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(len(self.rounds()), 3)
        self.assertIn("broker0=2/3 broker1=2/3 broker2=2/3", self.rounds()[-1])

    def test_the_2026_09_25_shape_waits_for_the_missing_link(self):
        # Two brokers each one link short (the pair that had not re-greeted after
        # the founder's re-arm restart), healing on the fourth scrape.
        short = [[1, 2]] * 3 + [[2, 3]]
        r = self.settle([[[2, 3]], short, short])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(len(self.rounds()), 6, "3 unsettled rounds, then 3 stable ones")
        self.assertIn("broker1=1/2", self.rounds()[0])

    def test_a_flap_restarts_the_stable_count(self):
        flap = [[2, 3], [2, 3], [1, 3], [2, 3]]
        r = self.settle([[[2, 3]], flap, [[2, 3]]])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(len(self.rounds()), 6)

    def test_an_unreachable_or_unsettled_mesh_runs_out_of_budget(self):
        for plan in ([[[2, 3]], [[1, 2]], [[2, 3]]], [[[2, 3]], [[None, None]], [[2, 3]]]):
            with self.subTest(plan=plan):
                for f in self.state.iterdir():
                    f.unlink()
                r = self.settle(plan, budget=0)
                self.assertEqual(r.returncode, 1)
                self.assertTrue(self.rounds(), "every round is evidence, even the failing one")


if __name__ == "__main__":
    unittest.main()
