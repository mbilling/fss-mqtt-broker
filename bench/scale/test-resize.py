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
env = {k: os.environ.get(k, "") for k in ("LANE_E_SITES_OVERRIDE", "KEEP_INFRA", "RUN_DIR", "PREFLIGHT_ONLY", "DRIVER_COUNT")}
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"argv": argv, "env": env}) + "\\n")
fail_on = os.environ.get("FAIL_ON")
if fail_on and fail_on in " ".join(argv):
    sys.exit(3)
# One-shot: MARK_BAD_BROKER=<substring> makes the matching call fail the way
# the driver gate does on a pinned broker — bad-brokers.txt, then a nonzero exit.
mark = os.environ.get("MARK_BAD_BROKER")
flag = os.path.join(os.path.dirname(os.environ["CALL_LOG"]), "marked")
if mark and argv[0] in ("run.sh", "run-curve.sh") and mark in " ".join(argv) and not os.path.exists(flag):
    open(flag, "w").close()
    run = env["RUN_DIR"] if argv[0] == "run.sh" else argv[1]
    size = argv[-1] if argv[0] == "run.sh" else argv[2].rsplit("-", 1)[-1].split(".")[0]
    lane = os.path.join(run, "results", f"nodes={size}", "laneE")
    os.makedirs(lane, exist_ok=True)
    if argv[0] == "run.sh":
        with open(os.environ["FULL_INVENTORY"]) as src, open(os.path.join(run, f"inventory-{size}.json"), "w") as dst:
            dst.write(src.read())
    open(os.path.join(lane, "bad-brokers.txt"), "w").write("2\\n")
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
        for name in ("lib.sh", "cloud.sh", "resize-cluster.sh", "replace-node.sh", "run-curve.sh",
                     "482-per-node-knee.sh", "482-per-node-knee.env"):
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


class ExecutableTests(unittest.TestCase):
    def test_every_script_the_rig_executes_directly_is_executable(self):
        # run.sh exports LANE_E_SWAP_HOOK=replace-node.sh and run-curve.sh runs it
        # as a program; on 2026-09-25 it was committed 0644 and the paid launch
        # died in preflight. The other tests here call scripts through bash, so
        # nothing else would notice.
        for name in ("replace-node.sh", "resize-cluster.sh", "482-per-node-knee.sh", "482-knee-smoke.sh",
                     "bootstrap-cluster.sh", "run-curve.sh", "run.sh", "collect.sh", "teardown.sh"):
            with self.subTest(script=name):
                self.assertTrue(os.access(SCALE / name, os.X_OK), f"{name} is not executable")


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

    def test_a_driver_prefix_scales_the_fleet_with_the_brokers(self):
        r = self.run_script(self.rig / "resize-cluster.sh", self.full, 3, self.root / "arm3", 6)
        self.assertEqual(r.returncode, 0, r.stderr)
        got = json.loads((self.root / "arm3/inventory-3.json").read_text())
        self.assertEqual(got["drivers"], inventory()["drivers"][:6])
        self.assertEqual(len(got["brokers"]), 3)
        self.assertIn("drivers=6 of 10", (self.root / "arm3/RESIZED.txt").read_text())
        hosts = {c["argv"][-2] for c in self.calls()}
        self.assertFalse(any(h.startswith("root@203.0.113.") for h in hosts), "drivers are never touched")
        for bad, msg in (("11", "exceeds the 10 drivers"), ("0", "positive integer")):
            with self.subTest(drivers=bad):
                r = self.run_script(self.rig / "resize-cluster.sh", self.full, 3, self.root / f"x{bad}", bad)
                self.assertIn(msg, r.stderr)

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
                     "teardown.sh", "observe.sh", "resize-cluster.sh", "replace-node.sh"):
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
                     "observe.sh", "resize-cluster.sh", "replace-node.sh"):
            text = (self.rig / name).read_text().replace(
                "os.path.basename(sys.argv[0])", repr(name))
            (self.rig / name).write_text(text)

    def campaign(self, env=None):
        return self.run_script(self.rig / "482-per-node-knee.sh", env=env)

    def ladders(self):
        """The ladders of the env's KNEE_ARMS, in arm order."""
        return [a.strip().split(":", 2)[2] for a in self.env["KNEE_ARMS"].split(";") if a.strip()]

    def steps(self):
        return [c["argv"][0] for c in self.calls()]

    def test_preflight_checks_all_three_arms_offline(self):
        r = self.campaign(env={"PREFLIGHT_ONLY": "1"})
        self.assertEqual(r.returncode, 0, r.stderr)
        calls = self.calls()
        self.assertEqual([c["argv"] for c in calls], [["run.sh", "full", "7"], ["run.sh", "full", "5"],
                                                     ["run.sh", "full", "7"]])
        self.assertEqual([c["env"]["LANE_E_SITES_OVERRIDE"] for c in calls], self.ladders())
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
        self.assertEqual(first["env"]["LANE_E_SITES_OVERRIDE"], self.ladders()[0])
        full = Path(first["env"]["RUN_DIR"]) / "inventory-7.json"
        resizes = [c["argv"] for c in calls if c["argv"][0] == "resize-cluster.sh"]
        # Both resizes start from the provisioning's own inventory, never a prefix.
        self.assertEqual([(a[1], a[2], a[4]) for a in resizes], [(str(full), "5", "18"), (str(full), "7", "18")])
        ladders = [c["env"]["LANE_E_SITES_OVERRIDE"] for c in calls if c["argv"][0] == "run-curve.sh"]
        self.assertEqual(ladders, self.ladders()[1:])
        boots = [c["argv"] for c in calls if c["argv"][0] == "bootstrap-cluster.sh"]
        self.assertEqual([b[-1] for b in boots], ["durable", "durable"], "same mode run.sh gives arm 1")
        self.assertTrue(boots[0][2].endswith("2-n5/inventory-5.json"))
        self.assertTrue(boots[1][2].endswith("3-n7/inventory-7.json"))

    def test_a_four_size_curve_scales_the_driver_fleet_with_each_arm(self):
        arms = "10:20:1 10 20; 7:14:1 7 14; 5:10:1 5 10; 3:6:1 3 6; 10:20:1 20"
        r = self.campaign(env={"KNEE_ARMS": arms, "DRIVER_COUNT": "20"})
        self.assertEqual(r.returncode, 0, r.stderr)
        calls = self.calls()
        self.assertEqual(calls[0]["argv"], ["run.sh", "full", "10"])
        resizes = [(c["argv"][2], c["argv"][4], Path(c["argv"][3]).name) for c in calls
                   if c["argv"][0] == "resize-cluster.sh"]
        self.assertEqual(resizes, [("7", "14", "2-n7"), ("5", "10", "3-n5"), ("3", "6", "4-n3"),
                                   ("10", "20", "5-n10")])
        self.assertEqual([c["env"]["LANE_E_SITES_OVERRIDE"] for c in calls if c["argv"][0] == "run-curve.sh"],
                         ["1 7 14", "1 5 10", "1 3 6", "1 20"])
        for arm in ("1-n10", "2-n7", "3-n5", "4-n3", "5-n10"):
            self.assertIn(f"{arm}/results", r.stderr)

    def test_preflight_checks_each_arm_with_its_own_drivers(self):
        r = self.campaign(env={"PREFLIGHT_ONLY": "1", "KNEE_ARMS": "10:20:1 20; 3:6:1 6", "DRIVER_COUNT": "20"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual([(c["argv"][2], c["env"].get("DRIVER_COUNT")) for c in self.calls()],
                         [("10", "20"), ("3", "6")])

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

    def test_a_bad_broker_in_the_opening_arm_is_swapped_and_the_arm_re_formed(self):
        r = self.campaign(env={"MARK_BAD_BROKER": "run.sh full 7"})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.steps(), [
            "run.sh", "collect.sh", "replace-node.sh",
            "resize-cluster.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh",
            "resize-cluster.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh",
            "resize-cluster.sh", "bootstrap-cluster.sh", "run-curve.sh", "collect.sh",
            "teardown.sh",
        ])
        calls = self.calls()
        full = Path(calls[0]["env"]["RUN_DIR"]) / "inventory-7.json"
        swap = calls[2]["argv"]
        self.assertEqual(swap[1:4], [str(full), "broker", "2"])
        resizes = [c["argv"] for c in calls if c["argv"][0] == "resize-cluster.sh"]
        self.assertEqual([(a[2], Path(a[3]).name) for a in resizes],
                         [("7", "1-n7-r2"), ("5", "2-n5"), ("7", "3-n7")])
        ladders = [c["env"]["LANE_E_SITES_OVERRIDE"] for c in calls if c["argv"][0] == "run-curve.sh"]
        self.assertEqual(ladders[0], self.ladders()[0], "the re-formed arm runs the opening ladder")
        self.assertIn("1-n7-r2/results", r.stderr, "the summary gates the arm that actually measured")

    def test_a_bad_broker_in_a_resized_arm_is_swapped_once(self):
        r = self.campaign(env={"MARK_BAD_BROKER": "2-n5"})
        self.assertEqual(r.returncode, 0, r.stderr)
        resizes = [Path(c["argv"][3]).name for c in self.calls() if c["argv"][0] == "resize-cluster.sh"]
        self.assertEqual(resizes, ["2-n5", "2-n5-r2", "3-n7"])
        self.assertEqual(self.steps().count("replace-node.sh"), 1)

    def test_a_failed_broker_swap_still_destroys(self):
        r = self.campaign(env={"MARK_BAD_BROKER": "run.sh full 7", "FAIL_ON": "replace-node.sh"})
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.steps()[-1], "teardown.sh")
        self.assertNotIn("resize-cluster.sh", self.steps())

    def test_a_failed_provisioning_is_destroyed(self):
        r = self.campaign(env={"FAIL_ON": "run.sh"})
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(self.steps(), ["run.sh", "teardown.sh"])

    def test_refuses_to_be_told_how_to_manage_the_cluster(self):
        for env, message in (({"KEEP_INFRA": "1"}, "do not set KEEP_INFRA"),
                             ({"RUN_DIR": "/x"}, "do not set RUN_DIR"),
                             ({"LANES": "AE"}, "LANES must be E"),
                             ({"KNEE_ARMS": "7:18:1 7"}, "at least two arms"),
                             ({"KNEE_ARMS": "7:18:1 7; 10:18:1 10"}, "more than the 7 the first arm provisions"),
                             ({"KNEE_ARMS": "7:18:1 7; 5:20:1 5"}, "more than the 18 the first arm provisions"),
                             ({"KNEE_ARMS": "7:18:1 7; 5:x:1 5"}, "is not <brokers>:<drivers>:<ladder>"),
                             ({"KNEE_ARMS": "7:18:1 7; 5:10:"}, "is not <brokers>:<drivers>:<ladder>"),
                             ({"DRIVER_COUNT": "10"}, "must equal the first arm's drivers (18)")):
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


TOFU_STUB = '''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["CALL_LOG"], "a") as f:
    f.write(json.dumps({"argv": ["tofu"] + sys.argv[1:], "cwd": os.getcwd(), "env": {}}) + "\\n")
if sys.argv[1:2] == ["apply"] and os.environ.get("TOFU_FAIL") == "1":
    sys.exit(1)
if sys.argv[1:2] == ["output"]:
    sys.stdout.write(open(os.environ["TOFU_OUTPUT"]).read())
'''


class ReplaceNodeTests(Rig):
    ARGS = ["-var", "node_count=7", "-var", "run_label=20260925T203528Z", "-var", "mqttd_url=https://x/y",
            "-var", "driver_count=10"]

    def setUp(self):
        super().setUp()
        (self.rig / "terraform").mkdir()
        tofu = self.bin / "tofu"
        tofu.write_text(TOFU_STUB)
        tofu.chmod(0o755)
        scp = self.bin / "scp"
        scp.write_text(STUB.replace("os.path.basename(sys.argv[0])", "'scp'"))
        scp.chmod(0o755)
        quoted = " ".join(f"[{i}]=\"{a}\"" for i, a in enumerate(self.ARGS))
        (self.provision / "tf-apply-args-7.sh").write_text(f"declare -a TF_APPLY_ARGS=({quoted})\n")
        (self.provision / "pki-7/cluster/ca").mkdir(parents=True)
        (self.provision / "pki-7/client-tls/certs").mkdir(parents=True)
        for f in ("pki-7/cluster/ca/peer-ca.pem", "pki-7/client-tls/certs/client.pem",
                  "pki-7/client-tls/certs/client.key"):
            (self.provision / f).write_text("cert")
        (self.provision / "known_hosts").write_text(
            "203.0.113.9 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOld\n"
            "198.51.100.1 ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKeep\n")
        self.env |= {"CLOUD": "hcloud", "HCLOUD_TOKEN": "t", "TOFU_OUTPUT": str(self.root / "tofu-out.json")}

    def tofu_output(self, kind, idx, public, private=None):
        inv = inventory()
        entry = inv[f"{kind}s"][idx]
        entry["public_ip"] = public
        if private:
            entry["private_ip"] = private
        (self.root / "tofu-out.json").write_text(json.dumps(inv))

    def replace(self, inv, kind, idx, env=None):
        return self.run_script(self.rig / "replace-node.sh", inv, kind, idx, "pinned core", env=env)

    def test_a_driver_is_replaced_with_the_recorded_variables_only(self):
        self.tofu_output("driver", 8, "192.0.2.99")
        r = self.replace(self.full, "driver", 8)
        self.assertEqual(r.returncode, 0, r.stderr)
        tofu = [c["argv"] for c in self.calls() if c["argv"][0] == "tofu"]
        self.assertEqual(tofu[0], ["tofu", "apply", "-auto-approve", "-input=false", *self.ARGS,
                                   "-replace=hcloud_server.driver[8]"])
        self.assertEqual(tofu[1], ["tofu", "output", "-json", "inventory"])
        got = json.loads(self.full.read_text())
        self.assertEqual(got["drivers"][8]["public_ip"], "192.0.2.99")
        self.assertEqual(got["drivers"][8]["private_ip"], inventory()["drivers"][8]["private_ip"])
        kh = (self.provision / "known_hosts").read_text()
        self.assertNotIn("203.0.113.9", kh, "the replaced host's old key is gone")
        self.assertIn("198.51.100.1", kh, "every other host's key is kept")
        # The client material of THIS arm's PKI reaches the new driver, and nothing else.
        scp = [c["argv"] for c in self.calls() if c["argv"][0] == "scp"]
        self.assertEqual(len(scp), 1)
        self.assertTrue(scp[0][-1].startswith("root@192.0.2.99:/opt/bench-certs"))
        self.assertTrue(any(a.endswith("pki-7/client-tls/certs/client.key") for a in scp[0]))
        line = (self.provision / "REPLACED.txt").read_text()
        self.assertIn("kind=driver index=8 private=10.99.2.19 old=203.0.113.9 new=192.0.2.99", line)
        self.assertIn("reason=pinned core", line)

    def test_a_resized_arm_and_its_provisioning_both_learn_the_new_address(self):
        subprocess.run(["bash", str(self.rig / "resize-cluster.sh"), str(self.full), "5", str(self.root / "arm2")],
                       cwd=self.rig, env=self.env, check=True, capture_output=True)
        arm = self.root / "arm2/inventory-5.json"
        (self.root / "arm2/pki-5/cluster/ca").mkdir(parents=True)
        (self.root / "arm2/pki-5/client-tls/certs").mkdir(parents=True)
        self.tofu_output("driver", 3, "192.0.2.50")
        r = self.replace(arm, "driver", 3)
        self.assertEqual(r.returncode, 0, r.stderr)
        got = json.loads(arm.read_text())
        self.assertEqual(got["drivers"][3]["public_ip"], "192.0.2.50")
        self.assertEqual((len(got["brokers"]), got["resized_from"]), (5, 7), "the arm keeps its own shape")
        self.assertEqual(json.loads(self.full.read_text())["drivers"][3]["public_ip"], "192.0.2.50")
        scp = [c["argv"] for c in self.calls() if c["argv"][0] == "scp"]
        self.assertTrue(any("arm2/pki-5/" in a for a in scp[0]), "the ARM's PKI, not the provisioning's")
        self.assertIn("kind=driver index=3", (self.root / "arm2/REPLACED.txt").read_text())

    def test_a_left_out_broker_changes_only_the_provisioning(self):
        subprocess.run(["bash", str(self.rig / "resize-cluster.sh"), str(self.full), "5", str(self.root / "arm2")],
                       cwd=self.rig, env=self.env, check=True, capture_output=True)
        arm = self.root / "arm2/inventory-5.json"
        before = arm.read_text()
        self.tofu_output("broker", 6, "192.0.2.66")
        r = self.replace(arm, "broker", 6)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(arm.read_text(), before)
        self.assertEqual(json.loads(self.full.read_text())["brokers"][6]["public_ip"], "192.0.2.66")
        self.assertIn("UNCONFIGURED", r.stderr, "a broker needs its cluster re-formed, and says so")
        self.assertFalse([c for c in self.calls() if c["argv"][0] == "scp"], "no client certs for a broker")

    def test_refuses_what_would_rebuild_more_than_one_host(self):
        self.tofu_output("driver", 8, "192.0.2.99", private="10.99.9.9")
        r = self.replace(self.full, "driver", 8)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("did not keep private IP", r.stderr)
        self.assertEqual(json.loads(self.full.read_text()), inventory(), "the inventory is not rewritten")
        for args, message in ((("driver", 10), "out of range"), (("node", 1), "kind must be"),
                              (("driver", "x"), "index must be")):
            with self.subTest(args=args):
                r = self.replace(self.full, *args)
                self.assertIn(message, r.stderr)
        r = self.replace(self.full, "driver", 1, env={"CLOUD": "upcloud"})
        self.assertIn("not supported", r.stderr)
        (self.provision / "tf-apply-args-7.sh").unlink()
        r = self.replace(self.full, "driver", 8)
        self.assertIn("no " + str(self.provision / "tf-apply-args-7.sh"), r.stderr)
        applies = [c for c in self.calls() if c["argv"][:2] == ["tofu", "apply"]]
        self.assertEqual(len(applies), 1, "only the moved-address case reaches apply; every refusal before it does not")

    def test_a_failed_apply_changes_nothing(self):
        self.tofu_output("driver", 8, "192.0.2.99")
        r = self.replace(self.full, "driver", 8, env={"TOFU_FAIL": "1"})
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual(json.loads(self.full.read_text()), inventory())
        self.assertFalse((self.provision / "REPLACED.txt").exists())


def mpstat(per_cpu_soft, rows=5):
    """An `mpstat -P ALL 1` capture whose CPUs hold the given %soft."""
    out = ["Linux 6.8.0 (bench-driver-1) \t09/25/26 \t_x86_64_\t(8 CPU)", ""]
    for t in range(rows):
        out.append("21:16:%02d     CPU    %%usr   %%nice    %%sys %%iowait    %%irq   %%soft  %%steal  %%guest  %%gnice   %%idle" % t)
        out.append("21:16:%02d     all   10.00    0.00   10.00    0.00    0.00   10.00    0.00    0.00    0.00   70.00" % t)
        for cpu, soft in enumerate(per_cpu_soft):
            out.append("21:16:%02d %7d   10.00    0.00   10.00    0.00    0.00 %7.2f    0.00    0.00    0.00 %7.2f"
                       % (t, cpu, soft, max(0.0, 80.0 - soft)))
    return "\n".join(out) + "\n"


class DriverHealthTests(Rig):
    """The run-curve.sh helpers, extracted verbatim and driven with stubbed hosts."""

    FUNCS = ("hottest_soft", "lane_e_pinned_drivers", "lane_e_swap_drivers", "lane_e_rung_checked",
             "lane_e_driver_gate")

    def harness(self, body, env=None):
        src = (self.rig / "run-curve.sh").read_text()
        funcs = []
        for name in self.FUNCS:
            start = src.index(f"\n{name}() {{") + 1
            funcs.append(src[start:src.index("\n}\n", start) + 3])
        script = f". {self.rig}/lib.sh\n" + "".join(funcs) + body
        return subprocess.run(["bash", "-c", script], text=True, capture_output=True, cwd=self.root,
                              env=self.env | {"OUT": str(self.root / "out"), "N": "3", "D": "4",
                                              "INVENTORY": str(self.full),
                                              "LANE_E_DRIVER_SOFTIRQ_MAX": "80"} | (env or {}))

    def test_the_hottest_core_decides_not_the_average(self):
        f = self.root / "d.txt"
        f.write_text(mpstat([99, 2, 3, 1, 0, 0, 0, 0]))
        r = self.harness(f"hottest_soft {f}")
        self.assertEqual(r.stdout.strip(), "cpu0 99")
        d = self.root / "cpu"
        d.mkdir()
        (d / "cpu-driver3.txt").write_text(mpstat([12, 14, 3, 1]))
        (d / "cpu-driver8.txt").write_text(mpstat([98, 5, 3, 1]))
        (d / "cpu-driver10.txt").write_text(mpstat([79, 5, 3, 1]))
        (d / "cpu-broker0.txt").write_text(mpstat([100, 5, 3, 1]))
        r = self.harness(f"lane_e_pinned_drivers {d}")
        self.assertEqual(r.stdout.split(), ["8"], "only drivers, and only at or over the threshold")

    def test_a_rung_on_a_pinned_driver_is_voided_swapped_and_run_again(self):
        body = f'''
lane_e_rung() {{
	local d="$OUT/laneE/sites-$1"; [ "${{2:-1}}" -gt 1 ] && d="$d-rep$2"
	mkdir -p "$d/cpu"; n=$(( $(cat "$OUT/count" 2>/dev/null || echo 0) + 1 )); echo $n >"$OUT/count"
	if [ "$n" = 1 ]; then printf '%s' "$PINNED" >"$d/cpu/cpu-driver2.txt"; else printf '%s' "$HEALTHY" >"$d/cpu/cpu-driver2.txt"; fi
	echo "run $n" >"$d/rung.txt"
}}
say() {{ :; }}; warn() {{ echo "WARN $*" >&2; }}
mkdir -p "$OUT/laneE"
lane_e_rung_checked 10 2 no 2
'''
        hook = self.root / "hook.sh"
        hook.write_text(f"#!/bin/sh\necho \"$@\" >>{self.root}/hook.log\n")
        hook.chmod(0o755)
        r = self.harness(body, env={"LANE_E_SWAP_HOOK": str(hook), "PINNED": mpstat([97, 1]),
                                    "HEALTHY": mpstat([10, 1])})
        self.assertEqual(r.returncode, 0, r.stderr)
        lane = self.root / "out/laneE"
        self.assertEqual((lane / "sites-10-rep2/rung.txt").read_text().strip(), "run 2", "the rerun is the rung")
        voided = [p for p in lane.iterdir() if p.name.startswith("voided-sites-10-rep2-")]
        self.assertEqual(len(voided), 1)
        self.assertEqual((voided[0] / "rung.txt").read_text().strip(), "run 1", "the spoiled run is kept")
        self.assertIn("pinned drivers: 2", (voided[0] / "VOIDED.txt").read_text())
        hook_call = (self.root / "hook.log").read_text().split()
        self.assertEqual(hook_call[:3], [str(self.full), "driver", "2"])

    def test_without_a_hook_a_pinned_rung_is_recorded_not_hidden(self):
        body = '''
lane_e_rung() { mkdir -p "$OUT/laneE/sites-$1/cpu"; printf '%s' "$PINNED" >"$OUT/laneE/sites-$1/cpu/cpu-driver1.txt"; }
say() { :; }; warn() { echo "WARN $*" >&2; }
lane_e_rung_checked 7 1 no 2
'''
        r = self.harness(body, env={"LANE_E_SWAP_HOOK": "", "PINNED": mpstat([95])})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual((self.root / "out/laneE/sites-7/pinned-drivers.txt").read_text().strip(), "1")
        self.assertIn("no hook can swap", r.stderr)

    def gate(self, driver_soft, broker_soft, hook=True, achieved=None):
        """Run lane_e_driver_gate against scripted hosts: each driver's burst and
        each broker's mpstat come from the dicts, keyed by public IP; a swapped
        driver reports healthy on its next burst."""
        inv = inventory(brokers=3, drivers=4)
        self.full.write_text(json.dumps(inv))
        plan = {}
        for i, d in enumerate(inv["drivers"]):
            plan[d["public_ip"]] = {"cpu": driver_soft[i], "rate": (achieved or {}).get(i, 15000)}
        for i, b in enumerate(inv["brokers"]):
            plan[b["public_ip"]] = {"cpu": broker_soft[i]}
        (self.root / "gate-plan.json").write_text(json.dumps(plan))
        rssh = f'''
rssh() {{ python3 - "$1" <<'PY'
import json, sys
sys.path.insert(0, "{SCALE}")
host = sys.argv[1]
plan = json.load(open("{self.root}/gate-plan.json"))[host]
import importlib.util
spec = importlib.util.spec_from_file_location("t", "{SCALE / "test-resize.py"}")
t = importlib.util.module_from_spec(spec); spec.loader.exec_module(t)
sys.stdout.write(t.mpstat(plan["cpu"]))
if "rate" in plan:
    print("--- pub")
    print(f"1m0s pub total=900000 rate={{plan['rate']}}.00/sec")
    print(f"1m0s pub_succ total=900000 rate={{plan['rate']}}.00/sec")
PY
}}
brokers_csv() {{ echo 10.99.1.11,10.99.1.12,10.99.1.13; }}
say() {{ :; }}; warn() {{ :; }}
mkdir -p "$OUT/laneE"
lane_e_driver_gate
'''
        hook_path = self.root / "hook.sh"
        # The hook "replaces" a driver by marking its host healthy in the plan.
        hook_path.write_text(f"""#!/usr/bin/env python3
import json, sys
inv = json.load(open(sys.argv[1])); kind, idx = sys.argv[2], int(sys.argv[3])
p = "{self.root}/gate-plan.json"; plan = json.load(open(p))
host = inv[kind + "s"][idx]["public_ip"]
plan[host] = {{"cpu": [5, 5], "rate": 15000}}
json.dump(plan, open(p, "w"))
open("{self.root}/hook.log", "a").write(" ".join(sys.argv[1:]) + "\\n")
""")
        hook_path.chmod(0o755)
        env = {"LANE_E_SWAP_HOOK": str(hook_path) if hook else "", "LANE_E_DRIVER_GATE": "1",
               "LANE_E_DRIVER_GATE_SECS": "1", "LANE_E_SITE_RATE": "30000", "LANE_E_PUBS_PER_SITE": "1200",
               "LANE_E_PUB_CONTAINERS_PER_SITE": "2", "LANE_E_CONNECT_RATE": "500", "LANE_E_QOS": "0",
               "LANE_E_PAYLOAD": "200", "DOCKER_RUN": "docker run", "BENCH_IMG": "img"}
        return self.harness(rssh, env=env)

    def gate_log(self):
        return (self.root / "out/laneE/driver-gate.txt").read_text()

    def test_the_gate_passes_healthy_hosts(self):
        r = self.gate([[10, 3]] * 4, [[40, 5]] * 3)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.gate_log().count("verdict=ok"), 4)
        self.assertFalse((self.root / "hook.log").exists())

    def test_the_gate_swaps_a_pinned_driver_and_regates_only_it(self):
        r = self.gate([[10, 3], [99, 3], [10, 3], [10, 3]], [[40, 5]] * 3)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual((self.root / "hook.log").read_text().split()[1:3], ["driver", "1"])
        log = self.gate_log()
        self.assertIn("attempt=1 driver=1 hottest=cpu0 99", log)
        self.assertIn("verdict=bad (cpu0 at 99% softirq)", log)
        self.assertIn("attempt=2 driver=1", log)
        self.assertNotIn("attempt=2 driver=0", log, "healthy drivers are not burst again")

    def test_the_gate_swaps_a_driver_that_cannot_offer_its_rate(self):
        r = self.gate([[10, 3]] * 4, [[40, 5]] * 3, achieved={2: 9000})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("offered 9000 of 15000", self.gate_log())
        self.assertEqual((self.root / "hook.log").read_text().split()[1:3], ["driver", "2"])

    def test_without_a_hook_a_bad_driver_stops_the_size(self):
        r = self.gate([[10, 3], [99, 3], [10, 3], [10, 3]], [[40, 5]] * 3, hook=False)
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("no LANE_E_SWAP_HOOK", r.stderr)

    def test_a_gate_failing_on_every_driver_swaps_nothing(self):
        r = self.gate([[10, 3]] * 4, [[40, 5]] * 3, achieved={0: 0, 1: 0, 2: 0, 3: 0})
        self.assertNotEqual(r.returncode, 0)
        self.assertIn("failed on EVERY driver", r.stderr)
        self.assertFalse((self.root / "hook.log").exists(), "no healthy host is rebuilt")

    def test_audit_mode_skips_the_stock_image_burst(self):
        (self.root / "gate-plan.json").write_text("{}")
        body = "say() { :; }; warn() { :; }; mkdir -p \"$OUT/laneE\"; lane_e_driver_gate"
        r = self.harness(body, env={"QOS1_DRIVER_ARCHIVE": "/x/driver.tar.gz", "LANE_E_DRIVER_GATE": "1",
                                    "LANE_E_SWAP_HOOK": ""})
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual((self.root / "out/laneE/driver-gate.txt").read_text().strip(), "status=skipped-audit-mode")

    def test_an_outlier_broker_is_named_and_stops_the_arm(self):
        r = self.gate([[10, 3]] * 4, [[20, 5], [95, 5], [25, 5]])
        self.assertNotEqual(r.returncode, 0)
        self.assertEqual((self.root / "out/laneE/bad-brokers.txt").read_text().split(), ["1"])
        self.assertIn("median broker's hottest core was 25%", r.stderr)

    def test_brokers_hot_together_are_the_broker_working_not_a_bad_host(self):
        r = self.gate([[10, 3]] * 4, [[85, 5], [90, 5], [88, 5]])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertFalse((self.root / "out/laneE/bad-brokers.txt").exists())


class KneeStopTests(Rig):
    """run-curve.sh's ladder loop, extracted verbatim, with the rung and its
    verdict stubbed: the rung records itself, the verdict comes from a script."""

    def ladder(self, sites, verdicts, stop_after):
        src = (self.rig / "run-curve.sh").read_text()
        start = src.index("declare -a e_seen=()")
        end = src.index("\ndone\n", src.index("for e_sites in", start)) + len("\ndone\n")
        loop = src[start:end]
        body = f'''
say() {{ echo "$*" >&2; }}
LANE_E_SITES=({sites})
lane_e_rung_checked() {{ local d="$OUT/laneE/sites-$1"; [ "$2" -gt 1 ] && d="$d-rep$2"; mkdir -p "$d"; echo "$1 $2" >>"$OUT/ran"; }}
lane_e_rung_verdict() {{ v=$(sed -n "$(wc -l <"$OUT/ran")p" "$OUT/verdicts"); echo "$v"; }}
mkdir -p "$OUT/laneE"
printf '%s\\n' {" ".join(repr(v) for v in verdicts)} >"$OUT/verdicts"
{loop}
'''
        return subprocess.run(["bash", "-c", body], text=True, capture_output=True, cwd=self.root,
                              env=self.env | {"OUT": str(self.root / "out"), "N": "7",
                                              "LANE_E_STOP_AFTER_FAILS": str(stop_after),
                                              "LANE_E_SECS": "60", "LANE_E_PAYLOAD": "200",
                                              "LANE_E_PUBS_PER_SITE": "1200", "LANE_E_SITE_RATE": "30000",
                                              "LANE_E_PUB_CONTAINERS_PER_SITE": "2"})

    def ran(self):
        return (self.root / "out/ran").read_text().split("\n")[:-1]

    def test_two_failing_rungs_stop_the_ladder_and_name_what_was_skipped(self):
        r = self.ladder("1 14 14 17 18 20 21", ["pass", "pass", "pass", "fail: p99", "pass", "fail: p99",
                                                "fail: OFFER NOT MET"], 2)
        self.assertEqual(r.returncode, 0, r.stderr)
        # One failing rung between passes resets the count: 17 fails, 18 passes.
        self.assertEqual(self.ran(), ["1 1", "14 1", "14 2", "17 1", "18 1", "20 1", "21 1"])
        self.assertFalse((self.root / "out/laneE/ladder-stop.txt").exists(), "the ladder ended on its own")
        r = self.ladder("1 14 14 17 18 20 21", ["pass", "pass", "pass", "fail: p99", "fail: p99", "pass", "pass"], 2)
        self.assertEqual(r.returncode, 0, r.stderr)

    def test_the_skipped_names_match_what_the_gate_derives(self):
        (self.root / "out").mkdir(exist_ok=True)
        r = self.ladder("1 10 11 11 12 13", ["pass", "fail: p99", "fail: p99", "x", "x", "x"], 2)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.ran(), ["1 1", "10 1", "11 1"])
        stop = (self.root / "out/laneE/ladder-stop.txt").read_text()
        self.assertIn("stopped_after=sites-11 consecutive_fails=2 threshold=2", stop)
        # 11 already ran once, so the skipped repeat is -rep2, as shape.txt counts it.
        self.assertIn("skipped=sites-11-rep2 sites-12 sites-13", stop)
        self.assertIn("KNEE", r.stderr)
        self.assertEqual((self.root / "out/laneE/ladder-verdicts.txt").read_text().splitlines(),
                         ["sites-1 pass", "sites-10 fail: p99", "sites-11 fail: p99"])

    def test_off_by_default_climbs_everything(self):
        r = self.ladder("1 10 11", ["fail: a", "fail: b", "fail: c"], 0)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(self.ran(), ["1 1", "10 1", "11 1"])
        self.assertFalse((self.root / "out/laneE/ladder-verdicts.txt").exists())


class DeliveryPollTests(Rig):
    """lane_e_recv_total and the drain loop, extracted verbatim from run-curve.sh,
    against drivers whose scrape fails a scripted number of times."""

    def recv_total_src(self):
        src = (self.rig / "run-curve.sh").read_text()
        start = src.index("\tlane_e_recv_total() {")
        return src[start:src.index("\n\t}\n", start) + 4]

    def drain_src(self):
        src = (self.rig / "run-curve.sh").read_text()
        # Lane D has an identical-looking drain loop earlier in the file: take the
        # one that polls through lane_e_recv_total.
        start = src.rindex('\t\tt0=$(date +%s)\n\t\twhile :; do', 0, src.index("lane_e_recv_total 1"))
        end = "\n\t\tdone\n"
        return src[start:src.index(end, start) + len(end)]

    def poll(self, body, fails, extra_env=None):
        stub = self.root / "stub"
        stub.mkdir(exist_ok=True)
        (stub / "lane-e-evidence.py").write_text(
            "import sys, pathlib\n"
            "print(sum(int(p.read_text() or 0) for p in pathlib.Path(sys.argv[2], '.poll').glob('*.prom')))\n")
        script = f'''
set -uo pipefail
SCALE_DIR={stub}; rdir={self.root}/rung; D=3; LANE_E_SITE_RATE=30000; LANE_E_POLL_TRIES=3
rm -rf "$rdir"; mkdir -p "$rdir/.batch"
pollscrape=(a b c)
say() {{ :; }}; warn() {{ echo "WARN $*" >&2; }}; die() {{ echo "DIE $*" >&2; exit 9; }}
driver_batch() {{   # fails FAILS[q] times for driver q, then prints its count
	local n; n=$(( $(cat "$rdir/.n$1" 2>/dev/null || echo 0) + 1 )); echo $n >"$rdir/.n$1"
	local f=({" ".join(str(x) for x in fails)})
	[ "$n" -le "${{f[$1]}}" ] && {{ echo "ssh: connect timed out" >&2; return 255; }}
	echo "@@@ d$1"; echo 100
}}
batch_split() {{ awk '/^@@@/ {{next}} {{print}}' "$3" >"$1/$(basename "$3").prom"; }}
{self.recv_total_src()}
{body}
'''
        return subprocess.run(["bash", "-c", script], text=True, capture_output=True, cwd=self.root,
                              env=self.env | (extra_env or {}))

    def test_a_transient_scrape_failure_is_retried(self):
        r = self.poll('lane_e_recv_total || echo FAILED', [0, 2, 0])
        self.assertEqual(r.stdout.strip(), "300", r.stderr)
        err = (self.root / "rung/.batch/poll-1.err").read_text()
        self.assertIn("connect timed out", err, "the cause is kept, not discarded")
        self.assertEqual(err.count("failed at"), 2)

    def test_a_driver_that_never_answers_fails_the_poll(self):
        r = self.poll('lane_e_recv_total || echo FAILED', [0, 9, 0])
        self.assertIn("FAILED", r.stdout)
        r2 = self.poll('lane_e_recv_total 1 || echo FAILED', [0, 1, 0])
        self.assertIn("FAILED", r2.stdout, "one try means one try")

    def test_a_failed_drain_poll_is_recorded_and_the_drain_goes_on(self):
        # Driver 1 fails the first two drain polls, then the backlog is flat.
        body = '''
LANE_E_DRAIN_POLL=0; LANE_E_DRAIN_SECS=60; LANE_E_FLAT_POLLS=3; sites=10
drained=no prev=-1 flat=0 elapsed=0
echo -e "elapsed_s\\trecv_total" >"$rdir/drain.tsv"
''' + self.drain_src() + '''
echo "drained=$drained"
'''
        r = self.poll(body, [0, 2, 0])
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertNotIn("DIE", r.stderr)
        self.assertIn("drained=yes", r.stdout)
        rows = (self.root / "rung/drain.tsv").read_text().splitlines()[1:]
        self.assertEqual([row.split("\t")[1] for row in rows],
                         ["poll-failed", "poll-failed", "300", "300", "300", "300"],
                         "the first good poll is the baseline; three flat ones after it drain")
        self.assertIn("recorded, not counted as flat", r.stderr)


if __name__ == "__main__":
    unittest.main()
