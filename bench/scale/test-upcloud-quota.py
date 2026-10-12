#!/usr/bin/env python3
"""Exercise both rigs' real HCL offline: the quota preconditions (terraform_data
only), the size lists, and the Hetzner broker layout — which broker gets which
private address and which placement group.

Requires OpenTofu; no Hetzner or UpCloud provider, state or credentials.
"""
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

MODULE = Path(__file__).resolve().parent / "terraform-upcloud"
HCLOUD_MODULE = Path(__file__).resolve().parent / "terraform"


class QuotaTests(unittest.TestCase):
    MODULE = MODULE
    FILES = ("quota.tf", "variables.tf")

    @classmethod
    def setUpClass(cls):
        cls.tf = shutil.which("tofu")
        if not cls.tf:
            raise RuntimeError("OpenTofu (tofu) is required; Terraform is not supported")
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        for name in cls.FILES:
            shutil.copy2(cls.MODULE / name, cls.root / name)
        cls.env = {"PATH": os.environ["PATH"], "HOME": str(cls.root)}
        subprocess.run([cls.tf, "init", "-backend=false", "-input=false"],
                       cwd=cls.root, env=cls.env, check=True, capture_output=True, timeout=30)

    def plan(self, **variables):
        variables = {"node_count": 10, "driver_count": 10} | variables
        args = [self.tf, "plan", "-input=false", "-refresh=false", "-no-color"]
        for key, value in variables.items():
            args += ["-var", f"{key}={value}"]
        return subprocess.run(args, cwd=self.root, env=self.env,
                              capture_output=True, text=True, timeout=30)

    def test_known_plans_fit(self):
        for variables in ({}, {"node_count": 1, "driver_count": 1,
                               "broker_server_type": "PREMIUM-48xCPU-96GB",
                               "driver_server_type": "PREMIUM-48xCPU-96GB"}):
            with self.subTest(variables=variables):
                result = self.plan(**variables)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_budgets_and_inputs_fail(self):
        for variables, expected in (
            ({"vcpu_quota": 79}, "this run needs 80 cores"),
            ({"public_ip_quota": 19}, "this run needs 20 public IPv4"),
            ({"broker_server_type": "PREMIUM-128xCPU-256GB"}, "Unknown UpCloud plan"),
            ({"driver_server_type": "typo"}, "Unknown UpCloud plan"),
            ({"driver_count": 1.5}, "driver_count must be an integer"),
            ({"admin_cidr": "::/0"}, "admin_cidr must be an IPv4 CIDR"),
            # 12 and 14 are the Hetzner rig's: nothing here establishes how many
            # servers one strict anti-affinity group admits, so UpCloud refuses.
            ({"node_count": 12, "driver_count": 1}, "Sizes 12 and 14 exist only on the Hetzner rig"),
            ({"node_count": 14, "driver_count": 1}, "Sizes 12 and 14 exist only on the Hetzner rig"),
        ):
            with self.subTest(variables=variables):
                result = self.plan(**variables)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, " ".join(result.stderr.split()))


class HcloudQuotaTests(QuotaTests):
    """The Hetzner project: 30 servers / 200 vCPUs since 2026-09-15."""
    MODULE = HCLOUD_MODULE
    # layout.tf is locals only (addresses, placement groups): no provider needed.
    FILES = ("quota.tf", "variables.tf", "layout.tf")

    def layout(self, node_count, driver_count):
        """The module's own answer for a shape, from `tofu console`."""
        expr = ("jsonencode({brokers = local.broker_ips, drivers = local.driver_ips, "
                "groups = local.broker_placement_group, second = local.second_placement_group})")
        result = subprocess.run(
            [self.tf, "console", "-var", f"node_count={node_count}", "-var", f"driver_count={driver_count}"],
            input=expr + "\n", cwd=self.root, env=self.env, capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(json.loads(result.stdout))

    def test_brokers_1_to_10_share_one_group_and_11_to_14_the_second(self):
        got = self.layout(14, 16)
        self.assertEqual(got["groups"], [0] * 10 + [1] * 4)
        self.assertTrue(got["second"])
        # Each broker is in exactly one group, and neither group exceeds Hetzner's 10.
        first = [i + 1 for i, g in enumerate(got["groups"]) if g == 0]
        second = [i + 1 for i, g in enumerate(got["groups"]) if g == 1]
        self.assertEqual((first, second), (list(range(1, 11)), [11, 12, 13, 14]))
        self.assertFalse(set(first) & set(second))
        self.assertLessEqual(max(len(first), len(second)), 10)
        got = self.layout(12, 16)
        self.assertEqual(got["groups"], [0] * 10 + [1] * 2)

    def test_shapes_of_ten_or_fewer_are_exactly_what_they_were(self):
        for size in (1, 3, 5, 7, 10):
            with self.subTest(size=size):
                got = self.layout(size, 20)
                self.assertEqual(got["groups"], [0] * size)
                self.assertFalse(got["second"], "no second placement group at ten or fewer")
                self.assertEqual(got["brokers"], [f"10.99.1.{11 + i}" for i in range(size)])
                self.assertEqual(got["drivers"], [f"10.99.1.{21 + i}" for i in range(20)])

    def test_no_broker_shares_a_private_address_with_a_driver(self):
        # A broker range sized for ten ran straight into the drivers': the
        # eleventh broker would have been 10.99.1.21, which is driver 1.
        for size, drivers in ((14, 16), (12, 18), (10, 20)):
            with self.subTest(size=size, drivers=drivers):
                got = self.layout(size, drivers)
                hosts = got["brokers"] + got["drivers"]
                self.assertEqual(len(set(hosts)), size + drivers, hosts)
                self.assertEqual(got["brokers"][:10], [f"10.99.1.{11 + i}" for i in range(10)])
                self.assertEqual(got["brokers"][10:], [f"10.99.1.{41 + i}" for i in range(size - 10)])

    def test_known_plans_fit(self):
        for variables in (
            # The 10-12-14 scale-out: 56 + 128 = 184 vCPU on exactly 30 servers.
            {"node_count": 14, "driver_count": 16},
            {"node_count": 14, "driver_count": 16, "vcpu_quota": 184, "server_quota": 30},
            {"node_count": 12, "driver_count": 18},  # 30 servers, 192 vCPU
            {"node_count": 7, "driver_count": 5},    # the #482 Option B arm: 12 servers, 68 vCPU
            {"node_count": 10, "driver_count": 12},  # the largest shape: 22 servers, 136 vCPU
            {"node_count": 1, "driver_count": 1,     # the 482-smoke.sh shared-vCPU shape
             "broker_server_type": "cpx32", "driver_server_type": "cpx42"},
        ):
            with self.subTest(variables=variables):
                result = self.plan(**variables)
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_budgets_and_inputs_fail(self):
        for variables, expected in (
            ({"node_count": 10, "driver_count": 12, "vcpu_quota": 135}, "this run needs 136 vCPUs"),
            ({"node_count": 10, "driver_count": 12, "server_quota": 21}, "this run needs 22 servers"),
            # The old 100-vCPU project refuses what the new one allows.
            ({"node_count": 10, "driver_count": 8, "vcpu_quota": 100}, "this run needs 104 vCPUs"),
            ({"node_count": 10, "driver_count": 21}, "driver_count must be an integer between 1 and 20"),
            ({"node_count": 10, "driver_count": 1.5}, "driver_count must be an integer between 1 and 20"),
            # 14 brokers: the server quota binds before the vCPU quota does.
            ({"node_count": 14, "driver_count": 17}, "this run needs 31 servers (14 brokers + 17 drivers)"),
            ({"node_count": 14, "driver_count": 20}, "this run needs 34 servers (14 brokers + 20 drivers)"),
            ({"node_count": 14, "driver_count": 16, "vcpu_quota": 183}, "this run needs 184 vCPUs"),
            ({"node_count": 12, "driver_count": 19}, "this run needs 31 servers"),
            # The size list stays explicit: nothing between or beyond its points.
            ({"node_count": 11, "driver_count": 1}, "node_count must be 1, 3, 5, 7, 10, 12 or 14"),
            ({"node_count": 13, "driver_count": 1}, "node_count must be 1, 3, 5, 7, 10, 12 or 14"),
            ({"node_count": 16, "driver_count": 1}, "node_count must be 1, 3, 5, 7, 10, 12 or 14"),
        ):
            with self.subTest(variables=variables):
                result = self.plan(**variables)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, " ".join(result.stderr.split()))


if __name__ == "__main__":
    unittest.main()
