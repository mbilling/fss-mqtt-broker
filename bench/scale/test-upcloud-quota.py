#!/usr/bin/env python3
"""Exercise both rigs' real HCL quota preconditions offline using only terraform_data.

Requires OpenTofu; no Hetzner or UpCloud provider, state or credentials.
"""
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

    @classmethod
    def setUpClass(cls):
        cls.tf = shutil.which("tofu")
        if not cls.tf:
            raise RuntimeError("OpenTofu (tofu) is required; Terraform is not supported")
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        for name in ("quota.tf", "variables.tf"):
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
        ):
            with self.subTest(variables=variables):
                result = self.plan(**variables)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)


class HcloudQuotaTests(QuotaTests):
    """The Hetzner project: 30 servers / 200 vCPUs since 2026-09-15."""
    MODULE = HCLOUD_MODULE

    def test_known_plans_fit(self):
        for variables in (
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
            ({"node_count": 10, "driver_count": 13}, "driver_count must be an integer between 1 and 12"),
            ({"node_count": 10, "driver_count": 1.5}, "driver_count must be an integer between 1 and 12"),
        ):
            with self.subTest(variables=variables):
                result = self.plan(**variables)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, " ".join(result.stderr.split()))


if __name__ == "__main__":
    unittest.main()
