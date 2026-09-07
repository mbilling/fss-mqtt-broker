#!/usr/bin/env python3
"""Exercise real HCL quota preconditions offline using only terraform_data.

Requires OpenTofu (or Terraform); no UpCloud provider, state or credentials.
"""
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

MODULE = Path(__file__).resolve().parent / "terraform-upcloud"


class QuotaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tf = shutil.which("tofu") or shutil.which("terraform")
        if not cls.tf:
            raise RuntimeError("Install OpenTofu or Terraform to run the quota tests")
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        for name in ("quota.tf", "variables.tf"):
            shutil.copy2(MODULE / name, cls.root / name)
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


if __name__ == "__main__":
    unittest.main()
