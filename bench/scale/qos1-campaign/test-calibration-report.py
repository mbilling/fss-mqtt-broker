#!/usr/bin/env python3
"""Prevent inadequate calibration evidence from authorizing a scaling campaign."""

import copy
import json
import subprocess
import tempfile
import unittest
from pathlib import Path


class Calibration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name)
        self.rows = [
            dict(
                nodes=3,
                placement="container",
                placement_version=2,
                subscriber_placement=[["sub-s0-0", 0], ["sub-s0-1", 1]],
                sub_containers_per_site=2,
                qos="1",
                sub_qos="1",
                pub_containers_per_site=cells,
                offered=120000,
                recv_rate=120000,
                control=False,
                measurements_valid=True,
                **{"pass": True},
                flags=[],
                path=f"cell{cells}-rep{rep}",
            )
            for cells in [2, 4, 6]
            for rep in [1, 2]
        ]
        control = copy.deepcopy(self.rows[0])
        control.update(control=True, offered=30000, recv_rate=30000)
        self.rows.append(control)

    def check_report(self, expected):
        (self.path / "rungs.json").write_text(json.dumps(self.rows))
        result = subprocess.run(
            [
                "python3",
                str(Path(__file__).with_name("calibration-report.py")),
                str(self.path),
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0 if expected else 1, result.stderr)
        report = json.loads((self.path / "CALIBRATION.json").read_text())
        self.assertEqual(report["pass"], expected)
        return report

    def test_two_good_allocations_can_calibrate_when_baseline_fails(self):
        self.rows[0]["pass"] = False
        self.assertEqual(
            self.check_report(True)["candidate_pub_containers_per_site"], 6
        )

    def test_missing_repeat_is_not_enough(self):
        self.rows = [
            r for r in self.rows if r["pub_containers_per_site"] == 6 or r["control"]
        ]
        self.check_report(False)

    def test_invalid_evidence_and_failed_control(self):
        self.rows[-1]["measurements_valid"] = False
        self.check_report(False)

    def test_mixed_nodes_cannot_qualify(self):
        self.rows[0]["nodes"] = 5
        self.check_report(False)

    def test_changing_subscriber_hosts_cannot_qualify(self):
        self.rows[0]["subscriber_placement"] = [["sub-s0-0", 6], ["sub-s0-1", 7]]
        self.check_report(False)

    def test_missing_subscriber_map_cannot_qualify(self):
        del self.rows[0]["subscriber_placement"]
        self.check_report(False)

    def test_incomplete_rung_cannot_qualify(self):
        row = copy.deepcopy(self.rows[0])
        row["flags"] = ["INCOMPLETE"]
        self.rows.append(row)
        self.check_report(False)

    def test_mixed_images_cannot_qualify(self):
        self.rows[0]["image_transition"] = True
        self.check_report(False)

    def test_rate_changes_with_parallelism_cannot_qualify(self):
        for r in self.rows:
            if r["pub_containers_per_site"] != 6:
                r["recv_rate"] = 110000 if r["pub_containers_per_site"] == 4 else 100000
        self.check_report(False)


if __name__ == "__main__":
    unittest.main()
