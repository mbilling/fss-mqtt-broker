#!/usr/bin/env python3
"""Reject unsynchronized, stale and misleadingly low-offset NTP evidence."""

import importlib.util
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).parent
spec = importlib.util.spec_from_file_location("clock", ROOT / "clock-check.py")
clock = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clock)
GOOD = """Reference ID : 0A63010B (10.99.1.11)
Stratum : 3
Ref time (UTC) : Sat Sep 19 02:00:00 2026
System time : 0.000001 seconds slow of NTP time
Root delay : 0.0002 seconds
Root dispersion : 0.0001 seconds
Leap status : Normal
Captured epoch : 1789783205
"""


class Clocks(unittest.TestCase):
    def test_complete_bound(self):
        report = clock.tracking(GOOD)
        self.assertAlmostEqual(report["absolute_error_ms"], 0.201)
        self.assertAlmostEqual(report["error_ms"], 0.001)  # the reference: its own slew only

    def test_the_references_upstream_path_cancels_between_fleet_hosts(self):
        """The 2026-09-19 fleet: driver6 was 5.056 ms from TRUE time and the run
        died on a 5 ms budget before offering load, while its error against the
        reference every host shares was 1.401 ms."""
        ref = GOOD.replace("0.0002 seconds", "0.007308611 seconds").replace("0.0001 seconds", "0.000358505 seconds")
        host = (GOOD.replace("Stratum : 3", "Stratum : 4").replace("0.000001", "0.000013309")
                .replace("0.0002 seconds", "0.007881215 seconds").replace("0.0001 seconds", "0.001101806 seconds"))
        reference = clock.tracking(ref, 5)
        report = clock.tracking(host, 5, reference=reference)
        self.assertAlmostEqual(report["absolute_error_ms"], 5.056, places=3)
        self.assertAlmostEqual(report["error_ms"], 1.401, places=3)
        # The host's OWN hop still gates: a slow private link is not forgiven.
        with self.assertRaisesRegex(ValueError, "exceeds"):
            clock.tracking(host.replace("0.007881215", "0.017881215"), 5, reference=reference)
        # No discount for a host that is not one hop below the reference.
        with self.assertRaisesRegex(ValueError, "not disciplined"):
            clock.tracking(host.replace("Stratum : 4", "Stratum : 3"), 5, reference=reference)

    def test_a_host_below_the_references_own_root_delay_is_not_a_failure(self):
        """The reports are snapshots moments apart and the reference's internet
        path varies, so a host that synced before the reference's delay grew
        records a SMALLER total. Measured 2026-09-19 at 120,000 msg/s: seven of
        ten hosts sat below the reference's 8.344 ms and the run was killed for
        it. The hop estimate is clamped at zero; the host's whole dispersion,
        which is kept, is the conservative term."""
        ref = GOOD.replace("0.0002 seconds", "0.008344115 seconds").replace("0.0001 seconds", "0.000358505 seconds")
        host = (GOOD.replace("Stratum : 3", "Stratum : 4")
                .replace("0.0002 seconds", "0.007886941 seconds")   # BELOW the reference
                .replace("0.0001 seconds", "0.000402320 seconds"))
        reference = clock.tracking(ref, 5)
        report = clock.tracking(host, 5, reference=reference)
        # Clamped at zero, so the bound is the offset plus the whole dispersion.
        self.assertAlmostEqual(report["error_ms"], 1000 * (0.000001 + 0.000402320), places=6)

    def test_the_reference_gets_a_longer_liveness_bound_than_a_fleet_host(self):
        """The reference polls the internet up to every 1024 s; a fleet host polls
        the reference every <=16 s. Two missed polls is the limit for each, so the
        bounds differ by an order of magnitude and neither is the accuracy gate."""
        self.assertGreaterEqual(clock.REFERENCE_MAX_AGE_S, 2 * 1024)
        self.assertLess(clock.FLEET_MAX_AGE_S, clock.REFERENCE_MAX_AGE_S)
        between = 1000  # past a fleet host's bound, inside the reference's
        reference = clock.tracking(GOOD.replace("1789783205", str(1789783200 + between)))
        self.assertAlmostEqual(reference["reference_age_s"], between)
        with self.assertRaisesRegex(ValueError, "stale"):
            clock.tracking(GOOD.replace("1789783205", str(1789783200 + between))
                           .replace("Stratum : 3", "Stratum : 4"), reference=reference)

    def test_staleness_is_priced_into_the_error_bound_not_gated_twice(self):
        """The 2026-09-19 fleet, measured: under load the last NTP sample aged to
        132 s while the worst fleet error only reached 1.656 ms. A 120 s age limit
        killed that run; the 5 ms budget it was really being judged against was
        never close. chrony grows root dispersion with age, so a clock stale
        enough to matter fails on ERROR, which is the honest measure."""
        stale = (GOOD.replace("Stratum : 3", "Stratum : 4")
                 .replace("1789783205", "1789783332")          # 132 s since Ref time
                 .replace("Root dispersion : 0.0001", "Root dispersion : 0.0016"))
        reference = clock.tracking(GOOD)
        report = clock.tracking(stale, 5, reference=reference)
        self.assertAlmostEqual(report["reference_age_s"], 132)
        self.assertLess(report["error_ms"], 5)
        # Stale ENOUGH to matter still fails — on the error budget, as it should.
        worse = stale.replace("Root dispersion : 0.0016", "Root dispersion : 0.02")
        with self.assertRaisesRegex(ValueError, "exceeds"):
            clock.tracking(worse, 5, reference=reference)
        # And a chrony that has stopped entirely is still caught by liveness.
        dead = stale.replace("1789783332", str(1789783200 + clock.FLEET_MAX_AGE_S + 61))
        with self.assertRaisesRegex(ValueError, "stale"):
            clock.tracking(dead, 5, reference=reference)

    def test_invalid_clock_evidence(self):
        for bad in [GOOD.replace("Normal", "Not synchronised"),
                    GOOD.replace("0.0001 seconds", "0.1 seconds"),
                    GOOD.replace("1789783205", "1789783105"),
                    GOOD.replace("0.000001", "nan"),
                    GOOD.replace("0A63010B", "7F7F0101")]:
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                clock.tracking(bad)

    def test_subscriber_hosts_stay_fixed_and_allocation_is_balanced(self):
        source = (ROOT / "run-curve.sh").read_text()
        fn = source[source.index("lane_e_driver() {"):].split("\n}\n", 1)[0] + "\n}\n"
        previous = None
        for publishers in (2, 4, 6):
            script = fn + f"D=8; LANE_E_PLACEMENT=container; LANE_E_SUB_CONTAINERS_PER_SITE=2; LANE_E_PUB_CONTAINERS_PER_SITE={publishers}\n"
            script += 'for role in sub pub; do for s in 0 1 2 3; do if [ "$role" = sub ]; then n=2; else n=$LANE_E_PUB_CONTAINERS_PER_SITE; fi; for ((j=0;j<n;j++)); do echo "$role $(lane_e_driver "$role" "$s" "$j" 4)"; done; done; done\n'
            rows = subprocess.check_output(["bash", "-c", script], text=True).splitlines()
            subscribers = [r for r in rows if r.startswith("sub")]
            if previous is not None:
                self.assertEqual(subscribers, previous)
            previous = subscribers
            counts = [sum(r.endswith(f" {d}") for r in rows) for d in range(8)]
            self.assertLessEqual(max(counts) - min(counts), 1)


if __name__ == "__main__":
    unittest.main()
