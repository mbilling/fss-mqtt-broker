#!/usr/bin/env python3
"""The clock gate, replayed against every capture that has ever killed a paid run.

Six evidence gates in this campaign rejected runs that were measuring correctly.
Each was found the same expensive way: provision a fleet, watch a rung die, read
the captures afterwards and discover the fleet had been fine. The captures are
the cheap part — a few hundred bytes per host — so they are kept here, and every
one of them must now pass before a fleet is paid for again.

Each directory under `testdata/clock-corpus/` is one real capture phase, named
for the defect it exposed. They are REAL chrony reports off Hetzner CCX machines,
not fixtures: the point is that a synthetic case is written by the same person
who wrote the wrong rule, and shares its blind spot.

The invariant is one line: **on this corpus the gate returns, and every host's
fleet-relative error is inside the budget the campaign declares.** A change that
tightens the gate until one of these fails has re-introduced a defect that has
already been paid for once.
"""

import importlib.util
import json
import unittest
from pathlib import Path

ROOT = Path(__file__).parent
CORPUS = ROOT / "testdata/clock-corpus"
spec = importlib.util.spec_from_file_location("clock", ROOT / "clock-check.py")
clock = importlib.util.module_from_spec(spec)
spec.loader.exec_module(clock)

# What `bench/scale/clock-sync.sh` declares, and what the campaign reports as the
# latency uncertainty. Every archived capture was inside it; the gates that failed
# were measuring something else.
BUDGET_MS = 5.0


class Corpus(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((CORPUS / "manifest.json").read_text())

    def test_every_archived_capture_passes_the_gate(self):
        """The regression this file exists for. `hosts[0]` is the fleet's NTP
        reference, exactly as `validate_phase` is called in production."""
        for case, meta in sorted(self.manifest.items()):
            with self.subTest(case=case, run=meta["run"]):
                report = clock.validate_phase(CORPUS / case, meta["hosts"], BUDGET_MS)
                self.assertEqual(sorted(report), sorted(meta["hosts"]))
                worst = max(r["error_ms"] for r in report.values())
                self.assertLess(worst, BUDGET_MS, f"{case}: worst fleet error {worst:.3f} ms")

    def test_the_corpus_still_contains_each_defect_it_was_collected_for(self):
        """A capture that no longer exhibits its defect has been quietly replaced,
        and would stop guarding anything. Each assertion below is the measurement
        that killed a run, so these numbers are history, not thresholds."""
        def hosts(case):
            meta = self.manifest[case]
            return {h: clock.tracking((CORPUS / case / f"{h}.txt").read_text(), 1e9)
                    for h in meta["hosts"]}

        # 1. The reference's own path to the internet is inherited by every host
        #    and cancels between them. Judged absolutely, driver6 was over budget.
        absolute = hosts("shared-upstream-path")
        self.assertGreater(max(r["absolute_error_ms"] for r in absolute.values()), BUDGET_MS)

        # 2 & 3. Staleness: the reference between two upstream polls, and the whole
        #    fleet's polls delayed under load. Both beyond the old 120 s limit.
        for case, floor in (("reference-between-upstream-polls", 120), ("host-stale-under-load", 120)):
            ages = [r["reference_age_s"] for r in hosts(case).values()]
            self.assertGreater(max(ages), floor, case)

        # 4. Hosts recording a SMALLER root delay than the reference then showed —
        #    which an earlier rule called "not disciplined to the fleet reference".
        delays = hosts("host-below-reference-root-delay")
        reference = delays["broker0"]["root_delay_s"]
        below = [h for h, r in delays.items() if h != "broker0" and r["root_delay_s"] < reference]
        self.assertGreaterEqual(len(below), 5, f"only {below} below the reference")

    def test_a_genuinely_bad_clock_is_still_refused(self):
        """The corpus proves the gate is not too strict. It must not become the
        reason it is too loose: an unsynchronised host still fails, on real
        evidence with one field changed."""
        case = "healthy-under-120k"
        good = (CORPUS / case / "driver0.txt").read_text()
        reference = clock.tracking((CORPUS / case / "broker0.txt").read_text(), BUDGET_MS)
        clock.tracking(good, BUDGET_MS, reference=reference)  # must not raise
        for field, bad in [("Leap status     : Normal", "Leap status     : Not synchronised"),
                           ("Root dispersion : 0.0", "Root dispersion : 9.0")]:
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    clock.tracking(good.replace(field, bad, 1), BUDGET_MS, reference=reference)


if __name__ == "__main__":
    unittest.main(verbosity=1)
