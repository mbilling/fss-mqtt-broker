#!/usr/bin/env python3
"""Offline tests for lane E's stop bookkeeping in run-curve.sh: the count of
consecutive failing rungs, as lane_e_fails_after keeps it, from the count the
script sets before its ladder loop. Both are extracted verbatim and run under
the script's own `set -euo pipefail`; the loop around them is covered by
test-resize.py's KneeStopTests."""
from pathlib import Path
import shutil
import subprocess
import unittest

SCALE = Path(__file__).resolve().parent
NEUTRAL = "NOT CARRIED: INVALID EVIDENCE (endpoint scrape window uncertainty exceeds 2%)"


def shell_fn(src, name):
    """One top-level function, verbatim, from a script's source."""
    start = src.index(f"\n{name}() {{") + 1
    return src[start:src.index("\n}\n", start) + 3]


def fold(verdicts, stop_after=2, bash="bash"):
    """The count after each rung, up to and including the one that stops the
    ladder. Returns (counts, stopped, completed process)."""
    src = (SCALE / "run-curve.sh").read_text()
    loop = src.index("\nfor e_sites in", src.index("declare -a e_seen=()"))
    init = src[src.index("declare -a e_seen=()"):loop]
    body = f'''
set -euo pipefail
{shell_fn(src, "lane_e_fails_after")}
{init}
while IFS= read -r e_verdict; do
	e_fails=$(lane_e_fails_after "$e_fails" "$e_verdict")
	echo "$e_fails"
	if [ "$e_fails" -ge "$LANE_E_STOP_AFTER_FAILS" ]; then echo stop; break; fi
done
'''
    r = subprocess.run([bash, "-c", body], input="".join(v + "\n" for v in verdicts), text=True,
                       capture_output=True, env={"PATH": "/usr/bin:/bin", "LANE_E_STOP_AFTER_FAILS": str(stop_after)})
    out = r.stdout.split()
    stopped = bool(out) and out[-1] == "stop"
    return [int(c) for c in out if c != "stop"], stopped, r


class FailsAfterTests(unittest.TestCase):
    def check(self, verdicts, counts, stopped, stop_after=2):
        # Whatever bash is first on PATH, and /bin/bash (3.2 on macOS) when it
        # differs: resolved here, as fold's stripped PATH would only find /bin/bash.
        for bash in dict.fromkeys(b for b in (shutil.which("bash"), "/bin/bash") if b and Path(b).exists()):
            with self.subTest(bash=bash):
                got, got_stop, r = fold(verdicts, stop_after, bash)
                self.assertEqual(r.returncode, 0, r.stderr)
                self.assertEqual(got, counts)
                self.assertEqual(got_stop, stopped)

    def test_a_neutral_first_rung_is_carried_not_an_unset_count(self):
        # The live failure: under `set -u` the first neutral rung read an unset count.
        self.check([NEUTRAL, "GREEN"], [0, 0], False)

    def test_a_neutral_first_rung_then_two_fails_stops_on_the_third(self):
        self.check([NEUTRAL, "RED", "RED", "GREEN"], [0, 1, 2], True)

    def test_two_fails_stop(self):
        self.check(["GREEN", "RED", "RED", "GREEN"], [0, 1, 2], True)

    def test_neutral_neither_counts_nor_resets(self):
        self.check(["RED", "NOT CARRIED: INVALID EVIDENCE (x); INVALID EVIDENCE (y)", "RED"], [1, 1, 2], True)

    def test_neutral_then_a_real_fail_counts(self):
        self.check(["NOT CARRIED: INVALID EVIDENCE (x)", "RED (p99 9 s)"], [0, 1], False)

    def test_any_other_flag_beside_invalid_evidence_counts(self):
        self.check(["NOT CARRIED: INVALID EVIDENCE (x); NOT STEADY (y)", "NOT CARRIED: INVALID EVIDENCE (x); PUBLISHERS LATE (9%)"],
                   [1, 2], True)

    def test_green_and_yellow_reset_failed_and_not_carried_count(self):
        self.check(["RED", "YELLOW; LATENCY FLOOR (x)", "FAILED: LOSS (0.01%)", "GREEN", "NOT CARRIED: OFFER NOT MET",
                    "NOT CARRIED: verdict unavailable"], [1, 0, 1, 0, 1, 2], True)


if __name__ == "__main__":
    unittest.main()
