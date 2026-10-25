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


class ClockCaptureTests(unittest.TestCase):
    """A clock past its bound grades the rung; it does not end the run."""

    def capture(self, ok):
        import tempfile
        src = (SCALE / "run-curve.sh").read_text()
        with tempfile.TemporaryDirectory() as td:
            rdir = Path(td) / "sites-17"
            body = f'''
set -euo pipefail
warn() {{ echo "WARN $*" >&2; }}
die() {{ echo "DIED $*" >&2; exit 9; }}
qos1_clock_capture() {{
	mkdir -p "$1"
	[ "{int(ok)}" = 1 ] && return 0
	echo "broker3: error bound 5.812 ms > 5 ms" >"$1/check.err"; return 1
}}
{shell_fn(src, "lane_e_clock_capture")}
lane_e_clock_capture "{rdir}" close
echo continued
'''
            r = subprocess.run(["bash", "-c", body], text=True, capture_output=True)
            note = rdir / "clock-invalid.txt"
            return r, note.read_text() if note.exists() else None

    def test_a_breach_is_recorded_and_the_run_continues(self):
        r, note = self.capture(ok=False)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIn("continued", r.stdout)
        self.assertNotIn("DIED", r.stderr)
        self.assertEqual(note, "close: broker3: error bound 5.812 ms > 5 ms\n")

    def test_a_healthy_clock_leaves_no_note(self):
        r, note = self.capture(ok=True)
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertIsNone(note)

    def test_no_clock_gate_dies_mid_run(self):
        src = (SCALE / "run-curve.sh").read_text()
        self.assertNotRegex(src, r"qos1_clock_capture[^\n]*\|\|\s*die")


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

    def test_a_clock_breach_is_neutral(self):
        # The verdict summarize-curve.py gives a rung with clock-invalid.txt.
        clock = "NOT CARRIED: INVALID EVIDENCE (clock health failed: close: broker3 error bound 5.812 ms > 5 ms, root dispersion)"
        self.check(["RED", clock, clock, "RED"], [1, 1, 1, 2], True)

    def test_a_pause_incomplete_rung_counts_as_a_fail(self):
        # 2026-10-21: withheld acknowledgements used to end the run; now they grade the rung.
        self.check(["NOT CARRIED: PAUSE INCOMPLETE (pub-s0-0: 590 of 600 connected workers paused)",
                    "FAILED: LOSS (the broker dropped 135 message(s): append-backlog-full 135)"], [1, 2], True)

    def test_green_and_yellow_reset_failed_and_not_carried_count(self):
        self.check(["RED", "YELLOW; LATENCY FLOOR (x)", "FAILED: LOSS (0.01%)", "GREEN", "NOT CARRIED: OFFER NOT MET",
                    "NOT CARRIED: verdict unavailable"], [1, 0, 1, 0, 1, 2], True)

    def test_harness_limited_and_latency_unreadable_are_neutral(self):
        # ADR 0048, amended 2026-10-25: the harness can only VOID a rung.
        harness = ("NOT CARRIED: HARNESS-LIMITED (the drivers sent 60.0% of the offer and the brokers received "
                   "60.1% of the offer — DRIVER CORE SATURATED (driver7 core 1)); NOTE: PUBLISHERS LATE (32.0%)")
        clock = "NOT CARRIED: LATENCY UNREADABLE (clock health failed: close: broker3 error bound 6.758 ms > 5 ms)"
        self.check(["RED", harness, clock, "RED"], [1, 1, 1, 2], True)

    def test_note_flags_never_count(self):
        self.check(["RED", "NOT CARRIED: INVALID EVIDENCE (x); NOTE: NOT STEADY (y, z); NOTE: PUBLISHERS LATE (9%)"],
                   [1, 1], False)

    def test_an_offer_the_brokers_did_not_take_counts(self):
        self.check(["NOT CARRIED: OFFER NOT TAKEN (the brokers received 97.2% of the offer); NOTE: PUBLISHERS LATE (32%)",
                    "NOT CARRIED: OFFER NOT TAKEN (x); LATENCY UNREADABLE (clock health failed: y)"], [1, 2], True)

    def test_notes_alone_do_not_make_a_rung_neutral(self):
        self.check(["NOT CARRIED: NOTE: PUBLISHERS LATE (9%)"], [1], False)

    def test_late_publishers_beside_a_certified_rung_reset(self):
        self.check(["RED", "GREEN; NOTE: PUBLISHERS LATE (5.7% of publishes behind their 100 ms slot, PUBACK p99 <=500ms)"],
                   [1, 0], False)


class GradedVerdictTests(unittest.TestCase):
    """The verdicts summarize-curve.py actually prints for audited rungs, folded
    through the shell's stop rule: the two must agree on what is neutral."""

    def test_summarizer_verdicts_through_the_stop_rule(self):
        import importlib.util
        import tempfile
        spec = importlib.util.spec_from_file_location("summarize_curve", SCALE / "summarize-curve.py")
        sc = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(sc)
        cases = [  # (fixture, the count after it, starting from 1)
            (dict(late=600, puback_le="500"), 0),                  # GREEN with late publishers: resets
            (dict(offered=150, cpu=True), 1),                       # HARNESS-LIMITED: neutral
            (dict(clock=True), 1),                                  # LATENCY UNREADABLE: neutral
            (dict(offered=150, puback_le="500"), 2),                # OFFER NOT TAKEN: counts
            (dict(broker_recv=5_880), 2),                           # OFFER NOT TAKEN: counts
            (dict(missing=True), 2),                                # FAILED: counts
            (dict(e2e_le="10000"), 2),                              # RED: counts
            (dict(e2e_le="2500"), 0),                               # YELLOW: resets
        ]
        with tempfile.TemporaryDirectory() as td:
            for i, (kw, want) in enumerate(cases):
                v = sc.rung_verdict(sc.lane_e_rung(sc.audited_case(Path(td), f"c{i}", **kw)))
                got, _, r = fold(["RED", v], stop_after=99)
                self.assertEqual(r.returncode, 0, r.stderr)
                self.assertEqual(got[-1], want, f"{kw}: {v}")


if __name__ == "__main__":
    unittest.main()
