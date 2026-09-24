#!/usr/bin/env python3
"""Counter validity is separate from benchmark exit status/receipt correctness."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("membership_counters", Path(__file__).with_name("run-membership-counters.py"))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class CounterValidity(unittest.TestCase):
    def parse(self, rows):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "counters.jsonl"
            path.write_text("\n".join(json.dumps(row) for row in rows))
            return module.counters(path)

    def rows(self):
        return [dict(event=event, **{"counter-value": "1000000", "pcnt-running": 100.0})
                for event in sorted(module.EVENTS)]

    def test_complete_hardware_counts(self):
        self.assertEqual(self.parse(self.rows()), {event: 1_000_000 for event in module.EVENTS})

    def test_missing_duplicate_unavailable_and_multiplexed_are_errors(self):
        rows = self.rows()
        for invalid in [rows[:1], rows + rows[:1]]:
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                self.parse(invalid)
        for key, value in [("counter-value", "<not counted>"), ("counter-value", "<not supported>"),
                           ("counter-value", "NaN"), ("counter-value", "0"), ("pcnt-running", 98.9)]:
            invalid = self.rows()
            invalid[0][key] = value
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                self.parse(invalid)


if __name__ == "__main__":
    unittest.main()
