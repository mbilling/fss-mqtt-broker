#!/usr/bin/env python3
"""The budget tree must catch the budget defects this campaign actually paid for."""

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).parent
spec = importlib.util.spec_from_file_location("budgets", ROOT / "budgets.py")
budgets = importlib.util.module_from_spec(spec)
# Registered before exec: @dataclass resolves annotations through sys.modules,
# and an unregistered module makes it fail on the class body.
sys.modules["budgets"] = budgets
spec.loader.exec_module(budgets)
Node = budgets.Node

PROFILE = {
    "LANE_E_SITES_OVERRIDE": "1 2 4 4 2 1",
    "LANE_E_CONTROL": "1",
    "LANE_E_QOS": "1",
}


class Combination(unittest.TestCase):
    """The arithmetic, on trees small enough to check by hand."""

    def test_sequential_children_sum_and_parallel_children_peak(self):
        kids = [Node("a", budget=10), Node("b", budget=30)]
        self.assertEqual(Node("seq", mode="seq", children=kids).children_cost, 40)
        self.assertEqual(Node("par", mode="par", children=kids).children_cost, 30)

    def test_a_repeated_child_counts_every_repetition(self):
        parent = Node("loop", budget=60, children=[Node("poll", budget=25, repeat=3)])
        self.assertEqual(parent.children_cost, 75)
        self.assertIn("sum to 75s against a budget of 60s", parent.violation)

    def test_a_node_costs_its_declared_ceiling_not_its_childrens_total(self):
        """A phase that MAY reach its budget is what an outer budget must reserve;
        charging the outer one for the children's sum instead would understate it."""
        inner = Node("wait", budget=100, children=[Node("poll", budget=5)])
        self.assertEqual(Node("outer", children=[inner]).children_cost, 100)

    def test_a_node_without_a_budget_costs_what_its_children_cost(self):
        self.assertEqual(Node("group", children=[Node("x", budget=7)]).cost, 7)

    def test_a_childless_or_budgetless_node_cannot_violate(self):
        self.assertIsNone(Node("leaf", budget=5).violation)
        self.assertIsNone(Node("group", children=[Node("x", budget=999)]).violation)


class ParsedFromTheScripts(unittest.TestCase):
    """The numbers must come from the harness, so the tree cannot drift from it."""

    def test_defaults_are_read_from_run_curve(self):
        defaults = budgets.shell_defaults(ROOT / "run-curve.sh")
        self.assertEqual(defaults["LANE_E_STEADY_BUDGET"], 180)
        self.assertEqual(defaults["LANE_E_DRAIN_POLL"], 5)

    def test_the_profile_branch_decides_the_window_and_drain(self):
        """LANE_E_SECS is set per profile, not as a `${VAR:-N}` default."""
        self.assertEqual(budgets.Budgets({"SMOKE": "1"}).get("LANE_E_SECS"), 15)
        self.assertEqual(budgets.Budgets({"STANDARD": "1"}).get("LANE_E_SECS"), 45)
        self.assertEqual(budgets.Budgets({}).get("LANE_E_SECS"), 60)

    def test_the_environment_overrides_the_parsed_default(self):
        self.assertEqual(budgets.Budgets({"LANE_E_SECS": "300"}).get("LANE_E_SECS"), 300)

    def test_a_missing_name_is_an_error_not_a_guess(self):
        with self.assertRaises(KeyError):
            budgets.Budgets({}).get("LANE_E_NOT_A_REAL_BUDGET")

    def test_the_ssh_dead_peer_bound_cannot_exceed_the_timeout_it_contains(self):
        """It did: ServerAliveCountMax was unset, so OpenSSH's 3 gave a dead peer
        45s inside loops bounded at 60s. lib.sh now states it, and the bound must
        stay <= the remote timeout it wraps, or the polls stop being bounded by
        what they declare."""
        ssh = budgets.ssh_bounds()
        self.assertEqual(ssh["connect"], 10)
        self.assertLessEqual(ssh["dead_peer"], 10)

    def test_an_absent_server_alive_count_max_falls_back_to_openssh_default(self):
        """The parser must not read a missing option as zero — that would hide
        exactly the defect it exists to catch."""
        with tempfile.TemporaryDirectory() as td:
            (Path(td) / "lib.sh").write_text("SSH_OPTS=(-o ConnectTimeout=10 -o ServerAliveInterval=15)\n")
            self.assertEqual(budgets.ssh_bounds(Path(td))["dead_peer"], 45)

    def test_the_clock_constant_comes_from_clock_check(self):
        self.assertEqual(budgets.Budgets({}).clock["REFERENCE_MAX_AGE_S"], 2 * 1024 + 60)


class TheDefectsItMustCatch(unittest.TestCase):
    """Each of these cost a paid fleet or a wasted rung before this file existed."""

    def build(self, **env):
        b = budgets.Budgets(dict(PROFILE, **env))
        return b, budgets.tree(b)

    def test_a_drain_that_cannot_fit_its_flat_run_is_a_violation(self):
        """run-curve.sh's own `LANE_E_DRAIN_SECS` check, generalised: the deadline
        must cover the polls it waits for."""
        _, root = self.build(LANE_E_DRAIN_SECS="5")
        self.assertTrue(any("drain" in v for v in budgets.check(root)))

    def test_the_clock_staleness_limits_cover_two_upstream_polls(self):
        """The 2026-09-19 defect: a 120s limit on a reference that polls its
        upstream every 64-1024s killed rung 1 at the second capture."""
        _, root = self.build()
        clock = next(n for n in root.walk() if n.name == "reference max age")
        self.assertIsNone(clock.violation)
        self.assertGreaterEqual(clock.budget, 2 * 1024)
        broken = Node("reference max age", budget=120,
                      children=[Node("upstream poll", budget=1024, repeat=2)])
        self.assertIn("sum to 2048s against a budget of 120s", broken.violation)

    def test_the_drain_fits_its_polls_now_that_the_ssh_hop_is_bounded(self):
        """With ServerAliveCountMax unset a poll could cost 45s and three of them
        overran the 60s drain. Bounded at 10s they fit with headroom — and the
        tree is what says so, for whatever the budgets become next."""
        _, root = self.build()
        drain = next(n for n in root.walk() if n.name == "drain")
        self.assertIsNone(drain.violation)
        self.assertGreater(drain.budget, drain.children_cost)
        self.assertEqual(budgets.check(root), [])

    def test_the_window_bracket_is_a_tolerance_not_a_budget(self):
        """Nothing stops a slow edge scrape during the run; the rung is measured
        and then rejected by lane-e-evidence.py. That is a risk, not a failure,
        and conflating the two would hide which one an operator can prevent."""
        _, root = self.build()
        bracket = next(n for n in root.walk() if n.name == "window edge bracket")
        self.assertEqual(bracket.kind, "tolerance")
        self.assertAlmostEqual(bracket.budget, 0.02 * 2 * 60)
        self.assertEqual(budgets.check(root, "budget").count(bracket.violation), 0)
        self.assertIn(bracket.violation, budgets.check(root, "tolerance"))

    def test_the_edges_bracket_the_window_rather_than_fitting_inside_it(self):
        """The hold is LANE_E_SECS; the scrapes sit outside it. Modelling them as
        children of the hold would invent a violation that does not exist."""
        _, root = self.build()
        window = next(n for n in root.walk() if n.name == "measurement window")
        self.assertIsNone(window.budget)
        self.assertEqual([c.name for c in window.children],
                         ["open scrape", "hold", "close scrape"])
        self.assertEqual(next(c for c in window.children if c.name == "hold").budget, 60)


class WholeRun(unittest.TestCase):
    def test_the_ladder_counts_every_rung_and_the_closing_control(self):
        self.assertEqual(budgets.Budgets(dict(PROFILE)).rungs(), 7)
        self.assertEqual(budgets.Budgets({"LANE_E_SITES_OVERRIDE": "1 2",
                                          "LANE_E_CONTROL": "0"}).rungs(), 2)

    def test_a_run_budget_is_enforced_when_one_is_given(self):
        b = budgets.Budgets(dict(PROFILE))
        self.assertIsNone(budgets.tree(b).violation)  # no ceiling declared: reported, not enforced
        self.assertIsNotNone(budgets.tree(b, max_run_secs=60).violation)

    def test_the_tree_renders_every_node(self):
        b = budgets.Budgets(dict(PROFILE))
        root = budgets.tree(b)
        self.assertEqual(len(budgets.render(root)), len(list(root.walk())))


if __name__ == "__main__":
    unittest.main(verbosity=1)
