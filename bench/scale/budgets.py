#!/usr/bin/env python3
"""The harness's time budgets as ONE tree, checked for self-consistency.

Every phase of a run has a budget, and each one is enforced in its own corner of
`run-curve.sh` — a `while` loop counting polls here, a `timeout` there, a curl
`-m` further down. Nothing ever compared them, so a budget could be smaller than
the work it had to contain and the only way to find out was to pay for a fleet
and watch a rung die. Three of those cost real money in this campaign:

  * the drain deadline could not fit the flat-run it waits for (fixed ad hoc at
    run-curve.sh's `LANE_E_DRAIN_SECS` check — the one precedent for this file);
  * the steady gate's 180 s test budget was inherited by the unit tests, which
    then hung for three minutes each;
  * the clock gate demanded an NTP sample newer than 120 s from a reference that
    polls its upstream every 64-1024 s, so rung 1 died at the second capture.

The rule this file enforces is the obvious one: **a parent's budget must cover
its children**. Sequential children SUM; parallel children take the MAX; a child
that runs `repeat` times counts `repeat` times. A node with no budget of its own
simply costs what its children cost.

Two things keep it honest rather than decorative:

  * the numbers are PARSED from the scripts (`${VAR:-default}` in run-curve.sh,
    the module constants in clock-check.py), never restated here, so this file
    cannot drift from what the harness actually does;
  * the environment wins over the defaults, so checking a profile means sourcing
    it and running this — the tree describes the run you are about to launch.

Usage:
    python3 budgets.py                 # render the tree and check it
    python3 budgets.py --quiet         # check only; exit 1 on a violation
    python3 budgets.py --max-run-secs N  # also bound the whole run
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
# lane-e-evidence.py: `(longest - shortest) / (2 * dt) > 0.02` fails a window.
EVIDENCE_UNCERTAINTY = 0.02


# ── the numbers, read from the scripts that use them ─────────────────────────


def shell_defaults(path: Path) -> dict[str, float]:
    """`VAR="${VAR:-7}"` -> {"VAR": 7.0}, for every numeric default in a script."""
    text = path.read_text()
    out: dict[str, float] = {}
    for name, default in re.findall(r'^([A-Z_][A-Z_0-9]*)="?\$\{[A-Z_0-9]+:-([0-9]+(?:\.[0-9]+)?)\}', text, re.M):
        out[name] = float(default)
    return out


def python_constants(path: Path) -> dict[str, float]:
    """`NAME = 3 * 4` -> {"NAME": 12.0}, for the module-level numeric constants."""
    out: dict[str, float] = {}
    for name, expr in re.findall(r"^([A-Z_][A-Z_0-9]*)\s*=\s*([0-9][0-9_.*+ -]*)$", path.read_text(), re.M):
        try:
            out[name] = float(eval(expr, {"__builtins__": {}}, {}))  # noqa: S307 — digits and operators only
        except (ValueError, SyntaxError, TypeError):
            continue
    return out


class Budgets:
    """Every declared duration, with the environment overriding the defaults."""

    def __init__(self, env: dict[str, str] | None = None, root: Path = HERE):
        self.env = os.environ if env is None else env
        self.defaults = shell_defaults(root / "run-curve.sh")
        self.defaults.update(shell_defaults(root / "clock-sync.sh"))
        self.clock = python_constants(root / "clock-check.py")
        self.defaults.update(self.profile_defaults(root))

    def profile_defaults(self, root: Path = HERE) -> dict[str, float]:
        """`LANE_E_SECS` and `LANE_E_DRAIN_SECS` are set per PROFILE inside
        run-curve.sh's `if SMOKE / elif STANDARD / else` block, not as plain
        `${VAR:-N}` defaults — so the branch the environment selects decides
        them, exactly as the script does."""
        text = (root / "run-curve.sh").read_text()
        branches = re.split(r'^if \[ "\$\{SMOKE:-0\}" = 1 \]; then|^elif \[ "\$\{STANDARD:-0\}" = 1 \]; then|^else$',
                            text, flags=re.M)[1:4]
        if len(branches) != 3:
            return {}
        smoke, standard, full = branches
        chosen = smoke if self.env.get("SMOKE", "0") == "1" else (
            standard if self.env.get("STANDARD", "0") == "1" else full)
        out: dict[str, float] = {}
        for name, value in re.findall(r'^\t([A-Z_0-9]+)="?(?:\$\{[A-Z_0-9]+:-)?([0-9]+)', chosen, re.M):
            out.setdefault(name, float(value))
        return out

    def get(self, name: str, fallback: float | None = None) -> float:
        raw = self.env.get(name)
        if raw not in (None, ""):
            try:
                return float(raw)
            except ValueError:
                pass
        if name in self.defaults:
            return self.defaults[name]
        if fallback is None:
            raise KeyError(f"{name} has no default in run-curve.sh and none was given")
        return fallback

    def rungs(self) -> int:
        """Rungs in the ladder, including the closing control."""
        override = self.env.get("LANE_E_SITES_OVERRIDE", "").split()
        ladder = len(override) if override else 4
        return ladder + (1 if self.env.get("LANE_E_CONTROL", "1") != "0" else 0)


# ── the tree ─────────────────────────────────────────────────────────────────


@dataclass
class Node:
    """One phase. `budget` is its declared ceiling; without one it costs what its
    children cost. `mode` says how the children combine: "seq" runs them one
    after another (they sum), "par" runs them at once (the slowest decides)."""

    name: str
    budget: float | None = None
    # "budget": the harness WAITS against this and the phase fails when it is
    #   exceeded — a violation changes what the run does.
    # "tolerance": nothing enforces it during the run; exceeding it invalidates
    #   the measurement afterwards. A violation here is a RISK: the run proceeds
    #   and the rung is thrown away, which is the more expensive failure.
    kind: str = "budget"
    mode: str = "seq"
    repeat: int = 1
    why: str = ""
    children: list["Node"] = field(default_factory=list)

    @property
    def children_cost(self) -> float:
        if not self.children:
            return 0.0
        costs = [c.cost for c in self.children]
        return sum(costs) if self.mode == "seq" else max(costs)

    @property
    def own_cost(self) -> float:
        """What this node occupies: its budget if it declares one, else its
        children's cost. A declared budget is a CEILING the phase may reach, so
        it is what an outer budget has to reserve — not the children's total."""
        return self.budget if self.budget is not None else self.children_cost

    @property
    def cost(self) -> float:
        return self.own_cost * self.repeat

    @property
    def violation(self) -> str | None:
        if self.budget is None or not self.children:
            return None
        if self.children_cost > self.budget:
            verb = "sum to" if self.mode == "seq" else "peak at"
            noun = "a budget of" if self.kind == "budget" else "a tolerance of"
            return (f"{self.name}: its {len(self.children)} children {verb} "
                    f"{self.children_cost:g}s against {noun} {self.budget:g}s")
        return None

    def walk(self):
        yield self
        for child in self.children:
            yield from child.walk()


def ssh_bounds(root: Path = HERE) -> dict[str, float]:
    """What `lib.sh` actually bounds an ssh hop by. `ServerAliveCountMax` is
    absent there, so OpenSSH's default of 3 applies — stated here because the
    absence is the point: nothing in the repo says 45 s, yet 45 s is the number
    every polled loop inherits."""
    opts = (root / "lib.sh").read_text()
    def opt(name: str, default: float) -> float:
        m = re.search(rf"-o {name}=([0-9]+)", opts)
        return float(m.group(1)) if m else default
    interval = opt("ServerAliveInterval", 15)
    count = opt("ServerAliveCountMax", 3)  # OpenSSH default; lib.sh does not set it
    return {"connect": opt("ConnectTimeout", 120), "dead_peer": interval * count}


def scrape(b: Budgets, remote_timeout: float = 10) -> Node:
    """One fan-out scrape: every host at once, so the SLOWEST host decides.

    A hop costs the connect, then the remote command — unless the peer stops
    answering, in which case ssh only gives up after ServerAliveInterval x
    ServerAliveCountMax. That last term is the one nothing in the harness names,
    and it dominates: it is three times the curl timeout it is supposed to
    contain. Window-edge scrapes of 8.6 s were measured on 2026-09-18, so this is
    not a theoretical tail."""
    # A rig that does not talk over ssh — the test suite's fakes — must say what
    # its transport costs instead, or the model judges it by a bound it never
    # pays. An override, never a bypass: the tree is still built and checked.
    override = b.env.get("BUDGET_SCRAPE_SECS")
    if override:
        return Node("scrape (declared transport cost)", budget=float(override),
                    why="BUDGET_SCRAPE_SECS — this rig does not use ssh")
    ssh = ssh_bounds()
    return Node("scrape (all hosts, parallel)", mode="par", why="the slowest host decides", children=[
        Node("ssh connect", budget=ssh["connect"]),
        Node(f"remote curl -m {remote_timeout:g}", budget=remote_timeout),
        Node("dead-peer detection", budget=ssh["dead_peer"],
             why="ServerAliveInterval x ServerAliveCountMax, unbounded by any `timeout`"),
    ])


def poll_tries(b: Budgets) -> Node:
    """lane_e_recv_total in the steady gate: each driver gets LANE_E_POLL_TRIES
    attempts, 2 s apart, before the poll counts as failed — so a steady poll
    costs up to that many scrapes, not one. The drain polls once."""
    tries = int(b.get("LANE_E_POLL_TRIES"))
    children = [Node("attempt", repeat=tries, children=[scrape(b)])]
    if tries > 1:
        children.append(Node("sleep 2 between attempts", repeat=tries - 1, budget=2))
    return Node(f"delivery poll (up to {tries} tries)", children=children)


def lane_e_tree(b: Budgets) -> Node:
    """Lane E as it actually runs: one canary, then the ladder of rungs."""
    drain_poll = b.get("LANE_E_DRAIN_POLL")
    steady_polls = b.get("LANE_E_STEADY_POLLS")
    flat_polls = b.get("LANE_E_FLAT_POLLS")

    reset = Node("reset wait", budget=b.get("LANE_E_RESET_BUDGET"),
                 why="the previous rung's connections must be gone", children=[
        Node("poll", repeat=1, children=[scrape(b), Node(f"sleep {drain_poll:g}", budget=drain_poll)]),
    ])

    settle = Node("settle", children=[
        Node("fixed warm-up", budget=b.get("LANE_E_SETTLE"), why="LANE_E_SETTLE, a flat sleep"),
        Node("settle wait", budget=b.get("LANE_E_SETTLE_BUDGET"),
             why="every client must have CONNECTED before the window opens", children=[
            Node("poll", children=[scrape(b), Node(f"sleep {drain_poll:g}", budget=drain_poll)]),
        ]),
    ])

    steady = Node("steady gate", budget=b.get("LANE_E_STEADY_BUDGET"),
                  why="delivery must sit inside the offer band before measuring", children=[
        Node("in-band poll", repeat=int(steady_polls), children=[poll_tries(b)]),
    ])

    # The edge scrapes BRACKET the window rather than fitting inside it, so the
    # window costs the hold plus both edges. What bounds an edge is not the hold
    # but lane-e-evidence.py's uncertainty rule: a counter read over a bracket of
    # width w, across a window of dt, is only worth (w / 2dt) <= 2%. That is the
    # tightest budget in the harness and the one no script states as a duration.
    secs = b.get("LANE_E_SECS")
    window = Node("measurement window", why="the rung itself", children=[
        Node("open scrape", children=[scrape(b)]),
        Node("hold", budget=secs, why="LANE_E_SECS of steady traffic"),
        Node("close scrape", children=[scrape(b)]),
    ])
    bracket = Node("window edge bracket", kind="tolerance", budget=EVIDENCE_UNCERTAINTY * 2 * secs,
                   why=f"lane-e-evidence.py rejects a bracket wider than "
                       f"{EVIDENCE_UNCERTAINTY:.0%} of 2x the window", children=[
        Node("one edge scrape", children=[scrape(b)]),
    ])

    drain = Node("drain", budget=b.get("LANE_E_DRAIN_SECS"),
                 why="the backlog must stop moving, or the rung is UNRESOLVED", children=[
        # The precedent this whole file generalises: run-curve.sh already refuses
        # a drain deadline that cannot fit the flat run it waits for.
        Node("flat-run poll", repeat=int(flat_polls),
             children=[scrape(b), Node(f"sleep {drain_poll:g}", budget=drain_poll)]),
    ])

    rung = Node("rung", mode="seq", repeat=b.rungs(), children=[
        reset,
        Node("control still visible", budget=4 * (10 + 2), why="4 guard tries, each a scrape + sleep 2"),
        Node("start containers", children=[scrape(b)]),
        settle,
        steady,
        window,
        bracket,
        Node("pause for protocol completion", budget=60),
        drain,
        Node("terminal capture", children=[scrape(b, remote_timeout=30)]),
    ])

    return Node("lane E (one size)", mode="seq", children=[
        Node("mesh settle", budget=b.get("LANE_E_MESH_SETTLE_BUDGET"),
             why="every broker at N members and N-1 links before the control"),
        Node("driver gate", budget=b.get("LANE_E_DRIVER_GATE_SECS") + 30,
             why="every driver bursts at once; +30 s for start-up and log collection. A swap adds replace-node.sh's own waits"),
        Node("forward canary", budget=b.get("LANE_E_FORWARD_CANARY_TIMEOUT"),
             why="100 QoS 1 messages over each directed broker pair"),
        Node("ladder", children=[rung]),
    ])


def clock_tree(b: Budgets) -> Node:
    """The clock gate is not a duration but the same shape of rule: a staleness
    limit must cover the polling that refreshes what it judges. Modelled here so
    the relationship that killed rung 1 on 2026-09-19 cannot come back silently.

    A fleet host polls the reference every <= 2^maxpoll seconds; the reference
    polls the internet on chrony's own schedule, up to 1024 s. Each limit must
    cover two consecutive polls — one missed sample is not a lost clock."""
    host_poll = 2 ** 4  # clock-sync.sh pins the fleet to `maxpoll 4`
    converge = Node("clock convergence wait", budget=b.get("QOS1_CLOCK_CONVERGE_BUDGET"),
                    why="root dispersion must fall inside the error budget before the run", children=[
        Node("re-check", budget=b.get("QOS1_CLOCK_CONVERGE_POLL")),
    ])
    return Node("clock freshness", children=[converge,
        Node("fleet host max age", budget=120.0, why="clock-check.py `max_age_s`", children=[
            Node("upstream poll", repeat=2, budget=host_poll, why="chrony maxpoll 4"),
        ]),
        Node("reference max age", budget=b.clock["REFERENCE_MAX_AGE_S"],
             why="clock-check.py REFERENCE_MAX_AGE_S", children=[
            Node("upstream poll", repeat=2, budget=1024, why="chrony's default maximum"),
        ]),
    ])


def tree(b: Budgets, max_run_secs: float | None = None) -> Node:
    root = Node("run", budget=max_run_secs, mode="seq", children=[
        Node("provision + bootstrap", budget=20 * 60, why="observed 13-15 min on a CCX fleet"),
        lane_e_tree(b),
        clock_tree(b),
    ])
    return root


# ── rendering ────────────────────────────────────────────────────────────────


def hms(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:g}s"
    if seconds < 3600:
        return f"{seconds / 60:.1f}m"
    return f"{seconds / 3600:.2f}h"


def render(node: Node, depth: int = 0, lines: list[str] | None = None) -> list[str]:
    lines = [] if lines is None else lines
    label = "  " * depth + node.name + (f" x{node.repeat}" if node.repeat > 1 else "")
    if node.budget is None:
        budget = "—"
    else:
        budget = hms(node.budget)
    if node.children and node.budget is not None:
        headroom = node.budget - node.children_cost
        mark = "OK " if headroom >= 0 else "OVER"
        fit = f"{mark} children {hms(node.children_cost)} ({node.mode}), headroom {hms(abs(headroom))}"
        if headroom < 0:
            fit = f"OVER children {hms(node.children_cost)} ({node.mode}), SHORT BY {hms(-headroom)}"
    elif node.children:
        fit = f"= {hms(node.children_cost)} ({node.mode})"
    else:
        fit = ""
    lines.append(f"{label:<44} {budget:>8}  {fit}")
    for child in node.children:
        render(child, depth + 1, lines)
    return lines


def check(root: Node, kind: str = "budget") -> list[str]:
    return [v for node in root.walk() if node.kind == kind and (v := node.violation)]


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--quiet", action="store_true", help="only report violations")
    parser.add_argument("--max-run-secs", type=float, default=None,
                        help="bound the whole run; without it the total is reported, not enforced")
    args = parser.parse_args(argv)

    b = Budgets()
    root = tree(b, args.max_run_secs)
    violations = check(root)
    if not args.quiet:
        print("harness time budgets — a parent must cover its children\n")
        print(f"{'phase':<44} {'budget':>8}  fits?")
        print("\n".join(render(root)))
        print(f"\nworst-case run: {hms(root.children_cost)} over {b.rungs()} rungs")
    for v in violations:
        print(f"BUDGET VIOLATION: {v}", file=sys.stderr)
    for r in check(root, "tolerance"):
        print(f"MEASUREMENT RISK: {r} — nothing enforces this during the run; "
              "the rung is measured and then thrown away", file=sys.stderr)
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
