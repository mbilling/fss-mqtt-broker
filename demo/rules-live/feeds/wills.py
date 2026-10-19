"""Will scenarios: each connection ends one way or another, and its Will is checked against
what MQTT says must happen.

    python3 wills.py                        # every scenario, round after round, to 127.0.0.1:1883
    python3 wills.py --once                 # each scenario once; exit 1 if any failed
    python3 wills.py --once --only v5-delay,v311-keepalive --port 1884

Each scenario connects a client whose Will goes to wills/<scenario>/will, then ends the
connection: it drops the socket, goes silent past its keep-alive, breaks the protocol, sends
a DISCONNECT (MQTT 5 with or without reason 0x04), or another client takes its client id
over. A watcher subscribed to wills/+/will checks the outcome: the Will arrives within its
window (after the Will Delay, or when the session ends, whichever is first), carrying what
the client set (QoS, retain, MQTT 5 properties in order), or it never arrives, when a clean
DISCONNECT deleted it or the session was resumed in time.

The results go to the broker too, retained: wills/<scenario>/result (one JSON document per
scenario: pass or fail, what was expected, what happened) and wills/summary. Without --once
the scenarios start a few seconds apart and a round begins every --round seconds (120).

Where mqttd is known to differ, the result names the issue (KNOWN_ISSUES), but only when
every problem the scenario found is that difference. With --once, --expect-fail lists
scenarios that are expected to fail, as CI runs it: the run then passes while exactly those
fail with exactly their known problem, and fails on any other problem in them, or when one
of them starts passing, so the list is kept honest.

Options can also be set in the environment: SIM_HOST, SIM_PORT, SIM_QUIET (1 or 0) and
WILLS_ROUND. Exit 0 when every scenario passed (or failed as expected), 1 otherwise, 2 for
a bad option.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import queue
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

# The demo's MQTT client: demo/rules/sim/mqtt.py (the stack mounts it at SIM_LIB).
sys.path.insert(0, os.environ.get("SIM_LIB") or str(Path(__file__).resolve().parents[2] / "rules"))

from sim.mqtt import Client, Message, MqttError  # noqa: E402

ROOT = "wills"
TAKEN_OVER = 0x8E  # MQTT 5 DISCONNECT reason: Session taken over (§3.1.4)
WITH_WILL = 0x04   # MQTT 5 DISCONNECT reason: Disconnect with Will Message (§3.14.2.1)
EARLY = 0.5        # a Will this much before its window opens is early
SLACK = 2.5        # ...and one this much after the window's nominal end is late

# Where mqttd differs from MQTT today, by scenario: the issue, and what each of the
# scenario's problems must contain to be that difference. Any other problem is a regression.
KNOWN_ISSUES = {
    "v5-resumed": ("#1034: with MQTTD_DURABLE_SESSIONS=0 a session without subscriptions "
                   "is not kept", ["Session Present 0"]),
}


def known(scenario: str, problems: list) -> Optional[str]:
    """The issue when every problem is the scenario's known difference, else None."""
    issue, signs = KNOWN_ISSUES.get(scenario, ("", []))
    if problems and signs and all(any(sig in p for sig in signs) for p in problems):
        return issue
    return None


@dataclass
class Outcome:
    """What a scenario expects: a Will between `lo` and `hi` seconds after the trigger
    (`hi` None: no Will at all, watched for `quiet` seconds)."""

    lo: float = 0.0
    hi: Optional[float] = None
    quiet: float = 3.0
    retained: bool = False   # a fresh subscriber afterwards gets the Will as retained
    props: Optional[dict] = None  # MQTT 5 properties the Will must carry

    def describe(self) -> str:
        if self.hi is None:
            return f"no Will within {self.quiet:g} s"
        lo, hi = max(0.0, self.lo - EARLY), self.hi + SLACK
        return (f"a Will {lo:g}-{hi:g} s after the trigger"
                + (", retained" if self.retained else "")
                + (", with its properties" if self.props else ""))


@dataclass
class Scenario:
    name: str
    version: int
    what: str
    spec: str
    expect: Outcome
    play: Callable[["Run"], None]
    will_qos: int = 1
    will_retain: bool = False
    will_props: Optional[dict] = None
    props: Optional[dict] = None
    keepalive: int = 30


@dataclass
class Run:
    """One scenario's run: its clients, its trigger time, what arrived."""

    scenario: Scenario
    host: str
    port: int
    round: int
    token: str = ""
    trigger: Optional[float] = None
    arrivals: "queue.Queue[tuple[float, Message]]" = field(default_factory=queue.Queue)
    notes: list = field(default_factory=list)      # MUST breaks: the scenario fails
    warnings: list = field(default_factory=list)   # what the spec only describes or SHOULDs
    clients: list = field(default_factory=list)

    def client(self, client_id: Optional[str] = None, keepalive: Optional[int] = None) -> Client:
        s = self.scenario
        c = Client(self.host, self.port, client_id or f"wills-{s.name}",
                   keepalive=keepalive or s.keepalive, timeout=5, version=s.version)
        self.clients.append(c)
        return c

    def will(self) -> tuple[str, bytes, int, bool]:
        payload = json.dumps({"scenario": self.scenario.name, "token": self.token,
                              "set_at": stamp(time.time())}).encode()
        return (f"{ROOT}/{self.scenario.name}/will", payload, self.scenario.will_qos,
                self.scenario.will_retain)

    def connect_with_will(self, c: Client, clean: bool = True) -> bool:
        return c.connect(clean=clean, will=self.will(), props=self.scenario.props,
                         will_props=self.scenario.will_props)

    def fire(self):
        """The moment the connection ended: the Will's window is measured from here."""
        self.trigger = time.monotonic()

    def close(self):
        for c in self.clients:
            c.drop()


def stamp(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).isoformat(timespec="milliseconds")


# ---------------------------------------------------------------------------- the plays


def drop(r: Run):
    c = r.client()
    r.connect_with_will(c)
    c.drop()
    r.fire()


def disconnect(reason: int = 0) -> Callable[[Run], None]:
    def play(r: Run):
        c = r.client()
        r.connect_with_will(c)
        c.disconnect(reason)
        r.fire()
    return play


def go_silent(r: Run):
    """Connect with a 2 s keep-alive, then send nothing: the broker must close the
    connection after 1.5 times the keep-alive (§3.1.2.10) and publish the Will."""
    c = r.client()
    r.connect_with_will(c)
    r.fire()  # the keep-alive runs from the last packet: the CONNECT
    # Nothing reads or pings `c` from here on; `check` closes it once it has judged.


def break_protocol(r: Run):
    """A PUBLISH with both QoS bits set, which the broker must answer by closing the
    connection (§3.3.1.2): a close the client did not ask for, so the Will goes."""
    c = r.client()
    r.connect_with_will(c)
    assert c.sock is not None
    c.sock.sendall(bytes([0x36, 0x05, 0x00, 0x01, 0x78, 0x00, 0x01]))
    r.fire()


def resume_after(seconds: float) -> Callable[[Run], None]:
    """Drop, then reconnect within the Will Delay with clean start 0: the session goes on
    and the Will must not be published (MQTT 5 §3.1.2.5). Then a normal DISCONNECT, which
    deletes it, with the session's expiry set to 0 so nothing is left behind."""
    def play(r: Run):
        c = r.client()
        r.connect_with_will(c)
        c.drop()
        r.fire()
        time.sleep(seconds)
        again = r.client()
        present = again.connect(clean=False, props={"session_expiry": 60})
        if not present:
            r.notes.append("the reconnect found no session (Session Present 0)")
        time.sleep(r.scenario.expect.quiet - seconds)
        again.disconnect(0, {"session_expiry": 0})
    return play


def take_over(clean: bool) -> Callable[[Run], None]:
    """A second connection with the same client id: the broker closes the first (MQTT 5:
    with DISCONNECT 0x8E, §3.1.4) and the first one's Will follows §3.1.2.5."""
    def play(r: Run):
        first = r.client()
        r.connect_with_will(first)
        second = r.client()
        props = {"session_expiry": 60} if r.scenario.version == 5 else None
        r.fire()  # the broker acts on the second CONNECT before it answers it
        second.connect(clean=clean, props=props)
        if r.scenario.version == 5:
            try:
                first.poll(2.0)
            except (MqttError, OSError):
                pass
            if first.disconnect_reason != TAKEN_OVER:
                # §3.1.4 describes the DISCONNECT; the MUST in [MQTT-3.1.4-3] is the close.
                got = ("no DISCONNECT" if first.disconnect_reason is None
                       else f"DISCONNECT 0x{first.disconnect_reason:02x}")
                r.warnings.append(f"the first connection got {got}, not DISCONNECT 0x8E "
                                  "(Session taken over, §3.1.4)")
        time.sleep(max(r.scenario.expect.quiet, 1.0))
        if r.scenario.version == 5:
            second.disconnect(0, {"session_expiry": 0})
        else:
            second.disconnect()
    return play


SCENARIOS = [
    Scenario("v311-drop", 4, "MQTT 3.1.1: the socket closes without a DISCONNECT",
             "[MQTT-3.1.2-8]", Outcome(0, 1.5), drop),
    Scenario("v311-disconnect", 4, "MQTT 3.1.1: a DISCONNECT deletes the Will",
             "[MQTT-3.1.2-8], [MQTT-3.1.2-10]", Outcome(quiet=3), disconnect(0)),
    Scenario("v311-keepalive", 4, "MQTT 3.1.1: silent past 1.5 times its 2 s keep-alive",
             "[MQTT-3.1.2-24]", Outcome(2.5, 3.5), go_silent, keepalive=2),
    Scenario("v311-protocol-error", 4, "MQTT 3.1.1: a malformed PUBLISH (QoS 3) closes it",
             "[MQTT-3.3.1-4], [MQTT-3.1.2-8]", Outcome(0, 1.5), break_protocol),
    Scenario("v311-takeover", 4, "MQTT 3.1.1: a second connection takes the client id over",
             "[MQTT-3.1.4-2], [MQTT-3.1.2-8]", Outcome(0, 1.5), take_over(clean=True)),
    Scenario("v311-retained", 4, "MQTT 3.1.1: a Will with retain set is kept as retained",
             "[MQTT-3.1.2-17]", Outcome(0, 1.5, retained=True), drop, will_retain=True),
    Scenario("v5-drop-props", 5, "MQTT 5: the Will carries its properties, user properties "
             "in order", "§3.1.3.2", Outcome(0, 1.5, props={
                 "payload_format": 1, "content_type": "application/json", "message_expiry": 600,
                 "user": [("site", "wf-falster"), ("unit", "T07"), ("site", "backup")]}),
             drop, will_props={
                 "payload_format": 1, "content_type": "application/json", "message_expiry": 600,
                 "user": [("site", "wf-falster"), ("unit", "T07"), ("site", "backup")]}),
    Scenario("v5-disconnect", 5, "MQTT 5: DISCONNECT 0x00 deletes the Will",
             "[MQTT-3.1.2-8], §3.14.2.1", Outcome(quiet=3), disconnect(0)),
    Scenario("v5-disconnect-with-will", 5, "MQTT 5: DISCONNECT 0x04 asks for the Will",
             "§3.14.2.1", Outcome(0, 1.5), disconnect(WITH_WILL)),
    Scenario("v5-delay", 5, "MQTT 5: Will Delay 4 s, session expiry 60 s",
             "§3.1.3.2.2", Outcome(4, 4), drop,
             will_props={"will_delay": 4}, props={"session_expiry": 60}),
    Scenario("v5-session-ends-first", 5, "MQTT 5: Will Delay 30 s, but the session expires "
             "after 3 s", "§3.1.3.2.2", Outcome(3, 3), drop,
             will_props={"will_delay": 30}, props={"session_expiry": 3}),
    Scenario("v5-no-session", 5, "MQTT 5: Will Delay 30 s with session expiry 0: the session "
             "ends with the connection", "§3.1.3.2.2", Outcome(0, 1.5), drop,
             will_props={"will_delay": 30}),
    Scenario("v5-resumed", 5, "MQTT 5: Will Delay 6 s, reconnected after 2 s",
             "§3.1.2.5", Outcome(quiet=9), resume_after(2),
             will_props={"will_delay": 6}, props={"session_expiry": 60}),
    Scenario("v5-takeover", 5, "MQTT 5: taken over, Will Delay 0",
             "§3.1.4, §3.1.2.5", Outcome(0, 1.5), take_over(clean=False),
             props={"session_expiry": 60}),
    Scenario("v5-takeover-resumed", 5, "MQTT 5: taken over within Will Delay 8 s, clean "
             "start 0", "§3.1.4, §3.1.2.5", Outcome(quiet=10), take_over(clean=False),
             will_props={"will_delay": 8}, props={"session_expiry": 60}),
    Scenario("v5-takeover-clean", 5, "MQTT 5: taken over with clean start 1, which ends the "
             "session", "§3.1.4, §3.1.2.5", Outcome(0, 1.5), take_over(clean=True),
             will_props={"will_delay": 8}, props={"session_expiry": 60}),
]


# ---------------------------------------------------------------------------- checking


class Watcher:
    """One MQTT 5 subscription to wills/+/will, handing each Will to the run it names."""

    def __init__(self, host: str, port: int, say: Callable[[str], None]):
        self.host, self.port, self.say = host, port, say
        self.runs: dict[str, Run] = {}
        self.lock = threading.Lock()
        self.client: Optional[Client] = None
        self.stopping = threading.Event()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self._connect()
        self.thread.start()

    def _connect(self):
        if self.client:
            self.client.drop()
        c = Client(self.host, self.port, f"wills-watch-{os.getpid()}", keepalive=30,
                   timeout=5, version=5)
        c.connect()
        c.subscribe([(f"{ROOT}/+/will", 1)])
        self.client = c

    def expect(self, run: Run):
        with self.lock:
            self.runs[run.token] = run

    def forget(self, run: Run):
        with self.lock:
            self.runs.pop(run.token, None)

    def _loop(self):
        while not self.stopping.is_set():
            try:
                assert self.client is not None
                m = self.client.poll(0.5)
            except (MqttError, OSError) as e:
                self.say(f"watcher: {e}; reconnecting")
                time.sleep(1)
                try:
                    self._connect()
                except (MqttError, OSError):
                    pass
                continue
            if m is None:
                continue
            now = time.monotonic()
            try:
                token = json.loads(m.payload).get("token", "")
            except (ValueError, AttributeError):
                token = ""
            with self.lock:
                run = self.runs.get(token)
            if run:
                run.arrivals.put((now, m))
            # Anything else (an earlier round's late Will, a retained Will being cleared)
            # names no run in progress and is ignored.

    def stop(self):
        self.stopping.set()
        self.thread.join(timeout=2)
        if self.client:
            self.client.disconnect()


def check(run: Run, watcher: Watcher) -> dict:
    """Play the scenario and judge what arrived."""
    s, ex = run.scenario, run.scenario.expect
    problems: list[str] = []
    observed = ""
    try:
        s.play(run)
    except (MqttError, OSError) as e:
        problems.append(f"the play failed: {e}")
    if run.trigger is None:
        run.trigger = time.monotonic()
    deadline = run.trigger + (ex.quiet if ex.hi is None else ex.hi + SLACK)
    got: Optional[tuple[float, Message]] = None
    try:
        got = run.arrivals.get_nowait()  # it may have come while the play was still on
    except queue.Empty:
        left = deadline - time.monotonic()
        if left > 0:
            try:
                got = run.arrivals.get(timeout=left)
            except queue.Empty:
                pass
    if ex.hi is None:
        if got:
            observed = f"a Will {got[0] - run.trigger:.2f} s after the trigger"
            problems.append("the Will was published, and must not be")
        else:
            observed = f"no Will within {ex.quiet:g} s"
    elif got is None:
        observed = f"no Will within {ex.hi + SLACK:g} s"
        problems.append("the Will never arrived")
    else:
        at, m = got
        after = at - run.trigger
        observed = f"a Will {after:.2f} s after the trigger"
        if after < ex.lo - EARLY:
            problems.append(f"early: the window opens at {max(0.0, ex.lo - EARLY):g} s")
        if after > ex.hi + SLACK:
            problems.append(f"late: the window closes at {ex.hi + SLACK:g} s")
        if m.qos != min(s.will_qos, 1):
            problems.append(f"QoS {m.qos}, not {min(s.will_qos, 1)}")
        if ex.props:
            problems += compare_props(ex.props, m.props or {})
        if ex.retained:
            problems += retained_check(run)
    # A second copy of one Will is a duplicate whatever else happened.
    time.sleep(0.3)
    if not run.arrivals.empty():
        problems.append("the Will arrived more than once")
    watcher.forget(run)
    problems += run.notes
    run.close()
    issue = known(s.name, problems)
    return {
        "scenario": s.name, "round": run.round,
        "result": "fail" if problems else "pass",
        **({"known_issue": issue} if issue else {}),
        "what": s.what, "spec": s.spec, "expected": ex.describe(), "observed": observed,
        "problems": problems, "warnings": run.warnings, "at": stamp(time.time()),
    }


def compare_props(want: dict, got: dict) -> list[str]:
    out = []
    for k, v in want.items():
        g = got.get(k)
        if k == "message_expiry":
            if g is None or not 0 < g <= v:
                out.append(f"message expiry {g}, expected at most {v} and above 0")
        elif k == "user":
            if list(g or []) != list(v):
                out.append(f"user properties {g}, expected {v} in this order")
        elif g != v:
            out.append(f"{k} {g!r}, expected {v!r}")
    return out


def retained_check(run: Run) -> list[str]:
    """A fresh subscriber must get the Will as a retained message; then clear it."""
    topic = f"{ROOT}/{run.scenario.name}/will"
    c = Client(run.host, run.port, f"wills-retained-{run.scenario.name}-{os.getpid()}",
               timeout=5)
    try:
        c.connect()
        c.subscribe([(topic, 1)])
        m = c.poll(2.0)
        if m is None or not m.retain:
            return ["a fresh subscriber did not get the Will as a retained message"]
        try:
            if json.loads(m.payload).get("token") != run.token:
                return ["the retained message is not this Will"]
        except ValueError:
            return ["the retained message is not this Will"]
        c.publish(topic, b"", qos=1, retain=True)  # clear it for the next round
        return []
    except (MqttError, OSError) as e:
        return [f"the retained check failed: {e}"]
    finally:
        c.disconnect()


class Reporter:
    """Publishes each result and the summary, retained, and prints them."""

    def __init__(self, host: str, port: int, quiet: bool, say: Callable[[str], None]):
        self.host, self.port, self.quiet, self.say = host, port, quiet, say
        self.lock = threading.Lock()
        self.sending = threading.Lock()  # one connection at a time: they share a client id
        self.results: dict[str, dict] = {}

    def report(self, result: dict):
        with self.lock:
            self.results[result["scenario"]] = result
        mark = "pass" if result["result"] == "pass" else "FAIL"
        line = f"{mark} {result['scenario']}: {result['observed']}"
        if result["problems"]:
            line += " — " + "; ".join(result["problems"])
        if result.get("known_issue"):
            line += f" [known: {result['known_issue']}]"
        if result["warnings"]:
            line += " (warning: " + "; ".join(result["warnings"]) + ")"
        if not self.quiet or result["problems"]:
            self.say(line)
        self._publish({f"{ROOT}/{result['scenario']}/result": result})

    def summary(self, round_no: int) -> dict:
        with self.lock:
            res = list(self.results.values())
        failed = sorted(r["scenario"] for r in res if r["result"] == "fail")
        doc = {"round": round_no, "scenarios": len(res), "passed": len(res) - len(failed),
               "failed": failed, "at": stamp(time.time())}
        self._publish({f"{ROOT}/summary": doc})
        return doc

    def _publish(self, docs: dict):
        with self.sending:
            self._send(docs)

    def _send(self, docs: dict):
        c = Client(self.host, self.port, f"wills-report-{os.getpid()}", timeout=5, version=5)
        try:
            c.connect()
            for topic, doc in docs.items():
                c.publish(topic, json.dumps(doc).encode(), qos=1, retain=True,
                          props={"payload_format": 1, "content_type": "application/json"})
            c.disconnect()
        except (MqttError, OSError) as e:
            self.say(f"could not publish the results: {e}")
            c.drop()


# ---------------------------------------------------------------------------- the loop


def play_round(scenarios: list[Scenario], host: str, port: int, round_no: int,
               spacing: float, watcher: Watcher, reporter: Reporter,
               stopping: Callable[[], bool]) -> dict:
    threads = []
    for i, s in enumerate(scenarios):
        if stopping():
            break
        run = Run(s, host, port, round_no, token=f"{s.name}:{round_no}:{os.getpid()}:{i}")
        watcher.expect(run)
        t = threading.Thread(target=lambda r=run: reporter.report(check(r, watcher)),
                             daemon=True)
        t.start()
        threads.append(t)
        end = time.monotonic() + spacing
        while spacing and not stopping() and time.monotonic() < end:
            time.sleep(0.1)
    for t in threads:
        t.join()
    return reporter.summary(round_no)


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.environ.get("SIM_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("SIM_PORT", "1883")))
    ap.add_argument("--once", action="store_true",
                    help="play each scenario once, all at the same time, and exit")
    ap.add_argument("--only", help="comma-separated scenario names")
    ap.add_argument("--round", type=float, default=float(os.environ.get("WILLS_ROUND", "120")),
                    metavar="SECONDS", help="seconds between rounds without --once (120)")
    ap.add_argument("--quiet", action="store_true",
                    default=os.environ.get("SIM_QUIET", "") in ("1", "true", "yes"),
                    help="print only failures and the round summaries")
    ap.add_argument("--expect-fail", default="", metavar="NAMES",
                    help="with --once: comma-separated scenarios expected to fail")
    ap.add_argument("--list", action="store_true", help="list the scenarios and exit")
    args = ap.parse_args(argv)
    names = [s.name for s in SCENARIOS]
    args.scenarios = SCENARIOS
    args.expected = {n.strip() for n in args.expect_fail.split(",") if n.strip()}
    if args.expected - set(names):
        ap.error(f"unknown scenario(s) in --expect-fail: {', '.join(sorted(args.expected - set(names)))}")
    if args.expected and not args.once:
        ap.error("--expect-fail goes with --once")
    if args.only:
        wanted = [n.strip() for n in args.only.split(",") if n.strip()]
        unknown = [n for n in wanted if n not in names]
        if unknown or not wanted:
            ap.error(f"unknown scenario(s): {', '.join(unknown) or '(none given)'}; "
                     f"known: {', '.join(names)}")
        args.scenarios = [s for s in SCENARIOS if s.name in wanted]
    if args.round <= 0:
        ap.error("--round must be positive")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.list:
        for s in args.scenarios:
            print(f"{s.name:24} {s.what} ({s.spec}); expects {s.expect.describe()}")
        return 0

    def say(text: str):
        print(f"wills: {text}", flush=True)

    stop: list[int] = []

    def on_signal(signum, _frame):
        stop.append(signum)
        signal.signal(signum, signal.SIG_DFL)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    watcher = Watcher(args.host, args.port, say)
    while True:
        try:
            watcher.start()
            break
        except (MqttError, OSError) as e:
            if args.once or stop:
                say(f"cannot reach {args.host}:{args.port}: {e}")
                return 1
            say(f"waiting for {args.host}:{args.port}: {e}")
            time.sleep(2)
    reporter = Reporter(args.host, args.port, args.quiet, say)
    say(f"{len(args.scenarios)} scenarios against {args.host}:{args.port}"
        + ("" if args.once else f", a round every {args.round:g} s"))
    round_no = 0
    try:
        while True:
            round_no += 1
            started = time.monotonic()
            spacing = 0.0 if args.once else min(4.0, args.round / max(len(args.scenarios), 1) / 2)
            doc = play_round(args.scenarios, args.host, args.port, round_no, spacing,
                             watcher, reporter, lambda: bool(stop))
            say(f"round {round_no}: {doc['passed']} of {doc['scenarios']} passed"
                + (f"; failed: {', '.join(doc['failed'])}" if doc["failed"] else ""))
            if args.once or stop:
                played = {s.name for s in args.scenarios}
                failed = set(doc["failed"])
                # A failure is the expected one only when it is the known difference.
                unknown = {n for n in failed if not reporter.results[n].get("known_issue")}
                surprises = sorted((failed - args.expected) | unknown)
                fixed = sorted((args.expected & played) - failed)
                if fixed:
                    say(f"expected to fail, and passed: {', '.join(fixed)} (drop them from "
                        "--expect-fail, and close their issue)")
                if args.expected and not surprises and not fixed:
                    say(f"every failure was expected: {', '.join(sorted(failed)) or 'none'}")
                return 1 if surprises or fixed else 0
            while not stop and time.monotonic() < started + args.round:
                time.sleep(0.2)
            if stop:
                return 0
    finally:
        watcher.stop()


if __name__ == "__main__":
    sys.exit(main())
