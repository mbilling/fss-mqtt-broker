"""Play the rule-engine demo's devices into mqttd for as long as it runs, in real time.

    python3 live.py                              # every domain, to 127.0.0.1:1883
    python3 live.py --domains cars --port 1884   # one domain
    python3 live.py --clock fixture              # the README's ten minutes, over and over
    python3 live.py --dry-run --windows 2        # what it would send, as JSON lines

simulate.py plays one run and stops. This plays the same simulation endlessly, in windows of
ten minutes aligned to the wall clock (09:40:00, 09:50:00, ... UTC), so every simulator
started in the same window, and one restarted later, plays the same schedule. Each window is
the README's ten minutes again: the same devices, seed and injected faults, sent at the
window's start plus each event's offset. What the devices' clocks say depends on --clock:

  now      device time is the wall clock. Time of day and season matter, as for
           `simulate.py --start now`: no PV ground fault at night, the EV peak-tariff
           alert only from 17:00 to 21:00 Danish time, heating faults only in the
           heating season.
  fixture  device time replays the README's window (from 2026-03-24T15:55:00Z) every
           time, so every window is the README's run again, the same alerts included.

Started mid-window, it joins the schedule where it is and skips what came before. An event
it cannot send within 5 s of its time (the broker was away, the machine slept) is dropped
and counted, never sent late; more than a window behind, it jumps to where the schedule is
now. When the broker goes away it reconnects with backoff. It prints a start line and a
heartbeat every minute, and on SIGTERM or SIGINT it disconnects every device cleanly and
exits 0 (2 for a bad option). Each option can also be set in the environment: SIM_HOST,
SIM_PORT, SIM_DOMAINS, SIM_SEED, SIM_CLOCK and SIM_QUIET (1 or 0). A flag wins over its
variable.

Device state starts afresh with each window: odometers, meter registers and charge levels
jump back, a vehicle returns to the start of its route, and a faulted turbine is healthy
again. The rules see one message at a time, so this raises no false alert.
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import signal
import sys
import time
from pathlib import Path
from typing import Callable

sys.path.insert(0, str(Path(__file__).resolve().parent))

from simulate import DEFAULT_START, DOMAINS, generate, parse_start, show  # noqa: E402
from sim.core import Event, to_json_line  # noqa: E402
from sim.live import LivePlayer  # noqa: E402

# One window: the README's ten minutes, and the simulation's scripted run.
WINDOW = 600.0
# An event this much later than its time is dropped, not sent.
LATE = 5.0
# The next window is generated while the next event is at least this far away.
LOOKAHEAD = 2.0
FIXTURE_T0 = parse_start(DEFAULT_START)


def anchor(now: float) -> float:
    """S0, the schedule's anchor: `now` floored to a whole window."""
    return math.floor(now / WINDOW) * WINDOW


def plan(now: float, s0: float) -> tuple[int, float]:
    """Where the schedule anchored at `s0` is at wall time `now`: the window k (which starts
    at s0 + k * WINDOW) and the seconds into it."""
    k, offset = divmod(now - s0, WINDOW)
    return int(k), offset


def utc(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class Schedule:
    """The windows to play: window k is sent from s0 + k * WINDOW, and its devices' clocks
    start at t0(k). Generating a window takes up to half a second, so the current and the
    next are kept; in fixture mode every window is the same one."""

    def __init__(self, domains: list[str], seed: int, clock: str, s0: float):
        self.domains, self.seed, self.clock, self.s0 = domains, seed, clock, s0
        self._cache: dict[float, list[Event]] = {}

    def start(self, k: int) -> float:
        return self.s0 + k * WINDOW

    def t0(self, k: int) -> float:
        return self.start(k) if self.clock == "now" else FIXTURE_T0

    def events(self, k: int) -> list[Event]:
        t0 = self.t0(k)
        if t0 not in self._cache:
            self._cache = {t: e for t, e in self._cache.items() if t >= t0 - WINDOW}
            self._cache[t0] = generate(self.domains, self.seed, t0, WINDOW)
        return self._cache[t0]

    def ready(self, k: int) -> bool:
        return self.t0(k) in self._cache


class Live:
    """The loop: wait for each event's time, send it, keep the schedule."""

    def __init__(self, schedule: Schedule, player: LivePlayer, heartbeat: float,
                 stopping: Callable[[], bool], say: Callable[[str], None]):
        self.schedule, self.player, self.heartbeat = schedule, player, heartbeat
        self.stopping, self.say = stopping, say
        self.k = 0
        self.events: list[Event] = []
        self.i = 0
        self.late = 0
        self.replans = 0

    def run(self, clock: Callable[[], float] = time.time):
        now = clock()
        self.seek(now)
        nxt = self.schedule.start(self.k + 1)
        mode = ("device time is the wall clock" if self.schedule.clock == "now" else
                f"device time replays {utc(FIXTURE_T0)} each window")
        self.say(f"{','.join(self.schedule.domains)} to {self.player.host}:{self.player.port}, "
                 f"seed {self.schedule.seed}, clock {self.schedule.clock} ({mode}); "
                 f"window {self.k} began {utc(self.schedule.start(self.k))}, joining it "
                 f"{now - self.schedule.start(self.k):.0f} s in; next window at {utc(nxt)}")
        beat = now + self.heartbeat
        while not self.stopping():
            self.player.tend()
            now = clock()
            if now >= beat:
                self.say(self.status(now))
                beat = now + self.heartbeat
            if self.i == len(self.events):
                self.k += 1
                self.events, self.i = self.schedule.events(self.k), 0
                self.player.begin(self.events)
                continue
            ev = self.events[self.i]
            lag = now - (self.schedule.start(self.k) + ev.at)
            if abs(lag) > WINDOW:
                # A window or more behind (the machine slept, the broker was away for long) or
                # ahead (the clock was set back): rejoin the schedule where it is now.
                self.replans += 1
                was = self.k
                self.seek(now)
                self.say(f"{abs(lag):.0f} s {'behind' if lag > 0 else 'ahead'} in window {was}: "
                         f"rejoining window {self.k} at {now - self.schedule.start(self.k):.0f} s")
                continue
            if lag < 0:
                if -lag > LOOKAHEAD and not self.schedule.ready(self.k + 1):
                    self.schedule.events(self.k + 1)
                    continue
                self.player.idle(min(-lag, 1.0))
                continue
            self.i += 1
            if lag > LATE:
                if ev.kind in ("publish", "connect"):
                    self.late += 1
                self.player.skip(ev)
            else:
                self.player.send(ev)
        self.player.close()
        self.say(f"stopped: {self.status(clock())}")

    def seek(self, now: float):
        """Join the schedule at `now`: the window it is in, from its offset. The events
        before the offset are skipped."""
        self.k, offset = plan(now, self.schedule.s0)
        self.events = self.schedule.events(self.k)
        self.i = 0
        while self.i < len(self.events) and self.events[self.i].at < offset:
            self.player.skip(self.events[self.i])
            self.i += 1
        self.player.begin(self.events[self.i:])

    def status(self, now: float) -> str:
        p = self.player
        into = now - self.schedule.start(self.k)
        return (f"window {self.k} +{into:.0f} s: sent {p.sent}, late {self.late}, unsent "
                f"{p.unsent}, reconnects {p.reconnects}, connected {p.connected()} of "
                f"{len(p.seen)}")


def clock_mode(text: str) -> str:
    if text not in ("now", "fixture"):
        raise argparse.ArgumentTypeError(f"not 'now' or 'fixture': {text!r}")
    return text


def port_number(text: str) -> int:
    try:
        n = int(text)
    except ValueError:
        n = 0
    if not 0 < n < 65536:
        raise argparse.ArgumentTypeError(f"not a port number: {text!r}")
    return n


def positive(convert):
    def check(text: str):
        try:
            v = convert(text)
        except ValueError:
            v = 0
        if not 0 < v < math.inf:
            raise argparse.ArgumentTypeError(f"not a positive number: {text!r}")
        return v
    return check


def switch(text: str) -> bool:
    lowered = text.strip().lower()
    if lowered in ("1", "true", "yes", "on"):
        return True
    if lowered in ("0", "false", "no", "off"):
        return False
    raise argparse.ArgumentTypeError(f"not 1 or 0: {text!r}")


# Option, environment variable, conversion, default.
FROM_ENV = [
    ("host", "SIM_HOST", str, "127.0.0.1"),
    ("port", "SIM_PORT", port_number, 1883),
    ("domains", "SIM_DOMAINS", str, "power,homes,cars"),
    ("seed", "SIM_SEED", int, 7),
    ("clock", "SIM_CLOCK", clock_mode, "now"),
    ("quiet", "SIM_QUIET", switch, False),
]


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", help="the broker's host [SIM_HOST, default 127.0.0.1]")
    ap.add_argument("--port", type=port_number,
                    help="the broker's plaintext MQTT port [SIM_PORT, default 1883]")
    ap.add_argument("--domains",
                    help="comma-separated: power,homes,cars [SIM_DOMAINS, default all three]")
    ap.add_argument("--seed", type=int, help="the random seed [SIM_SEED, default 7]")
    ap.add_argument("--clock", type=clock_mode,
                    help="now or fixture: what the devices' clocks say [SIM_CLOCK, default now]")
    ap.add_argument("--quiet", action="store_true", default=None,
                    help="do not print each device message sent [SIM_QUIET]")
    ap.add_argument("--heartbeat", type=positive(float), default=60.0, metavar="SECONDS",
                    help="seconds between status lines (default 60)")
    ap.add_argument("--dry-run", action="store_true",
                    help="print the events of --windows windows as JSON lines, as simulate.py "
                         "--dry-run prints one run (at is seconds into the window); no broker")
    ap.add_argument("--windows", type=positive(int), metavar="N",
                    help="with --dry-run: how many windows (default 1)")
    ap.add_argument("--now", type=parse_start, metavar="TIME",
                    help="with --dry-run: the wall-clock time to anchor the windows at, ISO 8601 "
                         "(default the current time)")
    args = ap.parse_args(argv)

    for dest, var, convert, default in FROM_ENV:
        if getattr(args, dest) is not None:
            continue
        raw = os.environ.get(var, "")
        if raw == "":
            setattr(args, dest, default)
            continue
        try:
            setattr(args, dest, convert(raw))
        except (ValueError, argparse.ArgumentTypeError) as e:
            ap.error(f"{var}={raw!r}: {e}")
    args.domain_list = [d.strip() for d in args.domains.split(",") if d.strip()]
    unknown = [d for d in args.domain_list if d not in DOMAINS]
    if unknown or not args.domain_list:
        ap.error(f"unknown domain(s): {', '.join(unknown) or '(none given)'}")
    if not args.dry_run:
        for flag in ("windows", "now"):
            if getattr(args, flag) is not None:
                ap.error(f"--{flag} goes with --dry-run")
    return args


def dry_run(args) -> int:
    s0 = anchor(time.time() if args.now is None else args.now)
    schedule = Schedule(args.domain_list, args.seed, args.clock, s0)
    try:
        for k in range(args.windows or 1):
            for ev in schedule.events(k):
                print(to_json_line(ev))
        sys.stdout.flush()
    except BrokenPipeError:  # `--dry-run | head`
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
    return 0


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.dry_run:
        return dry_run(args)

    stop = []

    def on_signal(signum, _frame):
        stop.append(signum)
        signal.signal(signum, signal.SIG_DFL)  # a second one ends the process at once

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    def say(text: str):
        print(f"live: {text}", flush=True)

    def on_send(ev: Event):
        print(show(f"{ev.client:>14} →", ev.topic, ev.payload), flush=True)

    player = LivePlayer(args.host, args.port, None if args.quiet else on_send)
    schedule = Schedule(args.domain_list, args.seed, args.clock, anchor(time.time()))
    Live(schedule, player, args.heartbeat, lambda: bool(stop), say).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
