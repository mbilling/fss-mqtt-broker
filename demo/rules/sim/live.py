"""The live demo's player: device events sent to a broker that may come and go.

`core.Player` plays one run and gives up at the first error, which is right for the README's
ten minutes. A publisher that runs for days (demo/rules-live) has to outlive the broker
instead: a restart, a rebuild, a laptop that sleeps. This player

- connects each device lazily, on its next event, and when the broker cannot be reached
  waits a capped exponential backoff with jitter before any device tries again, so 51
  devices do not hammer a broker that is down, nor retry in step once it is back;
- on a failed send, closes that device's connection and reconnects it on its next event;
  the message is not retried, as a device that lost its uplink does not resend old
  telemetry;
- reads and discards what the broker sends (PINGRESP) and pings idle connections about
  once a second, whether it is waiting or catching up, so a connection the broker closed is
  noticed while the device is quiet, and a quiet device is not dropped for its keepalive.

It counts what it sent, what it could not send and how many connections it re-made, for the
heartbeat the caller prints. Like `core.Player`, it is driven from one loop and is not
thread-safe.
"""

from __future__ import annotations

import random
import sys
import time
from typing import Callable, Optional

from .core import Event
from .mqtt import Client, MqttError

# How long one attempt may wait for the broker: the TCP connect, the CONNACK, a send.
CONNECT_TIMEOUT = 5.0
# How often idle connections are read and pinged.
TEND_EVERY = 1.0


def log(text: str):
    print(f"live: {text}", file=sys.stderr, flush=True)


class Backoff:
    """Capped exponential backoff with jitter: the n-th failure in a row waits a random time
    between half and all of min(cap, base * 2**n)."""

    def __init__(self, base: float = 0.5, cap: float = 30.0, rng: Optional[random.Random] = None):
        self.base, self.cap = base, cap
        self.rng = rng or random.Random()
        self.failures = 0
        self.until = 0.0

    def ready(self, now: float) -> bool:
        return now >= self.until

    def failed(self, now: float) -> float:
        """Record a failure at `now` and return how long to wait before the next attempt."""
        step = min(self.cap, self.base * 2 ** min(self.failures, 30))
        delay = step * self.rng.uniform(0.5, 1.0)
        self.failures += 1
        self.until = now + delay
        return delay

    def reset(self):
        self.failures = 0
        self.until = 0.0


class LivePlayer:
    """Applies events as they fall due. `on_send(event)` is called after each event sent."""

    def __init__(self, host: str, port: int, on_send: Optional[Callable[[Event], None]] = None):
        self.host, self.port, self.on_send = host, port, on_send
        self.clients: dict[str, Client] = {}
        self.backoff = Backoff()
        self.lost: set[str] = set()  # connections lost, re-made on the device's next event
        self.seen: set[str] = set()  # every device that has had an event
        self.sent = 0  # device messages sent
        self.unsent = 0  # device messages not sent: no connection, or the send failed
        self.reconnects = 0
        self._down_since: Optional[float] = None
        self._next_tend = 0.0
        self._last_loss_log = -60.0

    # ---- events -----------------------------------------------------------------------

    def send(self, ev: Event):
        """Apply an event that is due."""
        self.seen.add(ev.client)
        if ev.kind == "connect":
            old = self.clients.pop(ev.client, None)
            if old:
                old.disconnect()
            self._connect(ev.client, ev.will)
        elif ev.kind == "publish":
            c = self.clients.get(ev.client) or self._connect(ev.client)
            if c is None:
                self.unsent += 1
                return
            try:
                c.publish(ev.topic, ev.payload, ev.qos, ev.retain)
            except (MqttError, OSError) as e:
                self._lose(ev.client, e)
                self.unsent += 1
                return
            self.sent += 1
            if self.on_send:
                self.on_send(ev)
        else:
            # A disconnect or a drop ends the connection whether it is due or skipped.
            self.skip(ev)

    def skip(self, ev: Event):
        """An event that is not sent (it is late, or before where play starts). Nothing goes
        out, but a disconnect or a drop still ends the device's connection, so that it is
        offline where the script has it offline."""
        if ev.kind == "disconnect":
            c = self.clients.pop(ev.client, None)
            if c:
                c.disconnect()
        elif ev.kind == "drop":
            c = self.clients.pop(ev.client, None)
            if c:
                c.drop()
        elif ev.kind not in ("connect", "publish"):
            raise ValueError(f"unknown event kind {ev.kind!r}")

    def begin(self, events: list[Event]):
        """Play is about to start at `events[0]` (a new window, or a jump into one): a device
        whose first event from here is an explicit `connect` is offline until then, as at the
        start of the README's run, so a connection it still has from before is closed now."""
        first: dict[str, str] = {}
        for ev in events:
            first.setdefault(ev.client, ev.kind)
        for client, kind in first.items():
            if kind == "connect" and client in self.clients:
                self.clients.pop(client).disconnect()

    # ---- connections ------------------------------------------------------------------

    def tend(self):
        """Read what the broker sent and ping idle connections, at most once a second."""
        now = time.monotonic()
        if now < self._next_tend:
            return
        self._next_tend = now + TEND_EVERY
        for client, c in list(self.clients.items()):
            try:
                c.drain()
                c.inbox.clear()
                c.ping_if_idle()
            except (MqttError, OSError) as e:
                self._lose(client, e)

    def idle(self, seconds: float):
        """Wait `seconds`, tending the connections."""
        deadline = time.monotonic() + seconds
        while True:
            self.tend()
            left = deadline - time.monotonic()
            if left <= 0:
                return
            time.sleep(min(left, TEND_EVERY))

    def connected(self) -> int:
        return len(self.clients)

    def close(self):
        """Clean DISCONNECTs for every device still connected."""
        for c in list(self.clients.values()):
            c.disconnect()
        self.clients.clear()

    def _connect(self, client: str, will=None) -> Optional[Client]:
        now = time.monotonic()
        if not self.backoff.ready(now):
            return None
        c = Client(self.host, self.port, client, timeout=CONNECT_TIMEOUT)
        try:
            c.connect(will=will)
        except (MqttError, OSError) as e:
            delay = self.backoff.failed(now)
            if self._down_since is None:
                self._down_since = now
                log(f"cannot reach {self.host}:{self.port} ({e}); retrying with backoff")
            elif self.backoff.failures in (5, 10) or self.backoff.failures % 50 == 0:
                log(f"still cannot reach {self.host}:{self.port} after "
                    f"{now - self._down_since:.0f} s ({e}); next try in {delay:.1f} s")
            return None
        self.backoff.reset()
        if self._down_since is not None:
            log(f"reached {self.host}:{self.port} again after {now - self._down_since:.0f} s")
            self._down_since = None
        if client in self.lost:
            self.lost.discard(client)
            self.reconnects += 1
        self.clients[client] = c
        return c

    def _lose(self, client: str, err: Exception):
        c = self.clients.pop(client, None)
        if c:
            c.drop()
        self.lost.add(client)
        now = time.monotonic()
        # One line for a broker that went away, not one per device.
        if now - self._last_loss_log >= 60:
            self._last_loss_log = now
            log(f"lost the connection of {client} ({err}); devices reconnect on their next "
                f"event")
