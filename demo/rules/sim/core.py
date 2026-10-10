"""The event model every domain module produces, and the player that sends it.

A domain module exposes one function:

    def events(rng: random.Random, t0: float, duration: float) -> Iterable[Event]

`rng` is the module's own seeded generator (so one domain's output does not change when
another changes), `t0` the simulated start in Unix seconds (device timestamps are
`t0 + at`), and `duration` how many simulated seconds to cover. It yields events in any
order; the player sorts them by `at`, then by the order they were yielded.

A device connects on its first event unless the module yields an explicit `connect`
(needed for a Will or a non-clean session); `disconnect` is a clean DISCONNECT and `drop`
closes the socket without one, the way a device that loses power does.

A publish with MQTT 5 properties (`props`, as `mqtt.encode_props` takes them) is sent over
an MQTT 5 connection; its device's other publishes must carry them too. A large payload may
be built only when it is sent (`make`, instead of `payload`): the live demo's turbine fast
logs are half a megabyte each, too many to hold or build ahead for a whole window.
"""

from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional

from .mqtt import Client


@dataclass
class Event:
    at: float  # simulated seconds after the start
    client: str  # the device's MQTT client id
    kind: str = "publish"  # publish | connect | disconnect | drop
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    retain: bool = False
    will: Optional[tuple[str, bytes, int, bool]] = None  # for `connect`
    seq: int = field(default=0, compare=False)  # yield order, set by `merge`
    props: Optional[dict] = None  # MQTT 5 publish properties (see mqtt.encode_props)
    make: Optional[Callable[[], bytes]] = field(default=None, compare=False, repr=False)

    def body(self) -> bytes:
        """The payload, built now if it is built when sent."""
        return self.make() if self.make else self.payload


def ts_ms(t0: float, at: float) -> int:
    """A device timestamp in Unix milliseconds."""
    return int(round((t0 + at) * 1000))


def jbytes(obj) -> bytes:
    """Compact JSON, as most devices send it."""
    return json.dumps(obj, separators=(",", ":")).encode()


def merge(streams: list[Iterable[Event]]) -> list[Event]:
    out: list[Event] = []
    for stream in streams:
        for ev in stream:
            ev.seq = len(out)
            out.append(ev)
    out.sort(key=lambda e: (e.at, e.seq))
    return out


def to_json_line(ev: Event) -> str:
    """One fixture line. A payload that is not UTF-8 is written as `payload_b64`."""
    line: dict = {"at": round(ev.at, 3), "client": ev.client, "kind": ev.kind}
    if ev.kind == "publish":
        line.update(topic=ev.topic, qos=ev.qos, retain=ev.retain)
        payload = ev.body()
        try:
            line["payload"] = payload.decode("utf-8")
        except UnicodeDecodeError:
            line["payload_b64"] = base64.b64encode(payload).decode()
        if ev.props is not None:
            line["props"] = {k: ([list(p) for p in v] if k == "user" else v)
                             for k, v in ev.props.items()}
    if ev.kind == "connect" and ev.will:
        topic, payload, qos, retain = ev.will
        line["will"] = {"topic": topic, "payload": payload.decode("utf-8"), "qos": qos, "retain": retain}
    return json.dumps(line, separators=(",", ":"))


def from_json_line(text: str) -> Event:
    d = json.loads(text)
    payload = b""
    if "payload" in d:
        payload = d["payload"].encode("utf-8")
    elif "payload_b64" in d:
        payload = base64.b64decode(d["payload_b64"])
    will = None
    if "will" in d:
        w = d["will"]
        will = (w["topic"], w["payload"].encode("utf-8"), w["qos"], w["retain"])
    props = d.get("props")
    if props is not None and "user" in props:
        props["user"] = [tuple(p) for p in props["user"]]
    return Event(
        at=d["at"], client=d["client"], kind=d["kind"], topic=d.get("topic", ""),
        payload=payload, qos=d.get("qos", 0), retain=d.get("retain", False), will=will,
        props=props,
    )


class Player:
    """Sends events to a broker at `speed` times real time (0 = as fast as possible).

    `on_send(event)` is called just before each event is sent, so a device message is
    reported before anything the broker derives from it.
    """

    def __init__(self, host: str, port: int, speed: float, on_send=None):
        self.host, self.port, self.speed, self.on_send = host, port, speed, on_send
        self.clients: dict[str, Client] = {}

    def play(self, events: list[Event]):
        start = time.monotonic()
        for ev in events:
            if self.speed > 0:
                wait = start + ev.at / self.speed - time.monotonic()
                if wait > 0:
                    self._idle(wait)
            if self.on_send:
                self.on_send(ev)
            self._apply(ev)

    def close(self):
        for c in list(self.clients.values()):
            c.disconnect()
        self.clients.clear()

    def _idle(self, seconds: float):
        deadline = time.monotonic() + seconds
        while True:
            left = deadline - time.monotonic()
            if left <= 0:
                return
            for c in self.clients.values():
                c.ping_if_idle()
            time.sleep(min(left, 1.0))

    def _client(self, ev: Event, will=None) -> Client:
        c = self.clients.get(ev.client)
        if c is None:
            c = Client(self.host, self.port, ev.client, version=5 if ev.props is not None else 4)
            c.connect(will=will)
            self.clients[ev.client] = c
        return c

    def _apply(self, ev: Event):
        if ev.kind == "connect":
            old = self.clients.pop(ev.client, None)
            if old:
                old.disconnect()
            self._client(ev, will=ev.will)
        elif ev.kind == "publish":
            self._client(ev).publish(ev.topic, ev.body(), ev.qos, ev.retain, ev.props)
        elif ev.kind == "disconnect":
            c = self.clients.pop(ev.client, None)
            if c:
                c.disconnect()
        elif ev.kind == "drop":
            c = self.clients.pop(ev.client, None)
            if c:
                c.drop()
        else:
            raise ValueError(f"unknown event kind {ev.kind!r}")
