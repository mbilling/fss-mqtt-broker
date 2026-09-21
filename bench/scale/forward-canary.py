#!/usr/bin/env python3
"""Lane E forwarding positive control: make every broker forward before trusting it at zero.

Usage:
  ssh driver python3 - run --broker HOST:MQTT_PORT:HEALTH_PORT [...] --count K --timeout S < forward-canary.py
  python3 forward-canary.py verify DIR --nodes N --count K
  python3 forward-canary.py --self-test
  python3 forward-canary.py local-proof --mqttd BIN --nodes N [--base-port P] [--capture DIR]

Why this exists (#482 Option B). A healthy prefer-local ladder predicts ~0% crossing,
and mqttd's prometheus-client omits a labelled family that has no children: the
`mqttd_publish_forwarded_total{reason}` series does not exist until a broker's first
forward. So "no forwarded samples" on a rung reads the same whether forwarding is
genuinely idle, broken, unsupported, or the process restarted since the last forward.
A zero we cannot tell from a missing counter is not a measurement.

`run` removes the ambiguity once per cluster size, before calibration and before any
rung. For every ordered broker pair (i, j), i != j, a `$share` group has its ONLY member
on broker j and a publisher on broker i sends it K QoS 0 messages, so broker i must
count exactly K*(N-1) `shared-remote` forwards and every broker must receive and deliver
exactly K*(N-1). `ledger()` re-derives that from the raw scrapes and returns each
broker's post-canary floors; extract-lane-e.py then holds every later snapshot of that
broker to them. A forwarded or received sum below its floor means the counters were
reset — the process restarted — and the rung's zero cannot be certified.

At N=1 there is no peer to forward to: the single local pair (0, 0) still proves the
publish/receive/deliver path and the ledger reports `pass-local`.

`run` is shipped over ssh stdin and executed on the driver as `python3 - run ...`: it
never reads stdin nor needs __file__, and it must compile on the driver's Python 3.8 —
the whole file is compiled there, so the whole file stays 3.8-compatible.
"""
from __future__ import annotations

import argparse
import contextlib
import hashlib
import io
import math
import os
import re
import shutil
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

FORWARDED = "mqttd_publish_forwarded_total"
RECEIVED = "mqttd_publish_received_total"
DELIVERED = "mqttd_publish_delivered_total"
DROPPED = "mqttd_publish_dropped_total"
PEER_LINKS = "mqttd_peer_links"
CLIENTS_HEADER = "origin\tmember\tburst_seen\tburst_distinct"
# A chunk name is a file basename the harness writes verbatim; nothing else is allowed.
CHUNK_NAME = re.compile(r"^[A-Za-z0-9._-]+$")
STREAM_MARK = re.compile(r"(?m)^@@@ ([A-Za-z0-9._-]+)\n")


# ── MQTT 3.1.1 framing (just enough for clean-session QoS 0) ──────────────────


class ProtocolError(RuntimeError):
    """The broker answered, but not with what MQTT 3.1.1 promises for this request."""


class CanaryError(RuntimeError):
    """A run step failed; the message names the broker or pair it failed on."""


class Stopped(BaseException):
    """SIGTERM/SIGHUP: unwind through `finally` so connections still get DISCONNECT."""


DISCONNECT = b"\xe0\x00"


def encode_remaining_length(n: int) -> bytes:
    if n < 0 or n > 268435455:
        raise ValueError("remaining length out of range: %d" % n)
    out = bytearray()
    while True:
        digit, n = n % 128, n // 128
        out.append(digit | (0x80 if n else 0))
        if not n:
            return bytes(out)


def _utf8(text: str) -> bytes:
    raw = text.encode("utf-8")
    if len(raw) > 65535:
        raise ValueError("MQTT string longer than 65535 bytes")
    return struct.pack("!H", len(raw)) + raw


def packet(first: int, body: bytes) -> bytes:
    return bytes([first]) + encode_remaining_length(len(body)) + body


def connect_packet(client_id: str, keepalive: int) -> bytes:
    # Protocol level 4 (3.1.1), connect flags 0x02: clean session, no will, no credentials.
    return packet(0x10, _utf8("MQTT") + b"\x04\x02" + struct.pack("!H", keepalive) + _utf8(client_id))


def subscribe_packet(packet_id: int, topic_filter: str, qos: int = 0) -> bytes:
    return packet(0x82, struct.pack("!H", packet_id) + _utf8(topic_filter) + bytes([qos]))


def publish_packet(topic: str, payload: bytes, qos: int = 0, packet_id: int = 0) -> bytes:
    """A PUBLISH at `qos`. At QoS >= 1 the packet id sits between topic and payload."""
    if qos == 0:
        return packet(0x30, _utf8(topic) + payload)
    return packet(0x30 | (qos << 1), _utf8(topic) + struct.pack("!H", packet_id) + payload)


def puback_packet(packet_id: int) -> bytes:
    return packet(0x40, struct.pack("!H", packet_id))


def publish_id(first: int, body: bytes) -> Optional[int]:
    """The packet id of an inbound PUBLISH, or None at QoS 0."""
    if not (first >> 1) & 3:
        return None
    topic_len = struct.unpack("!H", body[:2])[0]
    return struct.unpack("!H", body[2 + topic_len:4 + topic_len])[0]


def decode_packet(buf: bytes) -> Optional[Tuple[int, bytes, int]]:
    """(first byte, body, bytes consumed) for one whole packet at the front of buf, or None."""
    if len(buf) < 2:
        return None
    length, multiplier, pos = 0, 1, 1
    while True:
        if pos >= len(buf):
            return None
        digit = buf[pos]
        pos += 1
        length += (digit & 0x7F) * multiplier
        if not digit & 0x80:
            break
        multiplier *= 128
        if pos > 4:
            raise ProtocolError("malformed remaining length (more than 4 bytes)")
    if len(buf) < pos + length:
        return None
    return buf[0], bytes(buf[pos:pos + length]), pos + length


def parse_publish(first: int, body: bytes) -> Tuple[str, bytes]:
    if len(body) < 2:
        raise ProtocolError("PUBLISH shorter than its topic length")
    topic_len = struct.unpack("!H", body[:2])[0]
    offset = 2 + topic_len + (2 if (first >> 1) & 3 else 0)
    if len(body) < offset:
        raise ProtocolError("PUBLISH shorter than its topic")
    return body[2:2 + topic_len].decode("utf-8", "replace"), body[offset:]


def _describe(exc: BaseException) -> str:
    text = str(exc) or repr(exc)
    return _one_line("%s: %s" % (type(exc).__name__, text))


def _one_line(text: str) -> str:
    return " ".join(text.replace("\t", " ").split())


class PacketReader:
    """Whole packets off a socket. A read timeout never loses a partial packet: the
    buffer survives, which is what lets a member poll a stop flag between reads."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.buf = bytearray()

    def read(
        self,
        deadline: Optional[float] = None,
        stop: Optional[threading.Event] = None,
        poll: float = 0.2,
    ) -> Optional[Tuple[int, bytes]]:
        while True:
            got = decode_packet(self.buf)
            if got is not None:
                first, body, used = got
                del self.buf[:used]
                return first, body
            if stop is not None and stop.is_set():
                return None
            wait = poll
            if deadline is not None:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise socket.timeout("timed out waiting for a packet")
                wait = min(poll, left)
            self.sock.settimeout(wait)
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                continue
            if not chunk:
                raise EOFError("connection closed by the broker")
            self.buf += chunk


class Conn:
    """One clean-session MQTT 3.1.1 connection."""

    def __init__(self, sock: socket.socket, label: str) -> None:
        self.sock = sock
        self.label = label
        self.reader = PacketReader(sock)
        # PUBLISHes that raced a SUBACK; the Member replays them first.
        self.early = []  # type: List[Tuple[int, bytes]]

    @classmethod
    def open(cls, host: str, port: int, client_id: str, keepalive: int, timeout: float) -> "Conn":
        deadline = time.monotonic() + timeout
        sock = socket.create_connection((host, port), timeout=timeout)
        try:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            conn = cls(sock, client_id)
            conn.send(connect_packet(client_id, keepalive), timeout)
            got = conn.reader.read(deadline=deadline)
            assert got is not None
            first, body = got
            if first >> 4 != 2 or len(body) != 2:
                raise ProtocolError("expected CONNACK, got packet type %d" % (first >> 4))
            if body[1] != 0:
                raise ProtocolError("CONNACK refused with return code %d" % body[1])
            return conn
        except BaseException:
            sock.close()
            raise

    def send(self, data: bytes, timeout: float) -> None:
        self.sock.settimeout(timeout)
        self.sock.sendall(data)

    def subscribe(self, filters: Sequence[str], timeout: float, qos: int = 0) -> None:
        deadline = time.monotonic() + timeout
        for packet_id, topic_filter in enumerate(filters, 1):
            self.send(subscribe_packet(packet_id, topic_filter, qos), max(0.1, deadline - time.monotonic()))
            while True:
                got = self.reader.read(deadline=deadline)
                assert got is not None
                first, body = got
                kind = first >> 4
                if kind == 3:
                    self.early.append((first, body))
                    continue
                if kind != 9 or len(body) != 3:
                    raise ProtocolError("expected SUBACK for %s, got packet type %d" % (topic_filter, kind))
                acked = struct.unpack("!H", body[:2])[0]
                if acked != packet_id:
                    raise ProtocolError("SUBACK for packet id %d, expected %d" % (acked, packet_id))
                if body[2] > 2:
                    raise ProtocolError("SUBACK refused %s with return code 0x%02x" % (topic_filter, body[2]))
                # A downgrade is not a refusal, and it is the one answer that would
                # let this control certify a different QoS than the rung it guards.
                if body[2] != qos:
                    raise ProtocolError(
                        "SUBACK granted QoS %d for %s, asked for %d — the control would certify a "
                        "different path than the rung" % (body[2], topic_filter, qos))
                break

    def disconnect(self) -> None:
        with contextlib.suppress(OSError):
            self.sock.settimeout(2.0)
            self.sock.sendall(DISCONNECT)
        with contextlib.suppress(OSError):
            self.sock.shutdown(socket.SHUT_RDWR)
        with contextlib.suppress(OSError):
            self.sock.close()


class Member(threading.Thread):
    """Reads one subscriber connection and records every payload by topic."""

    def __init__(self, conn: Conn, label: str, qos: int = 0) -> None:
        threading.Thread.__init__(self, name="member-" + label, daemon=True)
        self.conn = conn
        self.label = label
        self.qos = qos
        self.stop = threading.Event()
        self.error = None  # type: Optional[str]
        self._lock = threading.Lock()
        self._seen = {}  # type: Dict[str, List[str]]
        self._acked = 0

    def _record(self, first: int, body: bytes) -> None:
        topic, payload = parse_publish(first, body)
        with self._lock:
            self._seen.setdefault(topic, []).append(payload.decode("ascii", "replace"))
        # ACKNOWLEDGE, or the ledger is not a ledger. An unacked QoS 1 delivery
        # stays in the broker's outbound inflight window and is REDELIVERED on
        # timeout, so `delivered_delta` would climb past the K*(N-1) this control
        # asserts — the canary would fail on its own silence rather than on any
        # forwarding defect.
        if self.qos >= 1:
            pid = publish_id(first, body)
            if pid is not None:
                with contextlib.suppress(OSError):
                    self.conn.send(puback_packet(pid), 5.0)

    def run(self) -> None:
        try:
            for first, body in self.conn.early:
                self._record(first, body)
            while not self.stop.is_set():
                got = self.conn.reader.read(stop=self.stop)
                if got is None:
                    return
                if got[0] >> 4 == 3:
                    self._record(*got)
                elif got[0] >> 4 == 4:  # PUBACK for something this connection published
                    with self._lock:
                        self._acked += 1
        except Exception as exc:  # noqa: BLE001 - reported through .error, never swallowed
            if not self.stop.is_set():
                self.error = _describe(exc)

    def acked(self) -> int:
        """PUBACKs this connection has seen (publishers only)."""
        with self._lock:
            return self._acked

    def payloads(self, topic: str) -> List[str]:
        with self._lock:
            return list(self._seen.get(topic, ()))

    def close(self) -> None:
        self.stop.set()
        if self.is_alive():
            self.join(2.0)
        self.conn.disconnect()


def scrape(host: str, port: int, timeout: float, path: str = "/metrics") -> str:
    request = urllib.request.Request("http://%s:%d%s" % (host, port, path), headers={"Connection": "close"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", "replace")


def ends_with_eof(text: str) -> bool:
    lines = [line for line in text.splitlines() if line.strip()]
    return bool(lines) and lines[-1] == "# EOF"


# ── run (executed on a driver over ssh stdin) ─────────────────────────────────


def parse_broker(spec: str) -> Tuple[str, int, int]:
    parts = spec.rsplit(":", 2)
    if len(parts) != 3 or not parts[0]:
        raise ValueError("broker must be HOST:MQTT_PORT:HEALTH_PORT, got %r" % spec)
    host = parts[0][1:-1] if parts[0].startswith("[") and parts[0].endswith("]") else parts[0]
    try:
        mqtt_port, health_port = int(parts[1]), int(parts[2])
    except ValueError:
        raise ValueError("broker ports must be integers, got %r" % spec) from None
    for port in (mqtt_port, health_port):
        if not 0 < port < 65536:
            raise ValueError("broker port out of range in %r" % spec)
    return host, mqtt_port, health_port


class Emitter:
    """Writes `\\n@@@ <name>\\n<content>` chunks as they are produced, so an interrupted
    run still leaves the scrapes it took. A dead stdout (ssh gone) is noted, not fatal:
    the connections must still be closed."""

    def __init__(self, stream: io.RawIOBase) -> None:
        self.stream = stream
        self.broken = False

    def emit(self, name: str, content: str) -> None:
        if not CHUNK_NAME.match(name):
            raise ValueError("unsafe chunk name %r" % name)
        if self.broken:
            return
        try:
            self.stream.write(("\n@@@ %s\n%s" % (name, content)).encode("utf-8", "replace"))
            self.stream.flush()
        except (OSError, ValueError):
            self.broken = True


def split_stream(text: str) -> List[Tuple[str, str]]:
    """Inverse of Emitter: exact chunk contents. The newline before each marker is framing."""
    marks = list(STREAM_MARK.finditer(text))
    if not marks:
        if text.strip():
            raise ValueError("canary output has no @@@ chunks")
        return []
    if text[:marks[0].start()].strip():
        raise ValueError("stray canary output before the first @@@ chunk")
    out = []
    for n, mark in enumerate(marks):
        last = n + 1 == len(marks)
        body = text[mark.end():len(text) if last else marks[n + 1].start()]
        if not last:
            body = body[:-1]
        out.append((mark.group(1), body))
    return out


def run(brokers: Sequence[Tuple[str, int, int]], count: int, timeout: float, stream: io.RawIOBase,
        qos: int = 0) -> int:
    nodes = len(brokers)
    local = nodes == 1
    pairs = [(0, 0)] if local else [(i, j) for i in range(nodes) for j in range(nodes) if i != j]
    nonce = "%08x%s" % (int(time.time()) & 0xFFFFFFFF, os.urandom(5).hex())
    started = time.monotonic()
    deadline = started + timeout
    # Keepalive past the whole run: neither side may time a connection out mid-canary.
    keepalive = min(65535, int(math.ceil(timeout)) + 120)
    emitter = Emitter(stream)
    events = []  # type: List[str]
    members = {}  # type: Dict[int, Member]
    publishers = {}  # type: Dict[int, Conn]
    # Declared beside the other connection tables, not where it is filled: the
    # teardown below reads it, and a failure during SUBSCRIBE would otherwise
    # raise NameError there and bury the error that actually happened.
    pub_readers = {}  # type: Dict[int, Member]
    status = "error"

    def note(text: str) -> None:
        events.append("%.3f\t%s" % (time.monotonic() - started, _one_line(text)))

    def topic(i: int, j: int) -> str:
        return "fss-canary/%s/%d/%d" % (nonce, i, j)

    def budget(cap: float, what: str) -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise CanaryError("timeout (%gs) before %s" % (timeout, what))
        return min(cap, left)

    def pair_name(pair: Tuple[int, int]) -> str:
        return "broker%d->broker%d" % pair

    def seen(pair: Tuple[int, int], accept: Callable[[str], bool]) -> bool:
        member = members.get(pair[1])
        return member is not None and any(accept(p) for p in member.payloads(topic(*pair)))

    def check_members() -> None:
        for j, member in sorted(members.items()):
            if member.error:
                raise CanaryError("subscriber on broker%d failed: %s" % (j, member.error))

    def wait_all(accept: Callable[[str], bool], what: str, window: Optional[float] = None) -> List[Tuple[int, int]]:
        """Pairs still missing `what` when the window (or the run deadline) closes."""
        until = deadline if window is None else min(deadline, time.monotonic() + window)
        while True:
            check_members()
            missing = [p for p in pairs if not seen(p, accept)]
            if not missing or time.monotonic() >= until:
                return missing
            time.sleep(0.02)

    def burst_count(pair: Tuple[int, int]) -> Tuple[int, int]:
        member = members.get(pair[1])
        burst = [p for p in member.payloads(topic(*pair)) if p.startswith("burst-")] if member else []
        return len(burst), len(set(burst))

    def scrape_all(label: str) -> None:
        for i, (host, _, health) in enumerate(brokers):
            text, problem = "", ""
            for attempt in range(3):
                try:
                    text = scrape(host, health, max(2.0, min(10.0, deadline - time.monotonic())))
                    problem = "" if ends_with_eof(text) else "truncated scrape (no # EOF)"
                except (OSError, urllib.error.URLError) as exc:
                    problem = _describe(exc)
                if not problem or (attempt and time.monotonic() >= deadline):
                    break
                note("scrape %s broker%d attempt %d failed: %s" % (label, i, attempt + 1, problem))
                time.sleep(0.2)
            if text:
                emitter.emit("metrics-%s-broker%d.prom" % (label, i), text)
            if problem:
                raise CanaryError("scrape %s broker%d (%s:%d) failed: %s" % (label, i, host, health, problem))
            note("scraped %s broker%d (%d bytes)" % (label, i, len(text)))

    try:
        for j in sorted({j for _, j in pairs}):
            host, mqtt_port, _ = brokers[j]
            filters = ["$share/fsscanary-%d-%d/%s" % (i, jj, topic(i, jj)) for i, jj in pairs if jj == j]
            try:
                conn = Conn.open(host, mqtt_port, "fsscanary-s%d-%s" % (j, nonce), keepalive, budget(10.0, "subscribing"))
            except (OSError, EOFError, ProtocolError) as exc:
                raise CanaryError("subscriber connect to broker%d (%s:%d) failed: %s" % (j, host, mqtt_port, _describe(exc))) from None
            member = Member(conn, "broker%d" % j, qos)
            members[j] = member  # registered before SUBACKs so a failure still disconnects it
            try:
                conn.subscribe(filters, budget(10.0, "subscribing"), qos)
            except (OSError, EOFError, ProtocolError) as exc:
                raise CanaryError("subscribe on broker%d (%s:%d) failed: %s" % (j, host, mqtt_port, _describe(exc))) from None
            member.start()
            note("subscriber broker%d holds %d groups" % (j, len(filters)))
        for i in sorted({i for i, _ in pairs}):
            host, mqtt_port, _ = brokers[i]
            try:
                publishers[i] = Conn.open(host, mqtt_port, "fsscanary-p%d-%s" % (i, nonce), keepalive, budget(10.0, "publishing"))
            except (OSError, EOFError, ProtocolError) as exc:
                raise CanaryError("publisher connect to broker%d (%s:%d) failed: %s" % (i, host, mqtt_port, _describe(exc))) from None
            note("publisher broker%d connected" % i)
        note("subscribed %d directed pairs%s" % (len(pairs), " (local mode)" if local else ""))

        # At QoS >= 1 the broker answers every publish with a PUBACK on the
        # publisher's own socket, and nothing here reads it: the buffer fills and
        # the control stalls on a write it cannot explain. A reader per publisher
        # drains them AND counts them, which turns the silence into a check —
        # every message this control published was acknowledged before the post
        # scrape is taken.
        published = {}  # type: Dict[int, int]
        next_pid = {}  # type: Dict[int, int]
        if qos >= 1:
            for i in sorted(publishers):
                reader = Member(publishers[i], "pub-broker%d" % i)
                pub_readers[i] = reader
                reader.start()

        def pids(i: int, n: int) -> List[int]:
            out = []
            for _ in range(n):
                nxt = next_pid.get(i, 0) % 65535 + 1
                next_pid[i] = nxt
                out.append(nxt)
            return out

        def publish(i: int, packets: Sequence[bytes], what: str) -> None:
            if not packets:
                return
            published[i] = published.get(i, 0) + len(packets)
            try:
                publishers[i].send(b"".join(packets), budget(10.0, what))
            except OSError as exc:
                raise CanaryError("publish %s on broker%d failed: %s" % (what, i, _describe(exc))) from None

        # PILOT. Remote $share interest propagates asynchronously; a publish before it
        # lands is received and silently unrouted. Re-pilot only the pairs still dark,
        # until every pair has delivered once. All of it happens before the pre scrape.
        rounds = 0
        while True:
            dark = wait_all(lambda p: p.startswith("pilot-"), "pilot", window=0.0 if rounds == 0 else 0.1)
            if not dark:
                break
            if time.monotonic() >= deadline:
                for pair in dark:
                    note("pair %s: no pilot delivered after %d rounds" % (pair_name(pair), rounds))
                raise CanaryError("interest never propagated: %d of %d pairs dark after %d pilot rounds" % (len(dark), len(pairs), rounds))
            for i in sorted({i for i, _ in dark}):
                tgt = [j for ii, j in dark if ii == i]
                publish(i, [publish_packet(topic(i, j), b"pilot-%d" % rounds, qos, pid)
                            for j, pid in zip(tgt, pids(i, len(tgt)))], "pilot")
            rounds += 1
        note("pilot delivered on every pair after %d rounds" % rounds)
        for i in sorted(publishers):
            tgt = [j for ii, j in pairs if ii == i]
            publish(i, [publish_packet(topic(i, j), b"pilot-end", qos, pid)
                        for j, pid in zip(tgt, pids(i, len(tgt)))], "pilot-end")
        missing = wait_all(lambda p: p == "pilot-end", "pilot-end")
        if missing:
            for pair in missing:
                note("pair %s: pilot-end not delivered" % pair_name(pair))
            raise CanaryError("pilot-end not delivered on %d of %d pairs" % (len(missing), len(pairs)))
        note("pilot-end delivered on every pair")
        scrape_all("pre")

        # BURST. K messages per pair, interleaved across pairs one round at a time and
        # lightly paced, so the control measures forwarding rather than a QoS 0 queue.
        for s in range(count):
            payload = b"burst-end" if s == count - 1 else b"burst-%d" % s
            for i in sorted(publishers):
                tgt = [j for ii, j in pairs if ii == i]
                publish(i, [publish_packet(topic(i, j), payload, qos, pid)
                            for j, pid in zip(tgt, pids(i, len(tgt)))], "burst")
            time.sleep(0.001)
        note("published %d per pair" % count)
        missing = wait_all(lambda p: p == "burst-end", "burst-end")
        if missing:
            for pair in missing:
                note("pair %s: burst-end not delivered (burst_seen=%d burst_distinct=%d)" % ((pair_name(pair),) + burst_count(pair)))
            raise CanaryError("burst-end not delivered on %d of %d pairs" % (len(missing), len(pairs)))
        note("burst-end delivered on every pair")
        if qos >= 1:
            want = sum(published.values())
            until = min(deadline, time.monotonic() + 30.0)
            while True:
                have = sum(r.acked() for r in pub_readers.values())
                if have >= want or time.monotonic() >= until:
                    break
                time.sleep(0.02)
            check_members()
            if have < want:
                for i in sorted(pub_readers):
                    note("broker%d: %d of %d publishes acknowledged"
                         % (i, pub_readers[i].acked(), published.get(i, 0)))
                raise CanaryError(
                    "the brokers acknowledged %d of %d QoS %d publishes — an unacked publish is one "
                    "the broker has not taken responsibility for, so the ledger below would count "
                    "deliveries of messages that may still be redelivered" % (have, want, qos))
            note("every one of %d QoS %d publishes acknowledged" % (want, qos))
        scrape_all("post")
        status = "complete"
    except (Exception, Stopped, KeyboardInterrupt) as exc:
        note("ERROR " + (str(exc) if isinstance(exc, CanaryError) else _describe(exc)))
    finally:
        closed = 0
        for member in members.values():
            member.stop.set()
        for i in sorted(pub_readers):
            # Stop the reader before its socket closes under it, or a drained
            # publisher reports a teardown race as a canary failure.
            pub_readers[i].stop.set()
            if pub_readers[i].is_alive():
                pub_readers[i].join(2.0)
        for i in sorted(publishers):
            publishers[i].disconnect()
            closed += 1
        for j in sorted(members):
            members[j].close()
            closed += 1
        note("sent DISCONNECT on %d connections" % closed)
    rows = [CLIENTS_HEADER]
    for pair in pairs:
        rows.append("broker%d\tbroker%d\t%d\t%d" % (pair + burst_count(pair)))
    emitter.emit("clients.tsv", "\n".join(rows) + "\n")
    trailer = ["status\t%s" % status, "count\t%d" % count, "nodes\t%d" % nodes, "nonce\t%s" % nonce]
    emitter.emit("timeline.tsv", "\n".join(events + trailer) + "\n")
    return 0 if status == "complete" else 3


# ── the ledger (imported by extract-lane-e.py) ────────────────────────────────

SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)\s*$")
LABEL = re.compile(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"\s*(,|$)')

SeriesKey = Tuple[str, Tuple[Tuple[str, str], ...]]


class Snapshot:
    """One validated OpenMetrics scrape: exact series -> value."""

    def __init__(self, samples: Dict[SeriesKey, float], name: str) -> None:
        self.samples = samples
        self.name = name

    def family(self, metric: str) -> float:
        return sum(v for (n, _), v in self.samples.items() if n == metric)

    def series(self, metric: str, **labels: str) -> Optional[float]:
        return self.samples.get((metric, tuple(sorted(labels.items()))))


def _labels(raw: str) -> Optional[Tuple[Tuple[str, str], ...]]:
    pairs, pos = [], 0
    while pos < len(raw):
        m = LABEL.match(raw, pos)
        if not m or m.end() == pos:
            return None
        pairs.append((m.group(1), m.group(2)))
        pos = m.end()
    return tuple(sorted(pairs))


def parse_snapshot(text: str, name: str) -> Snapshot:
    lines = text.splitlines()
    content = [line for line in lines if line.strip()]
    if not content:
        raise ValueError("%s is empty" % name)
    if content[-1] != "# EOF":
        raise ValueError("%s does not end in # EOF (truncated scrape)" % name)
    if content.count("# EOF") != 1:
        raise ValueError("%s holds more than one # EOF" % name)
    samples = {}  # type: Dict[SeriesKey, float]
    for line in content:
        if line.startswith("#"):
            continue
        m = SAMPLE.match(line)
        labels = _labels(m.group(2) or "") if m else None
        if m is None or labels is None:
            raise ValueError("%s has a malformed sample: %s" % (name, line))
        try:
            value = float(m.group(3))
        except ValueError:
            raise ValueError("%s has a malformed value: %s" % (name, line)) from None
        key = (m.group(1), labels)
        if key in samples:
            raise ValueError("%s repeats a series: %s" % (name, line))
        samples[key] = value
    return Snapshot(samples, name)


def read_snapshot(path: Path) -> Snapshot:
    if not path.is_file():
        raise ValueError("%s is missing" % path.name)
    return parse_snapshot(path.read_text(encoding="utf-8", errors="replace"), path.name)


def _num(value: Optional[float]) -> str:
    if value is None:
        return "absent"
    return "%d" % value if float(value).is_integer() else repr(float(value))


def _read_timeline(path: Path) -> Dict[str, str]:
    keys = {}  # type: Dict[str, str]
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        head, _, rest = line.partition("\t")
        if head in ("status", "count", "nodes", "nonce"):
            keys[head] = rest.strip()
    return keys


def ledger_report(directory: object, nodes: int, count: int) -> Tuple[str, Dict[int, Dict[str, float]], List[str], List[str]]:
    """(status, floors, per-broker lines, errors) — `verify` prints it, `ledger` raises on it."""
    d = Path(str(directory))
    errors = []  # type: List[str]
    lines = []  # type: List[str]
    floors = {}  # type: Dict[int, Dict[str, float]]
    if nodes < 1 or count < 1:
        return "fail", floors, lines, ["nodes and count must be positive (nodes=%d count=%d)" % (nodes, count)]
    if not d.is_dir():
        return "fail", floors, lines, ["canary directory %s is missing" % d]
    local = nodes == 1
    per = count if local else count * (nodes - 1)

    try:
        keys = _read_timeline(d / "timeline.tsv")
        if keys.get("status") != "complete":
            errors.append("timeline.tsv status=%s, want complete" % keys.get("status", "absent"))
        if keys.get("nodes") != str(nodes):
            errors.append("timeline.tsv nodes=%s, want %d" % (keys.get("nodes", "absent"), nodes))
        if keys.get("count") != str(count):
            errors.append("timeline.tsv count=%s, want %d" % (keys.get("count", "absent"), count))
    except OSError:
        errors.append("timeline.tsv is missing")

    want_pairs = [(0, 0)] if local else [(i, j) for i in range(nodes) for j in range(nodes) if i != j]
    try:
        rows = [r for r in (d / "clients.tsv").read_text(encoding="utf-8", errors="replace").splitlines() if r.strip()]
        if not rows or rows[0] != CLIENTS_HEADER:
            errors.append("clients.tsv header is not '%s'" % CLIENTS_HEADER.replace("\t", " "))
            rows = rows[1:] if rows and rows[0].startswith("origin") else rows
        else:
            rows = rows[1:]
        if len(rows) != len(want_pairs):
            errors.append("clients.tsv has %d pair rows, want %d" % (len(rows), len(want_pairs)))
        got_pairs = set()
        for row in rows:
            cols = row.split("\t")
            m = [re.match(r"^broker(\d+)$", c) for c in cols[:2]]
            if len(cols) != 4 or not all(m) or not cols[2].isdigit() or not cols[3].isdigit():
                errors.append("clients.tsv malformed row: %s" % _one_line(row))
                continue
            pair = (int(m[0].group(1)), int(m[1].group(1)))
            name = "%s->%s" % (cols[0], cols[1])
            if pair not in want_pairs:
                errors.append("clients.tsv pair %s is not a pair of a %d-node canary" % (name, nodes))
            elif pair in got_pairs:
                errors.append("clients.tsv repeats pair %s" % name)
            got_pairs.add(pair)
            seen_n, distinct_n = int(cols[2]), int(cols[3])
            if seen_n != count or distinct_n != count:
                errors.append("pair %s member saw burst_seen=%d burst_distinct=%d, want %d" % (name, seen_n, distinct_n, count))
        for pair in want_pairs:
            if pair not in got_pairs:
                errors.append("clients.tsv is missing pair broker%d->broker%d" % pair)
    except OSError:
        errors.append("clients.tsv is missing")

    for extra in sorted(d.glob("metrics-*-broker*.prom")):
        m = re.match(r"^metrics-(pre|post)-broker(\d+)\.prom$", extra.name)
        if m and int(m.group(2)) >= nodes:
            errors.append("%s belongs to a broker beyond nodes=%d" % (extra.name, nodes))

    for i in range(nodes):
        snaps = {}  # type: Dict[str, Snapshot]
        for label in ("pre", "post"):
            try:
                snaps[label] = read_snapshot(d / ("metrics-%s-broker%d.prom" % (label, i)))
            except (OSError, ValueError) as exc:
                errors.append("broker%d: %s" % (i, exc))
        if len(snaps) != 2:
            lines.append("broker%d forwarded_post=absent received_post=absent shared_remote_delta=absent received_delta=absent "
                         "delivered_delta=absent dropped_delta=absent subscriber_remote_delta=absent peer_links=absent" % i)
            continue
        pre, post = snaps["pre"], snaps["post"]
        sr_pre = pre.series(FORWARDED, reason="shared-remote")
        sr_post = post.series(FORWARDED, reason="shared-remote")
        or_pre = pre.series(FORWARDED, reason="subscriber-remote")
        or_post = post.series(FORWARDED, reason="subscriber-remote")
        links = post.series(PEER_LINKS)
        links_pre = pre.series(PEER_LINKS)
        # Absent in a validated, EOF-terminated scrape means the lazy family has no child yet: zero.
        deltas = [
            ("received_delta", post.family(RECEIVED) - pre.family(RECEIVED), per),
            ("delivered_delta", post.family(DELIVERED) - pre.family(DELIVERED), per),
            ("dropped_delta", post.family(DROPPED) - pre.family(DROPPED), 0),
            ("subscriber_remote_delta", (or_post or 0.0) - (or_pre or 0.0), 0),
            ("shared_remote_delta", (sr_post or 0.0) - (sr_pre or 0.0), 0 if local else per),
        ]
        if not local and sr_post is None:
            errors.append('broker%d: %s{reason="shared-remote"} absent after the canary' % (i, FORWARDED))
        for key, got, want in deltas:
            if got != want:
                errors.append("broker%d: %s=%s, want %d" % (i, key, _num(got), want))
        # The burst ran between these two scrapes: the mesh must be whole at both ends.
        for edge, value in (("pre", links_pre), ("post", links)):
            if value is None:
                errors.append("broker%d: %s absent from the %s scrape" % (i, PEER_LINKS, edge))
            elif value != nodes - 1:
                errors.append("broker%d: %s peer_links=%s, want %d" % (i, edge, _num(value), nodes - 1))
        floors[i] = {"forwarded": 0.0 if local else (sr_post or 0.0), "received": post.family(RECEIVED)}
        values = dict((k, g) for k, g, _ in deltas)
        lines.append(
            "broker%d forwarded_post=%s received_post=%s shared_remote_delta=%s received_delta=%s "
            "delivered_delta=%s dropped_delta=%s subscriber_remote_delta=%s peer_links=%s"
            % (i, _num(sr_post), _num(post.family(RECEIVED)), _num(values["shared_remote_delta"]),
               _num(values["received_delta"]), _num(values["delivered_delta"]), _num(values["dropped_delta"]),
               _num(values["subscriber_remote_delta"]), _num(links))
        )
    status = "fail" if errors else ("pass-local" if local else "pass")
    return status, floors, lines, errors


def ledger(directory: object, nodes: int, count: int) -> Dict[str, object]:
    """{"status": "pass"|"pass-local", "floors": {i: {"forwarded", "received"}}}; ValueError otherwise."""
    status, floors, _, errors = ledger_report(directory, nodes, count)
    if errors:
        raise ValueError("forwarding positive control failed (%d): %s" % (len(errors), "; ".join(errors)))
    return {"status": status, "floors": floors}


def floor_check(snapshot: object, floor: Dict[str, float], nodes: int, broker: str = "broker") -> List[str]:
    """Errors when a later snapshot of the same broker sits below its canary floor.

    Counters never go down while a process lives, so below-floor means a restart
    since the canary, and a zero crossing read from it proves nothing."""
    try:
        snap = snapshot if isinstance(snapshot, Snapshot) else read_snapshot(Path(str(snapshot)))
    except (OSError, ValueError) as exc:
        return ["%s: %s" % (broker, exc)]
    errors = []
    forwarded = snap.series(FORWARDED, reason="shared-remote")
    if nodes > 1 and forwarded is None:
        errors.append('%s %s: %s{reason="shared-remote"} absent, canary floor %s (restarted since the canary?)'
                      % (broker, snap.name, FORWARDED, _num(floor["forwarded"])))
    elif (forwarded or 0.0) < floor["forwarded"]:
        errors.append("%s %s: forwarded{shared-remote}=%s below canary floor %s (counter reset)"
                      % (broker, snap.name, _num(forwarded or 0.0), _num(floor["forwarded"])))
    received = snap.family(RECEIVED)
    if received < floor["received"]:
        errors.append("%s %s: received=%s below canary floor %s (counter reset)"
                      % (broker, snap.name, _num(received), _num(floor["received"])))
    return errors


def verify(directory: str, nodes: int, count: int, out: Optional[io.TextIOBase] = None) -> int:
    out = out or sys.stdout
    status, _, lines, errors = ledger_report(directory, nodes, count)
    print("status=%s nodes=%d count=%d" % (status, nodes, count), file=out)
    for line in lines:
        print(line, file=out)
    for error in errors:
        print("error " + error, file=out)
    return 1 if errors else 0


# ── local-proof: the canary against N real local mqttd processes ─────────────


class ProofFailure(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _port_free(port: int) -> bool:
    for kind in (socket.SOCK_STREAM, socket.SOCK_DGRAM):
        probe = socket.socket(socket.AF_INET, kind)
        try:
            probe.bind(("127.0.0.1", port))
        except OSError:
            return False
        finally:
            probe.close()
    return True


def _free_ports(count: int) -> List[int]:
    """Ports free for both TCP and UDP, held open until all are chosen so none repeats."""
    held, ports = [], []  # type: List[socket.socket], List[int]
    try:
        while len(ports) < count:
            tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            tcp.bind(("127.0.0.1", 0))
            port = tcp.getsockname()[1]
            udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                udp.bind(("127.0.0.1", port))
            except OSError:
                tcp.close()
                udp.close()
                continue
            held += [tcp, udp]
            ports.append(port)
    finally:
        for sock in held:
            sock.close()
    return ports


class LocalCluster:
    """N loopback brokers: anonymous plaintext MQTT, SWIM with a fresh key, plaintext peer
    bus, durable plane on its default with a per-node data dir — the campaign's topology
    minus TLS. Node 0 is the founder; the restart negative kills the last node, which at
    N=1 is the founder itself."""

    def __init__(self, binary: Path, nodes: int, root: Path, base_port: Optional[int]) -> None:
        self.binary, self.nodes, self.root = binary, nodes, root
        if base_port is None:
            flat = _free_ports(4 * nodes)
        else:
            flat = [base_port + k for k in range(4 * nodes)]
            if flat[-1] > 65535:
                raise ProofFailure("--base-port %d leaves no room for %d ports" % (base_port, 4 * nodes))
            busy = [p for p in flat if not _port_free(p)]
            if busy:
                raise ProofFailure("ports already in use: %s" % " ".join(map(str, busy)))
        # Per node: mqtt, health, peer, swim.
        self.ports = [tuple(flat[4 * i:4 * i + 4]) for i in range(nodes)]
        self.key = os.urandom(32).hex()
        self.procs = {}  # type: Dict[int, subprocess.Popen]

    def mqtt(self, i: int) -> int:
        return self.ports[i][0]

    def health(self, i: int) -> int:
        return self.ports[i][1]

    def broker_specs(self) -> List[str]:
        return ["127.0.0.1:%d:%d" % (self.mqtt(i), self.health(i)) for i in range(self.nodes)]

    def spawn(self, i: int) -> None:
        mqtt_port, health_port, peer_port, swim_port = self.ports[i]
        data = self.root / ("n%d" % i)
        data.mkdir(parents=True, exist_ok=True)
        env = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
            "HOME": str(self.root),
            "MQTTD_NODE_ID": "node%d" % i,
            "MQTTD_PLAINTEXT_BIND": "127.0.0.1:%d" % mqtt_port,
            "MQTTD_ALLOW_ANONYMOUS": "1",
            "MQTTD_PEER_BIND": "127.0.0.1:%d" % peer_port,
            "MQTTD_PEER_ADVERTISE": "127.0.0.1:%d" % peer_port,
            "MQTTD_SWIM_BIND": "127.0.0.1:%d" % swim_port,
            "MQTTD_SWIM_KEY": self.key,
            "MQTTD_DATA_DIR": str(data),
            "MQTTD_HEALTH_BIND": "127.0.0.1:%d" % health_port,
            "MQTTD_READY_MIN_MEMBERS": str(self.nodes),
            "MQTTD_SHUTDOWN_GRACE": "0",
            "RUST_LOG": "warn",
        }
        if i:
            env["MQTTD_SWIM_SEEDS"] = ",".join("127.0.0.1:%d" % self.ports[j][3] for j in range(self.nodes) if j != i)
        log = (self.root / ("n%d.log" % i)).open("ab")
        try:
            self.procs[i] = subprocess.Popen([str(self.binary)], env=env, stdout=log, stderr=subprocess.STDOUT,
                                             stdin=subprocess.DEVNULL, start_new_session=True)
        finally:
            log.close()

    def pid(self, i: int) -> int:
        return self.procs[i].pid

    def _get(self, i: int, path: str) -> Optional[str]:
        try:
            return scrape("127.0.0.1", self.health(i), 2.0, path)
        except (OSError, urllib.error.URLError):
            return None

    def scrape(self, i: int, name: str) -> Tuple[str, Snapshot]:
        """The exposition text exactly as served, and its validated parse."""
        text = self._get(i, "/metrics")
        if text is None:
            raise ProofFailure("broker%d /metrics unreachable" % i)
        return text, parse_snapshot(text, name)

    def snapshot(self, i: int, name: str) -> Snapshot:
        return self.scrape(i, name)[1]

    def _check_alive(self) -> None:
        for i, proc in sorted(self.procs.items()):
            if proc.poll() is not None:
                raise ProofFailure("broker%d exited with %s; log tail:\n%s" % (i, proc.returncode, self.log_tail(i)))

    def log_tail(self, i: int, lines: int = 15) -> str:
        with contextlib.suppress(OSError):
            return "\n".join((self.root / ("n%d.log" % i)).read_text(errors="replace").splitlines()[-lines:])
        return ""

    def wait(self, what: str, seconds: float, predicate: Callable[[], bool]) -> float:
        started = time.monotonic()
        while True:
            self._check_alive()
            if predicate():
                return time.monotonic() - started
            if time.monotonic() - started > seconds:
                raise ProofFailure("timed out after %gs waiting for %s" % (seconds, what))
            time.sleep(0.25)

    def ready_mesh(self) -> bool:
        for i in range(self.nodes):
            if self._get(i, "/readyz") is None:
                return False
            text = self._get(i, "/metrics")
            try:
                links = parse_snapshot(text, "readiness").series(PEER_LINKS) if text else None
            except ValueError:
                return False
            if links != self.nodes - 1:
                return False
        return True

    def start(self) -> float:
        started = time.monotonic()
        self.spawn(0)
        self.wait("founder broker0 /livez", 30, lambda: self._get(0, "/livez") is not None)
        for i in range(1, self.nodes):
            self.spawn(i)
        self.wait("every broker /readyz with peer_links=%d" % (self.nodes - 1), 60 + 10 * self.nodes, self.ready_mesh)
        return time.monotonic() - started

    def kill(self, i: int) -> None:
        proc = self.procs[i]
        with contextlib.suppress(OSError):
            os.killpg(proc.pid, signal.SIGKILL)
        proc.wait(10)
        del self.procs[i]

    def stop_all(self) -> List[int]:
        """SIGKILL and reap every broker; the indexes of any that would not die."""
        left = []
        for i in sorted(self.procs):
            try:
                self.kill(i)
            except Exception:  # noqa: BLE001 - named in the result, never hidden
                left.append(i)
        return left


def _gauge_settled(cluster: LocalCluster, baseline: Dict[int, float]) -> Callable[[], bool]:
    """True once two sweeps ran after the canary exited and nothing it opened remains."""
    def check() -> bool:
        for i in range(cluster.nodes):
            snap = cluster.snapshot(i, "linger")
            sweeps = snap.series("mqttd_hub_dispatch_seconds_count", command="sweep") or 0.0
            if sweeps < baseline[i] + 2:
                return False
            for gauge in ("mqttd_connections_active", "mqttd_sessions", "mqttd_subscriptions"):
                if snap.series(gauge) not in (None, 0.0):
                    return False
        return True
    return check


def _now_ms() -> int:
    return int(time.time() * 1000)


def _zero_crossing_rung(cluster: LocalCluster, floors: Dict[int, Dict[str, float]], say: Callable[[str], None],
                        rung_dir: Path, messages: int = 500) -> None:
    """One $share group with a member on every node; every node's publisher publishes only
    locally. Prefer-local answers each publish on its own node, so forwarded must stay flat
    — and, after the canary, stay PRESENT: the certified zero the extractor accepts.

    Written the way run-curve.sh writes a Lane E rung (before, window-open, window-close,
    after; window.tsv with per-broker stamps and the PID that answered), so the
    extractor can certify it from the same process lifetime that passed the canary."""
    nodes = cluster.nodes
    nonce = os.urandom(5).hex()
    topic = "fss-proof/%s/site" % nonce
    members, publishers = [], []  # type: List[Member], List[Conn]
    rung_dir.mkdir(parents=True, exist_ok=True)
    snaps = {}  # type: Dict[str, List[Snapshot]]
    rows = ["host\tphase\tstart_ms\tend_ms\tmain_pid"]

    def take(label: str, stamped: str = "") -> None:
        snaps[label] = []
        for i in range(nodes):
            start = _now_ms()
            text, snap = cluster.scrape(i, "metrics-%s-broker%d.prom" % (label, i))
            end = _now_ms()
            (rung_dir / snap.name).write_text(text, encoding="utf-8")
            snaps[label].append(snap)
            if stamped:
                rows.append("broker%d\t%s\t%d\t%d\t%d" % (i, stamped, start, end, cluster.pid(i)))

    try:
        for i in range(nodes):
            conn = Conn.open("127.0.0.1", cluster.mqtt(i), "fssproof-s%d-%s" % (i, nonce), 300, 10.0)
            members.append(Member(conn, "broker%d" % i))
            conn.subscribe(["$share/fssproof-%s/%s" % (nonce, topic)], 10.0)
            members[-1].start()
        for i in range(nodes):
            publishers.append(Conn.open("127.0.0.1", cluster.mqtt(i), "fssproof-p%d-%s" % (i, nonce), 300, 10.0))
        take("before")
        take("window-open", "open")
        for s in range(messages):
            payload = b"rung-end" if s == messages - 1 else b"rung-%d" % s
            for conn in publishers:
                conn.send(publish_packet(topic, payload), 10.0)
            time.sleep(0.001)
        cluster.wait("every rung member to see rung-end", 30, lambda: all("rung-end" in m.payloads(topic) for m in members))
        take("window-close", "close")
        seen = [m.payloads(topic) for m in members]
    finally:
        for conn in publishers:
            conn.disconnect()
        for member in members:
            member.close()
    take("after")
    (rung_dir / "window.tsv").write_text("\n".join(rows) + "\n", encoding="utf-8")
    (rung_dir / "rung.txt").write_text(
        "sites=1 offered=— publishers=%d consumers=%d qos=0 sub_qos=0 window=aligned cpu_window=missing "
        "settled=yes drained=yes control=no source=local-proof\n" % (nodes, nodes), encoding="utf-8")
    problems = []
    for i in range(nodes):
        b, a = snaps["before"][i], snaps["after"][i]
        sr = (b.series(FORWARDED, reason="shared-remote"), a.series(FORWARDED, reason="shared-remote"))
        orr = (b.series(FORWARDED, reason="subscriber-remote"), a.series(FORWARDED, reason="subscriber-remote"))
        received = a.family(RECEIVED) - b.family(RECEIVED)
        delivered = a.family(DELIVERED) - b.family(DELIVERED)
        floor_errors = []  # type: List[str]
        for label in ("before", "window-open", "window-close", "after"):
            floor_errors += floor_check(snaps[label][i], floors[i], nodes, "broker%d" % i)
        say("  rung broker%d forwarded{shared-remote} before=%s after=%s subscriber-remote before=%s after=%s "
            "received_delta=%s delivered_delta=%s member_seen=%d distinct=%d floor=%s"
            % (i, _num(sr[0]), _num(sr[1]), _num(orr[0]), _num(orr[1]), _num(received), _num(delivered),
               len(seen[i]), len(set(seen[i])), "held" if not floor_errors else "BROKEN"))
        if nodes > 1 and (sr[0] is None or sr[1] is None):
            problems.append("broker%d: forwarded family absent around the rung" % i)
        if (sr[1] or 0.0) != (sr[0] or 0.0) or (orr[1] or 0.0) != (orr[0] or 0.0):
            problems.append("broker%d: forwarded moved during a local-only rung" % i)
        if received != messages or delivered != messages:
            problems.append("broker%d: received/delivered delta %s/%s, want %d" % (i, _num(received), _num(delivered), messages))
        if len(seen[i]) != messages or len(set(seen[i])) != messages:
            problems.append("broker%d: local member saw %d (%d distinct), want %d" % (i, len(seen[i]), len(set(seen[i])), messages))
        problems += floor_errors
    if problems:
        raise ProofFailure("zero-crossing rung: " + "; ".join(problems))


def _extract(tree: Path, say: Callable[[str], None], what: str) -> Tuple[int, str, str]:
    """extract-lane-e.py --crossing-gate 0.5 on a results tree, echoed line by line."""
    script = Path(__file__).resolve().parent / "extract-lane-e.py"
    if not script.is_file():
        raise ProofFailure("%s is missing: the extractor cannot be asked to certify the rung" % script)
    proc = subprocess.run([sys.executable, str(script), "--crossing-gate", "0.5", str(tree)],
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=120)
    out, err = proc.stdout.decode("utf-8", "replace"), proc.stderr.decode("utf-8", "replace")
    for line in err.splitlines() + [ln for ln in out.splitlines() if ln.startswith("GATE ")]:
        say("  %s %s" % (what, line))
    return proc.returncode, out, err


def local_proof(binary: str, nodes: int, base_port: Optional[int], capture: Optional[str], count: int, timeout: float) -> int:
    def say(text: str) -> None:
        print(text, flush=True)

    bin_path = Path(binary)
    if nodes < 1:
        say("local-proof: FAIL --nodes must be positive")
        return 1
    if not bin_path.is_file() or not os.access(str(bin_path), os.X_OK):
        say("local-proof: FAIL mqttd binary missing or not executable: %s" % binary)
        return 1
    source = Path(__file__).resolve().read_bytes()
    say("local-proof: mqttd %s" % bin_path)
    say("local-proof: sha256 %s" % _sha256(bin_path))
    say("local-proof: nodes=%d count=%d timeout=%gs python=%s" % (nodes, count, timeout, sys.version.split()[0]))
    if capture and Path(capture).exists() and (not Path(capture).is_dir() or any(Path(capture).iterdir())):
        say("local-proof: FAIL capture directory %s exists and is not empty" % capture)
        return 1
    root = Path(tempfile.mkdtemp(prefix="fss-forward-canary-proof-"))
    cluster = None  # type: Optional[LocalCluster]
    ok = False
    try:
        cluster = LocalCluster(bin_path, nodes, root, base_port)
        say("cluster: ports per broker (mqtt health peer swim): %s" % "; ".join(" ".join(map(str, p)) for p in cluster.ports))
        took = cluster.start()
        say("cluster: %d brokers ready, peer_links=%d on every broker after %.1fs" % (nodes, nodes - 1, took))

        # A results tree in run-curve.sh's layout, so the extractor reads it unchanged.
        tree = Path(capture) if capture else root / "results"
        lane = tree / ("nodes=%d" % nodes) / "laneE"
        cap = lane / "forward-canary"
        cap.mkdir(parents=True, exist_ok=True)
        argv = [sys.executable, "-", "run"]
        for spec in cluster.broker_specs():
            argv += ["--broker", spec]
        argv += ["--count", str(count), "--timeout", "%g" % timeout]
        # Exactly the driver's invocation: the source on stdin, `python3 - run ...`.
        proc = subprocess.run(argv, input=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout + 60)
        baseline = {}
        for i in range(nodes):
            baseline[i] = cluster.snapshot(i, "baseline").series("mqttd_hub_dispatch_seconds_count", command="sweep") or 0.0
        for name, content in split_stream(proc.stdout.decode("utf-8", "replace")):
            (cap / name).write_text(content, encoding="utf-8")
        (cap / "run.stderr").write_bytes(proc.stderr)
        for i in range(nodes):
            (cap / ("mainpid-broker%d.txt" % i)).write_text("%d\n" % cluster.pid(i))
        say("canary: run exit=%d, %d stdout bytes, %d stderr bytes, capture %s" % (proc.returncode, len(proc.stdout), len(proc.stderr), tree if capture else "(temporary)"))
        with contextlib.suppress(OSError):
            for line in (cap / "timeline.tsv").read_text().splitlines():
                say("  timeline " + line)
        text = io.StringIO()
        rc = verify(str(cap), nodes, count, text)
        (lane / "forward-canary.txt").write_text(text.getvalue(), encoding="utf-8")
        for line in text.getvalue().splitlines():
            say("  verify " + line)
        if proc.returncode != 0 or rc != 0:
            raise ProofFailure("canary did not pass on a healthy cluster (run exit %d, verify exit %d)" % (proc.returncode, rc))
        floors = ledger(cap, nodes, count)["floors"]  # type: ignore[index]

        took = cluster.wait("canary sessions/subscriptions/connections to clear", 30, _gauge_settled(cluster, baseline))
        say("canary: after 2 sweeps (%.1fs) no connection, session or subscription lingers on any broker" % took)

        say("rung: zero-crossing, one $share member per node, local-only publishers")
        _zero_crossing_rung(cluster, floors, say, lane / "sites-1")  # type: ignore[arg-type]
        say("rung: forwarded flat%s on every broker; floors held -> crossing 0 is certifiable"
            % (" and present" if nodes > 1 else ""))
        cert = "canary" if nodes > 1 else "structural"
        rc, out, err = _extract(tree, say, "extract")
        if rc != 0 or ("GATE nodes=%d PASS" % nodes) not in out or cert not in out or "INVALID" in err:
            raise ProofFailure("extract-lane-e.py --crossing-gate 0.5 did not certify the rung as %s (exit %d)" % (cert, rc))
        say("rung: extract-lane-e.py certifies it cert=%s and the 0.5%% gate passes" % cert)

        victim = nodes - 1
        old_pid = cluster.pid(victim)
        cluster.kill(victim)
        cluster.spawn(victim)
        took = cluster.wait("broker%d ready with peer_links=%d again" % (victim, nodes - 1), 60 + 10 * nodes, cluster.ready_mesh)
        say("restart: SIGKILL broker%d pid %d, respawned as pid %d, ready with peer_links=%d again after %.1fs"
            % (victim, old_pid, cluster.pid(victim), nodes - 1, took))
        recorded = int((cap / ("mainpid-broker%d.txt" % victim)).read_text())
        if recorded == cluster.pid(victim):
            raise ProofFailure("broker%d restarted but mainpid-broker%d.txt still matches it" % (victim, victim))
        say("  restart mainpid-broker%d.txt=%d, running pid=%d: a window main_pid would not match the canary's"
            % (victim, recorded, cluster.pid(victim)))
        restarted = {}  # type: Dict[int, Tuple[str, Snapshot]]
        for i in range(nodes):
            restarted[i] = cluster.scrape(i, "restart-broker%d" % i)
            errs = floor_check(restarted[i][1], floors[i], nodes, "broker%d" % i)  # type: ignore[index]
            if i == victim:
                if not errs:
                    raise ProofFailure("broker%d restarted but its snapshot still clears the canary floor" % i)
                for err_line in errs:
                    say("  restart floor check FAILS as it must: " + err_line)
            elif errs:
                raise ProofFailure("broker%d was not restarted but fails its floor: %s" % (i, "; ".join(errs)))
            else:
                say("  restart broker%d not restarted: floor holds" % i)
        keep = tree / "restart"
        keep.mkdir(parents=True, exist_ok=True)
        (keep / ("metrics-restart-broker%d.prom" % victim)).write_text(restarted[victim][0], encoding="utf-8")
        (keep / ("mainpid-restart-broker%d.txt" % victim)).write_text("%d\n" % cluster.pid(victim))
        # The same rung as if the restart had happened before it: every snapshot of the
        # victim is the new process, so nothing resets inside the rung and only the
        # canary floor and the MainPID can tell. The extractor must refuse it.
        negative = root / "restart-results"
        shutil.copytree(str(lane.parent.parent), str(negative))
        ndir = negative / ("nodes=%d" % nodes) / "laneE" / "sites-1"
        for label in ("before", "window-open", "window-close", "after"):
            (ndir / ("metrics-%s-broker%d.prom" % (label, victim))).write_text(restarted[victim][0], encoding="utf-8")
        tsv = (ndir / "window.tsv").read_text().splitlines()
        tsv = [re.sub(r"\t\d+$", "\t%d" % cluster.pid(victim), row) if row.startswith("broker%d\t" % victim) else row for row in tsv]
        (ndir / "window.tsv").write_text("\n".join(tsv) + "\n", encoding="utf-8")
        rc, out, err = _extract(negative, say, "restart extract")
        if rc == 0 or "INVALID" not in err or "below its canary floor" not in err or "the process that passed the canary" not in err:
            raise ProofFailure("extract-lane-e.py did not refuse a rung whose broker%d restarted since the canary (exit %d)" % (victim, rc))
        say("restart: extract-lane-e.py marks the restarted rung INVALID (floor and MainPID), gate FAIL")
        ok = True
    except ProofFailure as exc:
        say("local-proof: FAIL " + str(exc))
    except (Exception, KeyboardInterrupt, Stopped) as exc:  # noqa: BLE001 - reported, then still cleaned up
        say("local-proof: FAIL " + _describe(exc))
    finally:
        if cluster is not None:
            left = cluster.stop_all()
            if left:
                ok = False
                say("cluster: FAILED to kill broker%s %s" % ("s" if len(left) > 1 else "", " ".join(map(str, left))))
            else:
                say("cluster: killed and reaped every broker process")
        if ok:
            shutil.rmtree(str(root), ignore_errors=True)
        else:
            say("local-proof: broker logs and data kept in %s" % root)
    say("local-proof: %s" % ("PASS" if ok else "FAIL"))
    return 0 if ok else 1


# ── self-test ─────────────────────────────────────────────────────────────────

TESTDATA = "testdata/forward-canary"


class FakeCluster:
    """N fake brokers in this process: enough MQTT and OpenMetrics to drive `run` offline.

    Remote interest becomes visible `propagation` seconds after SUBSCRIBE, so pilots are
    really needed; families stay absent until their first increment, like mqttd's.
    `truncate` maps a broker to how many of its next scrapes lose their '# EOF'; every
    publish a broker receives is kept in `routed`, in arrival order."""

    def __init__(self, nodes: int, propagation: float = 0.8, lose: Optional[Tuple[int, int, bytes]] = None,
                 truncate: Optional[Dict[int, int]] = None) -> None:
        import http.server

        self.nodes, self.propagation, self.lose = nodes, propagation, lose
        self.member_acks = 0  # PUBACKs the fake's subscribers sent back (QoS 1 only)
        self.grant_override = None  # type: Optional[int]
        self.out_pid = 0
        self.truncate = dict(truncate or {})
        self.routed = []  # type: List[Tuple[int, str, bytes]]
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.subs = []  # type: List[Tuple[int, str, socket.socket, float]]
        self.received = [0] * nodes
        self.delivered = [0] * nodes
        self.forwarded = [0] * nodes
        self.disconnects = 0
        self.connections = 0
        self.listeners = []  # type: List[socket.socket]
        self.http = []  # type: list
        self.threads = []  # type: List[threading.Thread]
        cluster = self
        for b in range(nodes):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listener.listen(16)
            listener.settimeout(0.1)
            self.listeners.append(listener)

            class Handler(http.server.BaseHTTPRequestHandler):
                broker = b

                def do_GET(self) -> None:  # noqa: N802 - http.server's naming
                    text = cluster.render(self.broker)
                    with cluster.lock:
                        if cluster.truncate.get(self.broker, 0) > 0:
                            cluster.truncate[self.broker] -= 1
                            text = text[:text.rindex("# EOF")]
                    body = text.encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)

                def log_message(self, *args: object) -> None:
                    pass

            server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
            server.daemon_threads = True
            self.http.append(server)
            self._thread(server.serve_forever)
            self._thread(self._accept, b)

    def _thread(self, target: Callable[..., None], *args: object) -> None:
        thread = threading.Thread(target=target, args=args, daemon=True)
        thread.start()
        self.threads.append(thread)

    def specs(self) -> List[str]:
        return ["127.0.0.1:%d:%d" % (self.listeners[b].getsockname()[1], self.http[b].server_address[1]) for b in range(self.nodes)]

    def render(self, b: int) -> str:
        with self.lock:
            out = ["# TYPE mqttd_publish_received counter"]
            if self.received[b]:
                out.append('mqttd_publish_received_total{qos="0"} %d' % self.received[b])
            out.append("# TYPE mqttd_publish_delivered counter")
            if self.delivered[b]:
                out.append('mqttd_publish_delivered_total{qos="0"} %d' % self.delivered[b])
            if self.forwarded[b]:
                out += ["# TYPE mqttd_publish_forwarded counter", 'mqttd_publish_forwarded_total{reason="shared-remote"} %d' % self.forwarded[b]]
            out += ["# TYPE mqttd_peer_links gauge", "mqttd_peer_links %d" % (self.nodes - 1), "# EOF"]
        return "\n".join(out) + "\n"

    def _accept(self, b: int) -> None:
        while not self.stop.is_set():
            try:
                sock, _ = self.listeners[b].accept()
            except socket.timeout:
                continue
            except OSError:
                return
            self._thread(self._serve, b, sock)

    def _serve(self, b: int, sock: socket.socket) -> None:
        reader = PacketReader(sock)
        try:
            while not self.stop.is_set():
                got = reader.read(stop=self.stop, poll=0.1)
                if got is None:
                    return
                first, body = got
                kind = first >> 4
                if kind == 1:
                    with self.lock:
                        self.connections += 1
                    sock.sendall(b"\x20\x02\x00\x00")
                elif kind == 8:
                    topic_len = struct.unpack("!H", body[2:4])[0]
                    share_topic = body[4:4 + topic_len].decode().split("/", 2)[2]
                    with self.lock:
                        self.subs.append((b, share_topic, sock, time.monotonic()))
                    # Grant what was requested, as a broker does — the canary now
                    # refuses a downgrade, because a downgraded control would
                    # certify a different delivery path than the rung it guards.
                    granted = body[-1] if self.grant_override is None else self.grant_override
                    sock.sendall(b"\x90\x03" + body[:2] + bytes([granted]))
                elif kind == 3:
                    pid = publish_id(first, body)
                    if pid is not None:
                        with contextlib.suppress(OSError):
                            sock.sendall(puback_packet(pid))
                    self._route(b, *parse_publish(first, body), qos=(first >> 1) & 3)
                elif kind == 4:
                    with self.lock:
                        self.member_acks += 1
                elif kind == 14:
                    with self.lock:
                        self.disconnects += 1
                    return
        except (OSError, EOFError):
            return
        finally:
            with self.lock:
                self.subs = [s for s in self.subs if s[2] is not sock]
            sock.close()

    def _route(self, b: int, topic: str, payload: bytes, qos: int = 0) -> None:
        with self.lock:
            self.received[b] += 1
            self.routed.append((b, topic, payload))
            now = time.monotonic()
            visible = [s for s in self.subs if s[1] == topic and (s[0] == b or now - s[3] >= self.propagation)]
            if not visible:
                return
            member = sorted(visible, key=lambda s: s[0] != b)[0]
            if member[0] != b:
                self.forwarded[b] += 1
            self.delivered[member[0]] += 1
            if self.lose == (b, member[0], payload):
                return
            with contextlib.suppress(OSError):
                self.out_pid += 1
                member[2].sendall(publish_packet(topic, payload, qos, self.out_pid % 65535 + 1))

    def settle(self, disconnects: int) -> None:
        """Wait (bounded) for the DISCONNECTs a finished run already sent to be read."""
        until = time.monotonic() + 5
        while time.monotonic() < until:
            with self.lock:
                if self.disconnects >= disconnects:
                    return
            time.sleep(0.01)

    def close(self) -> None:
        self.stop.set()
        for server in self.http:
            server.shutdown()
            server.server_close()
        for listener in self.listeners:
            listener.close()
        for thread in self.threads:
            thread.join(2.0)


def _script_source() -> bytes:
    return Path(__file__).resolve().read_bytes()


def _run_via_stdin(specs: Sequence[str], count: int, timeout: float,
                   qos: int = 0) -> Tuple[int, List[Tuple[str, str]], str]:
    argv = [sys.executable, "-", "run"]
    for spec in specs:
        argv += ["--broker", spec]
    argv += ["--count", str(count), "--timeout", str(timeout), "--qos", str(qos)]
    proc = subprocess.run(argv, input=_script_source(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout + 30)
    return proc.returncode, split_stream(proc.stdout.decode()), proc.stderr.decode()


class FramingTests(unittest.TestCase):
    def test_remaining_length_boundaries_round_trip(self) -> None:
        for n in (0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455):
            encoded = encode_remaining_length(n)
            self.assertEqual(len(encoded), 1 if n < 128 else 2 if n < 16384 else 3 if n < 2097152 else 4)
            framed = b"\x30" + encoded
            if n:
                self.assertIsNone(decode_packet(framed), "a header alone is not a packet (n=%d)" % n)
            else:
                self.assertEqual(decode_packet(framed), (0x30, b"", 2))
            if n < 70000:
                self.assertEqual(decode_packet(framed + b"x" * n + b"tail"), (0x30, b"x" * n, len(framed) + n))
        with self.assertRaises(ValueError):
            encode_remaining_length(268435456)
        with self.assertRaises(ProtocolError):
            decode_packet(b"\x30\xff\xff\xff\xff\x01")

    def test_packets_are_byte_exact(self) -> None:
        self.assertEqual(connect_packet("c", 300), b"\x10\x0d\x00\x04MQTT\x04\x02\x01\x2c\x00\x01c")
        self.assertEqual(subscribe_packet(7, "$share/g/t"), b"\x82\x0f\x00\x07\x00\x0a$share/g/t\x00")
        self.assertEqual(publish_packet("a/b", b"xy"), b"\x30\x07\x00\x03a/bxy")
        self.assertEqual(parse_publish(0x30, b"\x00\x03a/bxy"), ("a/b", b"xy"))
        self.assertEqual(parse_publish(0x32, b"\x00\x03a/b\x00\x09xy"), ("a/b", b"xy"))

    def test_reader_keeps_a_partial_packet_across_a_timeout(self) -> None:
        a, b = socket.socketpair()
        try:
            reader = PacketReader(a)
            data = publish_packet("t/1", b"z" * 300)
            b.sendall(data[:3])
            with self.assertRaises(socket.timeout):
                reader.read(deadline=time.monotonic() + 0.05, poll=0.01)
            b.sendall(data[3:] + DISCONNECT)
            self.assertEqual(data[1:3], encode_remaining_length(305))
            self.assertEqual(reader.read(deadline=time.monotonic() + 2), (0x30, data[3:]))
            self.assertEqual(reader.read(deadline=time.monotonic() + 2), (0xE0, b""))
            b.close()
            with self.assertRaises(EOFError):
                reader.read(deadline=time.monotonic() + 2)
        finally:
            a.close()

    def test_stream_round_trips_exact_contents(self) -> None:
        chunks = [("metrics-pre-broker0.prom", "a 1\n# EOF\n"), ("clients.tsv", "no newline"), ("empty", ""), ("timeline.tsv", "x\n\n")]
        out = io.BytesIO()
        emitter = Emitter(out)  # type: ignore[arg-type]
        for name, content in chunks:
            emitter.emit(name, content)
        self.assertEqual(split_stream(out.getvalue().decode()), chunks)
        with self.assertRaises(ValueError):
            emitter.emit("../escape", "x")
        with self.assertRaises(ValueError):
            split_stream("noise\n@@@ a\nb")

    def test_whole_file_parses_as_python_38(self) -> None:
        import ast

        source = _script_source().decode()
        ast.parse(source, feature_version=(3, 8))
        # APIs newer than 3.8 that the 3.8 grammar check cannot see. Spelled split so this
        # list does not match itself.
        newer = ["remove" + "prefix", "remove" + "suffix", "Boolean" + "OptionalAction", "is_relative" + "_to",
                 "with" + "_stem(", "waitstatus" + "_to_exitcode", "file" + "_digest", "bit" + "_count(",
                 "exit_on" + "_error", "process" + "_group=", "ignore_cleanup" + "_errors", "root" + "_dir=",
                 "used" + "forsecurity", "zone" + "info", "graph" + "lib", "functools." + "cache(", "strict" + "=True"]
        self.assertEqual([name for name in newer if name in source], [])
        self.assertIsNone(re.search(r"(?m)^\s*with \(", source), "parenthesized context managers are 3.9+")
        self.assertNotIn("sys." + "stdin", source, "the run path is itself fed over stdin")
        self.assertNotIn("input" + "(", source)
        # Compiling the file must not need __file__ either: `python3 -` gives it no real path.
        namespace = {"__name__": "forward_canary_probe"}  # type: Dict[str, object]
        exec(compile(source, "<ssh stdin>", "exec"), namespace)
        self.assertTrue(callable(namespace["ledger"]))


class LedgerTests(unittest.TestCase):
    def fixture(self, name: str) -> Path:
        path = Path(__file__).resolve().parent / TESTDATA / name
        self.assertTrue(path.is_dir(), "missing fixture %s" % path)
        return path

    @contextlib.contextmanager
    def mutated(self, name: str, change: Callable[[Path], None]):  # type: ignore[no-untyped-def]
        with tempfile.TemporaryDirectory() as td:
            dst = Path(td) / name
            shutil.copytree(str(self.fixture(name)), str(dst))
            change(dst)
            yield dst

    def assert_fails(self, directory: Path, nodes: int, count: int, *needles: str) -> str:
        with self.assertRaises(ValueError) as caught:
            ledger(directory, nodes, count)
        message = str(caught.exception)
        for needle in needles:
            self.assertIn(needle, message)
        out = io.StringIO()
        self.assertEqual(verify(str(directory), nodes, count, out), 1)
        self.assertTrue(out.getvalue().startswith("status=fail nodes=%d count=%d\n" % (nodes, count)))
        return message

    def test_real_captures_pass_with_post_floors(self) -> None:
        for name, nodes in (("n2", 2), ("n3", 3)):
            d = self.fixture(name)
            result = ledger(d, nodes, 100)
            self.assertEqual(result["status"], "pass")
            floors = result["floors"]
            self.assertEqual(sorted(floors), list(range(nodes)))  # type: ignore[arg-type]
            for i in range(nodes):
                post = read_snapshot(d / ("metrics-post-broker%d.prom" % i))
                self.assertEqual(floors[i], {"forwarded": post.series(FORWARDED, reason="shared-remote"), "received": post.family(RECEIVED)})  # type: ignore[index]
                self.assertGreaterEqual(floors[i]["forwarded"], 100 * (nodes - 1))  # type: ignore[index]
            out = io.StringIO()
            self.assertEqual(verify(str(d), nodes, 100, out), 0)
            lines = out.getvalue().splitlines()
            self.assertEqual(lines[0], "status=pass nodes=%d count=100" % nodes)
            for i in range(nodes):
                self.assertRegex(lines[1 + i], r"^broker%d forwarded_post=\d+ received_post=\d+ shared_remote_delta=%d "
                                 r"received_delta=%d delivered_delta=%d .*peer_links=%d$" % (i, 100 * (nodes - 1), 100 * (nodes - 1), 100 * (nodes - 1), nodes - 1))

    def test_real_local_capture_passes_local(self) -> None:
        result = ledger(self.fixture("n1"), 1, 100)
        self.assertEqual(result["status"], "pass-local")
        self.assertEqual(result["floors"][0]["forwarded"], 0.0)  # type: ignore[index]

    def test_harness_line_splitter_output_still_passes(self) -> None:
        # run-curve.sh's batch_split is awk: `/^@@@ /` opens the next file and every other
        # line is printed with a newline, so each non-final chunk keeps the framing's blank
        # line. The ledger must read those files exactly as it reads split_stream's.
        src = self.fixture("n2")
        out = io.BytesIO()
        emitter = Emitter(out)  # type: ignore[arg-type]
        for name in sorted(p.name for p in src.iterdir() if CHUNK_NAME.match(p.name) and not p.name.startswith("mainpid")):
            emitter.emit(name, (src / name).read_text())
        with tempfile.TemporaryDirectory() as td:
            handle = None
            for line in out.getvalue().decode().split("\n")[:-1]:
                if line.startswith("@@@ "):
                    if handle:
                        handle.close()
                    handle = open(str(Path(td) / line.split()[1]), "w")
                elif handle:
                    handle.write(line + "\n")
            assert handle is not None
            handle.close()
            self.assertTrue((Path(td) / "metrics-post-broker0.prom").read_text().endswith("# EOF\n\n"))
            self.assertEqual(ledger(td, 2, 100), ledger(src, 2, 100))

    def test_count_mismatch_fails(self) -> None:
        self.assert_fails(self.fixture("n2"), 2, 99, "count=100, want 99", "want 99")

    def test_shared_remote_delta_off_by_one(self) -> None:
        with self.mutated("n3", lambda d: _bump(d / "metrics-post-broker0.prom", 'mqttd_publish_forwarded_total{reason="shared-remote"}', -1)) as d:
            self.assert_fails(d, 3, 100, "broker0: shared_remote_delta=199, want 200")

    def test_subscriber_remote_plus_one(self) -> None:
        def change(d: Path) -> None:
            _insert_after(d / "metrics-post-broker1.prom", 'mqttd_publish_forwarded_total{reason="shared-remote"}',
                          'mqttd_publish_forwarded_total{reason="subscriber-remote"} 1')
        with self.mutated("n3", change) as d:
            self.assert_fails(d, 3, 100, "broker1: subscriber_remote_delta=1, want 0")

    def test_dropped_plus_one(self) -> None:
        def change(d: Path) -> None:
            _insert_after(d / "metrics-post-broker2.prom", 'mqttd_publish_forwarded_total{reason="shared-remote"}',
                          "# HELP mqttd_publish_dropped Publishes dropped.\n# TYPE mqttd_publish_dropped counter\n"
                          'mqttd_publish_dropped_total{reason="outbound-full"} 1')
        with self.mutated("n3", change) as d:
            self.assert_fails(d, 3, 100, "broker2: dropped_delta=1, want 0")

    def test_peer_links_short_of_full_mesh(self) -> None:
        with self.mutated("n3", lambda d: _bump(d / "metrics-post-broker1.prom", "mqttd_peer_links", -1)) as d:
            self.assert_fails(d, 3, 100, "broker1: post peer_links=1, want 2")
        with self.mutated("n3", lambda d: _bump(d / "metrics-pre-broker2.prom", "mqttd_peer_links", -1)) as d:
            message = self.assert_fails(d, 3, 100, "broker2: pre peer_links=1, want 2")
            self.assertIn("(1)", message)

    def test_peer_links_absent(self) -> None:
        # HELP and TYPE stay: a declared gauge with no sample is still no evidence of a mesh.
        with self.mutated("n2", lambda d: _replace(d / "metrics-post-broker0.prom", "mqttd_peer_links 1\n", "")) as d:
            self.assertIn("# TYPE mqttd_peer_links gauge", (d / "metrics-post-broker0.prom").read_text())
            self.assert_fails(d, 2, 100, "broker0: mqttd_peer_links absent from the post scrape")
            out = io.StringIO()
            verify(str(d), 2, 100, out)
            self.assertRegex(out.getvalue(), r"(?m)^broker0 .* peer_links=absent$")
        with self.mutated("n2", lambda d: _replace(d / "metrics-pre-broker1.prom", "mqttd_peer_links 1\n", "")) as d:
            self.assert_fails(d, 2, 100, "broker1: mqttd_peer_links absent from the pre scrape")

    def test_missing_pair_row(self) -> None:
        def change(d: Path) -> None:
            rows = (d / "clients.tsv").read_text().splitlines()
            (d / "clients.tsv").write_text("\n".join(rows[:2] + rows[3:]) + "\n")
        with self.mutated("n3", change) as d:
            self.assert_fails(d, 3, 100, "clients.tsv has 5 pair rows, want 6", "clients.tsv is missing pair broker0->broker2")

    def test_burst_distinct_below_seen(self) -> None:
        def change(d: Path) -> None:
            text = (d / "clients.tsv").read_text()
            (d / "clients.tsv").write_text(text.replace("broker2\tbroker1\t100\t100", "broker2\tbroker1\t100\t99"))
        with self.mutated("n3", change) as d:
            self.assert_fails(d, 3, 100, "pair broker2->broker1 member saw burst_seen=100 burst_distinct=99")

    def test_status_error(self) -> None:
        with self.mutated("n2", lambda d: _replace(d / "timeline.tsv", "status\tcomplete", "status\terror")) as d:
            self.assert_fails(d, 2, 100, "timeline.tsv status=error, want complete")

    def test_nodes_mismatch(self) -> None:
        with self.mutated("n3", lambda d: _replace(d / "timeline.tsv", "nodes\t3", "nodes\t2")) as d:
            self.assert_fails(d, 3, 100, "timeline.tsv nodes=2, want 3")
        self.assert_fails(self.fixture("n3"), 2, 100, "timeline.tsv nodes=3, want 2", "metrics-post-broker2.prom belongs to a broker beyond nodes=2")

    def test_missing_post_file(self) -> None:
        with self.mutated("n2", lambda d: (d / "metrics-post-broker1.prom").unlink()) as d:
            self.assert_fails(d, 2, 100, "broker1: metrics-post-broker1.prom is missing")
            out = io.StringIO()
            verify(str(d), 2, 100, out)
            self.assertIn("\nbroker1 forwarded_post=absent received_post=absent shared_remote_delta=absent", out.getvalue())

    def test_post_without_eof(self) -> None:
        def change(d: Path) -> None:
            path = d / "metrics-post-broker0.prom"
            lines = path.read_text().splitlines()
            self.assertEqual(lines[-1], "# EOF")
            path.write_text("\n".join(lines[:-1]) + "\n")
        with self.mutated("n2", change) as d:
            self.assert_fails(d, 2, 100, "broker0: metrics-post-broker0.prom does not end in # EOF")

    def test_pre_copied_over_post(self) -> None:
        def change(d: Path) -> None:
            shutil.copyfile(str(d / "metrics-pre-broker2.prom"), str(d / "metrics-post-broker2.prom"))
        with self.mutated("n3", change) as d:
            self.assert_fails(d, 3, 100, "broker2: received_delta=0, want 200", "broker2: delivered_delta=0, want 200", "broker2: shared_remote_delta=0, want 200")

    def test_every_error_is_listed(self) -> None:
        def change(d: Path) -> None:
            _bump(d / "metrics-post-broker0.prom", 'mqttd_publish_forwarded_total{reason="shared-remote"}', 1)
            (d / "metrics-post-broker1.prom").unlink()
            _replace(d / "timeline.tsv", "status\tcomplete", "status\terror")
        with self.mutated("n2", change) as d:
            message = self.assert_fails(d, 2, 100, "broker0: shared_remote_delta=101", "broker1: metrics-post-broker1.prom is missing", "status=error")
            self.assertIn("(3)", message)

    def test_floor_check_catches_resets(self) -> None:
        d = self.fixture("n2")
        floors = ledger(d, 2, 100)["floors"]
        self.assertEqual(floor_check(d / "metrics-post-broker0.prom", floors[0], 2), [])  # type: ignore[index]
        below = floor_check(d / "metrics-pre-broker0.prom", floors[0], 2, "broker0")  # type: ignore[index]
        self.assertEqual(len(below), 2, below)
        self.assertIn("below canary floor", below[0])
        # A real scrape whose forwarded family never appeared: what a restarted broker serves.
        absent = floor_check(self.fixture("n1") / "metrics-post-broker0.prom", floors[1], 2, "broker1")  # type: ignore[index]
        self.assertTrue(absent and "absent" in absent[0], absent)
        self.assertIn("broker1", absent[0])


def _replace(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    assert text.count(old) == 1, (path, old)
    path.write_text(text.replace(old, new))


def _bump(path: Path, series: str, delta: int) -> None:
    lines = path.read_text().splitlines()
    hits = [n for n, line in enumerate(lines) if line.rsplit(" ", 1)[0] == series]
    assert len(hits) == 1, (path, series)
    value = float(lines[hits[0]].rsplit(" ", 1)[1]) + delta
    lines[hits[0]] = "%s %d" % (series, value)
    path.write_text("\n".join(lines) + "\n")


def _insert_after(path: Path, series: str, new: str) -> None:
    lines = path.read_text().splitlines()
    hits = [n for n, line in enumerate(lines) if line.rsplit(" ", 1)[0] == series]
    assert len(hits) == 1, (path, series)
    lines[hits[0] + 1:hits[0] + 1] = new.split("\n")
    path.write_text("\n".join(lines) + "\n")


class RunTests(unittest.TestCase):
    """`run` end to end over loopback, executed the way a driver executes it."""

    def capture(self, chunks: List[Tuple[str, str]]) -> Path:
        td = tempfile.mkdtemp(prefix="fss-canary-test-")
        self.addCleanup(shutil.rmtree, td, True)
        for name, content in chunks:
            (Path(td) / name).write_text(content)
        return Path(td)

    def test_mesh_run_passes_the_ledger_and_disconnects(self) -> None:
        fake = FakeCluster(3)
        try:
            rc, chunks, stderr = _run_via_stdin(fake.specs(), 20, 20)
            fake.settle(6)
        finally:
            fake.close()
        self.assertEqual((rc, stderr), (0, ""))
        names = [n for n, _ in chunks]
        self.assertEqual(names, ["metrics-pre-broker%d.prom" % i for i in range(3)] + ["metrics-post-broker%d.prom" % i for i in range(3)] + ["clients.tsv", "timeline.tsv"])
        d = self.capture(chunks)
        self.assertEqual(ledger(d, 3, 20)["status"], "pass")
        timeline = (d / "timeline.tsv").read_text()
        self.assertRegex(timeline, r"pilot delivered on every pair after ([2-9]|\d\d+) rounds")
        self.assertRegex(timeline, r"(?m)^nonce\t[0-9a-f]{18}$")
        self.assertEqual((fake.connections, fake.disconnects), (6, 6))
        # burst-end must be the K-th burst message of every pair, in publish order: the
        # post scrape waits for it, so a burst-end sent early scrapes a burst in flight.
        bursts = {}  # type: Dict[str, List[bytes]]
        for _, topic, payload in fake.routed:
            if payload.startswith(b"burst-"):
                bursts.setdefault(topic, []).append(payload)
        self.assertEqual(len(bursts), 6)
        for topic, payloads in bursts.items():
            self.assertEqual(payloads, [b"burst-%d" % s for s in range(19)] + [b"burst-end"], topic)

    def test_qos1_framing_carries_a_packet_id_and_qos0_is_unchanged(self) -> None:
        # QoS 0 framing must be byte-identical: the QoS 0 arm of lane E is already
        # published, and a control that changed shape would recertify it.
        self.assertEqual(publish_packet("a/b", b"xy"), b"\x30\x07\x00\x03a/bxy")
        self.assertEqual(publish_packet("a/b", b"xy", 0, 9), b"\x30\x07\x00\x03a/bxy")
        # At QoS 1 the header carries the level and the id sits after the topic.
        self.assertEqual(publish_packet("a/b", b"xy", 1, 9), b"\x32\x09\x00\x03a/b\x00\x09xy")
        self.assertEqual(puback_packet(9), b"\x40\x02\x00\x09")
        self.assertEqual(subscribe_packet(7, "$share/g/t", 1), b"\x82\x0f\x00\x07\x00\x0a$share/g/t\x01")
        # and the id round-trips out of an inbound PUBLISH, which is what the
        # member must echo — a wrong id acks someone else's message.
        first, body, _ = decode_packet(publish_packet("a/b", b"xy", 1, 9))
        self.assertEqual(publish_id(first, body), 9)
        self.assertEqual(parse_publish(first, body), ("a/b", b"xy"))
        first0, body0, _ = decode_packet(publish_packet("a/b", b"xy"))
        self.assertIsNone(publish_id(first0, body0))

    def test_a_granted_downgrade_is_refused(self) -> None:
        # A broker answering a QoS 1 SUBSCRIBE with QoS 0 is not refusing: it is
        # silently handing the control a different delivery path than the rung it
        # is supposed to certify. That must stop the canary, not pass it.
        fake = FakeCluster(1)
        try:
            host, port, _ = parse_broker(fake.specs()[0])
            conn = Conn.open(host, port, "downgrade-probe", 30, 5.0)
            try:
                fake.grant_override = 0
                with self.assertRaises(ProtocolError) as caught:
                    conn.subscribe(["$share/g/t"], 5.0, 1)
                self.assertIn("granted QoS 0", str(caught.exception))
            finally:
                conn.disconnect()
        finally:
            fake.close()

    def test_mesh_run_at_qos1_acks_every_publish_and_passes_the_ledger(self) -> None:
        # The whole point of the QoS 1 port: the ledger is only a ledger if every
        # delivery was acknowledged. An unacked QoS 1 delivery is redelivered, so
        # `delivered_delta` would climb past K*(N-1) for a reason that has nothing
        # to do with forwarding — the canary would fail on its own silence.
        fake = FakeCluster(3)
        try:
            rc, chunks, stderr = _run_via_stdin(fake.specs(), 20, 20, qos=1)
            fake.settle(6)
        finally:
            fake.close()
        self.assertEqual((rc, stderr), (0, ""))
        d = self.capture(chunks)
        self.assertEqual(ledger(d, 3, 20)["status"], "pass")
        timeline = (d / "timeline.tsv").read_text()
        self.assertIn("QoS 1 publishes acknowledged", timeline)
        # every delivery the fake made was acknowledged by a member
        self.assertGreater(fake.member_acks, 0, "members never acknowledged a QoS 1 delivery")

    def test_a_truncated_scrape_is_retried_and_never_kept(self) -> None:
        fake = FakeCluster(2, propagation=0.1, truncate={1: 1})
        try:
            rc, chunks, _ = _run_via_stdin(fake.specs(), 5, 20)
        finally:
            fake.close()
        self.assertEqual(rc, 0)
        d = self.capture(chunks)
        self.assertEqual([n for n, _ in chunks].count("metrics-pre-broker1.prom"), 1)
        self.assertEqual(ledger(d, 2, 5)["status"], "pass")
        self.assertIn("scrape pre broker1 attempt 1 failed: truncated scrape (no # EOF)", (d / "timeline.tsv").read_text())
        # A broker that never serves a whole scrape stops the run and names itself; the
        # cut text is still emitted as evidence, and the ledger refuses it.
        fake = FakeCluster(2, propagation=0.1, truncate={0: 99})
        try:
            rc, chunks, _ = _run_via_stdin(fake.specs(), 5, 20)
            fake.settle(4)
            port = fake.http[0].server_address[1]
        finally:
            fake.close()
        self.assertEqual(rc, 3)
        d = self.capture(chunks)
        timeline = (d / "timeline.tsv").read_text()
        self.assertIn("ERROR scrape pre broker0 (127.0.0.1:%d) failed: truncated scrape (no # EOF)" % port, timeline)
        self.assertIn("status\terror\n", timeline)
        self.assertNotIn("# EOF", (d / "metrics-pre-broker0.prom").read_text())
        with self.assertRaises(ValueError):
            ledger(d, 2, 5)
        self.assertEqual((fake.connections, fake.disconnects), (4, 4))

    def test_local_mode_passes_local(self) -> None:
        fake = FakeCluster(1)
        try:
            rc, chunks, _ = _run_via_stdin(fake.specs(), 5, 20)
        finally:
            fake.close()
        self.assertEqual(rc, 0)
        d = self.capture(chunks)
        self.assertEqual(ledger(d, 1, 5)["status"], "pass-local")
        self.assertEqual((d / "clients.tsv").read_text(), CLIENTS_HEADER + "\nbroker0\tbroker0\t5\t5\n")

    def test_a_lost_message_completes_but_fails_the_ledger(self) -> None:
        fake = FakeCluster(2, lose=(1, 0, b"burst-3"))
        try:
            rc, chunks, _ = _run_via_stdin(fake.specs(), 10, 20)
        finally:
            fake.close()
        self.assertEqual(rc, 0)
        with self.assertRaises(ValueError) as caught:
            ledger(self.capture(chunks), 2, 10)
        self.assertIn("pair broker1->broker0 member saw burst_seen=9 burst_distinct=9, want 10", str(caught.exception))

    def test_unreachable_broker_errors_names_it_and_still_disconnects(self) -> None:
        fake = FakeCluster(2)
        dead = _free_ports(1)[0]
        specs = [fake.specs()[0], "127.0.0.1:%d:%d" % (dead, dead)]
        try:
            rc, chunks, _ = _run_via_stdin(specs, 5, 10)
            fake.settle(1)
        finally:
            fake.close()
        self.assertEqual(rc, 3)
        self.assertEqual([n for n, _ in chunks], ["clients.tsv", "timeline.tsv"])
        timeline = dict(chunks)["timeline.tsv"]
        self.assertIn("ERROR subscriber connect to broker1 (127.0.0.1:%d) failed" % dead, timeline)
        self.assertIn("status\terror\n", timeline)
        self.assertEqual((fake.connections, fake.disconnects), (1, 1), "broker0's subscriber must still get DISCONNECT")


def self_test() -> int:
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite(loader.loadTestsFromTestCase(case) for case in (FramingTests, LedgerTests, RunTests))
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


# ── CLI ───────────────────────────────────────────────────────────────────────


def _positive_int(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value


def _positive_float(text: str) -> float:
    value = float(text)
    if not value > 0 or math.isinf(value):
        raise argparse.ArgumentTypeError("must be a positive number")
    return value


def _stop(signum: int, _frame: object) -> None:
    raise Stopped("signal %d" % signum)


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(prog="forward-canary.py", description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--self-test", action="store_true", help="offline unit tests on the real fixtures")
    sub = parser.add_subparsers(dest="cmd")
    run_p = sub.add_parser("run", help="the canary itself (driver side, over ssh stdin)")
    run_p.add_argument("--broker", action="append", required=True, help="HOST:MQTT_PORT:HEALTH_PORT, in broker-index order")
    run_p.add_argument("--count", type=_positive_int, default=100)
    run_p.add_argument("--timeout", type=_positive_float, default=90.0)
    # The control must certify the SAME delivery path the rung measures. At QoS 1
    # that path acks, and an unacked delivery is redelivered — which would break
    # the exact K*(N-1) ledger for a reason that has nothing to do with forwarding.
    run_p.add_argument("--qos", type=int, choices=(0, 1), default=0)
    verify_p = sub.add_parser("verify", help="re-derive the ledger from a canary directory")
    verify_p.add_argument("dir")
    verify_p.add_argument("--nodes", type=_positive_int, required=True)
    verify_p.add_argument("--count", type=_positive_int, required=True)
    proof_p = sub.add_parser("local-proof", help="the canary, a zero-crossing rung and a restart against real local brokers")
    proof_p.add_argument("--mqttd", required=True)
    proof_p.add_argument("--nodes", type=_positive_int, required=True)
    proof_p.add_argument("--base-port", type=_positive_int)
    proof_p.add_argument("--capture", help="keep the results tree (canary, rung, restart scrape) here (must be empty)")
    proof_p.add_argument("--count", type=_positive_int, default=100)
    proof_p.add_argument("--timeout", type=_positive_float, default=90.0)
    args = parser.parse_args(argv)
    if args.self_test:
        return self_test()
    if args.cmd in ("run", "local-proof"):
        for name in ("SIGTERM", "SIGHUP"):
            if hasattr(signal, name):
                signal.signal(getattr(signal, name), _stop)
    if args.cmd == "run":
        try:
            brokers = [parse_broker(spec) for spec in args.broker]
        except ValueError as exc:
            parser.error(str(exc))
        return run(brokers, args.count, args.timeout, sys.stdout.buffer, args.qos)
    if args.cmd == "verify":
        return verify(args.dir, args.nodes, args.count)
    if args.cmd == "local-proof":
        return local_proof(args.mqttd, args.nodes, args.base_port, args.capture, args.count, args.timeout)
    parser.error("choose run, verify or local-proof (or --self-test)")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
