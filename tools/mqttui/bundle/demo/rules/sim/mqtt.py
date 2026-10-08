"""A small MQTT 3.1.1 client: enough to play the simulator and watch what the rules derive.

No third-party package, so the demo runs on a bare Python 3.9+. It speaks CONNECT (with an
optional Will), PUBLISH at QoS 0 and 1, SUBSCRIBE, PINGREQ and DISCONNECT, and it can drop
its connection without a DISCONNECT, the way a device that loses power does.
"""

from __future__ import annotations

import socket
import struct
import time
from dataclasses import dataclass
from typing import Optional

CONNECT, CONNACK, PUBLISH, PUBACK = 1, 2, 3, 4
SUBSCRIBE, SUBACK, PINGREQ, PINGRESP, DISCONNECT = 8, 9, 12, 13, 14


class MqttError(Exception):
    """The broker refused or broke the conversation."""


@dataclass
class Message:
    """A PUBLISH the client received."""

    topic: str
    payload: bytes
    qos: int
    retain: bool


def _string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


def _remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


class Client:
    """One MQTT connection. Not thread-safe; the player drives it from one loop."""

    def __init__(self, host: str, port: int, client_id: str, keepalive: int = 60):
        self.host, self.port, self.client_id, self.keepalive = host, port, client_id, keepalive
        self.sock: Optional[socket.socket] = None
        self._buf = b""
        self._next_id = 1
        self._last_sent = 0.0
        self.inbox: list[Message] = []

    # ---- connection -------------------------------------------------------------------

    def connect(self, clean: bool = True, will: Optional[tuple[str, bytes, int, bool]] = None):
        """Open the connection; `will` is (topic, payload, qos, retain)."""
        self.sock = socket.create_connection((self.host, self.port), timeout=10)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        flags = 0x02 if clean else 0
        body = _string("MQTT") + bytes([4])
        tail = _string(self.client_id)
        if will:
            topic, payload, qos, retain = will
            flags |= 0x04 | (qos << 3) | (0x20 if retain else 0)
            tail += _string(topic) + struct.pack("!H", len(payload)) + payload
        body += bytes([flags]) + struct.pack("!H", self.keepalive) + tail
        self._send(CONNECT << 4, body)
        kind, _, data = self._read_packet()
        if kind != CONNACK or len(data) < 2 or data[1] != 0:
            raise MqttError(f"{self.client_id}: CONNECT refused ({data.hex()})")

    def disconnect(self):
        """A clean DISCONNECT: the broker discards the Will. A broker already gone is fine."""
        if self.sock:
            try:
                self._send(DISCONNECT << 4, b"")
            except OSError:
                pass
            finally:
                self.drop()

    def drop(self):
        """Close the socket without a DISCONNECT, as a device losing power does."""
        if self.sock:
            try:
                self.sock.close()
            finally:
                self.sock = None

    # ---- publish / subscribe -----------------------------------------------------------

    def publish(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False):
        """Publish; at QoS 1, wait for the PUBACK."""
        header = (PUBLISH << 4) | (qos << 1) | (1 if retain else 0)
        body = _string(topic)
        pid = 0
        if qos:
            pid = self._packet_id()
            body += struct.pack("!H", pid)
        self._send(header, body + payload)
        if qos:
            self._await(PUBACK, pid)

    def subscribe(self, filters: list[tuple[str, int]]):
        """Subscribe to `(filter, qos)` pairs and wait for the SUBACK."""
        pid = self._packet_id()
        body = struct.pack("!H", pid)
        for f, q in filters:
            body += _string(f) + bytes([q])
        self._send((SUBSCRIBE << 4) | 0x02, body)
        self._await(SUBACK, pid)

    def poll(self, timeout: float) -> Optional[Message]:
        """The next message received within `timeout` seconds, or None."""
        if self.inbox:
            return self.inbox.pop(0)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.ping_if_idle()
            got = self._read_packet(deadline - time.monotonic())
            if got is None:
                return None
            self._handle(*got)
            if self.inbox:
                return self.inbox.pop(0)
        return None

    def ping_if_idle(self):
        """Keep the connection alive between sparse publishes."""
        if self.sock and time.monotonic() - self._last_sent > self.keepalive / 2:
            self._send(PINGREQ << 4, b"")

    # ---- plumbing ------------------------------------------------------------------------

    def _packet_id(self) -> int:
        pid = self._next_id
        self._next_id = pid % 65535 + 1
        return pid

    def _send(self, header: int, body: bytes):
        if not self.sock:
            raise MqttError(f"{self.client_id}: not connected")
        self.sock.sendall(bytes([header]) + _remaining_length(len(body)) + body)
        self._last_sent = time.monotonic()

    def _await(self, kind: int, pid: int):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            got = self._read_packet(deadline - time.monotonic())
            if got is None:
                break
            k, flags, data = got
            if k == kind and data[:2] == struct.pack("!H", pid):
                return
            self._handle(k, flags, data)
        raise MqttError(f"{self.client_id}: no answer to packet {pid}")

    def _handle(self, kind: int, flags: int, data: bytes):
        if kind == PUBLISH:
            qos = (flags >> 1) & 3
            (tlen,) = struct.unpack("!H", data[:2])
            topic = data[2 : 2 + tlen].decode("utf-8")
            rest = data[2 + tlen :]
            if qos:
                pid, rest = rest[:2], rest[2:]
                self._send(PUBACK << 4, pid)
            self.inbox.append(Message(topic, rest, qos, bool(flags & 1)))

    def _read_packet(self, timeout: Optional[float] = None):
        """(type, flags, body), or None when `timeout` passes first."""
        if self.sock is None:
            raise MqttError(f"{self.client_id}: not connected")
        while True:
            parsed = self._parse()
            if parsed:
                return parsed
            self.sock.settimeout(None if timeout is None else max(timeout, 0.001))
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                return None
            if not chunk:
                raise MqttError(f"{self.client_id}: connection closed by the broker")
            self._buf += chunk

    def _parse(self):
        buf = self._buf
        if len(buf) < 2:
            return None
        length, mult, i = 0, 1, 1
        while True:
            if i >= len(buf):
                return None
            byte = buf[i]
            length += (byte & 0x7F) * mult
            mult *= 128
            i += 1
            if not byte & 0x80:
                break
        if len(buf) < i + length:
            return None
        self._buf = buf[i + length :]
        return buf[0] >> 4, buf[0] & 0x0F, buf[i : i + length]
