"""A small MQTT client: enough to play the simulator and watch what the rules derive.

No third-party package, so the demo runs on a bare Python 3.9+. It speaks MQTT 3.1.1, or
MQTT 5 with `version=5`: CONNECT (with an optional Will), PUBLISH at QoS 0 and 1,
SUBSCRIBE, PINGREQ and DISCONNECT, and it can drop its connection without a DISCONNECT, the
way a device that loses power does. In MQTT 5 a publish may carry a payload format
indicator, a content type and user properties (`props`), and a received one reports them;
every other property is skipped.
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
    props: Optional[dict] = None  # MQTT 5 only: what `decode_props` reads


def _string(s: str) -> bytes:
    raw = s.encode("utf-8")
    return struct.pack("!H", len(raw)) + raw


# The MQTT 5 properties (§2.2.2.2) this client writes and reads, by identifier.
PAYLOAD_FORMAT, CONTENT_TYPE, USER_PROPERTY = 0x01, 0x03, 0x26
# How to skip every other property a PUBLISH may carry: its value's size in bytes, "v" for
# a variable byte integer, "s" for a length-prefixed string or binary data.
_SKIP = {0x02: 4, 0x08: "s", 0x09: "s", 0x0B: "v", 0x23: 2}


def encode_props(props: Optional[dict]) -> bytes:
    """MQTT 5 properties, their length first. `props` may hold `payload_format` (0 binary,
    1 UTF-8), `content_type` (a string) and `user` (a list of (key, value) string pairs,
    sent in order)."""
    out = b""
    if props:
        if props.get("payload_format") is not None:
            out += bytes([PAYLOAD_FORMAT, props["payload_format"]])
        if props.get("content_type") is not None:
            out += bytes([CONTENT_TYPE]) + _string(props["content_type"])
        for k, v in props.get("user", ()):
            out += bytes([USER_PROPERTY]) + _string(k) + _string(v)
    return _remaining_length(len(out)) + out


def _varint_at(data: bytes, i: int) -> tuple[int, int]:
    n, mult = 0, 1
    while True:
        byte = data[i]
        i += 1
        n += (byte & 0x7F) * mult
        mult *= 128
        if not byte & 0x80:
            return n, i


def decode_props(data: bytes, i: int) -> tuple[dict, int]:
    """A PUBLISH's properties at `data[i:]` (their length first), as `encode_props` takes
    them, and the index after them. Other properties are skipped."""
    length, i = _varint_at(data, i)
    end = i + length
    props: dict = {}

    def text(at: int) -> tuple[str, int]:
        (n,) = struct.unpack("!H", data[at : at + 2])
        return data[at + 2 : at + 2 + n].decode("utf-8", "replace"), at + 2 + n

    while i < end:
        pid = data[i]
        i += 1
        if pid == PAYLOAD_FORMAT:
            props["payload_format"] = data[i]
            i += 1
        elif pid == CONTENT_TYPE:
            props["content_type"], i = text(i)
        elif pid == USER_PROPERTY:
            k, i = text(i)
            v, i = text(i)
            props.setdefault("user", []).append((k, v))
        elif _SKIP.get(pid) == "v":
            _, i = _varint_at(data, i)
        elif _SKIP.get(pid) == "s":
            _, i = text(i)
        elif pid in _SKIP:
            i += _SKIP[pid]
        else:
            raise MqttError(f"a PUBLISH with an unknown MQTT 5 property 0x{pid:02x}")
    return props, end


def _remaining_length(n: int) -> bytes:
    out = bytearray()
    while True:
        byte, n = n % 128, n // 128
        out.append(byte | (0x80 if n else 0))
        if not n:
            return bytes(out)


class Client:
    """One MQTT connection. Not thread-safe; the player drives it from one loop.

    `timeout` bounds every wait on the broker: the TCP connect, the CONNACK, a send it stops
    reading, and the PUBACK or SUBACK of a request.
    """

    def __init__(self, host: str, port: int, client_id: str, keepalive: int = 60,
                 timeout: float = 10, version: int = 4):
        if version not in (4, 5):
            raise ValueError(f"MQTT protocol level 4 (3.1.1) or 5, not {version}")
        self.host, self.port, self.client_id, self.keepalive = host, port, client_id, keepalive
        self.version = version
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None
        self._buf = b""
        self._next_id = 1
        self._last_sent = 0.0
        self.inbox: list[Message] = []

    # ---- connection -------------------------------------------------------------------

    def connect(self, clean: bool = True, will: Optional[tuple[str, bytes, int, bool]] = None):
        """Open the connection; `will` is (topic, payload, qos, retain). On any failure the
        socket is closed again."""
        self.sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        try:
            self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            flags = 0x02 if clean else 0
            props = encode_props(None) if self.version == 5 else b""
            body = _string("MQTT") + bytes([self.version])
            tail = _string(self.client_id)
            if will:
                topic, payload, qos, retain = will
                flags |= 0x04 | (qos << 3) | (0x20 if retain else 0)
                tail += props + _string(topic) + struct.pack("!H", len(payload)) + payload
            body += bytes([flags]) + struct.pack("!H", self.keepalive) + props + tail
            self._send(CONNECT << 4, body)
            # A broker that accepts the connection and never answers must not hang the caller.
            got = self._read_packet(self.timeout)
            if got is None:
                raise MqttError(f"{self.client_id}: no CONNACK within {self.timeout:g} s")
            kind, _, data = got
            if kind != CONNACK or len(data) < 2 or data[1] != 0:
                raise MqttError(f"{self.client_id}: CONNECT refused ({data.hex()})")
        except BaseException:
            self.drop()
            raise

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

    def publish(self, topic: str, payload: bytes, qos: int = 0, retain: bool = False,
                props: Optional[dict] = None):
        """Publish; at QoS 1, wait for the PUBACK. `props` (see `encode_props`) needs
        MQTT 5."""
        if props and self.version != 5:
            raise MqttError(f"{self.client_id}: properties need MQTT 5")
        header = (PUBLISH << 4) | (qos << 1) | (1 if retain else 0)
        body = _string(topic)
        pid = 0
        if qos:
            pid = self._packet_id()
            body += struct.pack("!H", pid)
        if self.version == 5:
            body += encode_props(props)
        self._send(header, body + payload)
        if qos:
            ack = self._await(PUBACK, pid)
            # MQTT 5: a reason code of 0x80 or more refuses the message (§3.4.2.1).
            if len(ack) > 2 and ack[2] >= 0x80:
                raise MqttError(f"{self.client_id}: PUBLISH to {topic} refused "
                                f"(reason 0x{ack[2]:02x})")

    def subscribe(self, filters: list[tuple[str, int]]):
        """Subscribe to `(filter, qos)` pairs and wait for the SUBACK."""
        pid = self._packet_id()
        body = struct.pack("!H", pid)
        if self.version == 5:
            body += encode_props(None)
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

    def drain(self):
        """Handle whatever the broker has sent, without waiting for more.

        A client that only publishes at QoS 0 never reads otherwise: its PINGRESPs pile up,
        and a broker that went away is noticed only when a send fails. Raises MqttError
        when the broker has closed the connection.
        """
        if self.sock is None:
            raise MqttError(f"{self.client_id}: not connected")
        self.sock.setblocking(False)
        try:
            while True:
                try:
                    chunk = self.sock.recv(65536)
                except (BlockingIOError, InterruptedError):
                    break
                if not chunk:
                    raise MqttError(f"{self.client_id}: connection closed by the broker")
                self._buf += chunk
        finally:
            if self.sock:
                self.sock.settimeout(self.timeout)
        while True:
            got = self._parse()
            if got is None:
                return
            self._handle(*got)

    # ---- plumbing ------------------------------------------------------------------------

    def _packet_id(self) -> int:
        pid = self._next_id
        self._next_id = pid % 65535 + 1
        return pid

    def _send(self, header: int, body: bytes):
        if not self.sock:
            raise MqttError(f"{self.client_id}: not connected")
        self.sock.settimeout(self.timeout)
        self.sock.sendall(bytes([header]) + _remaining_length(len(body)) + body)
        self._last_sent = time.monotonic()

    def _await(self, kind: int, pid: int) -> bytes:
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            got = self._read_packet(deadline - time.monotonic())
            if got is None:
                break
            k, flags, data = got
            if k == kind and data[:2] == struct.pack("!H", pid):
                return data
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
            props = None
            if self.version == 5:
                props, at = decode_props(rest, 0)
                rest = rest[at:]
            self.inbox.append(Message(topic, rest, qos, bool(flags & 1), props))

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
