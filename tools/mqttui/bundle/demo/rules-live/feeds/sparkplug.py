"""Sparkplug B (Eclipse Sparkplug 3.0) edge nodes, their devices and a host application,
playing the whole lifecycle into the broker.

    python3 sparkplug.py                    # to 127.0.0.1:1883, until stopped
    python3 sparkplug.py --port 1884 --fast # every event ten times as often
    python3 sparkplug.py --dump 1           # the first second's messages, decoded; no broker

Two edge nodes, on the demo's plants:

  spBv1.0/wf-falster/.../ppc       the wind farm's park power controller, a device per
                                   turbine (wtg01..wtg06)
  spBv1.0/pv-lolland/.../scada     the solar park's SCADA, a device per inverter

and a primary host application, scada-host, whose STATE (spBv1.0/STATE/scada-host, JSON,
retained) says whether it is online; its Will says it is not. Each edge node connects
with its NDEATH as its MQTT Will (with the bdSeq of its NBIRTH), then sends NBIRTH and a
DBIRTH per device naming every metric with its alias, data type and properties, then
NDATA and DDATA by alias only, by exception, every few seconds, seq 0-255 throughout.

What happens on the way, on a schedule (minutes, or seconds with --fast):

  every 10   the host asks wf-falster/ppc for a rebirth (NCMD Node Control/Rebirth): a
             new NBIRTH and DBIRTHs, seq from 0
  every 3    the host writes a turbine's power limit (DCMD), which the turbine applies
             and reports in its next DDATA
  every 12   one turbine goes into service: DDEATH, and its DBIRTH a minute later
  every 15   pv-lolland/scada loses power: no DISCONNECT, so the broker publishes its
             NDEATH (the Will); it is back 20 s later with the next bdSeq
  every 20   wf-falster/ppc restarts cleanly: it publishes its NDEATH itself, then
             disconnects, and is back 10 s later
  every 6    the park's alarm table (a DataSet) goes out compressed, the way Eclipse Tahu
             does it (uuid SPBV1.0_COMPRESSED, GZIP)

Between them the births carry every Sparkplug 3.0 data type: the integers, Float, Double,
Boolean, String, Text, UUID, DateTime, Bytes, File, every array type, a DataSet, a Template
definition with instances, and PropertySets (engineering units, ranges).

Options can also be set in the environment: SIM_HOST, SIM_PORT, SIM_QUIET (1 or 0).
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import random
import signal
import struct
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, os.environ.get("SIM_LIB") or str(HERE.parents[1] / "rules"))

from binfmt import Proto  # noqa: E402
from sim.mqtt import Client, MqttError  # noqa: E402

NS = "spBv1.0"
HOST_ID = "scada-host"

# Sparkplug 3.0 data types (tahu sparkplug_b.proto, DataType).
INT8, INT16, INT32, INT64 = 1, 2, 3, 4
UINT8, UINT16, UINT32, UINT64 = 5, 6, 7, 8
FLOAT, DOUBLE, BOOLEAN, STRING, DATETIME, TEXT, UUID = 9, 10, 11, 12, 13, 14, 15
DATASET, BYTES, FILE, TEMPLATE = 16, 17, 18, 19
INT8_ARRAY, INT16_ARRAY, INT32_ARRAY, INT64_ARRAY = 22, 23, 24, 25
UINT8_ARRAY, UINT16_ARRAY, UINT32_ARRAY, UINT64_ARRAY = 26, 27, 28, 29
FLOAT_ARRAY, DOUBLE_ARRAY, BOOLEAN_ARRAY, STRING_ARRAY, DATETIME_ARRAY = 30, 31, 32, 33, 34

_ARRAY_FMT = {INT8_ARRAY: "b", INT16_ARRAY: "h", INT32_ARRAY: "i", INT64_ARRAY: "q",
              UINT8_ARRAY: "B", UINT16_ARRAY: "H", UINT32_ARRAY: "I", UINT64_ARRAY: "Q",
              FLOAT_ARRAY: "f", DOUBLE_ARRAY: "d", DATETIME_ARRAY: "Q"}


def now_ms() -> int:
    return int(time.time() * 1000)


# ---------------------------------------------------------------------------- encoding


@dataclass
class DataSet:
    columns: List[str]
    types: List[int]
    rows: List[List[Any]]


@dataclass
class Template:
    metrics: List["Metric"]
    definition: bool = False
    ref: Optional[str] = None
    version: Optional[str] = None
    parameters: List[tuple] = field(default_factory=list)  # (name, type, value)


@dataclass
class Metric:
    name: str
    datatype: int
    value: Any
    alias: Optional[int] = None
    props: Optional[Dict[str, tuple]] = None  # key: (type, value)
    historical: bool = False
    transient: bool = False


def _scalar(p: Proto, base: int, datatype: int, value: Any):
    """A value in the oneof starting at field `base` (int_value): int, long, float,
    double, boolean, string, in that order."""
    if datatype in (INT8, INT16, INT32, UINT8, UINT16, UINT32):
        p.uint(base, value & 0xFFFFFFFF)
    elif datatype in (INT64, UINT64, DATETIME):
        p.uint(base + 1, value & 0xFFFFFFFFFFFFFFFF)
    elif datatype == FLOAT:
        p.float(base + 2, value)
    elif datatype == DOUBLE:
        p.double(base + 3, value)
    elif datatype == BOOLEAN:
        p.bool(base + 4, value)
    elif datatype in (STRING, TEXT, UUID):
        p.string(base + 5, value)
    else:
        raise ValueError(f"no scalar for data type {datatype}")


def _array(datatype: int, values: List[Any]) -> bytes:
    """Sparkplug 3.0 arrays, packed little-endian into bytes_value."""
    if datatype == BOOLEAN_ARRAY:
        bits = bytearray((len(values) + 7) // 8)
        for i, v in enumerate(values):
            if v:
                bits[i // 8] |= 0x80 >> (i % 8)
        return struct.pack("<I", len(values)) + bytes(bits)
    if datatype == STRING_ARRAY:
        return b"".join(v.encode("utf-8") + b"\x00" for v in values)
    return struct.pack(f"<{len(values)}{_ARRAY_FMT[datatype]}", *values)


def _properties(props: Dict[str, tuple]) -> Proto:
    ps = Proto()
    for k in props:
        ps.string(1, k)
    for t, v in props.values():
        pv = Proto().uint(1, t)
        _scalar(pv, 3, t, v)
        ps.message(2, pv)
    return ps


def _dataset(d: DataSet) -> Proto:
    p = Proto().uint(1, len(d.columns))
    for c in d.columns:
        p.string(2, c)
    for t in d.types:
        p.uint(3, t)
    for row in d.rows:
        r = Proto()
        for t, v in zip(d.types, row):
            r.message(1, _scalar_into(Proto(), 1, t, v))
        p.message(4, r)
    return p


def _scalar_into(p: Proto, base: int, t: int, v: Any) -> Proto:
    _scalar(p, base, t, v)
    return p


def _template(t: Template, birth: bool) -> Proto:
    p = Proto()
    if t.version:
        p.string(1, t.version)
    for m in t.metrics:
        p.message(2, _metric(m, birth=True))
    for name, ptype, value in t.parameters:
        par = Proto().string(1, name).uint(2, ptype)
        _scalar(par, 3, ptype, value)
        p.message(3, par)
    if t.ref:
        p.string(4, t.ref)
    p.bool(5, t.definition)
    return p


def _metric(m: Metric, birth: bool, ts: Optional[int] = None) -> Proto:
    p = Proto()
    if birth or m.alias is None:
        p.string(1, m.name)
    if m.alias is not None:
        p.uint(2, m.alias)
    if ts is not None:
        p.uint(3, ts)
    if birth:
        p.uint(4, m.datatype)
    if m.historical:
        p.bool(5, True)
    if m.transient:
        p.bool(6, True)
    if m.value is None:
        p.bool(7, True)
    if birth and m.props:
        p.message(9, _properties(m.props))
    if m.value is None:
        return p
    if m.datatype in (BYTES, FILE):
        p.bytes(16, m.value)
    elif m.datatype == DATASET:
        p.message(17, _dataset(m.value))
    elif m.datatype == TEMPLATE:
        p.message(18, _template(m.value, birth))
    elif m.datatype >= INT8_ARRAY:
        p.bytes(16, _array(m.datatype, m.value))
    else:
        _scalar(p, 10, m.datatype, m.value)
    return p


def payload(metrics: List[Proto], seq: Optional[int], ts: Optional[int] = None,
            uuid: Optional[str] = None, body: Optional[bytes] = None) -> bytes:
    p = Proto().uint(1, now_ms() if ts is None else ts)
    for m in metrics:
        p.message(2, m)
    if seq is not None:
        p.uint(3, seq)
    if uuid:
        p.string(4, uuid)
    if body is not None:
        p.bytes(5, body)
    return p.encode()


def compressed(inner: bytes, seq: int) -> bytes:
    """A payload compressed the way Eclipse Tahu does it: the inner payload gzipped into the
    body, uuid SPBV1.0_COMPRESSED, and an "algorithm" metric naming the method."""
    algo = Metric("algorithm", STRING, "GZIP")
    return payload([_metric(algo, birth=True)], seq, uuid="SPBV1.0_COMPRESSED",
                   body=gzip.compress(inner, mtime=0))


# ---------------------------------------------------------------------------- the plant

EU = lambda unit: {"engUnit": (STRING, unit)}  # noqa: E731


def turbine_model() -> Template:
    """The TurbineModel template definition, sent in the park controller's NBIRTH."""
    return Template(definition=True, version="2.1", parameters=[("Manufacturer", STRING, "Vestas")],
                    metrics=[Metric("Rated Power", FLOAT, 3600.0), Metric("Rotor Diameter", FLOAT, 117.0),
                             Metric("Hub Height", FLOAT, 91.5), Metric("Model", STRING, "V117-3.6")])


def turbine_instance(i: int) -> Template:
    return Template(ref="TurbineModel", version="2.1", parameters=[("Manufacturer", STRING, "Vestas")],
                    metrics=[Metric("Rated Power", FLOAT, 3600.0), Metric("Rotor Diameter", FLOAT, 117.0),
                             Metric("Hub Height", FLOAT, 91.5 if i != 6 else 84.0),
                             Metric("Model", STRING, "V117-3.6")])


class Device:
    """A device's metrics: names, aliases and data types fixed at birth, values that move."""

    def __init__(self, name: str, metrics: List[Metric]):
        self.name = name
        self.metrics = {m.name: m for m in metrics}
        self.online = True
        self.last: Dict[str, Any] = {}

    def birth(self) -> List[Proto]:
        self.last = {n: m.value for n, m in self.metrics.items()}
        return [_metric(m, birth=True) for m in self.metrics.values()]

    def changes(self, ts: int) -> List[Proto]:
        out = []
        for n, m in self.metrics.items():
            if m.datatype in (TEMPLATE, FILE):
                continue
            if self.last.get(n) != m.value:
                out.append(_metric(m, birth=False, ts=ts))
                self.last[n] = m.value
        return out


class Alias:
    def __init__(self):
        self.n = 0

    def __call__(self) -> int:
        self.n += 1
        return self.n


def wind_devices(alias: Alias) -> List[Device]:
    out = []
    for i in range(1, 7):
        rng = random.Random(i)
        out.append(Device(f"wtg{i:02d}", [
            Metric("Power", FLOAT, 0.0, alias(), {**EU("kW"), "Range High": (FLOAT, 3600.0)}),
            Metric("Wind Speed", FLOAT, 0.0, alias(), EU("m/s")),
            Metric("Rotor Speed", DOUBLE, 0.0, alias(), EU("rpm")),
            Metric("Pitch Angles", FLOAT_ARRAY, [0.0, 0.0, 0.0], alias(), EU("deg")),
            Metric("Breakers", BOOLEAN_ARRAY, [True, True, False, True], alias()),
            Metric("State", STRING, "RUNNING", alias()),
            Metric("Running", BOOLEAN, True, alias()),
            Metric("Alarm Code", INT16, 0, alias()),
            Metric("Yaw Error", INT8, 0, alias(), EU("deg")),
            Metric("Nacelle Temp", DOUBLE, 31.0, alias(), EU("°C")),
            Metric("Energy Total", UINT64, 41_000_000 + i * 977_311, alias(), EU("kWh")),
            Metric("Run Hours", UINT32, 52_000 + i * 113, alias(), EU("h")),
            Metric("Grid Code", INT64, -2 ** 40 + i, alias()),
            Metric("Tower Strain", INT32_ARRAY, [0, 0, 0, 0], alias(), EU("µε")),
            Metric("Vibration Spectrum", UINT16_ARRAY, [0] * 8, alias()),
            Metric("Event Log", STRING_ARRAY, ["start", "grid ok"], alias()),
            Metric("Last Service", DATETIME, 1_757_000_000_000 + i * 86_400_000, alias()),
            Metric("Serial", BYTES, bytes([0x0B, 0x3F, i, rng.randrange(256)]), alias()),
            Metric("Setpoint/Max Power", DOUBLE, 3600.0, alias(), EU("kW")),
            Metric("Model", TEMPLATE, turbine_instance(i), alias()),
        ]))
    return out


def pv_devices(alias: Alias) -> List[Device]:
    out = []
    for i in (1, 2):
        out.append(Device(f"inv{i:02d}", [
            Metric("AC Power", DOUBLE, 0.0, alias(), EU("kW")),
            Metric("DC Voltage", FLOAT, 0.0, alias(), EU("V")),
            Metric("Status", INT32, 0, alias()),
            Metric("Phase Currents", DOUBLE_ARRAY, [0.0, 0.0, 0.0], alias(), EU("A")),
            Metric("Cabinet Temps", INT8_ARRAY, [21, 22], alias(), EU("°C")),
            Metric("String Currents", UINT8_ARRAY, [0] * 6, alias(), EU("0.1 A")),
            Metric("Energy Counters", UINT32_ARRAY, [0, 0], alias(), EU("kWh")),
            Metric("Fault Words", INT16_ARRAY, [0, 0], alias()),
            Metric("Totals", INT64_ARRAY, [0, 0], alias()),
            Metric("Serials", UINT64_ARRAY, [0x1F00_0000_0000_0000 + i, 42], alias()),
            Metric("Sample Times", DATETIME_ARRAY, [0, 0], alias()),
            Metric("Counters", UINT16_ARRAY, [0, 0, 0], alias()),
        ]))
    return out


# ---------------------------------------------------------------------------- the actors


class Edge:
    """One edge node: its MQTT connection, bdSeq and seq, node metrics and devices."""

    def __init__(self, group: str, node: str, node_metrics: Callable[[Alias], List[Metric]],
                 devices: Callable[[Alias], List[Device]], host: str, port: int, out: "Out"):
        self.group, self.node, self.host, self.port, self.out = group, node, host, port, out
        alias = Alias()
        self.node_metrics = {m.name: m for m in node_metrics(alias)}
        self.devices = {d.name: d for d in devices(alias)}
        self.bd_seq = 0
        self.seq = 0
        self.client: Optional[Client] = None
        self.down_until = 0.0
        self.last_node: Dict[str, Any] = {}

    def topic(self, kind: str, device: Optional[str] = None) -> str:
        return f"{NS}/{self.group}/{kind}/{self.node}" + (f"/{device}" if device else "")

    def next_seq(self) -> int:
        s = self.seq
        self.seq = (self.seq + 1) % 256
        return s

    def ndeath(self) -> bytes:
        return payload([_metric(Metric("bdSeq", UINT64, self.bd_seq), birth=True)], None)

    def connect(self):
        c = Client(self.host, self.port, f"sparkplug-{self.group}-{self.node}", keepalive=30,
                   timeout=10)
        c.connect(clean=True, will=(self.topic("NDEATH"), self.ndeath(), 1, False))
        c.subscribe([(self.topic("NCMD"), 1), (f"{NS}/{self.group}/DCMD/{self.node}/+", 1),
                     (f"{NS}/STATE/{HOST_ID}", 1)])
        self.client = c
        self.out.note(f"{self.group}/{self.node} connected, bdSeq {self.bd_seq}")
        self.births()

    def births(self):
        """NBIRTH (seq 0, bdSeq, every node metric), then a DBIRTH per online device."""
        self.seq = 0
        self.node_metrics["bdSeq"].value = self.bd_seq
        self.last_node = {n: m.value for n, m in self.node_metrics.items()}
        self.send("NBIRTH", None, payload([_metric(m, birth=True) for m in self.node_metrics.values()],
                                          self.next_seq()))
        for d in self.devices.values():
            if d.online:
                self.send("DBIRTH", d.name, payload(d.birth(), self.next_seq()))

    def send(self, kind: str, device: Optional[str], body: bytes, qos: int = 0):
        assert self.client is not None
        self.client.publish(self.topic(kind, device), body, qos=qos)
        self.out.sent(self.topic(kind, device), body)

    def data(self, compress: bool = False):
        ts = now_ms()
        changed = []
        for n, m in self.node_metrics.items():
            if m.datatype != TEMPLATE and self.last_node.get(n) != m.value:
                changed.append(_metric(m, birth=False, ts=ts))
                self.last_node[n] = m.value
        if changed:
            seq = self.next_seq()
            inner = payload(changed, seq, ts=ts)
            self.send("NDATA", None, compressed(inner, seq) if compress else inner)
        for d in self.devices.values():
            if d.online:
                ch = d.changes(ts)
                if ch:
                    self.send("DDATA", d.name, payload(ch, self.next_seq(), ts=ts))

    def device_death(self, name: str):
        d = self.devices[name]
        d.online = False
        self.send("DDEATH", name, payload([], self.next_seq()))

    def device_birth(self, name: str):
        d = self.devices[name]
        d.online = True
        self.send("DBIRTH", name, payload(d.birth(), self.next_seq()))

    def lose_power(self, seconds: float):
        """No DISCONNECT: the broker publishes the NDEATH Will."""
        if self.client:
            self.client.drop()
        self.client = None
        self.bd_seq = (self.bd_seq + 1) % 256
        self.down_until = time.monotonic() + seconds
        self.out.note(f"{self.group}/{self.node} lost power; its NDEATH is the broker's to send")

    def restart(self, seconds: float):
        """A clean shutdown: the NDEATH from the edge node itself, then DISCONNECT."""
        if self.client:
            self.send("NDEATH", None, self.ndeath(), qos=1)
            self.client.disconnect()
        self.client = None
        self.bd_seq = (self.bd_seq + 1) % 256
        self.down_until = time.monotonic() + seconds
        self.out.note(f"{self.group}/{self.node} restarting")

    def handle(self, topic: str, body: bytes):
        """NCMD Node Control/Rebirth, and DCMD writes to Setpoint/Max Power."""
        if topic == self.topic("NCMD") and b"Node Control/Rebirth" in body:
            self.out.note(f"{self.group}/{self.node}: rebirth requested")
            self.births()
        elif topic.startswith(f"{NS}/{self.group}/DCMD/{self.node}/"):
            dev = self.devices.get(topic.rsplit("/", 1)[1])
            value = _first_double(body)
            if dev and value is not None:
                dev.metrics["Setpoint/Max Power"].value = value
                self.out.note(f"{self.group}/{self.node}/{dev.name}: max power set to {value:g} kW")

    def tend(self) -> bool:
        """Read what arrived; reconnect when it is time. False while down."""
        if self.client is None:
            if time.monotonic() < self.down_until:
                return False
            try:
                self.connect()
            except (MqttError, OSError) as e:
                self.out.note(f"{self.group}/{self.node}: {e}")
                self.client = None
                self.down_until = time.monotonic() + 5
                return False
        try:
            self.client.drain()
            while self.client.inbox:
                m = self.client.inbox.pop(0)
                self.handle(m.topic, m.payload)
            self.client.ping_if_idle()
        except (MqttError, OSError) as e:
            self.out.note(f"{self.group}/{self.node}: {e}; reconnecting")
            self.client.drop()
            self.client = None
            self.bd_seq = (self.bd_seq + 1) % 256
            self.down_until = time.monotonic() + 2
            return False
        return True


def _fields(b: bytes):
    """A protobuf message's fields: (number, wire type, value), the value an int, or bytes
    for the fixed and length-delimited types."""
    i = 0
    while i < len(b):
        key, i = _uvarint(b, i)
        number, wire = key >> 3, key & 7
        if wire == 0:
            v, i = _uvarint(b, i)
        elif wire == 1:
            v, i = b[i:i + 8], i + 8
        elif wire == 2:
            n, i = _uvarint(b, i)
            v, i = b[i:i + n], i + n
        elif wire == 5:
            v, i = b[i:i + 4], i + 4
        else:
            raise ValueError(f"wire type {wire}")
        yield number, wire, v


def _uvarint(b: bytes, i: int):
    n = shift = 0
    while True:
        byte = b[i]
        i += 1
        n |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return n, i


def _first_double(body: bytes) -> Optional[float]:
    """The double_value (field 13) of a command payload's first metric."""
    try:
        for number, _, metric in _fields(body):
            if number == 2:
                for n, wire, v in _fields(metric):
                    if n == 13 and wire == 1:
                        return struct.unpack("<d", v)[0]
                return None
    except (ValueError, IndexError, struct.error):
        return None
    return None


class Host:
    """The primary host application: STATE online (retained), its Will offline."""

    def __init__(self, host: str, port: int, out: "Out"):
        self.host, self.port, self.out = host, port, out
        self.client: Optional[Client] = None

    def state(self, online: bool) -> bytes:
        return json.dumps({"online": online, "timestamp": now_ms()}).encode()

    def connect(self):
        c = Client(self.host, self.port, HOST_ID, keepalive=30, timeout=10)
        topic = f"{NS}/STATE/{HOST_ID}"
        c.connect(clean=True, will=(topic, self.state(False), 1, True))
        c.subscribe([(f"{NS}/#", 0)])
        c.publish(topic, self.state(True), qos=1, retain=True)
        self.client = c
        self.out.note("host scada-host online")

    def command(self, group: str, node: str, device: Optional[str], metric: Metric):
        if self.client is None:
            return
        kind = "DCMD" if device else "NCMD"
        topic = f"{NS}/{group}/{kind}/{node}" + (f"/{device}" if device else "")
        body = payload([_metric(metric, birth=True)], None)
        self.client.publish(topic, body, qos=0)
        self.out.sent(topic, body)

    def tend(self):
        try:
            if self.client is None:
                self.connect()
            self.client.drain()
            self.client.inbox.clear()  # the host reads everything; the demo needs none of it
            self.client.ping_if_idle()
        except (MqttError, OSError) as e:
            self.out.note(f"host: {e}")
            if self.client:
                self.client.drop()
            self.client = None

    def close(self):
        if self.client:
            self.client.publish(f"{NS}/STATE/{HOST_ID}", self.state(False), qos=1, retain=True)
            self.client.disconnect()


class Out:
    def __init__(self, quiet: bool):
        self.quiet = quiet
        self.count = 0

    def note(self, text: str):
        print(f"sparkplug: {text}", flush=True)

    def sent(self, topic: str, body: bytes):
        self.count += 1
        if not self.quiet:
            print(f"sparkplug: → {topic} ({len(body)} B)", flush=True)


# ---------------------------------------------------------------------------- the play


def wind_node_metrics(alias: Alias) -> List[Metric]:
    return [
        Metric("bdSeq", UINT64, 0, alias()),
        Metric("Node Control/Rebirth", BOOLEAN, False, alias()),
        Metric("Node Control/Next Server", BOOLEAN, False, alias()),
        Metric("Node Info/Firmware", STRING, "ppc 4.12.3", alias()),
        Metric("Node Info/ID", UUID, "3f2b8a1e-6c57-4d0a-9b1e-2a7d5c9e4f10", alias()),
        Metric("Node Info/Description", TEXT, "Park power controller, Falster wind farm, 6 × V117-3.6", alias()),
        Metric("Node Info/Commissioned", DATETIME, 1_552_000_000_000, alias()),
        Metric("Node Info/Config", FILE, b'{"curtailment":"enabled","ramp_kw_s":120}\n', alias()),
        Metric("Park/Active Power", FLOAT, 0.0, alias(), {**EU("kW"), "Range Low": (FLOAT, 0.0),
                                                           "Range High": (FLOAT, 21600.0)}),
        Metric("Park/Setpoint", DOUBLE, 21600.0, alias(), EU("kW")),
        Metric("Park/Grid Frequency", DOUBLE, 50.0, alias(), EU("Hz")),
        Metric("Park/Uptime", UINT32, 0, alias(), EU("s")),
        Metric("Park/Alarms", DATASET, DataSet(["Code", "Text", "Active", "Since"],
                                               [INT32, STRING, BOOLEAN, DATETIME], []), alias()),
        Metric("Park/Curtailment Reason", STRING, None, alias()),
        Metric("TurbineModel", TEMPLATE, turbine_model(), alias()),
    ]


def pv_node_metrics(alias: Alias) -> List[Metric]:
    return [
        Metric("bdSeq", UINT64, 0, alias()),
        Metric("Node Control/Rebirth", BOOLEAN, False, alias()),
        Metric("Node Info/Firmware", STRING, "scada 2.0.7", alias()),
        Metric("Park/AC Power", DOUBLE, 0.0, alias(), EU("kW")),
        Metric("Park/Irradiance", FLOAT, 0.0, alias(), EU("W/m²")),
        Metric("Park/Module Temp", INT16, 15, alias(), EU("°C")),
        Metric("Park/Tracker Angles", INT8_ARRAY, [0, 0, 0, 0], alias(), EU("deg")),
    ]


def step(wind: Edge, pv: Edge, t: float, start: float):
    """Move every value on to wall time `t`."""
    minute = t / 60.0
    rng = random.Random(int(t))
    wind_ms = 8.5 + 3.0 * math.sin(minute / 7.0) + rng.uniform(-0.5, 0.5)
    total = 0.0
    for i, d in enumerate(wind.devices.values(), 1):
        m = d.metrics
        ws = max(0.0, wind_ms + rng.uniform(-0.8, 0.8))
        limit = m["Setpoint/Max Power"].value
        p = 0.0 if not d.online else min(limit, max(0.0, 3600.0 * ((ws - 3.0) / 9.0) ** 3))
        total += p
        m["Power"].value = round(p, 1)
        m["Wind Speed"].value = round(ws, 2)
        m["Rotor Speed"].value = round(min(14.8, 5.0 + ws * 0.85), 2) if p else 0.0
        pitch = round(max(0.0, (ws - 11.5) * 2.4), 1)
        m["Pitch Angles"].value = [pitch, round(pitch + 0.1, 1), round(pitch - 0.1, 1)]
        m["State"].value = "RUNNING" if p else "IDLE"
        m["Running"].value = p > 0
        m["Yaw Error"].value = rng.randint(-6, 6)
        m["Nacelle Temp"].value = round(31.0 + p / 400.0 + rng.uniform(-0.3, 0.3), 1)
        m["Energy Total"].value += int(p / 720)
        m["Tower Strain"].value = [rng.randint(-400, 400) for _ in range(4)]
        m["Vibration Spectrum"].value = [rng.randint(0, 900) for _ in range(8)]
        m["Alarm Code"].value = 3141 if pitch > 8 else 0
    n = wind.node_metrics
    n["Park/Active Power"].value = round(total, 1)
    n["Park/Grid Frequency"].value = round(50.0 + 0.02 * math.sin(minute / 3.0) + rng.uniform(-0.004, 0.004), 4)
    n["Park/Uptime"].value = int(t - start)
    curtailed = total > 0.9 * n["Park/Setpoint"].value
    n["Park/Curtailment Reason"].value = "grid operator limit" if curtailed else None
    hour = (t / 3600.0) % 24
    sun = max(0.0, math.sin((hour - 6.0) / 12.0 * math.pi))
    pvp = 0.0
    for i, d in enumerate(pv.devices.values(), 1):
        m = d.metrics
        ac = round(max(0.0, 2400.0 * sun + rng.uniform(-30, 30)), 1) if sun else 0.0
        pvp += ac
        m["AC Power"].value = ac
        m["DC Voltage"].value = round(640.0 + rng.uniform(-10, 10), 1) if sun else 0.0
        m["Status"].value = 1 if ac else 0
        amps = ac * 1000 / (1.732 * 630) if ac else 0.0  # a central inverter's 630 V AC side
        m["Phase Currents"].value = [round(amps + rng.uniform(-1, 1), 2) for _ in range(3)]
        m["Cabinet Temps"].value = [int(18 + 15 * sun + rng.uniform(-1, 1)) for _ in range(2)]
        m["String Currents"].value = [min(255, int(98 * sun + rng.uniform(0, 3))) for _ in range(6)]
        m["Energy Counters"].value = [int(t // 3600) % 100_000, int(t // 60) % 1_000_000]
        m["Totals"].value = [int(t // 10), -int(t // 100)]
        m["Sample Times"].value = [int(t * 1000) - 1000, int(t * 1000)]
        m["Counters"].value = [int(t) % 65536, i, 7]
        m["Fault Words"].value = [0, -1 if rng.random() < 0.05 else 0]
    pn = pv.node_metrics
    pn["Park/AC Power"].value = round(pvp, 1)
    pn["Park/Irradiance"].value = round(950.0 * sun, 1)
    pn["Park/Module Temp"].value = int(15 + 30 * sun)
    pn["Park/Tracker Angles"].value = [int(-60 + 120 * ((hour - 6) / 12)) if sun else 0] * 4


def alarms(t: float) -> DataSet:
    rng = random.Random(int(t // 60))
    rows = [[3141, "Pitch deviation blade B", True, int(t * 1000) - 120_000],
            [2210, "Gearbox oil temperature high", rng.random() < 0.5, int(t * 1000) - 3_600_000],
            [1007, "Yaw service completed", False, int(t * 1000) - 86_400_000]]
    rows += [[9000 + k, f"Converter event {k}", False, int(t * 1000) - k * 60_000] for k in range(1, 30)]
    return DataSet(["Code", "Text", "Active", "Since"], [INT32, STRING, BOOLEAN, DATETIME], rows)


def run(args) -> int:
    out = Out(args.quiet)
    stop: List[int] = []

    def on_signal(signum, _frame):
        stop.append(signum)
        signal.signal(signum, signal.SIG_DFL)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    unit = 6.0 if args.fast else 60.0  # seconds per scheduled "minute"
    host = Host(args.host, args.port, out)
    wind = Edge("wf-falster", "ppc", wind_node_metrics, wind_devices, args.host, args.port, out)
    pv = Edge("pv-lolland", "scada", pv_node_metrics, pv_devices, args.host, args.port, out)
    start = time.time()
    wind.node_metrics["Park/Alarms"].value = alarms(start)
    t0 = time.monotonic()
    done: Dict[str, int] = {}
    in_service: Optional[str] = None
    service_until = 0.0
    out.note(f"edge nodes wf-falster/ppc and pv-lolland/scada, host {HOST_ID}, to {args.host}:{args.port}")

    def due(name: str, every: float, offset: float = 0.0) -> bool:
        k = int((time.monotonic() - t0 - offset * unit) // (every * unit))
        if k >= 1 and done.get(name) != k:
            done[name] = k
            return True
        return False

    next_data = 0.0
    while not stop:
        host.tend()
        up_w, up_p = wind.tend(), pv.tend()
        now = time.monotonic()
        if now >= next_data:
            next_data = now + (1.0 if args.fast else 5.0)
            step(wind, pv, time.time(), start)
            if up_w:
                compress = due("alarms", 6)
                if compress:
                    wind.node_metrics["Park/Alarms"].value = alarms(time.time())
                wind.data(compress=compress)
            if up_p:
                pv.data()
        if up_w and due("rebirth", 10, 0.5):
            host.command("wf-falster", "ppc", None, Metric("Node Control/Rebirth", BOOLEAN, True))
        if up_w and due("setpoint", 3, 0.2):
            dev = f"wtg{random.Random(int(now)).randint(1, 6):02d}"
            host.command("wf-falster", "ppc", dev,
                         Metric("Setpoint/Max Power", DOUBLE, float(random.Random(int(now)).choice([3000, 3300, 3600]))))
        if up_w and in_service is None and due("service", 12, 1):
            in_service = f"wtg0{random.Random(int(now)).randint(1, 6)}"
            wind.device_death(in_service)
            service_until = now + unit
        if up_w and in_service and now >= service_until:
            wind.device_birth(in_service)
            in_service = None
        if up_p and due("power-loss", 15, 2):
            pv.lose_power(20.0 if not args.fast else 4.0)
        if up_w and due("restart", 20, 3):
            wind.restart(10.0 if not args.fast else 2.0)
        if args.seconds and now - t0 >= args.seconds:
            break
        time.sleep(0.1)
    for e in (wind, pv):
        if e.client:
            e.restart(0)
    host.close()
    out.note(f"stopped after {out.count} Sparkplug messages")
    return 0


def dump(seconds: float) -> int:
    """Print the births and the first data as JSON lines (topic, size, hex), without a broker."""
    class Fake:
        inbox: list = []

        def publish(self, topic, body, qos=0, retain=False, props=None):
            print(json.dumps({"topic": topic, "bytes": len(body), "hex": body.hex()}))

    out = Out(True)
    wind = Edge("wf-falster", "ppc", wind_node_metrics, wind_devices, "", 0, out)
    pv = Edge("pv-lolland", "scada", pv_node_metrics, pv_devices, "", 0, out)
    t = time.time()
    wind.node_metrics["Park/Alarms"].value = alarms(t)
    for e in (wind, pv):
        e.client = Fake()  # type: ignore[assignment]
        print(json.dumps({"topic": e.topic("NDEATH"), "will": True, "hex": e.ndeath().hex()}))
        step(wind, pv, t, t)
        e.births()
    for k in range(int(seconds)):
        step(wind, pv, t + k + 1, t)
        wind.node_metrics["Park/Alarms"].value = alarms(t)
        wind.data(compress=k == 0)
        pv.data()
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.environ.get("SIM_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("SIM_PORT", "1883")))
    ap.add_argument("--quiet", action="store_true",
                    default=os.environ.get("SIM_QUIET", "") in ("1", "true", "yes"),
                    help="print only the lifecycle, not each message")
    ap.add_argument("--fast", action="store_true", help="the schedule's minutes become 6 s")
    ap.add_argument("--seconds", type=float, default=0.0, help="stop after this many seconds")
    ap.add_argument("--dump", type=float, metavar="SECONDS",
                    help="print the births and SECONDS of data as JSON lines; no broker")
    args = ap.parse_args(argv)
    if args.dump is not None:
        return dump(args.dump)
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
