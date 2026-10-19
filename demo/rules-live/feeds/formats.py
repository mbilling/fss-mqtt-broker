"""Every payload format fss-mqtt decodes, each on its own topic under formats/, retained.

    python3 formats.py                       # publish them all every 60 s to 127.0.0.1:1883
    python3 formats.py --once --port 1884    # publish them all once and exit
    python3 formats.py --list                # the topics, content types and sizes; no broker

The demo's devices send JSON, and each turbine a Parquet fast log a minute. This sends
the rest of what an MQTT explorer meets in the field, about the same plants (the wind farm
wf-falster, the solar park pv-lolland, the peaker pk-koge) and homes: text (JSON, JSON
lines, XML, CSV with comma, semicolon or pipe, TSV, plain text), files (Parquet, Avro,
Arrow IPC, SQLite), changes (SQLite changesets and patchsets), compact encodings
(MessagePack, CBOR, Protobuf and its gRPC frame, Avro with a schema registry), and
compressed payloads (gzip, zstd). Each is written here with the standard library (see
binfmt.py, arrow.py and demo/rules/sim/parquet.py), published at QoS 1 with MQTT 5's
content type and payload format indicator, and retained, so a client that subscribes to
formats/# gets one of each at once. The values move with the wall clock; formats/empty is
an empty payload, so it is the one not retained.

The zstd variants (parquet-zstd, avro-zstd, arrow, zstd) need Python 3.14's
compression.zstd, as in the demo stack; an older Python skips them and says so.

Options can also be set in the environment: SIM_HOST, SIM_PORT and FORMATS_INTERVAL
(seconds). Exit 0, or 2 for a bad option.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import math
import os
import random
import signal
import sqlite3
import struct
import sys
import tempfile
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable, NamedTuple

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, os.environ.get("SIM_LIB") or str(HERE.parents[1] / "rules"))

import arrow  # noqa: E402
import binfmt as B  # noqa: E402
from sim import parquet  # noqa: E402
from sim.mqtt import Client, MqttError  # noqa: E402

ROOT = "formats"
WF, PV, PK = "wf-falster", "pv-lolland", "pk-koge"
TURBINES = [f"wtg{i:02d}" for i in range(1, 7)]
METERS = ["571313174112345678", "571313174112345685", "571313174112345692", "571313174112345609"]


def iso(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ms(t: float) -> int:
    return int(t * 1000)


class Plant:
    """The wind farm's state at a minute, the same for every format published that minute."""

    def __init__(self, now: float):
        self.now = now
        minute = int(now // 60)
        self.rng = random.Random(minute)
        hour = (now / 3600.0) % 24
        self.wind = round(8.5 + 3.0 * math.sin(minute / 47.0) + self.rng.uniform(-0.6, 0.6), 2)
        self.freq = round(50.0 + 0.02 * math.sin(minute / 3.0) + self.rng.uniform(-0.005, 0.005), 4)
        self.sun = max(0.0, math.sin((hour - 6.0) / 12.0 * math.pi))
        self.turbines = []
        for i, t in enumerate(TURBINES):
            stopped = (minute + i * 7) % 61 == 0
            ws = max(0.0, self.wind + self.rng.uniform(-0.8, 0.8))
            p = 0.0 if stopped else min(3600.0, max(0.0, 3600.0 * ((ws - 3.0) / 9.0) ** 3))
            self.turbines.append({
                "id": t, "state": "STOPPED" if stopped else ("RUNNING" if p > 0 else "IDLE"),
                "power_kw": round(p, 1), "wind_ms": round(ws, 2),
                "rotor_rpm": 0.0 if stopped else round(min(14.8, 5.0 + ws * 0.85), 2),
                "pitch_deg": 88.0 if stopped else round(max(0.0, (ws - 11.5) * 2.4), 2),
                "nacelle_temp_c": round(31.0 + 0.002 * p + self.rng.uniform(-0.5, 0.5), 1),
            })
        self.power_mw = round(sum(t["power_kw"] for t in self.turbines) / 1000.0, 3)


# ---------------------------------------------------------------------------- the formats


def f_json(p: Plant) -> bytes:
    doc = {"site": WF, "ts": iso(p.now), "power_mw": p.power_mw, "wind_ms": p.wind,
           "availability_pct": round(100.0 * sum(t["state"] != "STOPPED" for t in p.turbines)
                                     / len(p.turbines), 1),
           "grid": {"frequency_hz": p.freq, "voltage_kv": round(132.0 + p.rng.uniform(-1, 1), 2)},
           "turbines": p.turbines}
    return json.dumps(doc, ensure_ascii=False).encode()


def f_jsonl(p: Plant) -> bytes:
    lines = []
    for k in range(10, 0, -1):
        t = p.now - 6 * k
        rng = random.Random(int(t))
        lines.append(json.dumps({"ts": iso(t), "site": WF, "frequency_hz": round(p.freq + rng.uniform(-0.004, 0.004), 4),
                                 "power_mw": round(p.power_mw * (1 + rng.uniform(-0.02, 0.02)), 3)}))
    return ("\n".join(lines) + "\n").encode()


def f_xml(p: Plant) -> bytes:
    start = p.now - p.now % 900 + 900
    root = ET.Element("ProductionSchedule", {"xmlns": "urn:example:energy:schedule:1",
                                             "site": WF, "created": iso(p.now), "revision": "3"})
    ET.SubElement(root, "Sender", {"role": "BRP"}).text = "Falster Wind ApS"
    period = ET.SubElement(root, "Period", {"start": iso(start), "end": iso(start + 4 * 3600),
                                            "resolution": "PT15M"})
    for i in range(16):
        point = ET.SubElement(period, "Point", {"position": str(i + 1)})
        mw = max(0.0, p.power_mw * (1 + 0.15 * math.sin(i / 3.0)))
        ET.SubElement(point, "Quantity", {"unit": "MW"}).text = f"{mw:.2f}"
    ET.indent(root)
    return b'<?xml version="1.0" encoding="UTF-8"?>\n' + ET.tostring(root, encoding="utf-8")


def f_csv(p: Plant) -> bytes:
    day = dt.datetime.fromtimestamp(p.now, dt.timezone.utc).date() - dt.timedelta(days=1)
    rows = ["date,turbine,energy_mwh,availability_pct,avg_wind_ms,max_power_kw,alarms"]
    rng = random.Random(day.toordinal())
    for t in TURBINES:
        rows.append(f"{day},{t},{rng.uniform(38, 72):.2f},{rng.uniform(96, 100):.1f},"
                    f"{rng.uniform(7, 11):.1f},{3600 if rng.random() > 0.2 else 3000},{rng.randint(0, 3)}")
    return ("\n".join(rows) + "\n").encode()


def f_csv_semicolon(p: Plant) -> bytes:
    """A Danish meter export: semicolons, decimal commas, Danish headers."""
    start = p.now - p.now % 3600 - 12 * 3600
    rows = ["Måler-id;Fra;Til;Forbrug (kWh);Pris (kr./kWh)"]
    for h in range(12):
        t = start + h * 3600
        rng = random.Random(int(t))
        kwh = 0.25 + 0.6 * max(0.0, math.sin(((t / 3600) % 24 - 5) / 19 * math.pi)) + rng.uniform(0, 0.3)
        price = 1.2 + 1.6 * (1 if 17 <= (t / 3600) % 24 < 21 else 0) + rng.uniform(-0.2, 0.2)
        rows.append(f"{METERS[0]};{iso(t)};{iso(t + 3600)};{kwh:.3f}".replace(".", ",")
                    + f";{price:.2f}".replace(".", ","))
    return ("\r\n".join(rows) + "\r\n").encode()


def f_tsv(p: Plant) -> bytes:
    rows = ["string_id\tinverter\tvoltage_v\tcurrent_a\tpower_w\tmodule_temp_c"]
    rng = random.Random(int(p.now // 60) + 1)
    for inv in range(1, 3):
        for s in range(1, 7):
            v = 0.0 if p.sun == 0 else 620 + rng.uniform(-15, 15)
            a = round(9.8 * p.sun + rng.uniform(-0.2, 0.2), 2) if p.sun else 0.0
            rows.append(f"{PV}-inv{inv:02d}-s{s:02d}\tinv{inv:02d}\t{v:.1f}\t{a:.2f}\t{v * a:.0f}"
                        f"\t{14 + 30 * p.sun + rng.uniform(-1, 1):.1f}")
    return ("\n".join(rows) + "\n").encode()


def f_psv(p: Plant) -> bytes:
    """A SCADA alarm list, pipe-delimited and sent as plain text: the viewer finds the
    delimiter itself."""
    rows = ["time|unit|code|severity|text"]
    for i, t in enumerate(p.turbines):
        if t["state"] == "STOPPED":
            rows.append(f"{iso(p.now - 30)}|{t['id']}|3141|FAULT|Pitch deviation blade B")
    rows += [f"{iso(p.now - 600)}|{WF}-sub|2210|WARNING|Transformer oil temperature 71 °C",
             f"{iso(p.now - 1500)}|wtg04|1007|INFO|Yaw service completed"]
    return ("\n".join(rows) + "\n").encode()


def f_parquet(p: Plant) -> bytes:
    """Hourly energy per turbine for the last 24 hours: strings, nullable columns, GZIP,
    two row groups."""
    end = p.now - p.now % 3600
    cols = {"hour": [], "turbine": [], "energy_kwh": [], "availability_pct": [], "alarms": [],
            "curtailed": [], "note": []}
    for h in range(24, 0, -1):
        t = end - h * 3600
        rng = random.Random(int(t))
        for tid in TURBINES:
            down = rng.random() < 0.03
            cols["hour"].append(ms(t))
            cols["turbine"].append(tid)
            cols["energy_kwh"].append(None if down else round(rng.uniform(300, 3500), 1))
            cols["availability_pct"].append(0.0 if down else round(rng.uniform(97, 100), 2))
            cols["alarms"].append(rng.randint(0, 2))
            cols["curtailed"].append(rng.random() < 0.1)
            cols["note"].append("comms lost" if down else None)
    return parquet.write([
        ("hour", "timestamp_ms", cols["hour"]), ("turbine", "string", cols["turbine"]),
        ("energy_kwh", "double?", cols["energy_kwh"]),
        ("availability_pct", "double", cols["availability_pct"]),
        ("alarms", "int32", cols["alarms"]), ("curtailed", "bool", cols["curtailed"]),
        ("note", "string?", cols["note"]),
    ], [("site", WF), ("resolution", "PT1H")], codec="gzip", row_group_size=72,
        created_by="mqttd demo/rules-live feeds/formats.py (sim/parquet.py)")


def f_parquet_zstd(p: Plant) -> bytes:
    """Quarter-hour meter readings for four homes: ZSTD."""
    end = p.now - p.now % 900
    ts, meter, imp, exp, phases = [], [], [], [], []
    for q in range(48, 0, -1):
        t = end - q * 900
        rng = random.Random(int(t))
        for i, m in enumerate(METERS):
            ts.append(ms(t))
            meter.append(m)
            imp.append(round(rng.uniform(0.02, 0.9), 3))
            exp.append(round(rng.uniform(0, 1.2) * p.sun, 3) if i < 2 else None)
            phases.append(3 if i != 3 else 1)
    return parquet.write([("ts", "timestamp_ms", ts), ("meter_id", "string", meter),
                          ("import_kwh", "double", imp), ("export_kwh", "double?", exp),
                          ("phases", "int32", phases)],
                         [("unit", "kWh"), ("resolution", "PT15M")], codec="zstd",
                         created_by="mqttd demo/rules-live feeds/formats.py (sim/parquet.py)")


def f_zstd(p: Plant) -> bytes:
    """A zstd-compressed Parquet file (LZ4_RAW pages inside): the solar park, 5-minute."""
    from compression import zstd
    end = p.now - p.now % 300
    ts, inv, power, irr = [], [], [], []
    for k in range(36, 0, -1):
        t = end - k * 300
        rng = random.Random(int(t))
        for i in (1, 2):
            ts.append(ms(t))
            inv.append(f"inv{i:02d}")
            power.append(round(max(0.0, 2400 * p.sun + rng.uniform(-60, 60)), 1) if p.sun else 0.0)
            irr.append(round(950 * p.sun + rng.uniform(-20, 20), 1) if p.sun else 0.0)
    inner = parquet.write([("ts", "timestamp_ms", ts), ("inverter", "string", inv),
                           ("ac_power_kw", "double", power), ("irradiance_wm2", "double", irr)],
                          [("site", PV)], codec="lz4_raw",
                          created_by="mqttd demo/rules-live feeds/formats.py (sim/parquet.py)")
    return zstd.compress(inner)


def f_gzip(p: Plant) -> bytes:
    summary = {"ts": iso(p.now), "plants": [
        {"site": WF, "kind": "wind", "power_mw": p.power_mw, "capacity_mw": 21.6},
        {"site": PV, "kind": "solar", "power_mw": round(17.5 * p.sun, 2), "capacity_mw": 18.0},
        {"site": PK, "kind": "peaker", "power_mw": 0.0 if p.freq > 49.9 else 12.0,
         "capacity_mw": 40.0}]}
    return gzip.compress(json.dumps(summary).encode(), mtime=0)


EVENT_SCHEMA = {
    "type": "record", "name": "TurbineEvent", "namespace": "dk.example.wind",
    "fields": [
        {"name": "ts", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "site", "type": "string"},
        {"name": "turbine", "type": "string"},
        {"name": "code", "type": "int"},
        {"name": "severity", "type": {"type": "enum", "name": "Severity",
                                      "symbols": ["INFO", "WARNING", "FAULT"]}},
        {"name": "text", "type": "string"},
        {"name": "value", "type": ["null", "double"], "default": None},
    ],
}
EVENT_CODES = [(3141, "FAULT", "Pitch deviation blade B", 7.2), (2210, "WARNING", "Gearbox oil temperature high", 82.4),
               (1001, "INFO", "Turbine started", None), (1002, "INFO", "Turbine stopped by operator", None),
               (4410, "WARNING", "Yaw misalignment", 11.5), (5003, "FAULT", "Grid loss", None)]


def _events(p: Plant, n: int) -> list:
    out = []
    for k in range(n, 0, -1):
        t = p.now - p.now % 300 - k * 300
        rng = random.Random(int(t))
        code, sev, text, value = EVENT_CODES[rng.randrange(len(EVENT_CODES))]
        out.append({"ts": ms(t), "site": WF, "turbine": rng.choice(TURBINES), "code": code,
                    "severity": sev, "text": text, "value": value})
    return out


def f_avro(p: Plant) -> bytes:
    return B.avro_container(EVENT_SCHEMA, _events(p, 24), codec="deflate", block=12,
                            metadata=[("site", WF.encode())])


SESSION_SCHEMA = {
    "type": "record", "name": "ChargingSession", "namespace": "dk.example.home",
    "fields": [
        {"name": "session_id", "type": "string"},
        {"name": "charger", "type": "string"},
        {"name": "started", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "energy_kwh", "type": "double"},
        {"name": "max_power_kw", "type": "float"},
        {"name": "phases", "type": "int"},
        {"name": "vehicle", "type": ["null", "string"], "default": None},
        {"name": "smart", "type": "boolean"},
    ],
}


def f_avro_zstd(p: Plant) -> bytes:
    rng = random.Random(int(p.now // 3600))
    rows = []
    for i in range(10):
        t = p.now - p.now % 3600 - (10 - i) * 5400
        rows.append({"session_id": f"cs-{int(t)}-{i}", "charger": f"evse-{i % 4 + 1:02d}",
                     "started": ms(t), "energy_kwh": round(rng.uniform(4, 48), 2),
                     "max_power_kw": float(rng.choice([7.4, 11.0, 22.0])), "phases": rng.choice([1, 3]),
                     "vehicle": rng.choice([None, "XDKCE5K38RH287145", "WVWZZZE1ZPP012345"]),
                     "smart": rng.random() < 0.6})
    return B.avro_container(SESSION_SCHEMA, rows, codec="zstandard", block=5)


def f_avro_registry(p: Plant) -> bytes:
    return B.avro_registry(42, EVENT_SCHEMA, _events(p, 1)[0])


def _rabin(canonical: bytes) -> bytes:
    """CRC-64-AVRO, the Avro spec's schema fingerprint, little-endian."""
    empty = 0xC15D213AA4D7A795
    table = []
    for i in range(256):
        fp = i
        for _ in range(8):
            fp = (fp >> 1) ^ (empty & -(fp & 1))
        table.append(fp)
    fp = empty
    for b in canonical:
        fp = (fp >> 8) ^ table[(fp ^ b) & 0xFF]
    return struct.pack("<Q", fp)


def _canonical(schema, namespace: str = "") -> object:
    """The schema's Parsing Canonical Form (Avro spec): full names, only the attributes
    that matter, in the spec's order, primitives as plain names."""
    if isinstance(schema, str):
        return schema if schema in ("null", "boolean", "int", "long", "float", "double",
                                    "bytes", "string") or "." in schema else f"{namespace}.{schema}"
    if isinstance(schema, list):
        return [_canonical(s, namespace) for s in schema]
    t = schema["type"]
    if t in ("record", "enum", "fixed"):
        ns = schema.get("namespace", namespace)
        out = {"name": schema["name"] if "." in schema["name"] else f"{ns}.{schema['name']}", "type": t}
        if t == "record":
            out["fields"] = [{"name": f["name"], "type": _canonical(f["type"], ns)} for f in schema["fields"]]
        elif t == "enum":
            out["symbols"] = schema["symbols"]
        return out
    if t == "array":
        return {"type": "array", "items": _canonical(schema["items"], namespace)}
    if t == "map":
        return {"type": "map", "values": _canonical(schema["values"], namespace)}
    return _canonical(t, namespace)  # a primitive with attributes, such as a logical type


def avro_fingerprint(schema) -> bytes:
    return _rabin(json.dumps(_canonical(schema), separators=(",", ":")).encode())


def f_avro_single(p: Plant) -> bytes:
    """Avro single-object encoding: C3 01, the schema's fingerprint, then the datum."""
    return b"\xc3\x01" + avro_fingerprint(EVENT_SCHEMA) + B.avro_datum(EVENT_SCHEMA, _events(p, 1)[0], {})


def f_arrow(p: Plant) -> bytes:
    """Vibration samples from the condition monitoring system: 2 s at 50 Hz on three
    sensors, the sensor dictionary-encoded, ZSTD-compressed buffers."""
    t0 = ms(p.now - p.now % 60)
    ts, sensor, accel, freq = [], [], [], []
    rng = random.Random(t0)
    for i in range(100):
        for s, f in (("main_bearing", 12.5), ("gearbox", 37.5), ("generator", 115.0)):
            ts.append(t0 + 20 * i)
            sensor.append(s)
            accel.append(round(0.3 * math.sin(2 * math.pi * f * i / 50) + rng.gauss(0, 0.03), 4))
            freq.append(f)
    return arrow.file([("ts", "timestamp_ms", ts), ("sensor", "dict_string", sensor),
                       ("accel_g", "float", accel), ("freq_hz", "double", freq)],
                      [("site", WF), ("turbine", "wtg03"), ("source", "CMS")], codec="zstd")


def f_arrow_stream(p: Plant) -> bytes:
    """The peaker's one-second samples for a minute, as an Arrow IPC stream."""
    t0 = ms(p.now - p.now % 60)
    rng = random.Random(t0 + 1)
    running = p.freq < 49.9
    rows = 60
    return arrow.stream([
        ("ts", "timestamp_ms", [t0 + 1000 * i for i in range(rows)]),
        ("unit", "string", ["gt1"] * rows),
        ("running", "bool", [running] * rows),
        ("power_mw", "double", [round(12.0 + rng.uniform(-0.3, 0.3), 3) if running else 0.0 for _ in range(rows)]),
        ("exhaust_temp_c", "double", [None if i % 20 == 19 else round(410 + rng.uniform(-4, 4), 1) for i in range(rows)]),
        ("starts", "int64", [1873] * rows),
    ], [("site", PK)])


SQL_SETUP = """
PRAGMA page_size = 1024;
CREATE TABLE turbines (id TEXT PRIMARY KEY, model TEXT NOT NULL, rated_kw REAL, hub_height_m REAL, commissioned TEXT);
CREATE TABLE setpoints (turbine TEXT REFERENCES turbines(id), name TEXT, value REAL, set_by TEXT, PRIMARY KEY (turbine, name));
CREATE TABLE readings (ts TEXT, turbine TEXT, power_kw REAL, wind_ms REAL, state TEXT);
CREATE INDEX idx_readings_turbine ON readings(turbine, ts);
CREATE VIEW derated AS SELECT turbine, value FROM setpoints WHERE name = 'max_power_kw' AND value < 3600;
CREATE TRIGGER setpoint_audit AFTER UPDATE ON setpoints BEGIN SELECT 1; END;
"""


def _setpoint(i: int, minute: int) -> float:
    return 3000.0 if (i + minute // 10) % 4 == 0 else 3600.0


def f_sqlite(p: Plant) -> bytes:
    """The site's configuration and its last readings, as a SQLite database file."""
    db = sqlite3.connect(":memory:")
    db.executescript(SQL_SETUP)
    minute = int(p.now // 60)
    for i, t in enumerate(TURBINES):
        db.execute("INSERT INTO turbines VALUES (?,?,?,?,?)",
                   (t, "V117-3.6", 3600.0, 91.5, f"2019-0{3 + i % 6}-1{i}"))
        db.execute("INSERT INTO setpoints VALUES (?,?,?,?)", (t, "max_power_kw", _setpoint(i, minute), "ppc"))
        db.execute("INSERT INTO setpoints VALUES (?,?,?,?)", (t, "cut_out_ms", 25.0, "oem"))
    for t in p.turbines:
        db.execute("INSERT INTO readings VALUES (?,?,?,?,?)",
                   (iso(p.now), t["id"], t["power_kw"], t["wind_ms"], t["state"]))
    db.commit()
    if hasattr(db, "serialize"):  # Python 3.11
        out = db.serialize()
        db.close()
        return out
    with tempfile.TemporaryDirectory() as d:
        disk = sqlite3.connect(str(Path(d) / "site.db"))
        db.backup(disk)
        disk.close()
        db.close()
        return (Path(d) / "site.db").read_bytes()


_TURBINES = B.Table("turbines", ["id", "model", "rated_kw", "hub_height_m", "commissioned"], [1, 0, 0, 0, 0])
_SETPOINTS = B.Table("setpoints", ["turbine", "name", "value", "set_by"], [1, 2, 0, 0])


def _setpoint_changes(p: Plant) -> list:
    """What the park controller changed in the last ten minutes."""
    minute = int(p.now // 60)
    changes = []
    for i, t in enumerate(TURBINES):
        before, after = _setpoint(i, minute - 10), _setpoint(i, minute)
        if before != after:
            changes.append((B.UPDATE, (t, "max_power_kw", before, "ppc"), (t, "max_power_kw", after, "ppc")))
    changes.append((B.UPDATE, ("wtg02", "cut_out_ms", 25.0, "oem"), ("wtg02", "cut_out_ms", 22.0, "ppc")))
    changes.append((B.DELETE, ("wtg05", "cut_out_ms", 25.0, "oem")))
    changes.append((B.INSERT, ("wtg07", "max_power_kw", 4200.0, "ppc")))
    return [(_TURBINES, [(B.INSERT, ("wtg07", "V136-4.2", 4200.0, 112.0, "2026-10-01"))]),
            (_SETPOINTS, changes)]


def f_sqlite_changeset(p: Plant) -> bytes:
    return B.changeset(_setpoint_changes(p))


def f_sqlite_patchset(p: Plant) -> bytes:
    return B.changeset(_setpoint_changes(p), patchset=True)


def f_msgpack(p: Plant) -> bytes:
    t = p.turbines[4]
    return B.msgpack({"turbine": t["id"], "site": WF, "state": t["state"], "since": iso(p.now - 3721),
                      "power_kw": t["power_kw"], "yaw": {"position_deg": 247.5, "error_deg": 1.8},
                      "alarms": [], "derated": False, "fw": 0x0412})


def f_cbor(p: Plant) -> bytes:
    rng = random.Random(int(p.now // 60) + 2)
    return B.cbor({"turbine": "wtg02", "ts": B.Tag(1, int(p.now)),
                   "rms_g": {"main_bearing": round(rng.uniform(0.15, 0.3), 3),
                             "gearbox": round(rng.uniform(0.25, 0.45), 3),
                             "generator": round(rng.uniform(0.08, 0.16), 3)},
                   "peaks_hz": [12.5, 37.5, 115.0], "ok": True, "serial": b"\x0b\x3f\x10\x2c"})


def _reading(p: Plant) -> B.Proto:
    """A telemetry reading. Its schema, which does not travel with it:

        message Reading {
          string turbine = 1;  uint64 ts_ms = 2;  double power_kw = 3;  float wind_ms = 4;
          uint32 rotor_rpm_x10 = 5;  State state = 6;  Location location = 7;
          repeated uint32 temps_c = 8 [packed = true];  repeated string tags = 9;
          sint32 yaw_error_cdeg = 10;
        }
        message Location { double lat = 1; double lon = 2; }
    """
    t = p.turbines[3]
    loc = B.Proto().double(1, 54.7341).double(2, 11.9632)
    return (B.Proto().string(1, t["id"]).uint(2, ms(p.now)).double(3, t["power_kw"])
            .float(4, t["wind_ms"]).uint(5, int(t["rotor_rpm"] * 10))
            .uint(6, {"RUNNING": 1, "IDLE": 2, "STOPPED": 3}[t["state"]]).message(7, loc)
            .packed(8, [61, 58, 74, int(t["nacelle_temp_c"])]).string(9, "offshore")
            .string(9, WF).sint(10, -180))


def f_protobuf(p: Plant) -> bytes:
    return _reading(p).encode()


def f_grpc(p: Plant) -> bytes:
    return B.grpc_frame(_reading(p).encode())


def f_text(p: Plant) -> bytes:
    t = p.turbines[2]
    return (f"{iso(p.now)} {WF} {t['id']} state={t['state']} power={t['power_kw']}kW "
            f"wind={t['wind_ms']}m/s rotor={t['rotor_rpm']}rpm\n").encode()


def f_binary(p: Plant) -> bytes:
    """A meter's Modbus holding registers 40001-40032, as read: big-endian 16-bit words."""
    rng = random.Random(int(p.now // 60) + 3)
    regs = [2300 + rng.randint(-20, 20) for _ in range(3)]          # voltages, 0.1 V
    regs += [rng.randint(0, 3200) for _ in range(3)]                # currents, 0.01 A
    regs += [int(p.freq * 100)]                                     # frequency, 0.01 Hz
    energy = 12_345_678 + int(p.now // 60) % 100_000
    regs += [energy >> 16, energy & 0xFFFF] + [0] * 23
    return struct.pack(f">{len(regs)}H", *regs)


def f_empty(p: Plant) -> bytes:
    return b""


class Feed(NamedTuple):
    topic: str
    content_type: str
    build: Callable[[Plant], bytes]
    text: bool
    zstd: bool = False  # needs compression.zstd (Python 3.14)


try:
    from compression import zstd as _have_zstd  # noqa: F401
    HAVE_ZSTD = True
except ImportError:
    HAVE_ZSTD = False


FEEDS = [
    Feed("json", "application/json", f_json, True),
    Feed("jsonl", "application/jsonl", f_jsonl, True),
    Feed("xml", "application/xml", f_xml, True),
    Feed("csv", "text/csv", f_csv, True),
    Feed("csv-semicolon", "text/csv; charset=utf-8", f_csv_semicolon, True),
    Feed("tsv", "text/tab-separated-values", f_tsv, True),
    Feed("psv", "text/plain", f_psv, True),
    Feed("text", "text/plain", f_text, True),
    Feed("parquet", "application/vnd.apache.parquet", f_parquet, False),
    Feed("parquet-zstd", "application/vnd.apache.parquet", f_parquet_zstd, False, zstd=True),
    Feed("avro", "application/avro", f_avro, False),
    Feed("avro-zstd", "application/avro", f_avro_zstd, False, zstd=True),
    Feed("avro-registry", "application/vnd.confluent.avro", f_avro_registry, False),
    Feed("avro-single-object", "application/avro", f_avro_single, False),
    Feed("arrow", "application/vnd.apache.arrow.file", f_arrow, False, zstd=True),
    Feed("arrow-stream", "application/vnd.apache.arrow.stream", f_arrow_stream, False),
    Feed("sqlite", "application/vnd.sqlite3", f_sqlite, False),
    Feed("sqlite-changeset", "application/x-sqlite-changeset", f_sqlite_changeset, False),
    Feed("sqlite-patchset", "application/x-sqlite-patchset", f_sqlite_patchset, False),
    Feed("msgpack", "application/msgpack", f_msgpack, False),
    Feed("cbor", "application/cbor", f_cbor, False),
    Feed("protobuf", "application/x-protobuf", f_protobuf, False),
    Feed("grpc", "application/grpc", f_grpc, False),
    Feed("gzip", "application/gzip", f_gzip, False),
    Feed("zstd", "application/zstd", f_zstd, False, zstd=True),
    Feed("binary", "application/octet-stream", f_binary, False),
    Feed("empty", "application/octet-stream", f_empty, False),
]


def feeds() -> list:
    return [f for f in FEEDS if HAVE_ZSTD or not f.zstd]


def build_all(now: float) -> list[tuple[Feed, bytes]]:
    p = Plant(now)
    return [(f, f.build(p)) for f in feeds()]


def publish_all(client: Client, now: float) -> int:
    n = 0
    for feed, payload in build_all(now):
        client.publish(f"{ROOT}/{feed.topic}", payload, qos=1, retain=bool(payload), props={
            "payload_format": 1 if feed.text else 0, "content_type": feed.content_type,
            "user": [("format", feed.topic), ("source", "demo/rules-live feeds/formats.py")]})
        n += 1
    return n


def parse_args(argv=None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--host", default=os.environ.get("SIM_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("SIM_PORT", "1883")))
    ap.add_argument("--interval", type=float, default=float(os.environ.get("FORMATS_INTERVAL", "60")),
                    metavar="SECONDS", help="seconds between rounds (60)")
    ap.add_argument("--once", action="store_true", help="publish every format once and exit")
    ap.add_argument("--list", action="store_true",
                    help="print each topic, content type, size and SHA-256 prefix; no broker")
    args = ap.parse_args(argv)
    if args.interval <= 0:
        ap.error("--interval must be positive")
    return args


def main(argv=None) -> int:
    args = parse_args(argv)
    if not HAVE_ZSTD:
        print(f"formats: no compression.zstd (Python 3.14) here, so skipping "
              f"{', '.join(f.topic for f in FEEDS if f.zstd)}", file=sys.stderr, flush=True)
    if args.list:
        for feed, payload in build_all(time.time()):
            print(f"{ROOT}/{feed.topic:22} {feed.content_type:36} {len(payload):>7} B  "
                  f"{hashlib.sha256(payload).hexdigest()[:12]}")
        return 0

    def say(text: str):
        print(f"formats: {text}", flush=True)

    stop: list[int] = []

    def on_signal(signum, _frame):
        stop.append(signum)
        signal.signal(signum, signal.SIG_DFL)

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    say(f"{len(feeds())} formats under {ROOT}/ to {args.host}:{args.port}"
        + ("" if args.once else f", every {args.interval:g} s"))
    client = None
    backoff = 1.0
    while not stop:
        started = time.monotonic()
        try:
            if client is None:
                client = Client(args.host, args.port, "formats-feed", keepalive=60, timeout=10, version=5)
                client.connect()
                backoff = 1.0
            n = publish_all(client, time.time())
            if args.once:
                say(f"published {n} formats")
                client.disconnect()
                return 0
        except (MqttError, OSError) as e:
            if args.once:
                say(f"failed: {e}")
                return 1
            say(f"{e}; retrying in {backoff:g} s")
            if client:
                client.drop()
            client = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
            continue
        while not stop and time.monotonic() < started + args.interval:
            time.sleep(0.5)
            try:
                client.drain()
                client.ping_if_idle()
            except (MqttError, OSError):
                client.drop()
                client = None
                break
    if client:
        client.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main())
