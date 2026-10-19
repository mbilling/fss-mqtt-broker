"""Binary formats written by hand, standard library only: MessagePack, CBOR, Protocol
Buffers (and its gRPC frame), Apache Avro (container files and the schema-registry
framing), and SQLite changesets and patchsets.

Each writer covers what the demo's feeds send, from the format's own specification:

    msgpack(value)        https://github.com/msgpack/msgpack/blob/master/spec.md
    cbor(value)           RFC 8949; a Tag(n, value) is written as tag n
    Proto                 https://protobuf.dev/programming-guides/encoding/
    avro_container(...)   https://avro.apache.org/docs/1.12.0/specification/
    changeset(...)        https://www.sqlite.org/sessionintro.html, sqlite3session.c's
                          "changeset format"

Values are Python's: None, bool, int, float, str, bytes, list and dict. The same input
gives the same bytes on every run, apart from what the caller puts in.
"""

from __future__ import annotations

import json
import struct
import zlib
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence


# ---------------------------------------------------------------------------- MessagePack


def msgpack(v: Any) -> bytes:
    if v is None:
        return b"\xc0"
    if v is True:
        return b"\xc3"
    if v is False:
        return b"\xc2"
    if isinstance(v, int):
        if 0 <= v < 128:
            return bytes([v])
        if -32 <= v < 0:
            return struct.pack("b", v)
        if v >= 0:
            for tag, fmt, top in ((0xCC, "B", 1 << 8), (0xCD, "H", 1 << 16),
                                  (0xCE, "I", 1 << 32), (0xCF, "Q", 1 << 64)):
                if v < top:
                    return bytes([tag]) + struct.pack(">" + fmt, v)
        for tag, fmt, bits in ((0xD0, "b", 7), (0xD1, "h", 15), (0xD2, "i", 31), (0xD3, "q", 63)):
            if -(1 << bits) <= v:
                return bytes([tag]) + struct.pack(">" + fmt, v)
        raise ValueError(f"MessagePack: {v} is out of range")
    if isinstance(v, float):
        return b"\xcb" + struct.pack(">d", v)
    if isinstance(v, str):
        raw = v.encode("utf-8")
        n = len(raw)
        if n < 32:
            return bytes([0xA0 | n]) + raw
        if n < 1 << 8:
            return b"\xd9" + struct.pack(">B", n) + raw
        if n < 1 << 16:
            return b"\xda" + struct.pack(">H", n) + raw
        return b"\xdb" + struct.pack(">I", n) + raw
    if isinstance(v, (bytes, bytearray)):
        n = len(v)
        if n < 1 << 8:
            return b"\xc4" + struct.pack(">B", n) + bytes(v)
        if n < 1 << 16:
            return b"\xc5" + struct.pack(">H", n) + bytes(v)
        return b"\xc6" + struct.pack(">I", n) + bytes(v)
    if isinstance(v, (list, tuple)):
        n = len(v)
        head = (bytes([0x90 | n]) if n < 16 else
                b"\xdc" + struct.pack(">H", n) if n < 1 << 16 else b"\xdd" + struct.pack(">I", n))
        return head + b"".join(msgpack(x) for x in v)
    if isinstance(v, dict):
        n = len(v)
        head = (bytes([0x80 | n]) if n < 16 else
                b"\xde" + struct.pack(">H", n) if n < 1 << 16 else b"\xdf" + struct.pack(">I", n))
        return head + b"".join(msgpack(k) + msgpack(x) for k, x in v.items())
    raise TypeError(f"MessagePack: cannot write {type(v).__name__}")


# ---------------------------------------------------------------------------- CBOR


@dataclass
class Tag:
    """A CBOR tagged value: Tag(1, 1760000000) is an epoch time, Tag(0, "2026-...Z") a
    date-time string (RFC 8949 §3.4)."""

    number: int
    value: Any


def _cbor_head(major: int, n: int) -> bytes:
    if n < 24:
        return bytes([major << 5 | n])
    for ai, fmt, top in ((24, "B", 1 << 8), (25, "H", 1 << 16), (26, "I", 1 << 32), (27, "Q", 1 << 64)):
        if n < top:
            return bytes([major << 5 | ai]) + struct.pack(">" + fmt, n)
    raise ValueError(f"CBOR: {n} is out of range")


def cbor(v: Any) -> bytes:
    if v is False:
        return b"\xf4"
    if v is True:
        return b"\xf5"
    if v is None:
        return b"\xf6"
    if isinstance(v, int):
        return _cbor_head(0, v) if v >= 0 else _cbor_head(1, -1 - v)
    if isinstance(v, float):
        return b"\xfb" + struct.pack(">d", v)
    if isinstance(v, (bytes, bytearray)):
        return _cbor_head(2, len(v)) + bytes(v)
    if isinstance(v, str):
        raw = v.encode("utf-8")
        return _cbor_head(3, len(raw)) + raw
    if isinstance(v, (list, tuple)):
        return _cbor_head(4, len(v)) + b"".join(cbor(x) for x in v)
    if isinstance(v, dict):
        return _cbor_head(5, len(v)) + b"".join(cbor(k) + cbor(x) for k, x in v.items())
    if isinstance(v, Tag):
        return _cbor_head(6, v.number) + cbor(v.value)
    raise TypeError(f"CBOR: cannot write {type(v).__name__}")


# ---------------------------------------------------------------------------- Protobuf


def varint(n: int) -> bytes:
    if n < 0:
        n += 1 << 64  # negative int32/int64: ten bytes, two's complement
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


class Proto:
    """A protobuf message, field by field, in the order written:

        Proto().string(1, "T04").uint(2, ms).double(3, 2265.8).message(7, Proto()...)
    """

    def __init__(self):
        self.buf = bytearray()

    def _key(self, field: int, wire: int):
        self.buf += varint(field << 3 | wire)

    def uint(self, field: int, n: int) -> "Proto":   # int32, int64, uint32, uint64, enum
        self._key(field, 0)
        self.buf += varint(n)
        return self

    def sint(self, field: int, n: int) -> "Proto":   # sint32, sint64: zigzag
        self._key(field, 0)
        self.buf += varint((n << 1) ^ (n >> 63))
        return self

    def bool(self, field: int, b: bool) -> "Proto":
        return self.uint(field, 1 if b else 0)

    def double(self, field: int, x: float) -> "Proto":
        self._key(field, 1)
        self.buf += struct.pack("<d", x)
        return self

    def float(self, field: int, x: float) -> "Proto":
        self._key(field, 5)
        self.buf += struct.pack("<f", x)
        return self

    def fixed64(self, field: int, n: int) -> "Proto":
        self._key(field, 1)
        self.buf += struct.pack("<Q", n)
        return self

    def bytes(self, field: int, b: bytes) -> "Proto":
        self._key(field, 2)
        self.buf += varint(len(b)) + b
        return self

    def string(self, field: int, s: str) -> "Proto":
        return self.bytes(field, s.encode("utf-8"))

    def message(self, field: int, m: "Proto") -> "Proto":
        return self.bytes(field, bytes(m.buf))

    def packed(self, field: int, numbers: Iterable[int]) -> "Proto":
        return self.bytes(field, b"".join(varint(n) for n in numbers))

    def encode(self) -> bytes:
        return bytes(self.buf)


def grpc_frame(message: bytes) -> bytes:
    """A gRPC length-prefixed message: not compressed, then the length, big-endian."""
    return b"\x00" + struct.pack(">I", len(message)) + message


# ---------------------------------------------------------------------------- Avro


def _zigzag_long(n: int) -> bytes:
    return varint((n << 1) ^ (n >> 63))


def avro_datum(schema: Any, v: Any, named: Optional[dict] = None) -> bytes:
    """One value in Avro's binary encoding, by its schema (a parsed schema: dict, list or
    name)."""
    named = {} if named is None else named
    if isinstance(schema, str):
        if schema in named:
            return avro_datum(named[schema], v, named)
        t = schema
        schema = {"type": t}
    if isinstance(schema, list):  # a union: the branch, then the value
        for i, branch in enumerate(schema):
            if _avro_fits(branch, v, named):
                return _zigzag_long(i) + avro_datum(branch, v, named)
        raise ValueError(f"Avro: {v!r} fits no branch of {schema}")
    t = schema["type"]
    if isinstance(t, (dict, list)):
        return avro_datum(t, v, named)
    if "name" in schema:
        named[schema["name"]] = schema
    if t == "null":
        return b""
    if t == "boolean":
        return b"\x01" if v else b"\x00"
    if t in ("int", "long"):
        return _zigzag_long(int(v))
    if t == "float":
        return struct.pack("<f", v)
    if t == "double":
        return struct.pack("<d", v)
    if t in ("bytes", "string"):
        raw = v.encode("utf-8") if isinstance(v, str) else bytes(v)
        return _zigzag_long(len(raw)) + raw
    if t == "enum":
        return _zigzag_long(schema["symbols"].index(v))
    if t == "record":
        return b"".join(avro_datum(f["type"], v[f["name"]], named) for f in schema["fields"])
    if t == "array":
        if not v:
            return b"\x00"
        return (_zigzag_long(len(v)) + b"".join(avro_datum(schema["items"], x, named) for x in v)
                + b"\x00")
    if t == "map":
        if not v:
            return b"\x00"
        return (_zigzag_long(len(v)) + b"".join(avro_datum("string", k, named)
                                                 + avro_datum(schema["values"], x, named)
                                                 for k, x in v.items()) + b"\x00")
    raise ValueError(f"Avro: type {t!r} is not written here")


def _avro_fits(branch: Any, v: Any, named: dict) -> bool:
    t = branch if isinstance(branch, str) else branch.get("type")
    if isinstance(t, str) and t in named:
        t = named[t]["type"]
    return {"null": v is None, "boolean": isinstance(v, bool),
            "int": isinstance(v, int) and not isinstance(v, bool),
            "long": isinstance(v, int) and not isinstance(v, bool),
            "float": isinstance(v, float), "double": isinstance(v, float),
            "string": isinstance(v, str), "bytes": isinstance(v, bytes),
            "record": isinstance(v, dict), "map": isinstance(v, dict),
            "array": isinstance(v, list), "enum": isinstance(v, str)}.get(t, False)


def _zstd(data: bytes) -> bytes:
    from compression import zstd  # Python 3.14
    return zstd.compress(data)


AVRO_CODECS = {
    "null": lambda b: b,
    "deflate": lambda b: zlib.compress(b, 9)[2:-4],  # raw deflate: no zlib header or checksum
    "zstandard": _zstd,
}


def avro_container(schema: dict, records: Sequence[dict], codec: str = "deflate",
                   sync: bytes = b"wf-falster-sync!", block: int = 1000,
                   metadata: Iterable[tuple[str, bytes]] = ()) -> bytes:
    """An Avro object container file: the header (magic, metadata with the schema and the
    codec, a 16-byte sync marker), then blocks of `block` records."""
    if len(sync) != 16:
        raise ValueError("the sync marker is 16 bytes")
    meta = [("avro.schema", json.dumps(schema, separators=(",", ":")).encode()),
            ("avro.codec", codec.encode())] + list(metadata)
    out = bytearray(b"Obj\x01")
    out += _zigzag_long(len(meta))
    for k, v in meta:
        out += avro_datum("string", k) + avro_datum("bytes", v)
    out += b"\x00" + sync
    compress = AVRO_CODECS[codec]
    for at in range(0, len(records), block):
        part = records[at:at + block]
        data = compress(b"".join(avro_datum(schema, r, {}) for r in part))
        out += _zigzag_long(len(part)) + _zigzag_long(len(data)) + data + sync
    return bytes(out)


def avro_registry(schema_id: int, schema: dict, record: dict) -> bytes:
    """One record framed for a schema registry (Confluent's wire format): magic byte 0, the
    schema id big-endian, then the datum. The schema is not in the payload."""
    return b"\x00" + struct.pack(">I", schema_id) + avro_datum(schema, record, {})


# ---------------------------------------------------------------------------- SQLite changesets


def _sqlite_varint(n: int) -> bytes:
    """SQLite's big-endian varint (sqlite3PutVarint): 7 bits a byte, the ninth byte whole."""
    if n < 0x80:
        return bytes([n])
    if n >> 56:
        out = [n & 0xFF]
        n >>= 8
        for _ in range(8):
            out.append((n & 0x7F) | 0x80)
            n >>= 7
        return bytes(reversed(out))
    out = []
    while n:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out[0] &= 0x7F
    return bytes(reversed(out))


UNDEFINED = object()  # a column an UPDATE does not change


def _sqlite_value(v: Any) -> bytes:
    if v is UNDEFINED:
        return b"\x00"
    if v is None:
        return b"\x05"
    if isinstance(v, bool):
        v = int(v)
    if isinstance(v, int):
        return b"\x01" + struct.pack(">q", v)
    if isinstance(v, float):
        return b"\x02" + struct.pack(">d", v)
    if isinstance(v, str):
        raw = v.encode("utf-8")
        return b"\x03" + _sqlite_varint(len(raw)) + raw
    if isinstance(v, (bytes, bytearray)):
        return b"\x04" + _sqlite_varint(len(v)) + bytes(v)
    raise TypeError(f"SQLite changeset: cannot write {type(v).__name__}")


INSERT, UPDATE, DELETE = 18, 23, 9  # SQLITE_INSERT, SQLITE_UPDATE, SQLITE_DELETE


@dataclass
class Table:
    """A table as the session extension sees it: its columns and, per column, its place in
    the primary key (1, 2, ... or 0)."""

    name: str
    columns: Sequence[str]
    pk: Sequence[int]


def changeset(tables: Sequence[tuple[Table, Sequence[tuple]]], patchset: bool = False) -> bytes:
    """A changeset (or patchset) of each table's changes, in order:

        (INSERT, new_row)                      every column
        (DELETE, old_row)                      every column
        (UPDATE, old_row, new_row)             the whole rows; the unchanged columns are
                                               written as undefined, as the extension does
    """
    out = bytearray()
    for table, changes in tables:
        if not changes:
            continue
        out += b"P" if patchset else b"T"
        out += _sqlite_varint(len(table.columns)) + bytes(table.pk)
        out += table.name.encode("utf-8") + b"\x00"
        for change in changes:
            op = change[0]
            out += bytes([op, 0])  # the operation, and "not indirect"
            if op == INSERT:
                out += b"".join(_sqlite_value(v) for v in change[1])
            elif op == DELETE:
                old = change[1]
                if patchset:  # a patchset DELETE names the row by its key only
                    out += b"".join(_sqlite_value(v) for v, k in zip(old, table.pk) if k)
                else:
                    out += b"".join(_sqlite_value(v) for v in old)
            elif op == UPDATE:
                old, new = change[1], change[2]
                changed = [o != n for o, n in zip(old, new)]
                if not patchset:
                    out += b"".join(_sqlite_value(o if (k or c) else UNDEFINED)
                                    for o, k, c in zip(old, table.pk, changed))
                    out += b"".join(_sqlite_value(n if c else UNDEFINED)
                                    for n, c in zip(new, changed))
                else:  # a patchset UPDATE: the key, then the new values of what changed
                    out += b"".join(_sqlite_value(n if (k or c) else UNDEFINED)
                                    for n, k, c in zip(new, table.pk, changed))
            else:
                raise ValueError(f"unknown change {op}")
    return bytes(out)
