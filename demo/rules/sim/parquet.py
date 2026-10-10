"""A small Apache Parquet writer: enough for a turbine's fast log, standard library only.

It writes one row group of required (non-null) columns, each column one data page (v1),
PLAIN encoded and UNCOMPRESSED, so a file's size follows from its rows and columns alone:
8 bytes per INT64 or DOUBLE value, plus about 60 bytes of page header and metadata per
column and the footer. Page headers and the footer (FileMetaData) are Thrift Compact
Protocol, as the format specifies
(https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift).
Each column chunk carries its min, max and null count (0), with the type-defined column
order, so a reader may prune on them.

Two column kinds are enough here:

    ("ts", "timestamp_ms", [int, ...])   INT64, logical type TIMESTAMP(MILLIS, UTC)
                                          (converted type TIMESTAMP_MILLIS)
    ("name", "double", [float, ...])     DOUBLE

The same columns and metadata give the same bytes on every run and every platform.
"""

from __future__ import annotations

import math
import struct
from typing import Iterable, List, Sequence, Tuple

MAGIC = b"PAR1"

# parquet.thrift enums
_TYPE_INT64, _TYPE_DOUBLE = 2, 5
_REQUIRED = 0
_CONVERTED_TIMESTAMP_MILLIS = 9
_PLAIN, _RLE = 0, 3
_UNCOMPRESSED = 0
_DATA_PAGE = 0

# Thrift Compact Protocol field types
_T_TRUE, _T_FALSE, _T_I16, _T_I32, _T_I64, _T_BINARY, _T_LIST, _T_STRUCT = 1, 2, 4, 5, 6, 8, 9, 12

KINDS = {"timestamp_ms": (_TYPE_INT64, "q"), "double": (_TYPE_DOUBLE, "d")}


# ---------------------------------------------------------------------- Thrift Compact


def _varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b, n = n & 0x7F, n >> 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _zigzag(n: int) -> bytes:
    return _varint((n << 1) ^ (n >> 63))


# A struct is a list of (field id, type, value). Values by type: _T_I16/_I32/_I64 an int,
# _T_BINARY bytes or str, _T_TRUE a bool, _T_STRUCT a struct, _T_LIST (element type,
# [values]).
Field = Tuple[int, int, object]


def _value(ftype: int, value) -> bytes:
    if ftype in (_T_I16, _T_I32, _T_I64):
        return _zigzag(value)
    if ftype == _T_BINARY:
        raw = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        return _varint(len(raw)) + raw
    if ftype == _T_STRUCT:
        return _struct(value)
    if ftype == _T_LIST:
        etype, items = value
        n = len(items)
        head = bytes([(n << 4) | etype]) if n < 15 else bytes([0xF0 | etype]) + _varint(n)
        return head + b"".join(_value(etype, v) for v in items)
    raise ValueError(f"unsupported Thrift type {ftype}")


def _struct(fields: Sequence[Field]) -> bytes:
    out = bytearray()
    last = 0
    for fid, ftype, value in sorted(fields, key=lambda f: f[0]):
        wire = ftype
        if ftype == _T_TRUE:  # a bool field carries its value in the type nibble
            wire = _T_TRUE if value else _T_FALSE
        delta = fid - last
        if 0 < delta <= 15:
            out.append((delta << 4) | wire)
        else:
            out.append(wire)
            out += _zigzag(fid)
        last = fid
        if ftype != _T_TRUE:
            out += _value(ftype, value)
    out.append(0)  # stop
    return bytes(out)


# ---------------------------------------------------------------------- Parquet


def _stat_bytes(fmt: str, v) -> bytes:
    return struct.pack("<" + fmt, v)


def write(columns: Sequence[Tuple[str, str, Sequence]], key_values: Iterable[Tuple[str, str]] = (),
          created_by: str = "mqttd demo/rules sim/parquet.py") -> bytes:
    """A Parquet file of `columns` (name, kind, values), all the same length, with
    `key_values` as the file's key-value metadata."""
    if not columns:
        raise ValueError("a Parquet file needs at least one column")
    rows = len(columns[0][2])
    out = bytearray(MAGIC)
    chunks: List[list] = []
    schema: List[list] = [[(4, _T_BINARY, "schema"), (5, _T_I32, len(columns))]]
    total = 0
    for name, kind, values in columns:
        if len(values) != rows:
            raise ValueError(f"column {name!r} has {len(values)} values, not {rows}")
        ptype, fmt = KINDS[kind]
        if kind == "double":
            if any(math.isnan(v) for v in values):
                raise ValueError(f"column {name!r}: NaN is not written")
            values = [float(v) for v in values]
        data = struct.pack(f"<{rows}{fmt}", *values)
        lo, hi = min(values), max(values)
        if kind == "double":  # the spec's rule for zeros in DOUBLE statistics
            lo = -0.0 if lo == 0 else lo
            hi = 0.0 if hi == 0 else hi
        stats = [(3, _T_I64, 0), (5, _T_BINARY, _stat_bytes(fmt, hi)),
                 (6, _T_BINARY, _stat_bytes(fmt, lo))]
        header = _struct([
            (1, _T_I32, _DATA_PAGE),
            (2, _T_I32, len(data)),
            (3, _T_I32, len(data)),
            (5, _T_STRUCT, [
                (1, _T_I32, rows),
                (2, _T_I32, _PLAIN),
                (3, _T_I32, _RLE),
                (4, _T_I32, _RLE),
            ]),
        ])
        offset = len(out)
        out += header + data
        size = len(header) + len(data)
        total += size
        chunks.append([
            (2, _T_I64, offset),
            (3, _T_STRUCT, [
                (1, _T_I32, ptype),
                (2, _T_LIST, (_T_I32, [_PLAIN])),
                (3, _T_LIST, (_T_BINARY, [name])),
                (4, _T_I32, _UNCOMPRESSED),
                (5, _T_I64, rows),
                (6, _T_I64, size),
                (7, _T_I64, size),
                (9, _T_I64, offset),
                (12, _T_STRUCT, stats),
            ]),
        ])
        element = [(1, _T_I32, ptype), (3, _T_I32, _REQUIRED), (4, _T_BINARY, name)]
        if kind == "timestamp_ms":
            element += [
                (6, _T_I32, _CONVERTED_TIMESTAMP_MILLIS),
                # LogicalType.TIMESTAMP (8): isAdjustedToUTC, unit MILLIS (1)
                (10, _T_STRUCT, [(8, _T_STRUCT, [(1, _T_TRUE, True),
                                                 (2, _T_STRUCT, [(1, _T_STRUCT, [])])])]),
            ]
        schema.append(element)
    footer = _struct([
        (1, _T_I32, 2),
        (2, _T_LIST, (_T_STRUCT, schema)),
        (3, _T_I64, rows),
        (4, _T_LIST, (_T_STRUCT, [[
            (1, _T_LIST, (_T_STRUCT, chunks)),
            (2, _T_I64, total),
            (3, _T_I64, rows),
            (5, _T_I64, len(MAGIC)),
            (6, _T_I64, total),
            (7, _T_I16, 0),
        ]])),
        (5, _T_LIST, (_T_STRUCT, [[(1, _T_BINARY, k), (2, _T_BINARY, v)] for k, v in key_values])),
        (6, _T_BINARY, created_by),
        # ColumnOrder.TYPE_ORDER for every column: min and max are by the type's order
        (7, _T_LIST, (_T_STRUCT, [[(1, _T_STRUCT, [])] for _ in columns])),
    ])
    out += footer + struct.pack("<I", len(footer)) + MAGIC
    return bytes(out)
