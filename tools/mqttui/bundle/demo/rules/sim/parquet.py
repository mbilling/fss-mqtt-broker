"""A small Apache Parquet writer: enough for a turbine's fast log, standard library only.

By default it writes one row group of required (non-null) columns, each column one data
page (v1), PLAIN encoded and UNCOMPRESSED, so a file's size follows from its rows and
columns alone: 8 bytes per INT64 or DOUBLE value, plus about 60 bytes of page header and
metadata per column and the footer. Page headers and the footer (FileMetaData) are Thrift
Compact Protocol, as the format specifies
(https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift).
Each column chunk carries its min, max and null count, with the type-defined column
order, so a reader may prune on them.

The column kinds:

    ("ts", "timestamp_ms", [int, ...])   INT64, logical type TIMESTAMP(MILLIS, UTC)
                                          (converted type TIMESTAMP_MILLIS)
    ("name", "double", [float, ...])     DOUBLE
    ("n", "int32" or "int64", [int, ...])
    ("s", "string", [str, ...])          BYTE_ARRAY, logical type STRING (converted UTF8)
    ("b", "bool", [bool, ...])           BOOLEAN

A kind ending in "?" ("double?") makes the column OPTIONAL: its values may be None, and
each page then starts with its definition levels (RLE, bit width 1). `codec` compresses
every page: "uncompressed", "gzip", "zstd" (Python 3.14's compression.zstd), or "snappy"
and "lz4_raw", which are written as valid streams of literals only, so they are readable
but no smaller. `row_group_size` splits the rows into row groups of that many.

The same columns and metadata give the same bytes on every run and every platform.
"""

from __future__ import annotations

import gzip
import math
import struct
from typing import Iterable, List, Optional, Sequence, Tuple

MAGIC = b"PAR1"

# parquet.thrift enums
_TYPE_BOOLEAN, _TYPE_INT32, _TYPE_INT64, _TYPE_DOUBLE, _TYPE_BYTE_ARRAY = 0, 1, 2, 5, 6
_REQUIRED, _OPTIONAL = 0, 1
_CONVERTED_UTF8, _CONVERTED_TIMESTAMP_MILLIS = 0, 9
_PLAIN, _RLE = 0, 3
_UNCOMPRESSED = 0
_DATA_PAGE = 0

# Thrift Compact Protocol field types
_T_TRUE, _T_FALSE, _T_I16, _T_I32, _T_I64, _T_BINARY, _T_LIST, _T_STRUCT = 1, 2, 4, 5, 6, 8, 9, 12

KINDS = {"timestamp_ms": (_TYPE_INT64, "q"), "double": (_TYPE_DOUBLE, "d"),
         "int32": (_TYPE_INT32, "i"), "int64": (_TYPE_INT64, "q"),
         "string": (_TYPE_BYTE_ARRAY, ""), "bool": (_TYPE_BOOLEAN, "")}


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


# ---------------------------------------------------------------------- codecs


def _snappy_literals(data: bytes) -> bytes:
    """A Snappy block of literals only: the length, then the bytes in literals of at most
    64 KiB each (format_description.txt, "Literals")."""
    out = bytearray(_varint(len(data)))
    for at in range(0, len(data), 65536):
        chunk = data[at:at + 65536]
        n = len(chunk) - 1
        if n < 60:
            out.append(n << 2)
        elif n < 256:
            out += bytes([60 << 2, n])
        else:
            out += bytes([61 << 2]) + struct.pack("<H", n)
        out += chunk
    return bytes(out)


def _lz4_literals(data: bytes) -> bytes:
    """An LZ4 block of one sequence that is all literals and no match, which the block
    format allows for the last sequence."""
    n = len(data)
    if n < 15:
        return bytes([n << 4]) + data
    out = bytearray([0xF0])
    n -= 15
    while n >= 255:
        out.append(255)
        n -= 255
    out.append(n)
    return bytes(out) + data


def _zstd(data: bytes) -> bytes:
    from compression import zstd  # Python 3.14
    return zstd.compress(data)


# name: (CompressionCodec, compress)
CODECS = {
    "uncompressed": (_UNCOMPRESSED, lambda b: b),
    "snappy": (1, _snappy_literals),
    "gzip": (2, lambda b: gzip.compress(b, mtime=0)),
    "zstd": (6, _zstd),
    "lz4_raw": (7, _lz4_literals),
}


# ---------------------------------------------------------------------- Parquet


def _stat_bytes(fmt: str, v) -> bytes:
    return struct.pack("<" + fmt, v)


def _rle_levels(levels: Sequence[int]) -> bytes:
    """Definition levels of bit width 1 as RLE runs, with the 4-byte length the v1 data
    page puts before them."""
    out = bytearray()
    i = 0
    while i < len(levels):
        j = i
        while j < len(levels) and levels[j] == levels[i]:
            j += 1
        out += _varint((j - i) << 1) + bytes([levels[i]])
        i = j
    return struct.pack("<I", len(out)) + bytes(out)


def _plain(kind: str, fmt: str, values: Sequence) -> bytes:
    if kind == "string":
        return b"".join(struct.pack("<I", len(v)) + v
                        for v in (x.encode("utf-8") for x in values))
    if kind == "bool":
        out = bytearray((len(values) + 7) // 8)
        for i, v in enumerate(values):
            if v:
                out[i // 8] |= 1 << (i % 8)
        return bytes(out)
    return struct.pack(f"<{len(values)}{fmt}", *values)


def _stats(kind: str, fmt: str, values: Sequence, nulls: int) -> list:
    stats: list = [(3, _T_I64, nulls)]
    if not values:
        return stats
    if kind == "string":
        raw = [v.encode("utf-8") for v in values]
        hi, lo = max(raw), min(raw)
    elif kind == "bool":
        hi, lo = bytes([max(values)]), bytes([min(values)])
    else:
        lo, hi = min(values), max(values)
        if kind == "double":  # the spec's rule for zeros in DOUBLE statistics
            lo = -0.0 if lo == 0 else lo
            hi = 0.0 if hi == 0 else hi
        hi, lo = _stat_bytes(fmt, hi), _stat_bytes(fmt, lo)
    return stats + [(5, _T_BINARY, hi), (6, _T_BINARY, lo)]


def write(columns: Sequence[Tuple[str, str, Sequence]], key_values: Iterable[Tuple[str, str]] = (),
          created_by: str = "mqttd demo/rules sim/parquet.py", codec: str = "uncompressed",
          row_group_size: Optional[int] = None) -> bytes:
    """A Parquet file of `columns` (name, kind, values), all the same length, with
    `key_values` as the file's key-value metadata."""
    if not columns:
        raise ValueError("a Parquet file needs at least one column")
    codec_id, compress = CODECS[codec]
    rows = len(columns[0][2])
    cols = []
    for name, kind, values in columns:
        optional = kind.endswith("?")
        kind = kind.rstrip("?")
        if len(values) != rows:
            raise ValueError(f"column {name!r} has {len(values)} values, not {rows}")
        if not optional and any(v is None for v in values):
            raise ValueError(f"column {name!r} is required, and has a null")
        if kind == "double":
            if any(v is not None and math.isnan(v) for v in values):
                raise ValueError(f"column {name!r}: NaN is not written")
            values = [None if v is None else float(v) for v in values]
        cols.append((name, kind, optional, values))
    step = row_group_size or max(rows, 1)
    out = bytearray(MAGIC)
    groups: List[list] = []
    for first in range(0, max(rows, 1), step):
        chunks: List[list] = []
        total = compressed_total = 0
        group_offset = len(out)
        for name, kind, optional, column in cols:
            ptype, fmt = KINDS[kind]
            part = column[first:first + step]
            present = [v for v in part if v is not None]
            data = _plain(kind, fmt, present)
            if optional:
                data = _rle_levels([0 if v is None else 1 for v in part]) + data
            packed = compress(data)
            header = _struct([
                (1, _T_I32, _DATA_PAGE),
                (2, _T_I32, len(data)),
                (3, _T_I32, len(packed)),
                (5, _T_STRUCT, [
                    (1, _T_I32, len(part)),
                    (2, _T_I32, _PLAIN),
                    (3, _T_I32, _RLE),
                    (4, _T_I32, _RLE),
                ]),
            ])
            offset = len(out)
            out += header + packed
            size = len(header) + len(data)
            stored = len(header) + len(packed)
            total += size
            compressed_total += stored
            chunks.append([
                (2, _T_I64, offset),
                (3, _T_STRUCT, [
                    (1, _T_I32, ptype),
                    (2, _T_LIST, (_T_I32, [_PLAIN, _RLE] if optional else [_PLAIN])),
                    (3, _T_LIST, (_T_BINARY, [name])),
                    (4, _T_I32, codec_id),
                    (5, _T_I64, len(part)),
                    (6, _T_I64, size),
                    (7, _T_I64, stored),
                    (9, _T_I64, offset),
                    (12, _T_STRUCT, _stats(kind, fmt, present, len(part) - len(present))),
                ]),
            ])
        groups.append([
            (1, _T_LIST, (_T_STRUCT, chunks)),
            (2, _T_I64, total),
            (3, _T_I64, min(step, rows - first)),
            (5, _T_I64, group_offset),
            (6, _T_I64, compressed_total),
            (7, _T_I16, len(groups)),
        ])
    schema: List[list] = [[(4, _T_BINARY, "schema"), (5, _T_I32, len(cols))]]
    for name, kind, optional, _ in cols:
        ptype, _ = KINDS[kind]
        element = [(1, _T_I32, ptype), (3, _T_I32, _OPTIONAL if optional else _REQUIRED),
                   (4, _T_BINARY, name)]
        if kind == "timestamp_ms":
            element += [
                (6, _T_I32, _CONVERTED_TIMESTAMP_MILLIS),
                # LogicalType.TIMESTAMP (8): isAdjustedToUTC, unit MILLIS (1)
                (10, _T_STRUCT, [(8, _T_STRUCT, [(1, _T_TRUE, True),
                                                 (2, _T_STRUCT, [(1, _T_STRUCT, [])])])]),
            ]
        elif kind == "string":
            # LogicalType.STRING (1)
            element += [(6, _T_I32, _CONVERTED_UTF8), (10, _T_STRUCT, [(1, _T_STRUCT, [])])]
        schema.append(element)
    footer = _struct([
        (1, _T_I32, 2),
        (2, _T_LIST, (_T_STRUCT, schema)),
        (3, _T_I64, rows),
        (4, _T_LIST, (_T_STRUCT, groups)),
        (5, _T_LIST, (_T_STRUCT, [[(1, _T_BINARY, k), (2, _T_BINARY, v)] for k, v in key_values])),
        (6, _T_BINARY, created_by),
        # ColumnOrder.TYPE_ORDER for every column: min and max are by the type's order
        (7, _T_LIST, (_T_STRUCT, [[(1, _T_STRUCT, [])] for _ in cols])),
    ])
    out += footer + struct.pack("<I", len(footer)) + MAGIC
    return bytes(out)
