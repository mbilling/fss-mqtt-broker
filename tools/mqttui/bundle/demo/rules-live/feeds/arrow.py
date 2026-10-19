"""A small Apache Arrow IPC writer, standard library only: the file format (ARROW1 ... footer)
and the stream format, for flat columns.

    arrow.file(columns, metadata, codec="zstd")    the IPC file format
    arrow.stream(columns, metadata)                the IPC stream format

A column is (name, kind, values), `values` a list that may hold None (a nullable column):

    "timestamp_ms"   Timestamp(MILLISECOND, "UTC"), ints of ms since the epoch
    "int32", "int64", "float", "double", "bool", "string"
    "dict_string"    strings, dictionary-encoded: Int32 indices, a DictionaryBatch of values

The metadata (Schema.fbs, Message.fbs, File.fbs) is FlatBuffers, built here front to back:
each table follows its vtable and comes before what it points to, so every offset is a
forward one. Buffers are padded to 8 bytes; with codec "zstd" each is compressed on its own
(BodyCompression ZSTD, BUFFER method: the uncompressed length, then the frame).
https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
"""

from __future__ import annotations

import struct
from typing import Any, List, Optional, Sequence, Tuple

# ---------------------------------------------------------------------------- FlatBuffers

_SCALARS = {"bool": "<?", "u8": "<B", "i8": "<b", "i16": "<h", "u16": "<H", "i32": "<i",
            "u32": "<I", "i64": "<q", "u64": "<Q"}


class Str:
    def __init__(self, s: str):
        self.raw = s.encode("utf-8")


class Vec:
    """A vector of `kind`: a scalar name, "off" (tables or strings), or ("struct", size,
    align) with items as bytes."""

    def __init__(self, kind: Any, items: Sequence):
        self.kind, self.items = kind, list(items)


class Table:
    """A table's fields: (slot, kind, value), kind a scalar name, "off" or ("struct", size,
    align)."""

    def __init__(self, *fields: Tuple[int, Any, Any]):
        self.fields = [f for f in fields if f[2] is not None]


def _size_align(kind: Any) -> Tuple[int, int]:
    if kind == "off":
        return 4, 4
    if isinstance(kind, tuple):
        return kind[1], kind[2]
    n = struct.calcsize(_SCALARS[kind])
    return n, n


class _Builder:
    def __init__(self):
        self.buf = bytearray()
        self.patches: List[Tuple[int, Any]] = []  # (where the uoffset goes, what it points to)

    def pad(self, align: int, extra: int = 0):
        while (len(self.buf) + extra) % align:
            self.buf.append(0)

    def place(self, obj) -> int:
        if isinstance(obj, Str):
            self.pad(4)
            at = len(self.buf)
            self.buf += struct.pack("<I", len(obj.raw)) + obj.raw + b"\x00"
            return at
        if isinstance(obj, Vec):
            size, align = _size_align(obj.kind)
            self.pad(max(align, 4), 4)
            at = len(self.buf)
            self.buf += struct.pack("<I", len(obj.items))
            children = []
            for item in obj.items:
                if obj.kind == "off":
                    children.append((len(self.buf), item))
                    self.buf += b"\x00\x00\x00\x00"
                elif isinstance(obj.kind, tuple):
                    self.buf += item
                else:
                    self.buf += struct.pack(_SCALARS[obj.kind], item)
            self._children(children)
            return at
        if isinstance(obj, Table):
            # Inline layout: the soffset to the vtable, then the fields, largest first.
            fields = sorted(obj.fields, key=lambda f: -_size_align(f[1])[1])
            layout, size = [], 4
            for slot, kind, value in fields:
                n, align = _size_align(kind)
                size = (size + align - 1) // align * align
                layout.append((slot, kind, value, size))
                size += n
            table_align = max([4] + [_size_align(k)[1] for _, k, _ in fields])
            slots = 1 + max([f[0] for f in fields], default=-1)
            vtable = [0] * slots
            for slot, _, _, off in layout:
                vtable[slot] = off
            self.pad(2)
            vt_at = len(self.buf)
            self.buf += struct.pack(f"<HH{slots}H", 4 + 2 * slots, size, *vtable)
            self.pad(table_align)
            at = len(self.buf)
            body = bytearray(size)
            struct.pack_into("<i", body, 0, at - vt_at)
            children = []
            for slot, kind, value, off in layout:
                if kind == "off":
                    children.append((at + off, value))
                elif isinstance(kind, tuple):
                    body[off:off + len(value)] = value
                else:
                    struct.pack_into(_SCALARS[kind], body, off, value)
            self.buf += body
            self._children(children)
            return at
        raise TypeError(type(obj).__name__)

    def _children(self, children):
        for where, child in children:
            to = self.place(child)
            struct.pack_into("<I", self.buf, where, to - where)


def flatbuffer(root: Table) -> bytes:
    b = _Builder()
    b.buf += b"\x00" * 8  # the root uoffset, and padding to keep 8-byte fields aligned
    to = b.place(root)
    struct.pack_into("<I", b.buf, 0, to)
    return bytes(b.buf)


# ---------------------------------------------------------------------------- Arrow metadata

V5 = 4                                             # MetadataVersion
HEADER_SCHEMA, HEADER_DICTIONARY, HEADER_RECORD_BATCH = 1, 2, 3
TYPE_INT, TYPE_FLOAT, TYPE_UTF8, TYPE_BOOL, TYPE_TIMESTAMP = 2, 3, 5, 6, 10
ZSTD = 1                                           # CompressionType

_KINDS = {  # kind: (type id, type table, value width in bytes, struct format)
    "int32": (TYPE_INT, lambda: Table((0, "i32", 32), (1, "bool", True)), 4, "i"),
    "int64": (TYPE_INT, lambda: Table((0, "i32", 64), (1, "bool", True)), 8, "q"),
    "float": (TYPE_FLOAT, lambda: Table((0, "i16", 1)), 4, "f"),
    "double": (TYPE_FLOAT, lambda: Table((0, "i16", 2)), 8, "d"),
    "timestamp_ms": (TYPE_TIMESTAMP, lambda: Table((0, "i16", 1), (1, "off", Str("UTC"))), 8, "q"),
    "bool": (TYPE_BOOL, lambda: Table(), 0, ""),
    "string": (TYPE_UTF8, lambda: Table(), 0, ""),
}


def _key_values(pairs) -> Optional[Vec]:
    pairs = list(pairs)
    if not pairs:
        return None
    return Vec("off", [Table((0, "off", Str(k)), (1, "off", Str(v))) for k, v in pairs])


def _field(name: str, kind: str, nullable: bool, dict_id: int) -> Table:
    if kind == "dict_string":
        index = Table((0, "i32", 32), (1, "bool", True))
        return Table((0, "off", Str(name)), (1, "bool", nullable), (2, "u8", TYPE_UTF8),
                     (3, "off", Table()), (4, "off", Table((0, "i64", dict_id), (1, "off", index))),
                     (5, "off", Vec("off", [])))
    type_id, type_table, _, _ = _KINDS[kind]
    return Table((0, "off", Str(name)), (1, "bool", nullable), (2, "u8", type_id),
                 (3, "off", type_table()), (5, "off", Vec("off", [])))


def _schema(columns, metadata) -> Table:
    fields = []
    dict_id = 0
    for name, kind, values in columns:
        fields.append(_field(name, kind, any(v is None for v in values), dict_id))
        if kind == "dict_string":
            dict_id += 1
    return Table((1, "off", Vec("off", fields)), (2, "off", _key_values(metadata)))


def _bitmap(bits: Sequence[bool]) -> bytes:
    out = bytearray((len(bits) + 7) // 8)
    for i, b in enumerate(bits):
        if b:
            out[i // 8] |= 1 << (i % 8)
    return bytes(out)


def _buffers(kind: str, values: Sequence, dictionary: Optional[List[str]] = None) -> List[bytes]:
    """The column's buffers: validity, then values (and offsets, for strings)."""
    nulls = sum(v is None for v in values)
    validity = _bitmap([v is not None for v in values]) if nulls else b""
    if kind == "dict_string":
        assert dictionary is not None
        idx = [0 if v is None else dictionary.index(v) for v in values]
        return [validity, struct.pack(f"<{len(idx)}i", *idx)]
    if kind == "bool":
        return [validity, _bitmap([bool(v) for v in values])]
    if kind == "string":
        offsets, data = [0], bytearray()
        for v in values:
            if v is not None:
                data += v.encode("utf-8")
            offsets.append(len(data))
        return [validity, struct.pack(f"<{len(offsets)}i", *offsets), bytes(data)]
    _, _, _, fmt = _KINDS[kind]
    zero = 0.0 if fmt in "fd" else 0
    return [validity, struct.pack(f"<{len(values)}{fmt}", *[zero if v is None else v for v in values])]


def _zstd(data: bytes) -> bytes:
    from compression import zstd  # Python 3.14
    return zstd.compress(data)


def _record_batch(columns, dictionaries, codec: Optional[str]) -> Tuple[Table, bytes]:
    """A RecordBatch table and its body."""
    rows = len(columns[0][2]) if columns else 0
    body = bytearray()
    nodes, buffers = [], []
    for name, kind, values in columns:
        nodes.append(struct.pack("<qq", len(values), sum(v is None for v in values)))
        for buf in _buffers(kind, values, dictionaries.get(name)):
            if codec and buf:
                buf = struct.pack("<q", len(buf)) + _zstd(buf)
            buffers.append(struct.pack("<qq", len(body), len(buf)))
            body += buf
            while len(body) % 8:
                body.append(0)
    compression = Table((0, "i8", ZSTD), (1, "i8", 0)) if codec else None
    return Table((0, "i64", rows), (1, "off", Vec(("struct", 16, 8), nodes)),
                 (2, "off", Vec(("struct", 16, 8), buffers)), (3, "off", compression)), bytes(body)


def _message(header_type: int, header: Table, body_length: int) -> bytes:
    return flatbuffer(Table((0, "i16", V5), (1, "u8", header_type), (2, "off", header),
                            (3, "i64", body_length)))


def _encapsulate(meta: bytes, body: bytes) -> Tuple[bytes, int]:
    """A message as the IPC formats frame it: the continuation marker, the metadata's length,
    the metadata padded to 8 bytes, then the body. Also the metadata's framed length."""
    pad = (-(len(meta) + 8)) % 8
    head = b"\xff\xff\xff\xff" + struct.pack("<i", len(meta) + pad) + meta + b"\x00" * pad
    return head + body, len(head)


EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


def _messages(columns, metadata, codec):
    """The schema message, then a DictionaryBatch per dictionary column, then one record
    batch: (bytes, framed metadata length, body length, kind) each."""
    dictionaries = {}
    for name, kind, values in columns:
        if kind == "dict_string":
            seen: List[str] = []
            for v in values:
                if v is not None and v not in seen:
                    seen.append(v)
            dictionaries[name] = seen
    out = []
    meta = _message(HEADER_SCHEMA, _schema(columns, metadata), 0)
    framed, n = _encapsulate(meta, b"")
    out.append((framed, n, 0, "schema"))
    dict_id = 0
    for name, kind, _ in columns:
        if kind != "dict_string":
            continue
        batch, body = _record_batch([(name, "string", dictionaries[name])], {}, codec)
        header = Table((0, "i64", dict_id), (1, "off", batch), (2, "bool", False))
        framed, n = _encapsulate(_message(HEADER_DICTIONARY, header, len(body)), body)
        out.append((framed, n, len(body), "dictionary"))
        dict_id += 1
    batch, body = _record_batch(columns, dictionaries, codec)
    framed, n = _encapsulate(_message(HEADER_RECORD_BATCH, batch, len(body)), body)
    out.append((framed, n, len(body), "batch"))
    return out


def stream(columns, metadata=(), codec: Optional[str] = None) -> bytes:
    """The IPC stream format: the messages, then the end-of-stream marker."""
    return b"".join(m[0] for m in _messages(columns, metadata, codec)) + EOS


def file(columns, metadata=(), codec: Optional[str] = None) -> bytes:
    """The IPC file format: magic, the stream, a footer that indexes its blocks, magic."""
    out = bytearray(b"ARROW1\x00\x00")
    dict_blocks, batch_blocks = [], []
    for framed, meta_len, body_len, kind in _messages(columns, metadata, codec):
        block = struct.pack("<qi4xq", len(out), meta_len, body_len)
        if kind == "dictionary":
            dict_blocks.append(block)
        elif kind == "batch":
            batch_blocks.append(block)
        out += framed
    out += EOS
    footer = flatbuffer(Table((0, "i16", V5), (1, "off", _schema(columns, metadata)),
                              (2, "off", Vec(("struct", 24, 8), dict_blocks)),
                              (3, "off", Vec(("struct", 24, 8), batch_blocks))))
    out += footer + struct.pack("<i", len(footer)) + b"ARROW1"
    return bytes(out)
