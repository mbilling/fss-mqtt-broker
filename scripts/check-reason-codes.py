#!/usr/bin/env python3
"""Every MQTT reason code the broker can EMIT must have a test that provokes it.

A reason code you have never emitted in a test is a reason code you have never
verified. The catalogue in `mqtt-codec/src/reason.rs` is not the bar — most of it
is codes we never send — so this compares tests against what production actually
places on the wire, and fails when the broker can say something no test has ever
heard it say.

Scope: **failure codes only (>= 0x80)**. That boundary is not arbitrary. The
MQTT 3.1.1 CONNACK *return codes* (0x00-0x05) are a different code space that
happens to share the byte type: v3 0x05 means "not authorized" where v5 uses
0x87, and v3 0x04 means "bad credentials" where 0x04 in the v5 space is a
client's "disconnect with will". Restricting to >= 0x80 keeps the two spaces from
being conflated, with no need to migrate ~90 test literals to symbols.

Usage: scripts/check-reason-codes.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CATALOGUE = ROOT / "crates" / "mqtt-codec" / "src" / "reason.rs"
SRC_GLOBS = ("crates/*/src/*.rs", "crates/*/src/**/*.rs")
TEST_GLOBS = ("crates/*/tests/*.rs", "crates/*/tests/**/*.rs")

# Codes production can place on the wire but which no test currently provokes,
# each with the reason. An entry here is a claim that must stay true — it is not
# a way to silence the gate. The text prints on every successful run so it stays
# visible instead of rotting in a file nobody opens, and the gate FAILS if an
# exempt code becomes provoked, so the list cannot quietly outlive its reasons.
EXEMPT: dict[int, str] = {
    0x84: (
        "UNSUPPORTED_PROTOCOL_VERSION — mapped in conn.rs::codec_reason for "
        "totality, but unreachable: an unsupported protocol level is only "
        "detectable while decoding CONNECT, and [MQTT-3.14.0-1] forbids a "
        "DISCONNECT before a success CONNACK, so the connection closes silently."
    ),
    0x8B: (
        "SERVER_SHUTTING_DOWN — sent to live v5 sessions during graceful drain "
        "(conn.rs, ADR 0019). Genuinely tested, but only by a conn.rs unit test "
        "driving a duplex stream, and this gate deliberately counts integration "
        "provocations only. TODO: drive a drain from the in-process harness so "
        "the code is observed on a real socket, then delete this entry."
    ),
    0x88: (
        "SERVER_UNAVAILABLE — emitted when durable-session recovery passes its "
        "deadline during a lease handoff (conn.rs, via hub::recover_until_ready). "
        "Needs a real durable cluster losing quorum mid-attach; not provokable "
        "with the in-process MemorySessionStore the protocol suites use. TODO: "
        "provoke it in the out-of-process cluster harness rather than exempt it."
    ),
}


def catalogue() -> dict[str, int]:
    """`NAME -> value` for every reason constant."""
    return {
        m.group(1): int(m.group(2), 16)
        for m in re.finditer(
            r"pub const (\w+): u8 = (0x[0-9A-Fa-f]{2});", CATALOGUE.read_text()
        )
    }


def rust_files(*globs: str) -> list[Path]:
    return sorted({p for g in globs for p in ROOT.glob(g) if p.is_file()})


def _blank(seg: str) -> str:
    """Replace a span with spaces, preserving newlines so `^` anchors survive."""
    return "".join(ch if ch == "\n" else " " for ch in seg)


# Raw strings are located by explicit scan, not by regex: a non-greedy
# `r(#*)"(?s:.)*?"\1` backtracks catastrophically on files this size — it hung
# the first version of this script outright. `str.find` for the hash-balanced
# closer is linear and exact.
_RAW_OPEN = re.compile(r'(?<![A-Za-z0-9_])r(#*)"')

# Everything else, in precedence order. Raw strings are already blanked by the
# time this runs, so the ordinary-string rule cannot mispair their quotes.
_NOISE = re.compile(
    r"""(?P<line>//[^\n]*)
      | (?P<block>/\*.*?\*/)
      | (?P<chr>'(?:[^'\\\n]|\\.)')
      | (?P<str>"(?:[^"\\\n]|\\.)*")
    """,
    re.X | re.S,
)


def strip_prose(text: str) -> str:
    """Blank out comments and literals, leaving only code.

    Without this the gate is silently LENIENT, which is the one failure mode it
    must not have: a doc comment reading "rejected with 0x88", or an assertion
    *message* reading "must use reason 0x8B", would count as coverage — the audit
    would then report a code as provoked by prose that merely mentions it. Both
    cases were present on this script's first run.

    Raw strings get their own pass because getting them wrong is not a near miss.
    A naive string regex pairs quotes straight through `r#"..."#`, swallowing
    everything between two unrelated literals; in `conn.rs` that ate the
    `#[cfg(test)] mod tests` marker, so the entire test module was classified as
    production. A gate that reads tests as production cannot fail, which is the
    only thing it is for — hence `self_check` below.
    """
    out: list[str] = []
    i = 0
    for m in _RAW_OPEN.finditer(text):
        if m.start() < i:
            continue  # already inside a consumed raw string
        closer = '"' + m.group(1)
        end = text.find(closer, m.end())
        end = len(text) if end < 0 else end + len(closer)
        out.append(text[i : m.start()])
        out.append(_blank(text[m.start() : end]))
        i = end
    out.append(text[i:])
    return _NOISE.sub(lambda m: _blank(m.group(0)), "".join(out))


def split_test_modules(text: str) -> tuple[str, str]:
    """Split source into (production, in-file `mod tests`) halves.

    A `reason::X` inside `#[cfg(test)] mod tests` is an assertion, not an
    emission; counting it as production would let a test satisfy its own
    requirement.
    """
    m = re.search(r"^#\[cfg\(test\)\]\s*\nmod tests\b", text, re.M)
    return (text, "") if not m else (text[: m.start()], text[m.start() :])


def alias_map(text: str) -> dict[str, str]:
    """`ALIAS -> REASON_NAME` for `const ALIAS: u8 = reason::REASON_NAME;`.

    `conn.rs` defines broker-context aliases (`SUBACK_FAILURE`, `DISCONNECT_*`)
    so call sites read well. They are still emissions of the underlying code, and
    on the test side `DISCONNECT_SERVER_SHUTTING_DOWN` contains
    `SERVER_SHUTTING_DOWN` as a substring but not as a word — so without
    resolving aliases the gate calls a genuinely-tested code untested.
    """
    return dict(re.findall(r"const (\w+): u8 = reason::(\w+);", text))


def scan() -> tuple[dict[int, list[str]], set[int]]:
    """`(emitted value -> sites, asserted values)`."""
    names = catalogue()
    by_name = {n: v for n, v in names.items() if v >= 0x80}

    emits: dict[int, list[str]] = {}
    aliases: dict[str, int] = {}
    # Only INTEGRATION tests count as provocations. An in-`src` `mod tests` may
    # assert a code without the broker ever putting it on a socket — this
    # script's own `codec_reason` mapping test compares two constants and would
    # otherwise mark every code in that table "provoked" while proving nothing
    # about emission. Integration tests drive a real connection, so an assertion
    # there means the code was genuinely observed.
    test_texts: list[str] = [strip_prose(p.read_text()) for p in rust_files(*TEST_GLOBS)]

    for path in rust_files(*SRC_GLOBS):
        prod, _ = split_test_modules(strip_prose(path.read_text()))
        local = alias_map(prod)
        for alias, target in local.items():
            if target in by_name:
                aliases[alias] = by_name[target]
        for name, value in by_name.items():
            hit = f"reason::{name}" in prod or any(
                re.search(rf"\b{a}\b", prod) for a, t in local.items() if t == name
            )
            if hit:
                emits.setdefault(value, []).append(f"{path.relative_to(ROOT)} ({name})")

    asserted: set[int] = set()
    for text in test_texts:
        for lit in re.findall(r"0x([89A-Fa-f][0-9A-Fa-f])\b", text):
            asserted.add(int(lit, 16))
        for name, value in by_name.items():
            if re.search(rf"\b{name}\b", text):
                asserted.add(value)
        for alias, value in aliases.items():
            if re.search(rf"\b{alias}\b", text):
                asserted.add(value)
    return emits, asserted


def self_check() -> list[str]:
    """Guard the guard.

    Both real bugs found while writing this script made it *more permissive*, and
    a permissive coverage gate is worse than none: it reports success it has not
    checked. These assertions fail loudly on the two shapes that caused it.
    """
    problems = []
    conn = ROOT / "crates" / "mqttd" / "src" / "conn.rs"
    if conn.exists():
        prod, tests = split_test_modules(strip_prose(conn.read_text()))
        if not tests:
            problems.append(
                "strip_prose/split_test_modules found no `mod tests` in conn.rs — "
                "the literal-stripper is eating code again, so test code would be "
                "read as production and the gate could not fail."
            )
        if "codec_reason" not in prod:
            problems.append("conn.rs production half lost `codec_reason` — stripper bug.")
    sample = strip_prose('let x = 1; // rejected with 0x88\nassert!(a, "reason 0x8B");')
    if "0x88" in sample or "0x8B" in sample:
        problems.append("strip_prose left a code in a comment or string literal.")
    return problems


def main() -> int:
    names = catalogue()
    by_value = {v: n for n, v in names.items()}
    emits, asserted = scan()

    problems = self_check()
    gaps = {v: w for v, w in emits.items() if v not in asserted and v not in EXEMPT}
    stale = sorted(v for v in EXEMPT if v in asserted)

    print(
        f"Reason-code audit: {len(names)} defined, {len(emits)} emittable (>= 0x80), "
        f"{len(asserted & set(emits))} provoked, {len(gaps)} unprovoked."
    )
    for value, why in sorted(EXEMPT.items()):
        print(f"  exempt {value:#04x} {by_value.get(value, '?')}: {why}")

    if problems:
        print("\nFAIL: the checker's own invariants broke:")
        for p in problems:
            print(f"  {p}")
    if stale:
        print("\nFAIL: exempt codes that ARE now provoked — delete the exemption:")
        for value in stale:
            print(f"  {value:#04x} {by_value.get(value, '?')}")
    if gaps:
        print("\nFAIL: the broker can emit these, but no test provokes them:")
        for value, where in sorted(gaps.items()):
            print(f"  {value:#04x} {by_value.get(value, '?')} — emitted at {where[0]}")
        print(
            "\nWrite a test that provokes the code, or add an EXEMPT entry here "
            "saying why it cannot be."
        )

    return 1 if (gaps or stale or problems) else 0


if __name__ == "__main__":
    sys.exit(main())
