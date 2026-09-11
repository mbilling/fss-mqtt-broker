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
CLIENT_GUIDE = ROOT / "docs" / "CLIENT-GUIDE.md"
SRC_GLOBS = ("crates/*/src/*.rs", "crates/*/src/**/*.rs")
TEST_GLOBS = ("crates/*/tests/*.rs", "crates/*/tests/**/*.rs")
REASON_BEGIN = "<!-- reason-codes:begin -->"
REASON_END = "<!-- reason-codes:end -->"

# One line per emittable failure code, sourced from the production call sites the
# emission scan already found. A new emittable code without an entry here fails
# the run — CLIENT-GUIDE cannot silently omit a code the broker can send.
WHEN_EMITTED: dict[int, str] = {
    0x80: "SUBACK failure slot, or a codec value the broker maps as unspecified error.",
    0x81: "Malformed packet at decode (CONNACK/DISCONNECT depending on when it is caught).",
    0x82: "Protocol violation (illegal packet for the current state).",
    0x84: "Mapped in `conn.rs::codec_reason` for totality; unreachable on the wire (CONNECT with an unsupported protocol level closes silently per [MQTT-3.14.0-1]).",
    0x87: "Authentication or ACL denial (CONNACK, PUBACK/PUBREC, DISCONNECT on revocation sweep).",
    0x8B: "Graceful drain of live v5 sessions (ADR 0019 / `SIGTERM`).",
    0x8C: "Enhanced-authentication method the broker does not accept.",
    0x8F: "SUBSCRIBE/UNSUBSCRIBE filter the broker rejects (including a malformed `$share/...`).",
    0x93: "Client exceeded the server's advertised Receive Maximum (inbound QoS > 0 in flight).",
    0x94: "Topic alias out of range or used before it was bound (ADR 0011).",
    0x95: "Inbound packet larger than the advertised Maximum Packet Size.",
    0x97: "A quota or brownout refusal (sessions, subscriptions, retained growth, durable-append floor).",
    0x9C: "This node no longer owns the persistent session; reconnect and land on the owner (issue #284).",
}

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
    0x9C: (
        "USE_ANOTHER_SERVER — sent to a v5 session this node no longer owns after "
        "a placement roll, so the client reconnects and lands on the owner "
        "(hub.rs::rehome_misplaced_sessions, #284). Covered by eight hub unit "
        "tests via `await_rehome_disconnect`, which asserts this exact code — but "
        "they drive an mpsc channel, and this gate counts integration provocations "
        "only. Provoking it on a socket needs placement to MOVE under a live "
        "connection, so it belongs to the out-of-process cluster harness. TODO: "
        "provoke it there, then delete this entry."
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


def catalogue_markdown(emits: dict[int, list[str]], asserted: set[int], by_value: dict[int, str]) -> str:
    """The CLIENT-GUIDE table held to the gated emission list."""
    rows = [
        "| Code | Name | When mqttd emits it | Tests |",
        "|------|------|---------------------|-------|",
    ]
    for value in sorted(emits):
        name = by_value.get(value, "?")
        when = WHEN_EMITTED.get(value)
        if when is None:
            raise SystemExit(
                f"emittable {value:#04x} {name} has no WHEN_EMITTED line — "
                "CLIENT-GUIDE cannot invent one; add it next to the emission site."
            )
        if value in EXEMPT:
            status = "exempt (see `scripts/check-reason-codes.py`)"
        elif value in asserted:
            status = "provoked in integration tests"
        else:
            status = "unprovoked"
        rows.append(f"| `{value:#04x}` | `{name}` | {when} | {status} |")
    return "\n".join(rows) + "\n"


def refresh_client_guide(table: str, check: bool) -> list[str]:
    """Keep the marked catalogue in CLIENT-GUIDE.md in lock-step with the scan."""
    problems: list[str] = []
    if not CLIENT_GUIDE.exists():
        problems.append(f"{CLIENT_GUIDE.relative_to(ROOT)} is missing")
        return problems
    text = CLIENT_GUIDE.read_text(encoding="utf-8")
    if REASON_BEGIN not in text or REASON_END not in text:
        problems.append(
            f"{CLIENT_GUIDE.relative_to(ROOT)} needs {REASON_BEGIN} … {REASON_END} "
            "around the emitted-reason-code table"
        )
        return problems
    before, rest = text.split(REASON_BEGIN, 1)
    _, after = rest.split(REASON_END, 1)
    new = before + REASON_BEGIN + "\n\n" + table + "\n" + REASON_END + after
    if new != text:
        if check:
            problems.append(
                "docs/CLIENT-GUIDE.md reason-code catalogue is stale — "
                "the emission list moved; re-run scripts/check-reason-codes.py "
                "(it rewrites the marked table unless --check)"
            )
        else:
            CLIENT_GUIDE.write_text(new, encoding="utf-8")
    return problems


def main() -> int:
    names = catalogue()
    by_value = {v: n for n, v in names.items()}
    emits, asserted = scan()
    check = "--check" in sys.argv

    problems = self_check()
    gaps = {v: w for v, w in emits.items() if v not in asserted and v not in EXEMPT}
    stale = sorted(v for v in EXEMPT if v in asserted)
    missing_when = sorted(v for v in emits if v not in WHEN_EMITTED)
    extra_when = sorted(v for v in WHEN_EMITTED if v not in emits)

    print(
        f"Reason-code audit: {len(names)} defined, {len(emits)} emittable (>= 0x80), "
        f"{len(asserted & set(emits))} provoked, {len(gaps)} unprovoked."
    )
    for value, why in sorted(EXEMPT.items()):
        print(f"  exempt {value:#04x} {by_value.get(value, '?')}: {why}")

    if missing_when:
        problems.append(
            "WHEN_EMITTED is missing emittable codes: "
            + ", ".join(f"{v:#04x}" for v in missing_when)
        )
    if extra_when:
        problems.append(
            "WHEN_EMITTED lists codes the broker no longer emits: "
            + ", ".join(f"{v:#04x}" for v in extra_when)
        )

    try:
        table = catalogue_markdown(emits, asserted, by_value)
    except SystemExit as e:
        problems.append(str(e))
        table = ""
    if table:
        problems.extend(refresh_client_guide(table, check=check))

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
