#!/usr/bin/env python3
"""Verify an exported mqttd audit stream (ADR 0066 T3; docs/AUDIT-SCHEMA.md).

Reads the SIEM-side copy of the audit export — RFC 5424 syslog lines whose MSG
is one JSON object per record, or bare JSON lines — and proves, with no secret
and no access to the broker, that the stream is the one the broker wrote:

  1. CHAIN:    every record's `head` reproduces from SHA-256 over the previous
               head and the record's own fields (the broker's exact algorithm,
               reimplemented here so verification is independent);
  2. GENESIS:  each chain's first head derives from its announced boot id;
  3. SEQ:      sequence numbers are contiguous per boot — a gap means the
               broker's exporter shed under pressure (audit_export_dropped on
               the broker's metrics says the same thing from the other side);
  4. CLOSURE:  a chain ends with `audit.shutdown`, or the verifier says which
               boots did not — a crash or a suppression, either worth an alert.

Exit 0 = every chain verifies and closes. Exit 1 = any violation, each printed.
A duplicated record (the export is at-least-once across reconnects) is
de-duplicated on (boot, seq) before verification, as the schema documents.

Usage:
  scripts/audit-verify.py <file...>     verify exported streams (or - for stdin)
  scripts/audit-verify.py --self-test   verify the embedded golden vector
                                        (generated from the Rust implementation)
"""

from __future__ import annotations

import hashlib
import json
import struct
import sys

CHAIN_GENESIS = b"mqttd-audit-chain-genesis:v1"


def genesis_head(boot: str) -> bytes:
    return hashlib.sha256(CHAIN_GENESIS + b":boot:" + boot.encode()).digest()


def lp(b: bytes) -> bytes:
    """Length-prefixed field, exactly as the broker hashes it."""
    return struct.pack(">Q", len(b)) + b


def step(prev: bytes, seq: int, kind: str, subject: str | None, detail: str) -> bytes:
    h = hashlib.sha256()
    h.update(prev)
    h.update(struct.pack(">Q", seq))
    h.update(lp(kind.encode()))
    if subject is None:
        h.update(b"\x00")
    else:
        h.update(b"\x01" + lp(subject.encode()))
    h.update(lp(detail.encode()))
    return h.digest()


def parse_records(text: str) -> list[dict]:
    """Extract every JSON record from the input, whatever the framing.

    The syslog transport is RFC 6587 octet-counted — frames arrive back to back
    with no newline between them — and a file of bare JSON lines must parse too.
    So: scan for '{' and let the JSON decoder consume exactly one object.
    """
    records = []
    dec = json.JSONDecoder()
    i = 0
    while True:
        i = text.find("{", i)
        if i < 0:
            break
        try:
            obj, end = dec.raw_decode(text, i)
        except json.JSONDecodeError:
            i += 1
            continue
        if isinstance(obj, dict) and "boot" in obj and "head" in obj:
            records.append(obj)
            i = end
        else:
            i += 1
    return records


def verify(records: list[dict]) -> list[str]:
    problems: list[str] = []
    boots: dict[str, dict] = {}
    for rec in records:
        boots.setdefault(rec["boot"], {"genesis": None, "by_seq": {}})
        if rec.get("kind") == "audit.genesis":
            boots[rec["boot"]]["genesis"] = rec
        elif "seq" in rec:
            boots[rec["boot"]]["by_seq"].setdefault(rec["seq"], rec)  # dedup (boot, seq)

    for boot, data in boots.items():
        prev = genesis_head(boot)
        gen = data["genesis"]
        if gen is None:
            problems.append(f"boot {boot}: no genesis record in the stream")
        elif gen["head"] != prev.hex():
            problems.append(
                f"boot {boot}: genesis head {gen['head']} does not derive from the "
                f"announced boot id (expected {prev.hex()})"
            )
        seqs = sorted(data["by_seq"])
        expected = 0
        last_kind = None
        for seq in seqs:
            if seq != expected:
                problems.append(
                    f"boot {boot}: seq gap — expected {expected}, next exported is {seq} "
                    f"(the broker's exporter shed under pressure, or records were lost "
                    f"in transit; the CHAIN below still verifies across the gap ONLY if "
                    f"nothing was altered)"
                )
                # Cannot verify across a gap (the missing record's fields fed the
                # hash); restart verification from the gap's head.
                prev = bytes.fromhex(data["by_seq"][seq]["head"])
                expected = seq + 1
                last_kind = data["by_seq"][seq].get("kind")
                continue
            rec = data["by_seq"][seq]
            prev = step(prev, seq, rec.get("kind", ""), rec.get("subject"), rec.get("detail", ""))
            if rec["head"] != prev.hex():
                problems.append(
                    f"boot {boot}: HEAD MISMATCH at seq {seq} — the stream does not "
                    f"reproduce the broker's chain (record altered, reordered, or forged)"
                )
                prev = bytes.fromhex(rec["head"])  # continue past, resynced
            expected += 1
            last_kind = rec.get("kind")
        if seqs and last_kind != "audit.shutdown":
            problems.append(
                f"boot {boot}: chain does not close with audit.shutdown (last kind: "
                f"{last_kind}) — crash, or the tail was suppressed"
            )
    return problems


GOLDEN = [
    {"boot": "golden-boot", "kind": "audit.genesis",
     "head": "5b8e32416695232102b5a218dbef4e896d85a1a5fc091e2c5ca50df5a8c1274f"},
    {"boot": "golden-boot", "seq": 0, "kind": "auth.success", "subject": "alice",
     "detail": "client c-1 via password",
     "head": "1920c3ff24d82f578595ce44939ce1a1c1d489be751c9bb7d68a554ddc8339d2"},
    {"boot": "golden-boot", "seq": 1, "kind": "acl.deny.publish",
     "detail": "topic forbidden/x",
     "head": "931629e17dba2a0d920979b33fd79fa6032d0fb9f6c3a68e9807afdbf451ba80"},
    {"boot": "golden-boot", "seq": 2, "kind": "audit.shutdown",
     "detail": "graceful shutdown (drained); this record closes the chain",
     "head": "49aee52c2861f25240954172828e524d2f9ba2f090c7c15daacff19303db186c"},
]


def self_test() -> int:
    problems = verify(list(GOLDEN))
    if problems:
        print("SELF-TEST FAILED — the verifier disagrees with the Rust implementation:")
        for p in problems:
            print(f"  - {p}")
        return 1
    # And the negative: a tampered detail must be caught.
    tampered = json.loads(json.dumps(GOLDEN))
    tampered[2]["detail"] = "topic allowed/x"
    if not any("HEAD MISMATCH" in p for p in verify(tampered)):
        print("SELF-TEST FAILED — a tampered record went undetected")
        return 1
    print("self-test OK: golden chain verifies; tampering is detected")
    return 0


def main() -> int:
    args = sys.argv[1:]
    if args == ["--self-test"]:
        return self_test()
    if not args:
        print(__doc__)
        return 2
    text = ""
    for path in args:
        if path == "-":
            text += sys.stdin.read()
        else:
            with open(path, encoding="utf-8", errors="replace") as f:
                text += f.read()
    records = parse_records(text)
    if not records:
        print("no audit records found in the input")
        return 1
    problems = verify(records)
    boots = {r['boot'] for r in records}
    if problems:
        print(f"audit-verify: {len(records)} records, {len(boots)} boot(s) — VIOLATIONS:")
        for p in problems:
            print(f"  - {p}")
        return 1
    print(f"audit-verify: OK — {len(records)} records across {len(boots)} boot(s): "
          f"every chain reproduces, every chain closes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
