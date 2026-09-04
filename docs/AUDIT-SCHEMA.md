# Audit export schema

**Verified against `v1.0.15` + ADR 0066 T3 (2026-09-04).** The contract a SIEM
parser is written against: the record format, the complete kind vocabulary, the
boundary invariants, the delivery semantics, and the verification procedure.
Stability promise: **fields and semantics below are frozen; new `kind` values
are additive-only** — a parser that ignores unknown kinds keeps working.

## Transport

With `audit.syslog = "host:port"` (`MQTTD_AUDIT_SYSLOG`), every audit record is
exported as **RFC 5424 syslog over TCP, RFC 6587 octet-counted**:

```
<109>1 2026-08-19T15:42:51Z <hostname> mqttd <pid> <kind> - <JSON>
```

PRI 109 = facility 13 (log audit), severity 5 (notice). MSGID carries the record
kind so collectors can route before parsing. The MSG is **one JSON object per
record** — the machine-parseable form; the same records also land in the broker
log as `target: audit` tracing lines regardless of export. TLS is deliberately
not in this transport: ship via a localhost relay (rsyslog/vector/fluent-bit
own the TLS hop) or a network you already trust for logs.

## The record

```json
{"boot":"<32-hex>","seq":3,"kind":"auth.success","subject":"alice",
 "head":"<64-hex>","detail":"client c-1 via password"}
```

| Field | Presence | Meaning |
|---|---|---|
| `boot` | always | This boot's chain id (random per process start) |
| `seq` | absent on genesis | Position in this boot's chain, from 0, contiguous |
| `kind` | always | Event category (vocabulary below) |
| `subject` | when known | The principal — an identity or client id, **never a credential** |
| `head` | always | The chain head **after** this record (lowercase hex SHA-256) |
| `detail` | when non-empty | Human-readable specifics; contains no secrets |

## The chain

`head` = SHA-256 over (previous head ‖ seq as u64-BE ‖ length-prefixed kind ‖
subject presence-tag+length-prefix ‖ length-prefixed detail). The genesis head
is SHA-256 of `"mqttd-audit-chain-genesis:v1:boot:" + boot` — derivable from
the announced boot id by anyone. Verification therefore needs **no secret**:
the stream proves itself (the external-anchoring model; see the
[threat model](THREAT-MODEL.md)).

## Boundary invariants — what your SIEM should alert on

1. Every chain **opens** with `kind: "audit.genesis"` (no `seq`; announces
   `boot` and the genesis head).
2. Every chain **closes** with `kind: "audit.shutdown"` (its `detail` carries
   the drain outcome: `drained`, `grace-elapsed`, or `second-signal`).
3. **A chain that just stops** — no `audit.shutdown` — is a crash or a
   suppression. Alert.
4. **A genesis not preceded by a shutdown** (per host) is the same event seen
   from the other side. Alert once per incident, not twice.

## Delivery semantics

- The export is a **copy** of the chain, never a gate on the broker: the hot
  path never blocks on the SIEM.
- Bounded queue (8192 records); when full, records are **shed and counted**
  (`audit_export_dropped` metric, WARN once per episode). A shed shows up
  downstream as a **`seq` gap** — detectable by construction. The chain at the
  source is intact regardless.
- Delivery is **at-least-once** across reconnects: de-duplicate on
  `(boot, seq)`.
- On graceful shutdown the broker flushes the export (bounded, 3 s) so the
  closing record reaches you; a crash may lose the unexported tail — which
  invariant 3 turns into a visible event rather than a silent one.

## Kind vocabulary (complete at v1.0.0; additive-only from here)

| Kind | Subject | Emitted when |
|---|---|---|
| `audit.genesis` | — | Chain start, at boot (pseudo-record: no `seq`) |
| `audit.shutdown` | — | Graceful stop's last record; closes the chain |
| `auth.success` | identity | A client authenticated (method + relay noted in `detail`) |
| `auth.failure` | client id | Authentication refused |
| `auth.reauth` | identity | MQTT 5 re-authentication succeeded |
| `auth.reauth.failure` | identity | Re-authentication refused (session ends) |
| `acl.deny.connect` | identity | A `connect` rule refused this client id claim |
| `acl.deny.publish` | identity | A publish was refused (v5 told `0x87`; v3.1.1 dropped) |
| `acl.deny.subscribe` | identity | A subscription filter was refused (`0x87` in SUBACK) |
| `acl.deny.will` | identity | A will topic was refused at CONNECT |
| `session.bind.mismatch` | identity | A principal tried to resume a session owned by another (ADR 0031) |
| `security.reload` | — | Policy reload applied (ADR 0032) |
| `config.reload` | — | Config reload applied |
| `security.sweep` | identity | A live session terminated by a policy sweep (ADR 0040) |
| `security.evict` | identity | A live session's grants tightened/evicted on reload |
| `security.penalty` | source addr | Auth-failure penalty box engaged for a source (ADR 0041 T2) |

## Verification

```sh
scripts/audit-verify.py <captured-stream>...   # or - for stdin
scripts/audit-verify.py --self-test            # golden vector from the Rust impl
```

The verifier reproves the chain (head-by-head), the genesis derivation, seq
contiguity, and closure — exit 0 means the stream is the one the broker wrote
and nothing was cut from its tail. It parses both raw octet-counted captures
and line-oriented files. The end-to-end proof runs in CI:
`binary_smoke::the_audit_export_ships_a_verifiable_chain` boots the real
binary, captures the export, and replays the chain.
