# ADR 0008 — MQTT 5.0 codec

- **Status:** Accepted (design); implementation phased (codec milestone, gates workstream G)
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) (session/message expiry needs v5),
  [Capability Plan](../CAPABILITY-PLAN.md) Phase 4,
  [Cluster Durability Plan](../CLUSTER-DURABILITY-PLAN.md) workstream G

## Context

`mqtt-codec` implements the complete **MQTT 3.1.1** control-packet set and is the
broker's untrusted-input boundary (no-panic, no-`unsafe`, the prime fuzzing target).
`ProtocolVersion::V5` already exists, but every v5 packet is refused at the wire with
`V5_UNSUPPORTED` so a v5 frame is never mis-parsed as v4.

MQTT 5.0 is a strict superset of 3.1.1's framing with three additions:

1. **Properties** — most packets gain a variable-byte-length-prefixed block of typed
   key/value properties (27 identifiers: session expiry, receive maximum, topic
   alias, content type, user properties, …). This is the bulk of the work.
2. **Reason codes** — a single `u8` reason-code space replaces 3.1.1's per-packet
   return codes, and reason codes appear on packets that had none (PUBACK/REC/REL/COMP,
   DISCONNECT, AUTH, UNSUBACK), often in a *short form* that omits trailing fields.
3. **New shapes** — an AUTH packet; SUBSCRIBE gains a per-filter *subscription options*
   byte (No-Local, Retain-As-Published, Retain-Handling) in place of a bare QoS.

Several questions have one defensible answer each; this ADR fixes them so the phased
implementation is mechanical.

## Decision

### 1. One version-tagged `Packet` enum, not a parallel v5 hierarchy

Keep the existing single `Packet`/`Connect`/`Publish`/… types and the
`encode(&self, out, version)` / `decode(buf, version)` entry points. v5-only data
(properties, reason codes, subscription options) lives as fields on the existing
structs, **defaulted/empty for v4** and only written/read when `version == V5`.
`Connect` already carries `protocol` this way. A parallel `v5::Packet` hierarchy
would double every downstream `match` for no semantic gain — the broker logic is
overwhelmingly version-agnostic, and the few version-specific behaviours branch on
`protocol`, not on a type.

### 2. Properties: a generic `Property` enum + a `Properties` block codec

The wire foundation is a single `Property` enum with one variant per identifier
holding its typed value (e.g. `SessionExpiryInterval(u32)`, `ContentType(String)`,
`UserProperty(String, String)`), plus a `Properties(Vec<Property>)` newtype that owns
the block (de)serialization: a variable-byte **length** prefix, then the property
sequence, parsed to *exactly* that length.

- **Why generic, not per-packet typed structs.** A codec's job is faithful, total
  wire round-trip and exhaustive matching for the fuzzer; one block codec covers
  every packet. Per-packet typed property structs (10+ packets × their property
  subsets) are boilerplate that belongs *above* the wire. We add thin typed
  accessors (`props.session_expiry_interval()` → `Option<u32>`) where the broker
  needs them, reading the `Vec`.
- **Validation the codec owns** (all wire-level, per §3): a property identifier
  must be defined and **allowed on that packet type** (else Protocol Error); a
  non-repeatable property must not be **duplicated** (else Protocol Error); each
  value must decode within the block bounds (else Malformed Packet). User Property
  is the only repeatable identifier and its order is preserved.
- **Validation the codec does NOT own:** semantic policy (negotiated maximum packet
  size, whether a topic alias is in range for this connection, flow-control
  accounting). That is the broker's, above the codec.

### 3. Reason codes are `u8` with named constants

Carry every reason code as the raw `u8` the wire uses, with documented constants
(`reason::SUCCESS`, `reason::NOT_AUTHORIZED`, …). The codec does **not** reject an
unknown-but-structurally-valid reason byte — robustness over a closed enum, and the
broker decides what an unexpected code means. The existing `ConnAck.code: u8` already
follows this; the ack packets grow an analogous `reason: u8` (defaulting to `0` /
success, the value used to omit it in the v4 and v5-short forms).

### 4. Malformed Packet vs Protocol Error

MQTT 5.0 distinguishes *Malformed Packet* (cannot be parsed — bytes are wrong) from
*Protocol Error* (parses, but breaks a rule). These map to the existing
`CodecError::MalformedPacket` and `CodecError::ProtocolViolation`. Where the spec
says a server **responds** with a specific reason code before closing, that response
is the broker's job; the codec's contract is to surface the right error *category* so
the broker can pick the reason code. (A reason-code-bearing error variant may be
added if that mapping proves lossy; deferred until a consumer needs it.)

### 5. Phased implementation

Each phase is a tested, gated, committed unit:

1. **Properties foundation** — the `Property` enum, the `Properties` block codec,
   and the string-pair primitive, decoded/encoded in isolation. *(this commit)*
2. **CONNECT/CONNACK v5** — connect properties + will properties; connack reason +
   properties. Flip nothing yet.
3. **PUBLISH + acks v5** — publish properties; the four ack packets gain
   `reason` + properties with the short-form rules.
4. **SUBSCRIBE/SUBACK/UNSUBSCRIBE/UNSUBACK v5** — subscription options byte; reason
   codes + properties.
5. **DISCONNECT + AUTH v5** — reason + properties; the new AUTH packet.
6. **Accept v5 in the broker** — remove `V5_UNSUPPORTED`, negotiate at CONNECT, and
   honour the v5 behaviours the broker already has analogues for; the new v5
   *semantics* (expiry, aliases, shared subs, flow control) are **workstream G**, not
   the codec.

The codec milestone is phases 1–6: a faithful v5 wire that round-trips every packet.
It unblocks — but does not itself implement — workstream G.

## Consequences

- The fuzzing surface grows; property decoding (attacker-controlled identifiers,
  lengths, and nesting of length-prefixed values) is the new highest-risk path and is
  built bounds-checked and total from the start, like the rest of the codec.
- Carrying v5 fields on shared structs means v4 encode/decode must continue to ignore
  them; round-trip tests pin that a v4 packet is byte-identical before and after the
  v5 fields exist.
- Reason codes and properties as open/`Vec` types keep the broker free to handle
  unknown-but-valid input gracefully (forward-compatible with future spec points).
