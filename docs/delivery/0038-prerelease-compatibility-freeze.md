---
adr: "0038"
title: Pre-release compatibility freeze (versioned wire, stamped schemas, final codecs)
adr_status: Accepted
tasks:
  - id: 0038-T1
    title: Peer-bus version negotiation — Hello carries proto_min..proto_max, disjoint ranges reject the link loudly; Hello/ProxyHello encodings frozen
    status: done
    date: 2026-07-04
    evidence: "PeerMessage::Hello gains proto_min/proto_max (PROTO_MIN = PROTO_MAX = 1); pub fn negotiate_proto picks the newest version both ranges can speak (min of maxes, valid only if >= both mins) or None for disjoint ranges. Both handshake sides enforce it in mqttd::peer::handle via proto_compatible: the ACCEPT side rejects BEFORE announcing itself (an incompatible build gets a clean close, not half a handshake); the DIAL side rejects on the reply. Rejection is loud (warn with both ranges) and fail-closed — the link never registers with the hub. Hello and ProxyHello are documented as FROZEN frames (the bootstrap any two future builds must exchange to discover disagreement); all other frames evolve behind a proto_max bump, with the negotiated link version defined as min(proto_max_a, proto_max_b). Tests: negotiate_proto unit matrix (identical, overlapping, touching, disjoint both directions, own-constants sanity); integration over a real TCP listener (an_incompatible_peer_protocol_range_is_rejected_at_hello): a doctored Hello announcing 99..99 gets no Hello reply and a clean close, while the same handshake at the build's own range completes; wire roundtrip updated; whole workspace green (756 tests) proving same-version links are unaffected."
  - id: 0038-T2
    title: Schema-version stamps — shared redb schema gate (stamp fresh, pass equal, fail closed on mismatch) adopted by sessions/replicas/lease/retained stores
    status: done
    date: 2026-07-04
    evidence: "New mqtt_storage::schema module: gate(db, store, expected) reads the schema_meta marker table — a fresh file is stamped with the current version, a matching stamp passes, ANY other version returns SchemaError::Mismatch naming found-vs-expected plus the pre-1.0 recovery (wipe and rejoin; the durable plane rebuilds replicated state from peers). force_version is the test/recovery tool for simulating a foreign build's file. All four stores adopt it immediately after Database::create with a per-store version const (all v1): sessions.redb (PersistentLog), retained.redb (PersistentRetainedStore), replicas.redb (ReplicaState — v1 documented as including the ADR 0037 per-group fence rows), lease.redb (LeaseStore). Tests: gate module fresh-stamp/reopen/idempotence + mismatch naming both versions; a fail-closed foreign-version test per store (open, doctor to v999, reopen refuses mentioning v999 and expects v1). Every existing restart/reopen test doubles as the pass-on-equal proof; workspace green (762 tests)."
  - id: 0038-T3
    title: Retained MQTT 5 fidelity — app properties through the durable record codec, RetainedCommit/Update/Snapshot frames, and the persistent retained store
    status: done
    date: 2026-07-05
    evidence: "New mqtt_storage::app_props::AppProps: the serde-able stored/wire form of the forwardable MQTT 5 properties (payload-format indicator, Content Type, Response Topic, Correlation Data, User Properties in order), with a canonical byte encoding embedded in the durable retained record codec and folded into the retained digests so property-only changes are divergence-visible. Carried end to end: durable retained records, the persistent retained store (retained.redb), and the peer-bus retained frames (RetainedCommit/Update/Snapshot via WireAppProps / RetainedWireEntry) all round-trip the properties; hub replay paths reconstruct the full mqtt_core::AppProperties on delivery. Schema versions bumped to v2 for sessions.redb and replicas.redb (row bytes changed meaning) — a v1 file fails closed at the T2 gate, with the fail-closed tests asserting against the schema constants so they track future bumps. Tests: AppProps codec roundtrip + fail-closed decode and lossless core-type conversion (unit); record-codec roundtrip with properties; digest property-sensitivity; persistent-store restart round-trip replays properties exactly (retained_survives_reopen_and_clear_persists); and the end-to-end acceptance test retained_mqtt5_properties_replay_from_any_nodes_cache — over real severable TCP peer links, a v5 publish with the full property set lands on the NON-owner (properties ride owner-routed submit + committed record + commit fan-out), fresh v5 subscribers on both nodes replay payload and every property intact, then a severed-and-healed update proves queue-heal + token back-fill carry changed properties too (10/10 repeat runs green). Workspace green (765 tests), clippy zero warnings."
  - id: 0038-T4
    title: Wire-shape finalization — named serde structs for multi-field entries; frozen-vs-versioned frame inventory recorded
    status: done
    date: 2026-07-05
    evidence: "The last two positional wire shapes become named serde structs (bincode encodes struct fields positionally, so the bytes are unchanged): SharedGroupsWire's nested tuples are now SharedGroupWire { group, filter, members: Vec<SharedMemberWire { client, qos, online }> }, and ReplicaReadReply's (offset, record) pairs are now ReplicaEntryWire { offset, record } — joining T3's RetainedWireEntry, so every multi-field peer-bus entry is named and field additions are reviewable. New golden-bytes test (the_frozen_frames_encode_byte_for_byte_stably) pins the FROZEN Hello and ProxyHello encodings byte for byte — including their bincode variant indices (0 and 8), which also enforces the append-only rule for new frames: a failing golden test is a cross-version wire break, not a test to update. The delivery doc records the full 18-frame inventory (frozen: Hello, ProxyHello; versioned: all others, all proto 1) with the variant-index append-only rule. Workspace green, clippy zero warnings. ADR 0038 closes: Proposed -> Accepted."
---

# Delivery — ADR 0038: Pre-release compatibility freeze

Decision: [docs/adr/0038-prerelease-compatibility-freeze.md](../adr/0038-prerelease-compatibility-freeze.md).

The last free window: no deployments exist, so wire frames and disk schemas can still
change in place. This delivery adds the machinery that makes post-release evolution
possible (version negotiation, schema markers) and pays the remaining codec debt
(retained app-properties) before the formats freeze at the first release.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0038-T1** Version negotiation | `Hello` carries `proto_min..proto_max`; a link whose ranges are disjoint is rejected before registration, loudly, on both accept and dial sides; same-version links are unaffected (whole suite green). `Hello`/`ProxyHello` are documented as frozen. |
| **0038-T2** Schema stamps | Every redb store (`sessions`, `replicas`, `lease`, `retained`) stamps a schema version on first open, passes on equal, and **refuses to open** on mismatch with an error naming found-vs-expected. Covered per store by fresh/reopen/mismatch tests. |
| **0038-T3** Retained fidelity | A retained publish with MQTT 5 properties (Content-Type, User Properties, …) replays with those properties from **any** node's cache — committed record, commit fan-out, and token back-fill all carry them; the persistent retained store round-trips them across restart. |
| **0038-T4** Wire shapes | Multi-field wire entries are named structs; the delivery doc records the frame inventory (frozen: `Hello`, `ProxyHello`; versioned: all others). |

## Frame inventory (0038-T4)

The peer bus speaks length-prefixed bincode frames of `PeerMessage`
(`crates/mqtt-cluster/src/peer.rs`). bincode encodes the enum **variant index**, so
new frames must be **appended** to the enum, never inserted or reordered — the golden
test `the_frozen_frames_encode_byte_for_byte_stably` pins the frozen frames (bytes
*and* indices) and fails on any violation.

Two postures, per the ADR:

- **Frozen** — read before any version is negotiated; their encodings can never
  change again, in any future version.
- **Versioned** — negotiated behind `Hello`'s `proto_min..proto_max` range
  (ADR 0039: minors bump `PROTO_MAX` additively; raising `PROTO_MIN` is a major).
  Field additions and new frames ship under a `PROTO_MAX` bump.

| # | Frame | Posture | Since | Carries |
|---|-------|---------|-------|---------|
| 0 | `Hello` | **frozen** | proto 1 | node id + spoken protocol range (ADR 0038) |
| 1 | `Interest` | versioned | proto 1 | full local-subscription filter snapshot |
| 2 | `Publish` | versioned | proto 1 | forwarded publish + expiry + `WireAppProps` (ADR 0030/0038 T3) |
| 3 | `SharedInterest` | versioned | proto 1 | `Vec<SharedGroupWire>` shared-group membership (ADR 0015 §2) |
| 4 | `RetainedSnapshot` | versioned | proto 1 | `Vec<RetainedWireEntry>` chunked back-fill with tokens (ADR 0014 §3, 0037 P5) |
| 5 | `RetainedDigest` | versioned | proto 1 | retained topic-set count/hash + value hash (0014-T6, 0037 P1) |
| 6 | `RetainedRequest` | versioned | proto 1 | pull request for the retained set |
| 7 | `SharedDeliver` | versioned | proto 1 | targeted shared-group delivery (ADR 0015 §1) |
| 8 | `ProxyHello` | **frozen** | proto 1 | session-proxy bootstrap: vouched identity + via (ADR 0005) |
| 9 | `Replicate` | versioned | proto 1 | session-log replication op, epoch-fenced (ADR 0006 §1) |
| 10 | `ReplicateAck` | versioned | proto 1 | replica's accept/reject verdict |
| 11 | `RaftRpc` | versioned | proto 1 | opaque serialized Raft RPC (lease consensus) |
| 12 | `RaftRpcReply` | versioned | proto 1 | opaque serialized Raft RPC response |
| 13 | `ReplicaRead` | versioned | proto 1 | takeover recovery-read request (workstream F) |
| 14 | `ReplicaReadReply` | versioned | proto 1 | watermark + `Vec<ReplicaEntryWire>` stored entries (ADR 0018 §3b) |
| 15 | `RetainedCommit` | versioned | proto 1 | owner-routed retained mutation, acked + dedup'd on `seq` (ADR 0037 §1, T8) |
| 16 | `RetainedUpdate` | versioned | proto 1 | committed retained value fan-out with token (ADR 0037 P4) |
| 17 | `RetainedCommitAck` | versioned | proto 1 | owner's dedup-idempotent commit ack (ADR 0037 T8) |

Named multi-field entry shapes: `RetainedWireEntry` (T3), `SharedGroupWire` /
`SharedMemberWire`, `ReplicaEntryWire`, and the shared `WireAppProps`
(`mqtt_storage::app_props::AppProps`, whose canonical byte encoding is frozen into
the durable record codec — ADR 0038 T3).

## Progress

<!-- status-table:0038 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0038-T1 | ✅ done | 2026-07-04 | "PeerMessage::Hello gains proto_min/proto_max (PROTO_MIN = PROTO_MAX = 1); pub fn negotiate_proto picks the newest version both ranges can speak (min of maxes, valid only if >= both mins) or None for disjoint ranges. Both handshake sides enforce it in mqttd::peer::handle via proto_compatible: the ACCEPT side rejects BEFORE announcing itself (an incompatible build gets a clean close, not half a handshake); the DIAL side rejects on the reply. Rejection is loud (warn with both ranges) and fail-closed — the link never registers with the hub. Hello and ProxyHello are documented as FROZEN frames (the bootstrap any two future builds must exchange to discover disagreement); all other frames evolve behind a proto_max bump, with the negotiated link version defined as min(proto_max_a, proto_max_b). Tests: negotiate_proto unit matrix (identical, overlapping, touching, disjoint both directions, own-constants sanity); integration over a real TCP listener (an_incompatible_peer_protocol_range_is_rejected_at_hello): a doctored Hello announcing 99..99 gets no Hello reply and a clean close, while the same handshake at the build's own range completes; wire roundtrip updated; whole workspace green (756 tests) proving same-version links are unaffected." |
| 0038-T2 | ✅ done | 2026-07-04 | "New mqtt_storage::schema module: gate(db, store, expected) reads the schema_meta marker table — a fresh file is stamped with the current version, a matching stamp passes, ANY other version returns SchemaError::Mismatch naming found-vs-expected plus the pre-1.0 recovery (wipe and rejoin; the durable plane rebuilds replicated state from peers). force_version is the test/recovery tool for simulating a foreign build's file. All four stores adopt it immediately after Database::create with a per-store version const (all v1): sessions.redb (PersistentLog), retained.redb (PersistentRetainedStore), replicas.redb (ReplicaState — v1 documented as including the ADR 0037 per-group fence rows), lease.redb (LeaseStore). Tests: gate module fresh-stamp/reopen/idempotence + mismatch naming both versions; a fail-closed foreign-version test per store (open, doctor to v999, reopen refuses mentioning v999 and expects v1). Every existing restart/reopen test doubles as the pass-on-equal proof; workspace green (762 tests)." |
| 0038-T3 | ✅ done | 2026-07-05 | "New mqtt_storage::app_props::AppProps: the serde-able stored/wire form of the forwardable MQTT 5 properties (payload-format indicator, Content Type, Response Topic, Correlation Data, User Properties in order), with a canonical byte encoding embedded in the durable retained record codec and folded into the retained digests so property-only changes are divergence-visible. Carried end to end: durable retained records, the persistent retained store (retained.redb), and the peer-bus retained frames (RetainedCommit/Update/Snapshot via WireAppProps / RetainedWireEntry) all round-trip the properties; hub replay paths reconstruct the full mqtt_core::AppProperties on delivery. Schema versions bumped to v2 for sessions.redb and replicas.redb (row bytes changed meaning) — a v1 file fails closed at the T2 gate, with the fail-closed tests asserting against the schema constants so they track future bumps. Tests: AppProps codec roundtrip + fail-closed decode and lossless core-type conversion (unit); record-codec roundtrip with properties; digest property-sensitivity; persistent-store restart round-trip replays properties exactly (retained_survives_reopen_and_clear_persists); and the end-to-end acceptance test retained_mqtt5_properties_replay_from_any_nodes_cache — over real severable TCP peer links, a v5 publish with the full property set lands on the NON-owner (properties ride owner-routed submit + committed record + commit fan-out), fresh v5 subscribers on both nodes replay payload and every property intact, then a severed-and-healed update proves queue-heal + token back-fill carry changed properties too (10/10 repeat runs green). Workspace green (765 tests), clippy zero warnings." |
| 0038-T4 | ✅ done | 2026-07-05 | "The last two positional wire shapes become named serde structs (bincode encodes struct fields positionally, so the bytes are unchanged): SharedGroupsWire's nested tuples are now SharedGroupWire { group, filter, members: Vec<SharedMemberWire { client, qos, online }> }, and ReplicaReadReply's (offset, record) pairs are now ReplicaEntryWire { offset, record } — joining T3's RetainedWireEntry, so every multi-field peer-bus entry is named and field additions are reviewable. New golden-bytes test (the_frozen_frames_encode_byte_for_byte_stably) pins the FROZEN Hello and ProxyHello encodings byte for byte — including their bincode variant indices (0 and 8), which also enforces the append-only rule for new frames: a failing golden test is a cross-version wire break, not a test to update. The delivery doc records the full 18-frame inventory (frozen: Hello, ProxyHello; versioned: all others, all proto 1) with the variant-index append-only rule. Workspace green, clippy zero warnings. ADR 0038 closes: Proposed -> Accepted." |
<!-- /status-table:0038 -->

## Changelog

- **2026-07-05** — T4 (wire-shape finalization) landed, **closing ADR 0038**
  (Proposed → Accepted): the last positional wire shapes are named structs
  (`SharedGroupWire`/`SharedMemberWire`, `ReplicaEntryWire` — bincode encodes struct
  fields positionally, so the bytes are unchanged), the 18-frame inventory above
  records every frame's posture, and a golden-bytes test pins the frozen
  `Hello`/`ProxyHello` encodings — variant indices included, so frame insertion or
  reordering (a silent cross-version wire break) now fails CI. The compatibility
  freeze is complete: wire and schema formats are release-ready.
- **2026-07-05** — T3 (retained MQTT 5 fidelity) landed: the forwardable application
  properties now survive every path a retained message can take — committed record,
  commit fan-out, token back-fill, persistent store, restart — closing the last known
  MQTT-3.3.2-17 gap before the record codec freezes. Sessions/replicas schemas bumped
  to v2 behind the T2 gate (the gate's first real use). Landing the acceptance test
  also surfaced and fixed a real durability bug: QoS ≥ 1 acks were released before the
  hub's fan-out had durably applied the publish (ADR 0018's contract); acks are now
  gated on hub completion.
- **2026-07-04** — T2 (schema stamps) landed: every persistent store now opens through
  a shared version gate — stamped when fresh, refused loudly when foreign — so a future
  build gets a version to dispatch migrations on instead of guessing at bytes, and a
  mixed-version data dir fails closed instead of silently misreading.
- **2026-07-04** — T1 (version negotiation) landed: the peer-bus handshake now
  announces and checks a protocol range, failing closed (loudly) on disjoint ranges —
  the machinery a rolling upgrade needs to *detect* divergence is in the field before
  the first release can create divergence. `Hello`/`ProxyHello` are frozen from here on.
- **2026-07-04** — ADR proposed and delivery opened. Part of the pre-release plan
  (areas: ① this freeze, ② the "revocation reaches live state" security bundle,
  ③ resource governance, ④ a durable-plane stress/simulation harness — each of ②–④
  gets its own ADR when its work starts). This one goes first because its window
  closes at the first release: everything here is an in-place edit today and a
  migration-plus-mixed-cluster problem afterwards.
