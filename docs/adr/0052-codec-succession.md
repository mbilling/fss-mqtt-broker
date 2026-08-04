# ADR 0052 — Codec succession: postcard replaces bincode on every cluster surface

- **Status:** Accepted
- **Date:** 2026-08-04
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0052-codec-succession.md](../delivery/0052-codec-succession.md) — plan, progress, and changelog
- **Related:** [ADR 0038](0038-prerelease-compatibility-freeze.md) (the frozen bootstrap
  frames and schema gates this record works within), [ADR 0039](0039-versioning-and-upgrade-policy.md)
  (the pre-1.0 in-place-reshape rule this record exercises for the last time),
  [ADR 0003](0003-gossip-authentication.md) (the gossip datagrams whose payload codec moves),
  [ADR 0006](0006-consensus-and-replication.md) (the raft RPCs and lease store whose codec moves),
  [ADR 0041](0041-resource-governance.md) (the admission-cap posture the new decode limits extend),
  [ADR 0044](0044-release-readiness-assurance.md) (the fuzz harnesses that keep watching the
  new decoders)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0052-codec-succession.md).

## Context

Every cluster-plane byte — peer frames on the mTLS bus, SWIM gossip datagram payloads,
raft RPCs nested in peer frames, the persisted raft log/snapshot, and the application-
properties blob embedded in retained records — was encoded with `bincode` 1.3.3. That
crate is archived upstream (RUSTSEC-2025-0141: the team ceased development and declared
1.3.3 complete), and our `deny.toml` carried a standing ignore for it.

An unmaintained deserializer decoding attacker-reachable bytes is a supply-chain gap, not
a style problem. The SWIM decode runs post-HMAC but is fully pre-authentication when the
gossip plane runs unauthenticated; the peer bus is mTLS-gated but treats peers as
possibly-hostile (its decoder is fuzzed for exactly that reason). The security posture of
this project (SECURITY.md) assumes an attacker — human or model — with perfect knowledge
of the code; a known-unmaintained parser on those surfaces is a gap such an attacker sees
as clearly as we do. Additionally, none of the bincode decode paths imposed a byte limit
of their own — only the outer frame/datagram caps bounded them.

## Decision

1. **`postcard` succeeds `bincode` everywhere bincode was.** Chosen over `bincode` 2.x
   (same organizational maintenance question that produced the advisory; serde-compat is
   its secondary path) and `rmp-serde` (no decisive advantage): postcard is actively
   maintained, has a published stable wire specification, its encoding is canonical (no
   configuration knobs — which keeps the `AppProps` digest fold deterministic), and its
   decoder borrows from the input slice, so a decode can never allocate beyond the bytes
   the caps already admitted.

2. **The two ADR 0038 frozen bootstrap frames do not move.** `Hello` and `ProxyHello`
   are decoded before any protocol version is negotiated; their bytes are pinned forever.
   They are now produced and parsed by a hand-rolled ~100-line codec in
   `mqtt_cluster::peer::frozen` that reproduces the pinned layout exactly (the golden-bytes
   test passes unmodified — that test, not any library, is the contract). The frame
   decoder dispatches on the first body byte: frozen tags 0/8, postcard varints otherwise;
   the encoder never emits variants 0/8 through postcard, so the spaces are disjoint.
   A 0.9.0 node and a post-0052 node therefore still exchange mutually-readable Hellos
   and reject each other politely at proto negotiation.

3. **Peer-bus proto 6, floor raised with the ceiling** (`PROTO_MIN = PROTO_MAX = 6`) —
   the ADR 0039 pre-1.0 in-place reshape, legal exactly because no release owes
   compatibility. Post-1.0 the same change would be a MAJOR release.

4. **Store schema stamps reset to 1** (maintainer decision, 2026-08-04): `retained.redb`
   2→1, `replicas.redb` 3→1, `lease.redb` stays 1. Nothing is released, so the
   pre-release bump history was retired rather than extended; v1 on each store now means
   "the postcard layout" and the incremental history restarts at 1.0.0. A store stamped
   with a retired version fails closed at the ADR 0038 T2 gate. (`lease.redb` keeps its
   stamp but changes bytes — a pre-0052 dev store fails at value decode, not the gate;
   dev data dirs must be wiped, and no upgrade path is owed.)

5. **Explicit decode limits, both directions** (extending ADR 0041's cap posture to the
   cluster plane): every decode is strict (exactly one message, no trailing bytes, fail
   closed), and raft RPC payloads get their own `MAX_RAFT_RPC = 4 MiB` bound — checked by
   the receiver before decoding, by the sender before wrapping (an oversized send fails at
   the sender instead of severing the link at the receiver), and on InstallSnapshot bytes
   before they are decoded or persisted.

6. **The `deny.toml` ignore for RUSTSEC-2025-0141 is removed.** The advisories gate runs
   with zero exceptions again.

## Consequences

- **No rolling upgrade from v0.9.0.** A 0.9.0 node and a post-0052 node refuse each
  other's links (clean "incompatible peer-bus protocol range" rejection) and their SWIM
  datagrams do not interoperate. The documented path is wipe-and-rejoin / full restart.
  The next release's notes must say so prominently.
- `crates/mqttd/tests/cluster_upgrade.rs` `BASELINE_REF` must be bumped to this change's
  merge commit in an immediate follow-up; the nightly upgrade test is expectedly red in
  between.
- The peer-codec benches re-baseline (`docs/benchmarks/BASELINE.md`); postcard varints
  shrink most frames, and `ENTRY_OVERHEAD` chunk estimates become conservative.
- The committed fuzz seeds for `peer_decode`/`swim_message` gain postcard-encoded
  siblings; the old bincode-shaped seeds are kept as adversarial corpus entries.
- Hand-rolling the frozen frames adds ~100 lines of parser we own — bounds-checked,
  full-consumption, covered by the golden test, new adversarial tests, and the existing
  `peer_decode` fuzz target.
