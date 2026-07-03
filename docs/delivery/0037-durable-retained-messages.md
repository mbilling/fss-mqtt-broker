---
adr: "0037"
title: Durable single-owner retained messages (clock-free convergence)
adr_status: Proposed
tasks:
  - id: 0037-P1
    title: Divergence detection — value hash in the retained digest; warn + retained_divergence_total metric
    status: done
    date: 2026-07-03
    evidence: "PeerMessage::RetainedDigest gains value_hash (XOR of a stable 64-bit hash per (topic, payload, qos); the topic is length-prefixed against boundary collisions). handle_retained_digest pulls when EITHER the topic-set hash or the value hash differs — the value-only case is exactly what the topics-only digest was blind to. apply_retained_snapshot compares values for topics both sides hold: a difference increments retained_divergence_total (Prometheus + OTLP) and warns once per chunk (per-topic detail at debug — bounded logging); RemoteRetainedSnapshot gained the source node for attribution. Storage unchanged: gap-fill still keeps the local value (detection only, per the ADR's migration sequencing). Tests: a_value_only_digest_difference_triggers_a_pull, a_divergent_retained_value_is_detected_and_counted (metric increments exactly once for one divergent topic; agreeing and gap-filled topics do not count; local value proven kept via subscriber replay), digest order-independence extended to value hashes; codec roundtrip updated."
  - id: 0037-P2
    title: Retained keyspace in the group log — r/<topic> set/clear ops, last-value compaction, versioned tombstones; quorum/fencing/takeover/restart unit tests on the pure cores
    status: done
    date: 2026-07-03
    evidence: "ReplicatedLog gains epoch_for (default 0 for single-node backends; GroupRoutedLog answers with the routed ClusterLog's lease epoch, ClusterLog with its own) — the epoch half of the token, read from the same route the append commits through, so it can only understate (benign: offsets strictly increase per key regardless). New mqtt_storage::retained_log::ReplicatedRetained over any ReplicatedLog<Key=String>: key r/<topic> (2-byte prefix so the group router's placement-key recovery yields the topic — the ADR's illustrative ret/ would mis-route), set/clear append [epoch|qos|tombstone|payload] then compact via truncate(offset-1) — exactly one live record per topic; a clear is a versioned tombstone that wins/loses by token, never by absence; get returns the value with its (epoch, offset) token; decode fails closed on malformed records. Unit tests: codec roundtrip incl. malformed, token on first set, compaction collapses live_range, tombstone versioning, topic key independence, strictly-increasing tokens across sets and clears, restart recovery from the persisted log (ADR 0018: value + token + offset high-water recovered; no offset reuse). Cluster tests over the real GroupRoutedLog/ClusterLog/ReplicaState plumbing: a retained set quorum-commits with token (lease-epoch, offset) and both followers hold exactly the compacted record; a foreign topic is refused NotOwner (never a divergent local write); a stale-epoch owner is fenced with NoQuorum; a takeover (epoch bump) re-recovers the value with its original token from the replica set and the next write's token is strictly higher."
  - id: 0037-P3
    title: Write path — retained mutations route through the group lease-owner (live delivery unchanged); durable-off falls back to ADR 0014 behaviour
    status: planned
  - id: 0037-P4
    title: Commit fan-out — post-commit broadcast carries (epoch, offset); node caches apply monotonically per topic (idempotent, order-insensitive)
    status: planned
  - id: 0037-P5
    title: Offset-aware back-fill — digest entries carry the token; higher (epoch, offset) wins per topic on link-up (replaces gap-fill-only), chunking (0014-T8) retained
    status: planned
  - id: 0037-P6
    title: Partition semantics — bounded queue-until-heal for minority-side retained writes, loud drop counter at the bound; heal-convergence integration tests with divergent writes
    status: planned
  - id: 0037-P7
    title: Docs + closure — README/operator docs (CP trade, queue bound, durable-off caveat), ADR 0014 revision notes, close 0014-T7 on this evidence
    status: planned
---

# Delivery — ADR 0037: Durable single-owner retained messages

Decision: [docs/adr/0037-durable-retained-messages.md](../adr/0037-durable-retained-messages.md).

Retained-message conflicts are **prevented, not resolved**: every retained mutation commits
through its topic's placement-group lease-owner into the quorum-replicated group log, and
all cache/back-fill decisions reduce to a consensus-issued `(epoch, offset)` token — no
wall-clock anywhere. Detection lands first (P1) as the baseline and the after-proof; each
phase lands test-first and independently green.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0037-P1** Detect divergence | The link-up retained digest carries a value hash; peers holding different values for the same topic produce a `warn!` and increment `retained_divergence_total`. No behavioural change to what is stored. Lands independently of the migration and keeps working after it (post-migration the counter staying at zero is the convergence proof). |
| **0037-P2** Log keyspace | The group log supports `r/<topic>` set/clear ops with last-value compaction (the ADR's illustrative `ret/` prefix is 2 bytes in practice, matching the router's placement-key recovery); a zero-length clear is a **versioned tombstone**. Pure-core unit tests: quorum commit, stale-epoch fencing, owner-takeover recovery of the retained high-water, restart recovery from the persisted log (ADR 0018). |
| **0037-P3** Owner write path | A retained publish landing on any node routes its retained mutation to the group lease-owner (live delivery to subscribers unchanged and undelayed); with durable off, retained behaves exactly as ADR 0014 today (documented caveat). |
| **0037-P4** Commit fan-out | After commit, the owner broadcasts `(topic, value, epoch, offset)`; each node's local retained cache applies it only when the token exceeds the held one — monotonic per topic, idempotent, order-insensitive. Subscribe-time replay stays a local read. Integration: **concurrent different-value retained publishes to the same topic on two nodes (no partition — the everyday race) converge cluster-wide** to the owner-committed value, with `retained_divergence_total` staying at zero. |
| **0037-P5** Token back-fill | Digest entries carry the token; on link-up the receiver pulls and takes the higher-token value per topic (gap-fill-only rule replaced). Two nodes holding divergent values converge deterministically to the committed one; chunked snapshots (0014-T8) unchanged. |
| **0037-P6** Partition semantics | On the quorum-less side a retained mutation queues (bounded per node); the bound drops oldest **loudly** (counter). On heal the queue submits to the owner in order and commits. Integration: divergent retained writes across a partition converge after heal on **every** node; the 0014-T7 scenario closes. |
| **0037-P7** Docs + closure | README/operator docs state the CP trade (minority staleness, never divergence), the queue bound, and the durable-off fallback; ADR 0014 gains revision notes; 0014-T7 closes citing this delivery. |

## Progress

<!-- status-table:0037 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0037-P1 | ✅ done | 2026-07-03 | "PeerMessage::RetainedDigest gains value_hash (XOR of a stable 64-bit hash per (topic, payload, qos); the topic is length-prefixed against boundary collisions). handle_retained_digest pulls when EITHER the topic-set hash or the value hash differs — the value-only case is exactly what the topics-only digest was blind to. apply_retained_snapshot compares values for topics both sides hold: a difference increments retained_divergence_total (Prometheus + OTLP) and warns once per chunk (per-topic detail at debug — bounded logging); RemoteRetainedSnapshot gained the source node for attribution. Storage unchanged: gap-fill still keeps the local value (detection only, per the ADR's migration sequencing). Tests: a_value_only_digest_difference_triggers_a_pull, a_divergent_retained_value_is_detected_and_counted (metric increments exactly once for one divergent topic; agreeing and gap-filled topics do not count; local value proven kept via subscriber replay), digest order-independence extended to value hashes; codec roundtrip updated." |
| 0037-P2 | ✅ done | 2026-07-03 | "ReplicatedLog gains epoch_for (default 0 for single-node backends; GroupRoutedLog answers with the routed ClusterLog's lease epoch, ClusterLog with its own) — the epoch half of the token, read from the same route the append commits through, so it can only understate (benign: offsets strictly increase per key regardless). New mqtt_storage::retained_log::ReplicatedRetained over any ReplicatedLog<Key=String>: key r/<topic> (2-byte prefix so the group router's placement-key recovery yields the topic — the ADR's illustrative ret/ would mis-route), set/clear append [epoch|qos|tombstone|payload] then compact via truncate(offset-1) — exactly one live record per topic; a clear is a versioned tombstone that wins/loses by token, never by absence; get returns the value with its (epoch, offset) token; decode fails closed on malformed records. Unit tests: codec roundtrip incl. malformed, token on first set, compaction collapses live_range, tombstone versioning, topic key independence, strictly-increasing tokens across sets and clears, restart recovery from the persisted log (ADR 0018: value + token + offset high-water recovered; no offset reuse). Cluster tests over the real GroupRoutedLog/ClusterLog/ReplicaState plumbing: a retained set quorum-commits with token (lease-epoch, offset) and both followers hold exactly the compacted record; a foreign topic is refused NotOwner (never a divergent local write); a stale-epoch owner is fenced with NoQuorum; a takeover (epoch bump) re-recovers the value with its original token from the replica set and the next write's token is strictly higher." |
| 0037-P3 | ⬜ planned | — |  |
| 0037-P4 | ⬜ planned | — |  |
| 0037-P5 | ⬜ planned | — |  |
| 0037-P6 | ⬜ planned | — |  |
| 0037-P7 | ⬜ planned | — |  |
<!-- /status-table:0037 -->

## Changelog

- **2026-07-03** — P2 (retained keyspace in the group log) landed: retained state is now
  a first-class durable keyspace — `r/<topic>` (2-byte prefix so the group router's
  placement-key recovery yields the topic) holding exactly one live record per topic via
  append-then-compact, with clears as versioned tombstones. `ReplicatedLog` gained
  `epoch_for`, so every write returns its clock-free `(epoch, offset)` convergence token.
  Proven on the real cluster plumbing: quorum commit replicating the compacted record to
  followers, `NotOwner` refusal for foreign topics (the conflict-prevention invariant),
  stale-epoch fencing (`NoQuorum` for a superseded owner), takeover re-recovery of the
  value **with its original token** and strictly-higher tokens afterwards, and restart
  recovery from the persisted log (ADR 0018). Nothing routes through this keyspace yet —
  the broker's write path moves onto it in P3.
- **2026-07-03** — P1 (divergence detection) landed: the retained digest carries a value
  hash, a value-only difference now triggers the pull the topics-only digest missed, and a
  peer value differing from ours on a shared topic increments `retained_divergence_total`
  and warns (once per chunk) — with storage behaviour deliberately unchanged (gap-fill
  keeps the local value). This is the baseline meter: it quantifies real-world divergence
  before the migration and must read zero after P2–P6 land.
- **2026-07-03** — ADR proposed and delivery opened, resolving the 0014-T7 question
  (partition-heal retained divergence) by **prevention**: single-owner writes through the
  existing lease/group-log machinery with clock-free `(epoch, offset)` convergence tokens,
  local caches warmed by commit fan-out, offset-aware back-fill, and bounded
  queue-until-heal partition semantics. LWW/HLC resolution rejected (clocks in the trust
  base; silently dropped acked writes). Detection (P1) sequenced first as baseline and
  after-proof. Revises ADR 0014's earlier "rejected as the default" verdict on
  durable-plane retained with the post-T6/T7 analysis.
