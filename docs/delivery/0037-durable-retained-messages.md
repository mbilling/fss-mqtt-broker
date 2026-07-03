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
    status: done
    date: 2026-07-03
    evidence: "build_durable_node shares one Arc<GroupRoutedLog> between the session store and a new ReplicatedRetained, returned as Arc<dyn DurableRetained> (new object-safe trait in mqtt_storage::retained_log) and attached to the hub — one durable plane, two keyspaces. Hub::publish, after the unchanged live fan-out (deliver + shared + forward_to_peers), routes a retained mutation to its topic's group owner: owner-local commits off-loop via tokio::spawn (the quorum round-trip never stalls the hub actor); a peer-owned topic gets a targeted fire-and-forget PeerMessage::RetainedCommit (new variant, appended — bincode-safe) which the owner commits on receipt. Only the LANDING node routes (RemotePublish enters via deliver, never publish), so one publish is exactly one authority write. Zero-length payload commits the versioned tombstone. Unreachable owner / failed commit warns loudly (repair paths are P5/P6; queue is P6). Durable off = handle unset = ADR 0014 path byte-for-byte (§6 caveat). Tests: hub-level — owner-local retained publish commits with token (0,1) then tombstone (0,2) while the subscriber's live delivery is unchanged; a foreign topic routes RetainedCommit to exactly the owner peer after the ADR 0014 broadcast, with NO local durable write; the owner commits a peer's RetainedCommit (value then clear); durable-off never emits RetainedCommit while the broadcast still flows. Assembly-level — single_node_durable_store_bootstraps_and_serves extended: a retained commit through the returned handle lands under the real consensus-minted lease epoch (>= 1) and reads back with its token; the persistent-restart test now also proves the retained handle's shared log drops cleanly for lock release. Wire roundtrips for RetainedCommit (value + clear). The durable_sessions cluster harness wires attach_durable_retained exactly as production main.rs does."
  - id: 0037-P4
    title: Commit fan-out — post-commit broadcast carries (epoch, offset); node caches apply monotonically per topic (idempotent, order-insensitive)
    status: done
    date: 2026-07-03
    evidence: "New PeerMessage::RetainedUpdate (topic, payload, qos, epoch, offset): after the owner's off-loop commit resolves, HubCommand::RetainedCommitted posts back to the loop, which warms the LOCAL cache and fans the tokened value to every peer; each node's apply_retained_update applies iff the token exceeds the per-topic token it holds (hub retained_tokens map) — monotonic, idempotent, order-insensitive; an empty payload drops the topic from the cache but its tombstone token still fences staler values (no resurrection). Under durable the raw ADR 0014 broadcast STOPS warming caches (deliver skips the cache write; forward_to_peers no longer forces retain to all peers — interest-only): applying raw untokened values is exactly the everyday-race divergence. Subscribe replay stays a local cache read. App-props caveat: fan-out matches the ADR wire shape, so retained replay on OTHER nodes carries no MQTT 5 app properties (parity with today's snapshot back-fill; live delivery unaffected). Exposed + fixed a latent durable-plane bug: ReplicaState's fence was GLOBAL per node while lease epochs are minted per group from one globally-monotonic counter — the first workload replicating two groups through one shared follower (this phase's race test) had the highest-epoch group permanently fence out every other group's current lease-holder (NoQuorum). Fences are now per placement group, in memory and persisted (fence/<group> rows); the key→group derivation is shared (placement::group_of_key) so router and fence cannot disagree. Tests: monotonic/stale/duplicate/epoch-outranks-offset cache application; tombstone fencing (no zombie value); fan-out carries the commit token and the owner's own cache warms (late subscriber replays); raw RemotePublish{retain} live-delivers but leaves the cache cold under durable; durable-off byte-for-byte (existing suite); per-group fence regression test (a_groups_fence_does_not_reject_another_groups_older_epoch) + fence persistence per group across reopen; wire roundtrips. Integration (the everyday race, 10x stable): concurrent different-value retained publishes on two real durable nodes (SWIM + leases + peer mesh) converge cluster-wide to one owner-committed racing value with retained_divergence_total 0 on every node."
  - id: 0037-P5
    title: Offset-aware back-fill — digest entries carry the token; higher (epoch, offset) wins per topic on link-up (replaces gap-fill-only), chunking (0014-T8) retained
    status: done
    date: 2026-07-03
    evidence: "RetainedSnapshot entries gain the token: (topic, payload, qos, epoch, offset) — the token rides the SNAPSHOT entries (the digest aggregate stays value-based: equal values need no pull regardless of tokens). apply_retained_snapshot under durable applies each entry through the same monotonic gate as the commit fan-out (strictly-higher token wins; retained_tokens updated), so divergent caches converge deterministically on link-up; the P1 divergence count/warn still fires and the warn now reports resolution. A committed CLEAR back-fills as an empty-payload tombstone entry carrying its token — send_retained_snapshot emits one for every token held with no cached value, and send_retained_digest no longer goes silent when the cache is empty but tombstones exist (a stale peer value must see a digest difference to pull the clear). Untokened (0,0) entries (durable-off / pre-migration caches) gap-fill absent topics but never overwrite; a durable-off receiver keeps ADR 0014 gap-fill verbatim (P1 detection tests unchanged and green). Chunking (0014-T8) retained: per-entry overhead bumped for the two token halves; oversized-skip and chunk-budget tests updated and green. Tests: back_fill_takes_the_higher_token_value_per_topic (higher wins, staler rejected), a_committed_clear_back_fills_as_a_tombstone_and_fences (clear applies; zombie value cannot resurrect), the_snapshot_carries_tokens_and_tombstone_entries (outgoing snapshot: value with its commit token + tombstone entry with the clear's token), an_untokened_snapshot_entry_gap_fills_but_never_overwrites (and a committed token beats the uncommitted value), a_tombstone_only_node_still_offers_its_digest; wire roundtrips incl. a committed clear."
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
| 0037-P3 | ✅ done | 2026-07-03 | "build_durable_node shares one Arc<GroupRoutedLog> between the session store and a new ReplicatedRetained, returned as Arc<dyn DurableRetained> (new object-safe trait in mqtt_storage::retained_log) and attached to the hub — one durable plane, two keyspaces. Hub::publish, after the unchanged live fan-out (deliver + shared + forward_to_peers), routes a retained mutation to its topic's group owner: owner-local commits off-loop via tokio::spawn (the quorum round-trip never stalls the hub actor); a peer-owned topic gets a targeted fire-and-forget PeerMessage::RetainedCommit (new variant, appended — bincode-safe) which the owner commits on receipt. Only the LANDING node routes (RemotePublish enters via deliver, never publish), so one publish is exactly one authority write. Zero-length payload commits the versioned tombstone. Unreachable owner / failed commit warns loudly (repair paths are P5/P6; queue is P6). Durable off = handle unset = ADR 0014 path byte-for-byte (§6 caveat). Tests: hub-level — owner-local retained publish commits with token (0,1) then tombstone (0,2) while the subscriber's live delivery is unchanged; a foreign topic routes RetainedCommit to exactly the owner peer after the ADR 0014 broadcast, with NO local durable write; the owner commits a peer's RetainedCommit (value then clear); durable-off never emits RetainedCommit while the broadcast still flows. Assembly-level — single_node_durable_store_bootstraps_and_serves extended: a retained commit through the returned handle lands under the real consensus-minted lease epoch (>= 1) and reads back with its token; the persistent-restart test now also proves the retained handle's shared log drops cleanly for lock release. Wire roundtrips for RetainedCommit (value + clear). The durable_sessions cluster harness wires attach_durable_retained exactly as production main.rs does." |
| 0037-P4 | ✅ done | 2026-07-03 | "New PeerMessage::RetainedUpdate (topic, payload, qos, epoch, offset): after the owner's off-loop commit resolves, HubCommand::RetainedCommitted posts back to the loop, which warms the LOCAL cache and fans the tokened value to every peer; each node's apply_retained_update applies iff the token exceeds the per-topic token it holds (hub retained_tokens map) — monotonic, idempotent, order-insensitive; an empty payload drops the topic from the cache but its tombstone token still fences staler values (no resurrection). Under durable the raw ADR 0014 broadcast STOPS warming caches (deliver skips the cache write; forward_to_peers no longer forces retain to all peers — interest-only): applying raw untokened values is exactly the everyday-race divergence. Subscribe replay stays a local cache read. App-props caveat: fan-out matches the ADR wire shape, so retained replay on OTHER nodes carries no MQTT 5 app properties (parity with today's snapshot back-fill; live delivery unaffected). Exposed + fixed a latent durable-plane bug: ReplicaState's fence was GLOBAL per node while lease epochs are minted per group from one globally-monotonic counter — the first workload replicating two groups through one shared follower (this phase's race test) had the highest-epoch group permanently fence out every other group's current lease-holder (NoQuorum). Fences are now per placement group, in memory and persisted (fence/<group> rows); the key→group derivation is shared (placement::group_of_key) so router and fence cannot disagree. Tests: monotonic/stale/duplicate/epoch-outranks-offset cache application; tombstone fencing (no zombie value); fan-out carries the commit token and the owner's own cache warms (late subscriber replays); raw RemotePublish{retain} live-delivers but leaves the cache cold under durable; durable-off byte-for-byte (existing suite); per-group fence regression test (a_groups_fence_does_not_reject_another_groups_older_epoch) + fence persistence per group across reopen; wire roundtrips. Integration (the everyday race, 10x stable): concurrent different-value retained publishes on two real durable nodes (SWIM + leases + peer mesh) converge cluster-wide to one owner-committed racing value with retained_divergence_total 0 on every node." |
| 0037-P5 | ✅ done | 2026-07-03 | "RetainedSnapshot entries gain the token: (topic, payload, qos, epoch, offset) — the token rides the SNAPSHOT entries (the digest aggregate stays value-based: equal values need no pull regardless of tokens). apply_retained_snapshot under durable applies each entry through the same monotonic gate as the commit fan-out (strictly-higher token wins; retained_tokens updated), so divergent caches converge deterministically on link-up; the P1 divergence count/warn still fires and the warn now reports resolution. A committed CLEAR back-fills as an empty-payload tombstone entry carrying its token — send_retained_snapshot emits one for every token held with no cached value, and send_retained_digest no longer goes silent when the cache is empty but tombstones exist (a stale peer value must see a digest difference to pull the clear). Untokened (0,0) entries (durable-off / pre-migration caches) gap-fill absent topics but never overwrite; a durable-off receiver keeps ADR 0014 gap-fill verbatim (P1 detection tests unchanged and green). Chunking (0014-T8) retained: per-entry overhead bumped for the two token halves; oversized-skip and chunk-budget tests updated and green. Tests: back_fill_takes_the_higher_token_value_per_topic (higher wins, staler rejected), a_committed_clear_back_fills_as_a_tombstone_and_fences (clear applies; zombie value cannot resurrect), the_snapshot_carries_tokens_and_tombstone_entries (outgoing snapshot: value with its commit token + tombstone entry with the clear's token), an_untokened_snapshot_entry_gap_fills_but_never_overwrites (and a committed token beats the uncommitted value), a_tombstone_only_node_still_offers_its_digest; wire roundtrips incl. a committed clear." |
| 0037-P6 | ⬜ planned | — |  |
| 0037-P7 | ⬜ planned | — |  |
<!-- /status-table:0037 -->

## Changelog

- **2026-07-03** — P5 (token-aware back-fill) landed: link-up snapshots now carry each
  entry's `(epoch, offset)` token and the receiver applies them through the same
  monotonic gate as the commit fan-out — divergent caches converge deterministically to
  the committed value on link-up, replacing gap-fill-only. Committed **clears** ride the
  snapshot as tombstone entries (empty payload + the clear's token), and a
  tombstone-only node still offers its digest — closing the "peer that missed the clear
  keeps the value forever" gap. Untokened entries only gap-fill; durable-off keeps
  ADR 0014 verbatim; chunking (0014-T8) unchanged. The P1 divergence meter still counts
  every resolved difference, so it remains the honest migration meter. Remaining for
  the 0014-T7 close-out: P6's bounded queue-until-heal and the partition
  heal-convergence integration test.
- **2026-07-03** — P4 (commit fan-out) landed: caches are now warmed **exclusively by
  committed, tokened values**. The owner fans every commit out as `RetainedUpdate`
  carrying `(epoch, offset)`; each node applies it only above its held per-topic token
  (monotonic, idempotent, order-insensitive; a committed clear's token fences staler
  values from resurrecting the topic), and the raw ADR 0014 broadcast no longer touches
  caches under durable. The everyday race now provably converges: concurrent
  different-value retained publishes on two real durable nodes end with one committed
  value cluster-wide and the divergence meter at zero. Landing this **exposed and fixed
  a latent durable-plane bug**: the replica fence was global per node, but lease epochs
  are per-group terms from one shared counter — so the first two-groups-through-one-
  follower workload had the newest group permanently fence out every other group's
  writes (`NoQuorum`). Fences are now per placement group (in memory and on disk), with
  the key→group derivation shared between router and fence. Known caveat carried
  forward: cross-node retained replay does not carry MQTT 5 app properties (parity with
  the existing snapshot back-fill); P5's token back-fill is the natural place to close
  it if wanted.
- **2026-07-03** — P3 (owner write path) landed: every locally-originated retained
  mutation now also commits into the durable retained keyspace, routed to the topic's
  group lease-owner — locally (off-loop, so the hub actor never waits on the quorum
  round-trip) when this node owns the group, or via a targeted `RetainedCommit` peer
  frame to the owner otherwise. Live delivery and the ADR 0014 broadcast are unchanged
  and undelayed; only the landing node routes, so one publish is one authority write.
  Durable-off keeps the ADR 0014 path untouched (the §6 caveat). What P3 deliberately
  does **not** yet do: node caches are still warmed by the raw broadcast (P4 makes them
  token-monotonic from commit fan-out), back-fill is still gap-fill (P5), and an
  unreachable owner means a loudly-logged skipped commit rather than a queued one (P6).
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
