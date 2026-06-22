---
adr: "0006"
title: Consensus & replication for durable sessions
adr_status: Accepted
tasks:
  - id: 0006-P1
    title: Spike + ratify the engine (openraft chosen; engine-agnostic lease/fencing prototype)
    status: done
    date: 2026-06-15
    evidence: lease::overlapping_quorums_cannot_both_commit / superseded_holder_is_fenced; deny.toml cargo-deny gate
  - id: 0006-P2
    title: SessionStore over ReplicatedLog (ReplicatedSessionStore holds no durable state)
    status: done
    date: 2026-06-13
    evidence: logged::state_lives_in_the_log_not_the_store
  - id: 0006-P3a
    title: Epoch-fenced quorum-append core (ClusterLog + ReplicaState over a ReplicaTransport seam)
    status: done
    date: 2026-06-13
    evidence: cluster_log::append_is_quorum_durable_and_assigns_offsets / append_below_quorum_is_rejected_and_leaves_no_committed_hole / stale_leader_is_fenced
  - id: 0006-P3b-i
    title: Networked transport (PeerReplicaTransport over the peer mesh, req_id ack correlation, fail_node)
    status: done
    date: 2026-06-13
    evidence: repl_net::deliver_round_trips_and_applies_on_the_follower / stale_epoch_is_fenced_over_the_wire / fail_node_resolves_in_flight_requests
  - id: 0006-P3b-ii-1
    title: openraft lease state machine + type binding (LeaseMap, declare_raft_types!(LeaseConfig))
    status: done
    date: 2026-06-15
    evidence: lease_raft.rs LeaseMap + declare_raft_types!; assign-monotonic-epoch tests
  - id: 0006-P3b-ii-2
    title: openraft storage (LeaseStore RaftStorage over LeaseMap), passes openraft conformance Suite
    status: done
    date: 2026-06-15
    evidence: lease_store::passes_openraft_conformance_suite_in_memory
  - id: 0006-P3b-ii-3
    title: openraft network + in-memory bring-up (RaftNetwork; single + three-node group commit a lease)
    status: done
    date: 2026-06-15
    evidence: lease_group::single_node_group_elects_and_commits_a_lease / three_node_group_replicates_a_committed_lease
  - id: 0006-P3b-ii-4
    title: Mesh network (raft_mesh carries RaftRpc over the peer bus; leader elected + lease replicated over the wire)
    status: done
    date: 2026-06-15
    evidence: raft_mesh::two_nodes_elect_and_replicate_over_the_wire
  - id: 0006-P3c
    title: Replicated exactly-once state (QoS-2 dedup window + outbound packet-id counter in the replicated snapshot)
    status: done
    date: 2026-06-15
    evidence: logged::qos2_state_replicates_through_the_log; SessionStore record_received/clear_received/received/next_packet_id
  - id: 0006-P4
    title: Wire it in - swap MemorySessionStore for the durable backend; relocated-session owners write through it
    status: done
    date: 2026-06-22
    evidence: main.rs build_durable_node wiring (MQTTD_DATA_DIR); durable_sessions::a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover / enqueue_is_durable_across_a_three_node_cluster
  - id: 0006-P3c-i
    title: Replace in-memory backend O(n) cap count with a rebuildable per-key index
    status: deferred
    notes: correctness-neutral; in-memory backend's cap count reads the whole log (O(n)) per the 3c "remaining (minor)" note
---

# Delivery — ADR 0006: Consensus & replication for durable sessions

Decision: [docs/adr/0006-consensus-and-replication.md](../adr/0006-consensus-and-replication.md).

## Plan

The decision's "Phasing (workstream E)" (spike, SessionStore-over-log, the consensus-backed
log in sub-steps 3a/3b/3c, then wire-in) decomposes into these tasks. The engine selection
is ratified (openraft); realization tasks are marked done only where concrete code/tests
exist. Each carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0006-P1** Spike + engine | openraft is ratified against `deny.toml` (raft-rs rejected for an active DoS); the engine-agnostic ownership-lease/fencing prototype (`mqtt-cluster::lease`) pins split-brain safety (two epochs can never both reach quorum; a superseded holder is fenced). |
| **0006-P2** Store over log | `ReplicatedSessionStore` implements the full `SessionStore` over a `ReplicatedLog`, holding no durable state of its own; a second store over the same log sees the first's sessions in full. |
| **0006-P3a** Quorum-append core | `ClusterLog` implements `ReplicatedLog` by epoch-fenced quorum replication behind a `ReplicaTransport` seam; sans-I/O simulation pins quorum-durable append, single-replica-loss survival (R=3/q=2), below-quorum rejection with no committed hole, stale-leader fencing, and lazy local truncation. |
| **0006-P3b-i** Networked transport | `PeerReplicaTransport` realizes the `ReplicaTransport` seam over the peer mesh (`Replicate`/`ReplicateAck`, `req_id` correlation, `fail_node` on drop); pinned over real framed streams including fencing and disconnect. |
| **0006-P3b-ii-1** Raft types | `lease_raft` defines the replicated `LeaseMap` (`group -> (holder, epoch)`, monotonic epoch) and binds it to openraft via `declare_raft_types!(LeaseConfig)` with a compile-assert it is a valid `RaftTypeConfig`. |
| **0006-P3b-ii-2** Raft storage | `LeaseStore` implements openraft's `RaftStorage` over `LeaseMap` and passes openraft's conformance `Suite`. |
| **0006-P3b-ii-3** Raft network + bring-up | `lease_group` implements `RaftNetwork` and brings up a real group; a single-node group elects and commits a lease, and a three-node group elects a leader and replicates a committed lease to every replica. |
| **0006-P3b-ii-4** Mesh network | `raft_mesh` carries the same RPCs over the peer bus (`RaftRpc`/`RaftRpcReply`, `req_id`-correlated); a leader is elected and a committed lease replicated across nodes over the wire. |
| **0006-P3c** Exactly-once state | `SessionStore` gains `record_received`/`clear_received`/`received`/`next_packet_id`; `ReplicatedSessionStore` stores the QoS-2 dedup window and outbound packet-id counter in the replicated snapshot, so exactly-once survives failover. |
| **0006-P4** Wire it in | `mqttd` swaps `MemorySessionStore` for the durable backend so relocated-session owners write through it; ephemeral sessions become durable. |
| **0006-P3c-i** Cap-count index | The in-memory backend's O(n) cap count is replaced by a rebuildable per-key index. |

## Progress

<!-- status-table:0006 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0006-P1 | ✅ done | 2026-06-15 | lease::overlapping_quorums_cannot_both_commit / superseded_holder_is_fenced; deny.toml cargo-deny gate |
| 0006-P2 | ✅ done | 2026-06-13 | logged::state_lives_in_the_log_not_the_store |
| 0006-P3a | ✅ done | 2026-06-13 | cluster_log::append_is_quorum_durable_and_assigns_offsets / append_below_quorum_is_rejected_and_leaves_no_committed_hole / stale_leader_is_fenced |
| 0006-P3b-i | ✅ done | 2026-06-13 | repl_net::deliver_round_trips_and_applies_on_the_follower / stale_epoch_is_fenced_over_the_wire / fail_node_resolves_in_flight_requests |
| 0006-P3b-ii-1 | ✅ done | 2026-06-15 | lease_raft.rs LeaseMap + declare_raft_types!; assign-monotonic-epoch tests |
| 0006-P3b-ii-2 | ✅ done | 2026-06-15 | lease_store::passes_openraft_conformance_suite_in_memory |
| 0006-P3b-ii-3 | ✅ done | 2026-06-15 | lease_group::single_node_group_elects_and_commits_a_lease / three_node_group_replicates_a_committed_lease |
| 0006-P3b-ii-4 | ✅ done | 2026-06-15 | raft_mesh::two_nodes_elect_and_replicate_over_the_wire |
| 0006-P3c | ✅ done | 2026-06-15 | logged::qos2_state_replicates_through_the_log; SessionStore record_received/clear_received/received/next_packet_id |
| 0006-P4 | ✅ done | 2026-06-22 | main.rs build_durable_node wiring (MQTTD_DATA_DIR); durable_sessions::a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover / enqueue_is_durable_across_a_three_node_cluster |
| 0006-P3c-i | 💤 deferred | — | correctness-neutral; in-memory backend's cap count reads the whole log (O(n)) per the 3c "remaining (minor)" note |
<!-- /status-table:0006 -->

**Note carried from the ADR:** the engine selection (P1) is the *ratified* decision —
openraft chosen, ADR unchanged. The durable store itself is realized by these phases and
its on-disk form by ADR 0018; the `ReplicatedLog` interface is the v1 seam and may evolve.

## Changelog

- **2026-06-22** — P4 wire-in landed: `mqttd` builds the durable node (`build_durable_node`,
  `MQTTD_DATA_DIR`) so relocated-session owners write through the durable backend; cluster
  takeover and durable-enqueue proven in `durable_sessions`.
- **2026-06-15** — P1 engine ratified (openraft over raft-rs); P3b-ii openraft lease manager
  landed in four parts (types, storage + conformance Suite, in-memory bring-up, mesh network);
  P3c replicated exactly-once state. Cap-count index split out as P3c-i (deferred).
- **2026-06-13** — P2 SessionStore-over-log layering proof; P3a epoch-fenced quorum-append
  core; P3b-i networked replica transport.
