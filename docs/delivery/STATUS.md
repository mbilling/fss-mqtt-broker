# Delivery status

> **Generated** by `scripts/gen-status.py` from the frontmatter in each
> `docs/delivery/NNNN-*.md`. Do not edit by hand. See
> [README.md](README.md) for the artifact model and status vocabulary.

## Decisions and their build progress

| ADR | Title | Decision | Tasks | Open / deferred |
|-----|-------|----------|-------|-----------------|
| 0001 | Session durability in a horizontally-scalable cluster | Accepted (design); implementation phased (see Roadmap below) | _not migrated_ | — |
| 0002 | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted | _not migrated_ | — |
| 0003 | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted | _not migrated_ | — |
| 0004 | Identity model: mTLS Common Name first, deny by default | Accepted | _not migrated_ | — |
| 0005 | Session affinity: relocate persistent sessions to their owner | Accepted (design); implementation phased | _not migrated_ | — |
| 0006 | Consensus & replication for durable sessions | Accepted; engine **ratified (openraft)** by the workstream-E spike | _not migrated_ | — |
| 0007 | Wiring the durable cluster session store into the broker | Accepted (design); implementation phased (workstream E step 4) | _not migrated_ | — |
| 0008 | MQTT 5.0 codec | Accepted (design); implementation phased (codec milestone, gates workstream G) | _not migrated_ | — |
| 0009 | MQTT 5.0 session & message expiry | Accepted (design); implementation phased (workstream G) | _not migrated_ | — |
| 0010 | Shared subscriptions | Accepted (design); implementation phased (workstream G) | _not migrated_ | — |
| 0011 | MQTT 5.0 topic aliases | Accepted (design); implementation phased (workstream G) | _not migrated_ | — |
| 0012 | MQTT 5.0 flow control (Receive Maximum) | Accepted (design); implementation phased (workstream G) | _not migrated_ | — |
| 0013 | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted (design); implementation phased (workstream G) | _not migrated_ | — |
| 0014 | Cross-node retained-message replication | Accepted | _not migrated_ | — |
| 0015 | Cluster-wide shared subscriptions | Accepted | _not migrated_ | — |
| 0016 | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted | 3/4 done | 1 open |
| 0017 | Durable attach waits for an authoritative session, never downgrades | Accepted | _not migrated_ | — |
| 0018 | On-disk persistence for durable state | Accepted | 7/8 done | 1 deferred |
| 0019 | Graceful shutdown and connection draining | Accepted | 7/9 done | 2 deferred |
| 0020 | Metrics and runtime observability | Proposed (awaiting ratification) | _not migrated_ | — |
| 0021 | Bounded lease-consensus voter set | Proposed (awaiting ratification) | _not migrated_ | — |

## Open and deferred work

**0016 — SWIM membership stability (dead-node fencing + false-positive resistance)**

- `0016-T4` ⬜ planned: Failure-domain-aware voter selection (interaction with ADR 0021) — bounded-voter work (ADR 0021) should pick voters across failure domains; revisit when 0021 is built

**0018 — On-disk persistence for durable state**

- `0018-T7` 💤 deferred: Process-kill (SIGKILL mid-write) crash-consistency test — rests on redb's own ACID/crash suite; an in-repo subprocess kill test adds machinery for modest marginal coverage

**0019 — Graceful shutdown and connection draining**

- `0019-T8` 💤 deferred: Lease-leadership transfer when the leaving node is the Raft leader — avoids one election (~300-600ms) on a leaving leader; needs openraft 0.9 transfer-API evaluation first
- `0019-T9` 💤 deferred: In-flight QoS settle / hub Drain command — drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown
