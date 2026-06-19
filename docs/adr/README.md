# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: its context, the
choice, and the trade-offs accepted. Numbered sequentially; superseded ADRs are
kept (marked `Superseded by NNNN`) rather than deleted.

| # | Title | Status |
|---|-------|--------|
| [0001](0001-session-durability.md) | Session durability in a horizontally-scalable cluster | Accepted (design) |
| [0002](0002-transport-security.md) | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted |
| [0003](0003-gossip-authentication.md) | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted |
| [0004](0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default | Accepted |
| [0005](0005-session-affinity.md) | Session affinity: relocate persistent sessions to their owner | Accepted |
| [0006](0006-consensus-and-replication.md) | Consensus & replication for durable sessions | Accepted; engine ratified (openraft) |
| [0007](0007-durable-store-integration.md) | Wiring the durable cluster session store into the broker | Accepted (design); implementation phased |
| [0008](0008-mqtt-5-codec.md) | MQTT 5.0 wire codec | Accepted |
| [0009](0009-mqtt5-expiry.md) | MQTT 5.0 session & message expiry | Accepted (design); implementation phased |
| [0010](0010-shared-subscriptions.md) | Shared subscriptions | Accepted (design); implementation phased |
| [0011](0011-topic-aliases.md) | MQTT 5.0 topic aliases | Accepted (design); implementation phased |
| [0012](0012-flow-control.md) | MQTT 5.0 flow control (Receive Maximum) | Accepted (design); implementation phased |
| [0013](0013-enhanced-authentication.md) | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted (design); implementation phased |
| [0014](0014-cross-node-retained.md) | Cross-node retained-message replication | Accepted |
| [0015](0015-cluster-shared-subscriptions.md) | Cluster-wide shared subscriptions | Accepted |
| [0016](0016-swim-membership-stability.md) | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted; implemented |
| [0017](0017-durable-attach-readiness.md) | Durable attach waits for an authoritative session, never downgrades | Accepted |
| [0018](0018-on-disk-persistence.md) | On-disk persistence for durable state (Raft log, session log, retained) | Accepted; phases 1–4 implemented |
| [0019](0019-graceful-shutdown.md) | Graceful shutdown and connection draining | Proposed |
| [0020](0020-metrics-and-observability.md) | Metrics and runtime observability (Prometheus) | Proposed |
| [0021](0021-bounded-lease-voters.md) | Bounded lease-consensus voter set (small fixed quorum + learners) | Proposed |
