# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: its context, the
choice, and the trade-offs accepted. Numbered sequentially; superseded ADRs are
kept (marked `Superseded by NNNN`) rather than deleted.

An ADR records **the decision only**, and its `Status` is just the lifecycle
(`Proposed | Accepted | Superseded | Deprecated`). **How** a decision is being built
and **how far along** it is live in its delivery doc under
[`docs/delivery/`](../delivery/) — start with the
[**delivery dashboard**](../delivery/STATUS.md) for the at-a-glance, whole-project
overview, and see [`docs/delivery/README.md`](../delivery/README.md) for the model and
conventions.

> Every ADR now has a delivery doc, so the dashboard is the single source of truth for
> build status. The decision/plan/progress split has been applied to 0016, 0018, and
> 0019; the remaining ADR *bodies* still carry their original inline implementation
> prose pending a freeze-to-decision-only pass — but their **status** lives in the
> delivery docs regardless.

| # | Title | Status | Delivery |
|---|-------|--------|----------|
| [0001](0001-session-durability.md) | Session durability in a horizontally-scalable cluster | Accepted | [delivery](../delivery/0001-session-durability.md) |
| [0002](0002-transport-security.md) | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted | [delivery](../delivery/0002-transport-security.md) |
| [0003](0003-gossip-authentication.md) | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted | [delivery](../delivery/0003-gossip-authentication.md) |
| [0004](0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default | Accepted | [delivery](../delivery/0004-identity-and-authentication.md) |
| [0005](0005-session-affinity.md) | Session affinity: relocate persistent sessions to their owner | Accepted | [delivery](../delivery/0005-session-affinity.md) |
| [0006](0006-consensus-and-replication.md) | Consensus & replication for durable sessions | Accepted | [delivery](../delivery/0006-consensus-and-replication.md) |
| [0007](0007-durable-store-integration.md) | Wiring the durable cluster session store into the broker | Accepted | [delivery](../delivery/0007-durable-store-integration.md) |
| [0008](0008-mqtt-5-codec.md) | MQTT 5.0 wire codec | Accepted | [delivery](../delivery/0008-mqtt-5-codec.md) |
| [0009](0009-mqtt5-expiry.md) | MQTT 5.0 session & message expiry | Accepted | [delivery](../delivery/0009-mqtt5-expiry.md) |
| [0010](0010-shared-subscriptions.md) | Shared subscriptions | Accepted | [delivery](../delivery/0010-shared-subscriptions.md) |
| [0011](0011-topic-aliases.md) | MQTT 5.0 topic aliases | Accepted | [delivery](../delivery/0011-topic-aliases.md) |
| [0012](0012-flow-control.md) | MQTT 5.0 flow control (Receive Maximum) | Accepted | [delivery](../delivery/0012-flow-control.md) |
| [0013](0013-enhanced-authentication.md) | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted | [delivery](../delivery/0013-enhanced-authentication.md) |
| [0014](0014-cross-node-retained.md) | Cross-node retained-message replication | Accepted | [delivery](../delivery/0014-cross-node-retained.md) |
| [0015](0015-cluster-shared-subscriptions.md) | Cluster-wide shared subscriptions | Accepted | [delivery](../delivery/0015-cluster-shared-subscriptions.md) |
| [0016](0016-swim-membership-stability.md) | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted | [delivery](../delivery/0016-swim-membership-stability.md) |
| [0017](0017-durable-attach-readiness.md) | Durable attach waits for an authoritative session, never downgrades | Accepted | [delivery](../delivery/0017-durable-attach-readiness.md) |
| [0018](0018-on-disk-persistence.md) | On-disk persistence for durable state (Raft log, session log, retained) | Accepted | [delivery](../delivery/0018-on-disk-persistence.md) |
| [0019](0019-graceful-shutdown.md) | Graceful shutdown and connection draining | Accepted | [delivery](../delivery/0019-graceful-shutdown.md) |
| [0020](0020-metrics-and-observability.md) | Metrics and runtime observability (Prometheus) | Accepted | [delivery](../delivery/0020-metrics-and-observability.md) |
| [0021](0021-bounded-lease-voters.md) | Bounded lease-consensus voter set (small fixed quorum + learners) | Proposed | [delivery](../delivery/0021-bounded-lease-voters.md) |
| [0022](0022-signed-gossip.md) | Per-node signed gossip (authenticated SWIM identity) | Accepted | [delivery](../delivery/0022-signed-gossip.md) |
| [0023](0023-gossip-anti-replay.md) | Gossip anti-replay (persisted monotonic sequence + sliding window) | Accepted | [delivery](../delivery/0023-gossip-anti-replay.md) |
| [0024](0024-deterministic-testing.md) | Deterministic testing: inject time, synchronize causally, gate in CI | Accepted | [delivery](../delivery/0024-deterministic-testing.md) |
| [0025](0025-boundary-bridge.md) | Boundary MQTT bridge to brokers in other security zones | Proposed | [delivery](../delivery/0025-boundary-bridge.md) |
| [0026](0026-lease-timing-durable-storage.md) | Lease-group raft timing tolerant of durable-storage latency | Accepted | [delivery](../delivery/0026-lease-timing-durable-storage.md) |
| [0027](0027-replica-group-commit.md) | Group-commit for the durable replica apply path | Accepted | [delivery](../delivery/0027-replica-group-commit.md) |
| [0028](0028-link-gated-voter-admission.md) | Link-gated lease-group voter admission (fixes formation churn) | Accepted | [delivery](../delivery/0028-link-gated-voter-admission.md) |
| [0029](0029-durable-by-default.md) | Durable sessions by default (opt-out) | Accepted | [delivery](../delivery/0029-durable-by-default.md) |
| [0030](0030-user-property-forwarding.md) | MQTT 5.0 User Property / application-property forwarding | Accepted | [delivery](../delivery/0030-user-property-forwarding.md) |
| [0031](0031-session-identity-binding.md) | Bind the session to the authenticated identity | Proposed | [delivery](../delivery/0031-session-identity-binding.md) |
| [0032](0032-hot-reloadable-security-policy.md) | Hot-reloadable security policy (SIGHUP, validate-before-swap) | Accepted | [delivery](../delivery/0032-hot-reloadable-security-policy.md) |
| [0033](0033-config-file-watch-reload.md) | Filesystem-watch auto-reload of the security policy (opt-in) | Proposed | [delivery](../delivery/0033-config-file-watch-reload.md) |
| [0034](0034-foreign-client-interop-conformance.md) | Foreign-client (non-Rust) interop conformance testing | Proposed | [delivery](../delivery/0034-foreign-client-interop-conformance.md) |
