# Delivery status

> **Generated** by `scripts/gen-status.py` from the frontmatter in each
> `docs/delivery/NNNN-*.md`. Do not edit by hand. See
> [README.md](README.md) for the artifact model and status vocabulary.

## Decisions and their build progress

| ADR | Title | Decision | Tasks | Open / deferred |
|-----|-------|----------|-------|-----------------|
| [0001](../adr/0001-session-durability.md) | Session durability in a horizontally-scalable cluster | Accepted | [10/11 done](0001-session-durability.md) | 1 deferred |
| [0002](../adr/0002-transport-security.md) | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted | [10/10 done](0002-transport-security.md) | — |
| [0003](../adr/0003-gossip-authentication.md) | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted | [8/9 done](0003-gossip-authentication.md) | — |
| [0004](../adr/0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default | Accepted | [8/11 done](0004-identity-and-authentication.md) | 3 deferred |
| [0005](../adr/0005-session-affinity.md) | Session affinity: relocate persistent sessions to their owner | Accepted | [4/6 done](0005-session-affinity.md) | 2 deferred |
| [0006](../adr/0006-consensus-and-replication.md) | Consensus & replication for durable sessions | Accepted | [11/11 done](0006-consensus-and-replication.md) | — |
| [0007](../adr/0007-durable-store-integration.md) | Wiring the durable cluster session store into the broker | Accepted | [9/9 done](0007-durable-store-integration.md) | — |
| [0008](../adr/0008-mqtt-5-codec.md) | MQTT 5.0 codec | Accepted | [8/8 done](0008-mqtt-5-codec.md) | — |
| [0009](../adr/0009-mqtt5-expiry.md) | MQTT 5.0 session & message expiry | Accepted | [3/3 done](0009-mqtt5-expiry.md) | — |
| [0010](../adr/0010-shared-subscriptions.md) | Shared subscriptions | Accepted | [7/8 done](0010-shared-subscriptions.md) | 1 deferred |
| [0011](../adr/0011-topic-aliases.md) | MQTT 5.0 topic aliases | Accepted | [7/7 done](0011-topic-aliases.md) | — |
| [0012](../adr/0012-flow-control.md) | MQTT 5.0 flow control (Receive Maximum) | Accepted | [6/6 done](0012-flow-control.md) | — |
| [0013](../adr/0013-enhanced-authentication.md) | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted | [8/9 done](0013-enhanced-authentication.md) | 1 deferred |
| [0014](../adr/0014-cross-node-retained.md) | Cross-node retained-message replication | Accepted | [9/9 done](0014-cross-node-retained.md) | — |
| [0015](../adr/0015-cluster-shared-subscriptions.md) | Cluster-wide shared subscriptions | Accepted | [8/8 done](0015-cluster-shared-subscriptions.md) | — |
| [0016](../adr/0016-swim-membership-stability.md) | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted | [6/6 done](0016-swim-membership-stability.md) | — |
| [0017](../adr/0017-durable-attach-readiness.md) | Durable attach waits for an authoritative session, never downgrades | Accepted | [8/9 done](0017-durable-attach-readiness.md) | 1 deferred |
| [0018](../adr/0018-on-disk-persistence.md) | On-disk persistence for durable state | Accepted | [8/8 done](0018-on-disk-persistence.md) | — |
| [0019](../adr/0019-graceful-shutdown.md) | Graceful shutdown and connection draining | Accepted | [7/9 done](0019-graceful-shutdown.md) | 2 deferred |
| [0020](../adr/0020-metrics-and-observability.md) | Metrics and runtime observability | Accepted | [9/9 done](0020-metrics-and-observability.md) | — |
| [0021](../adr/0021-bounded-lease-voters.md) | Bounded lease-consensus voter set | Accepted | [9/9 done](0021-bounded-lease-voters.md) | — |
| [0022](../adr/0022-signed-gossip.md) | Per-node signed gossip (authenticated SWIM identity) | Accepted | [7/7 done](0022-signed-gossip.md) | — |
| [0023](../adr/0023-gossip-anti-replay.md) | Gossip anti-replay: persisted monotonic sequence + sliding window | Accepted | [6/6 done](0023-gossip-anti-replay.md) | — |
| [0024](../adr/0024-deterministic-testing.md) | Deterministic testing: inject time, synchronize causally, gate in CI | Accepted | [7/7 done](0024-deterministic-testing.md) | — |
| [0025](../adr/0025-boundary-bridge.md) | Boundary MQTT bridge to brokers in other security zones | Accepted | [11/11 done](0025-boundary-bridge.md) | — |
| [0026](../adr/0026-lease-timing-durable-storage.md) | Lease-group raft timing tolerant of durable-storage latency | Accepted | [7/7 done](0026-lease-timing-durable-storage.md) | — |
| [0027](../adr/0027-replica-group-commit.md) | Group-commit for the durable replica apply path | Accepted | [4/4 done](0027-replica-group-commit.md) | — |
| [0028](../adr/0028-link-gated-voter-admission.md) | Link-gated lease-group voter admission | Accepted | [3/3 done](0028-link-gated-voter-admission.md) | — |
| [0029](../adr/0029-durable-by-default.md) | Durable sessions by default | Accepted | [3/3 done](0029-durable-by-default.md) | — |
| [0030](../adr/0030-user-property-forwarding.md) | Forward MQTT 5 User Properties through delivery | Accepted | [5/5 done](0030-user-property-forwarding.md) | — |
| [0031](../adr/0031-session-identity-binding.md) | Bind the session to the authenticated identity | Accepted | [6/6 done](0031-session-identity-binding.md) | — |
| [0032](../adr/0032-hot-reloadable-security-policy.md) | Hot-reloadable security policy | Accepted | [8/9 done](0032-hot-reloadable-security-policy.md) | 1 deferred |
| [0033](../adr/0033-config-file-watch-reload.md) | Filesystem-watch auto-reload of the security policy | Accepted | [6/7 done](0033-config-file-watch-reload.md) | 1 deferred |
| [0034](../adr/0034-foreign-client-interop-conformance.md) | Foreign-client interop conformance testing | Accepted | [7/7 done](0034-foreign-client-interop-conformance.md) | — |
| [0035](../adr/0035-websocket-transport.md) | Native MQTT-over-WebSocket transport | Accepted | [7/7 done](0035-websocket-transport.md) | — |
| [0036](../adr/0036-quic-transport.md) | MQTT-over-QUIC transport (multi-stream) | Accepted | [10/11 done](0036-quic-transport.md) | 1 deferred |
| [0037](../adr/0037-durable-retained-messages.md) | Durable single-owner retained messages (clock-free convergence) | Accepted | [8/8 done](0037-durable-retained-messages.md) | — |
| [0038](../adr/0038-prerelease-compatibility-freeze.md) | Pre-release compatibility freeze (versioned wire, stamped schemas, final codecs) | Accepted | [4/4 done](0038-prerelease-compatibility-freeze.md) | — |
| [0039](../adr/0039-versioning-and-upgrade-policy.md) | Release versioning and upgrade policy (semver, adjacent skew, sequential majors) | Accepted | [2/3 done](0039-versioning-and-upgrade-policy.md) | 1 deferred |
| [0040](../adr/0040-revocation-reaches-live-state.md) | Revocation reaches live state (eviction on reload) | Accepted | [5/5 done](0040-revocation-reaches-live-state.md) | — |
| [0041](../adr/0041-resource-governance.md) | Resource governance (admission caps, per-client quotas, bounded state) | Accepted | [5/5 done](0041-resource-governance.md) | — |
| [0042](../adr/0042-durable-plane-stress-harness.md) | Durable-plane stress and simulation harness | Accepted | [9/9 done](0042-durable-plane-stress-harness.md) | — |
| [0043](../adr/0043-elastic-cluster-resize.md) | Elastic cluster resize (grow, shrink, replace) | Accepted | [5/5 done](0043-elastic-cluster-resize.md) | — |
| [0044](../adr/0044-release-readiness-assurance.md) | Release readiness: out-of-process cluster harness and continuous assurance | Accepted | [7/7 done](0044-release-readiness-assurance.md) | — |
| [0045](../adr/0045-release-engineering-and-distribution.md) | Release engineering and distribution (signed, reproducible, SBOM-attested) | Proposed | [3/5 done](0045-release-engineering-and-distribution.md) | 2 open |
| [0046](../adr/0046-file-based-configuration.md) | File-based configuration (layered over env, hot-reloadable, GitOps-friendly) | Accepted | [5/5 done](0046-file-based-configuration.md) | — |
| [0047](../adr/0047-kubernetes-deployment.md) | Kubernetes deployment (Helm chart, StatefulSet, safe scale-down) | Accepted | [5/5 done](0047-kubernetes-deployment.md) | — |
| [0048](../adr/0048-comparative-benchmarking.md) | Comparative performance benchmarking (published, reproducible, honest) | Proposed | [0/4 done](0048-comparative-benchmarking.md) | 4 open |
| [0049](../adr/0049-voter-eligible-durable-ownership.md) | Durable ownership must be lease-eligible, and a degraded durable plane must be visible | Accepted | [3/3 done](0049-voter-eligible-durable-ownership.md) | — |

## Open and deferred work

**0001 — Session durability in a horizontally-scalable cluster**

- `0001-T11` 💤 deferred: Client-facing reconnect during promotion + spec-legal QoS-1 redelivery bounds (takeover hardening) — takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass

**0004 — Identity model: mTLS Common Name first, deny by default**

- `0004-T9` 💤 deferred: Full OIDC discovery / JWKS rotation; MQTT5 enhanced auth after v5 codec — step 6 takes a single static key; enhanced auth waits on the v5 codec milestone
- `0004-T10` 💤 deferred: Delivery-time ACL re-check in the hub (enforcement is subscription-time only) — documented known limitation; needed only if policies change under live subscriptions; tracked with hot ACL reload
- `0004-T11` 💤 deferred: SAN-based identity, per-listener auth policies, hot ACL reload, %c (client-id) substitution — %c deferred until the Authorizer trait carries the client id; the rest are future config options

**0005 — Session affinity: relocate persistent sessions to their owner**

- `0005-P2c` 💤 deferred: Delivery/lifecycle hardening of the splice (best-effort on half-close) — splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up
- `0005-P3` 💤 deferred: MQTT 5 Server-Reference redirect replacing the relay for v5 clients — "Re-assessed 2026-07-02: the original blocker (no v5 codec) is gone (ADR 0008), so this is now buildable — but parked on the OTHER half of the original condition: mainstream v5 clients (paho, mosquitto) do not auto-follow Server Reference / 0x9C redirects, so the relay must remain the universal path regardless and a redirect would only serve clients that opt into handling it. Revisit if a redirect-capable client population materialises; the proxy serves 3.1.1 and v5 alike meanwhile."

**0010 — Shared subscriptions**

- `0010-T7` 💤 deferred: Subscription-Identifier handling for shared subscriptions — ADR 0010 Consequences notes no Subscription-Identifier handling yet; out of scope for the routing lever

**0013 — MQTT 5.0 enhanced authentication (AUTH exchange)**

- `0013-T8` 💤 deferred: Server-initiated re-auth (server sends AUTH 0x19 to demand re-authentication) — ADR section 4 explicitly defers this — needs a trigger mechanism and interacts with the select-loop outbound path; only client-initiated re-auth is implemented (no server-side AUTH 0x19 send exists in conn.rs).

**0017 — Durable attach waits for an authoritative session, never downgrades**

- `0017-T9` 💤 deferred: Make recovery deadline/backoff configurable (currently constants) — ATTACH_RECOVERY_TIMEOUT/BACKOFF are constants for now; ADR defers promoting them to config until an operator need appears

**0019 — Graceful shutdown and connection draining**

- `0019-T8` 💤 deferred: Lease-leadership transfer when the leaving node is the Raft leader — "Spike 2026-06-25 (openraft 0.9 transfer-API evaluation, the task's stated prerequisite): openraft 0.9.24 exposes NO public leadership-transfer/TimeoutNow API — Trigger has only elect/heartbeat/snapshot/purge_log. change_membership-remove-self steps the leader down internally (raft_core.rs:1311 -> leader_step_down) but does not provoke an immediate election, so the remaining voters still wait out their election timeout: it does not close the gap. Trigger::transfer_leader exists only on the alpha-only 0.10 line (latest 0.10.0-alpha.23, Jun 2026; no beta/RC/stable, no v0.9->v0.10 upgrade guide; maintainer keeps 0.9.24 as the production default). Deferred pending a stable openraft release exposing transfer_leader — pulling an alpha into the consensus core is a poor trade for a bounded ~1.5-3s graceful-leave gap (relaxed ADR 0026 timing) that already degrades safely via survivors' election."
- `0019-T9` 💤 deferred: In-flight QoS settle / hub Drain command — drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown

**0032 — Hot-reloadable security policy**

- `0032-T9` 💤 deferred: Follow-ons via the same mechanism — cert revocation (reloadable CRL → WebPkiClientVerifier) and peer-bus TLS reload — "Partly delivered. Cert revocation via a reloadable CRL → WebPkiClientVerifier is **done** (ADR 0002 T8: server_config_with_crl + MQTTD_TLS_CRL, applied through this ADR's reloadable acceptor; tests/tls.rs reloading_a_crl_revokes_a_client_in_place). Still deferred: peer-bus (cluster) TLS reload — the same pattern applied to the peer acceptor/connector, kept off the consensus bus for now to avoid coupling a client-facing change to membership/quorum. Now tracked as ADR 0040 T4 (revocation reaches live state)."

**0033 — Filesystem-watch auto-reload of the security policy**

- `0033-T7` 💤 deferred: Follow-on — optional notify-backed (inotify/FSEvents/kqueue) event-driven backend behind the same seam, if sub-second reaction is ever needed — polling covers the config-rollout use case with no new dependency; an event-driven backend is a latency optimisation that still needs the same retry-until-parse/debounce, so it is parked behind the watcher seam rather than bundled.

**0036 — MQTT-over-QUIC transport (multi-stream)**

- `0036-T11` 💤 deferred: Follow-on — 1-RTT resumption tuning (ticket lifetime / resumption policy under mTLS-on-every-connection) — 1-RTT session resumption is quinn/rustls-provided and replay-safe (0-RTT stays disabled, T1); explicit ticket-lifetime/policy tuning is a follow-on, separate from migration. Distinct from migration — resumption is a NEW connection reusing crypto, not a live connection surviving a path change.

**0039 — Release versioning and upgrade policy (semver, adjacent skew, sequential majors)**

- `0039-T3` 💤 deferred: At 1.0 — skew test in CI (adjacent-pair rolling-upgrade smoke) once two releases exist; blocked until then — "Needs two released versions to exist — impossible before 1.0 by definition. THE MACHINERY NOW EXISTS (ADR 0044 P3, 2026-07-17): cluster_upgrade::a_rolling_upgrade_and_rollback_lose_no_acked_fact rolls a live cluster between a pinned baseline binary and HEAD one node at a time in both directions under the acked-facts oracle; at 1.0 this task is that test pointed at two release tags plus a scheduled CI job. Until then the pinned baseline doubles as the pre-release compatibility tripwire."

**0045 — Release engineering and distribution (signed, reproducible, SBOM-attested)**

- `0045-T3` 🚧 in-progress: Keyless signing + provenance — cosign/sigstore signatures on every artifact and image, build-provenance attestation, transparency-log entry; a one-command documented verify path — "cosign keyless sign-blob (binaries/checksums/SBOM) + sign image + attest-build-provenance + attest-sbom all wired; RELEASING.md + README document the one-command verify; first real signatures/Rekor entries are produced by the first tag run (OIDC exists only in Actions)"
- `0045-T5` 🚧 in-progress: SBOM per release (CycloneDX or SPDX) attached to the release and image; cargo-deny/cargo-audit run on the release commit; RELEASING.md + README verify docs; cut the first 0.x release — "CycloneDX SBOM (cargo-cyclonedx) + cargo-deny/cargo-audit gate on the release commit + RELEASING.md + README Install/verify — all in place; remaining: cut the first 0.x release (a maintainer signed-tag push, gated on the ADR 0044 readiness checklist)"

**0048 — Comparative performance benchmarking (published, reproducible, honest)**

- `0048-T1` ⬜ planned: Containerized load harness — an established MQTT benchmark client + docker-compose that stands up each broker (ours, Mosquitto, EMQX) from its published image with documented reasonable config; same hardware, pinned versions, security posture held constant and disclosed
- `0048-T2` ⬜ planned: The selection metrics — sustained throughput (QoS 0/1/2), end-to-end latency p50/p99/p999, memory per idle connection at scale, connection-establishment rate (mTLS included); full distributions, never a single number
- `0048-T3` ⬜ planned: The scaling curve — the same workload against 1/3/5 nodes, throughput and p99 vs node count; tests capability claim 1 and the ADR 0015 shared-subscription mechanism end to end; a flat curve is a finding to fix
- `0048-T4` ⬜ planned: Honesty rules + publication — versions/hardware/config/date stated; losing dimensions reported as prominently as winning ones; results in docs/benchmarks/ linked from the README; self-benchmark runs nightly (ADR 0044 P4), cross-broker re-run per release
