# Delivery status

> **Generated** by `scripts/gen-status.py` from the frontmatter in each
> `docs/delivery/NNNN-*.md`. Do not edit by hand. See
> [README.md](README.md) for the artifact model and status vocabulary.

## Decisions and their build progress

| ADR | Title | Decision | Tasks | Open / deferred |
|-----|-------|----------|-------|-----------------|
| [0001](../adr/0001-session-durability.md) | Session durability in a horizontally-scalable cluster | Accepted | [11/12 done](0001-session-durability.md) | 1 deferred |
| [0002](../adr/0002-transport-security.md) | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted | [10/10 done](0002-transport-security.md) | — |
| [0003](../adr/0003-gossip-authentication.md) | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted | [8/9 done](0003-gossip-authentication.md) | — |
| [0004](../adr/0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default | Accepted | [14/17 done](0004-identity-and-authentication.md) | 1 deferred |
| [0005](../adr/0005-session-affinity.md) | Session affinity: relocate persistent sessions to their owner | Accepted | [4/6 done](0005-session-affinity.md) | 2 deferred |
| [0006](../adr/0006-consensus-and-replication.md) | Consensus & replication for durable sessions | Accepted | [13/13 done](0006-consensus-and-replication.md) | — |
| [0007](../adr/0007-durable-store-integration.md) | Wiring the durable cluster session store into the broker | Accepted | [9/9 done](0007-durable-store-integration.md) | — |
| [0008](../adr/0008-mqtt-5-codec.md) | MQTT 5.0 codec | Accepted | [12/12 done](0008-mqtt-5-codec.md) | — |
| [0009](../adr/0009-mqtt5-expiry.md) | MQTT 5.0 session & message expiry | Accepted | [4/4 done](0009-mqtt5-expiry.md) | — |
| [0010](../adr/0010-shared-subscriptions.md) | Shared subscriptions | Accepted | [7/8 done](0010-shared-subscriptions.md) | 1 deferred |
| [0011](../adr/0011-topic-aliases.md) | MQTT 5.0 topic aliases | Accepted | [7/7 done](0011-topic-aliases.md) | — |
| [0012](../adr/0012-flow-control.md) | MQTT 5.0 flow control (Receive Maximum) | Accepted | [6/6 done](0012-flow-control.md) | — |
| [0013](../adr/0013-enhanced-authentication.md) | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted | [8/9 done](0013-enhanced-authentication.md) | 1 deferred |
| [0014](../adr/0014-cross-node-retained.md) | Cross-node retained-message replication | Accepted | [10/10 done](0014-cross-node-retained.md) | — |
| [0015](../adr/0015-cluster-shared-subscriptions.md) | Cluster-wide shared subscriptions | Accepted | [8/8 done](0015-cluster-shared-subscriptions.md) | — |
| [0016](../adr/0016-swim-membership-stability.md) | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted | [7/7 done](0016-swim-membership-stability.md) | — |
| [0017](../adr/0017-durable-attach-readiness.md) | Durable attach waits for an authoritative session, never downgrades | Accepted | [8/9 done](0017-durable-attach-readiness.md) | 1 deferred |
| [0018](../adr/0018-on-disk-persistence.md) | On-disk persistence for durable state | Accepted | [8/8 done](0018-on-disk-persistence.md) | — |
| [0019](../adr/0019-graceful-shutdown.md) | Graceful shutdown and connection draining | Accepted | [7/9 done](0019-graceful-shutdown.md) | 2 deferred |
| [0020](../adr/0020-metrics-and-observability.md) | Metrics and runtime observability | Accepted | [9/9 done](0020-metrics-and-observability.md) | — |
| [0021](../adr/0021-bounded-lease-voters.md) | Bounded lease-consensus voter set | Accepted | [9/9 done](0021-bounded-lease-voters.md) | — |
| [0022](../adr/0022-signed-gossip.md) | Per-node signed gossip (authenticated SWIM identity) | Accepted | [8/8 done](0022-signed-gossip.md) | — |
| [0023](../adr/0023-gossip-anti-replay.md) | Gossip anti-replay: persisted monotonic sequence + sliding window | Accepted | [6/6 done](0023-gossip-anti-replay.md) | — |
| [0024](../adr/0024-deterministic-testing.md) | Deterministic testing: inject time, synchronize causally, gate in CI | Accepted | [7/7 done](0024-deterministic-testing.md) | — |
| [0025](../adr/0025-boundary-bridge.md) | Boundary MQTT bridge to brokers in other security zones | Accepted | [13/14 done](0025-boundary-bridge.md) | 1 open |
| [0026](../adr/0026-lease-timing-durable-storage.md) | Lease-group raft timing tolerant of durable-storage latency | Accepted | [7/7 done](0026-lease-timing-durable-storage.md) | — |
| [0027](../adr/0027-replica-group-commit.md) | Group-commit for the durable replica apply path | Accepted | [4/4 done](0027-replica-group-commit.md) | — |
| [0028](../adr/0028-link-gated-voter-admission.md) | Link-gated lease-group voter admission | Accepted | [3/3 done](0028-link-gated-voter-admission.md) | — |
| [0029](../adr/0029-durable-by-default.md) | Durable sessions by default | Accepted | [4/4 done](0029-durable-by-default.md) | — |
| [0030](../adr/0030-user-property-forwarding.md) | Forward MQTT 5 User Properties through delivery | Accepted | [6/6 done](0030-user-property-forwarding.md) | — |
| [0031](../adr/0031-session-identity-binding.md) | Bind the session to the authenticated identity | Accepted | [6/6 done](0031-session-identity-binding.md) | — |
| [0032](../adr/0032-hot-reloadable-security-policy.md) | Hot-reloadable security policy | Accepted | [8/9 done](0032-hot-reloadable-security-policy.md) | 1 deferred |
| [0033](../adr/0033-config-file-watch-reload.md) | Filesystem-watch auto-reload of the security policy | Accepted | [6/7 done](0033-config-file-watch-reload.md) | 1 deferred |
| [0034](../adr/0034-foreign-client-interop-conformance.md) | Foreign-client interop conformance testing | Accepted | [9/9 done](0034-foreign-client-interop-conformance.md) | — |
| [0035](../adr/0035-websocket-transport.md) | Native MQTT-over-WebSocket transport | Accepted | [7/7 done](0035-websocket-transport.md) | — |
| [0036](../adr/0036-quic-transport.md) | MQTT-over-QUIC transport (multi-stream) | Accepted | [10/11 done](0036-quic-transport.md) | 1 deferred |
| [0037](../adr/0037-durable-retained-messages.md) | Durable single-owner retained messages (clock-free convergence) | Accepted | [14/14 done](0037-durable-retained-messages.md) | — |
| [0038](../adr/0038-prerelease-compatibility-freeze.md) | Pre-release compatibility freeze (versioned wire, stamped schemas, final codecs) | Accepted | [4/4 done](0038-prerelease-compatibility-freeze.md) | — |
| [0039](../adr/0039-versioning-and-upgrade-policy.md) | Release versioning and upgrade policy (semver, adjacent skew, sequential majors) | Accepted | [2/3 done](0039-versioning-and-upgrade-policy.md) | 1 deferred |
| [0040](../adr/0040-revocation-reaches-live-state.md) | Revocation reaches live state (eviction on reload) | Accepted | [5/5 done](0040-revocation-reaches-live-state.md) | — |
| [0041](../adr/0041-resource-governance.md) | Resource governance (admission caps, per-client quotas, bounded state) | Accepted | [13/17 done](0041-resource-governance.md) | 4 open |
| [0042](../adr/0042-durable-plane-stress-harness.md) | Durable-plane stress and simulation harness | Accepted | [9/9 done](0042-durable-plane-stress-harness.md) | — |
| [0043](../adr/0043-elastic-cluster-resize.md) | Elastic cluster resize (grow, shrink, replace) | Accepted | [7/7 done](0043-elastic-cluster-resize.md) | — |
| [0044](../adr/0044-release-readiness-assurance.md) | Release readiness: out-of-process cluster harness and continuous assurance | Accepted | [10/11 done](0044-release-readiness-assurance.md) | 1 open |
| [0045](../adr/0045-release-engineering-and-distribution.md) | Release engineering and distribution (signed, reproducible, SBOM-attested) | Accepted | [6/6 done](0045-release-engineering-and-distribution.md) | — |
| [0046](../adr/0046-file-based-configuration.md) | File-based configuration (layered over env, hot-reloadable, GitOps-friendly) | Accepted | [5/6 done](0046-file-based-configuration.md) | 1 open |
| [0047](../adr/0047-kubernetes-deployment.md) | Kubernetes deployment (Helm chart, StatefulSet, safe scale-down) | Accepted | [13/13 done](0047-kubernetes-deployment.md) | — |
| [0048](../adr/0048-comparative-benchmarking.md) | Comparative performance benchmarking (published, reproducible, honest) | Accepted | [3/5 done](0048-comparative-benchmarking.md) | 2 open |
| [0049](../adr/0049-voter-eligible-durable-ownership.md) | Durable ownership must be lease-eligible, and a degraded durable plane must be visible | Accepted | [3/3 done](0049-voter-eligible-durable-ownership.md) | — |
| [0050](../adr/0050-oidc-token-authentication.md) | OIDC-integrated token authentication (discovery, JWKS rotation, proven against a real IdP) | Accepted | [5/5 done](0050-oidc-token-authentication.md) | — |
| [0051](../adr/0051-evaluation-readiness.md) | Evaluation readiness: an assessable, comparable, migratable first release | Proposed | [20/23 done](0051-evaluation-readiness.md) | 3 open |
| [0052](../adr/0052-codec-succession.md) | Codec succession: postcard replaces bincode on every cluster surface | Accepted | [4/4 done](0052-codec-succession.md) | — |
| [0053](../adr/0053-single-crypto-provider-aws-lc-rs.md) | One crypto provider: aws-lc-rs everywhere, ring evicted | Accepted | [4/5 done](0053-single-crypto-provider.md) | 1 open |
| [0054](../adr/0054-operator-facing-state-surface.md) | Operator-facing state surface: `/statusz` + state gauges | Accepted | [5/5 done](0054-operator-facing-state-surface.md) | — |
| [0055](../adr/0055-kubernetes-operator.md) | The mqttd Kubernetes operator (`MqttdCluster` CRD, kube-rs controller) | Accepted | [10/12 done](0055-kubernetes-operator.md) | 2 open |
| [0056](../adr/0056-mqttui.md) | `mqttui`: a terminal UI for running the demo, migration and test scripts | Proposed | [10/10 done](0056-mqttui.md) | — |
| [0057](../adr/0057-durable-outbound-inflight.md) | Durable outbound in-flight state: exactly-once across a broker crash | Proposed | [5/6 done](0057-durable-outbound-inflight.md) | 1 open |
| [0058](../adr/0058-one-dot-zero-stability-contract.md) | The 1.0 stability contract: upgrade-in-place, never wipe-and-rejoin | Proposed | [4/5 done](0058-one-dot-zero-stability-contract.md) | 1 open |
| [0059](../adr/0059-bridge-ha-topology-and-ordering.md) | Bridge HA topology and message ordering | Proposed | [5/6 done](0059-bridge-ha-topology-and-ordering.md) | 1 open |
| [0060](../adr/0060-bridge-durability-and-ack-contract.md) | Bridge durability and acknowledgement contract | Proposed | [7/8 done](0060-bridge-durability-and-ack-contract.md) | 1 open |
| [0061](../adr/0061-off-loop-durable-appends.md) | Off-loop durable appends: per-session lanes for the publish path | Accepted | [6/6 done](0061-off-loop-durable-appends.md) | — |
| [0062](../adr/0062-online-backup-and-restore.md) | Online backup and restore: a per-node export with a stated window | Accepted | [11/11 done](0062-online-backup-and-restore.md) | — |
| [0063](../adr/0063-external-consumer-integration.md) | External integration without a rule engine: the consumer-group pattern | Accepted | [3/3 done](0063-external-consumer-integration.md) | — |
| [0064](../adr/0064-hub-module-seams.md) | The hub's module seams | Accepted | [1/1 done](0064-hub-module-seams.md) | — |

## Open and deferred work

**0001 — Session durability in a horizontally-scalable cluster**

- `0001-T11` 💤 deferred: Client-facing reconnect during promotion + spec-legal QoS-1 redelivery bounds (takeover hardening) — takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass

**0004 — Identity model: mTLS Common Name first, deny by default**

- `0004-T13` 💤 deferred: Per-listener auth policies (each listener carrying its own authenticator/ACL) — "Needs the flat one-bind-per-transport Listeners struct (ADR 0046) to become a list of named listener definitions, each with its own policy and reload path — a config-model decision that earns its own record rather than an option bolted onto this one. The fourth item of the old bundled T11, hot ACL reload, was delivered by ADR 0032/0033 and reaches live state via ADR 0040."

**0005 — Session affinity: relocate persistent sessions to their owner**

- `0005-P2c` 💤 deferred: Delivery/lifecycle hardening of the splice (best-effort on half-close) — splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up
- `0005-P3` 💤 deferred: MQTT 5 Server-Reference redirect replacing the relay for v5 clients — "Re-assessed 2026-07-02: the original blocker (no v5 codec) is gone (ADR 0008), so this is now buildable — but parked on the OTHER half of the original condition: mainstream v5 clients (paho, mosquitto) do not auto-follow Server Reference / 0x9C redirects, so the relay must remain the universal path regardless and a redirect would only serve clients that opt into handling it. Revisit if a redirect-capable client population materialises; the proxy serves 3.1.1 and v5 alike meanwhile."

**0010 — Shared subscriptions**

- `0010-T7` 💤 deferred: Subscription-Identifier handling for shared subscriptions — "STAYS DEFERRED — issue #245 shipped only the honest wire posture (CONNACK 0x29 = 0; a SUBSCRIBE using an identifier is refused with DISCONNECT 0xA1), NOT delivery, so nothing here is done. Delivery is tracked by issue #266, which owns this task. Design fact to preserve: the peer wire does NOT need to grow. Per §3.3.4 only the RECEIVING client's own identifiers may appear on a shared delivery — never another session's — so RemoteSharedDeliver resolves them locally on the target node and SharedMemberWire / WireAppProps stay unchanged. Also §3.8.4: no retained messages on a shared subscribe, so the retained-with-identifier case does not arise here."

**0013 — MQTT 5.0 enhanced authentication (AUTH exchange)**

- `0013-T8` 💤 deferred: Server-initiated re-auth (server sends AUTH 0x19 to demand re-authentication) — ADR section 4 explicitly defers this — needs a trigger mechanism and interacts with the select-loop outbound path; only client-initiated re-auth is implemented (no server-side AUTH 0x19 send exists in conn.rs).

**0017 — Durable attach waits for an authoritative session, never downgrades**

- `0017-T9` 💤 deferred: Make recovery deadline/backoff configurable (currently constants) — ATTACH_RECOVERY_TIMEOUT/BACKOFF are constants for now; ADR defers promoting them to config until an operator need appears

**0019 — Graceful shutdown and connection draining**

- `0019-T8` 💤 deferred: Lease-leadership transfer when the leaving node is the Raft leader — "Spike 2026-06-25 (openraft 0.9 transfer-API evaluation, the task's stated prerequisite): openraft 0.9.24 exposes NO public leadership-transfer/TimeoutNow API — Trigger has only elect/heartbeat/snapshot/purge_log. change_membership-remove-self steps the leader down internally (raft_core.rs:1311 -> leader_step_down) but does not provoke an immediate election, so the remaining voters still wait out their election timeout: it does not close the gap. Trigger::transfer_leader exists only on the alpha-only 0.10 line (latest 0.10.0-alpha.23, Jun 2026; no beta/RC/stable, no v0.9->v0.10 upgrade guide; maintainer keeps 0.9.24 as the production default). Deferred pending a stable openraft release exposing transfer_leader — pulling an alpha into the consensus core is a poor trade for a bounded ~1.5-3s graceful-leave gap (relaxed ADR 0026 timing) that already degrades safely via survivors' election."
- `0019-T9` 💤 deferred: In-flight QoS settle / hub Drain command — drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown

**0025 — Boundary MQTT bridge to brokers in other security zones**

- `0025-T13` ⬜ planned: Bridge lag — how far behind a side is, as the age of its oldest spooled message (fss_bridge_spool_oldest_age_seconds); needs an enqueue timestamp in the spool record, so it is a spool-format change. Claimed by T9's original title and never built (2026-08-08 amendment)

**0032 — Hot-reloadable security policy**

- `0032-T9` 💤 deferred: Follow-ons via the same mechanism — cert revocation (reloadable CRL → WebPkiClientVerifier) and peer-bus TLS reload — "Partly delivered. Cert revocation via a reloadable CRL → WebPkiClientVerifier is **done** (ADR 0002 T8: server_config_with_crl + MQTTD_TLS_CRL, applied through this ADR's reloadable acceptor; tests/tls.rs reloading_a_crl_revokes_a_client_in_place). Still deferred: peer-bus (cluster) TLS reload — the same pattern applied to the peer acceptor/connector, kept off the consensus bus for now to avoid coupling a client-facing change to membership/quorum. Now tracked as ADR 0040 T4 (revocation reaches live state)."

**0033 — Filesystem-watch auto-reload of the security policy**

- `0033-T7` 💤 deferred: Follow-on — optional notify-backed (inotify/FSEvents/kqueue) event-driven backend behind the same seam, if sub-second reaction is ever needed — polling covers the config-rollout use case with no new dependency; an event-driven backend is a latency optimisation that still needs the same retry-until-parse/debounce, so it is parked behind the watcher seam rather than bundled.

**0036 — MQTT-over-QUIC transport (multi-stream)**

- `0036-T11` 💤 deferred: Follow-on — 1-RTT resumption tuning (ticket lifetime / resumption policy under mTLS-on-every-connection) — 1-RTT session resumption is quinn/rustls-provided and replay-safe (0-RTT stays disabled, T1); explicit ticket-lifetime/policy tuning is a follow-on, separate from migration. Distinct from migration — resumption is a NEW connection reusing crypto, not a live connection surviving a path change.

**0039 — Release versioning and upgrade policy (semver, adjacent skew, sequential majors)**

- `0039-T3` 💤 deferred: At 1.0 — skew test in CI (adjacent-pair rolling-upgrade smoke) once two releases exist; blocked until then — "Needs two released versions to exist — impossible before 1.0 by definition. THE MACHINERY NOW EXISTS (ADR 0044 P3, 2026-07-17): cluster_upgrade::a_rolling_upgrade_and_rollback_lose_no_acked_fact rolls a live cluster between a pinned baseline binary and HEAD one node at a time in both directions under the acked-facts oracle; at 1.0 this task is that test pointed at two release tags plus a scheduled CI job. Until then the pinned baseline doubles as the pre-release compatibility tripwire."

**0041 — Resource governance (admission caps, per-client quotas, bounded state)**

- `0041-T6` ⬜ planned: Per-session byte bound on the offline queue — MQTTD_MAX_QUEUED_BYTES beside the count bound, first-reached wins, same queue_overflow semantics, counted; SIZING.md updated (2026-08-04 amendment) — "Still open, and issue #241 deliberately did NOT claim it: T10 byte-bounded the IN-MEMORY flow-control backlog and left MQTTD_MAX_QUEUED_BYTES unclaimed for this task, so the name keeps meaning the offline (disk) queue. Why it was not folded in: ReplicatedSessionStore::enqueue_with_expiry enforces the count cap from log.live_range() — O(1), never materializing the queue — so an exact byte total needs a PERSISTED per-session counter that stays exact across append, truncate, crash recovery, quorum replication and on nodes that merely FOLLOW a group. MemorySessionStore could do it trivially, which is the trap: a byte knob exact on the ephemeral backend and absent on the durable default is worse than no knob, because the operator's number would silently mean nothing on the deployment that matters. Mosquitto's max_queued_bytes is THIS task, not T10 — COMPARISON's row states the axis split so the parity claim is not overread."
- `0041-T7` ⬜ planned: Bridge-spool byte bound — max_bytes joins max_messages in the mqtt-bridge spool, drop-oldest, counted (2026-08-04 amendment)
- `0041-T9` ⬜ planned: "Per-store disk REFUSAL, narrowed (issue #243): a share of MQTTD_STORE_MAX_BYTES for the two stores whose growth has a refusable client write — sessions and retained — with the same T4 behaviors counted per store; replicas and lease stay on the GLOBAL axis by decision (their growth is peers' committed appends and consensus, and refusing there would thin the group's replica count rather than enforce a watermark). The visibility half shipped under T14 (a WARN naming any store above 70% of the aggregate mark)" — "Blocked on a hub.rs change this lane deliberately did not make (another lane owns the file): (1) hub::BrownoutAxis must carry a store dimension while keeping brownout{axis} bounded-cardinality (ADR 0020 §3); (2) the CONSUMERS must stop reading the global OR — the plan pass / durable_append must ask 'is SESSIONS over?' and the retained-set path 'is RETAINED over?'; (3) the ADR must state that replicas/lease map to the global axis, or the semantics become 'some stores are enforceable and we did not say which'; (4) a hub test: sessions over its share and retained under => a new retained topic is accepted while a durable enqueue is refused. The store-watcher half (per-store marks -> per-store axis commands) is ~15 lines in store_watch.rs; all the cost and risk are in (2) and (3)."
- `0041-T15` ⬜ planned: "Refuse the PUBLISHER at the flow-control backlog's byte bound instead of shedding already-acked entries (issue #241 follow-up): a new PublishRefusal variant decided at the #238 freeze point, with a peer-bus wire code (T12) and the version-skew behaviour that implies, moving ADR 0041 §5 and store_watch's growth-is-refused enumeration" — "Strictly more honest than what T10 shipped — the publisher would be TOLD rather than the broker silently dropping messages it had already acked — and the decision point already exists (the #238 plan/submit freeze is on-loop and pre-effect, so it can read the target session's backlog bytes and answer Refused before the append, effect-free and idempotent on retry). Deferred deliberately: see 0041-T10's notes for the four reasons. The better end state is an online drain that re-reads the durable log so the backlog becomes a WINDOW over the log rather than the only copy, which needs an off-loop lane read (ADR 0061) and is a design rather than an amendment."

**0044 — Release readiness: out-of-process cluster harness and continuous assurance**

- `0044-P9` ⬜ planned: Nightly performance tolerance gate — compare the criterion suites against the recorded baseline and FAIL beyond a stated tolerance, instead of printing numbers for a human to read

**0046 — File-based configuration (layered over env, hot-reloadable, GitOps-friendly)**

- `0046-T6` ⬜ planned: Unrecognized CLI arguments are an error, not silence — mqttd accepts only --check-config and --decommission and IGNORES everything else, so a typo (`--check-confg`) or an unsupported flag (`--version`) starts a real broker instead of failing; refuse unknown args at startup and print the accepted set (2026-08-07 amendment)

**0048 — Comparative performance benchmarking (published, reproducible, honest)**

- `0048-T3` ⬜ planned: The scaling curve — the same workload against 1/3/5 nodes, throughput and p99 vs node count; tests capability claim 1 and the ADR 0015 shared-subscription mechanism end to end; a flat curve is a finding to fix
- `0048-T4` ⬜ planned: Honesty rules + publication — versions/hardware/config/date stated; losing dimensions reported as prominently as winning ones; results in docs/benchmarks/ linked from the README; self-benchmark runs nightly (ADR 0044 P4), cross-broker re-run per release

**0051 — Evaluation readiness: an assessable, comparable, migratable first release**

- `0051-T8` ⬜ planned: Migration from NanoMQ — scripts/migrate/from-nanomq.py (listeners, TLS, auth, bridge config → common-subset TOML) + guide + fixture tests; same three converter rules
- `0051-T10` ⬜ planned: The bridge made assessable — a demo/ second security zone (Mosquitto upstream + mqtt-bridge with directional rules), a walkthrough doc, and the Grafana screenshot into the README
- `0051-T11` ⛔ blocked: The 1.0.0 freeze — after the bake window, run the ADR 0038 wire/schema review consciously, run the 0039-T3 skew smoke against two real tags, then the maintainer cuts the freeze tag — "Needs v0.9.0 shipped (T4), a bake window survived, and a second tag (0.9.x/0.10) to make the 0039-T3 adjacent-skew smoke real — impossible before two releases exist by definition."

**0053 — One crypto provider: aws-lc-rs everywhere, ring evicted**

- `0053-T5` ⬜ planned: FIPS-mode evaluation (aws-lc-rs fips feature — the ADR 0002 certified-builds line) and rcgen 0.13→0.14 Issuer migration

**0055 — The mqttd Kubernetes operator (`MqttdCluster` CRD, kube-rs controller)**

- `0055-T5` ⬜ planned: Unattended gossip key rotation — three key_accept phases as Secret/config rolls, each gated on swim_keys_accepted returning to 1 and config checksum convergence
- `0055-T6` ⬜ planned: Drain-aware rolls — operator-set annotation via Downward API consulted by preStop (hook shipped identically in chart and operator paths); shrink = full drain, roll = rejoin-and-catch-up

**0057 — Durable outbound in-flight state: exactly-once across a broker crash**

- `0057-T3` ⬜ planned: "Restore: rebuild `pending` from the table, resume at PUBLISH+DUP or PUBREL under the original id, seed the allocator past restored ids"

**0058 — The 1.0 stability contract: upgrade-in-place, never wipe-and-rejoin**

- `0058-T5` ⬜ planned: "The freeze flip at the v1.0.0 tag: README/RELEASING language, final surface audit"

**0059 — Bridge HA topology and message ordering**

- `0059-T5` ⬜ planned: "Optional active/passive mode (liveness signal, whole-key-space takeover) for small fleets"

**0060 — Bridge durability and acknowledgement contract**

- `0060-T8` ⬜ planned: "Fast path waits for the downstream PUBACK: correlate the destination's pkid back to the source obligation, closing the dispatch->ack window (ADR 0060 §5.1-5.2)"
