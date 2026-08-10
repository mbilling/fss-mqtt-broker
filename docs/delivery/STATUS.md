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
| [0004](../adr/0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default | Accepted | [13/16 done](0004-identity-and-authentication.md) | 1 deferred |
| [0005](../adr/0005-session-affinity.md) | Session affinity: relocate persistent sessions to their owner | Accepted | [4/6 done](0005-session-affinity.md) | 2 deferred |
| [0006](../adr/0006-consensus-and-replication.md) | Consensus & replication for durable sessions | Accepted | [11/11 done](0006-consensus-and-replication.md) | — |
| [0007](../adr/0007-durable-store-integration.md) | Wiring the durable cluster session store into the broker | Accepted | [9/9 done](0007-durable-store-integration.md) | — |
| [0008](../adr/0008-mqtt-5-codec.md) | MQTT 5.0 codec | Accepted | [9/9 done](0008-mqtt-5-codec.md) | — |
| [0009](../adr/0009-mqtt5-expiry.md) | MQTT 5.0 session & message expiry | Accepted | [3/3 done](0009-mqtt5-expiry.md) | — |
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
| [0022](../adr/0022-signed-gossip.md) | Per-node signed gossip (authenticated SWIM identity) | Accepted | [7/7 done](0022-signed-gossip.md) | — |
| [0023](../adr/0023-gossip-anti-replay.md) | Gossip anti-replay: persisted monotonic sequence + sliding window | Accepted | [6/6 done](0023-gossip-anti-replay.md) | — |
| [0024](../adr/0024-deterministic-testing.md) | Deterministic testing: inject time, synchronize causally, gate in CI | Accepted | [7/7 done](0024-deterministic-testing.md) | — |
| [0025](../adr/0025-boundary-bridge.md) | Boundary MQTT bridge to brokers in other security zones | Accepted | [13/14 done](0025-boundary-bridge.md) | 1 open |
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
| [0037](../adr/0037-durable-retained-messages.md) | Durable single-owner retained messages (clock-free convergence) | Accepted | [10/10 done](0037-durable-retained-messages.md) | — |
| [0038](../adr/0038-prerelease-compatibility-freeze.md) | Pre-release compatibility freeze (versioned wire, stamped schemas, final codecs) | Accepted | [4/4 done](0038-prerelease-compatibility-freeze.md) | — |
| [0039](../adr/0039-versioning-and-upgrade-policy.md) | Release versioning and upgrade policy (semver, adjacent skew, sequential majors) | Accepted | [2/3 done](0039-versioning-and-upgrade-policy.md) | 1 deferred |
| [0040](../adr/0040-revocation-reaches-live-state.md) | Revocation reaches live state (eviction on reload) | Accepted | [5/5 done](0040-revocation-reaches-live-state.md) | — |
| [0041](../adr/0041-resource-governance.md) | Resource governance (admission caps, per-client quotas, bounded state) | Accepted | [6/10 done](0041-resource-governance.md) | 4 open |
| [0042](../adr/0042-durable-plane-stress-harness.md) | Durable-plane stress and simulation harness | Accepted | [9/9 done](0042-durable-plane-stress-harness.md) | — |
| [0043](../adr/0043-elastic-cluster-resize.md) | Elastic cluster resize (grow, shrink, replace) | Accepted | [5/5 done](0043-elastic-cluster-resize.md) | — |
| [0044](../adr/0044-release-readiness-assurance.md) | Release readiness: out-of-process cluster harness and continuous assurance | Accepted | [7/8 done](0044-release-readiness-assurance.md) | 1 open |
| [0045](../adr/0045-release-engineering-and-distribution.md) | Release engineering and distribution (signed, reproducible, SBOM-attested) | Proposed | [5/6 done](0045-release-engineering-and-distribution.md) | 1 open |
| [0046](../adr/0046-file-based-configuration.md) | File-based configuration (layered over env, hot-reloadable, GitOps-friendly) | Accepted | [5/6 done](0046-file-based-configuration.md) | 1 open |
| [0047](../adr/0047-kubernetes-deployment.md) | Kubernetes deployment (Helm chart, StatefulSet, safe scale-down) | Accepted | [9/9 done](0047-kubernetes-deployment.md) | — |
| [0048](../adr/0048-comparative-benchmarking.md) | Comparative performance benchmarking (published, reproducible, honest) | Accepted | [2/4 done](0048-comparative-benchmarking.md) | 2 open |
| [0049](../adr/0049-voter-eligible-durable-ownership.md) | Durable ownership must be lease-eligible, and a degraded durable plane must be visible | Accepted | [3/3 done](0049-voter-eligible-durable-ownership.md) | — |
| [0050](../adr/0050-oidc-token-authentication.md) | OIDC-integrated token authentication (discovery, JWKS rotation, proven against a real IdP) | Accepted | [5/5 done](0050-oidc-token-authentication.md) | — |
| [0051](../adr/0051-evaluation-readiness.md) | Evaluation readiness: an assessable, comparable, migratable first release | Proposed | [4/11 done](0051-evaluation-readiness.md) | 7 open |
| [0052](../adr/0052-codec-succession.md) | Codec succession: postcard replaces bincode on every cluster surface | Accepted | [4/4 done](0052-codec-succession.md) | — |
| [0053](../adr/0053-single-crypto-provider-aws-lc-rs.md) | One crypto provider: aws-lc-rs everywhere, ring evicted | Accepted | [4/5 done](0053-single-crypto-provider.md) | 1 open |
| [0054](../adr/0054-operator-facing-state-surface.md) | Operator-facing state surface: `/statusz` + state gauges | Accepted | [5/5 done](0054-operator-facing-state-surface.md) | — |
| [0055](../adr/0055-kubernetes-operator.md) | The mqttd Kubernetes operator (`MqttdCluster` CRD, kube-rs controller) | Accepted | [7/11 done](0055-kubernetes-operator.md) | 4 open |
| [0056](../adr/0056-mqttui.md) | `mqttui`: a terminal UI for running the demo, migration and test scripts | Proposed | [0/6 done](0056-mqttui.md) | 6 open |

## Open and deferred work

**0001 — Session durability in a horizontally-scalable cluster**

- `0001-T11` 💤 deferred: Client-facing reconnect during promotion + spec-legal QoS-1 redelivery bounds (takeover hardening) — takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass

**0004 — Identity model: mTLS Common Name first, deny by default**

- `0004-T13` 💤 deferred: Per-listener auth policies (each listener carrying its own authenticator/ACL) — "Needs the flat one-bind-per-transport Listeners struct (ADR 0046) to become a list of named listener definitions, each with its own policy and reload path — a config-model decision that earns its own record rather than an option bolted onto this one. The fourth item of the old bundled T11, hot ACL reload, was delivered by ADR 0032/0033 and reaches live state via ADR 0040."

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

- `0041-T6` ⬜ planned: Per-session byte bound on the offline queue — MQTTD_MAX_QUEUED_BYTES beside the count bound, first-reached wins, same queue_overflow semantics, counted; SIZING.md updated (2026-08-04 amendment)
- `0041-T7` ⬜ planned: Bridge-spool byte bound — max_bytes joins max_messages in the mqtt-bridge spool, drop-oldest, counted (2026-08-04 amendment)
- `0041-T9` ⬜ planned: Per-store disk bound — a share of MQTTD_STORE_MAX_BYTES per redb store (sessions/retained/replicas/lease) so one store cannot consume the whole watermark and brown out the others; same T4 refusal behaviors, counted per store (2026-08-07 amendment)
- `0041-T10` ⬜ planned: Per-connection write-buffer bound — MAX_BACKLOG (hub.rs, 10 000 messages per stalled subscriber) becomes byte-aware and configurable; today it is a hard-coded count, so a slow consumer's backlog is bounded in messages but unbounded in bytes and cannot be tuned (2026-08-07 amendment)

**0044 — Release readiness: out-of-process cluster harness and continuous assurance**

- `0044-P9` ⬜ planned: Nightly performance tolerance gate — compare the criterion suites against the recorded baseline and FAIL beyond a stated tolerance, instead of printing numbers for a human to read

**0045 — Release engineering and distribution (signed, reproducible, SBOM-attested)**

- `0045-T5` 🚧 in-progress: SBOM per release (CycloneDX or SPDX) attached to the release and image; cargo-deny/cargo-audit run on the release commit; RELEASING.md + README verify docs; cut the first 0.x release — "CycloneDX SBOM (cargo-cyclonedx) + cargo-deny/cargo-audit gate on the release commit + RELEASING.md + README Install/verify — all in place; remaining: cut the first 0.x release (a maintainer signed-tag push, gated on the ADR 0044 readiness checklist)"

**0046 — File-based configuration (layered over env, hot-reloadable, GitOps-friendly)**

- `0046-T6` ⬜ planned: Unrecognized CLI arguments are an error, not silence — mqttd accepts only --check-config and --decommission and IGNORES everything else, so a typo (`--check-confg`) or an unsupported flag (`--version`) starts a real broker instead of failing; refuse unknown args at startup and print the accepted set (2026-08-07 amendment)

**0048 — Comparative performance benchmarking (published, reproducible, honest)**

- `0048-T3` ⬜ planned: The scaling curve — the same workload against 1/3/5 nodes, throughput and p99 vs node count; tests capability claim 1 and the ADR 0015 shared-subscription mechanism end to end; a flat curve is a finding to fix
- `0048-T4` ⬜ planned: Honesty rules + publication — versions/hardware/config/date stated; losing dimensions reported as prominently as winning ones; results in docs/benchmarks/ linked from the README; self-benchmark runs nightly (ADR 0044 P4), cross-broker re-run per release

**0051 — Evaluation readiness: an assessable, comparable, migratable first release**

- `0051-T4` ⬜ planned: Cut v0.9.0 — flip ADR 0045 to Accepted, maintainer pushes the signed tag per RELEASING.md, verify the pipeline's artifacts end to end (first real signatures + SBOM complete 0045-T3/T5)
- `0051-T6` ⬜ planned: Migration from Mosquitto — scripts/migrate/from-mosquitto.py (mosquitto.conf → ADR 0046 TOML, acl_file → ACL TOML, bridge blocks → mqtt-bridge rules) + guide + fixture tests; loud unmapped report, secrets never transformed, output must pass --check-config
- `0051-T7` ⬜ planned: Migration from EMQX — scripts/migrate/from-emqx.py (listeners, TLS, authn/authz sources, bridges → common-subset TOML) + guide + fixture tests; same three converter rules
- `0051-T8` ⬜ planned: Migration from NanoMQ — scripts/migrate/from-nanomq.py (listeners, TLS, auth, bridge config → common-subset TOML) + guide + fixture tests; same three converter rules
- `0051-T9` 🚧 in-progress: NanoMQ and VerneMQ join the bench harness (amends ADR 0048's competitor set; VerneMQ under the disclosed-posture fairness terms in 0048's 2026-08-03 amendment) and the first comparative results are published to docs/benchmarks/ under 0048-T4's honesty rules — "Harness wired 2026-08-03: compose profiles vernemq (vernemq/vernemq:2.1.1 — 'latest' is stale=2.0.1, 2.1.2+ are pre-releases; EULA accepted = testing use; env-mapped mTLS listener, require_certificate on) and nanomq (emqx/nanomq:0.25.5-slim — the smallest variant WITH TLS, same variant both postures; configs/nanomq.conf with verify_peer+fail_if_no_peer_cert), run.sh broker list + env.txt versions, bench/README fairness notes (VerneMQ node-local durable queues; NanoMQ inflight window unenforced; EMQX 5.8.6 pin flagged for re-review — last Apache line, EOL'd; current is BSL 6.x). NOT yet smoke-run: no docker daemon on the wiring machine — smoke is the next action on a docker host. Publication (the 0048-T4 gate) additionally needs the dedicated-host run and the maintainer's EMQX re-pin decision."
- `0051-T10` ⬜ planned: The bridge made assessable — a demo/ second security zone (Mosquitto upstream + mqtt-bridge with directional rules), a walkthrough doc, and the Grafana screenshot into the README
- `0051-T11` ⛔ blocked: The 1.0.0 freeze — after the bake window, run the ADR 0038 wire/schema review consciously, run the 0039-T3 skew smoke against two real tags, then the maintainer cuts the freeze tag — "Needs v0.9.0 shipped (T4), a bake window survived, and a second tag (0.9.x/0.10) to make the 0039-T3 adjacent-skew smoke real — impossible before two releases exist by definition."

**0053 — One crypto provider: aws-lc-rs everywhere, ring evicted**

- `0053-T5` ⬜ planned: FIPS-mode evaluation (aws-lc-rs fips feature — the ADR 0002 certified-builds line) and rcgen 0.13→0.14 Issuer migration

**0055 — The mqttd Kubernetes operator (`MqttdCluster` CRD, kube-rs controller)**

- `0055-T5` ⬜ planned: Unattended gossip key rotation — three key_accept phases as Secret/config rolls, each gated on swim_keys_accepted returning to 1 and config checksum convergence
- `0055-T6` ⬜ planned: Drain-aware rolls — operator-set annotation via Downward API consulted by preStop (hook shipped identically in chart and operator paths); shrink = full drain, roll = rejoin-and-catch-up
- `0055-T8` ⬜ planned: Packaging + docs — deploy/helm/mqttd-operator chart (Deployment + CRD + namespaced RBAC), operator image in the ADR 0045 release pipeline (signed/reproducible/SBOM), OPERATIONS.md operator mode, COMPARISON.md Kubernetes cell update
- `0055-T10` ⬜ planned: ExpandPVC must also raise store_max_bytes through the config contract — expanding the volume without moving the watermark leaves the brownout it exists to clear still raised (ADR 0055 section 3.2, second half)

**0056 — `mqttui`: a terminal UI for running the demo, migration and test scripts**

- `0056-T1` ⬜ planned: The manifest + headless runner + the CI completeness guard — tasks.toml declaring every runnable script with its prerequisites and env surface, `mqttui --list` / `mqttui --run <id>`, and a test that fails when a script is missing from it — "Deliberately first, and useful with no UI at all: it proves the data model, and the manifest is machine-checked documentation of the operational surface on its own. The completeness guard is the load-bearing piece (ADR 0056 §3) — a launcher that silently shows 14 of 23 scripts becomes the list people trust. If phase 1 turns out to be sufficient, a justfile over the manifest is a legitimate place to stop; the manifest is where the value is, not the TUI."
- `0056-T2` ⬜ planned: The terminal UI — collapsible group/task tree, detail pane that becomes the output pane while running, cancel by process group — "LAYOUT SETTLED 2026-08-10. Two panes, not three: at 80x24 a third pane leaves ~6 lines of output, useless for a bench run — the right pane is Detail while browsing and Output while running, since you are either choosing or watching. Collapsible group tree (not a flat filtered list) because discoverability is the whole point and the set is expected to grow. Follow-mode auto-scrolls until the user scrolls up, then stops. A finished run leads with the verdict; a FAILED run jumps to the first FAIL/FATAL/error line rather than the tail. ONE RUN AT A TIME, enforced: these scripts bind fixed ports and start containers, and bench/run.sh explicitly requires an otherwise-idle host, so concurrency would produce failures that look like broker bugs."
- `0056-T3` ⬜ planned: Preflight + inline env editing — "The preflight block is the highest-value part of the UI: it turns 'run it and find out' into 'you are missing mosquitto-clients', named BEFORE the run rather than as a FATAL partway through. A task with missing required tools cannot be started at all, and the manifest carries an install hint per platform, because 'install kind' is where a newcomer stalls. Env editing is INLINE, not modal — a modal hides the description you are editing against. Manifest tasks may carry a `caution` string; bench/run.sh (pins the host, results invalid otherwise) and kind-smoke.sh genuinely need one. DECIDED 2026-08-10: no persisted last-run history — it is state that lies after a git pull, for little gain."
- `0056-T5` ⬜ planned: Environment panel — docker, kube context, kind clusters, compose stacks, stray processes, ports; with explicit cleanup actions — "Requested 2026-08-10, and justified on the spot: probing this machine while designing it found TWENTY orphaned mqttd processes from the day's test runs, invisible until asked for. The kube CONTEXT is the safety feature — kind-smoke.sh and operator-e2e.sh run kubectl against whatever is current, so showing `kube: prod-eu-west` before the user presses enter is the difference between a smoke test and an incident. Measured probe costs decide the polling: docker ps 0.01s, kind get clusters 0.06s, kubectl current-context 0.22s (2s timer, off the UI thread, bounded so an unreachable context shows `unreachable` instead of freezing); docker compose ls 1.9s (on demand only, staleness shown). Probing is read-only and unconditional; cleanup is never automatic and always confirmed with the specific processes/clusters listed — a tool that kills things you did not ask it to kill is worse than one that only shows them."
- `0056-T6` ⬜ planned: Cancel and quit tear down what they started — and VERIFY, reporting what survived — "DECIDED 2026-08-10: quitting must not leave orphans. What is achievable is stated precisely so this does not become an unkeepable claim. GUARANTEED: signal the process GROUP (so each script's own `trap EXIT` runs, which is what removes its brokers, containers and temp dirs) and WAIT for the group rather than detaching. NOT GUARANTEED: a script whose trap is buggy, or which was SIGKILLed, can still leak — mqttui cannot make another program's cleanup correct. So it signals, waits, then VERIFIES (stray processes, kind clusters, compose stacks) and reports what survived with an offer to remove it, leaving anything ignored visible in the environment panel. Claiming 'no orphans' outright would be the same unfalsifiable shape as the compose health check that could not fail (ADR 0047 T9)."
- `0056-T4` ⬜ planned: Decide developer-tool vs user-facing, and record it — "OPEN QUESTION, not a build task. It changes ADR 0056 §1: a user-facing launcher ships in the release, which puts ratatui back into the audited dependency graph and makes the separate workspace pointless. This record covers the developer tool only; a user-facing launcher earns its own ADR. Listed as a task so the question is closed deliberately rather than drifted past."
