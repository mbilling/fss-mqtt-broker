# Delivery status

> **Generated** by `scripts/gen-status.py` from the frontmatter in each
> `docs/delivery/NNNN-*.md`. Do not edit by hand. See
> [README.md](README.md) for the artifact model and status vocabulary.

## Decisions and their build progress

| ADR | Title | Decision | Tasks | Open / deferred |
|-----|-------|----------|-------|-----------------|
| 0001 | Session durability in a horizontally-scalable cluster | Accepted | 10/11 done | 1 deferred |
| 0002 | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted | 7/10 done | 3 deferred |
| 0003 | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted | 8/9 done | — |
| 0004 | Identity model: mTLS Common Name first, deny by default | Accepted | 8/11 done | 3 deferred |
| 0005 | Session affinity: relocate persistent sessions to their owner | Accepted | 3/6 done | 3 deferred |
| 0006 | Consensus & replication for durable sessions | Accepted | 11/11 done | — |
| 0007 | Wiring the durable cluster session store into the broker | Accepted | 8/9 done | 1 deferred |
| 0008 | MQTT 5.0 codec | Accepted | 8/8 done | — |
| 0009 | MQTT 5.0 session & message expiry | Accepted | 3/3 done | — |
| 0010 | Shared subscriptions | Accepted | 7/8 done | 1 deferred |
| 0011 | MQTT 5.0 topic aliases | Accepted | 7/7 done | — |
| 0012 | MQTT 5.0 flow control (Receive Maximum) | Accepted | 6/6 done | — |
| 0013 | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted | 8/9 done | 1 deferred |
| 0014 | Cross-node retained-message replication | Accepted | 6/9 done | 3 deferred |
| 0015 | Cluster-wide shared subscriptions | Accepted | 8/8 done | — |
| 0016 | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted | 3/4 done | 1 open |
| 0017 | Durable attach waits for an authoritative session, never downgrades | Accepted | 8/9 done | 1 deferred |
| 0018 | On-disk persistence for durable state | Accepted | 7/8 done | 1 deferred |
| 0019 | Graceful shutdown and connection draining | Accepted | 7/9 done | 2 deferred |
| 0020 | Metrics and runtime observability | Accepted | 9/9 done | — |
| 0021 | Bounded lease-consensus voter set | Proposed | 0/9 done | 9 open |
| 0022 | Per-node signed gossip (authenticated SWIM identity) | Accepted | 5/7 done | 2 deferred |
| 0023 | Gossip anti-replay: persisted monotonic sequence + sliding window | Accepted | 6/6 done | — |
| 0024 | Deterministic testing: inject time, synchronize causally, gate in CI | Accepted | 6/7 done | 1 deferred |
| 0025 | Boundary MQTT bridge to brokers in other security zones | Proposed | 0/11 done | 11 open |
| 0026 | Lease-group raft timing tolerant of durable-storage latency | Accepted | 7/7 done | — |
| 0027 | Group-commit for the durable replica apply path | Accepted | 4/4 done | — |
| 0028 | Link-gated lease-group voter admission | Accepted | 3/3 done | — |
| 0029 | Durable sessions by default | Accepted | 3/3 done | — |

## Open and deferred work

**0001 — Session durability in a horizontally-scalable cluster**

- `0001-T11` 💤 deferred: Client-facing reconnect during promotion + spec-legal QoS-1 redelivery bounds (takeover hardening) — takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass

**0002 — Transport security: TLS 1.3 everywhere, mTLS on the cluster bus**

- `0002-T8` 💤 deferred: CRL / OCSP stapling — no revocation checking in tree (rg crl|ocsp|revocation -> none); pairs with hot-reloadable policy, Capability Plan §3
- `0002-T9` 💤 deferred: Certificate rotation / hot-reload without dropping connections — TLS contexts built once at startup; no reload path exists; unblocks with hot-reloadable policy work
- `0002-T10` 💤 deferred: WebSocket-over-TLS listener — Transport::WebSocketTls enum variant exists but no listener/upgrade path; scheduled for Phase 4

**0004 — Identity model: mTLS Common Name first, deny by default**

- `0004-T9` 💤 deferred: Full OIDC discovery / JWKS rotation; MQTT5 enhanced auth after v5 codec — step 6 takes a single static key; enhanced auth waits on the v5 codec milestone
- `0004-T10` 💤 deferred: Delivery-time ACL re-check in the hub (enforcement is subscription-time only) — documented known limitation; needed only if policies change under live subscriptions; tracked with hot ACL reload
- `0004-T11` 💤 deferred: SAN-based identity, per-listener auth policies, hot ACL reload, %c (client-id) substitution — %c deferred until the Authorizer trait carries the client id; the rest are future config options

**0005 — Session affinity: relocate persistent sessions to their owner**

- `0005-P2c` 💤 deferred: Delivery/lifecycle hardening of the splice (best-effort on half-close) — splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up
- `0005-P2d` 💤 deferred: Durability across owner loss (ephemeral mode until replication) — owner death mid-session drops the session; durability is workstream E (ADR 0006), not this ADR
- `0005-P3` 💤 deferred: MQTT 5 Server-Reference redirect replacing the relay for v5 clients — needs the v5 codec and v5 clients; the proxy serves 3.1.1 and v5 alike until then

**0007 — Wiring the durable cluster session store into the broker**

- `0007-T8` 💤 deferred: Dynamic-reconfiguration hardening under rapid churn (flap -> ephemeral degrade) — v1 debounces stable join/leave; rapid flapping / lost-quorum degrades to ADR 0005 ephemeral per the accepted limitation; no flap-stress proof exists yet

**0010 — Shared subscriptions**

- `0010-T7` 💤 deferred: Subscription-Identifier handling for shared subscriptions — ADR 0010 Consequences notes no Subscription-Identifier handling yet; out of scope for the routing lever

**0013 — MQTT 5.0 enhanced authentication (AUTH exchange)**

- `0013-T8` 💤 deferred: Server-initiated re-auth (server sends AUTH 0x19 to demand re-authentication) — ADR section 4 explicitly defers this — needs a trigger mechanism and interacts with the select-loop outbound path; only client-initiated re-auth is implemented (no server-side AUTH 0x19 send exists in conn.rs).

**0014 — Cross-node retained-message replication**

- `0014-T6` 💤 deferred: Digest-diff back-fill (avoid re-sending the whole retained set on every link-up) — ADR §3 leaves this as a later optimization; current back-fill re-sends the full set on each link-up (no digest code in the tree).
- `0014-T7` 💤 deferred: Partition-heal conflict reconciliation (two nodes holding different values for the same topic) — ADR §3 leaves divergence unresolved — gap-fill keeps each side's own value; reconciling needs per-message timestamps / version vectors, out of scope.
- `0014-T8` 💤 deferred: Chunking a very large retained snapshot beyond the peer frame limit — ADR §3 — snapshot size is bounded by the peer frame limit; chunking is deferred.

**0016 — SWIM membership stability (dead-node fencing + false-positive resistance)**

- `0016-T4` ⬜ planned: Failure-domain-aware voter selection (interaction with ADR 0021) — bounded-voter work (ADR 0021) should pick voters across failure domains; revisit when 0021 is built

**0017 — Durable attach waits for an authoritative session, never downgrades**

- `0017-T9` 💤 deferred: Make recovery deadline/backoff configurable (currently constants) — ATTACH_RECOVERY_TIMEOUT/BACKOFF are constants for now; ADR defers promoting them to config until an operator need appears

**0018 — On-disk persistence for durable state**

- `0018-T7` 💤 deferred: Process-kill (SIGKILL mid-write) crash-consistency test — rests on redb's own ACID/crash suite; an in-repo subprocess kill test adds machinery for modest marginal coverage

**0019 — Graceful shutdown and connection draining**

- `0019-T8` 💤 deferred: Lease-leadership transfer when the leaving node is the Raft leader — "Spike 2026-06-25 (openraft 0.9 transfer-API evaluation, the task's stated prerequisite): openraft 0.9.24 exposes NO public leadership-transfer/TimeoutNow API — Trigger has only elect/heartbeat/snapshot/purge_log. change_membership-remove-self steps the leader down internally (raft_core.rs:1311 -> leader_step_down) but does not provoke an immediate election, so the remaining voters still wait out their election timeout: it does not close the gap. Trigger::transfer_leader exists only on the alpha-only 0.10 line (latest 0.10.0-alpha.23, Jun 2026; no beta/RC/stable, no v0.9->v0.10 upgrade guide; maintainer keeps 0.9.24 as the production default). Deferred pending a stable openraft release exposing transfer_leader — pulling an alpha into the consensus core is a poor trade for a bounded ~1.5-3s graceful-leave gap (relaxed ADR 0026 timing) that already degrades safely via survivors' election."
- `0019-T9` 💤 deferred: In-flight QoS settle / hub Drain command — drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown

**0021 — Bounded lease-consensus voter set**

- `0021-T1` ⬜ planned: MQTTD_LEASE_VOTERS config (default 5, odd; effective = min(N, live_eligible))
- `0021-T2` ⬜ planned: durable_node.rs - replace desired=all-members with alive set + RaftView passed to reconciler
- `0021-T3` ⬜ planned: Sticky vacancy-fill voter selection (promote lowest-id alive learner; never demote a live voter on join)
- `0021-T4` ⬜ planned: All members added as learners so the committed lease log replicates to every node
- `0021-T5` ⬜ planned: Reconciler reshape - decide returns target (voters, learners); apply_action adds/promotes/demotes-to-learner/drops-departed
- `0021-T6` ⬜ planned: Founder/bootstrap unaffected (sole-voter bootstrap then grows capped at N)
- `0021-T7` ⬜ planned: Pure policy tests (>N -> exactly N voters; dead voter replaced by lowest-id learner; high-id join no voter change; learner-owner reads lease; N>cluster all-voters; N=1 single voter)
- `0021-T8` ⬜ planned: Integration - 5+-node durable cluster with bounded voter set; learner-owned session survives a non-voter and a voter failure
- `0021-T9` ⬜ planned: Re-run openraft storage conformance (asserted unaffected)

**0022 — Per-node signed gossip (authenticated SWIM identity)**

- `0022-T6` 💤 deferred: Cert caching by fingerprint (send full cert periodically, fingerprint otherwise) to shrink datagrams — size optimisation only; inline self-contained certs are correct and bootstrap-safe, just larger
- `0022-T7` 💤 deferred: Certificate expiry / revocation handling for gossip certs — same deferred concern as peer-bus mTLS (ADR 0002); a CA-chained cert is trusted for gossip until revocation lands cluster-wide

**0024 — Deterministic testing: inject time, synchronize causally, gate in CI**

- `0024-T7` 💤 deferred: Deterministic simulation harness (madsim/turmoil-style) for seed-reproducible cluster ordering races — the gold standard for distributed ordering races, but a large investment; per-test causal barriers (T5) and bounded poll-retry close the flakes seen today without it. Revisit if cluster-ordering flakes recur or a seed-reproducible failure is needed.

**0025 — Boundary MQTT bridge to brokers in other security zones**

- `0025-T1` ⬜ planned: New mqtt-bridge crate and binary skeleton (MQTT client built on mqtt-codec/mqtt-net)
- `0025-T2` ⬜ planned: Config model and validation (upstreams, per-rule direction/filter/remap/qos, deny-by-default)
- `0025-T3` ⬜ planned: Client engine (connect cluster + each upstream over TLS/mTLS, subscribe/publish, reconnect with backoff)
- `0025-T4` ⬜ planned: Directional forwarding and topic remap; a one-way rule never opens the reverse path in code
- `0025-T5` ⬜ planned: Loop prevention via fss-bridge-hop-count user property + configurable hop-count-limit (plus remap discipline)
- `0025-T6` ⬜ planned: HA via cluster-side shared subscriptions and a persistent session (dedup across instances)
- `0025-T7` ⬜ planned: Bounded disk-backed store-and-forward spool for transient outages, replayed on reconnect
- `0025-T8` ⬜ planned: Per-side least-privilege credentials (publish-only/subscribe-only) and per-upstream mTLS identity + audit
- `0025-T9` ⬜ planned: Bridge observability (forwarded/dropped per upstream+direction, lag, reconnects) via mqtt-observability + OTLP
- `0025-T10` ⬜ planned: Adversarial tests (one-way never leaks reverse; loop prevention; ACL deny; reconnect/spool; multi-upstream; shared-sub dedup)
- `0025-T11` ⬜ planned: Demo + docs — bridge the cluster to a second isolated broker, one-way and bidirectional
