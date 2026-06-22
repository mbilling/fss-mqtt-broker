# Delivery status

> **Generated** by `scripts/gen-status.py` from the frontmatter in each
> `docs/delivery/NNNN-*.md`. Do not edit by hand. See
> [README.md](README.md) for the artifact model and status vocabulary.

## Decisions and their build progress

| ADR | Title | Decision | Tasks | Open / deferred |
|-----|-------|----------|-------|-----------------|
| 0001 | Session durability in a horizontally-scalable cluster | Accepted | 8/11 done | 3 deferred |
| 0002 | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted | 7/10 done | 3 deferred |
| 0003 | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted | 6/9 done | 1 open, 1 deferred |
| 0004 | Identity model: mTLS Common Name first, deny by default | Accepted | 8/11 done | 3 deferred |
| 0005 | Session affinity: relocate persistent sessions to their owner | Accepted | 3/6 done | 3 deferred |
| 0006 | Consensus & replication for durable sessions | Accepted | 10/11 done | 1 deferred |
| 0007 | Wiring the durable cluster session store into the broker | Accepted | 7/9 done | 2 deferred |
| 0008 | MQTT 5.0 codec | Accepted | 6/8 done | 2 deferred |
| 0009 | MQTT 5.0 session & message expiry | Accepted | 2/3 done | 1 deferred |
| 0010 | Shared subscriptions | Accepted | 6/8 done | 2 deferred |
| 0011 | MQTT 5.0 topic aliases | Accepted | 5/7 done | 2 deferred |
| 0012 | MQTT 5.0 flow control (Receive Maximum) | Accepted | 5/6 done | 1 deferred |
| 0013 | MQTT 5.0 enhanced authentication (AUTH exchange) | Accepted | 7/9 done | 2 deferred |
| 0014 | Cross-node retained-message replication | Accepted | 5/9 done | 4 deferred |
| 0015 | Cluster-wide shared subscriptions | Accepted | 6/8 done | 2 deferred |
| 0016 | SWIM membership stability (dead-node fencing + false-positive resistance) | Accepted | 3/4 done | 1 open |
| 0017 | Durable attach waits for an authoritative session, never downgrades | Accepted | 8/9 done | 1 deferred |
| 0018 | On-disk persistence for durable state | Accepted | 7/8 done | 1 deferred |
| 0019 | Graceful shutdown and connection draining | Accepted | 7/9 done | 2 deferred |
| 0020 | Metrics and runtime observability | Proposed | 0/9 done | 8 open, 1 deferred |
| 0021 | Bounded lease-consensus voter set | Proposed | 0/9 done | 9 open |
| 0022 | Per-node signed gossip (authenticated SWIM identity) | Accepted | 5/7 done | 2 deferred |
| 0023 | Gossip anti-replay: persisted monotonic sequence + sliding window | Accepted | 0/6 done | 6 open |

## Open and deferred work

**0001 — Session durability in a horizontally-scalable cluster**

- `0001-T9` 💤 deferred: Default-on durable sessions (retire the ephemeral default) — MQTTD_DURABLE_SESSIONS is off by default, so the shipping default is ephemeral mode — an owner's death drops its queues; durability requires enabling the durable store (R>=2 / quorum)
- `0001-T10` 💤 deferred: Durable session-expiry deadline across takeover (ADR 0009 phase 3) — message-expiry deadline is durable in the log, but the session-expiry timer restarts on takeover; the one open durability item (see ADR 0009 / delivery 0009-T3)
- `0001-T11` 💤 deferred: Client-facing reconnect during promotion + spec-legal QoS-1 redelivery bounds (takeover hardening) — takeover-serve is proven through the store (F-d); client-facing MQTT reconnect mid-promotion and redelivery bounds deferred to a later hardening pass

**0002 — Transport security: TLS 1.3 everywhere, mTLS on the cluster bus**

- `0002-T8` 💤 deferred: CRL / OCSP stapling — no revocation checking in tree (rg crl|ocsp|revocation -> none); pairs with hot-reloadable policy, Capability Plan §3
- `0002-T9` 💤 deferred: Certificate rotation / hot-reload without dropping connections — TLS contexts built once at startup; no reload path exists; unblocks with hot-reloadable policy work
- `0002-T10` 💤 deferred: WebSocket-over-TLS listener — Transport::WebSocketTls enum variant exists but no listener/upgrade path; scheduled for Phase 4

**0003 — Gossip-plane authentication: keyed MAC on SWIM datagrams**

- `0003-T6` 💤 deferred: Rejected-datagram metrics counter (operator signal for dropped gossip) — drop path logs at debug only, no metric; lands with the observability phase (no gossip-reject counter in mqtt-observability)
- `0003-T7` 🚧 in-progress: Anti-replay window / per-peer nonces — being implemented as ADR 0023 — a clock-free, restart-safe persisted-sequence + sliding-window design bound to ADR 0022's authenticated identity

**0004 — Identity model: mTLS Common Name first, deny by default**

- `0004-T9` 💤 deferred: Full OIDC discovery / JWKS rotation; MQTT5 enhanced auth after v5 codec — step 6 takes a single static key; enhanced auth waits on the v5 codec milestone
- `0004-T10` 💤 deferred: Delivery-time ACL re-check in the hub (enforcement is subscription-time only) — documented known limitation; needed only if policies change under live subscriptions; tracked with hot ACL reload
- `0004-T11` 💤 deferred: SAN-based identity, per-listener auth policies, hot ACL reload, %c (client-id) substitution — %c deferred until the Authorizer trait carries the client id; the rest are future config options

**0005 — Session affinity: relocate persistent sessions to their owner**

- `0005-P2c` 💤 deferred: Delivery/lifecycle hardening of the splice (best-effort on half-close) — splice is best-effort on half-close; a delivery/lifecycle hardening pass is a documented follow-up
- `0005-P2d` 💤 deferred: Durability across owner loss (ephemeral mode until replication) — owner death mid-session drops the session; durability is workstream E (ADR 0006), not this ADR
- `0005-P3` 💤 deferred: MQTT 5 Server-Reference redirect replacing the relay for v5 clients — needs the v5 codec and v5 clients; the proxy serves 3.1.1 and v5 alike until then

**0006 — Consensus & replication for durable sessions**

- `0006-P3c-i` 💤 deferred: Replace in-memory backend O(n) cap count with a rebuildable per-key index — correctness-neutral; in-memory backend's cap count reads the whole log (O(n)) per the 3c "remaining (minor)" note

**0007 — Wiring the durable cluster session store into the broker**

- `0007-T8` 💤 deferred: Dynamic-reconfiguration hardening under rapid churn (flap -> ephemeral degrade) — v1 debounces stable join/leave; rapid flapping / lost-quorum degrades to ADR 0005 ephemeral per the accepted limitation; no flap-stress proof exists yet
- `0007-T9` 💤 deferred: Connection-driven next_packet_id over the durable store — store impls next_packet_id but conn.rs never calls it; outbound packet-id allocation stays hub-side, so the per-packet durable path is record_received/clear_received only

**0008 — MQTT 5.0 codec**

- `0008-T7` 💤 deferred: Codec-owned property validation (allowed-on-packet-type + duplicate non-repeatable -> Protocol Error) — properties.rs deliberately round-trips any well-formed block; per-packet allow-list and duplicate-rejection are not implemented and have no tests; enforcement currently lives above the wire
- `0008-T8` 💤 deferred: Shared reason-code constants module (reason::SUCCESS, reason::NOT_AUTHORIZED, ...) — reason codes carried as bare u8 literals; no shared reason-constants module in mqtt-codec, only broker-local consts in conn.rs

**0009 — MQTT 5.0 session & message expiry**

- `0009-P3` 💤 deferred: Durable expiry deadline (persist disconnect time so takeover preserves the clock) — expiry deadline is in-memory only (hub expiring HashMap of Instant); SessionMeta snapshot has no disconnect-time field, so a takeover restarts the clock; documented §2 follow-up gated on workstream F

**0010 — Shared subscriptions**

- `0010-T7` 💤 deferred: Subscription-Identifier handling for shared subscriptions — ADR 0010 Consequences notes no Subscription-Identifier handling yet; out of scope for the routing lever
- `0010-T8` 💤 deferred: Indexed shared-group selection (avoid per-publish member-list clone) — matching/snapshot clone matching groups' member lists per publish; small in practice, ADR 0010 flags indexed selection as a later optimization

**0011 — MQTT 5.0 topic aliases**

- `0011-T6` 💤 deferred: Configurable server Topic Alias Maximum — SERVER_TOPIC_ALIAS_MAX is a fixed constant (16) in conn.rs, not yet configurable (ADR 0011 §2 / Consequences); still holds
- `0011-T7` 💤 deferred: Emit DISCONNECT 0x94 (Topic Alias Invalid) instead of bare close — invalid alias closes the connection rather than sending DISCONNECT 0x94; folded into the later act-on-v5-reason-codes work (ADR 0011 §2)

**0012 — MQTT 5.0 flow control (Receive Maximum)**

- `0012-T6` 💤 deferred: Strictly enforce client to server Receive Maximum (DISCONNECT 0x93 on overrun) — client to server direction is advertised but NOT strictly enforced; broker acks inbound promptly so it self-limits, DISCONNECT 0x93 folded into act-on-v5-reason-codes work (ADR 0012 §3); still holds

**0013 — MQTT 5.0 enhanced authentication (AUTH exchange)**

- `0013-T8` 💤 deferred: Server-initiated re-auth (server sends AUTH 0x19 to demand re-authentication) — ADR section 4 explicitly defers this — needs a trigger mechanism and interacts with the select-loop outbound path; only client-initiated re-auth is implemented (no server-side AUTH 0x19 send exists in conn.rs).
- `0013-T9` 💤 deferred: Dedicated per-round AUTH-exchange timeout — the exchange blocks on the client between rounds with no dedicated timeout (same surface as existing pre-CONNACK reads); ADR Consequences flags this as a known limit.

**0014 — Cross-node retained-message replication**

- `0014-T6` 💤 deferred: Digest-diff back-fill (avoid re-sending the whole retained set on every link-up) — ADR §3 leaves this as a later optimization; current back-fill re-sends the full set on each link-up (no digest code in the tree).
- `0014-T7` 💤 deferred: Partition-heal conflict reconciliation (two nodes holding different values for the same topic) — ADR §3 leaves divergence unresolved — gap-fill keeps each side's own value; reconciling needs per-message timestamps / version vectors, out of scope.
- `0014-T8` 💤 deferred: Chunking a very large retained snapshot beyond the peer frame limit — ADR §3 — snapshot size is bounded by the peer frame limit; chunking is deferred.
- `0014-T9` 💤 deferred: Carry message-expiry interval on the cross-node peer link — ADR Consequences — cross-node delivery carries no message-expiry deadline (the peer link does not yet carry the interval); pre-existing carried limitation.

**0015 — Cluster-wide shared subscriptions**

- `0015-T7` 💤 deferred: Carry message-expiry deadline on cross-node SharedDeliver — ADR Consequences — SharedDeliver carries no message-expiry deadline, same carried limitation as RemotePublish; the peer link does not yet carry the interval.
- `0015-T8` 💤 deferred: Remote-member liveness awareness in the selector — ADR Consequences — selector does not know a remote member's liveness, so it may target a member offline on its home node (which then queues) even when a local member is online; an accepted, spec-permitted selection-quality trade-off.

**0016 — SWIM membership stability (dead-node fencing + false-positive resistance)**

- `0016-T4` ⬜ planned: Failure-domain-aware voter selection (interaction with ADR 0021) — bounded-voter work (ADR 0021) should pick voters across failure domains; revisit when 0021 is built

**0017 — Durable attach waits for an authoritative session, never downgrades**

- `0017-T9` 💤 deferred: Make recovery deadline/backoff configurable (currently constants) — ATTACH_RECOVERY_TIMEOUT/BACKOFF are constants for now; ADR defers promoting them to config until an operator need appears

**0018 — On-disk persistence for durable state**

- `0018-T7` 💤 deferred: Process-kill (SIGKILL mid-write) crash-consistency test — rests on redb's own ACID/crash suite; an in-repo subprocess kill test adds machinery for modest marginal coverage

**0019 — Graceful shutdown and connection draining**

- `0019-T8` 💤 deferred: Lease-leadership transfer when the leaving node is the Raft leader — avoids one election (~300-600ms) on a leaving leader; needs openraft 0.9 transfer-API evaluation first
- `0019-T9` 💤 deferred: In-flight QoS settle / hub Drain command — drain closes after current packet; durable state already protected by ADR 0018 + raft shutdown

**0020 — Metrics and runtime observability**

- `0020-T1` ⬜ planned: Add prometheus-client to mqtt-observability; Metrics registry + typed handles + render()
- `0020-T2` ⬜ planned: Serve GET /metrics from health.rs (replace the 404 + its test); MQTTD_METRICS_BIND option
- `0020-T3` ⬜ planned: Instrument connections/handshakes/auth/ACL/keepalive in conn.rs
- `0020-T4` ⬜ planned: Instrument publish/deliver, queue depth, evictions, inflight, retained/subs gauges in hub.rs
- `0020-T5` ⬜ planned: Instrument listener accepts/errors in main.rs
- `0020-T6` ⬜ planned: Instrument cluster (members/states, peer links, lease role/epoch, durable append latency/failures)
- `0020-T7` ⬜ planned: Cardinality discipline (no per-client/per-topic labels; fixed small label sets)
- `0020-T8` ⬜ planned: Tests (valid exposition render; publish round-trip moves counters; assert no high-cardinality labels)
- `0020-T9` 💤 deferred: Later OpenTelemetry/OTLP export behind the same registry — explicitly out of scope now; addable later without changing instrumentation per the ADR

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

**0023 — Gossip anti-replay: persisted monotonic sequence + sliding window**

- `0023-P1` ⬜ planned: Sliding replay window (RFC 6479 bitmap) — pure, accept/reject by sequence
- `0023-P2` ⬜ planned: Persisted monotonic sequence allocator (block reservation + fsync; resumes above last block on restart)
- `0023-P3` ⬜ planned: Wire format v3 in swim_auth (seq + signature; v1/v2 still understood; require/prefer/off)
- `0023-P4` ⬜ planned: Driver integration — per-sender windows keyed by the authenticated CN; reject replays
- `0023-P5` ⬜ planned: mqttd wiring — MQTTD_SWIM_REPLAY require/prefer/off, data-dir + signed require guards
- `0023-P6` ⬜ planned: Over-UDP integration test — a replayed datagram is rejected; live traffic flows; prefer accepts v2
