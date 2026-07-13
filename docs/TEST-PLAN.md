# End-to-end test plan

Status: living document. Tracks the integration-test strategy and the
sunshine/darksky scenario catalog the suite is working toward.

## Where we are

Integration tests live in `crates/mqttd/tests/` (13 files, ~4.7k lines) and
`crates/mqtt-cluster/tests/`. They start an **in-process broker over real TCP
loopback** (`Hub::new()` + `conn::handle`/`handle_stream`) and drive it through
sockets. Coverage of the **MQTT 3.1.1 + cluster** surface is solid:

| Area | Files |
|---|---|
| Core pub/sub, retained | `end_to_end`, `retained` |
| QoS 1 / QoS 2 | `qos1`, `qos2` |
| Sessions (offline queue, durability) | `end_to_end`, `durable_sessions` |
| Keepalive & wills | `keepalive_lwt` |
| Security (auth, ACL, audit, TLS, gossip identity) | `auth`, `acl`, `audit`, `tls`, `peer_identity` |
| Cluster (routing, SWIM, placement, relocation) | `cluster`, `swim_routing`, `swim_cluster` |

### Two facts that shape this plan

1. **The client is the project's own codec** (`mqtt_net::FrameReader/Writer`). Every
   test validates the broker against the same encoder/decoder it ships, so a codec
   bug is invisible to both sides. There is no third-party-client interop.
2. **Zero v5 integration coverage.** No integration test connects as
   `ProtocolVersion::V5`. Everything from ADRs 0008–0013 — v5 codec, session/message
   expiry, shared subscriptions, topic aliases, flow control, enhanced auth, re-auth —
   is proven only at the unit / `conn`-module level. This is the largest gap: the
   features with the most recent change have the least end-to-end proof.

Minor: the `start_broker`/`Client` harness is duplicated across all 13 files.

## Strategy

Keep **both** client styles, deliberately:

- **Shared test-support harness** (`crates/mqttd/tests/common/mod.rs`): one broker
  starter (permissive and custom-policy variants) and one `Client` with v3.1.1 **and
  v5** helpers plus ergonomic `expect_publish`/`expect_closed`/`expect_disconnect`.
  Removes duplication and makes v5 tests cheap.
- **The self-codec client stays primary.** It is the only way to send the malformed
  and adversarial packets darksky tests need — a conformant client library will not
  emit a wildcard PUBLISH topic or an out-of-range topic alias.
- **A thin real-client interop suite** — **done** ([ADR 0034](adr/0034-foreign-client-interop-conformance.md),
  `scripts/interop/run.sh` + the `interop` CI job). Resolved the supply-chain question by
  choosing a **non-Rust** oracle (the Eclipse Mosquitto CLI) over the originally-sketched
  `rumqttc` dev-dep: stronger codec independence (shares zero code with the broker) and
  **zero** crates added to the dependency tree — the foreign client is an external process,
  not a `dev-dependency`. Drives the real `mqttd` binary through v3.1.1 QoS 0/1/2 round-trips,
  a retained-to-a-late-subscriber, a v5 User Property surviving a hop (ADR 0030), and
  OpenSSL↔rustls TLS 1.3 + mTLS. (`rumqttc` remains a possible Rust-side complement if an
  in-`cargo test` interop check is ever wanted.)
- **One process-level smoke test** — done (`binary_smoke`): launches the real
  `mqttd` binary (env-var config, plaintext listener) and drives a pub/sub
  round-trip, the only test exercising `main.rs`.

### Priority

1. ✅ Shared harness + **v5 sunshine** suite (the real risk).
2. ✅ **Darksky** protocol-violation + security suite.
3. ✅ Cluster routing gaps (cross-node QoS 1; shared per-node; retained-not-replicated).
4. ✅ Binary smoke test.
5. ✅ Real-client interop — non-Rust (Mosquitto) oracle, ADR 0034 (see Strategy).
6. ✅ Deeper cluster chaos — superseded by the ADR 0042 harness (below), which
   composes these faults from seeds instead of scripting them one at a time.
7. Retrofit the existing 13 files onto the shared harness (mechanical; lowest value).

### The durable-plane harness (ADR 0042) — done

Three layers guard the hardest correctness surface, each answering a different
question ([ADR 0042](adr/0042-durable-plane-stress-harness.md)):

- **Invariant catalog** (`mqtt_cluster::invariants`): the durable plane's guarantees
  stated once as executable checkers — acked durability, epoch fencing, lease
  monotonicity, retained tokens, session singularity, recovery honesty, bounded
  structures. Scenarios choose *what to do*; the catalog is always *what must hold*.
- **Deterministic simulation** (`mqtt-cluster/tests/durable_sim.rs`): the pure core
  (lease map, replica/fencing logic, token application, HRW placement) driven through
  seeded schedules — reorderings, drops, duplications — with the catalog asserted after
  every step. 1000 seeds per scenario on every push (cheap); a failure panics with its
  seed and `REPRO_SEED` replays it exactly.
- **Whole-cluster stress** (`mqttd/tests/cluster_stress.rs`): a real 3-node durable
  cluster (production wiring + a severable relay per node) under seed-composed fault
  schedules — owner kills, restarts over surviving data dirs, asymmetric link flaps,
  disk write-fault injection, brownout entry/exit, client churn — against an
  obligations ledger of **acked facts only**, judged post-quiesce by the catalog. A
  separate test power-cycles the whole cluster, and another grows 1→3 under acked
  facts then kills the founder (the ADR 0043 P1 catch-up path). The seed reproduces
  the *scenario* (tokio/I-O timing is real); every failure prints the seed and full
  schedule trace.

**Profiles:** every push runs the CI profile (1000 sim seeds; 1 stress seed, ~60–90 s;
the stop/start test, ~10 s) inside `cargo test --all`. Soak runs opt in via env:
`MQTTD_SIM_SEEDS=N` (simulation) and `MQTTD_STRESS_SEEDS=N` (whole-cluster). Findings
land in the [exhibit ledger](delivery/0042-durable-plane-stress-harness.md) — twelve
real defects found and fixed by this program to date.

## Scenario catalog

Legend: ☐ missing · ☑ covered (file).

### ☀️ Sunshine

**v5 protocol round-trips** (all ☐):
- ☐ v5 CONNECT→CONNACK negotiates; pub/sub round-trip works
- ☐ Session expiry: finite interval survives reconnect within window; gone after it
- ☐ Message expiry: stale queued copy dropped at replay; survivor arrives with reduced remaining interval
- ☐ Shared subs: two `$share/g/t` subscribers round-robined one-each; ordinary+shared both receive; shared subscriber gets **no** retained
- ☐ Topic aliases: inbound establish-then-reference resolves; outbound first carries name+alias, second empty-name+alias
- ☐ Flow control: a `Receive-Maximum`-of-N consumer never exceeds N in flight; backlog drains on PUBACK
- ☐ Enhanced auth: HMAC challenge/response connects; re-auth (`0x19`) succeeds mid-session

**3.1.1 / core** (☑ unless noted):
- ☑ pub reaches matching subscriber / non-matching not delivered (`end_to_end`)
- ☑ QoS 1/2 handshakes, DUP resume, exactly-once (`qos1`, `qos2`)
- ☑ retained replace/clear/wildcard/resubscribe (`retained`)
- ☑ persistent offline queue + replay; clean session discards (`end_to_end`, `durable_sessions`)

**Cluster**:
- ☑ QoS 0 publish crosses nodes (`cluster`)
- ☑ QoS 1 and QoS 2 delivered (exactly-once) across nodes (`cluster_chaos`)
- ☑ shared-subscription members split across two nodes — once **cluster-wide** (`cluster_chaos`, ADR 0015)
- ☑ retained **replicates** across nodes and **back-fills** a node that joins after the publish (`cluster_chaos`, ADR 0014)

### 🌑 Darksky

**Protocol violations → close / DISCONNECT, no state corruption** (mostly ☐ at e2e):
- ☐ PUBLISH topic with `+`/`#` → connection closed
- ☐ topic alias `0`, above max, or unmapped reference → closed
- ☐ re-auth with changed method → DISCONNECT `0x82`; AUTH with no prior enhanced auth → `0x82`
- ☐ first packet not CONNECT; a second CONNECT on a live connection
- ☐ truncated frame / bad remaining-length mid-packet
- ☐ QoS 3, packet-id 0 on QoS>0, oversized packet

**Security** (partly ☑):
- ☑ default policy rejects anonymous; mTLS accepted; bad password (`auth`)
- ☑ ACL deny still ACKs (no info leak), audited (`acl`, `audit`)
- ☐ enhanced auth: wrong proof → CONNACK `0x87`; unknown method → `0x8C`; client abandons mid-challenge (cleanup)
- ☐ mTLS untrusted CA / expired / CN-mismatch at connect
- ☑ peer with mismatched cert CN rejected (`peer_identity`); ☐ forged/replayed SWIM datagram dropped

**Resource / abuse**:
- ☑ offline-queue overflow → drop-oldest observed downstream (`resource_limits`)
- ☑ flow-control backlog is bounded (drop-oldest) under a stalled consumer (`hub` unit, ADR 0012)
- ☑ idle client reaped by keepalive (3.1.1, `keepalive_lwt`); ☐ same under v5
- ☑ client connects but never sends CONNECT; ☑ half-sent CONNECT stall (`protocol_violations`, connect deadline)

**Process-level**:
- ☑ the real `mqttd` binary serves a plaintext pub/sub round-trip (`binary_smoke`)

**Cluster chaos**:
- ☑ replica serves session after owner dies — quorum-durable message survives at the store layer (`durable_sessions`)
- ☑ a durable node serves ordinary MQTT clients (clean + persistent) through its hub (`durable_sessions`)
- ☑ partition + heal → routing reconverges (severed link, delivery resumes) (`cluster_chaos`)
- ☑ **client-observable durable failover** — a *persistent* client reconnecting to the
  **new owner after takeover** resumes its session (`session_present=true`)
  (`durable_sessions::a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover`,
  deterministic). This was a diagnosed-to-root gap that took two fixes:
  - ✅ **Membership** ([ADR 0016](adr/0016-swim-membership-stability.md) phase 1,
    tombstone `Dead`): the new owner's replica set is now exactly the live survivors
    (no resurrected corpse, no dropped survivor), so recovery sees a live quorum and
    never reads the dead node. The recovery read is also concurrent (`cluster_store`).
  - ✅ **Attach path** ([ADR 0017](adr/0017-durable-attach-readiness.md)): the persistent
    attach **waits** (off the hub loop) for the durable store to answer authoritatively
    while the group's lease reassigns, then resumes the session — or rejects with
    Server-unavailable so the client retries. It never silently downgrades a recoverable
    session to a fresh one, and the wait does not freeze the hub.
  - (ADR 0016 phase 2 — Lifeguard awareness + multi-source suspicion — remains a
    worthwhile follow-up to keep a *live* node from being falsely evicted under load,
    but is not required for this scenario.)
- ☑ session takeover across nodes (relocation) **with a message in flight**: a queued
  message durably committed before the owner dies is **replayed to the client** when it
  reconnects to the new owner
  (`durable_sessions::a_queued_message_is_replayed_to_the_client_after_takeover`,
  deterministic). Surfaced and fixed a real gap — the new owner's *queue-key* recovery
  was not warmed before the inline replay, so a resumed session could skip delivering its
  queued messages until a later reconnect; the off-loop recovery now warms it (ADR 0017).

## Conventions

- One concern per test; name as `behaviour_under_condition`.
- Darksky tests assert the **specific** reason code / close, not just "an error".
- Every test uses the shared harness; no new bespoke `start_broker` copies.
- Tests must be deterministic — drive acks explicitly, use bounded `recv` timeouts,
  never sleep-and-hope.
