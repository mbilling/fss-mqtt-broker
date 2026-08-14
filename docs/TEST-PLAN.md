# End-to-end test plan

Status: living document. Tracks the integration-test strategy and the
sunshine/darksky scenario catalog the suite is working toward.

## Where we are

Integration tests live in `crates/mqttd/tests/` (41 suites),
`crates/mqtt-cluster/tests/`, and `crates/mqtt-bridge/tests/` — ~25k lines in total.
Most start an **in-process broker over real TCP loopback** (`Hub::new()` +
`conn::handle`/`handle_stream`) and drive it through sockets; the harder suites drive
the **real `mqttd` binary** out of process (`proc_common/`). The workspace runs ~1,180
test functions.

> These counts drift. They are derivable from the tree
> (`ls crates/mqttd/tests/*.rs | wc -l`), and until a checker enforces them the way
> `scripts/check-readme-facts.py` does for the README, treat them as indicative.

| Area | Files |
|---|---|
| Core pub/sub, retained | `end_to_end`, `retained` |
| QoS 1 / QoS 2 | `qos1`, `qos2` |
| v5 protocol surface | `v5_protocol`, `protocol_violations` |
| **Byte-level framing + codec conformance** | **`wire`** |
| Sessions (offline queue, durability) | `end_to_end`, `durable_sessions`, `inflight_durability` |
| Keepalive & wills | `keepalive_lwt` |
| Security (auth, ACL, audit, TLS, gossip identity) | `auth`, `acl`, `audit`, `tls`, `peer_identity` |
| Cluster (routing, SWIM, placement, relocation) | `cluster`, `swim_routing`, `swim_cluster` |
| Cluster fault injection | `cluster_chaos`, `cluster_stress`, `cluster_proc`, `cluster_soak` |
| Transports | `ws`, `quic`, `tls` |
| Bridge | `mqtt-bridge/tests/{client,engine}` |

### The fact that shapes this plan

**Most tests speak through the project's own codec** (`mqtt_net::FrameReader/Writer`),
so a codec bug would be invisible to both sides of the test. Two things answer that:

1. **Foreign-client interop** — done (ADR 0034): the Eclipse Mosquitto CLI and Paho
   Python drive the real binary in the `interop` CI job. Non-Rust oracles sharing zero
   code with the broker, as external processes rather than `dev-dependencies`, so
   nothing enters the supply chain.
2. **The `wire` suite** — hand-assembled bytes on a raw socket
   (`common::RawClient`), for the frames an encoder *cannot* emit: reserved packet
   types, 5-byte variable byte integers, non-minimal length encodings, overlong and
   surrogate UTF-8, properties on the wrong packet type. A conformant client library
   will never send these, which is exactly why an implementation can be wrong about
   them and stay green forever. It found three real defects on its first run (below).

Still open: an independent conformance suite (`paho.mqtt.testing`) as a third oracle.

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
  OpenSSL↔rustls TLS 1.3 + mTLS. A second oracle (Eclipse Paho, ADR 0034 T7) reaches the
  control plane: v5 reason codes, per-filter granted QoS, session-present on resume, and
  (T8, issue #245) the **capability advertisement** — the CONNACK's `Subscription
  Identifiers Available = 0` and the `0xA1` refusal that must accompany it. The Mosquitto
  CLI cannot drive that last case: its `-D subscribe` accepts only `user-property`, so it
  cannot request an identifier. (`rumqttc` remains a possible Rust-side complement if an
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
  obligations ledger of **acked facts only**, judged post-quiesce by the catalog. That
  ledger is now unqualified: brownout acks used to be waived as ADR 0041's documented
  ack-and-drop trade, and since 0041-T11 / issue #238 a brownout-refused publish is
  simply never acked, so every ack observed is an obligation. The
  schedules also compose **resize** (ADR 0043 P4): seeded `join` steps grow the
  cluster mid-schedule and seeded `decommission` steps drain-then-leave (aborting
  honestly when the drain cannot converge under the running faults). Dedicated path
  tests cover the whole-cluster power cycle, grow 1→3 then kill the founder (P1),
  grow with a moved session and no deaths (P2), a 4-node decommission (P3), 3→5
  with live zone labels then losing two originals, 5→3 via two drains, and the
  rolling host replacement. The seed reproduces the *scenario* (tokio/I-O timing is
  real); every failure prints the seed and full schedule trace.

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

## Byte-level conformance — suite `wire`

`crates/mqttd/tests/wire.rs` writes hand-assembled bytes and asserts the server's
exact answer, distinguishing three outcomes that are **not** interchangeable:

- **silent close** — the only correct answer to a Malformed Packet detected before a
  session exists; the server cannot answer a packet it could not parse
  [MQTT-3.1.4-1].
- **refused** — rejected once a session exists. [MQTT-4.13.2] makes *closing* the MUST
  and the explanatory `DISCONNECT` a SHOULD, so both shapes are conformant.
- **accepted** — the connection stays open and usable.

Every test begins from a hand-built packet that a sanity test proves valid, then
corrupts exactly one thing. Without that sanity pin, a broken builder would read as
universal conformance.

**Three defects it found on its first run**, all fixed alongside it:

| Defect | Rule | Fix |
|---|---|---|
| An interior `U+0000` was accepted in topic names — a NUL byte *is* well-formed UTF-8, so `String::from_utf8` passed it | [MQTT-1.5.4-2] | `mqtt_codec::io::Reader::read_string` |
| Payload Format Indicator of `2` was accepted and coerced | §3.3.2.3.2 | `bool_byte` in `mqtt-codec/src/properties.rs` |
| A **zero-length topic filter was GRANTED** — as were `sport/tennis#`, `sport/#/ranking`, `sport+`. They then matched nothing, so the client held a successful subscription that was silently inert | [MQTT-4.7.1] | `mqtt_core::valid_filter` / `valid_topic_name`, refused per-filter with SUBACK `0x8F` |

The third is the instructive one: filter *validity* is invisible at the matching
layer, because `topic_matches` returns false for a malformed filter exactly as it
does for a valid filter with no traffic. Only a test that asserts the **refusal**
can see it.

## Policy register

Behaviours the spec leaves to the implementation, pinned by a test so they cannot
drift silently. (The MQTT 5.0 spec has several hundred server-applicable normative
statements; this register covers the choices, not the requirements.)

| Choice | Our behaviour | Pinned by |
|---|---|---|
| Codec error after CONNACK | Close **without** a `DISCONNECT` first. Permitted — [MQTT-4.13.2] makes the close the MUST and the DISCONNECT a SHOULD — but note it disagrees with the higher-level protocol-error path, which *does* announce (`conn.rs`, DISCONNECT `0xA1`, issue #245) | `wire::malformed_input_closes_without_a_disconnect` |
| Invalid topic filter | Refused **per filter** (SUBACK `0x8F`), not by closing the connection: the other filters in the same SUBSCRIBE are independently valid | `wire::a_hash_sharing_its_level_is_an_invalid_filter` |
| `$SYS` topic tree | **Not implemented.** `$SYS` appears only as something the bridge refuses to bridge (`mqtt-bridge/src/config.rs`) | — |
| Denied publish | Still ACKed, then dropped (no information leak about ACL shape) | `acl`, `audit` |
| Offline-queue overflow | Drop-oldest, bounded | `resource_limits` |

## Conventions

- One concern per test; name as `behaviour_under_condition`.
- Darksky tests assert the **specific** reason code / close, not just "an error".
- Every test uses the shared harness; no new bespoke `start_broker` copies.
- Tests must be deterministic — drive acks explicitly, use bounded `recv` timeouts,
  never sleep-and-hope.
- A test that pins a *policy* says so, and says that a future change should make it
  fail rather than be relaxed.
