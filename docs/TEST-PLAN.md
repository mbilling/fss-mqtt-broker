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
- **A thin real-client interop suite** (`rumqttc`, behind an opt-in `interop`
  feature so it is CI-only): a foreign client doing a pub/sub round-trip in v3.1.1
  and v5. This catches codec-conformance drift the self-codec cannot. ~6 tests.
  **Not yet added** — pulling a third-party client drags in a dependency tree that
  `cargo deny` will scrutinise; for a security-first broker this is a deliberate
  supply-chain decision for the maintainer, not an incidental dev-dep. Tracked as
  an explicit follow-up.
- **One process-level smoke test** — done (`binary_smoke`): launches the real
  `mqttd` binary (env-var config, plaintext listener) and drives a pub/sub
  round-trip, the only test exercising `main.rs`.

### Priority

1. ✅ Shared harness + **v5 sunshine** suite (the real risk).
2. ✅ **Darksky** protocol-violation + security suite.
3. ✅ Cluster routing gaps (cross-node QoS 1; shared per-node; retained-not-replicated).
4. ✅ Binary smoke test.
5. `rumqttc` interop — pending a supply-chain decision (see Strategy).
6. Deeper cluster chaos: partition+heal reconvergence, owner-dies-mid-publish,
   takeover-across-nodes with in-flight messages, QoS 2 across nodes.
7. Retrofit the existing 13 files onto the shared harness (mechanical; lowest value).

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
- 🔴 **Known gap (diagnosed to root):** client-observable durable failover — a
  *persistent* client reconnecting to the **new owner after takeover** stalls ~10s on
  CONNACK and comes up `session_present=false`. Chased it end to end: attach recovers
  the session's *meta* key from a quorum, but the new owner's `placement.members()` is
  momentarily **wrong** — it still lists the killed node (resurrected as Suspect/Alive
  by stale gossip) and has dropped a live survivor — so the recovery replica set has
  no live quorum, the read targets the dead node, times out (`rpc_timeout` × 2 = ~10s),
  and fails `NoQuorum`. The **placement and recovery logic are correct**; the
  membership feeding them **flaps** under the heavy durable test. **Real fix:** SWIM
  membership stability — fence a killed node from resurrection (incarnation numbers)
  and refute false suspicion of live nodes. A substantial, higher-risk membership-
  protocol effort, deliberately *not* attempted as a reactive change. The recovery
  read was made concurrent (`cluster_store`) as a related robustness win.
- ☐ session takeover across nodes (relocation) with messages in flight (blocked by the gap above)

## Conventions

- One concern per test; name as `behaviour_under_condition`.
- Darksky tests assert the **specific** reason code / close, not just "an error".
- Every test uses the shared harness; no new bespoke `start_broker` copies.
- Tests must be deterministic — drive acks explicitly, use bounded `recv` timeouts,
  never sleep-and-hope.
