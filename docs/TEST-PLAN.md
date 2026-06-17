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
- **One process-level smoke test** (later): launch the real `mqttd` binary with a
  config file, since the in-process harness never exercises `main.rs` wiring.

### Priority

1. Shared harness + **v5 sunshine** suite (the real risk).
2. **Darksky** protocol-violation + security suite.
3. Cluster sunshine/chaos gaps (QoS>0 across nodes, retained across nodes, partition heal).
4. `rumqttc` interop.
5. Binary smoke test.
6. Retrofit the existing 13 files onto the shared harness (mechanical; lowest value).

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

**Cluster** (gaps ☐):
- ☑ QoS 0 publish crosses nodes (`cluster`)
- ☐ QoS 1/2 delivered across nodes (not just QoS 0)
- ☐ retained published on node A delivered to later subscriber on node B
- ☐ shared-subscription members split across two nodes (documents one-per-node limit)

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
- ☐ offline-queue overflow → drop-oldest/reject-newest observed downstream
- ☐ flow-control backlog under a stalled consumer (documents unbounded-in-memory limit)
- ☑ idle client reaped by keepalive (3.1.1, `keepalive_lwt`); ☐ same under v5
- ☐ client connects but never sends CONNECT; dribble/slow-loris bytes

**Cluster chaos** (mostly ☐):
- ☑ replica serves session after owner dies (`durable_sessions`)
- ☐ owner dies mid-publish → in-flight not lost
- ☐ partition + heal → routing reconverges, no dup/lost delivery
- ☐ session takeover across nodes (relocation) with messages in flight
- ☐ node rejoins with stale interest → no ghost routes

## Conventions

- One concern per test; name as `behaviour_under_condition`.
- Darksky tests assert the **specific** reason code / close, not just "an error".
- Every test uses the shared harness; no new bespoke `start_broker` copies.
- Tests must be deterministic — drive acks explicitly, use bounded `recv` timeouts,
  never sleep-and-hope.
