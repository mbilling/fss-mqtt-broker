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
| Bridge, incl. **loop prevention across real topologies** | `mqtt-bridge/tests/{client,engine}` |

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

## Bridge loop prevention — which layer catches which shape

Three defences run at once, and they are **not** interchangeable. Testing one does not
test the others, which is why `mqtt-bridge/tests/engine.rs` now covers each shape
separately:

| Defence | Catches | Blind to | Pinned by |
|---|---|---|---|
| **No Local** (every bridge subscription sets it) | The single-broker echo, including a remap ping-pong written as two individually-legal one-way rules — config validation cannot see that one | Any cycle through more than one broker: each hop is a *different* client, so the broker has nothing to suppress on | `a_remap_ping_pong_within_one_bridge_is_stopped_by_no_local` |
| **Hop count** (`fss-bridge-hop-count`) | The multi-broker cycle — the shape No Local cannot see. It is the *only* defence there | A publisher that stamps the property itself (client-settable; ADR 0025 risk E, issue #191) | `a_three_broker_ring_is_terminated_by_the_hop_counter`, `hop_count_increments_along_a_chain` |
| **Direction** (`plan_forwards`) | Upstream→upstream: hub-and-spoke is structural, never routed | — | existing one-way leak tests |

The ring test is the one that matters most: before it, the hop counter's only real job —
terminating a cycle No Local is blind to — had never been exercised end to end. The
pre-existing test pre-stamps a message *already at* the limit and checks the counter moved,
which does not prove a cycle terminates.

Termination is asserted by **quiescence**: an uncut ring would keep feeding the drain, so
reaching silence at all is the proof, and the delivery count says how far it got first.
Raising `hop_count_limit` from 3 to 200 makes the ring amplify to ~266 deliveries and the
test fail — so it is load-bearing, not decorative.

## Policy register

Behaviours the spec leaves to the implementation, pinned by a test so they cannot
drift silently. (The MQTT 5.0 spec has several hundred server-applicable normative
statements; this register covers the choices, not the requirements.)

| Choice | Our behaviour | Pinned by |
|---|---|---|
| Decode failure **after** CONNACK | Announced: `DISCONNECT(reason)` then close, satisfying both halves of [MQTT-4.13.2]. `0x81` when the bytes are not parseable as MQTT, `0x82` when they parse and say something illegal, `0x95` when over the advertised size (`conn.rs::codec_reason`) | `wire::malformed_input_after_connack_is_announced_then_closed`, `wire::malformed_and_protocol_errors_are_told_apart` |
| Decode failure **before** CONNACK | Silence. [MQTT-3.14.0-1] forbids a server DISCONNECT before a success CONNACK, so the uniform "always announce" refactor is a spec violation | `wire::malformed_input_before_connack_is_met_with_silence` |
| Property carried on a packet type that disallows it | Refused as `0x82` Protocol Error. §2.2.2.2 is arguably read as Malformed (`0x81`); the codec classifies it `ProtocolViolation` and we answer accordingly rather than re-classify on a contested reading. The *refusal* is not in doubt — only the code | `wire::a_property_from_another_packet_type_is_refused` |
| Decode failure on a **QUIC** stream | Not announced — the QUIC mux (`mqtt-net/src/quic.rs`) ends its own read loop on a framing error, so it never reaches the announcing path. Known asymmetry, not yet closed | — |
| Invalid topic filter | Refused **per filter** (SUBACK `0x8F`), not by closing the connection: the other filters in the same SUBSCRIBE are independently valid | `wire::a_hash_sharing_its_level_is_an_invalid_filter` |
| `$SYS` topic tree | **Not implemented.** `$SYS` appears only as something the bridge refuses to bridge (`mqtt-bridge/src/config.rs`) | — |
| Denied publish | Still ACKed, then dropped (no information leak about ACL shape) | `acl`, `audit` |
| Offline-queue overflow | Drop-oldest, bounded | `resource_limits` |
| Subscription Identifiers | **Not implemented**, and said so: every v5 CONNACK carries `SubscriptionIdentifierAvailable = 0`, which §3.2.2.3.12 requires of a server that lacks them, and a SUBSCRIBE that uses one is refused `0xA1` rather than silently degraded (issue #245) | `conn::v5_connack_advertises_subscription_identifiers_unavailable`, `interop-paho` |
| Server Keep Alive | **No cap.** The client's requested keep alive is used verbatim and the property is never sent. §3.2.2.3.14 makes it optional — a server sends it only to override — so declining is conformant | `interop-paho-testing` (declared) |

## The independent oracle — `paho.mqtt.testing`

Everything above is checked by tests that share this implementation's reading of the
spec. `scripts/interop/paho-testing.sh` runs Eclipse's own MQTT 5 conformance suite at a
pinned commit against the real binary — written against a reference broker by people who
never saw this code, so its verdict is evidence in a way our own green suite is not.

**27 tests; 22 pass.** The five that do not are declared in the script with reasons, and
the script fails *both* ways — on an undeclared failure, and on a declared failure that
starts passing — so the list cannot decay into an ignore list.

| Test | Verdict |
|---|---|
| `test_session_expiry` | **Real deviation** ([#298](https://github.com/mbilling/fss-mqtt-broker/issues/298)): a DISCONNECT's Session Expiry Interval must override the CONNECT's [MQTT-3.14.2.2.2]; `conn.rs`'s DISCONNECT arm reads only the reason and drops the properties |
| `test_will_delay` | **Real gap** ([#299](https://github.com/mbilling/fss-mqtt-broker/issues/299)): Will Delay Interval is decoded but never honoured — the Will fired at 0.1 s where the suite expects 4 s [MQTT-3.1.3.2.2] |
| `test_subscribe_identifiers` | Legal difference — see the register row above; the suite assumes support without reading the advertisement |
| `test_server_keep_alive` | Legal difference — the suite asserts its reference broker's 60 s cap, not a spec requirement |
| `test_subscribe_failure` | Suite configuration we decline. Answering it needs an ACL denying `test/nosubscribe`, but our deny rules match by filter **overlap**, so that also denies the suite's own `cleanRetained()` subscription to `#` — retained state then leaks between tests and five unrelated ones fail (measured: 9 failures with the ACL, 4 without). Both behaviours are correct and simply incompatible; the SUBACK-failure path is covered directly in `crates/mqttd/tests` |

## Conventions

- One concern per test; name as `behaviour_under_condition`.
- Darksky tests assert the **specific** reason code / close, not just "an error".
- Every test uses the shared harness; no new bespoke `start_broker` copies.
- Tests must be deterministic — drive acks explicitly, use bounded `recv` timeouts,
  never sleep-and-hope. That rule was folklore until issue #260; it is now a build
  failure. See **Waiting** and **Skipping** below.
- A test that pins a *policy* says so, and says that a future change should make it
  fail rather than be relaxed.

### Waiting: the four shapes, and the only one that is banned

`scripts/check-test-hygiene.py` classifies every wall-clock wait it can see in test code — under
`crates/*/tests/`, `tools/mqttui/tests/`, and inside any `#[cfg(test)]` module — and fails
CI on the fourth shape. Production timers under `src/` are out of scope: a retry backoff in
`main.rs` is not a test wait.

"Every wait it can see" is deliberate wording. The gate knows the calls that *block on a clock*:
`sleep`, `sleep_until`, `park_timeout`, `recv_timeout`, `recv_deadline`, `interval().tick()`,
`timeout(d, pending())`, any rename of `sleep` (resolved repo-wide, so a `pub use … as settle` in
a helper module does not hide one), and a loop whose only exit is temporal and whose body does
nothing but yield. It is a list, and a list of ways to burn time is never complete — see the
boundary table at the end of this section.

| | shape | what makes it acceptable | annotation |
|---|---|---|---|
| **(a)** | **bounded poll** | a loop with a state-dependent exit **and** a bound (deadline, iteration range, or an incremented counter compared to a limit). It stops the moment the condition holds, and when the condition never holds it fails **naming what never happened** | none — the shape is the argument |
| **(d)** | **virtual-clock advance** | inside `#[tokio::test(start_paused = true)]`. Paused time only advances when every task is idle, so `sleep(X)` means "let the system settle, then move on by exactly X" — deterministic, and free | none |
| **(b)** | **deliberate settling delay** | a real wall-clock wait kept on purpose. Requires a reason a skeptic would accept, and it is listed in [docs/test-settling-delays.md](test-settling-delays.md) | `// SETTLE(<slug>): <reason>` at the site, ≥60 chars, plus a census row |
| **(c)** | **naked wait** | nothing. This is the defect | — |

A **(b)** must answer three questions, or it is a **(c)** wearing a label — and a mislabelled
wait is worse than an unlabelled one, because the next reviewer trusts the label:

1. **What state is being settled?** Not "let things settle" — which subsystem, reaching which
   condition.
2. **Why does no observable condition exist for it?** The legitimate answers seen so far are:
   the state is the *absence* of an event (proving a negative); observing it would *destroy the
   subject* (probing a pending session expiry cancels it); the wait *is* the stimulus (an
   injected fault, a fragmented write, a benchmark's measurement boundary); or there is no
   clock seam and real time must pass (`std::time::Instant` is reachable by neither
   `crate::clock` nor tokio's paused clock).
3. **What happens on a slow machine?** State the direction. A one-sided failure mode — slower
   makes the check *stronger* — is acceptable. A wait whose expiry would make the test **pass
   vacuously** is not, and must be paired with an assertion that turns that case into a loud
   failure. `cluster.rs`'s `PartitionProbe` and `reload_acl.rs`'s surviving-grant control are
   both there for exactly that reason.

Two further rules: `thread::sleep` inside an `async` test body is rejected outright (it parks
a runtime worker, so the thing being waited for may be the very task that now cannot run), and
one `SETTLE` slug vouches for exactly one wait.

**Prefer, in order:** (a) a bounded poll on an observable → (d) a paused clock → (b) a
documented delay. When no observable exists, the right move is often to add one — but say so
and ask, rather than widening production surface inside a test lane. Open asks are recorded on
issue #260.

### Skipping: allowed locally, fatal in CI

A test that returns early instead of asserting reports **success**. `cargo test` prints `ok`,
the summary counts it, and nothing says the coverage did not run — which is invisible in
exactly the place it matters, on the green check of the pull request that removed it.

Skipping is still right *locally*: `127.0.0.2` is not bindable on stock macOS (issue #217).
It is never right on the platform that gates merges. So:

- **Rust:** `skip_locally_or_fail_in_ci!("<what is missing and how to get it>")`
  (`crates/mqttd/tests/common/skip.rs`, byte-identical copies elsewhere and diff-checked).
  It asserts `CI` is unset, then returns. GitHub Actions sets `CI=true` on every runner, so
  this needs no workflow wiring. Its guard must **be** the `CI` check and nothing else — no
  disjunct, no `cfg!`, no extra term — because a condition that can be satisfied another way is
  not a guard: `|| cfg!(debug_assertions)` is true in every `cargo test` profile, and one such
  disjunct made the macro non-fatal everywhere while the gate reported success.

  The gate rejects any other bare early `return` in a test function — in every spelling,
  including `if cond { return }`, `let … else { return };` and `return Ok(())` — any `println!`
  announcing a skip, a `return` hidden inside a `macro_rules!` body, a test whose every statement
  sits inside an `if` that can simply not be taken (a skip with no `return` and no message at
  all), an assertion inside the right operand of a `&&`/`||`, and `process::exit`/`abort` in test
  code (which ends the whole *binary*, discarding every result in it). Two exemptions exist and
  are named in the code: a bounded poll's success exit — a `return` inside a loop whose
  exhaustion **fails loudly** — and a `return` inside a task handed to `spawn`. A trivial loop
  does not buy the first one: `for _ in 0..1 { if probe() { return } … }` is rejected.
- **Shell:** `skip_or_fail "<reason>"` in the CI-run scripts — fatal when `CI=true`. Its
  counterpart `skip_permitted "<reason>"` is the one deliberate exception, for a lane that
  genuinely cannot run in CI (the forward-looking compose image pin, `compose-smoke.sh`).
  The gate's scope is *derived* from `.github/workflows/`, so adding a script to a job brings
  it under the rule automatically.

  The rule is not a list of probe spellings — it was, and a `have() { command -v "$1"; }` helper
  used far below, an unset-variable test, a `uname` test and seven lines of distance each got a
  gate to report success having run nothing. So it is stated from the other side: **a top-level
  `exit 0` before the end of a CI-run script must be a declared skip** (`skip_or_fail` /
  `skip_permitted` immediately above) **or carry a `# NOT-A-SKIP: <why>` annotation** of 30+
  characters. Two real ones do: `bootstrap.sh`'s refusal to overwrite an existing password file,
  and `vendor-mqttui-examples.sh --check`'s success. One level of shell-function indirection is
  also resolved, so a helper that probes makes its call sites probes.
- **Compile-time gates:** a `#![cfg(…)]` at the top of a test file is the one vanishing a
  runtime check can never see — off-platform the file compiles to zero tests and the run is
  green, and an assertion inside it would be excluded by the same `cfg`. Every such file has a
  mirrored predicate in `crates/mqttd/tests/platform_coverage.rs`, which always compiles, and
  the gate fails if one is missing. The same applies to a `#[cfg(…)]` on an
  individual test: one character less, and a whole suite of tests reports `0 passed … ok`.

### The inventory: what the binaries actually contain

Everything above reads source text, and text is porous by construction — a rule can only see a
shape someone thought of, and this gate has three times been shown shapes its author had not. So
two checks are not text rules at all. The first asks the compiled binaries what they contain:

    scripts/check-test-hygiene.py --write-inventory   # regenerate after adding a test
    scripts/check-test-hygiene.py --check-inventory   # CI, in the jobs that build

`cargo test -- --list` reports the tests each binary really contains (and `--list --ignored` the
ones it will not run); they are compared against
[docs/test-inventory.md](test-inventory.md). That catches what no pattern over source reliably
can: **a test that is not there.** A `cfg` that excluded it, a file that compiled to zero
tests, a rename, a deletion inside a large diff, an `#[ignore]` that retired it. Tests that exist
only on some platforms are recorded with their predicate, so one inventory serves a laptop and
the Linux runner alike.

Adding a test costs one regeneration, the way `gen-status.py` costs one — and the diff *is*
the review: a line disappearing from that file is a test disappearing from CI.

### The results: what actually ran and passed

The inventory answers *what does the binary contain*. Two ways of losing coverage answer that
question identically before and after, and both were demonstrated against this gate:

- **`#[ignore]`.** The test stays in the binary, `--list` still prints it, and it runs nowhere.
  One attribute, no artifact changed, `cargo test` green over a smaller suite.
- **`std::process::exit(0)` inside one test.** The harness leaves mid-suite: `running 6 tests`
  and then *nothing* — no per-test lines, no `test result:` summary — and `cargo test` exits 0.
  One test's line discards **every** result in its binary.

Neither is visible to any rule over source text, and both are obvious in the run's own output.
So the run's own output is checked:

    scripts/check-test-hygiene.py --check-results test-output.txt --only root

For every binary the inventory says this host has, the run must show a **complete summary**, no
failures, **nothing filtered out**, the recorded **passed** count under this host's `cfg`
evaluation, and an **ignored set** that matches the inventory. It consumes the log CI already
tees, so it costs the test job nothing; given no log it runs the suite itself.

`#[ignore]` is now first-class rather than out of scope. The inventory records it (asked of the
binary, via `--list --ignored`), and every ignored test must be declared in `IGNORE_ALLOWLIST`
in the gate with a reason **and the tier that runs it** — where the tier is *verified* against
`.github/workflows/`, not believed. Two are so declared and so verified (`cluster_upgrade`,
`cluster_soak`, both nightly). Five are declared with **no tier at all**:

| `#[ignore]`d, run by no tier | why |
|---|---|
| `durable_bench::durable_path_floor` | macro-benchmark, minutes long, `--release` only |
| `durable_bench::degraded_group_does_not_delay_other_groups` | as above |
| `durable_bench::store_append_floor` | micro-benchmark, `--release` only |
| `durable_bench::device_barrier_floor` | micro-benchmark of the host's durability barrier |
| `durable_bench::multi_host_preflight` | needs an operator-provisioned multi-host cluster |

Those five are **coverage that exists only on paper**: not per-PR, not nightly, not release.
`--check-results` prints them on every successful run, and wiring the four runnable ones into a
release-gated lane is a follow-up. The point of the allowlist is that this is a declared,
reviewable fact instead of five attributes nobody re-reads.


### What this gate detects, and what it cannot

**Round 4 (2026-08-15) closed four more bypasses and left one class open; read the additions
below before trusting a row further down.** Every one of these was found by an adversary given
the source and told to get a vacuous test past the gate, and each is recorded with what
actually changed:

- **`--check-results` could not have passed in CI at all.** It anchors on `^\s+Running`, and
  `.github/workflows/ci.yml` sets `CARGO_TERM_COLOR: always` at file scope, so the tee'd log
  carries SGR escapes and every anchored pattern matched nothing — the flagship mechanism would
  have failed a fully green run. Escapes are now stripped before parsing, rather than the
  anchors loosened (an anchor that tolerates junk before `Running` would also accept a line
  that merely mentions it).
- **The CI-fatal guard was verified in text but never RUN.** B3 proved the condition is exactly
  the `CI` check, and both reviewers then defeated it by wrapping the assertion in
  `if false { … }`, which leaves the condition byte-identical. Two things changed: B3 now also
  rejects a guard nested inside a conditional (position, not text — a condition cannot reveal
  that it never runs), and `platform_coverage::the_skip_macro_is_fatal_under_ci` now *executes*
  the macro in a subprocess with `CI=true` and observes the panic, and with `CI` unset observes
  the skip. That is the residual the previous version could only name, and it is closed by
  running the thing rather than reading it.
- **The no-op rule was a menu of literal spellings**, so `let _unused = 0u8;`, `= "";` and
  `= 1.0;` were still silent self-skips. It now matches the *class* — any literal — while a
  call (`let _ = probe();`) stays a real statement.
- **The loop exemption accepted any assertion anywhere after the loop**, so
  `for _ in 0..1 { if probe() { return } } assert_eq!(a, b);` was exempt: the test's ordinary
  assertion was read as an exhaustion check. Exhaustion must now be **unconditionally** fatal
  and adjacent — a `panic!`, not an `assert_eq!` that could pass. Note the intermediate
  attempt that was not enough: requiring adjacency alone still passed the fig leaf, because
  that assertion *is* adjacent.

**Still open, and deliberately named rather than papered over:** the shell and text rules
remain vocabularies, and an adversary with source access can keep finding spellings — round 4
demonstrated three more in check C and one in check A5 (`std::time::Instant::now()` qualified
rather than bare). The mechanisms that do *not* have this property are the inventory and the
results check, because they ask the compiled binaries what they contain and what ran; prefer
extending those over adding another pattern. The load-sensitivity of the integration suites is
a separate open item: quiet, the workspace is 1294 passed / 1 failed / 7 ignored, but under
four concurrent cargo jobs roughly one run in six fails, and *which* test fails varies — it is
the shared per-test broker-plus-stub harness with fixed bounds, it reproduces identically on
`origin/main`, and it is filed rather than fixed here.


A gate whose limits are unwritten gets trusted past them — that is how the first version of
check B3 came to be satisfied by a comment quoting the assertion it no longer made, and it is
why three rounds of adversarial review went into this file rather than one. The table below is
derived from what was actually *tried*: every "detected" row was proven by reintroducing the
bypass and watching the gate name it by `file:line`, and every "not detected" row was proven the
other way — the shape ran green with the gate reporting success.

**Detected** (each mutation-proven, in this repository, at the check named):

| shape | check |
|---|---|
| a naked wall-clock wait: `sleep`, `sleep_until`, `park_timeout`, `recv_timeout`, `recv_deadline`, `interval().tick()`, `timeout(d, pending())` | A3 |
| the same, renamed — `use tokio::time::sleep as pause`, including a `pub use` rename in *another* file | A3 |
| a temporal loop that only burns: `while Instant::now() < deadline { yield_now().await }` | A5 |
| `while start.elapsed() < D { sleep }` — a duration wearing a poll's clothes | A3 |
| a wait inside a `macro_rules!` body, whose call site carries no wait token at all | A3 |
| a marked helper's wait acquiring further call sites (the census counts them; growth is a diff line) | census |
| `thread::sleep` in an `async` test body | A4 |
| a bare early `return` in a test, in every spelling (`return;`, `if c { return }`, `let … else { return }`, `return Ok(())`, a match arm) | B1 |
| a bare early `return` inside a loop that is not a poll — `for _ in 0..1 { … }`, `while let Some(()) = once.take() { … }` | B1 |
| a bare early `return` inside any *other* `macro_rules!` — a self-skip generator | B1 |
| a `println!`/`eprintln!` announcing a skip, message on any continuation line | B2 |
| a test whose every statement sits inside an `if` that can simply not be taken | B5 |
| the same chain "closed" by a branch that does nothing (`else { let _unused = (); }`, `else { () }`) | B5 |
| an assertion inside the right operand of a `&&`/`\|\|` — an `if` written as an expression, inside a binding | B7 |
| `process::exit` / `process::abort` in test code | B6 |
| the skip macro losing its `CI` guard — deleted, commented out, `debug_assert!`ed, or weakened with an always-true disjunct (`\|\| cfg!(debug_assertions)`, `\|\| true`, an extra `&&` term) | B3 |
| any copy of the skip macro drifting from the canonical one | B3 |
| a whole test file, or a single test, vanishing off-platform via `#[cfg]` with no mirrored assertion | B4 |
| a shell gate announcing a skip — any case, mid-line, or through a one-line `note` wrapper | C |
| a shell gate exiting 0 early for **any** reason: `command -v … \|\| exit 0`, a `have()` helper used far below, an unset-variable test, a `uname` test, a probe seven lines up | C |
| a Python gate whose probe reaches a success exit — `shutil.which`, `os.access`, `os.environ.get`, `except ImportError` → `sys.exit(0)` / `raise SystemExit(0)` | C |
| a test deleted, renamed, or `cfg`-gated out of existence; a file that compiled to zero tests; a whole binary leaving the build | inventory |
| a test retired with `#[ignore]`, and an ignored test whose declared tier no longer runs it | inventory + allowlist |
| a binary whose results vanished mid-run (no `test result:` summary) — what `process::exit(0)` in one test does to all of them | results |
| a run that passed fewer tests than the inventory accounts for, or was filtered | results |

**Not detected.** Each of these was run against the finished gate and passed green, or is a
known limit of the mechanism:

| shape | why it is left, and what would close it |
|---|---|
| **A wait behind a helper call.** `settle(250)` where `fn settle` is elsewhere is classified at its definition, and one `SETTLE` marker covers every caller. | The census names the helper and counts its call sites, so a new caller is a regenerated line — but the gate cannot re-judge whether the *reason* still holds for that caller. Only review can. |
| **A closure that asserts and is never called.** `let check = \|\| assert_eq!(1, 2, "…"); let _ = &check;` | Every statement is a binding, and B5 deliberately does not look inside bindings (most tests bind their subject first). Closing it needs reasoning about where a closure is invoked; the shape has zero instances here. |
| **A `match`-shaped skip.** B5 is rigorous for `if`, approximate for `match`. | An exhaustive `match` with a panicking arm is how checks are written here, so treating a `match` as skippable would flag correct code — the failure mode that gets a gate worked around rather than obeyed. |
| **A `return` taken on a probe *failure* inside a real poll.** `for i in 0..50 { if !probe() { return } … assert!(i < 49, "…") }` | B1 exempts a return whose loop fails loudly on exhaustion, because that is what all twelve real polls in this tree look like. Telling "returned because the state arrived" from "returned because the environment is missing" needs meaning, not structure. |
| **A busy-wait that computes.** A5 covers a temporal loop that only yields; one doing arithmetic burns the same time and is not flagged. | A duration-bounded loop that does real work is a load generator — `roll_cost.rs` has one, correctly — so the rule cannot widen without flagging it. The wait *vocabulary* is likewise a list, and a list is never complete: the gate now prints "wall-clock wait sites classified", not "every wait in this tree". |
| **A loop whose state-dependent exit can never be true.** | It fails loudly at its deadline rather than passing vacuously — the acceptable half of this. |
| **An empty collection.** `for case in cases { assert!(…) }` with `cases` empty asserts nothing. | A vacuity of a different family from waits and skips; not checked. |
| **A test that runs and proves nothing.** | No rule here reads what an assertion *means*. `quic.rs`'s fan-out test passed with fan-out disabled outright until it was given an observable that distinguishes the two paths; `quotas.rs`'s isolation claim passed against an unthrottled broker. Only a mutation finds these, and mutations are a reviewer's tool, not this script's. |
| **The skip macro's *behaviour*.** B3 proves the guard is exactly the `CI` check, in code, in every copy — structurally. | Nothing in this repository runs the macro with `CI=true` and observes the failure. A self-exec test (re-run the test binary with `CI=1`, assert a non-zero exit and the FATAL message) would close it, and is drafted as a follow-up. |
| **Shell indirection deeper than one level.** A helper that calls a helper that probes is not resolved. | The success-exit rule covers the general case from the other side: whatever led there, an early `exit 0` needs `skip_or_fail`/`skip_permitted` or a `# NOT-A-SKIP: <why>` annotation. A helper *function* that exits 0 mid-script is not covered — only top-level exits are, because a `return 0` inside `wait_ready` is ordinary shell. |
| **Python gates' skip *messages*.** Only the probe→success-exit hole is checked. | No rule can separate a gate's own prose about skipping from an actual skip, and this gate's own source is full of the former. (Its own literals no longer trip it: the Python view is tokenized and strings are blanked, the same rule as everywhere else here.) |
| **Skips in a script CI does not run.** Check C's scope is derived from `.github/workflows/`. | Deliberate: adding a script to a job brings it under the rule, and a user-facing tool's documented per-certificate skips stay out of a rule written for gates. |
| **Coverage that runs nowhere.** Five `durable_bench` benchmarks are `#[ignore]`d and run by no tier at all. | Now *declared*, and printed on every `--check-results` run instead of being invisible — but the gate cannot make a tier exist. Follow-up. |
| **Production coverage that a test rewrite displaces.** The inventory compares test *names*; a suite that stops exercising a production entry point changes no name. | This happened for real: `mqtt_net::quic::connect_mux` was left exercised by nothing after the fan-out rewrite, and one test now connects through it. Nothing in this gate would have caught it — region-level coverage tooling would, and this repository has none. |
| **A test that is wrong.** | Out of scope, and worth saying: this gate makes a suite that *stopped testing* visible. It has nothing to say about a suite that tests the wrong thing. |
