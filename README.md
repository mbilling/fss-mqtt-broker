# mqttd — a security-first, cluster-native MQTT broker

> An MQTT 3.1.1 + 5.0 broker built to be the most cyber-secure
> broker available, with linear horizontal scalability and a 100% open feature
> set.

**Status:** MQTT **3.1.1 and 5.0** are served — over TCP, TLS 1.3, WebSocket
(`ws://`/`wss://`), and QUIC. The v5 semantics are in place (session/message
expiry, topic aliases, flow control, shared subscriptions, User Properties,
enhanced `AUTH`), not just the wire codec. Transport security
(TLS 1.3 + mutually-authenticated cluster bus), authenticated gossip membership
with dynamic cross-node routing, and a full identity/authorization stack
(mTLS-CN / password / JWT → topic ACLs → tamper-evident audit) are in place.
**Durable, consensus-backed replicated session storage** (openraft lease group +
epoch-fenced quorum replication) is **on by default** and proven over a real
cluster, with **cross-node takeover** (a replica serves a session after its
owner dies) and **data-safe elastic resize** (grow, shrink, and rolling
replacement without losing an acknowledged fact). Prometheus metrics, resource
governance (connection caps, per-client quotas, publish-rate limits, bounded
queues), and a continuous-assurance program (out-of-process fault/upgrade
harness, hour-long soak, fuzzing of every attacker-reachable parser, recorded
performance baselines, and two independent foreign-client conformance oracles)
all ship. The main thing not yet done is cutting a tagged release.

See [`docs/CAPABILITY-PLAN.md`](docs/CAPABILITY-PLAN.md) for the product vision,
[`docs/adr/`](docs/adr/) for the decisions behind it, and the
[**delivery dashboard**](docs/delivery/STATUS.md) — the authoritative, live
record of exactly what is built (44 ADRs, per-task status).

## Principles

- **Security is the product.** Secure by default; every insecure mode must be
  opted into and is loudly logged.
- **Open == Enterprise.** One Apache-2.0 codebase, no gated features. Only
  support, SLAs, and certified builds are paid.
- **Linear scalability.** Shared-nothing nodes; no coordinator on the publish
  hot path.
- **Memory safety.** Rust, `#![forbid(unsafe_code)]` across crates.

## What works today

### Protocol (MQTT 3.1.1)
- CONNECT/CONNACK with full flag and client-id validation.
- **QoS 0/1/2 end-to-end**: per-session in-flight tracking, `DUP` redelivery on
  session resume, the QoS-2 four-way handshake, and inbound exactly-once
  deduplication.
- SUBSCRIBE/UNSUBSCRIBE with `+`/`#` wildcard filters; per-filter QoS grant.
- **Retained messages**: replayed (with the retain flag) on every new
  subscription, replaced by newer publishes, cleared by a zero-length payload.
- **Last Will & Testament**: published on any ungraceful end (abrupt drop,
  keepalive expiry, session takeover), discarded on a clean DISCONNECT.
- **Keepalive enforcement** (1.5× grace), and persistent sessions
  (`clean_session=0`) with offline queueing and replay.
- Zero-trust wire codec with a `cargo-fuzz` harness.

### Protocol (MQTT 5.0)
A v5 client connects, gets a v5 CONNACK with v5 reason codes, and exchanges
v5-framed packets with properties. The semantics are implemented, not just the codec:
- **Session & message expiry** ([ADR 0009](docs/adr/0009-mqtt5-expiry.md)):
  Session Expiry Interval and per-message Message Expiry Interval, honoured on
  queueing and replay.
- **Topic aliases** ([ADR 0011](docs/adr/0011-topic-aliases.md)) and **flow
  control** (Receive Maximum, [ADR 0012](docs/adr/0012-flow-control.md)).
- **Shared subscriptions** (`$share/<group>/<filter>`), including
  **cluster-wide** shared groups selected across the mesh
  ([ADR 0010](docs/adr/0010-shared-subscriptions.md),
  [0015](docs/adr/0015-cluster-shared-subscriptions.md)) — the linear-scaling lever.
- **User Properties** forwarded end to end through delivery
  ([ADR 0030](docs/adr/0030-user-property-forwarding.md)).
- **Enhanced authentication** — the v5 `AUTH` exchange, e.g. challenge/response
  ([ADR 0013](docs/adr/0013-enhanced-authentication.md)).
- Reason codes and DISCONNECT with reason on protocol/quota violations.

Both protocol versions round-trip against two independent foreign clients
(Mosquitto CLI + Eclipse Paho) in CI — see [Build & test](#build--test).

### Security
- **TLS 1.3** client listener (`rustls` + `ring`), optional per-listener client
  certificate mTLS — [ADR 0002](docs/adr/0002-transport-security.md). Also native
  **MQTT-over-WebSocket** (`ws://` / `wss://`, the latter sharing the same TLS 1.3 + mTLS),
  so browsers are first-class clients — [ADR 0035](docs/adr/0035-websocket-transport.md) —
  and **MQTT-over-QUIC** (UDP; TLS 1.3 + mTLS; **multi-stream** — one session across many QUIC
  streams, no head-of-line blocking) — [ADR 0036](docs/adr/0036-quic-transport.md).
- **Mutually-authenticated cluster bus** against a dedicated cluster CA; each
  peer's node id is bound to its certificate Common Name
  ([ADR 0004](docs/adr/0004-identity-and-authentication.md)).
- **Authenticated SWIM gossip**: every membership datagram carries an
  HMAC-SHA256 tag under a cluster-shared key
  ([ADR 0003](docs/adr/0003-gossip-authentication.md)).
- **Identity & authentication**: identity from the mTLS certificate CN; a
  deny-by-default CONNECT gate; pluggable Argon2id password and JWT (HS256/RS256)
  authenticators composed in a chain (cert → password → token).
- **Authorization**: deny-by-default TOML topic ACLs with `%i` identity
  substitution and asymmetric allow-covers / deny-overlaps semantics so a narrow
  grant can't widen and a broad subscription can't tunnel past a deny.
- **Session-identity binding** (ADR 0031): a persistent session is bound to the
  authenticated identity that created it — a different principal cannot resume or
  take it over (CONNACK Not-authorized + audit). Secure by default; an optional
  `connect` ACL rule can additionally namespace client ids per identity.
- **Hot-reloadable security policy**: `SIGHUP` re-reads the ACL, the
  authenticator chain, and the TLS cert/key/client-CA and swaps them on **live**
  connections — no restart, no dropped sessions. The reload is **validate-before-swap**:
  a missing or unparseable file is rejected and the running policy is kept intact
  (never fail open, never brick); every reload is audited and metered
  ([ADR 0032](docs/adr/0032-hot-reloadable-security-policy.md)).
- **Revocation reaches live state**: a successful reload **sweeps** live sessions,
  subscription grants, and peer links against the new policy — a CRL'd certificate, a
  removed user, or a connect-ACL deny evicts the live session; a tightened subscribe-ACL
  stops existing flows; a cluster-CRL'd node's established links are torn down. Identity
  revoked → session ends; permission revoked → flow ends
  ([ADR 0040](docs/adr/0040-revocation-reaches-live-state.md)).
- **Tamper-evident audit log**: a hash-chained record of auth and authorization
  decisions (no credential ever reaches it).
- **Secure by default**: plaintext listeners, anonymous access, an unkeyed
  gossip plane, and unenforced authorization are all opt-in and loudly logged.
- CI gates: `fmt`, `clippy` (pedantic, warnings denied), `cargo-deny`,
  `cargo-audit`.

### Clustering
- Shared-nothing nodes: a client connects to any node.
- **SWIM gossip membership** (failure detection + anti-entropy), authenticated.
- **Membership-driven mesh**: nodes discover each other via gossip and establish
  mTLS peer links automatically — no static peer list required.
- **Interest-based routing**: a publish fans out only to peers whose gossiped
  subscription interest matches the topic.
- **Session placement** (HRW rendezvous over live membership): every persistent
  session has a deterministic owner node, and ownership rebalances minimally as
  the cluster changes ([ADR 0001](docs/adr/0001-session-durability.md)).
- **Session relocation** ([ADR 0005](docs/adr/0005-session-affinity.md)): a
  persistent session connecting to a node that is not its owner is relayed to the
  owner over the mTLS bus and served there — sharded session capacity. The
  landing node vouches for the client's authenticated identity within the
  cluster-CA trust boundary. By default the owner's session log is
  quorum-replicated (below), so its death does not lose the session; opting out
  to the bounded in-memory store (`MQTTD_DURABLE_SESSIONS=0`) trades that
  durability for lower overhead, and there an owner's death does drop its sessions.

- **Durable, replicated session storage** ([ADR 0001](docs/adr/0001-session-durability.md),
  [0006](docs/adr/0006-consensus-and-replication.md),
  [0007](docs/adr/0007-durable-store-integration.md)) — **on by default**
  ([ADR 0029](docs/adr/0029-durable-by-default.md)). An openraft lease group (per placement
  group, leader-assigned) mints an epoch, and each persistent session's append-log is
  quorum-replicated across its replica set, epoch-fenced against a stale owner. Stable at
  rest, under load, and through formation (ADR [0026](docs/adr/0026-lease-timing-durable-storage.md)
  / [0027](docs/adr/0027-replica-group-commit.md) /
  [0028](docs/adr/0028-link-gated-voter-admission.md)). Opt out with
  `MQTTD_DURABLE_SESSIONS=0` for the bounded in-memory store. Proven by a 3-node
  integration test (an enqueue is quorum-durable across the real peer mesh).
  **Resizing a running durable cluster is data-safe**
  ([ADR 0043](docs/adr/0043-elastic-cluster-resize.md)): growing back-fills each new
  replica behind a durable caught-up watermark before it can anchor a recovery (P1),
  a ring change materializes moved sessions eagerly instead of on first touch (P2),
  and **planned removal is a decommission** (P3): `SIGUSR1` drains — the node hands
  every key it holds to each group's post-departure replica set and verifies the
  copies landed (progress on `/readyz`) — then leaves gracefully; a mid-drain crash
  is just a crash. Verified end to end: grow 1→3 under acked traffic and kill the
  founder; decommission a 4-node cluster's session owner — zero acked loss either way.
- **Durable single-owner retained messages** ([ADR 0037](docs/adr/0037-durable-retained-messages.md),
  on whenever durable sessions are — the default). Retained conflicts are **prevented,
  not resolved**: every retained mutation commits through its topic's group lease-owner
  into the quorum-replicated log, and all cache/back-fill decisions reduce to a
  consensus-issued `(epoch, offset)` token — **no wall-clock in correctness**, and no
  acknowledged write is ever silently discarded. Subscribe-time replay stays a local
  read; caches are warmed by the owner's post-commit fan-out and healed by
  token-aware back-fill on link-up (committed clears propagate as tombstones). The
  **CP trade, explicitly**: during a partition the quorum-less side serves the last
  *committed* value (staleness, never divergence) while its own retained writes
  **queue until heal** — bounded per node (1024), oldest dropped loudly
  (`retained_queue_dropped_total`) if the partition outlasts the queue. With durable
  off, retained falls back to ADR 0014's best-effort broadcast, divergence caveat
  included. Proven end to end: concurrent same-topic writes on two nodes and
  divergent writes across a severed-and-healed partition both converge cluster-wide
  (`retained_divergence_total` stays 0).

### Observability & resource governance
- **Prometheus metrics** on `GET /metrics` (`MQTTD_METRICS_BIND`), plus optional
  OTLP push to an OpenTelemetry Collector; Kubernetes-style `GET /livez` +
  `/readyz` health probes (`MQTTD_HEALTH_BIND`), the latter reporting membership,
  lease-group readiness, and any in-progress decommission
  ([ADR 0020](docs/adr/0020-metrics-and-observability.md)).
- **Resource governance** ([ADR 0041](docs/adr/0041-resource-governance.md)):
  global and per-IP **connection caps** (`MQTTD_MAX_CONNECTIONS[_PER_IP]`,
  enforced at accept before any TLS work), an **auth-failure penalty box**,
  per-client **subscription/session quotas**, **publish-rate limiting** by TCP
  backpressure (nothing dropped, nothing disconnected), a **retained-topic cap**,
  and a **disk watermark** that sheds load before the store fills.
- **Operator control is signal-driven, not an admin API** (deliberate: the
  health listener stays read-only and unauthenticated): `SIGHUP` reloads the
  security policy on live connections, `SIGUSR1` begins a decommission drain,
  `SIGTERM` graceful-shuts-down.

### Assurance
Continuous, not audited-once ([ADR 0044](docs/adr/0044-release-readiness-assurance.md)):
an in-process **acked-facts oracle** over seeded fault schedules and an
**out-of-process harness** driving real spawned binaries through kernel
`SIGKILL` (incl. mid-write), disk-full, partitions, and a **two-binary rolling
upgrade + rollback**; an hour-long **soak** watched for memory/FD/latency drift;
**fuzzing** of every attacker-reachable parser; recorded **performance
baselines** with a per-PR regression gate; and **two independent foreign-client
conformance oracles** (Mosquitto + Paho) plus a quickstart-as-test that runs the
README's own cluster commands. Security reporting is in [SECURITY.md](SECURITY.md).

### Planned
- **Subscription digests (bloom)** for sub-linear fan-out.
- **Signed, reproducible releases with an SBOM** — and the first tagged release
  itself (no release exists yet).
- MQTT 5 **Server-Reference redirect** for v5 clients that opt into following it
  (the session relay remains the universal path meanwhile — ADR 0005 P3).

## Workspace layout

| Crate | Responsibility |
|---|---|
| `mqtt-codec` | MQTT 3.1.1 + 5.0 wire codec (all packets, properties, reason codes) + fuzz harness |
| `mqtt-core` | Sessions, subscription table, topic matching, ACL filter relations |
| `mqtt-net` | Framing over any transport; the single audited TLS-config module |
| `mqtt-auth` | `Authenticator`/`Authorizer` traits; mTLS-CN, Argon2id, JWT, ACL providers |
| `mqtt-storage` | Pluggable persistence (`SessionStore`, `RetainedStore`) + in-memory impls |
| `mqtt-cluster` | SWIM membership + gossip auth, HRW placement ring, peer wire protocol |
| `mqtt-observability` | Tracing + a hash-chained, tamper-evident audit log |
| `mqtt-config` | Typed config with secure defaults |
| `mqttd` | The server binary: hub routing actor, connections, peer mesh |

## Build & test

```sh
cargo build
cargo test
cargo clippy --all-targets
cargo deny check          # supply-chain: licenses, advisories, bans, sources

# Fuzz any attacker-reachable parser (ADR 0044 P5). Requires nightly + cargo-fuzz:
#   cargo install cargo-fuzz
cargo +nightly fuzz run packet_decode --fuzz-dir crates/mqtt-codec/fuzz    # MQTT client codec
cargo +nightly fuzz run gossip_open  --fuzz-dir crates/mqtt-cluster/fuzz   # pre-auth SWIM datagram
cargo +nightly fuzz run peer_decode  --fuzz-dir crates/mqtt-cluster/fuzz   # peer-bus frames
# also: swim_message (mqtt-cluster), crl_parse + acl_parse (mqtt-auth)

# Hot-path benchmarks + the per-PR regression floor (ADR 0044 P6; see docs/benchmarks/BASELINE.md):
cargo bench -p mqtt-codec                     # codec encode/decode
cargo bench -p mqtt-cluster                   # replica apply + peer frame codec
cargo test  -p mqtt-codec --test perf_gate    # the throughput floor that runs on every PR

# Foreign-client interop conformance (ADR 0034): drives the real mqttd binary with the
# Eclipse Mosquitto CLI — a non-Rust client that shares no code with the broker's codec, so
# it catches conformance drift the self-codec tests cannot. Needs `mosquitto-clients`,
# `openssl`, `python3`, `curl` on PATH; adds NO crate to the dependency tree. Runs in CI.
./scripts/interop/run.sh
```

Security reporting and the continuous-assurance posture (fuzzing, the acked-facts oracle,
soak, rolling-upgrade tests) are documented in [SECURITY.md](SECURITY.md).

The interop suite asserts v3.1.1 round-trips at QoS 0/1/2, a retained message to a late
subscriber, an MQTT 5 **User Property** surviving a hop (ADR 0030), and OpenSSL↔rustls TLS 1.3
plus mTLS — all against an independent implementation.

## Running

> The examples below use the **plaintext** listener for a quick local loop.
> Plaintext is insecure, opt-in, and loudly logged. For a real deployment use
> the TLS + auth environment variables in [Configuration](#configuration).

### Single node (insecure, local testing)

```sh
MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 cargo run --bin mqttd
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

### Two-node cluster via gossip discovery (insecure, local testing)

Nodes find each other through SWIM and establish the peer mesh automatically —
no static peer list. Node B seeds off node A's gossip address.

```sh
# Node A — client :1883, peer :7001, gossip :7946 (seed)
MQTTD_NODE_ID=node-a MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 \
  MQTTD_PEER_BIND=127.0.0.1:7001 MQTTD_SWIM_BIND=127.0.0.1:7946 \
  cargo run --bin mqttd &
# Node B — client :1884, peer :7002, gossip :7947, seeds off A
MQTTD_NODE_ID=node-b MQTTD_PLAINTEXT_BIND=127.0.0.1:1884 \
  MQTTD_PEER_BIND=127.0.0.1:7002 MQTTD_SWIM_BIND=127.0.0.1:7947 \
  MQTTD_SWIM_SEEDS=127.0.0.1:7946 cargo run --bin mqttd &

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'fleet/+/telemetry' &           # on node A
mosquitto_pub -h 127.0.0.1 -p 1884 -t 'fleet/truck7/telemetry' -m hi  # on node B
```

## Configuration

Configuration is via environment variables until file-based config lands. Unset
or empty means "off"; every insecure fallback is logged at startup.

### Identity & client listeners
| Variable | Purpose |
|---|---|
| `MQTTD_NODE_ID` | This node's id (default `node-local`) |
| `MQTTD_MAX_QUEUED_MESSAGES` | Per-session offline-queue cap (default `100000`) |
| `MQTTD_QUEUE_OVERFLOW` | `drop-oldest` (default) or `reject-newest` |
| `MQTTD_TOPIC_ALIAS_MAX` | Topic Alias Maximum advertised to v5 clients (ADR 0011; default `16`, `0` disables) |
| `MQTTD_RECEIVE_MAXIMUM` | Receive Maximum advertised to v5 clients (ADR 0012; default `256`). Exceeding it → DISCONNECT `0x93` |
| `MQTTD_MAX_CONNECTIONS` | Global concurrent-connection cap (ADR 0041). An over-cap connection is closed **at accept, before any TLS work**; a freed slot is immediately reusable. Unset = uncapped |
| `MQTTD_MAX_CONNECTIONS_PER_IP` | Concurrent-connection cap per source IP (ADR 0041), enforced the same way. The accounting table is bounded by live connections. Unset = uncapped |
| `MQTTD_AUTH_PENALTY_THRESHOLD` | Auth-failure penalty box (ADR 0041): after this many failed authentications from one **source address**, its connections are closed at accept — before any Argon2 work — until the strikes decay. Keys on the address, never the username. Unset = disabled |
| `MQTTD_AUTH_PENALTY_DECAY_SECS` | How long one auth-failure strike takes to decay (default `60`; needs `…_THRESHOLD`) |
| `MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT` | Subscription quota (ADR 0041): a SUBSCRIBE filter beyond it is denied `0x97 Quota exceeded` (v5) / `0x80` (v3.1.1) in its SUBACK slot; in-cap filters in the same packet are granted, and re-subscribing a held filter never consumes quota. Unset = uncapped |
| `MQTTD_MAX_PUBLISH_RATE` | Per-connection inbound publish rate (messages/second, ADR 0041). An over-rate publisher is slowed by **pausing its socket read** (TCP backpressure) — nothing is dropped, nothing is disconnected. Unset = unlimited |
| `MQTTD_MAX_RETAINED_MESSAGES` | Retained-topic cap (ADR 0041). A retained publish creating a **new** topic beyond it is refused (`0x97` v5; v3.1.1 is delivered live but not retained, counted); overwriting or clearing an existing topic always works — the cap stops growth, never maintenance. Unset = uncapped |
| `MQTTD_MAX_SESSIONS` | Session cap (ADR 0041). A CONNECT creating a **new** session beyond it is refused (`0x97` v5, Server-unavailable v3.1.1); resuming an existing session is never refused — a full broker keeps serving its fleet and refuses only strangers. Unset = uncapped |
| `MQTTD_MAX_PACKET_SIZE` | Inbound packet ceiling in bytes (default 1 MiB, floor 1 KiB), advertised to v5 clients as the MQTT 5 **Maximum Packet Size** — the transport cap and the advertised contract cannot drift apart. Outbound, a message larger than the *client's* advertised maximum is dropped for that subscriber only, per spec |
| `MQTTD_STORE_MAX_BYTES` | Disk watermark over the node's on-disk stores, total bytes (ADR 0041; needs `MQTTD_DATA_DIR`). Above it the broker **browns out**: writes that *grow* durable state (new retained topics, new sessions, offline enqueues) are refused with the quota behaviors, while acks, deletes, expiry, and resumes continue — read-mostly, never the disk-full cliff; dropping back under restores writes. Per-store sizes are always exported as the `store_bytes{store}` gauge. Unset = no watermark |
| `MQTTD_AUTH_TIMEOUT` | Per-round enhanced-auth reply timeout, seconds (ADR 0013; default `10`) |
| `MQTTD_DURABLE_SESSIONS` | Durable, consensus-backed replicated session store (ADR 0006/0007) — **on by default** (ADR 0029); set `0`/`false`/`off`/`no` for the lightweight in-memory store. A node with no `MQTTD_SWIM_SEEDS` founds the lease group |
| `MQTTD_DATA_DIR` | Directory for on-disk persistence (ADR 0018). With durable on (default) the lease group + replicated log are on-disk, surviving a full-cluster restart (recommended for production); unset → in-memory |
| `MQTTD_LEASE_VOTERS` | Bounded lease-consensus voter set `N` (ADR 0021; default `5`, recommend odd). At most `N` members vote on lease ownership; every other member joins as a learner that still receives the lease log and can own/serve sessions — so consensus cost stays fixed (quorum `⌊N/2⌋+1`) as the cluster grows. `1` = no fault tolerance, `3` tolerates one voter loss, `5` two |
| `MQTTD_FAILURE_DOMAIN` | This node's own failure-domain label (ADR 0016 T5), e.g. `rack-a`. Advertised over the authenticated SWIM gossip so the topology **self-assembles** — the bounded voter set spreads across racks/zones (losing a whole domain can't take quorum) with each node setting only its own label. The preferred mechanism. Unset → this node is unlabelled unless a peer/static map supplies one. If the cluster-bus cert **attests** a label (ADR 0016 T6), the cert wins: this value must match it (or peers reject this node's gossip) and may be omitted |
| `MQTTD_FAILURE_DOMAINS` | Static failure-domain topology (ADR 0016 T4): `node-id=domain` pairs (e.g. `n1=rack-a,n2=rack-a,n3=rack-b`). A cluster-uniform seed/fallback; per-node gossip labels (`MQTTD_FAILURE_DOMAIN`) override it. Unset → no static spread (id-ordered selection unless labels are gossiped) |
| `MQTTD_TLS_BIND` | TLS 1.3 client listener, e.g. `0.0.0.0:8883` (needs `…_CERT`/`…_KEY`) |
| `MQTTD_TLS_CERT` / `MQTTD_TLS_KEY` | Server certificate chain + key (PEM) |
| `MQTTD_TLS_CLIENT_CA` | Require client certs (mTLS); identity = certificate CN |
| `MQTTD_TLS_CRL` | Certificate revocation list (PEM; needs `…_CLIENT_CA`). A client whose cert is listed is refused at the TLS handshake **and its live session is evicted on reload** (ADR 0002/0040); re-read on `SIGHUP`, so a published CRL applies with no restart |
| `MQTTD_WSS_BIND` | MQTT-over-WebSocket **over TLS** (`wss://`), e.g. `0.0.0.0:8884` (ADR 0035; reuses `…_CERT`/`…_KEY`/`…_CLIENT_CA` — same TLS 1.3 + mTLS + hot reload as the TLS listener) |
| `MQTTD_WS_BIND` | **Insecure** plaintext MQTT-over-WebSocket (`ws://`) — for browsers in local/dev only (ADR 0035) |
| `MQTTD_QUIC_BIND` | MQTT-over-QUIC (UDP), e.g. `0.0.0.0:8885` (ADR 0036; reuses `…_CERT`/`…_KEY`/`…_CLIENT_CA`). QUIC mandates TLS 1.3 (no plaintext mode); **multi-stream** (one session across many streams, no head-of-line blocking); **non-standard** (EMQX-style), identity = leaf CN, no 0-RTT for CONNECT |
| `MQTTD_PLAINTEXT_BIND` | **Insecure** plaintext TCP client listener |

### Client authentication & authorization
| Variable | Purpose |
|---|---|
| `MQTTD_ALLOW_ANONYMOUS` | **Insecure**: permit clients with no credentials |
| `MQTTD_PASSWORD_FILE` | Argon2id `username:phc-hash` password file |
| `MQTTD_JWT_HS256_SECRET` / `MQTTD_JWT_RS256_PEM` | JWT verification key |
| `MQTTD_JWT_ISSUER` / `MQTTD_JWT_AUDIENCE` | Optional JWT `iss`/`aud` constraints |
| `MQTTD_ACL_FILE` | TOML topic-ACL policy (deny by default) |
| `MQTTD_CONFIG_WATCH` | Opt-in filesystem auto-reload (ADR 0033): poll interval in **seconds**. When a configured policy file changes on disk, reload via the same validate-before-swap routine as `SIGHUP` (no restart) — the Kubernetes ConfigMap case. Unset/`0` = disabled (signal-only default) |

### Cluster transport & membership
| Variable | Purpose |
|---|---|
| `MQTTD_PEER_BIND` | Inter-node peer listener, e.g. `0.0.0.0:7001` |
| `MQTTD_PEER_TLS_CA` / `…_CERT` / `…_KEY` | Cluster-bus mTLS material (set all three). A leaf whose SANs include `URI:urn:fss:failure-domain:<label>` has its failure domain **CA-attested** (ADR 0016 T6): the label is authoritative on the gossip plane (a contradicting self-claim is rejected) and can replace `MQTTD_FAILURE_DOMAIN` entirely — relabel by reissuing the cert |
| `MQTTD_PEER_TLS_CRL` | Cluster-bus CRL (PEM, **signed by the cluster CA**; needs the three above). Signed gossip from a revoked cert is dropped (ADR 0022 T7), fresh peer handshakes are refused in both directions, and **established peer links are torn down on reload** (ADR 0040); expired/not-yet-valid certs are rejected regardless. Hot-reloads via `SIGHUP`/`MQTTD_CONFIG_WATCH`, so publishing a CRL evicts a compromised node with no restart |
| `MQTTD_PEERS` | Comma-separated static peer addresses (alternative to gossip) |
| `MQTTD_SWIM_BIND` | SWIM gossip UDP bind (needs `MQTTD_PEER_BIND`) |
| `MQTTD_SWIM_SEEDS` | Comma-separated gossip addresses of existing members |
| `MQTTD_SWIM_KEY` | 64-hex-char cluster gossip key (`openssl rand -hex 32`) |
| `MQTTD_HEALTH_BIND` | HTTP health-probe bind, e.g. `0.0.0.0:8080` — serves `GET /livez`, `/readyz` & `/metrics` (Prometheus) |
| `MQTTD_READY_MIN_MEMBERS` | Smallest mesh size `/readyz` accepts (default 1) |
| `MQTTD_METRICS_BIND` | Optional separate bind for `GET /metrics`, to isolate the scrape from the health probes (internal/ops network only) |
| `MQTTD_OTLP_ENDPOINT` | OTLP/HTTP base URL of an OpenTelemetry Collector, e.g. `http://collector:4318` — when set, metrics are also pushed via OTLP (`/v1/metrics` appended) |
| `MQTTD_OTLP_INTERVAL` | OTLP push interval in seconds (default `10`) |

### Health probes

With `MQTTD_HEALTH_BIND` set, the broker serves two Kubernetes-style endpoints over
plain HTTP (no framework — a minimal hand-rolled server):

- **`GET /livez`** (alias `/healthz`) — *liveness*: `200` while the routing hub is
  draining commands; `503` if it is wedged. Wire to a k8s **livenessProbe** (restart
  on failure).
- **`GET /readyz`** — *readiness*: `200` only when the node is live, the mesh has at
  least `MQTTD_READY_MIN_MEMBERS` members, and — with `MQTTD_DURABLE_SESSIONS` on —
  the lease group is ready (a leader exists and this node is a voter, so it can
  durably own the sessions it would be handed). Wire to a k8s **readinessProbe** so a
  node is pulled from the Service during a rolling restart or a transient lease blip
  *without* being killed. Body example: `{"status":"ok","live":true,"ready":true,"members":3,"lease_group_ready":true}`.

### Hot reload (SIGHUP)

Send `SIGHUP` to rotate the security policy **without a restart** and **without dropping
connections** (ADR 0032):

```sh
kill -HUP "$(pidof mqttd)"   # re-read ACL, authenticators, and TLS cert/key/client-CA
```

The broker re-reads the configured files in place and swaps them on **live** connections:

- **ACL** (`MQTTD_ACL_FILE`) — a tightened rule denies an *already-connected* client's next
  publish/subscribe; a loosened rule takes effect immediately.
- **Authenticators** (`MQTTD_PASSWORD_FILE`, `MQTTD_JWT_*`) — a rotated password file or JWT
  key authenticates the new credential and rejects the old on the next CONNECT.
- **TLS material** (`MQTTD_TLS_CERT` / `…_KEY` / `…_CLIENT_CA` / `…_CRL`, and the peer-bus
  `MQTTD_PEER_TLS_*` trio) — a renewed certificate is served on the next handshake;
  in-flight TLS sessions of *non-revoked* certs are undisturbed (rotation never drops a
  valid session).

**Revocation reaches live state (ADR 0040).** A successful reload also **sweeps** what is
already connected, with a two-tier rule — *who you are* revoked ends the session; *what you
may read* revoked ends the flow:

- a client whose certificate the new **CRL** names, whose **password user was removed**, or
  whose principal the new **connect-ACL** denies is **disconnected immediately** (MQTT 5
  clients get `DISCONNECT 0x87 Not authorized`; MQTT 3.1.1 has no server DISCONNECT, so the
  connection just closes; the will is published and session retention proceeds normally);
- an existing **subscription** whose filter the tightened ACL denies stops delivering — it
  is removed from routing *and* the durable session set (offline sessions are re-checked at
  resume, and queued messages only the revoked grant admits are not replayed). The client
  stays connected; its next SUBSCRIBE is denied;
- an established **peer link** whose remote certificate the new cluster CRL
  (`MQTTD_PEER_TLS_CRL`) revokes is torn down, and the revoked node cannot re-handshake in
  either direction. The mesh reacts as to any link loss.

An unchanged policy evicts no one (the sweep re-derives each admission verdict, so only
differences act). Each action emits a `security.evict` audit event with its reason
(`cert-revoked`, `user-removed`, `connect-denied`, `grant-revoked`, `peer-revoked`) and
increments `mqttd_revocation_evictions_total{reason}`; every sweep leaves one
`security.sweep` summary record with the counts. Durable session *state* of a removed user
is not destroyed — it is unreachable (resume fails at authentication; a different subject is
refused by the ADR 0031 owner binding) and expires on schedule.

The reload is **validate-before-swap and all-or-nothing**: every file is parsed first, and
the swap is applied only if *all* succeed. A missing or unparseable file is **rejected** —
the running policy is kept exactly as it was (the broker never fails open and never bricks
itself on a typo). Every reload, success or rejection, emits a `security.reload` audit event
and increments the `mqttd_security_reloads_total{outcome,trigger}` metric. To rotate paths (not
just file contents) restart the broker.

**Filesystem auto-reload (opt-in, ADR 0033).** For declarative/GitOps operation — a Kubernetes
ConfigMap/Secret is updated **on disk** with no process signal — set `MQTTD_CONFIG_WATCH=<seconds>`
to poll the configured policy files and reload automatically when one changes, through the **same**
validate-before-swap routine (a partial write is rejected and retried until it parses cleanly, so
no torn config is ever applied). It is **off by default**; `SIGHUP` stays the default trigger and
both can run at once. The reload audit/metric carry a `trigger` of `signal` or `watch`. On non-Unix
platforms (no `SIGHUP`) the watcher is the only reload mechanism.

### Metrics

The broker exports Prometheus-style metrics (connections, publish/deliver, sessions,
retained — including the `retained_divergence_total` convergence meter and the
`retained_queue_dropped_total` queue-until-heal bound counter (ADR 0037) — cluster
membership, lease role/epoch, durable-append latency/failures, gossip rejects,
security reloads) with bounded label sets — no per-client or per-topic labels. Two ways to consume
them, both from the one registry (ADR 0020):

- **Prometheus (pull)** — `GET /metrics` on the health server (`MQTTD_HEALTH_BIND`), or on a
  separate `MQTTD_METRICS_BIND` to keep the scrape off the probe port.
- **OTLP (push)** — set `MQTTD_OTLP_ENDPOINT` to an OpenTelemetry Collector's OTLP/HTTP base
  URL (e.g. `http://collector:4318`) and the same metrics are pushed every
  `MQTTD_OTLP_INTERVAL` seconds (default 10) as `service.name=mqttd`, in addition to the
  Prometheus endpoint. Unset = Prometheus only.

```sh
# Prometheus scrape
MQTTD_HEALTH_BIND=0.0.0.0:8080 cargo run --bin mqttd   # then GET :8080/metrics

# also push to an OpenTelemetry Collector
MQTTD_HEALTH_BIND=0.0.0.0:8080 MQTTD_OTLP_ENDPOINT=http://localhost:4318 \
  cargo run --bin mqttd
```

For a turnkey view of all of this, [`demo/`](demo/) brings up a **3-node durable cluster**
with **Grafana + Prometheus + Alloy** and a provisioned dashboard covering every metric —
both the Prometheus scrape and the OTLP push paths:

```sh
cd demo && docker compose up --build   # then http://localhost:3000
```

The cluster runs **durable sessions by default** (ADR 0029), each node persisting its lease
group and replicated session log to its own volume, so the `lease_*` / `durable_append_*`
panels populate with a real leader. The durable group forms in ~90s and holds a flat term
under load (ADR [0026](docs/adr/0026-lease-timing-durable-storage.md) /
[0027](docs/adr/0027-replica-group-commit.md) /
[0028](docs/adr/0028-link-gated-voter-admission.md)).

## Resizing the cluster

Grow, shrink, and replace are first-class, **data-safe** operations on a running
durable cluster ([ADR 0043](docs/adr/0043-elastic-cluster-resize.md)) — verified by
the same acked-facts stress oracle as every crash fault. Pulling a plug instead is
always allowed: that is crash semantics, and the survivors recover from their
replicas.

**Grow.** Start the new node with `MQTTD_SWIM_SEEDS` pointing at any member (and its
own `MQTTD_DATA_DIR` / cluster-bus cert). The cluster does the rest: the joiner
back-fills every replica set it enters behind a durable caught-up watermark — until
then it counts toward no recovery — and ownership it gains is materialized eagerly,
with publisher acknowledgements held honest through the window. Growing a 1-node
broker re-replicates its whole history the same way: the laptop→server upgrade is
just "start two more nodes". Watch `/readyz` on the joiner (`lease_group_ready`) and
route client traffic to it once ready.

**The two-node truth.** Two members mean replica sets of two and a write quorum of
2-of-2 — a two-node durable cluster has *strictly worse* write availability than one
node (either node down blocks durable writes). Two nodes are supported as a
waypoint, but the recommended upgrade is **1→3 in one motion**: start both new nodes,
then treat the pair-state as transient.

**Shrink (decommission).** Send the node `SIGUSR1`. It fails readiness immediately,
then **drains**: every durable key it holds is handed to the replica set each group
will have after its departure, and verified there — progress is visible on `/readyz`
as `decommission{pending,rounds,complete}` — and only then does it run the ordinary
graceful leave (ownership moves, voters rebalance). A drain that cannot converge
(unreachable successors) waits rather than lies; `SIGTERM` escalates to a plain
shutdown at any time, and a mid-drain crash is just a crash. Repeat one node at a
time for a 5→3 cost reduction, letting membership settle between steps.

**Replace a host.** Grow by the replacement first, then decommission the old node —
same size before and after, zero acked loss. Rolling binary upgrades
([ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md)) ride the same
one-node-at-a-time motion.

## Upgrades & versioning

From **1.0.0** ([ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md); until then
the pre-release freeze regime of [ADR 0038](docs/adr/0038-prerelease-compatibility-freeze.md)
applies — formats may change freely, wipe-and-rejoin on schema bumps):

- **Semantic versioning, defined by what breaks**: MAJOR = wire/disk/config breaking;
  MINOR = additive and fully compatible (a mixed cluster of adjacent minors works);
  PATCH = fixes only, no format changes.
- **Adjacent version skew only**: a cluster may mix release N and N+1 — the rolling
  upgrade state — and nothing wider. Enforced mechanically: the peer handshake
  negotiates a protocol range and fails closed (loudly) on disjoint ranges.
- **Sequential major upgrades, rolled through a gateway minor** (1 → 2 → 3, no
  skipping): each new major names the minor it upgrades from — by default the
  previous major's last minor, where known upgrade issues are fixed first — and the
  handshake refuses older nodes, so the path is "roll to the gateway minor, then roll
  to the new major". Store layouts migrate exactly one major back, dispatched on the
  per-store schema stamp; the gate's error names the version to route through.
- **Three supported lines**: patches and security fixes land on the latest three minor
  lines; older lines are EOL.
- **MQTT clients are exempt**: client compatibility is governed by the MQTT
  specifications (3.1.1 / 5.0), not by this policy — clients of any age keep working.

## Performance

Hot-path CPU costs, measured with [criterion](https://github.com/bheisler/criterion.rs)
on a 4-core Xeon, `--release` (full numbers and method in
[docs/benchmarks/BASELINE.md](docs/benchmarks/BASELINE.md)):

- **MQTT codec** — a 256-byte PUBLISH encodes in ~270 ns and decodes in ~190 ns; the
  codec alone sustains on the order of a couple of million messages per second per core.
- **Durable plane** — an in-memory replica apply runs in ~290 ns; a peer replication
  frame encodes in ~280 ns and decodes in ~420 ns (the fsync cost is the disk's, not the
  broker's — this is the CPU work a code change can regress).

These are micro-benchmarks — the broker's own CPU work, isolated from network and disk —
not an end-to-end throughput claim; what they guarantee is that the broker is not the
bottleneck and does not silently regress. A per-PR **regression floor**
(`cargo test -p mqtt-codec --test perf_gate`) fails the build on a gross slowdown, and the
nightly tier re-runs the full benches ([ADR 0044](docs/adr/0044-release-readiness-assurance.md) P6).

## Architecture decisions

Every significant decision is recorded as an ADR. See
[`docs/adr/`](docs/adr/README.md) for the model and conventions, and the generated
[**delivery dashboard**](docs/delivery/STATUS.md) for the full catalogue of decisions
and their live build status.

## License

Apache-2.0. See [LICENSE](LICENSE).
