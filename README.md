# mqttd — a security-first, cluster-native MQTT broker

> An MQTT 3.1.1 + 5.0 broker built to be the most cyber-secure
> broker available, with linear horizontal scalability and a 100% open feature
> set.

**Status:** single-node MQTT 3.1.1 is feature-complete (QoS 0/1/2, retained
messages, wills, keepalive, persistent sessions). Transport security
(TLS 1.3 + mutually-authenticated cluster bus), authenticated gossip membership
with dynamic cross-node routing, and a full identity/authorization stack
(mTLS-CN / password / JWT → topic ACLs → tamper-evident audit) are in place.
**Durable, consensus-backed replicated session storage** (openraft lease group +
epoch-fenced quorum replication, opt-in via `MQTTD_DURABLE_SESSIONS`) is built and
proven over a real cluster, with **cross-node takeover** (a replica serves a session
after its owner dies). The **MQTT 5.0 wire codec** is complete and the broker
**negotiates v5 at CONNECT**; the v5 *semantics* are the next milestone. See
[`docs/CAPABILITY-PLAN.md`](docs/CAPABILITY-PLAN.md) for the product vision,
[`docs/adr/`](docs/adr/) for the decisions behind it, and the
[**delivery dashboard**](docs/delivery/STATUS.md) for live build status.

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
  cluster-CA trust boundary. With `MQTTD_DURABLE_SESSIONS` the owner's session log
  is quorum-replicated (below); on the default in-memory path an owner's death
  still drops its sessions.

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

### In progress / planned
- **MQTT 5.0**: session/message expiry, topic aliases, flow control, shared
  subscriptions, enhanced auth. (Per [ADR 0008](docs/adr/0008-mqtt-5-codec.md), the
  v5 **wire codec is complete** and the broker **negotiates v5 at CONNECT** — a v5
  client connects, gets a v5 CONNACK with v5 reason codes, and exchanges v5-framed
  packets. The v5 *semantics* listed above are the remaining work.)
- Subscription digests (bloom) for sub-linear fan-out.
- WebSocket/WSS listener; Prometheus metrics; admin/management API. (Kubernetes-style
  `GET /livez` + `/readyz` health probes already ship — see `MQTTD_HEALTH_BIND`.)
- Bounded outbound queues, rate limits, connection caps.
- MQTT conformance suite, continuous fuzzing, SBOM + signed reproducible
  releases.

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

# Fuzz the codec (the untrusted-input boundary). Requires nightly + cargo-fuzz:
#   cargo install cargo-fuzz
cargo +nightly fuzz run packet_decode --fuzz-dir crates/mqtt-codec/fuzz

# Foreign-client interop conformance (ADR 0034): drives the real mqttd binary with the
# Eclipse Mosquitto CLI — a non-Rust client that shares no code with the broker's codec, so
# it catches conformance drift the self-codec tests cannot. Needs `mosquitto-clients`,
# `openssl`, `python3`, `curl` on PATH; adds NO crate to the dependency tree. Runs in CI.
./scripts/interop/run.sh
```

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
| `MQTTD_AUTH_TIMEOUT` | Per-round enhanced-auth reply timeout, seconds (ADR 0013; default `10`) |
| `MQTTD_DURABLE_SESSIONS` | Durable, consensus-backed replicated session store (ADR 0006/0007) — **on by default** (ADR 0029); set `0`/`false`/`off`/`no` for the lightweight in-memory store. A node with no `MQTTD_SWIM_SEEDS` founds the lease group |
| `MQTTD_DATA_DIR` | Directory for on-disk persistence (ADR 0018). With durable on (default) the lease group + replicated log are on-disk, surviving a full-cluster restart (recommended for production); unset → in-memory |
| `MQTTD_LEASE_VOTERS` | Bounded lease-consensus voter set `N` (ADR 0021; default `5`, recommend odd). At most `N` members vote on lease ownership; every other member joins as a learner that still receives the lease log and can own/serve sessions — so consensus cost stays fixed (quorum `⌊N/2⌋+1`) as the cluster grows. `1` = no fault tolerance, `3` tolerates one voter loss, `5` two |
| `MQTTD_FAILURE_DOMAIN` | This node's own failure-domain label (ADR 0016 T5), e.g. `rack-a`. Advertised over the authenticated SWIM gossip so the topology **self-assembles** — the bounded voter set spreads across racks/zones (losing a whole domain can't take quorum) with each node setting only its own label. The preferred mechanism. Unset → this node is unlabelled unless a peer/static map supplies one. If the cluster-bus cert **attests** a label (ADR 0016 T6), the cert wins: this value must match it (or peers reject this node's gossip) and may be omitted |
| `MQTTD_FAILURE_DOMAINS` | Static failure-domain topology (ADR 0016 T4): `node-id=domain` pairs (e.g. `n1=rack-a,n2=rack-a,n3=rack-b`). A cluster-uniform seed/fallback; per-node gossip labels (`MQTTD_FAILURE_DOMAIN`) override it. Unset → no static spread (id-ordered selection unless labels are gossiped) |
| `MQTTD_TLS_BIND` | TLS 1.3 client listener, e.g. `0.0.0.0:8883` (needs `…_CERT`/`…_KEY`) |
| `MQTTD_TLS_CERT` / `MQTTD_TLS_KEY` | Server certificate chain + key (PEM) |
| `MQTTD_TLS_CLIENT_CA` | Require client certs (mTLS); identity = certificate CN |
| `MQTTD_TLS_CRL` | Certificate revocation list (PEM; needs `…_CLIENT_CA`). A client whose cert is listed is refused at the TLS handshake; re-read on `SIGHUP`, so a published CRL applies with no restart (ADR 0002) |
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
| `MQTTD_PEER_TLS_CRL` | Cluster-bus CRL (PEM, **signed by the cluster CA**; needs the three above). Signed gossip from a revoked cert is dropped (ADR 0022 T7); expired/not-yet-valid certs are rejected regardless. Hot-reloads via `SIGHUP`/`MQTTD_CONFIG_WATCH`, so publishing a CRL evicts a compromised node with no restart |
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
- **TLS material** (`MQTTD_TLS_CERT` / `…_KEY` / `…_CLIENT_CA` / `…_CRL`) — a renewed
  certificate, or an updated **CRL**, is served on the next handshake; **in-flight TLS
  sessions are undisturbed**. A newly-revoked client cert is refused from the next handshake.

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

## Architecture decisions

Every significant decision is recorded as an ADR. See
[`docs/adr/`](docs/adr/README.md) for the model and conventions, and the generated
[**delivery dashboard**](docs/delivery/STATUS.md) for the full catalogue of decisions
and their live build status.

## License

Apache-2.0. See [LICENSE](LICENSE).
