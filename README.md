# mqttd — a security-first, cluster-native MQTT broker

> An MQTT 3.1.1 broker (5.0 in progress) built to be the most cyber-secure
> broker available, with linear horizontal scalability and a 100% open feature
> set.

**Status:** single-node MQTT 3.1.1 is feature-complete (QoS 0/1/2, retained
messages, wills, keepalive, persistent sessions). Transport security
(TLS 1.3 + mutually-authenticated cluster bus), authenticated gossip membership
with dynamic cross-node routing, and a full identity/authorization stack
(mTLS-CN / password / JWT → topic ACLs → tamper-evident audit) are in place.
**Durable replicated storage** and the **MQTT 5.0 codec** are the next
milestones. See [`docs/CAPABILITY-PLAN.md`](docs/CAPABILITY-PLAN.md) for the
full roadmap and [`docs/adr/`](docs/adr/) for the decisions behind it.

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
  certificate mTLS — [ADR 0002](docs/adr/0002-transport-security.md).
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
- HRW (rendezvous) placement primitive, ready for the durability design below.

### In progress / planned
- **Durable, replicated session storage + session takeover**
  ([ADR 0001](docs/adr/0001-session-durability.md);
  [implementation plan](docs/CLUSTER-DURABILITY-PLAN.md)). Sessions are in-memory
  today.
- **MQTT 5.0**: properties, reason codes, session/message expiry, topic aliases,
  flow control, shared subscriptions, enhanced auth. (v5 CONNECT is currently
  rejected at the codec.)
- Subscription digests (bloom) for sub-linear fan-out; retained-state
  replication across nodes.
- WebSocket/WSS listener; Prometheus metrics; admin/management API.
- Bounded outbound queues, rate limits, connection caps.
- MQTT conformance suite, continuous fuzzing, SBOM + signed reproducible
  releases.

## Workspace layout

| Crate | Responsibility |
|---|---|
| `mqtt-codec` | MQTT 3.1.1 wire codec (all packet types) + fuzz harness; 5.0 next |
| `mqtt-core` | Sessions, subscription table, topic matching, ACL filter relations |
| `mqtt-net` | Framing over any transport; the single audited TLS-config module |
| `mqtt-auth` | `Authenticator`/`Authorizer` traits; mTLS-CN, Argon2id, JWT, ACL providers |
| `mqtt-storage` | Pluggable persistence (`SessionStore`, `RetainedStore`) + in-memory impls |
| `mqtt-cluster` | SWIM membership + gossip auth, HRW placement, peer wire protocol |
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
```

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
| `MQTTD_TLS_BIND` | TLS 1.3 client listener, e.g. `0.0.0.0:8883` (needs `…_CERT`/`…_KEY`) |
| `MQTTD_TLS_CERT` / `MQTTD_TLS_KEY` | Server certificate chain + key (PEM) |
| `MQTTD_TLS_CLIENT_CA` | Require client certs (mTLS); identity = certificate CN |
| `MQTTD_PLAINTEXT_BIND` | **Insecure** plaintext client listener |

### Client authentication & authorization
| Variable | Purpose |
|---|---|
| `MQTTD_ALLOW_ANONYMOUS` | **Insecure**: permit clients with no credentials |
| `MQTTD_PASSWORD_FILE` | Argon2id `username:phc-hash` password file |
| `MQTTD_JWT_HS256_SECRET` / `MQTTD_JWT_RS256_PEM` | JWT verification key |
| `MQTTD_JWT_ISSUER` / `MQTTD_JWT_AUDIENCE` | Optional JWT `iss`/`aud` constraints |
| `MQTTD_ACL_FILE` | TOML topic-ACL policy (deny by default) |

### Cluster transport & membership
| Variable | Purpose |
|---|---|
| `MQTTD_PEER_BIND` | Inter-node peer listener, e.g. `0.0.0.0:7001` |
| `MQTTD_PEER_TLS_CA` / `…_CERT` / `…_KEY` | Cluster-bus mTLS material (set all three) |
| `MQTTD_PEERS` | Comma-separated static peer addresses (alternative to gossip) |
| `MQTTD_SWIM_BIND` | SWIM gossip UDP bind (needs `MQTTD_PEER_BIND`) |
| `MQTTD_SWIM_SEEDS` | Comma-separated gossip addresses of existing members |
| `MQTTD_SWIM_KEY` | 64-hex-char cluster gossip key (`openssl rand -hex 32`) |

## Architecture decisions

| # | Decision |
|---|---|
| [0001](docs/adr/0001-session-durability.md) | Session durability in a horizontally-scalable cluster (design; [plan](docs/CLUSTER-DURABILITY-PLAN.md)) |
| [0002](docs/adr/0002-transport-security.md) | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus |
| [0003](docs/adr/0003-gossip-authentication.md) | Gossip-plane authentication: keyed MAC on SWIM datagrams |
| [0004](docs/adr/0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default |

## License

Apache-2.0. See [LICENSE](LICENSE).
