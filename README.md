# mqttd — a security-first, cluster-native MQTT broker

> An MQTT 3.1.1 + 5.0 broker built to be the most cyber-secure broker available,
> with linear horizontal scalability and a 100% open feature set.

**Status:** Phase 0 — infrastructure scaffold.

## Principles

- **Security is the product.** Secure by default; insecure modes must be opted into and are loudly logged.
- **Open == Enterprise.** One Apache-2.0 codebase, no gated features. Only support is paid.
- **Linear scalability.** Shared-nothing nodes; no coordinator on the publish hot path.
- **Memory safety.** Rust, `#![forbid(unsafe_code)]` across crates.

See [`docs/CAPABILITY-PLAN.md`](docs/CAPABILITY-PLAN.md) for the full plan and roadmap.

## Workspace layout

| Crate | Responsibility |
|---|---|
| `mqtt-codec` | MQTT 3.1.1 wire codec (all packet types) + fuzz harness; 5.0 framing next |
| `mqtt-core` | Sessions, subscriptions, retained store, QoS state machines |
| `mqtt-net` | Listeners and connection lifecycle (TCP/TLS/WebSocket) |
| `mqtt-auth` | Authentication & authorization traits + providers (deny-by-default) |
| `mqtt-storage` | Pluggable persistence traits (`SessionStore`, `RetainedStore`) |
| `mqtt-cluster` | SWIM membership + HRW placement + peer protocol; cross-node routing (static peers) |
| `mqtt-observability` | Metrics, tracing, hash-chained audit log |
| `mqtt-config` | Typed config with secure defaults |
| `mqttd` | The server binary |

## Build & test

```sh
cargo build
cargo test
cargo clippy --all-targets

# Fuzz the codec (the untrusted-input boundary). Requires nightly + cargo-fuzz:
#   cargo install cargo-fuzz
cargo +nightly fuzz run packet_decode --fuzz-dir crates/mqtt-codec/fuzz
```

## Run a single node

```sh
MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 cargo run --bin mqttd
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

## Run a two-node cluster (static peers)

```sh
# Node A — client :1883, peer :7001, dials B's peer
MQTTD_NODE_ID=node-a MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 \
  MQTTD_PEER_BIND=127.0.0.1:7001 MQTTD_PEERS=127.0.0.1:7002 cargo run --bin mqttd &
# Node B — client :1884, peer :7002, dials A's peer
MQTTD_NODE_ID=node-b MQTTD_PLAINTEXT_BIND=127.0.0.1:1884 \
  MQTTD_PEER_BIND=127.0.0.1:7002 MQTTD_PEERS=127.0.0.1:7001 cargo run --bin mqttd &

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'fleet/+/telemetry' &              # subscriber on node A
mosquitto_pub -h 127.0.0.1 -p 1884 -t 'fleet/truck7/telemetry' -m hi      # publisher on node B
```

Inter-node links and the client listener are currently **plaintext** (opt-in, loudly
logged); mTLS is part of the security milestone.

## License

Apache-2.0. See [LICENSE](LICENSE).
