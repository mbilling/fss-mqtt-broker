# Architecture (for contributors)

**Verified against `v1.0.16` (2026-09-11).** The map a contributor needs before
opening `crates/mqttd`. Promoted from the crate-level docs in `main.rs` /
`lib.rs` and the hub seams in [ADR 0064](adr/0064-hub-module-seams.md). Decision
rationale stays in the ADRs; this page is the tour.

Human workflow is [CONTRIBUTING.md](../CONTRIBUTING.md). Agent sessions also
read [CONTRIBUTING-agent.md](CONTRIBUTING-agent.md). Live task status is the
[delivery dashboard](delivery/STATUS.md).

## What process owns what

`mqttd` is a thin binary over the `mqttd` library. The binary loads
`mqtt_config::Config` (defaults < TOML < `MQTTD_*` < CLI), installs the
process-default crypto provider, binds listeners, and wires:

- **connection tasks** (`conn.rs`) — one per client socket; they never share
  mutable session state.
- **the hub actor** (`hub/`) — single-threaded owner of routing, sessions,
  retained, and every outbound channel.
- **the peer mesh** (`peer.rs`, `mqtt-cluster`) — SWIM membership plus the
  authenticated cluster bus.
- **the durable plane** (`mqtt-storage` + `mqtt-cluster::durable_plane`) —
  lease group, replicated logs, on-disk stores.

Connection tasks send `HubCommand`s. Cross-node routing is another command
source feeding the same hub ([ADR 0001](adr/0001-session-durability.md)).

## Workspace crates

| Crate | Owns |
|---|---|
| `mqtt-codec` | MQTT 3.1.1 + 5.0 wire encode/decode. Fuzzed. |
| `mqtt-core` | Subscription tables (plain + shared), topic matching, message types. |
| `mqtt-net` | Listeners (TCP/TLS/WebSocket/QUIC), connection lifecycle. |
| `mqtt-auth` | `Authenticator` / `Authorizer` traits and built-in providers. |
| `mqtt-storage` | `SessionStore` / `RetainedStore` and the replicated log. |
| `mqtt-cluster` | SWIM, peer frames, placement, durable plane. |
| `mqtt-observability` | Prometheus/OTLP metrics, hash-chained audit. |
| `mqtt-config` | Typed config; `ENV_VARS` is the overlay inventory. |
| `mqttd` | Binary + hub + conn — this page. |
| `mqtt-bridge` | Zone-crossing MQTT client (not a cluster member). |
| `mqttd-operator` | `MqttdCluster` reconciler. |

## `mqttd` library modules (`crates/mqttd/src/lib.rs`)

| Module | Owns |
|---|---|
| `hub/` | The actor. See seams below. |
| `conn` | Per-connection MQTT state machine, reason-code emission, enhanced auth. |
| `peer` | Cluster-bus framing into hub commands. |
| `admission` | Accept-time connection caps (before TLS). |
| `backpressure` | Encapsulated backlog byte accounting (issue #241). |
| `aliases` | MQTT 5 topic aliases (ADR 0011). |
| `reload` / `config_watch` | SIGHUP and filesystem validate-before-swap (ADR 0032/0033). |
| `health` | `/livez`, `/readyz`, `/metrics`, `/statusz`. |
| `backup` | Online export / restore (ADR 0062). |
| `memory_watch` / `store_watch` | Watermark brownout axes (ADR 0041). |
| `store_probe` | Boot-time volume self-measurement (ADR 0076 T1). |
| `oidc` / `http_auth` | Token and remote-hook authenticators. |
| `cluster` | Binary-side mesh wiring. |
| `clock` | Time source for tests vs production. |

The binary's own `main.rs` crate docs are the **configuration and signals
surface**: precedence, `SIGTERM`/`SIGUSR1` (decommission) / `SIGUSR2` (backup),
and subcommands (`--check-config`, `--decommission`, `--backup`, `--hash-password`,
`--probe`). Generated operator copy is [CONFIGURATION.md](CONFIGURATION.md).

## Hub seams (ADR 0064)

`hub.rs` was a 19k-line god-actor. Extraction is **move-only**: each module
owns one invariant, and new code lands in the module whose invariant it
serves.

| Module | Owns |
|---|---|
| `hub/retained.rs` | A retained value is one cluster-wide fact — tokens, digests, tombstones, windowed replay (ADR 0037). |
| `hub/policy.rs` | A refusal is an on-loop, effect-free policy decision; brownout axes gate growth (ADR 0041). |
| `hub/lanes.rs` | Nothing the store must answer runs on the loop — frozen jobs, per-session FIFO, owned workers (ADR 0061). |
| `hub/delivery.rs` | One plan per answerable publish; ack-after-durable; the send chain stays `fn` so the compiler forbids on-loop store awaits. Shared-subscription selection (locality preference, `remote_by_filter`) lives here. |
| `hub/forwarding.rs` | An `Accepted` releases only against recorded evidence — obligations, verdicts, retransmission. |
| `hub/qos2.rs` | Outbound QoS 2 identity / retirement helpers. |
| `hub/mod.rs` | The actor itself: state, dispatch, session lifecycle, sweep, mesh/settle honesty gates. |

Struct fields remain in `mod.rs`. Tests remain in `mod.rs`'s `mod tests` so a
seam extraction cannot hide a behaviour change by moving the oracle.

## Delivery semantics the hub will not violate

From `hub/mod.rs`:

- Effective QoS is `min(publish, granted)` ([MQTT-3.8.4-6]).
- QoS 1/2 are tracked per session until acknowledged; resume redelivers with
  `DUP`; QoS 2 runs PUBREC/PUBREL/PUBCOMP.
- Retained messages replay (retain flag set) on every **ordinary** new
  subscription; shared subscriptions skip retained replay ([MQTT-3.8.4]).
- Persistent sessions (`clean_session=false` / v5 expiry > 0) keep
  subscriptions and the offline queue across disconnects.
- The offline queue is bounded (drop-oldest / reject-newest). The outbound
  socket channel is not bounded by a blocking cap — QoS 0 is shed instead
  (#123 / `MQTTD_MAX_OUTBOUND_BYTES`).

## Signals and subcommands (binary)

| Signal / flag | Effect |
|---|---|
| `SIGTERM` / `SIGINT` | ADR 0019 graceful drain; second signal forces exit. `/readyz` flips immediately. |
| `SIGUSR1` / `mqttd --decommission` | ADR 0043 drain: hand off durable keys, then leave. Kubernetes `preStop`. |
| `SIGUSR2` / `mqttd --backup` | ADR 0062 online export into `[backup] dir`. Handler is installed even with no dir configured (default SIGUSR2 would otherwise *kill* the process). |
| `SIGHUP` / `MQTTD_CONFIG_WATCH` | Validate-before-swap reload (ADR 0032/0033). |
| `--check-config` | Validate and exit; bind nothing. |

## Where to put a change

1. If it is a *decision*, it needs an ADR (or an amendment to one). Progress
   goes in the delivery doc; run `python3 scripts/gen-status.py`.
2. If it is hub behaviour, land it in the seam whose invariant it serves —
   not in `mod.rs` "because that is where the other match arms are".
3. If it is a config knob, add the field, the `ENV_VARS` entry, the
   `overlay_from` mapping, and regenerate [CONFIGURATION.md](CONFIGURATION.md).
4. If it is user-facing, name the stakeholder document that carries it
   (ADR 0070). The index is [README.md](README.md) in this directory.
