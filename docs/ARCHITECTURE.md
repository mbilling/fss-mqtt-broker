# Architecture (for contributors)

**Verified against `v1.0.16` (2026-09-11).** The map a contributor needs before
opening `crates/mqttd`. Promoted from the crate-level docs in `main.rs` /
`lib.rs` and the hub seams in [ADR 0064](adr/0064-hub-module-seams.md). Decision
rationale stays in the ADRs; this page is the tour.

Human workflow is [CONTRIBUTING.md](../CONTRIBUTING.md). Agent sessions also
read [CONTRIBUTING-agent.md](CONTRIBUTING-agent.md). Live task status is the
[delivery dashboard](delivery/STATUS.md).

## The picture

The README's Architecture section until 2026-10-25: one node, the mesh, the durable plane.

```text
        MQTT clients  (TCP · TLS 1.3 · WebSocket · QUIC)
              │  identity: mTLS-CN / password / JWT / OIDC
              ▼        ↓ deny-by-default topic ACL
       ┌──────────────────────────────────────────┐
       │  node                                    │
       │   listeners → per-connection tasks       │
       │                    ▼                     │
       │            hub (routing actor)           │   one hub per node;
       │        subscriptions · retained · queues │   no lock on the hot path
       └────────────────────┼─────────────────────┘
   ┌────────────────────────┼────────────────────────┐
   │ SWIM gossip            │ peer links (mTLS)      │  one trust domain =
   │ membership, interest   │ interest-based forward │  one logical broker
   └────────────────────────┼────────────────────────┘
                            ▼
              durable plane — openraft lease group
              epoch-fenced quorum replication
```

- shared-nothing nodes; one routing actor per node; cross-node traffic is just another command
- durable write **before** the wire send → "acked" means "replicated"; clean sessions and QoS 0 skip it
- consensus for control (epochs, ownership), small replica sets for data
- refuse at the edge: reason code or backpressure, never a silent drop
- bridge is a separate process and failure domain
- decisions: [`docs/adr/`](adr/) (91 ADRs, per-task status) · tour: this page · [THREAT-MODEL.md](THREAT-MODEL.md)

The crates are in [Workspace crates](#workspace-crates) below.

## What process owns what

`mqttd` is a thin binary over the `mqttd` library. The binary loads
`mqtt_config::Config` (defaults < TOML < `MQTTD_*` < CLI), installs the
process-default crypto provider, binds listeners, and wires:

- **connection tasks** (`conn.rs`) — one per client socket; they never share
  mutable session state. A client publish's rules are evaluated here, after the
  ACL and before the hub (ADR 0083), so rule work runs in parallel with ingress.
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
| `mqtt-core` | Sessions, subscription tables (plain + shared), topic matching, ACL relations, message types. |
| `mqtt-net` | Listeners (TCP/TLS/WebSocket/QUIC), connection lifecycle; the one audited TLS module. |
| `mqtt-auth` | `Authenticator` / `Authorizer` traits and built-in providers: mTLS-CN, Argon2id, JWT, OIDC, ACL. |
| `mqtt-storage` | `SessionStore` / `RetainedStore`, the replicated log, redb. |
| `mqtt-cluster` | SWIM, gossip auth, HRW placement, peer frames (the peer wire), durable plane. |
| `mqtt-observability` | Prometheus/OTLP metrics, hash-chained audit. |
| `mqtt-config` | Typed config, secure defaults; `ENV_VARS` is the overlay inventory. |
| `mqtt-rules` | The rule engine: EMQX rule SQL parser, evaluator, functions, templates, actions (republish/console); `edit` changes one rule of a rules file's text in place (ADR 0084). Pure (no I/O, no broker state). Fuzzed (ADR 0083). |
| `mqtt-wasm-sandbox` | ADR 0086: rule functions as WebAssembly modules in a sandbox (wasmtime); runs the rule engine's `jq`, the real jq, where `rules.jq` switches it on. |
| `mqttd` | Binary + hub + conn + peer mesh — this page. |
| `mqtt-bridge` | Zone-crossing MQTT client (not a cluster member): spool, QoS 1 replay. |
| `mqttd-operator` | `MqttdCluster` reconciler: the Kubernetes operator. |
| `history-check` | Independent checker of recorded client-visible histories. |
| `mqtt-load` | The benchmark rig's load generator (ADR 0090): a client of the broker, never linked into it. |

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
| `rules` | Rule-engine wiring (ADR 0083): the rules `watch`, evaluation on the connection task, and the derived messages' ack gates joined into the publisher's one ack; the trace capture and the per-rule last-error store (ADR 0084). |
| `rules_sys` | The `$SYS` publishers (ADR 0084): the per-rule statistics task and the rule trace task, each paying node-pool ingress credit for what it publishes. |
| `admin` | The admin API (ADR 0081): mTLS listener, roles, routes, the `mqttd --admin` client; `admin/rules` serves the rules endpoints and their atomic file write (ADR 0084). |
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
| `--check-config` | Validate and exit; bind nothing. Includes startup's pre-bind checks that need nothing from the host (bind syntax, TLS/QUIC/OIDC/cluster prerequisites). |
| `--check-config --preflight` | Also the host (issue #671): every bind resolved, every referenced file (TLS, password, ACL, JWT, cluster TLS, gossip key, rules) opened and parsed as the invoking user. |
| `--check-rules [file]` / `--rule-test --sql …` | Load a rules file and exit; run one statement over a sample message (ADR 0083), or with `--sequence <file>` replay a timed sequence of messages through it, or through the rules file, on a store and a clock of the replay's own (ADR 0085). |

## Where to put a change

1. If it is a *decision*, it needs an ADR (or an amendment to one). Progress
   goes in the delivery doc; run `python3 scripts/gen-status.py`.
2. If it is hub behaviour, land it in the seam whose invariant it serves —
   not in `mod.rs` "because that is where the other match arms are".
3. If it is a config knob, add the field, the `ENV_VARS` entry, the
   `overlay_from` mapping, and regenerate [CONFIGURATION.md](CONFIGURATION.md).
4. If it is user-facing, name the stakeholder document that carries it
   (ADR 0070). The index is [README.md](README.md) in this directory.
