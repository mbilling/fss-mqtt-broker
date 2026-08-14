# mqttd — a security-first, cluster-native MQTT broker

> An MQTT 3.1.1 + 5.0 broker built to be the most cyber-secure
> broker available, designed to scale horizontally, with a 100% open feature
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
replacement without losing an acknowledged fact). That covers a QoS 1/2 message
**already in flight to a connected subscriber**, not just one queued for a
disconnected one — the durable record is written before the packet reaches the
wire, so a crash in that window redelivers rather than loses it
([#124](https://github.com/mbilling/fss-mqtt-broker/issues/124)). At QoS 2 the
packet id and handshake phase are persisted too, so the redelivery resumes
**under the id the subscriber already knows** — exactly-once holds across a
broker crash, not only a client reconnect
([#130](https://github.com/mbilling/fss-mqtt-broker/issues/130)). Prometheus metrics, resource
governance (connection caps, per-client quotas, publish-rate limits, bounded
queues), and a continuous-assurance program (out-of-process fault/upgrade
harness, hour-long soak, fuzzing of every attacker-reachable parser, recorded
performance baselines, and two independent foreign-client conformance oracles)
all ship. **v0.9.0 is released** — signed, reproducible, SBOM-attested — and the
known gaps are listed
in [**Limitations**](#limitations) rather than left to be discovered — the
largest are that the total-memory watermark is backpressure rather than a hard
ceiling (the container limit remains the real bound), the Kubernetes operator
is not packaged for installation, and the horizontal scaling curve has not been
measured.

See [`docs/CAPABILITY-PLAN.md`](docs/CAPABILITY-PLAN.md) for the product vision,
[`docs/adr/`](docs/adr/) for the decisions behind it, and the
[**delivery dashboard**](docs/delivery/STATUS.md) — the authoritative, live
record of exactly what is built (60 ADRs, per-task status).

**Jump to:** [**Start here**](#start-here) ·
[Try it in two minutes](#try-it-in-two-minutes) ·
[New to MQTT?](#new-to-mqtt) · [Glossary](docs/GLOSSARY.md) · [Troubleshooting](docs/TROUBLESHOOTING.md) · [What works today](#what-works-today) ·
[Security](#security) · [Clustering](#clustering) ·
[Bridging](#bridging-to-other-security-zones) · [How it compares](#how-it-compares) ·
[**Limitations**](#limitations) · [Install](#install) ·
[Secured quickstart](#single-node-secured-tls-13--mtls--acl) ·
[Configuration](#configuration) · [Kubernetes](#on-kubernetes-helm) ·
[Performance](#performance) · [Contributing](#contributing)

## Start here

**`mqttui`** is the front door to everything runnable in this repository — the demo
cluster, the Mosquitto migration converter, the secured quickstarts, the Kubernetes
examples. It tells you what each task needs *before* it starts, instead of failing five
minutes in ([ADR 0056](docs/adr/0056-mqttui.md)):

```sh
git clone https://github.com/mbilling/fss-mqtt-broker && cd fss-mqtt-broker
cargo install --locked --path tools/mqttui
mqttui            # the terminal UI — `mqttui --list` is the same thing, headless
```

> **Prerequisites** (stated here because nothing is worse than a front door that
> assumes them): a Rust toolchain for the `cargo install` above
> ([rustup.rs](https://rustup.rs)); **Docker** for the demos and reference
> deployments; and the Mosquitto client tools for every pub/sub snippet in this
> file (`brew install mosquitto` / `apt install mosquitto-clients`). Signed
> prebuilt `mqttui` binaries ship with releases after v0.9.0.

**Migrating from a production broker, or evaluating one?**

```sh
mqttui migrate mosquitto /etc/mosquitto/mosquitto.conf
```

converts your config *and* your ACL file. Anything without an exact equivalent becomes a
`# TODO(migrate):` comment at the point it belongs — never a silent drop, because a
setting that quietly vanishes is how a migration ships the wrong policy. Then see it hold
up: `mqttui --run deploy-smoke` boots the three-node reference deployment (password auth,
deny-by-default ACL) and proves an **acknowledged QoS 1 message survives `SIGKILL`** of
the node that accepted it, in about a minute. `mqttui --run quickstart` is the two-node
version, including the TLS 1.3 + mTLS + ACL variant. What this broker does and does not
do versus Mosquitto, EMQX, VerneMQ and NanoMQ — including every cell it loses — is
[`docs/COMPARISON.md`](docs/COMPARISON.md); capacity planning is
[`docs/SIZING.md`](docs/SIZING.md).

The converter also works with **no clone at all** — the examples travel inside the
binary, and updates arrive as a cosign-signed bundle (`mqttui update`), never a
trust-the-branch download:

```sh
cargo install --locked --git https://github.com/mbilling/fss-mqtt-broker mqttui
```

**New to MQTT brokers?** Start with the
[two-minute single node](#try-it-in-two-minutes), then the
[five ideas the rest of this file assumes](#new-to-mqtt). When you want to see a real
cluster, `mqttui --run demo-stack` starts seven nodes with Prometheus and Grafana
dashboards on `localhost:3000` and a load generator so the panels move — it starts 25
containers, and `mqttui` warns you before it does.

Tasks that need this repository — building it, or the fixtures that will not fit in a
binary — are marked `-` in the list with the reason, rather than left to fail.

## Principles

- **Security is the product.** Secure by default; every insecure mode must be
  opted into and is loudly logged.
- **Open == Enterprise.** One Apache-2.0 codebase, no gated features. Only
  support, SLAs, and certified builds are paid.
- **Horizontal scalability by design.** Shared-nothing nodes; no coordinator on
  the publish hot path. The *shape* of the scaling curve is not yet measured —
  doing so honestly needs multi-host hardware
  ([ADR 0048](docs/adr/0048-comparative-benchmarking.md) T3), so this is a
  statement about the architecture, not a benchmarked result.
- **Memory safety.** Rust, `#![forbid(unsafe_code)]` across crates.

## What's different about it

Four things this does that the brokers it is usually compared against do not.
The full matrix — including every cell we lose — is
[`docs/COMPARISON.md`](docs/COMPARISON.md).

- **Durable sessions are on by default**, quorum-replicated, with an acked QoS
  1/2 message surviving the loss of the node that accepted it — whether it was
  queued for a disconnected subscriber *or already in flight to a connected
  one*. And when a group is too thin to keep that promise, the durable write is
  **refused** rather than acked on one copy: a group holding fewer copies than a
  majority of the members the node knows about — capped at the replication
  factor — takes no new durable writes by default, and QoS≥1 publishers get no
  ack and redeliver. One scope caveat, stated where the claim is: the floor
  covers writes that reach a group this node leases; a publish for a durable
  session whose owner is *gone* is still acked and dropped by the
  no-known-subscriber path (see [Limitations](#limitations)). Mosquitto and
  NanoMQ are single-node; VerneMQ documents queue loss on node death; EMQX's
  durable sessions are opt-in.
- **A policy reload evicts live sessions.** Revoke a certificate, remove a user,
  or tighten a grant, and the *already-connected* client is cut — not left
  running until it happens to reconnect. No compared broker documents this.
- **Clustering is not a paid feature.** Apache-2.0 including signed,
  reproducible binaries. EMQX has been BSL 1.1 since 5.9 with clustering
  commercial; VerneMQ's production binaries are EULA-paid.
- **The claims are checkable.** Every capability maps to a task with evidence in
  the [delivery dashboard](docs/delivery/STATUS.md), the numbers in this file are
  CI-guarded against the tree, and what is *missing* is listed in
  [Limitations](#limitations) rather than left to be discovered.

## How it fits together

```text
        MQTT clients  (TCP · TLS 1.3 · WebSocket · QUIC)
              │  identity: mTLS-CN / password / JWT / OIDC
              ▼        ↓ deny-by-default topic ACL
       ┌──────────────────────────────────────────┐
       │  node                                    │
       │   listeners → per-connection tasks       │
       │                    │                     │
       │                    ▼                     │
       │            hub (routing actor)           │   one hub per node owns
       │        subscriptions · retained · queues │   routing; no lock on the
       │                    │                     │   publish hot path
       └────────────────────┼─────────────────────┘
                            │
   ┌────────────────────────┼────────────────────────┐
   │ SWIM gossip            │ peer links (mTLS)      │  one trust domain =
   │ membership, interest   │ interest-based forward │  one logical broker
   └────────────────────────┼────────────────────────┘
                            ▼
              durable plane — openraft lease group
              epoch-fenced quorum replication of
              sessions, queues and retained state
```

Crossing into a **different** trust domain is a separate tool with its own
process and credentials — see [Bridging](#bridging-to-other-security-zones).

## Try it in two minutes

```sh
docker run -d --name mqttd -p 1883:1883 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd -v mqttd-data:/var/lib/mqttd \
  ghcr.io/mbilling/fss-mqtt-broker:latest

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

That is **plaintext with anonymous clients** — a first look, never a deployment. The
named volume is what makes it honest: durable sessions are on by default, and durable-on
with no data dir **refuses to start** (issue #240) rather than silently keeping acked
messages in RAM. (The volume outlives `docker rm -f mqttd`; `docker volume rm mqttd-data`
removes the state too.)
The broker says so in its own logs, loudly, every time. When you are ready for
something real, the [secured quickstart](#single-node-secured-tls-13--mtls--acl)
stands up TLS 1.3, mutual TLS and a deny-by-default ACL in about the same number
of commands, and CI runs those exact commands on every push.

## New to MQTT?

Skip this if you already run a broker. If you do not, these five ideas are what
the rest of this file assumes, and nothing else here explains them.

- **QoS 0 / 1 / 2** — how hard the broker tries to deliver. **0** is fire and
  forget (fastest, may be lost). **1** is at-least-once (may arrive twice; the
  usual choice). **2** is exactly-once (slowest, a four-packet handshake). You
  pick per message, and the subscriber's subscription can only lower it.
- **Retained message** — the broker keeps the *last* message on a topic and hands
  it to anyone who subscribes later. "What is the current temperature?" without
  waiting for the next reading. One per topic; publishing an empty payload clears
  it.
- **Session** — what the broker remembers about a client between connections: its
  subscriptions and any messages queued while it was away. A *clean* session
  forgets everything on disconnect; a *persistent* one does not, which is what
  makes offline devices work.
- **Last Will and Testament (LWT)** — a message the client registers at connect
  time that the broker publishes **if the client dies without saying goodbye**.
  How you detect a device dropping off, without polling.
- **Shared subscription** (`$share/<group>/<topic>`) — several subscribers join a
  named group and the broker gives each message to **exactly one** of them, so
  work is split rather than duplicated. Ordinary subscriptions give *every*
  subscriber a copy.

Two things about this broker specifically that surprise people, both explained
where they matter: a **two-node cluster is worse than one node** for write
availability ([Resizing](#resizing-the-cluster)), and there is **no admin API or
dashboard** — operations are signals and files, on purpose
([Configuration](#configuration)).

## What works today

### Protocol (MQTT 3.1.1)
- CONNECT/CONNACK with full flag and client-id validation.
- **QoS 0/1/2 end-to-end**: per-session in-flight tracking, `DUP` redelivery on
  session resume, the QoS-2 four-way handshake, inbound exactly-once
  deduplication — and the **outbound QoS 2 packet id and phase persisted with
  the session**, so the handshake resumes under the same id across a broker
  crash ([ADR 0057](docs/adr/0057-durable-outbound-inflight.md)).
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
  [0015](docs/adr/0015-cluster-shared-subscriptions.md)) — the lever for spreading
  one topic's load across nodes.
- **User Properties** forwarded end to end through delivery
  ([ADR 0030](docs/adr/0030-user-property-forwarding.md)).
- **Enhanced authentication** — the v5 `AUTH` exchange, e.g. challenge/response
  ([ADR 0013](docs/adr/0013-enhanced-authentication.md)).
- **Subscription identifiers** — declined outright rather than half-implemented, and the
  CONNACK says so rather than implying otherwise (see Limitations for the other v5 items
  that are deferred, e.g. server-initiated re-authentication): MQTT 5.0 §3.2.2.3.12 makes an
  absent `0x29` mean "supported", so mqttd advertises `Subscription Identifiers
  Available = 0`, refuses a SUBSCRIBE that carries one with DISCONNECT `0xA1`, and
  refuses a client PUBLISH that carries one with `0x82` (`[MQTT-3.3.4-6]`). See
  [Limitations](#limitations).
- Reason codes and DISCONNECT with reason on protocol/quota violations.

Both protocol versions round-trip against two independent foreign clients
(Mosquitto CLI + Eclipse Paho) in CI — see [Build & test](#build--test).

### Security
- **TLS 1.3** client listener (`rustls` on `aws-lc-rs` — one crypto provider for
  the whole build, [ADR 0053](docs/adr/0053-single-crypto-provider-aws-lc-rs.md)), optional
  per-listener client-certificate mTLS, **fleet-sized session resumption**
  (32k-entry cache by default, 24 h ceiling, `session_cache` to size or disable),
  and a **hardened TLS 1.2 opt-in** for legacy fleets (strict ECDHE+AEAD
  allowlist, Extended Master Secret required; see
  [Limitations](#limitations)) — [ADR 0002](docs/adr/0002-transport-security.md).
  Server and client certificates: **ECDSA P-256** (what the test suite runs end
  to end, including mTLS and CRL revocation) and RSA ≥ 2048. Also native
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
  deny-by-default CONNECT gate; pluggable Argon2id password, **remote HTTP
  auth hook** (one webhook reaches LDAP / OAuth2 / a bespoke user table;
  fail-closed — a hook error denies) and JWT (HS256/RS256) authenticators
  composed in a chain (cert → password → token → hook).
- **Authorization**: deny-by-default TOML topic ACLs with `%i` (identity) and `%c`
  (client id) substitution and asymmetric allow-covers / deny-overlaps semantics so a
  narrow grant can't widen and a broad subscription can't tunnel past a deny. Both
  substitutions fail closed on a value carrying `/`, `+` or `#`, so neither an identity
  nor a client-chosen session handle can smuggle topic structure into a pattern.
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

### Bridging to other security zones

The cluster mesh makes N nodes behave as **one logical broker inside one trust
domain**. Reaching a broker in a *different* zone — a partner's, a cloud IoT
platform, an edge site forwarding upward, or any third-party broker — is the
opposite problem, and gets its own tool: `mqtt-bridge`, a **standalone binary**
([ADR 0025](docs/adr/0025-boundary-bridge.md)).

It is an ordinary MQTT client to both sides rather than an in-process plugin, so
the boundary crossing is a small, isolated, auditable unit with its own identity,
credentials, and failure domain — a compromise of the far side does not land
inside the broker.

- **Deny by default, direction enforced.** Nothing is forwarded until a rule says
  so, and each rule is `out`, `in`, or `both` — one-way flow is a *mechanism*,
  not a configuration habit, so a data-diode-style crossing is expressible.
- **Loop prevention** on two levels: an `fss-bridge-hop-count` limit (default 8),
  and topic remapping that structurally stops a forwarded message from matching
  the rule that would send it straight back.
- **Store-and-forward** over a bounded spool: a momentarily unreachable side is
  buffered and replayed on reconnect (bounded by message count — see
  [Limitations](#limitations)).
- **HA without duplicates.** Two or more bridge instances sharing a
  `share_group` take the local stream through a **shared subscription**, so
  adding an instance adds redundancy rather than duplicate deliveries.
- Per-side TLS/mTLS and least-privilege credentials.

**Observability.** The bridge serves Prometheus text at `GET /metrics` on
`metrics_bind`. The set answers the questions a boundary actually raises:

| Metric | Type | Labels | Answers |
|---|---|---|---|
| `fss_bridge_connected` | gauge | `side` | **Is this side up right now?** (1/0) |
| `fss_bridge_spool_depth` | gauge | `side` | **How much is buffered** for a side that is down or behind |
| `fss_bridge_spool_capacity` | gauge | `side` | the bound, so depth reads as a fraction of it |
| `fss_bridge_forwarded_total` | counter | `upstream`, `direction` | messages across each boundary, each way |
| `fss_bridge_forwarded_bytes_total` | counter | `upstream`, `direction` | the same in bytes — a size change hides in message rate |
| `fss_bridge_dropped_total` | counter | `reason`, `side` | `hop-limit` (loop protection working) and **`spool-full` (real message loss)** |
| `fss_bridge_reconnects_total` | counter | `side` | flapping, per side |

There are deliberately **no 1/5/15-minute metrics**. Windowed rates belong in the
query layer — `rate(fss_bridge_forwarded_total[5m])` — because Prometheus computes
them correctly across counter resets and multiple bridge replicas, which an
in-process window cannot. A ready-made Grafana dashboard with those windows,
connection state, spool depth and loss panels is at
[`demo/grafana/dashboards/mqttd-bridge.json`](demo/grafana/dashboards/mqttd-bridge.json).

**Running it.** The bridge ships as its own signed binary and its own hardened
image — a separate process from the broker, as its own security rationale
requires:

```sh
docker run -d --name mqtt-bridge \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v ./bridge.toml:/etc/bridge.toml:ro -v bridge-spool:/var/lib/mqtt-bridge \
  -p 8090:8090 \
  ghcr.io/mbilling/fss-mqtt-broker-bridge:latest /etc/bridge.toml
```

`/var/lib/mqtt-bridge` is the image's spool directory — point `spool.dir` at it to
make buffering survive a restart.

On Kubernetes the chart deploys it beside the broker, opt-in:

```sh
helm upgrade --install mqttd deploy/helm/mqttd --set bridge.enabled=true
```

It renders a StatefulSet, not a Deployment, for two reasons: the spool is
per-replica state, and every replica needs its **own MQTT client id** — replicas
sharing one take over each other's session instead of forming the HA pair a
`share_group` promises. An unset `client_id` is generated per instance in MQTT's
guaranteed-support shape (≤23 bytes, alphanumeric — accepted by any broker), so it
is already per-pod; the chart also lets you write `__POD_NAME__` into one
explicitly. See [docs/BRIDGE.md](docs/BRIDGE.md).

Standalone and HA topologies, with schematics and what HA does *not* cover:
[**docs/BRIDGE.md**](docs/BRIDGE.md).

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
  and a **disk watermark** that sheds load before the store fills. Sizing a node
  with a fixed RAM/disk budget — which limits to set and the arithmetic — is
  [docs/SIZING.md](docs/SIZING.md), with a ready preset in
  [docs/examples/bounded-node.toml](docs/examples/bounded-node.toml).
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
- MQTT 5 **Server-Reference redirect** for v5 clients that opt into following it
  (the session relay remains the universal path meanwhile — ADR 0005 P3).
- **Production users.** `v0.9.0` is cut and every artifact verifies, but nobody is
  running this in anger yet — see [Limitations](#limitations).

## How it compares

The full, versioned, honesty-ruled matrix against **Mosquitto**, **EMQX**, **NanoMQ**,
and **VerneMQ** — including every cell we lose — is
[`docs/COMPARISON.md`](docs/COMPARISON.md) (dated 2026-08-11). The one-paragraph
version:

|  | mqttd's answer |
|---|---|
| Durable sessions | Quorum-replicated **by default**; acked QoS 1/2 survives node loss (proven under SIGKILL/partition harnesses), and covers a message **in flight to a connected subscriber** as well as one queued for a disconnected one — the durable append happens before the wire send ([#124](https://github.com/mbilling/fss-mqtt-broker/issues/124), reproduced against the real binary under SIGKILL). A group too thin to keep the promise **refuses** new durable writes by default (the min-replicas floor, `MQTTD_MIN_REPLICAS=majority`: a majority of the members the node knows about, capped at R) rather than acking on one copy; a node that has never known peers still serves fully. Above the (off-by-default) store or memory watermark the broker likewise **refuses the publisher** rather than acking a message it will not store — v5 gets `0x97 Quota exceeded`, v3.1.1 gets no ack and a close — including when the refusing session owner is a *peer* node: the refusal crosses the peer bus as a verdict (during a rolling upgrade, a link to an older build degrades to a withheld ack and a close). Nothing acked is lost; whether the message is re-sent is the *application's* decision — a v5 reason ≥ `0x80` completes the packet-id lifecycle (no client library retransmits it) and a clean-session v3.1.1 publisher resends nothing (ADR 0041 §5/T11/T12, counted as `quota_rejections_total{reason="brownout-publish"}`). The arms that still ack-and-drop, stated where the claim is: the **default** `drop-oldest` offline-queue overflow, which truncates the oldest *already-acked* entries out of a session's durable queue at the cap (counted `publish_dropped{reason="queue-overflow"}`); its opt-in `reject-newest` sibling, which acks and sheds the newest; for retained *values* only, a v3.1.1 retained publish over the retained quota or under brownout (delivered live, not retained); and a publish for a durable session whose owner is gone, acked-and-dropped by the no-known-subscriber path. Mosquitto/NanoMQ are single-node; VerneMQ documents queue loss on node death; EMQX's durable sessions are opt-in. |
| Revocation | A policy reload **evicts live sessions and flows** (CRL'd cert, removed user, tightened grant — ADR 0040). Not documented by any compared broker. |
| Licensing | Apache-2.0 including signed, reproducible binaries. EMQX is BSL 1.1 (clustering commercial) since 5.9; VerneMQ's production binaries are EULA-paid. |
| Where we lose | No dashboard, rule engine, HTTP admin API (by design — signal-driven ops), no MQTT-SN/CoAP, no subscription-identifier delivery — and the CONNACK says so (`0x29 = 0`), so clients fail fast rather than silently — and **no production track record**: the matrix says so in as many words. |

## Before production — a checklist

The four things most likely to bite a first deployment, each of which is silent if you
don't know to look. New to MQTT? Start with the [glossary](docs/GLOSSARY.md); hitting an
error? the [troubleshooting guide](docs/TROUBLESHOOTING.md).

- [ ] **Set `MQTTD_DATA_DIR` and mount a volume.** Durable sessions are on by default,
  and durable-on with no data dir **refuses to start** (issue #240) — in-memory
  replicated state loses acknowledged messages on a correlated restart. The refusal
  names the ways out; `MQTTD_ALLOW_EPHEMERAL_DURABILITY=1` is the development/test
  override, loudly warned while active — never production.
- [ ] **Configure an ACL (`MQTTD_ACL_FILE`).** With none, every authenticated client may
  publish and subscribe anywhere (logged as INSECURE at startup). And note: **a denied
  publish is still acknowledged** — the message is dropped after the ACK, so a
  misconfigured ACL looks like "missing data", not an error. The audit log is where denials
  are visible.
- [ ] **Check your fleet's TLS and certificates.** TLS 1.3 is the default (1.2 is a
  hardened opt-in), and client certificates **must** carry the `clientAuth` EKU or rustls
  rejects them — a trap for fleets minted against OpenSSL brokers.
- [ ] **Run ≥3 nodes for HA, never 2.** A two-node durable cluster has *worse* write
  availability than one node (write quorum is 2-of-2). Go from 1 to 3.

## Limitations

The gaps worth knowing before you evaluate this, stated here rather than left to
be found. Each is tracked; none is a silent surprise.

- **Subscription identifiers are not delivered — and this is now a hard refusal, not a
  silent degradation.** No identifier is ever attached to an outbound PUBLISH. MQTT 5.0
  §3.2.2.3.12 says that an *absent* CONNACK property `0x29` means identifiers **are**
  supported, so staying quiet was an affirmative false claim: a client library that keys
  its message callbacks on the identifier would lose its demux with no way to detect it.
  The CONNACK therefore advertises `Subscription Identifiers Available = 0`; a SUBSCRIBE
  carrying one is answered with DISCONNECT `0xA1` and closed (§3.2.2.3.12 prescribes
  exactly that, and `[MQTT-4.13.1-1]` makes the close mandatory); a client PUBLISH
  carrying one is answered with DISCONNECT `0x82` (`[MQTT-3.3.4-6]`). **This is a break
  for a client that ignores `0x29` and sends an identifier anyway** — such a client is now
  disconnected where it previously got a working-but-silently-degraded subscription.
  Whether a given client library reads the byte is its own business and we do not assert it
  here; the conformance lane exercises the refusal against Eclipse Paho, and
  `mosquitto_sub -V 5 -D subscribe subscription-identifier N` can drive it too. That trade is the spec's, and it is the
  point: failing fast beats losing messages quietly. Delivery is tracked
  (ADR [0030](docs/adr/0030-user-property-forwarding.md) §1 "As delivered",
  [0010](docs/adr/0010-shared-subscriptions.md)-T7).

- **Memory has a watermark, not a ceiling.** `MQTTD_MEMORY_MAX_BYTES` puts the
  broker into brownout above it — growth writes refused; subscriber acks, reads,
  deletes, expiry and resumes continue, while a publisher's `QoS` ≥ 1 ack is
  refused, not granted — but nothing can stop RSS rising, so a burst that outruns the 10-second
  poll can still OOM, and the container limit remains the hard bound. It needs
  `/proc` (Linux); elsewhere the broker logs that it is **not** enforcing rather than
  pretending. Underneath, the per-subscriber queues are still bounded by message
  count and not by bytes: QoS 1/2 by `MAX_BACKLOG` (10 000, drop-oldest) and QoS 0 by
  the outbound-queue cap (10 000 packets, shed and counted as
  `publish_dropped{reason="outbound-full"}`), both hard-coded. At the 1 MiB default
  packet size that is ~10 GiB of worst-case headroom per connection, so cap
  `MQTTD_MAX_PACKET_SIZE` to bound it in practice. Full arithmetic and a bounded
  preset: [SIZING.md](docs/SIZING.md) (ADR 0041 T6, T10).
- **Disk is bounded in aggregate, not per store.** One store can consume the whole
  `MQTTD_STORE_MAX_BYTES` watermark and brown out the others (ADR 0041 T9).
  Disk-full itself fails closed and is crash-tested mid-write.
- **The Kubernetes operator is not installable.** It is built and end-to-end
  tested, but no image is published and its manifest is pinned to a kind-local
  tag. The **Helm chart is the supported path** (ADR 0055 T8).
- **The horizontal scaling curve is unmeasured.** The architecture is
  shared-nothing with no coordinator on the publish hot path, but measuring what
  that yields needs multi-host hardware (ADR 0048 T3). Treat scaling claims here
  as design intent.
- **Durability costs a write on the delivery path.** A QoS 1/2 message for a
  **persistent** subscriber is appended to that session's durable log before it
  goes on the wire — that is what makes the guarantee above hold. Clean sessions
  skip it entirely (they have nothing to resume into), as does QoS 0. If your
  subscribers do not need redelivery across a broker restart, connect them with
  `clean_session` / a zero Session Expiry and the write never happens. See
  [`docs/SIZING.md`](docs/SIZING.md).
- **The write floor is derived, so it has three honest edges.** By default a group
  holding fewer copies than a majority of the members this node knows about —
  **capped at the replication factor**, so it is 2 in a 3-node cluster and still 2
  on 5 or 7 nodes — refuses new durable writes (`MQTTD_MIN_REPLICAS=majority`). The
  witness is the quorum-committed durable roster **when it has one** — the largest
  membership ever observed is only a pre-roster fallback, and `MQTTD_READY_MIN_MEMBERS`
  bounds the result from below. So (a) a bare-metal node that boots alone with
  `MQTTD_READY_MIN_MEMBERS=1` but really belongs to a mesh has a seconds-long window
  before the floor arms; (b) a shrink to a **single** member refuses durable writes
  until you consent explicitly. What consent means depends on why you are down to one:
  a *consented* decommission shrinks the committed roster, but the floor is still
  bounded below by `MQTTD_READY_MIN_MEMBERS`, which the operator and the chart render
  as a majority for every cluster of three or more — so that node keeps a floor of 2
  until you lower the readiness floor too. After an **unconsented** loss (two of three
  nodes gone for good, an AZ loss, a DR restore of one node's data dir) the roster
  stays at three, and then only `durable.min_replicas = 1` clears it — a restart-scoped
  `[durable]` edit, not a reload; lowering the readiness floor alone does nothing
  ([TROUBLESHOOTING](docs/TROUBLESHOOTING.md), [OPERATIONS](docs/OPERATIONS.md)). And (c) the floor covers only writes that
  reach a group this node leases: a publish for a durable session owned by a node that
  is gone is still acked and dropped by the pre-existing no-known-subscriber path, with
  no refusal logged. All three are stated in
  [ADR 0006](docs/adr/0006-consensus-and-replication.md) §4 rather than papered over.
- **Migration tooling covers Mosquitto only.**
  `scripts/migrate/from-mosquitto.py` translates `mosquitto.conf` and its
  `acl_file`, marking anything without an equivalent as `TODO(migrate)` in the
  output rather than dropping it silently. EMQX and HiveMQ converters do not
  exist yet.
- **TLS 1.3 by default.** Older device firmware that cannot negotiate 1.3 will
  fail to connect out of the box — and the failure looks like a network problem
  rather than a policy one, so check your fleet before planning a migration.
  For exactly that case, **TLS 1.2 is available as an explicit opt-in**
  (`MQTTD_TLS_ALLOW_TLS12` / `[tls].allow_tls12`) on the client-facing TLS
  listener only: off by default, loudly logged on every start while enabled,
  never spoken by the cluster bus or QUIC — and **hardened** (ECDHE+AEAD suites
  only, Extended Master Secret required), so opting into 1.2 does not opt into
  1.2's exploit classes.
- **Some auth and revocation mechanisms are deferred, by choice.** The MQTT 5
  enhanced-authentication (AUTH) framework is in place and an HMAC challenge
  example ships, but **SCRAM is not yet implemented**. Certificate revocation is
  by **CRL** — hot-reloadable, enforced on both the client listener and the
  cluster bus — with **OCSP not yet supported**. **PSK cipher suites** for
  constrained devices are not offered: X.509 or token (JWT/OIDC) authentication
  is the path today. Each is a planned fast-follow, not a design limit.
- **No production track record.** `v0.9.0` is released and verifiable, but nobody
  is running this in anger yet — there is no operational history behind it.

## Supported Rust, platforms, and stability

- **Minimum supported Rust: 1.88** for the broker and its libraries; **1.89** for
  the `mqttd-operator` crate alone. Both are verified nightly against those exact
  toolchains rather than asserted — see the `msrv` job in
  [`.github/workflows/nightly.yml`](.github/workflows/nightly.yml).
- **Builds** are produced with a pinned **1.97.0** toolchain
  ([`rust-toolchain.toml`](rust-toolchain.toml)). That is a *reproducibility*
  anchor, not a requirement on you: it is what makes "rebuild the tag and get
  identical bytes" checkable (ADR 0045 T2).
- **Released binaries:** `linux/amd64` and `linux/arm64`, statically linked
  against musl, signed with SBOM and SLSA provenance. Other platforms build from
  source; they are not released artifacts and are not tested in CI.
- **Stability:** this is **pre-1.0**. The compatibility policy of
  [ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md) — semver, adjacent
  version skew, sequential majors — **applies from 1.0.0**. Until then, wire and
  on-disk schema reshapes are permitted between releases, deliberately, so the
  cheap moment to fix a format is not missed. MQTT itself is unaffected: clients
  speak the published 3.1.1 / 5.0 specifications, which this policy does not
  touch.

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
| `mqtt-bridge` | Outbound bridging to an upstream broker: durable spool, QoS-1 replay |
| `mqttd` | The server binary: hub routing actor, connections, peer mesh |
| `mqttd-operator` | Kubernetes operator for the `MqttdCluster` CRD (**not yet packaged for install** — the Helm chart is the supported path) |
| `history-check` | Independent checker for recorded client-visible histories (issue #231): re-derives the durability promises from what clients actually saw, with no imports from the broker crates |

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
plus mTLS — all against an independent implementation. The Paho half additionally asserts the
control-plane facts a CLI cannot reach: v5 reason codes, per-filter granted QoS, session-present
on resume, and the **capability advertisement** — that the CONNACK says `Subscription
Identifiers Available = 0` and that a real client which uses one is refused with `0xA1` rather
than silently degraded.

## Install

Releases are cut from signed semver tags by an automated, security-grade pipeline
([ADR 0045](docs/adr/0045-release-engineering-and-distribution.md)): every artifact
is **reproducible**, **cosign-signed** (keyless, transparency-logged), carries **SLSA
build provenance**, and ships with a **CycloneDX SBOM**. Full cut/verify runbook:
[RELEASING.md](RELEASING.md).

```sh
# Container image — fully-static musl binary on distroless/static, non-root,
# multi-arch (linux/amd64 + linux/arm64), nothing but the broker and a CA bundle:
docker run --rm -e MQTTD_DATA_DIR=/var/lib/mqttd \
  ghcr.io/mbilling/fss-mqtt-broker:latest --check-config
# → config OK: env overlay validates. (BARE defaults now REFUSE — durable-on needs a
#   data dir or the explicit MQTTD_ALLOW_EPHEMERAL_DURABILITY opt-in, issue #240.)
#
# `mqttd --version` prints the version and exits; `mqttd --help` lists every flag.
# An unrecognised flag is now an ERROR (exit 2), not a silent broker start.

# Verify the image signature before trusting it:
cosign verify ghcr.io/mbilling/fss-mqtt-broker:0.9.0 \
  --certificate-identity-regexp 'https://github.com/mbilling/fss-mqtt-broker/.github/workflows/release.yml@.*' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com

# Run it, hardened, with durable state on a volume:
docker run -d --name mqttd \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v mqttd-data:/var/lib/mqttd -p 1883:1883 -p 8080:8080 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd -e MQTTD_HEALTH_BIND=0.0.0.0:8080 \
  ghcr.io/mbilling/fss-mqtt-broker:latest
# (plaintext + anonymous for a first look only — see the secured quickstart below)

# Or download a binary from the GitHub Release and verify + reproduce it — see RELEASING.md.
```

> **`/var/lib/mqttd` is the image's data directory** and the only path inside the
> image the broker's uid (65532) may write. Durable sessions are on by default and
> **require `MQTTD_DATA_DIR`** (issue #240), so persistence needs both the env var
> and a volume. The image runs non-root under a read-only root filesystem with
> every capability dropped.

> ⚠️ **Durable-on with no `MQTTD_DATA_DIR` refuses to start** (issue #240). In-memory
> durability survives one node's loss (peers still hold the state) but **a correlated
> restart of a quorum loses acknowledged messages** — so it is no longer a warning, it
> is a startup error naming both ways out: set `MQTTD_DATA_DIR` and mount a volume for
> real durability, or set `MQTTD_ALLOW_EPHEMERAL_DURABILITY=1` to accept the in-memory
> mode for development and tests (loudly `EPHEMERAL durability`-warned on every start
> while active). `MQTTD_DURABLE_SESSIONS=0` — the lightweight in-memory store — is an
> explicit choice already and needs no flag.

> **What exists today: `v0.9.0`** — both musl binaries with signatures and
> certificates, a signed CycloneDX SBOM, a multi-arch image, and SLSA provenance,
> plus the same set for `mqtt-bridge`. Every one has been verified end to end
> against the published artifacts, so this is a real thing you can pull and check,
> not a pipeline that merely exists. `:latest` tracks the newest non-prerelease;
> pin the version for reproducibility.

## Running

> The examples below use the **plaintext** listener for a quick local loop.
> Plaintext is insecure, opt-in, and loudly logged. For a real deployment use
> the TLS + auth environment variables in [Configuration](#configuration).

### Single node (insecure, local testing)

```sh
MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
  cargo run --bin mqttd
mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

### Single node, secured (TLS 1.3 + mTLS + ACL)

The path to run if you are evaluating this as a **secure** broker: no plaintext
listener, no anonymous clients, client certificates required, and a deny-by-default
topic policy. CI runs these exact commands (`scripts/quickstart-smoke.sh`).

```sh
# 1. A local CA, a server cert for 127.0.0.1, and a client cert whose CN is the
#    client's identity. The clientAuth EKU is REQUIRED — rustls rejects a client
#    certificate without it.
mkdir -p pki && (cd pki && \
  openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
    -keyout ca.key -out ca.crt -subj '/CN=mqttd-quickstart-ca' && \
  openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
    -subj '/CN=127.0.0.1' && \
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -out server.crt -days 365 \
    -extfile <(printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth') && \
  for cn in sensor-1 sensor-2; do \
    openssl req -newkey rsa:2048 -nodes -keyout "$cn.key" -out "$cn.csr" \
      -subj "/CN=$cn" && \
    openssl x509 -req -in "$cn.csr" -CA ca.crt -CAkey ca.key -CAcreateserial \
      -out "$cn.crt" -days 365 \
      -extfile <(printf 'extendedKeyUsage=clientAuth'); \
  done)

# 2. Deny by default. `%i` substitutes the authenticated identity — here the
#    certificate CN — so this ONE rule confines every device to its own subtree:
#    sensor-1 gets sensors/sensor-1/#, sensor-2 gets sensors/sensor-2/#, and
#    neither can reach the other's.
cat > acl.toml <<'EOF'
default = "deny"

[[rules]]
identities = ["sensor-1", "sensor-2"]
actions = ["publish", "subscribe"]
effect = "allow"
topics = ["sensors/%i/#"]
EOF

# 3. Run it. No MQTTD_PLAINTEXT_BIND, no MQTTD_ALLOW_ANONYMOUS — the broker logs
#    an INSECURE warning for either, and this configuration logs none.
#    (MQTTD_ALLOW_EPHEMERAL_DURABILITY is the local-evaluation escape hatch for
#    durable-on with no data dir, issue #240; production sets MQTTD_DATA_DIR instead.)
MQTTD_TLS_BIND=127.0.0.1:8883 \
MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
MQTTD_TLS_CERT=pki/server.crt MQTTD_TLS_KEY=pki/server.key \
MQTTD_TLS_CLIENT_CA=pki/ca.crt \
MQTTD_ACL_FILE=acl.toml \
cargo run --bin mqttd &

# 4. A foreign client over mutual TLS, inside its own subtree:
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile pki/ca.crt \
  --cert pki/sensor-1.crt --key pki/sensor-1.key -t 'sensors/sensor-1/#' &
mosquitto_pub -h 127.0.0.1 -p 8883 --cafile pki/ca.crt \
  --cert pki/sensor-1.crt --key pki/sensor-1.key \
  -t 'sensors/sensor-1/temp' -m '21.5C'

# 5. And the two refusals that make it a security boundary.
#    No client certificate at all — the TLS handshake itself fails:
mosquitto_pub -h 127.0.0.1 -p 8883 --cafile pki/ca.crt \
  -t 'sensors/sensor-1/temp' -m 'nope'
#    A valid, fully authenticated certificate reaching into ANOTHER device's
#    subtree — subscription denied, and nothing is ever delivered:
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile pki/ca.crt \
  --cert pki/sensor-2.crt --key pki/sensor-2.key -t 'sensors/sensor-1/#'
```

> **Checking the second refusal yourself:** don't read `mosquitto_sub`'s exit
> status — it exits **0** when every filter is denied and 27 on a clean timeout,
> which is the opposite of what it looks like. Judge it by delivery: publish as
> `sensor-1` while `sensor-2` is subscribed to `sensors/sensor-1/#`, and confirm
> nothing arrives. That is exactly what CI asserts, and it fails if the `%i` is
> dropped from the rule.

> **One behaviour to know before you rely on it:** a *denied publish* is dropped
> but still **acknowledged** — MQTT 3.1.1 has no negative PUBACK, and withholding
> the ack would leave a conforming publisher retrying forever. So a publisher
> cannot tell that it was refused. The denial is recorded in the audit log as
> `acl.deny.publish`; that, not the client's return code, is where you see it.
> Denied *subscriptions* are refused visibly, with a per-filter reason code.

### Two-node cluster via gossip discovery (insecure, local testing)

Nodes find each other through SWIM and establish the peer mesh automatically —
no static peer list. Node B seeds off node A's gossip address.

```sh
# Node A — client :1883, peer :7001, gossip :7946 (seed)
MQTTD_NODE_ID=node-a MQTTD_PLAINTEXT_BIND=127.0.0.1:1883 \
  MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
  MQTTD_PEER_BIND=127.0.0.1:7001 MQTTD_SWIM_BIND=127.0.0.1:7946 \
  cargo run --bin mqttd &
# Node B — client :1884, peer :7002, gossip :7947, seeds off A
MQTTD_NODE_ID=node-b MQTTD_PLAINTEXT_BIND=127.0.0.1:1884 \
  MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
  MQTTD_PEER_BIND=127.0.0.1:7002 MQTTD_SWIM_BIND=127.0.0.1:7947 \
  MQTTD_SWIM_SEEDS=127.0.0.1:7946 cargo run --bin mqttd &

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'fleet/+/telemetry' &           # on node A
mosquitto_pub -h 127.0.0.1 -p 1884 -t 'fleet/truck7/telemetry' -m hi  # on node B
```

> `MQTTD_ALLOW_EPHEMERAL_DURABILITY=1` in these local runs is the issue #240 opt-in:
> durable-on with no data dir would otherwise refuse to start. A production node sets
> `MQTTD_DATA_DIR` (and a volume) instead.

## Configuration

The broker is configured by a **TOML file**, environment variables, or both, layered in the
order **defaults < config file < `MQTTD_*` env vars < CLI flags** (ADR 0046). Point at the file
with `--config <path>` or `MQTTD_CONFIG`; with neither, the config is defaults + the env overlay,
so env-var-only deployments keep working exactly as before. Unset or empty means "off"; every
insecure fallback is logged at startup, and the effective config is logged at boot (secrets
redacted).

- **Example file:** [`docs/mqttd.example.toml`](docs/mqttd.example.toml) — a fully-commented
  template. Every setting below has a matching TOML key; the file's sections (`[node]`,
  `[listeners]`, `[tls]`, `[security]`, `[cluster]`, `[durable]`, `[limits]`, `[observability]`,
  `[runtime]`) mirror these env groups, and a CI test enforces the one-to-one mapping.
- **Strict schema:** unknown keys and wrong types fail the load with a **located** error — a typo
  is caught up front, never silently ignored.
- **Pre-flight check:** `mqttd --check-config [--config <path>]` validates the config the broker
  would boot with and exits **without binding any port** — the GitOps CI / pre-rollout gate.
  Exit `0` = OK, `1` = a clear located error.
- **Password hashing:** `mqttd --hash-password [<username>]` reads a password from **stdin** and
  prints the Argon2id line `MQTTD_PASSWORD_FILE` expects — with a username, the whole
  `username:hash` line; without one, the bare hash. The password goes on stdin, never in argv,
  so it stays out of shell history and `ps`:

  ```sh
  printf %s 'correct horse battery staple' | mqttd --hash-password alice >> /etc/mqttd/passwd
  ```

  `mosquitto_passwd` output is a different format and is **not** accepted — re-hash on migration.
  An end-to-end test hashes with this command and then logs in against a running broker, so the
  two can never drift apart (`crates/mqttd/tests/password_cli.rs`).
- **Hot reload:** edit the file and send `SIGHUP` (or set `[runtime] config_watch_secs` /
  `MQTTD_CONFIG_WATCH` to watch it) to reload the whole config through the validate-before-swap
  path — a bad edit is rejected and the running config kept. Live-swappable settings (ACL/auth,
  TLS material, `allow_anonymous`, the state quotas) change without a restart; everything else is
  logged + audited as **requires-restart**.
- **Secrets by reference:** the config file is safe to commit / mount from a ConfigMap — all
  secret material is referenced **by path** (TLS keys, `password_file`, the JWT keys via
  `…_FILE`, the gossip key via `MQTTD_SWIM_KEY_FILE`), mounted from a Secret. The only raw secret
  a value can hold is the inline `MQTTD_SWIM_KEY`; prefer `MQTTD_SWIM_KEY_FILE`.

The tables below are the authoritative reference for every `MQTTD_*` variable (and its TOML key).

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
| `MQTTD_STORE_MAX_BYTES` | Disk watermark over the node's on-disk stores, total bytes (ADR 0041; needs `MQTTD_DATA_DIR`). Above it the broker **browns out**: writes that *grow* durable state (new retained topics, new sessions, offline enqueues) are refused with the quota behaviors, while subscriber acks, reads, deletes, expiry and resumes continue — a publisher's `QoS` ≥ 1 ack is refused, not granted (v5 `0x97`, v3.1.1 no ack + close, cross-node as a peer-bus verdict — an answered refusal; re-sending is the application's decision) — read-mostly, never the disk-full cliff; dropping back under restores writes. Session metadata (SUBSCRIBEs, the `QoS` 2 dedup window, detach spills) is exempt and still grows slowly — set the mark with headroom (see SIZING). Per-store sizes are always exported as the `store_bytes{store}` gauge. Unset = no watermark |
| `MQTTD_MEMORY_MAX_BYTES` | **Memory watermark** over this process's RSS, bytes (ADR 0041 T8). Above it the broker **browns out** exactly as the disk watermark does — growth writes refused; subscriber acks, reads, deletes, expiry and resumes continue, while a publisher's `QoS` ≥ 1 ack is refused, not granted — and dropping back under restores growth. Brownout is active while **either** axis is over; `brownout{axis="memory"}` and `process_resident_bytes` say which. A **watermark, not a ceiling**: nothing here stops RSS rising, so keep the container/cgroup limit as the hard bound. Needs `/proc` (Linux); elsewhere the broker logs at WARN that it is not enforcing, rather than pretending. Unset = off |
| `MQTTD_AUTH_TIMEOUT` | Per-round enhanced-auth reply timeout, seconds (ADR 0013; default `10`) |
| `MQTTD_DURABLE_SESSIONS` | Durable, consensus-backed replicated session store (ADR 0006/0007) — **on by default** (ADR 0029); set `0`/`false`/`off`/`no` for the lightweight in-memory store (an explicit choice: it needs no ephemeral opt-in). A node with no `MQTTD_SWIM_SEEDS` founds the lease group. On with no `MQTTD_DATA_DIR` → **REFUSED at startup** (issue #240) unless `MQTTD_ALLOW_EPHEMERAL_DURABILITY` is set |
| `MQTTD_DATA_DIR` | Directory for on-disk persistence (ADR 0018). With durable on (default) the lease group + replicated log are on-disk, surviving a full-cluster restart (recommended for production); **unset with durable on → REFUSED at startup** (issue #240) unless the ephemeral opt-in below is set. With durable off, unset is plain in-memory |
| `MQTTD_ALLOW_EPHEMERAL_DURABILITY` | **Dev/tests only** (issue #240): any non-empty value (presence = on) permits durable-on with **no** data dir — replicated state in MEMORY only, so a correlated quorum restart loses acked messages. Without it that combination refuses to start (and fails `--check-config` and a live reload), naming both remedies. Loudly `EPHEMERAL durability`-warned on every start while active |
| `MQTTD_LEASE_VOTERS` | Bounded lease-consensus voter set `N` (ADR 0021; default `5`, recommend odd). At most `N` members vote on lease ownership; every other member joins as a learner that still receives the lease log and can own/serve sessions — so consensus cost stays fixed (quorum `⌊N/2⌋+1`) as the cluster grows. `1` = no fault tolerance, `3` tolerates one voter loss, `5` two |
| `MQTTD_MIN_REPLICAS` | Min-replicas write floor (issues #167/#239; default `majority`). Replica sets shrink with membership — `min(R, alive)` — so without a floor a shrinking cluster silently trades the configured durability for availability, down to quorum-of-1. A group below the floor **refuses** durable writes instead (QoS≥1 acks withheld so sources redeliver, retained mutations queue; reads, QoS 0, acked-driven truncation and removal keep serving, but QoS 2 in-flight bookkeeping does not). `majority` derives the floor from the members this node knows about — the quorum-committed durable roster (authoritative), falling back to the largest membership it has ever observed only before a roster exists, bounded below by `MQTTD_READY_MIN_MEMBERS` — capped at R: **no floor at all** while it has never known a peer (single-node stays fully operational) and **2** in a 3-node cluster, which the write quorum already needs. An integer sets an absolute floor; `1` disables it and accepts single-copy acks. Above R = rejected at startup (and by `--check-config`) |
| `MQTTD_FAILURE_DOMAIN` | This node's own failure-domain label (ADR 0016 T5), e.g. `rack-a`. Advertised over the authenticated SWIM gossip so the topology **self-assembles** — the bounded voter set spreads across racks/zones (losing a whole domain can't take quorum) with each node setting only its own label. The preferred mechanism. Unset → this node is unlabelled unless a peer/static map supplies one. If the cluster-bus cert **attests** a label (ADR 0016 T6), the cert wins: this value must match it (or peers reject this node's gossip) and may be omitted |
| `MQTTD_FAILURE_DOMAINS` | Static failure-domain topology (ADR 0016 T4): `node-id=domain` pairs (e.g. `n1=rack-a,n2=rack-a,n3=rack-b`). A cluster-uniform seed/fallback; per-node gossip labels (`MQTTD_FAILURE_DOMAIN`) override it. Unset → no static spread (id-ordered selection unless labels are gossiped) |
| `MQTTD_TLS_BIND` | TLS 1.3 client listener, e.g. `0.0.0.0:8883` (needs `…_CERT`/`…_KEY`) |
| `MQTTD_TLS_CERT` / `MQTTD_TLS_KEY` | Server certificate chain + key (PEM) |
| `MQTTD_TLS_CLIENT_CA` | Require client certs (mTLS); identity = certificate CN (see `MQTTD_MTLS_IDENTITY_SOURCE`) |
| `MQTTD_MTLS_IDENTITY_SOURCE` | Which field of a verified client certificate *is* the identity (ADR 0004 T11): `cn` (default), `san-dns`, `san-uri`, `san-email`. **No fallback and no ordering luck** — if the chosen field is absent, or the cert carries two of them, the connection is refused rather than silently identified as something else. A subject containing `+` or `#` is always refused, and `/` is refused for every source but `san-uri` (a URI identity keeps its slashes, and the ACL engine in turn refuses to substitute a `/`-bearing subject into `%i`) — so an identity can never smuggle topic structure into a pattern. Client listeners only (the cluster bus has its own identity rules, ADR 0016 T6); changing it is a restart-level edit, reported as such by a reload |
| `MQTTD_TLS_CRL` | Certificate revocation list (PEM; needs `…_CLIENT_CA`). A client whose cert is listed is refused at the TLS handshake **and its live session is evicted on reload** (ADR 0002/0040); re-read on `SIGHUP`, so a published CRL applies with no restart |
| `MQTTD_WSS_BIND` | MQTT-over-WebSocket **over TLS** (`wss://`), e.g. `0.0.0.0:8884` (ADR 0035; reuses `…_CERT`/`…_KEY`/`…_CLIENT_CA` — same TLS 1.3 + mTLS + hot reload as the TLS listener) |
| `MQTTD_WS_BIND` | **Insecure** plaintext MQTT-over-WebSocket (`ws://`) — for browsers in local/dev only (ADR 0035) |
| `MQTTD_QUIC_BIND` | MQTT-over-QUIC (UDP), e.g. `0.0.0.0:8885` (ADR 0036; reuses `…_CERT`/`…_KEY`/`…_CLIENT_CA`). QUIC mandates TLS 1.3 (no plaintext mode); **multi-stream** (one session across many streams, no head-of-line blocking); **non-standard** (EMQX-style), identity = leaf CN, no 0-RTT for CONNECT |
| `MQTTD_PLAINTEXT_BIND` | **Insecure** plaintext TCP client listener |

### Client authentication & authorization
| Variable | Purpose |
|---|---|
| `MQTTD_ALLOW_ANONYMOUS` | **Insecure**: permit clients with no credentials |
| `MQTTD_PASSWORD_FILE` | Argon2id `username:phc-hash` password file. Generate lines with `mqttd --hash-password <user>` (below) — `mosquitto_passwd` hashes are a different format and are not accepted |
| `MQTTD_JWT_HS256_SECRET_FILE` / `MQTTD_JWT_RS256_PEM` | Static JWT verification key, **by file** (ADR 0046 T5): the HS256 shared secret and the RS256 public key are both read from a path, so the key is mounted from a Secret, never inlined. A trailing newline in the HS256 file is trimmed |
| `MQTTD_JWT_ISSUER` / `MQTTD_JWT_AUDIENCE` | Optional JWT `iss`/`aud` constraints (static-key mode) |
| `MQTTD_OIDC_ISSUER` | **OIDC-mode token auth** (ADR 0050): the broker discovers the issuer's JWKS and follows key rotation live (no restart). Requires `MQTTD_OIDC_AUDIENCE`. Mutually exclusive with `MQTTD_JWT_*`. Asymmetric-only (RS256/ES256; a public JWKS never feeds an HMAC verify). Tokens ride in the CONNECT **password** field (EMQX convention). Proven against a real Keycloak, forced key rotation included (nightly `oidc` job) |
| `MQTTD_OIDC_AUDIENCE` | Required `aud` in OIDC mode (not optional — a token minted for another audience must be refused) |
| `MQTTD_OIDC_JWKS_REFRESH` / `MQTTD_OIDC_MAX_STALE` | JWKS background-refresh interval (s, default 300) and the last-known-good staleness window (s, default 86400) after which token auth **fails closed** on a persistent IdP outage |
| `MQTTD_OIDC_GROUPS_CLAIM` / `MQTTD_OIDC_ALLOW_HTTP` | Claim to read groups from (default `groups`); and a loud INSECURE override permitting an `http://` issuer (tests only) |
| `MQTTD_ACL_FILE` | TOML topic-ACL policy (deny by default) |
| `MQTTD_HTTP_AUTH_URL` | **Remote HTTP auth hook** (ADR 0004 T16): the broker POSTs `{client_id, username, password, method}` and reads the **HTTP status** as the verdict — `200` allow (an optional `{"groups":[…]}` body enriches the identity), `401`/`403` deny, **anything else — a 5xx, a timeout, an unreachable host — DENIES**. One hook reaches LDAP / OAuth2 / a bespoke user table without a broker integration per backend. Must be `https` unless `MQTTD_HTTP_AUTH_ALLOW_HTTP` is set (the password crosses this link). Tried after the local password file, so a user in both is answered without a round trip |
| `MQTTD_HTTP_AUTH_TIMEOUT` | Per-request timeout, seconds (default 5). The broker applies **no** timeout of its own around an authenticator, so this is the only bound on how long a CONNECT waits — and it expires **closed** |
| `MQTTD_HTTP_AUTH_CACHE_SECS` / `MQTTD_HTTP_AUTH_CACHE_MAX` | Cache **accepted** credentials for N seconds (default `0` = off), up to this many entries (default 10 000, bounded because the cache sits on an attacker-reachable path). Rejections are **never** cached: a fixed password takes effect at once, and caching denials would turn a hook blip into a lasting outage. Keys are a hash of the credential, never the credential |
| `MQTTD_CONFIG` | Path to the TOML config file (ADR 0046); `--config <path>` overrides it. Unset = defaults + this env overlay |
| `MQTTD_CONFIG_WATCH` | Opt-in filesystem auto-reload (ADR 0033): poll interval in **seconds**. When the config file **or** a referenced policy file changes on disk, reload the whole config via the same validate-before-swap routine as `SIGHUP` (no restart) — the Kubernetes ConfigMap case. Unset/`0` = disabled (signal-only default) |

### Cluster transport & membership
| Variable | Purpose |
|---|---|
| `MQTTD_PEER_BIND` | Inter-node peer listener, e.g. `0.0.0.0:7001` |
| `MQTTD_PEER_TLS_CA` / `…_CERT` / `…_KEY` | Cluster-bus mTLS material (set all three). A leaf whose SANs include `URI:urn:fss:failure-domain:<label>` has its failure domain **CA-attested** (ADR 0016 T6): the label is authoritative on the gossip plane (a contradicting self-claim is rejected) and can replace `MQTTD_FAILURE_DOMAIN` entirely — relabel by reissuing the cert |
| `MQTTD_PEER_TLS_CRL` | Cluster-bus CRL (PEM, **signed by the cluster CA**; needs the three above). Signed gossip from a revoked cert is dropped (ADR 0022 T7), fresh peer handshakes are refused in both directions, and **established peer links are torn down on reload** (ADR 0040); expired/not-yet-valid certs are rejected regardless. Hot-reloads via `SIGHUP`/`MQTTD_CONFIG_WATCH`, so publishing a CRL evicts a compromised node with no restart |
| `MQTTD_PEERS` | Comma-separated static peer addresses (alternative to gossip) |
| `MQTTD_SWIM_BIND` | SWIM gossip UDP bind (needs `MQTTD_PEER_BIND`) |
| `MQTTD_SWIM_SEEDS` | Comma-separated gossip addresses of existing members |
| `MQTTD_SWIM_KEY` | 64-hex-char cluster gossip key, **inline** (`openssl rand -hex 32`). A raw secret |
| `MQTTD_SWIM_KEY_FILE` | Path to a file holding the 64-hex gossip key (ADR 0046 T5): the secret-by-reference form, mountable from a Secret so it stays out of the config file. Mutually exclusive with the inline `MQTTD_SWIM_KEY` |
| `MQTTD_REFOUND_GUARD` | Refuse to serve after re-founding a cluster beside a live one — **on by default**; set `0`/`false`/`off`/`no` only to re-bootstrap deliberately beside a cluster you are abandoning. A node whose data dir was lost mints a second identity and would otherwise serve clients an empty store; it now latches NotReady once it hears the other cluster's gossip (a genuine first bootstrap hears none, so it is unaffected) |
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
# Prometheus scrape (the dev-only ephemeral opt-in, #240 — production sets MQTTD_DATA_DIR)
MQTTD_HEALTH_BIND=0.0.0.0:8080 MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
  cargo run --bin mqttd   # then GET :8080/metrics

# also push to an OpenTelemetry Collector
MQTTD_HEALTH_BIND=0.0.0.0:8080 MQTTD_OTLP_ENDPOINT=http://localhost:4318 \
  MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
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

### On Kubernetes (Helm)

A Helm chart under [`deploy/helm/mqttd`](deploy/helm/mqttd) runs the broker as a **StatefulSet**
that encodes the operational contract (ADR 0047), so the safe path is the default.
Day-2 procedures — cert/key rotation, scaling, PVC lifecycle, founder recovery, backup —
are in [`docs/OPERATIONS.md`](docs/OPERATIONS.md):

```sh
# Mints the gossip key, server TLS, a starter ACL and ONE CLUSTER-BUS CERTIFICATE PER NODE
# into the namespace, then prints the exact --set flags to wire them. Verifies every
# certificate property it reports before installing it.
NS=mqttd REPLICAS=3 ./deploy/helm/mqttd/bootstrap.sh

# mqttd-tls is a kubernetes.io/tls Secret; mqttd-peer-tls carries ca.crt plus
# <pod>.crt/<pod>.key per pod (the layout bootstrap.sh mints and prints).
helm install mqttd deploy/helm/mqttd -n mqttd \
  --set replicaCount=3 \
  --set secrets.tls.secretName=mqttd-tls \
  --set secrets.peerTls.secretName=mqttd-peer-tls \
  --set secrets.gossipKey.secretName=mqttd-gossip
```

Naming those Secrets is all that is needed: the chart derives the paths the broker reads
(`MQTTD_PEER_TLS_*` — each pod's **own** leaf — and `MQTTD_SWIM_KEY_FILE`) from the names, so
material that is mounted is always material that is used.

- **Per-pod PersistentVolume** (`volumeClaimTemplate`) for the redb data dir — a rescheduled pod
  reattaches its volume and recovers durable state, never the ephemeral-storage data-loss trap.
- **Self-forming mesh:** pod-0 founds the lease group; pods 1..N seed to it over the headless
  Service. Node id = the stable pod name. (An init container renders the per-pod config, since the
  image is distroless.)
- **Config from a ConfigMap, secrets by path from Secrets** (ADR 0046); a `--check-config` init
  container fails a bad config before the pod serves.
- **Safe scale-down:** a `preStop` runs `mqttd --decommission`, which drains every held key to its
  post-departure replica set (ADR 0043) and holds the pod open until the drain completes — a
  planned removal loses nothing.
- **Quorum-safe rollout:** one pod at a time (ADR 0039) + a `PodDisruptionBudget` (`maxUnavailable: 1`).
- **Mutually-authenticated cluster bus, one certificate per node.** The bus binds node identity to
  the certificate's Subject CN, so each pod reads its own leaf out of the peer-TLS Secret
  (`<pod>.crt` / `<pod>.key`) — a single shared certificate would drop every peer link. Growing the
  cluster means minting the new ordinals' leaves first; a pod whose ordinal has none fails its init
  container and says so. See
  [Cluster-bus certificates](docs/OPERATIONS.md#cluster-bus-certificates--one-per-node-and-why-that-is-not-negotiable).

Validate a rendered config without a cluster: `mqttd --check-config --config <file>`. See
[`docs/mqttd.example.toml`](docs/mqttd.example.toml) for every setting.

### Running the demo, migrations and test scripts

There are 27 runnable scripts here — the demo stack, the Mosquitto converter, the smoke and
conformance suites, the Kubernetes end-to-end runs, the benchmark harness. `mqttui` is the
one place they are listed, explained and started ([ADR 0056](docs/adr/0056-mqttui.md), and
[Start here](#start-here) for installing it):

```sh
mqttui --list                 # every task, and what `-` / `!` mean
mqttui --show deploy-smoke    # what it does, needs, and costs — before you run it
mqttui --run  deploy-smoke
mqttui                        # the same, as a terminal UI
```

It says what each task needs **before** you start it, rather than failing with
`FATAL: 'kind' not found` five minutes in, and CI fails if a script in the tree is missing
from its manifest — so the list cannot quietly go stale.

It is a **separate workspace with its own lockfile**: nothing it depends on can reach the
broker's dependency graph, which is what `cargo-deny`, `cargo-audit` and the SBOM are cut
from. Installed standalone it carries the examples inside the binary, marks the tasks that
need this repository rather than hiding them, and `mqttui update` fetches the latest
examples as a **cosign-signed bundle** published from `main` — never as a branch download.

### Without Kubernetes (Compose, systemd)

Kubernetes is not required, and the non-Kubernetes path is a shipped artifact rather than
prose. [`deploy/`](deploy) has all four packagings side by side; the two that need no
cluster:

```sh
cd deploy/compose && ./bootstrap.sh && docker compose up -d   # three nodes, one host
```

```sh
deploy/systemd/gen-certs.sh ca                                 # ONCE, on an admin box
deploy/systemd/gen-certs.sh node mqttd-1 mqttd-1.example.com   # ...then once per node
sudo install -m 0644 deploy/systemd/mqttd.service /etc/systemd/system/
sudo install -m 0640 deploy/systemd/mqttd.env.example /etc/mqttd/mqttd.env
sudo $EDITOR /etc/mqttd/mqttd.env                              # five marked lines
sudo systemctl enable --now mqttd                              # bare metal / VMs
```

Both are configured exactly as the chart is (`MQTTD_*`, secrets by path), and what they
default to is: **TLS 1.3 on the client listener (`8883`) with no plaintext listener at
all**, a **mutually authenticated cluster bus** — which is also what makes gossip per-node
signed ([ADR 0022](docs/adr/0022-signed-gossip.md)) rather than shared-key only —
authentication on, deny-by-default ACLs, majority-aware readiness, and a memory bound. The
systemd unit is hardened (`ProtectSystem=strict`, empty `CapabilityBoundingSet`,
`SystemCallFilter=@system-service`).

Neither ships a keypair, because a keypair in a git repository is not a keypair. Compose
mints a **throwaway starter CA** in a one-shot before the brokers start, so `up -d` stays
one command and is TLS on the first run; systemd ships the TLS lines uncommented and a
[`gen-certs.sh`](deploy/systemd/gen-certs.sh) that mints the material — one CA for the
cluster, one leaf set per node, run from an admin machine so the CA private key never
reaches a broker host — so an unedited install fails closed at startup, naming the setting
and the path it could not read, rather than serving cleartext. Both are self-signed starter
PKIs to replace before production.

Plaintext is still available and is now an explicit, named opt-in:
`docker compose -f compose.yaml -f compose.plaintext.yaml up -d` (or uncommenting one
labelled line in the systemd env file). Either way every broker logs `INSECURE: starting
PLAINTEXT MQTT listener` on every start, for as long as it is on.

Two things Kubernetes was doing for you become yours:

- **Seed lists and the founder rule.** Exactly one node bootstraps with an *empty* seed
  list — that is what makes it found the cluster — and must be given seeds afterwards.
  Both READMEs and the annotated env file say where this bites.
- **Health checks.** `mqttd --probe /readyz` (or `/livez`) asks this node's own health
  endpoint and exits non-zero on anything but `200`, because the image is distroless and
  Compose/systemd health is a *command*, not an HTTP GET. `/livez` passing while `/readyz`
  fails is a minority node: pull it from the load balancer, do not restart it.

`scripts/deploy-smoke.sh` boots three real nodes from the shipped env file on every CI run —
over TLS, with a mutually authenticated bus, using a PKI minted by the shipped
`deploy/compose/init.sh`, and two more from `deploy/systemd/gen-certs.sh` so neither shipped
recipe can rot — and proves the security posture (including that no node logs
`INSECURE`, that a cleartext client is refused, and that plaintext comes back *only* with
the overlay), cross-node routing, an acked QoS 1 message surviving `SIGKILL` of the node
that accepted it, and the readiness floor. `scripts/compose-smoke.sh` then brings the
actual `compose.yaml` up in containers on the nightly image lane, because a per-PR job that
never runs `docker compose up` cannot tell you the file works — always against an image built
from this repository, so `compose.yaml`'s published-`:latest` default is the one input those
lanes do not cover (issue #263).

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
route client traffic to it once ready. The min-replicas write floor does not get in
the way of that motion: the 1-node broker has never known a peer, so its derived
floor is no floor at all, and it arms itself once peers appear.

**The two-node truth.** Two members mean replica sets of two and a write quorum of
2-of-2 — a two-node durable cluster has *strictly worse* write availability than one
node (either node down blocks durable writes). The write floor makes that literally
true: before it, a two-node cluster silently resumed acking on a single copy once the
dead peer was declared `Dead`. Two nodes are supported as a waypoint, but the
recommended upgrade is **1→3 in one motion**: start both new nodes, then treat the
pair-state as transient.

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
  frame encodes in ~439 ns and decodes in ~357 ns (the fsync cost is the disk's, not the
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

## Contributing

Bug reports, questions and patches are welcome. Start with
[CONTRIBUTING.md](CONTRIBUTING.md) — it covers the build/test gates and the two
local conventions worth knowing before your first PR. Participation is governed
by the [Code of Conduct](CODE_OF_CONDUCT.md).

**Suspected vulnerabilities do not go in public issues** — use GitHub's private
vulnerability reporting; the policy is in [SECURITY.md](SECURITY.md).

Release notes live in [GitHub Releases](https://github.com/mbilling/fss-mqtt-broker/releases);
[CHANGELOG.md](CHANGELOG.md) explains where to look for what.

## License

Apache-2.0. See [LICENSE](LICENSE).
