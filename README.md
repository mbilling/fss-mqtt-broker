# mqttd — a security-first, cluster-native MQTT broker

[![CI](https://img.shields.io/github/actions/workflow/status/mbilling/fss-mqtt-broker/ci.yml?branch=main&label=CI)](https://github.com/mbilling/fss-mqtt-broker/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/mbilling/fss-mqtt-broker)](https://github.com/mbilling/fss-mqtt-broker/releases)
[![Release date](https://img.shields.io/github/release-date/mbilling/fss-mqtt-broker)](https://github.com/mbilling/fss-mqtt-broker/releases/latest)
[![Last commit](https://img.shields.io/github/last-commit/mbilling/fss-mqtt-broker)](https://github.com/mbilling/fss-mqtt-broker/commits/main)
[![Maintained](https://img.shields.io/maintenance/yes/2026)](SUPPORT.md)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/mbilling/fss-mqtt-broker/badge)](https://scorecard.dev/viewer/?uri=github.com/mbilling/fss-mqtt-broker)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/14161/badge)](https://www.bestpractices.dev/projects/14161)

> An MQTT 3.1.1 + 5.0 broker built to be the most cyber-secure
> broker available, designed to scale horizontally, with a 100% open feature
> set.

## Start here

Run the broker first; the claims about it can wait. The two-minute single node
below needs only Docker, and the five-idea primer right after it defines every
MQTT term the rest of this file leans on. Terms of art beyond those five — the
clustering and security vocabulary — are defined at first use or in the
[glossary](docs/GLOSSARY.md).

### Try it in two minutes

You need Docker, plus the standard mosquitto clients for the pub/sub half
(`brew install mosquitto` on macOS, `apt install mosquitto-clients` on
Debian/Ubuntu — Windows is covered in its own block below).

**macOS / Linux:**

```sh
docker run -d --name mqttd -p 1883:1883 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd -v mqttd-data:/var/lib/mqttd \
  ghcr.io/mbilling/fss-mqtt-broker:latest

mosquitto_sub -h 127.0.0.1 -p 1883 -t 'sensors/+/temp' &
mosquitto_pub -h 127.0.0.1 -p 1883 -t 'sensors/kitchen/temp' -m '21.5C'
```

**Windows (PowerShell)** — same demo, paste-safe: no `\` continuations, no `&`
backgrounding (both fail in PowerShell), and the clients are called by full path
because mosquitto's installer does not add itself to `PATH`:

```powershell
winget install --id EclipseFoundation.Mosquitto -e   # one-time: installs mosquitto_pub/sub

docker run -d --name mqttd -p 1883:1883 -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 -e MQTTD_ALLOW_ANONYMOUS=1 -e MQTTD_DATA_DIR=/var/lib/mqttd -v mqttd-data:/var/lib/mqttd ghcr.io/mbilling/fss-mqtt-broker:latest

$mq = "$Env:ProgramFiles\mosquitto"
Start-Process "$mq\mosquitto_sub.exe" -ArgumentList '-h 127.0.0.1 -p 1883 -t sensors/+/temp'
& "$mq\mosquitto_pub.exe" -h 127.0.0.1 -p 1883 -t sensors/kitchen/temp -m 21.5C
```

The subscriber opens in its own window; `21.5C` arrives there.

That is **plaintext with anonymous clients** — a first look, never a deployment. The
named volume is what makes it honest: durable sessions are on by default, and durable-on
with no data dir **refuses to start** (issue #240) rather than silently keeping acked
messages in RAM. (The volume outlives `docker rm -f mqttd`; `docker volume rm mqttd-data`
removes the state too.)
The broker says so in its own logs, loudly, every time. When you are ready for
something real, the [secured quickstart](#single-node-secured-tls-13--mtls--acl)
stands up TLS 1.3, mutual TLS and a deny-by-default ACL in about the same number
of commands, and CI runs those exact commands on every push.

### New to MQTT?

Skip this if you already run a broker. **MQTT** itself — the name — is Message
Queuing Telemetry Transport: a lightweight publish/subscribe protocol in which
clients publish messages to named *topics* on a broker, and the broker delivers
each message to whoever subscribed to a matching topic. Beyond that, these five
ideas are what the rest of this file assumes, and nothing else here explains them.

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

**Jump to:** [**Start here**](#start-here) ·
[Try it in two minutes](#try-it-in-two-minutes) ·
[New to MQTT?](#new-to-mqtt) · [Glossary](docs/GLOSSARY.md) · [Troubleshooting](docs/TROUBLESHOOTING.md) ·
[Where it stands](#where-it-stands) · [What works today](#what-works-today) ·
[Security](#security) · [Clustering](#clustering) ·
[Bridging](#bridging-to-other-security-zones) · [How it compares](#how-it-compares) ·
[Enterprise readiness](#enterprise-readiness) ·
[**Limitations**](#limitations) · [Install](#install) ·
[Secured quickstart](#single-node-secured-tls-13--mtls--acl) ·
[Configuration](#configuration) · [Kubernetes](#on-kubernetes-helm) ·
[Performance](#performance) · [Contributing](#contributing)

## Where it stands

**v1.0.0 is released** — signed, reproducible, [SBOM](docs/GLOSSARY.md#supply-chain)-attested,
with the [ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md) compatibility
promise in force. In place today:

- **Protocol**: MQTT 3.1.1 + 5.0 over TCP, TLS 1.3, WebSocket, and QUIC — full
  v5 semantics (session/message expiry, aliases, flow control, shared
  subscriptions, User Properties, enhanced `AUTH`), not just the wire codec.
- **Security**: [mTLS](docs/GLOSSARY.md#security-and-pki)-CN / password /
  [JWT](docs/GLOSSARY.md#security-and-pki)/OIDC identity → deny-by-default topic
  [ACLs](docs/GLOSSARY.md#security-and-pki) → tamper-evident audit; a mutually
  authenticated cluster bus and authenticated gossip membership.
- **Durability, on by default**: consensus-backed replicated sessions
  ([openraft](https://github.com/databendlabs/openraft) lease group,
  epoch-fenced quorum replication — terms in the
  [glossary](docs/GLOSSARY.md#mqttd-clustering-and-durability)), cross-node
  takeover, and data-safe elastic resize. An acked QoS 1/2 message survives the
  loss of the node that accepted it, **including one already in flight** — the
  durable record is written before the packet reaches the wire
  ([#124](https://github.com/mbilling/fss-mqtt-broker/issues/124)), and QoS 2
  redelivery resumes under the packet id the subscriber already knows
  ([#130](https://github.com/mbilling/fss-mqtt-broker/issues/130)).
- **Operations**: Prometheus/OTLP metrics, resource governance (caps, quotas,
  rate limits, bounded queues), Helm chart + Kubernetes operator, online
  backup/restore — and a continuous-assurance program (fault/upgrade/soak
  harnesses, fuzzing, foreign-client conformance oracles, published baselines).

The largest known gaps, stated rather than discovered (full list:
[**Limitations**](#limitations)): the memory watermark is backpressure, not a
hard ceiling (the container limit is the real bound), and the horizontal
scaling curve is unmeasured — the durable path's throughput/latency *is*
measured on one host, limits printed beside every number
([docs/benchmarks/DURABLE-PATH.md](docs/benchmarks/DURABLE-PATH.md)).

See [`docs/adr/`](docs/adr/) for the decisions and the
[**delivery dashboard**](docs/delivery/STATUS.md) — the authoritative, live
record of exactly what is built (76 ADRs, per-task status).

## The runnable map: mqttui

**`mqttui`** is the map of everything runnable in this repository — the demo
cluster, the Mosquitto / EMQX / HiveMQ migration converters, the secured quickstarts, the
Kubernetes examples. It tells you what each task needs *before* it starts, instead of failing five
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
mqttui migrate mosquitto /etc/mosquitto/mosquitto.conf      # Mosquitto
scripts/migrate/from-emqx.py /etc/emqx/emqx.conf --acl-file /etc/emqx/acl.conf
scripts/migrate/from-hivemq.py /opt/hivemq/conf/config.xml
```

Each converts your config *and* your ACL/RBAC policy, and **what it produces is a reviewed
DRAFT, not a translated configuration — read it before you deploy it.** Every construct a
converter *reads* is either translated or becomes a `# TODO(migrate):` comment at the point it
belongs, never a silent drop, because a setting that quietly vanishes is how a migration ships
the wrong policy; anything it could not derive from your input comes out **commented out** beside
that TODO, so the worst case is a config **you** finish rather than a live setting nobody derived.
What is *not* claimed: total coverage of any vendor's schema, and correctness of what was read —
[`docs/MIGRATION.md`](docs/MIGRATION.md#known-gaps-after-round-4) lists every construct known to
be misread or unhandled, with what to check by hand. And because
mqttd cannot import another broker's *session* state, the converter is only half the job:
[`docs/MIGRATION.md`](docs/MIGRATION.md) carries the per-broker mapping tables **and** a
dual-run cutover playbook (bridge both brokers, move clients in cohorts, verify, cut) whose
bridge step is exercised against a real third-party broker. Then see it hold up: `mqttui --run deploy-smoke` boots the three-node reference deployment (password auth,
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

Want to see a real cluster with dashboards? `mqttui --run demo-stack` starts seven
nodes with Prometheus and Grafana dashboards on `localhost:3000` and a load
generator so the panels move — it starts 25 containers, and `mqttui` warns you
before it does.

Tasks that need this repository — building it, or the fixtures that will not fit in a
binary — are marked `-` in the list with the reason, rather than left to fail.

## Principles

- **Security is the product.** Secure by default; every insecure mode must be
  opted into and is loudly logged.
- **Open == Enterprise.** One Apache-2.0 codebase, no gated features. Only
  support, SLAs, and certified builds are paid.
- **Horizontal scalability by design.** Shared-nothing nodes, no coordinator on
  the publish hot path — an architectural statement, not yet a benchmarked
  curve: what is measured is one 3-node point on one host, limits printed
  beside every number ([docs/benchmarks/DURABLE-PATH.md](docs/benchmarks/DURABLE-PATH.md)).
- **Memory safety.** Rust, `#![forbid(unsafe_code)]` across crates.

## What's different about it

Four things this does that the brokers it is usually compared against do not.
The full matrix — including every cell we lose — is
[`docs/COMPARISON.md`](docs/COMPARISON.md).

- **Durable sessions are on by default**, quorum-replicated: an acked QoS 1/2
  message survives the loss of the node that accepted it — queued *or already
  in flight*. When a group is too thin to keep that promise, the write is
  **refused**, never acked on one copy (publishers redeliver). The one scope
  caveat lives in [Limitations](#limitations). For contrast: Mosquitto and
  NanoMQ are single-node, VerneMQ documents queue loss on node death, EMQX's
  durable sessions are opt-in.
- **A policy reload evicts live sessions.** Revoke a certificate, remove a user,
  or tighten a grant, and the *already-connected* client is cut — not left
  running until it happens to reconnect. No compared broker documents this.
- **Clustering is not a paid feature.** Apache-2.0 including signed,
  reproducible binaries. EMQX has been BSL 1.1 (the source-available Business
  Source License) since 5.9 with clustering commercial; VerneMQ's production
  binaries are EULA-paid.
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

Two terms of art in that sketch: **SWIM** (Scalable Weakly-consistent
Infection-style process-group Membership) is the gossip protocol nodes use to
discover each other and detect failures, and the durable plane's vocabulary —
lease, epoch, quorum, replica set — is defined in the
[glossary](docs/GLOSSARY.md#mqttd-clustering-and-durability).

Crossing into a **different** trust domain is a separate tool with its own
process and credentials — see [Bridging](#bridging-to-other-security-zones).

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
  keepalive expiry, session takeover, protocol-violation close) and on a v5
  DISCONNECT with a non-zero reason (`0x04` Disconnect with Will Message);
  discarded only on a clean DISCONNECT (reason `0x00`).
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
- **Subscription identifiers** — delivered (issue #266): one packet carries every
  matching subscription's id (`[MQTT-3.3.4-4]`), an id-less match attaches none,
  retained and offline replay carry them, and the id survives reconnect as session
  state (persisted per subscription). The CONNACK advertises `0x29 = 1`; a client
  PUBLISH carrying an identifier is still refused with `0x82` (`[MQTT-3.3.4-6]`) —
  ids are the broker's to attach, never a publisher's to inject.
- Reason codes and DISCONNECT with reason on protocol/quota violations.

Both protocol versions round-trip against two independent foreign clients
(Mosquitto CLI + Eclipse Paho) in CI — see [Build & test](#build--test).

### Security
- **TLS 1.3** client listener (`rustls` on `aws-lc-rs` — one crypto provider for
  the whole build, [ADR 0053](docs/adr/0053-single-crypto-provider-aws-lc-rs.md)), optional
  per-listener client-certificate mTLS, **fleet-sized session resumption**
  (32k-entry cache by default, 24 h ceiling, `session_cache` to size or disable),
  and a **hardened TLS 1.2 opt-in** for legacy fleets (a strict ECDHE+AEAD —
  forward-secret, authenticated-encryption-only — allowlist, Extended Master
  Secret required; see
  [Limitations](#limitations)) — [ADR 0002](docs/adr/0002-transport-security.md).
  Server and client certificates: **ECDSA P-256** (what the test suite runs end
  to end, including mTLS and [CRL](docs/GLOSSARY.md#security-and-pki)
  (certificate-revocation-list) revocation) and RSA ≥ 2048. Also native
  **MQTT-over-WebSocket** (`ws://` / `wss://`, the latter sharing the same TLS 1.3 + mTLS),
  so browsers are first-class clients — [ADR 0035](docs/adr/0035-websocket-transport.md) —
  and **MQTT-over-QUIC** (UDP; TLS 1.3 + mTLS; **multi-stream** — one session across many QUIC
  streams, no head-of-line blocking) — [ADR 0036](docs/adr/0036-quic-transport.md).
- **Mutually-authenticated cluster bus** against a dedicated cluster CA; each
  peer's node id is bound to its certificate Common Name
  ([ADR 0004](docs/adr/0004-identity-and-authentication.md)).
- **Authenticated SWIM gossip**: every membership datagram carries an
  [HMAC](docs/GLOSSARY.md#security-and-pki)-SHA256 tag under a cluster-shared key
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

Want to *run* one, secured, without Kubernetes? The
[secured three-node tutorial](docs/SECURED-CLUSTER-TUTORIAL.md) walks the shipped
compose reference deployment end to end — TLS, mutual-TLS cluster bus, signed
gossip, deny-by-default ACL, majority-aware readiness — including the founder
rule and how the starter PKI maps to a real CA.

- Shared-nothing nodes: a client connects to any node.
- **SWIM gossip membership** (failure detection + anti-entropy), authenticated.
- **Membership-driven mesh**: nodes discover each other via gossip and establish
  mTLS peer links automatically — no static peer list required.
- **Interest-based routing**: a publish fans out only to peers whose gossiped
  subscription interest matches the topic.
- **Session placement** ([HRW](docs/GLOSSARY.md#mqttd-clustering-and-durability) —
  Highest Random Weight, "rendezvous" hashing — over live membership): every persistent
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
  **CP trade, explicitly** (in
  [CAP terms](docs/GLOSSARY.md#mqttd-clustering-and-durability): consistency kept,
  availability of new writes given up during a partition): during a partition the
  quorum-less side serves the last
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
[`docs/COMPARISON.md`](docs/COMPARISON.md) (dated 2026-08-19). The one-paragraph
version:

|  | mqttd's answer |
|---|---|
| Durable sessions | Quorum-replicated **by default**; acked QoS 1/2 survives node loss (proven under SIGKILL/partition harnesses), and covers a message **in flight to a connected subscriber** as well as one queued for a disconnected one — the durable append happens before the wire send ([#124](https://github.com/mbilling/fss-mqtt-broker/issues/124), reproduced against the real binary under SIGKILL). Those appends — and the QoS 2 outbound-id records and packet-id reservations that precede an online wire send — run **off the hub loop** in per-session lanes (ADR 0061, issue #242): a placement group with a degraded follower set delays only its own sessions' publishes and deliveries (bounded by the 5 s replication RPC timeout per lane job, 256 queued jobs per session, then the newest publish is withheld and retried) — never other groups' publishes, connects, or subscribes, with the residuals named in the ADR (the *ack* path's store writes — truncation, QoS 2 phase advances, id clears — still run on-loop, watched by the dispatch histogram's `ack` class, as does one publish-path corner: the eviction truncate past a 10 000-entry per-session backlog) — and time-on-loop is exported as `mqttd_hub_dispatch_seconds` so a regression pages before 3 a.m. does. A group too thin to keep the promise **refuses** new durable writes by default (the min-replicas floor, `MQTTD_MIN_REPLICAS=majority`: a majority of the members the node knows about, capped at R) rather than acking on one copy; a node that has never known peers still serves fully. Above the (off-by-default) store or memory watermark the broker likewise **refuses the publisher** rather than acking a message it will not store — v5 gets `0x97 Quota exceeded`, v3.1.1 gets no ack and a close — including when the refusing session owner is a *peer* node: the refusal crosses the peer bus as a verdict (during a rolling upgrade, a link to an older build degrades to a withheld ack and a close). Nothing acked is lost; whether the message is re-sent is the *application's* decision — a v5 reason ≥ `0x80` completes the packet-id lifecycle (no client library retransmits it) and a clean-session v3.1.1 publisher resends nothing (ADR 0041 §5/T11/T12, counted as `quota_rejections_total{reason="brownout-publish"}`). The arms that still ack-and-drop, stated where the claim is: the **default** `drop-oldest` offline-queue overflow, which truncates the oldest *already-acked* entries out of a session's durable queue at the cap (counted `publish_dropped{reason="queue-overflow"}`); its opt-in `reject-newest` sibling, which acks and sheds the newest; for retained *values* only, a v3.1.1 retained publish over the retained quota or under brownout (delivered live, not retained); a publish for a durable session whose owner is gone, acked-and-dropped by the no-known-subscriber path; and — on a **co-subscribed filter** — a publish acked on one subscriber's storage while a mid-move durable co-subscriber's copy is stored nowhere (issue #305: `Accepted` means stored for **at least one** subscriber owed the message, not every one; the sole-subscriber form withholds, and the gap is pinned by a test that fails the day the promise strengthens). One deliberate, double-opt-in exception (ADR 0072): an MQTT 5 publisher may weaken **its own** ack per message via the `mqttd-durability` user property — `local` (ack after the owner's fsync, single-copy) or `relaxed` (ack at accept+submit, everything still runs best-effort) — honored only when the operator sets `MQTTD_ALLOW_RELAXED_PUBLISH`; otherwise the property is ignored and the publish gets the full quorum path, stronger than asked, never weaker. Mosquitto/NanoMQ are single-node; VerneMQ documents queue loss on node death; EMQX's durable sessions are opt-in. |
| Revocation | A policy reload **evicts live sessions and flows** (CRL'd cert, removed user, tightened grant — ADR 0040). Not documented by any compared broker. |
| Licensing | Apache-2.0 including signed, reproducible binaries. EMQX is BSL 1.1 (clustering commercial) since 5.9; VerneMQ's production binaries are EULA-paid. |
| Where we lose | No dashboard, rule engine (the replacement — a CI-tested external-consumer pattern — is the blueprint in [docs/INTEGRATION.md](docs/INTEGRATION.md)), HTTP admin API (by design — signal-driven ops), no MQTT-SN/CoAP, and **no production track record**: the matrix says so in as many words. |

## Enterprise readiness

The evaluator's shelf, built as a deliberate program (ADRs 0065–0069) after
v1.0.0. Every artifact is version-stamped and re-verified per release
(RELEASING.md's checklist). What is deliberately **not** claimed: no
certification of any kind is held (the mappings accelerate *your* assessment),
no third-party audit has run yet (a funded audit is planned — ADR 0065), and
the bus-factor/track-record reality is stated in the threat model rather than
papered over.

### Threat model and hardening baseline

[docs/THREAT-MODEL.md](docs/THREAT-MODEL.md) is the one-document answer to
"what is your threat model?" — STRIDE over the five trust surfaces, every
mitigation citing its ADR and enforcement site, every accepted risk quoted
from the record that accepted it. [docs/HARDENING.md](docs/HARDENING.md) is
its checkable companion: 34 L1/L2 items, each with the knob, the shipped
default, and a verification an auditor can run — starting with
`grep INSECURE:`, because the broker announces its own insecure postures.

### Audit trail, SIEM-ready

[docs/AUDIT-SCHEMA.md](docs/AUDIT-SCHEMA.md) is the contract a SIEM parser is
written against: hash-chained records, the complete event vocabulary, and the
boundary invariants to alert on. `scripts/audit-verify.py` reproves a captured
stream with no secret — tamper-evidence you can check, not believe.

### Compliance mappings — EU CRA, IEC 62443, SOC 2 / ISO 27001

Claims documents held to the repository's evidence discipline, in
[docs/compliance/](docs/compliance/): [EU CRA readiness](docs/compliance/eu-cra.md)
(Annex I mapped to checkable facts, plus the Article 14 reporting runbook),
[IEC 62443](docs/compliance/iec-62443.md) (4-1 SDL and 4-2 component
requirements with honest SL-C reads — the OT procurement language), and the
[SOC 2 / ISO 27001 evidence map](docs/compliance/soc2-iso27001.md)
(feature → control → pullable artifact, for *your* audit).

### Cryptography and the FIPS variant

[docs/compliance/crypto-policy.md](docs/compliance/crypto-policy.md) states
exactly what each build's cryptography is: one audited provider (AWS-LC),
TLS 1.3 by default — and the **fips build variant**, shipping as
`mqttd-fips` release binaries (byte-reproducible, module claim pinned at build
time), with the honest boundary stated: Argon2id password hashing is not a
FIPS-approved algorithm, so strictly-approved deployments authenticate with
mTLS or OIDC.

### Supply chain, per release

Every release publishes a CycloneDX SBOM per binary,
[OpenVEX dispositions](security/vex/) (the machine-readable "is mqttd affected
by CVE-X?"), SLSA build provenance, and keyless cosign signatures —
verification one-liners in [RELEASING.md](RELEASING.md).

### Continuously scored posture

The OpenSSF Scorecard and Best Practices badges at the top of this file are
live results, not decoration: CodeQL and Dependabot run in CI, `main` is
ruleset-protected (PRs + green checks required, force-push and deletion
blocked), and the best-practices
[self-certification record](docs/compliance/openssf-best-practices.md) is
kept in-tree.

### The documents at the repository root

- [SECURITY.md](SECURITY.md) — how to report a vulnerability privately, what
  to expect, and how fixes ship, with every security link in one place.
- [SUPPORT.md](SUPPORT.md) — the support lifecycle as a dated table (three
  minor lines, adjacent-skew upgrades) plus the export-control (ECCN)
  statement for procurement.
- [RELEASING.md](RELEASING.md) — what a release contains and the runbook that
  cuts one, including one-command verification of signatures, SBOMs, and
  reproducible builds.
- [CONTRIBUTING.md](CONTRIBUTING.md) — how to build and test, the review bar,
  and the repo conventions a change is held to.
- [CHANGELOG.md](CHANGELOG.md) — deliberately a pointer: GitHub Releases is
  the canonical changelog, and this file explains why.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — the community standard
  contributors and maintainers are held to.
- [LICENSE](LICENSE) — Apache-2.0, including the signed release binaries.

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
  publish and subscribe anywhere (logged as INSECURE at startup). And note the per-version
  answer to a denied publish: an **MQTT 5** publisher is told `0x87 Not authorized` on the
  PUBACK/PUBREC (issue #246), but a **v3.1.1** publisher is **still acknowledged** — the
  message is dropped and the plain ACK stands (3.1.1 has no negative PUBACK), so with a
  v3.1.1 fleet a misconfigured ACL looks like "missing data", not an error. The audit log
  is where every denial is visible, in both versions.
- [ ] **Check your fleet's TLS and certificates.** TLS 1.3 is the default (1.2 is a
  hardened opt-in), and client certificates **must** carry the `clientAuth` EKU
  (Extended Key Usage — an X.509 field naming what a certificate may be used for) or
  rustls rejects them — a trap for fleets minted against OpenSSL brokers. What that
  looks like and how to check a certificate:
  [TROUBLESHOOTING](docs/TROUBLESHOOTING.md#a-client-with-a-certificate-is-rejected-mtls).
- [ ] **Run ≥3 nodes for HA, never 2.** A two-node durable cluster has *worse* write
  availability than one node (write quorum is 2-of-2). Go from 1 to 3.

## Limitations

The gaps worth knowing before you evaluate this, stated here rather than left to
be found. Each is tracked; none is a silent surprise.


- **Memory has a watermark, not a ceiling.** `MQTTD_MEMORY_MAX_BYTES` puts the
  broker into brownout above it — growth writes refused; subscriber acks, reads,
  deletes, expiry and resumes continue, while a publisher's `QoS` ≥ 1 ack is
  refused, not granted — but nothing can stop RSS rising. The mark is sampled, not charged
  at each allocation, so RSS can overshoot it by `MQTTD_WATERMARK_POLL x the allocation rate`
  (default 10 s; 1 s once within 10% of the mark) and a burst inside one interval can still
  OOM. Keep the watermark at 75-85% of the container limit — that gap IS the overshoot
  allowance — and the container limit remains the hard bound. It needs
  `/proc` (Linux); elsewhere the broker logs that it is **not** enforcing rather than
  pretending. Underneath, one stalled subscriber holds **three**
  per-subscriber in-memory structures, and since issue
  [#241](https://github.com/mbilling/fss-mqtt-broker/issues/241) all three are
  operator-lowerable: the `QoS` 1/2 **flow-control backlog** (`MQTTD_MAX_BACKLOG_MESSAGES`,
  default 10 000, **plus** `MQTTD_MAX_BACKLOG_BYTES`, exact byte accounting, drop-oldest,
  counted `mqttd_publish_dropped_total{reason="backlog-overflow"}`); the **in-flight
  window** (`MQTTD_MAX_INFLIGHT_MESSAGES` — a ceiling on the effective outbound Receive
  Maximum, which otherwise defaults to **65 535** for every v3.1.1 client and any v5
  client that sends no property); and the **outbound socket channel**
  (`MQTTD_MAX_OUTBOUND_BYTES` alongside the fixed 10 000-packet cap, `QoS` 0 shed and
  counted as `mqttd_publish_dropped_total{reason="outbound-full"}`). With all three unset
  the exposure at the 1 MiB default packet size is `(65 535 + 10 000 + 10 000) x
  max_packet_size` ≈ **84 GiB** per stalled subscriber — the earlier "~10 GiB" counted the
  backlog alone. Two of the three bounds shed messages; the in-flight ceiling is a pure
  gate on the wire window: it drops nothing itself, though the surplus it holds back waits in
  the drop-oldest backlog, so it bounds RAM without being loss-free. The backlog byte bound makes the
  ack-and-drop arm below reachable *earlier*: at that bound already-acked entries are
  truncated and the publisher is not told. `mqttd_backlog_bytes_max` — the LARGEST single
  subscriber's backlog — is the number to size a per-subscriber cap against;
  `mqttd_backlog_bytes` sums every session and answers a different question (this node's
  total RAM in backlogs). Byte-capping the **durable** offline queue (disk) is still open — that is
  mosquitto's `max_queued_bytes`, our 0041-T6; disk stays bounded by
  `MQTTD_MAX_QUEUED_MESSAGES` (count) and the `MQTTD_STORE_MAX_BYTES` watermark. Full
  arithmetic and a bounded preset: [SIZING.md](docs/SIZING.md) (ADR 0041 T6, T10).
- **Disk is bounded in aggregate, not per store.** One store can consume the whole
  `MQTTD_STORE_MAX_BYTES` watermark and brown out the others. The broker now WARNs once,
  naming the store, above 70% of the mark (and `store_bytes{store}` is always exported),
  but there is no per-store *refusal* — deliberately: the resource is one filesystem, and
  `replicas.redb`/`lease.redb` grow from peers' committed appends and from consensus, with
  no client write to refuse. Selective refusal for `sessions`/`retained` is tracked
  (ADR 0041 T9). Relatedly, **a browned-out node keeps growing `replicas.redb`** for groups
  it merely follows — the refusal is decided at the session's owner — so headroom must cover
  peer-driven growth too. Disk-full itself fails closed and is crash-tested mid-write.
- **The Kubernetes operator is young.** It is packaged
  (ADR 0055 T8, issue #252): an install chart (`deploy/helm/mqttd-operator`,
  CRD included) and an operator image cut by the same signed/reproducible/SBOM
  release pipeline as the broker — first published at `v0.9.1`, riding the
  release train (the chart pins the current release).
  The **Helm chart remains the fully-supported no-operator path**; the
  `MqttdCluster` CRD is `v1alpha1`, schema-pinned in CI against the operator's
  own types, and — per the Kubernetes alpha-API convention — may change until
  promoted to `v1beta1`, a promotion act of its own, versioned independently
  of the broker's semver.
- **The horizontal scaling curve is unmeasured; the durable path itself now is,
  on one host.** [docs/benchmarks/DURABLE-PATH.md](docs/benchmarks/DURABLE-PATH.md)
  publishes end-to-end **acked** QoS 1/2 throughput and latency percentiles against
  a real 3-node quorum with the durable plane on, from a harness whose multi-host
  invocation is documented and parameterised. What that does **not** settle: it is
  one developer machine (three broker processes and the driver sharing 8 cores and
  **one** disk, loopback, no TLS), so it is dev-grade and is not a capacity claim;
  and throughput-vs-node-count is still absent on purpose, because a single-host
  curve scales *negatively* and would manufacture false evidence (ADR 0048 §2 and
  the [2026-07-14 post-mortem](docs/postmortems/2026-07-14-ha-bridge-durable-refused.md)).
  Treat **scaling** claims as design intent; treat the durable-path numbers as a
  floor measured under stated, unflattering conditions. The multi-host run needs
  hardware, and is the one thing standing between the two.
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
- **Migration tooling covers Mosquitto, EMQX and HiveMQ — and what it produces is a
  reviewed DRAFT, not a translated configuration.**
  `scripts/migrate/from-{mosquitto,emqx,hivemq}.py` translate the config and the ACL/RBAC
  policy, marking anything without an equivalent as `TODO(migrate)` in the output rather than
  dropping it silently, and each converter's output is put through `mqttd --check-config` and
  booted by a real broker in CI. **Every security-relevant value they write — every bind,
  every `[tls]` path, `client_ca`, `acl_file`, `password_file`, `allow_anonymous`, the ACL
  `default`, every bridge upstream — carries the input key it was derived from
  (`# from: listener 8883 0.0.0.0`), because the one gate that emits those lines refuses to
  write a live one without it.** Anything a converter could not derive comes out **commented
  out** beside a TODO naming the decision, so the worst case is a config **you** have to
  finish rather than a live setting nobody derived — that is what makes the output reviewable,
  and it is enforced by a provenance invariant over 138 generated inputs plus a fuzz pass over
  mutated ones ([the draft contract](docs/MIGRATION.md#what-a-converter-produces-a-draft-where-anything-undecidable-is-inert-and-named)).
  **Read the output before deploying it: none of the above makes it correct, only honest.** What
  the gate does NOT close is **misreading** — a value genuinely derived from a real input key
  whose MEANING the converter got wrong (a Mosquitto TLS-PSK listener converted to a plaintext
  bind, an anonymous-scoped ACL block emitted as a grant to everyone). Five such were found and
  fixed on 2026-08-15 and the class is open, so every construct known to be misread or unhandled
  is enumerated in [KNOWN GAPS](docs/MIGRATION.md#known-gaps-after-round-4) with what to check by
  hand. Three further limits:
  **(a)** the EMQX and HiveMQ converters were built from each vendor's own shipped example
  configuration at a pinned tag — **no live EMQX or HiveMQ broker was ever run** (and no live
  Mosquitto either: its mappings come from `mosquitto.conf(5)` @ `v2.0.22`), and **no claim of
  total coverage over any vendor's schema is made** — a construct a converter has never seen
  is one it cannot report, though it also cannot turn into a live setting; **(b)** only the
  Mosquitto converter has a Rust twin in `mqttui`, so the other two need `python3`;
  **(c)** **no session state migrates** — a moved
  client's offline queue, subscriptions and in-flight QoS 2 exchanges are lost and it must
  resubscribe. Retained state *does* cross, through the bridge — but that sync runs in
  **both** directions on every reconnect, so a retained value deleted while the bridge is
  down is **resurrected** from the other side (prune with the bridge running, then check
  both sides). The [migration guide](docs/MIGRATION.md) proves both halves and spells out
  the dual-run cutover that the missing session state forces. NanoMQ, VerneMQ, AWS IoT Core, Azure IoT Hub and
  everything else have **no** converter, and no partial one:
  [what ships and what the manual path costs](docs/MIGRATION.md#what-ships) prices that
  case honestly — the config is an hour, the ACL is the part that scales with your fleet.
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
  by **CRL** (a certificate-revocation *list* the operator publishes) —
  hot-reloadable, enforced on both the client listener and the cluster bus — with
  **OCSP** (the Online Certificate Status Protocol, revocation checked per
  handshake against a responder) **not yet supported**. **PSK** (pre-shared-key)
  **cipher suites** for constrained devices are not offered: X.509 or token
  (JWT/OIDC) authentication is the path today. Each is a planned fast-follow, not a design limit.
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
- **Stability:** **v1.0.0 is the compatibility freeze** — the policy of
  [ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md) is **in force**:
  semver defined at the wire/disk layer; **adjacent-release version skew** (a
  cluster may mix release N and N+1 — the state every rolling upgrade passes
  through, and the only mixed state supported and tested); sequential majors
  through a designated gateway minor; patches for the **three most recent minor
  lines**. A schema bump now ships its migration in the same PR or CI fails
  (ADR 0058), and the nightly two-binary roll proves the adjacent upgrade in
  both directions against the previous release. MQTT itself is unaffected:
  clients speak the published 3.1.1 / 5.0 specifications, which this policy
  does not touch.

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
| `mqttd-operator` | Kubernetes operator for the `MqttdCluster` CRD — installable via `deploy/helm/mqttd-operator` (image published per release from `v0.9.1`; the plain Helm chart remains the no-operator path) |
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
# Container image — fully-static musl binary on distroless/static (a base image
# with no shell or package manager), non-root, multi-arch (linux/amd64 +
# linux/arm64), nothing but the broker and a CA bundle:
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
# (plaintext + anonymous for a first look only — the secured quickstart below has
#  the TLS + mTLS + ACL version, including the same-shape hardened `docker run`)

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
Prefer a container? The [same posture as one `docker run`](#single-node-secured-in-a-container)
follows below, reusing the PKI and ACL minted here.

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

> **One behaviour to know before you rely on it:** what a *denied publish* is told
> depends on the protocol version. An **MQTT 5** publisher is refused visibly —
> PUBACK/PUBREC reason `0x87 Not authorized` (issue #246), the connection staying
> open. An **MQTT 3.1.1** publisher (this quickstart's clients) is dropped but
> still **acknowledged** — 3.1.1 has no negative PUBACK, and withholding the ack
> would leave a conforming publisher retrying forever — so it cannot tell that it
> was refused. In both versions the denial is recorded in the audit log as
> `acl.deny.publish`; for 3.1.1 that, not the client's return code, is where you
> see it. Denied *subscriptions* are refused visibly in both versions, with a
> per-filter reason code.

### Single node, secured, in a container

The same posture — TLS 1.3, mutual TLS, deny-by-default ACL, durable state on a
volume — as one hardened `docker run`, reusing the `pki/` and `acl.toml` minted
in steps 1–2 above. This is the container shape of the secured walkthrough; the
plaintext `docker run` in [Install](#install) is only ever the first look.

```sh
# The image runs as uid 65532 (nonroot), so the mounted material must be
# readable to it. Plain read permission is fine for THIS THROWAWAY quickstart
# PKI and nothing else — a real deployment mounts secrets with owned
# permissions (the compose, systemd and Helm packagings all do).
chmod 0644 pki/server.key acl.toml

docker run -d --name mqttd-secured \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v "$PWD/pki":/etc/mqttd/pki:ro -v "$PWD/acl.toml":/etc/mqttd/acl.toml:ro \
  -v mqttd-secured-data:/var/lib/mqttd -p 8883:8883 \
  -e MQTTD_TLS_BIND=0.0.0.0:8883 \
  -e MQTTD_TLS_CERT=/etc/mqttd/pki/server.crt \
  -e MQTTD_TLS_KEY=/etc/mqttd/pki/server.key \
  -e MQTTD_TLS_CLIENT_CA=/etc/mqttd/pki/ca.crt \
  -e MQTTD_ACL_FILE=/etc/mqttd/acl.toml \
  -e MQTTD_DATA_DIR=/var/lib/mqttd \
  ghcr.io/mbilling/fss-mqtt-broker:latest

# The same foreign client over mutual TLS, inside its grant:
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile pki/ca.crt \
  --cert pki/sensor-1.crt --key pki/sensor-1.key -t 'sensors/sensor-1/#' &
mosquitto_pub -h 127.0.0.1 -p 8883 --cafile pki/ca.crt \
  --cert pki/sensor-1.crt --key pki/sensor-1.key \
  -t 'sensors/sensor-1/temp' -m '21.5C'
```

No `MQTTD_PLAINTEXT_BIND`, no `MQTTD_ALLOW_ANONYMOUS`, and no ephemeral opt-in:
durable sessions land on the `mqttd-secured-data` volume, and this configuration
logs no `INSECURE` warning. The nightly image lane runs this invocation
(`scripts/image-smoke.sh`, also runnable as `mqttui --run image-smoke`) and
asserts the mTLS round-trip inside the grant, the refusal of a client with no
certificate at the TLS handshake, and the absence of any `INSECURE` log line.
For more than one node in containers, the [compose reference
deployment](#without-kubernetes-compose-systemd) is the shipped three-node
version of exactly this posture.

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
| `MQTTD_MAX_QUEUED_MESSAGES` | Per-session offline-queue cap (default `100000`). Bounds **disk** — the durable queue — not the in-memory backlog below |
| `MQTTD_MAX_BACKLOG_MESSAGES` | Messages one online subscriber's in-memory **flow-control backlog** holds before drop-oldest evicts (ADR 0012, 0041-T10; default `10000` = the former hard-coded `MAX_BACKLOG`; range `1..=10000000`, **`0` is refused** — the backlog must be bounded, so there is no "unbounded" setting). Bounds **RAM**, per online subscriber, per node — never disk. Worst case unset: `10 000 x (MQTTD_MAX_PACKET_SIZE + 256)` ≈ **10 GiB** at the 1 MiB default. Read at startup only (a reload reports `limits` as requires-restart) |
| `MQTTD_MAX_BACKLOG_BYTES` | The same backlog's **byte** bound, with exact accounting (issue #241; unset = **off**, i.e. exactly the pre-#241 behaviour; if set, at least `4096`). A message counts as `256 + topic + payload + forwarded MQTT 5 application-property bytes` — not payload-only (topics and user properties are publisher-controlled) and not the encoded packet (that is version- and subscriber-dependent). Worst case when set: `MQTTD_MAX_BACKLOG_BYTES + 2 x (MQTTD_MAX_PACKET_SIZE + 256)` — one entry may exceed the whole cap and is kept so delivery still progresses, plus one already-admitted re-parked entry. Drop-oldest at the bound **sheds already-acked messages without telling the publisher** (`mqttd_publish_dropped_total{reason="backlog-overflow"}`; the WARN names the bound); a value below `MQTTD_MAX_PACKET_SIZE` makes that routine and is warned at startup. Bounds **RAM**, never disk. Startup only |
| `MQTTD_MAX_OUTBOUND_BYTES` | Accounted bytes that may sit unwritten in one client's **outbound socket channel** before `QoS` 0 is shed (issue #241; unset = off, minimum `4096`). The fixed 10 000-**packet** cap applies either way, and only the at-most-once class is shed — control packets and `QoS` 1/2 always flow (`mqttd_publish_dropped_total{reason="outbound-full"}`). Worst case unset: `10 000 x MQTTD_MAX_PACKET_SIZE`. Bounds **RAM**. Startup only |
| `MQTTD_MAX_INFLIGHT_MESSAGES` | Ceiling on the **effective outbound** Receive Maximum (issue #241): the broker keeps at most `min(client Receive Maximum, this)` unacked `QoS` > 0 publishes per subscriber. Unset = the client's own value verbatim, i.e. **65 535** for every v3.1.1 client and any v5 client that sends no property — worst case `65 535 x MQTTD_MAX_PACKET_SIZE`. Range `1..=65535`. A pure **gate** on the wire window: it drops nothing itself, and the surplus waits in the backlog instead. **But that is not the same as loss-free** — the backlog is drop-oldest, so holding messages back into it can make `backlog-overflow` shedding of already-acked messages *more* likely, not less. Lower this to bound RAM and to slow a subscriber that legitimately keeps thousands in flight; pair it with a backlog bound sized for the lag you expect, and watch `publish_dropped{reason="backlog-overflow"}` rather than assuming zero. Distinct from `MQTTD_RECEIVE_MAXIMUM`, which is the **inbound** grant advertised to publishers. Bounds **RAM**. Startup only |
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
| `MQTTD_STORE_MAX_BYTES` | Disk watermark over the node's on-disk stores, total bytes (ADR 0041; needs `MQTTD_DATA_DIR`). Above it the broker **browns out**: writes that *grow* durable state (new retained topics, new sessions, offline enqueues) are refused with the quota behaviors, while subscriber acks, reads, deletes, expiry and resumes continue — a publisher's `QoS` ≥ 1 ack is refused, not granted (v5 `0x97`, v3.1.1 no ack + close, cross-node as a peer-bus verdict — an answered refusal; re-sending is the application's decision) — read-mostly, never the disk-full cliff; dropping back under restores writes. Session metadata (SUBSCRIBEs, the `QoS` 2 dedup window, detach spills) is exempt and still grows slowly, and a browned-out node keeps applying peers' committed appends into `replicas.redb` for groups it merely follows — set the mark with headroom (see SIZING). Scanned every `MQTTD_WATERMARK_POLL` seconds, so the total can overshoot the mark by one interval's growth. The mark is **aggregate** over the four stores: per-store sizes are always exported as the `store_bytes{store}` gauge, and the broker WARNs once, naming the store, when any single store passes 70% of the mark. Unset = no watermark |
| `MQTTD_MEMORY_MAX_BYTES` | **Memory watermark** over this process's RSS, bytes (ADR 0041 T8). Above it the broker **browns out** exactly as the disk watermark does — growth writes refused; subscriber acks, reads, deletes, expiry and resumes continue, while a publisher's `QoS` ≥ 1 ack is refused, not granted — and dropping back under restores growth. Brownout is active while **either** axis is over; `brownout{axis="memory"}` and `process_resident_bytes` say which. A **watermark, not a ceiling**: nothing here stops RSS rising — the mark is sampled, so RSS can overshoot it by `MQTTD_WATERMARK_POLL x the allocation rate` (plus the allocation in flight). Set it to 75-85% of `resources.limits.memory` and keep that container/cgroup limit as the hard bound (the Helm chart ships none — set it yourself). Needs `/proc` (Linux); elsewhere the broker logs at WARN that it is not enforcing, rather than pretending. Unset = off |
| `MQTTD_WATERMARK_POLL` | How often **both** watermark watchers (disk, memory) sample their axis, seconds (`[limits] watermark_poll_secs`, ADR 0041 T14). Default 10; range **1-300** — outside it is a startup error. Within 10% of a mark the watchers re-check every `poll / 10` with a 1 s floor, which also bounds how long a *cleared* brownout takes to lift. This is the detection-lag knob: overshoot above a mark is bounded by `poll x growth rate`, so lower it to pay for a tighter bound. Read at startup only (a reload reports `limits` as requires-restart) |
| `MQTTD_AUTH_TIMEOUT` | Per-round enhanced-auth reply timeout, seconds (ADR 0013; default `10`) |
| `MQTTD_DURABLE_SESSIONS` | Durable, consensus-backed replicated session store (ADR 0006/0007) — **on by default** (ADR 0029); set `0`/`false`/`off`/`no` for the lightweight in-memory store (an explicit choice: it needs no ephemeral opt-in). A node with no `MQTTD_SWIM_SEEDS` founds the lease group. On with no `MQTTD_DATA_DIR` → **REFUSED at startup** (issue #240) unless `MQTTD_ALLOW_EPHEMERAL_DURABILITY` is set |
| `MQTTD_DATA_DIR` | Directory for on-disk persistence (ADR 0018). With durable on (default) the lease group + replicated log are on-disk, surviving a full-cluster restart (recommended for production); **unset with durable on → REFUSED at startup** (issue #240) unless the ephemeral opt-in below is set. With durable off, unset is plain in-memory |
| `MQTTD_ALLOW_EPHEMERAL_DURABILITY` | **Dev/tests only** (issue #240): any non-empty value (presence = on) permits durable-on with **no** data dir — replicated state in MEMORY only, so a correlated quorum restart loses acked messages. Without it that combination refuses to start (and fails `--check-config` and a live reload), naming both remedies. Loudly `EPHEMERAL durability`-warned on every start while active |
| `MQTTD_BACKUP_DIR` | Where **online backups** are written (ADR 0062, issue #249). An export is taken from the LIVE node — nothing stops, no second store handle is opened — and is written `0600`, fsynced, then renamed. Must be on a volume **separate** from `MQTTD_DATA_DIR` (`--check-config` refuses otherwise: exports there grow the volume the disk watermark protects while being counted by nothing); `--check-config` validates the setting, not the volume, so a missing or unwritable directory fails at the first run instead. **Neither the Helm chart nor the systemd unit mounts one by default** — [OPERATIONS](docs/OPERATIONS.md#backup-and-disaster-recovery) has the opt-in for each. **A per-node export is not a cluster snapshot**: a cluster backup is the set of every node's export, and a restore from an incomplete set is refused, naming what is missing. Unset = no backups |
| `MQTTD_BACKUP_EVERY` | Seconds between scheduled exports (default `0` = on demand only, via `mqttd --backup` / `SIGUSR2` — which is a logged no-op, never a kill, on a node with no backup dir). This is the RPO's cadence term: `RPO ≤ every + W`, where `W` is the export's own window width, recorded in every file's trailer as `finished_unix_ms − started_unix_ms` and on `/statusz` as `backup.window_ms` (`mqttd_backup_duration_ms` is the whole run's wall clock — an upper bound on `W`). Alert on the age of `mqttd_backup_last_success_timestamp_seconds` (with its `> 0` guard) or the RPO is fiction |
| `MQTTD_BACKUP_KEEP` | Exports kept **per node id** before the oldest is deleted (default `7`), so a directory shared by several nodes cannot have one node's rotation delete another's. Several generations in one restore directory are fine: a restore reads the **newest export of each node** and logs the rest as superseded |
| `MQTTD_RESTORE_FROM` | A backup **file or directory** to import at startup, before any client listener binds (ADR 0062). Only into a **fresh** node — no store files in the data dir, or the node refuses. The whole set is verified first (format stamp, sha-256, one generation per node, a single `cluster_id`, coverage); each node then imports the sessions it owns and skips the rest, so put every node's export in one directory and set `MQTTD_READY_MIN_MEMBERS` to the node count so the import waits for the assembled ring. `/readyz` is NotReady with reason `restore-in-progress` while it runs; any failure exits non-zero. **Leave it set afterwards**: a completed restore writes a `restored-from` stamp, and a later start reads it, reports the setting INERT and boots normally on the data it already holds (a *different* source is refused — that would be a merge) |
| `MQTTD_RESTORE_TIMEOUT` | Seconds a restore waits for the durable plane to become ready before giving up (default `300`) |
| `MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS` | **Forfeits data — read [OPERATIONS](docs/OPERATIONS.md#backup-and-disaster-recovery) before setting it.** `1`/`true`/`on`/`yes` (nothing else, so a stray value cannot license a lossy restore; default off) lets a restore proceed from a set that does **not** cover the cluster, instead of refusing. It exists for the one disaster where a node's data *and* its export are both permanently gone, which would otherwise make the surviving nodes' backups unrestorable too. Every missing node and forfeited session is named at startup, in `/statusz`'s `restore.detail` (`PARTIAL (data forfeited): …`), and permanently in the `restored-from` stamp |
| `MQTTD_LEASE_VOTERS` | Bounded lease-consensus [voter](docs/GLOSSARY.md#mqttd-clustering-and-durability) set `N` (ADR 0021; default `5`, recommend odd). At most `N` members vote on lease ownership; every other member joins as a learner that still receives the lease log and can own/serve sessions — so consensus cost stays fixed (quorum `⌊N/2⌋+1`) as the cluster grows. `1` = no fault tolerance, `3` tolerates one voter loss, `5` two |
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
  valid session). The **gossip signing identity** — the same peer-bus leaf/key — swaps in
  the same reload (issue #269): the rotated leaf signs, and is embedded in, the next
  outgoing gossip datagram, and mixed old/new leaves coexist mid-rotation (verification is
  per-datagram against the CA). Rotating the cluster **CA itself** still needs a rolling
  restart.

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
Day-2 procedures — cert/key rotation, scaling, PVC lifecycle, founder recovery, and
**online backup + restore** (`mqttd --backup` on every node; a per-node export is not a
cluster snapshot, the backup volume is an opt-in the chart does not mount by default, and
the RPO/RTO are measured in
[`docs/benchmarks/BACKUP-RESTORE.md`](docs/benchmarks/BACKUP-RESTORE.md)) —
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

- **Per-pod PersistentVolume** (`volumeClaimTemplate`) for the
  [redb](https://github.com/cberner/redb) data dir (redb is the embedded,
  pure-Rust key-value store the broker persists into) — a rescheduled pod
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

There are 48 runnable scripts here — the demo stack, the Mosquitto/EMQX/HiveMQ converters
and the dual-run cutover smoke, the smoke and conformance suites, the Kubernetes end-to-end
runs, the benchmark harness and the multi-host scale-curve rig. `mqttui` is the
one place they are listed, explained and started ([ADR 0056](docs/adr/0056-mqttui.md), and
[The runnable map: mqttui](#the-runnable-map-mqttui) for installing it):

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
never runs `docker compose up` cannot tell you the file works — twice: once against an
image built from this repository, and once resolving `compose.yaml`'s **pinned default
tag** with no override, the exact path a reader takes (issue #263: the default used to be
a floating `:latest` that no lane exercised, and it drifted behind the artifacts). A
per-PR gate (`scripts/check-deploy-image-pin.sh`) additionally proves every mqttd flag
the compose artifacts use exists in the binary at the pinned tag.

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

In force since **v1.0.0** ([ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md);
the pre-release freeze regime of [ADR 0038](docs/adr/0038-prerelease-compatibility-freeze.md)
— formats change freely, wipe-and-rejoin on schema bumps — ended at that tag):

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

**End-to-end, with the durable plane on**, is a separate and much harder number, and it is
published in [docs/benchmarks/DURABLE-PATH.md](docs/benchmarks/DURABLE-PATH.md): acked
QoS 1/QoS 2 throughput and p50/p95/p99 latency against a real 3-node quorum, measured
through the production binary. Read its first paragraph before its tables. In one line:
on the machine it was run on, an acked durable QoS 1 publish costs ~28 ms at p50 while
the same publish to a **clean** session costs ~0.03 ms — the price of the guarantee — and
the durable rate is pinned by that host's per-volume disk barrier, not by the broker's
CPU. It is **single-host and dev-grade**: three broker processes and the load driver on
8 cores and one disk.

**The multi-host scaling curve** (ADR 0048 §2 — the same workload against real 1-, 3- and
5-node clusters, one dedicated host and one disk per broker) is published in
[docs/benchmarks/SCALE-CURVE.md](docs/benchmarks/SCALE-CURVE.md), measured against the
signed `v1.0.1` release. In one line each: `$share` fan-out scaled ~4.6× from one node to
five with the p99 bound *tightening* (100→25 ms) and every rung driver-limited (floors,
not capacities); durable QoS 1 runs at ~2.0k acked msg/s on one node (p99 0.82 ms
uncontended) and ~0.6k across a 3-node quorum — the measured price of ack-after-quorum,
with ownership capacity bounded by the voter cap, not node count; 50k connections cost a
flat 19.3 KiB each at every size. The curve publishes its own defects: it caught, fixed
and re-measured #358 (v1.0.0's durable stall in production topology) and isolated the
still-open 5-node formation instability (#368).

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
