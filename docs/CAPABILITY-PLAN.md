# MQTT Broker — Capability Plan (v0.1)

**Codename:** TBD (working name: `mqttd`)
**Mission:** The most cyber-secure MQTT broker available, built cluster-native for
linear horizontal scalability, with a 100% open feature set. Revenue comes from
support, SLAs, certified builds, and managed hosting — never from gated features.

---

## 1. Guiding principles

1. **Security is the product.** Every design decision defaults to the most secure
   option. Insecure modes (plaintext, anonymous) must be *explicitly* enabled and
   are loudly logged.
2. **Open == Enterprise.** One codebase, Apache-2.0. No feature flags that gate
   functionality behind a paid tier. If it ships, everyone gets it.
3. **Linear scalability.** Shared-nothing nodes. Adding a node adds throughput.
   No single coordinator on the hot path.
4. **Memory safety end-to-end.** Rust core, `#![forbid(unsafe_code)]` in every
   crate that can afford it; any `unsafe` is isolated, justified, and tested.
5. **Standards-correct.** Full MQTT 3.1.1 (OASIS) and MQTT 5.0 (OASIS) compliance,
   validated against an automated conformance suite.
6. **Operable.** Metrics, structured logs, tamper-evident audit trail, and a
   tamper-resistant config story from day one.

---

## 2. Protocol support

### MQTT 3.1.1
- CONNECT/CONNACK with full flag validation
- PUBLISH QoS 0/1/2 with correct dup/retain semantics
- SUBSCRIBE/UNSUBSCRIBE, wildcard topic filters (`+`, `#`)
- PINGREQ/PINGRESP, keepalive enforcement
- Will (LWT) messages
- Retained messages
- Clean session semantics

### MQTT 5.0 (superset)
- Properties on every packet (user properties, content-type, etc.)
- Reason codes & reason strings on all acks
- Session expiry & message expiry intervals
- Topic aliases
- Shared subscriptions (`$share/<group>/<filter>`) — **load-balancing primitive,
  also key to linear scale**
- Flow control: Receive Maximum, Maximum Packet Size
- Request/Response pattern (response topic, correlation data)
- Server-side disconnect with reason code
- Enhanced authentication (AUTH packet, SASL-style challenge/response)
- Subscription identifiers
- Will delay interval

---

## 3. Security capabilities (the headline)

### Transport
- **TLS 1.3 by default** (TLS 1.2 opt-in only), pure-Rust `rustls` — no OpenSSL CVE surface
- **mTLS**: client-certificate authentication, configurable CA chains, CRL/OCSP stapling
- WebSocket-over-TLS (`wss`) for browser/edge clients
- Optional plaintext listener — disabled by default, requires explicit `--insecure-allow-plaintext`

### Authentication (pluggable, all open)
- mTLS client certs (identity from cert subject/SAN)
- Username/password with **Argon2id** hashing (no plaintext, no fast hashes)
- JWT / OIDC bearer tokens
- MQTT 5 enhanced auth (challenge/response) — extensible SASL mechanisms
- Pluggable `Authenticator` trait for external IdPs (LDAP, custom)

### Authorization
- Topic-level ACLs (publish/subscribe/both), allow + deny rules, wildcards
- Per-identity, per-group, and per-topic-pattern policies
- Deny-by-default option
- Hot-reloadable policy without dropping connections

### Hardening & supply chain
- `#![forbid(unsafe_code)]` where possible; audited `unsafe` elsewhere
- `cargo-deny` (license + advisory + ban checks) and `cargo-audit` in CI, gating merges
- Reproducible builds; signed releases (sigstore/cosign), SBOM (CycloneDX) per release
- Fuzzing of the wire codec (`cargo-fuzz`) — the #1 untrusted-input surface
- Rate limiting, connection caps, max packet size, slow-loris protection
- Per-client and per-listener quotas (in-flight, message rate, bandwidth)
- Tamper-evident **audit log** (hash-chained) of auth events and admin actions
- Secrets never logged; config secrets via env/secret-manager refs, not inline

---

## 4. Scalability architecture (cluster-native from day one)

```
        ┌──────────┐   ┌──────────┐   ┌──────────┐
client─▶│  node A  │◀─▶│  node B  │◀─▶│  node C  │◀─client
        └────┬─────┘   └────┬─────┘   └────┬─────┘
             └── cluster bus (gossip + routing) ──┘
        shared-nothing: any client to any node
```

- **Shared-nothing nodes.** A client connects to *any* node. No node holds global
  state required by another node's hot path.
- **Membership:** SWIM-style gossip (failure detection, anti-entropy) — no central
  registry, no fixed coordinator.
- **Subscription routing:** each node owns its local subscribers; a compact,
  gossiped **subscription digest** (bloom/interest map) tells nodes which peers
  *might* have subscribers for a topic, so PUBLISH fans out only where needed.
- **Sessions:** persistent sessions pinned to a node with a pluggable
  `SessionStore`; takeover/migration on node loss.
- **Retained & will state:** replicated via the cluster bus; consensus
  (Raft) reserved only for the small set of state that truly needs it.
- **Shared subscriptions** distribute a topic's load across group members —
  the in-protocol lever for linear consumer scaling.
- **No hot-path coordinator** ⇒ throughput grows ~linearly with node count.

---

## 5. Crate / module layout

| Crate | Responsibility |
|---|---|
| `mqtt-codec` | MQTT 3.1.1 + 5.0 wire encode/decode. Zero-trust parsing, fuzzed. |
| `mqtt-core` | Session state, QoS state machines, subscription tree, retained store. |
| `mqtt-net` | Listeners (TCP/TLS/WebSocket), connection lifecycle, backpressure. |
| `mqtt-auth` | `Authenticator` + `Authorizer` traits and built-in providers. |
| `mqtt-storage` | Pluggable persistence traits (`SessionStore`, `RetainedStore`). |
| `mqtt-cluster` | Gossip membership, subscription digests, cross-node routing. |
| `mqtt-observability` | Metrics (Prometheus), tracing, hash-chained audit log. |
| `mqtt-config` | Typed config load + validation; secure defaults. |
| `mqttd` | The server binary wiring everything together. |

All cross-cutting state (sessions, retained, subs, cluster transport) sits behind
**traits** so single-node, embedded, and distributed backends are swappable.

---

## 6. Phased roadmap

The original Phase 0–5 milestone plan (infrastructure → single-node core → security
depth → cluster → persistence & ops → compliance & hardening) has been delivered through
the **cluster + persistence** stage: the MQTT 3.1.1 + 5.0 core, the security depth, the
cluster (SWIM, routing, shared subscriptions, takeover, retained replication), and on-disk
persistence are all built. The remaining frontier is compliance & hardening (a formal
conformance suite, continuous fuzzing, signed/reproducible releases, scale benchmarking)
plus operational surface (metrics — [ADR 0020](adr/0020-metrics-and-observability.md), a
WebSocket listener, an admin API).

This roadmap is no longer tracked here. Live, per-decision build status is the
[**delivery dashboard**](delivery/STATUS.md), derived from the delivery docs under
[`docs/delivery/`](delivery/); the decisions themselves are in [`docs/adr/`](adr/).

---

## 7. Business model (informational)

- **License:** Apache-2.0. Every feature open, no enterprise edition.
- **Paid:** support contracts, SLAs, security response, certified/signed builds,
  training, and managed hosting. The software is free; *assurance* is the product.

---

## 8. Open questions for later

- Project name & branding
- Default durable storage engine (embedded KV vs. external)
- Consensus library choice for the small Raft-needing slice
- Bridge/federation to other brokers
- Multi-tenancy isolation model
