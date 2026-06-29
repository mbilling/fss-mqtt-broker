---
adr: "0002"
title: "Transport security: TLS 1.3 everywhere, mTLS on the cluster bus"
adr_status: Accepted
tasks:
  - id: 0002-T1
    title: rustls 0.23 with the ring provider (no aws-lc-rs / OpenSSL surface)
    status: done
    date: 2026-06-11
    evidence: Cargo.toml rustls 0.23 default-features=false features=["ring"]; Cargo.lock rustls 0.23.40 / ring 0.17.14
  - id: 0002-T2
    title: TLS 1.3 only, no protocol-version configuration surface
    status: done
    date: 2026-06-11
    evidence: tls.rs TLS_VERSIONS = &[&rustls::version::TLS13]; no TLS12 in tree
  - id: 0002-T3
    title: Single mqtt_net::tls module (PEM load, acceptor/connector, cert verify) with no skip-verification path; tests mint real CAs via rcgen
    status: done
    date: 2026-06-11
    evidence: mqtt-net/src/tls.rs server_acceptor/client_connector; missing_or_empty_material_fails_loudly; builds_mtls_client_connector (rcgen mint_pki, dev-dependency)
  - id: 0002-T4
    title: Client listener TLS server with optional per-listener client certs; require_client_cert default true
    status: done
    date: 2026-06-11
    evidence: mqtt-config require_client_cert default true (defaults_are_secure); mtls_listener_rejects_clients_without_certificates
  - id: 0002-T5
    title: Cluster bus mutual TLS against a dedicated cluster CA, trust roots separate from the client CA
    status: done
    date: 2026-06-11
    evidence: peer.rs PeerTls{acceptor,connector}; publish_routes_across_mtls_peer_links; plaintext_peer_is_rejected_by_mtls_listener
  - id: 0002-T6
    title: Node-id <-> certificate-CN binding (deferred to ADR 0004 step 5)
    status: done
    date: 2026-06-22
    evidence: peer.rs CN-vs-Hello-id check; cert_cn_mismatch_with_hello_node_id_is_rejected (realized by ADR 0004)
  - id: 0002-T7
    title: SWIM gossip-plane authentication (deferred to ADR 0003)
    status: done
    date: 2026-06-11
    evidence: swim_auth.rs HMAC-SHA256 seal/open; keyed_cluster_ignores_nodes_without_the_key (realized by ADR 0003)
  - id: 0002-T8
    title: CRL / OCSP stapling
    status: deferred
    notes: no revocation checking in tree (rg crl|ocsp|revocation -> none); pairs with hot-reloadable policy, Capability Plan §3
  - id: 0002-T9
    title: Certificate rotation / hot-reload without dropping connections
    status: deferred
    notes: TLS contexts built once at startup; no reload path exists; unblocks with hot-reloadable policy work
  - id: 0002-T10
    title: WebSocket-over-TLS listener
    status: done
    date: 2026-06-29
    evidence: "Delivered with the native WebSocket transport (ADR 0035). main.rs serve_wss_clients (gated on MQTTD_WSS_BIND) terminates TLS first with the reloadable ADR 0002 acceptor — so the mTLS client-cert CN identity is extracted exactly as for a TCP TLS client (ADR 0004) — then runs the WebSocket handshake over the TLS stream; a SIGHUP cert reload is picked up on the next handshake. tests/ws.rs wss_mtls_pubsub_roundtrip proves a pub/sub round-trip over wss:// with mutual TLS."
---

# Delivery — ADR 0002: Transport security: TLS 1.3 everywhere, mTLS on the cluster bus

Decision: [docs/adr/0002-transport-security.md](../adr/0002-transport-security.md).

## Plan

The single-shot decision decomposes into the five transport-security choices plus the
deferred items the ADR explicitly tracks. Each carries a stable id used by commits,
tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0002-T1** rustls/ring | The TLS stack is `rustls` 0.23 with the `ring` provider compiled in (`aws-lc-rs` default excluded), so there is no OpenSSL CVE surface and the build needs no cmake/NASM. |
| **0002-T2** TLS 1.3 only | The acceptor and connector negotiate TLS 1.3 exclusively; no TLS 1.2 and no protocol-version configuration field exist. |
| **0002-T3** One TLS module | All PEM loading, acceptor/connector construction, and client-cert verification live in `mqtt_net::tls`; there is no "skip verification"/"accept any certificate" path, and tests mint real throwaway CAs with `rcgen` (dev-dependency). |
| **0002-T4** Client listener | The client listener is a TLS server; `require_client_cert` (mTLS) is configurable per listener and defaults to `true`; a client without a cert is rejected when required. |
| **0002-T5** Cluster bus mTLS | Peer links authenticate both directions against a dedicated cluster CA (listener requires a client cert, dialer verifies the server cert); the cluster trust root is separate from the client-facing one, and a plaintext peer is rejected. |
| **0002-T6** Node-id ↔ cert CN | An admitted node may no longer claim an arbitrary id: the peer node id is bound to the cluster cert's Common Name. (Realized by ADR 0004 step 5.) |
| **0002-T7** Gossip auth | SWIM gossip datagrams are authenticated. (Realized by ADR 0003.) |
| **0002-T8** CRL/OCSP | Certificate revocation is checked via CRL or OCSP stapling. |
| **0002-T9** Cert rotation | TLS certificates can be rotated/reloaded without dropping live connections. |
| **0002-T10** WebSocket-over-TLS | A WebSocket-over-TLS listener accepts MQTT-over-WS clients. |

## Progress

<!-- status-table:0002 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0002-T1 | ✅ done | 2026-06-11 | Cargo.toml rustls 0.23 default-features=false features=["ring"]; Cargo.lock rustls 0.23.40 / ring 0.17.14 |
| 0002-T2 | ✅ done | 2026-06-11 | tls.rs TLS_VERSIONS = &[&rustls::version::TLS13]; no TLS12 in tree |
| 0002-T3 | ✅ done | 2026-06-11 | mqtt-net/src/tls.rs server_acceptor/client_connector; missing_or_empty_material_fails_loudly; builds_mtls_client_connector (rcgen mint_pki, dev-dependency) |
| 0002-T4 | ✅ done | 2026-06-11 | mqtt-config require_client_cert default true (defaults_are_secure); mtls_listener_rejects_clients_without_certificates |
| 0002-T5 | ✅ done | 2026-06-11 | peer.rs PeerTls{acceptor,connector}; publish_routes_across_mtls_peer_links; plaintext_peer_is_rejected_by_mtls_listener |
| 0002-T6 | ✅ done | 2026-06-22 | peer.rs CN-vs-Hello-id check; cert_cn_mismatch_with_hello_node_id_is_rejected (realized by ADR 0004) |
| 0002-T7 | ✅ done | 2026-06-11 | swim_auth.rs HMAC-SHA256 seal/open; keyed_cluster_ignores_nodes_without_the_key (realized by ADR 0003) |
| 0002-T8 | 💤 deferred | — | no revocation checking in tree (rg crl|ocsp|revocation -> none); pairs with hot-reloadable policy, Capability Plan §3 |
| 0002-T9 | 💤 deferred | — | TLS contexts built once at startup; no reload path exists; unblocks with hot-reloadable policy work |
| 0002-T10 | ✅ done | 2026-06-29 | "Delivered with the native WebSocket transport (ADR 0035). main.rs serve_wss_clients (gated on MQTTD_WSS_BIND) terminates TLS first with the reloadable ADR 0002 acceptor — so the mTLS client-cert CN identity is extracted exactly as for a TCP TLS client (ADR 0004) — then runs the WebSocket handshake over the TLS stream; a SIGHUP cert reload is picked up on the next handshake. tests/ws.rs wss_mtls_pubsub_roundtrip proves a pub/sub round-trip over wss:// with mutual TLS." |
<!-- /status-table:0002 -->

## Changelog

- **2026-06-29** — T10 (WebSocket-over-TLS) reconciled to **done**: it was delivered by the
  native WebSocket transport (ADR 0035) — `serve_wss_clients` (TLS-first via the reloadable
  acceptor, then the WS upgrade, mTLS CN as identity), proven by `wss_mtls_pubsub_roundtrip` —
  but this ADR's frontmatter still read "no listener/upgrade path exists". Status corrected.
- **2026-06-22** — T6 node-id ↔ certificate-CN binding landed via ADR 0004 step 5
  (`peer.rs` CN-vs-Hello check), closing a deferred item from this ADR.
- **2026-06-11** — Core transport security landed: rustls/ring (T1), TLS 1.3 only (T2),
  the single `mqtt_net::tls` module with no insecure verifier (T3), the client listener
  with `require_client_cert` defaulting true (T4), and cluster-bus mTLS against a
  dedicated cluster CA with separate trust roots (T5). SWIM gossip authentication (T7)
  landed concurrently via ADR 0003. CRL/OCSP, cert hot-reload, and WebSocket-over-TLS
  recorded as deliberate deferrals.
