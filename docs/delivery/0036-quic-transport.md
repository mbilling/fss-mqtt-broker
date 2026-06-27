---
adr: "0036"
title: MQTT-over-QUIC transport (multi-stream)
adr_status: Accepted
tasks:
  - id: 0036-T1
    title: QUIC endpoint in mqtt-net — build a quinn Endpoint from cert/key/client-CA (rustls 0.23 ServerConfig, ring, TLS 1.3, ALPN `mqtt`, mTLS client-cert verify); quinn as a direct dep on ring (no aws-lc)
    status: done
    date: 2026-06-27
    evidence: "mqtt-net::quic::server_endpoint reuses tls::server_config (refactored out of server_acceptor) + ALPN `mqtt` + max_early_data_size=0, via quinn::crypto::rustls::QuicServerConfig. quinn = 0.11, default-features=false, features=[runtime-tokio, rustls-ring, log] (no aws-lc). Transport::Quic added."
  - id: 0036-T2
    title: QUIC bidi-stream byte adapter — join quinn SendStream+RecvStream into one AsyncRead+AsyncWrite so the unchanged handle_stream<S> runs over it
    status: done
    date: 2026-06-27
    evidence: "mqtt-net::quic::byte_stream = tokio::io::join(recv, send) (quinn streams are tokio AsyncRead/AsyncWrite); peer_leaf_cert extracts the mTLS leaf from conn.peer_identity(). handle_stream<S> unchanged."
  - id: 0036-T3
    title: Control-stream listener — MQTTD_QUIC_BIND (UDP); accept connection, extract the mTLS leaf-CN identity from the peer cert, accept the first bidi stream, run the MQTT session over it via conn::handle_stream
    status: done
    date: 2026-06-27
    evidence: "main.rs serve_quic_clients: endpoint.accept() per connection, identity = mtls::identity_from_cert(peer_leaf_cert), accept_bi() control stream -> byte_stream -> handle_stream. MQTTD_QUIC_BIND parsed as a UDP SocketAddr; reuses MQTTD_TLS_CERT/KEY/CLIENT_CA; graceful close on shutdown."
  - id: 0036-T4
    title: End-to-end test — a real quinn client opens a QUIC connection (ALPN mqtt, client cert) + a bidi stream and completes a pub/sub round-trip; the CN becomes the session identity
    status: done
    date: 2026-06-27
    evidence: "tests/quic.rs (2 pass): quic_mtls_pubsub_roundtrip (real quinn client, ALPN mqtt, client cert, control stream, pub->sub delivery) and quic_without_client_cert_is_refused (mTLS enforced — a certless client never gets a CONNACK). Client reuses byte_stream + FrameReader/FrameWriter."
  - id: 0036-T5
    title: Multi-stream demux — accept additional bidi data streams and feed their PUBLISH packets into the SAME session; route outbound PUBLISH across streams (no cross-stream head-of-line blocking). The connection-model generalisation (one session, N streams), built on T1–T4
    status: done
    date: 2026-06-27
    evidence: "FrameReader::next_raw_frame (read one complete packet's raw bytes, version-agnostic; unit-tested). mqtt-net::quic::QuicMux + accept_mux: per-stream forwarder tasks read complete frames and merge them (never byte-interleaved) into one inbound stream via an mpsc channel; the control stream's send half carries all outbound; Drop closes the connection. serve_quic_clients uses accept_mux. Outbound multi-stream is a noted later enhancement (v1 writes on the control stream)."
  - id: 0036-T6
    title: Multi-stream test — two data streams carry independent PUBLISH flows into one session; a stalled/large publish on one stream does not block delivery on the other
    status: done
    date: 2026-06-27
    evidence: "quic::quic_multistream_demux_no_head_of_line_blocking: one publisher opens two QUIC data streams — an INCOMPLETE large publish on one and a complete small publish on the other; the complete one is delivered to a subscriber while the other is still mid-frame (no HoL blocking), then completing the large frame delivers it too (intact, 100 KB). Flake-checked 3x."
  - id: 0036-T7
    title: Docs — README transports + MQTTD_QUIC_BIND; note non-standard (EMQX-style), no 0-RTT for CONNECT, peer bus stays mTLS/TCP
    status: done
    date: 2026-06-27
    evidence: "README: MQTTD_QUIC_BIND row + the Security transport bullet (control stream today, multi-stream in progress; non-standard; no 0-RTT for CONNECT)."
  - id: 0036-T8
    title: Demo wiring — quic-certs one-shot PKI + MQTTD_QUIC_BIND on every node + a quic-demo client publishing over QUIC data streams; browser "+ QUIC demo feed" + Grafana accepts-by-listener show it
    status: done
    date: 2026-06-27
    evidence: "demo/quic/gen-certs.sh + quic-certs init service mint a throwaway PKI; the broker env enables MQTTD_QUIC_BIND on all nodes; crates/mqttd/examples/quic_demo.rs (built into the image) connects over QUIC (mTLS) and publishes across 3 data streams to quic/demo/stream{N}. Verified live: a plaintext subscriber sees the QUIC-originated ticks; mqttd_accepts_total{listener=quic} increments. Playground gained a '+ QUIC demo feed' button."
  - id: 0036-T9
    title: Outbound multi-stream fan-out — broker→client PUBLISH fans across broker-opened QUIC data streams (symmetric mux), capability-negotiated (a single-control-stream client is never stranded) and topic-affinity routed (per-topic order preserved); control + acks stay on the control stream
    status: done
    date: 2026-06-27
    evidence: "QuicMux outbound_writer task routes each outbound packet: PUBLISH → topic-hashed (FNV-1a, publish_topic + topic_slot, unit-tested) data stream from a fixed OUTBOUND_POOL of broker-opened streams; everything else (CONNACK, SUBACK, QoS acks, PINGRESP) stays on the control stream. Capability is observed, not advertised: the broker fans out only once the client has opened ≥1 data stream (capable flag set by the accept-side forwarder), so a single-control-stream client is never stranded — strictly additive. Symmetric mux: accept_mux (server) + connect_mux (client) both build via build_mux. tests/quic.rs quic_outbound_fans_publishes_across_streams: a connect_mux subscriber signals capability post-CONNECT, then receives all 6 publishes fanned across the pool; the 3 pre-existing control-stream tests still pass (backward-compatible)."
  - id: 0036-T10
    title: Connection-migration validation — prove a client path change (rebind) is carried on the SAME QUIC connection (no reconnect/handshake), observe it broker-side, and demo it
    status: done
    date: 2026-06-28
    evidence: "tests/quic.rs quic_connection_migration_survives_path_change: rebinds the publisher's endpoint to a fresh UDP socket (new source address on the same connection) and asserts the session survived without re-establishing — Connection::stable_id() unchanged, local addr moved, and BOTH directions work on the new path (forward PUBLISH reaches the subscriber; a QoS-1 PUBLISH gets its PUBACK back). Broker-side: spawn_quic_migration_watch (main.rs) watches Connection::remote_address(); on change it logs from→to for the identity and bumps mqttd_quic_path_migrations_total (new metric + Grafana panel). Demo: quic_demo --migrate (QUIC_MIGRATE_MS=10000 in compose) rebinds every 10s so the counter ticks and the broker logs migrations while the quic/demo/* feed keeps flowing. quinn default allows migration (no disable_active_migration set). ADR §3b records the evidence model."
  - id: 0036-T11
    title: Follow-on — 1-RTT resumption tuning (ticket lifetime / resumption policy under mTLS-on-every-connection)
    status: deferred
    notes: 1-RTT session resumption is quinn/rustls-provided and replay-safe (0-RTT stays disabled, T1); explicit ticket-lifetime/policy tuning is a follow-on, separate from migration. Distinct from migration — resumption is a NEW connection reusing crypto, not a live connection surviving a path change.
---

# Delivery — ADR 0036: MQTT-over-QUIC transport (multi-stream)

Decision: [docs/adr/0036-quic-transport.md](../adr/0036-quic-transport.md).

Native MQTT-over-QUIC via `quinn` (already in the tree on our rustls 0.23 + ring): mandatory
TLS 1.3 + mTLS (identity = leaf CN, as for TCP-TLS/WSS), ALPN `mqtt`, reusing the
`handle_stream<S>` seam. Built in the only order multi-stream allows — the **control stream**
(a complete, interoperable, mTLS single-stream MQTT-over-QUIC) first, then the **data-stream
demux** (one session, many streams; QUIC's no-head-of-line-blocking benefit) layered on it.
MQTT-over-QUIC is non-standard (EMQX the de-facto reference), so interop is limited to clients
that speak it; this is built test-first and staged.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0036-T1** QUIC endpoint | `quinn` endpoint built from cert/key/client-CA: rustls 0.23, ring, TLS 1.3, ALPN `mqtt`, mTLS verify. quinn pinned to ring (no aws-lc). |
| **0036-T2** Stream adapter | `quinn` `SendStream`+`RecvStream` joined into one `AsyncRead + AsyncWrite`; `handle_stream<S>` unchanged. |
| **0036-T3** Control-stream listener | `MQTTD_QUIC_BIND` (UDP); per connection: extract leaf-CN identity, accept the first bidi stream, run the MQTT session over it. |
| **0036-T4** E2E test | A real `quinn` client (ALPN `mqtt`, client cert) opens a bidi stream and round-trips a pub/sub; its CN is the session identity. |
| **0036-T5** Multi-stream demux | Additional bidi data streams feed PUBLISH into the *same* session; outbound PUBLISH routed across streams. The one-session-many-streams generalisation, on the T1–T4 base. |
| **0036-T6** Multi-stream test | Two data streams carry independent flows into one session; a stalled publish on one does not block the other. |
| **0036-T7** Docs | README + `MQTTD_QUIC_BIND`; non-standard note, no 0-RTT for CONNECT, peer bus stays mTLS/TCP. |
| **0036-T8** Demo wiring | One-shot PKI + `MQTTD_QUIC_BIND` on every node + a `quic_demo` client publishing across data streams; visible in the playground (`quic/demo/#`) and Grafana (accepts-by-listener). |
| **0036-T9** Outbound fan-out | Broker→client PUBLISH fans across broker-opened data streams (symmetric mux), capability-negotiated (single-control-stream clients never stranded), topic-affinity routed; control + acks stay on the control stream. |
| **0036-T10** Migration validation | A client path change (endpoint rebind) is carried on the *same* QUIC connection — `stable_id` unchanged, both directions work on the new path — observed broker-side (`mqttd_quic_path_migrations_total` + log) and demoed (`quic_demo --migrate`). |
| **0036-T11** Follow-on | *(deferred)* 1-RTT resumption tuning (ticket lifetime / policy). Distinct from migration. |

## Progress

<!-- status-table:0036 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0036-T1 | ✅ done | 2026-06-27 | "mqtt-net::quic::server_endpoint reuses tls::server_config (refactored out of server_acceptor) + ALPN `mqtt` + max_early_data_size=0, via quinn::crypto::rustls::QuicServerConfig. quinn = 0.11, default-features=false, features=[runtime-tokio, rustls-ring, log] (no aws-lc). Transport::Quic added." |
| 0036-T2 | ✅ done | 2026-06-27 | "mqtt-net::quic::byte_stream = tokio::io::join(recv, send) (quinn streams are tokio AsyncRead/AsyncWrite); peer_leaf_cert extracts the mTLS leaf from conn.peer_identity(). handle_stream<S> unchanged." |
| 0036-T3 | ✅ done | 2026-06-27 | "main.rs serve_quic_clients: endpoint.accept() per connection, identity = mtls::identity_from_cert(peer_leaf_cert), accept_bi() control stream -> byte_stream -> handle_stream. MQTTD_QUIC_BIND parsed as a UDP SocketAddr; reuses MQTTD_TLS_CERT/KEY/CLIENT_CA; graceful close on shutdown." |
| 0036-T4 | ✅ done | 2026-06-27 | "tests/quic.rs (2 pass): quic_mtls_pubsub_roundtrip (real quinn client, ALPN mqtt, client cert, control stream, pub->sub delivery) and quic_without_client_cert_is_refused (mTLS enforced — a certless client never gets a CONNACK). Client reuses byte_stream + FrameReader/FrameWriter." |
| 0036-T5 | ✅ done | 2026-06-27 | "FrameReader::next_raw_frame (read one complete packet's raw bytes, version-agnostic; unit-tested). mqtt-net::quic::QuicMux + accept_mux: per-stream forwarder tasks read complete frames and merge them (never byte-interleaved) into one inbound stream via an mpsc channel; the control stream's send half carries all outbound; Drop closes the connection. serve_quic_clients uses accept_mux. Outbound multi-stream is a noted later enhancement (v1 writes on the control stream)." |
| 0036-T6 | ✅ done | 2026-06-27 | "quic::quic_multistream_demux_no_head_of_line_blocking: one publisher opens two QUIC data streams — an INCOMPLETE large publish on one and a complete small publish on the other; the complete one is delivered to a subscriber while the other is still mid-frame (no HoL blocking), then completing the large frame delivers it too (intact, 100 KB). Flake-checked 3x." |
| 0036-T7 | ✅ done | 2026-06-27 | "README: MQTTD_QUIC_BIND row + the Security transport bullet (control stream today, multi-stream in progress; non-standard; no 0-RTT for CONNECT)." |
| 0036-T8 | ✅ done | 2026-06-27 | "demo/quic/gen-certs.sh + quic-certs init service mint a throwaway PKI; the broker env enables MQTTD_QUIC_BIND on all nodes; crates/mqttd/examples/quic_demo.rs (built into the image) connects over QUIC (mTLS) and publishes across 3 data streams to quic/demo/stream{N}. Verified live: a plaintext subscriber sees the QUIC-originated ticks; mqttd_accepts_total{listener=quic} increments. Playground gained a '+ QUIC demo feed' button." |
| 0036-T9 | ✅ done | 2026-06-27 | "QuicMux outbound_writer task routes each outbound packet: PUBLISH → topic-hashed (FNV-1a, publish_topic + topic_slot, unit-tested) data stream from a fixed OUTBOUND_POOL of broker-opened streams; everything else (CONNACK, SUBACK, QoS acks, PINGRESP) stays on the control stream. Capability is observed, not advertised: the broker fans out only once the client has opened ≥1 data stream (capable flag set by the accept-side forwarder), so a single-control-stream client is never stranded — strictly additive. Symmetric mux: accept_mux (server) + connect_mux (client) both build via build_mux. tests/quic.rs quic_outbound_fans_publishes_across_streams: a connect_mux subscriber signals capability post-CONNECT, then receives all 6 publishes fanned across the pool; the 3 pre-existing control-stream tests still pass (backward-compatible)." |
| 0036-T10 | ✅ done | 2026-06-28 | "tests/quic.rs quic_connection_migration_survives_path_change: rebinds the publisher's endpoint to a fresh UDP socket (new source address on the same connection) and asserts the session survived without re-establishing — Connection::stable_id() unchanged, local addr moved, and BOTH directions work on the new path (forward PUBLISH reaches the subscriber; a QoS-1 PUBLISH gets its PUBACK back). Broker-side: spawn_quic_migration_watch (main.rs) watches Connection::remote_address(); on change it logs from→to for the identity and bumps mqttd_quic_path_migrations_total (new metric + Grafana panel). Demo: quic_demo --migrate (QUIC_MIGRATE_MS=10000 in compose) rebinds every 10s so the counter ticks and the broker logs migrations while the quic/demo/* feed keeps flowing. quinn default allows migration (no disable_active_migration set). ADR §3b records the evidence model." |
| 0036-T11 | 💤 deferred | — | 1-RTT session resumption is quinn/rustls-provided and replay-safe (0-RTT stays disabled, T1); explicit ticket-lifetime/policy tuning is a follow-on, separate from migration. Distinct from migration — resumption is a NEW connection reusing crypto, not a live connection surviving a path change. |
<!-- /status-table:0036 -->

## Changelog

- **2026-06-27** — ADR proposed and delivery opened, immediately after ADR 0035 (WebSocket).
  Maintainer chose multi-stream. Sequenced: control-stream foundation (T1–T4) before the
  data-stream demux (T5–T6), because multi-stream *is* one session over many streams and needs
  the control-stream session first. `quinn` 0.11 already in the lock on our rustls/ring.
- **2026-06-27** — **Foundation delivered: T1–T4 + T7.** A working, mTLS-authenticated,
  single-(control-)stream MQTT-over-QUIC: `mqtt-net::quic` (endpoint + byte-stream + leaf-cert),
  `MQTTD_QUIC_BIND` listener, and `tests/quic.rs` (round-trip + mTLS-refusal) all green; README
  documents it. ADR stays **Proposed** — the chosen multi-stream mode (T5–T6) is the next
  milestone. `cargo deny` accepts `quinn` as a direct dep.
- **2026-06-27** — **Multi-stream delivered: T5–T6; ADR Accepted.** `FrameReader::next_raw_frame`
  reads one complete packet's raw bytes (version-agnostic; unit-tested), and
  `mqtt-net::quic::QuicMux`/`accept_mux` merge complete packets from N QUIC streams — never
  byte-interleaved — into one session via concurrent per-stream forwarder tasks (so a stalled
  packet on one stream never blocks another). `serve_quic_clients` and the test harness use
  `accept_mux`. `quic_multistream_demux_no_head_of_line_blocking` proves it (a mid-frame stall on
  one data stream doesn't delay a complete publish on another; the large publish then arrives
  intact), flake-checked 3×. Outbound stays on the control stream (a noted later enhancement).
