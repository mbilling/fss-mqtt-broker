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
    title: Follow-on — connection migration validation + 1-RTT resumption tuning; optional demo wiring
    status: deferred
    notes: QUIC connection migration and resumption are quinn-provided; explicit validation/tuning and any demo exposure are a follow-on once the transport + multi-stream land.
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
| **0036-T8** Follow-on | *(deferred)* Connection-migration validation, resumption tuning, demo wiring. |

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
| 0036-T8 | 💤 deferred | — | QUIC connection migration and resumption are quinn-provided; explicit validation/tuning and any demo exposure are a follow-on once the transport + multi-stream land. |
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
