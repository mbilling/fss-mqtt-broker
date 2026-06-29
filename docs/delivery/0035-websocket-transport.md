---
adr: "0035"
title: Native MQTT-over-WebSocket transport
adr_status: Accepted
tasks:
  - id: 0035-T1
    title: WebSocket byte-stream adapter in mqtt-net — present a tokio-tungstenite WebSocketStream as AsyncRead+AsyncWrite (concatenate inbound binary frames; writes emit binary frames; handle Ping/Pong/Close); unit-tested at the frame/byte boundary
    status: done
    date: 2026-06-27
    evidence: "mqtt-net/src/ws.rs WsByteStream: poll_read concatenates inbound binary frames (a packet may span frames), poll_write emits one binary frame per write, Ping/Pong/Close handled (Close->EOF), text frame is an InvalidData error. WsByteStream::wrap is public so client/test code reuses it. Proven by the ws_pubsub_roundtrip / wss_mtls tests below (MQTT packets cross the frame/byte boundary intact)."
  - id: 0035-T2
    title: WS accept helper — perform the HTTP Upgrade negotiating the `mqtt` subprotocol (reject clients that don't offer it), returning the adapter stream; add the Transport::WebSocket (plaintext) variant alongside WebSocketTls
    status: done
    date: 2026-06-27
    evidence: "mqtt-net::ws::accept does accept_hdr_async, requires the client to offer the `mqtt` subprotocol (else 400), echoes it back. Transport::WebSocket (plaintext) added alongside WebSocketTls. Test: ws_without_mqtt_subprotocol_is_refused."
  - id: 0035-T3
    title: Listeners + config — MQTTD_WSS_BIND (TLS-first via the existing reloadable acceptor, then WS) and MQTTD_WS_BIND (plaintext WS, insecure/loud); both call conn::handle_stream; WSS identity = mTLS leaf CN (ADR 0004)
    status: done
    date: 2026-06-27
    evidence: "main.rs: start_client_listeners builds ONE reloadable client-TLS acceptor shared by serve_tls_clients + serve_wss_clients; serve_ws_clients (plaintext, loud) + serve_wss_clients (TLS then ws::accept; identity via conn::tls_identity before wrapping). MQTTD_WS_BIND / MQTTD_WSS_BIND."
  - id: 0035-T4
    title: End-to-end tests — a WS client pub/sub round-trip (plaintext WS) and a WSS round-trip with mTLS (client cert -> identity), driven by a real tungstenite/WS client over a loopback listener
    status: done
    date: 2026-06-27
    evidence: "tests/ws.rs (3 tests, all pass): ws_pubsub_roundtrip (plaintext ws), wss_mtls_pubsub_roundtrip (client cert presented, CN identity, WS over TLS), ws_without_mqtt_subprotocol_is_refused. Client reuses WsByteStream::wrap + FrameReader/FrameWriter — same path as a TCP client."
  - id: 0035-T5
    title: Convert the demo playground to native mqttd WebSockets — point the page at mqttd's MQTTD_WS_BIND directly and drop the mosquitto gateway + relay (browser tabs become first-class mqttd sessions)
    status: done
    date: 2026-06-27
    evidence: "demo: MQTTD_WS_BIND=0.0.0.0:1890 on every node (host 8089 -> mqttd-1); playground-gw (mosquitto) + mosquitto.conf + entrypoint.sh relay removed; page connects ws://host:8089 directly and drops the play/up/down indirection. Verified end to end: native WS round-trip (paho -> mqttd-1:1890) RESULT OK; WS handshake over Tailscale returns 101 with sec-websocket-protocol: mqtt from mqttd itself."
  - id: 0035-T6
    title: Docs — README transports table + the WS/WSS env vars; note no permessage-deflate and that the peer bus stays mTLS/TCP
    status: done
    date: 2026-06-27
    evidence: "README: MQTTD_WSS_BIND / MQTTD_WS_BIND rows + the Security transport bullet; demo/README playground section rewritten for native WS. ADR documents no permessage-deflate and peer bus stays mTLS/TCP."
  - id: 0035-T7
    title: Follow-on — MQTT-over-QUIC (separate ADR) reuses the same handle_stream<S> seam
    status: done
    date: 2026-06-29
    evidence: "Delivered as ADR 0036 (MQTT-over-QUIC, 10/11 done). quic::byte_stream joins a quinn bidi stream into AsyncRead+AsyncWrite so the unchanged handle_stream<S> runs over it — the exact seam this follow-on predicted. The multi-stream mapping, mTLS identity, and connection migration all build on that seam; see docs/delivery/0036-quic-transport.md."
---

# Delivery — ADR 0035: Native MQTT-over-WebSocket transport

Decision: [docs/adr/0035-websocket-transport.md](../adr/0035-websocket-transport.md).

A browser can only speak MQTT-over-WebSockets, and the broker has none (only a
`Transport::WebSocketTls` placeholder) — so the demo bolts a mosquitto WS gateway in front and
bridges it in. This adds a **native** WebSocket client listener so a browser is a first-class
mqttd session (real client-id, placement, durability, ACL, auth, audit, metrics). WS framing is
delegated to the vetted `tokio-tungstenite`; WSS reuses the ADR 0002 TLS 1.3 + mTLS stack (and
the ADR 0032 reloadable acceptor); the MQTT engine (`handle_stream<S>`) is unchanged.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0035-T1** Byte-stream adapter | A `WebSocketStream` is presented as `AsyncRead + AsyncWrite`: inbound binary frames concatenate into the MQTT byte stream (a packet may span frames), writes emit binary frames, and `Ping`/`Pong`/`Close` are handled. Unit-tested at the boundary. |
| **0035-T2** WS accept | The HTTP `Upgrade` negotiates subprotocol `mqtt` and rejects a client that doesn't offer it; returns the adapter stream. `Transport::WebSocket` (plaintext) joins `WebSocketTls`. |
| **0035-T3** Listeners | `MQTTD_WSS_BIND` does TLS via the existing reloadable acceptor then the WS upgrade; `MQTTD_WS_BIND` is plaintext WS (insecure, loud). Both call `conn::handle_stream`; WSS identity is the mTLS leaf CN. |
| **0035-T4** E2E tests | A real WS client completes a pub/sub round-trip over plaintext WS; a WSS client presenting a client cert round-trips and its CN becomes the session identity. |
| **0035-T5** Playground | The demo page connects to mqttd's `MQTTD_WS_BIND` directly; the mosquitto gateway + relay are removed. Browser tabs are native mqttd sessions. |
| **0035-T6** Docs | README transports + `MQTTD_WS_BIND`/`MQTTD_WSS_BIND`; note no permessage-deflate and peer bus stays mTLS/TCP. |
| **0035-T7** Follow-on | *(deferred)* MQTT-over-QUIC, separate ADR, same `handle_stream<S>` seam. |

## Progress

<!-- status-table:0035 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0035-T1 | ✅ done | 2026-06-27 | "mqtt-net/src/ws.rs WsByteStream: poll_read concatenates inbound binary frames (a packet may span frames), poll_write emits one binary frame per write, Ping/Pong/Close handled (Close->EOF), text frame is an InvalidData error. WsByteStream::wrap is public so client/test code reuses it. Proven by the ws_pubsub_roundtrip / wss_mtls tests below (MQTT packets cross the frame/byte boundary intact)." |
| 0035-T2 | ✅ done | 2026-06-27 | "mqtt-net::ws::accept does accept_hdr_async, requires the client to offer the `mqtt` subprotocol (else 400), echoes it back. Transport::WebSocket (plaintext) added alongside WebSocketTls. Test: ws_without_mqtt_subprotocol_is_refused." |
| 0035-T3 | ✅ done | 2026-06-27 | "main.rs: start_client_listeners builds ONE reloadable client-TLS acceptor shared by serve_tls_clients + serve_wss_clients; serve_ws_clients (plaintext, loud) + serve_wss_clients (TLS then ws::accept; identity via conn::tls_identity before wrapping). MQTTD_WS_BIND / MQTTD_WSS_BIND." |
| 0035-T4 | ✅ done | 2026-06-27 | "tests/ws.rs (3 tests, all pass): ws_pubsub_roundtrip (plaintext ws), wss_mtls_pubsub_roundtrip (client cert presented, CN identity, WS over TLS), ws_without_mqtt_subprotocol_is_refused. Client reuses WsByteStream::wrap + FrameReader/FrameWriter — same path as a TCP client." |
| 0035-T5 | ✅ done | 2026-06-27 | "demo: MQTTD_WS_BIND=0.0.0.0:1890 on every node (host 8089 -> mqttd-1); playground-gw (mosquitto) + mosquitto.conf + entrypoint.sh relay removed; page connects ws://host:8089 directly and drops the play/up/down indirection. Verified end to end: native WS round-trip (paho -> mqttd-1:1890) RESULT OK; WS handshake over Tailscale returns 101 with sec-websocket-protocol: mqtt from mqttd itself." |
| 0035-T6 | ✅ done | 2026-06-27 | "README: MQTTD_WSS_BIND / MQTTD_WS_BIND rows + the Security transport bullet; demo/README playground section rewritten for native WS. ADR documents no permessage-deflate and peer bus stays mTLS/TCP." |
| 0035-T7 | ✅ done | 2026-06-29 | "Delivered as ADR 0036 (MQTT-over-QUIC, 10/11 done). quic::byte_stream joins a quinn bidi stream into AsyncRead+AsyncWrite so the unchanged handle_stream<S> runs over it — the exact seam this follow-on predicted. The multi-stream mapping, mTLS identity, and connection migration all build on that seam; see docs/delivery/0036-quic-transport.md." |
<!-- /status-table:0035 -->

## Changelog

- **2026-06-26** — ADR proposed and delivery opened. Maintainer chose: WebSocket before QUIC;
  `tokio-tungstenite` (vetted) for WS framing rather than hand-rolling; WSS reuses the existing
  rustls/mTLS stack. Tasks `planned`; QUIC tracked as the deferred follow-on (its own ADR,
  multi-stream).
- **2026-06-27** — T1–T6 delivered; ADR Accepted. `mqtt-net::ws` does the `mqtt`-subprotocol WS
  handshake (tokio-tungstenite) + a `WsByteStream` adapter, so the generic `handle_stream<S>` is
  unchanged. `MQTTD_WS_BIND` / `MQTTD_WSS_BIND` listeners (WSS shares the reloadable TLS acceptor;
  identity = mTLS CN). Tests `tests/ws.rs` (ws, wss+mTLS, subprotocol-refusal) pass. The demo
  playground now connects to mqttd's native WS directly — the mosquitto gateway + relay are
  deleted; verified end to end (paho→mqttd-1:1890 RESULT OK, `101` + `mqtt` subprotocol over
  Tailscale from mqttd itself). QUIC (multi-stream) remains the deferred follow-on (its own ADR).
- **2026-06-29** — T7 (the QUIC follow-on) marked **done**: delivered as ADR 0036
  (MQTT-over-QUIC, 10/11). `quic::byte_stream` joins a quinn bidi stream into
  `AsyncRead + AsyncWrite` over the same `handle_stream<S>` seam this task predicted, with
  multi-stream demux, mTLS identity, and connection migration on top.
