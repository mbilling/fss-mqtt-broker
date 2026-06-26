# ADR 0035 — Native MQTT-over-WebSocket transport

- **Status:** Accepted
- **Date:** 2026-06-26
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0035-websocket-transport.md](../delivery/0035-websocket-transport.md) — plan, progress, and changelog
- **Related:** [ADR 0002](0002-transport-security.md) (the TLS 1.3 / mTLS this reuses for WSS;
  WebSocket was named there as a later milestone), [ADR 0032](0032-hot-reloadable-security-policy.md)
  (the reloadable TLS acceptor WSS shares), [ADR 0004](0004-identity-and-authentication.md)
  (mTLS identity, unchanged over WSS), a precursor to **MQTT-over-QUIC** (separate ADR)

> This record states the decision only. How it is being built and how far along it is live in
> the [delivery doc](../delivery/0035-websocket-transport.md).

## Context

Browsers cannot open raw MQTT/TCP sockets — only **MQTT-over-WebSockets** (the OASIS-defined
binding: an HTTP `Upgrade` to WebSocket with subprotocol `mqtt`, MQTT packets carried in binary
frames). The broker has no WebSocket listener: `mqtt-net` carries only a `Transport::WebSocketTls`
**enum placeholder** ("later milestone"), with nothing behind it. So today a browser cannot be a
first-class mqttd client — the demo playground has to bolt a separate `mosquitto` WebSocket
gateway in front and *bridge* it into the cluster, which means browser sessions live on
mosquitto (foreign client-ids, no mqttd placement/durability/ACL/audit), not on mqttd.

The connection engine is already transport-agnostic: `conn::handle_stream<S>` is generic over
`S: AsyncRead + AsyncWrite + Unpin`, and `FrameReader`/`FrameWriter` read/write MQTT packets over
any such stream. A WebSocket connection is just one more way to obtain that byte stream.

## Decision

**Add a native MQTT-over-WebSocket client listener to mqttd, so a browser (or any WS MQTT
client) is a first-class mqttd session — same client-id, placement, durability, ACL,
authentication, audit, and metrics as a TCP client.**

### 1. WebSocket framing via `tokio-tungstenite`; MQTT codec stays ours

The HTTP `Upgrade` handshake and WebSocket frame codec (masking, fragmentation, control frames)
are handled by **`tokio-tungstenite`** — the de-facto, fuzzed Rust WS library. The broker keeps
owning the thing that is its domain (the MQTT codec, which it already hand-rolls *and* fuzzes);
it does **not** hand-roll a second network-facing binary parser. This is the deliberate
exception to the minimal-supply-chain default (ADR 0002): a vetted WS parser is safer than a
bespoke one, and the WebSocket framing is incidental, not core. `cargo deny` vets the addition.

### 2. WSS reuses the ADR 0002 TLS stack — no second TLS path

A secure WebSocket (`wss://`) is **TLS first, then the WS handshake over the TLS stream**: the
listener performs the rustls handshake with the *existing* `tls::server_acceptor` (so WSS shares
ADR 0002's TLS 1.3, cipher policy, and **mTLS** client-cert verification, and ADR 0032's
*reloadable* acceptor), then runs the WebSocket upgrade over the resulting `TlsStream`.
`tokio-tungstenite` is therefore added **without** its own TLS feature — there is exactly one TLS
implementation in the broker. Over WSS, the client-certificate **identity is the TLS leaf CN**,
identical to a TCP TLS client (ADR 0004): WebSocket changes the framing, not the identity.

### 3. A byte-stream adapter so `handle_stream` is unchanged

MQTT-over-WS carries the MQTT byte stream inside WebSocket **binary** frames (a packet may span
frames; a frame may hold several). A small adapter presents the `WebSocketStream` as
`AsyncRead + AsyncWrite` — reads concatenate inbound binary-frame payloads into a byte stream;
writes emit binary frames; `Ping`/`Pong`/`Close` control frames are handled transparently. The
MQTT engine (`FrameReader`/`FrameWriter`, `handle_stream`) runs over it **unmodified**. The
listener negotiates the `mqtt` subprotocol and rejects a client that does not offer it.

### 4. Listeners and configuration

Two new binds, mirroring the TCP pair: `MQTTD_WSS_BIND` (WebSocket over TLS — the intended,
secure path; reuses `MQTTD_TLS_CERT`/`_KEY`/`_CLIENT_CA`) and `MQTTD_WS_BIND` (plaintext
WebSocket — **insecure**, loudly logged, for local/dev only, exactly like `MQTTD_PLAINTEXT_BIND`).
`Transport::WebSocketTls` is joined by a `WebSocket` (plaintext) variant; both feed the same hub.

### 5. Scope

Client listener only. The **cluster peer bus stays mTLS/TCP** (ADR 0002/0003) — WebSocket is a
client-facing convenience, not an internal transport. Per-message WS compression
(`permessage-deflate`) is out of scope (a CRIME/BREACH-class footgun on a security broker; left
off). This ADR is also the transport-integration precursor to **MQTT-over-QUIC** (separate ADR),
which plugs into the same `handle_stream<S>` seam.

## Consequences

- **Good:** browsers and WS MQTT clients become **real mqttd sessions** — full placement,
  durability, ACL, auth, audit, metrics — so the demo playground can drop its mosquitto gateway
  and talk to the cluster directly. WSS gets TLS 1.3 + mTLS + hot reload for free from ADR
  0002/0032. The MQTT engine is untouched (one generic `handle_stream`).
- **Cost:** one new dependency (`tokio-tungstenite`, no-TLS) that `cargo deny` must accept; a
  byte-stream adapter to own and test; two new listener binds.
- **Risk:** a new network-facing handshake/parser, but the WS framing is delegated to a vetted
  crate and the MQTT layer — the security-critical part — is unchanged. The adapter (frame↔byte
  boundary, control frames, partial packets) is the new surface and is tested directly.
- **Security posture preserved:** WSS is the documented path; plaintext WS is opt-in and
  loud; identity over WSS is the same mTLS CN; no compression footgun; peer bus unchanged.

## Alternatives considered

- **Hand-roll the WebSocket layer (no dependency).** Consistent with the project hand-rolling +
  fuzzing its MQTT codec, but WebSocket framing (masking, fragmentation, continuation, control
  frames, close handshake) is a second security-critical parser to own for an *incidental*
  protocol. Rejected: a vetted WS crate is the safer call; we spend our parser-ownership budget
  on MQTT, which is the domain.
- **Keep the mosquitto WS gateway (status quo).** Works for the demo, but browser clients are
  never real mqttd sessions (foreign ids, no placement/durability/ACL/audit) and it is a
  separate broker to run. Rejected for production; fine as the pre-0035 demo shim.
- **Bundle tokio-tungstenite's own TLS.** A second TLS stack alongside rustls — more surface,
  divergent policy. Rejected: do TLS once with the existing acceptor, run WS over it.
- **permessage-deflate compression.** Bandwidth win, but compression-oracle (CRIME/BREACH) risk
  on a security broker, and MQTT payloads are often already small/structured. Rejected.
