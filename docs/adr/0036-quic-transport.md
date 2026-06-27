# ADR 0036 — MQTT-over-QUIC transport (multi-stream)

- **Status:** Accepted
- **Date:** 2026-06-27
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0036-quic-transport.md](../delivery/0036-quic-transport.md) — plan, progress, and changelog
- **Related:** [ADR 0035](0035-websocket-transport.md) (the `handle_stream<S>` transport seam this
  reuses), [ADR 0002](0002-transport-security.md) (TLS 1.3 / mTLS — QUIC *mandates* TLS 1.3 and
  carries the same client-cert identity), [ADR 0004](0004-identity-and-authentication.md)
  (mTLS identity = leaf CN), [ADR 0032](0032-hot-reloadable-security-policy.md) (cert material)

> This record states the decision only. How it is being built and how far along it is live in
> the [delivery doc](../delivery/0036-quic-transport.md).

## Context

QUIC (RFC 9000, over UDP) brings real benefits to an MQTT broker: **mandatory TLS 1.3** (no
plaintext mode exists), **0-RTT/1-RTT** fast (re)connect, **connection migration** (a client
that changes network/IP keeps its session — valuable for mobile/IoT), and — the headline —
**independent streams with no head-of-line blocking**: a stalled large PUBLISH on one stream
does not block delivery on another, unlike a single TCP/TLS byte stream.

MQTT-over-QUIC is **not** in the OASIS MQTT specification — it is a **vendor extension**, with
EMQX the de-facto reference. Two shapes exist in practice:

1. **Single (control) stream** — the whole MQTT byte stream runs over one QUIC bidirectional
   stream. This is essentially "MQTT-over-TLS, but on QUIC," and is the most interoperable.
2. **Multi-stream** (EMQX's advanced mode) — **one MQTT session spread across many QUIC
   streams**: a *control stream* (the first bidi stream) carries CONNECT/CONNACK, SUBSCRIBE,
   PINGREQ, etc.; additional *data streams* carry PUBLISH flows so independent topics/QoS levels
   don't head-of-line-block each other. This is the QUIC-native shape — and the maintainer's
   choice — but it is non-standard, so interop is limited to clients that speak it.

The broker's connection engine is transport-agnostic: `conn::handle_stream<S>` runs over any
`S: AsyncRead + AsyncWrite + Unpin` (ADR 0035). A QUIC bidirectional stream is exactly such a
byte stream. **Multi-stream, by definition, contains a control stream** — so the control stream
is not an alternative to multi-stream, it is multi-stream's foundation.

## Decision

**Add a native MQTT-over-QUIC client listener (via `quinn`, already in the tree on our rustls
0.23 + ring), building toward EMQX-style multi-stream — control stream first, data streams
layered on it. QUIC's mandatory TLS 1.3 + mTLS reuse the ADR 0002 cert material; the client
identity is the leaf CN, exactly as for TCP-TLS and WSS.**

### 1. `quinn` on our existing rustls/ring; ALPN `mqtt`

The QUIC endpoint is built from a rustls 0.23 `ServerConfig` (our crypto: ring, TLS 1.3) with
**ALPN `mqtt`** and the same client-certificate verifier as the TLS listener (mTLS). `quinn` is
already resolved at 0.11 transitively, so `cargo deny` already accepts it — the marginal
supply-chain cost is small. There is **one TLS implementation** in the broker; QUIC just uses
rustls' QUIC API instead of the TLS-record API.

### 2. The control stream carries the MQTT session (reuses `handle_stream`)

On an accepted QUIC connection the broker takes the first **bidirectional** stream and runs the
MQTT session over it: `quinn`'s `SendStream`/`RecvStream` are joined into one
`AsyncRead + AsyncWrite` and handed to the unchanged `handle_stream<S>`. The mTLS **identity is
the leaf CN** read from the QUIC connection's peer certificate (ADR 0004). This alone is a
complete, interoperable single-stream MQTT-over-QUIC — and the base every richer mode builds on.

### 3. Multi-stream: one session, many streams

Data streams are layered on the control-stream session: the connection task accepts **additional
bidi streams** and demultiplexes their PUBLISH packets into the *same* MQTT session, and routes
outbound PUBLISH flows across streams so independent topics do not head-of-line-block. This is
the one place the broker's "one stream = one session loop" assumption is generalised to "one
session fed by N streams." It follows EMQX's de-facto framing so an EMQX-style client interops;
the control-stream mode (2) remains the fallback for clients that don't open data streams.

### 3a. Outbound fan-out: capability-negotiated, topic-affinity (added)

The inbound demux (§3) merges client→broker PUBLISH from many streams. The **outbound**
direction is symmetric: broker→client PUBLISH fans across QUIC data streams the **broker
opens**, so a large/slow delivery on one stream does not head-of-line-block another. Two
constraints make this sound:

1. **Capability negotiation — never strand a single-stream client.** A client that reads only
   the control stream would *miss* publishes the broker fanned onto data streams. So the broker
   fans outbound **only to a client that has demonstrated multi-stream capability** — concretely,
   one that has itself opened ≥1 QUIC data stream (and therefore runs the symmetric mux that
   accepts broker-opened streams). The capability is **observed, not advertised**: the broker
   marks the connection capable the first time its mux reads a frame from a non-control stream.
   Until then — and forever, for a plain single-control-stream client — **all** outbound stays
   on the control stream. The control-stream mode remains the interoperable default; fan-out is
   strictly additive and backward-compatible.

2. **Topic affinity — preserve per-topic order.** MQTT guarantees ordering only **within a
   topic** (for a given QoS, to a given subscriber) and makes no cross-topic promise. So each
   outbound PUBLISH is routed to a data stream by **hashing its topic** — same topic → same
   stream (order preserved); different topics may use different streams (the spec permits it).
   Control packets and QoS acks (PUBACK/PUBREC/PUBREL/PUBCOMP) stay on the control stream. A
   small fixed pool of outbound data streams bounds resource use.

The mux is therefore **symmetric**: each side accepts the peer's opened streams (inbound) and
opens its own for PUBLISH (outbound), so the demo/test clients run the same `QuicMux` as the
broker. A single-stream client is never broken — it simply never triggers fan-out.

### 4. Security and scope

- **No 0-RTT for CONNECT initially.** QUIC 0-RTT early data is **replayable**; a replayed CONNECT
  (or any non-idempotent control packet) is a security hazard, so early data is disabled for the
  session-establishing exchange. (1-RTT resumption — fast *and* replay-safe — is kept.)
- **Client listener only.** The cluster peer bus stays mTLS/TCP (ADR 0002/0003).
- **Identity unchanged:** leaf CN over QUIC mTLS, same as TCP-TLS/WSS — QUIC changes the
  transport, not the identity or the MQTT semantics.
- WSS/TCP remain; QUIC is additive (`MQTTD_QUIC_BIND`, UDP).

## Consequences

- **Good:** TLS-1.3-only transport with fast reconnect, connection migration, and no
  cross-stream head-of-line blocking; reuses the `handle_stream` seam and the ADR 0002 mTLS
  material; `quinn` is already vetted in the tree.
- **Cost:** `quinn` as a direct dependency; a QUIC endpoint + stream adapter to own; and — for
  multi-stream — a genuine generalisation of the connection model (one session, many streams),
  which is the substantial, correctness-sensitive part and is built in stages on the tested
  control-stream foundation.
- **Risk / honesty:** MQTT-over-QUIC is non-standard, so **interop is limited** to clients that
  speak it (EMQX-style); multi-stream more so. The control-stream mode maximises interop; the
  data-stream demux is the bespoke surface and is tested directly. Built test-first, staged so a
  working, mTLS-authenticated single-stream QUIC listener lands before the demux.

## Alternatives considered

- **Single-stream only (no data streams).** Simplest and most interoperable, but forgoes QUIC's
  signature no-HoL-blocking benefit — the reason to choose QUIC over WSS. Kept as the built-in
  fallback and the foundation, not the end state (the maintainer chose multi-stream).
- **HTTP/3 + WebTransport.** Carry MQTT over WebTransport streams on h3. More moving parts (an
  h3 stack) and even less client support than EMQX-style MQTT-over-QUIC. Rejected.
- **Enable 0-RTT for lower latency.** Rejected for the CONNECT exchange: 0-RTT early data is
  replayable, unacceptable for session establishment on a security broker. 1-RTT resumption is
  kept (replay-safe).
- **aws-lc-rs crypto for quinn.** A second crypto provider alongside ring. Rejected: use ring,
  matching the rest of the broker (one provider).
