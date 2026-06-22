# ADR 0013 — MQTT 5.0 enhanced authentication (AUTH exchange)

- **Status:** Accepted
- **Date:** 2026-06-17
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0013-enhanced-authentication.md](../delivery/0013-enhanced-authentication.md) — plan, progress, and changelog
- **Related:** [ADR 0004](0004-identity-and-authentication.md) (identity model,
  deny-by-default), [ADR 0008](0008-mqtt-5-codec.md) (the v5 wire, AUTH packet)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0013-enhanced-authentication.md).

## Context

MQTT 3.1.1 and the broker today authenticate a CONNECT in a single shot: username
+ password, an mTLS certificate subject, or a bearer token, verified once by an
[`Authenticator`](../../crates/mqtt-auth/src/lib.rs). MQTT 5.0 adds **enhanced
authentication**: a SASL-style, multi-round challenge/response negotiated with the
**Authentication Method** (`0x15`) and **Authentication Data** (`0x16`) properties
and carried by **AUTH** control packets.

The flow:

1. CONNECT carries an Authentication Method and (optionally) initial Authentication
   Data. Its presence is what selects enhanced auth.
2. The server may answer with an AUTH packet, reason **`0x18`** (Continue
   authentication), echoing the method and carrying challenge data.
3. The client answers with an AUTH, reason `0x18`, carrying its response data.
4. Steps 2–3 repeat until the server **accepts** — a normal CONNACK with reason
   `0x00` — or **rejects** — a CONNACK with a failure reason. The Authentication
   Method must stay constant for the whole exchange.

This lets a client prove possession of a secret without ever putting it on the
wire — the property the single-shot password flow lacks.

## Decision

### 1. A dedicated exchange abstraction in `mqtt-auth`, beside `Authenticator`

The single-shot `Authenticator` trait stays as is; enhanced auth gets its own
small abstraction so a method can hold per-exchange state and drive multiple
rounds:

- `EnhancedAuthenticator` — registered by its **method name**; `start()` begins one
  exchange.
- `AuthSession` — one in-flight exchange for one connection; `step(client, data)`
  consumes the CONNECT's initial data, then each AUTH's data, and returns an
  `AuthStep`.
- `AuthStep` — `Challenge(bytes)` (send an AUTH `0x18` and await the reply),
  `Success(Identity)`, or `Failure`.

The broker holds an optional `EnhancedAuthenticator` in its `ConnPolicy`. Keeping
exchange state in an `AuthSession` object (not the trait) means the method
implementation owns its nonces and round counter without the connection layer
knowing the mechanism.

### 2. The connection layer owns the AUTH packet plumbing

`run_framed` selects the path from the CONNECT: an Authentication Method present
runs the exchange; absent falls through to the existing single-shot
`authenticate_connect`. The exchange loop sends AUTH `0x18` challenges, reads the
client's AUTH replies, enforces that each reply is an AUTH with reason `0x18` and
the **same** method, and feeds the data to the `AuthSession` until it yields
`Success` (→ proceed to the normal CONNACK and session attach) or `Failure`
(→ rejecting CONNACK, no session). A CONNECT whose method has **no** matching
configured authenticator is rejected with CONNACK reason `0x8C` (Bad
Authentication Method); a malformed exchange (wrong packet, mismatched method)
closes the connection. As everywhere, a rejected client never reaches the hub.

### 3. Reference mechanism: HMAC-SHA256 challenge/response

A concrete `HmacChallengeAuthenticator` ships so the path is real and tested. The
client names itself in the CONNECT's initial Authentication Data; the server
replies with a random 32-byte nonce (`ring::rand`); the client returns
`HMAC-SHA256(secret, nonce)`; the server verifies it **constant-time**
(`ring::hmac::verify`) against the subject's configured shared secret. The secret
never crosses the wire. An unknown subject is still issued a challenge before the
inevitable failure, to blunt user enumeration (the verify-time difference remains
a known minor side channel). This reuses the `ring` primitive already vetted for
the gossip MAC (ADR 0003).

### 4. Re-authentication reuses the same exchange in the serve loop

Post-connect **re-authentication** ([MQTT-4.12]) is supported for client-initiated
exchanges. An AUTH packet arriving on an established session is intercepted in the
serve loop: reason `0x19` (Re-authenticate) starts a fresh exchange driven by the
**same** challenge/response code as connect-time auth (`drive_auth_exchange`),
differing only at the edges — the Authentication Method must match the one from
CONNECT [MQTT-4.12.1-1] (else DISCONNECT `0x82`), success is signalled with an
AUTH `0x00` (not a CONNACK) and **updates the principal** used for subsequent ACL
checks, and failure sends DISCONNECT `0x87` and closes. Any other AUTH on an
established session (a stray `0x18`/`0x00`, or `0x19` when connect did not use
enhanced auth) is a protocol error (DISCONNECT `0x82`).

**Server-initiated** re-auth (the server sending AUTH `0x19` to demand the client
re-authenticate, e.g. on credential expiry) is deferred: it needs a trigger
mechanism and interacts with the select loop's outbound path, and is independent
of the client-initiated case.

## Consequences

- **Good:** secrets are proven, not transmitted; pluggable per method via one small
  trait pair; the single-shot path and every existing authenticator are untouched;
  the reference HMAC mechanism is real crypto with constant-time verification.
- **Cost / limits:** server-initiated re-auth is not yet supported (§4); the AUTH
  exchange blocks on the client between rounds with no dedicated timeout (the same
  surface as the existing pre-CONNACK reads), and a client-initiated re-auth
  likewise stalls outbound delivery for its (brief) duration; enumeration resistance
  is partial (§3); the reference mechanism configures secrets in memory — a real
  deployment would back it with a secret store.

## Alternatives considered

- **Extend `Authenticator` with an optional multi-step method.** Rejected: it would
  bolt round state onto a trait designed to be stateless and single-shot, muddying
  every existing implementation. A separate `AuthSession` keeps each concern clean.
- **Drive the exchange from the hub.** Rejected: authentication is per-connection and
  precedes any session state; the hub is shared across connections and must never see
  an unauthenticated client. The connection layer is the right owner.
- **Ship only the trait, no mechanism.** Rejected: an untested abstraction with no
  real implementation tends to be the wrong abstraction. The HMAC method exercises
  every part of the exchange.
