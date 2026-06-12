# ADR 0003 — Gossip-plane authentication: keyed MAC on SWIM datagrams

- **Status:** Accepted
- **Date:** 2026-06-11
- **Deciders:** project maintainers
- **Related:** ADR 0002 (deferred this), `mqtt-cluster::swim_driver`, Capability Plan §3/§4

## Context

ADR 0002 secured the client listener and the peer-link bus, leaving one
unauthenticated network surface: SWIM gossip over UDP. Anyone who can reach the
gossip port can inject membership claims — join the cluster's view, map its
topology, or claim a healthy node `Dead`. Since SWIM now *drives* the routing
layer, a forged `Dead` claim is a remote kill switch for a node's links.

UDP gossip cannot reuse the mTLS machinery (no connections), so it needs its
own integrity/authenticity mechanism.

## Decision

1. **Every SWIM datagram carries an HMAC-SHA256 tag** computed with a
   cluster-shared 32-byte key over the serialized message:
   `[version byte][32-byte tag][payload]`. Receivers **verify before decode**
   (constant-time, via `ring`); failures are dropped without reaching the
   protocol state machine. The pure `swim` module stays crypto-free — sealing
   lives at the I/O boundary in `swim_auth`/`swim_driver`.

2. **`ring` over a new dependency.** HMAC-SHA256 from `ring`, which is already
   in the tree via `rustls` — no new supply-chain surface. BLAKE3 keyed mode
   would be faster but adds a crate; gossip volume (a few datagrams per second
   per node) makes MAC speed irrelevant.

3. **Key provisioning:** a 64-hex-char (32-byte) key via `MQTTD_SWIM_KEY`
   (interim env shim, like all current config). Short or malformed keys are
   startup errors — there is no weak-key mode. Generate with
   `openssl rand -hex 32`. Until config-file loading lands, running SWIM
   without a key remains possible and is loudly logged as INSECURE,
   consistent with the plaintext listener shims.

## Replay: accepted, bounded, self-healing

A MAC authenticates but does not prevent replaying captured datagrams. We
accept this for now because SWIM's incarnation mechanism bounds the damage:

- Replayed `Alive`/`Suspect`/`Dead` updates carry their original incarnation;
  any claim at or below a member's current incarnation is superseded.
- The worst case — replaying a captured `Dead` claim at the victim's current
  incarnation — triggers the victim's standard refutation (incarnation bump +
  `Alive` gossip), exactly like a false suspicion. Disruption is transient and
  the attacker cannot escalate it.
- Replayed `Ping`/`Join` cost one response datagram (no amplification: replies
  go to the spoofable-but-MAC'd sender address only if the MAC verified, i.e.
  to a datagram a cluster member originally sent).

Full anti-replay (timestamp windows or per-peer nonces) needs loose clock
sync or per-peer state and is deferred until operational experience says the
transient-refutation cost matters.

## Consequences

- A network position no longer suffices to influence membership; key
  possession does. Key rotation requires a cluster restart for now — rotation
  without downtime (dual-key acceptance window) is follow-up work.
- 33 bytes of overhead per datagram; negligible against the 64 KiB bound.
- Rejected datagrams are logged at `debug` (per-packet `warn` would hand an
  attacker a log-flooding lever); a rejected-datagram metrics counter is the
  proper operator signal and lands with the observability phase.

## Deferred

- Anti-replay window; per-peer nonces.
- Zero-downtime key rotation (accept old+new during a window).
- Deriving the gossip key from cluster-CA material instead of a second secret.
