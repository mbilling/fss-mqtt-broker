# ADR 0022 — Per-node signed gossip (authenticated SWIM identity)

- **Status:** Accepted
- **Date:** 2026-06-22
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0022-signed-gossip.md](../delivery/0022-signed-gossip.md) — plan, progress, and changelog
- **Related:** [ADR 0003](0003-gossip-authentication.md) (the shared-key gossip MAC this builds on),
  [ADR 0002](0002-transport-security.md) / [ADR 0004](0004-identity-and-authentication.md)
  (the cluster PKI this reuses), [ADR 0016](0016-swim-membership-stability.md)
  (membership claims being authenticated)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0022-signed-gossip.md).

## Context

[ADR 0003](0003-gossip-authentication.md) authenticates every SWIM datagram with an
HMAC-SHA256 tag under a cluster-shared key (`MQTTD_SWIM_KEY`). That proves a datagram came
from **a holder of the shared key** — but not from **which node**. Two gaps follow:

- The HMAC is symmetric, so *any* key-holder can forge a datagram claiming any
  `from`. A single compromised node (or a leaked key) can gossip `from: nodeA, nodeB is
  Dead` — a forged third-party kill switch — and the receiver cannot tell it did not come
  from nodeA. The `suspecter` attribution that ADR 0016 §3 relies on for independent
  confirmation is likewise spoofable.
- The shared key is a *second secret* to provision, sync, and rotate, on top of the
  cluster PKI every node already has.

Meanwhile, with the cluster bus mTLS ([ADR 0002](0002-transport-security.md)), **every node
already holds a CA-issued leaf certificate and its private key**, and every node holds the
cluster **CA certificate** to verify peers. That is exactly the material needed to
authenticate a datagram to its *originating node* — asymmetrically, with no new shared
secret. (Deriving a key from the CA *certificate* is not an option: a CA cert is public, so
a secret derived from it is not secret. See Alternatives.)

## Decision

Add a **per-node signature** layer to gossip, **in addition to** ADR 0003's shared-key
HMAC (defense in depth — a datagram must satisfy both). The HMAC remains the cheap,
bootstrap-friendly cluster-membership gate; the signature adds authenticated node identity.

### 1. Sign with the node key, verify against the CA-chained cert

Each node signs its outgoing gossip with the private key of its **cluster-bus leaf
certificate** (`MQTTD_PEER_TLS_KEY`) and carries that certificate inline. A receiver:

1. verifies the shared-key HMAC (ADR 0003) — cluster gate + whole-datagram integrity;
2. verifies the inline certificate **chains to the cluster CA** (`MQTTD_PEER_TLS_CA`) it
   already holds — so the cert is self-contained and needs no prior knowledge of the peer
   (this avoids any bootstrap chicken-and-egg);
3. verifies the **signature over the payload** with the certificate's public key;
4. extracts the certificate's Common Name and hands it up; the driver enforces that the
   authenticated CN **equals the SWIM `from` node id**, binding the datagram to its sender.

Forging `from: nodeX` now requires nodeX's private key. A compromised node can speak only
*as itself*: the blast radius of one node's compromise shrinks from "impersonate the whole
cluster" to "impersonate one node."

### 2. Wire format v2 (additive; v1 still understood)

```
[VERSION=2][HMAC-SHA256 tag (32)][cert_len u16][leaf cert DER][sig_len u16][signature][payload]
```

The HMAC covers everything after the tag (`cert_len … payload`), so tampering with the
cert, signature, or payload also fails the HMAC. The signature covers `payload`. ADR 0003's
v1 (`[VERSION=1][tag][payload]`) remains a valid wire format for clusters without mTLS
material; a `require` node, however, accepts only v2 (each posture is strict — see §4).

### 3. Reuse the cluster PKI; supported key types

Signing/verification reuse the existing crypto surface — `ring` (HMAC, ECDSA/Ed25519) and
`x509-parser` (already used by [ADR 0004](0004-identity-and-authentication.md)'s mTLS
identity extraction) for chain verification and SPKI extraction. No new dependency. The
broker's own certificates are ECDSA P-256 (the `rcgen` default); the implementation supports
**ECDSA P-256/P-384 and Ed25519**, selected from the certificate's SPKI algorithm, and
fails closed with a clear error on an unsupported key type. The pure `swim` state machine
stays crypto-free; signing/verifying live at the I/O boundary, like the ADR 0003 MAC.

### 4. Posture (strict)

Selected by `MQTTD_SWIM_SIGNED`:

- **`require`** — outgoing gossip is signed and incoming gossip **must** carry a valid
  signature (v1 datagrams are rejected). Requires every node to have cluster-bus TLS
  material.
- **`off`** — ADR 0003 behaviour (shared-key MAC only).

Each posture is **strict**: a `require` node emits and accepts only v2, an `off` node only
v1. There is no mixed-version coexistence — a node accepts only its own wire format. (A
transitional `prefer` mode — sign outgoing but still accept v1 during a node-by-node
rollout — existed earlier but was **removed before any production release**, since the
mainline was never deployed and so never needed a zero-downtime upgrade path. The cluster's
posture is now a single, uniform deployment-time choice.)

`require` needs `MQTTD_PEER_TLS_{CA,CERT,KEY}`; absent them it is a startup error rather
than a silent downgrade. The standing rule holds: any weaker-than-strict posture is
explicit and loudly logged. It also defaults to `require` when both the shared key and the
cluster-bus TLS material are present.

## Consequences

- **Good:** membership claims are authenticated to their originating node, not merely to
  "a key-holder"; a compromised node can no longer impersonate peers or forge third-party
  `Dead`/`Suspect` claims; the `suspecter` confirmation signal (ADR 0016) becomes
  trustworthy. The shared key is retained as a second, independent factor.
- **Cost:** larger datagrams — an inline P-256 cert (~0.4–0.6 KiB) plus a signature
  (~64–72 B) per datagram, well under the 64 KiB bound but a real bandwidth increase on the
  gossip plane. A per-datagram asymmetric verify and chain check replaces a single HMAC;
  negligible at gossip's few-datagrams-per-second-per-node volume. Requires cluster-bus
  mTLS material to be provisioned.
- **Risk:** this is correctness-critical security code. It is built **test-first**, with
  known-answer tests pinning the wire format and signing, adversarial tests (forged `from`,
  cert not chaining to the CA, swapped signature, tampered fields), and a two-node
  over-the-wire integration test — the same bar as ADR 0003/0016.

## Certificate lifecycle (T7, added after acceptance)

Chain-verification alone trusted a CA-issued certificate *forever* — a compromised node
kept a valid gossip identity until its cert file was rotated everywhere. T7 closes both
lifecycle gaps on the verify path:

- **Validity window:** a leaf outside its `notBefore`/`notAfter` is rejected (bounded drop
  reason `expired`). The check takes an injected epoch-seconds clock, so it is
  deterministic under test; an unrepresentable clock fails closed.
- **Revocation:** `MQTTD_PEER_TLS_CRL` (requires the peer-TLS trio) loads a CRL whose
  revoked serials are checked on every inbound signed datagram (drop reason `revoked`).
  The CRL must itself be **signed by the cluster CA** — an unauthenticated revocation
  list would be a denial-of-service lever against healthy nodes. A malformed/unsigned CRL
  is a **startup error**, never a silently-skipped check. The list is hot-reloadable
  through the ADR 0032 validate-before-swap reload (SIGHUP or the ADR 0033 file watcher),
  so publishing a new CRL evicts a compromised node's gossip on the next datagram with no
  restart — pairing with the client-listener CRL (ADR 0002 T8) to give both planes the
  same revocation story.

The certificate is also the carrier for **CA-attested failure-domain labels**
(ADR 0016 T6): a SAN of `URI:urn:fss:failure-domain:<label>` makes the CA the authority
for the holder's topology label; the verify path surfaces it and the gossip driver
enforces it over any self-claim.

## Alternatives considered

- **Derive the gossip key from the CA *certificate*.** Unsound: a CA cert is public
  (distributed to every node, sent in every TLS handshake), so a secret derived from it is
  not secret — anyone who can read the cert could forge gossip. Rejected outright.
- **Derive from the CA *private* key.** Cryptographically fine but requires distributing the
  CA private key to every broker node — a PKI anti-pattern (one node compromise leaks the
  CA). Not aligned with this design's provisioning model. Rejected.
- **Replace the HMAC entirely with signatures.** Viable, but the shared key is a cheap,
  bootstrap-friendly cluster gate and a useful independent second factor; keeping both is
  strictly stronger. Retained as defense in depth.
- **Learn peer public keys at mTLS link-establishment instead of carrying certs inline.**
  Smaller datagrams, but gossip about a node can arrive before its peer link exists
  (SWIM drives link establishment), creating a verify-before-you-know-the-key ordering
  hazard. Inline, CA-verifiable certs are self-contained and avoid it; cert caching by
  fingerprint is a later size optimization.
- **Distribute a generated shared key over the mTLS bus.** Solves provisioning ergonomics
  but keeps the symmetric "any key-holder can impersonate any node" ceiling — no added
  authentication. Rejected as not meeting the security goal.
