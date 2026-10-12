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
identity extraction) for chain verification and SPKI extraction. No new dependency.

> **Amended by [ADR 0053](0053-single-crypto-provider-aws-lc-rs.md) (2026-08-04):**
> signatures now run on `aws_lc_rs`, and x509-parser is parsing-only — its ring-backed
> `verify` feature is replaced by the first-party `verify_x509_signature` (which also
> drops the SHA-1 RSA legacy arm: fail closed). The
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
- **Cost:** larger datagrams — a signature (~64–72 B) per datagram plus the certificate
  reference: since T6 (below), most datagrams carry a **32-byte fingerprint** rather than
  the inline ~0.4–0.6 KiB certificate, keeping routine signed gossip under a typical
  1500-byte MTU; the full certificate rides only first-contact (Join/Sync) and periodic
  refresh datagrams. A per-datagram asymmetric verify and chain check replaces a single
  HMAC; negligible at gossip's few-datagrams-per-second-per-node volume. Requires
  cluster-bus mTLS material to be provisioned.
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

## Certificate fingerprinting (T6, added after acceptance)

Inlining the full leaf certificate in **every** datagram — correct and bootstrap-safe —
made each signed datagram ~0.5 KiB heavier, pushing gossip with piggybacked updates past
a typical 1500-byte MTU (UDP fragmentation amplifies loss on lossy networks). T6 replaces
the always-inline certificate with a **cert-ref**: the full certificate on **priming**
datagrams (Join/Sync — the first-contact and full-state moments, chosen by the driver
from the message kind) and on every 16th datagram as a loss-recovery bound; a **32-byte
SHA-256 fingerprint** otherwise.

The receiver caches certificates by fingerprint — but **only certificates that passed the
full verification** enter the cache, and fingerprinting is *pure wire compression*, not a
trust change: every datagram, fingerprint-form included, still runs the complete
verification (CA chain, validity window, CRL revocation, signature) against the cached
DER — so a CRL published mid-stream (T7) applies to fingerprint datagrams immediately. A
fingerprint that misses the cache is a bounded, recoverable drop (`cert-miss`): the
sender's next priming or periodic full-cert datagram re-primes it. The cache is bounded
(128 entries — far above any real cluster; cleared and re-primed on overflow, which only
fully-verified certificates can reach).

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

## Amendment (2026-08-14): the signing identity is hot-swappable (issue #269)

The signer was a startup snapshot — the one piece of this plane the ADR 0032 reload never
reached. After a peer-bus leaf rotation the node kept signing gossip with the old key and
embedding the old certificate, which still chain-verified against the unchanged CA, so
nothing failed until the *old* leaf's `notAfter` — up to a full leaf lifetime after the
rotation, with no correlating change to point at.

The signing identity now lives behind a swappable `SignerSlot` (`swim_auth`), read once per
sealed datagram: the signer and its certificate fingerprint swap together (a
fingerprint-form datagram can never reference a leaf other than the one that signed it),
and the full-cert send counter resets on swap so the first post-rotation datagram primes
peers with the complete new certificate instead of drawing recoverable `cert-miss` drops.
The broker's reloader rebuilds the signer from the re-read leaf/key in the same atomic
validate-before-swap reload as the peer-bus TLS contexts (ADR 0040 T4), and the peer-bus
CA/cert/key joined the ADR 0033 file-watch scope. Mid-rotation coexistence needs no new
mechanism: verification is per-datagram against the CA and the receiver cache is keyed by
fingerprint, so old-leaf and new-leaf nodes accept each other while a fleet rolls one node
at a time. Rotating the **CA itself** remains restart-bound (the verifier and the CRL
check pin the startup CA). Delivered as 0022-T8; the deployment-facing account is in
ADR 0047's 2026-08-14 amendment.

## Amendment (2026-10-12): who is shown the certificate, and when (issue #971)

T6 decided *that* most datagrams carry a fingerprint. It left *who has seen the
certificate* to two rules: `Join`/`Sync` carry it, and so does every 16th datagram a node
seals — one counter per node, whoever the datagram is for. Only seeds are sent a `Join`.
So two nodes that are not each other's seed met through gossip and then spoke in pings
and acks, each dropping everything the other sent as `cert-miss` until one of its
datagrams to that peer happened to be its 16th overall; with N−1 destinations sharing
the counter, the wait grew with the cluster. A node probed during the wait did not
answer, was suspected, and after 4 s was declared dead.

That was the storm of false `SUSPECT`/`DEAD` the 12- and 14-node formations of the
durable scale-out campaign logged, and the pair of brokers one of which held the other
out for more than four and a half minutes. It reproduces without a cloud: fourteen
brokers on loopback, started the way the rig starts them, did not settle in 169 s, with
37 to 69 suspicions and 4 to 12 false deaths per broker, 115 to 193 `cert-miss` drops on
every broker that is not a seed — and one broker still dropping, at the end, the
datagrams of the peer it had buried 113 s earlier.

### What the simulation measured

`crates/mqtt-cluster/tests/swim_formation_sim.rs` forms N signed nodes over a virtual
clock and a lossless network, through the driver's real endpoint, with the rig's seeding
and start order (a founder, followers a few seconds apart, the founder restarted with
seeds). Sixteen seeds per size, each watched for 300 s after the last start; per-node
figures are the median seed's. "Settled" is the moment after which every node held every
other alive for good.

| N | | settled, median (worst) | not settled in 300 s | `SUSPECT` / node | `DEAD` / node | `cert-miss` / node | longest eviction |
|---|---|---|---|---|---|---|---|
| 10 | 1.1.0 | 80 s (263 s) | 1 of 16 | 22.6 | 2.0 | 86 | 293 s |
| | now | 4.5 s (8.0 s) | 0 | 0 | 0 | 0.1 | none |
| 12 | 1.1.0 | 194 s (252 s) | 6 of 16 | 34.4 | 4.3 | 114 | ≥ 300 s |
| | now | 6.6 s (9.3 s) | 0 | 0 | 0 | 0.1 | none |
| 14 | 1.1.0 | 215 s (294 s) | 4 of 16 | 50.6 | 8.1 | 143 | ≥ 300 s |
| | now | 7.5 s (11.9 s) | 0 | 0 | 0 | 0.1 | none |
| 20 | 1.1.0 | 252 s (286 s) | 9 of 16 | 94.0 | 32.6 | 222 | 264 s |
| | now | 15.9 s (19.1 s) | 0 | 0 | 0 | 0.1 | none |
| 30 | 1.1.0 | never | 16 of 16 | 153 | 126 | 330 | 277 s |
| | now | 26.3 s (32.1 s) | 0 | 0 | 0 | 0.1 | none |

At 3 and 5 nodes neither build raises a suspicion: most pairs include a seed. The 1.1.0
rows agree with what the rig's brokers logged (14 nodes: 19–73 `SUSPECT`, 3–12 `DEAD`,
100–260 `cert-miss` each).

Each cause was then removed on its own (14 nodes, same seeds):

| 1.1.0, with only… | settled, median (worst) | `SUSPECT` / node | `DEAD` / node | `cert-miss` / node | longest eviction |
|---|---|---|---|---|---|
| nothing changed | 215 s (294 s; 4 of 16 never) | 50.6 | 8.1 | 143 | ≥ 300 s |
| every certificate handed out in advance | 7.1 s (11.9 s) | 0 | 0 | 0 | none |
| the certificate on the first datagram to each destination | 16.5 s (119 s) | 1.1 | 0 (worst 0.6) | 6.8 | 110 s |
| a request to the sender of a `cert-miss` | 35 s (49 s) | 0.1 | 0.7 | 5.4 | 42 s |
| the re-greet counting probes that only helpers rescued (ADR 0016) | 113 s (161 s) | 25.4 | 9.5 | 77 | 131 s |
| a parting greeting when a tombstone is pruned (ADR 0016) | 60 s (74 s) | 37.7 | 6.2 | 99 | 35 s |

So the storm is entirely the certificate's distribution: with certificates known in
advance, 1.1.0's state machine forms a cluster of 30 without one suspicion, and in the
time the fixed build takes. The 4 s suspicion window is not what failed. (It does not
scale with the cluster, though, and that has a cost of its own: see ADR 0016's amendment
of the same date.)

### Decision

Three rules, all in the driver, none on the wire.

1. **The first datagram to each destination carries the full certificate**, whatever its
   kind (`SwimAuth::seal_to`): the node remembers which addresses it has sealed to since
   it started or last rotated its leaf. The every-16th refresh stays as it was.
2. **A member's new life un-primes it.** A restarted process has an empty cache, while
   every peer remembers having shown the old one its certificate. When the state machine
   sees a member at a higher generation — in the member's own datagram or in gossip —
   the driver forgets that it primed that address before it seals anything else, so the
   reply to the datagram that revealed the restart already carries the certificate.
   Without this rule the restarted founder of the table above was still suspected and,
   in a few seeds, declared dead for up to 16 s (30 nodes).
3. **A `cert-miss` is answered with a request.** The receiver sends the datagram's UDP
   source one `Join` — the frame that carries the sender's certificate and that every
   release answers with a `Sync` carrying its own — holding only the node's own claim.
   At most one per source address per second, sixteen per second in all, the sources
   remembered in a table of 256 entries (`swim_driver::CertRequests`). This is what
   heals the state rules 1 and 2 cannot see coming: a cache lost without a restart, a
   peer still on 1.1.0, a priming datagram lost on the network.

Rule 3 is the request the issue's reading called for; rules 1 and 2 are why it is
almost never needed: a formation of 14 now counts one or two `cert-miss` drops in all,
around the founder's restart.

**The wire is unchanged.** The request is an ordinary `Join`; a full certificate in a
ping or an ack is the cert-ref form T6 already defined and 1.1.0 already accepts on
every 16th datagram. There is no new message kind, field or version, so no protocol
number moves and no pinned bytes change. (A new kind was the other candidate; a 1.1.0
node cannot decode an enum variant it does not know and would count the datagram as
`decode`. It bought nothing the `Join` does not.)

**Size.** No run on either build sealed a datagram larger than 1,067 bytes (a 560-byte
leaf and six piggybacked updates), against a budget of 1,452 (a 1500-byte MTU less IPv6
and UDP headers): a ping with the certificate is no larger than the `Sync` that always
carried it. A request is under 800 bytes.

### What a datagram can trigger that has not been authenticated to a node

A `cert-miss` is decided after the cluster-key HMAC verifies and before any signature
can be checked — the certificate is what is missing. So:

- **Without the gossip key: nothing.** The datagram fails the HMAC and is dropped as
  `auth`, as before. No reply, no state.
- **With a valid HMAC and an unknown fingerprint** (a holder of the gossip key, or
  anyone replaying a captured fingerprint datagram at a node that does not hold its
  sender's certificate): the datagram is dropped **undecoded** — no membership statement
  in it is read, no member is added, nothing is acknowledged — and its UDP source,
  which nothing vouches for, is sent one `Join` within the limits above. That is the
  whole of it: per reflecting node at most sixteen datagrams of under 800 bytes a second,
  about five times the bytes of the datagram that drew each, to addresses the sender
  chose. The `Join` discloses the node's certificate, id and addresses, which every
  datagram it sends discloses anyway.
- **Every membership statement a node accepts is still verified** exactly as before:
  HMAC, certificate chain, validity, revocation, signature, and the certificate's CN
  against the claimed sender. Nothing in this amendment accepts anything.
- A key-holder who spends a node's sixteen requests a second on forged sources delays
  that node's learning of genuine certificates; rules 1 and 2, the every-16th refresh
  and ADR 0016's re-greet do not depend on the request and still apply — the node is
  then no worse off than on 1.1.0.
- The anti-replay window (ADR 0023) does not cover the request: it is keyed on the
  authenticated sender, which a `cert-miss` datagram does not have. The rate limit is
  the bound.

Memory per unauthenticated datagram: none unless a request is granted, then one address
string and a timestamp in a table that cannot exceed 256 entries. CPU: the HMAC, one
table lookup, and for a granted request one signature. Tested in
`swim_driver::cert_request_tests`.

### Mixed versions

A 1.1.0 node needs nothing new: it answers the `Join`, and it accepts a full certificate
on any datagram. What it lacks is its own half — it neither primes on first contact nor
asks — so:

- a cluster with **one node left on 1.1.0** forms like an upgraded one up to 14 nodes
  (no suspicion; settled within 12.1 s); at 30 nodes the old node was declared dead
  once in more than half the seeds, and was back within 31 s;
- **half way through**, the 1.1.0 nodes still storm among themselves as before, and a
  pair of them can still hold each other out; no pair with an upgraded node in it stays
  broken;
- **one upgraded node** among 1.1.0 nodes changes little — the storm is between the
  others.

A rolling upgrade does not re-form the cluster: nodes restart one at a time into a mesh
whose certificates are cached, and the restarting node is the upgraded one. The numbers
above are formations, the worst case.

### On real processes

The same formation with fourteen brokers on loopback (release binaries, signed gossip,
peer mTLS, the rig's seeding, the founder restarted with seeds), one run each:

| brokers | mesh whole on every broker | `SUSPECT` / broker | `cert-miss` / broker |
|---|---|---|---|
| 14 built without this change | not within 169 s | 37–69 | 115–193 (seeds: 0 and 60) |
| 14 with it | 8 s after the founder's restart, and for the 102 s watched | 0 | 0 (the restarted founder: 2, both answered with a request) |
| 7 with it, 7 on the released 1.1.0 binary | after 75 s, and for the 95 s more watched | 1–4 | upgraded: 1–7, and 37 of the 38 answered with a request; 1.1.0: 28–86 (the seed: 0) |

In the mixed run no broker rejected a datagram for any reason but `cert-miss`: the two
versions decode each other's gossip. `crates/mqttd/tests/swim_formation_proc.rs` keeps
the second row as a test, with twelve brokers.

### Observability

`mqttd_gossip_cert_requests_total` counts the requests a node sends. It follows
`mqttd_gossip_rejected_total{reason="cert-miss"}` while a certificate is being learned
and stops when it is. `cert-miss` climbing while requests stand still is the rate limit
holding; `cert-miss` climbing on one node with its member count one short was the
signature of the stuck pair.

### Not decided here

- Formation time is now what it is with certificates known in advance, and grows with
  the cluster (26 s at 30 nodes) because a `Sync` carries six updates, not the whole
  view: a node that restarts re-learns the others about one a second. Sending the view
  in several datagrams would shorten it and is a wire and amplification question of its
  own.
- What a publish does while one node holds a live peer dead is the hub's question, not
  gossip's; it loses acknowledged QoS 1 messages, and is issue #973.
