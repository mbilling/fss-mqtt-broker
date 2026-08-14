# Glossary

The vocabulary the rest of the documentation assumes. Two halves: **MQTT** terms any
broker uses, and **mqttd** terms specific to this one's clustering and security model.

## MQTT protocol

- **QoS 0 / 1 / 2** — the delivery guarantee, chosen per message. **0** is fire-and-forget
  (fastest, may be lost). **1** is at-least-once (may arrive twice; the common choice).
  **2** is exactly-once (slowest, a four-packet handshake). A subscriber's subscription can
  only *lower* a message's QoS, never raise it.
- **PUBLISH / PUBACK / PUBREC / PUBREL / PUBCOMP** — the packets that carry a message and
  its acknowledgements. QoS 1 is PUBLISH→PUBACK. QoS 2 is the four-packet exchange
  PUBLISH→PUBREC→PUBREL→PUBCOMP, which is what makes exactly-once possible.
- **CONNECT / CONNACK** — a client opens a session with CONNECT; the broker answers CONNACK
  with a reason code (0x00 success; 0x04/0x05/0x87 various refusals).
- **SUBSCRIBE / SUBACK / UNSUBSCRIBE** — a client registers topic filters; the SUBACK grants
  (or refuses, code 0x80) each filter at a QoS.
- **DUP flag** — set on a re-sent PUBLISH so the receiver knows it may be a duplicate.
- **Retained message** — the broker keeps the *last* message published to a topic and
  delivers it to anyone who subscribes later. One per topic; publishing an empty payload
  clears it. Answers "what is the current value?" without waiting for the next update.
- **Session** — what the broker remembers about a client between connections: its
  subscriptions and any messages queued while it was away.
- **`clean_session` (v3.1.1) / Clean Start + Session Expiry (v5)** — the flag that decides
  whether a session persists. A **clean** session forgets everything on disconnect; a
  **persistent** one does not, which is what makes offline devices work. In mqttd, a
  persistent session is the one that gets durable, replicated storage; a clean session is
  in-memory and cheaper. (See "durable sessions" below — persistence is the *client's*
  choice via this flag; durability is the *broker's* mechanism for honouring it.)
- **Last Will and Testament (LWT)** — a message a client registers at connect time that the
  broker publishes **if the client dies without disconnecting cleanly**. How you detect a
  device dropping off without polling.
- **Shared subscription** (`$share/<group>/<topic>`) — several subscribers join a named
  group and the broker gives each message to **exactly one** of them, spreading work.
  Ordinary subscriptions give *every* subscriber a copy.
- **Receive Maximum** — the cap on how many QoS>0 messages a client will accept in flight at
  once (v5 flow control). The broker holds the rest.

## mqttd clustering and durability

- **Durable sessions** — mqttd's default: a persistent session's state (subscriptions,
  queued messages, the in-flight QoS window) is written to disk and replicated across nodes,
  so an acknowledged message survives the loss of the node that accepted it. Turn it off
  (`MQTTD_DURABLE_SESSIONS=0`) for a lighter in-memory store. **Durable-on with no
  `MQTTD_DATA_DIR` is in-memory durability** — replicated but not on disk, so a correlated
  restart of a quorum loses acknowledged facts. Since issue #240 the broker **refuses to
  start** in that configuration; development and tests opt in explicitly with
  `MQTTD_ALLOW_EPHEMERAL_DURABILITY=1`, and only then does the loud warning fire.
- **Placement group** — the unit of ownership. Every client id hashes to one of a fixed
  number of groups; a group has one owner and one replica set, so ownership and consensus
  scale with the number of *groups*, not the number of sessions.
- **Owner / replica set** — the node that serves a group (its owner) plus the nodes that
  hold copies of its data (its replica set, size **R**, default 3). A durable write must
  reach a quorum of the replica set before it is acknowledged.
- **Quorum** — a majority of a set: `⌊N/2⌋+1`. A durable append commits once a quorum of the
  replica set has it on disk. Losing a minority is survivable; losing a majority is not.
- **Lease / epoch** — a group's ownership is a **lease** held by one node for a leadership
  term numbered by an **epoch**. A newer epoch **fences** an older one: followers reject
  writes tagged with a stale epoch, which is what stops a deposed-but-alive owner from
  corrupting the log.
- **Voter / learner** — the lease consensus runs among a bounded **voter** set (default 5);
  every other node joins as a **learner** that still receives the lease log and can own and
  serve groups, so consensus cost stays fixed as the cluster grows.
- **SWIM gossip** — SWIM (Scalable Weakly-consistent Infection-style process-group
  Membership) is the membership protocol nodes use to discover each other and detect
  failures: each node periodically pings a random peer and spreads what it learns
  epidemically ("gossip"). Every datagram is HMAC-authenticated under a shared key
  (`MQTTD_SWIM_KEY`).
- **HRW / rendezvous hashing** — HRW (Highest Random Weight) is how a session is
  assigned an owner node: every node's id is hashed against the session's key and the
  highest score wins. Deterministic (every node computes the same answer with no
  coordinator), and when membership changes only the sessions whose winner changed
  move — minimal rebalancing.
- **CP / CAP** — the CAP theorem: during a network **P**artition a distributed store
  must choose between **C**onsistency (every answer is the agreed one) and
  **A**vailability (every request gets an answer). mqttd's durable plane chooses
  **CP**: the side of a partition without a quorum refuses new durable writes and
  serves last-committed values (staleness, never divergence) until the partition
  heals.
- **Founder / seed** — the first node (started with no seeds) *founds* the cluster; every
  other node *seeds* to an existing member to join. The `clusterEstablished` guard prevents
  a restarted founder whose volume was lost from founding a *second* cluster.
- **Brownout** — a backpressure mode: above a disk or memory watermark, writes that **grow**
  state (new sessions, new retained topics, offline enqueues) are refused while reads,
  *subscriber* acks, deletes and resumes continue. The acks that continue are the ones that
  shrink state; a **publisher's** `QoS` ≥ 1 ack is refused above the mark (v5 `0x97`,
  v3.1.1 no ack and a close) rather than granted for a message the store will not take —
  when the refusing session owner is a peer node the refusal crosses the peer bus as a
  verdict (a link to an older build, mid-rolling-upgrade, withholds the ack and closes
  instead). It is a watermark, not a hard ceiling — the container limit is still the real
  bound.
- **allow-covers / deny-overlaps** — the ACL matching rule. An `allow` grant applies if it
  *covers* the topic (its filter matches); a `deny` applies if it *overlaps* (matches any
  part), so deny is deliberately broader than allow — the safe direction for authorization.

## Security and PKI

- **TLS / mTLS** — TLS (Transport Layer Security) encrypts and authenticates the
  connection; the client verifies the server's certificate. **mTLS** (mutual TLS) adds
  the reverse: the server also requires and verifies a *client* certificate, so the
  connection itself proves who the client is. mqttd's cluster bus is always mTLS;
  client listeners opt in via `MQTTD_TLS_CLIENT_CA`.
- **CA / CN** — a CA (Certificate Authority) is the keypair that signs certificates;
  trusting the CA is what makes a presented certificate mean something. The CN
  (Common Name) is the certificate's subject name — mqttd's default source of a
  client's identity, and the binding for a cluster node's id.
- **ACL** — an Access Control List: the deny-by-default TOML policy mapping an
  authenticated identity to the topics it may publish or subscribe to
  (`MQTTD_ACL_FILE`).
- **JWT / OIDC** — a JWT (JSON Web Token) is a signed token carrying identity claims,
  verified against a static key (`MQTTD_JWT_*`). OIDC (OpenID Connect) is the
  discovery layer on top: the broker fetches the issuer's published keys (JWKS) and
  follows rotation live (`MQTTD_OIDC_*`).
- **CRL / OCSP** — two ways to revoke a certificate before it expires. A CRL
  (Certificate Revocation List) is a signed file listing revoked certificates,
  published by the operator and hot-reloaded by mqttd; OCSP (Online Certificate
  Status Protocol) asks a responder per handshake instead — not yet supported here.
- **EKU / `clientAuth`** — Extended Key Usage, the X.509 field naming what a
  certificate may be used for. rustls requires the `clientAuth` EKU on client
  certificates (and `serverAuth` on server ones); a client certificate minted without
  it is rejected at the handshake — see
  [TROUBLESHOOTING](TROUBLESHOOTING.md#a-client-with-a-certificate-is-rejected-mtls).
- **PSK** — a pre-shared key: TLS authenticated by a symmetric secret both sides
  already hold, common on constrained devices. mqttd does not offer PSK cipher
  suites; X.509 or token authentication is the path.
- **HMAC** — a hash-based message authentication code: a keyed hash proving a message
  was produced by a holder of the shared key and not altered. What authenticates
  every SWIM gossip datagram (HMAC-SHA256).

## Supply chain

- **cosign / SLSA / SBOM** — release-integrity tooling. **cosign** signs each artifact
  (keylessly, via GitHub OIDC); **SLSA provenance** attests what commit and workflow built
  it; the **SBOM** (Software Bill of Materials, CycloneDX format) lists every dependency.
  All three ship with each release and are verifiable with `cosign verify-blob`. Skippable
  if you trust the source; there for when you must prove it.
- **distroless** — a container base image containing no shell, package manager or libc —
  nothing but the binary and CA certificates. Shrinks the attack surface (nothing to
  exec) at the cost of no in-container debugging; it is why mqttd's health checks are a
  broker subcommand (`mqttd --probe`) rather than `curl`.
- **redb** — the embedded, pure-Rust, ACID key-value store (like SQLite in spirit,
  key-value in shape) that `MQTTD_DATA_DIR` persistence is built on. In-process — no
  database server to run.
