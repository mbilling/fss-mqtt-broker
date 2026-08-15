# ADR 0005 — Session affinity: relocate persistent sessions to their owner

- **Status:** Accepted
- **Date:** 2026-06-12
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0005-session-affinity.md](../delivery/0005-session-affinity.md) — plan, progress, and changelog
- **Related:** [ADR 0001](0001-session-durability.md) §5, `mqtt-cluster::placement`

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0005-session-affinity.md).

## Context

Workstream B gave every node a deterministic [`Placement`] ring:
`owner(client_id)` and an R-node replica set over the live membership. For the
cluster to deliver **sharded session capacity** — no node holding all sessions —
and to make cross-node takeover possible (workstream F), a *persistent* session
(`clean_session=0`) must live on its placement owner regardless of which node
the client's load balancer happened to land it on.

Today a persistent session lives on the landing node. A client that reconnects
to a different node finds none of its prior state (subscriptions, offline
queue), and the queue is stranded on whichever node it last used. Affinity
closes that gap.

MQTT 3.1.1 has no client-redirect mechanism, so the landing node cannot tell the
client "reconnect to node O." It must **relocate the session itself**.

### The security crux

The landing node authenticated the client (mTLS CN / password / JWT) and applied
connect-time ACLs. To relocate the session to the owner it must carry that
*established identity* to the owner — but the client's credential is not
forwardable: the certificate was consumed in the TLS handshake with the
*landing* node, and a JWT/password is a one-time CONNECT input the owner never
sees. So the owner cannot re-authenticate. **The landing node must vouch for the
client's identity to the owner.** That is a widening of intra-cluster trust and
is the decision this ADR exists to settle.

## Decision

1. **Scope to persistent sessions.** `clean_session=1` sessions hold no durable
   state worth relocating and are served on the landing node. Only
   `clean_session=0` sessions consult placement.

2. **Relocate by proxying over the mutually-authenticated peer mesh, not the
   client listener.** The landing node forwards the session's MQTT stream to the
   owner over the cluster bus (ADR 0002 mTLS). The client listener is the wrong
   channel: it would re-run client authentication the landing node already did
   (and can't, without the forwardable credential).

3. **Trust handoff: the landing node vouches for the authenticated identity over
   the peer link.** The owner accepts the vouched identity as established (as if
   it were a local certificate identity) because the frame arrived over a
   mutually-authenticated link from a node holding a valid cluster certificate.
   - **Why this is acceptable:** a node admitted to the mesh (ADR 0002/0004)
     already routes every cross-node publish and reads all cross-node traffic —
     it can already inject a publish as any topic/identity. Vouching for a client
     identity grants it no capability a malicious-but-admitted peer lacks. The
     trust boundary is the *cluster CA*, unchanged.
   - **Mitigations:** the owner records the vouching node id alongside the
     client identity in the audit trail (`auth.success` gains a `via=<node>`
     detail); peer node-id↔cert-CN binding (ADR 0004 §5) already ensures the
     vouching node is who it claims.

4. **Ephemeral until replication (workstream E).** With a single home and no
   replicated log yet, the owner's death loses its persistent sessions. This is
   the explicit, loudly-documented **ephemeral-sessions** mode — sharded
   capacity without durability across owner loss. Workstream E (the
   quorum-replicated log) upgrades it to durable; this ADR does not.

5. **Degrade, don't refuse.** If the owner is not `Alive` (or membership is
   unknown — single-node, or SWIM disabled), the landing node serves the session
   locally. Affinity is best-effort: better a locally-served session than a
   refused connection.

### As delivered (2026-08-15, issue #284): relocation is re-evaluated for a LIVE session

The decision above places a session at CONNECT and says nothing about what happens if
its group's ownership moves while the client stays connected. It moves: a node
readmitted after a roll rejoins gossip membership — and turns `/readyz` green — a couple
of seconds *before* it is back in the lease voter set, so its groups' leases are still
parked on the interim holder they were handed during its absence. A session that resumes
in that window is relocated onto the interim holder **correctly**, and is stranded there
seconds later when the lease legitimately returns. From then on the hosting node refuses
every publish toward it (`NotOwner`, publisher's ack withheld — honest, but unavailable),
and nothing re-decided the placement. Measured: unbounded, still wedged after two minutes,
on a cluster reporting fully converged and ready.

As delivered, a node that finds itself hosting a **live** persistent session whose group's
*committed lease* names another node **closes that connection** on the hub's sweep tick,
so §1's placement decision is re-run on the client's next CONNECT — the reconnect it
already knows how to do, immediately, rather than after a keepalive interval of dead air.
A v5 client is told why (`0x9C` Use another server); v3.1.1 sees the relay's half-close,
which is the "no client-redirect mechanism" this ADR's Consequences already acknowledge.
No Server Reference property is attached: placement holds **peer-bus** addresses, and
handing a client one would point it at the cluster's internal listener — the Consequences'
Server-Reference end-state still needs the client-facing address it does not have.

§5 (degrade, don't refuse) still wins where it applies: if the owner's peer-link address
is unknown, the next CONNECT here would be served locally anyway, so closing would only
loop — the session is kept, counted (`mqttd_session_rehomes_total{reason="unrelocatable"}`)
and gauged (`mqttd_misplaced_sessions`) instead. That is the one shape that can still sit
undeliverable, and it is now visible rather than silent.

A *transparent* re-relocation was rejected: the proxy is structurally one hop
(`run_framed` refuses to re-proxy, because a chain would loop), the CONNECT and its
CONNACK — with a `session_present` computed against the interim holder's state — are
already on the wire with no vocabulary for replaying them elsewhere, and a hand-off would
turn exactly-one-owner (ADR 0005/0007) from a structural property into a timing argument.
The close is single-writer by construction: the interim holder drops the session before
the client can learn to reconnect, and the reconnect is an ordinary CONNECT through the
existing takeover fence. Nothing durable moves — the session's queue is in its group's
replicated log, which the real owner holds.

**The close ends the CONNECTION and nothing else.** The first cut released the session's
in-memory routing in the same tick as the close, and that was an acked-but-dropped
publish: a lease move arms no settle window on *any* node (leases are pushed into the
shared placement ring by the cluster driver; nothing is told), so between "this node
stopped advertising the session's filters" and "the owner started" a publisher — typically
on a third node, which decides its fan-out purely from gossiped interest — matches nobody,
concludes nothing is owed, and is **acked** for a message no node stored. As delivered the
close therefore changes no routing, no interest and no settle state: it turns the session
into an ordinary *offline* persistent session on a node that no longer owns its group,
which is exactly what `release_moved_sessions` (ADR 0043 P2) has always handled, on its own
pre-existing scan cadence and paired with the only thing that clears a held ack. Until that
scan lands, the session still matches here, so every publish toward it fails its group-gated
append (`NotOwner`) and the publisher's ack is **withheld** — byte-identical to the honest,
pre-fix posture, and at every entry point: locally, as a peer's forward (answered *failed*),
and at a third node, where the fan-out reaches both this node and the new owner and the
first terminal verdict wins, so this node's failure withholds the ack even if the owner's
copy was stored first.

The cost is that for the length of that hold **both** nodes advertise the session's filters,
so publishers to them keep retrying briefly even once the session is healthy on its owner —
about a second inside a roll (whose membership change makes every node scan eagerly), up to
the 30 s reconcile cadence for a lease move with no membership change. Arming the eager
window at the close would shorten that to about two ticks and was **rejected**: it works by
making this node release *sooner*, and the release is justified only by evidence that this
node is not the owner, never by evidence that the new owner routes the session. Accelerating
it therefore widens the window in which no node advertises the session at all and a publish
is acked with nothing stored. An unbounded honest refusal beats a bounded lie. (That
unwitnessed release is a pre-existing property of `release_moved_sessions` — reachable with
no rehome in the story at all, when a client simply disconnects and its group's lease then
moves — and closing it honestly needs per-session hand-off evidence on the peer bus, which
carries filters rather than client ids. It is recorded as a follow-up in 0043-P6.)

**The Will fires on a rehome close.** `graceful` controls exactly one thing — will
publication — and a server DISCONNECT is not a client DISCONNECT: [MQTT-3.1.2-8] /
§3.14.4 delete the will only on a client DISCONNECT with reason `0x00`. Suppressing it
here would make the rehome the only broker-initiated close in mqttd that hides a will
(session takeover and `evict` both publish it, and issue #265 existed precisely because
broker-initiated closes were silently *not* publishing it — its "document the
suppression" exit was rejected as a spec violation). The cost is real and is stated
where operators read it: one LWT per rehomed session, so a roll — and every resize —
emits a burst of false "device offline" events, paced by the per-tick close cap. The
spec's own answer to "this close is not a death" is the **Will Delay Interval** (0x18),
which mqttd decodes and does not honour; honouring it in a *cluster* needs the delay and
its cancellation to survive the client reconnecting on a different node, which no peer
frame or durable record expresses today. That is the named follow-up, not a local
`graceful = true`.

## Consequences

- **Sharded session capacity** — the workstream-C scalability milestone — and
  the substrate for cross-node takeover (F).
- A new **cross-node data plane**: the session proxy, with its own lifecycle
  (open / relay / close), backpressure, and failure handling. Every packet of a
  proxied session crosses one extra node hop.
- A **documented widening of intra-cluster trust** (identity vouching), bounded
  by the existing cluster-CA trust root and recorded in the audit trail.
- The proxy is a *transitional* mechanism: MQTT 5 Server-Reference (deferred to
  the v5 codec) lets clients reconnect to the owner directly, retiring the relay
  for v5 clients.

## Alternatives considered

- **Remote `SessionStore` RPC** (keep the connection on the landing node, route
  only the session's store operations to the owner). Rejected as the first cut:
  it splits a session's routing from its storage across nodes, multiplying the
  moving parts (live delivery on one node, offline queue on another, interest
  registration ambiguous). Proxying keeps a session *whole* on one node, which is
  simpler to reason about and to make durable in E. The store-RPC shape may still
  return for the replicated backend, where the store *is* the replicated thing.
- **MQTT 5 Server-Reference redirect.** Cleaner long-term (no relay; the client
  connects to the owner directly) but needs the v5 codec and v5 clients.
  Deferred; the proxy serves 3.1.1 and v5 clients alike until then.
- **Refuse non-owner connections.** Breaks the single-address load balancer the
  shared-nothing design assumes. Rejected.
