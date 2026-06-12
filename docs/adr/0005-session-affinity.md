# ADR 0005 — Session affinity: relocate persistent sessions to their owner

- **Status:** Accepted (design); implementation phased
- **Date:** 2026-06-12
- **Deciders:** project maintainers
- **Related:** [ADR 0001](0001-session-durability.md) §5, [Cluster Durability
  Plan](../CLUSTER-DURABILITY-PLAN.md) workstream C, `mqtt-cluster::placement`

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

## Implementation phasing

1. **Live placement + ownership awareness** *(foundation)*: a shared, membership-
   driven `Placement` in the broker; persistent CONNECT consults it; the
   ephemeral limitation is logged when a session is served off its owner.
2. **The proxy data plane**: peer-mesh session relay with the vouched-identity
   handoff and audit; the owner runs the real session.
3. **MQTT 5 Server-Reference** as the eventual redirect-based replacement for the
   relay (with the v5 codec milestone).
