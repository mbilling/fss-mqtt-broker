# ADR 0025 — Boundary MQTT bridge to brokers in other security zones

- **Status:** Accepted
- **Date:** 2026-06-23 (accepted 2026-06-25)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0025-boundary-bridge.md](../delivery/0025-boundary-bridge.md) — plan, progress, and changelog
- **Related:** [ADR 0004](0004-identity-and-authentication.md) (the deny-by-default identity
  + ACL posture the bridge's per-side least-privilege credentials extend across the
  boundary), [ADR 0002](0002-transport-security.md) (the TLS/mTLS the upstream connections
  reuse), [ADR 0010](0010-shared-subscriptions.md) / [ADR 0015](0015-cluster-shared-subscriptions.md)
  (the shared subscriptions the bridge uses for HA without duplicate forwarding),
  [ADR 0008](0008-mqtt-5-codec.md) (the codec the client side reuses),
  [ADR 0020](0020-metrics-and-observability.md) (the registry the bridge metrics report
  to). **Contrast:** [ADR 0003](0003-gossip-authentication.md)/[0005](0005-session-affinity.md)/[0006](0006-consensus-and-replication.md)/[0007](0007-durable-store-integration.md)
  are the *cluster* mesh — one logical broker within one trust domain; the bridge is the
  opposite tool, a loose crossing between trust domains.

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0025-boundary-bridge.md).

## Context

The cluster mesh (SWIM gossip, peer links, durable replication — ADR 0003/0005/0006/0007)
makes N nodes behave as **one logical broker within one trust domain**. It is the wrong
tool for connecting to a broker in a **different security or administrative zone**: a
partner's broker, a cloud IoT platform, an edge site forwarding up to a central broker, or
a heterogeneous broker (Mosquitto/EMQX/…) that cannot speak the internal peer protocol.

The requirement is a **boundary bridge**: forward a *configured* set of topics between this
broker and one or more external brokers, with a security layer deciding which topics flow
to which broker. Crucially, each topic mapping must be able to be **unidirectional** — a
one-way crossing (data-diode-like) is a primary security control, not a convenience, so it
must be enforceable, not merely a setting.

## Decision

Build the bridge as a **standalone component that is an MQTT client to both sides**, not an
in-process broker plugin. The boundary crossing is thereby a small, isolated, auditable
unit with its own identity — which is the entire point of a boundary.

### 1. A separate component, not a plugin

An in-process plugin would make the broker process hold live connections and credentials to
**both** zones at once: compromise the broker and you compromise the crossing — defeating
the boundary. A separate process has its own identity, credentials, and network namespace;
its failure domain is contained (a bridge fault cannot destabilize the broker); and
one-wayness can be enforced **below** the application (firewall/diode, per-side
credentials), which an in-process socket cannot offer. In-process bridging is fine for
*trusted federation*; this is explicitly a *security boundary*, so it is separated.

The bridge ships as a `mqtt-bridge` crate/binary, reusing `mqtt-codec` and `mqtt-net` (TLS)
for the client side — it is a client, not a second broker.

### 2. Client to both sides; not a cluster member

The bridge connects to the local cluster as an ordinary MQTT client (to a node or a service
VIP) and to each external broker as a client. It does **not** join SWIM/Raft — it is outside
the cluster's trust and failure domain by design, and cannot affect membership or consensus.
Because the cluster already routes cluster-wide (cross-node delivery, shared subscriptions),
subscribing on any one node yields the whole cluster's stream for the bridged topics, with
no internal coupling.

### 3. Config model: many upstreams, per-rule direction

Configuration (TOML) declares one local-cluster connection and **N upstreams**, each with
its own URL, TLS/mTLS identity, and credentials. Each upstream carries a list of **topic
mapping rules**, every rule with: a **direction** (`out` = local→upstream, `in` =
upstream→local, `both`), a topic **filter**, an optional **remap** (strip/prefix), and a
**QoS**. Forwarding is **deny-by-default**: only topics matching a configured rule cross.

### 4. Unidirectional enforced in depth (the security control)

A one-way rule is enforced in three independent layers, not just config:

1. **Config** — the rule's direction is `out` (or `in`) only.
2. **Code** — for a one-way rule the bridge *never opens* the reverse path: an `out` rule
   subscribes only on the local side and publishes only on the upstream; it never subscribes
   on the upstream for that topic.
3. **Credentials + network** — the bridge's account on each broker is least-privilege:
   publish-only or subscribe-only on the allowed topics (deny-by-default, ADR 0004 posture),
   so even a code fault cannot open the reverse path; and the deployment can firewall the
   return path entirely. Separation (decision 1) is what makes layers 2–3 enforceable.

### 5. HA via cluster-side shared subscriptions

For high availability without duplicate forwarding, run ≥2 bridge instances subscribing on
the **cluster side** via a shared subscription (`$share/<group>/<filter>`, ADR 0010/0015):
the cluster load-balances the stream across them (dedup for free), and a **persistent
session** means a brief bridge restart does not drop messages. No bridge-side election is
invented — it reuses existing broker features.

### 6. Loop prevention: a bridge hop counter

Structural prevention comes first from **directionality** and topic **remap** — a one-way
rule cannot echo, and a remap (local `telemetry/#` → upstream `ourorg/telemetry/#`) keeps a
forwarded message from matching the rule that would send it straight back. But remap
discipline alone cannot catch a cycle through *several* bridges (A→B→C→A), so every
forwarded message also carries a hop counter that bounds **any** loop to a finite length:

- An MQTT 5 user property **`fss-bridge-hop-count`** records how many fss bridges a message
  has traversed. On each forward the bridge reads it, **increments** it (initialising it to
  `1` when absent — this is the first bridge hop), and republishes with the new value.
- A bridge **refuses to forward** a message whose hop count has reached the configured
  **`hop-count-limit`** (a bridge setting): it drops the message and records the drop under
  reason `hop-limit`. A loop therefore self-terminates within a bounded number of hops
  regardless of any remap mistake. The default limit is conservative (a small TTL, e.g. `8`)
  and must be set at least as high as the longest *legitimate* bridge chain; lower it for
  stricter boundaries.

Because MQTT 5 user properties do not exist in MQTT 3.1.1, the hop counter survives
end-to-end only when every broker and bridge on the path speaks MQTT 5; across a 3.1.1
boundary the property is stripped, so loop-bounding there falls back to the structural
direction + remap discipline (which still prevents the immediate echo). The bridge logs this
limitation for a configured 3.1.1 upstream so it is never a silent gap.

### 7. Store-and-forward across transient outages

The bridge tolerates a side being briefly unreachable: a **bounded, disk-backed spool**
holds messages for a down side and replays them on reconnect, within a configured cap
(bounded like the broker's offline queues, ADR 0001 §6 — never grow without limit). The
bridge is, in effect, "an offline client" to whichever side is down; persistent sessions on
both brokers cover the rest. Delivery is **at-least-once** for QoS≥1 rules; exactly-once
across two independent brokers is not promised.

**As delivered — the durability contract ([ADR 0060](0060-bridge-durability-and-ack-contract.md)).**
This section originally left *when* the bridge acknowledges the source unspecified, and the
implementation acked on arrival — which made the at-least-once promise above untrue, because the
source dropped its copy before the bridge had done anything durable with the message. The
contract is now explicit:

- **Acknowledge only after durable acceptance.** The source PUBACK is sent once the message is
  spooled (fsync'd on commit) or dispatched to a live destination — never on arrival. A
  destination that cannot accept it withholds the ack entirely, so the source keeps its copy and
  redelivers. *Residual:* on the fast path the ack fires at dispatch, not on the destination's
  own PUBACK; closing that window is ADR 0060 T8.
- **Overflow: refuse, do not shed.** At the cap a `QoS`≥1 spool **refuses the newest** message
  rather than dropping the oldest, because everything already spooled was acknowledged to the
  source — shedding it would lose a message the source believes was delivered. A `QoS`-0-only
  bridge still drops the oldest (`QoS` 0 promises nothing, and a stalled crossing is worse).
- **Never silently non-durable.** A `QoS`≥1 rule with no durable spool is refused at startup
  unless the operator explicitly opts into ephemeral operation; the old silent in-memory
  fallback is gone.
- **Losses are audited.** A shed message emits an audit record (topic + reason), not just a
  counter — a lost auditable crossing must leave a trail.

### 8. Security posture and observability

Each upstream gets a **distinct mTLS identity** (ADR 0002); secrets arrive by file/env, never
inline; an **audit record** notes what crossed and in which direction; and the bridge
exports **metrics** (forwarded/dropped per upstream+direction, lag, reconnects) to the same
registry as the broker (ADR 0020), so the crossing is observable.

### 9. Placement is a deployment choice

Being separate, the bridge can sit wherever the security architecture wants the crossing —
co-located sidecar → DMZ pod → dedicated boundary host with separate interfaces. For a true
zone boundary it should **not** default to running next to cluster nodes.

## Consequences

- **Good:** the boundary crossing is a small, isolated, auditable unit; unidirectional flow
  is a real (multi-layer-enforced) control, not a setting; it interoperates with *any*
  external broker; HA reuses shared subscriptions; and a bridge fault cannot touch the
  cluster.
- **Cost:** a new deployable to operate — its own config, secrets, spool, and observability;
  one network hop; and store-and-forward is the bridge's own concern rather than the
  broker's session store.
- **Risk:** this is a security-crossing component, so it is built **test-first** with
  adversarial tests — a one-way rule must *never* leak the reverse direction, loops must be
  impossible, ACL denial must hold, and reconnect/spool must not lose or duplicate beyond
  the QoS contract. Same bar as ADR 0003/0004/0022/0023.

## Alternatives considered

- **In-process plugin (Mosquitto-style bridge).** Convenient and lower-latency, and the norm
  for *trusted* federation. Rejected for a *security boundary*: the broker would span both
  zones (large blast radius), one-wayness could not be enforced below the app, a bridge fault
  could destabilize the broker, and a clustered deployment would need internal owner-election.
- **Stretch the cluster mesh across the boundary.** Wrong by construction: you do not extend
  SWIM membership or Raft consensus into another trust/failure domain, and the far broker may
  be a different implementation entirely.
- **In-broker bridge owned by the lease/placement layer** (elect one node to bridge). Reuses
  existing machinery, but it is still in-process (broker spans zones) and couples the crossing
  to broker internals. The standalone component with shared-subscription HA is cleaner and
  isolatable.
- **Bidirectional-only.** Rejected: unidirectional is a primary requirement and must be
  enforceable across config, code, credentials, and network — which only a separate,
  least-privilege component delivers.

## Amendments (2026-08-11) — fidelity, durability & security gaps found in a code audit

A code-level audit (epic #186) of the delivered `mqtt-bridge` against this record confirmed a
set of fidelity/durability/security costs that the original decision did not state. Two are
architectural and get their own records; the rest are recorded here with their fix.

### A. HA covers only outbound; per-topic ordering forfeited → [ADR 0059](0059-bridge-ha-topology-and-ordering.md)

§5's shared-subscription HA is correct only for `out` rules (the subscribing side is our
cluster). For `in`/`both` rules the subscribing side is the foreign broker, so two instances
**double-deliver** inbound, and `$share` load-balances per message so **per-topic ordering** is
lost. §5 is superseded for the HA topology by ADR 0059 (hash-partitioned rule ownership; `$share`
demoted to an opt-in local optimisation).

### B. Ack ordering unspecified; at-least-once not actually held → [ADR 0060](0060-bridge-durability-and-ack-contract.md)

§7 promised at-least-once but the bridge PUBACKs the source **before** the spool commit, silently
falls back to an in-memory spool, and does not audit drops. ADR 0060 specifies ack-on-durable,
explicit fsync, no-silent-non-durable, and audited overflow.

### C. Retained state does not cross (RAP), recorded here

A plain subscription clears the RETAIN flag `[MQTT-3.3.1-9]`, so retained state never entered the
far broker's store, and the reconnect replay re-published the retained set as fake-live traffic.
**Decision:** set **Retain As Published** (MQTT 5 §3.8.3.1) on the bridge's ingress subscriptions
and copy the source retain bit onto the forwarded PUBLISH; keep **retain-handling = 0** so the
reconnect replay is an idempotent retained re-sync (not a fake-live storm); add a per-rule
**`outgoing_retain = false`** escape for far brokers that cannot handle retained (e.g. AWS IoT
Core). This matches the settled cross-broker consensus — Mosquitto (RETAIN_AS_PUBLISHED +
SEND_RETAIN_ALWAYS, plus `bridge_outgoing_retain`), EMQX (`retain_as_published` on ingress),
HiveMQ (`preserve-retained`). Accepted residual: one duplicate *live* delivery of each retained
value downstream per reconnect. Tracked as #189.

### D. Spool at rest — encrypt the volume, not the payload (deployment requirement), recorded here

§8 mandates distinct mTLS identities and a hash-chained audit, but the store-and-forward spool
(§7) holds cross-zone payloads in cleartext on the boundary host — a plaintext copy of everything
crossing the boundary, on the most-exposed box in the topology.

**Decision:** the spool relies on **volume/disk encryption at rest**, made an explicit
**deployment requirement**, not application-level payload encryption. This is exactly how the
field handles data-at-rest — Mosquitto (`mosquitto.db`), EMQX and HiveMQ (RocksDB/LMDB) all
persist their message stores in the clear and require the underlying volume to be encrypted.
App-level spool encryption was rejected as a non-standard deviation that is also *less* complete
(it leaves logs, swap, temp files and core dumps, which carry the same payloads, exposed) and
adds a key-management burden inside a security-boundary component. The requirement is stated
where an operator provisions storage: the Helm chart `persistence.storageClassName` (point it at
an encrypted StorageClass), the demo `docker-compose.yml` spool volume, and
[docs/BRIDGE.md](../BRIDGE.md) ("Encrypt the spool at rest"). For callers who need the payload
opaque to the bridge *itself*, end-to-end payload encryption by the publishers is the orthogonal
answer — the spool then holds ciphertext for free. Tracked as #190.

### E. Loop-prevention hop counter is attacker-controlled (§6), recorded here

The `fss-bridge-hop-count` user property is trusted verbatim from any publisher, so an
authenticated client can stamp it at the limit to self-censor across the boundary — a selective
denial-of-crossing no topic ACL catches, dropped without an attributable audit record.
**Decision:** on ingress from anything that is not an authenticated **bridge identity**, strip/
reset the counter before it is trusted; only believe an inbound counter over a link whose peer is
a known bridge account; and emit a labelled audit event on the hop-limit drop. Tracked as #191.

### F. Honest limitations now stated (not implied)

- **`both`-rule remap has no inverse** — the same strip/prefix is applied both ways, so a
  remapped `both` rule mis-maps the reverse direction. Fix: inverse remap on the reverse leg, or
  reject `both`+`remap` at validation (#192).
- **Reserved filters accepted** — `$SYS/#` and `$share/...` pass config validation; they are now
  refused (#193).
- **No trusted internal federation.** This component is a *security boundary* by design (§1);
  two of your **own** clusters bridged through it inherit its lossy semantics (QoS 2→1 downgrade,
  hop/remap defences). A trusted in-process federation path remains an open question, not a
  delivered capability.
- **Foreign-broker capability dependence.** Retained crossing, `$share`, and MQTT 5 user
  properties (the hop counter) all depend on what the far broker supports; where it does not, the
  bridge falls back or strips, and logs it. AWS IoT Core is the recurring example.
