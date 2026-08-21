# 0072. Per-message durability selection: the `mqttd-durability` user property

Date: 2026-08-21
Status: Accepted

## Context

mqttd's differentiating promise is **acked means durable, cluster-wide**: a
QoS ≥ 1 PUBACK is issued only after the message is fsync'd and replicated to a
quorum. That guarantee has a measured price (SCALE-CURVE.md: ~2k acked msg/s
per owner against a ~2.2k/s disk-barrier floor; a 3-node quorum pays the
slowest replica's disk), and not every message a real fleet publishes is worth
it — telemetry that is superseded every second does not need what a billing
event needs. Operators asked for a way to trade durability for latency
**per message**, publisher-chosen — framed as CP-vs-AP, though what is actually
selectable is the ack's durability meaning; partition behavior is unchanged.

The project has twice rejected *silent* weakening (ADR 0017: a durable attach
"never downgrades"; ADR 0057 §2: the fallback is "a flag to **disable** it —
loudly — not one to enable it"), and once explicitly reserved this exact
feature: ADR 0018 — a relaxed mode "(group-commit / periodic fsync) MAY be
offered later as an **opt-in, loudly logged**". Group commit shipped as pure
optimization (ADR 0027, ADR 0071); this ADR is the reserved opt-in, made
per-message and publisher-explicit.

## Decision

**A publisher may weaken its own ack, per message, under a double opt-in.**

1. **The property:** `mqttd-durability` (MQTT 5 user property on PUBLISH),
   values `quorum` | `local` | `relaxed`. Anything else — including absence,
   unknown values, and every v3.1.1 publish (v3 has no user properties) — is
   `quorum`, today's full contract. The property is forwarded to subscribers
   unaltered, as MQTT-3.3.2-17 requires; it is informative downstream.
2. **The tiers** (what the PUBACK/PUBREC means):
   - `quorum` — fsync'd on a majority of the replica set, cluster-wide.
     Unchanged, the default.
   - `local` — fsync'd on the **owner's own durable copy** (required acks = 1,
     and the self-ack still counts only once durable, ADR 0042 T8 — single-copy,
     never zero-copy). Replication to followers continues **detached,
     best-effort**; losing the owner's disk before it completes loses the
     message. Loss window: one disk.
   - `relaxed` — accepted and **submitted**: every durable append is queued to
     its session lane and proceeds with full quorum semantics, every forward
     and the retained commit still run — but the ack does not wait for any of
     it. Loss window: a broker crash in the following instants. Ordering is
     preserved (the message still rides the lanes); nothing is structurally
     skipped; only the ack gate releases at `local_done`.
3. **The operator opt-in:** `MQTTD_ALLOW_RELAXED_PUBLISH` (presence = on;
   `[durable] allow_relaxed_publish`), default **off**. Without it the property
   is ignored and the publish receives the full quorum path — **stronger than
   asked, never weaker**. Set it on every node of a cluster (remote-owned
   appends honor the owning node's setting).
4. **Operator rails outrank publisher wishes.** The min-replicas write floor
   (issues #167/#239) still gates **every** tier: `local` weakens what the ack
   waits for, not which replica sets may accept writes. A brownout refusal
   decided at the plan pass still refuses a relaxed publish — a stated-policy
   refusal is never bypassed by asking for less durability.
5. **Placement:** the tier is derived where it is acted on — at the hub's ack
   gate for `relaxed`, and inside `ReplicatedSessionStore` for `local` (the
   property rides in the stored record, so a remote-owned append derives the
   same tier on the owning node with **no peer-protocol change**; the strict
   peer codec cannot gain frame fields, and does not need to).
6. **Observability:** `mqttd_publish_tier{tier}` counts gated publishes by
   tier; non-default tiers appear only under the opt-in.

## Consequences

- The canonical promise gains one clause: *acked means durable, cluster-wide —
  unless the publisher explicitly requested a weaker tier for that message and
  the operator explicitly allowed it.* README and COMPARISON.md state it.
- QoS 2's PUBREC gate follows the same tier (it rides the same pending-publish
  completion), so exactly-once retains its wire semantics while its
  durability anchor weakens accordingly for opted messages.
- A `relaxed` publish that is already acked can no longer be refused; failures
  in its background obligations surface in metrics/logs, not to the publisher.
  That asymmetry is the tier's definition, stated here once.
- Session replay after the loss window may miss relaxed/local messages that
  were acked — the publisher accepted precisely that trade per message.

## Alternatives considered

- **Global no-fsync mode** (`Durability::Eventual` everywhere): one silent
  asterisk on every ack — the exact shape ADR 0057 §2 rejects. Refused.
- **QoS-based inference** (QoS 1 = relaxed, QoS 2 = quorum): overloads spec
  semantics that mean delivery assurance, not storage durability. Refused.
- **A per-listener or per-topic config default**: moves the choice away from
  the party paying the latency; may still arrive later as policy *bounding*
  (e.g. an ACL forbidding tiers per principal), which composes with this.
- **A new refusal reason code** for disallowed tiers: rejected for v1 — the
  ignore-and-upgrade behavior is strictly safer and needs no wire vocabulary.
