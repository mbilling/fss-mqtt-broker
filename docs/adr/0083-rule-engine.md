# ADR 0083 — A rule engine on the publish path: EMQX rule SQL, evaluated once per message where it lands

- **Status:** Accepted
- **Date:** 2026-10-07
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0083-rule-engine.md](../delivery/0083-rule-engine.md) — plan, progress, and changelog
- **Supersedes in part:** [ADR 0063](0063-external-consumer-integration.md) §1, its "no
  built-in rule engine" clause. Its "no built-in Kafka/webhook sinks" clause and §2's
  consumer-group pattern for sinks stand.
- **Related:** [ADR 0018](0018-on-disk-persistence.md) / [ADR 0042](0042-durable-plane-stress-harness.md)
  T9 (an acknowledgement is released only for a message the broker owns; the pending-publish
  gate), [ADR 0082](0082-bounded-hub-ingress.md) (ingress credit and the FIFO data lane),
  [ADR 0015](0015-cluster-shared-subscriptions.md) (cluster routing a derived message rides),
  [ADR 0030](0030-user-property-forwarding.md) (an original crosses the broker byte for
  byte), [ADR 0032](0032-hot-reloadable-security-policy.md) / [ADR 0046](0046-file-based-configuration.md)
  (validate-before-swap reload), [ADR 0020](0020-metrics-and-observability.md) (label
  cardinality), [ADR 0051](0051-evaluation-readiness.md) (the migration converters), the
  2026-08-13 review panel (row 1: "Rule-engine gap erases licence savings").

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0083-rule-engine.md).

## Context

ADR 0063 reaffirmed that mqttd would not ship a rule engine and documented a substitute:
every rule becomes an MQTT consumer the adopter writes and operates. That substitute is
sound for sinks, where it still stands. For the commonest rules it is expensive: a filter,
a reshape and a re-route between topics. Each is a service to deploy, monitor and keep
consistent with the broker's QoS. The 2026-08-13 review panel's EMQX migrator ranked that
cost first, and the maintainers asked for an in-broker rule engine with four requirements:

1. **Realtime processing of messages passing through the broker.**
2. **Linear scaling, as the broker already scales**: flat per-node capacity from 3 to 10
   nodes, 114k msg/s QoS 0 per 4-vCPU node.
3. **QoS 0, 1 and 2.** A rule's output must not weaken the guarantee its input had.
4. **A SQL-like syntax, similar to or compatible with EMQX's or HiveMQ's.**

Two incumbent designs were candidates for (4). **EMQX's rule engine** is SQL: `SELECT …
FROM "<topic filter>" WHERE …`, plus `FOREACH`, `CASE` and about 125 documented built-in
functions, with `republish` and `console` as built-in actions. Its grammar (`rulesql`) and
runtime are open, and its documentation gives worked examples with expected outputs.
**HiveMQ Data Hub** is not SQL. Its policies are JSON documents with JavaScript transform
scripts, so it does not meet (4) and would put a JavaScript runtime in the broker.

Three facts about mqttd constrain where a rule can run. The **hub is a single-threaded
actor**. At the measured knee its loop is near half a core, and the README names it as
the next limit, so per-message SQL on the hub would make it the scaling ceiling, which
fails (2). A **QoS ≥ 1 acknowledgement is released only for a message the broker owns**
(ADR 0018, ADR 0042 T9); a message a rule produces must not be the one exception. And the
hub's **data lane is FIFO per connection, bounded by ingress credit** (ADR 0082).

## Decision

1. **mqttd has a rule engine, and its language is EMQX's rule SQL.** The grammar and its
   precedence follow `rulesql`, and evaluation follows `emqx_rule_runtime`: lookup order,
   lazy payload JSON decoding, `undefined` against `null`, comparison coercions, alias and
   path semantics. The built-in functions are EMQX's, by name and behaviour, and the
   actions are `republish` and `console` with EMQX's argument names and defaults. mqttd
   departs from EMQX only where this record or safety requires it, and every departure is
   listed in [docs/RULES.md](../RULES.md#differences-from-emqx). EMQX's documented
   examples are the test oracle. The grammar accepts a small superset (`AND` in `SELECT`,
   keywords as path segments) only where the meaning is unambiguous. The engine is the
   `mqtt-rules` crate, which holds no broker state and does no network I/O: it reads only
   its rules file, the clock and a random source.

2. **A client publish is evaluated on its connection task, after the ACL and before the
   hub, once, on the node it arrived at.** Connection tasks run in parallel on every core,
   so rule work scales with connections and with nodes as ingress does, and the hub loop
   runs no SQL for client publishes. A copy forwarded from a peer is never evaluated
   again. The one hub-side evaluation is the **Will**, because the hub is what publishes
   it; Wills are rare.

3. **A derived message is an ordinary publish with no publisher, and never re-enters the
   rule engine.** It is routed, forwarded, retained, queued and replicated like any
   publish. It has no publisher, so No Local does not apply, as in EMQX where the rule is
   the sender. It is not evaluated again, so no rule can loop: EMQX's `direct_dispatch`,
   always on. The original is never altered or suppressed (ADR 0030 holds): a rule is a
   tap.

4. **A QoS ≥ 1 publish's acknowledgement waits for what its rules produced, and
   answers with the original's fate.** A publish and its derived messages reach the hub
   as one command; the hub routes the original first and routes the derived messages
   only if it accepted it. Each derived message at QoS ≥ 1 behind a gated original gets
   its own acknowledgement gate, and the PUBACK/PUBREC is released once every gate has
   answered, so an acknowledged publish's derived messages were each stored where owed
   or have failed and been counted. **The answer is exactly the original's own** —
   accepted, refused, or withheld when its fate is unknown. A derived message that
   fails (refused under a brownout, or of unknown fate: a failed durable write, a
   peer's refusal after a local copy was stored) is counted as a failed action and never
   changes the answer. An earlier draft withheld whenever the original and a derived
   message disagreed, and then whenever a derived message's fate was unknown. Both
   re-delivered an already-delivered original on every retry for as long as a sticky
   condition lasted (a brownout, a peer's brownout), and for QoS 2 the held-unacked
   dedup record made each resend a first sighting (review of PR #871). Inbound QoS 2
   deduplication precedes evaluation, so a rule fires once per QoS 2 message. A QoS 0
   original gates nothing. Client/session events and Wills hold back no acknowledgement,
   because there is none to hold: the messages they derive are routed ungated, as a
   Will is, each action counted `ok` once the hub has routed it (a durable copy a
   brownout refuses is counted as a drop, as a Will's is), and hold no entry in the
   pending-publish table, so a burst of events cannot evict a client publish's entry
   and withhold its ack (review of PR #871). A graceful shutdown awaits a hub barrier
   that answers once everything sent before it is dispatched, no durable append is in
   flight and no publish awaits an answer; while the broker drains, the hub gates what
   rules derive from events itself, so a forward to another node is acked and waited
   for (a peer in a brownout that refuses one is sent it again unacked, which it delivers
   live: nobody would retry it). What the drain's own disconnects derive is stored, here
   and on the nodes it was forwarded to, before exit — except on a node draining in a
   brownout, which does not gate them (a brownout refuses a gated publish owing a durable
   copy outright, live copies and all), past the pending-publish table's bound (the
   evicted are not waited for, and the drain logs how many), and when a peer link stays
   down for the whole drain (a draining node does not redial), which loses it at the
   grace deadline. A
   connection's pipeline of parked acknowledgements is
   bounded in hub gates, not publishes, so a rule that fans a publish out cannot
   multiply what one connection holds in the hub's pending-publish table.

5. **No sinks.** ADR 0063 §1's sink clause and its reasons stand. An action naming a data
   bridge is refused at load, and §2's consumer group remains the way out of the broker.

6. **Rules are per-node operator configuration, loaded all-or-nothing and
   hot-reloadable.** `[rules] file` / `MQTTD_RULES_FILE` names a TOML file of
   `[rules.<id>]` tables, the shape of EMQX's `rule_engine.rules.<id>`. A file that does
   not load refuses the boot and rejects a reload, keeping the running rules
   (validate-before-swap, ADR 0032). Rules sit behind a `watch`, so a reload reaches live
   connections. The file's SHA-256 is exported as `mqttd_rules_info{checksum}` so rule
   drift across nodes is visible, like `config_info`.

7. **Per-rule metrics, labelled by rule id: a bounded exception to ADR 0020's
   fixed-vocabulary rule.** `mqttd_rule_evaluations_total{rule,result}` and
   `mqttd_rule_actions_total{rule,result}` are EMQX's per-rule counters. The `rule` label
   comes from the operator's file, never from a client or a topic, and the file holds at
   most 1,024 rules.

8. **Bounded by construction.** Per file: at most 1,024 rules, 16 actions per rule, 64 KiB
   of SQL per rule, and expressions at most 256 levels deep and nested at most 64 levels
   (parentheses, signs, `NOT`, function calls and array literals inside each other),
   because evaluating an expression recurses once per level. Per message —
   rules routinely pass payload fields to functions, so these bound what a publisher can
   make one message cost: a `FOREACH` iterates at most 10,000 elements and produces at
   most 256 outputs; at most 1,024 effects, carrying together at most 4 MiB beyond four
   times the message's payload; its functions may build at most 1 MiB beyond their
   inputs, together (a pad length, a replacement repeated at every match, a separator
   repeated between items — anything whose output grows with the product of two
   inputs; a per-call bound alone lets a `FOREACH` multiply it);
   `map_put`/`mput` paths have at most 64 segments; timestamps must be renderable in
   every offset; decoding a payload object is linear in its keys; regular expressions
   compiled from a payload are remembered for the message (four of them), so a
   `FOREACH` compiles each once. Regular expressions use a
   linear-time engine with a bounded automaton. Past a bound the function fails, so the
   rule fails and is counted, and the message is still routed. Unknown functions and
   wrong argument counts fail at load. `getenv` is not provided, because a rule must not
   read the broker's environment. **What a publish derives is charged to its connection's
   ingress credit** before the batch is queued, like any publish (ADR 0082 §2), clamped
   to what the per-connection cap leaves beside the original so the wait always ends. The
   connection first tries for the derived charge; if it is not there, it drops its
   original's permit and waits for the original's and the derived charge together in one
   acquire, so no connection holds credit while it waits for more — ADR 0082's
   no-credit-cycle invariant. A QoS 0 or 1 batch waits parked, as a publish waiting for
   credit does (reading paused, deliveries and acks flowing, keepalive not enforced); a
   QoS 2 batch waits in place, because its PUBREC already waits there for the hub's
   answer and a parked batch would never be sent, and the keepalive restarts once that
   wait is over. In place means the connection sees nothing else meanwhile — no
   deliveries, no shutdown drain, no hangup — as during the PUBREC's own wait; both
   waits end as the hub makes progress, so neither can hang the drain past its grace. Under `shed-qos0` a QoS 0 publish's derived messages are dropped and
   counted instead of waited for. The batch holds its credit until it has been
   dispatched. What a client/session event or a Will derives is charged to no
   connection's credit, because no publish carries it: each event and each Will is held
   to the per-message bounds above instead. The clamp means the pool bounds hub memory
   only within a factor for rule-heavy traffic: a batch may carry up to 4 MiB plus five
   times its payload (the original and its derived bytes) while being charged at most one
   per-connection cap. Every connection evaluates against its own cached view of the
   rule set, refreshed only when a reload swaps it — at its next publish, event or
   PINGREQ, so an idle connection does not hold a superseded set — and the publish path
   does not write to shared state to read the rules.

9. **The EMQX converter carries rules.** `from-emqx.py --out-rules` writes each rule's SQL
   verbatim with its `republish`/`console` actions. Every sink action becomes a
   `TODO(migrate)`, and every construct mqttd's engine lacks becomes a commented-out rule
   with the reason. The output must pass `mqttd --check-rules` in CI.

## Consequences

- **Good.** An EMQX user's SQL rules run on mqttd unchanged, or come out of the converter
  as a reviewed file. Rule work scales with the cluster: it costs about 1.8 µs per
  matching publish on a connection task, against a hub loop near half a core at the knee.
  A rule's output keeps its input's guarantee, and a rule cannot loop. Rules can be tested
  offline: `mqttd --check-rules` loads a file and lists every rule, and `mqttd --rule-test`
  runs a statement against a simulated publish or a sample event.
- **A new parser and evaluator on the publish path.** It reads publisher-controlled
  payloads, so it is bounded (decision 8), errors fail the rule rather than the message,
  and the parser and evaluator have nightly fuzz targets beside the codec's.
- **A rule can publish where its publisher cannot.** The ACL decides whether the original
  is accepted; what a rule republishes is operator configuration with the ACL file's
  trust. THREAT-MODEL.md records this as an operator-trusted surface. A topic template
  filled from the payload, the client id or the username lets the publisher choose the
  topic level (`x/../admin` is a valid one); docs/RULES.md shows the `WHERE` guard that
  confines it.
- **A derived message can be lost while its original is acknowledged**: under a
  brownout (a derived message that needs storage is a growth write, and is refused), or
  when its own durable write fails. It is counted
  (`mqttd_rule_actions_total{result="failed"}`). The alternative — withholding the
  original until the derived message can be stored too — re-delivered the original on
  every retry for as long as the condition lasted.
- **An original refused after its derived messages were routed** — refused by a peer's
  verdict, which arrives after the hub's own pass — leaves those derived messages
  delivered, and a resend derives them again. An original of unknown fate withholds and
  can duplicate what was already delivered: QoS 1 allows this, and for QoS 2 it is the
  same residual as an existing mid-fan-out store failure.
- **Amplification is the operator's choice, and it is paid for.** One publish can
  produce up to 1,024 derived messages, each costing a publish's routing, and all of
  them are charged to the publisher's ingress credit (decision 8). A batch is one
  data-lane command, so the hub routes up to 1,025 messages back to back before the
  control lane gets a turn — a few milliseconds at worst, bounded by decision 8, in
  exchange for an atomic decision about the original and everything derived from it.
- **Event- and Will-derived messages are not charged to ingress credit.** There is no
  publish to charge them to and no connection to pause. Each event, and each Will, is
  bounded by decision 8's per-message limits (at most 1,024 derived messages, carrying at
  most 4 MiB plus four times a Will's payload). Events are raised only by connects,
  disconnects and subscription changes, and nothing limits their rate: the connection
  caps bound how many connections are open at once, not how fast they come and go; the
  auth penalty box acts only on failed logins; `limits.max_publish_rate` counts
  publishes only. `limits.max_subscriptions_per_client` and the packet size limit bound
  how many events one SUBSCRIBE raises, since a rule on `$events/session/subscribed` runs
  once per granted filter. A client that connects and disconnects, or subscribes and
  unsubscribes, in a loop makes the hub route what the event rules derive on every turn,
  paying a CONNECT or a SUBSCRIBE for each; what a turn costs the broker is the
  operator's choice, like the amplification above. Accepted; THREAT-MODEL.md lists it.
- **A graceful shutdown delivers what its disconnects derive.** Draining raises
  `client/disconnected` with reason `shutdown` for each connection; the messages rules
  derive from those events are routed, and stored where owed — on another node too,
  forwarded acked and answered — before the broker exits, within the drain's
  `shutdown_grace_secs`. Not waited for: forwards from a node draining in a brownout
  (left ungated, so they still go out live), what the pending-publish bound evicts during
  the drain (counted in a WARN), and what a peer link down for the whole drain owed
  there. A crash raises no events.
- **Rules are node-local configuration.** A node with a different file evaluates its own
  clients' publishes differently. `mqttd_rules_info` makes that visible; nothing prevents
  it, just as nothing prevents ACL drift.
- **The README, COMPARISON, EVALUATION, INTEGRATION, MIGRATION, GUIDE and THREAT-MODEL
  statements that mqttd has no rule engine are corrected** in the same change. The
  consumer-group blueprint stays, now as the answer to sinks rather than to rules.

## Alternatives considered

- **HiveMQ Data Hub compatibility.** Rejected: JSON policies with JavaScript transforms do
  not meet the SQL requirement, and an embedded script runtime is a larger surface than a
  SQL evaluator.
- **Evaluate on the hub.** Rejected: one thread for every node's rule work makes the hub
  loop the scaling ceiling, which fails requirement (2).
- **Evaluate at delivery, per subscriber.** Rejected: N subscribers mean N evaluations,
  and rules about publishes would run once per receiving node.
- **EMQX's default re-triggering** (a republish runs the rules again). Rejected: a rule
  republishing into its own `FROM` loops, and EMQX documents `direct_dispatch` as the fix.
- **Acknowledge the original immediately, publish derived messages best-effort.**
  Rejected: the derived messages would be the one QoS ≥ 1 message an acknowledgement does
  not cover, which fails requirement (3).
- **External sinks (Kafka, HTTP).** Rejected, as in ADR 0063: a second product inside the
  broker, held to weaker contracts.
- **A bespoke SQL dialect.** Rejected: compatibility is what makes migrating rules and
  reusing rule knowledge possible.
