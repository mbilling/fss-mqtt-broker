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
   `mqtt-rules` crate, which is pure: no I/O and no broker state.

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

4. **A QoS ≥ 1 publish's acknowledgement waits for what its rules produced.** Each
   derived message at QoS ≥ 1 behind a gated original gets its own acknowledgement gate,
   and the publisher's PUBACK/PUBREC answers all of them together:
   - all accepted → acknowledged;
   - all refused → the original's refusal (nothing was stored anywhere);
   - otherwise → **withheld** (no ack, connection closed, publisher retries).
   A refusal claims nothing was stored and an acknowledgement claims everything was;
   for a half-stored batch both are false, so the withhold rule `refuse_pending` applies
   per message (issue #238) is applied per batch. Inbound QoS 2 deduplication precedes
   evaluation, so a rule fires once per QoS 2 message. A QoS 0 original gates nothing.
   Client/session events gate nothing, because there is no acknowledgement to hold.

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
   of SQL per rule. Per message: at most 256 `FOREACH` outputs and 1,024 effects. Regular
   expressions use a linear-time engine with a bounded automaton, because a rule can apply
   a pattern taken from a payload. Unknown functions and wrong argument counts fail at
   load. `getenv` is not provided, because a rule must not read the broker's environment.
   The original's ingress permit rides the last command of its batch, so it is held until
   every derived message has been dispatched (ADR 0082 §2).

9. **The EMQX converter carries rules.** `from-emqx.py --out-rules` writes each rule's SQL
   verbatim with its `republish`/`console` actions. Every sink action becomes a
   `TODO(migrate)`, and every construct mqttd's engine lacks becomes a commented-out rule
   with the reason. The output must pass `mqttd --check-rules` in CI.

## Consequences

- **Good.** An EMQX user's SQL rules run on mqttd unchanged, or come out of the converter
  as a reviewed file. Rule work scales with the cluster: it costs about 1.8 µs per
  matching publish on a connection task, against a hub loop near half a core at the knee.
  A rule's output keeps its input's guarantee, and a rule cannot loop. Rules can be tested
  offline with `mqttd --rule-test` and `--check-rules`.
- **A new parser and evaluator on the publish path.** It reads publisher-controlled
  payloads, so it is bounded (decision 8), errors fail the rule rather than the message,
  and the parser and evaluator have nightly fuzz targets beside the codec's.
- **A rule can publish where its publisher cannot.** The ACL decides whether the original
  is accepted; what a rule republishes is operator configuration with the ACL file's
  trust. THREAT-MODEL.md records this as an operator-trusted surface.
- **Withholding a half-stored batch can duplicate the original** when the publisher
  retries. QoS 1 allows this. For QoS 2 it is the same residual as an existing mid-fan-out
  store failure (`refuse_pending`). A mixed outcome needs one part refused while another
  is stored, which in practice means a brownout.
- **Amplification is the operator's choice.** One publish can produce up to 1,024 derived
  messages, each costing a publish's routing. Derived bytes are not charged ingress credit
  separately; they ride the original's permit, bounded by decision 8.
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
