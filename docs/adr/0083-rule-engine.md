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
   live, and a shared group that runs out of members to try gets it unacked at the first
   member tried that is still in it: nobody would retry it). What the drain's own disconnects derive is stored, here
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
   linear-time engine with a bounded automaton (amended 2026-10-10: PCRE2 in byte mode
   with OTP's limits, as in EMQX; see the amendment below). Past a bound the function fails, so the
   rule fails and is counted, and the message is still routed. Unknown functions and
   wrong argument counts fail at load. `getenv` reads only `EMQXVAR_…` variables (amended
   2026-10-10; it was not provided), so a rule cannot read the rest of the broker's
   environment. **What a publish derives is charged to its connection's
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

## Amendment (2026-10-10): evaluation is EMQX's, clause by clause

Decided 2026-10-10: rule evaluation is to be EMQX-compatible without exception, so no
optimisation may change an outcome. Two changes follow, checked against EMQX's
`emqx_rule_runtime.erl` (`evaluate_select`, `evaluate_foreach`, `filter_collection`,
`eval/2`).

**The order is EMQX's again.** #882 evaluated the `WHERE` first, on only the `SELECT`
fields it reads, which made a rejected message with computed fields about 3.8x cheaper
(2.58 µs to 0.68 µs). It is reverted, for two reasons:

- **It differed by design.** A field that would fail on a message the `WHERE` rejects was
  never evaluated, so that message counted `no_result` where EMQX counts `failed`. That
  changes the metrics, the last error and the trace.
- **It had a bug.** Its planner skipped `*`, so in `SELECT 'x' AS clientid, * … WHERE
  clientid = 'c'` the `WHERE` read the alias where EMQX reads the input's `clientid`, and
  the rule silently produced nothing.

Every `SELECT` field again runs before the `WHERE`.

**Lookup follows EMQX's layering.** Until now every clause looked a name up the way a
`SELECT` field does: in what had been selected, then, if the path was `undefined`, in the
input. In EMQX the layering depends on the clause:

| Clause | EMQX evaluates it against |
|---|---|
| `SELECT`, `DO` fields | `[SelectedSoFar, Columns]`. A path `undefined` in the first falls through. |
| `WHERE` | `maps:merge(Columns, Selected)`, one map. A selected top-level key hides the input's. |
| `INCASE` | `maps:merge(ColumnsAndSelected, #{Item => Element})`, one map. |

So `SELECT payload.x … WHERE payload.y = 1` was true in mqttd and is false in EMQX, because
the selected `payload` is `{"x": …}`. `EvalCtx::merged_from` now marks, per clause, where
lookup stops falling through. The `WHERE` and `INCASE` read the merged map, and a `DO`
field reads its own selection, then the merged map. Pinned by
`the_where_reads_the_input_with_the_selection_merged_over_it`,
`a_failing_select_field_fails_the_rule_even_when_the_where_is_false` and
`incase_and_do_read_the_merged_scope_like_emqx` (the first and the last fail on the old
lookup).

Rule cost is to be lowered only by changes that keep every output, error and counter
EMQX's. One candidate: evaluating the `WHERE` first only when no field can fail and none
is hidden by a later `*`.

## Amendment (2026-10-10): events are EMQX's, field by field

The same decision reaches the events. Checked against EMQX's
`apps/emqx_rule_engine/src/emqx_rule_events.erl` (`event_topics_enum/0`, `eventmsg_*`,
`match_event_names/1`), `emqx_channel.erl`, `emqx_access_control.erl` and
`emqx_utils_maps:printable_props/1`:

- **Fields.** Each event carries exactly its EMQX builder's fields, `mountpoint` aside (no
  such feature). New: `sockname` (the accepted socket's local address, passed from the
  listener as an `Arrival`), `receive_maximum`, `proto_name`/`proto_ver` on
  `client.disconnected`, `peername` on the session events and `qos` on
  `session.unsubscribed` (the hub's UNSUBSCRIBE answer now carries the removed grant's
  QoS). `expiry_interval` is seconds on `client.connected` and milliseconds on
  `client.connack` and `client.ping`, as EMQX's builders have it. Addresses print as
  `emqx_utils:ntoa/1` prints them. `client_attrs` is set only where EMQX sets it, and
  `username` always (`undefined` when none was sent). A live EMQX 6.3.1 was probed with
  the same connections and agreed on each of these, `metadata.namespace` aside.
- **Property maps.** `conn_props`, `disconn_props`, `sub_props` and `unsub_props` carry the
  packet's properties, printed as `printable_props/1` prints them (`User-Property` always
  present); `pub_props` shares the printer.
- **Disconnect reasons.** The hub records why it closed a connection on the outbound
  channel's shared state before dropping it (`HubClose`): `takenover` (the same client id
  without clean start), `discarded` (with clean start), `kicked`, `not_authorized`
  (revocation), `server_busy`, `use_another_server`. A broker close names the reason code
  it sends (or would send, to an MQTT 3.1.1 client), as `handle_out(disconnect, …)` does;
  an undecodable packet is `frame_error`/`frame_too_large`; a client close is `tcp_closed`
  or `ssl_closed` by transport, a socket error its `inet` name. `server_closed`, which
  EMQX never says, is gone.
- **New events.** `client.connack` (every CONNACK, refusals included, after
  `client.connected` as in EMQX), `client.ping`, `client.check_authn_complete` (every
  authentication and re-authentication) and `client.check_authz_complete` (every publish,
  the Will's included, and every SUBSCRIBE filter; `authz_source` is `file` when an ACL
  rule decided, else `default`, read from `Authorizer::explain` only when a rule selects the
  event). Each is built only after `wants()` says a rule selects it, so the per-publish and
  per-PINGREQ events cost one uncontended lock when nothing selects them. All run on the
  connection task, never on the hub loop.
- **`FROM`.** A wildcard `$events/…` filter selects every event whose EMQX topic it
  matches; one matching only events mqttd does not raise is refused, one also matching
  some is warned about. `$sources/…` is refused like `$bridges/…`.
- **`flags.dup`** is always `false`: EMQX evaluates `emqx_message:clean_dup(Msg)`.

Pinned by `every_event_carries_exactly_emqx_s_fields`,
`event_values_follow_emqx_s_builders`,
`wildcard_event_filters_select_what_emqx_s_match_selects` and
`a_rule_never_sees_the_dup_flag` (mqtt-rules), and end to end by
`a_connection_s_events_carry_emqx_s_fields_end_to_end`,
`a_refused_connect_raises_its_connack_and_authentication_events`,
`a_closed_connection_reports_emqx_s_reason`, `a_rule_reads_the_dup_flag_as_false` and
`authorization_events_name_the_acl_file_or_the_default` (`tests/rules.rs`). The message
events stay out of this amendment.

## Amendment (2026-10-10): functions and literals are EMQX's too

The same decision — EMQX-compatible without exception — applies to what a function
returns and to how a literal reads. EMQX's source is the oracle, not its reference:
`emqx_rule_funcs.erl` and `emqx_variform_bif.erl` (emqx/emqx master), the `rulesql` 0.2.1
lexer and parser, and the OTP 28 modules they call. Each value below was probed on EMQX
6.3.1 (`emqx_rule_sqltester:test/1`) and is pinned by a test that fails on the old code.

- **Strings are grapheme clusters.** EMQX's string functions call Erlang's `string`
  module, which counts clusters (Unicode 16) and finds a match by code point, then
  requires the cluster that starts there to be the pattern's last character. `strlen`,
  `substr`, `pad`, `trim`/`ltrim`/`rtrim` (`Pattern_White_Space`, not Unicode white
  space), `find`, `split`, `replace` and `reverse` now follow OTP's `string.erl` step by
  step, quirks included: `\n` is found inside `\r\n` and `\r` is not; `reverse` writes
  each character as a byte (Latin-1, failing above U+00FF); `tokens` works on bytes;
  `ascii` is the first byte; `lower` has no final-sigma rule; `true`/`false` are strings
  to them; `unescape` keeps a trailing backslash; `rtrim/2` exists.
  `unicode-segmentation` is held at 1.12, whose tables are Unicode 16 as OTP 28's are.
  Pinned by `string_functions_work_on_grapheme_clusters_like_emqx`.
- **`regex_replace` replacements are `re:replace`'s** as `precomp_repl/1` reads them:
  `\0` is a literal `0`, `\g{0}` and `&` the match, `\N`/`\gN`/`\g{N}` group N, `$`
  nothing special, a malformed `\g` an error. The regular-expression engine itself is
  unchanged here. Pinned by `regex_replace_reads_its_replacement_as_erlang_re_does`.
- **A doubled quote stays doubled.** The `rulesql` lexer keeps a quoted token whole and the
  parser unquotes it with `string:trim(Text, both, "'")`: `'it''s'` is `it''s` and every
  quote at either end goes (`'''x'''` is `x`). Same for `"…"` names. Pinned by
  `doubled_quotes_in_a_literal_stay_doubled_and_edge_quotes_go`.
- **`sprintf` is `io_lib:format`** (and `sprintf_s` its array form): every control
  sequence (`~s ~ts ~p ~P ~w ~W ~e ~f ~g ~b ~B ~x ~X ~# ~+ ~c ~i ~n ~~`), field width,
  precision, padding and the `t`, `l`, `k` modifiers, over the terms EMQX holds (binaries,
  lists, maps with binary keys, atoms), with Erlang's float notations and the
  `iolist_to_binary` that fails on a character above U+00FF. One part is not reproduced:
  `~p` keeps a long list or map on one line where `io_lib_pretty` breaks it at the line
  width. Field widths from the payload are bounded by the growth budget. Pinned by
  `sprintf_is_erlang_io_lib_format` and `sprintf_widths_from_the_payload_are_bounded`.
- **The missing functions exist.** `bitsize`, `bytesize`, `subbits` (an integer outside
  64 bits or a bit string that is not whole bytes fails the rule: mqttd's values cannot
  hold them); `gzip`, `gunzip`, `zip`, `unzip`, `zip_compress`, `zip_uncompress`,
  `lz4_compress`, `lz4_uncompress`, byte-identical to EMQX's because they use the same C
  zlib (built from source through `libz-sys`) and C liblz4 (through `lz4`) — the pure-Rust
  backends tried (miniz_oxide, zlib-rs, lz4_flex) emit valid but different bytes, and
  decompression stops at the per-message growth budget, so a small payload cannot inflate
  into gigabytes; `hash` over every `crypto:hash/2` digest (RustCrypto's `md4`, `ripemd`,
  `sha2`, `sha3`, `shake`, `sm3` and the already-shipped `blake2` for what aws-lc-rs lacks);
  `map_to_redis_hset_args`, `join_to_sql_values_string`; and the exports EMQX's reference
  leaves out: `div(a, b)` and `mod(a, b)` (its grammar allows the call form), `eq`, `null`,
  `timezone_to_second`, `contains_topic` and `contains_topic_match` (which match only
  maps with an atom key, so any rule value gives `false`), `bin2hexstr/2`,
  `hexstr2bin/2` (and `hexstr2bin` of an odd digit count), `format_date/3`.
- **`getenv` is provided** (§8 said it was not): EMQX's reads only `EMQXVAR_<name>`, so the
  operator decides what a rule may see by naming a variable so, and nothing else in the
  broker's environment is reachable. Values are kept once read, as EMQX keeps them.
- **`is_empty` of a string** goes through EMQX's `map/1`: `''` is empty, a JSON object is
  read, and anything else — a JSON array, `null`, text that is not JSON — fails.
- **Still refused:** `kv_store_get`/`put`/`del` and `proc_dict_*` keep values between
  messages in a node-local table. That is the state ADR 0085 designs as a replicated,
  namespaced store behind a trait; implementing EMQX's unreplicated table now would
  preempt it, so they stay refused at load and ADR 0085 decides their mapping.

## Amendment (2026-10-10): regular expressions are PCRE2 in byte mode, as in EMQX

§8 chose a linear-time engine (Rust's `regex`) so that a pattern could never make a
message expensive: no backtracking, no backreferences, no look-around. On 2026-10-10 the
user decided that rules must be 100% EMQX-compatible, and the regex functions were where
the engines visibly disagreed. EMQX's `regex_match`, `regex_replace` and
`regex_extract` (`emqx_variform_bif.erl`) call `re:run(S, RE, [global, {capture,
none}])`, `re:replace(S, RE, Rep, [global, {return, binary}])` and `re:run(S, RE,
[{capture, all_but_first, binary}])` with the pattern uncompiled and without `unicode`:
Erlang's `re`, which is PCRE2 in **byte mode**. Probed on EMQX 6.3.1 (OTP 28, PCRE2
10.47) against the old engine: `'abc$'` matches `abc` plus a newline (old: no); `.`
matches one byte of `é`, so `regex_replace('é', '.', 'x')` is `xx` (old: `x`); `\d` and
`\w` are ASCII (old: Unicode); `(a)\1` matches `aa` (old: refused at load).

**The engine is PCRE2.** `crates/mqtt-rules/src/funcs/re.rs` compiles with the `pcre2`
crate (a safe wrapper; the workspace forbids `unsafe`) over `pcre2-sys`, which builds
PCRE2 10.46 from source and links it statically — on musl by itself, everywhere through
`PCRE2_SYS_STATIC` in `.cargo/config.toml`, so no build links a system PCRE2. The compile
options are OTP's defaults: none — no UTF, no UCP, newline LF, the C-locale tables, no
JIT. Around `pcre2_match` it repeats what `re.erl` does: `loopexec/8`'s global loop, its
anchored `notempty_atstart` retry after an empty match (run unanchored and kept only when
it starts at the same offset — the crate cannot pass `anchored`, and the unanchored
search tries that offset first, exactly as the anchored one would), its CRLF-aware step,
its suppression of a repeated match, `do_mlist/5`'s replacement, and the capture count
`re` reports (groups up to the last that took part). A pattern that is not UTF-8 is
passed as `\xHH` escapes for the crate's `&str`. A differential set of 171 subjects and
patterns, each run on the EMQX container, is pinned row for row
(`regex_functions_match_emqx_row_for_row`).

**What replaces linear-time safety is OTP's bounds, plus one.** OTP builds PCRE2 with a
match limit and a depth limit of 10,000,000 (`erts/emulator/pcre/local_config.h`, PCRE2's
defaults too) and turns reaching either into `nomatch` (`erl_bif_re.c`), so a
catastrophic pattern is *no match* in EMQX, not an error; it is here too, and
`(a+)+b|z` turning from match to no match between 21 and 22 `a`s in both engines pins the
limits as equal. OTP's heap limit is 20,000,000 KiB — in effect none — and EMQX's
matches yield to the scheduler; here a match runs on the publisher's connection task, so
its backtracking frames are bounded at 64 MiB, set as a `(*LIMIT_HEAP=…)` item after the
pattern's own start-of-pattern items so a pattern cannot raise it. A match needing more
is no match, where EMQX goes on: `'^(a|b)*$'` past about 220 KB of subject. Compiled
size is PCRE2's own bound, 64K code units (`LINK_SIZE` 2, as in OTP).

What remains is CPU: the match limit applies per start position, so a pattern that fails
slowly at every position of a long subject costs up to the limit (about 0.15 s) at each.
That is EMQX's exposure too, and it needs a pattern written by the operator or taken from
the payload by the operator's rule; it is recorded as an accepted risk in
`docs/THREAT-MODEL.md`. A per-message regex budget was not built: PCRE2 reports no step
count to charge one with, and any bound short of OTP's would change results.

**Literal patterns.** EMQX compiles a pattern on every call; this engine compiles a
literal once when the file loads and keeps it, and remembers a payload pattern for the
rest of the message (four of them). A literal that does not compile no longer fails the
load — EMQX's parser accepts it and every call raises `badarg` — but warns, and the call
fails the rule. ADR 0084 D3's cap of 96 distinct literals existed for the linear-time
engine's automata (1 MiB each); a PCRE2 pattern costs at most about 0.9 ms and 190 KiB, so
the cap is 512 distinct literals, plus 256 KiB of them together for named groups, whose
compile time is quadratic (about 0.15 s for a 64 KiB pattern): a worst-case file loads in
under a second and about 100 MiB.

**Still different from EMQX:** the 64 MiB heap bound above; PCRE2's default nesting limit
of 250 parentheses (OTP raises it to 10,000; the `pcre2` crate cannot set it, and PCRE2's
own workspace stops most such patterns below 2,000 anyway); PCRE2 10.46 against OTP 28's
10.47 (bug fixes only); a literal that does not compile warns at load. Pinned by the
tests named above and `the_match_limit_is_otps_and_reaching_it_is_no_match`,
`a_matchs_backtracking_heap_is_bounded`,
`an_invalid_pattern_loads_with_a_warning_and_fails_each_call`.

## Amendment (2026-10-10): republished messages re-enter the rules, as in EMQX

§3 and the rejected "EMQX's default re-triggering" are reversed: rules are to be
EMQX-compatible without exception, and EMQX's republish re-enters the rule engine. Checked
against `emqx_rule_actions.erl` (`republish/3`, `safe_publish/7`, `do_safe_publish/2`,
`republish_clientinfo/1`), `emqx_rule_engine_schema.erl`, `emqx_broker.erl`
(`safe_publish2/2`, `publish2/2`, `eval_hook_and_publish/2`) and
`emqx_rule_events:eventmsg_publish/1` (emqx/emqx master), and probed on EMQX 6.3.1.

- **`direct_dispatch = false`, the default, re-enters.** EMQX publishes the message through
  `safe_publish2(Msg, #{bypass_hook => false})`, so the `message.publish` hook — every
  rule — runs on it. mqttd evaluates it on the task that evaluated the message it came
  from (the connection task, or the hub for a Will), never the hub loop for client
  publishes, as the original is. The rules see EMQX's fields: `clientid` is the rule id,
  `username`, `peerhost` and `peername` are `undefined`, `pub_props` are the action's,
  `flags` are the trigger's with the action's `retain` (`{"retain": …}` alone in a chain
  started by an event, which has no `flags`), `publish_received_at` is when it was
  republished. `SELECT *` now shows an `undefined` `username`, `peerhost` and `peername`,
  as EMQX's `eventmsg_publish/1` always sets them, for client messages too.
- **The guards.** EMQX's own: a rule does not run its `republish` actions on a message it
  republished (`republish_by` = its id; `recursive_republish_detected`, counted as a
  successful action); the rule itself still runs. EMQX has no other: two rules
  republishing into each other recurse in the publisher's process until it reaches its
  heap limit and is killed (6.3.1: about 1,500 rounds, the connection dropped, nothing
  delivered). mqttd stops a chain **32 republishes** deep instead (`MAX_REPUBLISH_DEPTH`):
  the message 32 deep is evaluated, its `republish` actions fail. Both are counted in
  `mqttd_rule_recursive_republish_total{rule,guard}` (`same_rule`, `depth`).
- **Amplification.** The original's per-message limits (1,024 effects; 4 MiB plus four
  times its payload) cover the whole tree, through a `Budget` shared by every evaluation
  it leads to, so a chain or a loop cannot multiply a publish further than one rule could.
- **Acknowledgement and durability are unchanged.** The whole tree travels in the
  original's one `PublishBatch`, charged to its ingress credit, each QoS ≥ 1 message gated
  behind a QoS ≥ 1 original. What an event or a Will leads to goes out ungated, as before.
- **Order.** EMQX routes a republished message inside the original's publish hook, so
  before the original, and its own republishes before it (`t/a` → `t/b` → `out/b` is
  delivered `out/b`, `t/b`, `t/a`). Routing the original after what it derived would
  break the rule that a refused original routes nothing it derived, so the hub still
  routes the original first; the derived messages follow in EMQX's order, each after what
  it caused (`t/a`, `out/b`, `t/b`).
- **`direct_dispatch = true`** skips the rules, and the retained store: EMQX's retainer is
  a `message.publish` hook, which direct dispatch bypasses (6.3.1 delivered the message
  live and kept no retained copy). It is delivered live with the retain flag clear, as an
  over-quota retained derived message already is.
- **A templated `direct_dispatch`** (`union([boolean(), template()])`) is rendered per
  message: only a boolean `true` is true; a missing value is the default `false`, and any
  other value is `false` (EMQX logs `bad_direct_dispatch_resolved_value`). A literal string
  other than `"true"`, `"false"` or `""` loads with a warning.

Pinned by `a_republished_message_is_seen_as_emqx_shows_it`,
`a_message_republished_from_an_event_has_no_dup_flag`,
`a_rule_does_not_republish_its_own_republished_message`,
`republishing_stops_at_the_depth_cap`, `the_budget_spans_every_reentry` and
`direct_dispatch_renders_per_message` (mqtt-rules), and end to end by
`a_republished_message_runs_the_rules_that_select_it`,
`a_rule_republishing_into_its_own_from_cannot_loop`,
`rules_republishing_into_each_other_stop_at_the_depth_cap`,
`direct_dispatch_skips_the_rules_and_the_retained_store`,
`a_templated_direct_dispatch_is_rendered_per_message` and
`a_message_republished_from_an_event_runs_the_rules` (`tests/rules.rs`).
