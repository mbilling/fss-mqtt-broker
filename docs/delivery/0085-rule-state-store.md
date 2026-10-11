---
adr: "0085"
title: "State for rules: a keyed, replicated store read and written from SQL"
adr_status: Accepted
tasks:
  - id: 0085-T1
    title: "The store and its SQL functions on local namespaces: the StateStore trait handed over in Functions, [state.*] and the rule key state, EMQX's kv_store_* with EMQX's behaviour, the kv_* functions incl. kv_held and kv_changed, state calls in EMQX's evaluation order, bounds, the store's ceiling and memory accounting, dry runs on a scratch store"
    status: in-progress
    issue: 917
  - id: 0085-T2
    title: "Typed merges as CRDTs (lww, first, max, min, sum, union, hll, deletes) on a per-node hybrid logical clock, and the replicated form of a key"
    status: planned
    issue: 918
  - id: 0085-T3
    title: "eventual mode: every member holds the namespace; changed keys sent per peer link at peer protocol 14, applied off the hub, repaired by anti-entropy after partitions and restarts"
    status: planned
    issue: 919
  - id: 0085-T4
    title: "Expiry and $events/state/expired: selected by its own name only, raised by one member, rate-bounded and late rather than lost, at least once around membership changes"
    status: planned
    issue: 920
  - id: 0085-T5
    title: "kv_incr idempotency keys, remembered as entries of the namespace for its dedupe window"
    status: planned
    issue: 921
  - id: 0085-T6
    title: "Observability and admin: state metrics, per-namespace $SYS statistics, the admin state endpoints and the mqttd --admin state verbs"
    status: planned
    issue: 922
  - id: 0085-T7
    title: "mqttd --rule-test --sequence: timed sequences replayed through a statement or a rules file against an in-memory store with a driven clock"
    status: planned
    issue: 923
  - id: 0085-T8
    title: "Documentation: RULES.md (functions, types, modes, when a state call runs, the worked sequences, the billing example), the departures from EMQX, threat model and hardening rows, the converter"
    status: planned
    issue: 924
  - id: 0085-T9
    title: "strong mode (phase 2): first the design amendment that settles how an evaluation waits, then the durable plane, with partition and failover tests"
    status: planned
    issue: 925
  - id: 0085-T10
    title: "An optional snapshot to disk for local and eventual namespaces (phase 3)"
    status: planned
    issue: 926
  - id: 0085-T11
    title: "EMQX's proc_dict_* as a dictionary per evaluating task, with EMQX's return values, bounded per connection"
    status: planned
    issue: 927
---

# Delivery 0085 — state for rules

**ADR:** [docs/adr/0085-rule-state-store.md](../adr/0085-rule-state-store.md)

The plan, progress, and changelog for ADR 0085. Task status lives in the frontmatter
above; the table below is generated from it.

The ADR was accepted on 2026-10-11, after a review against the EMQX evaluation order (ADR
0083, amended 2026-10-10), ADR 0086 and the rest of what landed since it was written.
Five choices were decided by default and are listed in the ADR with what the other answer
costs: `eventual` replicates to every member; `strong` is phase 2 behind a design
amendment; EMQX's `proc_dict_*` are provided; state needs no switch beyond its ceiling
(`rules.state_max_bytes`); the names stay `kv_*`. T1, the store and its functions on
one node, is built and in review; nothing after it is started.

## Plan

| Task | Follows | Acceptance |
|---|---|---|
| **0085-T1** Store and functions (D1–D3, D9, D11, D13, D14) | — | Every function in D3 but `kv_hll_*` works in a `local` namespace, in the broker and in `mqttd --rule-test`; `kv_store_put/get/del` return what EMQX 6.3.1 returns (probed there, a two-message sequence included) and are no longer refused at load or commented out by the converter. Pinned by tests: a state call in a `SELECT` field runs for a message the `WHERE` rejects; one behind `AND`, `OR` or an untaken `CASE` branch does not; a write stays when a later field fails the rule; a later rule and a re-entered rule see it; `FOREACH` calls run per element. `mqtt-rules` still holds no broker state; a file that uses state is refused at load where there is no store, naming `rules.state_max_bytes` when that is `0`. A rule names its namespace with `state`; a key never selects one. A full namespace refuses new keys (or evicts with `eviction = "lru"`), the store's ceiling refuses across namespaces, a brownout refuses new keys, each counted by reason; the 1,025th state call of a message fails its rule; a read is charged to the build budget. Across a reload a namespace keeps, changes or drops its keys as D1 says. `--rule-test`, `--check-rules` and `POST /admin/v1/rules/test` leave the running store untouched. The per-publish benchmark with no state rule does not move outside its spread, and the cost of each call, uncontended and contended, is recorded. |
| **0085-T2** Merges (D4, D6) | T1 | Each type converges to one value from any delivery order of concurrent updates, deletes included (property tests over random interleavings); a causally later `lww` write is never lost; a delete covers exactly the writes stamped before it; `kv_hll_add` and `kv_hll_count` work, within the stated error; a key's replicated form round-trips every rule value, integers past 64 bits and binaries that are not UTF-8 included; a fuzz target feeds the decoder arbitrary bytes. |
| **0085-T3** `eventual` (D5, D15) | T2 | Two nodes updating the same keys across a partition converge after it heals; a restarted node is refilled from the others; a key written many times while a link is slow is sent once, and what waits is bounded by the key count; state frames enter no hub lane (a flood of them does not delay a publish's round trip through the hub); nothing new is on the wire of a cluster without an `eventual` namespace, and a link that negotiated 13 carries none (`cluster_upgrade.rs`); a frame for a namespace declared otherwise is refused and counted; replication lag and what is pending are metrics. |
| **0085-T4** Expiry events (D7) | T1; its cluster half T3 | A key past its expiry reads as absent at once; `$events/state/expired` fires on one node in a stable cluster and at least once around a membership change; `"$events/#"` does not select it; a mass expiry is delivered at the configured rate, in order, none shed; its rules run off the hub loop; "silent for X" works end to end in the live demo. |
| **0085-T5** Idempotency (D8) | T2 | A resent message with the same idempotency key does not count twice within the window on one node, nor on another node once the entry has replicated; remembered keys count toward the namespace's bounds and expire. |
| **0085-T6** Observability and admin (D10) | T1; replication series T3 | Every metric in D10 is exported; `$SYS` carries no key and no value and lives outside the rule statistics' subtree; the trace carries a failed call's reason; a viewer sees counts only, an operator keys and values, a delete needs the operator role and a listed rules writer and is audited; the CLI verbs print control characters escaped. |
| **0085-T7** Sequence testing (D11) | T1; expiry in the replay T4 | A sequence file replays with its own clock and prints each step's outputs and what it changed in the store; expiry events fire in the replay; a statement and a whole rules file both run; RULES.md's worked sequences are run by CI. |
| **0085-T8** Documentation (D12) | T3–T7, T11 | RULES.md documents every function, type and mode, when a state call runs (EMQX's order, with the `CASE` and `AND` forms that write only on a match), the gotchas (re-entry, a resent QoS 1, `clientid` of a republished message) and the billing example; "Differences from EMQX" lists `kv_*`, `[state.*]`, `state`, the expiry event, the bounds on EMQX's own functions, the scratch store of a dry run and the two `proc_dict_*` differences; THREAT-MODEL.md has rows for state exhaustion, payload-derived values and a rules writer's reach; HARDENING.md says how to bound or switch off the store; CONFIGURATION.md has the new keys; the converter carries `kv_store_*` and `proc_dict_*` rules live. |
| **0085-T9** `strong` (phase 2) | T3, and an amendment to the ADR | First: the ADR is amended with how an evaluation waits for a quorum (the bound on concurrent waits ADR 0086 left open, the timeout, a Will's rules), and accepted. Then: a quorum-acknowledged write survives the leader's loss; a minority partition's calls fail and are counted; no wait holds the hub or another connection. |
| **0085-T10** Snapshot (phase 3) | T3 | A restarted node restores its `local` and `eventual` namespaces from the last snapshot. |
| **0085-T11** `proc_dict_*` (D13) | T1 | `proc_dict_put/get/del` return what EMQX 6.3.1 returns (probed there across two publishes of one connection and across a reconnect); the dictionary is the connection's, empty at connect, gone at disconnect, seen by a re-entered rule; a Will and an event without a connection get an empty one; 64 keys and 64 KiB per connection, counted against the store's ceiling; no longer refused at load. |

**Order.** T1 first. Then T2, T6 (without the replication series), T7, T11 and the
single-node half of T4 can run in parallel; T3 and T5 follow T2; the cluster half of T4
and the replication series of T6 follow T3; T8 closes phase 1. T9 and T10 are later
phases.

## Progress

<!-- status-table:0085 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0085-T1 | 🚧 in-progress | [#917](https://github.com/mbilling/fss-mqtt-broker/issues/917) | — |  |
| 0085-T2 | ⬜ planned | [#918](https://github.com/mbilling/fss-mqtt-broker/issues/918) | — |  |
| 0085-T3 | ⬜ planned | [#919](https://github.com/mbilling/fss-mqtt-broker/issues/919) | — |  |
| 0085-T4 | ⬜ planned | [#920](https://github.com/mbilling/fss-mqtt-broker/issues/920) | — |  |
| 0085-T5 | ⬜ planned | [#921](https://github.com/mbilling/fss-mqtt-broker/issues/921) | — |  |
| 0085-T6 | ⬜ planned | [#922](https://github.com/mbilling/fss-mqtt-broker/issues/922) | — |  |
| 0085-T7 | ⬜ planned | [#923](https://github.com/mbilling/fss-mqtt-broker/issues/923) | — |  |
| 0085-T8 | ⬜ planned | [#924](https://github.com/mbilling/fss-mqtt-broker/issues/924) | — |  |
| 0085-T9 | ⬜ planned | [#925](https://github.com/mbilling/fss-mqtt-broker/issues/925) | — |  |
| 0085-T10 | ⬜ planned | [#926](https://github.com/mbilling/fss-mqtt-broker/issues/926) | — |  |
| 0085-T11 | ⬜ planned | [#927](https://github.com/mbilling/fss-mqtt-broker/issues/927) | — |  |
<!-- /status-table:0085 -->

## Changelog

- 2026-10-09 — ADR proposed (#880), revised with #882's `WHERE`-first evaluation.
- 2026-10-11 — **Reviewed and accepted.** The review's findings are the ADR's amendment of
  the same date. What it changed in this plan:
  - **T1** now carries EMQX's own `kv_store_put/get/del` (ADR 0083 had left their mapping
    to this ADR), the tests that pin a state call to EMQX's evaluation order (`SELECT`
    before `WHERE`: #882 was reverted by #884), the hand-over through `Functions` (ADR
    0086) in place of a trait on `EvalCtx`, the rule key `state` in place of `ns:key`
    addressing, `kv_max`, `kv_min` and `kv_add` (the ADR's merge table named them without
    functions), a byte bound per namespace and a ceiling for the store in the broker's
    configuration, the per-message call budget, the reload rules, and the requirement
    that no dry run writes the running store.
  - **T2** adds the replicated form of a key and its fuzz target; `kv_hll_*` moved here
    from T1 (the sketch is only interesting once it merges).
  - **T3** replicates to every member instead of a replica set per key (a node outside
    the set would read nothing), names the wire (peer protocol 14, additive) and requires
    state frames to stay out of the hub's lanes (ADR 0082).
  - **T4** adds that the event is selected only by its own name, its rate bound and that
    it is late rather than lost.
  - **T5** remembers idempotency keys as entries of the namespace, so they replicate.
  - **T6** names the endpoints and verbs in ADR 0084's shape and moves the `$SYS`
    statistics to a subtree of their own.
  - **T7** drops "the rule-authoring skill uses it" (no such skill exists); RULES.md's
    worked sequences run in CI instead.
  - **T8** adds the converter and the hardening and configuration chapters.
  - **T9** cannot be built as written: an evaluation is synchronous and the only way to
    wait is ADR 0086's hand-off, whose cap is undecided. It now opens with a design
    amendment. The Will special case ("submitted without waiting") is gone.
  - **T11** is new: EMQX's `proc_dict_*`.
  - One issue per task (#880 stays open as the feature's tracker until T8 is done).
- 2026-10-11 — **T1 built** (#917, in review). What it is:
  - **The engine's half** (`crates/mqtt-rules/src/state.rs`): the `StateStore` and
    `StateNamespace` traits, handed over in `Functions` (`with_state`, `without_state`);
    `[state.<name>]` and the rule key `state`; a state call resolved to its namespace at
    load and refused where there is no store (naming `rules.state_max_bytes` when that is
    `0`); EMQX's `kv_store_put/get/del` and mqttd's eleven `kv_*` functions, their names in
    one table; argument checks (key, value, TTL), the 1,024-call budget in the message's
    `Budget`, reads charged to the build budget; `RuleSet::evaluate_one` given the scratch
    store a dry run evaluates on. `mqtt-rules` holds no state: its tests use a store that
    only records calls.
  - **The broker's store** (`crates/mqttd/src/rule_state.rs`): `local` namespaces of 64
    shards, each a map behind its own mutex; typed keys (`lww`, `first`, `max`, `min`,
    `sum`, `union`); `max_keys`, `max_bytes`, the store's ceiling and the brownout, each a
    refusal counted by reason; `eviction = "lru"`; TTLs, with a key past its expiry
    absent at once; namespaces held by the rule sets that use them, which is what keeps,
    changes or drops keys across a reload. `rules.state_max_bytes`
    (`MQTTD_RULES_STATE_MAX_BYTES`, 256 MiB; `0` = off; read at startup).
  - **EMQX 6.3.1 probed:** 141 probes of `kv_store_*` and of when a call runs, pinned as
    tests. Nothing in the ADR was contradicted; the ADR's amendment of this date lists
    what they added. One of them changed the engine: an `IN` list is evaluated whole, as
    EMQX evaluates it (ADR 0083, amended).
  - **Dry runs:** `--rule-test`, `--check-rules`, `--check-config` and
    `POST /admin/v1/rules/test` work on a store of their own.
  - **The converter** carries a rule that calls `kv_store_*` live, with a note on the
    bounds; `proc_dict_*` rules stay commented out until T11.
  - **Measured** (`cargo bench -p mqttd --bench rule_state`, Apple M1 Pro): a publish's
    evaluation goes from 1.38 µs to 1.74 µs with one `kv_incr`; a call on the store is
    0.11 to 0.14 µs alone, 0.20 µs with four threads on keys of their own and 0.33 µs with
    eight, 0.63 µs and 1.9 µs when they all increment one key. The per-publish benchmark
    without a state rule (`cargo bench -p mqtt-rules`) moved between −1.4% and +1.8%
    against `main` over two runs, which is its spread from one run to the next.
  - **Left for later tasks, as planned:** documentation beyond what keeps RULES.md true
    (T8: the threat model and hardening rows, the worked sequences); metrics, `$SYS` and
    the admin verbs (T6; the refusal counts exist in the store and are not exported yet);
    the expiry sweep and its event (T4: until then an expired key is removed by the call
    that finds it, or when its namespace needs room).
