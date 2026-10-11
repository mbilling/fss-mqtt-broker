---
adr: "0085"
title: "State for rules: a keyed, replicated store read and written from SQL"
adr_status: Accepted
tasks:
  - id: 0085-T1
    title: "The store and its SQL functions on local namespaces: the StateStore trait handed over in Functions, [state.*] and the rule key state, EMQX's kv_store_* with EMQX's behaviour, the kv_* functions incl. kv_held and kv_changed, state calls in EMQX's evaluation order, bounds, the store's ceiling and memory accounting, dry runs on a scratch store"
    status: done
    issue: 917
    date: 2026-10-11
    evidence: "PR 929: crates/mqtt-rules/src/state.rs (StateStore and StateNamespace handed over in Functions, [state.<name>] and the rule key state, one table of function names) and crates/mqttd/src/rule_state.rs (local namespaces, typed keys, TTLs, bounds, LRU, the rules.state_max_bytes ceiling, brownout); 141 probes of EMQX 6.3.1 (emqx_rule_funcs and emqx_rule_sqltester) pinned in rule_state::tests, none contradicting the ADR; mqtt-rules tests_state (21), mqttd rule_state::tests (33), tests/rules_state.rs (6), additions to rules_binary.rs, admin_rules.rs and the converter test; dry runs on a scratch store; the rule_state benchmark (0.11-0.14 us per call on one thread, Apple M1 Pro)."
  - id: 0085-T2
    title: "Typed merges as CRDTs (lww, first, max, min, sum, union, hll, deletes) on a per-node hybrid logical clock, and the replicated form of a key"
    status: done
    issue: 918
    date: 2026-10-11
    evidence: "PR 935: crates/mqtt-rules/src/crdt.rs (the types, a merge that is commutative, associative and idempotent over every state the decoder accepts, the hybrid logical clock with a 60 s skew bound, the canonical bounded encoded form, kv_hll_add and kv_hll_count on a 2^14-register sketch hashed with SipHash-2-4 under a fixed key) and the store keeping its keys in crdt::Data; mqtt-rules tests_crdt (40); the state_merge fuzz target in the nightly tier (about 1.2 million inputs locally, no finding); T1 store tests and the 141 EMQX probes pass unmodified; the ADR amended for the delete rule of non-lww types, first-write expiry, the skew bound and the counter bound."
  - id: 0085-T3
    title: "eventual mode: every member holds the namespace; changed keys sent per peer link at peer protocol 14, applied off the hub, repaired by anti-entropy after partitions and restarts"
    status: done
    issue: 919
    date: 2026-10-11
    evidence: "PR 950: crates/mqttd/src/rule_state/replica.rs and link.rs (changed keys read per link with no per-link queue, repair by a 1,024-part digest, refusals counted by reason), peer protocol 14 with StateKeys, StateDigest and StateWant pinned in crates/mqtt-cluster/src/peer.rs; tests_replica and crates/mqttd/tests/rules_state_cluster.rs (convergence, restart refill, declaration mismatch, bounds, a v13 peer sent nothing); the rule_state benchmark (publish with one kv_incr 1.80 us local, 1.84 us eventual; 100k keys caught up in 0.14 s on loopback, Apple M1 Pro); the ADR amended for what T3 decided."
  - id: 0085-T4
    title: "Expiry and $events/state/expired: selected by its own name only, raised by one member, rate-bounded and late rather than lost, at least once around membership changes"
    status: in-progress
    issue: 920
  - id: 0085-T5
    title: "kv_incr idempotency keys, remembered as entries of the namespace for its dedupe window"
    status: done
    issue: 921
    date: 2026-10-11
    evidence: "PR 956: kv_incr(key, by, ttl, idempotency_key) remembers a key of 1 to 255 bytes as an entry of the namespace for its dedupe_window; exact on one node (8 threads presenting 4,000 keys give 4,000 increments, local and eventual), at most once per node inside the replication lag in a cluster, as D8 says; a refused increment is not remembered; a real QoS 1 DUP resend through the broker is counted once with a publisher-given id and every time with the broker's id; replay transcripts in rules_sequence.rs and RULES.md; the rule_state benchmark shows kv_incr without a key unchanged."
  - id: 0085-T6
    title: "Observability and admin: state metrics, per-namespace $SYS statistics, the admin state endpoints and the mqttd --admin state verbs"
    status: done
    issue: 922
    date: 2026-10-11
    evidence: "PR 948: crates/mqttd/src/rule_state/admin.rs and crates/mqttd/src/admin/state.rs (the overview for a viewer, the paged key listing and single-key read for an operator, the audited delete for an operator who is a listed rules writer, replicated in an eventual namespace), the CLI verbs state, state-keys, state-get and state-del with control characters escaped, the mqttd_rule_state_* series and $SYS/brokers/<node>/rule_state/<namespace> with no key and no value; tests for roles, paging under concurrent writes and merges, the key and value representation round trip, the audit record (key_sha256 and key_bytes) and a three-node delete that converges; the rule_state inspect benchmark."
  - id: 0085-T7
    title: "mqttd --rule-test --sequence: timed sequences replayed through a statement or a rules file against an in-memory store with a driven clock"
    status: done
    issue: 923
    date: 2026-10-11
    evidence: "PR 947 and PR 952: mqttd --rule-test --sequence (crates/mqttd/src/rule_replay.rs) replays a file of JSON lines on a driven clock against a scratch store, for one statement or the configured rules file with its actions, re-entry and [state.*] tables; crates/mqttd/tests/rules_sequence.rs (the kv_held, quota, kv_changed and kv_store_* transcripts, the file format and its errors, the running store untouched) and the RULES.md transcripts run by rules_docs.rs; since PR 952 the replay raises $events/state/expired per key at its expiry time before the step's own message."
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
    status: done
    issue: 927
    date: 2026-10-11
    evidence: "PR 933: crates/mqtt-rules/src/state/dict.rs (a Dictionary per evaluating task, the three names in state::FUNCS) and one dictionary per connection in crates/mqttd/src/rules.rs; 99 probes of EMQX 6.3.1 (direct calls, emqx_rule_sqltester, the REST SQL test, and clients on a running node for whose process holds the dictionary per trigger) pinned in mqtt-rules tests_proc_dict (14) and crates/mqttd/tests/rules_proc_dict.rs (10), plus one test each in admin_rules.rs and rules_binary.rs; bounds of 64 keys and 64 KiB with dictionary_full; the ADR amended where the probes contradicted it; the converter carries proc_dict_* rules live."
  - id: 0085-T12
    title: "View the contents of the store: each key's type, size and expiry in the listing, optionally values, filters by expiry (expires within, no expiry), kv_store included"
    status: done
    issue: 940
    date: 2026-10-11
    evidence: "PR 957: GET /admin/v1/rules/state/keys lists each key with type, bytes and expires_at, and with values=true its value, filtered by prefix, expires_within and no_expiry (AND; a contradiction is a 400), for local, eventual and kv_store namespaces, with no field added to an entry (ENTRY_OVERHEAD asserted at 128); expires_within in a local namespace walks the existing expiry order; the single-key read returns bytes; the cursor is hashed in the audit record; state-keys gains --values, --expires-within and --no-expiry; 16 tests in the mqttd lib and admin_rules.rs, the rule_state filtered benchmark."
  - id: 0085-T13
    title: "Expire entries manually: a bulk purge of one namespace by prefix, by expiry (expires within, no expiry), or already expired; dry run, batches off the publish path, 100,000 keys a call with a cursor, audited; no expiry event unless asked"
    status: planned
    issue: 941
  - id: 0085-T14
    title: "Change expiry on stored keys: set, extend or clear the expiry of one key or a selection, within max_ttl; dry run, audited; a sooner expiry refused where it would not replicate"
    status: planned
    issue: 942
  - id: 0085-T15
    title: "CLI and documentation for viewing, purging and changing expiry: state-keys filters, mqttd --admin state-purge and state-expire; ADMIN-API.md, RULES.md, the threat model row"
    status: planned
    issue: 943
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
(`rules.state_max_bytes`); the names stay `kv_*`. The user confirmed the last two on
2026-10-11 and approved the operator's tools on the store (T12 to T15: view the
contents, purge, change expiry), which the ADR's amendment of that date decides. T1, the store and its functions on
one node, is done; the table under Progress has where each of the others stands.

## Plan

| Task | Follows | Acceptance |
|---|---|---|
| **0085-T1** Store and functions (D1–D3, D9, D11, D13, D14) | — | Every function in D3 but `kv_hll_*` works in a `local` namespace, in the broker and in `mqttd --rule-test`; `kv_store_put/get/del` return what EMQX 6.3.1 returns (probed there, a two-message sequence included) and are no longer refused at load or commented out by the converter. Pinned by tests: a state call in a `SELECT` field runs for a message the `WHERE` rejects; one behind `AND`, `OR` or an untaken `CASE` branch does not; a write stays when a later field fails the rule; a later rule and a re-entered rule see it; `FOREACH` calls run per element. `mqtt-rules` still holds no broker state; a file that uses state is refused at load where there is no store, naming `rules.state_max_bytes` when that is `0`. A rule names its namespace with `state`; a key never selects one. A full namespace refuses new keys (or evicts with `eviction = "lru"`), the store's ceiling refuses across namespaces, a brownout refuses new keys, each counted by reason; the 1,025th state call of a message fails its rule; a read is charged to the build budget. Across a reload a namespace keeps, changes or drops its keys as D1 says. `--rule-test`, `--check-rules` and `POST /admin/v1/rules/test` leave the running store untouched. The per-publish benchmark with no state rule does not move outside its spread, and the cost of each call, uncontended and contended, is recorded. |
| **0085-T2** Merges (D4, D6) | T1 | Each type converges to one value from any delivery order of concurrent updates, deletes included (property tests over random interleavings); a causally later `lww` write is never lost; a delete covers exactly the `lww` writes stamped before it, and for the other types those and the writes of nodes that had not heard of it (the ADR's amendment of 2026-10-11 says why no bounded state does better); `kv_hll_add` and `kv_hll_count` work, within the stated error; a key's replicated form round-trips every rule value, integers past 64 bits and binaries that are not UTF-8 included; a fuzz target feeds the decoder arbitrary bytes. |
| **0085-T3** `eventual` (D5, D15) | T2 | Two nodes updating the same keys across a partition converge after it heals; a restarted node is refilled from the others; a key written many times while a link is slow is sent once, and what waits is bounded by the key count; state frames enter no hub lane (a flood of them does not delay a publish's round trip through the hub); nothing new is on the wire of a cluster without an `eventual` namespace, and a link that negotiated 13 carries none (`cluster_upgrade.rs`); a frame for a namespace declared otherwise is refused and counted; replication lag and what is pending are metrics. |
| **0085-T4** Expiry events (D7) | T1; its cluster half T3 | A key past its expiry reads as absent at once; `$events/state/expired` fires on one node in a stable cluster and at least once around a membership change; `"$events/#"` does not select it; a mass expiry is delivered at the configured rate, in order, none shed; its rules run off the hub loop; "silent for X" works end to end in the live demo. |
| **0085-T5** Idempotency (D8) | T2 | A resent message with the same idempotency key does not count twice within the window on one node, nor on another node once the entry has replicated; remembered keys count toward the namespace's bounds and expire. |
| **0085-T6** Observability and admin (D10) | T1; replication series T3 | Every metric in D10 is exported; `$SYS` carries no key and no value and lives outside the rule statistics' subtree; the trace carries a failed call's reason; a viewer sees counts only, an operator keys and values, a delete needs the operator role and a listed rules writer and is audited; the CLI verbs print control characters escaped. |
| **0085-T7** Sequence testing (D11) | T1; expiry in the replay T4 | A sequence file replays with its own clock and prints each step's outputs and what it changed in the store; expiry events fire in the replay; a statement and a whole rules file both run; RULES.md's worked sequences are run by CI. |
| **0085-T8** Documentation (D12) | T3–T7, T11 | RULES.md documents every function, type and mode, when a state call runs (EMQX's order, with the `CASE` and `AND` forms that write only on a match), the gotchas (re-entry, a resent QoS 1, `clientid` of a republished message) and the billing example; "Differences from EMQX" lists `kv_*`, `[state.*]`, `state`, the expiry event, the bounds on EMQX's own functions, the scratch store of a dry run and the two `proc_dict_*` differences; THREAT-MODEL.md has rows for state exhaustion, payload-derived values and a rules writer's reach; HARDENING.md says how to bound or switch off the store; CONFIGURATION.md has the new keys; the converter carries `kv_store_*` and `proc_dict_*` rules live. |
| **0085-T9** `strong` (phase 2) | T3, and an amendment to the ADR | First: the ADR is amended with how an evaluation waits for a quorum (the bound on concurrent waits ADR 0086 left open, the timeout, a Will's rules), and accepted. Then: a quorum-acknowledged write survives the leader's loss; a minority partition's calls fail and are counted; no wait holds the hub or another connection. |
| **0085-T10** Snapshot (phase 3) | T3 | A restarted node restores its `local` and `eventual` namespaces from the last snapshot. |
| **0085-T11** `proc_dict_*` (D13) | T1 | `proc_dict_put/get/del` return what EMQX 6.3.1 returns (probed there across two publishes of one connection and across a reconnect); the dictionary is the connection's, empty at connect, gone at disconnect, seen by a re-entered rule; a Will and an event without a connection get an empty one; 64 keys and 64 KiB per connection, counted against the store's ceiling; no longer refused at load. |
| **0085-T12** View the contents (amendment of 2026-10-11) | T6; its `eventual` half T3 | The listing shows `type`, `bytes` and `expires_at` for every key of a declared namespace, a rule's own and `kv_store`, and values only with `values=true` and only to an operator; `expires_within` and `no_expiry`, alone and combined with each other and `prefix`, select exactly the keys a test wrote that way, with the store's clock driven; a page with values ends at 1,000 keys or 1 MiB, whichever comes first; the walk holds a shard's lock for at most 64 keys at a time and runs on no connection's task; no field is added to an entry and no write call is changed (`ENTRY_OVERHEAD` stays 128 bytes). With T3: the listing of an `eventual` namespace shows a replicated key with its type and expiry on every node, tombstones are not listed, and nothing new is on the wire. |
| **0085-T13** Purge (amendment of 2026-10-11) | T12; its `eventual` half T3; `events=true` T4 | `prefix`, `expires_within`, `no_expiry` and `expired=true`, alone and combined, remove exactly the keys a test wrote that way; a call with no selector is refused; a dry run changes nothing, writes no audit record and answers the count the purge then removes, with a sample of 20 keys and no values; more than 100,000 matching keys take two calls, the second from the first's cursor; a second purge on the node while one runs is refused; a rule's state call during a purge of a full namespace waits for one batch of 64 keys at the most (measured); the `state.purge` audit record carries the subject, the namespace, the selection, the count and whether events were asked for; an operator who is not a listed rules writer is refused, and may still run the dry run; no `$events/state/expired` by default, and with `events=true` one per purged key at the sweep's rate, on the node that ran the purge; `kv_store` can be purged by prefix (every key of it has no expiry), and refuses `events=true`. With T3: a purge on one node of an `eventual` namespace removes the keys on every node, stays removed after a partition heals, and its tombstones count toward `max_keys` until the tombstone window has passed. |
| **0085-T14** Change expiry (amendment of 2026-10-11) | T12; its `eventual` half T3 | `ttl`, `extend` and `clear` on one key and on a selection do what the listing then shows, with the store's clock driven; a result past `max_ttl` is refused for one key and counted as `past_max_ttl` in a selection whose other keys are changed; every call on `kv_store` is refused; a dry run changes nothing and answers the counts the call then reports; a rule's next write with a TTL sets the expiry again; a key whose expiry was moved sooner expires then and, with T4, raises its event; the `state.expire` audit record carries the subject, the namespace, the selection, the change and the counts by outcome; an operator who is not a listed rules writer is refused. With T3, on two nodes: a later expiry and `clear` hold on both after repair for every type; any change of an `lww` key holds on both; a sooner expiry of a key of each other type is refused as `not_shortened` and leaves both copies as they were. |
| **0085-T15** CLI and documentation of T12–T14 | T13, T14 | `state-keys` takes `--values`, `--expires-within` and `--no-expiry`; `state-purge <ns>` and `state-expire <ns> [<key>]` reach their endpoints with every parameter those take, `--dry-run` included; without `--all` they stop at one call's cap and print the cursor, with it they go on until there is none; `--all-nodes` asks every node; `--json` prints valid JSON and the terminal views print control characters escaped; `mqttd --admin help` lists the verbs. ADMIN-API.md has the endpoints with their parameters, roles, audit records and refusals; RULES.md says what each selector means, what a purge raises and does not, that a purge there leaves tombstones, and which expiry changes replicate; THREAT-MODEL.md's row for a rules writer's reach names the purge. |

**Order.** T1 first. Then T2, T6 (without the replication series), T7, T11 and the
single-node half of T4 can run in parallel; T3 and T5 follow T2; the cluster half of T4
and the replication series of T6 follow T3; T8 closes phase 1. T9 and T10 are later
phases.

**The operator's tools (T12–T15),** added on 2026-10-11: T12 follows T6; T13 and T14
follow T12 and can run side by side, each with an `eventual` half that waits for T3 (and
the purge's `events=true` for T4); T15 follows both. They are not part of what T8
follows: T8 closes phase 1's documentation and the feature's tracker (#880) as planned,
and T15 carries the documentation of these three.

## Progress

<!-- status-table:0085 -->
| Task | Status | Issue | When | Evidence / notes |
|------|--------|-------|------|------------------|
| 0085-T1 | ✅ done | [#917](https://github.com/mbilling/fss-mqtt-broker/issues/917) | 2026-10-11 | "PR 929: crates/mqtt-rules/src/state.rs (StateStore and StateNamespace handed over in Functions, [state.<name>] and the rule key state, one table of function names) and crates/mqttd/src/rule_state.rs (local namespaces, typed keys, TTLs, bounds, LRU, the rules.state_max_bytes ceiling, brownout); 141 probes of EMQX 6.3.1 (emqx_rule_funcs and emqx_rule_sqltester) pinned in rule_state::tests, none contradicting the ADR; mqtt-rules tests_state (21), mqttd rule_state::tests (33), tests/rules_state.rs (6), additions to rules_binary.rs, admin_rules.rs and the converter test; dry runs on a scratch store; the rule_state benchmark (0.11-0.14 us per call on one thread, Apple M1 Pro)." |
| 0085-T2 | ✅ done | [#918](https://github.com/mbilling/fss-mqtt-broker/issues/918) | 2026-10-11 | "PR 935: crates/mqtt-rules/src/crdt.rs (the types, a merge that is commutative, associative and idempotent over every state the decoder accepts, the hybrid logical clock with a 60 s skew bound, the canonical bounded encoded form, kv_hll_add and kv_hll_count on a 2^14-register sketch hashed with SipHash-2-4 under a fixed key) and the store keeping its keys in crdt::Data; mqtt-rules tests_crdt (40); the state_merge fuzz target in the nightly tier (about 1.2 million inputs locally, no finding); T1 store tests and the 141 EMQX probes pass unmodified; the ADR amended for the delete rule of non-lww types, first-write expiry, the skew bound and the counter bound." |
| 0085-T3 | ✅ done | [#919](https://github.com/mbilling/fss-mqtt-broker/issues/919) | 2026-10-11 | "PR 950: crates/mqttd/src/rule_state/replica.rs and link.rs (changed keys read per link with no per-link queue, repair by a 1,024-part digest, refusals counted by reason), peer protocol 14 with StateKeys, StateDigest and StateWant pinned in crates/mqtt-cluster/src/peer.rs; tests_replica and crates/mqttd/tests/rules_state_cluster.rs (convergence, restart refill, declaration mismatch, bounds, a v13 peer sent nothing); the rule_state benchmark (publish with one kv_incr 1.80 us local, 1.84 us eventual; 100k keys caught up in 0.14 s on loopback, Apple M1 Pro); the ADR amended for what T3 decided." |
| 0085-T4 | 🚧 in-progress | [#920](https://github.com/mbilling/fss-mqtt-broker/issues/920) | — |  |
| 0085-T5 | ✅ done | [#921](https://github.com/mbilling/fss-mqtt-broker/issues/921) | 2026-10-11 | "PR 956: kv_incr(key, by, ttl, idempotency_key) remembers a key of 1 to 255 bytes as an entry of the namespace for its dedupe_window; exact on one node (8 threads presenting 4,000 keys give 4,000 increments, local and eventual), at most once per node inside the replication lag in a cluster, as D8 says; a refused increment is not remembered; a real QoS 1 DUP resend through the broker is counted once with a publisher-given id and every time with the broker's id; replay transcripts in rules_sequence.rs and RULES.md; the rule_state benchmark shows kv_incr without a key unchanged." |
| 0085-T6 | ✅ done | [#922](https://github.com/mbilling/fss-mqtt-broker/issues/922) | 2026-10-11 | "PR 948: crates/mqttd/src/rule_state/admin.rs and crates/mqttd/src/admin/state.rs (the overview for a viewer, the paged key listing and single-key read for an operator, the audited delete for an operator who is a listed rules writer, replicated in an eventual namespace), the CLI verbs state, state-keys, state-get and state-del with control characters escaped, the mqttd_rule_state_* series and $SYS/brokers/<node>/rule_state/<namespace> with no key and no value; tests for roles, paging under concurrent writes and merges, the key and value representation round trip, the audit record (key_sha256 and key_bytes) and a three-node delete that converges; the rule_state inspect benchmark." |
| 0085-T7 | ✅ done | [#923](https://github.com/mbilling/fss-mqtt-broker/issues/923) | 2026-10-11 | "PR 947 and PR 952: mqttd --rule-test --sequence (crates/mqttd/src/rule_replay.rs) replays a file of JSON lines on a driven clock against a scratch store, for one statement or the configured rules file with its actions, re-entry and [state.*] tables; crates/mqttd/tests/rules_sequence.rs (the kv_held, quota, kv_changed and kv_store_* transcripts, the file format and its errors, the running store untouched) and the RULES.md transcripts run by rules_docs.rs; since PR 952 the replay raises $events/state/expired per key at its expiry time before the step's own message." |
| 0085-T8 | ⬜ planned | [#924](https://github.com/mbilling/fss-mqtt-broker/issues/924) | — |  |
| 0085-T9 | ⬜ planned | [#925](https://github.com/mbilling/fss-mqtt-broker/issues/925) | — |  |
| 0085-T10 | ⬜ planned | [#926](https://github.com/mbilling/fss-mqtt-broker/issues/926) | — |  |
| 0085-T11 | ✅ done | [#927](https://github.com/mbilling/fss-mqtt-broker/issues/927) | 2026-10-11 | "PR 933: crates/mqtt-rules/src/state/dict.rs (a Dictionary per evaluating task, the three names in state::FUNCS) and one dictionary per connection in crates/mqttd/src/rules.rs; 99 probes of EMQX 6.3.1 (direct calls, emqx_rule_sqltester, the REST SQL test, and clients on a running node for whose process holds the dictionary per trigger) pinned in mqtt-rules tests_proc_dict (14) and crates/mqttd/tests/rules_proc_dict.rs (10), plus one test each in admin_rules.rs and rules_binary.rs; bounds of 64 keys and 64 KiB with dictionary_full; the ADR amended where the probes contradicted it; the converter carries proc_dict_* rules live." |
| 0085-T12 | ✅ done | [#940](https://github.com/mbilling/fss-mqtt-broker/issues/940) | 2026-10-11 | "PR 957: GET /admin/v1/rules/state/keys lists each key with type, bytes and expires_at, and with values=true its value, filtered by prefix, expires_within and no_expiry (AND; a contradiction is a 400), for local, eventual and kv_store namespaces, with no field added to an entry (ENTRY_OVERHEAD asserted at 128); expires_within in a local namespace walks the existing expiry order; the single-key read returns bytes; the cursor is hashed in the audit record; state-keys gains --values, --expires-within and --no-expiry; 16 tests in the mqttd lib and admin_rules.rs, the rule_state filtered benchmark." |
| 0085-T13 | ⬜ planned | [#941](https://github.com/mbilling/fss-mqtt-broker/issues/941) | — |  |
| 0085-T14 | ⬜ planned | [#942](https://github.com/mbilling/fss-mqtt-broker/issues/942) | — |  |
| 0085-T15 | ⬜ planned | [#943](https://github.com/mbilling/fss-mqtt-broker/issues/943) | — |  |
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
- 2026-10-11 — **T11 built** (#927, in review): EMQX's `proc_dict_put`, `proc_dict_get`
  and `proc_dict_del`.
  - **EMQX 6.3.1 probed:** 99 probes, of what the functions answer and of which process's
    dictionary each kind of trigger works on (clients on a running node, and a scan of
    every process's dictionary). The answers are as the ADR had them. Two things were
    not: the dictionary lives as long as the *process*, which a session that outlives
    its connection keeps until it is resumed; and `message_dropped` is evaluated by
    whoever published the message, a rule's republish and a Will included. The ADR's
    amendment of this date has the table of triggers.
  - **The engine's half** (`crates/mqtt-rules/src/state/dict.rs`): a `Dictionary` that
    whoever evaluates owns and hands over (`RuleSet::on_publish_in`, `on_event_in`,
    `evaluate_one_on`); the three names in the one table of state functions; 64 keys
    and 64 KiB, `dictionary_full` past either; the bytes counted in the store
    (`StateStore::dictionary_grow`, `dictionary_shrink`). It is `Send` and not `Sync`:
    nothing can share one between tasks.
  - **The broker's half:** one dictionary per connection, in its `ConnRules`, for
    everything that connection's task evaluates; an empty one per Will and per note of
    the notes task; a scratch one per dry run. The store refuses a dictionary room at
    its ceiling and in a brownout, and counts it.
  - **Three evaluations differ from EMQX** and are listed in RULES.md: a Will, an event
    about a session without a connection, and `message_dropped` for a message a rule
    republished or a Will. Each gets an empty dictionary where EMQX's sees the client's.
  - **The converter** carries a rule that calls `proc_dict_*` live, with a note on the
    bounds and those three.
  - **Measured** (`cargo bench -p mqtt-rules`, Apple M1 Pro): the per-publish benchmark,
    which has no state rule, with `main`'s binary and this change's run alternately, four
    times each, taking the fastest sample of each case: `no_match` 109.2 to 109.7 ns,
    `filter_reject` 964 to 959 ns, `filter_republish` 1.788 to 1.780 µs,
    `select_star_republish` 4.519 to 4.513 µs, `foreach_10` 8.127 to 8.148 µs. Between
    −0.5% and +0.5%, which is inside the spread from one run to the next. (The machine
    was busy, so the figures are higher than T1's; the comparison is like for like.) An
    evaluation that is handed no dictionary makes none unless a rule calls `proc_dict_*`.
- 2026-10-11 — **T2 built** (#918, in review). What it is:
  - **The types and their merge** (`crates/mqtt-rules/src/crdt.rs`): a key's state is the
    latest delete it has seen, what it holds since and when that expires; `lww`, `first`,
    `max`, `min`, `sum` (per node, what it added and what it took away), `union`, `hll`.
    The merge is a join (commutative, associative, idempotent) over every state the decoder
    accepts. The ADR's amendment of this date says where D4 could not be built as written:
    a delete covers exactly the writes stamped before it for `lww`; for the other types it
    also covers the writes of nodes that had not heard of it, because no state of bounded
    size can do otherwise.
  - **The clock:** 48 bits of milliseconds and a 16-bit counter, one compare-and-swap a
    stamp, a tag of the node to break ties; it carries on when the wall clock steps back,
    and a stamp more than 60 seconds ahead of the receiver's wall clock is refused rather
    than followed (the bound is the amendment's).
  - **`kv_hll_add` and `kv_hll_count`:** 2^14 registers, listed while few are set and a
    12 KiB array after; values hashed by their exact identity with SipHash-2-4 under a
    fixed key (pinned); Ertl's estimator. Measured error at most 0.6% from 1,000 to 300,000
    values.
  - **The replicated form of a key:** its whole state, at most 265,246 bytes, values as
    ADR 0086 D3 has them; one encoding per state, and a decoder that accepts nothing else.
  - **The store** keeps its keys in these types. A `local` namespace is not stamped and
    not merged, so it answers as T1 did: T1's tests pass as they were, but for the two
    lines that expected `kv_hll_add` to be an unknown function. One answer changed: a set
    that holds maps of equal size reads them in one fixed order, where the order they were
    added in decided before.
  - **Proved by:** property tests over every type, all 256 pairs of concurrent operations
    from each of 17 starting states and three clock orders, 300 random interleavings on
    three nodes with skewed clocks, and the join's laws over reachable and arbitrary
    states; the fuzz target `state_merge` (the decoder on arbitrary bytes; scripts of
    writes, deletes, deliveries, injected states and forgotten tombstones on three nodes,
    which must end with the same bytes), in the nightly matrix.
  - **Measured** (`cargo bench -p mqttd --bench rule_state`): Apple M1 Pro, this build
    and `main`'s run in turn and the best of six rounds taken, because the machine was
    shared and single runs moved by tens of percent. None of T1's cases got dearer: they
    came out 2% to 18% cheaper, a read of a missing key (which this change does not
    touch) 12% among them, so that is the spread between two builds and not a gain. A
    `local` namespace takes no clock, so there is no stamp on its path; a counter that
    only this node counts is held in place, without which all threads on one key cost
    13% to 30% more. The per-publish benchmark without a state rule
    (`cargo bench -p mqtt-rules`, the same way): +0.0% to +1.1%. New: `kv_hll_add` is
    0.30 µs in the store beside 0.15 µs for `kv_incr` in the same run; a stamp of the
    clock is 4 ns alone and 0.2 to 1 µs when eight threads take nothing but stamps.
  - **Measured on Linux** (the same benchmark in an arm64 container, eight cores; T1's
    figures were macOS only): a call is 0.19 µs alone; on keys of their own 0.23 µs with
    two threads, 0.32 µs with four and 0.49 µs with eight; all on one key 0.30 to
    0.39 µs, 0.95 µs and 3.0 µs. The store tries a shard's lock 24 times before it
    waits, a count T1 chose on macOS. On Linux, not trying at all is 7% to 12% dearer on
    one key at four and eight threads and trying 100 times 30% to 75% dearer, so 24
    stays.
  - **Left for T3, as planned:** anything on the wire, stamping the writes of an
    `eventual` namespace, tombstones in the store, the refusal count for clock skew.
- 2026-10-11 — **Admin features approved, two defaults confirmed** (the ADR's amendment
  "admin features and confirmed defaults").
  - **Confirmed by the user:** state is on without a switch, under
    `rules.state_max_bytes` = 256 MiB (`0` turns it off); the names stay `kv_*`. The
    other three defaults stand; `proc_dict_*` is delivered (#933).
  - **T12 to T15 are new** (#940 to #943): view the contents, purge, change expiry, and
    their verbs and documentation. T6 is unchanged and builds D10 as written; T8 is
    unchanged and does not wait for them.
  - **Decided with them:** a purge raises no expiry event unless asked; stale means not
    written for a given time; `kv_store` can be viewed and purged but its keys get no
    expiry; in an `eventual` namespace a purge or a change of expiry replicates, in a
    `local` one it is per node; operator and a listed rules writer; 100,000 keys a call
    with a cursor.
  - **What the code changed in the plan:** the last-write time was to come from T2's
    clock stamp in `eventual` namespaces. Only `lww` keys carry the stamp of their last
    write, and `local` namespaces and `kv_store` carry none, so T12 adds a last-write
    time to every entry: 8 bytes a key, nothing on the wire, each node's own in an
    `eventual` namespace.
  - **Three new choices decided by default** (the ADR's table, 6 to 8): the last-write
    time is not replicated; moving an expiry sooner on a key that is not `lww` in an
    `eventual` namespace is refused, because an expiry merges as the later of the two;
    `clear` is offered although it leaves `max_ttl`.
- 2026-10-11 — **No last-write time: selecting stale keys is dropped** (the ADR's
  amendment "no last-write time").
  - **The user:** "ok, drop the stale key idea completely. drop the extra bytes". Nothing
    had been built on it.
  - **Taken out of T12 to T15** (#940 to #943, titles and text edited): the last-write
    time per key (8 bytes a key, `written_at`, `written_known_since`), the selector
    `not_written_for` in the listing, the purge and the change of expiry, the CLI's
    `--not-written-for`, and T12's measurement of `ENTRY_OVERHEAD`. Decision 6 of the
    ADR's table is withdrawn; 7 and 8 stand by default. The entry above says what was
    planned before this.
  - **What stays:** T12 shows type, size and expiry, values on request, and filters by
    `prefix`, `expires_within` and `no_expiry`, `kv_store` included; T13 purges by those
    and by `expired=true`; T14 changes expiry on T13's selection; T15 carries their
    verbs and documentation.
  - **"Expire stale entries by hand"** is served by purging by prefix, already expired
    and expires within.
- 2026-10-11 — **T7 built, but for the expiry event** (#923, in review):
  `mqttd --rule-test --sequence`. The ADR's amendment of this date has the format and the
  report in full.
  - **The replay** (`crates/mqttd/src/rule_replay.rs`, a library that the binary and the
    tests both call): a file of JSON lines, each a message or a sample event with its time
    (`at` from the start, or `after` the step before), bounded (10,000 steps, 1 MiB a
    line), every error naming its line; one store for the sequence, empty at the start
    and dropped at the end, and one dictionary per client id; the rules run as on a
    connection's task (every matching rule, re-entry, EMQX's order). `--sql` replays one
    statement, as `--rule-test` runs one; without it the configured rules file, actions
    and `[state.*]` tables included. Each step prints its outputs and what it changed in
    the store (`+`, `~`, `-` a key, with value, type and expiry); `--show-state` prints
    the store at the end. Exit 1 when a step had a rule fail or nothing was selected, 2
    for a sequence that cannot be read.
  - **The clock is the sequence's.** The store, `now_timestamp()`, `now_rfc3339()`,
    `format_date`, `timestamp` and `publish_received_at` read the step's time, and message
    ids count from 1, so a replay prints the same every time. The engine reads a replay's
    clock only from an input that carries one (`Input::clock_ms`); no input of a running
    broker does.
  - **What waits for T4:** "expiry events fire in the replay". A key past its expiry reads
    as absent and the next step reports it as expired, in order of expiry; no rule runs
    on it, because `$events/state/expired` does not exist yet. `rule_replay::expire` is
    where T4 raises it. The task stays `in-progress` until then.
  - **Tests:** `crates/mqttd/tests/rules_sequence.rs` (20: the ADR's worked examples as
    transcripts, "hot for five minutes" with `kv_held`, a quota in a window, change
    detection with `kv_changed`, an EMQX `kv_store_*` counter; a rules file with
    re-entry; the clock; TTL expiry; dictionaries; events; failures; the file's format,
    errors and bounds; the running store untouched), four in `rules_binary.rs` (options,
    standard input, the rules file from the environment and from `--config`, exit
    codes), four in mqtt-rules `tests_state`; RULES.md's two transcripts run in
    `rules_docs.rs`.
  - **Also:** `--rule-test` accepts `--config` (it always read the effective
    configuration, and a replay of the rules file needs to name it).
  - **Measured** (`cargo bench -p mqtt-rules`, Apple M1 Pro, `main`'s binary and this
    change's in turn, six rounds, the fastest median of each case; the machine was
    shared): `no_match` 113.3 to 114.0 ns, `filter_reject` 987.8 to 984.6 ns,
    `filter_republish` 1.8335 to 1.8328 µs, `select_star_republish` 4.597 to 4.594 µs,
    `foreach_10` 8.462 to 8.467 µs: −0.3% to +0.6%, inside the spread between runs.
  - **Left for T8, as planned:** the cookbook's stateful recipes as sequences, and the
    "Differences from EMQX" rows.
- 2026-10-11 — **T3 built** (#919, in review): `eventual` namespaces, held by every
  member. What it is:
  - **The copy** (`crates/mqttd/src/rule_state/replica.rs`): a key of an `eventual`
    namespace is kept as T2's state that merges, with its tombstone, and a rule's call is
    one stamped write to it under one shard's lock. A `local` namespace is as T1 and T2
    left it; its path gained one branch.
  - **Sending** (`rule_state/link.rs`, and the peer link's own task in `peer.rs`): a
    shard numbers its keys' changes and a link reads on from where it stopped, 64 KiB a
    frame. Nothing is queued per link, so what waits is at most the keys there are and a
    slow or absent peer costs no memory; a key written a thousand times meanwhile is
    sent once. A write that leaves its key as it was is not sent.
  - **Repair:** a digest of 1,024 parts by a fixed hash of the key; roots compared when
    a link comes up and every 30 seconds, then the parts, then the keys of the parts
    that differ; one repair of a namespace at a time on a node, so that a node that
    joins is filled once and not once per member.
  - **The wire:** `StateKeys`, `StateDigest` and `StateWant` appended at peer protocol
    14 (`PROTO_MIN` stays 6), each naming its namespace and a fingerprint of its
    declaration (the mode, the state's form, the tombstone window). A link that
    negotiated 13 carries none. Applied by the link's reader; never a hub command.
  - **Refused and counted:** a frame for a namespace declared otherwise, or not at all;
    a state stamped more than 60 seconds ahead; a key new to a node, or one that would
    grow, with no room under `max_keys`, `max_bytes`, the store's ceiling or in a
    brownout (what the node held is kept, and repair offers it again); anything
    malformed.
  - **Declared with** `consistency = "eventual"` and, new, `tombstone_window` (an hour
    unless set). `eviction = "lru"` with it is a load error; a namespace cannot change
    its mode while rules hold it; `strong` is still refused.
  - **Metrics:** `mqttd_rule_state_replication_pending`, `…_lag_seconds` and
    `…_refused_total{reason}`, per namespace.
  - **What building it found**, each in the ADR's amendment of this date: a restarted
    node would lose what it counts before it is refilled, so a node counts under a name
    of its own per run (a sixth choice decided by default: it costs a counter a row per
    restart, up to T2's 256); a delete of a key the node holds nothing of does nothing;
    a merge that would grow a key is bounded as a new key is; clock skew can only be
    judged on a state that carries a stamp.
  - **Proved by:** `rule_state::tests_replica` (20: the links driven by hand over
    several stores, so partitions, a slow link, loss, repetition, reordering, restart,
    join and leave are what the test says they are; the bounds after each of 3,000
    random and damaged frames; an `eventual` namespace against a `local` one,
    statement by statement); two tests of the link's task in `peer.rs` (10,000 keys
    cross and the hub is sent nothing for them; a peer announcing 13 is sent no state
    in ten minutes of link time); `tests/rules_state_cluster.rs` (3: real nodes and
    links, rules on every node counting one counter and filling one set, a partition
    healed, a restarted node refilled); the frames' bytes pinned in `mqtt-cluster`,
    with seeds for the `peer_decode` fuzz target.
  - **Measured** (`cargo bench -p mqttd --bench rule_state`, Apple M1 Pro, best of
    three rounds; the ADR's amendment has every figure): a publish with one `kv_incr`
    is 1.80 µs in a `local` namespace and 1.84 µs in an `eventual` one; in the store a
    `kv_set` is 0.15 µs `local`, 0.16 µs `eventual` with no link reading and 0.30 µs
    with one; eight threads on keys of their own 0.92 µs a call beside 0.34 µs, and
    1.65 µs when every write is stamped, where the one clock word shows; a counter is 65
    bytes on the wire and a 44-byte value 105; a node that comes up empty beside
    100,000 keys holds them 0.14 s later (one process, loopback TCP, the broker's own
    link tasks); a write is read on the other node 0.4 to 0.8 ms later. Not compared
    against `main`'s binary run in turn: no room for a second build.
  - **Not run here:** the two-binary rolling upgrade (`cluster_upgrade.rs`, nightly):
    the machine had no room for a second build. It declares no `eventual` namespace,
    and its links to the previous release negotiate below 14, where there is no state
    lane at all.
  - **Left for later tasks:** the expiry sweep and which member raises the event (T4);
    the admin listing and lookup of an `eventual` namespace's keys, which are kept apart
    from a `local` one's (T6); the documentation beyond what keeps RULES.md true (T8).
- 2026-10-11 — **T6 built** (#922, in review): what an operator sees of the store, and
  one key's delete. The ADR's amendment "T6: what an operator sees, as built" has what
  D10 left open.
  - **Metrics:** `mqttd_rule_state_keys`, `_bytes`, `_expired_total` and
    `_expiry_backlog` by namespace, `mqttd_rule_state_ops_total` by namespace, operation
    and result (`ok`, or the store's reason for refusing), and two for the connections'
    dictionaries, which T11 left counted and unexported
    (`mqttd_rule_state_dictionary_bytes`, `_dictionary_refused_total`). The store counts
    an answered call in its shard, under the lock the call holds, and a scrape reads the
    counts; no label is ever a key.
  - **`$SYS`:** one message per namespace per `sys_interval_secs` on
    `$SYS/brokers/<node>/rule_state/<namespace>`, a subtree a grant on the rule
    statistics does not cover: mode, bounds, counts, rates. No key, no value.
  - **The trace** already carried a refused call as its rule's error
    (`kv_set(): namespace_full: …`); a test pins it.
  - **The admin API** (`crates/mqttd/src/admin/state.rs`): `GET /admin/v1/rules/state`
    (viewer: counts only), `GET …/state/keys` and `GET …/state/key` (operator),
    `DELETE …/state/key` (operator and a listed rules writer, audited as
    `state.delete`). A key is shown and addressed as `key` with `key_encoding` (`utf8`,
    `base64`, and `term` for a `kv_store` key that is not a string), so what a listing
    shows is what a read and a delete take; a value as the rule engine writes it. The
    audit log names a key by its SHA-256 and length, in `state.delete` and in the
    requests' own records.
  - **The listing** (`crates/mqttd/src/rule_state/admin.rs`) is in the store's order,
    shard by shard and by the key's bytes within one, with a cursor that is a place in
    it: a key that stays is listed exactly once and no key twice, whatever is written
    meanwhile. A page is at most 1,000 keys, 256 KiB of keys and 50 ms. Looking is not
    using: nothing is moved in an `lru` namespace's order and nothing is removed.
  - **The CLI:** `mqttd --admin state`, `state-keys <ns>`, `state-get <ns> <key>`,
    `state-del <ns> <key>`; control characters in a key or a value are printed escaped,
    in the tables and in `--json`.
  - **Measured** (`cargo bench -p mqttd --bench rule_state -- inspect`, Apple M1 Pro;
    one run on a shared machine at a quieter hour; earlier rounds under load were within
    20% to 40%). A call on the publish path, each thread on keys
    of its own, in a namespace of 100,000 keys: 0.14 µs alone, 0.30 µs while another
    thread lists every key over and over, 0.24 µs while one reads the counts over and
    over; with four threads calling 0.35, 0.49 and 0.45 µs; with eight 0.63, 0.98 and
    0.73 µs. At 1,000,000 keys: 0.26, 2.6 and 0.34 µs alone, 0.62, 3.0 and 0.68 µs with
    four, 1.14, 3.1 and 1.23 µs with eight. The listing looks at a shard 64 keys per
    hold of its lock; the first build took one hold per shard and cost a call 1.2 to
    6.2 µs at 100,000 keys, which is why it was changed. One reading of the counts is
    0.8 µs; one page of 1,000 keys 0.3 ms at 100,000 keys.
  - **With T3 merged** (#950, while this was in review): `eventual` namespaces are in
    the overview and on `$SYS` with their tombstone window and how replication stands
    (links, pending, lag, merged, refused by reason); their keys are listed and read
    with the same cursor and the same promise, tombstones left out; and an operator's
    delete there is a replicated delete, as a rule's `kv_del`, which a test on three
    nodes sees converge and an old copy fail to undo. T3 exports the replication
    series; the two tasks' series share one label type and no name.
  - **Not in T6:** a `state` field for `PUT /admin/v1/rule`, left to T8; and T12's finer
    walk, on which the ADR's amendment has a note.
- 2026-10-11 — **T4 built on one node** (#920, in review): a key's expiry as an event,
  `$events/state/expired`, for `local` namespaces. The ADR's amendment of this date ("T4:
  the expiry event on one node, as built") has every decision. T3 merged while this was
  built (#950); the cluster half is not in this change, and the task stays `in-progress`.
  - **The event** (`mqtt-rules`: `EventKind::StateExpired`): `event`, `namespace`, `key`,
    `value`, `type`, `expired_at`, `timestamp`, `node`. Selected by its exact name and by
    no wildcard: it is not in the table of EMQX's event topics that a wildcard is matched
    against, so `"$events/#"` selects what it selects in EMQX. `"$events/state/#"` is
    refused, naming the topic.
  - **The sweep** (`crates/mqttd/src/rule_state/expiry.rs`): walks the order of expiry
    each shard already keeps, one shard's lock at a time, 64 keys a hold; nothing was
    added to an entry or to a call. Without a rule on the event it only removes. With
    one, an expired key is held (absent to every read, still counted toward `max_keys`
    and `max_bytes`) until its event is handed over, at `rules.state_expiry_per_sec`
    (`MQTTD_RULES_STATE_EXPIRY_PER_SEC`, 1,000; read at startup), a tenth of a second's
    worth at a time, earliest second first, none shed. Nothing is raised after a restart.
  - **Where its rules run** (`Rules::spawn_expiry`): on one task the hub starts with the
    rules, on an empty dictionary, never on the hub loop or a connection; what they
    republish is posted ungated. The task waits for a reload, with no timer, while no
    rule keeps state.
  - **The replay:** `mqttd --rule-test --sequence` raises the event per key, at the key's
    expiry time and before the step's own message (`rule_replay::expire`), 10,000 a step
    at the most. **That is the last of T7's acceptance.**
  - **For T13:** `Store::expire_now` is what a purge with `events=true` calls.
  - **The live demo:** two rules in `demo/rules/rules.toml` (24 now): `car_last_heard`
    sets a key per vehicle with a two-minute expiry on every telemetry record and OBD
    frame, `car_silent` turns its expiry into `alerts/cars/<VIN>/silent`. Seen on the
    wall clock only; the ten-minute replay's expected output is unchanged. Run in a
    Compose project of its own (this build, the car simulator): the car that loses
    its connection at 16:02:21 was last heard at 16:02:16 and its alert arrived at
    16:04:17, a second after the key's expiry; two vehicles whose ignition went off got
    theirs the same way.
  - **Tests:** mqttd `rule_state::expiry::tests` (9: the sweep with and without a rule on
    the event, held keys, a mass expiry of 5,000 keys at the rate in order with none
    shed, the bound, `lru`, `expire_now`, the pace), `rules::tests` (1: the event's rules
    run with no hub at all), `tests/rules_state.rs` (2: end to end on a broker, a rule on
    `"$events/#"` beside it; a reload switching the event on and off),
    `tests/rules_sequence.rs` (4), mqtt-rules `tests_state` (3: the exact name and no
    wildcard, the fields, the sample); RULES.md's new transcript runs in `rules_docs.rs`.
  - **Measured** (Apple M1 Pro, `main`'s binary and this change's in turn, four rounds, the
    fastest median of each case; the machine was shared): a publish evaluated against
    one rule (`cargo bench -p mqttd --bench rule_state`) without a state call 1.431 to
    1.401 µs, with `kv_incr` 1.791 to 1.801 µs, `kv_held` 1.896 to 1.921 µs: −2.1% to
    +1.3% over the six cases. The per-publish benchmark without a state rule
    (`cargo bench -p mqtt-rules`): −3.3% to +2.7%, inside the spread between rounds. A
    call on the store alone came out 4% to 14% cheaper, the spread between two builds.
    New: a sweep pass that finds nothing due in a namespace of 100,000 keys is 0.50 µs,
    once a second; a key the sweep removes 0.135 µs, with or without its event. A
    namespace is 2 KiB larger.
  - **Left: the cluster half.** In an `eventual` namespace a key expires on every
    member at once and no member raises the event yet. What it needs: the sweep over a
    replicated namespace's own order of forgetting (`rule_state/replica.rs`, which also
    holds tombstones), a replicated key held for its event without the hold being
    replicated, the member a hash of the key picks among those alive (the store has no
    view of the membership today), and tests on a cluster for "one node when stable, at
    least once around a membership change". A replay notes a replicated namespace whose
    expiries raise nothing.
  - **With T6,** which merged while this was in review: its expired count takes the
    sweep's removals, its backlog gauge the keys held for their events, and an
    operator's delete of an expired key holds it for its event.
  - **Left, as planned:** full documentation (T8: the threat model's row for a namespace
    held full by owed events, the cookbook recipe).
- 2026-10-11 — **T5 built** (#921, in review): `kv_incr` with an idempotency key counts a
  message once. The ADR's amendment of this date ("T5: counting a message once, as
  built") has every decision; the guarantee is D8's, unchanged.
  - **The call:** `kv_incr(key, by, ttl, idempotency_key)`
    (`crates/mqtt-rules/src/state.rs`: `StateOp::IncrOnce`). The key is a string of 1 to
    255 bytes (D8 said 256), and anything else fails the rule. It is two of a message's
    1,024 calls.
  - **The store** (`crates/mqttd/src/rule_state.rs`: `Handle::incr_once`): the entry that
    remembers the key is inserted if it is not there, under its shard's lock, and only
    the call that inserted it increments; a refused increment takes the entry out again.
    Two map operations, never two locks at once. On one node the check is exact.
  - **The entry** is a key of the namespace under a NUL byte and the idempotency key,
    type `first`, holding when it was first seen, expiring after the namespace's
    `dedupe_window` (new in `[state.<name>]`: five minutes by default, at most an hour
    and at most `max_ttl`), counted toward every bound, evicted by `lru`, replicated and
    merged in an `eventual` namespace. **A `kv_*` key may no longer start with a NUL
    byte** (`bad_key`), so no payload value can read, write or pass for one. It raises
    no `$events/state/expired` and takes none of the events' pace.
  - **Across nodes** (`rule_state/replica.rs`: `Replica::remember`): not counted again on
    a node the entry has reached; counted once more by each node that sees the message
    before it has heard, as D8 said. The merge does not take that count back, and the
    amendment says why it cannot be made exact that way.
  - **What makes a key:** a value the publisher gave the message. Not `id`, which is new
    for every PUBLISH the broker receives, a resend included (pinned by a test on a
    broker). RULES.md has the table.
  - **An operator** sees remembered keys in counts and listings, as `first` keys that
    start with a NUL byte; the prefix of that byte lists exactly them, and deleting one
    lets its message count again. T6's tests are unchanged.
  - **The replay** prints an entry as `idempotency key "<key>"`.
  - **Tests:** mqtt-rules `tests_state` (4 new: the key handed to the store and what is
    refused, no `kv_*` key under a NUL byte, two calls of the budget, `dedupe_window`), mqttd `rule_state::tests::once` (8: once within the
    window, the namespace's scope and window, eight threads with one key, a full
    namespace, a refused increment, the operator's view, expiry without an event,
    `lru`), `rule_state::tests_replica::once` (5: a node that has heard, two nodes that
    have not, the window on every node, a refused increment, threads beside a link),
    `tests/rules_state.rs` (1: a real QoS 1 DUP resend on the same connection and after
    a reconnect, counted once by the publisher's id and each time by `id`),
    `tests/rules_state_cluster.rs` (1: two real nodes), `tests/rules_sequence.rs` (2: a
    message sent again, a rule that re-enters itself); RULES.md's new transcript runs in
    `rules_docs.rs`.
  - **Measured** (`cargo bench -p mqttd --bench rule_state`, Apple M1 Pro, `main`'s
    binary and this change's in turn, eight rounds, the fastest median of each case; the
    machine was heavily loaded, and rounds of one binary differed by up to a factor of
    five): without an idempotency key nothing moved (a publish with `kv_incr` 1.888 to
    1.807 µs, `kv_incr` in the store 127.8 to 126.9 ns; −5.5% to +5.4% over nine cases,
    inside the spread). With one: 224 ns in the store for a remembered key, 449 ns for a
    new one; a publish evaluated with one 2.38 µs.
  - **Left:** a type or filter of their own for remembered keys in an operator's listing
    (T12) and `dedupe_window` in the overview; the cluster half of T4 must leave
    remembered keys out of the expiry event; the threat model's row and the cookbook's
    recipe (T8).
- 2026-10-11 — **T12 built** (#940, in review): looking into the store. The ADR's
  amendment of this date ("T12: looking into the store, as built") has every decision.
  - **The listing** (`GET /admin/v1/rules/state/keys`) says of each key its `type`, its
    `bytes` (what the namespace accounts it as) and `expires_at` (`null`: never), all
    read from the entry: no field was added to an entry, no write call was changed,
    `ENTRY_OVERHEAD` is 128 bytes and nothing new is on the wire. `values=true` adds
    `value`, `value_encoding` and `value_type` as the read of one key shows them;
    operators only, as before.
  - **The read of one key** (`GET /admin/v1/rules/state/key`) says `bytes` too.
  - **Remembered idempotency keys** (T5, which merged while this was in review) are
    listed as the entries they are: type `first`, their size, and an expiry at the end
    of the dedupe window, by which `expires_within` selects them; `prefix` with their
    NUL byte selects exactly them. No type or filter of their own was added.
  - **Filters:** `expires_within=<duration>` and `no_expiry=true`, each alone or with
    `prefix`; a key is listed when every one given selects it, and the two expiry
    filters together are `400`. A duration is the rules file's (`10m`, `1h`) or whole
    seconds. `kv_store` is listed like any namespace: no key of it expires, so
    `no_expiry` selects every key there. Elsewhere a key without a TTL takes its
    namespace's default, so `no_expiry` finds nothing until T14 clears an expiry.
  - **Bounds:** T6's (1,000 keys, 256 KiB of keys, 50 ms of looking), and with values
    1 MiB of them as they stand in the answer; a page always holds one key.
  - **Under concurrent change:** a key that is there and selected from the first page to
    the last is listed exactly once, none twice, in `local` and `eventual` namespaces,
    whether a rule writes or a state is merged in. A key whose expiry changes meanwhile
    may or may not be listed.
  - **One selection for T13 and T14:** `Selector` with `ByExpiry` in the store
    (`rule_state/admin.rs`) and `Selection`, read from a request once, in the admin API
    (`admin/state.rs`). T13 adds `expired=true` as one more `ByExpiry`.
  - **The walk's cost, decided:** no index and nothing per key. `expires_within` in a
    `local` namespace walks the order of expiry the shard keeps for the sweep, 64
    entries a hold, entered where the hold before ended: nothing is passed over and
    nothing starts again. `prefix`, `no_expiry`, and every filter in an `eventual`
    namespace, walk the map as T6 does. Measured (Apple M1 Pro, heavily loaded, the
    faster of two rounds), one whole listing that selects one key in a hundred: by the
    map 1.7 to 2.7 ms at 100,000 keys and 193 to 216 ms at 1,000,000 (2.8 to 4.6 ms and
    238 to 254 ms beside writers); by the order of expiry 0.06 ms and 0.67 ms (0.08 and
    2.0 ms beside writers). A call on the publish path beside a `no_expiry` listing that
    does nothing but walk: 0.61 µs at 100,000 keys and 3.4 µs at 1,000,000, against 0.29
    and 0.74 µs alone; beside an `expires_within` listing 0.32 and 0.90 µs. One page of
    an unfiltered listing went from 0.106 to 0.142 ms at 100,000 keys (each key carries
    more) and is unchanged at 1,000,000.
  - **The audit log:** the request's record says which filters were used and names no
    key. A listing's `cursor` holds the last key listed and was recorded whole by T6;
    it is now recorded by hash and length, as `key` and `prefix` are.
  - **The CLI, ahead of T15:** `mqttd --admin state-keys <ns>` takes `--values`,
    `--expires-within <duration>` and `--no-expiry`, and prints `BYTES` and
    `EXPIRES_AT`, with `VALUE` (cut to 60 characters; whole in `--json`) and
    `VALUE_TYPE` under `--values`. They are T15's by the plan and are built here because
    the listing cannot be tried from a terminal without them. **T15's remaining scope is
    `state-purge`, `state-expire` and the prose** (RULES.md's section on operating the
    store, the threat model's row). ADMIN-API.md and ADMIN-CLI.md describe what is
    built.
  - **Tests:** mqttd `rule_state::admin::tests` (7: the fields, each filter and
    combination on a driven clock, EMQX's table, the values bound, a change of expiry
    between pages, filtered listings beside writers, the order's walk),
    `rule_state::tests_replica` (1: three nodes, merges between pages), `admin::state`
    (3) and `admin::cli` (1, and the verbs' test extended), `tests/admin_rules.rs` (4:
    roles and the kinds of namespace, the filters over the wire, a mebibyte of values,
    the audit record), `tests/rules_binary.rs` (the CLI against a running broker).
  - **Left open:** a map that can be entered by position would make every walk cost
    what it looks at once, with nothing per key and nothing added to a write; it would
    replace the map every state call goes through, so it is its own change.
