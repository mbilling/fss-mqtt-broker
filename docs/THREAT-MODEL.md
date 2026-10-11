# Threat model

**Verified against `v1.1.0` (2026-10-07); ADR 0084's surfaces added 2026-10-08, ADR 0086's (rule functions in a WebAssembly sandbox) 2026-10-11.** This is the one-document answer to "what
is your threat model?" (ADR 0066 T1). It consolidates — it does not invent: every
mitigation row names the ADR that decided it and the code that enforces it, and every
accepted risk is quoted from the record that accepted it. The maintenance rule: a PR
that adds a listener, a peer frame, a store, or a control-plane verb must touch this
file (the ADR 0038 §D frozen-surface enumeration is the checklist for "must touch"),
and each release re-stamps the version header.

Method: STRIDE per trust surface — **S**poofing, **T**ampering, **R**epudiation,
**I**nformation disclosure, **D**enial of service, **E**levation of privilege. Five
surfaces: the client-facing MQTT listener, the authenticated peer bus, the SWIM
gossip plane, the on-disk stores and backups, and the control plane.

**Trust boundaries in one paragraph.** Clients are untrusted until authenticated and
stay authorization-bounded after. Cluster peers are **mutually trusted once
admitted** — mTLS against a dedicated cluster CA admits a node, and an admitted peer
is trusted for everything the peer bus carries (the model defends admission and
detects misbehaviour; it does not defend against an admitted-and-malicious node).
The gossip plane authenticates datagrams but its availability signals are advisory.
Disk is trusted for integrity of what the broker wrote (crash-consistency, schema
gates) but not against a local attacker with filesystem access — file modes and the
audit chain bound, not prevent, that case. The operator is trusted; the control
plane's job is making operator mistakes loud and reversible rather than resisting
the operator.

---

## Surface 1 — Client-facing MQTT listener (TCP / TLS / WS / QUIC)

### Spoofing (identity)

| Threat | Mitigation | Where |
|---|---|---|
| Credential guessing | Argon2id PHC hashes; identical error for unknown-user and wrong-password (no enumeration oracle) | ADR 0004; `mqtt-auth/src/password.rs` |
| Client-cert forgery / CA misuse | mTLS with EKU-checked client certs; SAN-source selection never silently falls back to CN (a CA that can mint any CN must not impersonate a SAN-named workload); absent/ambiguous identity fails closed to anonymous policy = deny | ADR 0004 T11; `mqtt-auth/src/mtls.rs` |
| Token forgery (OIDC/JWT) | Asymmetric-only allow-list (RS256/ES256), `HS*` refused outright (key-confusion class); `iss`+`aud` required; JWKS staleness fails closed past 24 h | ADR 0050; `mqtt-auth/src/oidc.rs` |
| Identity as topic-injection vector | Identities containing `+`, `#`, `/` rejected at the door for every auth source | ADR 0004; `mqtt-auth/src/mtls.rs:151` |
| **Session theft by client-id collision** | Session-owner guard, on by default with no config: a persistent session records its owning principal; a different principal resuming it gets CONNACK `0x87` | ADR 0031; `mqtt-storage/src/lib.rs` (`SessionClaim`) |
| Anonymous access | Default-off; enabling it logs `INSECURE` at startup | ADR 0046; `mqttd/src/main.rs` |
| Forged broker messages on `$SYS` (fake rule statistics or trace) | `$SYS` and everything under `$SYS/` are reserved for the broker by one predicate, whatever the ACL says: a client publish is refused at the ACL step (v5 `0x87`, v3.1.1 dropped, after topic-alias resolution), a Will on it refuses the CONNECT, and a rule republish that renders it fails its action. The one exception is `$SYS/broker/connection/<id>/state`, where a Mosquitto bridge reports its connection state: it is outside the broker's own `$SYS/brokers/`, so nothing the broker publishes can be forged through it, and the ACL decides it like any topic. In the hub only the broker's own `SysPublish` may route a reserved topic; every other path (client and derived publishes, Wills, retained restores, a peer forward that would be retained, is QoS 1 or 2, or names this node) drops it. Retained `$SYS` values stored before the upgrade are cleared by the node that owns them during the first minute after boot (the count is logged at WARN), a restore skips them, and subscribe-time replay never delivers a retained `$SYS` value, so one not yet cleared, or replicated from an older peer, is never seen. The authorization dry run answers such a publish `allowed: false`. Rolling-upgrade window: see the accepted risks | ADR 0084 D1; `mqtt-core` `is_reserved_topic`, `mqttd/src/conn.rs`, `hub/`, `mqtt-rules/src/action.rs`, `admin/authz.rs` |

### Tampering / Elevation (authorization)

| Threat | Mitigation | Where |
|---|---|---|
| Unauthorized publish/subscribe | Deny-by-default at every layer: `DenyAll` until policy configured; file ACL defaults `deny`, deny-wins; SUBSCRIBE denied per-filter (`0x80`/`0x87`) before the hub, PUBLISH dropped before the hub, will topic refused at CONNECT | ADR 0004; `mqtt-auth/src/acl.rs`, `mqttd/src/conn.rs` |
| A deny bypassed through a shared subscription (`$share/g/$SYS/#` past a deny on `$SYS/#`) | A subscribe deny applies when it overlaps the whole `$share/<g>/<f>` string **or** `<f>`. An allow must cover the `$share/…` form, and for a `$`-rooted `<f>` its own inner filter must cover `<f>`, so `$share/+/#` grants no `$SYS`. The dry run explains the same decision | ADR 0084 D2; `mqtt-auth/src/acl.rs` |
| Placeholder abuse (`%i`/`%c`) | A pattern whose placeholder is empty or contains topic metacharacters is unusable — allow grants nothing, deny refuses outright | ADR 0004 T12; `mqtt-auth/src/acl.rs:33` |
| Revoked-but-connected clients | Policy reload sweeps **live** sessions: identity revocation terminates, permission tightening removes grants | ADR 0040; `mqttd/src/reload.rs` |
| TLS downgrade / weak crypto | TLS 1.3 only by default (1.2 is per-listener opt-in); one audited build site, no skip-verification path, one crypto provider passed explicitly | ADR 0002, 0053; `mqtt-net/src/tls.rs` |
| QUIC 0-RTT replay | `max_early_data_size = 0` — 0-RTT disabled | ADR 0036; `mqtt-net/src/quic.rs:52` |
| A rule republishing where its publisher cannot | Not mitigated by the ACL, by design: the ACL decides whether the *original* is accepted, and what a rule derives from it is operator configuration with the ACL file's trust (see the control-plane accepted risks). A derived message carries no publisher identity and never re-enters the rule engine, so a client cannot steer one rule's output into another. A topic template filled from the payload, the client id or the username lets the publisher choose those topic levels (`x/../admin`, a `$`-prefixed level); a rendered topic with a wildcard, NUL, `$share/` or a reserved `$SYS` topic fails the action, and docs/RULES.md documents the `WHERE … regex_match(…)` guard that confines the rest — the rule author's to apply | ADR 0083 §3; `mqttd/src/rules.rs`, `mqtt-rules/src/action.rs`; [RULES.md](RULES.md#security-values-the-publisher-chooses) |

### Repudiation

Auth successes and failures, every ACL denial, and admin actions flow into the
hash-chained audit log (SHA-256, boot-scoped genesis, head emitted on every record —
ADR 0004, 0066 T3; `mqtt-observability/src/lib.rs`). Failures are keyed by client
id, never a credential.

### Information disclosure

The client listener carries broker-originated data only when the operator turns it on
(ADR 0084), and then under the ACL like any topic. A leading wildcard never matches a
`$`-topic, so a grant of `#` grants none of it.

| Threat | Mitigation | Where |
|---|---|---|
| Rule statistics (`$SYS/brokers/<node>/rules[/<id>]`) read by any client | Off unless `rules.sys_interval_secs` is set. They carry rule ids, enabled flags, counts, rates, last activity, a keyed definition hash, the running digest and error **kinds** (`sql`, `action`, `delivery`; a reload's `config`, `rules`, `tls`, … or `policy`), never a rule's SQL, description or actions (a rules file can hold secrets such as a pseudonym salt), a file path, the writer list, or an error's text, whether or not the trace is on (an evaluation error can quote a payload value; a configuration error can quote a configuration line). The definition hash is an HMAC-SHA256 under a key drawn at random when the process starts, so it cannot be used to test guesses of a rule's text. The running digest is an unsalted SHA-256 of the whole file, also on `/metrics` (`mqttd_rules_info`) and `/statusz`: whoever knows the rest of the file can test guesses of a secret in it, so a secret in a rules file must be high-entropy random (`openssl rand -hex 16`), never a name that can be guessed. Read access is an ACL grant on `$SYS/brokers/+/rules/#` | ADR 0084 D4; `mqttd/src/rules_sys.rs`, `rules.rs` |
| The rule trace (`$SYS/brokers/<node>/trace/rules/<id>`) read past the ACL | Off by default (`rules.trace`); turning it on logs a WARN, and `INSECURE:` with no ACL file or `default = "allow"`. It is a separate subtree, so `$SYS/brokers/+/rules/#` does not cover it and a trace grant is written on purpose. **A subscribe grant on a rule's trace topic is a read grant on every message that rule's `FROM` matches** — topic, up to 1 KiB of payload, client id and username — whatever the subscriber's own ACL says about those topics, plus, for a `$events` rule, the connect metadata it selects, and whatever the rule's outputs render (`peerhost`, user properties). Error text, which can quote a payload value, is on `$SYS` only here: a SQL failure as the record's `error`, a failed action as its output's `error`; the statistics never carry it | ADR 0084 D5; `mqttd/src/rules.rs`, `rules_sys.rs` |
| No ACL file, or an ACL with `default = "allow"` | Both statistics and trace are then readable by every client, anonymous ones included where anonymous access is on; each posture already logs `INSECURE:`, and the trace logs its own | ADR 0004, 0084; `mqttd/src/main.rs` |
| A `$SYS` deny bypassed through `$share/<g>/$SYS/…` | Closed: a subscribe deny also matches the filter inside `$share` (Tampering above) | ADR 0084 D2 |
| A broad `$share` allow reaching `$SYS` | Closed: for a `$`-rooted inner filter an allow counts only when its own inner filter covers it, so `$share/+/#` or `$share/#` does not grant `$share/<g>/$SYS/…`, just as `#` does not grant `$SYS/…`. An explicit `$share/+/$SYS/brokers/+/rules/#` still does. Under `default = "allow"`, only a deny on `$SYS/#` (which D2 makes hold for `$share`) keeps it closed; HARDENING.md H-3.5 | ADR 0084 D2; `mqtt-auth/src/acl.rs` (`share_inner_pattern`) |

### Denial of service

| Threat | Mitigation | Where |
|---|---|---|
| Connection floods | Global + per-source-IP caps enforced **at accept, before the TLS handshake** (RAII permits) | ADR 0041 T1; `mqttd/src/admission.rs` |
| Password-hash burn (Argon2 cost as a weapon) | Auth-failure penalty box keyed by source **address only** (never username — no victim-aimed lever), closing at accept before any hash work; hard-bounded table | ADR 0041 T2; `mqttd/src/admission.rs` |
| Oversized packets | Inbound ceiling advertised as MQTT 5 Maximum Packet Size and enforced; outbound honors the client's advertised maximum | ADR 0041 T4; `mqtt-net/src/frame.rs` |
| Slow/stalled subscribers | Per-subscriber bounds on backlog (messages **and** bytes — accounting includes topic+properties, or it would be evadable ~100×), in-flight window, outbound socket bytes | ADR 0041 T10; `mqttd/src/backpressure.rs` |
| Publish floods | Read-pause (TCP backpressure), not drops or kills; in-flight overrun is a protocol error (`0x93`) | ADR 0012, 0041; `mqttd/src/conn.rs` |
| Disk/memory exhaustion | Watermarks → **brownout**: growth writes refused effect-free while acks/reads/expiry continue; two independent axes ORed; refusal travels cross-node as a peer-bus verdict | ADR 0041 T5/T8/T12; `mqttd/src/store_watch.rs`, `hub/policy.rs` |
| The broker's own `$SYS` publishes crowding out clients | Statistics and trace publishes take node-pool ingress credit (ADR 0082), as a peer's QoS 0 forward does, one permit per message, the statistics summary first; when the pool is short the rest of a statistics tick is skipped and a trace record dropped, each counted (`stats_dropped`, `trace_dropped`), never queued unbounded. The trace is also bounded at `trace_rate` records per rule per second (and as many `no_result` records), max(`trace_rate`, 200) per node, and a queue of 1,024 records and 4 MiB; a record copies at most 1 KiB of a payload, 16 outputs and 256 bytes of a topic, client id or username. QoS 0, never retained and live-only: never queued for an offline session or shared member, so no durable append and no stale backlog on reconnect, and each carries a Message Expiry Interval (statistics max(2 × `sys_interval_secs`, 10) s, trace 10 s) for a copy an older node queues anyway; no retained quota | ADR 0084 D1/D4/D5; `mqttd/src/rules_sys.rs`, `hub/` |
| A rules file built to be expensive to load (thousands of worst-case regular expressions) | Identical regex literals are compiled once and a file holds at most 512 distinct ones, 256 KiB together, so a worst-case load stays under a second and about 100 MiB; past either the load fails before the next pattern is compiled. It applies to boot, reload, `--check-rules` and the admin API alike; PCRE2 bounds each compiled pattern itself (64K code units, as in OTP) | ADR 0084 D3, ADR 0083 amendment 2026-10-10; `mqtt-rules/src/parser.rs` |
| Rule work driven by payloads (deep JSON, hostile regex patterns, `FOREACH` fan-out, payload fields passed as function sizes) | Rules evaluate on the publisher's own connection task, never the hub loop; payload JSON is decoded once, in time linear in its keys, with serde_json's recursion limit; regular expressions are PCRE2 as in EMQX (backtracking), bounded per match by OTP's match and depth limits (10,000,000 each per start position; reaching one is no match, as in EMQX) and a 64 MiB backtracking-heap limit, with PCRE2's own compiled-size limit, and a pattern from the payload is compiled once per message; a `FOREACH` iterates at most 10,000 elements and produces at most 256 outputs, and a publish at most 1,024 derived messages carrying together at most 4 MiB beyond four times its payload — counting what its republished messages lead to through other rules, a chain stopping 32 republishes deep and a rule never republishing its own output — all charged to the publisher's ingress credit (what a client/session event or a Will derives is held to the same per-event bounds but charged to no credit — an accepted risk below); a message's functions may build at most 1 MiB beyond their inputs, together (pad lengths, replacements and separators repeated per match or item), `map_put`/`mput` paths have at most 64 segments, timestamps must be renderable in every offset (chrono panics past its range), decimals are Erlang's 0..=253; checked arithmetic; expressions deeper than 256 levels, or nested deeper than 64, are refused at load; an evaluation error fails the rule, never the message or the connection; a publish waiting for its derived messages' credit gives back its own first and waits parked, so waiting connections hold no credit between them; a connection's parked acks are bounded in hub gates. Both untrusted inputs (the rules file, payloads under fixed rules, with payload-supplied sizes) are nightly fuzz targets | ADR 0083 §8; `mqtt-rules`, `mqtt-rules/fuzz` |

### Rule functions in the WebAssembly sandbox (ADR 0086)

Two things run in the sandbox, and neither exists in a broker that did not ask for it:
the rule engine's `jq` function (EMQX's jq, C compiled to WebAssembly), **off unless
`rules.jq = true`**, and the functions of modules a rules file declares
(`[functions.<name>]`), of which there are none by default. With neither, no runtime is
created: no compiler runs, no thread starts, no fault handler is installed, no page is
mapped executable, and every row and risk below is moot. A build without the `jq` cargo
feature does not contain the runtime at all (HARDENING.md H-7.12).

The untrusted input is the **payload**: a value a publisher chose, handed to a program the
operator wrote. The **module** (jq's, or an operator's) is code the operator vouches for;
the rows about it are about how far that trust has to reach and what bounds a mistake.
"Tests" names the test that fails if the mitigation is removed: `jq_module.rs` is
`crates/mqtt-wasm-sandbox/tests/jq_module.rs` (run in CI's `wasm sandbox` job against the
module built there), `sandbox tests` is `crates/mqtt-wasm-sandbox/src/tests.rs`,
`tests_modules.rs` is in `crates/mqtt-rules/src/`, and `rules_binary.rs` is
`crates/mqttd/tests/rules_binary.rs`, which drives the shipped binary.

| Threat | Mitigation | Where |
|---|---|---|
| **A hostile payload driving `jq`: CPU.** A publisher sends input that makes a rule's program run without end (or, where the rule takes its program from the payload, sends the program) | Every call has a deadline that always stops it: `jq/3`'s timeout or `rules.jq_default_timeout_ms`, and never later than the broker's ceiling on one call, `rules.function_max_ms` (the next row). Compiled code checks a counter in every loop and at every function entry; at the deadline the program is asked to stop, and 2 ms later it is stopped by a trap and its instance dropped, which also ends a program inside one long C call (a regular-expression match), where EMQX's own timeout cannot. Compiling a program is bounded separately, at 5 s. The call runs on its publisher's connection task for its first millisecond and after that with the worker thread's other connections handed to another thread, so it delays its own publisher and nobody else; a Will's rules leave the hub while the loaded rules call a sandboxed function. A stopped call is a failed rule, counted (`mqttd_rule_function_errors_total{kind="timeout"}`), never a dropped message or connection | ADR 0086 D6, D7 and its T2 amendment; `mqtt-wasm-sandbox/src/sandbox.rs`, `timer.rs`, `mqttd/src/rules.rs` (`hand_off`). Tests: `a_runaway_program_stops_at_the_timeout_and_the_next_call_is_unaffected`, `a_program_that_cannot_be_asked_to_stop_is_stopped_anyway` (`jq_module.rs`); `a_ten_second_jq_on_one_connection_does_not_delay_another_connections_publish`, `a_will_whose_rules_call_jq_does_not_hold_the_hub` (`rules_binary.rs`) |
| **A publisher choosing how long a call runs.** `jq/3`'s third argument is a value like any other, so a rule such as `jq(payload.program, payload.data, payload.ms)` (legal, as in EMQX) takes its timeout from the payload: in EMQX any number of milliseconds below 2^64, and from 2^63 a timer that never fires. One message would hold a thread and an instance for as long as its sender liked | A broker-wide ceiling on how long any one call may run: `rules.function_max_ms` (default 10000, 1 to 600000, read at startup). No `jq/2`, no `jq/3` whatever its third argument, and no module function runs longer. A `jq/3` that asks for more runs with the ceiling as its timeout and, still running then, fails its rule with EMQX's `timeout` error, counted as `kind="timeout"`; a program that ends in time is unaffected. The ceiling is on the whole call: compiling a program the instance has not seen (otherwise bounded only at 5 s) and making an instance for the call are inside it, and a call ends within some tens of milliseconds of it (under 50 ms at the very most). A rules file cannot raise it: a module's `timeout_ms` above it fails the load, and with `jq` on a `rules.jq_default_timeout_ms` above it is a configuration error; each names both keys. What a payload still chooses is the program, where a rule takes it from the payload (accepted risks, below) | ADR 0086, amendment of 2026-10-11 ("A ceiling on how long any one call may run"); `mqtt-wasm-sandbox/src/jq.rs`, `pool.rs` (`Limits::max_call`), `mqtt-rules/src/plugin.rs`, `mqtt-config` (`refuse_invalid_rules_watch`). Tests: `a_publisher_cannot_make_a_jq_or_module_call_outlast_the_brokers_ceiling`, `rule_test_cuts_a_jq_call_at_the_brokers_ceiling_whatever_timeout_it_names`, `the_function_ceiling_is_checked_and_reported_before_the_broker_runs` (`rules_binary.rs`); `a_ceiling_holds_a_call_whatever_timeout_it_names`, `a_ceiling_covers_compiling_the_program` (`jq_module.rs`); `one_ceiling_holds_a_slow_allocation_and_the_call_after_it_together`, `making_an_instance_for_a_call_is_under_the_calls_ceiling` (sandbox tests); `a_module_call_is_held_to_the_brokers_ceiling_on_one_call` (`tests_modules.rs`); `the_function_ceiling_loads_has_a_range_and_bounds_the_jq_default_timeout` (`mqtt-config`) |
| **A hostile payload driving `jq`: memory, output, stack.** Input that makes the program build gigabytes (`[range(1e9)]`), print them, or nest without end | 64 MiB of linear memory per instance: the growth that would pass it traps. A call's output is charged whole to the message's 1 MiB build budget, which all function calls of the message share; the module is told the bound before it runs and the host refuses to read a larger result. A call runs on a stack of its own, 4 MiB with a guard page below it, whatever the calling thread has left; the module's C stack is 1 MiB inside its own memory. Reaching any of them fails the rule for that message, counted by kind, and the instance is dropped; the next call is served by a new one. An instance that grew past 16 MiB is not kept. EMQX has none of these bounds | ADR 0086 D7; `mqtt-wasm-sandbox/src/sandbox.rs`, `jq.rs`, `mqtt-rules/src/jq.rs`. Tests: `memory_and_output_are_bounded` (`jq_module.rs`); `memory_growth_stops_at_the_cap`, `endless_recursion_overflows_the_modules_stack_and_not_the_hosts`, `a_result_larger_than_the_limit_is_refused_unread` (sandbox tests); `jq_runs_in_the_broker_with_emqx_results_limits_and_metrics` (`rules_binary.rs`) |
| **A hostile payload reaching a memory-safety bug in jq's C code** (jq 1.8.1, oniguruma, decNumber: parsers and a regular-expression engine on publisher-chosen bytes). In EMQX this code is linked into the broker, and `reduce range(20000) as $i (null; [.])` ends a 6.3.1 node | The C code is a guest: it has one linear memory and cannot name anything outside it, so a wild write damages the instance and nothing else. A fault is a trap, the trap is a failed rule, and the instance is dropped. That program returns its result here, and a deeper one fails the call. 400 programs and inputs drawn at random each end in outputs or an error, with the next call served | ADR 0086 D1, D11, spike §4 and §7; `mqtt-wasm-sandbox/src/sandbox.rs`. Tests: `the_module_answers_as_emqx_does_except_where_recorded` (the 285-case oracle, the node-ending case among them), `garbage_programs_and_inputs_end_in_an_answer_or_an_error` (`jq_module.rs`); `a_trap_ends_the_call` (sandbox tests) |
| **`jq` as a way to read the broker's secrets**: `$ENV` and `env` (in EMQX, the broker's whole environment), files through `include` and `import`, and the output republished to a topic the publisher can read | A program sees no environment variable unless the operator lists it in `rules.jq_env`, read at startup. Of the sixteen WASI functions the module imports, none reaches the operating system: every path and descriptor is refused (`include "/etc/passwd"` answers jq's "module not found"), there are no sockets and no randomness, and standard output and error are discarded | ADR 0086 D8 (the one knowing departure from EMQX); `mqtt-wasm-sandbox/src/wasi.rs`. Tests: `the_module_sees_only_what_it_is_granted` (`jq_module.rs`); `jq_sees_only_the_environment_variables_the_operator_listed` (`rules_binary.rs`) |
| **One message's data reaching another's evaluation** through a warm instance, or a publisher filling an instance with programs it sends | A jq instance keeps compiled programs between calls and nothing else: fourteen calls give the same answers on one instance as on fourteen. It keeps at most 32 programs, the most recently used | ADR 0086 spike §5; `jq-module/shim.c`. Tests: `one_call_leaves_nothing_for_the_next`, `the_program_cache_is_bounded` (`jq_module.rs`); `instances_share_no_state` (sandbox tests) |
| **A hostile or broken module** (`[functions.<name>]`): code that loops, allocates without end, traps, returns garbage, or tries to reach the host | It is loaded only if it imports nothing but the sixteen provided functions, exports the ABI at this broker's version, describes the functions the table names, and uses only the WebAssembly features a C module needs (no SIMD, threads, tail calls, 64-bit memories, garbage collector or component model, which keeps whole areas of the compiler out of its reach); a file is at most 16 MiB and a rules file declares at most 16. Running, it can burn its `timeout_ms` (at most the broker's `rules.function_max_ms`, 10 s unless set; an instance's start made for a call is inside it) and its `memory_mib` (at most 64) on each call it gets, and return any value. It cannot read or write memory outside its own, reach a file or a socket, read the clock without `clock = true` or an environment variable outside both its table and the broker's `rules.functions_env`, keep the broker from stopping it, survive its own trap, or see another module's or instance's memory. A thousand damaged variants of a module are each refused or run under the limits, and a coverage-guided fuzz target feeds the loader module files nightly | ADR 0086 D2, D9, D11 and its T3 amendment; `mqtt-wasm-sandbox/src/sandbox.rs`, `mqtt-rules/src/plugin.rs`. Tests: `an_import_outside_the_list_is_refused_at_load`, `another_abi_version_is_refused_at_load`, `damaged_modules_are_refused_or_run_under_the_limits` (sandbox tests); `a_modules_limits_fail_the_rule_and_are_counted_by_kind`, `a_module_is_granted_only_environment_the_broker_lists` (`tests_modules.rs`); `a_declared_module_runs_in_the_broker_under_its_limits_and_stays_pinned` (`rules_binary.rs`); fuzz target `crates/mqtt-wasm-sandbox/fuzz` `module_load` |
| **A module correlating the messages it is called with**: state kept in its memory from one publisher's message to the next | Not prevented by default: a warm instance serves many messages, and the sandbox isolates the module from the broker, not one call from the next. `fresh_instance = true` gives every call a new instance, so nothing survives a call, at the cost of an instantiation each | ADR 0086 D11. Test: `a_fresh_instance_per_call_keeps_no_state_between_messages` (`tests_modules.rs`) |
| **A module file replaced on disk, or rules text naming a file as code** | The control-plane row "Rules text as a way to make the broker load a file as code" (Surface 5): a module is read only from under the rules file's directory, pinned by a SHA-256 that is checked on every load and never disclosed by an error | ADR 0086 D9 and its T3 amendment; `mqtt-rules/src/plugin.rs`. Tests: `a_module_is_only_read_from_under_the_rules_files_directory` (`tests_modules.rs`); `check_rules_refuses_a_module_that_fails_a_check_and_says_why` (`rules_binary.rs`) |
| **The jq module's supply chain**: the one part of the broker that is not Rust (jq, oniguruma and decNumber sources; a C compiler and its libc) | Nothing binary is committed. The module is built in the pipeline from source tarballs pinned by SHA-256, by a compiler (zig) pinned by version and by its archive's SHA-256, and the result is byte-reproducible: the tree records its hash (`jq-module/jq.wasm.sha256`), CI builds it on every change to its inputs and fails on another hash, the broker's build script refuses to embed a file with another hash, and the release recipe builds it under `target/` on every run and requires it, so the release's build-twice comparison covers it. The release notes state the module's hash and the source commits. Whatever those sources contain still runs as a guest of the sandbox, under the rows above | ADR 0086 D11, spike §1 and §2; `crates/mqtt-wasm-sandbox/jq-module/build.sh`, `fetch-zig.sh`, `crates/mqtt-wasm-sandbox/build.rs`, `scripts/release/build-repro.sh`. Tests: `the_module_is_the_recorded_build` (`jq_module.rs`); CI job `wasm sandbox` |
| **The runtime's supply chain**: wasmtime and its Cranelift compiler, 54 lockfile entries | `cargo deny` (licenses, sources, bans) and `cargo audit` (advisories against `wasmtime` and `cranelift-codegen` among them) gate every change. Only the runtime's safe API is used: machine code is never loaded from a file or from bytes (`Module::deserialize` is not called and there is no on-disk code cache, so nobody who can write a directory gets code executed), and the features taken leave out the component model, the WASI crate, the garbage collector, threads and profiling hooks | ADR 0086 §3, D1, D11; `crates/mqtt-wasm-sandbox/Cargo.toml`, `deny.toml`; CI job `supply-chain audit` |
| **Rules as a way to make the broker spend start-up work**: loading rules that call `jq` compiles its module, and a declared module is compiled when its table is loaded | The jq module is compiled once per process (0.6 to 0.8 s), by the first load that calls `jq`, on the thread doing that load, never on a publisher's connection and never at the first message. A module file is at most 16 MiB and a file declares at most 16; a table that did not change keeps its compiled module across reloads. Only the operator's file, a listed rules writer and an operator's dry run can cause a load, and rules text through the admin API is parsed one at a time per node, off the async workers | ADR 0086 D5, §8; `mqtt-rules/src/plugin.rs`, `mqttd/src/admin/rules.rs`. Tests: `a_pool_compiles_on_first_use_and_shares_warm_instances` (sandbox tests); `a_reload_swaps_a_module_and_a_call_in_flight_finishes_on_the_old_one` (`tests_modules.rs`) |

### Accepted risks (client surface)

- **A backtracking regular expression can cost CPU in proportion to its subject** —
  OTP's match limit, which this engine keeps for EMQX's results, applies per start
  position: a pattern that fails slowly at every position of a long subject spends up to
  the limit (about 0.15 s) at each, on the publisher's connection task. Only a pattern
  the operator writes, or one a rule takes from the payload, can do it; a rule that lets
  the payload choose its pattern hands this cost to the publisher. EMQX has the same
  exposure, softened by its scheduler. (ADR 0083 amendment 2026-10-10.)
- **A v3.1.1 denied publish is still plainly acknowledged** — the protocol has no
  negative PUBACK; the denial is visible only in the audit log (v5 clients are told
  `0x87`). QoS 0 denial is a silent drop in both versions. (ADR 0004, issue #246.)
- **`%c` is not a tenant boundary**: the client id is client-chosen; `%c`-scoped
  rules bound a *session handle*, not a principal, unless paired with opt-in
  `connect` rules — which default to permitting every connect. (ADR 0004/0031.)
- **The memory watermark is a watermark, not a ceiling** — overshoot ≤ poll
  interval × allocation rate; the container limit is the hard bound; Linux-only
  (elsewhere it logs once and exits rather than pretending). (ADR 0041 T8/T14.)
- **No byte cap on the durable offline queue** (count cap only) — recorded open as
  0041-T6.
- **Brownout refuses a whole publish** if any matching subscriber's copy needs
  storage (MQTT acks are per-publish, not per-subscriber). (ADR 0041 §5.)
- **A server can never force re-auth** — MQTT 5 re-auth is client-initiated; the
  compensating control is the reload sweep. (ADR 0040.)
- **Will Delay pending state is node-local and in-memory**: a node dying inside the
  window loses the Will — preferred to firing one from a node that no longer owns
  the session. (ADR 0005.)
- **HTTP auth hook outage denies everybody** — the stated cost of fail-closed.
  (ADR 0004 T16.)
- **Event- and Will-derived messages are not charged to ingress credit.** "There is no
  publish to charge them to and no connection to pause. Each event, and each Will, is
  bounded by decision 8's per-message limits (at most 1,024 derived messages, carrying at
  most 4 MiB plus four times a Will's payload)." Nothing limits how often a client raises
  events: the connection caps bound how many connections are open at once, not how fast
  they come and go; the auth penalty box acts only on failed logins; and
  `limits.max_publish_rate` counts publishes, not SUBSCRIBE or UNSUBSCRIBE packets.
  `limits.max_subscriptions_per_client` and the packet size limit bound only how many
  events one SUBSCRIBE raises. A client that may connect can therefore connect and
  disconnect, or subscribe and unsubscribe, in a loop, and make the hub route what the
  operator's event rules derive on every turn without pausing for credit. Each turn costs
  the client a CONNECT (with TLS, a handshake) or a SUBSCRIBE; what it costs the broker is
  set by the event rules, so keep them to a few republishes per event and watch
  `mqttd_rule_evaluations_total` for them. (ADR 0083, Consequences.)
- **The `$SYS` reservation is complete only once every node runs it.** During a rolling
  upgrade an older node still accepts client publishes to `$SYS/…` and forwards them. An
  upgraded node drops such a forward when it would be retained, is QoS 1 or 2, or names
  that node (`$SYS/brokers/<its id>/…`), but a live forged message naming an older node
  reaches subscribers on upgraded nodes until the roll ends. (ADR 0084, Consequences.)
- **A Mosquitto bridge's state topic is the ACL's.** `$SYS/broker/connection/<id>/state`
  is not reserved, so a Mosquitto bridge with notifications on can connect. With no ACL
  file or `default = "allow"`, any client may write a bridge's state there, as on
  Mosquitto. It is outside `$SYS/brokers/`, so it cannot forge the broker's statistics or
  trace. (ADR 0084 D1.)
- **A trace grant is a data grant.** Whoever may subscribe to a rule's trace topic reads
  what that rule selects, past its own ACL. The operator decides who; the broker cannot
  narrow it to the subscriber's own grants without evaluating the ACL per record and
  subscriber. (ADR 0084 D5.)
- **With `jq` on, or a module declared, a compiler is in the trust base.** The rows of
  "Rule functions in the WebAssembly sandbox" hold as long as wasmtime is sound, and
  since the module runs as machine code that includes its compiler, Cranelift: a
  miscompilation is a sandbox escape, and such bugs have been published as advisories.
  What stands against it: the configuration used is the runtime's default and most
  fuzzed one, the WebAssembly features a module may use are cut to the few a C module
  needs, the bytes compiled are a module built from pinned sources or one the operator
  pinned by hash, and a payload reaches compiled code only as data, never as
  WebAssembly: no publisher can hand the compiler input. `cargo audit` gates the
  advisories, and the runtime's minimum Rust version moves the workspace's. Not
  mitigated beyond that; an installation that will not carry it leaves the feature off,
  or out of the build. (ADR 0086 D11, Open question 1.)
- **Once a module is loaded the process has executable pages and process-wide fault
  handlers.** Compiled code needs memory mapped executable (written first, then made
  read-and-execute, never both at once), which is why the shipped systemd unit's
  `MemoryDenyWriteExecute=true` has to be given up for it. The runtime installs handlers
  for `SIGSEGV`, `SIGBUS`, `SIGILL` and `SIGFPE` (Mach exception ports on macOS) when its
  engine is first made, and they stay for the life of the process; a fault that is not in
  compiled module code is passed on to the previous handler. The broker installs none of
  these itself. One timer thread per loaded module runs while calls have deadlines.
  (ADR 0086 §8, D11, D13.)
- **The number of concurrent long sandbox calls is not capped; the time each may take
  is.** A call that outlasts its first millisecond occupies a thread, up to the async
  runtime's limit on blocking threads (tokio's default, 512, which mqttd does not
  change), and an instance of up to its memory limit (64 MiB for `jq`; a module's
  `memory_mib`, 16 MiB by default) beyond the warm ones. A publisher runs one call at a
  time, so the worst case at any moment is as many such calls as there are connections
  publishing to a rule's topics at once, at most 512, times the memory of each: 32 GiB
  with every default and programs that fill their memory. Asked how to bound this
  (2026-10-11), the maintainer chose **a ceiling on how long any one call may run**
  (`rules.function_max_ms`, default 10 s; the row "A publisher choosing how long a call
  runs" above) **over a limit on how many run at once, which is not built**. So the peak
  is bounded only as before, by the blocking-thread limit and the number of publishing
  connections, and what is bounded now is its duration: every one of those calls ends
  at the ceiling, one message buys at most the ceiling of a thread and an instance, and
  keeping them occupied takes a sustained publish rate (ten messages a second per
  connection at a ceiling of 100 ms). It remains a memory and thread lever in the hands
  of whoever may publish to such a rule. The operator's mitigations: a low
  `rules.function_max_ms` (and `rules.jq_default_timeout_ms` and module `timeout_ms`
  under it), a low `memory_mib`, the ACL on the topics such rules select, a limit on
  connections, and a memory limit on the process (the shipped unit's `MemoryMax`, a
  container limit), which is the only bound on the product. Watch
  `mqttd_rule_module_instances` and `mqttd_rule_function_errors_total{kind="timeout"}`.
  (ADR 0086 T2 and T3 amendments, and the amendment of 2026-10-11.)
- **A `jq` rule may take its program from the payload**, as in EMQX:
  `jq(payload.program, payload.data, payload.ms)` is legal rule SQL. Whoever may publish
  to such a rule's topics then chooses what runs, up to the limits above, and with
  `rules.jq_env` set, reads the listed variables. The loader cannot tell an intended use
  from a mistake; the rule author decides. The timeout such a rule takes from the
  payload is no longer the publisher's to choose beyond `rules.function_max_ms`: that
  part of this risk is mitigated (the row "A publisher choosing how long a call runs").
  (ADR 0086 D4, D5.)
- **A long sandbox call in a rule on events about offline clients delays the events
  behind it.** Those events (`$events/message/dropped` for a session that is not
  connected, and the like) are evaluated by one task; a flood that outruns it is shed and
  logged. The hub and every connection are unaffected. (ADR 0086 T2 amendment.)
- **The jq module's source tarballs come from GitHub's archive endpoint**, whose bytes
  are stable in practice and not by contract. A change there fails the build (the
  tarballs are checked against recorded hashes) and never passes other sources; CI caches
  the built module, so an outage fails only a run that has to rebuild. (ADR 0086 Open
  question 8.)

---

## Surface 2 — Authenticated peer bus

### Spoofing

| Threat | Mitigation | Where |
|---|---|---|
| Rogue node joins the mesh | Mutual TLS against a **dedicated cluster CA** — possession of a cluster cert is admission; client cert required on accept, server cert verified on dial | ADR 0002; `mqttd/src/peer.rs` |
| Admitted cert claims another node's id | `Hello.node_id` must equal the certificate Subject CN, checked on **both** link directions | ADR 0004 step 5; `mqttd/src/peer.rs:340` |
| Revoked node keeps its links | Cluster CRL read per accept/per dial from a `watch` — a reload applies without restart; revoked fails closed | ADR 0040 T4; `mqttd/src/peer.rs` |

### Tampering / Elevation

| Threat | Mitigation | Where |
|---|---|---|
| Frame confusion across versions | Proto negotiation (`PROTO_MIN..PROTO_MAX`); no overlap → link dropped loudly; `Hello`/`ProxyHello` byte-frozen (readable before negotiation, forever); strict codec — unknown variant or trailing bytes tears the link down | ADR 0038, 0039; `mqtt-cluster/src/peer.rs` |
| Stale owner writes after takeover | Epoch fencing per placement group: followers fence a superseded lease-holder on a newer epoch (deliberately not one global fence, which would let the highest epoch fence everyone) | ADR 0006, 0037, 0042; `mqtt-cluster/src/cluster_log.rs` |
| Forward loops | A peer `Publish` reaches local subscribers only, never re-forwarded — a protocol invariant | ADR 0014; `mqtt-cluster/src/peer.rs` |

### Accepted risks (peer bus)

- **An admitted-and-malicious peer is inside the trust boundary**: it can already
  inject publishes as any topic. Session-proxy vouching (`ProxyHello`) grants no
  *new* capability and records `via=<node>` in the audit trail — detection, not
  prevention. (ADR 0005 §3.)
- **The plaintext peer mesh (opt-in, logged INSECURE) has no CN binding.** (ADR 0004.)
- **The starter Helm path puts every node's peer key in one Secret** — any broker
  pod can read any other's key; CN binding stops outsiders, not a compromised pod.
  Per-pod isolation is the documented cert-manager path. (ADR 0047.)

---

## Surface 3 — SWIM gossip plane

### Spoofing / Tampering

| Threat | Mitigation | Where |
|---|---|---|
| Forged datagrams | Keyed MAC on **every** datagram, verified constant-time **before decode** — unauthenticated bytes never reach the state machine | ADR 0003; `mqtt-cluster/src/swim_auth.rs` |
| Node-level impersonation | V2/V3 postures add per-node signatures chained to the cluster CA; authenticated cert CN must equal the claimed `from`; strict postures — no cross-posture acceptance | ADR 0022/0023; `swim_auth.rs`, `swim_driver.rs` |
| Replay | V3 posture: per-node monotonic sequence persisted by clock-free block reservation + RFC 6479 sliding window keyed on the **authenticated** sender | ADR 0023 |
| Revoked node keeps gossiping | Cluster-CA-signed CRL checked on every inbound signed datagram; an unsigned CRL is refused (an unauthenticated revocation list is a DoS lever) | ADR 0022 T7; `mqtt-auth/src/signed_gossip.rs` |
| Foreign-cluster confusion / split-brain | Cluster identity: founder mints, joiners adopt on first authenticated contact; foreign gossip dropped and counted (`cluster-mismatch`); the refound guard latches NotReady when surviving peers contradict a re-bootstrap | ADR 0054; `mqtt-cluster/src/cluster_identity.rs` |

### Accepted risks (gossip)

- **ADR 0003 accepts replay in the V1/V2 postures** — bounded and self-healing via
  incarnation supersession; a replayed `Dead` costs one refutation. The V3 posture
  closes it; V3 is opt-in hardening.
- **A claim at a higher generation is deliberately not fought** — it means another
  process runs with the same id; refutation yielding is the correct behaviour.
- **Unkeyed SWIM remains possible**, loudly logged INSECURE; weak keys are startup
  errors, not a degraded mode. (ADR 0003 §3.)

---

## Surface 4 — On-disk stores and backups

### Tampering / integrity

| Threat | Mitigation | Where |
|---|---|---|
| Foreign/older/newer store files | Schema gate on every store (the four broker stores + the bridge spool): fresh stamped, newer refused, older migrated one committed step at a time or refused on a gap | ADR 0038 T2, 0058; `mqtt-storage/src/schema.rs` |
| Cross-node data-dir mixups | `node-id` ownership stamp — a directory stamped by another node refuses to open; `cluster-id` persisted beside it | ADR 0018, 0054; `mqtt-storage/src/data_dir.rs` |
| Concurrent opens | redb exclusive `flock`; no second reader in- or cross-process | ADR 0061; `mqttd/src/backup.rs` |
| Truncated/tampered backups | Trailer with SHA-256 over every prior byte; missing/malformed trailer refuses; `complete=false` refuses; unknown record kind refuses (a silently skipped kind is data loss); mixed cluster ids refuse naming both | ADR 0062; `mqttd/src/backup.rs` |
| Restore onto live data | Restore only into a **fresh** data dir, checked before any store opens; interrupted restores never resume; `restored-from` stamp is the licence to boot | ADR 0062 §7 |
| Restored-session theft | Session owners travel in exports and are re-applied through `claim_session` — a foreign principal cannot adopt a restored session | ADR 0031/0062 |

### Accepted risks (disk/backup)

- **Export files are plaintext data-plane content** (payloads, client ids, owners).
  Mode 0600, but at-rest encryption and the backup volume's trust are the
  operator's; a shared backup volume is a lateral-movement path. (ADR 0062.)
- **Restore verifies the set, never the target** — a complete set from the *wrong*
  cluster is accepted; nothing declares which cluster a node expects. (ADR 0062.)
- **No cross-store atomic cut**: an export claims a window, not an instant; a
  restore resurrects sessions cleanly ended inside that window. (ADR 0062.)
- **A local attacker with filesystem write access is out of scope** — the audit
  chain makes after-the-fact tampering with the *audit record* detectable once
  heads are shipped, but store files themselves carry no MAC.
- **The aggregate disk mark cannot name the store eating the budget** (the >70%
  skew WARN is the compensating signal), and **a browned-out follower keeps
  applying peers' committed appends into `replicas.redb`** — the dominant store's
  growth is not gated locally on a cluster node. (ADR 0041 §5.)

---

## Surface 5 — Control plane

### Design posture

There is **no dashboard**, and **configuration is never written
over the network** (ADR 0033/0051, kept by ADR 0081 §5), with one opt-in exception: an
operator whose subject is listed in `[rules] admin_writers` may replace the rules file
through the admin API (ADR 0084). Nothing else is written, and that write is a write of the
file, applied by the ordinary reload: the file stays the only source.
The lifecycle surface is signals and files: SIGHUP reload, SIGUSR1 decommission, SIGUSR2
backup, SIGTERM drain. The unauthenticated HTTP surface is strictly read-only GET/HEAD
(`/livez`, `/readyz`, `/statusz`, `/metrics`), hand-rolled, carrying no secret material,
on an ops-network trust model (ADR 0020, 0054).

The **admin API** (ADR 0081) is a separate, **authenticated** listener, off unless
`admin.bind` is set. It serves what `/statusz` must not (client and session detail) and a
short, fixed list of audited actions:

| Threat | Mitigation | Where |
|---|---|---|
| Spoofed admin caller | TLS 1.3 with a **required** client certificate from `admin.client_ca` (or the cluster CA); no plaintext mode, no anonymous access; resumption off, so every connection is fully verified | ADR 0081 §1; `mqtt-net/src/tls.rs` `admin_acceptor` |
| Elevation (viewer → operator, cert → any role) | Roles only from the verified subject matched against the live `admin.viewers` / `admin.operators`; a subject in neither is refused; every endpoint declares its least role | ADR 0081 §1; `mqttd/src/admin/roles.rs`, `routes.rs` |
| A node certificate used as an admin credential | A cluster-CA certificate in no list gets only the `peer` role (this node's own state), checked by re-verifying the chain against the cluster CA alone, not by subject | ADR 0081 §2; `ChainCheck` |
| Repudiation / disclosure of identifying data | Every request, reads included, is audited (`admin.request`: subject, role, method, target, status) into the hash-chained log | ADR 0081 §1; `mqttd/src/admin/mod.rs` |
| An admin reload as a config-injection path | `POST /admin/v1/reload` takes no input (the rules writes, below, are the one input path, and only for the rules file): it runs the same validate-before-swap reload as `SIGHUP` over the config file; operator role only; serialized with the other triggers; audited twice (`admin.request`, `security.reload trigger=admin`) | ADR 0081 §4/§5; `mqttd/src/admin/config.rs`, `reload.rs` |
| A rules write as a policy-injection path | Off unless `[rules] admin_writers` names subjects; a write needs the operator role **and** a listed subject (a kick-and-cordon operator cannot write rules). The new text is loaded all-or-nothing before anything is written; `if_match` against the on-disk digest (required for a whole file) stops a lost update, and writes are serialized per node, so two writes naming the same digest cannot both win; the write is node-local and never forwarded; every write is a `rules.write` audit record (subject, operation, rule, old and new digest, applied) and the previous file stays as `<file>.prev` for investigation and rollback | ADR 0084 D6; `mqttd/src/admin/rules.rs` |
| The rules write path abused on the filesystem | The path comes from the live config, never the request. The configured path is canonicalized, so a symlink's target is replaced, never the link; the temporary file is created exclusively (`create_new`) with a pid-and-random name in the target's directory, with the old file's mode and, where the broker may set it, its group; when it cannot keep the group, the group permission bits are cleared rather than carried to another group, and a file the API creates is 0600; write, fsync, rename, directory fsync; the temporary file is removed on every error; a read-only or foreign mount answers `409 rules-file-unwritable` with the OS error, and a WARN at boot and on reload names a configured but unwritable directory. Keep a writable rules file in a directory of its own, never beside the config, ACL or password files | ADR 0084 D6; `mqttd/src/admin/rules.rs` |
| Rules text as a way to make the broker load a file as code (a `[functions.<name>]` table naming a WebAssembly module, whether the text comes from the file, a rules write, a `check` or a `test`) | A module is read only from under the directory of the configured rules file: a relative path of plain names ending in `.wasm` that still lies there after every symlink is resolved, a regular file of at most 16 MiB. The broker writes nothing there that is a module (only the rules file, which is TOML, its `.prev` and a temporary file), so a caller can name only a file the operator placed, and only by a SHA-256 they already know: the error for another hash never states the file's own. The file is hashed on every load. What loads runs in the WebAssembly sandbox: no file, socket or host function, the clock only if the table says so, per-call time, memory, output and stack limits; and the environment variables a table may grant are only those the broker's own `rules.functions_env` lists, which no rules text widens. Residual: a rules writer (or an operator, by a dry run) can run any module the operator placed whose hash they know, under the names it describes; module files should not be writable by the broker's user; and the sandbox is as sound as its runtime and compiler ([Rule functions in the WebAssembly sandbox](#rule-functions-in-the-webassembly-sandbox-adr-0086) has the rows for what a loaded module can and cannot do, and the accepted risks under it) | ADR 0086 D9, D11 and its T3 amendment; `mqtt-rules/src/plugin.rs` |
| Rules text disclosing secrets (pseudonym salts in SQL) | The rules file's text, the SQL and actions, error text and `test` (which computes a salted pseudonym for any input: an oracle) are operator-only; a viewer's `GET /admin/v1/rules` is redacted to ids, descriptions, FROM filters and events, counts, definition hashes and error kinds: no SQL, actions, load warnings or error text; `$SYS` never carries them. A definition hash is keyed per process, so it is no oracle. The whole file's digest is an unsalted SHA-256 on `/metrics`, `/statusz`, `$SYS` and the admin API, so a secret in the rules file must be high-entropy random (`openssl rand -hex 16`), never a guessable name | ADR 0084 D4/D6; `mqttd/src/admin/rules.rs`, `rules.rs` |
| Payload text reaching an operator's terminal (a last error quoting a payload with terminal escapes) | `mqttd --admin` prints every control character in text the server sends (C0, DEL, C1, U+2028/U+2029) as its escape: `\u{1b}`, `\r`, `\u{9b}` in the terminal views, `\u001b`, `\u009b` in `--json`; `rules-source` is verbatim | ADR 0084 D7; `mqttd/src/admin/cli.rs` |
| A dry run disturbing the running rules | `check` and `test` evaluate with a no-op report: no counter, no `last_error`, no trace record, no WARN slot, no console log line | ADR 0084 D6 |
| Secrets through `GET /admin/v1/config` | Served from `Config::redacted`: gossip keys and URL credentials/queries become `sha256:` fingerprints; key material is in files (paths only) | ADR 0081 T6, T11; `mqttd/src/config_view.rs` |
| A forged or replayed forwarded action | Only the `peer` role may forward, only to `kick`/`purge`, and only with the `forwarded_for` marker; `peer` is granted by re-verifying the chain against the cluster CA; the receiving node never forwards again; both nodes audit (the owner's record names the peer and the operator) | ADR 0081 §4; `mqttd/src/admin/actions.rs`, `routes.rs` |
| Cordon as a denial of service | Operator role only; this node only; not persisted (a restart clears it); visible on `/readyz`, `/statusz` and `admission_rejected{reason="cordon"}`; audited | ADR 0081 §4; `mqttd/src/admission.rs`, `health.rs` |
| Hiding activity by lowering the log level | Audit records are `tracing` events under target `audit`; every override keeps `audit=info` and a filter naming `audit` is refused; overrides expire (at most 1 h) and are visible on `/statusz` and audited | ADR 0081 §4; `mqttd/src/log_filter.rs` |
| Mistaken purge (data loss by operator error) | Operator role only; one client id per request (no wildcards, no bulk); audited; `kick` is the non-destructive option | ADR 0081 §4 |
| Amplification through the cluster view | One viewer request fans out to one `/admin/v1/node` call per member, each with a 3 s deadline, all within the 30 s handler deadline; the `peer` role cannot itself fan out, so fan-out never recurses | ADR 0081 §2 amendment; `mqttd/src/admin/cluster.rs` |
| Denial of service via the admin port | One request per connection, 10 s to send it, 16 KiB head / 64 KiB body caps (1 MiB for the rules `check`, `test` and write routes, granted from the caller's role before the body is read, so an unlisted certificate cannot make the broker buffer more), 32 concurrent connections, a 30 s handler deadline, paged lists; rules text is parsed off the async workers, one parse at a time per node, under the regex budget (Surface 1) | `mqttd/src/admin/http.rs`, `mod.rs`, `admin/rules.rs` |
| Admin detail on the ops network | Never on the health/metrics listener: `Config::validate` refuses an `admin.bind` equal to either | ADR 0081 §1; `mqtt-config` |

| Threat | Mitigation | Where |
|---|---|---|
| Bad config brick / fail-open reload | Atomic validate-before-swap: new values built first, published only on success; malformed file leaves running policy unchanged; every reload audited; `--check-config` validates before any port binds | ADR 0032, 0046; `mqttd/src/reload.rs` |
| Secret leakage via config | Secrets referenced by path only, never inlined; unknown config keys refuse (listing all) unless the rollback-window hatch is set | ADR 0046 T5, 0058 T4 |
| Operator (Kubernetes) overreach | Every destructive remediation opt-in per scenario, defaults Alert; **no action deletes data, ever** (fenced PVCs are labelled, not deleted); ambiguous evidence → no action; at most one destructive act per reconcile | ADR 0055; `mqttd-operator/src/remediate.rs` |
| Repudiation of admin acts | Reloads, sweeps, backups audited into the same hash-chained log | ADR 0004/0032 |
| A rules file as an injection path | The rules file is operator configuration on disk, like the ACL file: loaded all-or-nothing, a file that does not load refuses the boot and rejects a reload with the running rules kept; no network verb writes it unless `[rules] admin_writers` names the caller (the rules-write rows above); actions are confined to the broker (`republish`, `console` — a sink action is refused at load), and `getenv` reads only `EMQXVAR_…` variables (as EMQX's does), so a rule sees only the environment the operator exposes to it; `mqttd_rules_info{checksum}`, `/statusz` and the cluster view's `same_rules` expose per-node drift | ADR 0083 §5/§6/§8, ADR 0084; `mqttd/src/reload.rs`, `mqtt-rules` |

### Accepted risks (control plane)

- **The operator is trusted.** Signals-and-files means anyone with process/file
  access is the operator; the only in-broker RBAC is the admin API's two roles. Host and
  orchestrator access control is the boundary for everything else.
- **Admin credentials are as strong as their CA.** Whoever can mint a certificate from
  `admin.client_ca` with a listed subject holds that role; the admin listener's
  certificate, key and CA are restart-scoped (the role lists hot-reload). An admitted
  cluster node can read any node's state through the `peer` role, consistent with the
  peer-bus trust model above.
- **A rule can publish where its publisher cannot.** Whoever writes the rules file
  can derive messages onto any topic from any accepted publish, bypassing the
  publisher's ACL for the derived copy, and can amplify one publish into up to 1,024.
  That is the ACL file's trust level, held by the same operator. (ADR 0083.) A subject in
  `[rules] admin_writers` holds the same trust over mTLS: data-plane read and write of
  every topic through rules and their trace. (ADR 0084.) With modules in the rules
  file's directory, the same subject, and any operator through a dry run, can also run
  any of them whose SHA-256 they know, under the names it describes and with limits up
  to the ceilings (10 s, 64 MiB). They cannot bring a module of their own. (ADR 0086 T3
  amendment.)
- **Rules writes are per node.** A write changes the node that answered; the others keep
  their file until written too. `same_rules` and `mqttd_rules_info` show the drift; nothing
  prevents it. A ConfigMap or GitOps pipeline that owns the file overwrites a write on its
  next sync, which is why `admin_writers` stays empty there. (ADR 0084.)
- **The live rules demo's editor is an unauthenticated operator.** `demo/rules-live` runs
  a small web server holding an operator and writer certificate, with no login: whoever
  reaches it may rewrite the demo broker's rules. It listens on loopback only, refuses a
  foreign `Host` or `Origin`, requires a JSON content type and a custom header on every
  change, renders everything with `textContent` under a strict CSP, and its broker is
  anonymous with no ACL. A demo, never a deployment pattern. (ADR 0084 D8.)
- **Metrics/health are unauthenticated by design** on the ops network; they carry
  no secrets, but topology and load are visible to anyone who can reach the port.
  (ADR 0020 §2.) The rules file's digest is among them (`mqttd_rules_info`, `/statusz`):
  an unsalted SHA-256, so a secret in that file must be high-entropy random. (ADR 0084.)

---

## Cross-cutting residuals (the honest list)

These are the accepted risks most likely to matter in a deployment review,
consolidated from the ADRs that accepted them:

1. **Bus factor and track record** — one maintainer, no production users; the
   panel's Bucket C. Time and adoption move it; nothing in this file does.
2. **Single node has no quorum to defend** — a lone broker runs the durable
   machinery with R=1; without a data dir it is not restart-durable (refused
   unless explicitly opted in). (ADR 0029.)
3. **Durable capacity is pinned to the lease voter set** (default 5), not node
   count. (ADR 0021/0049.)
4. **The SIEM export story is in flight** (ADR 0066 T3): the chain is now
   cryptographic with anchored heads, but syslog/OTLP transport, the frozen kind
   vocabulary, the drop policy, and the verify tool are scheduled, not shipped.
5. **Mid-roll protocol skew windows** are documented per feature (e.g. a proto<7
   link answers brownout the v3.1.1 way). (ADR 0041 §5.)
6. **Rule functions in the WebAssembly sandbox are opt-in, and opting in adds a
   compiler to what the broker's memory safety rests on.** Off by default and absent
   unless used; on, the sandbox is as sound as wasmtime and Cranelift, the process maps
   executable pages and carries process-wide fault handlers, and concurrent long calls
   are bounded only by the runtime's thread limit and each call's own time and memory
   limits: a cap is not decided. (ADR 0086; Surface 1, accepted risks.)

Corrections to this document follow the same rule as COMPARISON.md: versioned,
dated, and cited — a threat model that drifts from the code is worse than none.
