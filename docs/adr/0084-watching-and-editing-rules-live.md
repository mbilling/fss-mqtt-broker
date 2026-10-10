# ADR 0084 — Watching and editing rules live

- **Status:** Accepted
- **Date:** 2026-10-08
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0084-watching-and-editing-rules-live.md](../delivery/0084-watching-and-editing-rules-live.md) — plan, progress, and changelog
- **Revisits:** [ADR 0081](0081-admin-api.md) §3 (the CLI's verbs follow the endpoints one
  to one), §5 (the API sets no configuration) and its rejected `$SYS` alternative;
  [ADR 0051](0051-evaluation-readiness.md) (`$SYS` among the deliberate absences). Their
  decision text stands; each carries a "Revisited by" note pointing here.
- **Related:** [ADR 0083](0083-rule-engine.md) (the rule engine this record makes
  observable and editable), [ADR 0082](0082-bounded-hub-ingress.md) (ingress credit, which
  the broker's own publishes now take), [ADR 0020](0020-metrics-and-observability.md) (the
  per-rule metrics the statistics read), [ADR 0004](0004-identity-and-authentication.md) /
  [ADR 0010](0010-shared-subscriptions.md) (the ACL and `$share`, where D2 closes a
  bypass), [ADR 0032](0032-hot-reloadable-security-policy.md) /
  [ADR 0033](0033-config-file-watch-reload.md) / [ADR 0046](0046-file-based-configuration.md)
  (validate-before-swap reload; the file as the source of truth),
  [ADR 0037](0037-durable-retained-messages.md) / [ADR 0062](0062-online-backup-and-restore.md)
  (retained messages and restores, which D1 filters),
  [ADR 0054](0054-operator-facing-state-surface.md) (`/statusz`),
  [ADR 0058](0058-one-dot-zero-stability-contract.md) (the frozen client-visible behaviour
  D1 and D2 change).

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0084-watching-and-editing-rules-live.md).

## Context

ADR 0083 put a rule engine on the publish path. Writing a rule and watching it work is a
loop of edit, reload and look, and today every step of that loop is indirect:

- **Watching.** Each rule has two Prometheus counters, `mqttd_rule_evaluations_total` and
  `mqttd_rule_actions_total`. Nothing reads them inside the broker: there is no "last
  fired", no "last error" (the error text is only logged, one WARN per rule per 10 s), no
  rate, and no record of the last reload. What a rule produced from a given message is
  visible only through a `console` action in the log.
- **Editing.** The rules file changes only on disk. The admin API's `reload` takes no
  input, and ADR 0081 §5 says the API sets no configuration.
- **A demo.** The maintainers asked for an interactive demo: simulated devices, the rule
  engine applied to them, topics a user points their own MQTT client at to see what the
  rules ran, and a simple page to edit rules and apply them while the data flows.

The MQTT convention for a broker's own information is the `$SYS/` topic tree (MQTT 5
§4.7.2; Mosquitto and EMQX both publish there). mqttd has none, by record: ADR 0051 lists
`$SYS` among the deliberate absences, and ADR 0081 rejected `$SYS` for admin data because
it "puts operator data on the client listener under the client authorization model". Four
facts about the code as it stands make `$SYS` unsafe to start publishing on:

1. **Clients can publish to `$SYS/…`.** `valid_topic_name` refuses only wildcards and NUL;
   only the ACL stands in the way, and with no ACL file every client may publish anywhere.
   A Will topic is checked the same way. A rule can republish into `$SYS/…` too: a topic
   template made only of placeholders reaches any topic. Anything the broker published
   there could be forged.
2. **A deny on `$SYS/#` can be bypassed with `$share`.** The ACL matches a subscribe
   against the whole filter string. `$share/g/$SYS/#` does not overlap `$SYS/#` (their
   first levels differ), so with a broad `$share/#` grant, or `default = "allow"`, the deny
   does nothing, while the shared subscription receives what `$SYS/#` matches.
3. **Parsing a rules file has no file-wide bound.** Each regex literal is compiled at load
   and bounded on its own (1 MiB), but the file is not: a 15.9 KB file of copies of one
   worst-case pattern took 4.0 s and 414 MB to parse, and a 795 KB file was OOM-killed at
   13.9 GB. Once the admin API accepts rules text, that is one request away.
4. **The broker's own publishes are uncharged.** A `HubCommand::Publish` with no
   publisher takes no ingress credit, and the hub's channel is unbounded: the shape
   ADR 0082 was written to end.

## Decision

### D1. `$SYS/` is reserved for the broker

`$SYS`, and every topic under `$SYS/`, belongs to the broker, with one exception:
`$SYS/broker/connection/<id>/state`, where a Mosquitto bridge reports its connection
state. One predicate, `mqtt_core::is_reserved_topic`, decides it everywhere,
case-sensitively (`$sys/x` is an ordinary topic):

- **A client publish** to a reserved topic is refused at the ACL step, whatever the ACL
  says, through the ACL's own refusal: MQTT 5 `0x87` on the PUBACK or PUBREC, MQTT 3.1.1
  acknowledged and dropped, no rule evaluated, an `acl.deny.publish` audit record. A topic
  alias is resolved first, so an alias cannot get around it.
- **A Will** on a reserved topic refuses the CONNECT (`0x87`, `acl.deny.will`).
- **A rule's republish** that renders a reserved topic fails that action. The rules loader
  warns when a republish topic's fixed start (or the whole topic, for one without
  placeholders) already puts every topic it can render in the reserved tree, such as
  `$SYS/brokers/${x}`, and when a `FROM` can match only reserved topics (`$SYS/brokers/#`;
  not `$SYS/#`, which matches a bridge's state): such a rule never fires, because clients
  cannot publish there and the broker's own `$SYS` messages never run rules. A template
  whose placeholder decides, such as `$SYS/${x}/y`, is not warned about and fails at run
  time.
- **The hub** routes a reserved topic only from one command,
  `HubCommand::SysPublish` (QoS 0, never retained, no publisher). Every other path —
  client and derived publishes, Wills, a retained restore (skipped and answered as done),
  a peer's forward that would be retained, is QoS 1 or 2, or names this node — drops it
  and counts it. Retained values under `$SYS/` are cleared by their owner during the first
  minute after boot, and subscribe-time replay never delivers one.
- **The broker's own `$SYS` messages are live-only.** They are never queued for an
  offline session or an offline shared member, on the node that publishes them or on a
  node a peer forwards them to. Each carries a Message Expiry Interval (the statistics
  max(2 × `sys_interval_secs`, 10) s, the trace 10 s), so a copy that is queued anyway,
  by an older node during a rolling upgrade, expires. A persistent subscriber that
  reconnects gets the next tick, not a backlog.
- **The authorization dry run** (`GET /admin/v1/authz`) answers a publish to a reserved
  topic `allowed: false`, with the reason `reserved: $SYS/ is the broker's (ADR 0084)`,
  so it never gives a verdict the broker would not.

**The exception.** A Mosquitto bridge with `notifications` on, its default, connects with
a retained Will on `$SYS/broker/connection/<remote_clientid>/state` and then publishes `1`
there. Mosquitto allows clients exactly this pattern. Reserving it would refuse the
bridge's whole CONNECT, so mqttd leaves it to the ACL like any other topic, and a retained
value there is kept and replayed. It is outside mqttd's own `$SYS/brokers/`, so nothing
the broker publishes can be forged through it. Every other topic under `$SYS/` is
reserved, whatever the ACL says.

Subscribing to `$SYS/…` stays a matter for the ACL. A leading wildcard never matches a
`$`-topic (MQTT-4.7.2-1), so `#` does not include it, and a grant must name it.

### D2. An ACL deny also matches the filter inside `$share`

A subscribe deny applies to `$share/<group>/<filter>` when it overlaps the whole string
**or** `<filter>`. A deny on `$SYS/#` therefore refuses `$share/g/$SYS/#`, and a deny on
`a/#` refuses `$share/g/a/b`. A shared subscription still needs an allow that covers its
`$share/…` form, so nothing is loosened, and one allow is tightened: when `<filter>` is
`$`-rooted, the allow must be a `$share/<g>/<f>` pattern whose own `<f>` covers it, just
as `#` never covers `$SYS/x`. `$share/+/#` and `$share/#` therefore no longer grant
`$share/g/$SYS/…`; `$share/+/$SYS/brokers/+/rules/#` does. The authorization dry run
explains the decision the same way.

### D3. A rules file has a regex compile budget

Identical regex literals in one rules file are compiled once and shared. A file may hold
at most 96 distinct ones; the next is a load error at its position, before it is compiled.
The number is measured: a distinct pattern at the per-pattern limit costs about 6-7.5 ms
and 1.05 MiB to compile, so 96 parse in 0.6-1.1 s and 110-123 MiB on the broker's own
build, and the running rules plus one candidate stay under 256 MiB. Which patterns reach
the per-pattern limit depends on the build (the broker admits about `\w{20}`, mqtt-rules
alone `\w{50}`); the cost at the limit does not. The budget applies wherever a rules
file is parsed: boot, reload, `mqttd --check-rules` and the admin API. The per-pattern
limits stay.

*Amended 2026-10-10* (ADR 0083's amendment "regular expressions are PCRE2 in byte mode,
as in EMQX"): the engine is PCRE2, whose compiled patterns are bounded by PCRE2 itself and
cost at most about 0.9 ms and 190 KiB, so the budget is 512 distinct literals and 256 KiB
of them together (named groups compile in quadratic time). A literal that does not
compile is a load warning, no longer an error.

### D4. Opt-in per-rule statistics on `$SYS`

With `[rules] sys_interval_secs` (`MQTTD_RULES_SYS_INTERVAL`, 1 to 3600; 0, the default,
is off) each node publishes, every interval:

- `$SYS/brokers/<node>/rules`: a summary: the rule and enabled counts, the running set's
  digest, the trace settings, how many trace records and statistics ticks were dropped,
  and the last reload: when, its trigger, whether it applied, the error's **kind**
  (`config`, `rules`, `tls`, `admin tls`, `peer tls`, `client crl`, `gossip crl`,
  `gossip signer`, or `policy` for the ACL, the authenticators and anything else), never
  its text, and `repeats`: how many identical rejected attempts came right before it,
  such as the file watcher retrying a broken file (0 for a success);
- `$SYS/brokers/<node>/rules/<id>`, one per rule, enabled or not: its counts
  (`matched`, `passed`, `no_result`, `failed`, `actions_ok`, `actions_failed`), their rates
  over the last interval, the time spent evaluating it (`eval_ns`, added 2026-10-09 with
  `mqttd_rule_eval_seconds_total`) and its average per evaluation, since start and over
  the last interval (`eval_us_avg`), when the statistics last saw the rule run, a short keyed hash of
  the rule's definition, and the last error's time and kind.

The counts are the Prometheus counters, read without creating a series, so they are
cumulative since the broker started and keyed by rule id, as `/metrics` has them. The
last time a rule ran (`last_active_at`) is the tick at which its `matched` was seen to
grow against a baseline the statistics keep per id across reloads. It is `null` until the
statistics see the rule run: runs from before they were turned on, or while they were off,
are not seen, and while they are off it keeps the time they last saw. A rule deleted and
added again keeps that time (`null` if they never saw it run) until it runs again. An id
they never saw (deleted before the statistics were first on, or past the 4,096 ids they
keep) reads as active at the first tick after it is added back, if it had run before. The definition hash (`def`) is an HMAC-SHA256 under a key drawn at random
when the process starts, cut to 16 hex digits: it changes when the rule does, and after
a restart, and cannot be used to test guesses of the rule's text. Every message is JSON,
QoS 0, never retained and live-only (D1), published through `SysPublish`. Each one takes
node-pool ingress credit (ADR 0082), summary first; when the pool is short, the rest of
that tick is skipped and counted once in `stats_dropped`, so a tick may publish only its
summary. `$SYS` never carries a rule's SQL, description or actions (a rules file can hold
secrets, such as a pseudonym salt), a file path, the list of writers, or an error's
text, whether or not the trace is on. Times are RFC 3339 UTC with milliseconds.

The running digest is the file's unsalted SHA-256, as `/metrics` (`mqttd_rules_info`) and
`/statusz` publish it too. Whoever knows the rest of the file can test guesses of a secret
in it, so a secret in a rules file, such as a pseudonym salt, must be high-entropy random
(`openssl rand -hex 16`), never a name that can be guessed.

The interval and the trace settings are live: a reload that changes them applies at once,
and only a committed reload changes them, never a candidate the reload rejects. With
statistics or the trace on, `node.id` must be a single topic level (no `/`, `+`, `#` or
NUL), or the configuration is refused.

### D5. An opt-in rule trace, in a subtree of its own

With `[rules] trace = true` (`MQTTD_RULES_TRACE`; off by default) each node publishes, on
`$SYS/brokers/<node>/trace/rules/<id>`, what a rule did with a message: the trigger (a
publish's topic, QoS, retain flag, client id, username and payload; a Will; or a client
event), the result (`passed`, `no_result` or `failed`), the error, and what each action
rendered. The outputs are what the rule rendered; whether a derived message was then
delivered is in the counters.

- **Bounded.** At most `trace_rate` records per rule per second (`MQTTD_RULES_TRACE_RATE`,
  default 20, 1 to 1,000), with `no_result` records on a window of their own, twice that
  in all, and at most max(`trace_rate`, 200) per node per second. A record copies at most
  1 KiB of each payload (the original length is kept), at most 16 outputs, and at most 256
  bytes of a topic, client id or username. A `console` output is the selected fields as
  a JSON object, or past 1 KiB the first 1 KiB as text with `output_bytes` and
  `truncated: true`. Records wait in a queue of 1,024 records and 4 MiB; each publish
  takes node-pool credit. What does not fit is dropped and counted. Records still queued
  when the trace is turned off are discarded, uncounted.
- **Cheap when off.** One relaxed atomic load per evaluation.
- **Loud when on.** Turning it on logs a WARN naming the topic it copies payloads onto, and
  an `INSECURE:` line when there is no ACL file or the ACL's default is `allow`. The
  `INSECURE:` line comes again only when that reason changes while the trace is on, not
  at every reload.
- **Not under the statistics.** `$SYS/brokers/+/rules/#` does not cover
  `…/trace/rules/…`, so a statistics grant never grants the trace. A subscribe grant on a
  rule's trace topic is a read grant on every message that rule's `FROM` matches, with the
  publishers' client ids and usernames, whatever the subscriber's own ACL says about those
  topics. It is written on purpose or not at all.
- **Error text only here.** An evaluation error can quote a payload value, so its text is
  in the trace and nowhere else on `$SYS`: a SQL failure as the record's `error`, a failed
  action as its output's `error`. The statistics' `last_error` is a time and a kind,
  whether or not the trace is on.

### D6. Rules endpoints on the admin API

The admin API (ADR 0081) gains rules endpoints. Every one answers for the node it runs
on, names that node in its answer, and takes its parameters in the query string.

| Endpoint | Role | Does |
|---|---|---|
| `GET /admin/v1/rules` | viewer | The running rules with their counts, last activity (as the statistics last saw it) and last error; the running digest and the digest of the file on disk; the load warnings; the last reload. A viewer gets no SQL, actions or load warnings (`redacted: true`), the last error's time and kind only, and the last reload's error kind only. An operator gets all of it. |
| `GET /admin/v1/rules/source` | operator | The rules file's text as it is on disk, with its digest and the running one. |
| `POST /admin/v1/rules/check` | operator | Loads a whole file, or one rule spliced into the file on disk, as the broker would; writes nothing. |
| `POST /admin/v1/rules/test` | operator | Runs a simulated message through the running rules, a candidate file, or one rule (forced on, so a disabled or unsaved rule can be tried), and returns what each rule rendered. |
| `PUT /admin/v1/rules` | operator and writer | Replaces the whole file. `if_match` (the on-disk digest, or `*`) is required. |
| `PUT` / `DELETE /admin/v1/rule?id=` | operator and writer | Inserts, updates or removes one `[rules.<id>]` table, keeping the rest of the file, comments included. |

- **Writes need a second list.** A write needs the operator role **and** a subject listed
  in `[rules] admin_writers` (`MQTTD_RULES_ADMIN_WRITERS`). The list is empty by default,
  and empty means no writes. A listed writer has the rules file's trust, which is the ACL
  file's (ADR 0083): it can derive messages onto any topic from any accepted publish.
  Kick-and-cordon operators do not get that by holding an operator certificate.
- **The file stays the single source of truth.** Writes are serialized per node: each one
  reads the file on disk under one lock, compares `if_match` with that file's digest, and
  writes and reloads before the next starts, so two writes naming the same digest cannot
  both win. A write replaces the file atomically: a new file in the same directory with
  the old file's mode and, where the broker may set it, its group (when it cannot keep the
  group, the group permission bits are cleared rather than carried to another group),
  fsynced, the old file kept as `<file>.prev`, a rename (the target of a symlink, never the
  link), and the directory fsynced. A file that `if_match=*` creates has mode 0600. Then
  the ordinary validate-before-swap reload runs, with the trigger `admin-rules`, and the
  answer reports the digest that is running after it. The write survives a restart,
  because it is the file. A file that does not load is
  refused before anything is written; a reload rejected for another reason (a broken ACL
  file, say) leaves the new file written and says so.
- **Audited.** A write is a `rules.write` record (the subject, the operation, the rule,
  the old and new digests, whether the reload applied it), recorded after the rename.
- **Bounded.** Check, test and write parse inside `spawn_blocking`, one at a time per node.
  Their request bodies may be up to 1 MiB, decided from the caller's role before the body
  is read; every other route and caller keeps 64 KiB. A dry run touches no counter, no last
  error, no trace and no WARN slot of the running rules, and logs no console output.
- **Cluster.** Writes are node-local and never forwarded: apply them to each node.
  `/statusz` gains a `rules` block (digest, rule and enabled counts), and the cluster view
  gains each node's rules digest and a `same_rules` check, so drift shows.

### D7. CLI verbs for the rules endpoints

`mqttd --admin` gains `rules` (a table of the rules and their counts), `rules-source`
(the file's text, verbatim, so `> rules.toml` round-trips), `rules-apply <file>`
(`PUT /admin/v1/rules` with the local file's text) and `rule-delete <id>`. The per-rule
`PUT` and `test` take structured JSON bodies and have no verb: this amends ADR 0081 §3,
whose verbs followed the endpoints one to one. The CLI prints each control character in
the text the server sends (C0, DEL, the C1 set, U+2028 and U+2029) as its escape: `\u{1b}`
for ESC, `\r`, `\u{9b}` in the terminal views, `\u001b` and `\u009b` in `--json`, which
stays valid JSON. So a last error that quotes a payload cannot drive the operator's
terminal. `rules-source` prints the file verbatim.

### D8. Still no web UI in the broker

The broker serves no page. The demo's editor (`demo/rules-live`) is a separate container
and an admin API client like any other: a small Python server that holds an operator and
writer certificate, proxies the rules endpoints, and streams the `$SYS` statistics and
trace to the browser. Whoever reaches it is an operator who may rewrite the rules, with no
login, so it listens on the user's loopback only and is a demo, never a deployment. It
refuses a request whose `Host` is not its loopback origin, requires its own origin, a JSON
content type and a custom header on every change, renders every string with
`textContent`, and sends a strict Content-Security-Policy.

## Consequences

- **Rules can be watched where MQTT users look.** A subscriber to
  `$SYS/brokers/+/rules/#` sees every rule's counts and rates on every node it can reach,
  and the trace shows what a rule made of a message, without the log. The statistics and
  trace publishes are ordinary QoS 0 publishes to the hub: they count in
  `mqttd_publish_received_total{qos="0"}` and the delivery-latency histogram, and with
  1,024 rules at a 1 s interval they are 1,025 messages a second. They are live-only, so
  a persistent subscriber that is offline costs nothing: no queued copy, no durable
  append, no backlog on reconnect. Nothing is published unless an operator turns it on.
- **Compatibility (ADR 0058).** Three client-visible behaviours change, and the release
  notes of the first release with them say so. A client publish or Will to `$SYS/…` that
  an ACL allowed is now refused. MQTT 5 §4.7.2 reserves `$`-topics for the server ("the
  Server SHOULD prevent Clients from using such Topic Names to exchange messages with
  other Clients"), so this is the specified behaviour, not a new one. The one exception
  keeps Mosquitto bridges working: with `notifications` on, their default, they write
  only `$SYS/broker/connection/<id>/state`, which stays the ACL's to decide. A bridge
  whose `notification_topic` points elsewhere under `$SYS/` is refused at CONNECT, because
  its Will is there, and must move it out of `$SYS` or set `notifications false`
  (MIGRATION.md). A shared subscription that an allow on `$share/…` admitted past a deny
  on its inner filter is now refused: the deny always meant it. A shared subscription to a
  `$`-rooted filter (`$share/g/$SYS/…`, or any `$share/g/$x/…`) that a broad `$share/+/#`
  or `$share/#` allow admitted is now refused: the grant must name the `$` level inside
  its `$share` pattern. The rules file's regex budget changes no released behaviour,
  because no release has the rule engine yet. The new `[rules]` keys are additive; an
  older binary refuses them unless `config_unknown_keys = "warn"`, as for any new key.
- **The rolling-upgrade window.** The reservation is complete only once every node runs a
  version with it. Until then an older node accepts client publishes to `$SYS/…` and
  forwards them. An upgraded node drops a forwarded `$SYS` message that would be retained,
  that is QoS 1 or 2, or that names it (`$SYS/brokers/<its id>/…`), since no other node
  speaks for it; a forged message naming an older node, sent live at QoS 0, still reaches
  subscribers on upgraded nodes during the roll. Retained `$SYS/…` messages a client
  stored before the upgrade are cleared during the first minute after boot (the count is
  logged), never replayed meanwhile, and skipped by a restore, because no client can
  clear them any more. An older node may queue the statistics and trace it is forwarded
  for its offline subscribers; their expiry bounds how long such a copy lives.
- **Writes are per node.** A rules write changes the node that answered. A cluster needs
  the same write on each node, and `same_rules` in the cluster view shows when they
  differ. Writes and file-managed rules do not mix: a ConfigMap or a GitOps pipeline that
  owns the file overwrites an API write on its next sync, and a ConfigMap volume is
  read-only (a write answers `409 rules-file-unwritable`). Keep `admin_writers` empty
  there. A writable rules file should sit in a directory of its own, because the broker
  must be able to replace files in it.
- **Writing the rules file over the network does not bring back ADR 0081 §5's drift.** That
  objection was to a second source of configuration: a value set through the API, lost on
  restart or overwritten by the file. A rules write is a write of the file, applied by the
  same reload a `SIGHUP` runs. What remains true of §5: no other configuration is written,
  and nothing is written unless `admin_writers` names someone.
- **The demo UI is an unauthenticated operator.** On the user's own loopback, with the
  Host, Origin, content-type and custom-header checks, it is safe enough for a demo; it is
  not a pattern for production, where the admin API and the CLI are the surface.
- **New disclosure classes on the client listener**, recorded in THREAT-MODEL.md:
  statistics (rule ids, counts, keyed definition hashes, the file digest, error kinds)
  and, with the trace on, payloads, client ids and usernames, under the ACL. A deployment
  with no ACL file, or with `default = "allow"`, gives both to every client.
- **The dry run is an oracle for operators only.** A rule that pseudonymizes with a salt
  in its SQL computes the pseudonym of any input `test` is given; that is why `source`,
  `check` and `test` need the operator role, and why viewers see no SQL.

## Alternatives considered

- **Statistics from the admin API only, no `$SYS`.** This is ADR 0081's answer, and the
  admin API does serve the same data (D6). On its own it does not meet the ask: an MQTT
  client cannot subscribe to it, and a client that polls it needs an admin certificate.
  ADR 0081's objection to `$SYS` was operator data under the client authorization model;
  here the data on `$SYS` is limited to counts, digests and error kinds, it is off by
  default, the ACL is fixed so that a deny holds (D2), and the payload-bearing trace has
  its own opt-in and its own subtree.
- **Statistics for admin identities only** (a role list for `$SYS` subscribers, or `$SYS`
  served on the admin listener). Rejected: a second authorization model for one subtree.
  The ACL already authorizes the client listener and can express a statistics-only grant
  exactly; HARDENING.md says which.
- **A web UI in the broker.** Rejected for ADR 0081 §5's reasons: login sessions,
  CSRF and XSS exposure on the admin plane, and a frontend to maintain. The demo builds
  one outside the broker on the JSON API, which is what §5 said anyone could do.
- **Writing configuration generally** (`PUT /admin/v1/config`). Rejected, as in ADR 0081
  §5: most settings cannot change on a running node, and a value set through the API would
  fight the file and the operator's renderer. The rules file is the one piece of
  configuration people iterate on against live traffic, and it can be written as a whole
  file through the ordinary reload.
- **A cluster-wide rules store**, replicated through the durable plane or gossip, so one
  write reaches every node. Rejected: a second source of truth beside the file, which would
  fight a ConfigMap; consensus for a setting the rest of the configuration does without;
  and rules are per-node configuration (ADR 0083 §6). Node-local writes plus a visible
  `same_rules` keep the file model.
- **Reserving all of `$SYS/`, a Mosquitto bridge's state included.** Rejected: it refuses
  the whole CONNECT of every Mosquitto bridge with notifications on, its default. Accepting
  the CONNECT and dropping only the Will would lose the bridge's state for no gain: the
  topic is outside `$SYS/brokers/`, and Mosquitto allows it too.
- **Retained statistics**, so a late subscriber gets the last value at once. Rejected: a
  durable write, and with durable retained messages a quorum commit, on every tick; they
  count against the retained quota, are exported in backups, and outlive the node that
  wrote them. Queueing them for an offline persistent subscriber is rejected for the same
  reason: a durable append per tick per subscriber, then a stale backlog on reconnect.
- **A trace switch per rule in the rules file.** Rejected: it changes the rules file
  format away from EMQX's `rule_engine.rules.<id>` shape, and a rules file is not where a
  disclosure decision belongs. The trace is a node setting with a per-rule rate limit.
- **Letting `$SYS` messages run rules** (EMQX's `ignore_sys_message = false`). Rejected: a
  rule on `FROM "$SYS/#"` would change the statistics that trigger it every tick. The
  broker's own publishes never run rules, as for every message the broker originates.
