# Threat model

**Verified against `v1.1.0` (2026-10-07); ADR 0084's surfaces added 2026-10-08.** This is the one-document answer to "what
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
| A rule republishing where its publisher cannot | Not mitigated by the ACL, by design: the ACL decides whether the *original* is accepted, and what a rule derives from it is operator configuration with the ACL file's trust (see the control-plane accepted risks). A derived message carries no publisher identity and never re-enters the rule engine, so a client cannot steer one rule's output into another. A topic template filled from the payload, the client id or the username lets the publisher choose those topic levels (`x/../admin`, a `$`-prefixed level); a rendered topic with a wildcard, NUL, `$share/` or `$SYS` fails the action, and docs/RULES.md documents the `WHERE … regex_match(…)` guard that confines the rest — the rule author's to apply | ADR 0083 §3; `mqttd/src/rules.rs`, `mqtt-rules/src/action.rs`; [RULES.md](RULES.md#security-values-the-publisher-chooses) |

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
| A rules file built to be expensive to load (thousands of worst-case regular expressions) | Identical regex literals are compiled once and a file holds at most 96 distinct ones, so a worst-case load costs about a second and about 120 MiB; past it the load fails before the next pattern is compiled. It applies to boot, reload, `--check-rules` and the admin API alike; each pattern keeps its own 1 MiB limits | ADR 0084 D3; `mqtt-rules/src/parser.rs` |
| Rule work driven by payloads (deep JSON, hostile regex patterns, `FOREACH` fan-out, payload fields passed as function sizes) | Rules evaluate on the publisher's own connection task, never the hub loop; payload JSON is decoded once, in time linear in its keys, with serde_json's recursion limit; regular expressions use a linear-time engine with compiled-size limits and a pattern from the payload is compiled once per message; a `FOREACH` iterates at most 10,000 elements and produces at most 256 outputs, and a publish at most 1,024 derived messages carrying together at most 4 MiB beyond four times its payload, all charged to the publisher's ingress credit (what a client/session event or a Will derives is held to the same per-event bounds but charged to no credit — an accepted risk below); a message's functions may build at most 1 MiB beyond their inputs, together (pad lengths, replacements and separators repeated per match or item), `map_put`/`mput` paths have at most 64 segments, timestamps must be renderable in every offset (chrono panics past its range), decimals are Erlang's 0..=253; checked arithmetic; expressions deeper than 256 levels, or nested deeper than 64, are refused at load; an evaluation error fails the rule, never the message or the connection; a publish waiting for its derived messages' credit gives back its own first and waits parked, so waiting connections hold no credit between them; a connection's parked acks are bounded in hub gates. Both untrusted inputs (the rules file, payloads under fixed rules, with payload-supplied sizes) are nightly fuzz targets | ADR 0083 §8; `mqtt-rules`, `mqtt-rules/fuzz` |

### Accepted risks (client surface)

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
| Rules text disclosing secrets (pseudonym salts in SQL) | The rules file's text, the SQL and actions, error text and `test` (which computes a salted pseudonym for any input: an oracle) are operator-only; a viewer's `GET /admin/v1/rules` is redacted to ids, descriptions, FROM filters and events, counts, definition hashes and error kinds: no SQL, actions, load warnings or error text; `$SYS` never carries them. A definition hash is keyed per process, so it is no oracle. The whole file's digest is an unsalted SHA-256 on `/metrics`, `/statusz`, `$SYS` and the admin API, so a secret in the rules file must be high-entropy random (`openssl rand -hex 16`), never a guessable name | ADR 0084 D4/D6; `mqttd/src/admin/rules.rs`, `rules.rs` |
| Payload text reaching an operator's terminal (a last error quoting a payload with terminal escapes) | `mqttd --admin` escapes control characters in the text the server sends before printing it | ADR 0084 D7; `mqttd/src/admin/cli.rs` |
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
| A rules file as an injection path | The rules file is operator configuration on disk, like the ACL file: loaded all-or-nothing, a file that does not load refuses the boot and rejects a reload with the running rules kept; no network verb writes it unless `[rules] admin_writers` names the caller (the rules-write rows above); actions are confined to the broker (`republish`, `console` — a sink action is refused at load), and `getenv` is not provided, so a rule cannot read the broker's environment; `mqttd_rules_info{checksum}`, `/statusz` and the cluster view's `same_rules` expose per-node drift | ADR 0083 §5/§6/§8, ADR 0084; `mqttd/src/reload.rs`, `mqtt-rules` |

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
  every topic through rules and their trace. (ADR 0084.)
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

Corrections to this document follow the same rule as COMPARISON.md: versioned,
dated, and cited — a threat model that drifts from the code is worse than none.
