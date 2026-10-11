# Hardening baseline

**Verified against `v1.1.0` (2026-10-07); ADR 0084's items added 2026-10-08, ADR 0086's (H-7.12 to H-7.15) 2026-10-11, ADR 0085's (H-7.16 to H-7.19) 2026-10-12.** The checkable companion to the
[threat model](THREAT-MODEL.md) (ADR 0066 T2): numbered items, each stating the
control, the knob that enforces it, the shipped default, and a **verification an
auditor can run**. The [SECURED-CLUSTER-TUTORIAL](SECURED-CLUSTER-TUTORIAL.md)
teaches the path; this document is the list you check a deployment against —
shaped so a formal benchmark (CIS/STIG style) can be derived from it mechanically.

**Levels.** **L1** items are the essentials: a deployment failing one is insecure
in a way an attacker can use. **L2** items are defense-in-depth: they raise the
cost of an attacker who has already breached something.

**The self-reporting rule.** mqttd announces every insecure posture with an
`INSECURE:` log line at startup. That makes the cheapest broad check item **H-0**:

> **H-0 (L1).** The startup log contains **no `INSECURE:` lines**.
> *Verify:* `grep INSECURE: <broker log>` returns nothing. Every hit names the
> item below that fixes it.

Config knobs are given in env form; each has a TOML equivalent (see
[`mqttd.example.toml`](mqttd.example.toml)). `mqttd --check-config` validates a
configuration before any port binds and is the pre-rollout gate for all of it.

---

## 1. Transport

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-1.1 | L1 | No plaintext MQTT listener (`MQTTD_PLAINTEXT_BIND` unset) | off by default; enabling logs `INSECURE` | `grep "PLAINTEXT MQTT listener" <log>` returns nothing; a raw-TCP CONNECT to the TLS port fails |
| H-1.2 | L1 | TLS listener with cert+key | `MQTTD_TLS_BIND` + `MQTTD_TLS_CERT`/`MQTTD_TLS_KEY` | `openssl s_client -connect host:8883 </dev/null` completes a TLS 1.3 handshake |
| H-1.3 | L1 | TLS 1.3 only — 1.2 stays off | `MQTTD_TLS_ALLOW_TLS12` off by default (on = loudly logged) | `openssl s_client -tls1_2 -connect host:8883 </dev/null` **fails** |
| H-1.4 | L1 | No WS/WSS/QUIC listeners beyond those intended; plaintext WS off | `MQTTD_WS_BIND` unset; `MQTTD_WSS_BIND`/`MQTTD_QUIC_BIND` only where meant | `grep "PLAINTEXT WebSocket" <log>` returns nothing; port scan matches the intended listener set |
| H-1.5 | L2 | mTLS for client auth where the fleet supports it | `MQTTD_TLS_CLIENT_CA`; certificates need the `clientAuth` EKU | a client without a cert is refused at handshake; one with a wrong-EKU cert is refused (see [TROUBLESHOOTING](TROUBLESHOOTING.md#a-client-with-a-certificate-is-rejected-mtls)) |
| H-1.6 | L2 | Client-cert revocation live | `MQTTD_TLS_CRL` (requires `client_ca`; refused without it) | connect with a revoked cert → refused; SIGHUP after CRL update applies without restart |
| H-1.7 | L2 | TLS 1.2 unsafe-features hatch closed | `MQTTD_TLS_ALLOW_UNSAFE_TLS12_FEATURES` unset | absent from the effective config (`--check-config` output / env) |

## 2. Authentication

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-2.1 | L1 | Anonymous access off | `MQTTD_ALLOW_ANONYMOUS` off by default; on = `INSECURE` log | a CONNECT with no credentials gets CONNACK `0x86`/rc 4-5, not success |
| H-2.2 | L1 | Password file is Argon2id via the shipped tool | `MQTTD_PASSWORD_FILE`; lines minted by `mqttd --hash-password` | file contains only `$argon2id$` PHC lines: `grep -vc '\$argon2id\$' <file>` = 0 |
| H-2.3 | L1 | Password file unreadable to others | operator-managed | `stat -c %a <file>` (Linux) / `stat -f %Lp` (macOS) ≤ 640, owner = broker user |
| H-2.4 | L2 | OIDC: HTTPS issuer only | `MQTTD_OIDC_ISSUER` https; `MQTTD_OIDC_ALLOW_HTTP` unset (on = `INSECURE`) | issuer URL scheme is https; no `INSECURE: OIDC` log line |
| H-2.5 | L2 | Auth timeout bounds pre-auth sockets | `MQTTD_AUTH_TIMEOUT` (default on) | a socket that connects and sends nothing is closed within the timeout |

## 3. Authorization

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-3.1 | L1 | An ACL file is configured — without one, every authenticated client may publish/subscribe anywhere | `MQTTD_ACL_FILE`; absent = `INSECURE` log | `grep "no MQTTD_ACL_FILE" <log>` returns nothing |
| H-3.2 | L1 | The ACL defaults to deny | `default = "deny"` in the ACL file | `grep 'default *= *"deny"' <acl file>`; a client publishing outside its grants sees v5 `0x87` on PUBACK (v3.1.1: the message silently drops — **check the audit log**, `acl.deny.publish`) |
| H-3.3 | L2 | Identity-scoped topics use `%i` (principal), not `%c` (client-chosen id), for tenant boundaries | ACL patterns | review: `grep '%c' <acl file>` hits are each justified or paired with H-3.4 |
| H-3.4 | L2 | `connect` rules constrain which client ids an identity may claim | `[[connect]]` rules in the ACL file (opt-in; absent = any id) | a client connecting with another identity's id is refused CONNACK `0x87` |
| H-3.5 | L2 | `$SYS` is granted on purpose, and only the rule statistics: monitoring identities get `subscribe` on `$SYS/brokers/+/rules/#` (counts and error kinds, no error text or payload); nobody gets `$SYS/#`, which includes the rule trace (message payloads); a shared grant on the statistics names them (`$share/+/$SYS/brokers/+/rules/#`): a broad `$share/+/#` does not reach `$SYS` | ACL rules; `#` never covers a `$`-topic, so only `default = "allow"` (H-3.2), an explicit `$SYS` pattern, or a `$share` pattern whose inner filter names `$SYS` grants it | `mqttd --admin authz <monitor> subscribe '$SYS/brokers/+/trace/rules/+'` and `… subscribe '$share/g/$SYS/brokers/+/trace/rules/+'` both answer `allowed: false`; `grep -nE '"\$SYS/#"\|"\$share/[^"]*/\$SYS/#"' <acl file>` finds only deny rules |

## 4. Cluster planes

Single-node deployments: verify none of the cluster binds are set and skip to §5.

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-4.1 | L1 | Peer bus over mTLS with the **dedicated cluster CA** — never the client CA | `MQTTD_PEER_TLS_CA/CERT/KEY` | `grep "PLAINTEXT peer listener" <log>` returns nothing; the cluster CA signs no client certs |
| H-4.2 | L1 | Peer cert CN equals the node id (both link directions enforce it) | certificate minting discipline | `openssl x509 -in <peer cert> -noout -subject` CN matches `MQTTD_NODE_ID` |
| H-4.3 | L1 | SWIM gossip keyed | `MQTTD_SWIM_KEY`/`MQTTD_SWIM_KEY_FILE`; unkeyed = `INSECURE` log | `grep "SWIM gossip is UNAUTHENTICATED" <log>` returns nothing |
| H-4.4 | L2 | Signed gossip posture (per-node certs) | `MQTTD_SWIM_SIGNED` | `/statusz` shows the signed posture; a V1 datagram from a test sender is dropped (counted `auth`) |
| H-4.5 | L2 | Anti-replay posture (signed **and** sequenced) | `MQTTD_SWIM_REPLAY` | `/statusz` posture; replayed datagram dropped (counted `replay`) |
| H-4.6 | L2 | Peer-bus CRL configured and hot | `MQTTD_PEER_TLS_CRL` | revoke a node's cert, SIGHUP: its links drop without a broker restart |
| H-4.7 | L2 | Kubernetes: per-pod peer keys (cert-manager csi-driver), not the starter one-Secret-for-all | deployment choice | `kubectl get secret` — no single secret holding every node's key |
| H-4.8 | L2 | Refound guard on (split-brain containment) | `MQTTD_REFOUND_GUARD` (default on) | absent from config (default) or explicitly `true`; never `false` in production |
| H-4.9 | L2 | Kubernetes: the cluster ports (7001, 7946, 9443) reachable only pod to pod, under an enforcing CNI | chart `networkPolicy.enabled` / CR `spec.networkPolicy` (default off) | `kubectl get networkpolicy`; a probe pod outside the release cannot connect to a pod's 7001 (the kind smoke does exactly this) |

## 5. Resource governance

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-5.1 | L1 | Connection caps set to deployment capacity | `MQTTD_MAX_CONNECTIONS`, `MQTTD_MAX_CONNECTIONS_PER_IP` | over-cap connect is closed **at accept** (no CONNACK) |
| H-5.2 | L1 | Disk high-water mark set below the volume size | `MQTTD_STORE_MAX_BYTES` | fill toward the mark in staging: publishers see v5 `0x97` refusals while subscribers keep draining (brownout, not crash) |
| H-5.3 | L2 | Memory watermark set (Linux) — and the container limit remains the hard bound | `MQTTD_MEMORY_MAX_BYTES` **plus** a cgroup/container memory limit | both present; remember the mark is a brownout trigger, **not a ceiling** (see threat model) |
| H-5.4 | L2 | Per-subscriber bounds tuned to device profile | `MQTTD_MAX_QUEUED_MESSAGES`, `MQTTD_MAX_INFLIGHT_MESSAGES`, backlog byte knobs | `/metrics` exposes `publish_dropped{reason="backlog-overflow"}` after a deliberate overrun in staging |

## 6. Durability and disk

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-6.1 | L1 | Durable sessions have a real data dir | `MQTTD_DATA_DIR`; durable-on without one **refuses to start** unless `MQTTD_ALLOW_EPHEMERAL_DURABILITY` opts in | the opt-in is unset; boot succeeds with a data dir |
| H-6.2 | L1 | Data dir owned by the broker user, not world-readable | operator-managed | `stat` the dir: mode ≤ 750, owner = broker user |
| H-6.3 | L1 | Write floor on for clusters | `MQTTD_MIN_REPLICAS` (default: majority) | `/statusz` reports the floor; a minority partition **refuses** durable publishes |
| H-6.4 | L2 | Backups land outside the data dir, on a volume with restricted access | `MQTTD_BACKUP_DIR` (inside the data dir is refused at validate) | exports are mode 0600: `stat` a fresh export; **treat backup content as plaintext data-plane data** (threat model §4) |
| H-6.5 | L2 | Restore only per runbook: fresh dir, complete set | `MQTTD_RESTORE_PARTIAL_ACCEPT_DATA_LOSS` stays unset | the flag is absent; restores into a used dir are refused |

## 7. Configuration and operations

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-7.1 | L1 | Config validated before rollout | `mqttd --check-config --config <file>` in the deploy pipeline; `--check-config --preflight` as the service account on the target host | exit 0; CI/CD step exists |
| H-7.2 | L1 | Unknown config keys refuse (typo net) | `MQTTD_CONFIG_UNKNOWN_KEYS` unset (= refuse); `warn` only during a rollback/skew window | env/flag absent outside upgrade windows |
| H-7.3 | L1 | Secrets by path, never inline | config discipline (schema enforces for keys) | `grep -iE 'key *= *"-----|password *= *"[^/]' <config>` returns nothing |
| H-7.4 | L2 | Health/metrics ports on the ops network only, never internet-exposed | `MQTTD_HEALTH_BIND`, `MQTTD_METRICS_BIND` on internal interfaces | external scan: 200-serving `/statusz` unreachable from outside |
| H-7.5 | L2 | systemd deployments use the shipped hardened unit | `deploy/systemd/mqttd.service` | `systemctl show mqttd -p ProtectSystem,NoNewPrivileges,User,MemoryDenyWriteExecute` → `strict`, `yes`, `mqttd`, `yes` (`no` only with H-7.15 recorded) |
| H-7.7 | L1 | The admin API, when on, is reachable only from operator networks and its CA issues admin certificates only | `MQTTD_ADMIN_BIND` on an internal interface; a dedicated `MQTTD_ADMIN_CLIENT_CA` (not the client-listener CA, and not the cluster CA — with the cluster CA every unlisted node certificate is admitted as `peer` instead of refused) | external scan: the admin port is unreachable from outside; `openssl x509 -in <admin CA> -noout -subject` names an admin-only CA |
| H-7.8 | L2 | Admin roles are least-privilege | `MQTTD_ADMIN_OPERATORS` lists only the people/automation that act; everyone else in `MQTTD_ADMIN_VIEWERS` | `mqttd --admin whoami` for each issued certificate shows the intended role; operators list reviewed with access reviews |
| H-7.9 | L1 | The rule trace is off: it copies message payloads, client ids and usernames onto `$SYS` | `MQTTD_RULES_TRACE` unset (default off); on, it logs a WARN, and `INSECURE:` with no ACL file or `default = "allow"` | `mqttd --print-config` (or `mqttd --admin config --json`) shows `rules.trace = false` |
| H-7.10 | L1 | Rules writes over the admin API are off, or limited to the people who may edit the ACL file: a rules writer can derive messages onto any topic | `MQTTD_RULES_ADMIN_WRITERS` empty (the default: no writes); a listed subject must also be in `admin.operators` | `mqttd --admin config --json \| jq .config.rules.admin_writers` is `[]`, or each entry is reviewed with the ACL file's editors |
| H-7.11 | L2 | A rules file the admin API may write sits in a directory of its own, owned by the broker user, never beside the config, ACL, password or key files (making a directory writable lets a compromised broker rewrite whatever is in it) | `rules.file` when `MQTTD_RULES_ADMIN_WRITERS` is set; the broker WARNs at boot when that directory is not writable | `ls -la "$(dirname <rules.file>)"` lists only the rules file and its `.prev`, owned by the broker user, file mode ≤ 640 (a file the API creates is 0600), directory mode ≤ 750 |
| H-7.12 | L2 | The WebAssembly sandbox is not in use unless a rule needs it: the rule engine's `jq` function is off and the rules file declares no module. With neither, the broker creates nothing of the runtime (no compiler run, no thread, no fault handler, no executable page) and is exactly as hardened as without the feature | `MQTTD_RULES_JQ` unset (default off); no `[functions.<name>]` table in the rules file (default: none) | `mqttd --print-config` shows `jq = false` under `[rules]`; `grep -c '^\[functions\.' <rules file>` prints `0`; a rules file that calls `jq` is refused by `mqttd --check-rules` with `jq is switched off` |
| H-7.13 | L2 | An installation that must not contain a compiler at all runs a build without the runtime. Release binaries and images contain it (unused unless H-7.12 is deviated from), so this is a build of your own from the release tag, outside the release's signature and reproducibility proof | `cargo build --release -p mqttd --no-default-features`: the `jq` cargo feature, the only default one, is the runtime | `MQTTD_RULES_JQ=1 mqttd --check-config` prints ``warning: rules.jq is on, but this mqttd was built without jq (the `jq` cargo feature is off): a rules file that calls jq will not load``; the binary is 7 MB or more smaller (7.4 MB for a static `aarch64-unknown-linux-musl` build) |
| H-7.14 | L2 | Where the sandbox is on, what a publisher can make it spend is bounded on purpose: the ceiling on one call (`rules.function_max_ms`) lowered from its ten seconds to what the slowest program or function needs, and the timeouts under it with it, most of all where rules run `jq` or modules on payloads from publishers you do not trust; no secret among the variables shown to programs and modules; module files not writable by the broker; a memory limit on the process, because the number of concurrent long calls is not capped, only the time each may take ([threat model](THREAT-MODEL.md#rule-functions-in-the-webassembly-sandbox-adr-0086)) | `MQTTD_RULES_FUNCTION_MAX_MS` (default 10000; a few hundred milliseconds is generous for a program over one message), `MQTTD_RULES_JQ_DEFAULT_TIMEOUT_MS` (default 10000, at most the ceiling) and each module's `timeout_ms`, `memory_mib`; `MQTTD_RULES_JQ_ENV`, `MQTTD_RULES_FUNCTIONS_ENV` (default empty); file ownership; `MemoryMax` or a container limit | `mqttd --check-config` prints `rules.function_max_ms is <N>: no jq or module call runs longer, whatever a rule or a payload asks for` with the N you chose; `mqttd --print-config` shows the ceiling, the timeout and the two lists, each entry reviewed; no rule takes a `jq` program from a payload (`grep -n 'jq(' <rules file>` shows literal first arguments; a third argument taken from a payload is held to the ceiling); `ls -l <rules dir>/**/*.wasm` shows an owner other than the broker's user and no write bit for it; `systemctl show mqttd -p MemoryMax` (or the pod's `resources.limits.memory`) is set |
| H-7.15 | L2 | systemd: where the sandbox is on, the one restriction it cannot live with is lifted in a drop-in, alone, and recorded as a deviation from H-7.5; the shipped unit file is not edited | `MemoryDenyWriteExecute=false` under `[Service]` in `/etc/systemd/system/mqttd.service.d/override.conf` (`systemctl edit mqttd`); the unit ships `true` | `systemctl cat mqttd` shows the drop-in with that one line; `systemctl show mqttd -p MemoryDenyWriteExecute,ProtectSystem,NoNewPrivileges,SystemCallFilter` shows `no` and the unit's other values unchanged; run under the service's own settings, `MQTTD_RULES_JQ=1 mqttd --check-config` prints `rules.jq is on: this host allows compiled code` |
| H-7.16 | L2 | Rule state is off where no rule needs it. A rules file that declares no `[state.<name>]` and calls no state function already holds no store; setting the ceiling to `0` makes that a property of the node, which no rules file and no rules writer can undo: a file that uses state is then refused at load | `MQTTD_RULES_STATE_MAX_BYTES=0` (default 268435456 = 256 MiB; read at startup) | `mqttd --print-config` shows `state_max_bytes = 0` under `[rules]`; `mqttd --admin state` says rule state is switched off; a rules file that calls `kv_incr` is refused by `mqttd --check-rules` with ``rule state is switched off: `rules.state_max_bytes` is 0`` |
| H-7.17 | L2 | Where rule state is on, what publishers can make the broker hold is bounded on purpose: the ceiling lowered from its 256 MiB to what the rules need and counted in the process's memory limit; every namespace's `max_keys` and `max_bytes` sized for the device population it keys by, not left at 100,000 and 64 MiB by default; TTLs as short as the rule's meaning allows; `eviction = "fail"` (the default) wherever a forgotten key would be a wrong decision, `"lru"` only for caches ([threat model](THREAT-MODEL.md#state-for-rules-adr-0085), [sizing](SIZING.md#rule-state)) | `MQTTD_RULES_STATE_MAX_BYTES`; `max_keys`, `max_bytes`, `default_ttl`, `max_ttl`, `eviction`, `dedupe_window` in each `[state.<name>]`, `[state.default]` and `[state.kv_store]` of the rules file; `MemoryMax` or a container limit | `mqttd --admin state` lists every namespace with its bounds and what it holds, each reviewed; no namespace sits at its `max_keys`; `mqttd_rule_state_ops_total{result!="ok"}` and `mqttd_rule_state_replication_refused_total` are alerted on (H-8.6) |
| H-7.18 | L2 | Rules that keep state key by values the publisher cannot choose freely: `clientid` where `connect` rules or mTLS pin it (H-3.4), or a topic level the ACL pins, not a bare payload field; an idempotency key has the publisher's identity in front; a value read from state is guarded like a payload field before it becomes a topic level | the rules file; the ACL file | review: every `kv_*` key and every fourth argument of `kv_incr` in `mqttd --admin rules-source` starts from `clientid` or `topic(n)`, or the rule's `FROM` is a topic only trusted publishers may write; `mqttd --rule-test --sequence` with a hostile sequence shows what one publisher can do to another's keys |
| H-7.19 | L2 | Reading and deleting rule state is least-privilege: keys and values are payload data. Viewers see counts only; only operators list keys and read values; only an operator who is also a listed rules writer deletes one. `$SYS/brokers/+/rule_state/#` is granted on purpose, apart from the rule statistics | `MQTTD_ADMIN_VIEWERS`, `MQTTD_ADMIN_OPERATORS`, `MQTTD_RULES_ADMIN_WRITERS` (H-7.8, H-7.10); the ACL file | `mqttd --admin state-keys <ns>` with a viewer's certificate is refused (`403`); the ACL grants `$SYS/brokers/+/rule_state/#` to no client that should not see how many keys each namespace holds |
| H-7.6 | L2 | Containers run the shipped image (distroless, nonroot) pinned to a release tag | compose/chart defaults | image ref is `ghcr.io/mbilling/fss-mqtt-broker:<X.Y.Z>` — exact version, never `latest`; cosign verification per [RELEASING](../RELEASING.md) |

### When to leave rule functions in the sandbox out (H-7.12 to H-7.15)

The rule engine's `jq` function and the modules a rules file may declare
([RULES.md](RULES.md#jq)) run as WebAssembly compiled to machine code inside the broker
([ADR 0086](adr/0086-wasm-rule-functions.md)). They are off by default, and that is the
hardened position. Leave them off unless a rule needs what only they can do; most
`jq` calls in rules ported from EMQX have a plain rule SQL form that needs none of this
([RULES-COOKBOOK.md](RULES-COOKBOOK.md#you-do-not-need-jq-for-this)).

There are three positions, from most to least restrictive:

| Position | How | What it means |
|---|---|---|
| The runtime is not in the binary | Build with `--no-default-features` (H-7.13) | No compiler in the build, a binary 7 MB or more smaller, and no way to switch it on by configuration. `rules.jq = true` and a `[functions.<name>]` table are each refused with the reason. You run a build of your own, not the signed release |
| In the binary, not in use (the default) | Do not set `rules.jq`; declare no modules (H-7.12) | Nothing of the runtime is created in the process. The broker is exactly as it is without the feature, and the shipped systemd unit keeps `MemoryDenyWriteExecute=true` |
| In use | `rules.jq = true`, or a `[functions.<name>]` table (H-7.14, H-7.15) | What follows |

What is given up when it is in use:

- **Under systemd, `MemoryDenyWriteExecute=true`.** Compiled code needs memory the process
  may map executable, and the two exclude each other: there is no slower mode that keeps
  both. The operator sets it to `false` in a drop-in; with it still `true`, a rules file
  that calls `jq` or declares a module fails to load and says so. The unit's other
  restrictions stay. The container image and the Helm chart's security context impose no
  such restriction, so nothing changes there (a seccomp profile or SELinux policy of your
  own that denies executable memory has to allow it for the broker).
- **A compiler in the trust base.** The broker's memory safety then also rests on the
  WebAssembly runtime and on the machine code its compiler generates, on input a
  publisher influences. Its security advisories become yours to follow
  ([threat model, accepted risks](THREAT-MODEL.md#accepted-risks-client-surface)).
- **A process without fault handlers of its own.** The runtime installs process-wide
  handlers for `SIGSEGV`, `SIGBUS`, `SIGILL` and `SIGFPE` once a module is loaded.
- **The microsecond bound on what one message costs.** A sandboxed call may run for as long as
  its timeout allows and hold up to its memory limit, and concurrent long calls are not
  capped. Rule SQL's own functions are bounded in microseconds and kilobytes.
- **Memory and start-up**: 7 to 42 MiB once, depending on the allocator, a megabyte or
  more resident per warm instance (one per worker thread), and about half a second
  when rules that call `jq` are first loaded.

What is **not** given up: a module, jq's or yours, reaches no file, no socket, no
environment variable that was not listed for it, and nothing of the broker's memory; a
fault in it fails the rule it was called from. Those hold as long as the runtime is sound.

### How to bound rule state, or switch it off (H-7.16 to H-7.19)

Rules can keep values between messages ([RULES.md](RULES.md#state-what-a-rule-remembers),
[ADR 0085](adr/0085-rule-state-store.md)). Unlike `jq`, this needs no switch: a rules
file that uses state gets a store, under a ceiling. The keys are built from what
publishers send, so the store is memory the broker holds on their behalf, and the
hardened position is to decide how much.

| Position | How | What you get |
|---|---|---|
| Off for the node | `MQTTD_RULES_STATE_MAX_BYTES=0` (H-7.16) | No store, no sweeping task, no state frame sent or kept. A rules file that declares a namespace or calls `kv_*`, `kv_store_*` or `proc_dict_*` does not load, at boot, at a reload or through the admin API, and the error names the key. Rules that keep no state run as before |
| Not in use (the default with rules that keep none) | A rules file with no `[state.<name>]` and no state call | The same absence, but a later rules file, or a rules writer, can start using state, up to the ceiling |
| In use, bounded | A ceiling you chose, and bounds per namespace (H-7.17 to H-7.19) | What follows |

- **The ceiling is the broker's, not the rules file's.** `rules.state_max_bytes` is read
  at startup from the configuration; a rules file can lower a namespace's bounds under
  it and never raise it, and the admin API cannot change it. It covers every namespace
  and every connection's `proc_dict_*` dictionary. Count it in the process's memory
  limit: on every node, for an `eventual` namespace.
- **Bound each namespace for what it keys by.** A namespace keyed by client id needs
  about as many keys as there are devices, plus, with idempotency keys, the messages of
  one `dedupe_window`, plus, in an `eventual` namespace, the deletes of one
  `tombstone_window`. A bound far above that is room a hostile publisher can fill; a
  bound below it fails rules in normal operation. The arithmetic is in
  [SIZING.md](SIZING.md#rule-state).
- **`eviction`.** `"fail"` refuses a new key in a full namespace and fails the rule,
  which is visible (`namespace_full`) and loses no existing state. `"lru"` keeps the
  rule working and silently forgets the least recently used key: right for a cache,
  wrong for a quota or a `kv_held` run, where a forgotten key is a wrong decision.
- **Short TTLs are a bound too.** Every key expires; a key that lives for 30 days is
  room held for 30 days after one message.
- **Who should be a rules writer.** A subject in `[rules] admin_writers` can make a node
  hold up to the ceiling, can store and read back fields of any message the rules
  select, and can drop any namespace's keys by editing the file. That is the reach of
  whoever edits the ACL file, and the list should be the same people (H-7.10). An
  operator who only needs to *look* at state needs the operator role and not this list.
- **Not available:** state is not encrypted in memory, is not written to disk, and has
  no per-publisher quota inside a namespace.

## 8. Audit and monitoring

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-8.1 | L1 | The audit chain reaches the SIEM | `MQTTD_AUDIT_SYSLOG` (RFC 5424/TCP, see [AUDIT-SCHEMA](AUDIT-SCHEMA.md)) or a log shipper carrying the `target: audit` lines | the SIEM shows `audit.genesis` at each boot; `scripts/audit-verify.py` over a captured stream exits 0 |
| H-8.2 | L1 | The chain-boundary invariant is alerted on | SIEM rule | rule exists: a chain ending **without** `audit.shutdown`, or a genesis **not** preceded by one, raises an alert (crash or suppression) |
| H-8.3 | L2 | Metrics scraped; refusal/drop counters dashboarded | `MQTTD_METRICS_BIND` or `MQTTD_OTLP_ENDPOINT` | dashboards show `publish_dropped_*`, gossip drop counters, brownout state |
| H-8.5 | L2 | Admin actions are alerted on | SIEM rule over `admin.request` records, and over `rules.write` records | rule exists: any `role=operator` record with a non-`GET` target, and any `rules.write` record, raises a notification |
| H-8.6 | L2 | Where rules keep state, refusals by the store are alerted on: a full namespace or store fails rules, and refused replication means the nodes' copies differ | Alert rules over `mqttd_rule_state_ops_total{result!="ok"}`, `mqttd_rule_state_replication_refused_total` and `mqttd_rule_state_keys` against each namespace's `max_keys`; a SIEM rule over `state.delete` records | rules exist; a test namespace filled to its bound raises the alert |
| H-8.4 | L2 | `/readyz` drives load-balancer membership | orchestrator wiring | draining/browned-out/quorumless nodes leave rotation automatically |

---

## Deviation record

A deployment that cannot meet an L1 item should record the deviation the way this
repository records accepted risks: the item id, the reason, the compensating
control, and a review date. An unrecorded deviation is the finding.

Corrections and additions follow the threat-model rule: versioned, dated, and
verified against a release — a baseline that drifts from the knobs is worse than
none.
