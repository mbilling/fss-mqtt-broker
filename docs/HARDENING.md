# Hardening baseline

**Verified against `v1.0.10` (2026-08-27).** The checkable companion to the
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
| H-7.1 | L1 | Config validated before rollout | `mqttd --check-config --config <file>` in the deploy pipeline | exit 0; CI/CD step exists |
| H-7.2 | L1 | Unknown config keys refuse (typo net) | `MQTTD_CONFIG_UNKNOWN_KEYS` unset (= refuse); `warn` only during a rollback/skew window | env/flag absent outside upgrade windows |
| H-7.3 | L1 | Secrets by path, never inline | config discipline (schema enforces for keys) | `grep -iE 'key *= *"-----|password *= *"[^/]' <config>` returns nothing |
| H-7.4 | L2 | Health/metrics ports on the ops network only, never internet-exposed | `MQTTD_HEALTH_BIND`, `MQTTD_METRICS_BIND` on internal interfaces | external scan: 200-serving `/statusz` unreachable from outside |
| H-7.5 | L2 | systemd deployments use the shipped hardened unit | `deploy/systemd/mqttd.service` | `systemctl show mqttd -p ProtectSystem,NoNewPrivileges,User` → `strict`, `yes`, `mqttd` |
| H-7.6 | L2 | Containers run the shipped image (distroless, nonroot) pinned to a release tag | compose/chart defaults | image ref is `ghcr.io/mbilling/fss-mqtt-broker:<X.Y.Z>` — exact version, never `latest`; cosign verification per [RELEASING](../RELEASING.md) |

## 8. Audit and monitoring

| # | Lvl | Control | Knob / default | Verify |
|---|-----|---------|----------------|--------|
| H-8.1 | L1 | The audit chain reaches the SIEM | `MQTTD_AUDIT_SYSLOG` (RFC 5424/TCP, see [AUDIT-SCHEMA](AUDIT-SCHEMA.md)) or a log shipper carrying the `target: audit` lines | the SIEM shows `audit.genesis` at each boot; `scripts/audit-verify.py` over a captured stream exits 0 |
| H-8.2 | L1 | The chain-boundary invariant is alerted on | SIEM rule | rule exists: a chain ending **without** `audit.shutdown`, or a genesis **not** preceded by one, raises an alert (crash or suppression) |
| H-8.3 | L2 | Metrics scraped; refusal/drop counters dashboarded | `MQTTD_METRICS_BIND` or `MQTTD_OTLP_ENDPOINT` | dashboards show `publish_dropped_*`, gossip drop counters, brownout state |
| H-8.4 | L2 | `/readyz` drives load-balancer membership | orchestrator wiring | draining/browned-out/quorumless nodes leave rotation automatically |

---

## Deviation record

A deployment that cannot meet an L1 item should record the deviation the way this
repository records accepted risks: the item id, the reason, the compensating
control, and a review date. An unrecorded deviation is the finding.

Corrections and additions follow the threat-model rule: versioned, dated, and
verified against a release — a baseline that drifts from the knobs is worse than
none.
