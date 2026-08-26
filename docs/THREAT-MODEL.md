# Threat model

**Verified against `v1.0.9` (2026-08-26).** This is the one-document answer to "what
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

### Tampering / Elevation (authorization)

| Threat | Mitigation | Where |
|---|---|---|
| Unauthorized publish/subscribe | Deny-by-default at every layer: `DenyAll` until policy configured; file ACL defaults `deny`, deny-wins; SUBSCRIBE denied per-filter (`0x80`/`0x87`) before the hub, PUBLISH dropped before the hub, will topic refused at CONNECT | ADR 0004; `mqtt-auth/src/acl.rs`, `mqttd/src/conn.rs` |
| Placeholder abuse (`%i`/`%c`) | A pattern whose placeholder is empty or contains topic metacharacters is unusable — allow grants nothing, deny refuses outright | ADR 0004 T12; `mqtt-auth/src/acl.rs:33` |
| Revoked-but-connected clients | Policy reload sweeps **live** sessions: identity revocation terminates, permission tightening removes grants | ADR 0040; `mqttd/src/reload.rs` |
| TLS downgrade / weak crypto | TLS 1.3 only by default (1.2 is per-listener opt-in); one audited build site, no skip-verification path, one crypto provider passed explicitly | ADR 0002, 0053; `mqtt-net/src/tls.rs` |
| QUIC 0-RTT replay | `max_early_data_size = 0` — 0-RTT disabled | ADR 0036; `mqtt-net/src/quic.rs:52` |

### Repudiation

Auth successes and failures, every ACL denial, and admin actions flow into the
hash-chained audit log (SHA-256, boot-scoped genesis, head emitted on every record —
ADR 0004, 0066 T3; `mqtt-observability/src/lib.rs`). Failures are keyed by client
id, never a credential.

### Denial of service

| Threat | Mitigation | Where |
|---|---|---|
| Connection floods | Global + per-source-IP caps enforced **at accept, before the TLS handshake** (RAII permits) | ADR 0041 T1; `mqttd/src/admission.rs` |
| Password-hash burn (Argon2 cost as a weapon) | Auth-failure penalty box keyed by source **address only** (never username — no victim-aimed lever), closing at accept before any hash work; hard-bounded table | ADR 0041 T2; `mqttd/src/admission.rs` |
| Oversized packets | Inbound ceiling advertised as MQTT 5 Maximum Packet Size and enforced; outbound honors the client's advertised maximum | ADR 0041 T4; `mqtt-net/src/frame.rs` |
| Slow/stalled subscribers | Per-subscriber bounds on backlog (messages **and** bytes — accounting includes topic+properties, or it would be evadable ~100×), in-flight window, outbound socket bytes | ADR 0041 T10; `mqttd/src/backpressure.rs` |
| Publish floods | Read-pause (TCP backpressure), not drops or kills; in-flight overrun is a protocol error (`0x93`) | ADR 0012, 0041; `mqttd/src/conn.rs` |
| Disk/memory exhaustion | Watermarks → **brownout**: growth writes refused effect-free while acks/reads/expiry continue; two independent axes ORed; refusal travels cross-node as a peer-bus verdict | ADR 0041 T5/T8/T12; `mqttd/src/store_watch.rs`, `hub/policy.rs` |

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

There is **no HTTP admin API, no dashboard, no rule engine** — deliberate absences,
each a rejected authenticated network surface (ADR 0033/0051). The admin surface is
signals and files: SIGHUP reload, SIGUSR1 decommission, SIGUSR2 backup, SIGTERM
drain. The HTTP surface is strictly read-only GET/HEAD (`/livez`, `/readyz`,
`/statusz`, `/metrics`), hand-rolled, carrying no secret material, on an
ops-network trust model (ADR 0020, 0054).

| Threat | Mitigation | Where |
|---|---|---|
| Bad config brick / fail-open reload | Atomic validate-before-swap: new values built first, published only on success; malformed file leaves running policy unchanged; every reload audited; `--check-config` validates before any port binds | ADR 0032, 0046; `mqttd/src/reload.rs` |
| Secret leakage via config | Secrets referenced by path only, never inlined; unknown config keys refuse (listing all) unless the rollback-window hatch is set | ADR 0046 T5, 0058 T4 |
| Operator (Kubernetes) overreach | Every destructive remediation opt-in per scenario, defaults Alert; **no action deletes data, ever** (fenced PVCs are labelled, not deleted); ambiguous evidence → no action; at most one destructive act per reconcile | ADR 0055; `mqttd-operator/src/remediate.rs` |
| Repudiation of admin acts | Reloads, sweeps, backups audited into the same hash-chained log | ADR 0004/0032 |

### Accepted risks (control plane)

- **The operator is trusted.** Signals-and-files means anyone with process/file
  access is the operator; there is no in-broker RBAC for admin verbs. Host and
  orchestrator access control is the boundary.
- **Metrics/health are unauthenticated by design** on the ops network; they carry
  no secrets, but topology and load are visible to anyone who can reach the port.
  (ADR 0020 §2.)

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
