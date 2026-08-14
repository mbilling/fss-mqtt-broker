# How mqttd compares — Mosquitto · EMQX · NanoMQ · VerneMQ

**Dated 2026-08-13.** Versions compared: **mqttd** `v0.9.0` (released — signed,
reproducible, SBOM-attested) · **Mosquitto** 2.0.22 / 2.1.2 (cells note where the lines
differ) · **EMQX** 6.2.2 (documentation cells; the benchmark ran 5.8.6, the last
Apache-licensed line — its results are **not yet published in-tree**, and issue #244
tracks the publishable multi-host run) · **NanoMQ** 0.25.5 ·
**VerneMQ** 2.1.1.

This document obeys [ADR 0048](adr/0048-comparative-benchmarking.md)'s honesty rules,
applied to prose: versions pinned, claims dated, **losing cells printed as prominently
as winning ones**, and deliberate absences distinguished from not-yet. Legend:

- ✅ supported / solid · ⚠️ partial (note says how) · ✖ absent
- **by design** — a recorded decision not to build it (the ADR is cited)
- **n/v** — not verified in this pass. We would rather print n/v than guess. Cells are
  re-verified at each cross-broker benchmark re-run (ADR 0048 §5).

mqttd cells were verified against this repository's source; competitor cells against
their documentation, release notes, changelogs, and source, researched 2026-07-29 →
2026-08-03. Corrections are welcome and will be applied with a dated changelog line.

## The short version

**Choose mqttd when** the broker is part of your security perimeter (deny-by-default
everything, mTLS/OIDC identity, policy reload that evicts live sessions on revocation,
tamper-evident audit, signed reproducible artifacts) and when durable sessions must
survive node loss without an ops runbook — quorum replication and data-safe resize are
the defaults, not add-ons.

**Choose Mosquitto when** you want the smallest, most battle-tested single-node broker
in existence and clustering is out of scope. **Choose EMQX when** you want the largest
feature surface (dashboard, rule engine, gateways) and accept BSL licensing with
commercially-licensed clustering. **Choose NanoMQ when** the broker runs at the edge
next to the sensor and kilobytes matter. **Choose VerneMQ when** you want
Erlang-ecosystem clustering and can accept node-local queue durability and EULA'd
production binaries.

**Do not choose mqttd (yet) if** you need a built-in dashboard, a rule engine, MQTT-SN
or CoAP gateways, an HTTP management API — or a broker with a production track record:
mqttd has a released version (`v0.9.0`) but **no production users**. What it offers
against that last, honestly disqualifying-for-some fact is verifiability: reproducible
builds, signed artifacts, SBOM, continuous fuzzing, and conformance CI against foreign
clients — trust that can be checked instead of assumed.

## Protocol: MQTT 3.1.1 + 5.0

All five serve MQTT 3.1.1 and 5.0. The differences are in the v5 details:

| Feature | mqttd | Mosquitto 2.0→2.1 | EMQX 6.2 | NanoMQ 0.25 | VerneMQ 2.1 |
|---|---|---|---|---|---|
| Session expiry | ✅ | ✅ | ✅ | ⚠️ offline redelivery bug open (nanomq#1934) | ⚠️ subscriptions survive node loss; queued messages don't (documented) |
| Message expiry | ✅ | ✅ | ✅ | ✖ broker-side undocumented | ✅ |
| Topic aliases | ✅ both directions | ⚠️ 2.0 inbound-only → ✅ 2.1 both | ✅ | ✅ | ✅ both |
| Flow control (Receive Maximum) | ✅ enforced (`0x93`) | ⚠️ 2.0 outbound-only → ✅ 2.1 enforced | ✅ | ✖ inflight window marked "unsupported now" in shipped config | ✅ both directions |
| Shared subscriptions | ✅ incl. **cluster-wide** (ADR 0010/0015) | ✅ (single node) | ✅ cluster-wide | ⚠️ `$share`+wildcard protocol error (nanomq#1883) | ⚠️ cluster-wide; recurring cluster bugs 2018→2025 (vernemq#1570, #2405) |
| Subscription identifiers | **✖ not delivered — and the wire says so**: CONNACK advertises `Subscription Identifiers Available = 0`, a SUBSCRIBE carrying one is refused with DISCONNECT `0xA1` (§3.2.2.3.12), so a client fails fast instead of silently losing its demux. Delivery tracked. | ✅ | ✅ | ✅ | ✅ |
| User properties | ✅ end-to-end | ✅ | ✅ | ⚠️ property size cap (default 32) | ✅ |
| Enhanced auth (AUTH) | ⚠️ **built-in** challenge/response; server-initiated re-auth deferred (0013-T8) | ⚠️ plugin events only, no built-in | ✅ | ✖ documented unsupported | ⚠️ plugin-provided only, no built-in |
| Will delay | ✅ | ✅ | ✅ | n/v (documented for bridges only) | ⚠️ lost across netsplit "window of uncertainty" (documented) |
| Request/response forwarding | ✅ Response Topic + Correlation Data | ✅ | ✅ | ✅ | ✅ |
| Assigned client id (empty-id clients) | ✅ for clean sessions (MQTT 5 `AssignedClientIdentifier` returned); **✖ refused for persistent ones** — a generated id has no session to resume (`crates/mqttd/src/conn.rs`) | ✅ | ✅ | n/v | ✅ |
| Maximum packet size | ✅ advertised + enforced both ways | ✅ | ✅ | ✅ configurable | ⚠️ inbound yes; outbound enforcement unverified in source |

Neither mqttd nor VerneMQ sends CONNACK Response Information or Server Keep Alive;
other brokers n/v — none of these is commonly load-bearing.

**Transports:** TCP + TLS + WebSocket (ws/wss) on all five (NanoMQ TLS requires the
`-slim`/`-full` build; Mosquitto 2.1 made WebSockets built-in). **QUIC**: mqttd ✅
listener (multi-stream; non-standard, EMQX-style) · EMQX ✅ listener (GA, disabled by
default) · NanoMQ ⚠️ bridge-client only · Mosquitto ✖ · VerneMQ ✖.

## Clustering & durability

The load-bearing differences. mqttd's own weak spot is printed first: **durable-session
serving capacity scales with the lease voter set (`MQTTD_LEASE_VOTERS`, default 5), not
the node count** (ADR 0021/0049) — connections, fan-out, and shared-subscription
throughput scale with nodes; durable ownership does not. Tested envelope to date: 3–5
node clusters under kill/partition/upgrade/soak harnesses.

| | mqttd | Mosquitto | EMQX 6.2 | NanoMQ | VerneMQ 2.1 |
|---|---|---|---|---|---|
| Clustering model | masterless mesh (authenticated SWIM + HRW placement); Raft lease group for config only | ✖ none (bridges only) | Mria: core nodes (full mesh) + replicants; **production clustering requires a commercial license** (BSL) | ✖ single node (bridge to cloud) | masterless; fully-replicated EC metadata (SWC) |
| Session queues replicated across nodes | ✅ **default** (quorum R=3, epoch-fenced) | n/a (single-node disk persistence) | ⚠️ opt-in "durable sessions" feature, non-default (per EMQX docs) | ⚠️ single-node SQLite cache | ✖ node-local only; **documented: lost on node death** |
| Acked QoS 1/2 survives node loss | ✅ proven under SIGKILL/partition harnesses (acked-facts oracle) — a group holding fewer copies than a majority of the members the node knows about — **capped at the replication factor**, so 2 copies on a 3-node cluster and still 2 on 5 or 7 — **REFUSES** new durable promises by default (QoS≥1 acks withheld so sources redeliver; retained mutations queue) rather than acking on a shrunken quorum; a node that has never known peers still serves fully, a publish for a durable session whose owner is gone is still acked-and-dropped by the no-known-subscriber path, and a correlated restart of a quorum still loses. See [README Limitations](../README.md#limitations) | n/a | n/v | ✖ | ✖ documented |
| Elastic resize without data loss | ✅ grow/shrink/replace (ADR 0043; decommission drain verified) | n/a | n/v | n/a | ⚠️ graceful leave migrates queues; node death loses them |
| Partition behavior | CP, documented: minority retained writes queue-until-heal; sessions keep serving (ADR 0037) | n/a | n/v | n/a | default **fails closed cluster-wide** on detected netsplit; opt-in AP flags; documented "window of uncertainty" (lost wills, possible duplicate client ids) |
| Cross-node delivery under backpressure | TCP backpressure — nothing dropped | n/a | n/v | n/a | bounded buffer (default 10 KB), **drop + counter** |
| Session takeover safety | ✅ single-owner + identity-bound sessions (ADR 0031) | n/a | ✅ | n/a | ⚠️ leader-coordinated registration; duplicate ids possible during splits (documented) |

## Security

| | mqttd | Mosquitto | EMQX 6.2 | NanoMQ | VerneMQ 2.1 |
|---|---|---|---|---|---|
| TLS stack | rustls on aws-lc-rs, **TLS 1.3 by default**; hardened 1.2 opt-in (ECDHE+AEAD allowlist, EMS required, loudly logged) | OpenSSL | OTP ssl | mbedTLS (1.3 n/v) | OTP ssl |
| mTLS client certs | ✅ identity from CN/SAN, no-fallback (ADR 0004) | ✅ | ✅ | ✅ | ✅ |
| Built-in authentication | mTLS + Argon2id passwords + JWT + **OIDC with live JWKS rotation** | password file + dynamic-security plugin | extensive built-ins (DB, JWT, HTTP, LDAP, …) | password file + HTTP auth | files/DB via plugins; no built-in OIDC |
| Authorization | deny-by-default TOML ACLs, `%i`/`%c`, connect ACL; a denied MQTT 5 publish is answered `0x87 Not authorized` | `acl_file` + dynsec | built-in authz sources | HOCON ACL | `vmq_acl` file / DB plugins |
| Policy hot reload | ✅ validate-before-swap, **and the reload sweeps live state** — revoked cert/user/grant evicts running sessions and flows (ADR 0040) | ⚠️ SIGHUP reloads; live eviction not documented | n/v | ⚠️ HTTP `/reload`, subset | ⚠️ live reconfig via CLI; live eviction not documented |
| Audit trail | ✅ hash-chained, tamper-evident | not documented | n/v | not documented | not documented |
| Memory safety | Rust, `#![forbid(unsafe_code)]` | C | Erlang/BEAM | C | Erlang/BEAM |
| Release integrity | reproducible builds, keyless cosign signatures, SLSA provenance, SBOM — shipped with `v0.9.0` (15 signed assets) | — | n/v | — | — |

Every mqttd insecure mode (plaintext, anonymous, unenforced ACL) is opt-in and loudly
logged; the same is not uniformly true elsewhere (e.g. NanoMQ defaults to
`allow_anonymous = true`, as does Mosquitto below 2.0).

## Operations, observability, integration

| | mqttd | Mosquitto | EMQX 6.2 | NanoMQ | VerneMQ 2.1 |
|---|---|---|---|---|---|
| Metrics | Prometheus + OTLP push + k8s probes | `$SYS` topics | dashboard + Prometheus + `$SYS` | HTTP API + Prometheus endpoint + limited `$SYS` events | Prometheus + `vmq-admin` |
| Dashboard / UI | ✖ **by design** — signal-driven ops, read-only health listener; a provisioned Grafana demo ships instead (ADR 0020 posture) | ✖ | ✅ | ✖ | ✖ (CLI + HTTP mgmt API) |
| HTTP management API | ✖ **by design** (same decision) | ⚠️ dynsec over MQTT topics | ✅ | ✅ | ✅ |
| Rule engine | ✖ not planned — boundary bridge + standard integrations instead | ✖ | ✅ SQL | ✅ SQL (full build) | ✖ |
| Bridging | ✅ standalone bridge, deny-by-default directional rules, hop-count loop prevention, spool (ADR 0025) | ✅ built-in (the reference implementation) | ✅ data-integration bridges | ✅ TCP/QUIC/AWS bridges | ✅ basic `vmq_bridge` |
| MQTT-SN / CoAP gateways | ✖ | ✖ (separate projects) | ✅ (SN, CoAP, LwM2M, …) | ✖ (DDS/SOME-IP/ZMQ instead) | ✖ |
| Kubernetes | Helm chart: StatefulSet, per-pod PV, decommission-draining scale-down, automatic cert/policy rotation via file-watch, PVC lifecycle on shrink (ADR 0047). A Kubernetes **operator** (`MqttdCluster` CRD, split-brain detection and fencing — ADR 0055) is built and end-to-end tested, but **not yet packaged for installation**: the chart is the supported path today | — | Operator + Helm | container | k8s discovery in image |
| Config | TOML + env, strict schema, `--check-config`, whole-config hot reload | conf file, SIGHUP | HOCON + dashboard/API | HOCON + env, hot reload | conf file + env mapping, live reconfig |

## Operational limits & resource governance

What an operator can *bound*. mqttd's quota layer is ADR 0041
(refuse-at-the-edge: reason codes and backpressure, never silent drops); every knob
below ships **unset = unbounded** except where a default is printed — the sizing
consequences and a bounded-node recipe live in [SIZING.md](SIZING.md).
Packet-size enforcement is compared in the Protocol table above.

| | mqttd | Mosquitto | EMQX 6.2 | NanoMQ | VerneMQ 2.1 |
|---|---|---|---|---|---|
| Max connections | ✅ global + per-IP (`MQTTD_MAX_CONNECTIONS`, `_PER_IP`), refused at accept before TLS work | ✅ `max_connections` | ✅ per-listener | n/v | ✅ `listener.max_connections` (default 10 000) |
| Queued/offline messages per session | ✅ count, default 100 000, overflow `drop-oldest` or `reject-newest` (`MQTTD_MAX_QUEUED_MESSAGES`, `MQTTD_QUEUE_OVERFLOW`) — both overflow policies **ack-and-drop** at the cap: the default `drop-oldest` truncates the oldest *already-acked* entries out of the durable queue, `reject-newest` acks and sheds the newest (counted `publish_dropped{reason="queue-overflow"}`); ✖ **no byte-based cap** — accepted, tracked (ADR 0041 amendment T6) | ✅ count (`max_queued_messages`, default 1000) **and bytes** (`max_queued_bytes`) | ✅ `max_mqueue_len` (default 1000); byte variant n/v | ⚠️ `msq_len`; byte variant n/v | ✅ `max_online_messages` / `max_offline_messages` (1000/1000) |
| Subscriptions per client | ✅ `MQTTD_MAX_SUBSCRIPTIONS_PER_CLIENT` (per-slot `0x97`) | n/v | ✅ | n/v | n/v |
| Publish rate limiting | ✅ token bucket + TCP backpressure — pause, not drop (`MQTTD_MAX_PUBLISH_RATE`) | ✖ | ✅ rate limiters | n/v | n/v |
| Retained-store bound | ✅ topic count (`MQTTD_MAX_RETAINED_MESSAGES`); overwrite/clear always allowed | ✖ | ✅ retainer limits | n/v | n/v |
| Sessions cap | ✅ `MQTTD_MAX_SESSIONS` (new refused; resume never refused) | ✖ | ✅ | n/v | n/v |
| Disk bound / full-disk behavior | ⚠️ one aggregate high-water mark (`MQTTD_STORE_MAX_BYTES`) → **brownout**: growth writes refused — subscriber acks/reads/expiry continue, while a `QoS` ≥ 1 publisher is **refused** (v5 `0x97`, v3.1.1 no ack + close) rather than acked for a message the store will not take — cross-node too, as a peer-bus verdict; mid-rolling-upgrade an older link degrades to a withheld ack + close (issue #238); disk-full itself fails closed, crash-tested mid-write (ADR 0044 P2). ✖ no per-store quota (ADR 0041 amendment T9) | ⚠️ persistence file + autosave, no quota | n/v | n/v | n/v (node-local LevelDB) |
| Total-memory limit | ⚠️ **watermark, not a ceiling**: `MQTTD_MEMORY_MAX_BYTES` → brownout (growth refused; subscriber acks/reads/expiry/resumes continue, while a `QoS` ≥ 1 publisher is refused — v5 `0x97`, v3.1.1 no ack + close, cross-node as a peer-bus verdict (older links mid-upgrade: withheld ack + close) — ADR 0041 T8/T11/T12). Cannot stop RSS rising — the container limit is still the hard bound, and it needs `/proc` (Linux) | ✅ `memory_limit` (hard heap cap) | ⚠️ per-connection `force_shutdown` (heap default 32 MiB + mailbox 1000) — kills the connection, not a broker-wide cap | n/v | n/v |
| Per-connection write buffering | ⚠️ `MAX_BACKLOG`: **10 000 messages** per stalled subscriber, **hard-coded and not configurable** — bounded in count, not in bytes. With the 1 MiB default packet size that is ~10 GiB of worst-case headroom per connection; cap `MQTTD_MAX_PACKET_SIZE` to bound it. Accepted, tracked (ADR 0041 amendment T10) | ⚠️ bounded by `max_queued_bytes` | ⚠️ per-connection `force_shutdown` heap cap | n/v | n/v |
| Auth-failure penalty | ✅ per-source threshold + decay, bounded table (`MQTTD_AUTH_PENALTY_*`; default off) | ✖ | ✅ flapping detect / banning | n/v | n/v |

## Licensing & distribution

| | mqttd | Mosquitto | EMQX | NanoMQ | VerneMQ |
|---|---|---|---|---|---|
| License | **Apache-2.0, everything** | EPL-2.0 / EDL-1.0 | **BSL 1.1** since 5.9 (2025): single node free, production clustering commercial, each release converts to Apache-2.0 after 4 years; last Apache line (5.8) EOL 2026-02-28 | MIT | Apache-2.0 **source**; official binaries/images under EULA — paid for commercial production since 1.10 (2019) |
| Binaries | signed (keyless cosign), reproducible, SBOM-attested, multi-arch, free — shipped with `v0.9.0` | free | free image; features licensed | free | EULA (free to test) |
| Paid tier | support/SLA only — no gated features (project principle) | none (foundation project) | license-gated production features | none | binary packages + support |

## The two names newcomers ask about: HiveMQ and AWS IoT Core

Neither is in the benchmark set (ADR 0048 compares self-hostable, like-for-like brokers),
but they are the names most evaluations start from, so their shape belongs here. Claims
below are limited to each vendor's own published positioning; no benchmark numbers.

- **HiveMQ** is an enterprise MQTT platform: full MQTT 5, mature tooling, and clustering —
  in the **commercial** edition. The Apache-2.0 HiveMQ Community Edition is single-node
  (clustering, and most of the operational platform, are enterprise features). If the
  comparison is "open-source clustered broker", HiveMQ CE is not in that category; the
  commercial edition competes on maturity and support, which this project does not claim
  to match.
- **AWS IoT Core** is a managed MQTT-compatible service, not a broker you run: no cluster
  to operate, per-message pricing, deep AWS integration — and a **protocol subset**. Most
  notably it does not support QoS 2 (publishes at QoS 2 are rejected), and its retained
  messages, session lifetimes and payload sizes are governed by service quotas rather than
  configuration. If exactly-once delivery, self-hosting, data locality, or broker-level
  control matter, it is a different product category; if "no broker to operate" is the
  requirement, nothing self-hosted competes with it.

## Footprint & maturity — where we lose

- **Footprint:** NanoMQ (sub-MB binary claims, ~4.6 MB image) and Mosquitto (a few-MB
  single-threaded C daemon) beat everyone; mqttd's distroless image is ~14 MB; EMQX and
  VerneMQ carry a BEAM runtime. If kilobytes decide, mqttd is not the answer.
- **Maturity:** Mosquitto has been ubiquitous since ~2010; EMQX runs enormous fleets;
  VerneMQ has a decade of production; NanoMQ is LF Edge-backed. **mqttd has a
  release (`v0.9.0`) but no operational track record and no production users.** The mitigations are verifiability (reproduce the
  build, check the signature, read the SBOM), two foreign-client conformance oracles in
  CI, and a continuous-assurance program (fuzzing, soak, fault/upgrade harnesses) — but
  a mitigations list is not a track record, and pretending otherwise would break this
  document's own rules.
- **Performance:** no cross-broker numbers are printed here, deliberately. The
  [benchmark harness](../bench/) runs all five brokers under disclosed postures;
  numbers appear in [docs/benchmarks/](benchmarks/) only from dedicated, documented
  hardware (ADR 0048's dev-grade/publishable line). Our own micro-baselines are in
  [BASELINE.md](benchmarks/BASELINE.md).

## Sources & staleness

Competitor facts: official docs, changelogs, release notes, Docker Hub metadata, and
source (researched 2026-07-29 → 2026-08-03; VerneMQ cluster behavior additionally from
its documented netsplit semantics and public issue tracker). mqttd facts: this
repository at `v0.9.0`. Cells marked n/v were not verified and are treated as unknown,
not as absent. This file is re-checked at every cross-broker benchmark re-run
(per release, ADR 0048 §5); staleness beyond one release cycle is a defect.

## Changelog

- 2026-08-14 — Drift sweep from the 2026-08-13 panel (issue #253): the header's
  benchmark citation pointed at a results file under bench/results/ that is not
  tracked in this repository (that directory is gitignored) — the one document
  claiming verifiability had a dangling citation;
  it now says plainly that the results are unpublished and points at issue #244. The
  2026-08-11 changelog bullet below asserted "v0.9.0 shipped 2026-07-22", which is the
  **RC** date — the release itself shipped 2026-08-07 (verified against the GitHub
  release list); corrected in place. `scripts/check-readme-facts.py` now asserts this
  file's citations resolve to tracked files and that the README's stated date matches
  this header, so both classes fail CI instead of waiting for the next panel.
- 2026-08-13 — The subscription-identifiers cell is rewritten (issue #245): it read
  "not delivered (codec-only; tracked)", which was true of the *feature* but omitted that
  the wire actively claimed the opposite — MQTT 5.0 §3.2.2.3.12 makes an absent CONNACK
  `0x29` mean "supported". mqttd now advertises `0x29 = 0`, refuses an identifier-bearing
  SUBSCRIBE with DISCONNECT `0xA1`, and refuses a client PUBLISH carrying one with `0x82`
  ([MQTT-3.3.4-6]). Delivery itself remains unimplemented and is tracked by #266.
- 2026-08-11 — Corrections from the review-panel re-run, which found this file stale in
  seven checkable places (all five reviewers hit at least one): release status
  ("unreleased"/"first tag pending" — v0.9.0 shipped 2026-08-07; its RC on
  2026-07-22), the date header, the
  assigned-client-id cell (the code ASSIGNS for clean sessions and refuses only
  persistent ones — the cell under-claimed against ourselves), the TLS cell (1.2
  hardened opt-in shipped), the overflow modes (`reject-newest`, not `disconnect`), and
  the EMQX version framing (docs cells 6.2.2, benchmark 5.8.6).
- 2026-08-11 — Added the HiveMQ / AWS IoT Core section (the two names newcomers ask
  about were absent). Claims limited to vendor-published positioning; both are outside
  the ADR 0048 benchmark set (not self-hostable like-for-like).
- 2026-08-04 — Kubernetes row: chart hardening (file-watch rotation, PVC lifecycle)
  and the recorded no-operator decision with reopen triggers (ADR 0047 amendment).
- 2026-08-04 — Added "Operational limits & resource governance" (the ADR 0041 surface
  was implemented but absent here). Competitor limit cells verified against mosquitto
  man pages, EMQX 5.x/latest MQTT config docs, and VerneMQ options/listeners docs this
  date; unverified cells print n/v. mqttd's missing byte-based queue cap and
  total-memory limit are printed as losses with their tracking tasks.
