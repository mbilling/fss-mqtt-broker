---
adr: "0039"
title: Release versioning and upgrade policy (semver, adjacent skew, sequential majors)
adr_status: Accepted
tasks:
  - id: 0039-T1
    title: Policy documented — ADR + README upgrade section (semver semantics, adjacent skew, sequential majors, 3 supported lines, 1.0 activation)
    status: done
    date: 2026-07-05
    evidence: "ADR 0039 states the full policy with the model survey (Kubernetes skew + 3-line maintenance, etcd/Cassandra adjacent stepping, Elasticsearch gateway-minor major rolls, Kafka negotiation, Postgres offline majors) and the reasoning for the composite; README gains an 'Upgrades & versioning' section stating, operator-facing: semver defined by wire/disk breakage, adjacent-only skew, sequential majors rolled through a designated gateway minor (the previous major's last minor, where known upgrade issues are fixed first) and dispatched on schema stamps, three supported lines, the MQTT-client carve-out, and the explicit pre-1.0 (ADR 0038 freeze) regime until activation."
  - id: 0039-T2
    title: Enforcement notes wired to the mechanisms — proto_min policy rule and schema-gate migration rule recorded beside PROTO_MIN/PROTO_MAX and the schema module docs
    status: done
    date: 2026-07-05
    evidence: "PROTO_MIN's doc now states the release rule (frozen for a major's lifetime; a new major sets it to the gateway minor's proto — raising it is a MAJOR act) and PROTO_MAX's doc states the additive-minor rule (minors may bump it while still speaking every proto back to the floor; dropping an old proto is a PROTO_MIN raise, i.e. a MAJOR). The mqtt_storage::schema module docs state the migration rule (store versions bump only in majors; each major migrates exactly one major back; the rolling path starts from the gateway minor, enforced by the handshake). The policy lives beside the constants that enforce it, so they cannot drift apart silently."
  - id: 0039-T3
    title: At 1.0 — skew test in CI (adjacent-pair rolling-upgrade smoke) once two releases exist; blocked until then
    status: deferred
    notes: "Needs two released versions to exist — impossible before 1.0 by definition. Recorded so the promise is not forgotten: when the first post-1.0 release ships, CI gains a mixed adjacent-pair rolling-upgrade smoke (join, serve, converge)."
---

# Delivery — ADR 0039: Release versioning and upgrade policy

Decision: [docs/adr/0039-versioning-and-upgrade-policy.md](../adr/0039-versioning-and-upgrade-policy.md).

The policy layer over ADR 0038's mechanisms: semver defined by wire/disk breakage,
adjacent-only version skew (enforced by the peer-proto handshake), sequential major
upgrades rolled through a designated gateway minor (enforced by the handshake and the
schema gate), and patches for the three most recent minor lines. Active from 1.0.0;
the pre-release freeze regime (ADR 0038) governs until then.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0039-T1** Policy docs | The ADR states the full policy; the README gains an "Upgrades & versioning" section an operator can act on (what a minor/major means, the adjacent-roll rule, sequential majors, supported lines, MQTT-client carve-out). |
| **0039-T2** Enforcement notes | The `PROTO_MIN`/`PROTO_MAX` docs state the release rules (`proto_min` frozen within a major, raised only by a new major to the gateway minor's proto; `proto_max` bumps additively in minors); the schema-module docs state the migration rule (each major migrates exactly one major back). The policy and the mechanism can never drift apart silently. |
| **0039-T3** Skew CI (post-1.0) | Once two releases exist: a CI job rolls a mixed adjacent-pair cluster through an upgrade (join, serve, converge). Blocked until 1.0 — recorded here so it is not forgotten. |

## Progress

<!-- status-table:0039 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0039-T1 | ✅ done | 2026-07-05 | "ADR 0039 states the full policy with the model survey (Kubernetes skew + 3-line maintenance, etcd/Cassandra adjacent stepping, Elasticsearch gateway-minor major rolls, Kafka negotiation, Postgres offline majors) and the reasoning for the composite; README gains an 'Upgrades & versioning' section stating, operator-facing: semver defined by wire/disk breakage, adjacent-only skew, sequential majors rolled through a designated gateway minor (the previous major's last minor, where known upgrade issues are fixed first) and dispatched on schema stamps, three supported lines, the MQTT-client carve-out, and the explicit pre-1.0 (ADR 0038 freeze) regime until activation." |
| 0039-T2 | ✅ done | 2026-07-05 | "PROTO_MIN's doc now states the release rule (frozen for a major's lifetime; a new major sets it to the gateway minor's proto — raising it is a MAJOR act) and PROTO_MAX's doc states the additive-minor rule (minors may bump it while still speaking every proto back to the floor; dropping an old proto is a PROTO_MIN raise, i.e. a MAJOR). The mqtt_storage::schema module docs state the migration rule (store versions bump only in majors; each major migrates exactly one major back; the rolling path starts from the gateway minor, enforced by the handshake). The policy lives beside the constants that enforce it, so they cannot drift apart silently." |
| 0039-T3 | 💤 deferred | — | "Needs two released versions to exist — impossible before 1.0 by definition. Recorded so the promise is not forgotten: when the first post-1.0 release ships, CI gains a mixed adjacent-pair rolling-upgrade smoke (join, serve, converge)." |
<!-- /status-table:0039 -->

## Changelog

- **2026-07-05** — Amended before merge: major upgrades roll through a **gateway
  minor** (Elasticsearch 6.8 → 7.x model) — a new major names the previous major's
  minor it upgrades from (by default the last one, where known upgrade issues are
  fixed first) and enforces it by setting `proto_min` to the gateway's proto; within
  a major `proto_min` is frozen and minors may bump `proto_max` additively. Keeps the
  tested cross-major surface at exactly one pair per transition.
- **2026-07-05** — ADR proposed and delivery opened, from the pre-release planning
  discussion: the mechanisms (ADR 0038 T1/T2) needed a policy to enforce. Model chosen
  as a composite of the proven pieces — semver labeling, etcd/Cassandra adjacent-step
  skew and sequential majors, Kubernetes 3-line maintenance — over K8s-width skew
  (untestable matrix for a young project; widening later is compatible, narrowing is
  not), Kafka any-to-any negotiation (every historical frame alive forever), and
  Postgres offline major migrations (wrong shape for rolling clusters).
