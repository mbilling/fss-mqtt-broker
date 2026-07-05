---
adr: "0039"
title: Release versioning and upgrade policy (semver, adjacent skew, sequential majors)
adr_status: Accepted
tasks:
  - id: 0039-T1
    title: Policy documented — ADR + README upgrade section (semver semantics, adjacent skew, sequential majors, 3 supported lines, 1.0 activation)
    status: done
    date: 2026-07-05
    evidence: "ADR 0039 states the full policy with the model survey (Kubernetes skew + 3-line maintenance, etcd/Cassandra adjacent stepping, Kafka negotiation, Postgres offline majors) and the reasoning for the composite; README gains an 'Upgrades & versioning' section stating, operator-facing: semver defined by wire/disk breakage, adjacent-only skew, sequential majors dispatched on schema stamps, three supported lines, the MQTT-client carve-out, and the explicit pre-1.0 (ADR 0038 freeze) regime until activation."
  - id: 0039-T2
    title: Enforcement notes wired to the mechanisms — proto_min policy rule and schema-gate migration rule recorded beside PROTO_MIN/PROTO_MAX and the schema module docs
    status: done
    date: 2026-07-05
    evidence: "PROTO_MIN's doc now states the release rule (every release sets it to the previous release's proto — the window IS the skew policy; raising it further in a minor is forbidden) and PROTO_MAX's doc marks a bump as a MAJOR release. The mqtt_storage::schema module docs state the migration rule (store versions bump only in majors; each major migrates exactly one major back; an older mismatch means upgrade through the intermediate). The policy lives beside the constants that enforce it, so they cannot drift apart silently."
  - id: 0039-T3
    title: At 1.0 — skew test in CI (adjacent-pair rolling-upgrade smoke) once two releases exist; blocked until then
    status: deferred
    notes: "Needs two released versions to exist — impossible before 1.0 by definition. Recorded so the promise is not forgotten: when the first post-1.0 release ships, CI gains a mixed adjacent-pair rolling-upgrade smoke (join, serve, converge)."
---

# Delivery — ADR 0039: Release versioning and upgrade policy

Decision: [docs/adr/0039-versioning-and-upgrade-policy.md](../adr/0039-versioning-and-upgrade-policy.md).

The policy layer over ADR 0038's mechanisms: semver defined by wire/disk breakage,
adjacent-only version skew (enforced by the peer-proto handshake), sequential major
upgrades (enforced by the schema gate), and patches for the three most recent minor
lines. Active from 1.0.0; the pre-release freeze regime (ADR 0038) governs until then.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0039-T1** Policy docs | The ADR states the full policy; the README gains an "Upgrades & versioning" section an operator can act on (what a minor/major means, the adjacent-roll rule, sequential majors, supported lines, MQTT-client carve-out). |
| **0039-T2** Enforcement notes | The `PROTO_MIN`/`PROTO_MAX` docs state the release rule (`proto_min` = previous release's proto); the schema-module docs state the migration rule (each major migrates exactly one major back). The policy and the mechanism can never drift apart silently. |
| **0039-T3** Skew CI (post-1.0) | Once two releases exist: a CI job rolls a mixed adjacent-pair cluster through an upgrade (join, serve, converge). Blocked until 1.0 — recorded here so it is not forgotten. |

## Progress

<!-- status-table:0039 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0039-T1 | ✅ done | 2026-07-05 | "ADR 0039 states the full policy with the model survey (Kubernetes skew + 3-line maintenance, etcd/Cassandra adjacent stepping, Kafka negotiation, Postgres offline majors) and the reasoning for the composite; README gains an 'Upgrades & versioning' section stating, operator-facing: semver defined by wire/disk breakage, adjacent-only skew, sequential majors dispatched on schema stamps, three supported lines, the MQTT-client carve-out, and the explicit pre-1.0 (ADR 0038 freeze) regime until activation." |
| 0039-T2 | ✅ done | 2026-07-05 | "PROTO_MIN's doc now states the release rule (every release sets it to the previous release's proto — the window IS the skew policy; raising it further in a minor is forbidden) and PROTO_MAX's doc marks a bump as a MAJOR release. The mqtt_storage::schema module docs state the migration rule (store versions bump only in majors; each major migrates exactly one major back; an older mismatch means upgrade through the intermediate). The policy lives beside the constants that enforce it, so they cannot drift apart silently." |
| 0039-T3 | 💤 deferred | — | "Needs two released versions to exist — impossible before 1.0 by definition. Recorded so the promise is not forgotten: when the first post-1.0 release ships, CI gains a mixed adjacent-pair rolling-upgrade smoke (join, serve, converge)." |
<!-- /status-table:0039 -->

## Changelog

- **2026-07-05** — ADR proposed and delivery opened, from the pre-release planning
  discussion: the mechanisms (ADR 0038 T1/T2) needed a policy to enforce. Model chosen
  as a composite of the proven pieces — semver labeling, etcd/Cassandra adjacent-step
  skew and sequential majors, Kubernetes 3-line maintenance — over K8s-width skew
  (untestable matrix for a young project; widening later is compatible, narrowing is
  not), Kafka any-to-any negotiation (every historical frame alive forever), and
  Postgres offline major migrations (wrong shape for rolling clusters).
