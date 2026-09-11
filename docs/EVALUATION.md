# Evaluating mqttd

**Verified against `v1.0.16` (2026-09-11).** One page for a decision-maker.
The dated competitor matrix is [COMPARISON.md](COMPARISON.md); this page is the
fork that says whether that matrix is worth reading.

## Should you run this?

**Yes, if** the broker is part of the security perimeter — deny-by-default
identity and ACLs, mTLS or OIDC, a policy reload that evicts live sessions on
revocation, a tamper-evident audit chain, signed reproducible artifacts — **and**
persistent sessions must survive the loss of the node that accepted them without
an ops runbook. Durable, quorum-replicated sessions are the default, not an
add-on.

**No, if** you need a vendor dashboard, a SQL rule engine, or built-in
Kafka/HTTP sinks. Those are recorded absences ([ADR 0063](adr/0063-external-consumer-integration.md),
[COMPARISON.md](COMPARISON.md) losing cells), not gaps on a roadmap you can wait
out. External consumers are an ordinary MQTT `$share` group; see
[INTEGRATION.md](INTEGRATION.md).

**Not yet, if** your decision hinges on a measured near-linear scale-out curve
across QoS 0/1/2. Horizontal scalability is an architectural claim with
published one-host and small-cluster numbers
([DURABLE-PATH.md](benchmarks/DURABLE-PATH.md),
[SCALE-CURVE.md](benchmarks/SCALE-CURVE.md)); the ordered plan to measure and
then optimize is issue #537, not evidence that linear scaling is already
achieved.

## What is in force today

- **Protocol:** MQTT 3.1.1 + 5.0 over TCP, TLS 1.3, WebSocket, QUIC.
- **Compatibility promise:** [ADR 0039](adr/0039-versioning-and-upgrade-policy.md)
  from `v1.0.0` — adjacent-release version skew, migrations with every schema
  bump, patches for the three most recent minor lines ([SUPPORT.md](../SUPPORT.md)).
- **Checkable claims:** every capability maps to a task with evidence on the
  [delivery dashboard](delivery/STATUS.md). What is missing is listed in the
  README [Limitations](../README.md#limitations).

## Next pages, by question

| Question | Go to |
|---|---|
| How does it compare, including the cells it loses? | [COMPARISON.md](COMPARISON.md) |
| Can I stand up a secured cluster this afternoon? | [SECURED-CLUSTER-TUTORIAL.md](SECURED-CLUSTER-TUTORIAL.md) |
| Will my clients' sessions and reason codes match what I write? | [CLIENT-GUIDE.md](CLIENT-GUIDE.md) |
| What does day-2 look like on Kubernetes? | [KUBERNETES.md](KUBERNETES.md), [OPERATIONS.md](OPERATIONS.md) |
| Can I migrate from Mosquitto / EMQX / HiveMQ? | [MIGRATION.md](MIGRATION.md) |
| What is the threat model and the auditor checklist? | [THREAT-MODEL.md](THREAT-MODEL.md), [HARDENING.md](HARDENING.md) |
| EU CRA / IEC 62443 / SOC 2 mappings? | [compliance/](compliance/) |

Start at the repository [README](../README.md) for the two-minute run; this page
is only the evaluator fork.
