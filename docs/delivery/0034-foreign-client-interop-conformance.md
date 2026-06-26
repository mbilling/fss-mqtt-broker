---
adr: "0034"
title: Foreign-client interop conformance testing
adr_status: Proposed
tasks:
  - id: 0034-T1
    title: Interop harness — scripts/interop/run.sh boots the real mqttd (plaintext listener), waits on /readyz, runs a mosquitto_pub/_sub round-trip, asserts the payload, tears down; exits non-zero on mismatch; runnable locally
    status: planned
  - id: 0034-T2
    title: v3.1.1 matrix — QoS 0/1/2 payload-integrity round-trips plus a retained message delivered to a late subscriber
    status: planned
  - id: 0034-T3
    title: v5 round-trip — mosquitto -V 5; assert a v5 User Property survives to the subscriber (ties the foreign oracle to ADR 0030)
    status: planned
  - id: 0034-T4
    title: TLS interop — a Mosquitto client completes a TLS 1.3 handshake against the rustls listener (--cafile), proving OpenSSL↔rustls; an mTLS variant presents a client cert
    status: planned
  - id: 0034-T5
    title: CI job — a gating `interop` job in .github/workflows/ci.yml installs mosquitto-clients, builds the broker, runs scripts/interop/run.sh; isolated from the unit gate; deterministic (no flake)
    status: planned
  - id: 0034-T6
    title: Docs — README + docs/TEST-PLAN.md note (what it asserts, how to run locally, the no-new-crate supply-chain property)
    status: planned
  - id: 0034-T7
    title: Follow-on — a second foreign client (Paho Python) behind the same harness for richer assertions (reason codes, properties, flow control)
    status: deferred
    notes: start with one independent oracle (Mosquitto) to bound CI surface and flake sources; a second client adds coverage on the same harness once the first is stable in CI.
---

# Delivery — ADR 0034: Foreign-client interop conformance testing

Decision: [docs/adr/0034-foreign-client-interop-conformance.md](../adr/0034-foreign-client-interop-conformance.md).

Every automated test drives the broker through its **own** codec, so a symmetric encoder/decoder
bug cancels out and is invisible. This adds an independent, **non-Rust** oracle — the Eclipse
Mosquitto CLI — driving the real `mqttd` binary through pub/sub round-trips in v3.1.1 and v5 and
over TLS, gated in CI, **without adding any crate** to the broker's dependency tree (the foreign
client is an external process, not a dev-dependency). The self-codec client stays primary for
adversarial/malformed input; this complements it for well-formed foreign-encoding conformance.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0034-T1** Harness | `scripts/interop/run.sh` boots `mqttd` (plaintext), waits on `/readyz`, runs a `mosquitto_sub`/`mosquitto_pub` round-trip, asserts the payload, and tears the broker down — non-zero exit on any mismatch. Runs locally with `mosquitto-clients` on `PATH`. |
| **0034-T2** v3.1.1 matrix | Payload-integrity round-trips at QoS 0, 1, and 2; a **retained** message delivered to a **late** subscriber. |
| **0034-T3** v5 round-trip | `mosquitto_* -V 5` round-trip; a **User Property** set on publish is observed on the delivered message (ADR 0030), proving a v5 property survives against a foreign client. |
| **0034-T4** TLS interop | A Mosquitto client completes a TLS 1.3 handshake against the rustls listener (`--cafile`); an mTLS variant presents a client cert (`--cert/--key`). Proves OpenSSL↔rustls interop. |
| **0034-T5** CI job | A dedicated, **gating** `interop` job builds the broker, installs `mosquitto-clients`, and runs the harness; isolated from the unit `test` job; deterministic (readiness-gated, no client-id/port races). |
| **0034-T6** Docs | README + `docs/TEST-PLAN.md`: what the suite asserts, how to run it locally, and that it adds no crate to the supply chain. |
| **0034-T7** Follow-on | *(deferred)* A second foreign client (Paho Python) behind the same harness for richer assertions. |

## Progress

<!-- status-table:0034 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0034-T1 | ⬜ planned | — |  |
| 0034-T2 | ⬜ planned | — |  |
| 0034-T3 | ⬜ planned | — |  |
| 0034-T4 | ⬜ planned | — |  |
| 0034-T5 | ⬜ planned | — |  |
| 0034-T6 | ⬜ planned | — |  |
| 0034-T7 | 💤 deferred | — | start with one independent oracle (Mosquitto) to bound CI surface and flake sources; a second client adds coverage on the same harness once the first is stable in CI. |
<!-- /status-table:0034 -->

## Changelog

- **2026-06-26** — ADR proposed and delivery opened, addressing `docs/TEST-PLAN.md` priority #5
  (real-client interop) with a **non-Rust** oracle chosen over the previously-sketched `rumqttc`
  dev-dep: stronger codec independence and **zero** added supply chain. Mechanism: a locally-
  runnable `scripts/interop/run.sh` driving the real `mqttd` with the Mosquitto CLI, gated in a
  dedicated CI job. Tasks `planned`; T7 (a second foreign client, Paho) deferred.
