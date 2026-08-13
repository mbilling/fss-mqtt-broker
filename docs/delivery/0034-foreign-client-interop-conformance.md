---
adr: "0034"
title: Foreign-client interop conformance testing
adr_status: Accepted
tasks:
  - id: 0034-T1
    title: Interop harness — scripts/interop/run.sh boots the real mqttd (plaintext listener), waits on /readyz, runs a mosquitto_pub/_sub round-trip, asserts the payload, tears down; exits non-zero on mismatch; runnable locally
    status: done
    date: 2026-06-26
    evidence: "scripts/interop/run.sh — boots the real mqttd binary (MQTTD_BIN override or cargo build), gates on /readyz, runs concurrent mosquitto_sub/_pub round-trips via a roundtrip() helper with per-test timeout + unique client ids, asserts payloads, traps cleanup of every broker PID + tempdir. Verified locally: 8/8 checks pass (INTEROP OK)."
  - id: 0034-T2
    title: v3.1.1 matrix — QoS 0/1/2 payload-integrity round-trips plus a retained message delivered to a late subscriber
    status: done
    date: 2026-06-26
    evidence: "Phase A: QoS 0/1/2 payload round-trips + a retained message published-then-read by a late subscriber (and cleared). All pass against target/debug/mqttd."
  - id: 0034-T3
    title: v5 round-trip — mosquitto -V 5; assert a v5 User Property survives to the subscriber (ties the foreign oracle to ADR 0030)
    status: done
    date: 2026-06-26
    evidence: "mosquitto_pub -V mqttv5 -D publish user-property zone kitchen → mosquitto_sub -V mqttv5 -F '%p|%P' observes 'hello-v5|zone:kitchen' — the User Property survives the broker hop against a foreign client (ADR 0030)."
  - id: 0034-T4
    title: TLS interop — a Mosquitto client completes a TLS 1.3 handshake against the rustls listener (--cafile), proving OpenSSL↔rustls; an mTLS variant presents a client cert
    status: done
    date: 2026-06-26
    evidence: "gen_pki() mints a CA + 127.0.0.1 server leaf + client leaf via openssl. Phase A: OpenSSL client ↔ rustls server TLS 1.3 round-trip. Phase B: mTLS — a CA-signed client cert round-trips; a client with no cert is refused (empty receive). All pass."
  - id: 0034-T5
    title: CI job — a gating `interop` job in .github/workflows/ci.yml installs mosquitto-clients, builds the broker, runs scripts/interop/run.sh; isolated from the unit gate; deterministic (no flake)
    status: done
    date: 2026-06-26
    evidence: ".github/workflows/ci.yml `interop` job: installs mosquitto-clients (apt), cargo build -p mqttd, runs MQTTD_BIN=target/debug/mqttd ./scripts/interop/run.sh; isolated from the unit `test` job. Determinism via readiness-gating + unique client ids + per-test timeouts."
  - id: 0034-T6
    title: Docs — README + docs/TEST-PLAN.md note (what it asserts, how to run locally, the no-new-crate supply-chain property)
    status: done
    date: 2026-06-26
    evidence: "README Build & test: how to run scripts/interop/run.sh, what it asserts, the no-new-crate property. docs/TEST-PLAN.md priority #5 marked done with the non-Rust-oracle rationale (chosen over rumqttc)."
  - id: 0034-T7
    title: Follow-on — a second foreign client (Paho Python) behind the same harness for richer assertions (reason codes, properties, flow control)
    status: done
    date: 2026-07-17
    evidence: "Delivered by ADR 0044 P7: scripts/interop/paho_conformance.py drives Eclipse Paho (Python) programmatically — a second independent MQTT implementation — to assert the control-plane semantics the Mosquitto CLI cannot surface: v5 CONNACK reason code + session-present flag, per-filter SUBACK GRANTED QoS (QoS-1 filter granted 1, QoS-2 granted 2), a User Property surviving the hop (both keys), retained delivery with the retain flag SET on the late-subscriber delivery, and session-present TRUE on resume of a persistent (SessionExpiryInterval) session across a disconnect. External process (pip), zero cargo-supply-chain addition, wired into the per-PR interop CI job alongside the Mosquitto suite (which installs paho-mqtt too). 10/10 assertions green. Two independent foreign oracles now gate every PR."
  - id: 0034-T8
    title: Foreign-oracle case for the subscription-identifier capability advertisement and refusal
    status: done
    date: 2026-08-13
    evidence: "Issue #245 acceptance criterion 2 — the fix must be provable against a real client, not only our own codec. Four assertions added to scripts/interop/paho_conformance.py section 6, taking the lane from 10 to 14. (a) Paho's decoded CONNACK exposes SubscriptionIdentifierAvailable == 0; Paho allows property 41 on CONNACK natively (properties.py maps id 41, attribute name = spec name with spaces stripped) so this needs no workaround. (b) A DEDICATED client id 'paho-subid' — never 'paho-main', because this case ends in a server-initiated disconnect that would poison sections 3-5 which reuse that client — sets props.SubscriptionIdentifier = 7 on its SUBSCRIBE and is disconnected by the server with no SUBACK first. Paho does no client-side validation of 0x29 (grep -c SubscriptionIdentifier client.py == 0), so it genuinely puts the property on the wire. on_disconnect installed at the VERSION2 signature (client, userdata, disconnect_flags, reason_code, properties), read out of paho 2.1.0's _do_on_disconnect and confirmed by running the lane locally, not guessed; reconnect_on_failure=False stops the loop thread re-dialling after the close. (c) FINDING, and why the reason byte is asserted on a bare socket instead of through Paho: paho 2.1.0's Client._handle_disconnect only decodes the DISCONNECT reason code when Remaining Length is > 2, and a reason-plus-empty-properties DISCONNECT is exactly 2 bytes (e0 02 a1 00). Its own inline comment says 'if reason is absent (remaining length < 1)', so the intent was >= 1 — an upstream off-by-one that hides EVERY reason code this broker sends (0x82, 0x93, 0x94, 0xA1) behind reason 0 'Normal disconnection'. Our encoding is spec-legal: §3.14.2.2.1 permits omitting the property length only when Remaining Length < 2, and an explicit zero is always allowed. So a small hand-built v5 CONNECT + SUBSCRIBE on a plain socket asserts the exact frame e0 02 a1 00, which is what keeps the lane able to catch 0xA1 being swapped for 0xA2. Run locally against the real binary: 14/14 green; with the CONNACK push removed and the reason changed to 0xA2, exactly those two assertions FAIL and the script exits 1. The Mosquitto CLI cannot drive this case at all — its -D subscribe accepts only user-property, so the CLI cannot request an identifier (its -F '%S' specifier only becomes useful once identifiers are delivered). CI step renamed to name the new coverage; scripts/interop/ is outside the vendored mqttui trees, so no re-vendor."
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
| **0034-T8** Capability advertisement | The Paho oracle asserts the broker's v5 CONNACK advertises `Subscription Identifiers Available = 0`, and that a real client which sets a Subscription Identifier on its SUBSCRIBE is disconnected with no SUBACK. The reason byte `0xA1` is pinned on the wire, because Paho cannot decode a 2-byte DISCONNECT's reason. |

## Progress

<!-- status-table:0034 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0034-T1 | ✅ done | 2026-06-26 | "scripts/interop/run.sh — boots the real mqttd binary (MQTTD_BIN override or cargo build), gates on /readyz, runs concurrent mosquitto_sub/_pub round-trips via a roundtrip() helper with per-test timeout + unique client ids, asserts payloads, traps cleanup of every broker PID + tempdir. Verified locally: 8/8 checks pass (INTEROP OK)." |
| 0034-T2 | ✅ done | 2026-06-26 | "Phase A: QoS 0/1/2 payload round-trips + a retained message published-then-read by a late subscriber (and cleared). All pass against target/debug/mqttd." |
| 0034-T3 | ✅ done | 2026-06-26 | "mosquitto_pub -V mqttv5 -D publish user-property zone kitchen → mosquitto_sub -V mqttv5 -F '%p|%P' observes 'hello-v5|zone:kitchen' — the User Property survives the broker hop against a foreign client (ADR 0030)." |
| 0034-T4 | ✅ done | 2026-06-26 | "gen_pki() mints a CA + 127.0.0.1 server leaf + client leaf via openssl. Phase A: OpenSSL client ↔ rustls server TLS 1.3 round-trip. Phase B: mTLS — a CA-signed client cert round-trips; a client with no cert is refused (empty receive). All pass." |
| 0034-T5 | ✅ done | 2026-06-26 | ".github/workflows/ci.yml `interop` job: installs mosquitto-clients (apt), cargo build -p mqttd, runs MQTTD_BIN=target/debug/mqttd ./scripts/interop/run.sh; isolated from the unit `test` job. Determinism via readiness-gating + unique client ids + per-test timeouts." |
| 0034-T6 | ✅ done | 2026-06-26 | "README Build & test: how to run scripts/interop/run.sh, what it asserts, the no-new-crate property. docs/TEST-PLAN.md priority #5 marked done with the non-Rust-oracle rationale (chosen over rumqttc)." |
| 0034-T7 | ✅ done | 2026-07-17 | "Delivered by ADR 0044 P7: scripts/interop/paho_conformance.py drives Eclipse Paho (Python) programmatically — a second independent MQTT implementation — to assert the control-plane semantics the Mosquitto CLI cannot surface: v5 CONNACK reason code + session-present flag, per-filter SUBACK GRANTED QoS (QoS-1 filter granted 1, QoS-2 granted 2), a User Property surviving the hop (both keys), retained delivery with the retain flag SET on the late-subscriber delivery, and session-present TRUE on resume of a persistent (SessionExpiryInterval) session across a disconnect. External process (pip), zero cargo-supply-chain addition, wired into the per-PR interop CI job alongside the Mosquitto suite (which installs paho-mqtt too). 10/10 assertions green. Two independent foreign oracles now gate every PR." |
| 0034-T8 | ✅ done | 2026-08-13 | "Issue #245 acceptance criterion 2 — the fix must be provable against a real client, not only our own codec. Four assertions added to scripts/interop/paho_conformance.py section 6, taking the lane from 10 to 14. (a) Paho's decoded CONNACK exposes SubscriptionIdentifierAvailable == 0; Paho allows property 41 on CONNACK natively (properties.py maps id 41, attribute name = spec name with spaces stripped) so this needs no workaround. (b) A DEDICATED client id 'paho-subid' — never 'paho-main', because this case ends in a server-initiated disconnect that would poison sections 3-5 which reuse that client — sets props.SubscriptionIdentifier = 7 on its SUBSCRIBE and is disconnected by the server with no SUBACK first. Paho does no client-side validation of 0x29 (grep -c SubscriptionIdentifier client.py == 0), so it genuinely puts the property on the wire. on_disconnect installed at the VERSION2 signature (client, userdata, disconnect_flags, reason_code, properties), read out of paho 2.1.0's _do_on_disconnect and confirmed by running the lane locally, not guessed; reconnect_on_failure=False stops the loop thread re-dialling after the close. (c) FINDING, and why the reason byte is asserted on a bare socket instead of through Paho: paho 2.1.0's Client._handle_disconnect only decodes the DISCONNECT reason code when Remaining Length is > 2, and a reason-plus-empty-properties DISCONNECT is exactly 2 bytes (e0 02 a1 00). Its own inline comment says 'if reason is absent (remaining length < 1)', so the intent was >= 1 — an upstream off-by-one that hides EVERY reason code this broker sends (0x82, 0x93, 0x94, 0xA1) behind reason 0 'Normal disconnection'. Our encoding is spec-legal: §3.14.2.2.1 permits omitting the property length only when Remaining Length < 2, and an explicit zero is always allowed. So a small hand-built v5 CONNECT + SUBSCRIBE on a plain socket asserts the exact frame e0 02 a1 00, which is what keeps the lane able to catch 0xA1 being swapped for 0xA2. Run locally against the real binary: 14/14 green; with the CONNACK push removed and the reason changed to 0xA2, exactly those two assertions FAIL and the script exits 1. The Mosquitto CLI cannot drive this case at all — its -D subscribe accepts only user-property, so the CLI cannot request an identifier (its -F '%S' specifier only becomes useful once identifiers are delivered). CI step renamed to name the new coverage; scripts/interop/ is outside the vendored mqttui trees, so no re-vendor." |
<!-- /status-table:0034 -->

## Changelog

- **2026-08-13** — T8 landed (issue #245): the lane now proves the subscription-identifier
  capability advertisement and refusal against a real client, 10 assertions → 14. Recorded a
  genuine upstream finding while wiring it: paho 2.1.0 cannot decode the reason code of a
  minimal (2-byte) DISCONNECT, so every reason this broker sends reads as "Normal
  disconnection" to a Paho application. Our framing is spec-legal, so the reason byte is
  asserted on a bare socket in the same script rather than papered over.
- **2026-06-26** — ADR proposed and delivery opened, addressing `docs/TEST-PLAN.md` priority #5
  (real-client interop) with a **non-Rust** oracle chosen over the previously-sketched `rumqttc`
  dev-dep: stronger codec independence and **zero** added supply chain. Mechanism: a locally-
  runnable `scripts/interop/run.sh` driving the real `mqttd` with the Mosquitto CLI, gated in a
  dedicated CI job. Tasks `planned`; T7 (a second foreign client, Paho) deferred.
- **2026-06-26** — T1–T6 delivered. `scripts/interop/run.sh` drives the real `mqttd` binary
  with the Eclipse Mosquitto CLI through **8 checks** — v3.1.1 QoS 0/1/2, retained-to-a-late-
  subscriber, a v5 User Property surviving a hop (ADR 0030), OpenSSL↔rustls TLS 1.3, and mTLS
  (client cert accepted / no-cert refused) — all green locally (`INTEROP OK`). Added the gating
  `interop` CI job and the README / TEST-PLAN docs. **No crate added** to the dependency tree:
  the foreign client is an external process. T7 (a second foreign client, Paho) stays deferred.
