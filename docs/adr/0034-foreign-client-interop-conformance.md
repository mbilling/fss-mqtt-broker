# ADR 0034 — Foreign-client interop conformance testing

- **Status:** Accepted
- **Date:** 2026-06-26
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0034-foreign-client-interop-conformance.md](../delivery/0034-foreign-client-interop-conformance.md) — plan, progress, and changelog
- **Related:** [ADR 0008](0008-mqtt-5-codec.md) (the codec this proves against a foreign
  encoder), [ADR 0030](0030-user-property-forwarding.md) (a v5 property a foreign client can
  observe surviving a round-trip), [ADR 0002](0002-transport-security.md) (the rustls listener
  a foreign TLS stack must interoperate with — minimal-supply-chain principle weighed below),
  [ADR 0025](0025-boundary-bridge.md) (round-trips a foreign *broker*, not a foreign *client*),
  `docs/TEST-PLAN.md` (which lists this as the open priority #5)

> This record states the decision only. How it is being built and how far along it is live in
> the [delivery doc](../delivery/0034-foreign-client-interop-conformance.md).

## Context

Every automated test drives the broker through its **own** codec: the integration suites build
packets with `mqtt-codec` and write them over `mqtt-net`'s `FrameReader`/`FrameWriter` — the
exact encoder/decoder the broker itself uses. This is the right *primary* tool: it is the only
way to send the malformed and adversarial frames the `protocol_violations` suite needs (a
conformant client library will not emit a wildcard PUBLISH topic or an out-of-range topic
alias), and it makes the v5 suites cheap.

But a broker tested **only** against its own codec has a blind spot: **a symmetric bug
cancels out.** If the encoder and decoder mis-handle a field the same way, every self-codec
test still passes while a real third-party client breaks on the wire. Nothing in CI exercises
the broker against an *independent* MQTT implementation, so codec-conformance drift against the
ecosystem is currently invisible until a user hits it.

`docs/TEST-PLAN.md` already names the missing piece — "a thin real-client interop suite … this
catches codec-conformance drift the self-codec cannot" — and parks it on a supply-chain
concern: its instinct was `rumqttc` (a Rust client as a dev-dependency). The foreign clients
that *do* touch the broker today are confined to the **non-gating** `demo/` (Eclipse Mosquitto
CLI in `loadgen.sh`, the `eclipse-mosquitto` image) and README copy-paste examples — none of
which fail a build.

A **non-Rust** oracle is in fact the stronger choice: it shares **zero** code with the broker,
so it cannot replicate the broker's own codec mistakes, and it adds **nothing** to the broker's
Rust dependency tree (the standing supply-chain concern). Mosquitto is already present in
`demo/`, ubiquitous, and speaks both 3.1.1 and 5.

## Decision

**Add an opt-in, CI-gated interop conformance suite that drives the real `mqttd` binary with a
foreign, non-Rust MQTT client — the Eclipse Mosquitto CLI as the baseline oracle — asserting
pub/sub round-trips in both v3.1.1 and v5 (and over TLS), adding no crate to the broker's
dependency tree.**

### 1. Foreign, non-Rust client as the oracle; the binary under test

The suite runs the **real `mqttd` binary** (as `binary_smoke` does) and exercises it with
`mosquitto_pub`/`mosquitto_sub` — a C implementation, independent codec, OpenSSL TLS. Because
the oracle shares no code with the broker, a passing round-trip is real evidence the broker
frames MQTT the way the ecosystem expects, not merely self-consistently.

### 2. No new Rust supply chain — a process/CI dependency, not a crate

The foreign client is an **external process** (installed via the distro's `mosquitto-clients`
package or the `eclipse-mosquitto` container), **not** a `dev-dependency`. `Cargo.toml`,
`Cargo.lock`, and `cargo deny` are untouched — the decisive advantage over a `rumqttc` dev-dep.
The cost moves from the crate graph to the CI image / a developer's `mosquitto` install.

### 3. A self-contained, locally-runnable harness

A script (`scripts/interop/run.sh`) boots `mqttd` with a plaintext (and a TLS) listener, waits
on `/readyz`, runs the round-trips, asserts payloads, and tears the broker down — exit non-zero
on any mismatch. It is runnable **locally** (`mosquitto-clients` on `PATH`), not CI-only magic,
so drift is reproducible on a laptop. CI invokes the same script.

### 4. What is asserted (focused smoke, not a full conformance battery)

- **v3.1.1** pub/sub round-trip at **QoS 0, 1, and 2** (payload integrity end to end).
- **Retained** message delivered to a **late** subscriber.
- **v5** round-trip (`-V 5`), asserting a v5-specific behaviour survives — a **User Property**
  echoed to the subscriber (`-D publish user-property k v`), tying the foreign oracle to
  ADR 0030.
- **TLS**: a Mosquitto client completes a TLS 1.3 handshake against the rustls listener
  (`--cafile …`), proving the OpenSSL↔rustls interop; an mTLS variant presents a client cert.

These are deliberately a thin, high-signal set — enough to catch wire-level framing/feature
drift, not a re-implementation of the OASIS conformance cases.

### 5. CI shape: a dedicated, gating job

A new job in `.github/workflows/ci.yml` (e.g. `interop`) installs `mosquitto-clients`, builds
the broker, and runs `scripts/interop/run.sh`. It is a **required** check on PRs — the entire
point is to fail the build when the broker mis-frames against a foreign client — but isolated
in its own job so a Mosquitto/image hiccup is diagnosable apart from the unit gate.

### 6. Scope and non-goals

- One foreign client to start (Mosquitto). A **second** (Paho Python — richer assertions on
  reason codes, properties, flow control) is a follow-on behind the same harness, not bundled.
- Not a full conformance suite (OASIS/Paho interoperability battery) — a focused smoke.
- The self-codec client stays **primary** for adversarial/malformed-input testing; this suite
  complements it (well-formed foreign encodings), it does not replace it.
- Does **not** address cluster config drift or multi-broker interop (ADR 0025 covers the
  foreign-*broker* bridge).

## Consequences

- **Good:** closes the symmetric-codec blind spot with an independent, non-Rust oracle; proves
  OpenSSL↔rustls TLS interop and v5 feature round-trips against a real client; adds **zero**
  crates to the broker's supply chain; reproducible locally.
- **Cost:** a CI job that builds the broker and depends on `mosquitto-clients`/an image (slower,
  one more moving part); a shell harness to maintain; foreign-client CLI quirks (timing,
  client-id collisions) must be handled to keep it non-flaky.
- **Risk:** a poorly-written harness is **flaky** (a broker that races a subscriber's
  subscription, ephemeral-port reuse) — mitigated by readiness-gating and ret*-until-subscribed
  patterns already proven in `binary_smoke`. A flaky required check is worse than none, so
  determinism is a first-class acceptance criterion.
- **Bounded blast radius:** interop lives in a script + a CI job + docs; it touches no broker
  code and no crate manifest.

## Alternatives considered

- **`rumqttc` dev-dependency behind an `interop` feature (the TEST-PLAN proposal).** Runs in
  plain `cargo test`, no Docker. But it is **Rust** (a weaker independence guarantee than a
  foreign stack) and pulls a client dependency tree `cargo deny` must vet — the supply-chain
  cost the TEST-PLAN itself flagged. Rejected as the *first* oracle; could be added later as a
  Rust-side complement if an in-`cargo test` interop check is wanted.
- **A full OASIS/Paho conformance battery.** Far higher fidelity, far higher cost and
  maintenance, and much of it overlaps the self-codec darksky suite. Deferred; start with a
  focused smoke.
- **Multiple foreign clients up front (Paho Python/Java, MQTT.js, HiveMQ).** More coverage but
  more CI surface and flake sources at once. Start with one (Mosquitto) behind a harness that a
  second client can slot into (T-deferred).
- **Codec fuzzing only (already present).** The fuzz target finds panics/crashes on *malformed*
  input at the untrusted boundary — complementary, but it does not prove *well-formed* foreign
  encodings round-trip, which is exactly the conformance question here. Not a substitute.
- **Status quo — `demo/` Mosquitto + manual README examples.** A foreign client does touch the
  broker, but nothing **asserts** an outcome or **gates** a merge, so drift still ships.
  Rejected.
