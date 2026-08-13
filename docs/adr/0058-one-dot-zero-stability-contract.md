# ADR 0058 — The 1.0 stability contract: upgrade-in-place, never wipe-and-rejoin

**Status:** Proposed
**Date:** 2026-08-11
**Relates to:** ADR 0038 (the freeze mechanisms), ADR 0039 (the versioning policy this
contract operationalises), ADR 0018 (on-disk persistence), #125 (release plan)

## Context

Every persona on the 2026-08-11 review panel named the same precondition, unprompted:
nobody migrates a fleet onto a broker whose releases may reshape wire and disk formats,
and whose documented recovery for a schema bump is *wipe the store and rejoin*. The
Mosquitto migrator said it sharpest: **"durable state that a release can invalidate isn't
durable."**

ADR 0039 already decided the policy — semver defined at the wire/disk layer, adjacent-only
skew, sequential majors through gateway minors, "applies from 1.0.0". ADR 0038 built the
mechanisms — a version-negotiating peer handshake and schema-stamped stores, both
fail-closed. What does **not** exist is the machinery the promise runs on: the schema gate
refuses every mismatch, migrations are "writable in the future", and the nightly rolling-
upgrade test guards today's formats without being named as the contract's oracle.

This ADR is that last mile: the contract stated as an operator-facing promise, and the
machinery landed **now** so that tagging 1.0 flips a sentence, not builds a subsystem.

## The contract (effective at the v1.0.0 tag)

1. **Your data survives every upgrade.** An upgrade never requires wiping a store. Within
   a major, a newer binary opens every store the major has ever written, unchanged or
   via automatic in-place migration. Across majors, the binary migrates from exactly one
   major back (sequential majors, ADR 0039 §2); a store more than one major old names the
   intermediate release to go through.
2. **Rolling upgrades hold, in both directions of the roll.** Adjacent minors interoperate
   on the wire indefinitely — not just mid-roll — enforced by the peer handshake floor
   (ADR 0038). Majors roll only from the previous major's gateway minor.
3. **A newer store refuses an older binary loudly.** Downgrade is not silent corruption;
   the gate names found-vs-expected and the release to run.
4. **Config is compatible within a major.** A config that started vX.Y starts vX.(Y+1);
   new keys ship with safe defaults. (Unknown-key strictness interacts with rollback —
   see T6.)
5. **Patches change no format of any kind** (ADR 0039 §1, restated as a promise).

Until the v1.0.0 tag, ADR 0038's freeze-and-break regime stands, and the README keeps
saying so.

## Decisions

### A. Migrations are per-step, transactional, and resumable

`mqtt-storage::schema` gains `gate_or_migrate`: a store found at an older version is
migrated **one version step at a time, each step in its own write transaction that also
bumps the stamp**. A crash mid-chain resumes at the stamped step on next open — there is
no partially-migrated state that a stamp does not describe. Found-newer refuses; a gap in
the chain refuses with the sequential-upgrade message. The existing `gate` stays for
callers with no registry (pre-1.0 semantics unchanged until the tag).

### B. The registries are wired now, empty

All four gated stores — `sessions.redb`, `replicas.redb`, `lease.redb`, `retained.redb` —
open through `gate_or_migrate` with (initially empty) migration registries. The first
post-1.0 schema bump writes a migration function next to the layout change, in the same
PR, or the store's own cross-version test fails. Wiring now means the 1.0 tag changes no
open-path code.

### C. The nightly rolling-upgrade test is the wire oracle, by name

`cluster_upgrade.rs`'s two-binary roll (BASELINE_REF → HEAD) is the enforcement of
contract clause 2. At 1.0, BASELINE_REF pins to the release tag and advances only along
the policy (previous minor; gateway minor across majors). A reshape that breaks the roll
is caught nightly, before it can become a release.

### D. What is frozen, enumerated

The surfaces the contract covers, so "breaking change" is checkable rather than argued:
the peer-bus frame protocol (negotiated range, ADR 0038), the SWIM datagram format
(HMAC-tagged, ADR 0003), the four store schemas above, the client-visible MQTT behaviour
matrix (COMPARISON.md's own cells), and the config surface (keys + semantics).

### E. Config forward-compatibility (T4, as delivered — issue #230)

`deny_unknown_fields` on every table made rollback within a major a lie: the moment an
X.(Y+1) minor adds a config key, the shared rendered config bricks an X.Y binary (the
crash-loop is the REVERSE of the upgrade the contract promises to survive). Delivered
rule: the schema stays **strict by default** — unknown keys fail the load, now with the
COMPLETE list and the escape hatch named in the error, replacing seventeen per-table
attributes that stopped at the first unknown key. The escape hatch is
`runtime.config_unknown_keys = "warn"` / `MQTTD_CONFIG_UNKNOWN_KEYS=warn` (the env layer
wins for this knob, since the file may be the very thing an older binary cannot read
strictly): the broker boots, each ignored key is warn-logged at boot AND on every hot
reload, and type mismatches still always fail. The posture deliberately trades the typo
net for rollback safety, which is why it is not the default, why the chart's
`--check-config` gate stays strict, and why the recommendation is to set it for the skew
window only. Blanket leniency (HiveMQ-style ignore-always) was rejected for the typo
hole; blanket strictness (EMQX-style) for the rollback hole — the knob takes the safe
half of each.

## Tasks

| id | title |
|----|-------|
| 0058-T1 | `gate_or_migrate`: per-step transactional resumable migrations in `mqtt-storage::schema`, with crash-resume and refusal tests |
| 0058-T2 | Wire all four stores through `gate_or_migrate` with empty registries; per-store cross-version fixture tests that fail when a schema bump lands without its migration |
| 0058-T3 | Name the nightly two-binary roll as the wire oracle: BASELINE_REF discipline documented in RELEASING.md, advancing per ADR 0039's skew policy from 1.0 |
| 0058-T4 | Config forward-compatibility decision: reconcile `deny_unknown_fields` with rollback within a major (an X.Y+1 config key must not brick an X.Y binary) |
| 0058-T5 | The freeze flip: at the v1.0.0 tag, update README/RELEASING wipe-and-rejoin language to the contract above; the pre-1.0 reshape window closes with a final surface audit |
