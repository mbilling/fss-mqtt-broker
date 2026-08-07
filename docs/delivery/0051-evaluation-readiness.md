---
adr: "0051"
title: "Evaluation readiness: an assessable, comparable, migratable first release"
adr_status: Proposed
tasks:
  - id: 0051-T1
    title: README truthing & restructure — differentiators and a TOC up top, an architecture sketch, a Bridge section + the missing mqtt-bridge crate-table row, stale facts fixed (44→55 ADRs) with a CI guard on derivable facts, and the MSRV / supported-platforms / pre-1.0-stability statements
    status: planned
  - id: 0051-T2
    title: Secured quickstart — generate certs → TLS + mTLS + ACL → foreign client connects, as a copy-paste block beside the plaintext one, wired into the quickstart-as-test CI job so the secure path cannot rot
    status: done
    date: 2026-08-07
    evidence: "Every getting-started command in the README ran with a plaintext listener and anonymous clients — for a security-first broker, the only copy-pasteable path was the insecure one, and the secure path existed solely as a config table the reader had to assemble. New README section 'Single node, secured (TLS 1.3 + mTLS + ACL)': a local CA, a server leaf, and TWO device certs, with one deny-by-default rule topics=[\"sensors/%i/#\"] — %i substitutes the authenticated identity (the cert CN), so a single rule confines each device to its own subtree. scripts/quickstart-smoke.sh (already wired into ci.yml) runs the block, adding six assertions: the config logs NO INSECURE warning (every opt-out of a secure default is loudly logged, so their absence is a checkable signal and a plaintext bind creeping in would otherwise pass unnoticed); an mTLS round-trip inside the grant; a client with no certificate refused at the TLS handshake; sensor-2 denied sensor-1's subtree and receiving NOTHING; and the same cert working normally in its own subtree, so the isolation assertion proves isolation rather than a broken client. Isolation check verified falsifiable by weakening the rule to sensors/# — fails with 'sensor-2 received sensor-1's traffic: [private]'. TWO TRAPS FOUND AND DOCUMENTED: (1) mosquitto_sub exits 0 when every filter is DENIED and 27 on a clean timeout — the first version of this test asserted on exit status and passed a broker that was refusing correctly; it now judges by delivery, and the README warns the reader off the same trap. (2) A denied PUBLISH is dropped but still acknowledged (3.1.1 has no negative PUBACK; withholding it would leave a conforming publisher retrying forever), so a publisher cannot tell it was refused — the denial is recorded in the audit log as acl.deny.publish, and the README now says so, because someone relying on the ACL as a security boundary would otherwise look in the wrong place. Denied SUBSCRIPTIONS are refused visibly with a per-filter reason code. Verified in CI on Linux, not only locally: run 31212672552 shows all six ok lines and QUICKSTART OK. openssl added to the script's tool preflight. PR #112."
  - id: 0051-T3
    title: Community surface — CONTRIBUTING.md (human-facing), CODE_OF_CONDUCT.md, issue/PR templates, changelog policy (GitHub Releases canonical, CHANGELOG.md pointer)
    status: done
    date: 2026-08-07
    evidence: "None of these existed. CONTRIBUTING.md is written for a person rather than as a formality: the build/test gates stated as gates (CI runs with RUSTFLAGS=-D warnings, so a warning is a failed build — the most common reason a first PR goes red), the local check commands worth running, an explicit note that the heavy assurance tiers run NIGHTLY so a contributor is not expected to run them, the MSRV (1.88; 1.89 for mqttd-operator) with rust-toolchain.toml explained as a reproducibility anchor rather than a requirement on the reader, and the two conventions that actually trip people here: ADR-records-why vs delivery-doc-records-progress, and 'a task title is a claim' — when scope changes you deliver the missing clause or narrow the title and open a task for the remainder, never delete the clause quietly. CODE_OF_CONDUCT.md is Contributor Covenant 2.1 with enforcement routed through GitHub private vulnerability reporting (the one private channel this repo already has). CHANGELOG.md is deliberately a POINTER, not a hand-maintained list: GitHub Releases is canonical, and this repo already retired one hand-maintained catalogue that drifted three ADRs behind — a second source of truth is always the one that goes stale. It carries a where-to-look table (releases / delivery dashboard / ADRs / Limitations / upgrade rules) and the pre-1.0 versioning statement. .github/ISSUE_TEMPLATE/: bug_report.yml (repro, logs with RUST_LOG, version, and a durable-sessions dropdown because several behaviours differ between durable and not; asks for the quote if a DOC created the wrong expectation — a misleading doc is treated as a real defect here), feature_request.yml (problem before solution, alternatives — which tend to become the ADR record — and a security-boundary dropdown), config.yml routing vulnerabilities to private reporting and 'is it built yet' to the dashboard. .github/pull_request_template.md asks why-not-what, how it was verified (a test that FAILS against the old behaviour is worth more than one that passes against the new), and includes the no-claim-broader-than-what-was-built check. README gained a Contributing section linking all of it. All template YAML parses; every relative link in the four new/edited docs resolves."
  - id: 0051-T4
    title: Cut v0.9.0 — flip ADR 0045 to Accepted, maintainer pushes the signed tag per RELEASING.md, verify the pipeline's artifacts end to end (first real signatures + SBOM complete 0045-T3/T5)
    status: planned
  - id: 0051-T5
    title: docs/COMPARISON.md + condensed README matrix — Mosquitto / EMQX / NanoMQ / VerneMQ; every cell matched / exceeded / missing-by-design (with the deciding ADR) / missing-for-now (with the tracking task); versions pinned, claims dated, losses as prominent as wins
    status: done
    date: 2026-08-03
    evidence: "docs/COMPARISON.md (dated 2026-08-03; pinned: mosquitto 2.0.22/2.1.2, EMQX 6.2.2, NanoMQ 0.25.5, VerneMQ 2.1.1, mqttd 0.9.0-rc) + README 'How it compares' condensed table. Competitor cells from docs/changelogs/source researched 2026-07-29→08-03; mqttd cells verified against source (conn.rs: ResponseTopic/CorrelationData forwarded, Receive Maximum 0x93 enforced; codec-only: subscription identifiers NOT delivered, no ServerKeepAlive, no AssignedClientIdentifier — all printed as losses). Unverifiable competitor cells marked n/v, never guessed; by-design absences cite their ADRs (dashboard/HTTP-API per signal-driven-ops posture). Losses printed: footprint, maturity (no released version, no users — stated verbatim), subscription ids, assigned client id, voter-set-bounded durable capacity."
  - id: 0051-T6
    title: Migration from Mosquitto — scripts/migrate/from-mosquitto.py (mosquitto.conf → ADR 0046 TOML, acl_file → ACL TOML, bridge blocks → mqtt-bridge rules) + guide + fixture tests; loud unmapped report, secrets never transformed, output must pass --check-config
    status: planned
  - id: 0051-T7
    title: Migration from EMQX — scripts/migrate/from-emqx.py (listeners, TLS, authn/authz sources, bridges → common-subset TOML) + guide + fixture tests; same three converter rules
    status: planned
  - id: 0051-T8
    title: Migration from NanoMQ — scripts/migrate/from-nanomq.py (listeners, TLS, auth, bridge config → common-subset TOML) + guide + fixture tests; same three converter rules
    status: planned
  - id: 0051-T9
    title: NanoMQ and VerneMQ join the bench harness (amends ADR 0048's competitor set; VerneMQ under the disclosed-posture fairness terms in 0048's 2026-08-03 amendment) and the first comparative results are published to docs/benchmarks/ under 0048-T4's honesty rules
    status: in-progress
    notes: "Harness wired 2026-08-03: compose profiles vernemq (vernemq/vernemq:2.1.1 — 'latest' is stale=2.0.1, 2.1.2+ are pre-releases; EULA accepted = testing use; env-mapped mTLS listener, require_certificate on) and nanomq (emqx/nanomq:0.25.5-slim — the smallest variant WITH TLS, same variant both postures; configs/nanomq.conf with verify_peer+fail_if_no_peer_cert), run.sh broker list + env.txt versions, bench/README fairness notes (VerneMQ node-local durable queues; NanoMQ inflight window unenforced; EMQX 5.8.6 pin flagged for re-review — last Apache line, EOL'd; current is BSL 6.x). NOT yet smoke-run: no docker daemon on the wiring machine — smoke is the next action on a docker host. Publication (the 0048-T4 gate) additionally needs the dedicated-host run and the maintainer's EMQX re-pin decision."
  - id: 0051-T10
    title: The bridge made assessable — a demo/ second security zone (Mosquitto upstream + mqtt-bridge with directional rules), a walkthrough doc, and the Grafana screenshot into the README
    status: planned
  - id: 0051-T11
    title: The 1.0.0 freeze — after the bake window, run the ADR 0038 wire/schema review consciously, run the 0039-T3 skew smoke against two real tags, then the maintainer cuts the freeze tag
    status: blocked
    notes: "Needs v0.9.0 shipped (T4), a bake window survived, and a second tag (0.9.x/0.10) to make the 0039-T3 adjacent-skew smoke real — impossible before two releases exist by definition."
---

# Delivery: ADR 0051 — Evaluation readiness

[ADR 0051](../adr/0051-evaluation-readiness.md) · tasks and status in the frontmatter
above · this file is the plan, progress log, and changelog.

<!-- status-table:0051 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0051-T1 | ⬜ planned | — |  |
| 0051-T2 | ✅ done | 2026-08-07 | "Every getting-started command in the README ran with a plaintext listener and anonymous clients — for a security-first broker, the only copy-pasteable path was the insecure one, and the secure path existed solely as a config table the reader had to assemble. New README section 'Single node, secured (TLS 1.3 + mTLS + ACL)': a local CA, a server leaf, and TWO device certs, with one deny-by-default rule topics=[\"sensors/%i/#\"] — %i substitutes the authenticated identity (the cert CN), so a single rule confines each device to its own subtree. scripts/quickstart-smoke.sh (already wired into ci.yml) runs the block, adding six assertions: the config logs NO INSECURE warning (every opt-out of a secure default is loudly logged, so their absence is a checkable signal and a plaintext bind creeping in would otherwise pass unnoticed); an mTLS round-trip inside the grant; a client with no certificate refused at the TLS handshake; sensor-2 denied sensor-1's subtree and receiving NOTHING; and the same cert working normally in its own subtree, so the isolation assertion proves isolation rather than a broken client. Isolation check verified falsifiable by weakening the rule to sensors/# — fails with 'sensor-2 received sensor-1's traffic: [private]'. TWO TRAPS FOUND AND DOCUMENTED: (1) mosquitto_sub exits 0 when every filter is DENIED and 27 on a clean timeout — the first version of this test asserted on exit status and passed a broker that was refusing correctly; it now judges by delivery, and the README warns the reader off the same trap. (2) A denied PUBLISH is dropped but still acknowledged (3.1.1 has no negative PUBACK; withholding it would leave a conforming publisher retrying forever), so a publisher cannot tell it was refused — the denial is recorded in the audit log as acl.deny.publish, and the README now says so, because someone relying on the ACL as a security boundary would otherwise look in the wrong place. Denied SUBSCRIPTIONS are refused visibly with a per-filter reason code. Verified in CI on Linux, not only locally: run 31212672552 shows all six ok lines and QUICKSTART OK. openssl added to the script's tool preflight. PR #112." |
| 0051-T3 | ✅ done | 2026-08-07 | "None of these existed. CONTRIBUTING.md is written for a person rather than as a formality: the build/test gates stated as gates (CI runs with RUSTFLAGS=-D warnings, so a warning is a failed build — the most common reason a first PR goes red), the local check commands worth running, an explicit note that the heavy assurance tiers run NIGHTLY so a contributor is not expected to run them, the MSRV (1.88; 1.89 for mqttd-operator) with rust-toolchain.toml explained as a reproducibility anchor rather than a requirement on the reader, and the two conventions that actually trip people here: ADR-records-why vs delivery-doc-records-progress, and 'a task title is a claim' — when scope changes you deliver the missing clause or narrow the title and open a task for the remainder, never delete the clause quietly. CODE_OF_CONDUCT.md is Contributor Covenant 2.1 with enforcement routed through GitHub private vulnerability reporting (the one private channel this repo already has). CHANGELOG.md is deliberately a POINTER, not a hand-maintained list: GitHub Releases is canonical, and this repo already retired one hand-maintained catalogue that drifted three ADRs behind — a second source of truth is always the one that goes stale. It carries a where-to-look table (releases / delivery dashboard / ADRs / Limitations / upgrade rules) and the pre-1.0 versioning statement. .github/ISSUE_TEMPLATE/: bug_report.yml (repro, logs with RUST_LOG, version, and a durable-sessions dropdown because several behaviours differ between durable and not; asks for the quote if a DOC created the wrong expectation — a misleading doc is treated as a real defect here), feature_request.yml (problem before solution, alternatives — which tend to become the ADR record — and a security-boundary dropdown), config.yml routing vulnerabilities to private reporting and 'is it built yet' to the dashboard. .github/pull_request_template.md asks why-not-what, how it was verified (a test that FAILS against the old behaviour is worth more than one that passes against the new), and includes the no-claim-broader-than-what-was-built check. README gained a Contributing section linking all of it. All template YAML parses; every relative link in the four new/edited docs resolves." |
| 0051-T4 | ⬜ planned | — |  |
| 0051-T5 | ✅ done | 2026-08-03 | "docs/COMPARISON.md (dated 2026-08-03; pinned: mosquitto 2.0.22/2.1.2, EMQX 6.2.2, NanoMQ 0.25.5, VerneMQ 2.1.1, mqttd 0.9.0-rc) + README 'How it compares' condensed table. Competitor cells from docs/changelogs/source researched 2026-07-29→08-03; mqttd cells verified against source (conn.rs: ResponseTopic/CorrelationData forwarded, Receive Maximum 0x93 enforced; codec-only: subscription identifiers NOT delivered, no ServerKeepAlive, no AssignedClientIdentifier — all printed as losses). Unverifiable competitor cells marked n/v, never guessed; by-design absences cite their ADRs (dashboard/HTTP-API per signal-driven-ops posture). Losses printed: footprint, maturity (no released version, no users — stated verbatim), subscription ids, assigned client id, voter-set-bounded durable capacity." |
| 0051-T6 | ⬜ planned | — |  |
| 0051-T7 | ⬜ planned | — |  |
| 0051-T8 | ⬜ planned | — |  |
| 0051-T9 | 🚧 in-progress | — | "Harness wired 2026-08-03: compose profiles vernemq (vernemq/vernemq:2.1.1 — 'latest' is stale=2.0.1, 2.1.2+ are pre-releases; EULA accepted = testing use; env-mapped mTLS listener, require_certificate on) and nanomq (emqx/nanomq:0.25.5-slim — the smallest variant WITH TLS, same variant both postures; configs/nanomq.conf with verify_peer+fail_if_no_peer_cert), run.sh broker list + env.txt versions, bench/README fairness notes (VerneMQ node-local durable queues; NanoMQ inflight window unenforced; EMQX 5.8.6 pin flagged for re-review — last Apache line, EOL'd; current is BSL 6.x). NOT yet smoke-run: no docker daemon on the wiring machine — smoke is the next action on a docker host. Publication (the 0048-T4 gate) additionally needs the dedicated-host run and the maintainer's EMQX re-pin decision." |
| 0051-T10 | ⬜ planned | — |  |
| 0051-T11 | ⛔ blocked | — | "Needs v0.9.0 shipped (T4), a bake window survived, and a second tag (0.9.x/0.10) to make the 0039-T3 adjacent-skew smoke real — impossible before two releases exist by definition." |
<!-- /status-table:0051 -->

## Plan

| Task | Done means |
|---|---|
| **0051-T1** README truthing | A reader discovers every shipped capability (bridge included), the top of the file argues the differentiators, no stated fact is stale, and MSRV/platforms/stability are explicit. The CI guard fails on a drifted derivable fact. |
| **0051-T2** Secured quickstart | Copy-paste from a clean checkout to a TLS+mTLS+ACL node with a foreign client connected — and CI runs those exact commands. |
| **0051-T3** Community surface | A first-time contributor and a first-time issue reporter each have a path; the changelog policy is written down. |
| **0051-T4** v0.9.0 | ADR 0045 Accepted; the signed tag exists; every published artifact verifies per RELEASING.md (cosign, provenance, SBOM, reproduce). |
| **0051-T5** Comparison | An expert from any of the four brokers can see in one read what they gain, what they lose, and which losses are on purpose. |
| **0051-T6..T8** Migration | A working competitor config converts to a validating mqttd TOML, with every unconverted directive explained — proven by fixture tests per product. |
| **0051-T9** New brokers benched | NanoMQ and VerneMQ run in the same harness under disclosed postures (VerneMQ per the 0048 fairness terms); the first honest numbers are in `docs/benchmarks/` linked from the README. |
| **0051-T10** Bridge assessable | Ten minutes with `demo/` shows a message crossing a security boundary under a deny-by-default rule — and the README shows the dashboard. |
| **0051-T11** 1.0.0 | The freeze is a reviewed act on evidence: bake survived, skew smoke green, ADR 0038 checklist walked. |

## Phased execution plan

| Phase | Tasks | Gate | Output |
|---|---|---|---|
| **A — truth the surface, ship** | T1, T2, T3 → T4 | fixes what actively misleads | `v0.9.0` exists; the repo stops under-selling and mis-stating |
| **B — the evaluation package** | T5–T10 (parallelizable; T9 rides ADR 0048's harness) | during the 0.9.x bake | an evaluator can assess, compare, and migrate without doing our work for us |
| **C — the conscious freeze** | T11 | bake + second tag + ADR 0038 review | `v1.0.0` |

Order inside A: T1/T2/T3 in any order, each its own PR; T4 last (the tag push is the
maintainer's act, gated on the ADR 0044 checklist which is already green).

## Changelog

- **2026-07-27** — ADR 0051 drafted from the release-readiness review: engineering ready
  (ADR 0044 green, ADR 0045 pipeline waiting on a tag), evaluation experience not — the
  finished bridge invisible in the README, no comparison or migration story, plaintext-only
  quickstarts, unpublished benchmarks, stale hand-written facts, missing community files.
  Decision: ship `v0.9.0` now (gated only on what misleads), deliver the evaluation package
  during the bake, keep `1.0.0` as the conscious ADR 0038 freeze. Maintainer widened the
  comparison set to include **NanoMQ** (benchmark + matrix + migration), triggering ADR
  0048's "widen if there is demand" clause — recorded there as an amendment.
- **2026-08-03** — Second widening: **VerneMQ** joins the benchmark and comparison set
  (maintainer decision, informed by the 2026-07-29 VerneMQ architecture + MQTT 5 analysis:
  masterless clustering makes it the closest structural neighbor and the most informative
  head-to-head for the clustering/durability claims). Fairness terms recorded in ADR
  0048's 2026-08-03 amendment: durability postures disclosed (their queues are node-local
  and unreplicated — like-for-like pairs our in-memory opt-out against their default),
  partition regime stated per scenario (their default fails closed on netsplit), pinned
  EULA'd image (free for testing). T5/T9 widened accordingly. Comparison-matrix rows
  already drafted from the analysis: durable sessions across node loss, cross-node
  backpressure vs bounded-buffer drop, partition behavior, built-in vs plugin enhanced
  auth, binary licensing (their packages are paywalled since 1.10; ours signed and free).
  Migration tooling (a from-vernemq converter) deliberately **not** added yet — pending a
  maintainer call on whether VerneMQ operators are a migration audience or a comparison
  audience only.
- **2026-08-03** — T5 delivered and T9's harness half wired (maintainer: "implement the
  amendment and add the comparison data"). `docs/COMPARISON.md` + README condensed matrix
  land with pinned versions and printed losses; competitor research 2026-07-29→08-03
  (VerneMQ arch + MQTT 5; NanoMQ; Mosquitto/EMQX/pinning verification), mqttd cells
  verified in source — which surfaced three of our own gaps now printed as ✖ and worth
  backlog consideration: **subscription-identifier delivery** (codec-only today),
  **assigned client ids** (empty v5 id refused, not assigned), **Server Keep Alive**
  (never sent). Licensing landscape shifted under the matrix: **EMQX is BSL 1.1 since
  5.9** (single node free, clustering commercial, last Apache line 5.8 EOL'd 2026-02-28)
  — bench's EMQX 5.8.6 pin flagged for re-review at publication. VerneMQ + NanoMQ compose
  profiles, nanomq.conf, run.sh wiring, and bench/README fairness notes committed;
  smoke pending a docker host (none on the wiring machine — honestly recorded in T9).
