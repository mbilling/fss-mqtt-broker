# Documentation index

**Verified against `v1.0.16` (2026-09-11).** Every top-level document in `docs/`
is listed here with its stakeholder and one line. An unlisted file fails CI
(ADR 0070 T8 / `scripts/check-readme-facts.py`). Decision records and delivery
progress live under [`adr/`](adr/) and [`delivery/`](delivery/) — they are the
*source* these pages distill, never the thing an external reader is sent to.

The front door is the repository [README](../README.md). Its "Start here" forks
by persona: *evaluate it · run it · build against it · secure and audit it ·
contribute*.

## Stakeholder documents

| Document | Stakeholder | One line |
|---|---|---|
| [EVALUATION.md](EVALUATION.md) | Evaluator / decision-maker | One-page "should we run this?" before the comparison matrix. |
| [COMPARISON.md](COMPARISON.md) | Evaluator / decision-maker | Dated competitor matrix (Mosquitto, EMQX, NanoMQ, VerneMQ) with losing cells printed. |
| [SECURED-CLUSTER-TUTORIAL.md](SECURED-CLUSTER-TUTORIAL.md) | Newcomer standing it up | Three-node TLS + mTLS + ACL cluster on Compose, CI-smoked. |
| [CLIENT-GUIDE.md](CLIENT-GUIDE.md) | Application developer | Session/expiry, the emitted reason-code catalogue, flow-control, refusal, worked examples. |
| [OPERATIONS.md](OPERATIONS.md) | Operator / SRE (day 2) | Rotation, backup/restore, shipped alerts with per-alert runbooks. |
| [CONFIGURATION.md](CONFIGURATION.md) | Operator / SRE | Generated `MQTTD_*` / TOML reference; CI-checked against the config code. |
| [SIZING.md](SIZING.md) | Operator / SRE | Capacity arithmetic: what each quota bounds, and what still merely browns out. |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | Operator / SRE | First-deployment failure modes and how to read them. |
| [KUBERNETES.md](KUBERNETES.md) | Kubernetes user | Helm chart READMEs, values reference, `MqttdCluster` CRD. |
| [MIGRATION.md](MIGRATION.md) | Migrator | Per-source entry points (Mosquitto, EMQX, HiveMQ) plus the dual-run cutover. |
| [THREAT-MODEL.md](THREAT-MODEL.md) | Security architect | STRIDE over five trust surfaces; every mitigation names the ADR that decided it. |
| [HARDENING.md](HARDENING.md) | Auditor | Checkable L1/L2 baseline: control, knob, default, auditor-runnable verification. |
| [AUDIT-SCHEMA.md](AUDIT-SCHEMA.md) | Auditor / SIEM | Hash-chained audit export: record format, kinds, boundary invariant, verifier. |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Contributor | Binary module map and hub seams, promoted from `main.rs` / ADR 0064. |
| [TEST-PLAN.md](TEST-PLAN.md) | Contributor | Living integration-test strategy and sunshine/darksky catalogue. |
| [CONTRIBUTING-agent.md](CONTRIBUTING-agent.md) | Contributor (agent sessions) | Git/MCP workflow for automated sessions. |
| [BRIDGE.md](BRIDGE.md) | Operator (boundary) | Single-instance vs HA pair topologies for the zone-crossing bridge. |
| [INTEGRATION.md](INTEGRATION.md) | Application developer | External consumers (Kafka/webhook/DB) as an ordinary `$share` group, no rule engine. |
| [GLOSSARY.md](GLOSSARY.md) | All | MQTT + mqttd clustering/security vocabulary. |
| [REVIEW-PANEL.md](REVIEW-PANEL.md) | Maintainer | Method for external-style documentation review panels. |
| [CAPABILITY-PLAN.md](CAPABILITY-PLAN.md) | Historical | v0.1 mission/principles; live status is the [delivery dashboard](delivery/STATUS.md). |

## Generated catalogues (not stakeholder primaries)

| Document | Stakeholder | One line |
|---|---|---|
| [test-inventory.md](test-inventory.md) | Contributor / CI | Every test function the binaries contain; `check-test-hygiene.py` holds it. |
| [test-settling-delays.md](test-settling-delays.md) | Contributor / CI | Deliberate wall-clock waits in tests, catalogued by the same gate. |

## Directories

| Path | Stakeholder | One line |
|---|---|---|
| [`adr/`](adr/) | Contributor (internal) | Why a decision was made. Frozen once Accepted. |
| [`delivery/`](delivery/) | Contributor (internal) | How it is built and where each task stands. [STATUS.md](delivery/STATUS.md) is generated. |
| [`benchmarks/`](benchmarks/) | Evaluator / operator | Published numbers with method, limits, and dated caveats. |
| [`compliance/`](compliance/) | Compliance / procurement | ADR 0067 mappings (EU CRA, IEC 62443, SOC 2/ISO 27001, crypto policy, OpenSSF). |
| [`postmortems/`](postmortems/) | Operator / contributor | Incident write-ups. |
| [`review-panels/`](review-panels/) | Maintainer | Dated panel runs. |
| [`examples/`](examples/) | Operator | Ready-made config presets (e.g. bounded-node). |
| [`mqttd.example.toml`](mqttd.example.toml) | Operator | Fully-commented TOML template matching the generated config reference. |

The compliance mappings at [`compliance/`](compliance/) plus [SUPPORT.md](../SUPPORT.md)
are the procurement surface; they are not duplicated here because they already
carry their own version stamps (ADR 0067).
