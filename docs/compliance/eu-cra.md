# EU Cyber Resilience Act — readiness statement

**Verified against `v1.0.0` (2026-08-19).** ADR 0067 T2: the CRA's essential
requirements as a checklist against shipped, checkable facts, plus the
reporting-duty runbook. **This is not a conformity assessment and confers no CE
marking**; it is the technical-documentation groundwork a manufacturer or
steward would build on, written now because the Act's reporting obligations
begin **September 2026** and its full obligations apply from **December 2027**.

## Scope and classification, stated honestly

- mqttd is open-source software, published in full, monetized by no one today.
  Under the CRA, obligations attach to products made available **in the course
  of a commercial activity**; a non-commercial OSS project falls largely outside
  the manufacturer regime, and organizations that *commercially* ship mqttd
  (embedded in a product, or as a paid service) inherit manufacturer or
  **open-source software steward** duties themselves. This file exists so that
  any such downstream party — and this project, should it ever commercialize —
  starts from evidence instead of archaeology.
- Product-category read: an MQTT broker is not among Annex III/IV's enumerated
  *important*/*critical* classes; the conservative working assumption is the
  **default category** (self-assessment route). A downstream integrator whose
  product IS listed (e.g., a network management system embedding mqttd) owes its
  own classification.

## Annex I Part I — essential cybersecurity requirements, mapped

| # | Requirement (condensed) | Shipped fact | Check it |
|---|---|---|---|
| 1 | Designed/produced with appropriate level of cybersecurity | Security-first design record: 70 ADRs, [threat model](../THREAT-MODEL.md), [hardening baseline](../HARDENING.md) | the documents; CI gates |
| 2 | Delivered **without known exploitable vulnerabilities** | `cargo audit` gates every PR and release; findings fixed or dispositioned in the [VEX](../../security/vex/README.md) | release pipeline logs; `vex-<version>.openvex.json` per release |
| 3 | **Secure by default** configuration | Deny-by-default authn/authz; TLS 1.3 only; every insecure posture opt-in and self-announced (`INSECURE:` lines — [HARDENING H-0](../HARDENING.md)) | boot log grep; hardening items H-1..H-4 |
| 4 | Security updates, automatic where appropriate; **separable from functional updates** | Patch releases change no format of any kind (ADR 0039 §1), so a security fix is adoptable without functional risk; three supported minor lines ([SUPPORT.md](../../SUPPORT.md)) | release notes; the patch rule in ADR 0039 |
| 5 | Protection from unauthorised access (authn, identity/access mgmt) | Argon2id passwords, mTLS with EKU + SAN discipline, OIDC (asymmetric-only), deny-by-default ACL, session-owner guard | THREAT-MODEL §1; HARDENING §2–3 |
| 6 | Confidentiality of stored/transmitted data | TLS 1.3 (client and cluster planes), no plaintext by default; at-rest encryption is the platform's (stated, not implied — [THREAT-MODEL §4](../THREAT-MODEL.md)) | HARDENING H-1.x, H-4.1 |
| 7 | Integrity of data, commands, configuration | Schema-gated stores, epoch fencing, hash-chained audit, backup sha-256 trailers, validate-before-swap config | THREAT-MODEL §2/§4/§5 |
| 8 | Process only what is **necessary** (data minimisation) | The broker stores payloads it is asked to retain/queue and identity subjects — nothing else; audit records carry **no credentials** by contract | [AUDIT-SCHEMA](../AUDIT-SCHEMA.md); `AuditSink` doc |
| 9 | Availability of essential functions; resilience to DoS | Admission caps before TLS work, auth penalty box, per-subscriber bounds, watermark **brownout** (refuse growth, keep serving), quorum write floor | THREAT-MODEL §1-DoS; ADR 0041 |
| 10 | Minimise own attack surface | No HTTP admin API, no dashboard (deliberate absences, recorded); read-only ops endpoints; signals-and-files control plane | ADR 0033/0051; THREAT-MODEL §5 |
| 11 | Reduce incident impact; exploitation mitigation | Fail-closed everywhere; effect-free refusals; distroless nonroot images; hardened systemd unit; memory-safe implementation language | HARDENING H-7.5/7.6 |
| 12 | Security-relevant information recorded (logging/monitoring) | Hash-chained audit trail with SIEM export, boundary-alert invariants, independent verifier | [AUDIT-SCHEMA](../AUDIT-SCHEMA.md); `scripts/audit-verify.py` |
| 13 | Users can securely remove data / transfer | Session takeover semantics, backup export/restore (documented window semantics), data-dir ownership | ADR 0062; OPERATIONS DR section |

## Annex I Part II — vulnerability handling, mapped

| Requirement | Shipped fact |
|---|---|
| Identify and document vulnerabilities; **SBOM** | CycloneDX per shipped binary, per release, signed |
| Address without delay; provide security updates | SUPPORT.md lifecycle; patches on all supported lines |
| Regular testing | CI battery per PR; nightly chaos/soak/upgrade; continuous fuzzing (6 targets) |
| Publish fixed-vulnerability information | GitHub Security Advisories + release notes; VEX per release |
| Coordinated disclosure **policy** | [SECURITY.md](../../SECURITY.md): private reporting channel, expectations, timelines |
| Share/receive vulnerability reports (contact point) | The repository's private advisory form (SECURITY.md names it) |
| Secure distribution of updates | Signed artifacts (cosign, keyless), SLSA provenance, reproducible builds — verification one-liners in [RELEASING.md](../../RELEASING.md) |

## The reporting-duty runbook (Article 14 — applies from September 2026)

When a vulnerability in mqttd is **actively exploited**, or a **severe incident**
affects it, the party with manufacturer/steward duties reports via **ENISA's
single reporting platform** to the designated CSIRT:

1. **≤ 24 h** — early warning (exploitation is happening; minimal detail).
2. **≤ 72 h** — vulnerability notification: nature, severity, affected
   versions (SUPPORT.md's table is the version source of truth), corrective
   measures available or planned.
3. **≤ 14 days** (vulnerability) / **≤ 1 month** (incident) — final report:
   root cause, mitigation, fix versions.
4. In parallel, the ordinary machinery runs: GHSA advisory drafted, fix lands
   on every supported line, VEX updated, users informed via the advisory and
   release notes.

For this repository today, the practical trigger is a report arriving through
SECURITY.md's channel with evidence of active exploitation: the maintainer
executes steps 1–3 **if** a steward/manufacturer duty applies to them, and in
all cases executes step 4 — the disclosure machinery does not wait on the
legal classification.

## Honest gaps

- **No CE marking, no conformity assessment, no notified body** — this file is
  evidence, not certification.
- **Support-period declaration**: SUPPORT.md declares the policy (three minor
  lines) but not a calendar end-date per product generation; a commercial
  shipper must declare one.
- **Automatic updates** (Annex I Part I item 4's "where appropriate"): mqttd
  ships no auto-updater by design (operators roll deliberately; the nightly
  two-binary roll proves the path). Recorded as a deliberate posture, not an
  oversight.

Corrections follow the repository rule: versioned, dated, re-stamped each
release (ADR 0067 T4).
