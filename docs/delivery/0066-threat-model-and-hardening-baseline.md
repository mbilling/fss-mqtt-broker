---
adr: "0066"
title: "Threat model, hardening baseline, and SIEM-consumable audit"
adr_status: Proposed
tasks:
  - id: 0066-T1
    title: "docs/THREAT-MODEL.md — STRIDE over the five surfaces, every row naming its mechanism + ADR or its accepted risk; kept current by the frozen-surface checklist"
    status: done
    date: 2026-08-19
    evidence: "docs/THREAT-MODEL.md, verified against v1.0.0: the trust boundaries stated in one paragraph (clients untrusted until authenticated; peers mutually trusted ONCE ADMITTED — the model defends admission and detects, it does not defend against an admitted-malicious node; disk trusted for what the broker wrote, not against filesystem-level attackers; the operator trusted, with the control plane making mistakes loud rather than resisting them). STRIDE per surface with every mitigation row naming its ADR and enforcement site, consolidated from a full-tree sweep: client listener (auth spoofing through session-owner guard, ACL enforcement points, admission caps/penalty box before TLS and Argon2 work, watermark brownouts), peer bus (cluster-CA admission, CN-binding both directions, live CRL, frozen frames, epoch fencing), gossip (MAC-before-decode, three postures with no cross-acceptance, V3 anti-replay, cluster identity + refound guard), stores/backup (schema gates incl. the spool, ownership stamps, trailer sha-256, fresh-dir restore, claim_session), control plane (the deliberate no-HTTP-admin absence, validate-before-swap, operator never-deletes-data). Accepted risks quoted per surface from the ADRs that accepted them (v3.1.1 silent denial ack, %c not a tenant boundary, watermark-not-ceiling, admitted-peer trust, plaintext-mesh no CN binding, one-Secret Helm starter, V1/V2 replay window, plaintext exports, set-not-target restore verification, unauthenticated ops HTTP) plus a five-item cross-cutting residual list led by bus factor/track record. Maintenance rule in the header: PRs touching a listener/frame/store/control verb must touch this file (ADR 0038 SS D is the checklist), and each release re-stamps the version header. Found and fixed during the sweep: SchemaError::Mismatch's runtime message still advised the pre-1.0 'wipe the store' recovery on BOTH refusal paths — post-freeze that is data-loss advice; reworded to name the correct action per direction, no test pinned the old text."
  - id: 0066-T2
    title: "docs/HARDENING.md — numbered, levelled baseline items, each with knob, default, and verification command"
    status: planned
  - id: 0066-T3
    title: "Audit-log SIEM export (RFC 5424 syslog and/or OTLP), documented schema, honest integrity story"
    status: planned
    notes: "The one product change in the record; the export is a copy of the ADR 0004 tamper-evident stream, never its replacement."
---

# Delivery — ADR 0066: Threat model, hardening baseline, SIEM-consumable audit

Decision: [docs/adr/0066-threat-model-and-hardening-baseline.md](../adr/0066-threat-model-and-hardening-baseline.md).

Consolidation, not invention: the threat reasoning distributed across sixty-four ADRs
becomes the one document a security architect asks for; the secure-configuration
narrative becomes a checkable baseline; the audit trail becomes ingestable by the
SIEMs that enterprise control sets actually run on.

## Progress

<!-- status-table:0066 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0066-T1 | ✅ done | 2026-08-19 | "docs/THREAT-MODEL.md, verified against v1.0.0: the trust boundaries stated in one paragraph (clients untrusted until authenticated; peers mutually trusted ONCE ADMITTED — the model defends admission and detects, it does not defend against an admitted-malicious node; disk trusted for what the broker wrote, not against filesystem-level attackers; the operator trusted, with the control plane making mistakes loud rather than resisting them). STRIDE per surface with every mitigation row naming its ADR and enforcement site, consolidated from a full-tree sweep: client listener (auth spoofing through session-owner guard, ACL enforcement points, admission caps/penalty box before TLS and Argon2 work, watermark brownouts), peer bus (cluster-CA admission, CN-binding both directions, live CRL, frozen frames, epoch fencing), gossip (MAC-before-decode, three postures with no cross-acceptance, V3 anti-replay, cluster identity + refound guard), stores/backup (schema gates incl. the spool, ownership stamps, trailer sha-256, fresh-dir restore, claim_session), control plane (the deliberate no-HTTP-admin absence, validate-before-swap, operator never-deletes-data). Accepted risks quoted per surface from the ADRs that accepted them (v3.1.1 silent denial ack, %c not a tenant boundary, watermark-not-ceiling, admitted-peer trust, plaintext-mesh no CN binding, one-Secret Helm starter, V1/V2 replay window, plaintext exports, set-not-target restore verification, unauthenticated ops HTTP) plus a five-item cross-cutting residual list led by bus factor/track record. Maintenance rule in the header: PRs touching a listener/frame/store/control verb must touch this file (ADR 0038 SS D is the checklist), and each release re-stamps the version header. Found and fixed during the sweep: SchemaError::Mismatch's runtime message still advised the pre-1.0 'wipe the store' recovery on BOTH refusal paths — post-freeze that is data-loss advice; reworded to name the correct action per direction, no test pinned the old text." |
| 0066-T2 | ⬜ planned | — |  |
| 0066-T3 | ⬜ planned | — | "The one product change in the record; the export is a copy of the ADR 0004 tamper-evident stream, never its replacement." |
<!-- /status-table:0066 -->

## Changelog

- **2026-08-19** — T1 shipped: docs/THREAT-MODEL.md. The sweep also caught the
  schema gate's runtime refusal message still advising the pre-1.0 wipe-and-rejoin
  recovery — reworded, since post-freeze that is data-loss advice.

- **2026-08-19** — ADR proposed and delivery opened, from the post-1.0-freeze
  enterprise-readiness review.
