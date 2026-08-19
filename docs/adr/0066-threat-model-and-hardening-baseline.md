# ADR 0066 — Threat model, hardening baseline, and SIEM-consumable audit

- **Status:** Proposed
- **Date:** 2026-08-19
- **Delivery:** [docs/delivery/0066-threat-model-and-hardening-baseline.md](../delivery/0066-threat-model-and-hardening-baseline.md)
- **Related:** [ADR 0003](0003-gossip-authentication.md), [ADR 0005](0005-session-affinity.md),
  [ADR 0013](0013-enhanced-authentication.md), [ADR 0004](0004-identity-and-authentication.md)
  (the hash-chained audit log T3 exports), [ADR 0038](0038-prerelease-compatibility-freeze.md)
  §D (the frozen surfaces the model enumerates)

> This record states the decision only. Progress lives in the delivery doc.

## Context

The threat reasoning exists but is *distributed*: sixty-four ADRs each defend their own
surface (SWIM HMAC against gossip forgery, the vouched ProxyHello against session
theft, deny-by-default ACL, EKU-checked mTLS, effect-free refusals). A security
architect evaluating the broker asks for one document — "your threat model" — and there
is no honest answer that fits in a meeting. The same goes for configuration: the
SECURED-CLUSTER-TUTORIAL teaches the secure path narratively, but an auditor wants a
*checkable baseline* — numbered items, each with the knob that enforces it and the
command that verifies it. And the hash-chained audit log (ADR 0004) is only
compliance-real if the enterprise's SIEM can ingest it; an audit trail that cannot
reach Splunk or Sentinel does not exist for their control set.

## Decision

Three artifacts, each consolidating what already exists rather than inventing posture:

1. **`docs/THREAT-MODEL.md`** — a STRIDE pass over the four trust surfaces the ADRs
   already defend: the client-facing MQTT listener, the authenticated peer bus, the
   SWIM gossip plane, and the on-disk stores (plus the operator/CRD path as a fifth,
   control-plane surface). Each threat row names the mitigating mechanism and its ADR,
   or records an accepted risk with its rationale. The document is versioned and
   review-gated like an ADR: a PR that adds a listener, frame, or store must touch it
   (the ADR 0038 §D frozen-surface enumeration is the checklist for "must touch").
2. **`docs/HARDENING.md`** — a CIS-benchmark-shaped secure-configuration baseline:
   numbered, levelled items (L1 essential / L2 defense-in-depth), each stating the
   config knob or deployment setting, the shipped default, and a **verification
   command** an auditor can run. Grown from the tutorial and OPERATIONS content;
   written so a future government STIG can be derived from it mechanically.
3. **Audit-log SIEM export** — the one product change in this record: the ADR 0004
   audit stream becomes ingestable by standard collectors — RFC 5424 syslog and/or
   OTLP logs, schema documented field-by-field, with the integrity story (what
   tamper-evidence survives export, what a collector must do to keep it) stated
   honestly rather than implied.

## Consequences

- The threat model makes accepted risks quotable. That is its value and its cost:
  writing "single-node deployments have no quorum to defend" in one place invites the
  question in every sales conversation — which is better than the question arriving
  after deployment.
- The hardening baseline becomes a compatibility surface of its own: renaming a knob
  breaks a numbered item, so the doc joins the config surface under the ADR 0039
  contract's discipline.
- SIEM export must not weaken the audit trail: export is a *copy* of the
  tamper-evident stream, never a replacement for it, and the schema documents which
  guarantees do not survive the wire.

## Tasks

| id | title |
|----|-------|
| 0066-T1 | docs/THREAT-MODEL.md — STRIDE over the five surfaces, every row naming its mechanism + ADR or its accepted risk; kept current by the frozen-surface checklist |
| 0066-T2 | docs/HARDENING.md — numbered, levelled baseline items, each with knob, default, and verification command |
| 0066-T3 | Audit-log SIEM export (RFC 5424 syslog and/or OTLP), documented schema, honest integrity story |
