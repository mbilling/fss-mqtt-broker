# ADR 0067 — Compliance framework mappings: IEC 62443, EU CRA, SOC 2 / ISO 27001

- **Status:** Accepted
- **Date:** 2026-08-19
- **Delivery:** [docs/delivery/0067-compliance-framework-mappings.md](../delivery/0067-compliance-framework-mappings.md)
- **Related:** [ADR 0065](0065-security-legibility.md) (the artifacts these mappings
  cite), [ADR 0066](0066-threat-model-and-hardening-baseline.md) (threat model and
  baseline the mappings lean on), [ADR 0039](0039-versioning-and-upgrade-policy.md) /
  [ADR 0058](0058-one-dot-zero-stability-contract.md) (the support and update promises
  every framework asks about)

> This record states the decision only. Progress lives in the delivery doc.

## Context

Enterprises do not evaluate brokers against our vocabulary; they evaluate them against
their frameworks' vocabulary. Three matter for this product, in this order:

- **IEC 62443** — MQTT's largest buyers are industrial/OT, and 62443 is the language
  of every OT procurement: part 4-1 asks "was this developed under a secure
  development lifecycle?", part 4-2 asks "does the component meet these technical
  requirements?". The repo's practices (ADR discipline, red-first tests, fuzzing,
  coordinated disclosure, the delivery-evidence convention) map startlingly well to
  4-1 already — unwritten, that maps to zero.
- **EU Cyber Resilience Act** — the first *law* with product-level obligations for
  software sold into the EU: SBOM, secure defaults, coordinated disclosure, an
  actively-exploited-vulnerability reporting duty (from September 2026), and a
  declared support period, with full obligations from December 2027. The product
  already meets most of the substance; a conformance-shaped technical file says so
  while competitors scramble.
- **SOC 2 / ISO 27001** — these certify the *operator's organisation*, not the
  product, and no repo artifact changes that. What the product can do is make the
  customer's evidence collection trivial: a mapping from product features (audit
  log, mTLS, ACL, brownout refusals, backup integrity) to the SOC 2 CC-series and
  ISO 27001 Annex A controls they satisfy on the customer's behalf.

A mapping is a *claims document*: every row cites the mechanism (code, config, gate)
and, where one exists, the test that proves it — the delivery-doc evidence discipline
applied outward. An aspirational row is worse than an absent one.

## Decision

Three mapping documents under `docs/compliance/`, each row citing mechanism + proof:

1. **`iec-62443.md`** — 4-1 practice-by-practice against the actual SDL (with honest
   gaps: e.g. formal security-requirements tracing), and 4-2 component requirements
   (CRs) against broker features, stating the achievable security level per CR and
   naming what a higher level would require.
2. **`eu-cra.md`** — the Annex I essential requirements as a checklist against
   shipped facts (SBOM per release, secure defaults per the hardening baseline,
   disclosure process, the ADR 0039 support window as the declared support period),
   plus the reporting-duty runbook the maintainer would follow.
3. **`soc2-iso27001.md`** — the customer-facing control map: feature → CC-series /
   Annex A control → the evidence artifact the customer can pull (audit log export,
   release signature, SBOM, hardening-baseline verification output).

Statements of fact about the product, versioned with it: each document carries a
"verified against" version header and joins the release checklist — a claims document
that drifts from the product is a liability, not an asset.

## What this deliberately is not

Not certification: the org holds no ISO cert, no SOC 2 report, no 62443 certificate,
and the documents say so in their first paragraph. They are evidence accelerators for
the customer's own assessments — the honest form of compliance support an
open-source product can ship.

## Tasks

| id | title |
|----|-------|
| 0067-T1 | docs/compliance/iec-62443.md — 4-1 SDL mapping + 4-2 component-requirement mapping with achievable security levels and honest gaps |
| 0067-T2 | docs/compliance/eu-cra.md — Annex I essential-requirements checklist against shipped facts + the reporting-duty runbook |
| 0067-T3 | docs/compliance/soc2-iso27001.md — feature → control → pullable-evidence map for customer assessments |
| 0067-T4 | The mappings join the release checklist: "verified against" headers re-stamped per release, drift treated as a doc bug |
