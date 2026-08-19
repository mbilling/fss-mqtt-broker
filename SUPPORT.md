# Support lifecycle

The [ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md) policy, restated as
the table a procurement or vendor-risk review can quote. The policy is in force
since `v1.0.0` (2026-08-19, the compatibility freeze — ADR 0058).

## Supported release lines

**The three most recent minor lines receive fixes.** Security and correctness
fixes land as patches on each supported line; a patch changes no wire, disk, or
config format of any kind (ADR 0039 §1). When a new minor ships, the oldest of
the three rotates out of support.

| Line | First release | Status | Supported until |
|------|---------------|--------|-----------------|
| 1.0.x | `v1.0.0` — 2026-08-19 | **current** | two more minor lines ship |

(Pre-1.0 releases — `v0.9.0`, `v0.9.1` — were pre-freeze by definition and are
unsupported; `v0.9.1` is the designated upgrade entry into the 1.0 line.)

## What "supported" means precisely

- **Upgrades**: a cluster may mix **adjacent releases** (N and N+1) indefinitely
  — the state a rolling upgrade passes through, and the only mixed state tested
  (the nightly two-binary roll proves it against the previous release in both
  directions). Major upgrades are sequential, through the previous major's
  designated **gateway minor** named in the new major's release notes.
- **Data**: your data survives every supported upgrade in place. A schema bump
  ships its migration in the same PR or CI refuses it (ADR 0058); a newer store
  refuses an older binary loudly instead of corrupting silently.
- **Security fixes**: reported per [SECURITY.md](SECURITY.md), fixed on every
  supported line, shipped as signed releases with SBOM and SLSA provenance
  ([RELEASING.md](RELEASING.md) documents artifact verification).

## Compliance evidence

For vendor-risk and regulatory reviews, the mapping documents live in
[`docs/compliance/`](docs/compliance/): [EU CRA readiness](docs/compliance/eu-cra.md)
(with the Article 14 reporting runbook), [IEC 62443](docs/compliance/iec-62443.md)
(4-1 SDL and 4-2 component requirements), and the
[SOC 2 / ISO 27001 evidence map](docs/compliance/soc2-iso27001.md). Each is
version-stamped and re-verified per release; none claims a certification the
project does not hold.

## Export control

mqttd contains cryptographic functionality (TLS 1.3 via rustls/AWS-LC, HMAC-
authenticated cluster gossip). Its source code is published and publicly
available in this repository. Publicly available open-source software using
standard published cryptography is **not subject to the U.S. Export
Administration Regulations** per EAR §734.7(a)(3) (as amended 2021-03-29,
86 FR 16482); to the extent a classification is requested for procurement
paperwork, the conventional self-classification for such software is
**ECCN 5D002, License Exception TSU / publicly-available treatment**. This
statement is self-classification guidance recorded for reviewers' convenience,
not legal advice; importers remain responsible for their own jurisdictions'
import rules.
