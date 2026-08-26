# OpenSSF Best Practices badge — the self-certification answer sheet

**Verified against `v1.0.9` (2026-08-26).** ADR 0069 T7. The registration at
<https://www.bestpractices.dev> is a maintainer act (browser login, ~15 min);
this sheet makes it paste-through: every **passing-level** criterion, the
answer, and the evidence URL. It stays in-tree afterwards as the public record
behind the badge.

Steps: log in with GitHub at bestpractices.dev → *Projects → Add* → pick this
repository → answer from the table → publish. Most criteria auto-detect from
the repo; the justification strings below fill the rest.

## Basics

| Criterion | Answer | Evidence |
|---|---|---|
| Project website, description | Met | the repository README |
| FLOSS license, license location | Met | `LICENSE` (Apache-2.0, OSI-approved) |
| Documentation: basics + interface | Met | README quickstart; docs/ (OPERATIONS, MIGRATION, GLOSSARY, mqttd.example.toml) |
| HTTPS project sites | Met | GitHub-hosted |
| Discussion mechanism | Met | GitHub issues/discussions |
| English supported | Met | — |

## Change control

| Criterion | Answer | Evidence |
|---|---|---|
| Public VCS, change history | Met | this repository |
| Unique version numbers; semver | Met | ADR 0039; SUPPORT.md |
| Release notes per release | Met | GitHub Releases (generated per tag; CHANGELOG.md explains the pointer) |

## Reporting

| Criterion | Answer | Evidence |
|---|---|---|
| Bug reporting process | Met | GitHub issues; the review-panel method files findings as issues |
| Vulnerability reporting process (private) | Met | SECURITY.md → private advisory form |
| Response ≤ 14 days | Met | SECURITY.md's stated expectations; advisory history |

## Quality

| Criterion | Answer | Evidence |
|---|---|---|
| Working build system | Met | cargo; reproducible release builds (RELEASING.md) |
| Automated test suite; invoked by CI | Met | ci.yml (`cargo test --all`, 6 gated jobs); nightly tiers |
| New functionality adds tests (policy) | Met | CONTRIBUTING.md; the red-first/mutation discipline; check-test-hygiene gates it structurally |
| Warning flags / linters enabled | Met | clippy (all targets) gated per PR |

## Security

| Criterion | Answer | Evidence |
|---|---|---|
| Secure development knowledge | Met | docs/THREAT-MODEL.md; 70 ADRs; docs/HARDENING.md |
| Use basic good crypto practices | Met | one provider (aws-lc-rs, ADR 0053); TLS 1.3 default; Argon2id; docs/compliance/crypto-policy.md |
| Secured delivery mechanism | Met | signed releases (cosign keyless), SLSA provenance, SBOM — RELEASING.md verify one-liners |
| Known vulnerabilities fixed | Met | cargo-audit gated; Dependabot; per-release VEX |
| No leaked credentials | Met | secrets by path only (config schema enforces); gitignored key material |

## Analysis

| Criterion | Answer | Evidence |
|---|---|---|
| Static analysis; before release | Met | clippy + CodeQL (codeql.yml) per PR |
| Dynamic analysis | Met | continuous fuzzing (6 cargo-fuzz targets, nightly); chaos/soak/upgrade harnesses |
| Memory-safety tools where applicable | Met | memory-safe Rust across the workspace; `unsafe` absent from product code |

## Honest gaps at silver/gold (not required for passing)

Two-person review and role separation fail on bus factor 1 — the same honesty
the threat model records. The passing level does not require them; do not claim
them at higher levels until a second maintainer exists.

**Published 2026-08-20: project 14161, passing (100%).** The badge lives beside
the Scorecard badge in the README; the live record is
<https://www.bestpractices.dev/projects/14161>.
