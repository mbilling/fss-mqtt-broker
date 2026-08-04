# ADR 0053 — One crypto provider: aws-lc-rs everywhere, ring evicted

- **Status:** Accepted
- **Date:** 2026-08-04
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0053-single-crypto-provider.md](../delivery/0053-single-crypto-provider.md) — plan, progress, and changelog
- **Related:** [ADR 0002](0002-transport-security.md) (the provider decision this record
  amends — and whose "revisit when FIPS work starts" exit it takes early),
  [ADR 0003](0003-gossip-authentication.md) / [ADR 0013](0013-enhanced-authentication.md) /
  [ADR 0022](0022-signed-gossip.md) (the direct crypto call sites that move),
  [ADR 0036](0036-quic-transport.md) (whose "one provider" argument now points the other
  way), [ADR 0050](0050-oidc-token-authentication.md) (the JWT verifier whose backend
  moved in the staged prep), [ADR 0052](0052-codec-succession.md) (the sibling
  supply-chain succession)

> This record states the decision only. How it is being built and how far along it is
> live in the [delivery doc](../delivery/0053-single-crypto-provider.md).

## Context

ADR 0002 chose `ring` as the rustls provider for build simplicity, and ADRs 0003/0013/0022
reused it for gossip HMAC, AUTH nonces, and gossip signatures — "already in the tree, no
new dependency". Two things changed under that decision:

1. **ring went into maintenance mode upstream** (2025). The crate carrying this broker's
   highest-consequence code had the weakest maintenance story in the tree, and every
   security-review of the dependency set flagged it first.
2. **The premise "one provider" had silently become false.** The OTLP exporter's
   reqwest/rustls chain (ADR 0020 T9) enables `rustls/aws-lc-rs` through feature
   unification, defeating the workspace's `ring` pins: the build compiled **both** crypto
   stacks, `quinn-proto` linked both, and which one reqwest used depended on whether
   `install_default()` had run in that binary. ADR 0036's rejection of aws-lc-rs
   ("a second provider alongside ring") was inverted by reality — ring had become the
   second provider.

The security posture (SECURITY.md, restated in ADR 0052): assume an attacker with perfect
knowledge of the code; close gaps, don't rely on nobody noticing.

## Decision

**aws-lc-rs is the workspace's single crypto provider.** Concretely:

1. **Feature flips**: `rustls`/`tokio-rustls` on `aws-lc-rs`, `quinn` on
   `rustls-aws-lc-rs` (quinn-proto now links exactly one provider), the workspace `ring`
   dependency replaced by `aws-lc-rs` (its ring-compatible API makes the ports import
   renames), `rcgen` 0.13 on its `aws_lc_rs` feature (dev-only), `jsonwebtoken` 11 on
   `aws_lc_rs` (staged prep, landed separately).
2. **Provider selection stays explicit**: `mqtt-net`'s `provider()` and `mqttd`'s
   `install_default()` name `aws_lc_rs` — with one provider compiled this is
   belt-and-braces determinism, and no binary or test can silently resolve a different
   stack again.
3. **x509-parser is demoted to parsing only.** Its ring-backed `verify` feature is
   replaced by a first-party `verify_x509_signature` in `mqtt-auth` — the same
   OID→algorithm dispatch (RSA-PKCS1 SHA-256/384/512, ECDSA P-256/P-384 with the curve
   read from the issuer SPKI, Ed25519) onto `aws_lc_rs::signature::UnparsedPublicKey`,
   used for the gossip CRL and leaf-certificate chain checks. **Deliberate narrowing:**
   the library's SHA-1 RSA legacy arm is dropped — a SHA-1-signed cluster CA artifact is
   rejected, and any unknown algorithm fails closed.
4. **`deny.toml` bans ring** so it cannot re-enter the build via a transitive default
   feature. The ban is wrapper-scoped to `quinn-proto`/`rustls-webpki`, whose *disabled*
   optional ring features leave a phantom Cargo.lock entry (the lockfile records optional
   dependencies regardless of features); the compiled-graph check is
   `cargo tree -i ring` → nothing.

## Consequences

- **This is a maintenance-posture consolidation, not a memory-safety change.** Both
  providers are C/assembly under the hood and sit outside the workspace's
  `unsafe_code = "forbid"` boundary. What improves: active upstream maintenance, one
  stack instead of two, and a smaller story to audit.
- **Wire bytes are unchanged.** HMAC-SHA256, SHA-256 fingerprints, ECDSA/Ed25519
  signatures are algorithm-identical; the swim_auth golden HMAC vector (independently
  computed) pins this, and the full gossip/TLS/QUIC/WS test sweep ran green.
- **FIPS mode becomes reachable** (`aws-lc-rs`'s FIPS feature) — the certified-builds
  business line ADR 0002 deferred this decision to. Not enabled; now a feature flag away.
- **Build toolchain**: aws-lc-sys needs `cc` (+cmake on some targets) — already paid,
  since the OTLP chain has compiled aws-lc-sys into every build and the release
  pipeline's musl setup predates this change. No CI changes required.
- Ed25519 PKCS#8 acceptance is slightly wider (v1 and v2 documents both load); ECDSA
  signing no longer takes an RNG argument at key load. No call-site behavior change.
- ADRs 0002 (decision 1, FIPS consequence), 0003 (decision 2), 0013, 0022, and 0036
  (rejected-alternative note) carry amendment notes pointing here.
