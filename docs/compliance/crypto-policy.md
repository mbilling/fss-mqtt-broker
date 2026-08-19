# Cryptographic policy

**Verified against `v1.0.0` + ADR 0068 (2026-08-19).** What each build variant's
cryptography is, precisely — written so the true claim is quotable and the
overclaim is impossible.

## The standard build

One crypto provider across the entire workspace (ADR 0053): **aws-lc-rs**
(AWS-LC). TLS 1.3 by default (TLS 1.2 a hardened per-listener opt-in), rustls
with the aws-lc-rs provider on every plane — client listeners, WS/WSS, QUIC,
the cluster peer bus — and the same crate for every direct primitive: gossip
HMAC-SHA256, audit-chain SHA-256, backup integrity SHA-256, JWT/OIDC signature
verification, TLS key handling.

## The fips build (`--features fips`)

A **build variant, not a default** (ADR 0068). What changes:

- The whole graph's aws-lc-rs switches to **AWS-LC's FIPS module**
  (`aws-lc-fips-sys`); rustls runs its FIPS configuration.
- **Non-approved algorithms disappear** rather than being policy-filtered:
  no ChaCha20-Poly1305 suites, no standalone X25519 key exchange — P-256/P-384
  and the approved TLS 1.3 suites remain. Pinned by test
  (`mqtt-net fips_posture_tests`), not asserted.
- **The TLS 1.2 opt-in is refused at startup** (`MQTTD_TLS_ALLOW_TLS12` errors,
  naming the reason): the variant serves the approved posture only.
- The provider seam **asserts FIPS mode at construction** — a fips binary that
  somehow ran non-approved crypto would abort rather than serve.
- **Runtime visibility**, so an auditor verifies the running binary and not the
  artifact's name: the startup banner logs `crypto = "aws-lc-rs (FIPS mode)"`,
  `/statusz` carries a `crypto` field, and the metric
  `crypto_module_info{module="..."} 1` exports the same fact.

## The claim, stated exactly

The **module** carries the validation: AWS-LC's FIPS 140-3 CMVP validation
belongs to AWS-LC, referenced from the aws-lc-rs FIPS documentation. **The
product is not itself FIPS-certified**, and this document never says otherwise.
When fips release artifacts ship (0068-T4), each will name the exact
`aws-lc-fips-sys` version it embeds and the CMVP certificate number that
version's module holds — pinned at build time, not quoted from memory.

## Honest boundaries

- **Password verification uses Argon2id**, which is not a FIPS-approved
  algorithm. In a strictly-approved deployment, authenticate with **mTLS or
  OIDC** (both run entirely on approved primitives in the fips build) and
  leave password auth unconfigured. This is the sharpest known caveat and it
  is deliberate: Argon2id is the right password hash, and swapping it for an
  approved-but-weaker construction to launder a checkbox would be the kind of
  overclaim this document exists to prevent.
- The **platform matrix is narrower**: the FIPS module builds where AWS-LC
  supports it (the validated platforms are Linux; the variant also compiles on
  macOS for development). The standard musl-static release targets need their
  own fips build spike before fips artifacts ship — recorded in 0068-T4.
- The build toolchain grows: **cmake and Go** are required to compile the FIPS
  module.
- Operator obligations do not shrink: the [hardening baseline](../HARDENING.md)
  TLS items still apply — the variant narrows what *can* be configured, not
  what *must* be.
