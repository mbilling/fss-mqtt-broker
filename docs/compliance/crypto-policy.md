# Cryptographic policy

**Verified against `v1.2.0` + ADR 0068 (2026-10-12).** What each build variant's
cryptography is, precisely — written so the true claim is quotable and the
overclaim is impossible.

## The standard build

One crypto provider across the entire workspace (ADR 0053): **aws-lc-rs**
(AWS-LC). TLS 1.3 by default (TLS 1.2 a hardened per-listener opt-in), rustls
with the aws-lc-rs provider on every plane — client listeners, WS/WSS, QUIC,
the cluster peer bus, the admin API — and the same crate for every direct primitive: gossip
HMAC-SHA256, audit-chain SHA-256, backup integrity SHA-256, JWT/OIDC signature
verification, TLS key handling, and what the rule engine (ADR 0083 to 0086) adds for
the broker's own use: the SHA-256 that pins a WebAssembly module and names a rules
file, the per-process HMAC-SHA256 of a rule's definition, the SHA-256 by which the
audit log names a rule-state key, and the random source behind `uuid_v4` and `random`.

**Digests a rule author asks for are a different thing.** Rule SQL has EMQX's `md5`,
`sha`, `sha256` and `hash(Algorithm, Data)` functions. They transform message data at
the operator's request; the broker rests no security decision on them. `sha`/`sha1`,
`sha224`, `sha256`, `sha384`, `sha512` and `sha512_256` are computed by aws-lc-rs.
The rest are computed by other code: `md5` by an
implementation in `mqtt-rules`, and `md4`, `sha512_224`, `sha3_224` to `sha3_512`,
`shake128`, `shake256`, `blake2b`, `blake2s`, `ripemd160` and `sm3` by the RustCrypto
crates (`crates/mqtt-rules/src/funcs.rs`, `hash`). The jq module in the WebAssembly
sandbox is given no randomness and contains no cryptography the broker uses.

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
Fips release artifacts ship from the same pipeline as everything else
(`mqttd-fips-<version>-<target>`, checksummed, cosign-signed,
provenance-attested, **byte-reproducible** — proven by the same clean-rebuild
comparison as the standard binaries), accompanied by
`fips-module-<version>.txt` naming the exact `aws-lc-fips-sys` version the
binaries embed (from the tagged lockfile) and pointing at the module's
authoritative CMVP documentation — pinned at build time, not quoted from
memory.

## Honest boundaries

- **Password verification uses Argon2id**, which is not a FIPS-approved
  algorithm. In a strictly-approved deployment, authenticate with **mTLS or
  OIDC** (both run entirely on approved primitives in the fips build) and
  leave password auth unconfigured. This is the sharpest known caveat and it
  is deliberate: Argon2id is the right password hash, and swapping it for an
  approved-but-weaker construction to launder a checkbox would be the kind of
  overclaim this document exists to prevent.
- **Rule SQL's digest functions are not narrowed by the fips build.** `md5`, and
  `hash` with `md4`, `md5`, `sha512_224`, the `sha3_*` and `shake*` family, `blake2b`,
  `blake2s`, `ripemd160` or `sm3`, run outside the validated module in every build,
  and several of those algorithms are not approved at all. They exist for EMQX
  compatibility and compute over message data only. A strictly-approved deployment
  writes no rule that calls them for a security purpose
  (`grep -nE 'md5\(|hash\(' <rules file>`); `sha`, `sha256` and `hash` with `sha224`,
  `sha256`, `sha384`, `sha512` or `sha512_256` run in the module.
- The **platform matrix held**: the FIPS module builds on both musl-static
  release targets (proven 2026-08-20 — build, execution, and byte-identical
  rebuild on x86_64 and aarch64; the one gap was kernel UAPI headers on
  Debian's musl-gcc, shimmed with `-idirafter` in the release recipe). The
  validated platforms remain Linux; macOS compiles for development.
- The build toolchain grows: **cmake and Go** are required to compile the FIPS
  module.
- Operator obligations do not shrink: the [hardening baseline](../HARDENING.md)
  TLS items still apply — the variant narrows what *can* be configured, not
  what *must* be.
