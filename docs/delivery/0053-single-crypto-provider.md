---
adr: "0053"
title: "One crypto provider: aws-lc-rs everywhere, ring evicted"
adr_status: Accepted
tasks:
  - id: 0053-T1
    title: jsonwebtoken 9→11 on the aws_lc_rs backend (staged prep, independently shippable); MSRV floor 1.85→1.88 with manifest lineage comment
    status: done
    date: 2026-08-04
    evidence: "PR #69; zero call-site changes; cargo +1.88.0 check --workspace green; mqtt-auth 120/120, mqttd auth 8/8"
  - id: 0053-T2
    title: Provider consolidation — rustls/tokio-rustls/quinn/rcgen feature flips, mqtt-net provider() + mqttd install_default() + 5 test/example pins to aws_lc_rs, ring→aws_lc_rs ports in swim_auth/enhanced/signed_gossip (+3 test files), from_pkcs8 arity fix
    status: done
    date: 2026-08-04
    evidence: "cargo tree -i ring → nothing compiled; quinn-proto feature graph shows only aws-lc; full sweep green: mqtt-auth 120, mqtt-cluster 229 (swim_auth golden HMAC vector = wire bytes unchanged), mqtt-net, mqttd tls/reload_tls/ws/quic/peer_identity/auth/protocol_violations"
  - id: 0053-T3
    title: First-party verify_x509_signature replaces x509-parser's ring-backed `verify` feature (CRL + leaf chain checks; SHA-1 legacy arm dropped, unknown algorithms fail closed)
    status: done
    date: 2026-08-04
    evidence: "signed_gossip tests green: ECDSA P-256/P-384 + Ed25519 round-trips, a_crl_not_signed_by_the_cluster_ca_is_rejected_at_load, a_cert_not_chaining_to_the_ca_is_rejected — both call sites exercise the new dispatch; x509-parser verify feature dropped from mqtt-auth"
  - id: 0053-T4
    title: deny.toml ring ban (wrapper-scoped to the quinn-proto/rustls-webpki phantom lock entries) + ADR amendments to 0002/0003/0013/0022/0036
    status: done
    date: 2026-08-04
    evidence: "cargo deny check → advisories/bans/licenses/sources all ok; ban verified to trip unscoped (lockfile records disabled optionals) and pass wrapper-scoped; amendment notes added at each ADR's affected passage"
  - id: 0053-T5
    title: FIPS-mode evaluation (aws-lc-rs fips feature — the ADR 0002 certified-builds line) and rcgen 0.13→0.14 Issuer migration
    status: planned
---

# 0053 — Single crypto provider: delivery

**Decision:** [ADR 0053](../adr/0053-single-crypto-provider-aws-lc-rs.md). One-line
story: the build was silently shipping two crypto stacks (ring pinned, aws-lc-rs pulled
in by the OTLP chain); it now ships exactly one — the actively maintained one — and
ring is banned from returning.

<!-- status-table:0053 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0053-T1 | ✅ done | 2026-08-04 | "PR #69; zero call-site changes; cargo +1.88.0 check --workspace green; mqtt-auth 120/120, mqttd auth 8/8" |
| 0053-T2 | ✅ done | 2026-08-04 | "cargo tree -i ring → nothing compiled; quinn-proto feature graph shows only aws-lc; full sweep green: mqtt-auth 120, mqtt-cluster 229 (swim_auth golden HMAC vector = wire bytes unchanged), mqtt-net, mqttd tls/reload_tls/ws/quic/peer_identity/auth/protocol_violations" |
| 0053-T3 | ✅ done | 2026-08-04 | "signed_gossip tests green: ECDSA P-256/P-384 + Ed25519 round-trips, a_crl_not_signed_by_the_cluster_ca_is_rejected_at_load, a_cert_not_chaining_to_the_ca_is_rejected — both call sites exercise the new dispatch; x509-parser verify feature dropped from mqtt-auth" |
| 0053-T4 | ✅ done | 2026-08-04 | "cargo deny check → advisories/bans/licenses/sources all ok; ban verified to trip unscoped (lockfile records disabled optionals) and pass wrapper-scoped; amendment notes added at each ADR's affected passage" |
| 0053-T5 | ⬜ planned | — |  |
<!-- /status-table:0053 -->

## Notes

- 2026-08-04 — Verified before the flip: `quinn-proto` linked **both** providers, and
  reqwest resolved ring inside `mqttd` (via `install_default`) but aws-lc-rs in any
  binary/test that never installed a default. The "one provider" premise of ADR 0036
  had already inverted.
- 2026-08-04 — `cargo tree -i ring` → nothing compiled; ring's Cargo.lock entry is a
  phantom kept alive by quinn-proto/rustls-webpki *disabled* optional features (the
  lockfile records optionals regardless of features), hence the wrapper-scoped ban.
