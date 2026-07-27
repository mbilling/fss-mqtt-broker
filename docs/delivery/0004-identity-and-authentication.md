---
adr: "0004"
title: Identity model: mTLS Common Name first, deny by default
adr_status: Accepted
tasks:
  - id: 0004-T1
    title: mTLS leaf-cert Common Name -> Identity extraction (rejects bad CA bytes, no panics)
    status: done
    date: 2026-06-12
    evidence: mtls::extracts_common_name_as_subject / garbage_bytes_are_rejected_without_panic
  - id: 0004-T2
    title: CONNECT auth gate before the hub; deny-by-default BasicAuthenticator + MQTTD_ALLOW_ANONYMOUS opt-in
    status: done
    date: 2026-06-12
    evidence: auth::default_policy_rejects_anonymous_with_not_authorized / mtls_identity_is_accepted_even_without_anonymous
  - id: 0004-T3
    title: CONNACK code mapping (0x04 bad password, 0x05 not authorized) then close
    status: done
    date: 2026-06-12
    evidence: auth::password_credentials_are_rejected_with_bad_credentials
  - id: 0004-T4
    title: Topic ACL engine (MQTTD_ACL_FILE; deny>allow>default-deny; coverage/overlap subscription tests; %i)
    status: done
    date: 2026-06-12
    evidence: acl::deny_overlap_blocks_broad_subscription / narrow_allow_does_not_cover_broad_subscription / identity_substitution_scopes_topics
  - id: 0004-T5
    title: ACL enforcement (SUBSCRIBE 0x80 per-filter, PUBLISH drop-but-ack, will-topic 0x05 at CONNECT)
    status: done
    date: 2026-06-12
    evidence: acl::denied_publish_is_dropped_but_acked / unauthorized_will_topic_is_refused_at_connect
  - id: 0004-T6
    title: Step 4 audit trail - hash-chained AuditLog, structured tracing, client-id keyed (no secrets)
    status: done
    date: 2026-06-12
    evidence: AuditLog::audit_log_hash_chains_recorded_events; audit::successful_connect_is_audited / rejected_connect_is_audited / denied_publish_and_subscribe_are_audited
  - id: 0004-T7
    title: Step 5 peer node-id <-> certificate CN binding on the cluster bus (both link directions)
    status: done
    date: 2026-06-12
    evidence: peer_identity::cert_cn_mismatch_with_hello_node_id_is_rejected / honest_nodes_with_matching_cert_cn_link_and_route
  - id: 0004-T8
    title: Step 6 password (Argon2id) + token (JWT HS256/RS256) authenticators + ChainAuthenticator
    status: done
    date: 2026-06-12
    evidence: password::correct_password_authenticates_with_username_as_subject / unknown_username_is_rejected_indistinguishably_from_wrong_password; token::tampered_or_wrong_secret_signature_is_rejected; chain::first_abstains_second_accepts_yields_ok
    notes: "CORRECTION (2026-07-25): the TokenAuthenticator was verified only at the authenticator boundary — no CONNECT path constructed Credentials::Token, so JWT auth was unreachable from a real client (the unit tests hand-built the credential). The wire→Token bridge (JWT-in-password) that makes it actually reachable landed with ADR 0050; password auth was and is reachable."
  - id: 0004-T9
    title: Full OIDC discovery / JWKS rotation; MQTT5 enhanced auth after v5 codec
    status: cut
    notes: "SUPERSEDED — both halves shipped under their own records: OIDC discovery + JWKS rotation as ADR 0050 (proven against a live Keycloak, forced mid-run key rotation included), MQTT 5 enhanced auth as ADR 0013. Cut, not done: the capability is in the broker, but the work is counted once, where it was built."
  - id: 0004-T10
    title: Delivery-time ACL re-check in the hub (enforcement is subscription-time only)
    status: cut
    notes: "SUPERSEDED by ADR 0040. The goal — a tightened ACL reaching already-established subscriptions — ships as the reload-triggered grant sweep (hub::sweep_grants: revoked filters leave live routing and the durable subscription set; offline sessions are re-checked at resume). The mechanism named here, a per-delivery re-check, was weighed in ADR 0040's alternatives and rejected: it closes only the subscription gap, at an O(messages) hot-path cost that grows with fan-out, where the sweep is O(state) once per policy change and also reaches revoked certs, removed users, and peer links."
  - id: 0004-T11
    title: "SAN-based identity selection (MQTTD_MTLS_IDENTITY_SOURCE = cn | san-dns | san-uri | san-email), fail-closed on an absent or ambiguous SAN"
    status: planned
  - id: 0004-T12
    title: "%c (client-id) substitution in ACL topic patterns — carry the ClientId through the Authorizer trait, failing closed on unsafe ids"
    status: planned
  - id: 0004-T13
    title: Per-listener auth policies (each listener carrying its own authenticator/ACL)
    status: deferred
    notes: "Needs the flat one-bind-per-transport Listeners struct (ADR 0046) to become a list of named listener definitions, each with its own policy and reload path — a config-model decision that earns its own record rather than an option bolted onto this one. The fourth item of the old bundled T11, hot ACL reload, was delivered by ADR 0032/0033 and reaches live state via ADR 0040."
---

# Delivery — ADR 0004: Identity model: mTLS Common Name first, deny by default

Decision: [docs/adr/0004-identity-and-authentication.md](../adr/0004-identity-and-authentication.md).

## Plan

The decision's numbered points and the later "Steps 3–6 (implemented)" sections decompose
into these tasks. Each carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0004-T1** CN identity | The TLS-verified leaf's Subject CN maps to `Identity { subject }`; unparseable certs, trailing DER garbage, and missing/empty/non-string CNs are rejected without panicking on CA-controlled bytes. |
| **0004-T2** Auth gate + deny-by-default | Authentication is a CONNECT-time gate before the hub; `BasicAuthenticator` accepts cert identities, accepts anonymous only behind `MQTTD_ALLOW_ANONYMOUS` (logged INSECURE), and refuses password/token credentials. |
| **0004-T3** CONNACK mapping | Failed password credentials map to `0x04`, everything else to `0x05`; the connection then closes. |
| **0004-T4** ACL engine | A TOML policy (`MQTTD_ACL_FILE`) evaluated per identity/action/topic; deny>allow>default(deny); allow uses coverage and deny uses overlap for subscriptions; `%i` substitutes the identity subject. Without a policy file authorization is not enforced and is logged loudly. |
| **0004-T5** ACL enforcement | SUBSCRIBE returns per-filter `0x80` (denied filters never reach the hub); PUBLISH is dropped but still acked per QoS; a denied will topic is refused with `0x05` at CONNECT. |
| **0004-T6** Audit trail | `auth.success` / `auth.failure` / `acl.deny.*` flow into an `AuditSink`; the production `AuditLog` hash-chains every event and emits a structured `tracing` event; failures are keyed by client id, never a credential. |
| **0004-T7** Peer CN binding | On the cluster bus a peer's `Hello { node_id }` must equal its certificate's Subject CN, checked on both link directions; no binding on the plaintext mesh. |
| **0004-T8** Password + token auth | `PasswordAuthenticator` (Argon2id, no enumeration oracle) and `TokenAuthenticator` (JWT HS256/RS256, exp/iss/aud validation); `ChainAuthenticator` tries cert → password → token, first real verdict final. |
| **0004-T9** OIDC / enhanced auth | ~~Full OIDC discovery / JWKS rotation; MQTT 5 enhanced auth.~~ Cut — delivered as ADR 0050 and ADR 0013. |
| **0004-T10** Delivery-time ACL | ~~A delivery-time ACL re-check in the hub so live subscriptions honor policy changes.~~ Cut — the goal is met by the ADR 0040 reload-triggered grant sweep; the per-delivery mechanism was rejected there on merit. |
| **0004-T11** SAN identity | An operator selects which verified-leaf field is the subject: `cn` (default, unchanged) or a SAN of a chosen kind. Fail closed — the selected source absent, empty, or present more than once yields *no* identity, never a guess. The safety rules the CN path already enforces travel with it. |
| **0004-T12** `%c` substitution | ACL topic patterns substitute the connecting client id, so a policy can scope topics per session handle. The `Authorizer` trait carries the `ClientId` at every decision point (SUBSCRIBE, PUBLISH, will, and the ADR 0040 grant sweep). Client ids are client-chosen, so substitution fails closed on the same metacharacters as `%i`. |
| **0004-T13** Per-listener policies | Deferred — each listener carrying its own authenticator/ACL needs the listener config to become a list of named definitions (own record). |

## Progress

<!-- status-table:0004 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0004-T1 | ✅ done | 2026-06-12 | mtls::extracts_common_name_as_subject / garbage_bytes_are_rejected_without_panic |
| 0004-T2 | ✅ done | 2026-06-12 | auth::default_policy_rejects_anonymous_with_not_authorized / mtls_identity_is_accepted_even_without_anonymous |
| 0004-T3 | ✅ done | 2026-06-12 | auth::password_credentials_are_rejected_with_bad_credentials |
| 0004-T4 | ✅ done | 2026-06-12 | acl::deny_overlap_blocks_broad_subscription / narrow_allow_does_not_cover_broad_subscription / identity_substitution_scopes_topics |
| 0004-T5 | ✅ done | 2026-06-12 | acl::denied_publish_is_dropped_but_acked / unauthorized_will_topic_is_refused_at_connect |
| 0004-T6 | ✅ done | 2026-06-12 | AuditLog::audit_log_hash_chains_recorded_events; audit::successful_connect_is_audited / rejected_connect_is_audited / denied_publish_and_subscribe_are_audited |
| 0004-T7 | ✅ done | 2026-06-12 | peer_identity::cert_cn_mismatch_with_hello_node_id_is_rejected / honest_nodes_with_matching_cert_cn_link_and_route |
| 0004-T8 | ✅ done | 2026-06-12 | password::correct_password_authenticates_with_username_as_subject / unknown_username_is_rejected_indistinguishably_from_wrong_password; token::tampered_or_wrong_secret_signature_is_rejected; chain::first_abstains_second_accepts_yields_ok |
| 0004-T9 | ✂️ cut | — | "SUPERSEDED — both halves shipped under their own records: OIDC discovery + JWKS rotation as ADR 0050 (proven against a live Keycloak, forced mid-run key rotation included), MQTT 5 enhanced auth as ADR 0013. Cut, not done: the capability is in the broker, but the work is counted once, where it was built." |
| 0004-T10 | ✂️ cut | — | "SUPERSEDED by ADR 0040. The goal — a tightened ACL reaching already-established subscriptions — ships as the reload-triggered grant sweep (hub::sweep_grants: revoked filters leave live routing and the durable subscription set; offline sessions are re-checked at resume). The mechanism named here, a per-delivery re-check, was weighed in ADR 0040's alternatives and rejected: it closes only the subscription gap, at an O(messages) hot-path cost that grows with fan-out, where the sweep is O(state) once per policy change and also reaches revoked certs, removed users, and peer links." |
| 0004-T11 | ⬜ planned | — |  |
| 0004-T12 | ⬜ planned | — |  |
| 0004-T13 | 💤 deferred | — | "Needs the flat one-bind-per-transport Listeners struct (ADR 0046) to become a list of named listener definitions, each with its own policy and reload path — a config-model decision that earns its own record rather than an option bolted onto this one. The fourth item of the old bundled T11, hot ACL reload, was delivered by ADR 0032/0033 and reaches live state via ADR 0040." |
<!-- /status-table:0004 -->

## Changelog

- **2026-07-27** — Re-triaged the three open tasks; two of them had been quietly
  delivered elsewhere and one was a grab-bag hiding the fact.
  **T9 cut** (OIDC/JWKS is ADR 0050, enhanced auth is ADR 0013 — both shipped).
  **T10 cut**: ADR 0040 delivers its goal by a different, deliberately chosen
  mechanism (reload-triggered grant sweep, not a per-delivery check), and ADR 0040's
  own alternatives section records why. The bundled **T11 is unbundled**: its hot-ACL-reload
  item shipped as ADR 0032/0033, so what is left splits into **T11** (SAN-based identity),
  **T12** (`%c` substitution) and **T13** (per-listener policies, still deferred — it is a
  config-model change, not a flag). T11/T12 move from `deferred` to `planned`: the reason
  T12 was deferred ("until the `Authorizer` trait carries the client id") is a thing to
  build, not a thing to wait for.
- **2026-06-12** — Steps 4–6 landed: hash-chained audit trail (T6), peer node-id↔cert-CN
  binding (T7), and the password/token/chain authenticators (T8). Remaining items
  (OIDC/JWKS, delivery-time ACL, SAN/per-listener/hot-reload/`%c`) split out as T9–T11.
- **2026-06-12** — Step 3 ACL engine landed (T4 evaluation, T5 enforcement).
- **2026-06-12** — Core identity model landed: CN extraction (T1), CONNECT auth gate +
  deny-by-default (T2), CONNACK mapping (T3).
