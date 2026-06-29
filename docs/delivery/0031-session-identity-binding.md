---
adr: "0031"
title: Bind the session to the authenticated identity
adr_status: Accepted
tasks:
  - id: 0031-T1
    title: Decide the mechanism (resume/takeover guard vs key namespacing) and the rotation/mismatch policy
    status: done
    date: 2026-06-29
    evidence: "Ratified in the ADR: mechanism = the secure-by-default takeover/resume guard (option C); mismatch failure mode = reject the CONNACK (0x87 / 3.1.1 code 5) + audit; identity-rotation policy = strict principal.subject match (a changed subject cannot resume — documented secure default); anonymous = one shared namespace under allow_anonymous; key-namespacing recorded as the candidate end state. ADR Open-questions section resolved; status Accepted."
  - id: 0031-T2
    title: SessionMeta carries the owning identity (durable codec + cluster carry, backward-compatible)
    status: done
    date: 2026-06-29
    evidence: "SessionMeta.owner: Option<String> added in mqtt-storage (logged.rs durable codec + the MemorySessionStore SessionEntry), appended after the expiry field with the EOF-defaulted pattern (ADR 0030) so pre-0031 records decode with owner=None and adopt their next claimant. The owner travels in the replicated meta snapshot, so it survives restart and cross-node takeover. Tests: logged decodes_pre_owner_meta_records / decodes_pre_expiry_meta_records (owner None), the_session_owner_is_durable_across_the_log (a second store over the same log enforces the binding)."
  - id: 0031-T3
    title: Attach guard — a persistent resume/takeover requires the connecting principal to match the session owner; mismatch is a reason-coded reject + audit
    status: done
    date: 2026-06-29
    evidence: "SessionStore::claim_session(client, owner) -> SessionClaim {Granted{present}|Denied{owner}} binds/verifies the owner atomically (default delegates to ensure_session + grants, for stubs). The hub's off-loop recovery (recover_once) calls it; a Denied becomes SessionRecovery::Denied -> AttachOutcome::OwnerMismatch. conn.rs maps that to CONNACK Not-authorized (connack_code(0x05) -> 0x87 for v5) and records a session.bind.mismatch audit event. Tests: hub a_different_identity_cannot_resume_a_persistent_session, a_different_identity_cannot_take_over_an_online_session; storage claim_session_binds_then_guards_the_owner, a_legacy_session_without_an_owner_adopts_its_next_claimant."
  - id: 0031-T4
    title: Anonymous-principal handling (shared namespace under allow_anonymous, documented as insecure-by-toggle)
    status: done
    date: 2026-06-29
    evidence: "The owner is principal.subject, which is the shared \"anonymous\" subject under allow_anonymous (mqtt-auth basic), so anonymous clients share one session namespace — no isolation promised, the documented insecure-by-toggle mode (ADR 0004 / ADR 0031 Boundaries). Test: hub anonymous_clients_share_one_identity_namespace."
  - id: 0031-T5
    title: Optional authorize_connect(identity, client_id) Authorizer hook + ACL syntax for id-namespacing policy
    status: done
    date: 2026-06-29
    evidence: "Authorizer::authorize_connect(identity, client_id) added with a default-allow body (opt-in; existing authorizers unaffected). AclPolicy implements it via `connect` rules: actions=[\"connect\"] with a `clients` glob list (%i-substitutable, fail-closed), mutually exclusive with topic rules. Enforcement is keyed on the policy declaring any connect rule — absent → unrestricted; present → deny-by-default within the namespace (deny wins, else allow, else refuse). conn.rs calls it after authentication (before relocation/attach) and rejects with CONNACK Not-authorized + an acl.deny.connect audit event. Tests: acl unit (namespacing, deny-wins, opt-in, mixed-action/clients validation), tests/acl.rs connect_acl_namespaces_client_ids_per_identity (e2e CONNACK 0x05)."
  - id: 0031-T6
    title: Adversarial tests (a different principal never resumes/takes over another's session; same principal always can; cross-node; offline-queue inheritance blocked)
    status: done
    date: 2026-06-29
    evidence: "hub.rs: a_different_identity_cannot_resume_a_persistent_session (mallory refused, alice resumes), a_different_identity_cannot_take_over_an_online_session (live session not seized), anonymous_clients_share_one_identity_namespace. storage: claim_session_binds_then_guards_the_owner, a_legacy_session_without_an_owner_adopts_its_next_claimant, logged the_session_owner_is_durable_across_the_log (cross-node/durable). A refused claim returns before finish_attach, so the rejected identity never registers an Outbound and inherits no queued messages or subscriptions."
---

# Delivery — ADR 0031: Bind the session to the authenticated identity

Decision: [docs/adr/0031-session-identity-binding.md](../adr/0031-session-identity-binding.md).

The MQTT session is keyed on the Client Identifier alone; the authenticated `principal` is
not part of the key and is not consulted on resume/takeover, so a different authenticated
identity can seize another's persistent session by reusing its id. This binds a session to
its authenticated owner. **Accepted** — the secure-by-default resume/takeover guard is built
and proven (T1–T4, T6), and the optional connect ACL (T5) is now implemented as an opt-in
refinement.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0031-T1** Mechanism | Ratify the mechanism (the secure-by-default resume/takeover **guard**, with key-namespacing recorded as the candidate end state) and the policies for identity rotation and the mismatch failure mode (reject vs fresh session). |
| **0031-T2** Owning identity | `SessionMeta` stores the creating `principal.subject`; the durable queued/meta codec and the cluster session carry it, backward-compatibly (the ADR 0030 EOF-defaulted pattern). |
| **0031-T3** Attach guard | A persistent CONNECT for an existing id resumes/takes over **only** if the connecting principal matches the stored owner; a mismatch rejects (CONNACK `0x87` / 3.1.1 code 5) and records an audit event — it never silently seizes or leaks the prior session. |
| **0031-T4** Anonymous | Under `allow_anonymous`, anonymous clients share one identity namespace (no isolation promised — the existing insecure-by-toggle mode), documented; authenticated connections are bound. |
| **0031-T5** Connect ACL | An optional `authorize_connect(identity, client_id)` on the `Authorizer` + ACL syntax lets a deployment constrain which ids an identity may claim (e.g. a per-identity prefix), layered on the guard. |
| **0031-T6** Adversarial tests | A different principal **never** resumes or takes over another's session (no message/subscription inheritance), including cross-node; the same principal always resumes; the anonymous mode behaves as documented. |

## Progress

<!-- status-table:0031 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0031-T1 | ✅ done | 2026-06-29 | "Ratified in the ADR: mechanism = the secure-by-default takeover/resume guard (option C); mismatch failure mode = reject the CONNACK (0x87 / 3.1.1 code 5) + audit; identity-rotation policy = strict principal.subject match (a changed subject cannot resume — documented secure default); anonymous = one shared namespace under allow_anonymous; key-namespacing recorded as the candidate end state. ADR Open-questions section resolved; status Accepted." |
| 0031-T2 | ✅ done | 2026-06-29 | "SessionMeta.owner: Option<String> added in mqtt-storage (logged.rs durable codec + the MemorySessionStore SessionEntry), appended after the expiry field with the EOF-defaulted pattern (ADR 0030) so pre-0031 records decode with owner=None and adopt their next claimant. The owner travels in the replicated meta snapshot, so it survives restart and cross-node takeover. Tests: logged decodes_pre_owner_meta_records / decodes_pre_expiry_meta_records (owner None), the_session_owner_is_durable_across_the_log (a second store over the same log enforces the binding)." |
| 0031-T3 | ✅ done | 2026-06-29 | "SessionStore::claim_session(client, owner) -> SessionClaim {Granted{present}|Denied{owner}} binds/verifies the owner atomically (default delegates to ensure_session + grants, for stubs). The hub's off-loop recovery (recover_once) calls it; a Denied becomes SessionRecovery::Denied -> AttachOutcome::OwnerMismatch. conn.rs maps that to CONNACK Not-authorized (connack_code(0x05) -> 0x87 for v5) and records a session.bind.mismatch audit event. Tests: hub a_different_identity_cannot_resume_a_persistent_session, a_different_identity_cannot_take_over_an_online_session; storage claim_session_binds_then_guards_the_owner, a_legacy_session_without_an_owner_adopts_its_next_claimant." |
| 0031-T4 | ✅ done | 2026-06-29 | "The owner is principal.subject, which is the shared \"anonymous\" subject under allow_anonymous (mqtt-auth basic), so anonymous clients share one session namespace — no isolation promised, the documented insecure-by-toggle mode (ADR 0004 / ADR 0031 Boundaries). Test: hub anonymous_clients_share_one_identity_namespace." |
| 0031-T5 | ✅ done | 2026-06-29 | "Authorizer::authorize_connect(identity, client_id) added with a default-allow body (opt-in; existing authorizers unaffected). AclPolicy implements it via `connect` rules: actions=[\"connect\"] with a `clients` glob list (%i-substitutable, fail-closed), mutually exclusive with topic rules. Enforcement is keyed on the policy declaring any connect rule — absent → unrestricted; present → deny-by-default within the namespace (deny wins, else allow, else refuse). conn.rs calls it after authentication (before relocation/attach) and rejects with CONNACK Not-authorized + an acl.deny.connect audit event. Tests: acl unit (namespacing, deny-wins, opt-in, mixed-action/clients validation), tests/acl.rs connect_acl_namespaces_client_ids_per_identity (e2e CONNACK 0x05)." |
| 0031-T6 | ✅ done | 2026-06-29 | "hub.rs: a_different_identity_cannot_resume_a_persistent_session (mallory refused, alice resumes), a_different_identity_cannot_take_over_an_online_session (live session not seized), anonymous_clients_share_one_identity_namespace. storage: claim_session_binds_then_guards_the_owner, a_legacy_session_without_an_owner_adopts_its_next_claimant, logged the_session_owner_is_durable_across_the_log (cross-node/durable). A refused claim returns before finish_attach, so the rejected identity never registers an Outbound and inherits no queued messages or subscriptions." |
<!-- /status-table:0031 -->

## Changelog

- **2026-06-29** — T5 (optional connect ACL) landed: `Authorizer::authorize_connect`
  (default-allow) + ACL `connect` rules (`actions=["connect"]`, `clients` globs with `%i`),
  checked at CONNECT and rejected with Not-authorized + an `acl.deny.connect` audit event.
  Opt-in: absent connect rules leave connect unrestricted; the secure-by-default owner guard
  is independent. ADR 0031 now fully delivered.
- **2026-06-29** — ADR **Accepted**; the secure-by-default guard landed (T1–T4, T6). A
  session now records its owning `principal.subject` in `SessionMeta` (durable, EOF-defaulted
  so older records adopt their next claimant; travels via the replicated log for cross-node
  takeover). `SessionStore::claim_session` binds/verifies the owner atomically; the hub's
  off-loop recovery calls it and a mismatch becomes `AttachOutcome::OwnerMismatch`, which
  `conn.rs` turns into a CONNACK Not-authorized (`0x87` / code 5) plus a `session.bind.mismatch`
  audit event. Anonymous remains one shared namespace under `allow_anonymous`. Proven by
  hub + storage adversarial tests (resume, live-takeover, anonymous, cross-node durable,
  legacy adoption). The optional connect ACL (T5) stays planned — an opt-in refinement.
- **2026-06-26** — ADR proposed and delivery opened. Surfaced from a review of session
  keying: the session (a security-relevant resource — queued data + subscriptions) is keyed
  on the Client Identifier alone, decoupled from the authenticated identity, so a takeover
  across identities is possible. Tasks `planned` pending ratification of the mechanism and
  the rotation/mismatch policy.
