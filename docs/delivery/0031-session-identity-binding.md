---
adr: "0031"
title: Bind the session to the authenticated identity
adr_status: Proposed
tasks:
  - id: 0031-T1
    title: Decide the mechanism (resume/takeover guard vs key namespacing) and the rotation/mismatch policy
    status: planned
  - id: 0031-T2
    title: SessionMeta carries the owning identity (durable codec + cluster carry, backward-compatible)
    status: planned
  - id: 0031-T3
    title: Attach guard — a persistent resume/takeover requires the connecting principal to match the session owner; mismatch is a reason-coded reject + audit
    status: planned
  - id: 0031-T4
    title: Anonymous-principal handling (shared namespace under allow_anonymous, documented as insecure-by-toggle)
    status: planned
  - id: 0031-T5
    title: Optional authorize_connect(identity, client_id) Authorizer hook + ACL syntax for id-namespacing policy
    status: planned
  - id: 0031-T6
    title: Adversarial tests (a different principal never resumes/takes over another's session; same principal always can; cross-node; offline-queue inheritance blocked)
    status: planned
---

# Delivery — ADR 0031: Bind the session to the authenticated identity

Decision: [docs/adr/0031-session-identity-binding.md](../adr/0031-session-identity-binding.md).

The MQTT session is keyed on the Client Identifier alone; the authenticated `principal` is
not part of the key and is not consulted on resume/takeover, so a different authenticated
identity can seize another's persistent session by reusing its id. This binds a session to
its authenticated owner. **Proposed** — the mechanism (a resume/takeover guard vs namespacing
the key) and the identity-rotation policy are open; nothing is built until ratified.

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
| 0031-T1 | ⬜ planned | — |  |
| 0031-T2 | ⬜ planned | — |  |
| 0031-T3 | ⬜ planned | — |  |
| 0031-T4 | ⬜ planned | — |  |
| 0031-T5 | ⬜ planned | — |  |
| 0031-T6 | ⬜ planned | — |  |
<!-- /status-table:0031 -->

## Changelog

- **2026-06-26** — ADR proposed and delivery opened. Surfaced from a review of session
  keying: the session (a security-relevant resource — queued data + subscriptions) is keyed
  on the Client Identifier alone, decoupled from the authenticated identity, so a takeover
  across identities is possible. Tasks `planned` pending ratification of the mechanism and
  the rotation/mismatch policy.
