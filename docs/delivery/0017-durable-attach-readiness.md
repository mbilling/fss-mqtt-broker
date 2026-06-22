---
adr: "0017"
title: Durable attach waits for an authoritative session, never downgrades
adr_status: Accepted
tasks:
  - id: 0017-T1
    title: Typed transient error (ReplError::{NotOwner,NoQuorum} -> StorageError::Unavailable)
    status: done
    date: 2026-06-19
    evidence: logged.rs maps ReplError::{NotOwner,NoQuorum} => StorageError::Unavailable
  - id: 0017-T2
    title: Off-loop bounded recovery (spawn recover_session -> HubCommand::SessionRecovered)
    status: done
    date: 2026-06-19
    evidence: recover_session / recover_until_ready; recovery_wait_does_not_block_the_hub_loop
  - id: 0017-T3
    title: Authoritative-or-reject attach (never present=false on transient/unknown)
    status: done
    date: 2026-06-19
    evidence: transient_lease_does_not_downgrade_a_persistent_attach; permanently_unavailable_store_rejects_rather_than_downgrades
  - id: 0017-T4
    title: Reject CONNACK (v3.1.1 0x03 / v5 0x88) on AttachOutcome::Unavailable
    status: done
    date: 2026-06-19
    evidence: conn.rs CONNACK_SERVER_UNAVAILABLE (0x03 -> 0x88) on Ok(AttachOutcome::Unavailable)
  - id: 0017-T5
    title: Last-writer-wins across the off-loop window (connecting map guard)
    status: done
    date: 2026-06-19
    evidence: connecting HashMap conn_id guard; overlapping_connects_are_last_writer_wins
  - id: 0017-T6
    title: Queue-key warming so inline replay is reliable (pending probe in recovery)
    status: done
    date: 2026-06-19
    evidence: recover_until_ready warms pending; a_queued_message_is_replayed_to_the_client_after_takeover
  - id: 0017-T7
    title: Clean-start discard off-loop (SessionRecovery::Cleaned, best-effort)
    status: done
    date: 2026-06-19
    evidence: recover_clean / SessionRecovery::Cleaned; a_clean_session_client_connects_promptly_on_the_group_owner
  - id: 0017-T8
    title: Client-observable durable-failover integration proof (re-added)
    status: done
    date: 2026-06-19
    evidence: a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover
  - id: 0017-T9
    title: Make recovery deadline/backoff configurable (currently constants)
    status: deferred
    notes: ATTACH_RECOVERY_TIMEOUT/BACKOFF are constants for now; ADR defers promoting them to config until an operator need appears
---

# Delivery — ADR 0017: Durable attach waits for an authoritative session, never downgrades

Decision: [docs/adr/0017-durable-attach-readiness.md](../adr/0017-durable-attach-readiness.md).

## Plan

The decision's four numbered parts plus the 2026-06-19 update decompose into these tasks.
Each carries a stable id used by commits, tests, and the dashboard.

| Task | Acceptance criterion |
|------|----------------------|
| **0017-T1** Typed transient error | A new `StorageError::Unavailable`; the cluster store maps `ReplError::{NotOwner,NoQuorum}` to it while `Backend`/`NotFound` stay terminal, so attach can classify transient vs terminal without string-matching. |
| **0017-T2** Off-loop recovery | A persistent attach spawns a recovery task holding a cloned `Arc<dyn SessionStore>` that does the bounded retry (`ensure_session`/`subscriptions`/`pending` probe) off the hub loop, then posts `HubCommand::SessionRecovered`; the loop keeps serving other commands meanwhile. |
| **0017-T3** Authoritative-or-reject | Attach never reports `session_present=false` from a transient/unknown error: a transient-then-ready store yields `Present(true)`; a permanently-unavailable store yields a reject, not a downgrade. |
| **0017-T4** Reject CONNACK | An `Unavailable` recovery rejects the CONNECT with *Server unavailable* (v3.1.1 `0x03`, v5 `0x88`) and closes, so the client retries with its durable session intact. |
| **0017-T5** Last-writer-wins | The hub tracks `connecting[client]=conn_id`; a `SessionRecovered` proceeds only if that entry still names its `conn_id`, so an overlapping takeover connect supersedes an in-flight recovery. |
| **0017-T6** Queue-key warming | Recovery also probes `pending`, warming the offline-queue key so the inline replay in `finish_attach` reliably delivers queued messages on the resuming connect. |
| **0017-T7** Clean-start off-loop | The clean-start in-memory wipe stays on the loop (fast); the durable `remove` runs off-loop and posts `SessionRecovered::Cleaned`, gating the CONNACK without freezing the hub — best-effort (never rejects a clean connect). |
| **0017-T8** Failover proof | The re-added client-observable durable-failover integration test passes: a persistent client reconnecting on the new owner after takeover resumes (`session_present=true`), never silently reset. |
| **0017-T9** Configurable wait | The recovery deadline/backoff become operator config rather than compile-time constants. |

## Progress

<!-- status-table:0017 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0017-T1 | ✅ done | 2026-06-19 | logged.rs maps ReplError::{NotOwner,NoQuorum} => StorageError::Unavailable |
| 0017-T2 | ✅ done | 2026-06-19 | recover_session / recover_until_ready; recovery_wait_does_not_block_the_hub_loop |
| 0017-T3 | ✅ done | 2026-06-19 | transient_lease_does_not_downgrade_a_persistent_attach; permanently_unavailable_store_rejects_rather_than_downgrades |
| 0017-T4 | ✅ done | 2026-06-19 | conn.rs CONNACK_SERVER_UNAVAILABLE (0x03 -> 0x88) on Ok(AttachOutcome::Unavailable) |
| 0017-T5 | ✅ done | 2026-06-19 | connecting HashMap conn_id guard; overlapping_connects_are_last_writer_wins |
| 0017-T6 | ✅ done | 2026-06-19 | recover_until_ready warms pending; a_queued_message_is_replayed_to_the_client_after_takeover |
| 0017-T7 | ✅ done | 2026-06-19 | recover_clean / SessionRecovery::Cleaned; a_clean_session_client_connects_promptly_on_the_group_owner |
| 0017-T8 | ✅ done | 2026-06-19 | a_persistent_client_resumes_its_session_on_the_new_owner_after_takeover |
| 0017-T9 | 💤 deferred | — | ATTACH_RECOVERY_TIMEOUT/BACKOFF are constants for now; ADR defers promoting them to config until an operator need appears |
<!-- /status-table:0017 -->

## Changelog

- **2026-06-19** — Update follow-ups landed: T6 queue-key warming (off-loop `pending` probe)
  and T7 clean-start discard moved off-loop (`SessionRecovery::Cleaned`, CONNACK still gated,
  best-effort). T9 (make the wait configurable) split out and deferred — the deadline/backoff
  stay constants.
- **2026-06-19** — Core landed: T1 typed `StorageError::Unavailable` mapping, T2 off-loop
  `recover_session`/`SessionRecovered`, T3 authoritative-or-reject attach, T4 *Server
  unavailable* CONNACK, T5 last-writer-wins `connecting` guard, and T8 the re-added
  client-observable failover integration proof.
