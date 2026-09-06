# Delivery tracking

This directory tracks **how** decisions get built and **where each one stands**.
It is deliberately separate from `docs/adr/`, which records **why** a decision was
made. Keeping the two apart is the whole point: a decision is settled and frozen,
while a plan and its progress churn constantly. Mixing them is what made the old
single-file ADRs impossible to scan.

## The three artifacts

1. **Decision record** — `docs/adr/NNNN-title.md`
   The *why*. Context, Decision, Consequences, Alternatives. Frozen once `Accepted`;
   changing the decision means a **new** ADR that supersedes it. Its `Status` is only
   the lifecycle (`Proposed | Accepted | Superseded | Deprecated`) — never
   implementation state.

2. **Delivery doc** — `docs/delivery/NNNN-title.md`
   The *how* + *where we are*. Carries the structured task list in frontmatter and
   three sections: **Plan** (what the tasks are and their acceptance), **Progress**
   (current status, generated table), and **Changelog** (append-only, dated). One
   delivery doc realizes one ADR (occasionally several — list them in `adr`).

3. **Status dashboard** — `docs/delivery/STATUS.md`
   The *overview*. A single scannable grid across every ADR and task. **Generated** —
   do not hand-edit. Run `python3 scripts/gen-status.py` after changing any delivery
   doc; CI checks it is up to date.

## Controlled vocabulary

ADR lifecycle (`adr_status`): `Proposed`, `Accepted`, `Superseded`, `Deprecated`.

Task status (`status`): one of —

| status        | meaning                                                        |
|---------------|----------------------------------------------------------------|
| `planned`     | agreed, not started                                            |
| `in-progress` | actively being built                                           |
| `blocked`     | cannot proceed; `notes` says on what                           |
| `done`        | built **and** verified; `evidence` names the test/commit       |
| `deferred`    | intentionally postponed; `notes` says why and what unblocks it |
| `cut`         | decided against; kept for the record, not coming back          |

Only `done` requires `evidence`. `blocked`/`deferred` require `notes`.

**Every open task requires `issue`.** `planned`, `in-progress` and `blocked` are
the statuses that mean *this still needs doing*, and work that needs doing is
owned by a GitHub issue — the delivery record reflects that state, it is not a
second plan running beside it. `gen-status.py` fails if an open task has no
issue, so the two cannot drift. `deferred` and `cut` need no issue (they are
explicitly not work in progress); `done` keeps whatever issue it had, next to
its `evidence`.

## Frontmatter schema

The delivery doc begins with a YAML block. Keep it to this constrained shape — the
generator parses a deliberate subset (flat scalars + a `tasks` list), so do not nest
beyond what is shown.

```yaml
---
adr: "0019"                 # ADR number(s) this realizes; comma-separated if several
title: Graceful shutdown and connection draining
adr_status: Accepted
tasks:
  - id: 0019-T1             # stable id: <ADR>-T<n> (task) or <ADR>-P<n> (phase)
    title: SIGTERM/SIGINT handling + second-signal escalation
    status: done
    date: 2026-06-21        # YYYY-MM-DD; when it reached its current status
    evidence: graceful_shutdown_drains_an_established_connection
    notes: optional one-liner
  - id: 0019-T2
    title: Drain deadline surfaced to the operator
    status: planned
    issue: 544              # REQUIRED while open. Bare number: no '#', no URL.
---
```

**Task ids are stable and permanent.** Commits, tests, and the dashboard all
reference them, so never renumber — append new ids, mark obsolete ones `cut`.

## Workflow

- Adding work to a decision → **open a GitHub issue first**, then add the task to
  its delivery doc's frontmatter (`planned` + `issue:`). The issue is where the
  work is discussed and closed; the task is where its status is recorded.
- Starting it → `in-progress`; finishing it → `done` + `evidence` + `date`. Keep
  the `issue:` line: a done task's issue is its audit trail.
- Closing an issue → set its task `done` with `evidence` in the same change, so
  the dashboard never claims planned work that has already shipped.
- Regenerate the dashboard: `python3 scripts/gen-status.py`.
- The ADR body does not change — if the *decision* changed, write a new ADR. An
  ADR's **Delivery** footer summarises task state in prose, so it goes stale the
  moment tasks move; re-read it whenever you change a task's status.
