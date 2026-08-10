---
adr: "0056"
title: "mqttui: a task runner for the repository's scripts, outside the broker's dependency graph"
adr_status: Proposed
tasks:
  - id: 0056-T1
    title: The manifest + headless runner + the CI completeness guard — tasks.toml declaring every runnable script with its prerequisites and env surface, `mqttui --list` / `mqttui --run <id>`, and a test that fails when a script is missing from it
    status: planned
    notes: "Deliberately first, and useful with no UI at all: it proves the data model, and the manifest is machine-checked documentation of the operational surface on its own. The completeness guard is the load-bearing piece (ADR 0056 §3) — a launcher that silently shows 14 of 23 scripts becomes the list people trust. If phase 1 turns out to be sufficient, a justfile over the manifest is a legitimate place to stop; the manifest is where the value is, not the TUI."
  - id: 0056-T2
    title: The terminal UI — group/task list, detail pane, run with streamed output, cancel by process group
    status: planned
    notes: "Cancellation signals the process GROUP, not the child (ADR 0056 §4): every script traps EXIT to clean up brokers and containers, and signalling only the wrapper orphans them. Not hypothetical — stray brokers from panicking tests actively poisoned later runs while issue #124's regression test was being written. Output goes to a bounded ring buffer with the full log on disk, so a long bench run cannot grow without limit."
  - id: 0056-T3
    title: Preflight + env form + persisted last-used values
    status: planned
    notes: "The preflight row is the highest-value part of the UI: it turns 'run it and find out' into 'you are missing mosquitto-clients'. Most of these scripts currently fail with a bare FATAL after the user has already committed to the run."
  - id: 0056-T4
    title: Decide developer-tool vs user-facing, and record it
    status: planned
    notes: "OPEN QUESTION, not a build task. It changes ADR 0056 §1: a user-facing launcher ships in the release, which puts ratatui back into the audited dependency graph and makes the separate workspace pointless. This record covers the developer tool only; a user-facing launcher earns its own ADR. Listed as a task so the question is closed deliberately rather than drifted past."
---

# Delivery — ADR 0056: `mqttui`, a task runner for the repository's scripts

Decision: [docs/adr/0056-repo-task-runner.md](../adr/0056-repo-task-runner.md).

Origin: proposed 2026-08-10, tracked as issue
[#138](https://github.com/mbilling/fss-mqtt-broker/issues/138). Not release-blocking —
[ADR 0051](../adr/0051-evaluation-readiness.md)'s items come first.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0056-T1** Manifest + headless | `tasks.toml` declares every runnable script under `scripts/`, `demo/` and `bench/` with its group, description, required and optional tools, and env surface. `mqttui --list` and `mqttui --run <id>` work with no UI. A CI test fails if any executable is missing from the manifest, `hidden = true` marking CI plumbing that should not be offered. |
| **0056-T2** Terminal UI | Groups and tasks on the left, detail on the right, streamed output below. A cancelled task leaves no stray broker, container or temporary directory behind — signalled by process group. |
| **0056-T3** Preflight + env | Missing prerequisites are named *before* a run, not discovered by it. Env vars are editable, with the manifest's defaults and help; last-used values persist. |
| **0056-T4** Scope decision | The developer-tool / user-facing question is answered in writing, in this record or a new ADR — not left implicit. |

Order: T1 → (T4 informs T2) → T2 → T3. T1 stands alone and is where the value is.

## Progress

<!-- status-table:0056 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0056-T1 | ⬜ planned | — | "Deliberately first, and useful with no UI at all: it proves the data model, and the manifest is machine-checked documentation of the operational surface on its own. The completeness guard is the load-bearing piece (ADR 0056 §3) — a launcher that silently shows 14 of 23 scripts becomes the list people trust. If phase 1 turns out to be sufficient, a justfile over the manifest is a legitimate place to stop; the manifest is where the value is, not the TUI." |
| 0056-T2 | ⬜ planned | — | "Cancellation signals the process GROUP, not the child (ADR 0056 §4): every script traps EXIT to clean up brokers and containers, and signalling only the wrapper orphans them. Not hypothetical — stray brokers from panicking tests actively poisoned later runs while issue #124's regression test was being written. Output goes to a bounded ring buffer with the full log on disk, so a long bench run cannot grow without limit." |
| 0056-T3 | ⬜ planned | — | "The preflight row is the highest-value part of the UI: it turns 'run it and find out' into 'you are missing mosquitto-clients'. Most of these scripts currently fail with a bare FATAL after the user has already committed to the run." |
| 0056-T4 | ⬜ planned | — | "OPEN QUESTION, not a build task. It changes ADR 0056 §1: a user-facing launcher ships in the release, which puts ratatui back into the audited dependency graph and makes the separate workspace pointless. This record covers the developer tool only; a user-facing launcher earns its own ADR. Listed as a task so the question is closed deliberately rather than drifted past." |
<!-- /status-table:0056 -->

## Changelog

- **2026-08-10** — ADR drafted, `Proposed`. Recorded because the *decisions* here are
  hard to reverse — the separate workspace and its own lockfile (so a developer
  convenience can never fail the broker's supply-chain gate or enter its SBOM), the
  declare-don't-discover manifest, the CI completeness guard, and above all whether this
  is a developer tool or something that ships. The proposal had previously lived only in a
  conversation and then only in issue #138, which is where this project keeps defects and
  tracking plans, not decisions.
