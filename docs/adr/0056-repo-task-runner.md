# ADR 0056 — `mqttui`: a task runner for the repository's scripts, outside the broker's dependency graph

- **Status:** Proposed
- **Date:** 2026-08-10
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0056-repo-task-runner.md](../delivery/0056-repo-task-runner.md) — plan, progress, and changelog
- **Related:** [ADR 0044](0044-release-readiness-assurance.md) (the assurance scripts this
  would front), [ADR 0045](0045-release-engineering-and-distribution.md) (what ships in a
  release, and therefore what must *not* enter its dependency graph),
  [ADR 0051](0051-evaluation-readiness.md) (onboarding — the reason a newcomer cannot find
  these scripts is the same reason they cannot evaluate the broker), issue #138.

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0056-repo-task-runner.md).

## Context

The repository holds **23 runnable scripts** across `scripts/`, `demo/` and `bench/`: the
demo stack, the Mosquitto migration converter, six smoke/conformance suites, the kind and
operator end-to-end runs, the comparative benchmark harness, and the reproducible release
build. They are the project's own operational surface, and they are close to unusable
without reading their source:

- **Discovery.** Nothing lists them. `demo/scale-cluster.py` and `scripts/oidc/run.sh` are
  not obviously different in kind from `scripts/gen-status.py`, which is CI plumbing nobody
  should run by hand.
- **Prerequisites.** Most need tools the host may not have — `mosquitto_pub`, `kind`,
  `docker compose`, `systemd-analyze`, `paho-mqtt`. The usual outcome is a bare
  `FATAL: 'x' not found` *after* the decision to run has been made.
- **Parameters.** Each honours a small, undocumented set of environment variables
  (`MQTTD_BIN`, `DURATION`, `KC_PORT`, `READY_TIMEOUT`, `IMAGE`). They are discoverable
  only by grep.

Two of the five reviewers in the 2026-08-09 panel ([`docs/REVIEW-PANEL.md`](../REVIEW-PANEL.md))
never found the demo stack at all.

A terminal UI over these scripts is straightforward. The decisions worth recording are
**where its dependencies live**, **how it avoids going stale**, and **who it is for** —
each of which is expensive to reverse once chosen.

## Decision

### 1. It is `mqttui`, and it lives in a separate workspace

The binary is **`mqttui`**. `tools/mqttui/` carries its **own `[workspace]` and its own
`Cargo.lock`**, excluded from the root workspace.

The name shares the broker's `mqtt` prefix so the two read as one family, and the `ui`
suffix says what it is without claiming to be a second daemon. It is a *developer* binary
and is not installed alongside the broker (see the open question below) — but if that ever
changes, the name is already the one a user would reach for.

`ratatui` + `crossterm` pull roughly twenty crates. The broker's workspace is audited by
`cargo-deny` and `cargo-audit` on every push, and [`docs/COMPARISON.md`](../COMPARISON.md)
makes public claims about the broker's dependency surface. A developer convenience must not
be able to fail the broker's supply-chain gate, appear in its SBOM, or widen what a security
reviewer must read.

A sibling workspace is still built and linted by CI. It simply never enters the graph the
release is cut from.

*Rejected:* a root-workspace member behind a feature flag. Feature flags do not remove a
crate from `Cargo.lock`, from `cargo-deny`'s view, or from the SBOM — they only remove it
from the compiled artifact. The boundary has to be the lockfile, not a cfg.

### 2. Tasks are declared, not discovered

A `tasks.toml` manifest is the single source of truth: id, group, description, script path,
required and optional tools, expected duration, and each environment variable with its
default and help text.

Discovery by directory walk cannot know a script's description, cannot tell a user-facing
script from CI plumbing, and cannot know what a script needs before it runs. The manifest
is also documentation that survives the tool.

### 3. Completeness is CI-gated

A test walks the script directories and **fails if any executable is absent from the
manifest**, with an explicit `hidden = true` for CI plumbing that should not be offered.

This is the load-bearing decision. Without it the launcher silently drifts into showing
fourteen of twenty-three scripts, and a tool that is quietly incomplete is worse than no
tool: it becomes the list people trust. That failure mode is well attested in this
repository — the README's ADR count drifted three behind before anyone noticed (fixed by
`scripts/check-readme-facts.py`, ADR 0051 T1), the delivery dashboard drifted in both
directions (`gen-status.py --check`), and a compose health check that could not fail
shipped in the reference deployment (ADR 0047 T9). The guard is the feature.

### 4. Cancellation signals the process group

Every script traps `EXIT` to clean up brokers, containers and temporary directories.
Signalling only the immediate child leaves those orphaned. The runner signals the **process
group**, so a cancelled task cleans up exactly as an interrupted terminal run does.

Not hypothetical: stray brokers accumulating from panicking tests actively poisoned later
runs while issue #124's regression test was being written, until a `Drop` guard was added.

### 5. Headless mode shares the manifest

`mqttui --list` and `mqttui --run <id>` work without the TUI, over the same declarations. This keeps the
manifest honest (it must be sufficient to *run* a task, not merely describe it) and leaves
open a later consolidation where CI invokes tasks through the manifest instead of
duplicating command lines in `ci.yml`.

## Consequences

**Positive**
- The scripts become discoverable, with their prerequisites checked *before* a run.
- The manifest documents the operational surface in one machine-checked place.
- Zero effect on the broker's dependency graph, SBOM, or supply-chain gates.

**Negative / accepted trade-offs**
- **A second workspace is a second thing to maintain** — its own lockfile, its own CI job,
  its own MSRV drift. This is the price of the dependency boundary and is paid knowingly.
- **The manifest is duplication.** A script's requirements exist in the script *and* in
  `tasks.toml`. The CI guard catches an *absent* task, not a *stale* one — a script that
  gains a dependency without a manifest update fails at preflight, correctly, but only when
  someone runs it.
- **It is a developer tool.** It does nothing for an operator running the released binary.

## Open question — developer tool or user-facing?

Recorded, not decided, because it changes the first decision.

As specified, this is a **developer tool**: it runs *this repository's* scripts and assumes
a checkout. A user-facing "watch mqttd do something" launcher is a different product — it
would ship in the release, which puts `ratatui` back into the audited graph and makes the
separate workspace pointless.

The two can coexist later (a shipped demo binary is not this tool), but building one while
imagining the other is how the dependency boundary gets quietly abandoned. **This ADR
covers the developer tool only.** A user-facing launcher earns its own record.

## Alternatives considered

- **A `justfile` / `Makefile`.** Far cheaper, no dependencies at all, and genuinely
  sufficient for *running* a task by name. Rejected as the whole answer because it cannot
  do the two things that actually matter here: report which prerequisites are missing
  before you commit to a run, and be CI-gated for completeness. Worth revisiting as the
  phase-1 implementation if the manifest alone proves enough — the manifest, not the TUI,
  is where the value is.
- **A `cargo xtask` in the root workspace.** The conventional Rust answer, and rejected for
  decision 1: `xtask` is a workspace member, so its dependencies are in the broker's
  lockfile.
- **Discovery by directory walk, no manifest.** Rejected under decision 2 — and it would
  make the CI completeness guard meaningless, since the walk and the guard would derive
  from the same source and could never disagree. A check that cannot fail.
