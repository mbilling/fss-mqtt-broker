# Contributing as an agent (Claude Code sessions)

The git workflow for AI-agent sessions working on this repository. Human contributors:
most of this applies to you too, minus the sandbox constraints.

## Branch model

- **`main` is the integration branch.** Never push to it directly — all work lands via
  PR merge.
- Develop on the **designated `claude/*` working branch** your session was given. The
  remote git proxy only permits pushes to that branch; pushes to other branches or to
  **tags return HTTP 403** — that is org policy, not a transient error. Do not retry
  403s; surface them to the user with the exact command they should run instead.
- The maintainer commits to `main` in parallel. Before building on `main`, always
  `git fetch origin main` and check for commits you didn't make. After your PR merges,
  treat your branch as history: restart it from the new main
  (`git checkout -B <branch> origin/main`) rather than stacking on merged commits —
  a force-with-lease push is fine when the remote branch holds only merged history.

## Commits

- Prefixes as used in history: `feat(scope):`, `fix(scope):`, `style:`, `docs(...):`,
  `diag(...):`, `release(x.y.z):`, `chore:` — with the ADR/task id in the scope where
  relevant (e.g. `fix(0047-T5): …`).
- Bodies explain the **why** with evidence (CI run ids, post-mortem links, test names),
  not just the what.
- End every commit message with the `Co-Authored-By:` and `Claude-Session:` trailers
  configured for your session. Never put a model identifier in commits, PR bodies, code
  comments, or any other pushed artifact.
- Push with `git push -u origin <branch>`. Retry up to 4× with exponential backoff on
  **network** errors only — a 403 is policy, stop and report.

## Pre-commit gates (run these locally; both have bounced PRs when skipped)

```sh
cargo fmt --all -- --check
cargo clippy -p <changed-crate> --all-targets -- -D warnings   # generous timeout;
                                                               # version bumps force full recompiles
cargo test -p <changed-crate>
```

The workspace forbids `unsafe` (`unsafe_code = "forbid"` — an `#[allow]` cannot
override it; use safe wrappers such as `rustix`).

## PRs and merging

- GitHub access is **MCP-only** (`mcp__github__*` tools); there is no `gh` CLI. Load
  tool schemas via ToolSearch before calling.
- One PR per unit of work (an ADR task, or a bug fix + its regression test). No PR
  template exists; write What / Why / Test-evidence sections.
- **Never merge red.** PR CI (`ci.yml`) has five required checks: *build, test, lint*
  (fmt → clippy `-D warnings` → tests), *delivery dashboard*, *foreign-client interop*,
  *supply-chain audit* (cargo-deny + cargo-audit), *helm chart*. Note that
  `pull_request_read get_status` shows legacy statuses only — use **`get_check_runs`**.
- Merge method: **merge commit** (`merge_method: "merge"`, matching the repo's
  `Merge PR #NN` history). Merge only when the user asked for it; the usual cadence is
  per-task PR → merge → next task.

## Process docs travel with the code

- Every ADR (`docs/adr/`) has a delivery doc (`docs/delivery/`) whose frontmatter tracks
  tasks. `status: done` **requires** an `evidence:` field. After editing, run
  `python3 scripts/gen-status.py` — CI gates on `--check`. Flip the matching ADR file's
  `Status:` header in the same change.
- Incidents get a post-mortem in `docs/postmortems/YYYY-MM-DD-slug.md`, cross-referenced
  from commits, ADRs, and follow-up tasks. A bug fix derived from an incident cites it.

## Releases (ADR 0045)

- A release is **only** a pushed `v*.*.*` tag on `main`; that fires
  `.github/workflows/release.yml` (audit → reproducible amd64/arm64 musl builds with a
  byte-identity rebuild check → CycloneDX SBOM → keyless cosign signing → SLSA
  provenance → GitHub Release + GHCR image). Verification steps live in `RELEASING.md`.
- A SemVer **hyphen** (`v0.9.0-rc.2`) marks the release *prerelease* and skips the
  `:latest` image tag. The Cargo workspace version stays at the base version for an RC.
- **Agent sandboxes cannot push tags (403).** Hand the user the exact
  `git tag -a … && git push origin <tag>` command, then watch the run via
  `mcp__github__actions_list` / `get_job_logs` and verify per `RELEASING.md`.
- Version bumps must move together: `[workspace.package] version`, every internal
  path-dep `version = "…"` pin in `crates/*/Cargo.toml`, `Cargo.lock`
  (`cargo update --workspace --offline`), and the Helm chart `version`/`appVersion`.
  Validate with `cargo metadata --locked`.

## Operational habits

- `nightly.yml` is dispatchable (`actions_run_trigger run_workflow`) for the slow tiers
  (kube-smoke, stress sweeps, fuzzing). Nightly failures are not PR blockers, but
  investigate before writing one off as pre-existing.
- Large MCP log responses save to a file as **one giant line** — split on timestamps
  with python, then grep. Don't paste raw logs into context or replies.
- When CI is the only place a change can run (kind, cross-arch, OIDC signing), instrument
  first, then fix: land a low-noise diagnostic, read the failing run's evidence, and only
  then change behavior — cheaper than guess-and-re-kick loops.
