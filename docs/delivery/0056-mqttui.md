---
adr: "0056"
title: "mqttui: a terminal UI for running the demo, migration and test scripts"
adr_status: Proposed
tasks:
  - id: 0056-T1
    title: The manifest + headless runner + the CI completeness guard — tasks.toml declaring every runnable script with its prerequisites and env surface, `mqttui --list` / `mqttui --run <id>`, and a test that fails when a script is missing from it
    status: done
    date: 2026-08-10
    evidence: "tools/mqttui/ — its OWN workspace with its OWN lockfile (18 packages against the broker's 429; `cargo metadata` on the root shows 11 members with mqttui absent, and the broker's Cargo.lock is untouched). tasks.toml declares all 23 scripts under scripts/, demo/ and bench/ — 19 offered, 4 CI plumbing marked hidden — each with about/requires/optional/duration/caution/env. `--list`, `--show <id>`, `--run <id>`, `--check`. Preflight blocks a run whose REQUIRED tools are missing and names them; absent OPTIONAL tools are reported and the run proceeds. 11 tests. CI job `mqttui (separate workspace)` runs fmt, clippy -D warnings, the tests, and `--check`."
    notes: "The completeness guard is proven in BOTH directions, not just asserted: dropping an undeclared scripts/mqttui-guard-probe.sh made `--check` exit 1 and the test fail by name; removing it made both pass. A companion test asserts the walk finds >=15 scripts, so the guard cannot pass vacuously. FOUND WHILE BUILDING IT: the first walk required the executable bit, which made scripts/gen-status.py and scripts/gen-bridge-dashboard.py (mode 644, invoked as `python3 <file>`) invisible — a completeness guard with a blind spot, and a new script landing without chmod +x would have slipped past in silence. The walk now takes any .sh/.py; the count went 21 -> 23."
  - id: 0056-T2
    title: The terminal UI — collapsible group/task tree, detail pane that becomes the output pane while running, cancel by process group
    status: done
    date: 2026-08-10
    evidence: "tools/mqttui/src/ui.rs + runner.rs. Two panes: a collapsible group tree left, and a right pane that is Detail while browsing and Output while running. One run at a time, enforced — starting a second says so instead of racing. Output is pumped from stdout AND stderr into one ordered buffer bounded at 10k lines, with the full stream always written to target/mqttui-logs/<id>-<pid>.log and its path shown on completion. Follow-mode auto-scrolls until the user scrolls, then stops. A finished run leads with its verdict; a FAILED run jumps to the first FAIL/FATAL/ERROR/panicked line rather than the tail. Cancel signals the process GROUP (setpgid on the child, SIGINT to -pgid) so each script's own trap EXIT runs."
    notes: "Two defects found by USING it rather than reading it: `mqttui --list | head` PANICKED on a broken pipe, because Rust ignores SIGPIPE — an ordinary pipeline turning into a panic. The default handler is now restored, as every other CLI does. And starting the UI without a tty failed with `Device not configured (os error 6)` from inside crossterm; it now detects the missing terminal, names the headless commands, and exits 2."
  - id: 0056-T3
    title: Preflight + inline env editing
    status: done
    date: 2026-08-10
    evidence: "The detail pane carries the preflight block (required tools with present/absent, optional tools shown separately so an absent one reads as 'that part will be skipped'), the declared environment with defaults and help, and any caution string. A task whose REQUIRED tools are missing shows `!` in the tree, is drawn dimmed, and refuses to start with the missing tools named. Env editing is inline in the same pane (`e`), never a modal, so the description stays visible while editing. src/env.rs resolves manifest defaults + overrides in ONE place used by both the UI and the runner."
    notes: "An empty value leaves the variable UNSET rather than set-to-empty, because several scripts branch on an empty MQTTD_BIN to decide whether to build first — and the UI shows `(unset)`, which is what actually happens. Tested: defaults apply, overrides win, an emptied override unsets again, and the displayed value is proven identical to what would be exported."
  - id: 0056-T5
    title: Environment panel — docker, kube context, kind clusters, compose stacks, stray processes, ports; with explicit cleanup actions
    status: done
    date: 2026-08-10
    evidence: "src/teardown.rs — Environment::probe() reports Docker, the current kube context, kind clusters, compose stacks and stray broker processes, reachable with `E`. Each probe is bounded at 3s so an unreachable context reports rather than freezing, and `docker compose ls` (measured at 1.9s) runs only on entering the pane, never on a timer. The pane warns when the current kube context is NOT a kind- one, because kind-smoke and operator-e2e target whatever is current."
    notes: "Probe has THREE states, not two: Value, None, and Unavailable — 'we could not ask' and 'there are none' are different facts, and a test asserts they never render alike. Conflating them is how a dashboard starts asserting the absence of things it never checked. Killing strays is bound to `k` and is never automatic."
  - id: 0056-T6
    title: Cancel and quit tear down what they started — and VERIFY, reporting what survived
    status: done
    date: 2026-08-10
    evidence: "Cancel and quit signal the process GROUP and WAIT for it (20s) rather than detaching, then teardown::report() verifies and states what survived — stray brokers by pid, kind clusters by name — with the command to remove them. A test asserts the report always says something about strays either way, because silence would read as 'nothing leaked', which is precisely the claim this cannot make."
    notes: "The guarantee is signal-correctly-and-tell-you, not no-orphans: mqttui cannot make another program's trap EXIT correct. Stated in the module docs at the point somebody would otherwise assume more."
  - id: 0056-T4
    title: Decide developer-tool vs user-facing, and record it
    status: done
    date: 2026-08-10
    evidence: "Answered by the ADR 0056 amendment of 2026-08-10: BOTH, as one binary that detects whether it is inside a checkout — standalone it offers the embedded set and states how many tasks are hidden and why; in a checkout it offers everything. Decision 1 (separate workspace, own lockfile) survived the change and now separates two PUBLISHED artifacts rather than one published and one internal, so ratatui still never enters the broker's dependency graph."
    notes: "The original record predicted that building the developer tool while imagining the user-facing one would erode the dependency boundary. It did not, because the boundary was written down first — kept on the record as evidence that writing the constraint down is what made it hold."
  - id: 0056-T7
    title: Embed the example surface — demo, Helm chart, CRDs, compose deployment, migration, k8s scripts — and detect checkout vs standalone
    status: planned
    notes: "MEASURED: the whole surface is 161 KB compressed (demo/ deploy/helm/ deploy/compose/ deploy/crds/ scripts/migrate/ scripts/k8s/), so include_dir! costs nothing worth discussing. Embedding buys offline operation, version-locking to the binary that was tested with it, and — the load-bearing property — executing nothing that arrived over the network. Standalone mode must state how many tasks are hidden and why; a tool that silently showed a subset would be the same defect as a manifest that silently went stale (ADR 0056 §3). Four tasks (release/build-repro.sh, k8s/render-parity.sh, gen-status.py, check-readme-facts.py) operate ON the repository and can never be freed from a checkout — the ADR says so rather than letting it be discovered."
  - id: 0056-T8
    title: "`mqttui update` — fetch the examples bundle from a SIGNED release, verified before unpacking; CI publishes a rolling bundle on merges to main"
    status: planned
    notes: "This is the answer to 'can it fetch the latest from main': yes, by way of a signed artifact built from main, not by trusting the branch. Fetching a branch tarball at runtime was REJECTED as a default — release binaries are cosign-signed with SLSA provenance and an SBOM, builds are reproducible, and every dependency is audited on every push (ADR 0045/0053); downloading shell from a mutable branch and running it discards all of that with one command, and repeats it on every launch. An explicit `--channel main` remains available for maintainers testing unreleased examples: loudly marked unverified, never a default."
  - id: 0056-T9
    title: Distribution — signed musl binaries through the ADR 0045 pipeline (primary) plus crates.io (`cargo install mqttui`)
    status: planned
    notes: "cargo install requires a Rust toolchain and compiles ~20 crates on the user's machine; the audience is somebody evaluating an MQTT broker who may have neither. The signed-binary pipeline already exists and is extended to a second artifact. Makes mqttui a published product with its own version, changelog, semver, signing and SBOM obligations — accepted knowingly. The name `mqttui` was verified free on crates.io on 2026-08-10 (HTTP 404 from the registry API, not inferred)."
  - id: 0056-T10
    title: "`mqttui migrate mosquitto` — the converter reimplemented in Rust, with a differential test against the Python original"
    status: done
    date: 2026-08-10
    evidence: "tools/mqttui/src/migrate.rs — `mqttui migrate mosquitto <conf> [--out-config P] [--out-acl P]`, needing neither python3 nor a checkout: it reads the USER's mosquitto.conf. Runs BEFORE the checkout check in main, since it depends on nothing of ours. 8 unit tests over the semantics that silently change a policy if got wrong (a missing access word means readwrite; %u becomes %i; %c is carried but flagged as fail-closed; user blocks are positional; unmapped settings become visible TODOs; TLS material binds to the listener it follows). Plus tests/differential.rs: both converters over shared fixtures, compared BYTE FOR BYTE, wired into the mqttui CI job where python3 is present."
    notes: "The differential test is the acceptance criterion ADR 0056 set, and it was verified to FAIL rather than assumed to work: injecting a plausible bug — treating a missing access word as read-only instead of readwrite — made it fail on exactly the affected rule (`denied/topic` narrowed from publish+subscribe to subscribe), which is a silent privilege change no eyeball would catch. It also asserts both sides produced real output, so equality cannot pass vacuously. One honesty fix on the way: the generated header credited `scripts/migrate/from-mosquitto.py` even when mqttui produced it — a script the user may not have. BOTH sides now say `the mqttd Mosquitto converter`, keeping byte-identity and making the attribution true either way."
---

# Delivery — ADR 0056: `mqttui`, a terminal UI for the repository's scripts

Decision: [docs/adr/0056-mqttui.md](../adr/0056-mqttui.md).

Origin: proposed 2026-08-10, tracked as issue
[#138](https://github.com/mbilling/fss-mqtt-broker/issues/138). Not release-blocking —
[ADR 0051](../adr/0051-evaluation-readiness.md)'s items come first.

## Plan

| Task | Acceptance criterion |
|------|----------------------|
| **0056-T1** Manifest + headless | `tasks.toml` declares every runnable script under `scripts/`, `demo/` and `bench/` with its group, description, required and optional tools, and env surface. `mqttui --list` and `mqttui --run <id>` work with no UI. A CI test fails if any executable is missing from the manifest, `hidden = true` marking CI plumbing that should not be offered. |
| **0056-T2** Terminal UI | Groups and tasks on the left, detail on the right, streamed output below. A cancelled task leaves no stray broker, container or temporary directory behind — signalled by process group. |
| **0056-T3** Preflight + env | Missing prerequisites are named *before* a run, not discovered by it. Env vars are editable, with the manifest's defaults and help; last-used values persist. |
| **0056-T5** Environment panel | Docker, the current kube context, `kind` clusters, compose stacks, stray broker processes and the relevant ports are on screen before a task is started. Cleanup actions exist, name what they will remove, and are never automatic. |
| **0056-T6** Teardown | Cancel and quit signal the process **group** and wait for it, then verify and report what survived. The report is the deliverable — not a claim that nothing survives. |
| **0056-T4** Scope decision | The developer-tool / user-facing question is answered in writing, in this record or a new ADR — not left implicit. |
| **0056-T7** Embedded examples | A freshly installed `mqttui`, with no checkout anywhere, can run the demo, the Kubernetes examples and the compose deployment. It states how many tasks are hidden and why. |
| **0056-T8** Signed updates | `mqttui update` reaches examples newer than the binary, verified before use. Nothing unverified is ever executed by default. |
| **0056-T9** Distribution | One line installs it, without a Rust toolchain. The crates.io path exists for those who have one. |
| **0056-T10** Built-in migration | `mqttui migrate mosquitto <conf>` works on a machine with neither Python nor a clone, and provably agrees with the Python converter. |

Order: T1 → T2 → T5/T6 → T3, then T7 → T9 → T8, with T10 landable at any point. T1 stands
alone and is where the value is; T6 is a prerequisite for trusting T2's cancel key; T7 is
what makes T9 worth doing (a published binary with no examples is a published binary with
nothing to run).

## Progress

<!-- status-table:0056 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0056-T1 | ✅ done | 2026-08-10 | "tools/mqttui/ — its OWN workspace with its OWN lockfile (18 packages against the broker's 429; `cargo metadata` on the root shows 11 members with mqttui absent, and the broker's Cargo.lock is untouched). tasks.toml declares all 23 scripts under scripts/, demo/ and bench/ — 19 offered, 4 CI plumbing marked hidden — each with about/requires/optional/duration/caution/env. `--list`, `--show <id>`, `--run <id>`, `--check`. Preflight blocks a run whose REQUIRED tools are missing and names them; absent OPTIONAL tools are reported and the run proceeds. 11 tests. CI job `mqttui (separate workspace)` runs fmt, clippy -D warnings, the tests, and `--check`." |
| 0056-T2 | ✅ done | 2026-08-10 | "tools/mqttui/src/ui.rs + runner.rs. Two panes: a collapsible group tree left, and a right pane that is Detail while browsing and Output while running. One run at a time, enforced — starting a second says so instead of racing. Output is pumped from stdout AND stderr into one ordered buffer bounded at 10k lines, with the full stream always written to target/mqttui-logs/<id>-<pid>.log and its path shown on completion. Follow-mode auto-scrolls until the user scrolls, then stops. A finished run leads with its verdict; a FAILED run jumps to the first FAIL/FATAL/ERROR/panicked line rather than the tail. Cancel signals the process GROUP (setpgid on the child, SIGINT to -pgid) so each script's own trap EXIT runs." |
| 0056-T3 | ✅ done | 2026-08-10 | "The detail pane carries the preflight block (required tools with present/absent, optional tools shown separately so an absent one reads as 'that part will be skipped'), the declared environment with defaults and help, and any caution string. A task whose REQUIRED tools are missing shows `!` in the tree, is drawn dimmed, and refuses to start with the missing tools named. Env editing is inline in the same pane (`e`), never a modal, so the description stays visible while editing. src/env.rs resolves manifest defaults + overrides in ONE place used by both the UI and the runner." |
| 0056-T5 | ✅ done | 2026-08-10 | "src/teardown.rs — Environment::probe() reports Docker, the current kube context, kind clusters, compose stacks and stray broker processes, reachable with `E`. Each probe is bounded at 3s so an unreachable context reports rather than freezing, and `docker compose ls` (measured at 1.9s) runs only on entering the pane, never on a timer. The pane warns when the current kube context is NOT a kind- one, because kind-smoke and operator-e2e target whatever is current." |
| 0056-T6 | ✅ done | 2026-08-10 | "Cancel and quit signal the process GROUP and WAIT for it (20s) rather than detaching, then teardown::report() verifies and states what survived — stray brokers by pid, kind clusters by name — with the command to remove them. A test asserts the report always says something about strays either way, because silence would read as 'nothing leaked', which is precisely the claim this cannot make." |
| 0056-T4 | ✅ done | 2026-08-10 | "Answered by the ADR 0056 amendment of 2026-08-10: BOTH, as one binary that detects whether it is inside a checkout — standalone it offers the embedded set and states how many tasks are hidden and why; in a checkout it offers everything. Decision 1 (separate workspace, own lockfile) survived the change and now separates two PUBLISHED artifacts rather than one published and one internal, so ratatui still never enters the broker's dependency graph." |
| 0056-T7 | ⬜ planned | — | "MEASURED: the whole surface is 161 KB compressed (demo/ deploy/helm/ deploy/compose/ deploy/crds/ scripts/migrate/ scripts/k8s/), so include_dir! costs nothing worth discussing. Embedding buys offline operation, version-locking to the binary that was tested with it, and — the load-bearing property — executing nothing that arrived over the network. Standalone mode must state how many tasks are hidden and why; a tool that silently showed a subset would be the same defect as a manifest that silently went stale (ADR 0056 §3). Four tasks (release/build-repro.sh, k8s/render-parity.sh, gen-status.py, check-readme-facts.py) operate ON the repository and can never be freed from a checkout — the ADR says so rather than letting it be discovered." |
| 0056-T8 | ⬜ planned | — | "This is the answer to 'can it fetch the latest from main': yes, by way of a signed artifact built from main, not by trusting the branch. Fetching a branch tarball at runtime was REJECTED as a default — release binaries are cosign-signed with SLSA provenance and an SBOM, builds are reproducible, and every dependency is audited on every push (ADR 0045/0053); downloading shell from a mutable branch and running it discards all of that with one command, and repeats it on every launch. An explicit `--channel main` remains available for maintainers testing unreleased examples: loudly marked unverified, never a default." |
| 0056-T9 | ⬜ planned | — | "cargo install requires a Rust toolchain and compiles ~20 crates on the user's machine; the audience is somebody evaluating an MQTT broker who may have neither. The signed-binary pipeline already exists and is extended to a second artifact. Makes mqttui a published product with its own version, changelog, semver, signing and SBOM obligations — accepted knowingly. The name `mqttui` was verified free on crates.io on 2026-08-10 (HTTP 404 from the registry API, not inferred)." |
| 0056-T10 | ✅ done | 2026-08-10 | "tools/mqttui/src/migrate.rs — `mqttui migrate mosquitto <conf> [--out-config P] [--out-acl P]`, needing neither python3 nor a checkout: it reads the USER's mosquitto.conf. Runs BEFORE the checkout check in main, since it depends on nothing of ours. 8 unit tests over the semantics that silently change a policy if got wrong (a missing access word means readwrite; %u becomes %i; %c is carried but flagged as fail-closed; user blocks are positional; unmapped settings become visible TODOs; TLS material binds to the listener it follows). Plus tests/differential.rs: both converters over shared fixtures, compared BYTE FOR BYTE, wired into the mqttui CI job where python3 is present." |
<!-- /status-table:0056 -->

## Changelog

- **2026-08-10** — **T10 done: the Mosquitto converter is built in.** `mqttui migrate
  mosquitto <conf>` needs neither Python nor a checkout — it reads the *user's* config, not
  ours, which is what makes it the one task more useful standalone. Three of the five
  reviewers in the 2026-08-09 panel named missing migration tooling their single largest
  blocker.

  The Python script stays, because CI already proves *its* output boots the real broker
  (0051-T6), and the two are held together by a **byte-for-byte differential test** over
  shared fixtures. That test was verified to fail rather than assumed to work: injecting a
  plausible bug — a missing access word read as `read` instead of `readwrite` — made it fail
  on exactly the affected rule, a silent privilege narrowing no eyeball would catch.

  One honesty fix along the way. The generated header credited
  `scripts/migrate/from-mosquitto.py` even when `mqttui` produced it — a script the user may
  not have. Both sides now say *the mqttd Mosquitto converter*, which keeps byte-identity
  and is true whichever produced it.
- **2026-08-10** — **T2, T3, T5 and T6 done: the terminal UI.** Two panes as specified, a
  collapsible group tree, one run at a time, follow-mode that stops when you scroll, and a
  finished run that leads with its verdict — jumping to the first failing line rather than
  the tail when it failed. The environment pane (`E`) reports Docker, the current kube
  context, kind clusters and stray brokers; cancel and quit signal the process group, wait,
  and then report what survived.

  Two defects came from *using* it rather than reading it, and both were ordinary usage:
  `mqttui --list | head` **panicked** on a broken pipe, because Rust ignores `SIGPIPE` — an
  everyday pipeline turned into a panic. And launching the UI without a terminal failed
  with `Device not configured (os error 6)` from inside crossterm. It now restores the
  default `SIGPIPE` handler like every other CLI, and detects a missing tty to name the
  headless commands and exit 2.

  One design detail worth recording: `Probe` has three states — `Value`, `None`,
  `Unavailable`. "We could not ask" and "there are none" are different facts, and a test
  asserts they never render alike. Collapsing them is how a status pane starts asserting
  the absence of things it never checked.
- **2026-08-10** — **T1 done: the manifest, the headless runner, and the guard.** `mqttui`
  builds from its own workspace with its own lockfile — 18 packages against the broker's
  429, and the broker's `Cargo.lock` is untouched, which is the whole point of ADR 0056 §1.
  All 23 scripts are declared; `--list`, `--show`, `--run` and `--check` work; preflight
  blocks a run whose required tools are missing and names them.

  The guard is proven in both directions rather than asserted: an undeclared script makes
  `--check` exit 1 and fails the test by name, and removing it makes both pass. A companion
  test asserts the walk finds at least fifteen scripts, so it cannot pass vacuously.

  Building it turned up a hole in the guard itself. The first walk required the executable
  bit — which made `gen-status.py` and `gen-bridge-dashboard.py` (mode 644, run as
  `python3 <file>`) invisible to it. A completeness guard with a blind spot is not a
  completeness guard; a new script landing without `chmod +x` would have slipped past in
  silence. Fixed, and the count went 21 → 23.
- **2026-08-10** — **`mqttui` becomes an installed product, not a checkout tool** (ADR
  amendment; closes T4, adds T7–T10). A one-line install must give somebody the demo, the
  Kubernetes examples and the migration converter without cloning anything — which the
  original scope could not, because the scripts *are* the product and `cargo install`
  produces no checkout.

  The example surface measured **161 KB compressed**, so it is embedded rather than
  fetched. That buys offline operation, version-locking to the binary that was tested with
  it, and — the property that matters — executing nothing that arrived over the network.

  On fetching the latest from `main`: **yes, but as a signed release artifact built from
  main, not as a branch tarball.** Release binaries here are cosign-signed with SLSA
  provenance and an SBOM, builds are reproducible, and every dependency is audited on every
  push. A tool that downloads shell from a mutable branch and runs it discards all of that
  with one command — and repeats it every launch. `--channel main` stays available for
  maintainers, explicit and loudly unverified.

  Recorded honestly: **"everything possible" is not everything.** Four tasks operate *on*
  the repository (`build-repro.sh`, `render-parity.sh`, `gen-status.py`,
  `check-readme-facts.py`) and cannot be freed from a checkout by any amount of embedding.
  `mqttui` detects which mode it is in and says how many tasks are hidden and why, rather
  than silently showing a subset.
- **2026-08-10** — **Layout and behaviour settled** after a mockup review, and two tasks
  added. Two panes rather than three (at 80×24 a third leaves ~6 lines of output, useless
  for a bench run); a collapsible group tree rather than a flat filtered list, because the
  set is expected to grow; **one run at a time**, enforced, since these scripts bind fixed
  ports and `bench/run.sh` requires an otherwise-idle host; no persisted run history, which
  would be state that lies after a `git pull`.

  **T5, the environment panel**, came out of the review and justified itself immediately:
  probing this machine while designing it found **twenty** orphaned `mqttd` processes from
  one day's test runs, none visible until something asked. The Kubernetes context display
  is the safety feature — `kind-smoke.sh` and `operator-e2e.sh` target whatever context is
  current.

  **T6 fixes the shape of the teardown claim.** The ask was "quitting should leave no
  orphans". What is deliverable is: signal the process group, wait for it, then verify and
  report what survived. `mqttui` cannot make another program's `trap EXIT` correct, and
  "leaves no orphans" is a claim that cannot be checked and would quietly stop being true —
  the same shape as the compose health check that could not fail (ADR 0047 T9). Reporting
  what leaked is both honest and more useful.
- **2026-08-10** — ADR drafted, `Proposed`. Recorded because the *decisions* here are
  hard to reverse — the separate workspace and its own lockfile (so a developer
  convenience can never fail the broker's supply-chain gate or enter its SBOM), the
  declare-don't-discover manifest, the CI completeness guard, and above all whether this
  is a developer tool or something that ships. The proposal had previously lived only in a
  conversation and then only in issue #138, which is where this project keeps defects and
  tracking plans, not decisions.
