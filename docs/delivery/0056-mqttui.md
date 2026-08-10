---
adr: "0056"
title: "mqttui: a terminal UI for running the demo, migration and test scripts"
adr_status: Proposed
tasks:
  - id: 0056-T1
    title: The manifest + headless runner + the CI completeness guard — tasks.toml declaring every runnable script with its prerequisites and env surface, `mqttui --list` / `mqttui --run <id>`, and a test that fails when a script is missing from it
    status: planned
    notes: "Deliberately first, and useful with no UI at all: it proves the data model, and the manifest is machine-checked documentation of the operational surface on its own. The completeness guard is the load-bearing piece (ADR 0056 §3) — a launcher that silently shows 14 of 23 scripts becomes the list people trust. If phase 1 turns out to be sufficient, a justfile over the manifest is a legitimate place to stop; the manifest is where the value is, not the TUI."
  - id: 0056-T2
    title: The terminal UI — collapsible group/task tree, detail pane that becomes the output pane while running, cancel by process group
    status: planned
    notes: "LAYOUT SETTLED 2026-08-10. Two panes, not three: at 80x24 a third pane leaves ~6 lines of output, useless for a bench run — the right pane is Detail while browsing and Output while running, since you are either choosing or watching. Collapsible group tree (not a flat filtered list) because discoverability is the whole point and the set is expected to grow. Follow-mode auto-scrolls until the user scrolls up, then stops. A finished run leads with the verdict; a FAILED run jumps to the first FAIL/FATAL/error line rather than the tail. ONE RUN AT A TIME, enforced: these scripts bind fixed ports and start containers, and bench/run.sh explicitly requires an otherwise-idle host, so concurrency would produce failures that look like broker bugs."
  - id: 0056-T3
    title: Preflight + inline env editing
    status: planned
    notes: "The preflight block is the highest-value part of the UI: it turns 'run it and find out' into 'you are missing mosquitto-clients', named BEFORE the run rather than as a FATAL partway through. A task with missing required tools cannot be started at all, and the manifest carries an install hint per platform, because 'install kind' is where a newcomer stalls. Env editing is INLINE, not modal — a modal hides the description you are editing against. Manifest tasks may carry a `caution` string; bench/run.sh (pins the host, results invalid otherwise) and kind-smoke.sh genuinely need one. DECIDED 2026-08-10: no persisted last-run history — it is state that lies after a git pull, for little gain."
  - id: 0056-T5
    title: Environment panel — docker, kube context, kind clusters, compose stacks, stray processes, ports; with explicit cleanup actions
    status: planned
    notes: "Requested 2026-08-10, and justified on the spot: probing this machine while designing it found TWENTY orphaned mqttd processes from the day's test runs, invisible until asked for. The kube CONTEXT is the safety feature — kind-smoke.sh and operator-e2e.sh run kubectl against whatever is current, so showing `kube: prod-eu-west` before the user presses enter is the difference between a smoke test and an incident. Measured probe costs decide the polling: docker ps 0.01s, kind get clusters 0.06s, kubectl current-context 0.22s (2s timer, off the UI thread, bounded so an unreachable context shows `unreachable` instead of freezing); docker compose ls 1.9s (on demand only, staleness shown). Probing is read-only and unconditional; cleanup is never automatic and always confirmed with the specific processes/clusters listed — a tool that kills things you did not ask it to kill is worse than one that only shows them."
  - id: 0056-T6
    title: Cancel and quit tear down what they started — and VERIFY, reporting what survived
    status: planned
    notes: "DECIDED 2026-08-10: quitting must not leave orphans. What is achievable is stated precisely so this does not become an unkeepable claim. GUARANTEED: signal the process GROUP (so each script's own `trap EXIT` runs, which is what removes its brokers, containers and temp dirs) and WAIT for the group rather than detaching. NOT GUARANTEED: a script whose trap is buggy, or which was SIGKILLed, can still leak — mqttui cannot make another program's cleanup correct. So it signals, waits, then VERIFIES (stray processes, kind clusters, compose stacks) and reports what survived with an offer to remove it, leaving anything ignored visible in the environment panel. Claiming 'no orphans' outright would be the same unfalsifiable shape as the compose health check that could not fail (ADR 0047 T9)."
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
    status: planned
    notes: "The one task that is MORE useful standalone than in a checkout: it reads the USER's mosquitto.conf, not ours. Today it needs python3 and a clone; built in it needs neither. Three of the five reviewers in the 2026-08-09 panel named missing migration tooling their single largest blocker. The Python script STAYS — CI already proves its output boots the real broker (0051-T6) — and the acceptance criterion is a DIFFERENTIAL test over the same fixtures: two converters that disagree are worse than one."
    notes: "OPEN QUESTION, not a build task. It changes ADR 0056 §1: a user-facing launcher ships in the release, which puts ratatui back into the audited dependency graph and makes the separate workspace pointless. This record covers the developer tool only; a user-facing launcher earns its own ADR. Listed as a task so the question is closed deliberately rather than drifted past."
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
| 0056-T1 | ⬜ planned | — | "Deliberately first, and useful with no UI at all: it proves the data model, and the manifest is machine-checked documentation of the operational surface on its own. The completeness guard is the load-bearing piece (ADR 0056 §3) — a launcher that silently shows 14 of 23 scripts becomes the list people trust. If phase 1 turns out to be sufficient, a justfile over the manifest is a legitimate place to stop; the manifest is where the value is, not the TUI." |
| 0056-T2 | ⬜ planned | — | "LAYOUT SETTLED 2026-08-10. Two panes, not three: at 80x24 a third pane leaves ~6 lines of output, useless for a bench run — the right pane is Detail while browsing and Output while running, since you are either choosing or watching. Collapsible group tree (not a flat filtered list) because discoverability is the whole point and the set is expected to grow. Follow-mode auto-scrolls until the user scrolls up, then stops. A finished run leads with the verdict; a FAILED run jumps to the first FAIL/FATAL/error line rather than the tail. ONE RUN AT A TIME, enforced: these scripts bind fixed ports and start containers, and bench/run.sh explicitly requires an otherwise-idle host, so concurrency would produce failures that look like broker bugs." |
| 0056-T3 | ⬜ planned | — | "The preflight block is the highest-value part of the UI: it turns 'run it and find out' into 'you are missing mosquitto-clients', named BEFORE the run rather than as a FATAL partway through. A task with missing required tools cannot be started at all, and the manifest carries an install hint per platform, because 'install kind' is where a newcomer stalls. Env editing is INLINE, not modal — a modal hides the description you are editing against. Manifest tasks may carry a `caution` string; bench/run.sh (pins the host, results invalid otherwise) and kind-smoke.sh genuinely need one. DECIDED 2026-08-10: no persisted last-run history — it is state that lies after a git pull, for little gain." |
| 0056-T5 | ⬜ planned | — | "Requested 2026-08-10, and justified on the spot: probing this machine while designing it found TWENTY orphaned mqttd processes from the day's test runs, invisible until asked for. The kube CONTEXT is the safety feature — kind-smoke.sh and operator-e2e.sh run kubectl against whatever is current, so showing `kube: prod-eu-west` before the user presses enter is the difference between a smoke test and an incident. Measured probe costs decide the polling: docker ps 0.01s, kind get clusters 0.06s, kubectl current-context 0.22s (2s timer, off the UI thread, bounded so an unreachable context shows `unreachable` instead of freezing); docker compose ls 1.9s (on demand only, staleness shown). Probing is read-only and unconditional; cleanup is never automatic and always confirmed with the specific processes/clusters listed — a tool that kills things you did not ask it to kill is worse than one that only shows them." |
| 0056-T6 | ⬜ planned | — | "DECIDED 2026-08-10: quitting must not leave orphans. What is achievable is stated precisely so this does not become an unkeepable claim. GUARANTEED: signal the process GROUP (so each script's own `trap EXIT` runs, which is what removes its brokers, containers and temp dirs) and WAIT for the group rather than detaching. NOT GUARANTEED: a script whose trap is buggy, or which was SIGKILLed, can still leak — mqttui cannot make another program's cleanup correct. So it signals, waits, then VERIFIES (stray processes, kind clusters, compose stacks) and reports what survived with an offer to remove it, leaving anything ignored visible in the environment panel. Claiming 'no orphans' outright would be the same unfalsifiable shape as the compose health check that could not fail (ADR 0047 T9)." |
| 0056-T4 | ✅ done | 2026-08-10 | "Answered by the ADR 0056 amendment of 2026-08-10: BOTH, as one binary that detects whether it is inside a checkout — standalone it offers the embedded set and states how many tasks are hidden and why; in a checkout it offers everything. Decision 1 (separate workspace, own lockfile) survived the change and now separates two PUBLISHED artifacts rather than one published and one internal, so ratatui still never enters the broker's dependency graph." |
| 0056-T7 | ⬜ planned | — | "MEASURED: the whole surface is 161 KB compressed (demo/ deploy/helm/ deploy/compose/ deploy/crds/ scripts/migrate/ scripts/k8s/), so include_dir! costs nothing worth discussing. Embedding buys offline operation, version-locking to the binary that was tested with it, and — the load-bearing property — executing nothing that arrived over the network. Standalone mode must state how many tasks are hidden and why; a tool that silently showed a subset would be the same defect as a manifest that silently went stale (ADR 0056 §3). Four tasks (release/build-repro.sh, k8s/render-parity.sh, gen-status.py, check-readme-facts.py) operate ON the repository and can never be freed from a checkout — the ADR says so rather than letting it be discovered." |
| 0056-T8 | ⬜ planned | — | "This is the answer to 'can it fetch the latest from main': yes, by way of a signed artifact built from main, not by trusting the branch. Fetching a branch tarball at runtime was REJECTED as a default — release binaries are cosign-signed with SLSA provenance and an SBOM, builds are reproducible, and every dependency is audited on every push (ADR 0045/0053); downloading shell from a mutable branch and running it discards all of that with one command, and repeats it on every launch. An explicit `--channel main` remains available for maintainers testing unreleased examples: loudly marked unverified, never a default." |
| 0056-T9 | ⬜ planned | — | "cargo install requires a Rust toolchain and compiles ~20 crates on the user's machine; the audience is somebody evaluating an MQTT broker who may have neither. The signed-binary pipeline already exists and is extended to a second artifact. Makes mqttui a published product with its own version, changelog, semver, signing and SBOM obligations — accepted knowingly. The name `mqttui` was verified free on crates.io on 2026-08-10 (HTTP 404 from the registry API, not inferred)." |
| 0056-T10 | ⬜ planned | — | "OPEN QUESTION, not a build task. It changes ADR 0056 §1: a user-facing launcher ships in the release, which puts ratatui back into the audited dependency graph and makes the separate workspace pointless. This record covers the developer tool only; a user-facing launcher earns its own ADR. Listed as a task so the question is closed deliberately rather than drifted past." |
<!-- /status-table:0056 -->

## Changelog

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
