# ADR 0056 — `mqttui`: a terminal UI for running the demo, migration and test scripts

- **Status:** Proposed
- **Date:** 2026-08-10
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0056-mqttui.md](../delivery/0056-mqttui.md) — plan, progress, and changelog
- **Related:** [ADR 0044](0044-release-readiness-assurance.md) (the assurance scripts this
  would front), [ADR 0045](0045-release-engineering-and-distribution.md) (what ships in a
  release, and therefore what must *not* enter its dependency graph),
  [ADR 0051](0051-evaluation-readiness.md) (onboarding — the reason a newcomer cannot find
  these scripts is the same reason they cannot evaluate the broker), issue #138.

> This record states the decision only. How it is being built and how far along it is live
> in the [delivery doc](../delivery/0056-mqttui.md).

**`mqttui` is a terminal UI for running this repository's own scripts** — the demo stack,
the Mosquitto migration converter, the smoke and conformance suites, the Kubernetes
end-to-end runs, the benchmark harness. It is **not** part of the broker and does not talk
MQTT: it finds, explains and runs the scripts that are already here.

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

Building the UI is straightforward and is not what this record is for. The decisions worth
recording are **where its dependencies live**, **how it avoids going stale**, and **who it
is for** — each expensive to reverse once chosen.

## Decision

### 1. It is `mqttui`, and its dependencies stay out of the broker's

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

### 4. Teardown is signal-the-group, wait, verify, report — not a promise of no orphans

Every script traps `EXIT` to clean up brokers, containers and temporary directories.
Signalling only the immediate child leaves those orphaned. So `mqttui` signals the
**process group** and **waits** for it, rather than detaching — a cancelled task then
tears down exactly as an interrupted terminal run does.

That is the part that can be guaranteed, and the guarantee stops there. A script whose
trap is buggy, or which was `SIGKILL`ed, can still leak; `mqttui` cannot make another
program's cleanup correct. Rather than claim otherwise, it **verifies** afterwards — stray
broker processes, `kind` clusters, compose stacks — and **reports what survived**, offering
to remove it and leaving anything declined visible in the environment panel (§5).

The distinction is the whole point. "Quitting leaves no orphans" is a claim that cannot be
checked and would quietly stop being true; "it signals correctly, waits, and tells you what
is left" is a claim that can be, and it is strictly more useful when something does leak.
This project has shipped the other kind before — a compose health check that could not fail
(ADR 0047 T9) — and the lesson is cheaper to apply than to relearn.

Not hypothetical: stray brokers accumulating from panicking tests actively poisoned later
runs while issue #124's regression test was being written, until a `Drop` guard was added.
Probing this machine while designing the panel below found **twenty** orphaned brokers from
one day's test runs, none of them visible until something asked.

### 5. It shows the state of the machine it is about to act on

Docker, the current Kubernetes context, `kind` clusters, compose stacks, stray broker
processes, and the ports these scripts use — surfaced continuously, with explicit,
confirmed cleanup actions.

The **Kubernetes context is a safety feature, not a convenience.**
`scripts/k8s/kind-smoke.sh` and `scripts/k8s/operator-e2e.sh` run `kubectl` against
whatever context is current. Showing that context before the user commits is the difference
between a smoke test and an incident, and no amount of documentation substitutes for it
being on screen.

Two constraints, from measured costs (`docker ps` 0.01s, `kind get clusters` 0.06s,
`kubectl config current-context` 0.22s, `docker compose ls` **1.9s**): probes run off the
UI thread under a bound, so an unreachable context reports `unreachable` instead of
freezing the interface; and the expensive probe refreshes on demand, with staleness shown
rather than hidden.

**Probing is read-only and unconditional. Acting is never automatic.** No implicit cleanup
at startup, and every destructive action names the specific processes or clusters it will
remove. A tool that kills things the user did not ask it to kill is worse than one that
only shows them.

### 6. Headless mode shares the manifest

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

## Open question — developer tool or user-facing? *(answered by the 2026-08-10 amendment: both)*

Recorded, not decided, because it changes the first decision.

As specified, this is a **developer tool**: it runs *this repository's* scripts and assumes
a checkout. A user-facing "watch mqttd do something" launcher is a different product — it
would ship in the release, which puts `ratatui` back into the audited graph and makes the
separate workspace pointless.

The two can coexist later (a shipped demo binary is not this tool), but building one while
imagining the other is how the dependency boundary gets quietly abandoned. **This ADR
covers the developer tool only.** A user-facing launcher earns its own record.

> **Superseded by the 2026-08-10 amendment below.** It is both: one binary that detects
> whether it is in a checkout. The dependency boundary was *not* abandoned — it survived
> the change and now separates two published artifacts rather than one published and one
> internal. The prediction that building one while imagining the other would erode the
> boundary is worth keeping on the record; it did not happen, because the boundary was
> written down first.

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

## Amendment (2026-08-10): `mqttui` is installed, not cloned — and carries its own examples

This record originally scoped `mqttui` as a **developer tool** that runs *this repository's*
scripts and assumes a checkout, and left the developer-vs-user-facing question open as
`0056-T4`. **That question is now answered: it is both, and the user-facing half is the
point.** A one-line install must give somebody the demo, the Kubernetes examples and the
migration converter without cloning anything.

### The constraint that shapes everything

`cargo install mqttui` does not produce a checkout, and the scripts *are* the product. They
also do not stand alone: `deploy-smoke.sh` reads `deploy/systemd/mqttd.env.example` and
`deploy/compose/acl.toml`; `kind-smoke.sh` needs the whole Helm chart; `bench/run.sh` needs
a compose file and five Dockerfiles.

So the task set splits three ways, and the split is decided by what each task **reads**,
not by preference:

| | |
|---|---|
| **Standalone** — ships inside the binary | the demo stack, the Kubernetes examples (chart + CRDs), the compose reference deployment, and the Mosquitto migration converter |
| **Checkout-only, permanently** | anything that operates *on the repository*: `release/build-repro.sh` builds it, `k8s/render-parity.sh` and `render-parity-one.sh` diff the chart against the operator, `k8s/kind-smoke.sh` and `k8s/operator-e2e.sh` build images from source, `migrate/test-from-mosquitto.sh` boots a broker it does not build, and `gen-status.py` / `check-readme-facts.py` / `gen-bridge-dashboard.py` read and rewrite the repo's own documents — **nine in all** |
| **Checkout-only for now** | `bench/run.sh` (embeddable later; its 12 MB is generated `results/`, the harness is ~50 KB), the interop suites, the OIDC fixture, and the smoke tests that `cargo build` a broker — including **`quickstart-smoke.sh`**, which an earlier draft of this table wrongly listed as standalone: it builds `mqttd` from source when `MQTTD_BIN` is unset, so it cannot run beside a binary with no toolchain and no source |

`mqttui` **detects whether it is inside a checkout**: standalone it offers the embedded
set and says plainly how many tasks are hidden and why; in a checkout it offers everything.
A tool that silently showed a subset would be the same defect as a manifest that silently
went stale (§3).

**"Everything possible" is not everything**, and this record says so rather than letting it
be discovered. Nine tasks operate on the repository itself and cannot be freed from it by
any amount of embedding.

**Availability is declared, not inferred** — the same rule as §2, and for the same reason.
Implementation first inferred it from whether the script had been embedded, which put
`k8s-render-parity` in front of a standalone user, ran it, and produced
`could not find Cargo.toml`: the script travels perfectly well, and what it needs is the
operator crate. A file being present says nothing about whether it can work. `tasks.toml`
therefore carries `needs_checkout`, and a test walks each bundled task's script **and the
scripts it invokes** to catch the declaration going stale — `render-parity.sh` contains no
`cargo` at all, it delegates.

The two ways a task can be unavailable are also kept apart in what the user is told:
*needs a clone* and *is not bundled* both currently end in "clone the repository", but only
the second may change in a later release, and reporting them alike would send someone to do
work that cannot help.

### Decision A — the examples are embedded, not fetched

The full example surface — `demo/`, `deploy/helm/`, `deploy/compose/`, `deploy/crds/`,
`scripts/migrate/`, `scripts/k8s/` — is **161 KB compressed** (measured). Embedding it with
`include_dir!` costs nothing worth discussing and buys three things: it works offline, it
is version-locked to the binary that was tested with it, and **it executes nothing that
arrived over the network**.

### Decision B — "latest" comes from a signed release, never from a branch

The obvious way to keep examples current is to fetch them from `main` at runtime. It is
rejected as a default, and the reason is this project's own posture: release binaries are
cosign-signed with SLSA provenance and a CycloneDX SBOM, builds are reproducible, and every
dependency is audited on every push (ADR 0045, ADR 0053). **A tool that downloads shell
scripts from a mutable branch and runs them discards all of that with one command** — and
worse than `curl | sh`, because it repeats on every launch.

So:

1. **Embedded is the default.** Always available, offline, matching the binary.
2. **`mqttui update` fetches a signed examples bundle from a GitHub _release_**, verified
   before it is unpacked. CI publishes a rolling bundle on merges to `main`, so "the latest
   examples" *is* reachable — it is simply reachable as a signed artifact rather than as a
   branch tarball. This is the answer to "can it fetch the latest from main": yes, by way
   of a signed thing built from main, not by trusting the branch.
3. **Fetching an unverified branch is possible but explicit** (`--channel main`), loudly
   marked, never a default, and intended for maintainers testing unreleased examples.

### Decision C — distribution is a signed binary first, crates.io second

`cargo install mqttui` requires a Rust toolchain and compiles ~20 crates on the user's
machine. The audience is somebody evaluating an MQTT broker, who may have neither. The
project already builds signed, reproducible musl binaries, so the primary install is that
pipeline extended to a second artifact; `cargo install` is offered for Rust users.

This makes `mqttui` a **published product with its own obligations** — version, changelog,
semver, and by this project's own standard (ADR 0045) signing and an SBOM. Accepted
knowingly.

Decision 1 is unaffected and is now doing more work than before: `mqttui` is published as
its **own** crate from its **own** workspace, so `ratatui` never enters the broker's
dependency graph even though both are now released artifacts.

### Decision D — the migration converter is reimplemented in Rust

`scripts/migrate/from-mosquitto.py` requires `python3` and, today, a checkout. It reads the
**user's** `mosquitto.conf`, not ours, so it is the one task that is *more* useful
standalone than in a checkout. As a built-in it becomes:

```sh
mqttui migrate mosquitto /etc/mosquitto/mosquitto.conf -o mqttd.toml
```

— no Python, no clone, no prerequisites. Three of the five reviewers in the 2026-08-09
panel named missing migration tooling their single largest blocker, and this is the
shortest path from "curious" to "it understands my configuration".

The Python script stays: CI already proves its output boots the real broker
(`scripts/migrate/test-from-mosquitto.sh`, ADR 0051 T6), and the two must agree. **A
differential test over the same fixtures is the acceptance criterion** — two converters
that disagree are worse than one.
