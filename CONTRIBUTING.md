# Contributing

Thanks for considering it. This document is what a contributor actually needs:
how to build and test, what the review bar is, and the two conventions here that
are unusual enough to trip you up if nobody says so. The binary and hub map is
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md); the stakeholder index is
[docs/README.md](docs/README.md).

## Getting a toolchain

Any Rust install works — if you already have `cargo`, skip this. `rustup` reads
`rust-toolchain.toml` on its own and will fetch the pinned toolchain the first
time you build.

If you use [mise](https://mise.jdx.dev), the repo's `mise.toml` makes it one
command:

```sh
mise trust && mise install
```

That config deliberately names **no version**. It only tells mise to read
`rust-toolchain.toml`, so the pin lives in exactly one file and bumping it moves
everything. `mise ls --current` prints `rust-toolchain.toml` as the source, which
is how you check the two have not drifted apart.

## Build and test

```sh
cargo build
cargo test                              # the whole workspace
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

Those last two are **gates, not suggestions** — CI runs with `RUSTFLAGS: -D
warnings`, so a warning is a failed build. Run both before you push; it is the
single most common reason a first PR goes red.

Minimum supported Rust is **1.88** (**1.89** for the `mqttd-operator` crate).
`rust-toolchain.toml` pins **1.97.0**, which is a reproducible-build anchor, not
a requirement on you — it is simply what `cargo` will use in this repo unless you
override it.

`cargo test --all-features` additionally needs a CMake and Go toolchain, which
the FIPS crypto backend builds from source; without them that one command fails
to build while everything above still passes. CI covers it.

A few checks are cheap locally and worth running if you touched what they cover:

```sh
python3 scripts/gen-status.py --check       # delivery dashboard is current
python3 scripts/check-issue-alignment.py    # every open delivery task names an open issue
python3 scripts/check-readme-facts.py       # README's counts/crate table match the tree
./scripts/quickstart-smoke.sh             # the README's own quickstarts still work
./scripts/interop/run.sh                  # mosquitto + paho conformance
```

The heavier assurance tiers — fault-injection sweeps, an hour-long soak, fuzzing,
kind-based Kubernetes runs, a real Keycloak — run nightly, not per-PR. You are
not expected to run them; if one of them catches your change, that is the system
working, and the fix is a normal follow-up.

## The two conventions worth knowing up front

**1. Decisions live in ADRs; progress lives in delivery docs.** `docs/adr/NNNN-*.md`
records *why* a decision was made and is frozen once `Accepted`. `docs/delivery/NNNN-*.md`
records *how* it is being built and *how far along* it is. They are deliberately
separate — see [`docs/delivery/README.md`](docs/delivery/README.md).

If your change completes or changes the scope of a delivery task, update that
task's frontmatter and run `python3 scripts/gen-status.py`. The dashboard is
generated and CI-checked, so a stale one fails the build.

**2. A task title is a claim, and the claim must be true.** This is the one
convention that has cost the most rework here. If a title says a mechanism exists,
that mechanism must exist. When scope changes, the options are to *deliver the
missing clause* or to *narrow the title and open a task for the remainder* —
never to quietly delete the clause. Several tasks in this repo have been narrowed
exactly that way, with the reason recorded in place.

The same applies to documentation: the release bar for user-facing prose is that
nothing **actively misleads**. An honest, stated limitation is fine and welcome.
A number that is stale, or a capability described as measured when it was only
designed, is not.

## Pull requests

- **One PR per change**, branched off `main`.
- Commit messages follow `type(scope): summary` — e.g. `fix(0037): …`,
  `test(0044): …`, `docs(0051-T2): …`. The scope is usually the ADR number the
  work belongs to. Browse `git log --oneline` for the house style.
- **Explain why, not just what.** The commit body and PR description are where
  the reasoning is preserved; the diff already shows the what.
- **Say how you verified it.** A new test is the strongest form. If you fixed a
  bug, a test that fails against the old behaviour is worth more than one that
  merely passes against the new — and if you claim a test would have caught
  something, check that it actually does by reintroducing the defect.
- If you found a real problem you are *not* fixing, open an issue rather than
  leaving it in a comment. Undocumented known problems are how they get
  rediscovered expensively.

Small fixes — typos, a stale link, a clearer error message — do not need an
issue first. For anything that changes behaviour, an issue or a draft PR to agree
the approach first will save you rework.

## Security

**Do not open a public issue for a suspected vulnerability.** Use GitHub's
private vulnerability reporting; the full policy is in
[SECURITY.md](SECURITY.md).

## Licence

Contributions are accepted under the [Apache-2.0](LICENSE) licence, the same as
the rest of the project. There is no CLA.
