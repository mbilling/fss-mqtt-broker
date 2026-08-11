# Releasing mqttd

A release is a **signed, annotated semver tag** and nothing else. Pushing
`vX.Y.Z` triggers [`.github/workflows/release.yml`](.github/workflows/release.yml),
which builds, signs, and publishes everything **from the tagged commit alone**
(ADR 0045). This document is the runbook for cutting a release and the reference
for **verifying** one.

## What a release contains

Every release publishes, for the tagged commit:

- **Reproducible binaries** — `linux/amd64` and `linux/arm64`, each with a
  `.sha256` checksum. Anyone can rebuild the tag and get byte-identical output
  (see [Verifying](#verifying-a-release)).
- **A reproducible bridge binary** — `mqtt-bridge`, both targets, checksummed and
  signed exactly like the broker's. The boundary bridge (ADR 0025) used to ship in
  no artifact at all, so running it meant building from source.
- **Two hardened container images** — `ghcr.io/mbilling/fss-mqtt-broker:X.Y.Z` and
  `ghcr.io/mbilling/fss-mqtt-broker-bridge:X.Y.Z` (each also `:latest` for a
  non-prerelease), distroless/static (fully-static musl binaries), non-root,
  multi-arch. The bridge is a **separate image**, not a second entrypoint into the
  broker's: it is a separate process with its own identity, credentials and failure
  domain, usually deployed where the broker is not.
- **A reproducible `mqttui` binary** — both targets, checksummed and signed like the
  others. It is a terminal UI for a human at a keyboard, so it ships as a binary and is
  deliberately **not** added to the container images: nothing in a distroless image can use
  a TUI, and putting one there would grow the runtime attack surface for no one. It is also
  published to crates.io as its own crate (step 7 below), from its own workspace, so
  `ratatui` never enters the broker's dependency graph.
- **Keyless signatures** — every binary, checksum, and the SBOM is
  cosign-signed via GitHub OIDC (no long-lived key); the image is cosign-signed
  by digest. All signatures are recorded in the public Rekor transparency log.
- **Build provenance** — SLSA build-provenance attestations for the binaries and
  the image (what commit, workflow, and inputs produced them).
- **An SBOM per binary** — CycloneDX `sbom-X.Y.Z.cdx.json`, `sbom-bridge-X.Y.Z.cdx.json`
  and `sbom-mqttui-X.Y.Z.cdx.json`, each enumerating only *its* transitive graph, so the
  bridge's dependencies are not implied to be in the broker image or vice versa. `mqttui`'s
  is cut from its own lockfile, which is the point of the separate workspace. Attached to
  the release and to the matching image.

The release commit must also pass the **supply-chain audit** (`cargo deny` +
`cargo audit`) — it is the first gate in the pipeline.

## Versioning

Versions follow [ADR 0039](docs/adr/0039-versioning-and-upgrade-policy.md):

- **`0.x` (pre-1.0):** breaking wire/schema changes are allowed on a MINOR bump.
  Ship `0.x` now — it is how the broker gets its first users and feedback.
- **`1.0.0` is the compatibility commitment.** The tag that becomes `1.0.0` is
  the one that freezes the wire and schema per
  [ADR 0038](docs/adr/0038-prerelease-compatibility-freeze.md). Cutting it is a
  deliberate, reviewed decision — not just the next increment.
- **Pre-release tags** (`vX.Y.Z-rc.1`, any tag with a hyphen) are published as
  GitHub pre-releases and do **not** move the `:latest` image tag.

### The stability contract and its oracles (ADR 0058)

From `1.0.0`, [ADR 0058](docs/adr/0058-one-dot-zero-stability-contract.md) promises
**upgrade-in-place, never wipe-and-rejoin**. Two mechanisms enforce it, and a release is
not cut unless both are green:

- **Disk:** every store opens through `schema::gate_or_migrate` with a migration registry.
  A schema bump that lands without its `MigrationStep` fails each store's
  `the_migration_registry_covers_the_contract_range` test in CI — so the disk contract
  cannot be broken by omission.
- **Wire:** the nightly two-binary rolling-upgrade test (`cluster_upgrade.rs`,
  `BASELINE_REF`) is the **wire oracle**. Pre-1.0 the baseline is bumped by hand with each
  reshape (see the pre-commit checklist). **From `1.0.0`, `BASELINE_REF` pins to the
  previous release and advances only along ADR 0039's skew policy** — the previous minor
  within a major, the previous major's *gateway minor* across a major boundary. A reshape
  that breaks the roll turns the nightly red before it can reach a tag.

When cutting `1.0.0`: pin `BASELINE_REF` to the `1.0.0` commit, set each store's
`MIGRATE_FLOOR` to its current `SCHEMA_VERSION`, and flip the wipe-and-rejoin language in
this file and the README to the contract (0058-T5).

## Cutting a release

1. **Pick the version** and make sure `main` is green (CI, nightly tier).
2. **Bump `version`** in the workspace `Cargo.toml` (`[workspace.package]`) to
   `X.Y.Z`, commit, and merge to `main`. `mqttui` versions **independently** — it is a
   separate workspace and a separate published crate (ADR 0056 §1), so bump
   `tools/mqttui/Cargo.toml` only when `mqttui` itself changed.
3. **Confirm the readiness checklist** in
   [ADR 0044](docs/adr/0044-release-readiness-assurance.md) is satisfied — the
   release is gated on it.
4. **Tag and push** an annotated, signed tag on the release commit:

   ```sh
   git tag -s -a v0.1.0 -m "mqttd 0.1.0"
   git push origin v0.1.0
   ```

   > `-s` signs the tag with your git signing key; the pipeline's artifact
   > signing is separate (keyless cosign) and always runs.

5. **Watch the `Release` workflow.** On success it has: pushed the multi-arch
   image, signed every asset, attested provenance, generated the SBOM, and
   created the GitHub Release with all assets attached.
6. **Sanity-check** by running the [verify steps](#verifying-a-release) against
   the published release yourself.
7. **Publish `mqttui` to crates.io** — a *manual* step, deliberately. It is the one part of
   a release that cannot be undone: crates.io versions are permanent and cannot be
   re-uploaded, so it is not automated behind a tag push. CI has already proven the crate
   packs and builds from its own tarball on every PR (`cargo publish --dry-run`), so this
   should be uneventful:

   ```sh
   scripts/vendor-mqttui-examples.sh --check     # the bundled examples match the repo
   cargo publish --manifest-path tools/mqttui/Cargo.toml
   ```

   Skip it if `tools/mqttui/Cargo.toml`'s version is unchanged since the last publish —
   crates.io will reject a duplicate, which is the correct outcome, not a failure to fix.
   The signed musl `mqttui` binaries are attached to the GitHub Release by the pipeline
   either way; crates.io is the secondary channel (ADR 0056 Decision C).

If the workflow fails, fix forward on `main` and cut the next patch tag — a tag
is immutable; never force-move a published one.

## Verifying a release

You need [`cosign`](https://github.com/sigstore/cosign) and the toolchain in
[`rust-toolchain.toml`](rust-toolchain.toml). Set:

```sh
VERSION=0.1.0
REPO=mbilling/fss-mqtt-broker
IDENTITY="https://github.com/${REPO}/.github/workflows/release.yml@refs/tags/v${VERSION}"
ISSUER="https://token.actions.githubusercontent.com"
```

### 1. Verify a binary's signature and checksum

```sh
NAME=mqttd-${VERSION}-x86_64-unknown-linux-musl
# ...download NAME, NAME.sha256, NAME.sig, NAME.pem from the release...

cosign verify-blob "$NAME" \
  --certificate "${NAME}.pem" \
  --signature "${NAME}.sig" \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER"

sha256sum -c "${NAME}.sha256"
```

> On **cosign 3.x**, `--certificate` and `--signature` print a deprecation notice
> pointing at `--bundle`. They still work and still verify; the release publishes
> the separate `.pem`/`.sig` files these flags expect. Expect to revisit this when
> cosign removes them.

### 2. Reproduce the binary yourself (the strongest check)

The build is byte-reproducible. Rebuild the tag and confirm the checksum matches
what the release published:

```sh
git clone https://github.com/${REPO} && cd fss-mqtt-broker
git checkout v${VERSION}
scripts/release/build-repro.sh x86_64-unknown-linux-musl
sha256sum -c "path/to/${NAME}.sha256"   # same hash as the release
```

Same tag + same target + this script ⇒ the same bytes, on any machine. The
determinism recipe (pinned toolchain, locked deps, path remapping,
`SOURCE_DATE_EPOCH`) lives in
[`scripts/release/build-repro.sh`](scripts/release/build-repro.sh).

### 3. Verify the container image

```sh
cosign verify "ghcr.io/${REPO}:${VERSION}" \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" | jq .

# Provenance attestation (attached to the image in the registry).
# NOTE: the type is `slsaprovenance1`, not `slsaprovenance` — the bare name is
# cosign's alias for SLSA v0.2, while this pipeline emits v1, so the shorter
# spelling fails with "none of the attestations matched the predicate type".
cosign verify-attestation "ghcr.io/${REPO}:${VERSION}" \
  --type slsaprovenance1 \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" >/dev/null && echo "provenance OK"
```

### 4. Read the SBOM

The SBOM is attached to the GitHub Release as `sbom-${VERSION}.cdx.json` (with
its own `.sig`/`.pem`, verifiable exactly like a binary in step 1), and to the
image as a **signed attestation**:

```sh
# From the release — verify it, then read it:
cosign verify-blob "sbom-${VERSION}.cdx.json" \
  --certificate "sbom-${VERSION}.cdx.json.pem" \
  --signature "sbom-${VERSION}.cdx.json.sig" \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER"
jq '.components | length' "sbom-${VERSION}.cdx.json"   # dependency count

# Or from the image, authenticated in one step:
cosign verify-attestation "ghcr.io/${REPO}:${VERSION}" \
  --type https://cyclonedx.org/bom \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" \
  | jq -r .payload | base64 -d | jq '.predicate.components | length'
```

> Do **not** use `cosign download sbom`. It is deprecated, prints nothing here,
> and — as cosign itself warns — would not authenticate the SBOM even if it did.
> The runbook used to suggest it with a trailing `|| true`, which turned the
> failure into a silent success.

## The reproducibility contract

Reproducible builds depend on **not** introducing nondeterminism:

- No `SystemTime::now()`, `Instant::now()`, or RNG in build scripts or `const`
  initializers that reach the binary.
- Dependencies stay pinned (`Cargo.lock` committed; the pipeline builds
  `--locked`).
- The toolchain is pinned in `rust-toolchain.toml`; bumping it is a reviewed
  change that re-baselines reproducibility (the pipeline re-checks byte-identity
  on every release, so a regression fails the release, not a user's verify).
