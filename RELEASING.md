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
- **A hardened container image** — `ghcr.io/mbilling/fss-mqtt-broker:X.Y.Z`
  (and `:latest` for a non-prerelease), distroless/static (fully-static musl binary), non-root, multi-arch.
- **Keyless signatures** — every binary, checksum, and the SBOM is
  cosign-signed via GitHub OIDC (no long-lived key); the image is cosign-signed
  by digest. All signatures are recorded in the public Rekor transparency log.
- **Build provenance** — SLSA build-provenance attestations for the binaries and
  the image (what commit, workflow, and inputs produced them).
- **An SBOM** — a CycloneDX (`sbom-X.Y.Z.cdx.json`) enumerating the binary's full
  transitive dependency graph, attached to the release and to the image.

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

## Cutting a release

1. **Pick the version** and make sure `main` is green (CI, nightly tier).
2. **Bump `version`** in the workspace `Cargo.toml` (`[workspace.package]`) to
   `X.Y.Z`, commit, and merge to `main`.
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

# Provenance + SBOM attestations (attached to the image in the registry):
cosign verify-attestation "ghcr.io/${REPO}:${VERSION}" \
  --type slsaprovenance \
  --certificate-identity "$IDENTITY" \
  --certificate-oidc-issuer "$ISSUER" >/dev/null && echo "provenance OK"
```

### 4. Read the SBOM

```sh
# Attached to the GitHub Release as sbom-${VERSION}.cdx.json, and to the image:
cosign download sbom "ghcr.io/${REPO}:${VERSION}" 2>/dev/null || true
jq '.components | length' sbom-${VERSION}.cdx.json   # dependency count
```

## The reproducibility contract

Reproducible builds depend on **not** introducing nondeterminism:

- No `SystemTime::now()`, `Instant::now()`, or RNG in build scripts or `const`
  initializers that reach the binary.
- Dependencies stay pinned (`Cargo.lock` committed; the pipeline builds
  `--locked`).
- The toolchain is pinned in `rust-toolchain.toml`; bumping it is a reviewed
  change that re-baselines reproducibility (the pipeline re-checks byte-identity
  on every release, so a regression fails the release, not a user's verify).
