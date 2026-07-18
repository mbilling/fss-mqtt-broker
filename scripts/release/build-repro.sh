#!/usr/bin/env bash
# Reproducible release build of the mqttd binary (ADR 0045 T2).
#
# This is THE build recipe: the release workflow runs it, and a third party
# verifying a release runs the *same* script against the same tag. Same tag +
# same target + this script => byte-identical binary. That is what makes the
# published checksum and cosign signature meaningful — anyone can regenerate the
# bytes and confirm they match.
#
# Determinism comes from four things, all fixed here:
#   1. rustc pinned by rust-toolchain.toml (channel 1.97.0).
#   2. Cargo.lock pinned (--locked): the exact dependency graph, no resolution.
#   3. Path remapping: absolute build/registry paths are rewritten to fixed
#      logical roots so the binary does not embed the machine it was built on.
#   4. SOURCE_DATE_EPOCH pinned to the commit time, and incremental compilation
#      off, so nothing time- or cache-dependent leaks in.
#
# codegen-units=1 and lto=thin are already set in [profile.release] (Cargo.toml).
#
# We build for the fully-static *musl* targets: the binary carries no libc, so it
# runs on any Linux (no glibc-version skew between the build host and the runtime
# image) and ships in a `distroless/static` / scratch image. Static linking also
# removes the dynamic loader from the attack surface — the security posture the
# broker demands of itself.
#
# Usage: scripts/release/build-repro.sh <rust-target-triple>
#   e.g. scripts/release/build-repro.sh x86_64-unknown-linux-musl
#
# Prints the path to the built binary on stdout (last line).
set -euo pipefail

TARGET="${1:?usage: build-repro.sh <rust-musl-target-triple>}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$REPO_ROOT"

CARGO_HOME_DIR="${CARGO_HOME:-$HOME/.cargo}"

# Commit time is the single source of "now" for the build — deterministic for a
# given tag, independent of when the build runs.
SOURCE_DATE_EPOCH="$(git log -1 --pretty=%ct)"
export SOURCE_DATE_EPOCH

# Rewrite absolute paths to fixed logical roots so build-machine paths never
# reach the binary; strip symbols; force fully-static CRT linkage. Order matters
# in the remap list: longer prefixes first.
export RUSTFLAGS="--remap-path-prefix=${CARGO_HOME_DIR}=/cargo --remap-path-prefix=${REPO_ROOT}=/build -C strip=symbols -C target-feature=+crt-static"

# The C dependencies (aws-lc-rs, ring) need a musl C compiler. `musl-tools`
# provides `musl-gcc` for the *native* arch, so each arch builds on its own
# native runner (no cross-toolchain). Point the target's CC at it, e.g.
# CC_x86_64_unknown_linux_musl=musl-gcc.
export "CC_${TARGET//-/_}=${CC_MUSL:-musl-gcc}"

# No incremental artifacts, no build-time locale/tz surprises.
export CARGO_INCREMENTAL=0
export LC_ALL=C
export TZ=UTC

echo "reproducible build: target=${TARGET} SOURCE_DATE_EPOCH=${SOURCE_DATE_EPOCH}" >&2

rustup target add "$TARGET" >&2 2>/dev/null || true
cargo build --release --locked --target "$TARGET" -p mqttd >&2

echo "${REPO_ROOT}/target/${TARGET}/release/mqttd"
