#!/usr/bin/env bash
# Reproducible release build of the shipped binaries — mqttd, mqtt-bridge, mqttui,
# and mqttd-operator (ADR 0045 T2; operator added by ADR 0055 T8).
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

# The C dependency (aws-lc-rs) needs a musl C compiler. `musl-tools`
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

# Both shipped binaries, built under the same recipe so both are reproducible and
# both can be verified the same way. The bridge is a SEPARATE process by design
# (ADR 0025: its own identity, credentials and failure domain), so it is a second
# binary and a second image rather than a second entrypoint into the broker's.
PACKAGES=("${@:2}")
if [ "${#PACKAGES[@]}" -eq 0 ]; then
  PACKAGES=(mqttd mqtt-bridge)
fi
for pkg in "${PACKAGES[@]}"; do
  if [ "$pkg" = "mqttd-fips" ]; then
    # The FIPS variant (ADR 0068 T4): the same broker built with the validated
    # AWS-LC module. An ISOLATED target dir (still under target/, so the repro
    # proof's `cargo clean` covers it) keeps the feature-flagged build from
    # overwriting the standard binary's path — order-independent by construction.
    #
    # The FIPS module's entropy code includes Linux kernel UAPI headers
    # (linux/random.h), which Debian's musl-gcc wrapper keeps off its include
    # path by design. Kernel UAPI headers are libc-agnostic, so append the
    # distro's header dirs with -idirafter: musl's own headers always win, and
    # only the kernel interfaces resolve from the glibc locations.
    KERNEL_INC="-idirafter /usr/include/$(uname -m)-linux-gnu -idirafter /usr/include"
    CFLAGS_VAR="CFLAGS_${TARGET//-/_}"
    export "${CFLAGS_VAR}=${!CFLAGS_VAR:-} ${KERNEL_INC}"
    cargo build --release --locked --target "$TARGET" -p mqttd --features fips \
      --target-dir "${REPO_ROOT}/target/fips-build" >&2
    cp "${REPO_ROOT}/target/fips-build/${TARGET}/release/mqttd" \
       "${REPO_ROOT}/target/${TARGET}/release/mqttd-fips"
  elif [ "$pkg" = "mqttui" ]; then
    # mqttui is a SEPARATE workspace with its own lockfile (ADR 0056 §1) — the boundary is
    # the lockfile, so that a terminal UI's dependency tree can never reach the broker's.
    # It is therefore built through its own manifest, which is what makes `--locked` bind
    # to *its* lock rather than the broker's. Same recipe otherwise: same rustc, same
    # remapping, same SOURCE_DATE_EPOCH, so it is reproducible and verifiable identically.
    cargo build --release --locked --target "$TARGET" \
      --manifest-path tools/mqttui/Cargo.toml >&2
  else
    cargo build --release --locked --target "$TARGET" -p "$pkg" >&2
  fi
done

# stdout stays the broker path: callers (and RELEASING.md) treat this script's
# output as "the binary to checksum", and quietly changing that would break them.
echo "${REPO_ROOT}/target/${TARGET}/release/mqttd"
