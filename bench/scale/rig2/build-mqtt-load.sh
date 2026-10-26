#!/usr/bin/env bash
# build-mqtt-load.sh [out-path]
#
# A static x86_64 Linux (musl) mqtt-load for the generator hosts, built in Docker as
# linux/amd64 so it builds the same from a Mac or a Linux laptop:
#
#   docker run --platform linux/amd64 rust:<rust-toolchain.toml channel>-bookworm
#     apt-get install musl-tools; rustup target add x86_64-unknown-linux-musl
#     CC_x86_64_unknown_linux_musl=musl-gcc RUSTFLAGS='-C target-feature=+crt-static -C strip=symbols'
#     cargo build --release --locked --target x86_64-unknown-linux-musl -p mqtt-load
#
# The flags are scripts/release/build-repro.sh's (musl-gcc for the C dependency, a
# fully static CRT, stripped, --locked); the path remapping and SOURCE_DATE_EPOCH that
# make a release byte-reproducible are left out, because a generator binary is not a
# shipped artifact. On Apple silicon the container runs emulated: a cold build takes
# several minutes, a warm one (the cargo registry and target dir are cached in Docker
# volumes and under target/rig2-amd64) well under one.
#
# Prints the binary's path on the last line (default target/rig2-amd64/mqtt-load).

set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
OUT="${1:-$ROOT/target/rig2-amd64/mqtt-load}"
CHANNEL=$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$ROOT/rust-toolchain.toml")
[ -n "$CHANNEL" ] || { echo "no channel in rust-toolchain.toml" >&2; exit 1; }
IMAGE="rust:${CHANNEL}-bookworm"
command -v docker >/dev/null || { echo "docker is required to build the linux/amd64 generator" >&2; exit 1; }

mkdir -p "$ROOT/target/rig2-amd64"
echo "building mqtt-load for x86_64-unknown-linux-musl in $IMAGE (linux/amd64)" >&2
docker run --rm --platform linux/amd64 \
	-v "$ROOT:/src" -w /src \
	-v mqtt-rig2-cargo-registry:/usr/local/cargo/registry \
	-e CARGO_TARGET_DIR=/src/target/rig2-amd64/cargo \
	-e CC_x86_64_unknown_linux_musl=musl-gcc \
	-e RUSTFLAGS="-C target-feature=+crt-static -C strip=symbols" \
	-e CARGO_INCREMENTAL=0 \
	"$IMAGE" bash -c '
		set -e
		command -v musl-gcc >/dev/null || { apt-get update -qq && apt-get install -y -qq musl-tools >/dev/null; }
		rustup target add x86_64-unknown-linux-musl >/dev/null 2>&1
		cargo build --release --locked --target x86_64-unknown-linux-musl -p mqtt-load
	' >&2
BIN="$ROOT/target/rig2-amd64/cargo/x86_64-unknown-linux-musl/release/mqtt-load"
[ -x "$BIN" ] || { echo "build produced no binary at $BIN" >&2; exit 1; }
if [ "$BIN" != "$OUT" ]; then cp "$BIN" "$OUT"; fi
file "$OUT" >&2 2>/dev/null || true
echo "$OUT"
