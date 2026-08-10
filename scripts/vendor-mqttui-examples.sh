#!/usr/bin/env bash
# Copy the example surface into the mqttui package so it can be published (ADR 0056 T9).
#
# WHY A COPY EXISTS AT ALL
#
# `include_dir!` reads from the filesystem at compile time, and `cargo package` includes
# only files *beneath the package root*. `include_dir!("$CARGO_MANIFEST_DIR/../../demo")`
# therefore works perfectly from a checkout and produces a crate that cannot compile once
# published — verified, not assumed: `cargo publish --dry-run` failed with
# `"…/target/package/mqttui-0.1.0/../../scripts/k8s" is not a directory`.
#
# So the examples are vendored into tools/mqttui/bundle/. The originals stay the source of
# truth; this copy is generated. `--check` fails if they have diverged, and CI runs it, so a
# change to demo/ or deploy/ cannot silently ship a stale copy to crates.io — the same
# arrangement as scripts/gen-status.py and the delivery docs.
#
# Usage:
#   scripts/vendor-mqttui-examples.sh           refresh the copy
#   scripts/vendor-mqttui-examples.sh --check   fail if the copy is stale (CI)
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"
DEST="tools/mqttui/bundle"

# Source → path inside the bundle. The bundle layout must match the repository layout,
# because tasks.toml addresses scripts by their repository path and the unpacked root is
# what a standalone run uses as its working directory.
SOURCES=(
  "demo:demo"
  "deploy:deploy"
  "scripts/migrate:scripts/migrate"
  "scripts/k8s:scripts/k8s"
)

CHECK=0
[ "${1:-}" = "--check" ] && CHECK=1

STAGE="$(mktemp -d)"
trap 'rm -rf "$STAGE"' EXIT

for pair in "${SOURCES[@]}"; do
  src="${pair%%:*}"
  dst="${pair##*:}"
  [ -d "$src" ] || { echo "FATAL: $src does not exist"; exit 2; }
  mkdir -p "$STAGE/$(dirname "$dst")"
  # -a preserves the executable bit, which is what makes an unpacked .sh runnable.
  cp -a "$src" "$STAGE/$dst"
done

# Generated artefacts must never be vendored: bench results, rendered output and local state
# are large, machine-specific, and are not examples.
find "$STAGE" \( -name '.DS_Store' -o -name '*.pyc' -o -name '__pycache__' \) \
  -exec rm -rf {} + 2>/dev/null || true

if [ "$CHECK" = "1" ]; then
  if [ ! -d "$DEST" ]; then
    echo "FATAL: $DEST does not exist — run scripts/vendor-mqttui-examples.sh"
    exit 1
  fi
  if ! diff -r -q "$STAGE" "$DEST" >/dev/null 2>&1; then
    echo "FATAL: the vendored examples in $DEST are STALE."
    echo
    echo "They are a generated copy of demo/, deploy/, scripts/migrate/ and scripts/k8s/,"
    echo "and they are what a 'cargo install mqttui' user actually gets. Shipping the stale"
    echo "copy would hand that user a different demo from the one in this repository."
    echo
    diff -r "$STAGE" "$DEST" | head -40
    echo
    echo "Fix: scripts/vendor-mqttui-examples.sh"
    exit 1
  fi
  count=$(find "$DEST" -type f | wc -l | tr -d ' ')
  echo "vendored examples are current ($count files)"
  exit 0
fi

rm -rf "$DEST"
mkdir -p "$DEST"
cp -a "$STAGE"/. "$DEST"/
count=$(find "$DEST" -type f | wc -l | tr -d ' ')
size=$(du -sh "$DEST" | awk '{print $1}')
echo "vendored $count files ($size) into $DEST"
