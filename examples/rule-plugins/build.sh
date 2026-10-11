#!/bin/sh
# Builds the two worked examples of rule-function modules (ADR 0086 D9):
#
#   out/geo.wasm         from rust/   (cargo, target wasm32-unknown-unknown)
#   out/telemetry.wasm   from c/      (zig cc, target wasm32-wasi)
#
#   examples/rule-plugins/build.sh [OUT_DIR]
#
# Needs: the repository's pinned Rust toolchain with the wasm32-unknown-unknown target
# (`rustup target add wasm32-unknown-unknown`), and zig 0.16.0, the compiler the
# broker's own jq module is built with (`ZIG=…` names the binary;
# crates/mqtt-wasm-sandbox/jq-module/fetch-zig.sh fetches it, checked by sha256).
# Prints each file's sha256: what a rules file pins it by.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
out=${1:-$here/out}
zig=${ZIG:-zig}

ZIG_VERSION=$(sed -n 's/^ZIG_VERSION=//p' "$here/../../crates/mqtt-wasm-sandbox/jq-module/build.sh")
have=$("$zig" version)
if [ "$have" != "$ZIG_VERSION" ]; then
  echo "build.sh: zig $ZIG_VERSION is the pinned toolchain, found $have" >&2
  exit 1
fi

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

mkdir -p "$out"

# Rust. The target and the WebAssembly features are in rust/.cargo/config.toml, which
# cargo reads from the directory it runs in.
(cd "$here/rust" && cargo build --release --locked)
cp "$here/rust/target/wasm32-unknown-unknown/release/mqttd_rule_plugin_geo.wasm" "$out/geo.wasm"

# C. The same WebAssembly features as the broker's jq module, spelled out; a reactor
# (no main: the broker calls the exported functions), with nothing of the build
# machine in the file. The stack and the memory the module starts with are set small:
# the linker's defaults are larger than the 16 MiB a module's instance gets unless its
# table says `memory_mib`.
"$zig" cc -target wasm32-wasi \
  -mcpu=mvp+bulk_memory+sign_ext+nontrapping_fptoint+mutable_globals \
  -O2 -DNDEBUG -fno-ident -Wall -Wextra -Werror \
  -ffile-prefix-map="$here"=. \
  -mexec-model=reactor -Wl,--strip-all -Wl,--no-entry \
  -Wl,-z,stack-size=65536 -Wl,--initial-memory=131072 \
  -o "$out/telemetry.wasm" "$here/c/telemetry.c"

for module in geo telemetry; do
  echo "$(sha256 "$out/$module.wasm")  $module.wasm"
done
