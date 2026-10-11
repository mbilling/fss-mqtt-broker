#!/bin/sh
# Fetches the compiler build.sh is pinned to: the zig release named there, as the
# archive ziglang.org publishes for this machine, checked against the sha256 recorded
# below before anything of it is unpacked or run (ADR 0086 T1).
#
#   crates/mqtt-wasm-sandbox/jq-module/fetch-zig.sh DIR
#
# Unpacks into DIR and prints the path of the zig binary on the last line, for
#   ZIG=$(…/fetch-zig.sh DIR | tail -n 1) …/build.sh
#
# The hashes are those of https://ziglang.org/download/index.json for the version.
# Changing the version in build.sh means changing the four hashes here, and
# jq.wasm.sha256 if the module's bytes move.
set -eu

here=$(cd "$(dirname "$0")" && pwd)
dest=${1:?usage: fetch-zig.sh DIR}
version=$(sed -n 's/^ZIG_VERSION=//p' "$here/build.sh")

case "$version/$(uname -s)-$(uname -m)" in
  0.16.0/Linux-x86_64)
    platform=x86_64-linux
    expected=70e49664a74374b48b51e6f3fdfbf437f6395d42509050588bd49abe52ba3d00 ;;
  0.16.0/Linux-aarch64 | 0.16.0/Linux-arm64)
    platform=aarch64-linux
    expected=ea4b09bfb22ec6f6c6ceac57ab63efb6b46e17ab08d21f69f3a48b38e1534f17 ;;
  0.16.0/Darwin-arm64)
    platform=aarch64-macos
    expected=b23d70deaa879b5c2d486ed3316f7eaa53e84acf6fc9cc747de152450d401489 ;;
  0.16.0/Darwin-x86_64)
    platform=x86_64-macos
    expected=0387557ed1877bc6a2e1802c8391953baddba76081876301c522f52977b52ba7 ;;
  *)
    echo "fetch-zig.sh: no recorded hash for zig $version on $(uname -s)-$(uname -m)" >&2
    exit 1 ;;
esac

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

mkdir -p "$dest"
archive="$dest/zig-$platform-$version.tar.xz"
curl -sSfL --retry 3 -o "$archive" \
  "https://ziglang.org/download/$version/zig-$platform-$version.tar.xz"
got=$(sha256 "$archive")
if [ "$got" != "$expected" ]; then
  echo "fetch-zig.sh: zig-$platform-$version.tar.xz: sha256 $got, expected $expected" >&2
  rm -f "$archive"
  exit 1
fi
rm -rf "$dest/zig"
mkdir -p "$dest/zig"
tar xJf "$archive" -C "$dest/zig" --strip-components 1
rm -f "$archive"
echo "zig $version ($platform), sha256 $expected" >&2
echo "$dest/zig/zig"
