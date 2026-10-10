#!/bin/sh
# Builds jq.wasm: EMQX's jq (jq 1.8.1 + jq_cancel) with its bundled oniguruma and
# decNumber, behind the rule-function ABI in shim.c, for wasm32-wasi (ADR 0086 spike).
#
#   crates/mqtt-wasm-sandbox/jq-module/build.sh [OUT.wasm]
#
# Needs: zig 0.16.0 (its clang and its bundled wasi-libc are the whole toolchain),
# curl, tar, shasum/sha256sum. Sources are fetched as tarballs of pinned commits and
# checked against the hashes below. The build is byte-reproducible: same zig version,
# same sources, same flags give the same file on any host (measured: macOS arm64, and
# Linux arm64 and amd64 in a container; see the ADR). The last line printed is the
# file's sha256; `jq-module/jq.wasm.sha256` records the one this revision must give.
set -eu

ZIG_VERSION=0.16.0
# emqx/jqc branch jq-1.8-emqx, the commit emqx/jq v0.4.1 builds: jq 1.8.1 (jqlang/jq
# 4467af7) plus jq_cancel.
JQC_COMMIT=4c60b10c8db2bc83b24dbc46f1e1858a7e23689e
JQC_SHA256=761ab9d986ef53d1aa7a03328e6dfe748b607de41941b412ff97cc2b171fd4da
# kkos/oniguruma at the commit jq 1.8.1's vendor/oniguruma submodule names.
ONIG_COMMIT=4ef89209a239c1aea328cf13c05a2807e5c146d1
ONIG_SHA256=70bfed97ee8390f5ac08fea28e3e930a3b33df871c6fc1888c8d436c6c6b755d

here=$(cd "$(dirname "$0")" && pwd)
out=${1:-$here/jq.wasm}
work=${JQ_WASM_WORK:-${TMPDIR:-/tmp}/mqttd-jq-wasm}
zig=${ZIG:-zig}

have=$("$zig" version)
if [ "$have" != "$ZIG_VERSION" ]; then
  echo "build.sh: zig $ZIG_VERSION is the pinned toolchain, found $have" >&2
  exit 1
fi

sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$1" | cut -d' ' -f1
  else shasum -a 256 "$1" | cut -d' ' -f1; fi
}

fetch() { # repo commit sha256 dir
  if [ ! -f "$work/$4.tar.gz" ] || [ "$(sha256 "$work/$4.tar.gz")" != "$3" ]; then
    curl -sSfL -o "$work/$4.tar.gz" "https://codeload.github.com/$1/tar.gz/$2"
  fi
  got=$(sha256 "$work/$4.tar.gz")
  if [ "$got" != "$3" ]; then
    echo "build.sh: $1@$2: sha256 $got, expected $3" >&2
    exit 1
  fi
  rm -rf "${work:?}/$4"
  mkdir -p "$work/$4"
  tar xzf "$work/$4.tar.gz" -C "$work/$4" --strip-components 1
}

mkdir -p "$work"
fetch emqx/jqc "$JQC_COMMIT" "$JQC_SHA256" jqc
fetch kkos/oniguruma "$ONIG_COMMIT" "$ONIG_SHA256" onig
cd "$work"

# The three files jq's Makefile generates.
mkdir -p gen/src
od -v -A n -t o1 -- jqc/src/builtin.jq |
  sed -e 's/$/ /' -e 's/\([0123456789]\) /\1, /g' -e 's/ $//' -e 's/ 0/  0/g' \
      -e 's/ \([123456789]\)/ 0\1/g' >gen/src/builtin.inc
echo '#define JQ_VERSION "1.8.1"' >gen/src/version.h
cat >gen/config.h <<'EOF'
/* oniguruma's config.h for wasm32-wasi. */
#define HAVE_ALLOCA 1
#define HAVE_ALLOCA_H 1
#define HAVE_STDINT_H 1
#define HAVE_SYS_TIME_H 1
#define HAVE_SYS_TYPES_H 1
#define HAVE_UNISTD_H 1
#define HAVE_INTTYPES_H 1
#define PACKAGE "onig"
#define PACKAGE_VERSION "6.9.10"
#define VERSION "6.9.10"
#define SIZEOF_INT 4
#define SIZEOF_LONG 4
#define SIZEOF_LONG_LONG 8
#define SIZEOF_VOIDP 4
EOF

# What jq's configure finds on Linux, where EMQX builds it: every libm function, the
# time functions, decNumber and oniguruma. `gamma` and the pthread calls are not in
# wasi-libc; compat.c supplies them.
MATH="ACOS ACOSH ASIN ASINH ATAN2 ATAN ATANH CBRT CEIL COPYSIGN COS COSH DREM ERF ERFC
EXP10 EXP2 EXP EXPM1 FABS FDIM FLOOR FMA FMAX FMIN FMOD FREXP GAMMA HYPOT J0 J1 JN
LDEXP LGAMMA LOG10 LOG1P LOG2 LOG LOGB MODF LGAMMA_R NEARBYINT NEXTAFTER NEXTTOWARD
POW REMAINDER RINT ROUND SCALB SCALBLN SIGNIFICAND SCALBN ILOGB SIN SINH SQRT TAN TANH
TGAMMA TRUNC Y0 Y1 YN"
OTHER="MEMMEM ISATTY STRPTIME STRFTIME SETENV TIMEGM GMTIME_R GMTIME LOCALTIME_R
LOCALTIME GETTIMEOFDAY TM_TM_GMT_OFF SETLOCALE PTHREAD_KEY_CREATE PTHREAD_ONCE ATEXIT
LIBONIG"
defs="-DUSE_DECNUM=1 -DIEEE_8087=1 -D_GNU_SOURCE"
for f in $MATH $OTHER; do defs="$defs -DHAVE_$f=1"; done

# The WebAssembly features the module may use, spelled out (not `generic`, which moves
# with the compiler): bulk memory makes memcpy and memset one instruction each, which an
# interpreter runs natively. All four are on in wasmi by default.
FEATURES=mvp+bulk_memory+sign_ext+nontrapping_fptoint+mutable_globals

# No timestamps, no host paths, no build id: nothing of the build machine in the file.
# decNumber includes <signal.h> (it can raise SIGFPE on a trapped condition; jq sets no
# traps), which wasi-libc offers only as an emulation.
CFLAGS="-target wasm32-wasi -mcpu=$FEATURES -O2 -DNDEBUG -fno-ident -D_WASI_EMULATED_SIGNAL
  -ffile-prefix-map=$work=. -Wno-everything"

objs=""
cc() { # name, source, extra flags...
  o="obj/$1.o"
  src=$2
  shift 2
  # shellcheck disable=SC2086
  "$zig" cc $CFLAGS "$@" -c "$src" -o "$o"
  objs="$objs $o"
}
rm -rf obj
mkdir -p obj

for f in builtin bytecode compile execute jv jv_alloc jv_aux jv_dtoa jv_file jv_parse \
         jv_print jv_unicode linker locfile util jv_dtoa_tsd lexer parser; do
  # shellcheck disable=SC2086
  cc "jq_$f" "jqc/src/$f.c" $defs -Igen -Igen/src -Ijqc -Ijqc/src -Ijqc/vendor -Ionig/src \
     -include "$here/compat.h"
done
for f in decContext decNumber; do
  cc "dec_$f" "jqc/vendor/decNumber/$f.c" -Ijqc/vendor/decNumber
done
# libonig_la_SOURCES of oniguruma's src/Makefile.am, without the POSIX API.
for f in regparse regcomp regexec regenc regerror regext regsyntax regtrav regversion st \
         reggnu unicode unicode_unfold_key unicode_fold1_key unicode_fold2_key \
         unicode_fold3_key ascii utf8 utf16_be utf16_le utf32_be utf32_le euc_jp \
         euc_jp_prop sjis sjis_prop iso8859_1 iso8859_2 iso8859_3 iso8859_4 iso8859_5 \
         iso8859_6 iso8859_7 iso8859_8 iso8859_9 iso8859_10 iso8859_11 iso8859_13 \
         iso8859_14 iso8859_15 iso8859_16 euc_tw euc_kr big5 gb18030 koi8_r cp1251 \
         onig_init; do
  cc "onig_$f" "onig/src/$f.c" -Igen -Ionig/src
done
cc compat "$here/compat.c"
cc shim "$here/shim.c" -Ijqc/src

# A reactor: no main, `_initialize` runs the constructors, the exports are the ABI.
# shellcheck disable=SC2086
"$zig" cc $CFLAGS -mexec-model=reactor -Wl,--strip-all -Wl,--no-entry \
  -Wl,-z,stack-size=1048576 -Wl,--initial-memory=2097152 \
  $objs -lwasi-emulated-signal -o "$out"

echo "$(wc -c <"$out" | tr -d ' ') bytes"
echo "$(sha256 "$out")  $(basename "$out")"
