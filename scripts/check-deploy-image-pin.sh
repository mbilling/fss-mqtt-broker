#!/usr/bin/env bash
# The gate that keeps issue #263's failure class dead: deploy/compose ships artifacts
# that pass CLI flags to the image tag they default to, and nothing used to prove those
# flags exist in that tag's binary. The published default (`:latest` = v0.9.0) predated
# `--probe` and `--hash-password`, so the reference deployment's healthcheck could never
# pass and bootstrap.sh wrote a corrupt password file — and no lane noticed, because
# every lane overrode the image.
#
# Three properties, checked on every PR (no docker, no network — pure git):
#
#   1. THE PIN IS A PIN. The compose default and bootstrap.sh's fallback must be the
#      SAME `ghcr.io/...:vX.Y.Z` reference — an exact release tag, never `latest` or any
#      other floating name. A floating tag is exactly how the skew arrived unnoticed.
#   2. THE FLAG LIST IS CLOSED. Every `--flag` these artifacts hand to an mqttd
#      invocation must be in REQUIRED_FLAGS below, so a new flag added to the artifacts
#      without extending this gate fails here rather than shipping unchecked.
#   3. THE TAG HAS THE FLAGS. If the pinned tag exists in this clone, each flag must
#      appear in `git show TAG:crates/mqttd/src/main.rs` — the binary at that release
#      parses it. If the tag is NOT yet released, the pin must be forward-looking
#      (strictly newer than every existing v* tag, so a stale pin cannot hide as
#      "pending") and the flags must exist in the WORKING TREE's main.rs (so releasing
#      HEAD satisfies the pin) — printed as a loud notice, because the nightly
#      default-image lane stays in loud-skip until the tag is published.
#
# CI must fetch tags for property 3's released branch to engage (actions/checkout is
# tagless by default); the workflow step does `git fetch --tags`.
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE=deploy/compose/compose.yaml
BOOTSTRAP=deploy/compose/bootstrap.sh
MAIN_RS=crates/mqttd/src/main.rs

# The mqttd flags the compose artifacts pass to the default image. Extend this list in
# the same change that adds a flag to the artifacts — check (2) makes forgetting loud.
REQUIRED_FLAGS=(--probe --hash-password)

fail() { echo "FAIL — $1" >&2; exit 1; }
ok() { echo "  ok   — $1"; }

# ── 1. one pin, and actually pinned ──────────────────────────────────────────────────
compose_ref="$(grep -oE 'MQTTD_IMAGE:-[^}]+' "$COMPOSE" | head -1 | cut -d- -f2-)"
bootstrap_ref="$(grep -oE 'MQTTD_IMAGE:-[^}]+' "$BOOTSTRAP" | head -1 | cut -d- -f2-)"
[[ -n "$compose_ref" ]] || fail "no MQTTD_IMAGE default found in $COMPOSE"
[[ -n "$bootstrap_ref" ]] || fail "no MQTTD_IMAGE default found in $BOOTSTRAP"
[[ "$compose_ref" == "$bootstrap_ref" ]] \
  || fail "the defaults disagree: $COMPOSE says '$compose_ref', $BOOTSTRAP says '$bootstrap_ref'"
TAG="${compose_ref##*:}"
[[ "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] \
  || fail "the default image tag '$TAG' is not an exact release tag (vX.Y.Z) — a floating tag is how issue #263 happened"
ok "one pinned default: $compose_ref"

# ── 2. the flag list is closed ───────────────────────────────────────────────────────
# Flags handed to an mqttd invocation in the artifacts: the healthcheck's exec-form
# array, and bootstrap.sh's `"$IMAGE" --flag` / `mqttd --flag` call sites. Flags on the
# docker CLI itself (e.g. `docker run --rm`) sit BEFORE the image token and are excluded
# by taking only what follows an mqttd/image token on the line.
used_flags="$(
  { grep -oE '/usr/local/bin/mqttd[^]]*' "$COMPOSE" || true
    grep -ohE '(\$IMAGE"|MQTTD_BIN"|mqttd) +--[a-z][a-z-]*' "$BOOTSTRAP" || true
  } | grep -oE -- '--[a-z][a-z-]*' | sort -u
)"
for f in $used_flags; do
  found=0
  for r in "${REQUIRED_FLAGS[@]}"; do [[ "$f" == "$r" ]] && found=1; done
  [[ $found -eq 1 ]] \
    || fail "the artifacts use '$f' but this gate does not check it — add it to REQUIRED_FLAGS"
done
ok "every artifact flag is on the checked list ($(echo "$used_flags" | tr '\n' ' '))"

# ── 3. the tag's binary parses every flag ────────────────────────────────────────────
if git rev-parse -q --verify "refs/tags/$TAG" >/dev/null; then
  for f in "${REQUIRED_FLAGS[@]}"; do
    git show "$TAG:$MAIN_RS" | grep -q -- "\"$f\"" \
      || fail "the pinned tag $TAG does not parse '$f' ($MAIN_RS at that tag) — the artifacts would break against their own default image"
  done
  ok "released: $TAG parses every checked flag"
else
  newest="$(git tag --list 'v[0-9]*' | sort -V | tail -1)"
  if [[ -n "$newest" ]]; then
    top="$(printf '%s\n%s\n' "$newest" "$TAG" | sort -V | tail -1)"
    [[ "$top" == "$TAG" && "$TAG" != "$newest" ]] \
      || fail "the pinned tag $TAG is not newer than the newest release ($newest) yet does not exist — a stale or bogus pin"
  fi
  for f in "${REQUIRED_FLAGS[@]}"; do
    grep -q -- "\"$f\"" "$MAIN_RS" \
      || fail "'$f' is not parsed by the working tree's $MAIN_RS — releasing HEAD cannot satisfy the pin"
  done
  echo "NOTICE — the pinned tag $TAG is not released yet: the pin is forward-looking and"
  echo "         HEAD parses every checked flag, so pushing the $TAG release tag makes it"
  echo "         real. Until then the nightly default-image compose lane skips loudly."
fi

echo "deploy image pin: OK"
