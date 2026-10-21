#!/usr/bin/env bash
# Upgrade a running mqttd cluster one node at a time (a rolling upgrade, ADR 0039).
#
# Kubernetes does this already: `helm upgrade` (or the operator) rolls one pod at a time
# with a drain before each restart (docs/OPERATIONS.md, "Rolling upgrades"). This script
# is for the other deployments: systemd hosts over ssh, the Compose reference deployment,
# and anything else through two commands of your own.
#
#   scripts/roll-upgrade.sh systemd --binary ./mqttd host1 host2 host3
#   scripts/roll-upgrade.sh compose --image ghcr.io/mbilling/fss-mqtt-broker:1.2.0 \
#       --dir deploy/compose mqttd-1 mqttd-2 mqttd-3
#   scripts/roll-upgrade.sh custom --upgrade-cmd 'my-deploy {node}' \
#       --ready-cmd 'my-probe {node}' --version-cmd 'my-version {node}' --target 1.2.0 n1 n2 n3
#
# For each node, in the order given:
#   1. every node must be ready (/readyz) before anything is touched;
#   2. the node is stopped gracefully (SIGTERM: drain, flush, gossip leave — ADR 0019),
#      upgraded and started again;
#   3. the script waits until that node and every other node are ready again, and, where
#      /readyz's JSON is readable, until each one counts the whole cluster as members;
#      then all of them must STAY ready for --settle seconds (default 15) before the next
#      node is touched. /readyz can turn green before a restarted node is fully
#      readmitted (5-11 s measured, docs/OPERATIONS.md), so ready once is not enough.
# It stops at the first node that does not come back, and touches no node after it. The
# nodes already upgraded stay upgraded: a cluster may run two adjacent releases
# (ADR 0039), so the half-rolled state is supported while you look.
#
# Refused unless --force: a target that is not the same release or the next minor (or
# the next patch) of every node's current one. Skipping a minor, or crossing a major,
# is outside the version-skew policy (ADR 0039 §2, §3).
#
# --dry-run prints the plan and the preflight, and changes nothing.
#
# Rollback, per mode: systemd keeps the previous binary as /usr/local/bin/mqttd.prev on
# each host (`--rollback` restores it the same way, one node at a time); compose rolls
# back by running again with the previous --image; custom is yours.
set -euo pipefail

usage() {
  sed -n '2,/^set -euo/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

MODE="${1:-}"
[ -n "$MODE" ] || usage
shift
case "$MODE" in -h | --help) usage 0 ;; systemd | compose | custom) ;; *) echo "unknown mode: $MODE" >&2; usage ;; esac

BINARY="" IMAGE="" DIR="." TARGET="" FORCE=0 DRY=0 ROLLBACK=0
SSH_OPTS="${ROLL_SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=10}"
SSH_USER="" INSTALL_PATH="/usr/local/bin/mqttd" UNIT="mqttd" HEALTH="127.0.0.1:8080"
UPGRADE_CMD="" READY_CMD="" VERSION_CMD=""
READY_TIMEOUT="${ROLL_READY_TIMEOUT:-300}"
SETTLE="${ROLL_SETTLE:-15}"
NODES=()
while [ $# -gt 0 ]; do
  case "$1" in
    --binary) BINARY="$2"; shift 2 ;;
    --image) IMAGE="$2"; shift 2 ;;
    --dir) DIR="$2"; shift 2 ;;
    --target) TARGET="$2"; shift 2 ;;
    --ssh-user) SSH_USER="$2"; shift 2 ;;
    --install-path) INSTALL_PATH="$2"; shift 2 ;;
    --unit) UNIT="$2"; shift 2 ;;
    --health) HEALTH="$2"; shift 2 ;;
    --upgrade-cmd) UPGRADE_CMD="$2"; shift 2 ;;
    --ready-cmd) READY_CMD="$2"; shift 2 ;;
    --version-cmd) VERSION_CMD="$2"; shift 2 ;;
    --ready-timeout) READY_TIMEOUT="$2"; shift 2 ;;
    --settle) SETTLE="$2"; shift 2 ;;
    --rollback) ROLLBACK=1; shift ;;
    --force) FORCE=1; shift ;;
    --dry-run) DRY=1; shift ;;
    -h | --help) usage 0 ;;
    -*) echo "unknown option: $1" >&2; usage ;;
    *) NODES+=("$1"); shift ;;
  esac
done
[ "${#NODES[@]}" -gt 0 ] || { echo "name the nodes to upgrade, in order" >&2; exit 2; }

say() { printf '==> %s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die() { printf 'FAIL %s\n' "$*" >&2; exit 1; }

# ---- per-mode primitives: run on a node, its version, its readiness -----------------

# On the host: `sudo` only when not already root (a root login may have no sudo).
AS_ROOT='S=; [ "$(id -u)" -eq 0 ] || S=sudo'

on_host() { # systemd: run a command on host $1
  local host="$1"; shift
  # shellcheck disable=SC2086
  ssh $SSH_OPTS "${SSH_USER:+$SSH_USER@}$host" "$@"
}

compose() { (cd "$DIR" && docker compose "$@"); }

fill() { printf '%s' "${1//\{node\}/$2}"; } # custom: substitute {node}

node_version() { # the version the node's installed binary reports, e.g. 1.1.0
  local v
  case "$MODE" in
    systemd) v=$(on_host "$1" "$INSTALL_PATH --version" 2>/dev/null) ;;
    compose) v=$(compose exec -T "$1" mqttd --version 2>/dev/null) ;;
    custom) [ -n "$VERSION_CMD" ] || { echo unknown; return; }; v=$(sh -c "$(fill "$VERSION_CMD" "$1")" 2>/dev/null) ;;
  esac
  printf '%s' "$v" | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || true
}

readyz_body() { # /readyz's JSON where it can be read, else nothing
  case "$MODE" in
    systemd) on_host "$1" "curl -s --max-time 3 http://$HEALTH/readyz" 2>/dev/null || true ;;
    *) true ;;
  esac
}

node_ready() { # exit 0 when the node's own /readyz says ready
  case "$MODE" in
    systemd) on_host "$1" "$INSTALL_PATH --probe /readyz" >/dev/null 2>&1 ;;
    compose) compose exec -T "$1" mqttd --probe /readyz >/dev/null 2>&1 ;;
    custom) sh -c "$(fill "$READY_CMD" "$1")" >/dev/null 2>&1 ;;
  esac
}

members_of() { # members the node counts, where /readyz's JSON is readable
  readyz_body "$1" | grep -Eo '"members":[0-9]+' | head -1 | cut -d: -f2
}

# ---- preflight ------------------------------------------------------------------------

target_version() {
  if [ -n "$TARGET" ]; then printf '%s' "$TARGET"; return; fi
  case "$MODE" in
    systemd)
      [ "$ROLLBACK" = 1 ] && { node_version_prev "${NODES[0]}"; return; }
      [ -n "$BINARY" ] || die "--binary is required for systemd mode"
      [ -f "$BINARY" ] || die "no such binary: $BINARY"
      # Linux binaries do not run on a macOS operator host: ask the first node instead.
      "$BINARY" --version 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1 && return
      scp_to "${NODES[0]}" "$BINARY" /tmp/mqttd.roll-check >/dev/null
      on_host "${NODES[0]}" "chmod 0755 /tmp/mqttd.roll-check && /tmp/mqttd.roll-check --version; rm -f /tmp/mqttd.roll-check" \
        | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1 ;;
    compose)
      [ -n "$IMAGE" ] || die "--image is required for compose mode"
      docker run --rm "$IMAGE" --version 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1 ;;
    custom) die "custom mode needs --target <version>" ;;
  esac
}

node_version_prev() {
  on_host "$1" "$INSTALL_PATH.prev --version" 2>/dev/null | grep -Eo '[0-9]+\.[0-9]+\.[0-9]+' | head -1 \
    || die "$1 has no $INSTALL_PATH.prev to roll back to"
}

scp_to() { # scp_to host local remote
  # shellcheck disable=SC2086
  scp -q $SSH_OPTS "$2" "${SSH_USER:+$SSH_USER@}$1:$3"
}

# ADR 0039: the target may equal the current release, or be its next patch or next minor.
# A rollback is the same check in the other direction.
adjacent() {
  local from="$1" to="$2"
  [ "$from" = "$to" ] && return 0
  local fM fm tM tm
  IFS=. read -r fM fm _ <<<"$from"
  IFS=. read -r tM tm _ <<<"$to"
  [ "$fM" = "$tM" ] || return 1
  local d=$((tm - fm))
  [ "$d" -ge -1 ] && [ "$d" -le 1 ]
}

all_ready() { # prints what is not ready yet; empty when every node is ready
  local want="${#NODES[@]}" n m pending=""
  for n in "${NODES[@]}"; do
    if ! node_ready "$n"; then pending="$pending $n(not ready)"; continue; fi
    m=$(members_of "$n")
    if [ -n "$m" ] && [ "$m" -lt "$want" ]; then pending="$pending $n(members $m/$want)"; fi
  done
  printf '%s' "$pending"
}

wait_ready() { # every node ready (and counting all members), then staying so for SETTLE s
  local deadline=$((SECONDS + READY_TIMEOUT)) pending stable_since=""
  while :; do
    pending=$(all_ready)
    if [ -z "$pending" ]; then
      [ -n "$stable_since" ] || stable_since=$SECONDS
      [ $((SECONDS - stable_since)) -ge "$SETTLE" ] && return 0
    else
      stable_since=""
    fi
    [ "$SECONDS" -ge "$deadline" ] && { echo "still waiting on:${pending:- (not yet stable for ${SETTLE}s)}" >&2; return 1; }
    sleep 3
  done
}

TARGET_V=$(target_version)
[ -n "$TARGET_V" ] || die "could not read the target version"
say "mode $MODE, ${#NODES[@]} node(s): ${NODES[*]}; target $TARGET_V"

# Indexed like NODES (bash 3.2, the macOS default, has no associative arrays).
CURRENT=()
for n in "${NODES[@]}"; do
  v=$(node_version "$n")
  [ -n "$v" ] || v=unknown
  CURRENT+=("$v")
  say "  $n runs $v"
  if [ "$v" != unknown ] && ! adjacent "$v" "$TARGET_V"; then
    if [ "$FORCE" = 1 ]; then
      say "  WARNING: $v -> $TARGET_V is outside the adjacent-release policy (ADR 0039); --force given"
    else
      die "$n: $v -> $TARGET_V skips a minor or crosses a major (ADR 0039: adjacent releases only; gateway minor for a major). Roll through the releases in between, or pass --force"
    fi
  fi
done

say "preflight: every node ready before anything is touched"
wait_ready || die "the cluster is not ready; fix that first (nothing was changed)"

if [ "$DRY" = 1 ]; then
  for k in "${!NODES[@]}"; do say "  would upgrade ${NODES[$k]} (${CURRENT[$k]} -> $TARGET_V), then wait for the cluster"; done
  say "dry run: nothing changed"
  exit 0 # NOT-A-SKIP: --dry-run asked for the plan only; the preflight above ran in full
fi

# ---- the roll -------------------------------------------------------------------------

upgrade_node() {
  local n="$1"
  case "$MODE" in
    systemd)
      if [ "$ROLLBACK" = 1 ]; then
        on_host "$n" "set -e; $AS_ROOT; \$S install -m 0755 $INSTALL_PATH.prev $INSTALL_PATH.new && \$S mv $INSTALL_PATH.new $INSTALL_PATH && \$S systemctl restart $UNIT"
      else
        scp_to "$n" "$BINARY" /tmp/mqttd.roll
        # Checked on the host before it replaces anything: a binary that does not run there
        # (wrong architecture, truncated copy) must not take the node down.
        on_host "$n" "set -e; $AS_ROOT; chmod 0755 /tmp/mqttd.roll; /tmp/mqttd.roll --version | grep -q '$TARGET_V'; \
          \$S cp -p $INSTALL_PATH $INSTALL_PATH.prev; \
          \$S install -m 0755 /tmp/mqttd.roll $INSTALL_PATH.new && \$S mv $INSTALL_PATH.new $INSTALL_PATH; \
          rm -f /tmp/mqttd.roll; \$S systemctl restart $UNIT"
      fi ;;
    compose)
      # Recreates only this service on the new image; stop_grace_period covers the
      # graceful shutdown. Put MQTTD_IMAGE in $DIR/.env afterwards, or a later plain
      # `docker compose up -d` would recreate every node on the old default.
      # --force-recreate: without it, a node already on --image is left running and the
      # roll would report it ready without having restarted it.
      (cd "$DIR" && MQTTD_IMAGE="$IMAGE" docker compose up -d --no-deps --force-recreate "$n") ;;
    custom)
      sh -c "$(fill "$UPGRADE_CMD" "$n")" ;;
  esac
}

for k in "${!NODES[@]}"; do
  n="${NODES[$k]}"
  i=$((k + 1))
  say "[$i/${#NODES[@]}] $n: ${CURRENT[$k]} -> $TARGET_V"
  upgrade_node "$n" || die "$n: the upgrade command failed. Nodes after it were not touched"
  say "[$i/${#NODES[@]}] $n: waiting for the whole cluster to be ready again (up to ${READY_TIMEOUT}s)"
  if ! wait_ready; then
    die "$n did not come back, or the cluster did not settle. Stopped here; nodes after it were not touched. Look at $n's log; to undo it: $(case "$MODE" in systemd) echo "scripts/roll-upgrade.sh systemd --rollback $n" ;; compose) echo "run again with the previous --image for $n" ;; *) echo "your own rollback for $n" ;; esac)"
  fi
  v=$(node_version "$n")
  if [ -n "$v" ] && [ "$v" != "$TARGET_V" ]; then
    die "$n reports $v after the upgrade, not $TARGET_V. Stopped here"
  fi
  say "[$i/${#NODES[@]}] $n: ready on ${v:-$TARGET_V}"
done

say "done: ${#NODES[@]} node(s) on $TARGET_V"
[ "$MODE" = compose ] && say "remember: set MQTTD_IMAGE=$IMAGE in $DIR/.env so a later 'docker compose up -d' keeps it"
exit 0
