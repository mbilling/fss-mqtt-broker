#!/usr/bin/env bash
# Tests scripts/roll-upgrade.sh without a cluster: its `custom` mode drives three stub
# nodes (a directory per node holding its version and whether it is ready), so what is
# checked is the roll's own logic, not ssh or Docker:
#   - nodes are upgraded one at a time, in the order given, each only after the cluster
#     is ready again;
#   - a node that does not come back stops the roll, and no node after it is touched;
#   - a target outside the adjacent-release policy (ADR 0039) is refused before
#     anything changes, and --force overrides it;
#   - a cluster that is not ready is refused before anything changes;
#   - --dry-run changes nothing.
set -euo pipefail

ROLL="$(cd "$(dirname "$0")" && pwd)/roll-upgrade.sh"
W=$(mktemp -d)
trap 'rm -rf "$W"' EXIT
fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "ok - $*"; }

# A stub node: $W/<node>/version, $W/<node>/ready (present = ready), and an upgrade log.
reset() { # reset <version> for n1 n2 n3
  rm -rf "${W:?}"/n? "$W/log"
  for n in n1 n2 n3; do mkdir -p "$W/$n"; echo "$1" > "$W/$n/version"; touch "$W/$n/ready"; done
}
# The upgrade command: record the order, write the new version; a node named in
# $W/broken comes back not ready.
UPGRADE="echo {node} >> $W/log; echo \$TARGET_V_STUB > $W/{node}/version; \
  if grep -qx {node} $W/broken 2>/dev/null; then rm -f $W/{node}/ready; fi"
READY="test -e $W/{node}/ready"
VERSION="cat $W/{node}/version"
roll() { # roll <target> [extra args...]
  local t="$1"; shift
  TARGET_V_STUB="$t" ROLL_READY_TIMEOUT=4 ROLL_SETTLE=0 "$ROLL" custom --upgrade-cmd "$UPGRADE" \
    --ready-cmd "$READY" --version-cmd "$VERSION" --target "$t" "$@" n1 n2 n3
}
export TARGET_V_STUB

# 1. The whole roll, in order.
reset 1.1.0
roll 1.2.0 > "$W/out" 2>&1 || { cat "$W/out"; fail "a healthy roll failed"; }
[ "$(tr '\n' ' ' < "$W/log")" = "n1 n2 n3 " ] || fail "upgrade order was $(cat "$W/log")"
for n in n1 n2 n3; do [ "$(cat "$W/$n/version")" = 1.2.0 ] || fail "$n not on 1.2.0"; done
pass "three nodes rolled 1.1.0 -> 1.2.0, one at a time, in order"

# 2. A node that does not come back stops the roll.
reset 1.1.0
echo n2 > "$W/broken"
if roll 1.2.0 > "$W/out" 2>&1; then fail "the roll went on past a node that did not come back"; fi
rm -f "$W/broken"
grep -q 'n2 did not come back' "$W/out" || { cat "$W/out"; fail "no explanation for the stop"; }
[ "$(tr '\n' ' ' < "$W/log")" = "n1 n2 " ] || fail "nodes touched: $(cat "$W/log")"
[ "$(cat "$W/n3/version")" = 1.1.0 ] || fail "n3 was touched after n2 failed"
pass "a node that does not come back stops the roll; the next node is not touched"

# 3. Skipping a minor is refused before anything changes; --force overrides.
reset 1.0.0
if roll 1.2.0 > "$W/out" 2>&1; then fail "1.0.0 -> 1.2.0 was not refused"; fi
grep -q 'ADR 0039' "$W/out" || fail "the refusal does not name the policy"
[ ! -e "$W/log" ] || fail "a node was touched before the refusal"
roll 1.2.0 --force > "$W/out" 2>&1 || { cat "$W/out"; fail "--force did not override"; }
pass "a skipped minor is refused untouched, and --force overrides it"

# 4. A cluster that is not ready is refused before anything changes.
reset 1.1.0
rm -f "$W/n3/ready"
if roll 1.2.0 > "$W/out" 2>&1; then fail "a not-ready cluster was rolled"; fi
[ ! -e "$W/log" ] || fail "a node was touched while the cluster was not ready"
pass "a cluster that is not ready is refused before anything changes"

# 5. --dry-run changes nothing.
reset 1.1.0
roll 1.2.0 --dry-run > "$W/out" 2>&1 || { cat "$W/out"; fail "the dry run failed"; }
[ ! -e "$W/log" ] || fail "the dry run touched a node"
grep -q 'would upgrade n3' "$W/out" || fail "the dry run did not print the plan"
pass "--dry-run prints the plan and changes nothing"

echo "roll-upgrade: all checks passed"
