#!/usr/bin/env bash
# Run ONE scenario file on a fresh cluster (scripts/admin-e2e.sh scenarios calls this):
# render state, let the scenario adjust config (`setup`), start the cluster, `run` the
# checks, and always — pass or fail — save logs on failure, tear the cluster down and
# write "<passed> <failed>" to $RESULT.
#
# A scenario file may set, before its functions:
#   SCENARIO_NODES  nodes the compose file defines (default 3)
#   SCENARIO_START  nodes to start at first, e.g. "1 2 3" when NODES is 4 (default: all)
#   SCENARIO_DATA   1 = per-node data dirs (state survives a restart; default), 0 = ephemeral
# and defines `setup` (optional, runs after the state is rendered and before the cluster
# starts) and `run` (the checks).
# Usage: RESULT=<file> ADMIN_E2E_DIR=<dir> run-scenario.sh <scenario.sh>
set -u
KIT="$(cd "$(dirname "$0")" && pwd)"
SCENARIO_FILE="${1:?usage: run-scenario.sh <scenario.sh>}"
RESULT="${RESULT:?set RESULT}"

# shellcheck source=/dev/null
. "$SCENARIO_FILE"
export ADMIN_E2E_NODES="${SCENARIO_NODES:-3}"
export ADMIN_E2E_DATA="${SCENARIO_DATA:-1}"
# shellcheck source=lib.sh
. "$KIT/lib.sh"

finish() {
  local jobs_left
  jobs_left=$(jobs -p)
  [ -n "$jobs_left" ] && kill $jobs_left 2>/dev/null
  if [ "$FAIL" != 0 ]; then collect_logs; fi
  cluster_down
  echo "$PASS $FAIL" > "$RESULT"
  echo "  -> $PASS passed, $FAIL failed"
}
trap finish EXIT

prepare_state
if declare -F setup >/dev/null; then setup; fi
# shellcheck disable=SC2086
if ! cluster_up ${SCENARIO_START:-}; then
  bad "the cluster came up"
  exit 1
fi
run
