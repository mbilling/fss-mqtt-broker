#!/usr/bin/env bash
# run-workload.sh <workload> [sizes...] — run one named workload's scale curve.
#
#   ./run-workload.sh market-data          # sizes 1 3 5 (the default curve)
#   ./run-workload.sh market-data 5        # one size
#   LANE_B_SUBS_OVERRIDE=480 ./run-workload.sh market-data 5   # sweep its own axis
#
# A workload is a NAMED SHAPE, not a new mechanism: each <name>.env sets the
# lane knobs run-curve.sh already has, so a workload run is reproducible from
# its file and comparable across releases. Anything exported in the caller's
# environment WINS over the file — that is how a tuning pass toggles one broker
# setting (MQTTD_*) or sweeps the workload's own axis without editing the file.
#
# MQTTD_VERSION is required by run.sh (the binary under test is a disclosure
# item, never a default) and DRIVER_COUNT defaults to 2 here: at these shapes
# two CCX33s offer the load with the drivers provably idle, which is three
# fewer paid hosts than the rig's default.
set -euo pipefail
cd "$(dirname "$0")"
WORKLOAD="${1:?usage: run-workload.sh <workload> [sizes...]}"
shift || true
FILE="./$WORKLOAD.env"
[ -f "$FILE" ] || {
	have=""
	for f in ./*.env; do have="$have $(basename "$f" .env)"; done
	echo "no such workload: $WORKLOAD (have:$have)" >&2
	exit 2
}
# The file's values are DEFAULTS: only set what the caller has not.
while IFS= read -r line; do
	case "$line" in '' | \#*) continue ;; esac
	key=${line%%=*}
	val=${line#*=}
	val=${val%\"}
	val=${val#\"}
	[ -n "${!key+x}" ] || export "${key?}=$val"
done <"$FILE"
[ -z "${WORKLOAD_NOT_IMPLEMENTED:-}" ] || {
	echo "workload '$WORKLOAD' is not runnable yet — see $FILE for what it needs" >&2
	exit 2
}
export DRIVER_COUNT="${DRIVER_COUNT:-2}"
[ $# -gt 0 ] || set -- 1 3 5
echo "workload=$WORKLOAD sizes=$* drivers=$DRIVER_COUNT mqttd=${MQTTD_VERSION:-<unset — run.sh will refuse>}" >&2
exec ../run.sh full "$@"
