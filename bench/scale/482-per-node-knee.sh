#!/usr/bin/env bash
# #482 / #613: N=7 vs N=5 per-node knee on ONE provisioning — the A/B/A.
#
#   set -a && . ./482-per-node-knee.env && set +a
#   PREFLIGHT_ONLY=1 ./482-per-node-knee.sh    # offline: all three arms' shapes
#   ./482-per-node-knee.sh                     # PAID: provisions 7 + drivers once
#
# Arm 1  run.sh provisions KNEE_FULL brokers (7) and runs KNEE_LADDER_FULL.
# Arm 2  resize-cluster.sh re-forms the SAME hosts as KNEE_SMALL nodes (5; the
#        rest left stopped), bootstrap-cluster.sh starts them, KNEE_LADDER_SMALL.
# Arm 3  back to KNEE_FULL on the same hosts, KNEE_LADDER_CLOSE: the drift
#        control. (482-knee-smoke.sh runs the same three arms at 3 / 1 / 3.)
#
# Every arm is a fresh cluster (new PKI, empty stores, founder-first) with its
# own forwarding canary and calibration, so each is certified exactly as a
# run.sh size is. Only the hardware is shared — which is the point: two
# provisionings differ by ~40%, one provisioning repeats within ~1%.
#
# Teardown is trapped here, not in run.sh: arm 1 has to run with KEEP_INFRA=1 so
# the cluster outlives it, which disables run.sh's own destroy. Any exit from
# this script — success, failure, Ctrl-C — runs teardown.sh.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

: "${KNEE_FULL:?source 482-per-node-knee.env first}"
: "${KNEE_SMALL:?source 482-per-node-knee.env first}"
: "${KNEE_LADDER_FULL:?source 482-per-node-knee.env first}"
: "${KNEE_LADDER_SMALL:?source 482-per-node-knee.env first}"
: "${KNEE_LADDER_CLOSE:?source 482-per-node-knee.env first}"
[ "$KNEE_SMALL" -lt "$KNEE_FULL" ] || die "KNEE_SMALL ($KNEE_SMALL) must be below KNEE_FULL ($KNEE_FULL)"
[ "${LANES:-}" = E ] || die "LANES must be E for this campaign (got '${LANES:-}')"
[ "${KEEP_INFRA:-0}" = 0 ] || die "do not set KEEP_INFRA — this script manages the cluster's lifetime itself"
[ -z "${RUN_DIR:-}" ] || die "do not set RUN_DIR — each arm gets its own run dir under one campaign dir"

if [ "${PREFLIGHT_ONLY:-0}" = 1 ]; then
	# run.sh's preflight writes into a scratch run dir; the three calls must not
	# share one or the second would read as a resume of the first.
	pre="$(mktemp -d "${TMPDIR:-/tmp}/knee-preflight.XXXXXX")"
	for arm in "$KNEE_FULL:$KNEE_LADDER_FULL" "$KNEE_SMALL:$KNEE_LADDER_SMALL" "$KNEE_FULL:$KNEE_LADDER_CLOSE"; do
		n="${arm%%:*}" ladder="${arm#*:}"
		say "preflight: $n nodes, ladder: $ladder"
		RUN_DIR="$pre/$n-$(echo "$ladder" | tr ' ' '_')" LANE_E_SITES_OVERRIDE="$ladder" \
			"$SCALE_DIR/run.sh" full "$n"
	done
	say "all three arms' shapes valid; no cloud calls made (scratch: $pre)"
	exit 0
fi

CAMPAIGN="$SCALE_DIR/.runs/knee-$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$CAMPAIGN"
say "campaign dir: $CAMPAIGN"

finish() {
	local rc=$?
	trap - EXIT INT TERM
	# A failed arm's evidence dies with its servers — pull it off the hosts first,
	# as run.sh's own trap would have had KEEP_INFRA not switched it off.
	# An arm that died before its own inventory existed (a failed resize) is
	# collected against the whole provisioning instead.
	local inv="${ARM_INV:-}"
	[ -f "$inv" ] || inv="${FULL_INV:-}"
	if [ "$rc" -ne 0 ] && [ -n "${ARM_DIR:-}" ] && [ -f "$inv" ]; then
		say "capturing failure evidence off the hosts before destroying them"
		mkdir -p "$ARM_DIR"
		"$SCALE_DIR/collect.sh" "$ARM_DIR" "$inv" || true
	fi
	say "tearing down the provisioning (exit code was $rc)"
	if ! "$SCALE_DIR/teardown.sh" >>"$CAMPAIGN/teardown.log" 2>&1; then
		tail -20 "$CAMPAIGN/teardown.log" >&2
		warn "teardown FAILED — recover with: CLOUD=${CLOUD:-hcloud} bench/scale/teardown.sh (then --force in the dedicated project)"
		exit 1
	fi
	say "provisioning destroyed and audited (log: $CAMPAIGN/teardown.log)"
	exit "$rc"
}
trap finish EXIT INT TERM

# run.sh's per-size tail (run-curve, collect, observe) for an arm that
# resize-cluster.sh + bootstrap-cluster.sh brought up instead of run.sh.
resized_arm() { # resized_arm <size> <arm-dir> <ladder>
	local n="$1" dir="$2" ladder="$3"
	say "════ arm $(basename "$dir"): $n nodes on the same hosts ════"
	ARM_DIR="$dir" ARM_INV="$dir/inventory-$n.json"
	"$SCALE_DIR/resize-cluster.sh" "$FULL_INV" "$n" "$dir"
	"$SCALE_DIR/bootstrap-cluster.sh" "$dir" "$dir/inventory-$n.json" durable
	if [ "${OBSERVE:-1}" = 1 ]; then
		"$SCALE_DIR/observe.sh" attach "$dir" "$dir/inventory-$n.json" || warn "observe attach failed — continuing unobserved"
	fi
	LANE_E_SITES_OVERRIDE="$ladder" "$SCALE_DIR/run-curve.sh" "$dir" "$dir/inventory-$n.json"
	"$SCALE_DIR/collect.sh" "$dir" "$dir/inventory-$n.json"
	if [ "${OBSERVE:-1}" = 1 ]; then
		"$SCALE_DIR/observe.sh" detach || true
	fi
}

A1="1-n$KNEE_FULL" A2="2-n$KNEE_SMALL" A3="3-n$KNEE_FULL-close"
ARM_DIR="$CAMPAIGN/$A1" ARM_INV="$CAMPAIGN/$A1/inventory-$KNEE_FULL.json"
say "════ arm $A1: provision $KNEE_FULL brokers + ${DRIVER_COUNT:-?} drivers, ladder: $KNEE_LADDER_FULL ════"
KEEP_INFRA=1 RUN_DIR="$ARM_DIR" LANE_E_SITES_OVERRIDE="$KNEE_LADDER_FULL" \
	"$SCALE_DIR/run.sh" full "$KNEE_FULL"
FULL_INV="$ARM_INV"
[ -f "$FULL_INV" ] || die "arm $A1 left no inventory at $FULL_INV"

resized_arm "$KNEE_SMALL" "$CAMPAIGN/$A2" "$KNEE_LADDER_SMALL"
resized_arm "$KNEE_FULL" "$CAMPAIGN/$A3" "$KNEE_LADDER_CLOSE"

say "all three arms complete — gate each before reading any number:"
for arm in "$A1" "$A2" "$A3"; do
	echo "  python3 extract-lane-e.py --crossing-gate 0.5 $CAMPAIGN/$arm/results" >&2
done
