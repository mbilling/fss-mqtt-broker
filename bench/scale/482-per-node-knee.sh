#!/usr/bin/env bash
# Per-node knee across cluster sizes on ONE provisioning (#482 / #613).
#
#   set -a && . ./knee-3-5-7-10.env && set +a     # or 482-per-node-knee.env
#   PREFLIGHT_ONLY=1 ./482-per-node-knee.sh       # offline: every arm's shape
#   ./482-per-node-knee.sh                        # PAID: provisions the first arm once
#
# KNEE_ARMS is a ';'-separated list of arms, each `<brokers>:<drivers>:<ladder>`,
# optionally followed by `:<broker env>` — comma-separated KEY=VALUE lines added
# to that arm's mqttd env after any global EXTRA_BROKER_ENV (so they win):
#
#   KNEE_ARMS="10:20:1 10 20 30; 7:14:1 7 14 21; 10:20:1 20"
#   KNEE_ARMS="3:8:3 6 9:MQTTD_REPLICA_STORE=redb; 3:8:3 6 9:MQTTD_REPLICA_STORE=log"
#   KNEE_ARMS="3:8:1 16:RIG_NIC_SPREAD=off; 3:8:1 16:RIG_NIC_SPREAD=on"
#
# RIG_NIC_SPREAD=on|off is one of the two non-MQTTD_ keys: it sets RPS/RFS on every
# broker live, before the arm (set_nic_spread), so the #505 softirq spread can be
# compared on one provisioning. Arm 1 runs as the hosts booted, so its value must
# match BROKER_NIC_SPREAD.
#
# RIG_BINARY=main|alt swaps every broker's binary in place before the arm
# (swap-binary.sh), so two builds are compared on one provisioning: `alt` is
# ALT_MQTTD_URL/ALT_MQTTD_SHA256, `main` is MQTTD_URL/MQTTD_SHA256. Arm 1 is main.
#
# Every arm starts from empty stores, so a store-changing variable is safe to
# vary per arm: that is how two configurations are compared on the SAME
# hardware (two provisionings differ by ~40%; the disks alone by 3.5x).
#
# The FIRST arm is provisioned by run.sh at its size, with DRIVER_COUNT drivers
# (which must equal its <drivers>), and is the provisioning every later arm
# re-forms: resize-cluster.sh keeps the first <brokers> brokers and the first
# <drivers> drivers, bootstrap-cluster.sh starts a fresh cluster on them. The
# last arm repeating the first arm's size is the drift control. Arm k runs in
# <campaign>/<k>-n<brokers>. (482-knee-smoke.sh runs 3 / 1 / 3 on small hosts.)
#
# A pinned driver is swapped for a fresh server in place (replace-node.sh) and
# the rung it spoiled runs again; a pinned broker is swapped before its arm
# measures anything, and that arm re-forms once.
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
require_modern_bash

: "${KNEE_ARMS:?source a knee env first (KNEE_ARMS=<brokers>:<drivers>:<ladder>;...)}"
# Parse once, refuse early: a malformed arm must not surface after provisioning.
ARM_N=() ARM_D=() ARM_L=() ARM_E=() ARM_RPS=() ARM_BIN=()
IFS=';' read -r -a _arms <<<"$KNEE_ARMS"
for _a in "${_arms[@]}"; do
	_a="$(echo "$_a" | sed 's/^ *//; s/ *$//')"
	[ -n "$_a" ] || continue
	IFS=':' read -r _n _d _l _e <<<"$_a"
	[[ "$_n" =~ ^[1-9][0-9]*$ && "$_d" =~ ^[1-9][0-9]*$ && -n "${_l// /}" ]] ||
		die "KNEE_ARMS: '$_a' is not <brokers>:<drivers>:<ladder>[:<broker env>]"
	_env="" _rps="" _bin=""
	if [ -n "${_e// /}" ]; then
		IFS=',' read -r -a _kvs <<<"$_e"
		for _kv in "${_kvs[@]}"; do
			_kv="$(echo "$_kv" | sed 's/^ *//; s/ *$//')"
			# RIG_NIC_SPREAD and RIG_BINARY are the only non-broker keys: host settings, not env.
			if [[ "$_kv" =~ ^RIG_NIC_SPREAD=(on|off)$ ]]; then
				_rps="${BASH_REMATCH[1]}"
				continue
			fi
			if [[ "$_kv" =~ ^RIG_BINARY=(main|alt)$ ]]; then
				_bin="${BASH_REMATCH[1]}"
				continue
			fi
			[[ "$_kv" =~ ^MQTTD_[A-Z0-9_]+=[^[:space:]]*$ ]] ||
				die "KNEE_ARMS: '$_kv' in '$_a' is not MQTTD_<NAME>=<value>, RIG_NIC_SPREAD=on|off or RIG_BINARY=main|alt"
			_env+="${_env:+$'\n'}$_kv"
		done
	fi
	ARM_N+=("$_n") ARM_D+=("$_d") ARM_L+=("$_l") ARM_E+=("$_env") ARM_RPS+=("$_rps") ARM_BIN+=("$_bin")
done
# Arm 1 boots the hosts, so its spread is BROKER_NIC_SPREAD's, set by cloud-init.
case "${ARM_RPS[0]}:${BROKER_NIC_SPREAD:-false}" in
"on:true" | "off:false" | ":"*) ;;
*) die "arm 1 has RIG_NIC_SPREAD=${ARM_RPS[0]} but BROKER_NIC_SPREAD=${BROKER_NIC_SPREAD:-false} — arm 1 runs as the hosts booted; set BROKER_NIC_SPREAD to match" ;;
esac
# RIG_BINARY: arm 1 runs the binary the hosts installed (MQTTD_URL, or the
# release); a later arm swaps every broker in place (swap-binary.sh) — `alt` to
# ALT_MQTTD_URL/ALT_MQTTD_SHA256, `main` back to MQTTD_URL/MQTTD_SHA256.
[ "${ARM_BIN[0]:-main}" = main ] || die "arm 1 has RIG_BINARY=${ARM_BIN[0]} — arm 1 runs the provisioned binary (main)"
for _b in "${ARM_BIN[@]}"; do
	case "$_b" in
	alt) [[ -n "${ALT_MQTTD_URL:-}" && -n "${ALT_MQTTD_SHA256:-}" ]] ||
		die "an arm uses RIG_BINARY=alt but ALT_MQTTD_URL / ALT_MQTTD_SHA256 are not both set" ;;
	main) [[ -n "${MQTTD_URL:-}" && -n "${MQTTD_SHA256:-}" ]] ||
		die "an arm uses RIG_BINARY=main but MQTTD_URL / MQTTD_SHA256 are not both set (swapping back needs a URL)" ;;
	esac
done
[ "${#ARM_N[@]}" -ge 2 ] || die "KNEE_ARMS needs at least two arms (a comparison), got ${#ARM_N[@]}"
for ((k = 1; k < ${#ARM_N[@]}; k++)); do
	[ "${ARM_N[$k]}" -le "${ARM_N[0]}" ] || die "arm $((k + 1)) has ${ARM_N[$k]} brokers, more than the ${ARM_N[0]} the first arm provisions"
	[ "${ARM_D[$k]}" -le "${ARM_D[0]}" ] || die "arm $((k + 1)) has ${ARM_D[$k]} drivers, more than the ${ARM_D[0]} the first arm provisions"
done
[ "${DRIVER_COUNT:-}" = "${ARM_D[0]}" ] ||
	die "DRIVER_COUNT=${DRIVER_COUNT:-unset} must equal the first arm's drivers (${ARM_D[0]}): it is what gets provisioned"
[ "${LANES:-}" = E ] || die "LANES must be E for this campaign (got '${LANES:-}')"
[ "${KEEP_INFRA:-0}" = 0 ] || die "do not set KEEP_INFRA — this script manages the cluster's lifetime itself"
[ -z "${RUN_DIR:-}" ] || die "do not set RUN_DIR — each arm gets its own run dir under one campaign dir"

if [ "${PREFLIGHT_ONLY:-0}" = 1 ]; then
	# run.sh's preflight writes into a scratch run dir; the calls must not share
	# one or the second would read as a resume of the first. Each arm is checked
	# with ITS driver count, which is what its ladder will be dealt over.
	pre="$(mktemp -d "${TMPDIR:-/tmp}/knee-preflight.XXXXXX")"
	for ((k = 0; k < ${#ARM_N[@]}; k++)); do
		say "preflight: arm $((k + 1)) — ${ARM_N[$k]} nodes, ${ARM_D[$k]} drivers, ladder: ${ARM_L[$k]}${ARM_E[$k]:+, env: ${ARM_E[$k]//$'\n'/ }}"
		RUN_DIR="$pre/$((k + 1))-n${ARM_N[$k]}" DRIVER_COUNT="${ARM_D[$k]}" LANE_E_SITES_OVERRIDE="${ARM_L[$k]}" \
			"$SCALE_DIR/run.sh" full "${ARM_N[$k]}"
	done
	say "all ${#ARM_N[@]} arms' shapes valid; no cloud calls made (scratch: $pre)"
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
# A pinned DRIVER is swapped in place by run-curve.sh itself (LANE_E_SWAP_HOOK,
# replace-node.sh): drivers hold no cluster state. A pinned BROKER cannot be,
# because it is a member of the formed cluster — so the driver gate stops the arm
# and names it in bad-brokers.txt, and this script swaps it and re-forms THAT arm
# once, at the same size, on the otherwise same hosts.
export LANE_E_SWAP_HOOK="${LANE_E_SWAP_HOOK-$SCALE_DIR/replace-node.sh}"

# swap_bad_brokers <failed-arm-dir> <size>: 0 when the arm failed on named bad
# brokers and they have been replaced; 1 when the failure was anything else.
swap_bad_brokers() {
	local marker="$1/results/nodes=$2/laneE/bad-brokers.txt" b
	[ -s "$marker" ] || return 1
	"$SCALE_DIR/collect.sh" "$1" "$1/inventory-$2.json" || true
	while read -r b; do
		[ -n "$b" ] || continue
		# Called from a condition, where errexit is off: fail loudly by hand.
		"$SCALE_DIR/replace-node.sh" "$FULL_INV" broker "$b" "driver gate: pinned softirq core, outlier among brokers ($(basename "$1"))" ||
			die "could not replace broker $b — see the tf-replace log beside $FULL_INV"
	done <"$marker"
}

# arm_env <k>: the broker env arm k runs with — the global EXTRA_BROKER_ENV,
# then the arm's own lines, which therefore win.
arm_env() {
	printf '%s' "${EXTRA_BROKER_ENV:-}${EXTRA_BROKER_ENV:+${ARM_E[$1]:+$'\n'}}${ARM_E[$1]}"
}

# set_nic_spread <on|off> <inventory>: RPS/RFS on every broker, live, so one
# provisioning can A/B the softirq spread (#505) the way cloud-init's
# `broker_nic_spread` applies it at boot: every rx queue's rps_cpus set to all
# CPUs (on) or none (off), with the RFS tables sized to match. Each broker's
# resulting mask is printed, and the caller records it in the arm dir.
set_nic_spread() {
	local mode="$1" INVENTORY="$2" i ip tmp RUN
	# rssh keys known_hosts off RUN: the arm dir, where resize-cluster.sh copied
	# the provisioning's.
	RUN="$(cd "$(dirname "$INVENTORY")" && pwd)"
	tmp=$(mktemp)
	for ((i = 0; i < $(broker_count); i++)); do
		ip=$(broker_pub_ip "$i")
		rssh "$ip" "set -e
			if [ '$mode' = on ]; then MASK=\$(printf '%x' \$(( (1 << \$(nproc)) - 1 ))); FLOWS=32768; QF=2048;
			else MASK=0; FLOWS=0; QF=0; fi
			echo \$FLOWS > /proc/sys/net/core/rps_sock_flow_entries || true
			for IF in \$(ls /sys/class/net | grep -v '^lo\$'); do
				for Q in /sys/class/net/\$IF/queues/rx-*; do
					[ -d \$Q ] || continue
					echo \$MASK > \$Q/rps_cpus 2>/dev/null || true
					echo \$QF > \$Q/rps_flow_cnt 2>/dev/null || true
				done
				echo \"broker$i \$IF rps_cpus=\$(cat /sys/class/net/\$IF/queues/rx-0/rps_cpus)\"
			done" >"$tmp" || die "could not reach broker $i ($ip) to set RIG_NIC_SPREAD=$mode"
		cat "$tmp"
		# A spread that did not take must not be measured as one.
		if [ "$mode" = on ] && grep -qE 'rps_cpus=0+(,0+)*$' "$tmp"; then
			die "RIG_NIC_SPREAD=on did not take on broker $i: $(tr '\n' ' ' <"$tmp")"
		fi
		# And symmetrically: an "off" that left a mask set would measure spread.
		if [ "$mode" = off ] && grep -E 'rps_cpus=' "$tmp" | grep -qvE 'rps_cpus=0+(,0+)*$'; then
			die "RIG_NIC_SPREAD=off did not take on broker $i: $(tr '\n' ' ' <"$tmp")"
		fi
	done
	rm -f "$tmp"
}

# run.sh's per-size tail (run-curve, collect, observe) for an arm that
# resize-cluster.sh + bootstrap-cluster.sh brought up instead of run.sh.
resized_arm() { # resized_arm <size> <drivers> <arm-dir> <ladder> <broker-env> <nic-spread> <binary> [retry]
	local n="$1" d="$2" dir="$3" ladder="$4" env="$5" rps="$6" bin="$7" retry="${8:-0}" rc=0
	say "════ arm $(basename "$dir"): $n nodes, $d drivers, on the same hosts — ladder: $ladder${env:+ — broker env: ${env//$'\n'/ }}${rps:+ — nic spread: $rps}${bin:+ — binary: $bin} ════"
	ARM_DIR="$dir" ARM_INV="$dir/inventory-$n.json"
	"$SCALE_DIR/resize-cluster.sh" "$FULL_INV" "$n" "$dir" "$d"
	mkdir -p "$dir" && printf '%s\n' "$env" >"$dir/arm-env.txt"
	if [ -n "$rps" ]; then
		set_nic_spread "$rps" "$dir/inventory-$n.json" | tee "$dir/nic-spread.txt" >&2
	fi
	case "$bin" in
	alt) "$SCALE_DIR/swap-binary.sh" "$dir/inventory-$n.json" "$ALT_MQTTD_URL" "$ALT_MQTTD_SHA256" 2>&1 | { grep -v "^  " || true; } >&2
		printf 'binary=alt\nurl=%s\nsha256=%s\n' "$ALT_MQTTD_URL" "$ALT_MQTTD_SHA256" >"$dir/arm-binary.txt" ;;
	main) "$SCALE_DIR/swap-binary.sh" "$dir/inventory-$n.json" "$MQTTD_URL" "$MQTTD_SHA256" 2>&1 | { grep -v "^  " || true; } >&2
		printf 'binary=main\nurl=%s\nsha256=%s\n' "$MQTTD_URL" "$MQTTD_SHA256" >"$dir/arm-binary.txt" ;;
	esac
	EXTRA_BROKER_ENV="$env" "$SCALE_DIR/bootstrap-cluster.sh" "$dir" "$dir/inventory-$n.json" durable
	if [ "${OBSERVE:-1}" = 1 ]; then
		"$SCALE_DIR/observe.sh" attach "$dir" "$dir/inventory-$n.json" || warn "observe attach failed — continuing unobserved"
	fi
	LANE_E_SITES_OVERRIDE="$ladder" "$SCALE_DIR/run-curve.sh" "$dir" "$dir/inventory-$n.json" || rc=$?
	if [ "$rc" -ne 0 ]; then
		if [ "$retry" = 0 ] && swap_bad_brokers "$dir" "$n"; then
			resized_arm "$n" "$d" "$dir-r2" "$ladder" "$env" "$rps" "$bin" 1
			return
		fi
		return "$rc"
	fi
	LAST_ARM="$(basename "$dir")"
	"$SCALE_DIR/collect.sh" "$dir" "$dir/inventory-$n.json"
	if [ "${OBSERVE:-1}" = 1 ]; then
		"$SCALE_DIR/observe.sh" detach || true
	fi
}

A1="1-n${ARM_N[0]}"
ARM_DIR="$CAMPAIGN/$A1" ARM_INV="$CAMPAIGN/$A1/inventory-${ARM_N[0]}.json"
say "════ arm $A1: provision ${ARM_N[0]} brokers + ${ARM_D[0]} drivers, ladder: ${ARM_L[0]} ════"
rc=0
mkdir -p "$ARM_DIR" && arm_env 0 >"$ARM_DIR/arm-env.txt" && echo >>"$ARM_DIR/arm-env.txt"
KEEP_INFRA=1 RUN_DIR="$ARM_DIR" LANE_E_SITES_OVERRIDE="${ARM_L[0]}" EXTRA_BROKER_ENV="$(arm_env 0)" \
	"$SCALE_DIR/run.sh" full "${ARM_N[0]}" || rc=$?
FULL_INV="$ARM_INV"
[ -f "$FULL_INV" ] || die "arm $A1 left no inventory at $FULL_INV"
DONE_ARMS=("$A1")
if [ "$rc" -ne 0 ]; then
	swap_bad_brokers "$CAMPAIGN/$A1" "${ARM_N[0]}" || exit "$rc"
	resized_arm "${ARM_N[0]}" "${ARM_D[0]}" "$CAMPAIGN/$A1-r2" "${ARM_L[0]}" "$(arm_env 0)" "${ARM_RPS[0]}" "" 1
	DONE_ARMS=("$LAST_ARM")
fi

for ((k = 1; k < ${#ARM_N[@]}; k++)); do
	resized_arm "${ARM_N[$k]}" "${ARM_D[$k]}" "$CAMPAIGN/$((k + 1))-n${ARM_N[$k]}" "${ARM_L[$k]}" "$(arm_env "$k")" "${ARM_RPS[$k]}" "${ARM_BIN[$k]}"
	DONE_ARMS+=("$LAST_ARM")
done

say "all ${#ARM_N[@]} arms complete — gate each before reading any number:"
for arm in "${DONE_ARMS[@]}"; do
	echo "  python3 extract-lane-e.py --crossing-gate 0.5 $CAMPAIGN/$arm/results" >&2
done
