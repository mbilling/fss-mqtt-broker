#!/usr/bin/env bash
# The scale-curve orchestrator (ADR 0048 T3). Run from a laptop, never from CI:
#
#   export HCLOUD_TOKEN=...     # Read & Write token from a DEDICATED project
#   ./run.sh smoke              # 1 node, minutes, <€0.50 — proves the whole rig
#   ./run.sh standard           # the ~€10 release profile: sizes 1, 5, 10
#   ./run.sh full               # the curve: fresh 1-, 3- and 5-node clusters
#   ./run.sh full 3 5           # a subset of sizes
#
# WHICH PROFILE: `standard` is the default for a release — three sizes (the
# floor, the voter cap, the biggest cluster we run) with the shape trimmed to
# what a release claim actually rests on. `full` is for durable-PATH releases,
# where the tier arms and every rung are themselves the evidence; it costs
# ~3-4× as much and takes ~3× as long.
#
# Each size is applied FRESH and destroyed before the next: a grown cluster is a
# known-degraded configuration (replica groups tracked under an earlier
# membership never re-green), so every curve point is an independent cluster.
#
# Teardown is trapped on EXIT/INT/TERM — Ctrl-C destroys the paid servers.
# KEEP_INFRA=1 skips that for debugging and says so loudly.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

MODE="${1:-}"
case "$MODE" in
smoke)
	SIZES=(1)
	export SMOKE=1
	# One driver: smoke proves the pipeline, not the 50k load. And SHARED-vCPU
	# types: a fresh Hetzner project's dedicated-core limit can be below 12,
	# and smoke's job (terraform, cloud-init, PKI, lanes, teardown) does not
	# need dedicated cores — only the published measurement runs do, and those
	# require the limit increase the README describes anyway. Overridable.
	DRIVER_COUNT="${DRIVER_COUNT:-1}"
	BROKER_TYPE="${BROKER_TYPE:-cpx32}"
	DRIVER_TYPE="${DRIVER_TYPE:-cpx42}"
	;;
standard)
	# The ~€10 release profile. Same rig, same measurement code, same
	# dedicated-core machine types — only the SHAPE is trimmed, and only
	# where the trimmed part is not itself a release claim:
	#   sizes 1, 5, 10   the floor, the voter cap, and the top of our range
	#                    (3 and 7 interpolate a line we have measured five
	#                    times; they come back the moment the line bends)
	#   no tier arms     ADR 0072's tiers are a durable-PATH claim, measured
	#                    on `full`; a release that does not touch them does
	#                    not re-pay for them
	#   3 lane-B rungs   the knee bracket, not the full ladder
	#   shorter lane C   50k connections still established, held 60s not 120
	#   2 lane-A reps    median of 2 with the spread reported, not 3
	shift || true
	if [ $# -gt 0 ]; then SIZES=("$@"); else SIZES=(1 5 10); fi
	export STANDARD=1
	;;
full)
	shift || true
	if [ $# -gt 0 ]; then SIZES=("$@"); else SIZES=(1 3 5); fi
	;;
*)
	echo "usage: $0 smoke | standard [sizes...] | full [sizes...]" >&2
	exit 2
	;;
esac

[ -n "${HCLOUD_TOKEN:-}" ] || die "HCLOUD_TOKEN is not set (Read & Write token from the dedicated Hetzner project)"
command -v terraform >/dev/null || command -v tofu >/dev/null || die "terraform (or tofu) not installed"
command -v jq >/dev/null || die "jq not installed"
TF=$(command -v terraform || command -v tofu)

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN="${RUN_DIR:-$SCALE_DIR/.runs/$STAMP}"
mkdir -p "$RUN"
say "run dir: $RUN"

TFDIR="$SCALE_DIR/terraform"
CURRENT_SIZE=""

teardown() {
	local rc=$?
	trap - EXIT INT TERM
	if [ "${KEEP_INFRA:-0}" = 1 ]; then
		printf '\033[1;31m!! KEEP_INFRA=1 — PAID SERVERS ARE STILL RUNNING. %s\033[0m\n' \
			"Destroy with: bench/scale/teardown.sh" >&2
		exit "$rc"
	fi
	# A failed run's evidence dies with its servers — pull journals/cloud-init
	# logs off every host FIRST (best-effort, bounded by ssh's own timeouts).
	if [ "$rc" -ne 0 ] && [ -n "${CURRENT_SIZE:-}" ] && [ -f "$RUN/inventory-$CURRENT_SIZE.json" ]; then
		say "capturing failure evidence off the hosts before destroying them"
		"$SCALE_DIR/collect.sh" "$RUN" "$RUN/inventory-$CURRENT_SIZE.json" || true
	fi
	say "tearing down (trap; exit code was $rc)"
	if ! (cd "$TFDIR" && "$TF" destroy -auto-approve \
		-var node_count="${CURRENT_SIZE:-1}" -var run_label="$STAMP" >>"$RUN/teardown.log" 2>&1); then
		warn "terraform destroy FAILED — sweep with: bench/scale/teardown.sh --force"
		exit 1
	fi
	say "all cloud resources destroyed"
	exit "$rc"
}
trap teardown EXIT INT TERM

(cd "$TFDIR" && "$TF" init -input=false >"$RUN/tf-init.log" 2>&1) || {
	cat "$RUN/tf-init.log" >&2
	die "terraform init failed"
}

for N in "${SIZES[@]}"; do
	if [ -f "$RUN/done-$N" ]; then
		say "size $N already completed in this run dir — skipping (RESUME)"
		continue
	fi
	CURRENT_SIZE="$N"
	say "════ cluster size $N ════"

	(cd "$TFDIR" && "$TF" apply -auto-approve -input=false \
		-var node_count="$N" -var run_label="$STAMP" \
		${MQTTD_VERSION:+-var mqttd_version="$MQTTD_VERSION"} \
		${BENCH_GIT_REF:+-var bench_git_ref="$BENCH_GIT_REF"} \
		${SSH_KEY:+-var ssh_public_key_path="${SSH_KEY}.pub"} \
		${DRIVER_COUNT:+-var driver_count="$DRIVER_COUNT"} \
		${BROKER_TYPE:+-var broker_server_type="$BROKER_TYPE"} \
		${DRIVER_TYPE:+-var driver_server_type="$DRIVER_TYPE"} \
		>"$RUN/tf-apply-$N.log" 2>&1) || {
		tail -30 "$RUN/tf-apply-$N.log" >&2
		die "terraform apply failed for size $N"
	}
	INVENTORY="$RUN/inventory-$N.json"
	(cd "$TFDIR" && "$TF" output -json inventory) >"$INVENTORY"
	# Hetzner recycles public IPs BETWEEN SIZES of the same run; a host key
	# recorded for the previous size's server at the same address makes
	# accept-new refuse the new one. Every size starts with fresh servers,
	# so it starts with a fresh slate.
	: >"$RUN/known_hosts"
	say "applied: $(jq -r '.brokers | length' "$INVENTORY") brokers + $(jq -r '.drivers | length' "$INVENTORY") drivers"

	# cloud-init must have finished everywhere (it verifies the release binary's
	# checksum; a failure there surfaces here, loudly).
	# Exit 2 = "degraded done": recoverable warnings only — and Hetzner's own
	# vendor-data trips deprecation warnings on every boot, so 2 is the NORMAL
	# outcome here. Exit 1 (error) is a real failure — most often the private-net
	# attach race (cloud-init's init-local boots while the network attachment is
	# still settling and Tracebacks on the half-written metadata). A reboot after
	# `cloud-init clean` re-runs everything against settled metadata, so each
	# host gets exactly one such retry before the run gives up.
	CI_OK='cloud-init status --wait >/dev/null 2>&1; rc=$?; [ $rc -eq 0 ] || [ $rc -eq 2 ]'
	# True once <ip> presents a kernel boot id that is non-empty and differs
	# from <old> — i.e. the machine has verifiably completed its reboot. Probes
	# use a THROWAWAY known_hosts: a poll that lands in the pre-reboot window
	# would re-record the old host key and poison every post-reboot connect.
	boot_id_changed() {
		local now
		now=$(ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile=/dev/null \
			"root@$1" "cat /proc/sys/kernel/random/boot_id" 2>/dev/null) || return 1
		[ -n "$now" ] && [ "$now" != "$2" ]
	}
	for pair in $(jq -r '(.brokers[], .drivers[]) | "\(.public_ip)=\(.private_ip)"' "$INVENTORY"); do
		ip="${pair%%=*}" priv="${pair##*=}"
		wait_for "ssh on $ip" 300 rssh "$ip" "true"
		if ! rssh "$ip" "$CI_OK"; then
			warn "cloud-init errored on $ip — one clean+reboot retry (private-net attach race)"
			OLD_BOOT=$(rssh "$ip" "cat /proc/sys/kernel/random/boot_id" 2>/dev/null || echo unknown)
			rssh "$ip" "cloud-init clean --logs; reboot" || true
			# cloud-init clean makes the next boot regenerate SSH HOST KEYS; drop
			# the stale entry or every reconnect is refused as a key mismatch.
			ssh-keygen -R "$ip" -f "$RUN/known_hosts" >/dev/null 2>&1 || true
			# The reboot takes seconds to sever ssh — a fast reconnect reaches the
			# OLD boot and reads its error status as the retry's verdict. Nothing
			# counts until the kernel boot id has actually changed.
			wait_for "reboot of $ip (new boot id)" 300 boot_id_changed "$ip" "$OLD_BOOT"
			# Drop whatever key a pre-reboot poll may have re-recorded; the next
			# rssh accepts the NEW boot's key fresh.
			ssh-keygen -R "$ip" -f "$RUN/known_hosts" >/dev/null 2>&1 || true
			rssh "$ip" "$CI_OK" ||
				die "cloud-init failed on $ip even after a clean reboot — check /var/log/cloud-init-output.log there"
		fi
		# The lanes ride the private network; do not proceed until this host's
		# private address is actually configured.
		wait_for "private ip $priv on $ip" 180 rssh "$ip" "ip -4 addr show | grep -qF $priv"
	done

	# Private-net truth gate (issue #368): an address can be PRESENT while the
	# interface is ONE-WAY — outbound dead, inbound fine — the shape that let a
	# cleanly-formed cluster evict one node minutes later on three separate
	# runs (always the host whose private-net attach raced; the address check
	# above cannot see it). Verify actual pairwise reachability from every
	# host, heal the SOURCE host (outbound is the broken side) with the same
	# clean+reboot retry once, and fail the run fast if it stays one-way —
	# minutes here instead of an unmeasurable hour later.
	ALL_PRIVS=$(jq -r '[(.brokers[], .drivers[]) | .private_ip] | join(" ")' "$INVENTORY")
	mesh_ok() { # mesh_ok <public-ip> — prints the first unreachable private ip
		rssh "$1" "for t in $ALL_PRIVS; do ping -c1 -W2 \$t >/dev/null 2>&1 || { echo \$t; exit 1; }; done"
	}
	for ip in $(jq -r '(.brokers[], .drivers[]) | .public_ip' "$INVENTORY"); do
		if ! BAD=$(mesh_ok "$ip"); then
			warn "private-net mesh FAILED from $ip (cannot reach ${BAD:-?}) — one clean+reboot retry (one-way attach, issue #368)"
			OLD_BOOT=$(rssh "$ip" "cat /proc/sys/kernel/random/boot_id" 2>/dev/null || echo unknown)
			rssh "$ip" "cloud-init clean --logs; reboot" || true
			ssh-keygen -R "$ip" -f "$RUN/known_hosts" >/dev/null 2>&1 || true
			wait_for "reboot of $ip (new boot id)" 300 boot_id_changed "$ip" "$OLD_BOOT"
			ssh-keygen -R "$ip" -f "$RUN/known_hosts" >/dev/null 2>&1 || true
			wait_for "cloud-init after mesh retry on $ip" 300 rssh "$ip" "$CI_OK"
			BAD=$(mesh_ok "$ip") ||
				die "private-net STILL one-way from $ip (cannot reach ${BAD:-?}) after a clean reboot — Hetzner attach fault; tear down and re-apply"
		fi
	done
	say "private-net mesh verified: every host reaches every private address"

	# lane A's driver binary: wait for driver-1's background cargo build.
	say "waiting for driver-1's durable_bench build (overlaps with nothing else now)"
	wait_for "durable_bench build on driver-1" 1800 \
		rssh "$(jq -r '.drivers[0].public_ip' "$INVENTORY")" "test -s /run/bench-driver-build-done"

	"$SCALE_DIR/bootstrap-cluster.sh" "$RUN" "$INVENTORY" durable
	# OBSERVE=1: live Grafana on the laptop, fed by an Alloy scraper on
	# driver-1 through a reverse tunnel (bench/scale/observe.sh). Off by
	# default so the unobserved measurement path stays byte-identical.
	if [ "${OBSERVE:-0}" = 1 ]; then
		"$SCALE_DIR/observe.sh" attach "$RUN" "$INVENTORY" || warn "observe attach failed — continuing unobserved"
	fi
	"$SCALE_DIR/run-curve.sh" "$RUN" "$INVENTORY"
	"$SCALE_DIR/collect.sh" "$RUN" "$INVENTORY"
	if [ "${OBSERVE:-0}" = 1 ]; then
		"$SCALE_DIR/observe.sh" detach || true
	fi

	say "destroying size $N before the next point (fresh clusters only)"
	(cd "$TFDIR" && "$TF" destroy -auto-approve \
		-var node_count="$N" -var run_label="$STAMP" >"$RUN/tf-destroy-$N.log" 2>&1) || {
		tail -20 "$RUN/tf-destroy-$N.log" >&2
		die "terraform destroy failed for size $N — sweep with teardown.sh --force before re-running"
	}
	touch "$RUN/done-$N"
done

CURRENT_SIZE=""
say "curve complete. Summarize with:"
say "  python3 bench/scale/summarize-curve.py $RUN/results"
