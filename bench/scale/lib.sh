#!/usr/bin/env bash
# Shared helpers for the scale-curve rig (ADR 0048 T3). Sourced, not executed.
# Everything talks to the hosts over SSH as root (the Hetzner cloud-init default
# for an image provisioned with an SSH key); nothing here stores a secret.

set -euo pipefail

# The rig is written for bash >= 4.4 (empty arrays under `set -u`, subshell traps).
# macOS ships bash 3.2, and under it the CPU samplers died inside every rung of the
# #504 acceptance run (2026-10-01) — `__cpu_pids[@]: unbound variable` — leaving
# cpu_window=incomplete everywhere. Called by the PAID entry points only: teardown
# also sources this file, and must keep working on any shell.
require_modern_bash() {
	if [ "${BASH_VERSINFO[0]}" -lt 4 ] || { [ "${BASH_VERSINFO[0]}" -eq 4 ] && [ "${BASH_VERSINFO[1]}" -lt 4 ]; }; then
		printf 'FATAL: the scale rig needs bash >= 4.4; this is %s (%s).\n' "$BASH_VERSION" "$BASH" >&2
		printf '       macOS: brew install bash, and put its bin directory first in PATH (scripts use #!/usr/bin/env bash).\n' >&2
		exit 2
	fi
}

SCALE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC2034 # consumed by the sourcing scripts (bootstrap-cluster.sh)
REPO_ROOT="$(cd "$SCALE_DIR/../.." && pwd)"

# Every line carries a UTC time: a run that dies hours in must be reconstructible from
# run.log alone (the 2026-10-03 knee run's bring-up failure could not be placed in time).
say() { printf '\033[1;34m==>\033[0m %s %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }
warn() { printf '\033[1;33mWARN\033[0m %s %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }
die() {
	printf '\033[1;31mFAIL\033[0m %s %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2
	exit 1
}

# A rig run is orchestrated from this machine: if it sleeps, every ssh session to the
# fleet freezes while the servers keep billing, and the run dies on wake (2026-10-02 and
# 2026-10-03: idle and clamshell sleep killed two 30-server runs). On macOS hold an
# idle/system-sleep assertion for the OUTERMOST rig script's lifetime. A closed lid on
# battery cannot be held off by any assertion — say so up front.
keep_awake() {
	[ "$(uname -s)" = Darwin ] && command -v caffeinate >/dev/null 2>&1 || return 0
	[ -z "${BENCH_KEEP_AWAKE_PID:-}" ] || return 0
	caffeinate -ims -w $$ </dev/null >/dev/null 2>&1 &
	export BENCH_KEEP_AWAKE_PID=$!
	if pmset -g batt 2>/dev/null | grep -q "Battery Power"; then
		warn "on battery: closing the lid will SUSPEND this run (servers keep billing) — plug in, or keep the lid open"
	fi
}
keep_awake

# run_bounded <secs> <cmd...>: run <cmd>, and kill it if it is still running after <secs>
# seconds. Returns <cmd>'s own status, or 124 if it had to be killed (GNU timeout's code;
# macOS ships no timeout(1), so this is bash-native). ssh needs this: ConnectTimeout bounds
# only the TCP connect and ServerAlive* only an ESTABLISHED session, so a connection that
# stalls between the two (the key exchange of a host reconfiguring its network mid-boot)
# waits for TCP to give up — measured on 2026-10-03 as longer than the 10-minute budget.
run_bounded() {
	local secs="$1"
	shift
	"$@" &
	local pid=$!
	(
		sleep "$secs"
		kill -TERM "$pid" 2>/dev/null || exit 0
		sleep 5
		kill -KILL "$pid" 2>/dev/null || true
	) </dev/null >/dev/null 2>&1 & # detached stdio: an orphaned sleep must not hold a caller's $(...) pipe open
	local watcher=$!
	local rc=0
	wait "$pid" || rc=$?
	kill "$watcher" 2>/dev/null || true
	wait "$watcher" 2>/dev/null || true
	# 143 = TERM, 137 = KILL: the watcher's doing, not the command's verdict.
	if [ "$rc" -eq 143 ] || [ "$rc" -eq 137 ]; then
		return 124
	fi
	return "$rc"
}

# ssh/scp with a per-run known_hosts file: fresh servers mean fresh host keys,
# and polluting the operator's global known_hosts with short-lived IPs helps no one.
# SSH_KEY=<path to private key> selects a non-default identity (e.g. ~/.ssh/hetzner);
# the uploaded public key is ${SSH_KEY}.pub. OpenTofu evaluates
# file(pathexpand(var.ssh_public_key_path)) on destroy as well as apply, so every
# tofu destroy / teardown must pass the same -var apply used. When SSH_KEY is
# set and destroy omits it, OpenTofu falls back to ~/.ssh/id_ed25519.pub and
# fails if that file is missing — leaving paid servers up (#482, 2026-09-14).
# ServerAliveCountMax is set EXPLICITLY: without it OpenSSH's default of 3 gave a
# dead peer 15x3 = 45s to be noticed — three times the `curl -m 10` that hop is
# supposed to contain, inside polling loops whose whole budget is 60s. One hung
# hop could therefore eat a rung's entire drain deadline and report UNRESOLVED.
# 5x2 = 10s matches the remote timeout, so a scrape now costs at most what it
# declares. See budgets.py, which reads these three numbers from this line.
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2)
# shellcheck disable=SC2034 # consumed by run.sh / teardown.sh tofu apply+destroy
TOFU_SSH_PUBKEY_ARGS=()
if [ -n "${SSH_KEY:-}" ]; then
	[ -f "$SSH_KEY" ] || die "SSH_KEY=$SSH_KEY does not exist"
	SSH_OPTS+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
	TOFU_SSH_PUBKEY_ARGS=(-var "ssh_public_key_path=${SSH_KEY}.pub")
fi
rssh() { # rssh <public-ip> <command...>
	local ip="$1"
	shift
	# Refuse to fall back to ~/.ssh/known_hosts: an unset RUN once made rig ssh
	# traffic rewrite the operator's DEFAULT known_hosts (evicting github.com).
	[ -n "${RUN:-}" ] || die "rssh called with RUN unset — refusing to touch the default known_hosts"
	ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" "root@$ip" "$@"
}
rscp() { # rscp <src...> <public-ip>:<dst>  (or <public-ip>:<src> <dst>)
	scp -q "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" "$@"
}

# cloud_init_rc <ip>: cloud-init's verdict on <ip> — 0 done (or degraded-done), 3 still
# running when the budget ran out, 124/255 unreachable for the whole budget, anything
# else cloud-init's own error. Each ATTEMPT is a short, wall-bounded poll; the BUDGET
# (CLOUD_INIT_BUDGET, default 20 min) bounds the whole wait.
#  - Poll, never `cloud-init status --wait`: a --wait session spans the whole boot,
#    exactly while the host reconfigures its network, so it is the session most likely
#    to be cut or wedged. A poll is in and out in seconds.
#  - Bound every attempt (run_bounded): on 2026-10-03 one ssh attempt to a HEALTHY
#    driver (cloud-init finished cleanly at 192 s) hung past the whole 10-minute budget
#    and ended in "Broken pipe"; the deadline was only checked between attempts, so the
#    run died without a single retry. No attempt can outlive CI_ATTEMPT_SECS now.
#  - ssh exiting 255 (session dropped: "server not responding", "Connection reset") and
#    an attempt killed at its bound (124) are lost connections, not verdicts: ask again.
#    Measured on 2026-10-02 and 2026-10-03 on otherwise healthy hosts.
CI_POLL='s=$(cloud-init status 2>/dev/null); rc=$?; case "$s" in *running* | *"not started"* | *"not run"*) exit 3 ;; esac; [ $rc -eq 0 ] || [ $rc -eq 2 ]'
CI_ATTEMPT_SECS=45
cloud_init_rc() {
	local deadline=$((SECONDS + ${CLOUD_INIT_BUDGET:-1200})) rc
	while :; do
		rc=0
		local t0=$SECONDS
		run_bounded "$CI_ATTEMPT_SECS" rssh "$1" "$CI_POLL" || rc=$?
		# An attempt is bounded, so one that took far longer means THIS machine was
		# suspended (a laptop sleeping under the run: 2026-10-03, ~2 h). Nobody was waiting
		# on the host then; that time does not count against its cloud-init budget.
		local took=$((SECONDS - t0))
		if [ "$took" -gt $((CI_ATTEMPT_SECS + 30)) ]; then
			warn "orchestrator was suspended ~$((took / 60)) min (laptop sleep?) during the check on $1 — not counted against its cloud-init budget"
			deadline=$((deadline + took))
		fi
		case "$rc" in
		0) return 0 ;;
		3) ;; # still booting — keep polling
		124 | 255) warn "ssh to $1 lost (exit $rc) while cloud-init was finishing — reconnecting" ;;
		*) return "$rc" ;; # cloud-init's own error verdict
		esac
		[ "$SECONDS" -lt "$deadline" ] || return "$rc"
		sleep 5
	done
}

# Inventory accessors — the JSON written by `tofu output -json inventory`.
inv() { jq -r "$1" "$INVENTORY"; }
broker_count() { inv '.brokers | length'; }
broker_pub_ip() { inv ".brokers[$1].public_ip"; }
broker_priv_ip() { inv ".brokers[$1].private_ip"; }
broker_node_id() { inv ".brokers[$1].node_id"; }
driver_count() { inv '.drivers | length'; }
driver_pub_ip() { inv ".drivers[$1].public_ip"; }

# wait_for <label> <deadline-secs> <command...>: poll a command (usually rssh)
# until it succeeds or the budget elapses. Bounded, never sleeps blind.
wait_for() {
	local label="$1" budget="$2"
	shift 2
	local start elapsed
	start=$(date +%s)
	while ! "$@" >/dev/null 2>&1; do
		elapsed=$(($(date +%s) - start))
		[ "$elapsed" -lt "$budget" ] || die "timed out after ${budget}s waiting for: $label"
		sleep 3
	done
}

# wait_ready <broker-index> <budget>: the broker's own /readyz on its own host —
# the health port is private-network-only, so the check rides SSH.
wait_ready() {
	wait_for "broker $1 /readyz" "$2" \
		rssh "$(broker_pub_ip "$1")" "curl -sf http://localhost:8080/readyz"
}

# await_full_mesh <budget-secs> <stable-polls> <evidence-file>: poll every
# broker until ALL report mqttd_cluster_members = N and mqttd_peer_links = N-1,
# on <stable-polls> consecutive rounds MESH_POLL_SECS (default 5) apart. /readyz is majority-only, so a
# cluster bootstrap-cluster.sh calls READY can still be rebuilding links after
# the founder's re-arm restart: on 2026-09-25 (N=7) the storm it set off left
# one link missing ~35s after READY, and the forwarding control rightly failed
# the size. Every round is appended to <evidence-file>; returns 1 when the
# budget runs out rather than dying, so the caller names the consequence.
await_full_mesh() {
	local budget="$1" want_stable="$2" evidence="$3"
	local n start stable=0 i ok row links members
	n=$(broker_count)
	start=$(date +%s)
	: >"$evidence"
	while :; do
		ok=1 row=""
		for ((i = 0; i < n; i++)); do
			read -r links members < <(rssh "$(broker_pub_ip "$i")" "curl -s -m 10 http://localhost:8080/metrics" 2>/dev/null |
				awk '$1 == "mqttd_peer_links" { l = $2 } $1 == "mqttd_cluster_members" { m = $2 }
					END { printf "%s %s\n", (l == "" ? "-" : l), (m == "" ? "-" : m) }') || true
			row+=" broker$i=${links:--}/${members:--}"
			[ "${links:-}" = "$((n - 1))" ] && [ "${members:-}" = "$n" ] || ok=0
		done
		echo "t=$(($(date +%s) - start))s links/members:$row" >>"$evidence"
		if [ "$ok" = 1 ]; then stable=$((stable + 1)); else stable=0; fi
		[ "$stable" -ge "$want_stable" ] && return 0
		[ $(($(date +%s) - start)) -lt "$budget" ] || return 1
		sleep "${MESH_POLL_SECS:-5}"
	done
}

# every_broker <command-template>: run over all broker indices sequentially.
every_broker() { # every_broker fn — calls fn <index>
	local fn="$1" i n
	n=$(broker_count)
	for ((i = 0; i < n; i++)); do "$fn" "$i"; done
}

# The OpenSSL binary for PKI minting: macOS ships LibreSSL, which gen-certs.sh
# rejects loudly; prefer Homebrew's openssl@3 when present.
pick_openssl() {
	for c in /opt/homebrew/opt/openssl@3/bin/openssl /usr/local/opt/openssl@3/bin/openssl openssl; do
		if "$c" version 2>/dev/null | grep -q '^OpenSSL'; then
			echo "$c"
			return
		fi
	done
	die "no real OpenSSL found — brew install openssl@3 (macOS LibreSSL cannot mint the PKI)"
}
