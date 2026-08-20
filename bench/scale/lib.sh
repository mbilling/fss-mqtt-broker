#!/usr/bin/env bash
# Shared helpers for the scale-curve rig (ADR 0048 T3). Sourced, not executed.
# Everything talks to the hosts over SSH as root (the Hetzner cloud-init default
# for an image provisioned with an SSH key); nothing here stores a secret.

set -euo pipefail

SCALE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck disable=SC2034 # consumed by the sourcing scripts (bootstrap-cluster.sh)
REPO_ROOT="$(cd "$SCALE_DIR/../.." && pwd)"

say() { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33mWARN\033[0m %s\n' "$*" >&2; }
die() {
	printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2
	exit 1
}

# ssh/scp with a per-run known_hosts file: fresh servers mean fresh host keys,
# and polluting the operator's global known_hosts with short-lived IPs helps no one.
# SSH_KEY=<path to private key> selects a non-default identity (e.g. ~/.ssh/hetzner);
# run.sh derives the uploaded public key from it as ${SSH_KEY}.pub.
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 -o ServerAliveInterval=15)
if [ -n "${SSH_KEY:-}" ]; then
	[ -f "$SSH_KEY" ] || die "SSH_KEY=$SSH_KEY does not exist"
	SSH_OPTS+=(-i "$SSH_KEY" -o IdentitiesOnly=yes)
fi
rssh() { # rssh <public-ip> <command...>
	local ip="$1"
	shift
	ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" "root@$ip" "$@"
}
rscp() { # rscp <src...> <public-ip>:<dst>  (or <public-ip>:<src> <dst>)
	scp -q "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" "$@"
}

# Inventory accessors — the JSON written by `terraform output -json inventory`.
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
