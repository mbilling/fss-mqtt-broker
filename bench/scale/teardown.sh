#!/usr/bin/env bash
# Standalone teardown / leak sweeper. Normal runs destroy themselves; this is
# for a dead laptop, a lost state file, or paranoia before reading the invoice.
#
#   ./teardown.sh           # tofu destroy from local state, then AUDIT by label
#   ./teardown.sh --force   # also DELETE whatever the label audit finds (needs hcloud CLI)
#
# Non-default SSH_KEY is passed through to tofu destroy as ${SSH_KEY}.pub (the
# same path apply used). That file must exist: OpenTofu reads ssh_public_key_path
# while tearing down.
#
# Everything this rig creates carries the label purpose=mqttd-bench-scale, and
# the README tells the operator to use a DEDICATED Hetzner project — so a forced
# sweep can never touch anything that is not ours.

set -euo pipefail
. "$(dirname "$0")/lib.sh"
. "$SCALE_DIR/cloud.sh"
select_scale_cloud

FORCE=0
[ "${1:-}" = --force ] && FORCE=1

TF=$(command -v tofu) || die "OpenTofu (tofu) is required; Terraform is not supported"

if [ "$CLOUD" = upcloud ]; then
	# No hcloud commands here: a provider mismatch could delete an unrelated
	# Hetzner benchmark while leaving every UpCloud server billing.
	[ "$FORCE" = 0 ] || die "CLOUD=upcloud does not support --force; use state-backed teardown or inspect the UpCloud console"
	require_scale_token
	[ -f "$TFDIR/terraform.tfstate" ] || die "UpCloud state missing; inspect servers, storage, networks and server groups in the UpCloud console (do not use the Hetzner sweeper)"
	(cd "$TFDIR" && "$TF" init -input=false && "$TF" destroy -auto-approve -input=false -var node_count=1 "${TOFU_SSH_PUBKEY_ARGS[@]}") ||
		die "UpCloud destroy failed; preserve state and inspect the UpCloud console before retrying"
	say "UpCloud resources tracked in this state destroyed. No account-wide leak audit was performed; verify orphaned resources in the UpCloud console."
	exit 0
fi

# A network blip must not turn into a fleet left billing: retry the destroy, and an
# audit that cannot reach the API is a FAILURE, never an all-clear. On 2026-10-23 the
# laptop's DNS went away for a few minutes; the destroy failed, every audit listing
# failed into an empty answer, and "nothing leaked" was printed over 30 running servers.
TEARDOWN_TRIES=${TEARDOWN_TRIES:-3}
TEARDOWN_RETRY_SECS=${TEARDOWN_RETRY_SECS:-20}

if [ -f "$TFDIR/terraform.tfstate" ]; then
	destroyed=0
	for ((try = 1; try <= TEARDOWN_TRIES; try++)); do
		say "OpenTofu destroy from local state (attempt $try of $TEARDOWN_TRIES)"
		if (cd "$TFDIR" && "$TF" destroy -auto-approve -var node_count=1 "${TOFU_SSH_PUBKEY_ARGS[@]}"); then
			destroyed=1
			break
		fi
		[ "$try" -lt "$TEARDOWN_TRIES" ] && sleep "$TEARDOWN_RETRY_SECS"
	done
	[ "$destroyed" = 1 ] || warn "OpenTofu destroy failed $TEARDOWN_TRIES times — continuing to the label audit"
fi

if ! command -v hcloud >/dev/null; then
	warn "hcloud CLI not installed — cannot audit by label."
	warn "Verify by hand in the Hetzner console that NO servers remain in the bench project."
	exit 0
fi

say "auditing by label purpose=mqttd-bench-scale"
LEAKED=0
UNAUDITED=()
AUDIT_ERR=$(mktemp)
trap 'rm -f "$AUDIT_ERR"' EXIT
for kind in server network firewall placement-group ssh-key; do
	out="" listed=0
	for ((try = 1; try <= 5; try++)); do
		if out=$(hcloud "$kind" list -l purpose=mqttd-bench-scale -o noheader 2>"$AUDIT_ERR"); then
			listed=1
			break
		fi
		sleep "$TEARDOWN_RETRY_SECS"
	done
	if [ "$listed" = 0 ]; then
		UNAUDITED+=("$kind")
		warn "could not list ${kind}s: $(head -c 200 "$AUDIT_ERR")"
		continue
	fi
	if [ -n "$out" ]; then
		LEAKED=1
		printf '\033[1;31mLEAKED %s:\033[0m\n%s\n' "$kind" "$out" >&2
		if [ "$FORCE" = 1 ]; then
			echo "$out" | awk '{print $1}' | while read -r id; do
				hcloud "$kind" delete "$id" && say "deleted $kind $id"
			done
		fi
	fi
done

if [ "${#UNAUDITED[@]}" -gt 0 ]; then
	die "the label audit could NOT reach the Hetzner API for: ${UNAUDITED[*]}. Resources may still be billing. Re-run ./teardown.sh --force when the network is back, and check 'hcloud server list'"
fi
if [ "$LEAKED" = 1 ] && [ "$FORCE" != 1 ]; then
	die "leaked resources found (listed above). Re-run with --force to delete them."
fi
say "nothing leaked — you are not paying for anything"
