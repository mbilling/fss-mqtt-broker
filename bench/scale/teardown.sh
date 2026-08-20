#!/usr/bin/env bash
# Standalone teardown / leak sweeper. Normal runs destroy themselves; this is
# for a dead laptop, a lost state file, or paranoia before reading the invoice.
#
#   ./teardown.sh           # terraform destroy from local state, then AUDIT by label
#   ./teardown.sh --force   # also DELETE whatever the label audit finds (needs hcloud CLI)
#
# Everything this rig creates carries the label purpose=mqttd-bench-scale, and
# the README tells the operator to use a DEDICATED Hetzner project — so a forced
# sweep can never touch anything that is not ours.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

FORCE=0
[ "${1:-}" = --force ] && FORCE=1

TFDIR="$SCALE_DIR/terraform"
TF=$(command -v terraform || command -v tofu || true)

if [ -n "$TF" ] && [ -f "$TFDIR/terraform.tfstate" ]; then
	say "terraform destroy from local state"
	(cd "$TFDIR" && "$TF" destroy -auto-approve -var node_count=1) ||
		warn "terraform destroy failed — continuing to the label audit"
fi

if ! command -v hcloud >/dev/null; then
	warn "hcloud CLI not installed — cannot audit by label."
	warn "Verify by hand in the Hetzner console that NO servers remain in the bench project."
	exit 0
fi

say "auditing by label purpose=mqttd-bench-scale"
LEAKED=0
for kind in server network firewall placement-group ssh-key; do
	out=$(hcloud "$kind" list -l purpose=mqttd-bench-scale -o noheader 2>/dev/null || true)
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

if [ "$LEAKED" = 1 ] && [ "$FORCE" != 1 ]; then
	die "leaked resources found (listed above). Re-run with --force to delete them."
fi
say "nothing leaked — you are not paying for anything"
