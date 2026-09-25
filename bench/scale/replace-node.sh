#!/usr/bin/env bash
# Swap ONE bad host of a live provisioning for a fresh one, in place.
#
#   ./replace-node.sh <inventory.json> <driver|broker> <index> [reason]
#
# WHY THIS EXISTS. On 2026-09-25 one driver of eighteen (bench-driver-9) came up
# with a core pinned at 98-100% softirq under the same one-site load its peers
# carried at <20%. Its scrapes took ~7.5 s, which voided every rung it loaded,
# and its site under-offered by ~29%. That is a host draw, not a broker result,
# and the only remedy the rig had was to destroy all 25 servers and hope.
#
# Hetzner servers are cattle and this rig's addressing is by index: a server's
# PRIVATE IP is fixed by terraform (brokers 10.99.1.11+i, drivers 10.99.1.21+i),
# so `tofu apply -replace` swaps the machine and keeps its address. Only the
# public IP and the host key change, and both are refreshed here.
#
# WHAT IT DOES. Re-applies with the exact variables run.sh recorded for this
# provisioning (tf-apply-args-<N>.sh) plus -replace for that one server, so
# nothing else changes; rewrites the provisioning inventory from tofu's output
# and carries the new public IP into the arm inventory that was passed (a
# resized arm's prefix); drops the stale host keys; waits for ssh, cloud-init,
# the private address and a clean private mesh in both directions; and, for a
# driver, installs the cluster CA and client certificate of the arm's PKI — a
# driver needs nothing else, since every rung starts its containers fresh.
#
# A BROKER is replaced but left unconfigured: it has no certificates and is not
# a member of anything. Its cluster must be re-formed (resize-cluster.sh at the
# arm's size, then bootstrap-cluster.sh), which is also the only honest thing to
# do — a broker swapped under a running arm changes the hardware the arm
# measures. Swap brokers only before an arm's measurement starts.
#
# Every swap is appended to <arm-dir>/REPLACED.txt: a result is attributable to
# the hosts that produced it.

set -euo pipefail
. "$(dirname "$0")/lib.sh"
. "$SCALE_DIR/cloud.sh"
select_scale_cloud

usage="usage: replace-node.sh <inventory.json> <driver|broker> <index> [reason]"
INVENTORY="${1:?$usage}"
KIND="${2:?$usage}"
IDX="${3:?$usage}"
REASON="${4:-unspecified}"

[ "$CLOUD" = hcloud ] || die "replace-node.sh addresses hcloud_server resources; CLOUD=$CLOUD is not supported"
case "$KIND" in driver | broker) ;; *) die "kind must be driver or broker, got '$KIND'" ;; esac
case "$IDX" in '' | *[!0-9]*) die "index must be a non-negative integer, got '$IDX'" ;; esac
case "$INVENTORY" in
/*) ;;
*) INVENTORY="$PWD/$INVENTORY" ;;
esac
[ -f "$INVENTORY" ] || die "no such inventory: $INVENTORY"

ARM_DIR="$(cd "$(dirname "$INVENTORY")" && pwd)"
ARM_INV="$INVENTORY"
# A resized arm's inventory names the provisioning it is a prefix of; that is
# where run.sh left the apply arguments, the tofu-shaped inventory and the
# provisioning's known_hosts.
PROV_INV="$(jq -r '.resized_inventory // empty' "$ARM_INV")"
[ -n "$PROV_INV" ] || PROV_INV="$ARM_INV"
[ -f "$PROV_INV" ] || die "the provisioning inventory $PROV_INV is gone"
PROV_DIR="$(cd "$(dirname "$PROV_INV")" && pwd)"
FULL=$(jq -r '.brokers | length' "$PROV_INV")
ARGS_FILE="$PROV_DIR/tf-apply-args-$FULL.sh"
[ -f "$ARGS_FILE" ] || die "no $ARGS_FILE — only a provisioning run.sh recorded can have a host replaced (anything else would re-apply with different variables and rebuild every server)"
# shellcheck disable=SC1090 # written by run.sh as `declare -p TF_APPLY_ARGS`
. "$ARGS_FILE"
[ "${#TF_APPLY_ARGS[@]}" -gt 0 ] || die "$ARGS_FILE holds no arguments"

plural="${KIND}s"
COUNT=$(jq -r ".$plural | length" "$PROV_INV")
[ "$IDX" -lt "$COUNT" ] || die "$KIND index $IDX is out of range (the provisioning has $COUNT)"
OLD_PUB=$(jq -r ".${plural}[$IDX].public_ip" "$PROV_INV")
PRIV=$(jq -r ".${plural}[$IDX].private_ip" "$PROV_INV")
TF=$(command -v tofu) || die "OpenTofu (tofu) is required"
require_scale_token

start=$(date +%s)
stamp=$(date -u +%Y%m%dT%H%M%SZ)
say "replacing $KIND $IDX ($OLD_PUB / $PRIV) — $REASON"
(cd "$TFDIR" && "$TF" apply -auto-approve -input=false "${TF_APPLY_ARGS[@]}" \
	-replace="hcloud_server.${KIND}[$IDX]" \
	>"$PROV_DIR/tf-replace-$KIND$IDX-$stamp.log" 2>&1) || {
	tail -30 "$PROV_DIR/tf-replace-$KIND$IDX-$stamp.log" >&2
	die "tofu apply -replace failed for $KIND $IDX — the provisioning may be short one host; teardown if in doubt"
}
tmp="$(mktemp "$PROV_DIR/.inventory.XXXXXX")"
(cd "$TFDIR" && "$TF" output -json inventory) >"$tmp"
for k in brokers drivers; do
	[ "$(jq -r ".$k | length" "$tmp")" = "$(jq -r ".$k | length" "$PROV_INV")" ] ||
		die "after the replace tofu reports a different number of $k — refusing to rewrite the inventory (kept at $tmp)"
done
[ "$(jq -r ".${plural}[$IDX].private_ip" "$tmp")" = "$PRIV" ] ||
	die "the replacement did not keep private IP $PRIV — the rig's addressing no longer holds (kept at $tmp)"
NEW_PUB=$(jq -r ".${plural}[$IDX].public_ip" "$tmp")
mv "$tmp" "$PROV_INV"
if [ "$ARM_INV" != "$PROV_INV" ]; then
	# Carry only the replaced entry into the arm's prefix inventory; every other
	# field of the arm (its resize stamp, its broker prefix) stays as it was.
	if [ "$KIND" = driver ] || [ "$IDX" -lt "$(jq -r '.brokers | length' "$ARM_INV")" ]; then
		entry=$(jq -c ".${plural}[$IDX]" "$PROV_INV")
		jq --argjson e "$entry" ".${plural}[$IDX] = \$e" "$ARM_INV" >"$ARM_INV.tmp" && mv "$ARM_INV.tmp" "$ARM_INV"
	fi
fi
for dir in "$PROV_DIR" "$ARM_DIR"; do
	[ -f "$dir/known_hosts" ] || continue
	ssh-keygen -R "$OLD_PUB" -f "$dir/known_hosts" >/dev/null 2>&1 || true
	ssh-keygen -R "$NEW_PUB" -f "$dir/known_hosts" >/dev/null 2>&1 || true
done
say "  $KIND $IDX is now $NEW_PUB (private $PRIV unchanged)"

# The same readiness run.sh demands of a fresh provisioning, for one host.
RUN="$ARM_DIR"
export RUN
CI_OK='cloud-init status --wait >/dev/null 2>&1; rc=$?; [ $rc -eq 0 ] || [ $rc -eq 2 ]'
wait_for "ssh on $NEW_PUB" 300 rssh "$NEW_PUB" true
rssh "$NEW_PUB" "$CI_OK" || die "cloud-init failed on the replacement $KIND $IDX ($NEW_PUB)"
wait_for "cloud-init marker on $NEW_PUB" 900 rssh "$NEW_PUB" "test -f /run/bench-cloudinit-done"
wait_for "private ip $PRIV on $NEW_PUB" 180 rssh "$NEW_PUB" "ip -4 addr show | grep -qF $PRIV"
ALL_PRIVS=$(jq -r '[(.brokers[], .drivers[]) | .private_ip] | join(" ")' "$PROV_INV")
wait_for "private mesh from the replacement" 300 \
	rssh "$NEW_PUB" "for t in $ALL_PRIVS; do ping -c1 -W2 \$t >/dev/null || exit 1; done"
for pub in $(jq -r '(.brokers[], .drivers[]) | .public_ip' "$ARM_INV"); do
	[ "$pub" = "$NEW_PUB" ] && continue
	wait_for "private mesh from $pub to $PRIV" 120 rssh "$pub" "ping -c1 -W2 $PRIV >/dev/null"
done

if [ "$KIND" = driver ]; then
	PKI="$ARM_DIR/pki-$(jq -r '.brokers | length' "$ARM_INV")"
	[ -d "$PKI" ] || die "no PKI at $PKI — bootstrap-cluster.sh has not formed this arm's cluster, so there are no client certificates to give the driver"
	rssh "$NEW_PUB" "mkdir -p /opt/bench-certs"
	rscp "$PKI/cluster/ca/peer-ca.pem" "$PKI/client-tls/certs/client.pem" \
		"$PKI/client-tls/certs/client.key" "root@$NEW_PUB:/opt/bench-certs/"
	rssh "$NEW_PUB" "chmod 644 /opt/bench-certs/*"
fi

took=$(($(date +%s) - start))
echo "$stamp kind=$KIND index=$IDX private=$PRIV old=$OLD_PUB new=$NEW_PUB secs=$took reason=$REASON" >>"$ARM_DIR/REPLACED.txt"
if [ "$KIND" = broker ]; then
	say "broker $IDX replaced in ${took}s and left UNCONFIGURED — re-form the cluster (resize-cluster.sh, then bootstrap-cluster.sh) before measuring"
else
	say "driver $IDX replaced and ready in ${took}s"
fi
