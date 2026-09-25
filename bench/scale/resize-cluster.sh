#!/usr/bin/env bash
# Re-form a LIVE provisioning at a smaller (or its full) broker count, so two
# cluster SIZES can be compared on the SAME hardware.
#
#   ./resize-cluster.sh <full-inventory.json> <size> <new-run-dir>
#
# WHY THIS EXISTS. swap-binary.sh made a binary A/B a one-provisioning
# comparison, and said what it could not do: "anything that varies the cluster
# SIZE — sizes are still separate provisionings, so per-node efficiency ratios
# keep their confound." That confound is ~40% (two provisionings of nominally
# identical hardware, same binary, same shape — ADR 0077 T4), and a 5-vs-7
# per-node efficiency question lives in the 10-20% range. Within one
# provisioning repeated rungs land within ~1%. So: provision the LARGEST size
# once, and run the smaller sizes on a prefix of the same brokers and the SAME
# driver fleet.
#
# WHAT IT DOES. Stops mqttd on EVERY broker of the full inventory and clears its
# data dir — a size must never start on another size's store, and a broker left
# out must not be running to rejoin anything. Then writes <new-run-dir>/
# inventory-<size>.json: the first <size> brokers, every driver, node_count set
# to match, every other field untouched. It starts nothing; bootstrap-cluster.sh does, exactly as for a
# fresh cluster (new PKI, founder-first, armed at the new majority).
#
# Always pass the FULL inventory — the one run.sh wrote at provisioning. Growing
# back to the full size is the same call with <size> = its broker count, which
# is how the closing control of an A/B/A returns to the size it opened on.
#
# THE DISCIPLINE. The smaller arm runs on hosts that have already carried the
# larger arm's load, and the brokers it leaves out sit idle. Run A (full), B
# (smaller), then A AGAIN, and void the comparison if the closing A does not
# match the opening A within the ~1% the archive establishes.
#
# WHAT THIS CANNOT DO. The left-out brokers still exist and still bill; the
# smaller arm's per-node work is measured on the prefix only. It does not change
# cloud-init, the systemd unit or kernel posture, and it never touches drivers.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

usage="usage: resize-cluster.sh <full-inventory.json> <size> <new-run-dir>"
INVENTORY="${1:?$usage}"
SIZE="${2:?$usage}"
NEW_RUN="${3:?$usage}"

case "$INVENTORY" in
/*) ;;
*) INVENTORY="$PWD/$INVENTORY" ;;
esac
[ -f "$INVENTORY" ] || die "no such inventory: $INVENTORY"
case "$NEW_RUN" in
/*) ;;
*) NEW_RUN="$SCALE_DIR/$NEW_RUN" ;;
esac

FULL=$(broker_count)
case "$SIZE" in
'' | *[!0-9]* | 0*) die "size must be a positive integer, got '$SIZE'" ;;
esac
[ "$SIZE" -le "$FULL" ] || die "size $SIZE exceeds the $FULL brokers this provisioning has"
# A resized inventory is a PREFIX: resizing it again would silently shrink the
# provisioning the next arm believes it has. Refuse, and name the full one.
if [ "$(inv '.resized_from // empty')" != "" ]; then
	die "$INVENTORY is already a resized inventory (from $(inv '.resized_from')); pass the FULL inventory run.sh wrote"
fi

# rssh keys its known_hosts off RUN. The provisioning run's known_hosts already
# trusts these hosts, and it lives beside the full inventory (swap-binary.sh's
# rule), so the stop/wipe below talks to them under the keys first recorded.
RUN="$(cd "$(dirname "$INVENTORY")" && pwd)"
export RUN
[ -f "$RUN/known_hosts" ] || warn "no known_hosts in $RUN — every host will be accepted on first sight"
[ "$NEW_RUN" != "$RUN" ] || die "the new run dir must not be the provisioning run dir ($RUN) — each size needs its own results/ and PKI"
[ ! -e "$NEW_RUN/inventory-$SIZE.json" ] || die "$NEW_RUN/inventory-$SIZE.json already exists — one arm per run dir"

say "re-forming a $FULL-broker provisioning as a $SIZE-node cluster"

# Every broker, not just the kept prefix: fail closed. A broker that could not be
# stopped may still be a member of the previous cluster, and a number measured
# beside it describes neither size.
for ((i = 0; i < FULL; i++)); do
	ip=$(broker_pub_ip "$i")
	rssh "$ip" "
		set -e
		systemctl stop mqttd
		rm -rf /var/lib/mqttd/*
		! systemctl is-active --quiet mqttd
	" || die "could not stop and clear broker $i ($ip) — the provisioning is in an unknown state; teardown and start over"
	if [ "$i" -lt "$SIZE" ]; then
		say "  broker $i ($(broker_node_id "$i")) stopped and cleared — kept"
	else
		say "  broker $i ($(broker_node_id "$i")) stopped and cleared — LEFT OUT, stays stopped"
	fi
done

mkdir -p "$NEW_RUN"
# The new run starts from the hosts' known keys rather than accepting them again.
[ ! -f "$RUN/known_hosts" ] || cp "$RUN/known_hosts" "$NEW_RUN/known_hosts"
jq --argjson n "$SIZE" --argjson full "$FULL" --arg from "$INVENTORY" \
	'.brokers |= .[:$n] | .node_count = $n | .resized_from = $full | .resized_inventory = $from' \
	"$INVENTORY" >"$NEW_RUN/inventory-$SIZE.json"

# Stamped like SWAPPED-BINARY.txt: the arm's hardware history is part of its result.
{
	echo "RESIZED IN PLACE"
	echo "size=$SIZE"
	echo "provisioned=$FULL"
	echo "full_inventory=$INVENTORY"
	echo "kept=$(jq -r '[.brokers[].node_id] | join(",")' "$NEW_RUN/inventory-$SIZE.json")"
	echo "left_out=$(jq -r --argjson n "$SIZE" '[.brokers[$n:][].node_id] | join(",")' "$INVENTORY")"
} >"$NEW_RUN/RESIZED.txt"

say "every broker is STOPPED with an empty store; $((FULL - SIZE)) left out"
say "next: bootstrap-cluster.sh $NEW_RUN $NEW_RUN/inventory-$SIZE.json durable   # starts the $SIZE kept"
