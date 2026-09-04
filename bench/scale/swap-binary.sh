#!/usr/bin/env bash
# Replace the broker binary on a LIVE cluster, so two builds can be compared on
# the SAME hardware.
#
#   ./swap-binary.sh <inventory.json> <url> <sha256>
#
# WHY THIS EXISTS. The rig provisions a fresh cluster per run and installs the
# broker from cloud-init, so comparing two builds meant comparing two
# provisionings — and two provisionings of nominally identical hardware have
# been measured 40% apart on the same binary at the same shape (ADR 0077 T4).
# That spread is wider than almost every effect a performance change produces,
# so the rig could measure a 93x improvement and nothing smaller.
#
# Within ONE provisioning it is a different instrument entirely. Auditing every
# repeated rung in the archive (2026-09-04): 15 of 17 landed within 1% of their
# first pass. The two that did not are both 12-site rungs with locality OFF,
# where the peer queues are unbounded (issue #504) — a known defect in a
# configuration that has not been the default since v1.0.14.
#
# So: swap the binary in place, keep the hardware, and a 1%-resolution
# comparison becomes possible.
#
# THE DISCIPLINE THIS IS ONLY HALF OF. Arm B necessarily runs on hosts that have
# already carried arm A's load. Run A, then B, then A AGAIN, and void the whole
# comparison if the closing A does not match the opening A within the ~1% the
# archive establishes. That turns "the cluster surely recovered" from an
# assumption into a measured precondition. See the recipe at the bottom.
#
# WHAT THIS CANNOT DO. It swaps a BINARY. A change that alters cloud-init, the
# systemd unit, or the kernel/sysctl posture is not comparable this way, and
# neither is anything that varies the cluster SIZE — sizes are still separate
# provisionings, so per-node efficiency ratios keep their confound.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

INVENTORY="${1:?usage: swap-binary.sh <inventory.json> <url> <sha256>}"
URL="${2:?a URL for a musl x86_64 mqttd binary}"
SHA="${3:?the sha256 of that binary — an arbitrary URL publishes no .sha256 beside it}"

[ -f "$INVENTORY" ] || die "no such inventory: $INVENTORY"
case "$SHA" in
[0-9a-f]*) [ ${#SHA} -eq 64 ] || die "sha256 must be 64 hex characters, got ${#SHA}" ;;
*) die "sha256 must be lowercase hex, got '$SHA'" ;;
esac

# rssh keys its known_hosts off RUN and refuses to fall back to the operator's
# default file — an unset RUN once had rig traffic evict github.com from it. The
# inventory lives in the run directory that provisioned this cluster, and that
# is the run whose known_hosts already trusts these hosts, so derive it rather
# than asking the caller for something it has no way to get wrong.
RUN="$(cd "$(dirname "$INVENTORY")" && pwd)"
export RUN
[ -f "$RUN/known_hosts" ] || warn "no known_hosts in $RUN — every host will be accepted on first sight"

N=$(broker_count)
say "swapping the broker binary on $N node(s)"
say "  url    $URL"
say "  sha256 $SHA"

# One host at a time, and fail closed. A cluster running two different builds is
# worse than a failed swap: it would produce a number that describes neither.
for ((i = 0; i < N; i++)); do
	ip=$(broker_pub_ip "$i")
	rssh "$ip" "
		set -e
		systemctl stop mqttd
		curl -fsSL -o /tmp/mqttd.new '$URL'
		echo '$SHA  /tmp/mqttd.new' | sha256sum -c -
		install -m 0755 /tmp/mqttd.new /usr/local/bin/mqttd
		rm -f /tmp/mqttd.new
		# A new binary must never start on the previous arm store: stale
		# sessions or retained state would be attributed to the build.
		rm -rf /var/lib/mqttd/*
	" || die "swap failed on broker $i ($ip) — the cluster is now MIXED; teardown and start over"
	say "  broker $i swapped and stopped (bootstrap will start it)"
done

# Stamp it the way run.sh stamps an unreleased run: a number measured against a
# hand-swapped binary is not a published-curve point and must not be mistaken
# for one.
{
	echo "BINARY SWAPPED IN PLACE"
	echo "url=$URL"
	echo "sha256=$SHA"
	echo "brokers=$N"
	echo "inventory=$INVENTORY"
} >"$RUN/SWAPPED-BINARY.txt"

say "every broker now holds the new binary and is STOPPED"
say "next: bootstrap-cluster.sh <new-run-dir> $INVENTORY clean   # starts them"
cat >&2 <<'RECIPE'

  ── the A/B/A recipe ────────────────────────────────────────────────────────
  KEEP_INFRA=1 RUN_DIR=.runs/x-A1 ./run.sh standard 5
  ./swap-binary.sh .runs/x-A1/inventory-5.json "$URL_B" "$SHA_B"
  ./bootstrap-cluster.sh .runs/x-B .runs/x-A1/inventory-5.json clean
  ./run-curve.sh "$PWD/.runs/x-B" .runs/x-A1/inventory-5.json
  ./swap-binary.sh .runs/x-A1/inventory-5.json "$URL_A" "$SHA_A"
  ./bootstrap-cluster.sh .runs/x-A2 .runs/x-A1/inventory-5.json clean
  ./run-curve.sh "$PWD/.runs/x-A2" .runs/x-A1/inventory-5.json
  ./teardown.sh

  Then compare A1 with A2 FIRST. If they differ by more than ~1% the hardware
  drifted under the experiment and the A-vs-B difference means nothing.
  ─────────────────────────────────────────────────────────────────────────────
RECIPE
