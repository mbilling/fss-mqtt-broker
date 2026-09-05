#!/usr/bin/env bash
# bootstrap-cluster.sh <run-dir> <inventory.json> <durable|clean>
#
# Takes a freshly applied cluster from "cloud-init finished" to "every node
# READY and armed": mints the per-size secrets on THIS machine, pushes them over
# SSH, renders /etc/mqttd/mqttd.env per node, and performs the founder-first
# start + arm sequence that scripts/deploy-smoke.sh proves in CI and
# deploy/systemd/README.md documents for operators.
#
# Secret custody: the cluster CA key and the client CA key never leave this
# machine (gen-certs.sh's rule); only leaf material, the CA *certificates* and
# the swim key travel — over SSH, never through cloud-init user_data or
# terraform state.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

RUN="${1:?usage: bootstrap-cluster.sh <run-dir> <inventory.json> <durable|clean>}"
# shellcheck disable=SC2034 # consumed by lib.sh's inv() helper
INVENTORY="${2:?inventory.json}"
MODE="${3:?durable|clean}"

# The inventory path must be absolute for the same reason the run dir must be:
# steps below expand `inv()` INSIDE a subshell that has already `cd`-ed
# elsewhere (the PKI mint does exactly this), and a relative path stops
# resolving there. It fails as empty jq output rather than an error, so the
# symptom is a certificate minted with no CN — not "file not found".
case "$INVENTORY" in
/*) ;;
*) INVENTORY="$PWD/$INVENTORY" ;;
esac
[ -f "$INVENTORY" ] || die "no such inventory: $INVENTORY"


# A RELATIVE run dir is resolved against the rig, exactly as run.sh does and for
# exactly the same reason: several steps `cd` into a subdirectory and then
# redirect using the same path, which stops resolving the moment it is relative.
# run.sh always passes an absolute path, so this only bites a caller driving the
# scripts directly — which is precisely what the swap-binary.sh A/B recipe does.
case "$RUN" in
/*) ;;
*) RUN="$SCALE_DIR/$RUN" ;;
esac
mkdir -p "$RUN"


N=$(broker_count)
say "bootstrapping a $N-node cluster (mode: $MODE)"

# ── 1. Mint per-size secrets locally ─────────────────────────────────────────
PKI="$RUN/pki-$N"
if [ ! -d "$PKI" ]; then
	mkdir -p "$PKI"
	OPENSSL_BIN=$(pick_openssl)
	say "minting cluster PKI with deploy/systemd/gen-certs.sh (OpenSSL: $OPENSSL_BIN)"
	(cd "$PKI" && OPENSSL="$OPENSSL_BIN" PKI_DIR="$PKI/cluster" \
		sh "$REPO_ROOT/deploy/systemd/gen-certs.sh" ca >"$PKI/gen-certs.log" 2>&1) ||
		{ cat "$PKI/gen-certs.log" >&2; die "gen-certs.sh ca failed"; }
	for ((i = 0; i < N; i++)); do
		# The private IP is both the peer-advertise host and the address MQTT
		# clients dial, so it goes in as an extra (client-facing) SAN too.
		(cd "$PKI" && OPENSSL="$OPENSSL_BIN" PKI_DIR="$PKI/cluster" \
			sh "$REPO_ROOT/deploy/systemd/gen-certs.sh" node \
			"$(broker_node_id "$i")" "$(broker_priv_ip "$i")" "$(broker_priv_ip "$i")" \
			>>"$PKI/gen-certs.log" 2>&1) ||
			{ cat "$PKI/gen-certs.log" >&2; die "gen-certs.sh node $(broker_node_id "$i") failed"; }
	done
	# ONE swim key for the whole cluster (the env example's loudest rule).
	python3 -c "import secrets;print(secrets.token_hex(32))" >"$PKI/swim.key"
	# The CLIENT CA for the mTLS posture — a different CA from the cluster bus,
	# minted by the comparative bench's throwaway-PKI script. That script writes
	# to certs/ beside itself, so it runs from a copy inside the run dir.
	CLIENT_TLS="$PKI/client-tls"
	mkdir -p "$CLIENT_TLS"
	cp "$REPO_ROOT/bench/tls/gen-certs.sh" "$CLIENT_TLS/gen-certs.sh"
	sh "$CLIENT_TLS/gen-certs.sh" >>"$PKI/gen-certs.log" 2>&1 ||
		{ cat "$PKI/gen-certs.log" >&2; die "bench/tls/gen-certs.sh (client CA) failed"; }
fi

# ── 2. Render one env file per node ──────────────────────────────────────────
# Majority floor for the target size; the founder boots at 1 and is ARMED to the
# majority after formation (deploy/systemd/README.md's sequence).
MAJORITY=$(((N / 2) + 1))
seeds_for() { # seeds_for <index> — two other nodes' swim addresses (founder: empty)
	local i="$1" out=() j
	[ "$i" -eq 0 ] && { echo ""; return; }
	for ((j = 0; j < N; j++)); do
		[ "$j" -ne "$i" ] && out+=("$(broker_priv_ip "$j"):7946")
	done
	(IFS=,; echo "${out[*]:0:2}")
}

render_env() { # render_env <index> <ready-min> <seeds> > file
	local i="$1" ready="$2" seeds="$3" durable_line
	if [ "$MODE" = clean ]; then
		durable_line="MQTTD_DURABLE_SESSIONS=0"
	else
		durable_line="# durable plane ON (lane A)"
	fi
	sed -e "s|@NODE_ID@|$(broker_node_id "$i")|g" \
		-e "s|@PRIVATE_IP@|$(broker_priv_ip "$i")|g" \
		-e "s|@SWIM_SEEDS@|$seeds|g" \
		-e "s|@READY_MIN_MEMBERS@|$ready|g" \
		-e "s|@DURABLE_LINE@|$durable_line|g" \
		"$SCALE_DIR/templates/mqttd.env.tmpl"
	# Disclosed per-variant override (e.g. MQTTD_LEASE_VOTERS=7 for the
	# past-the-cap lane). Appended last so it wins, and it lands in the env
	# dumps the run archives — a variant can never run undisclosed.
	[ -z "${EXTRA_BROKER_ENV:-}" ] || printf '%s\n' "$EXTRA_BROKER_ENV"
}

# ── 3. Push secrets + env to every node ──────────────────────────────────────
push_node() { # push_node <index> <ready-min> <seeds>
	local i="$1" ready="$2" seeds="$3"
	local ip id
	ip=$(broker_pub_ip "$i")
	id=$(broker_node_id "$i")
	render_env "$i" "$ready" "$seeds" >"$RUN/mqttd-$((i + 1)).env"
	rscp "$RUN/mqttd-$((i + 1)).env" "root@$ip:/tmp/mqttd.env"
	rscp "$PKI/cluster/$id/peer-ca.pem" \
		"$PKI/cluster/$id/peer.pem" "$PKI/cluster/$id/peer.key" \
		"$PKI/cluster/$id/server.pem" "$PKI/cluster/$id/server.key" \
		"$PKI/client-tls/certs/ca.pem" "$PKI/swim.key" "root@$ip:/tmp/"
	# Per-file installs with per-file modes — gen-certs.sh's printed recipe; a
	# glob chmod would hand every key on the host to the mqttd group.
	rssh "$ip" '
		set -e
		# A boot-time auto-start before the config existed may have exhausted
		# the unit start limit (the clean+reboot retry path); clear it so the
		# restart below is judged on THIS start, not the doomed earlier ones.
		systemctl reset-failed mqttd 2>/dev/null || true
		install -m 0640 -o root -g mqttd /tmp/mqttd.env /etc/mqttd/mqttd.env
		install -m 0640 -o root -g mqttd /tmp/swim.key /etc/mqttd/swim.key
		install -m 0644 -o root -g mqttd /tmp/peer-ca.pem /etc/mqttd/tls/peer-ca.pem
		install -m 0644 -o root -g mqttd /tmp/peer.pem /etc/mqttd/tls/peer.pem
		install -m 0640 -o root -g mqttd /tmp/peer.key /etc/mqttd/tls/peer.key
		install -m 0644 -o root -g mqttd /tmp/server.pem /etc/mqttd/tls/server.pem
		install -m 0640 -o root -g mqttd /tmp/server.key /etc/mqttd/tls/server.key
		install -m 0644 -o root -g mqttd /tmp/ca.pem /etc/mqttd/tls/client-ca.pem
		rm -f /tmp/mqttd.env /tmp/swim.key /tmp/peer-ca.pem /tmp/peer.pem /tmp/peer.key \
			/tmp/server.pem /tmp/server.key /tmp/ca.pem
	'
}

for ((i = 0; i < N; i++)); do
	wait_for "cloud-init on broker $i" 600 \
		rssh "$(broker_pub_ip "$i")" "test -f /run/bench-cloudinit-done"
done

# Drivers get the mTLS-posture client material: the cluster CA *certificate*
# (verifies the brokers' server certs) and the client leaf. No private CA key
# ever leaves this machine.
ND=$(driver_count)
for ((i = 0; i < ND; i++)); do
	dip=$(driver_pub_ip "$i")
	wait_for "cloud-init on driver $i" 600 rssh "$dip" "test -f /run/bench-cloudinit-done"
	rssh "$dip" "mkdir -p /opt/bench-certs"
	rscp "$PKI/cluster/ca/peer-ca.pem" "$PKI/client-tls/certs/client.pem" \
		"$PKI/client-tls/certs/client.key" "root@$dip:/opt/bench-certs/"
	rssh "$dip" "chmod 644 /opt/bench-certs/*"
done

# ── 3.5 private-net full-mesh gate (issue #393 forensics) ────────────────────
# After the cloud-init attach retry the fabric can drop a host's OUTBOUND
# private-net packets for tens of minutes while still DELIVERING inbound —
# a cluster formed inside that window evicts and permanently removes the
# muted node (SWIM acks and raft dials all die one-way; #393). Prove every
# broker reaches every other over the private net, stably, before the first
# mqttd starts.
mesh_sweep() {
	local i j targets
	for ((i = 0; i < N; i++)); do
		targets=""
		for ((j = 0; j < N; j++)); do
			[ "$i" = "$j" ] || targets="$targets $(broker_priv_ip "$j")"
		done
		rssh "$(broker_pub_ip "$i")" "for t in$targets; do ping -c1 -W2 \$t >/dev/null || exit 1; done" || return 1
	done
}
mesh_stable() { mesh_sweep && sleep 5 && mesh_sweep && sleep 5 && mesh_sweep; }
if [ "$N" -gt 1 ]; then
	wait_for "private-net full mesh (every broker reaches every other)" 1200 mesh_sweep
	wait_for "private-net mesh stability (3 clean sweeps 5s apart)" 300 mesh_stable
	say "private-net full mesh verified stable"
fi

# ready_within <index> <secs>: 0 once the node's /readyz answers 200, 1 when the
# budget runs out — the non-dying sibling of lib.sh's wait_ready, for a retry.
ready_within() {
	local i="$1" budget="$2" start
	start=$(date +%s)
	until rssh "$(broker_pub_ip "$i")" "curl -sf http://localhost:8080/readyz" >/dev/null 2>&1; do
		[ $(($(date +%s) - start)) -lt "$budget" ] || return 1
		sleep 3
	done
}
# follower_ready <index>: every 10-node formation on record goes through an
# election storm ~30s after the followers start (early joiners cannot reach the
# founder, terms climb, the founder re-wins); it normally settles inside the
# 180s budget, but twice on 2026-08-25 — on a 1.0.0 binary the rig had defaulted
# to, six releases behind the formation fixes — one follower's join was lost in
# it and the node sat SWIM-green and Raft-leaderless for the whole budget, which
# then cost the entire run (issue #426). A follower's readiness cannot be waited for
# one at a time (it needs the majority floor of members alive), so the remedy
# is what an operator would do: keep the evidence, restart THAT node once so it
# re-announces and joins after the storm has settled, and wait once more.
follower_ready() {
	local i="$1" ip id
	ip=$(broker_pub_ip "$i")
	id=$(broker_node_id "$i")
	ready_within "$i" 180 && return 0
	mkdir -p "$RUN/formation"
	rssh "$ip" "curl -s 'http://localhost:8080/readyz?verbose=1'; echo; curl -s http://localhost:8080/statusz; echo; journalctl -u mqttd --no-pager | tail -60" \
		>"$RUN/formation/$id-not-ready.txt" 2>&1 || true
	warn "$id not ready after 180s — evidence kept in $RUN/formation/$id-not-ready.txt; restarting it once (issue #426)"
	rssh "$ip" "systemctl restart mqttd"
	ready_within "$i" 180 ||
		die "$id still not ready 180s after a restart — see $RUN/formation/$id-not-ready.txt"
	say "  $id ready after its restart"
}

# ── 4. Founder-first start, then the rest, then ARM the founder ──────────────
# Founder: no seeds, floor 1 (or it can never come up alone).
push_node 0 1 ""
rssh "$(broker_pub_ip 0)" "systemctl restart mqttd"
wait_ready 0 300
say "founder $(broker_node_id 0) is READY"

for ((i = 1; i < N; i++)); do
	push_node "$i" "$MAJORITY" "$(seeds_for "$i")"
	rssh "$(broker_pub_ip "$i")" "systemctl restart mqttd"
done
for ((i = 1; i < N; i++)); do
	follower_ready "$i"
done

if [ "$N" -gt 1 ]; then
	# Arm the founder BEFORE any load: seeds filled, floor raised to majority.
	# During measurement, readiness means majority on every node.
	push_node 0 "$MAJORITY" "$(broker_priv_ip 1):7946,$(broker_priv_ip $((N - 1))):7946"
	rssh "$(broker_pub_ip 0)" "systemctl restart mqttd"
	# The founder's restart is the second fragile moment (run 170554Z, mqttd
	# 1.0.6): one follower's peer link to the restarted founder was never
	# rebuilt — peer_links 8 on both ends, SWIM alive 10 — and it campaigned
	# alone to term 45 while the other eight sat ready at term 15. The same
	# one-restart retry as the join phase: a fresh process re-greets.
	wait_ready 0 180
	for ((i = 1; i < N; i++)); do
		follower_ready "$i"
	done
	say "founder armed; all $N nodes READY at majority floor $MAJORITY"
fi

say "cluster is up ($MODE mode)"
