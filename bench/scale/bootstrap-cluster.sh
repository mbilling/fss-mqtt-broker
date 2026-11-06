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
#
# JOIN_FROM=<k>: grow a running cluster instead. Brokers 0..k-1 of the inventory
# already run as one cluster this run dir bootstrapped (from an inventory of its
# first k brokers, so its PKI is <run-dir>/pki-<k>); brokers k..N-1 join it under
# whatever load it carries, one at a time, as an operator grows a cluster
# (deploy/systemd/README.md): a node certificate from the SAME cluster CA, the same
# swim key, seeds naming two running members, the readiness floor at the majority of
# the new size, then start it, wait for its /readyz, and then for the
# cluster to be FULLY FORMED (cluster-formed.py, on every member: all members alive and
# none suspect, a full peer mesh, the voter set) before the next joiner starts: a
# joiner is ready at its majority floor seconds before the others have it in their
# membership, and a second joiner started then overlaps the first one's join. The
# running members are not touched: their seed lists and floors stay as they were (a
# restart to rewrite them is not part of growing a cluster). Each joiner is appended
# to JOINS_TSV (default <run-dir>/joins.tsv): index, start and ready (unix ms, its own
# clock), and formed (unix ms, this machine's clock).

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
JOIN_FROM="${JOIN_FROM:-}"
if [ -n "$JOIN_FROM" ]; then
	case "$JOIN_FROM" in '' | *[!0-9]*) die "JOIN_FROM must be a broker count, not '$JOIN_FROM'" ;; esac
	[ "$JOIN_FROM" -ge 1 ] && [ "$JOIN_FROM" -lt "$N" ] ||
		die "JOIN_FROM=$JOIN_FROM: the inventory has $N brokers, so 1..$((N - 1)) of them can be running"
	say "joining brokers $JOIN_FROM..$((N - 1)) to the running $JOIN_FROM-node cluster (mode: $MODE)"
else
	say "bootstrapping a $N-node cluster (mode: $MODE)"
fi
JOINS_TSV="${JOINS_TSV:-$RUN/joins.tsv}"

# PEER_NET=private|public picks the network the peer bus (MQTTD_PEER_ADVERTISE,
# :7001) runs over (#662): replication between brokers is one TCP connection
# per pair, and the private network is an overlay (MTU 1450) whose path may
# behave differently from the public one. SWIM, client traffic and scrapes stay
# private. `public` puts each broker's PUBLIC address in its advertise line and
# its peer certificate's SAN, and opens :7001 on the cloud firewall to exactly
# the brokers' own public /32s (the firewall filters only the public side).
PEER_NET="${PEER_NET:-private}"
# How long the node certificates of a rig cluster live. gen-certs.sh mints nothing without
# a stated lifetime (ADR 0088), so the rig states one: seven days outlasts any campaign on
# a cluster that is destroyed when its run ends, and at two thirds of it (4 d 16 h) the
# brokers start warning. A run meant to cross an expiry sets RIG_CERT_LIFETIME (<N>h|<N>d).
CERT_LIFETIME="${RIG_CERT_LIFETIME:-7d}"
case "$PEER_NET" in private | public) ;; *) die "PEER_NET must be private or public, not '$PEER_NET'" ;; esac
[ "$PEER_NET" = private ] || [ "${CLOUD:-hcloud}" = hcloud ] || die "PEER_NET=public is only wired for CLOUD=hcloud"
peer_host() { # peer_host <index> — the address the peer bus advertises
	if [ "$PEER_NET" = public ]; then broker_pub_ip "$1"; else broker_priv_ip "$1"; fi
}

# ── 1. Mint per-size secrets locally ─────────────────────────────────────────
PKI="$RUN/pki-$N"
# A join extends the running cluster's PKI: the same CA signs the joiners.
[ -z "$JOIN_FROM" ] || PKI="$RUN/pki-$JOIN_FROM"
# A PKI minted for one PEER_NET has the other network's SANs on its peer certs.
if [ -d "$PKI" ] && [ "$(cat "$PKI/peer-net" 2>/dev/null || echo private)" != "$PEER_NET" ]; then
	die "$PKI was minted for PEER_NET=$(cat "$PKI/peer-net" 2>/dev/null || echo private), not $PEER_NET — use a fresh run dir"
fi
if [ -n "$JOIN_FROM" ]; then
	[ -d "$PKI/cluster/ca" ] || die "JOIN_FROM=$JOIN_FROM: no running cluster's PKI at $PKI (bootstrap the first $JOIN_FROM brokers from this run dir first)"
	[ "$PEER_NET" = private ] || die "a join is only wired for PEER_NET=private (the public peer port's firewall rule names the brokers of the bootstrap)"
	OPENSSL_BIN=$(pick_openssl)
	for ((i = JOIN_FROM; i < N; i++)); do
		[ ! -d "$PKI/cluster/$(broker_node_id "$i")" ] || continue
		say "minting a node certificate for joiner $(broker_node_id "$i") from the running cluster's CA"
		(cd "$PKI" && OPENSSL="$OPENSSL_BIN" PKI_DIR="$PKI/cluster" \
			sh "$REPO_ROOT/deploy/systemd/gen-certs.sh" node --lifetime "$CERT_LIFETIME" \
			"$(broker_node_id "$i")" "$(peer_host "$i")" "$(broker_priv_ip "$i")" \
			>>"$PKI/gen-certs.log" 2>&1) ||
			{ cat "$PKI/gen-certs.log" >&2; die "gen-certs.sh node $(broker_node_id "$i") failed"; }
	done
elif [ ! -d "$PKI" ]; then
	mkdir -p "$PKI"
	echo "$PEER_NET" >"$PKI/peer-net"
	OPENSSL_BIN=$(pick_openssl)
	say "minting cluster PKI with deploy/systemd/gen-certs.sh (OpenSSL: $OPENSSL_BIN)"
	(cd "$PKI" && OPENSSL="$OPENSSL_BIN" PKI_DIR="$PKI/cluster" \
		sh "$REPO_ROOT/deploy/systemd/gen-certs.sh" ca >"$PKI/gen-certs.log" 2>&1) ||
		{ cat "$PKI/gen-certs.log" >&2; die "gen-certs.sh ca failed"; }
	for ((i = 0; i < N; i++)); do
		# The peer-advertise host (the private IP unless PEER_NET=public) is the
		# peer certificate's SAN; the private IP is the address MQTT clients
		# dial, so it goes in as the extra (client-facing) SAN.
		(cd "$PKI" && OPENSSL="$OPENSSL_BIN" PKI_DIR="$PKI/cluster" \
			sh "$REPO_ROOT/deploy/systemd/gen-certs.sh" node --lifetime "$CERT_LIFETIME" \
			"$(broker_node_id "$i")" "$(peer_host "$i")" "$(broker_priv_ip "$i")" \
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
		-e "s|@PEER_HOST@|$(peer_host "$i")|g" \
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

for ((i = ${JOIN_FROM:-0}; i < N; i++)); do
	wait_for "cloud-init on broker $i" 600 \
		rssh "$(broker_pub_ip "$i")" "test -f /run/bench-cloudinit-done"
done

# Drivers get the mTLS-posture client material: the cluster CA *certificate*
# (verifies the brokers' server certs) and the client leaf. No private CA key
# ever leaves this machine.
ND=$(driver_count)
[ -z "$JOIN_FROM" ] || ND=0
for ((i = 0; i < ND; i++)); do
	dip=$(driver_pub_ip "$i")
	wait_for "cloud-init on driver $i" 600 rssh "$dip" "test -f /run/bench-cloudinit-done"
	rssh "$dip" "mkdir -p /opt/bench-certs"
	rscp "$PKI/cluster/ca/peer-ca.pem" "$PKI/client-tls/certs/client.pem" \
		"$PKI/client-tls/certs/client.key" "root@$dip:/opt/bench-certs/"
	rssh "$dip" "chmod 644 /opt/bench-certs/*"
done

# ── 3.4 PEER_NET=public: open the peer port to the brokers, and only them ────
# Done here, after any resize, because the firewall is tofu's: a re-apply drops
# the rule. replace-node.sh re-opens it after its own apply when the arm's PKI
# was minted public; the next public arm opens it anyway. Teardown destroys the
# firewall with it.
[ "$PEER_NET" = private ] || open_public_peer_port "$RUN"

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

# ── 3.6 JOIN_FROM: the joiners, one at a time, then nothing else ─────────────
# join_ready <index>: poll the joiner's /readyz ON ITS HOST every 0.2 s for up to
# 180 s and print the host's unix ms when it first answers 200 (one ssh session, so
# the time is the host's, not this machine's); with one restart if it never does,
# as follower_ready (issue #426).
join_ready() {
	local i="$1" ip id t
	ip=$(broker_pub_ip "$i")
	id=$(broker_node_id "$i")
	local poll='for k in $(seq 1 900); do curl -sf -m 2 http://localhost:8080/readyz >/dev/null && { date +%s%3N; exit 0; }; sleep 0.2; done; exit 1'
	t=$(rssh "$ip" "$poll") && { echo "$t"; return 0; }
	mkdir -p "$RUN/formation"
	rssh "$ip" "curl -s 'http://localhost:8080/readyz?verbose=1'; echo; curl -s http://localhost:8080/statusz; echo; journalctl -u mqttd --no-pager | tail -60" \
		>"$RUN/formation/$id-join-not-ready.txt" 2>&1 || true
	warn "joiner $id not ready after 180s — evidence kept in $RUN/formation/$id-join-not-ready.txt; restarting it once"
	rssh "$ip" "systemctl restart mqttd"
	t=$(rssh "$ip" "$poll") || die "joiner $id still not ready 180s after a restart — see $RUN/formation/$id-join-not-ready.txt"
	echo "$t"
}
# formed <n>: brokers 0..n-1 are one fully formed cluster: every one's view passes
# cluster-formed.py (asked of all of them at once). The last pass that was not formed
# is kept in <run-dir>/formation/not-formed-<n>.txt.
VOTER_CAP=$(printf '%s\n' "${EXTRA_BROKER_ENV:-}" | sed -n 's/^MQTTD_LEASE_VOTERS=\([0-9][0-9]*\).*/\1/p' | tail -n 1)
VOTER_CAP="${VOTER_CAP:-5}"
formed() {
	local n="$1" i dir rc=0 why
	dir=$(mktemp -d)
	for ((i = 0; i < n; i++)); do
		rssh "$(broker_pub_ip "$i")" "curl -s -m 3 'http://localhost:8080/readyz?verbose=1'; echo; curl -sf -m 3 http://localhost:8080/metrics | grep -E '^mqttd_(members|peer_links|voters)[ {]'" \
			>"$dir/$i" 2>/dev/null &
	done
	wait
	: >"$dir/why"
	for ((i = 0; i < n; i++)); do
		why=$(python3 "$SCALE_DIR/cluster-formed.py" "$n" "$VOTER_CAP" <"$dir/$i") || { rc=1; echo "$(broker_node_id "$i"): $why" >>"$dir/why"; }
	done
	if [ "$rc" != 0 ]; then
		mkdir -p "$RUN/formation"
		cp "$dir/why" "$RUN/formation/not-formed-$n.txt"
	fi
	rm -rf "$dir"
	return "$rc"
}
if [ -n "$JOIN_FROM" ]; then
	MAJORITY=$(((N / 2) + 1))
	for ((i = JOIN_FROM; i < N; i++)); do
		# Two running members as seeds (seeds_for names the first two other
		# indices, which for a joiner are brokers 0 and 1).
		push_node "$i" "$MAJORITY" "$(seeds_for "$i")"
		started=$(rssh "$(broker_pub_ip "$i")" "date +%s%3N; systemctl restart mqttd") ||
			die "could not start joiner $(broker_node_id "$i")"
		ready=$(join_ready "$i")
		say "joiner $(broker_node_id "$i") READY $((ready - started)) ms after its start (floor $MAJORITY of $N); waiting for the cluster to form"
		# A subshell: wait_for dies on its budget, and the reasons are named after it.
		( wait_for "a fully formed cluster of $((i + 1))" 300 formed "$((i + 1))" ) ||
			die "the cluster did not form at $((i + 1)) nodes in 300 s: $(tr '\n' ' ' <"$RUN/formation/not-formed-$((i + 1)).txt" 2>/dev/null)"
		formed_at=$(python3 -c 'import time; print(int(time.time() * 1000))')
		printf '%s\t%s\t%s\t%s\n' "$i" "$started" "$ready" "$formed_at" >>"$JOINS_TSV"
		say "the cluster is fully formed at $((i + 1)) nodes, $((formed_at - started)) ms after $(broker_node_id "$i") started"
	done
	say "cluster grown to $N nodes ($MODE mode)"
	exit 0
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
	# /readyz is majority-only, so a public peer bus with a pair the cloud
	# firewall still blocks can come up READY. Name that here, not as a
	# forwarding-control failure later.
	if [ "$PEER_NET" = public ]; then
		mkdir -p "$RUN/formation"
		await_full_mesh 180 3 "$RUN/formation/peer-mesh-public.txt" ||
			die "PEER_NET=public: not every broker has its $((N - 1)) peer links within 180s — is :7001 open between the public addresses? Evidence: $RUN/formation/peer-mesh-public.txt"
		say "public peer bus: full mesh on every broker"
	fi
fi

say "cluster is up ($MODE mode)"
