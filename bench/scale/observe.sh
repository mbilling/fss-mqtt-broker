#!/usr/bin/env bash
# observe.sh attach <run-dir> <inventory.json> | attach-local <run-dir> <brokers.tsv> [gen-metrics-addr]
#            | detach | stop | phase <name> [detail]
#
# Live Grafana for a bench/scale run (on by default; run.sh and rig2/run.sh call this
# unless OBSERVE=0):
#
#   laptop: docker compose (Grafana :3000 + Prometheus :9090 + Alloy)
#      │ forward SSH tunnel  (laptop's :8096 → driver-1's :8096)
#      ▼
#   driver-1: curl-bridge writing every broker's /metrics to files it serves
#             over the PRIVATE network at 2s — zero load on broker hosts.
#
#   ALLOY IS LOCAL, not on driver-1: it outlives the cluster, so a teardown no
#   longer destroys the agent and its write-ahead log, and the only thing that
#   stops between runs is the tunnel. The bridge cannot move with it — the
#   released broker's health server RSTs real Prometheus clients (#364), so it
#   has to be a curl loop inside the private network.
#
#   attach  start local stack (once), start the bridge on driver-1, rewrite
#           Alloy's targets and restart it, open the tunnel
#   attach-local  the same stack for rig2's TIER=local: no bridge and no tunnel, Alloy
#           scrapes the local brokers (and mqtt-load) on the operator's loopback
#   detach  close the tunnel (the bridge dies with its server; Alloy idles on)
#   stop    detach + stop the local docker compose stack
#
# OBSERVE_GEN_METRICS_PORT=<port> (rig2): the bridge also curls every generator host's
# mqtt-load --metrics endpoint at <private_ip>:<port> into gen-<g>.prom, deleting the
# file when no mqtt-load is running (between steps), so a stale exposition never
# looks live; Alloy scrapes them as job "mqtt-load", instance gen-<g>.
#
# Watch at http://localhost:3000 (anonymous admin, demo mqttd dashboards; rig2 adds
# "mqtt-load (bench)").

set -euo pipefail
. "$(dirname "$0")/lib.sh"

OBS_DIR="$SCALE_DIR/observe"
TUNNEL_PID_FILE="${TMPDIR:-/tmp}/bench-scale-observe-tunnel.pid"

cmd="${1:?usage: observe.sh attach <run-dir> <inventory.json> | detach | stop | phase <name> [detail]}"

stop_tunnel() {
	if [ -f "$TUNNEL_PID_FILE" ]; then
		local pid
		pid=$(cat "$TUNNEL_PID_FILE")
		# The supervisor and the ssh it runs.
		pkill -P "$pid" 2>/dev/null || true
		kill "$pid" 2>/dev/null || true
		rm -f "$TUNNEL_PID_FILE"
	fi
}

# tunnel_supervisor <driver-ip> <known_hosts>: keep laptop:8096 -> driver:8096 open.
# Twice on 2026-10-27 the tunnel stopped carrying data mid-run, once with its ssh gone
# and once with it alive and the forward hung, and the dashboards went flat while the
# bridge on the driver was fine. Keep-alives end a dead connection; the probe catches a
# hung forward. Either way the ssh is restarted.
tunnel_supervisor() {
	local ip="$1" kh="$2" ssh_pid
	trap 'kill "$ssh_pid" 2>/dev/null; exit 0' TERM INT
	while :; do
		ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile="$kh" -o ServerAliveInterval=10 -o ServerAliveCountMax=3 \
			-o ExitOnForwardFailure=yes -N -L 8096:localhost:8096 "root@$ip" &
		ssh_pid=$!
		sleep 5
		while kill -0 "$ssh_pid" 2>/dev/null; do
			curl -s -m 5 -o /dev/null http://localhost:8096/ || break
			sleep 20
		done
		kill "$ssh_pid" 2>/dev/null || true
		wait "$ssh_pid" 2>/dev/null || true
		echo "observe: tunnel to $ip stopped answering at $(date -u +%TZ); reopening" >&2
		sleep 2
	done
}

start_stack() {
	command -v docker >/dev/null || die "docker is not running locally (needed for the Grafana/Prometheus stack)"

	# The per-run Alloy config is GENERATED into the compose mount below, so it is
	# untracked; recreate the idle placeholder if this is a fresh checkout, or the
	# stack would start with no config file at all.
	if [ ! -f "$OBS_DIR/alloy/config.alloy" ]; then
		mkdir -p "$OBS_DIR/alloy"
		cp "$OBS_DIR/alloy/config.alloy.idle" "$OBS_DIR/alloy/config.alloy"
	fi
	# The TSDB volume is `external` in the compose file so no `down -v` of this
	# stack can take the history with it, which also means compose will not create
	# it: a fresh operator machine failed attach on exactly that (2026-09-15, #482).
	docker volume inspect observe_prom-data >/dev/null 2>&1 ||
		docker volume create observe_prom-data >/dev/null ||
		die "could not create the observe_prom-data docker volume"
	say "observe: starting local Grafana(:3000) + Prometheus(:9090)"
	(cd "$OBS_DIR" && docker compose up -d --quiet-pull 2>/dev/null || docker compose up -d)
}

# Where Alloy finds the operator's loopback. Alloy runs with host networking (see the
# compose file), which on Linux IS the operator's network, so 127.0.0.1 reaches the ssh
# tunnel. On Docker Desktop (macOS) "host" is the Docker VM's network: ports Docker
# publishes (Prometheus :9090, the phase gateway :9091) are there, but the tunnel is an
# ssh process on the Mac and is not — every broker scrape failed (up=0 for a whole run
# on 2026-10-03; the dashboards stayed empty while the phase marker, a Docker port,
# worked). host.docker.internal reaches the Mac from that network (measured: 200 vs no
# answer).
loopback_host() {
	case "$(uname -s)" in
	Darwin) echo host.docker.internal ;;
	*) echo 127.0.0.1 ;;
	esac
}

# scrape_block <component> <address> <path> <instance> [job]
scrape_block() {
	echo "prometheus.scrape \"$1\" {"
	if [ -n "${5:-}" ]; then
		echo "  targets         = [{ __address__ = \"$2\", instance = \"$4\", job = \"$5\" }]"
	else
		echo "  targets         = [{ __address__ = \"$2\", instance = \"$4\" }]"
	fi
	echo "  metrics_path    = \"$3\""
	echo '  scrape_interval = "2s"'
	echo '  scrape_timeout  = "2s"'
	echo '  forward_to      = [prometheus.remote_write.local.receiver]'
	echo '}'
}

# The phase heartbeat and the remote write: the tail of every generated config.
alloy_tail() {
	echo 'prometheus.scrape "bench_phase" {'
	echo '  targets         = [{ __address__ = "127.0.0.1:9091" }]'
	echo '  scrape_interval = "5s"'
	# Alloy's default scrape_timeout (10s) is refused against a 5s interval.
	echo '  scrape_timeout  = "3s"'
	echo '  honor_labels    = true'
	echo '  forward_to      = [prometheus.remote_write.local.receiver]'
	echo '}'
	echo 'prometheus.remote_write "local" {'
	echo '  endpoint { url = "http://127.0.0.1:9090/api/v1/write" }'
	echo '}'
}

restart_alloy() {
	(cd "$OBS_DIR" && docker compose restart alloy >/dev/null 2>&1) ||
		warn "observe: could not restart the local Alloy container"
}

live_banner() {
	say "observe: live at http://localhost:3000 — dashboards: mqttd (bench) http://localhost:3000/d/mqttd-overview-bench${1:+, mqtt-load (bench) http://localhost:3000/d/mqtt-load-bench}"
}

case "$cmd" in
attach)
	RUN="${2:?run-dir}"
	INVENTORY="${3:?inventory.json}"
	GEN_PORT="${OBSERVE_GEN_METRICS_PORT:-}"
	start_stack

	DRIVER_IP=$(driver_pub_ip 0)

	# CURL-BRIDGE (issue #364): the released broker's health server RSTs any
	# client that sends normal request headers — real Prometheus clients cannot
	# scrape it over a network, while curl wins the race. Until the fix ships,
	# a loop on driver-1 curls every broker's /metrics into files that a
	# well-behaved python http.server serves to Alloy. Remove with #364.
	BRIDGE="$RUN/prom-bridge.sh"
	{
		echo '#!/bin/sh'
		echo 'mkdir -p /var/lib/bench-prom'
		echo 'while true; do'
		jq -r '.brokers[] | "  curl -sf --max-time 2 http://\(.health)/metrics -o /var/lib/bench-prom/\(.node_id).tmp && mv /var/lib/bench-prom/\(.node_id).tmp /var/lib/bench-prom/\(.node_id).prom"' "$INVENTORY"
		# A generator's endpoint exists only while its mqtt-load runs: no file then.
		if [ -n "$GEN_PORT" ]; then
			jq -r --arg port "$GEN_PORT" '.drivers | to_entries[] | "  curl -sf --max-time 2 http://\(.value.private_ip):\($port)/metrics -o /var/lib/bench-prom/gen-\(.key).tmp && mv /var/lib/bench-prom/gen-\(.key).tmp /var/lib/bench-prom/gen-\(.key).prom || rm -f /var/lib/bench-prom/gen-\(.key).prom"' "$INVENTORY"
		fi
		echo '  sleep 2'
		echo 'done'
	} >"$BRIDGE"
	rscp "$BRIDGE" "root@$DRIVER_IP:/opt/prom-bridge.sh"
	rssh "$DRIVER_IP" "chmod +x /opt/prom-bridge.sh; \
		systemctl reset-failed bench-prom-bridge bench-prom-files 2>/dev/null; \
		systemctl stop bench-prom-bridge bench-prom-files 2>/dev/null; \
		systemd-run --unit bench-prom-bridge /opt/prom-bridge.sh; \
		systemd-run --unit bench-prom-files python3 -m http.server 8096 --directory /var/lib/bench-prom" \
		>/dev/null 2>&1 || true

	# Alloy's targets are this run's brokers, reached through the forward tunnel.
	# host.docker.internal is how the container gets at the tunnel on the
	# operator's loopback. Written into the compose mount (a copy is kept in the
	# run dir for the archive) and picked up by a restart.
	ALLOY_CFG="$OBS_DIR/alloy/config.alloy"
	TUNNEL_HOST=$(loopback_host)
	{
		echo "// Generated by observe.sh for run $(basename "$RUN") — do not edit by hand."
		i=0
		while IFS=$'\t' read -r node_id; do
			scrape_block "broker_$i" "$TUNNEL_HOST:8096" "/$node_id.prom" "$node_id"
			i=$((i + 1))
		done < <(jq -r '.brokers[].node_id' "$INVENTORY")
		if [ -n "$GEN_PORT" ]; then
			for ((g = 0; g < $(jq -r '.drivers | length' "$INVENTORY"); g++)); do
				scrape_block "mqtt_load_$g" "$TUNNEL_HOST:8096" "/gen-$g.prom" "gen-$g" mqtt-load
			done
		fi
		alloy_tail
	} >"$ALLOY_CFG"
	cp "$ALLOY_CFG" "$RUN/alloy-config.alloy"

	# The tunnel must exist BEFORE Alloy restarts, or its first scrapes fail and
	# it backs off while the first rung is already running.
	say "observe: opening forward tunnel (laptop:8096 -> driver-1:8096)"
	stop_tunnel
	tunnel_supervisor "$DRIVER_IP" "$RUN/known_hosts" </dev/null >>"$RUN/observe-tunnel.log" 2>&1 &
	echo $! >"$TUNNEL_PID_FILE"

	say "observe: pointing local Alloy at $(jq -r '.brokers | length' "$INVENTORY") broker(s)${GEN_PORT:+ and $(jq -r '.drivers | length' "$INVENTORY") generator(s)}"
	restart_alloy
	live_banner "$GEN_PORT"
	say "observe: the tunnel is this run's ssh -N -L 8096:localhost:8096 root@$DRIVER_IP, kept open by a supervisor (pid $(cat "$TUNNEL_PID_FILE"); its log is $RUN/observe-tunnel.log); Grafana listens on every interface of this machine (:3000)"
	;;
attach-local)
	# rig2 TIER=local: the brokers and mqtt-load run on this machine, so Alloy scrapes
	# them directly (no bridge, no tunnel). <brokers.tsv> is local-cluster.sh's: index,
	# mqtt address, health address.
	RUN="${2:?run-dir}"
	TSV="${3:?brokers.tsv}"
	GEN_ADDR="${4:-}"
	start_stack
	ALLOY_CFG="$OBS_DIR/alloy/config.alloy"
	HOST=$(loopback_host)
	{
		echo "// Generated by observe.sh attach-local for run $(basename "$RUN") — do not edit by hand."
		while IFS=$'\t' read -r idx _ health; do
			scrape_block "broker_$idx" "$HOST:${health##*:}" "/metrics" "local-$idx"
		done <"$TSV"
		[ -z "$GEN_ADDR" ] || scrape_block "mqtt_load_0" "$HOST:${GEN_ADDR##*:}" "/metrics" "gen-0" mqtt-load
		alloy_tail
	} >"$ALLOY_CFG"
	cp "$ALLOY_CFG" "$RUN/alloy-config.alloy"
	say "observe: pointing local Alloy at $(wc -l <"$TSV" | tr -d ' ') local broker(s)${GEN_ADDR:+ and mqtt-load at $GEN_ADDR}"
	restart_alloy
	live_banner "$GEN_ADDR"
	;;
phase)
	# Push the run's current phase so a blank dashboard is never ambiguous.
	# Pushgateway replaces the whole group per push, so the previous phase clears
	# itself and exactly one is ever set. Best-effort by design: a rig run must
	# never fail because the operator's Grafana stack is not up.
	NAME="${2:?usage: observe.sh phase <name> [detail]}"
	DETAIL="${3:-}"
	printf '%s\n' \
		'# TYPE bench_run_phase gauge' \
		"bench_run_phase{phase=\"$NAME\",detail=\"$DETAIL\"} 1" |
		curl -sf --max-time 3 --data-binary @- \
			http://localhost:9091/metrics/job/bench_run >/dev/null 2>&1 || true
	;;
detach)
	stop_tunnel
	say "observe: tunnel closed (the bridge dies with its server; local Alloy idles on)"
	;;
stop)
	stop_tunnel
	(cd "$OBS_DIR" && docker compose down 2>/dev/null) || true
	say "observe: local stack stopped"
	;;
*)
	die "unknown observe command: $cmd"
	;;
esac
