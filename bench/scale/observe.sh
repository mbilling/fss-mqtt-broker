#!/usr/bin/env bash
# observe.sh attach <run-dir> <inventory.json> | detach | stop
#
# Live Grafana for a bench/scale run (opt-in; run.sh calls this with OBSERVE=1):
#
#   laptop: docker compose (Grafana :3000 + Prometheus :9090, remote-write in)
#      ▲ reverse SSH tunnel  (driver-1's localhost:9094 → laptop's :9090)
#   driver-1: Alloy container scraping every broker's /metrics over the
#             PRIVATE network at 2s — zero agents, zero load on broker hosts.
#
#   attach  start local stack (once), push+start Alloy on driver-1, open tunnel
#   detach  stop Alloy + tunnel for the current size (servers die anyway)
#   stop    detach + stop the local docker compose stack
#
# Watch at http://localhost:3000 (anonymous admin, demo mqttd dashboards).

set -euo pipefail
. "$(dirname "$0")/lib.sh"

OBS_DIR="$SCALE_DIR/observe"
TUNNEL_PID_FILE="${TMPDIR:-/tmp}/bench-scale-observe-tunnel.pid"

cmd="${1:?usage: observe.sh attach <run-dir> <inventory.json> | detach | stop}"

stop_tunnel() {
	if [ -f "$TUNNEL_PID_FILE" ]; then
		kill "$(cat "$TUNNEL_PID_FILE")" 2>/dev/null || true
		rm -f "$TUNNEL_PID_FILE"
	fi
}

case "$cmd" in
attach)
	RUN="${2:?run-dir}"
	INVENTORY="${3:?inventory.json}"
	command -v docker >/dev/null || die "docker is not running locally (needed for the Grafana/Prometheus stack)"

	say "observe: starting local Grafana(:3000) + Prometheus(:9090)"
	(cd "$OBS_DIR" && docker compose up -d --quiet-pull 2>/dev/null || docker compose up -d)

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

	# Alloy scrapes the bridge's files (one path per broker, instance label kept).
	ALLOY_CFG="$RUN/alloy-config.alloy"
	{
		i=0
		while IFS=$'\t' read -r node_id; do
			echo "prometheus.scrape \"broker_$i\" {"
			echo "  targets         = [{ __address__ = \"localhost:8096\", instance = \"$node_id\" }]"
			echo "  metrics_path    = \"/$node_id.prom\""
			echo '  scrape_interval = "2s"'
			echo '  scrape_timeout  = "2s"'
			echo '  forward_to      = [prometheus.remote_write.laptop.receiver]'
			echo '}'
			i=$((i + 1))
		done < <(jq -r '.brokers[].node_id' "$INVENTORY")
		echo 'prometheus.remote_write "laptop" {'
		echo '  endpoint { url = "http://localhost:9094/api/v1/write" }'
		echo '}'
	} >"$ALLOY_CFG"
	say "observe: starting Alloy on driver-1 ($DRIVER_IP)"
	rscp "$ALLOY_CFG" "root@$DRIVER_IP:/opt/alloy-config.alloy"
	rssh "$DRIVER_IP" "docker rm -f bench-alloy >/dev/null 2>&1 || true; \
		docker run -d --name bench-alloy --network host \
		-v /opt/alloy-config.alloy:/etc/alloy/config.alloy:ro \
		grafana/alloy:latest run /etc/alloy/config.alloy" >/dev/null

	say "observe: opening reverse tunnel (driver-1:9094 -> laptop:9090)"
	stop_tunnel
	ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" \
		-N -R 9094:localhost:9090 "root@$DRIVER_IP" &
	echo $! >"$TUNNEL_PID_FILE"
	say "observe: live at http://localhost:3000 (dashboards: mqttd)"
	;;
detach)
	stop_tunnel
	say "observe: tunnel closed (Alloy dies with its server)"
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
