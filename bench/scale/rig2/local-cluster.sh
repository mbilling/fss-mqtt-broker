#!/usr/bin/env bash
# local-cluster.sh start|stop <state-dir> [nodes]
#
# The local tier's cluster (ADR 0090 D7, bench/scale/rig2): N mqttd processes on this
# machine, durable (each with its own data dir), plaintext MQTT on 127.0.0.1, peer bus
# and SWIM on loopback, founder first. The same documented MQTTD_* surface the
# out-of-process tests use (crates/mqttd/tests/proc_common): node 0 has no seeds and
# bootstraps the lease group, the others seed off every other node.
#
#   MQTTD_BIN        the broker binary (default target/release/mqttd)
#   LOCAL_PORT_BASE  node i listens on BASE+10i+1 (MQTT), +2 (health), +3 (peer),
#                    +4 (SWIM, UDP); default 27000
#
# `start` writes <state-dir>/brokers.tsv (index, mqtt, health) and waits until every
# node's /readyz is ready with all N members. `stop` sends SIGTERM to every node and
# waits for them to exit.

set -euo pipefail

CMD="${1:?usage: local-cluster.sh start|stop <state-dir> [nodes]}"
STATE="${2:?state dir}"
N="${3:-3}"
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
BIN="${MQTTD_BIN:-$ROOT/target/release/mqttd}"
BASE="${LOCAL_PORT_BASE:-27000}"
# A fixed test-only gossip key, as in proc_common: the mesh runs authenticated.
SWIM_KEY=5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a

port() { echo $((BASE + 10 * $1 + $2)); }

ready_all() {
	local i body
	for ((i = 0; i < N; i++)); do
		body=$(curl -s -m 2 "http://127.0.0.1:$(port "$i" 2)/readyz?verbose=1") || return 1
		python3 -c 'import json,sys
d=json.loads(sys.argv[1]); n=int(sys.argv[2])
sys.exit(0 if d.get("ready") is True and int(d.get("members", d.get("cluster", {}).get("members", 0)) or 0) >= n else 1)' "$body" "$N" || return 1
	done
}

case "$CMD" in
start)
	[ -x "$BIN" ] || { echo "no mqttd binary at $BIN (cargo build --release -p mqttd, or set MQTTD_BIN)" >&2; exit 1; }
	mkdir -p "$STATE"
	: >"$STATE/brokers.tsv"
	for ((i = 0; i < N; i++)); do
		seeds=""
		if [ "$i" -gt 0 ]; then
			for ((j = 0; j < N; j++)); do
				[ "$j" = "$i" ] || seeds="${seeds:+$seeds,}127.0.0.1:$(port "$j" 4)"
			done
		fi
		rm -rf "$STATE/node$i"
		mkdir -p "$STATE/node$i/data"
		env MQTTD_NODE_ID="local-$i" \
			MQTTD_PLAINTEXT_BIND="127.0.0.1:$(port "$i" 1)" \
			MQTTD_ALLOW_ANONYMOUS=1 \
			MQTTD_HEALTH_BIND="127.0.0.1:$(port "$i" 2)" \
			MQTTD_PEER_BIND="127.0.0.1:$(port "$i" 3)" \
			MQTTD_PEER_ADVERTISE="127.0.0.1:$(port "$i" 3)" \
			MQTTD_SWIM_BIND="127.0.0.1:$(port "$i" 4)" \
			MQTTD_SWIM_SEEDS="$seeds" \
			MQTTD_SWIM_KEY="$SWIM_KEY" \
			MQTTD_DATA_DIR="$STATE/node$i/data" \
			MQTTD_SHUTDOWN_GRACE=2 \
			RUST_LOG=warn \
			${LOCAL_BROKER_ENV:-} \
			nohup "$BIN" >"$STATE/node$i/mqttd.log" 2>&1 </dev/null &
		echo $! >"$STATE/node$i/pid"
		printf '%s\t127.0.0.1:%s\t127.0.0.1:%s\n' "$i" "$(port "$i" 1)" "$(port "$i" 2)" >>"$STATE/brokers.tsv"
		if [ "$i" = 0 ]; then
			# Founder first: it must have bootstrapped the lease group before anyone joins.
			for ((t = 0; t < 60; t++)); do
				curl -sf -m 2 "http://127.0.0.1:$(port 0 2)/readyz" >/dev/null && break
				kill -0 "$(cat "$STATE/node0/pid")" 2>/dev/null || { tail -20 "$STATE/node0/mqttd.log" >&2; exit 1; }
				sleep 1
			done
		fi
	done
	for ((t = 0; t < 120; t++)); do
		if ready_all; then
			echo "local cluster: $N nodes ready" >&2
			exit 0
		fi
		for ((i = 0; i < N; i++)); do
			kill -0 "$(cat "$STATE/node$i/pid")" 2>/dev/null || { echo "node $i exited:" >&2; tail -20 "$STATE/node$i/mqttd.log" >&2; exit 1; }
		done
		sleep 1
	done
	echo "local cluster: not every node reached ready with $N members in 120 s" >&2
	exit 1
	;;
stop)
	for pidf in "$STATE"/node*/pid; do
		[ -f "$pidf" ] || continue
		kill -TERM "$(cat "$pidf")" 2>/dev/null || true
	done
	for pidf in "$STATE"/node*/pid; do
		[ -f "$pidf" ] || continue
		for ((t = 0; t < 30; t++)); do
			kill -0 "$(cat "$pidf")" 2>/dev/null || break
			sleep 1
		done
		kill -KILL "$(cat "$pidf")" 2>/dev/null || true
		rm -f "$pidf"
	done
	;;
*)
	echo "usage: local-cluster.sh start|stop <state-dir> [nodes]" >&2
	exit 2
	;;
esac
