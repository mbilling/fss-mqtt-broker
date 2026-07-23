#!/bin/sh
# ADR 0048 T1 — comparative benchmark runner. Runs the SAME emqtt-bench scenarios against
# one broker at a time (compose profile), capturing raw output per broker per scenario:
#
#   ./run.sh                 # all brokers (mqttd, mosquitto, emqx), standard durations
#   ./run.sh smoke           # all brokers, seconds-long smoke pass (harness check)
#   ./run.sh smoke mqttd     # one broker
#
# Results land in results/<UTC-stamp>/<broker>/<scenario>.log with an env.txt recording
# versions, host, and parameters — raw output is kept verbatim (ADR 0048 §3: methodology,
# configs, and raw output are published alongside any summary).
#
# HONESTY NOTE (ADR 0048 §2/§3): numbers from a laptop/dev machine are DEV-GRADE — they
# guide work and prove the harness; they are not publishable. Publishable runs use a
# dedicated host (driver and broker separated) and are labeled with their hardware.
set -eu

cd "$(dirname "$0")"

MODE="${1:-full}"
ONLY="${2:-}"
BENCH_IMG="emqx/emqtt-bench:0.6.3"
NET="mqttd-bench_default"
BROKERS="${ONLY:-mqttd mosquitto emqx}"

# Scenario parameters (small enough for a laptop driver; a publishable run scales these
# up on real hardware — the point of T1 is the harness, not the numbers).
if [ "$MODE" = "smoke" ]; then
	DURATION=10 CONNS=100 CONN_RATE=100 PUBS=20 SUBS=20 INTERVAL_MS=10 SIZE=256
else
	DURATION=60 CONNS=1000 CONN_RATE=200 PUBS=50 SUBS=50 INTERVAL_MS=5 SIZE=256
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="results/$STAMP"
mkdir -p "$OUT"

bench() { # bench <broker> <name> <args...>
	broker="$1" name="$2"
	shift 2
	echo "  scenario: $name"
	# shellcheck disable=SC2086
	docker run --rm --network "$NET" "$BENCH_IMG" "$@" \
		>"$OUT/$broker/$name.log" 2>&1 || true
}

for broker in $BROKERS; do
	echo "=== $broker ==="
	mkdir -p "$OUT/$broker"
	docker compose --profile "$broker" up -d --quiet-pull 2>/dev/null
	# Wait until the broker accepts TCP on 1883. (NOT emqtt_bench conn: conn mode holds
	# its connections open and never exits, so it cannot be a probe.)
	tries=0
	until docker run --rm --network "$NET" busybox:1.36 nc -z broker 1883 >/dev/null 2>&1; do
		tries=$((tries + 1))
		[ "$tries" -gt 60 ] && { echo "$broker never became ready"; exit 1; }
		sleep 2
	done

	# 1. Connection establishment: CONNS connections at CONN_RATE conns/s. emqtt_bench
	# conn HOLDS its connections (it never exits), so run it detached for a fixed window
	# sized to the ramp, then capture its output and stop it.
	echo "  scenario: conn"
	conn_window=$((CONNS / CONN_RATE + 5))
	docker run -d --network "$NET" --name "bench-conn-$broker" "$BENCH_IMG" \
		conn -h broker -c "$CONNS" -R "$CONN_RATE" >/dev/null
	sleep "$conn_window"
	docker logs "bench-conn-$broker" >"$OUT/$broker/conn.log" 2>&1 || true
	docker rm -f "bench-conn-$broker" >/dev/null 2>&1 || true

	# 2..4. Sustained pub/sub throughput at each QoS: SUBS subscribers on the topic set,
	# PUBS publishers at 1000/INTERVAL_MS msg/s each, SIZE-byte payloads, DURATION secs.
	for qos in 0 1 2; do
		docker run -d --rm --network "$NET" --name "bench-sub-$broker-$qos" "$BENCH_IMG" \
			sub -h broker -c "$SUBS" -t 'bench/%i' -q "$qos" >/dev/null
		sleep 3
		bench "$broker" "pubsub-qos$qos" \
			pub -h broker -c "$PUBS" -t 'bench/%i' -q "$qos" -s "$SIZE" \
			-I "$INTERVAL_MS" -L $((PUBS * 1000 / INTERVAL_MS * DURATION / 1000))
		docker rm -f "bench-sub-$broker-$qos" >/dev/null 2>&1 || true
	done

	# Broker memory after load (RSS proxy, dev-grade; T2 formalizes per-connection memory).
	docker stats --no-stream --format "{{.Name}} {{.MemUsage}}" >"$OUT/$broker/mem.log" || true

	docker compose --profile "$broker" down -v 2>/dev/null
done

{
	echo "date: $STAMP"
	echo "mode: $MODE  duration=${DURATION}s conns=$CONNS conn_rate=$CONN_RATE pubs=$PUBS subs=$SUBS interval_ms=$INTERVAL_MS size=$SIZE"
	echo "bench tool: $BENCH_IMG"
	echo "brokers: mqttd=$(git -C .. rev-parse --short HEAD 2>/dev/null || echo '?') mosquitto=2.0.20 emqx=5.8.6"
	echo "host: $(uname -sm) (DEV-GRADE unless a dedicated, documented bench host)"
} >"$OUT/env.txt"

echo "results: bench/$OUT"
