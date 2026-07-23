#!/bin/sh
# ADR 0048 T1/T2 — comparative benchmark runner. Runs the SAME emqtt-bench scenarios
# against one broker at a time (compose profile), capturing raw output per scenario:
#
#   ./run.sh                 # all brokers (mqttd, mosquitto, emqx), standard durations
#   ./run.sh smoke           # all brokers, seconds-long smoke pass (harness check)
#   ./run.sh smoke mqttd     # one broker
#
# Scenarios (each in both postures where marked):
#   conn            connection-establishment rate + memory per idle connection
#                   (broker RSS snapshotted before/after the ramp)
#   pubsub-qos0/1/2 sustained pub/sub throughput + END-TO-END LATENCY distribution
#                   (emqtt-bench --payload-hdrs ts; the subscriber's Prometheus
#                   e2e_latency histogram is scraped to <scenario>.prom)
#   tls-conn        connection establishment under mTLS (client certs REQUIRED)
#   tls-pubsub-qos1 sustained throughput + latency under mTLS
#
# Results land in results/<UTC-stamp>/<broker>/ with an env.txt recording versions,
# host, and parameters — raw output is the record (ADR 0048 §3). Summarize with
# ./summarize.py results/<stamp>.
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
CERTS="$PWD/tls/certs"

# Scenario parameters (small enough for a laptop driver; a publishable run scales these
# up on real hardware — the point of the harness is method, not laptop numbers).
if [ "$MODE" = "smoke" ]; then
	DURATION=10 CONNS=100 CONN_RATE=100 PUBS=20 SUBS=20 INTERVAL_MS=10 SIZE=256
else
	DURATION=60 CONNS=5000 CONN_RATE=500 PUBS=50 SUBS=50 INTERVAL_MS=5 SIZE=256
fi

# The throwaway PKI for the mTLS posture (client certs required on 8883).
[ -f "$CERTS/ca.pem" ] || ./tls/gen-certs.sh

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
OUT="results/$STAMP"
mkdir -p "$OUT"

TLS_ARGS="-p 8883 -S true --cacertfile /certs/ca.pem --certfile /certs/client.pem --keyfile /certs/client.key"

rss() { # rss <broker> — the broker container's memory usage line
	docker stats --no-stream --format "{{.Name}} {{.MemUsage}}" 2>/dev/null |
		grep "mqttd-bench-$1" || echo "$1 unknown"
}

# conn_scenario <broker> <name> <extra emqtt-bench args...>: emqtt_bench conn HOLDS its
# connections (it never exits), so run it detached for a window sized to the ramp,
# snapshot broker RSS before and after (memory per idle connection), then stop it.
conn_scenario() {
	broker="$1" name="$2"
	shift 2
	echo "  scenario: $name"
	sleep 5 # let the broker's RSS settle before the baseline snapshot
	rss "$broker" >"$OUT/$broker/$name.rss-before"
	# shellcheck disable=SC2086
	docker run -d --network "$NET" -v "$CERTS:/certs:ro" --name "bench-$name-$broker" \
		"$BENCH_IMG" conn -h broker -c "$CONNS" -R "$CONN_RATE" "$@" >/dev/null
	sleep $((CONNS / CONN_RATE + 8))
	rss "$broker" >"$OUT/$broker/$name.rss-after"
	docker logs "bench-$name-$broker" >"$OUT/$broker/$name.log" 2>&1 || true
	docker rm -f "bench-$name-$broker" >/dev/null 2>&1 || true
}

# pubsub_scenario <broker> <name> <qos> <extra args...>: subscribers first (with the
# Prometheus e2e-latency histogram exposed), then publishers; on completion scrape the
# histogram and stop the subscribers.
pubsub_scenario() {
	broker="$1" name="$2" qos="$3"
	shift 3
	echo "  scenario: $name"
	# shellcheck disable=SC2086
	docker run -d --network "$NET" -v "$CERTS:/certs:ro" --name "bench-sub-$broker" \
		"$BENCH_IMG" sub -h broker -c "$SUBS" -t 'bench/%i' -q "$qos" \
		--payload-hdrs ts --prometheus --restapi 9090 "$@" >/dev/null
	sleep 3
	# Publisher runs in a TIMED window (emqtt-bench -L limit semantics are ambiguous —
	# per-client vs total — so wall time bounds the scenario; -L stays as a safety cap).
	# shellcheck disable=SC2086
	docker run -d --network "$NET" -v "$CERTS:/certs:ro" --name "bench-pub-$broker" \
		"$BENCH_IMG" pub -h broker -c "$PUBS" -t 'bench/%i' -q "$qos" -s "$SIZE" \
		-I "$INTERVAL_MS" -L $((PUBS * DURATION * 1000 / INTERVAL_MS)) \
		--payload-hdrs ts "$@" >/dev/null
	sleep "$DURATION"
	docker logs "bench-pub-$broker" >"$OUT/$broker/$name.log" 2>&1 || true
	docker rm -f "bench-pub-$broker" >/dev/null 2>&1 || true
	sleep 2
	docker run --rm --network "$NET" busybox:1.36 \
		wget -qO- "http://bench-sub-$broker:9090/metrics" >"$OUT/$broker/$name.prom" 2>/dev/null || true
	docker logs "bench-sub-$broker" >"$OUT/$broker/$name.sub.log" 2>&1 || true
	docker rm -f "bench-sub-$broker" >/dev/null 2>&1 || true
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

	# Plaintext posture.
	conn_scenario "$broker" conn
	for qos in 0 1 2; do
		pubsub_scenario "$broker" "pubsub-qos$qos" "$qos"
	done

	# mTLS posture (client certificates required — the security cost, shown).
	# shellcheck disable=SC2086
	conn_scenario "$broker" tls-conn $TLS_ARGS
	# shellcheck disable=SC2086
	pubsub_scenario "$broker" tls-pubsub-qos1 1 $TLS_ARGS

	docker compose --profile "$broker" down -v 2>/dev/null
done

{
	echo "date: $STAMP"
	echo "mode: $MODE  duration=${DURATION}s conns=$CONNS conn_rate=$CONN_RATE pubs=$PUBS subs=$SUBS interval_ms=$INTERVAL_MS size=$SIZE"
	echo "bench tool: $BENCH_IMG"
	echo "brokers: mqttd=$(git -C .. rev-parse --short HEAD 2>/dev/null || echo '?') mosquitto=2.0.20 emqx=5.8.6"
	echo "postures: plaintext/anonymous (1883) and TLS+required-client-certs (8883)"
	echo "host: $(uname -sm) (DEV-GRADE unless a dedicated, documented bench host)"
} >"$OUT/env.txt"

echo "results: bench/$OUT"
