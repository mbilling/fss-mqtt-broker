#!/usr/bin/env bash
# collect.sh <run-dir> <inventory.json> — pull everything off the hosts that the
# lanes did not already write locally: broker journals (the only way to diagnose
# a broker-side stall after the servers are destroyed), cloud-init logs, and a
# final /metrics snapshot per broker. Runs right before the size is destroyed.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

RUN="${1:?usage: collect.sh <run-dir> <inventory.json>}"
# shellcheck disable=SC2034 # consumed by lib.sh's inv() helper
INVENTORY="${2:?inventory.json}"

N=$(broker_count)
D=$(driver_count)
OUT="$RUN/results/nodes=$N/hosts"
mkdir -p "$OUT"

for ((i = 0; i < N; i++)); do
	ip=$(broker_pub_ip "$i")
	rssh "$ip" "journalctl -u mqttd --no-pager" >"$OUT/broker$i-journal.log" 2>&1 || true
	rssh "$ip" "curl -s http://localhost:8080/metrics" >"$OUT/broker$i-final-metrics.prom" 2>&1 || true
	rssh "$ip" "cat /var/log/cloud-init-output.log" >"$OUT/broker$i-cloud-init.log" 2>&1 || true
done
for ((i = 0; i < D; i++)); do
	ip=$(driver_pub_ip "$i")
	rssh "$ip" "cat /var/log/cloud-init-output.log /var/log/bench-build.log 2>/dev/null" \
		>"$OUT/driver$i-logs.log" 2>&1 || true
done

say "host logs collected -> $OUT"
