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

# Transient SSH failures must not erase the only failure evidence. Bound each
# attempt and run hosts in parallel so unreachable hosts cannot delay teardown.
collect_one() {
    local ip="$1" dest="$2" command="$3" attempt
    for attempt in 1 2 3; do
        if timeout 20 ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" \
            "root@$ip" "$command" >"$dest.attempt$attempt" 2>&1; then
            cp "$dest.attempt$attempt" "$dest"
            printf 'success attempt=%s\n' "$attempt" >"$dest.status"
            return 0
        fi
        [ "$attempt" = 3 ] || sleep 2
    done
    printf 'unavailable after 3 bounded attempts\n' >"$dest.status"
    cp "$dest.attempt3" "$dest"
}
driver_command=$(cat <<'COMMAND'
date -u
uptime
chronyc -n tracking
chronyc -n sources -v
journalctl -u chrony --since '-30 minutes' --no-pager
free -h
df -h
journalctl -k --since '-30 minutes' --no-pager
journalctl -u ssh -u docker --since '-30 minutes' --no-pager
docker ps -a
for c in $(docker ps -aq); do
    docker inspect --format '{{.Name}} {{json .State}} {{.Image}}' "$c"
    # BOTH ends, not just the tail. emqtt-bench prints the reason a client could
    # not connect ("client(N): connect error - ...") during the connect ramp, in
    # the first seconds; at ~2 progress lines a second a 50-line tail covers only
    # the last 25 of a six-minute rung, so the one line that explains a failure
    # scrolls off. Measured 2026-09-22: one container reported connect_fail=27 of
    # 750, no broker logged a refusal, and the reason was unrecoverable because
    # the capture had already discarded it.
    echo "--- head of $c"
    docker logs "$c" 2>&1 | head -80
    echo "--- tail of $c"
    docker logs --tail 50 "$c" 2>&1
done
cat /var/log/cloud-init-output.log
test ! -f /var/log/bench-build.log || cat /var/log/bench-build.log
COMMAND
)
pids=()
for ((i = 0; i < N; i++)); do
    (
        ip=$(broker_pub_ip "$i")
        collect_one "$ip" "$OUT/broker$i-journal.log" "journalctl -u mqttd --no-pager"
        collect_one "$ip" "$OUT/broker$i-final-metrics.prom" "curl -fsS -m 10 http://localhost:8080/metrics"
        collect_one "$ip" "$OUT/broker$i-cloud-init.log" "cat /var/log/cloud-init-output.log"
    ) & pids+=($!)
done
for ((i = 0; i < D; i++)); do
    (
        ip=$(driver_pub_ip "$i")
        collect_one "$ip" "$OUT/driver$i-logs.log" "$driver_command"
    ) & pids+=($!)
done
for pid in "${pids[@]}"; do wait "$pid" || true; done
say "host evidence capture completed (see *.status for unavailable hosts) -> $OUT"
