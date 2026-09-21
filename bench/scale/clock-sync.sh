#!/usr/bin/env bash
# Sourced by run-curve.sh. Only operates on the disposable benchmark fleet.
QOS1_CLOCK_MAX_ERROR_MS=${QOS1_CLOCK_MAX_ERROR_MS:-5}

qos1_clock_hosts() {
    local i
    for ((i=0; i<N; i++)); do echo "broker$i"; done
    for ((i=0; i<D; i++)); do echo "driver$i"; done
}

qos1_clock_capture() { # destination directory; retain even failed reports
    local dest=$1 i pid
    local -a pids=() hosts=()
    mkdir -p "$dest"
    local command='set -e; LC_ALL=C chronyc -n tracking; printf "Captured epoch : %s\n" "$(date +%s.%N)"'
    for ((i=0; i<N; i++)); do
        rssh "$(broker_pub_ip "$i")" "$command" >"$dest/broker$i.txt" 2>"$dest/broker$i.err" & pids+=($!)
    done
    for ((i=0; i<D; i++)); do
        rssh "$(driver_pub_ip "$i")" "$command" >"$dest/driver$i.txt" 2>"$dest/driver$i.err" & pids+=($!)
    done
    local failed=0
    for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
    mapfile -t hosts < <(qos1_clock_hosts)
    [ "$failed" = 0 ] || return 1
    python3 "$SCALE_DIR/clock-check.py" "$dest" "$QOS1_CLOCK_MAX_ERROR_MS" "${hosts[@]}" >"$dest/validated.json"
}

qos1_clock_setup() {
    local reference ip i pid
    local -a pids=()
    reference=$(inv '.brokers[0].private_ip')
    # Use the first broker's existing external NTP sources. Do not invent local
    # stratum or serve an unsynchronized clock. Allow only fleet private IPs.
    local allow='set -e; systemctl enable --now chrony; '
    while read -r ip; do allow+="chronyc allow $ip; "; done < <(inv '(.brokers[], .drivers[]) | .private_ip')
    allow+='chronyc waitsync 30 0.001 0 1; chronyc makestep; chronyc makestep 0.001 0; chronyc waitsync 30 0.001 0 1'
    rssh "$(broker_pub_ip 0)" "$allow" >"$OUT/clock-reference-setup.log" 2>&1 || die "NTP reference failed to synchronize"
    # No makestep directive: one explicit correction BEFORE clients start,
    # followed by continuous slewing. A restart cannot enable new clock steps.
    local config="server $reference iburst minpoll 0 maxpoll 4
driftfile /var/lib/chrony/chrony.drift
rtcsync
logdir /var/log/chrony
log tracking measurements statistics
"
    local command="set -e; printf '%s' '$config' > /etc/chrony/chrony.conf; systemctl restart chrony; chronyc waitsync 60 0 0 1; chronyc makestep; chronyc waitsync 60 0.001 0 1"
    for ((i=1; i<N; i++)); do
        rssh "$(broker_pub_ip "$i")" "$command" >"$OUT/clock-broker$i-setup.log" 2>&1 & pids+=($!)
    done
    for ((i=0; i<D; i++)); do
        rssh "$(driver_pub_ip "$i")" "$command" >"$OUT/clock-driver$i-setup.log" 2>&1 & pids+=($!)
    done
    local failed=0
    for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
    [ "$failed" = 0 ] || die "benchmark NTP synchronization failed; see clock setup logs"
    qos1_clock_capture "$OUT/clock-preflight" || die "clock accuracy outside declared budget"
}
