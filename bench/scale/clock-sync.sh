#!/usr/bin/env bash
# Sourced by run-curve.sh. Only operates on the disposable benchmark fleet.
QOS1_CLOCK_MAX_ERROR_MS=${QOS1_CLOCK_MAX_ERROR_MS:-5}
# How long the fleet may take to bring its error bound inside the budget, and how
# often to re-check. Root dispersion falls over the first few poll intervals.
QOS1_CLOCK_CONVERGE_BUDGET=${QOS1_CLOCK_CONVERGE_BUDGET:-300}
QOS1_CLOCK_CONVERGE_POLL=${QOS1_CLOCK_CONVERGE_POLL:-15}
# The reference broker's upstream NTP servers; empty (the default) keeps the image's
# pool. Every host's bound inherits the reference's root dispersion, so the upstream
# must be a LOW-dispersion source. Hetzner's own servers are not: on 2026-10-05 they
# reported ~9-10 ms root dispersion, failing the gate at the first rung with no load,
# against ~1 ms from the image's pool in every earlier run.
QOS1_CLOCK_UPSTREAM=${QOS1_CLOCK_UPSTREAM-}

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
    if [ -n "$QOS1_CLOCK_UPSTREAM" ]; then
        # Replace the reference's sources with the declared upstream, serving the
        # fleet's private IPs only (the same allow list as below, as config lines).
        local refconf="" s
        # Poll the upstream every 16-64 s, not chrony's default up to ~17 min:
        # the gate keeps root dispersion WHOLE, and the reference's dispersion,
        # which every host inherits, grows between its polls. On 2026-10-05 it was
        # 2.85 ms of a driver's 6.8 ms bound at an 18-site rung, every offset
        # under 0.32 ms.
        for s in $QOS1_CLOCK_UPSTREAM; do refconf+="server $s iburst minpoll 4 maxpoll 6"$'\n'; done
        refconf+=$'driftfile /var/lib/chrony/chrony.drift\nrtcsync\nlogdir /var/log/chrony\nlog tracking measurements statistics\n'
        while read -r ip; do refconf+="allow $ip"$'\n'; done < <(inv '(.brokers[], .drivers[]) | .private_ip')
        allow+="printf '%s' '$refconf' > /etc/chrony/chrony.conf; systemctl restart chrony; chronyc waitsync 60 0 0 1; "
    else
        while read -r ip; do allow+="chronyc allow $ip; "; done < <(inv '(.brokers[], .drivers[]) | .private_ip')
        # Keep the image's sources, but poll them every 16-64 s instead of up to
        # ~17 min: the reference's root dispersion, which every host inherits, grows
        # between its upstream polls (2.85 ms of a 6.8 ms bound at an 18-site rung).
        # shellcheck disable=SC2016 # expanded by the REMOTE shell
        allow+='chronyc waitsync 30 0 0 1; for a in $(chronyc -n sources | awk '"'"'/^\^/ {print $2}'"'"'); do chronyc minpoll "$a" 4 >/dev/null || true; chronyc maxpoll "$a" 6 >/dev/null || true; done; '
    fi
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
    # `chronyc waitsync` returns when the OFFSET is small, but the gate judges the
    # error BOUND — offset plus the hop to the reference plus root dispersion —
    # and dispersion starts high, shrinking as chrony accumulates measurements.
    # Measured 2026-09-21: every one of 17 hosts had a sub-microsecond offset and
    # a Normal leap status, and driver4 still bounded at 5.493ms because its
    # dispersion was 4.518ms, ten times a converged fleet's. Nothing was wrong
    # with the clocks; the capture was simply too early. So wait for the bound the
    # gate uses rather than a proxy for it, and say so when it took time.
    local waited=0
    until qos1_clock_capture "$OUT/clock-preflight"; do
        [ "$waited" -lt "$QOS1_CLOCK_CONVERGE_BUDGET" ] ||
            die "clock accuracy outside declared budget after ${waited}s of convergence — see $OUT/clock-preflight"
        sleep "$QOS1_CLOCK_CONVERGE_POLL"
        waited=$((waited + QOS1_CLOCK_CONVERGE_POLL))
    done
    [ "$waited" -eq 0 ] || say "lane E: fleet clocks converged after ${waited}s"
}
