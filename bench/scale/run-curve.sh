#!/usr/bin/env bash
# run-curve.sh <run-dir> <inventory.json>
#
# Runs every measurement lane against ONE already-bootstrapped cluster size and
# collects raw results under <run-dir>/results/nodes=<N>/. The lanes implement
# the ADR 0048 §2 workload — the SAME total client population at every size,
# spread across all brokers — and ADR 0049's split into two curves:
#
#   lane A  durable QoS1, closed-loop (durable_bench, exact client-side p99)
#   lane B  non-durable $share fan-out at an offered-rate ladder (emqtt-bench)
#   lane C  50k idle connections (memory per connection, establishment rate)
#
# Every rung snapshots each broker's /metrics before and after (counter
# cross-checks) and samples every host's CPU via mpstat (the remote stand-in
# for durable_bench's local driver-bound check, which cannot run multi-host).
#
# SMOKE=1 shrinks every knob to prove the pipeline for cents; the numbers from
# a smoke run are for the pipeline, never for the doc.
#
# LANES=<subset of ABC> (default ABC) runs only the named lanes — e.g. LANES=B
# reruns the fan-out ladder alone (a driver-count experiment does not need to
# pay for lane A's hour). Skipping lane A also skips its barrier probes; a
# lane-B/C-only result dir is therefore NOT a Curve 1 point and the summarizer
# will render it as such.

set -euo pipefail
. "$(dirname "$0")/lib.sh"

RUN="${1:?usage: run-curve.sh <run-dir> <inventory.json>}"
INVENTORY="${2:?inventory.json}"

N=$(broker_count)
D=$(driver_count)
OUT="$RUN/results/nodes=$N"
mkdir -p "$OUT"

LANES="${LANES:-ABC}"
case "$LANES" in
*[!ABC]* | "") die "LANES must be a non-empty subset of ABC (got: $LANES)" ;;
esac

# ── knobs (each SMOKE value proves the path, not the number) ─────────────────
if [ "${SMOKE:-0}" = 1 ]; then
	LANE_B_RUNGS=(20000)
	LANE_B_SECS=15
	LANE_C_CONNS=2000
	LANE_C_RAMP=200
	LANE_C_HOLD=20
	A_REPS=1 A_SECS=15 A_WARMUP=3
	BARRIER_OPS=50
else
	# Overridable for a focused knee hunt (issue #258 follow-up: the 7-node
	# fan-out plateau at ~140k with idle aggregate CPU): a space-separated
	# rung list, e.g. LANE_B_RUNGS_OVERRIDE="50000 100000 200000 300000".
	if [ -n "${LANE_B_RUNGS_OVERRIDE:-}" ]; then
		read -r -a LANE_B_RUNGS <<<"$LANE_B_RUNGS_OVERRIDE"
	else
		LANE_B_RUNGS=(20000 50000 100000 200000 300000)
	fi
	LANE_B_SECS=60
	LANE_C_CONNS=50000
	LANE_C_RAMP=2500
	LANE_C_HOLD=120
	A_REPS=3 A_SECS=60 A_WARMUP=10
	BARRIER_OPS=150
fi
LANE_B_PUBS=600 # total publishers; per-rung rate = 600 * 1000/I
LANE_B_SUBS=300 # total subscribers, ONE shared group ($share/g1)
# Publisher in-flight window. Without it each QoS1 publisher is a WINDOW-1
# closed loop whose send rate tracks the broker's ack rate — the ladder then
# degenerates into a saturation probe that can never offer above it (measured:
# every rung identical). 100 matches bench/run.sh's saturate posture.
LANE_B_INFLIGHT=100
LANE_B_REF_RUNG=50000
BENCH_IMG="emqx/emqtt-bench:0.6.3"

# ── helpers ──────────────────────────────────────────────────────────────────
brokers_csv() { # brokers_csv <suffix-jq> — comma list over brokers
	jq -r "[.brokers[] | $1] | join(\",\")" "$INVENTORY"
}

snapshot_metrics() { # snapshot_metrics <dir> <label>
	local dir="$1" label="$2" i
	mkdir -p "$dir"
	for ((i = 0; i < N; i++)); do
		rssh "$(broker_pub_ip "$i")" "curl -s http://localhost:8080/metrics" \
			>"$dir/metrics-$label-broker$i.prom" || true
	done
}

start_cpu_sampling() { # start_cpu_sampling <dir> <secs> — background mpstat on every host
	local dir="$1" secs="$2" i
	CPU_PIDS=()
	mkdir -p "$dir"
	for ((i = 0; i < N; i++)); do
		rssh "$(broker_pub_ip "$i")" "mpstat -P ALL 5 $((secs / 5 + 1))" \
			>"$dir/cpu-broker$i.txt" 2>/dev/null &
		CPU_PIDS+=($!)
	done
	for ((i = 0; i < D; i++)); do
		rssh "$(driver_pub_ip "$i")" "mpstat -P ALL 5 $((secs / 5 + 1))" \
			>"$dir/cpu-driver$i.txt" 2>/dev/null &
		CPU_PIDS+=($!)
	done
}
stop_cpu_sampling() {
	local p
	for p in "${CPU_PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done
	CPU_PIDS=()
}

drun() { # drun <driver-index> <name> <docker args...> — detached container on a driver
	local di="$1" name="$2"
	shift 2
	rssh "$(driver_pub_ip "$di")" \
		"docker run -d --network host --ulimit nofile=1048576:1048576 --name $name $*" >/dev/null
}

dstop() { # dstop <driver-index> <name> <log-file> — capture logs, remove
	local di="$1" name="$2" log="$3"
	rssh "$(driver_pub_ip "$di")" "docker logs $name" >"$log" 2>&1 || true
	rssh "$(driver_pub_ip "$di")" "docker rm -f $name" >/dev/null 2>&1 || true
}

# TLS client material lives on every driver at /opt/bench-certs (bootstrap
# pushes it): peer-ca.pem verifies the broker's server cert, client.pem/key is
# the mTLS identity against the separate client CA.
TLS_ARGS="-S true --cacertfile /opt/bench-certs/peer-ca.pem --certfile /opt/bench-certs/client.pem --keyfile /opt/bench-certs/client.key"

# ── 0. preflight snapshot ────────────────────────────────────────────────────
say "[$N nodes] preflight: readyz + statusz per broker"
mkdir -p "$OUT/preflight"
for ((i = 0; i < N; i++)); do
	rssh "$(broker_pub_ip "$i")" \
		"curl -s http://localhost:8080/readyz; echo; curl -s http://localhost:8080/statusz" \
		>"$OUT/preflight/broker$i.json" || true
done
# The 5-node replica-convergence plateau is CAPTURED here, not just tolerated —
# the published doc quotes it beside the 5-node point.
jq -s '.' "$OUT"/preflight/broker*.json >/dev/null 2>&1 || warn "statusz not all JSON — kept raw"

# ── 1. barrier probes on every broker host (DURABLE-PATH.md's prerequisite) ──
# The durable_bench test binary carries the probes; driver-1 built it and serves
# it over the private network so each broker can fetch it LAN-fast.
# The guard spans sections 1 and 2 (probes gate Curve 1 only; both bodies keep
# their top-level indentation to keep this diff reviewable).
if [[ "$LANES" != *A* ]]; then
	say "[$N nodes] LANES=$LANES — skipping barrier probes and lane A"
else
say "[$N nodes] barrier probes on every broker host"
BIN_PATH=$(rssh "$(driver_pub_ip 0)" "cat /run/bench-driver-build-done")
[ -n "$BIN_PATH" ] || die "driver-1 build marker is empty — durable_bench binary missing"
# Older markers held a path relative to the checkout — anchor it either way.
case "$BIN_PATH" in /*) ;; *) BIN_PATH="/opt/mqtt-broker/$BIN_PATH" ;; esac
BIN_NAME=$(basename "$BIN_PATH")
# A real transient unit, not `nohup … &` over ssh — a backgrounded process
# whose stdin is the ssh channel dies with the session.
rssh "$(driver_pub_ip 0)" \
	"systemctl is-active bench-probe-server >/dev/null 2>&1 || systemd-run --unit bench-probe-server --working-directory=$(dirname "$BIN_PATH") python3 -m http.server 8093"
# The server needs a beat to bind before anyone fetches from it.
wait_for "probe file server on driver-1" 60 \
	rssh "$(driver_pub_ip 0)" "curl -sfI -o /dev/null http://localhost:8093/$BIN_NAME"
DRIVER1_PRIV=$(inv '.drivers[0].private_ip')
mkdir -p "$OUT/probes"
probe_one() {
	local i="$1" ip
	ip=$(broker_pub_ip "$i")
	wait_for "probe binary fetch on broker $i" 120 \
		rssh "$ip" "curl -sf -o /usr/local/bin/durable_bench http://$DRIVER1_PRIV:8093/$BIN_NAME && chmod +x /usr/local/bin/durable_bench"
	# TMPDIR on the data-dir filesystem: the probe must measure the volume the
	# broker commits on, not the OS temp mount.
	rssh "$ip" "TMPDIR=/var/lib/mqttd-probe MQTTD_BENCH_BARRIER_OPS=$BARRIER_OPS /usr/local/bin/durable_bench device_barrier_floor --ignored --nocapture" \
		>"$OUT/probes/broker$i-device_barrier_floor.txt"
	rssh "$ip" "TMPDIR=/var/lib/mqttd-probe MQTTD_BENCH_STORE_OPS=$BARRIER_OPS /usr/local/bin/durable_bench store_append_floor --ignored --nocapture" \
		>"$OUT/probes/broker$i-store_append_floor.txt"
	say "  broker $i probes done"
}
every_broker probe_one

# ── 2. lane A — durable QoS1 closed-loop (cluster is in durable mode) ────────
# A transient one-way UDP loss in the minutes after boot can get one member
# evicted and fully REMOVED (issue #393; the ghost survives even a process
# restart, because its probes still get acked and nobody probes a non-member).
# Bootstrap only enforces the majority floor, so the run would otherwise burn
# every lane A arm's whole preflight deadline against an N-1/N split. Gate on
# full membership here; if the split already formed, re-form the cluster once —
# by then the private net has converged, and the shrunk voter roster is
# committed state, so only a wipe-and-re-form heals it (no in-place path does).
full_membership() {
	local i out
	for ((i = 0; i < N; i++)); do
		out=$(rssh "$(broker_pub_ip "$i")" "curl -sf -m 4 http://localhost:8080/readyz") || return 1
		jq -e --argjson n "$N" '.ready == true and .members == $n' <<<"$out" >/dev/null || return 1
	done
}
membership_gate() {
	local deadline=$(($(date +%s) + 120))
	until full_membership; do
		[ "$(date +%s)" -lt "$deadline" ] || return 1
		sleep 5
	done
}
if ! membership_gate; then
	warn "membership split after boot (the issue #393 shape) — wiping and re-forming once"
	wipe_for_reform() {
		rssh "$(broker_pub_ip "$1")" "systemctl stop mqttd; rm -rf /var/lib/mqttd/*"
	}
	every_broker wipe_for_reform
	"$SCALE_DIR/bootstrap-cluster.sh" "$RUN" "$INVENTORY" durable
	wait_for "full membership after re-formation" 180 full_membership
fi
say "[$N nodes] lane A: durable QoS1 closed-loop (spread ownership)"
mkdir -p "$OUT/laneA"
BROKERS_LIST=$(brokers_csv '.mqtt_plain')
HEALTH_LIST=$(brokers_csv '.health')
IDS_LIST=$(brokers_csv '.node_id')
lane_a() { # lane_a <name> <publishers> <window> <subs>
	local name="$1" pubs="$2" window="$3" subs="$4"
	snapshot_metrics "$OUT/laneA" "before-$name"
	start_cpu_sampling "$OUT/laneA/cpu-$name" $((A_REPS * (A_SECS + A_WARMUP + 10)))
	rssh "$(driver_pub_ip 0)" \
		"MQTTD_BENCH_BROKERS=$BROKERS_LIST MQTTD_BENCH_HEALTH=$HEALTH_LIST MQTTD_BENCH_NODE_IDS=$IDS_LIST \
		 MQTTD_BENCH_SPREAD=1 MQTTD_BENCH_PUBLISHERS=$pubs MQTTD_BENCH_WINDOW=$window \
		 MQTTD_BENCH_SUBS=$subs MQTTD_BENCH_SECS=$A_SECS MQTTD_BENCH_WARMUP_SECS=$A_WARMUP \
		 MQTTD_BENCH_REPS=$A_REPS $BIN_PATH durable_path_floor --ignored --nocapture" \
		>"$OUT/laneA/$name.txt" 2>&1 || warn "lane A $name exited nonzero — kept output"
	stop_cpu_sampling
	snapshot_metrics "$OUT/laneA" "after-$name"
}
lane_a sat 48 8 48
lane_a lat "$N" 1 "$N"
# ADR 0072 tier showcase: the SAME saturating workload with the publisher
# selecting a weaker ack per message (mqttd-durability property, MQTT 5).
# Same brokers, same sessions-spread shape — only the ack's meaning moves.
lane_a_tier() { # lane_a_tier <tier>
	local t="$1"
	snapshot_metrics "$OUT/laneA" "before-tier-$t"
	start_cpu_sampling "$OUT/laneA/cpu-tier-$t" $((A_REPS * (A_SECS + A_WARMUP + 10)))
	rssh "$(driver_pub_ip 0)" \
		"MQTTD_BENCH_BROKERS=$BROKERS_LIST MQTTD_BENCH_HEALTH=$HEALTH_LIST MQTTD_BENCH_NODE_IDS=$IDS_LIST \
		 MQTTD_BENCH_SPREAD=1 MQTTD_BENCH_TIER=$t MQTTD_BENCH_PUBLISHERS=48 MQTTD_BENCH_WINDOW=8 \
		 MQTTD_BENCH_SUBS=48 MQTTD_BENCH_SECS=$A_SECS MQTTD_BENCH_WARMUP_SECS=$A_WARMUP \
		 MQTTD_BENCH_REPS=$A_REPS $BIN_PATH durable_path_floor --ignored --nocapture" \
		>"$OUT/laneA/tier-$t.txt" 2>&1 || warn "lane A tier $t exited nonzero — kept output"
	stop_cpu_sampling
	snapshot_metrics "$OUT/laneA" "after-tier-$t"
	# The tier's true face is LATENCY below saturation: under closed-loop
	# saturation the lanes flow-control every tier back to the durable
	# pipeline's rate, so sat throughput converges — but the uncontended ack
	# RTT is what the publisher actually bought (relaxed ≈ hub round trip).
	rssh "$(driver_pub_ip 0)" \
		"MQTTD_BENCH_BROKERS=$BROKERS_LIST MQTTD_BENCH_HEALTH=$HEALTH_LIST MQTTD_BENCH_NODE_IDS=$IDS_LIST \
		 MQTTD_BENCH_SPREAD=1 MQTTD_BENCH_TIER=$t MQTTD_BENCH_PUBLISHERS=$N MQTTD_BENCH_WINDOW=1 \
		 MQTTD_BENCH_SUBS=$N MQTTD_BENCH_SECS=$A_SECS MQTTD_BENCH_WARMUP_SECS=$A_WARMUP \
		 MQTTD_BENCH_REPS=$A_REPS $BIN_PATH durable_path_floor --ignored --nocapture" \
		>"$OUT/laneA/tier-$t-lat.txt" 2>&1 || warn "lane A tier $t lat exited nonzero — kept output"
	say "  lane A tier $t done"
}
if [ "${SMOKE:-0}" != 1 ]; then
	lane_a_tier local
	lane_a_tier relaxed
fi

# Past-the-voter-cap variant: at N > MQTTD_LEASE_VOTERS' default (5), durable
# ownership capacity is architecturally flat (ADR 0021/0049) — the default run
# above MEASURES that flat line. This variant then re-forms the cluster with
# the cap raised to N (data dirs wiped first: the voter set is committed Raft
# state, so only a fresh formation honestly measures the larger cap) and runs
# the same sat/lat shape — the actually-new question at this size: what does a
# majority-of-N quorum cost over a majority-of-5?
if [ "$N" -gt 5 ] && [ "${SMOKE:-0}" != 1 ]; then
	say "[$N nodes] lane A voters variant: fresh formation with MQTTD_LEASE_VOTERS=$N"
	wipe_broker_data() {
		rssh "$(broker_pub_ip "$1")" "systemctl stop mqttd; rm -rf /var/lib/mqttd/*"
	}
	every_broker wipe_broker_data
	EXTRA_BROKER_ENV="MQTTD_LEASE_VOTERS=$N" \
		"$SCALE_DIR/bootstrap-cluster.sh" "$RUN" "$INVENTORY" durable
	lane_a "sat-voters$N" 48 8 48
	lane_a "lat-voters$N" "$N" 1 "$N"
	say "  lane A voters variant done"
fi
fi # LANES *A*

# ── switch the brokers to the non-durable posture for lanes B and C ──────────
if [[ "$LANES" == *B* || "$LANES" == *C* ]]; then
	say "[$N nodes] switching brokers to non-durable posture (lanes B/C parity)"
	"$SCALE_DIR/bootstrap-cluster.sh" "$RUN" "$INVENTORY" clean
fi

# ── 3. lane B — $share fan-out ladder ────────────────────────────────────────
# ONE shared subscription group over everything the publishers emit: the ADR
# 0015 mechanism, end to end. Populations are split evenly across drivers and
# round-robin across brokers; the TOTAL population and ladder are identical at
# every cluster size (the §2 "same workload" rule).
if [[ "$LANES" != *B* ]]; then
	say "[$N nodes] LANES=$LANES — skipping lane B"
else
say "[$N nodes] lane B: \$share fan-out ladder (${LANE_B_RUNGS[*]} msg/s)"
lane_b_rung() { # lane_b_rung <total-rate> <posture:plain|mtls>
	local rate="$1" posture="$2"
	local rdir="$OUT/laneB/rung-$rate-$posture"
	mkdir -p "$rdir"
	local interval=$((LANE_B_PUBS * 1000 / rate)) # ms between msgs per publisher
	[ "$interval" -ge 1 ] || die "rung $rate needs sub-ms publish intervals with $LANE_B_PUBS publishers"
	local subs_per=$((LANE_B_SUBS / (D * N))) pubs_per=$((LANE_B_PUBS / (D * N)))
	[ "$subs_per" -ge 1 ] || subs_per=1
	[ "$pubs_per" -ge 1 ] || pubs_per=1
	local args="" port_base=9200
	[ "$posture" = mtls ] && args="$TLS_ARGS"
	snapshot_metrics "$rdir" before
	start_cpu_sampling "$rdir/cpu" "$LANE_B_SECS"
	local di bi host port
	# subscribers first (they expose the e2e histogram), then publishers
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			host=$(inv ".brokers[$bi].private_ip")
			port=$([ "$posture" = mtls ] && echo 8883 || echo 1883)
			drun "$di" "sub-$di-$bi" "-v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG \
				sub -h $host -p $port -c $subs_per -t '\$share/g1/bench/#' -q 1 \
				--payload-hdrs ts --prometheus --restapi $((port_base + bi)) $args"
		done
	done
	sleep 5
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			host=$(inv ".brokers[$bi].private_ip")
			port=$([ "$posture" = mtls ] && echo 8883 || echo 1883)
			drun "$di" "pub-$di-$bi" "-v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG \
				pub -h $host -p $port -c $pubs_per -t 'bench/%i' -q 1 -s 256 \
				-I $interval -F $LANE_B_INFLIGHT --payload-hdrs ts $args"
		done
	done
	sleep "$LANE_B_SECS"
	# scrape each subscriber's histogram BEFORE stopping anything
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			rssh "$(driver_pub_ip "$di")" "curl -s http://localhost:$((port_base + bi))/metrics" \
				>"$rdir/sub-$di-$bi.prom" 2>/dev/null || true
		done
	done
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			dstop "$di" "pub-$di-$bi" "$rdir/pub-$di-$bi.log"
			dstop "$di" "sub-$di-$bi" "$rdir/sub-$di-$bi.log"
		done
	done
	stop_cpu_sampling
	snapshot_metrics "$rdir" after
	say "  rung $rate ($posture) done"
}
for rate in "${LANE_B_RUNGS[@]}"; do
	lane_b_rung "$rate" plain
done
# mTLS posture: the reference rung only, per size (ADR 0048 §3 discloses both
# postures without paying for the full ladder twice).
if [ "${SMOKE:-0}" != 1 ]; then
	lane_b_rung "$LANE_B_REF_RUNG" mtls
fi
fi # LANES *B*

# ── 4. lane C — idle connection fan-out ──────────────────────────────────────
if [[ "$LANES" != *C* ]]; then
	say "[$N nodes] LANES=$LANES — skipping lane C"
else
say "[$N nodes] lane C: $LANE_C_CONNS idle connections"
lane_c() { # lane_c <total-conns> <posture>
	local total="$1" posture="$2"
	local cdir="$OUT/laneC/$posture-$total"
	mkdir -p "$cdir"
	# >=2 containers per driver so a 1-node cluster never exhausts one source
	# tuple's ephemeral ports; containers round-robin across brokers.
	local per_driver=$((total / D)) containers=2
	local per_container=$((per_driver / containers))
	local ramp_per=$((LANE_C_RAMP / (D * containers)))
	[ "$ramp_per" -ge 1 ] || ramp_per=1
	local args=""
	[ "$posture" = mtls ] && args="$TLS_ARGS"
	rss_snap() { # rss_snap <label>
		local i
		for ((i = 0; i < N; i++)); do
			rssh "$(broker_pub_ip "$i")" \
				"grep VmRSS /proc/\$(systemctl show -p MainPID --value mqttd)/status; systemctl show -p MemoryCurrent --value mqttd; ls /proc/\$(systemctl show -p MainPID --value mqttd)/fd | wc -l" \
				>"$cdir/rss-$1-broker$i.txt" || true
		done
	}
	rss_snap before
	snapshot_metrics "$cdir" before
	local di j bi host port
	for ((di = 0; di < D; di++)); do
		for ((j = 0; j < containers; j++)); do
			bi=$(((di * containers + j) % N))
			host=$(inv ".brokers[$bi].private_ip")
			port=$([ "$posture" = mtls ] && echo 8883 || echo 1883)
			drun "$di" "conn-$di-$j" "-v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG \
				conn -h $host -p $port -c $per_container -R $ramp_per $args"
		done
	done
	local ramp_secs=$((total / LANE_C_RAMP + 10))
	say "  ramping ~${LANE_C_RAMP}/s for ~${ramp_secs}s, then holding ${LANE_C_HOLD}s"
	sleep $((ramp_secs + LANE_C_HOLD))
	rss_snap after
	snapshot_metrics "$cdir" after
	for ((di = 0; di < D; di++)); do
		for ((j = 0; j < containers; j++)); do
			dstop "$di" "conn-$di-$j" "$cdir/conn-$di-$j.log"
		done
	done
	say "  lane C $posture/$total done"
}
lane_c "$LANE_C_CONNS" plain
if [ "${SMOKE:-0}" != 1 ]; then
	lane_c 10000 mtls
fi
fi # LANES *C*

# ── 5. host facts for the disclosure block ───────────────────────────────────
mkdir -p "$OUT/env"
for ((i = 0; i < N; i++)); do
	rssh "$(broker_pub_ip "$i")" \
		"uname -a; lscpu | head -20; free -h; df -T /var/lib/mqttd; chronyc tracking 2>/dev/null | head -5; cat /etc/mqttd/mqttd.env" \
		>"$OUT/env/broker$i.txt" || true
done
for ((i = 0; i < D; i++)); do
	rssh "$(driver_pub_ip "$i")" \
		"uname -a; lscpu | head -20; free -h; chronyc tracking 2>/dev/null | head -5; docker --version" \
		>"$OUT/env/driver$i.txt" || true
done
cp "$INVENTORY" "$OUT/env/inventory.json"

say "[$N nodes] all lanes complete -> $OUT"
