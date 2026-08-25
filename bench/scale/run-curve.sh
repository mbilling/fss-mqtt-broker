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
#
# SHAPE_ONLY=1 validates the knobs and lane B's shape against the inventory and
# exits before the first ssh (README: "Checking a shape without paying").

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
elif [ "${STANDARD:-0}" = 1 ]; then
	# The short release profile (run.sh standard): the same measurements, one
	# pass. Every knob below is a DURATION or a COUNT — nothing here changes
	# what is measured or how, so a standard number is directly comparable to a
	# full one; what it loses is CONFIDENCE (one rep, no median, no spread) and
	# COVERAGE (one size, one posture, no ladder tail). That is the trade a
	# regression gate makes and a published curve does not.
	LANE_B_RUNGS=(50000 150000 300000)
	LANE_B_SECS=45
	A_REPS=1 A_SECS=45 A_WARMUP=10
	BARRIER_OPS=150
	# Lane C does NOT run in this profile — `run.sh standard` sets LANES=AB.
	# These exist only so an explicit `LANES=ABC ./run.sh standard` still has
	# them defined (this script runs under `set -u`); at the full profile's
	# hold, since a deliberately re-enabled lane should measure, not sample.
	LANE_C_CONNS=50000
	LANE_C_RAMP=2500
	LANE_C_HOLD=120
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
# Total publishers. Sized so EVERY rung of the ladder is actually offerable:
# each publisher sends on an INTEGER-millisecond timer (emqtt-bench `-I`), so
# the offered rate is `LANE_B_PUBS * 1000 / interval_ms` and a high rung with
# few publishers demands a sub-5ms timer the drivers cannot hold. Measured on
# the v1.0.5 7-node campaign with 600 publishers: the drivers tracked the offer
# to 96% at 6ms (100k) and then collapsed — 74% at 3ms (200k), 52% at 2ms
# (300k), saturating near 150k/s no matter what was asked. Every "knee" above
# that was the LOAD GENERATOR's timer, not the broker, and the idle CPU on both
# sides was the tell. 3000 keeps the top rung at a 10ms timer, inside the
# regime the drivers demonstrably sustain.
# LANE_B_PUBS_OVERRIDE reproduces an older campaign's population verbatim.
LANE_B_PUBS="${LANE_B_PUBS_OVERRIDE:-3000}" # per-rung rate = LANE_B_PUBS * 1000/I
# Total subscribers in the ONE shared group ($share/g1). This is the variable
# that has never changed in this project's history, and the fan-out ceiling has
# tracked it exactly: every saturated configuration — publisher counts 3000,
# 4000 and 6000, socket mode `once` and `true`, offers from 200k to 800k —
# delivered 300-326k, which is ~1000 msg/s per subscriber every time, with
# brokers at 26-35% CPU and latency flat at p50 <=10ms. A per-subscriber
# ceiling and a fixed subscriber count multiply out to a fixed wall.
# LANE_B_SUBS_OVERRIDE raises it to test exactly that.
LANE_B_SUBS="${LANE_B_SUBS_OVERRIDE:-300}"
# Containers per (driver, broker) pair. Every dedicated-hardware measurement so
# far has put ONE emqtt-bench process at ~5.3-6.5k msg/s — whether it carried
# 60 clients or 3000, with 1 sibling or 19 — so lane B has looked like
# "D*N containers x ~5.5k" (5 x 10 x ~5.7k is the ~285k wall). k containers per
# pair carry the SAME total population split k ways; only the number of OS
# processes changes. Default 1 = today's rig exactly. Untested on dedicated
# hardware so far — and the shape check below refuses a k the population does
# not divide (k=4 at 5x10 would silently have run 200 of the 300 subscribers).
LANE_B_CONTAINERS="${LANE_B_CONTAINERS:-1}"
# Floor for the per-publisher timer (`-I`, whole milliseconds). Measured on the
# v1.0.5 7-node campaign (#421): the drivers track the offer to 96% at a 6ms
# timer and collapse below it (74% at 3ms, 52% at 2ms). A rung that needs a
# shorter timer is a driver measurement, not a broker one, so the shape check
# refuses it — raise LANE_B_PUBS_OVERRIDE instead. 6 is the evidenced floor,
# not a safe harbour: 96% sits just under the summarizer's 0.97 DRIVER_OK gate,
# so a 6-9ms rung can still be struck DRIVER-LIMITED downstream; 10ms is the
# regime a full ladder has sustained. Lowering the floor is for knowingly
# reproducing a collapsed campaign.
LANE_B_MIN_INTERVAL="${LANE_B_MIN_INTERVAL:-6}"
# Publisher in-flight window. Without it each QoS1 publisher is a WINDOW-1
# closed loop whose send rate tracks the broker's ack rate — the ladder then
# degenerates into a saturation probe that can never offer above it (measured:
# every rung identical). 100 matches bench/run.sh's saturate posture.
LANE_B_INFLIGHT=100
LANE_B_REF_RUNG=50000
# Seconds between the publishers starting and the latency baseline scrape: the
# ramp, excluded from the measured window (see the scrape in lane_b_rung).
LANE_B_SETTLE="${LANE_B_SETTLE:-15}"
BENCH_IMG="emqx/emqtt-bench:0.6.3"
# Extra Erlang VM flags for every bench container, passed as ERL_FLAGS (read by
# erlexec — verified against this image: a refused value fails at startup with
# "bad scheduler busy wait threshold", and the preflight below turns that into
# a die instead of a campaign of zeros). Default EMPTY: no env var is passed and
# the containers run exactly as before the knob existed. One candidate lever,
# untested on dedicated hardware: Erlang schedulers BUSY-WAIT by default, and
# with D*N*k VMs sharing one driver host the spin competes for the cores the
# publishers need. The mechanism would be a throughput ceiling, not timer
# drift: emqtt-bench 0.6.3 paces on an absolute schedule (next = begin +
# attempts x interval) and counts a late wake as `pub_overrun` instead of
# letting lateness accumulate —
#   BENCH_ERL_FLAGS="+sbwt none +sbwtdcpu none +sbwtdio none"
# One variable per campaign: do not move this and LANE_B_CONTAINERS together.
BENCH_ERL_FLAGS="${BENCH_ERL_FLAGS-}"

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
	# BENCH_ERL_FLAGS rides in as ERL_FLAGS only when set; its single quotes
	# survive the rssh single-string hop the way `-t '$share/...'` already does.
	# shellcheck disable=SC2016 # those quotes are for the REMOTE shell; $BENCH_ERL_FLAGS expands here
	rssh "$(driver_pub_ip "$di")" \
		"docker run -d --network host --ulimit nofile=1048576:1048576 --name $name ${BENCH_ERL_FLAGS:+-e ERL_FLAGS='$BENCH_ERL_FLAGS' }$*" >/dev/null
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

# ── lane B shape: refused HERE, before any lane pays for anything ────────────
# Two ways the rig has quietly offered something other than the rung's label:
#  * `-I` is WHOLE milliseconds per client, so a rung that does not divide
#    LANE_B_PUBS*1000 got a floored timer — 3000 publishers asked for 400k get
#    I=7 and offer 428k under a 400k label. A rung is honest only if it needs
#    an integer timer at or above LANE_B_MIN_INTERVAL.
#  * populations were floored per container, so a shape that does not divide
#    them ran FEWER clients than the doc says (7 brokers x 5 drivers: 2975
#    publishers, 280 subscribers) — the ADR 0048 §2 same-workload rule, broken
#    by rounding. LANE_B_CONTAINERS makes this easy to hit.
# Pure arithmetic on knobs the script already has, so it runs before the first
# ssh: a wrong shape costs the provisioning minutes, not lane A's hour, and the
# message names the fix. The table it prints is kept as laneB/shape.txt — the
# disclosure record of what the load stack was actually asked to do.
# SHAPE_ONLY=1 stops right after it, so a knob set can be checked offline
# against a synthesized inventory (README: "Checking a shape without paying").
positive_int() { # positive_int <what> <value> — a plain positive integer, or die
	[[ "$2" =~ ^[1-9][0-9]*$ ]] || die "$1 must be a positive integer (got '$2')"
}
lane_b_interval() { # lane_b_interval <total-rate> — per-publisher timer in ms, or die
	local rate="$1"
	# Unvalidated, a rung token reaches bash arithmetic: "0" divides by zero with
	# no die message, "20000,50000" evaluates the comma operator to the 50k rung
	# and files it under a directory name the summarizer cannot parse, "0200000"
	# is octal.
	positive_int "lane B rung" "$rate"
	[ $((LANE_B_PUBS * 1000 % rate)) -eq 0 ] ||
		die "lane B rung $rate is not exactly offerable by $LANE_B_PUBS publishers (fractional -I) — use a divisor of $((LANE_B_PUBS * 1000)), or a LANE_B_PUBS_OVERRIDE that every rung divides"
	local interval=$((LANE_B_PUBS * 1000 / rate))
	[ "$interval" -ge "$LANE_B_MIN_INTERVAL" ] ||
		die "lane B rung $rate needs -I ${interval}ms with $LANE_B_PUBS publishers; the drivers collapse below ${LANE_B_MIN_INTERVAL}ms (#421) — raise LANE_B_PUBS_OVERRIDE (LANE_B_MIN_INTERVAL lowers the floor knowingly)"
	echo "$interval"
}
lane_b_shape() { # prints the shape as key=value lines; dies on any distortion
	positive_int LANE_B_CONTAINERS "$LANE_B_CONTAINERS"
	positive_int LANE_B_PUBS "$LANE_B_PUBS"
	positive_int LANE_B_SUBS "$LANE_B_SUBS"
	positive_int LANE_B_MIN_INTERVAL "$LANE_B_MIN_INTERVAL"
	local cells=$((D * N * LANE_B_CONTAINERS))
	local over="$D drivers x $N brokers x $LANE_B_CONTAINERS containers ($cells cells)"
	local fix="set the _OVERRIDE to a multiple of $cells, or change LANE_B_CONTAINERS / DRIVER_COUNT"
	if [ $((LANE_B_PUBS % cells)) -ne 0 ] || [ "$LANE_B_PUBS" -lt "$cells" ]; then
		die "LANE_B_PUBS=$LANE_B_PUBS does not split evenly over $over — $fix"
	fi
	if [ $((LANE_B_SUBS % cells)) -ne 0 ] || [ "$LANE_B_SUBS" -lt "$cells" ]; then
		die "LANE_B_SUBS=$LANE_B_SUBS does not split evenly over $over — $fix"
	fi
	echo "brokers=$N drivers=$D containers_per_pair=$LANE_B_CONTAINERS cells=$cells"
	echo "publishers=$LANE_B_PUBS per_container=$((LANE_B_PUBS / cells))"
	echo "subscribers=$LANE_B_SUBS per_container=$((LANE_B_SUBS / cells)) group=\$share/g1"
	local rungs=("${LANE_B_RUNGS[@]}") rate interval
	if [ "${SMOKE:-0}" != 1 ] && [ "${STANDARD:-0}" != 1 ]; then rungs+=("$LANE_B_REF_RUNG"); fi
	for rate in "${rungs[@]}"; do
		interval=$(lane_b_interval "$rate")
		echo "rung=$rate interval_ms=$interval per_container_msg_s=$((rate / cells))"
	done
	echo "inflight=$LANE_B_INFLIGHT settle_s=$LANE_B_SETTLE measure_s=$LANE_B_SECS min_interval_ms=$LANE_B_MIN_INTERVAL"
	echo "image=$BENCH_IMG erl_flags='$BENCH_ERL_FLAGS'"
}
if [[ "$LANES" == *B* ]]; then
	mkdir -p "$OUT/laneB"
	lane_b_shape >"$OUT/laneB/shape.txt"
	say "[$N nodes] lane B shape (kept as laneB/shape.txt):"
	sed 's/^/    /' "$OUT/laneB/shape.txt" >&2
fi
if [ "${SHAPE_ONLY:-0}" = 1 ]; then
	say "SHAPE_ONLY=1 — knobs and lane B shape verified; touching no host"
	exit 0
fi

# ── bench VM flags: proved on a driver before any container is paid for ─────
# `docker run -d` returns 0 whether or not beam accepts ERL_FLAGS. A refused
# value kills every load container ~100ms after start and nothing downstream
# would notice: the rung sleeps out its window, the scrapes come back empty
# under `|| true`, and the summarizer prints a campaign of zeros. So a
# non-empty BENCH_ERL_FLAGS is tried once against the image on driver 0 —
# `pub --help` exits 1 with beam's own message when a flag is refused, 0 when
# it is accepted (verified against 0.6.3) — before lanes B/C start anything.
if [[ "$LANES" == *[BC]* ]] && [ -n "$BENCH_ERL_FLAGS" ]; then
	probe=$(rssh "$(driver_pub_ip 0)" "docker run --rm -e ERL_FLAGS='$BENCH_ERL_FLAGS' $BENCH_IMG pub --help 2>&1") ||
		die "BENCH_ERL_FLAGS='$BENCH_ERL_FLAGS' is refused by the Erlang VM in $BENCH_IMG: $(printf '%s\n' "$probe" | head -1)"
	say "BENCH_ERL_FLAGS accepted by $BENCH_IMG on driver 0: $BENCH_ERL_FLAGS"
fi

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
# The tier arms are a durable-PATH claim: they belong to the release that
# changes the durable path, and the standard profile does not re-pay for them
# (they are ~a third of lane A's wall clock).
if [ "${SMOKE:-0}" != 1 ] && [ "${STANDARD:-0}" != 1 ]; then
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
# Skipped on the standard profile: the committee-tax question was answered at
# 7 nodes (zero steady-state cost over majority-of-5), and re-answering it
# costs a whole extra formation plus two lane-A runs at the largest size.
if [ "$N" -gt 5 ] && [ "${SMOKE:-0}" != 1 ] && [ "${STANDARD:-0}" != 1 ]; then
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
	# The timer and the per-container split were both verified exact by
	# lane_b_shape at the top of the run; nothing here can floor.
	local interval nc=$LANE_B_CONTAINERS
	interval=$(lane_b_interval "$rate") # ms between msgs per publisher
	local subs_per=$((LANE_B_SUBS / (D * N * nc))) pubs_per=$((LANE_B_PUBS / (D * N * nc)))
	local args="" port_base=9200
	[ "$posture" = mtls ] && args="$TLS_ARGS"
	# emqtt-bench's socket mode. Its own help: "once" is the LEGACY DEFAULT,
	# "suitable for high-concurrency clients with LOW MESSAGE FREQUENCY"; "true"
	# is "highly active (RECOMMENDED), optimized for full speed, ideal for
	# HIGH-FREQUENCY messages and balanced scheduling". Every rung this rig has
	# ever run is a high-frequency saturation test, and every one of them ran on
	# the low-frequency default: `once` re-arms the socket after each message,
	# so every message costs a syscall and a scheduler round trip. That is
	# latency-bound, not CPU-bound — which is exactly the shape we measured
	# (a hard ceiling near 950 msg/s per connection with idle cores on both
	# sides). Use what the tool recommends for what we are actually doing.
	local active="-A true"
	snapshot_metrics "$rdir" before
	start_cpu_sampling "$rdir/cpu" $((LANE_B_SETTLE + LANE_B_SECS))
	local di bi k host port
	# subscribers first (they expose the e2e histogram), then publishers
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			host=$(inv ".brokers[$bi].private_ip")
			port=$([ "$posture" = mtls ] && echo 8883 || echo 1883)
			for ((k = 0; k < nc; k++)); do
				drun "$di" "sub-$di-$bi-$k" "-v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG \
					sub -h $host -p $port -c $subs_per -t '\$share/g1/bench/#' -q 1 $active \
					--payload-hdrs ts --prometheus --restapi $((port_base + bi * nc + k)) $args"
			done
		done
	done
	sleep 5
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			host=$(inv ".brokers[$bi].private_ip")
			port=$([ "$posture" = mtls ] && echo 8883 || echo 1883)
			# `-t bench/%i` numbers topics by CLIENT SEQNO, which restarts at
			# --startnumber in every container. Without an offset all D*N
			# containers publish the same `bench/1..pubs_per` set, so the run
			# has `LANE_B_PUBS / (D*N)` distinct topics — 60 where the doc
			# claims 3000 — and the count silently changes with fleet shape,
			# which is precisely what ADR 0048 §2's same-workload-at-every-size
			# rule forbids. emqtt-bench documents `-n` for exactly this ("useful
			# when running multiple emqtt-bench instances to test the same
			# broker"). Offset each container's block so topics are globally
			# distinct and the topic count IS LANE_B_PUBS by construction.
			for ((k = 0; k < nc; k++)); do
				local seq_base=$((((di * N + bi) * nc + k) * pubs_per))
				drun "$di" "pub-$di-$bi-$k" "-v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG \
					pub -h $host -p $port -c $pubs_per -t 'bench/%i' -q 1 -s 256 $active \
					-n $seq_base -I $interval -F $LANE_B_INFLIGHT --payload-hdrs ts $args"
			done
		done
	done
	# Let the publisher ramp finish BEFORE the latency measurement starts. The
	# subscriber histograms are CUMULATIVE over the container's life, so a
	# single end-of-rung scrape bakes every message delivered while the
	# publishers were still connecting into the published percentiles — a fine
	# median with a heavy tail, for reasons that have nothing to do with the
	# broker's steady state. Baseline here, scrape again at the end, and the
	# summarizer reports the DIFFERENCE: the same steady-window discipline the
	# throughput numbers already use.
	sleep "$LANE_B_SETTLE"
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			for ((k = 0; k < nc; k++)); do
				rssh "$(driver_pub_ip "$di")" "curl -s http://localhost:$((port_base + bi * nc + k))/metrics" \
					>"$rdir/sub-$di-$bi-$k-base.prom" 2>/dev/null || true
			done
		done
	done
	sleep "$LANE_B_SECS"
	# scrape each subscriber's histogram BEFORE stopping anything
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			for ((k = 0; k < nc; k++)); do
				rssh "$(driver_pub_ip "$di")" "curl -s http://localhost:$((port_base + bi * nc + k))/metrics" \
					>"$rdir/sub-$di-$bi-$k.prom" 2>/dev/null || true
			done
		done
	done
	for ((di = 0; di < D; di++)); do
		for ((bi = 0; bi < N; bi++)); do
			for ((k = 0; k < nc; k++)); do
				dstop "$di" "pub-$di-$bi-$k" "$rdir/pub-$di-$bi-$k.log"
				dstop "$di" "sub-$di-$bi-$k" "$rdir/sub-$di-$bi-$k.log"
			done
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
# postures without paying for the full ladder twice). The standard profile skips
# it — one posture is a regression gate; TWO is what ADR 0048 §3 requires of a
# PUBLISHED curve, which is `full`'s job.
if [ "${SMOKE:-0}" != 1 ] && [ "${STANDARD:-0}" != 1 ]; then
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
