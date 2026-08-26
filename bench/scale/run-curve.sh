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
	LANE_B_RUNGS=(300000 150000 50000)
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
	LANE_B_RUNGS=(300000 200000 100000 50000 20000)
	LANE_B_SECS=60
	LANE_C_CONNS=50000
	LANE_C_RAMP=2500
	LANE_C_HOLD=120
	A_REPS=3 A_SECS=60 A_WARMUP=10
	BARRIER_OPS=150
fi
# Ladders run TOP RUNG FIRST: the most demanding rung is the one that decides
# whether the rig can offer the load at all, so its verdict should not wait
# behind rungs that were never in doubt. Rungs are independent (fresh
# containers each, brokers stay up), so the order changes nothing measured.
# LANE_B_RUNGS_OVERRIDE (a space-separated list, any profile) replaces the
# ladder — one rung is the cheapest way to ask one question.
if [ -n "${LANE_B_RUNGS_OVERRIDE:-}" ]; then
	read -r -a LANE_B_RUNGS <<<"$LANE_B_RUNGS_OVERRIDE"
fi
# Total publishers. Sized so EVERY rung of the ladder is actually offerable,
# against the two floors the load generator has:
#  * the TIMER floor: each publisher sends on an INTEGER-millisecond timer
#    (emqtt-bench `-I`), so the offered rate is LANE_B_PUBS * 1000 / interval_ms
#    and the drivers collapse below ~6ms (#421: 96% at 6ms, 74% at 3ms, 52% at
#    2ms with 600 publishers);
#  * the ROUND-TRIP floor: emqtt-bench's TCP publish is SYNCHRONOUS per client
#    (emqtt:publish/4 returns on the PUBACK; `-F` only sets emqtt's max_inflight,
#    which a one-at-a-time loop never fills), so a client can never exceed
#    1/RTT. Measured 2026-08-25 at 10 nodes with 3000 publishers: 50/s per
#    client (150k) on time, 100/s per client (300k) capped at ~80/s with 99% of
#    publishes late and every CPU idle — a ~12ms PUBACK round trip under load.
#    That one mechanism is every "wall" this rig ever hit: 3000 x ~1/12ms is
#    the ~250-300k of five campaigns, and 60 clients x ~95/s the "~5.5k per
#    container".
# 12000 puts the 300k rung at 25/s per client (a 40ms budget) and a 40ms timer;
# every ladder rung is an exact integer timer and 480 per container spreads
# exactly over 3, 5 and 10 brokers. LANE_B_PUBS_OVERRIDE reproduces an older
# campaign's population verbatim (and its ceilings).
LANE_B_PUBS="${LANE_B_PUBS_OVERRIDE:-12000}" # per-rung rate = LANE_B_PUBS * 1000/I
# Total subscribers in the ONE shared group ($share/g1). This is the variable
# that has never changed in this project's history, and the fan-out ceiling has
# tracked it exactly: every saturated configuration — publisher counts 3000,
# 4000 and 6000, socket mode `once` and `true`, offers from 200k to 800k —
# delivered 300-326k, which is ~1000 msg/s per subscriber every time, with
# brokers at 26-35% CPU and latency flat at p50 <=10ms. A per-subscriber
# ceiling and a fixed subscriber count multiply out to a fixed wall.
# LANE_B_SUBS_OVERRIDE raises it to test exactly that.
LANE_B_SUBS="${LANE_B_SUBS_OVERRIDE:-300}"
# Load containers per DRIVER, one vCPU each, every container spanning ALL
# brokers (emqtt-bench's `-h a,b,c` places client i on host i mod |hosts|, so
# the per-broker spread is exact — and checked below). Why this shape:
#  * a bench VM costs ~575 MB whatever it carries (measured 2026-08-25: 20 or
#    75 clients, 8 schedulers or 1 — 572-585 MiB), so containers are memory-
#    bound at ~40 per 32 GB driver; the 60-per-driver experiment (run
#    133322Z) had the kernel OOM-kill beam processes mid-rung;
#  * a VM's default scheduler set burns 2.5x the CPU of one scheduler for the
#    same work (30k msg/s: 235% vs 96% for +S 1:1), and 60 of them put the
#    drivers at 98% CPU while the brokers sat at 65%;
#  * one scheduler pushes ~38k QoS1 msg/s on a laptop core; a 300k rung over
#    25 publisher containers is 12k each.
# 5 + 3 = 8 = a CCX33's vCPUs. Keep PUB + SUB at or below the driver's cores.
LANE_B_PUB_CONTAINERS="${LANE_B_PUB_CONTAINERS:-5}"
LANE_B_SUB_CONTAINERS="${LANE_B_SUB_CONTAINERS:-3}"
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
# Publisher in-flight window, passed as emqtt-bench `-F`. It sets emqtt's
# max_inflight and nothing else: the tool's TCP publish loop is one synchronous
# publish per client at a time (see LANE_B_PUBS), so this window never fills
# and does NOT decouple the publishers from the broker's ack rate — the
# population does that. Kept at 100 for parity with bench/run.sh's posture.
LANE_B_INFLIGHT=100
# Connect pacing for every load container (emqtt-bench `-R`, connections per
# second). The default 10ms-per-client ramp would put 480 publishers on the
# wire over ~5s and a 1-node smoke's 2400 over 24s — past the settle. Kept
# below 1000: emqtt-bench adds a worker per 1000/s of connect rate, and a
# second worker would break the one-worker broker spread (see BENCH_ERL_FLAGS).
LANE_B_CONNECT_RATE="${LANE_B_CONNECT_RATE:-500}"
# Fan-out PERCENTAGE, 0-100: the share of subscriber CONTAINERS that receive
# the FULL publish stream on a plain `bench/#` wildcard; the rest stay in one
# $share/g1 group and collectively get one copy per publish. So:
#   0   = today's ADR 0015 fan-IN exactly (delivered ~= offered);
#   100 = every subscriber gets every publish (delivered ~= offered x SUBS);
#   P   = the first P% of sub containers are wildcard, delivered ~=
#         offered x (P% of SUBS + 1 for the shared remainder).
# Egress fan-out is what stresses the per-connection write path — issue
# #443's batching only engages when a subscriber's outbound backlog is deep,
# which $share (~1/SUBS of the traffic per connection) never produces. Above
# 0 the rung value is the PUBLISH rate; keep it modest (a 5000 rung at 100%
# x 300 subs is 1.5M deliveries/s). The offered-rate honesty checks are
# unchanged: they police what the PUBLISHERS emit, which the rung still names.
LANE_B_FANOUT="${LANE_B_FANOUT:-0}"
# Publish/subscribe QoS for the lane, 0 or 1 (default 1), applied to BOTH
# sides. A QoS 1 publisher is synchronous — it waits for each PUBACK, so its
# rate is throttled by the broker and the offered load oscillates near
# saturation. QoS 0 fires at a steady rate regardless, which a clean
# broker-saturation sweep needs; on the sub side QoS 0 lets the broker shed
# what a slow subscriber cannot take rather than backpressure. QoS 2 is out
# of scope for this lane.
LANE_B_QOS="${LANE_B_QOS:-1}"
# The mTLS reference rung (ADR 0048 §3 discloses both postures per size without
# paying for the full ladder twice). Overridable because it must SUIT THE SHAPE:
# 50000 is a sane publish rate for a fan-in ladder, but under a fan-out workload
# it is 50000 x LANE_B_SUBS deliveries — and it must divide LANE_B_PUBS*1000 into
# an integer -I like any other rung, which the shape check enforces for it too.
LANE_B_REF_RUNG="${LANE_B_REF_RUNG:-50000}"
# Seconds between the publishers starting and the latency baseline scrape: the
# ramp, excluded from the measured window (see the scrape in lane_b_rung).
LANE_B_SETTLE="${LANE_B_SETTLE:-15}"
BENCH_IMG="emqx/emqtt-bench:0.6.3"
# Erlang VM flags for every bench container, passed as ERL_FLAGS (read by
# erlexec — a refused value fails at startup with "bad scheduler busy wait
# threshold", and the preflight below turns that into a die instead of a
# campaign of zeros). Default: ONE scheduler per container — the container is
# one vCPU (see LANE_B_PUB_CONTAINERS) — and no busy-wait, since an idle
# scheduler spinning is a core stolen from a neighbour. Measured on one VM at
# 30k msg/s: 235% CPU with the default scheduler set, 110% with +S 2:2, 96%
# with +S 1:1, the delivered rate identical. BENCH_ERL_FLAGS= (empty) runs the
# VM's own defaults; whatever is used lands in laneB/shape.txt.
BENCH_ERL_FLAGS="${BENCH_ERL_FLAGS-+S 1:1 +sbwt none +sbwtdcpu none +sbwtdio none}"

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

# The fleet-wide docker run prefix: host networking, the fd limit the 50k lane
# needs, and BENCH_ERL_FLAGS as ERL_FLAGS only when set — its single quotes
# survive the rssh single-string hop the way `-t '$share/...'` already does.
# shellcheck disable=SC2016 # those quotes are for the REMOTE shell; $BENCH_ERL_FLAGS expands here
DOCKER_RUN="docker run -d --network host --ulimit nofile=1048576:1048576${BENCH_ERL_FLAGS:+ -e ERL_FLAGS='$BENCH_ERL_FLAGS'}"

drun() { # drun <driver-index> <name> <docker args...> — detached container on a driver
	local di="$1" name="$2"
	shift 2
	rssh "$(driver_pub_ip "$di")" "$DOCKER_RUN --name $name $*" >/dev/null
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
#    by rounding. A container count the population does not divide, or a
#    per-container count the broker count does not divide, hits it.
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
	positive_int LANE_B_PUB_CONTAINERS "$LANE_B_PUB_CONTAINERS"
	positive_int LANE_B_SUB_CONTAINERS "$LANE_B_SUB_CONTAINERS"
	positive_int LANE_B_PUBS "$LANE_B_PUBS"
	positive_int LANE_B_SUBS "$LANE_B_SUBS"
	positive_int LANE_B_MIN_INTERVAL "$LANE_B_MIN_INTERVAL"
	positive_int LANE_B_CONNECT_RATE "$LANE_B_CONNECT_RATE"
	case "$LANE_B_QOS" in 0 | 1) ;; *) die "LANE_B_QOS must be 0 or 1, got '$LANE_B_QOS'" ;; esac
	[[ "$LANE_B_FANOUT" =~ ^[0-9]+$ ]] && [ "$LANE_B_FANOUT" -le 100 ] ||
		die "LANE_B_FANOUT must be a percentage 0-100 (0=shared fan-in, 100=full wildcard fan-out), got '$LANE_B_FANOUT'"
	[ "$LANE_B_CONNECT_RATE" -le 1000 ] ||
		die "LANE_B_CONNECT_RATE=$LANE_B_CONNECT_RATE: emqtt-bench adds a worker per 1000 connections/s and a second worker breaks the one-worker broker spread — keep it at or below 1000"
	# Each population must split evenly over its containers (D x per-driver
	# count). Within a container emqtt-bench places client i on host i mod N
	# (one worker — see BENCH_ERL_FLAGS), and lane_b_rung rotates each
	# container's host list by its index, so a per-container remainder lands on
	# different brokers from one container to the next. spread_range works out
	# the resulting per-broker min..max exactly; the totals are exact whenever
	# N divides the per-container count or the container count, and a skew of
	# one client is the granularity of the split. More than one is refused.
	local pub_cells=$((D * LANE_B_PUB_CONTAINERS)) sub_cells=$((D * LANE_B_SUB_CONTAINERS))
	spread_range() { # spread_range <total> <cells> — min..max clients per broker
		local per=$(($1 / $2)) cells="$2" base extra c i
		local -a count
		for ((i = 0; i < N; i++)); do count[i]=0; done
		base=$((per / N))
		extra=$((per % N))
		for ((c = 0; c < cells; c++)); do
			for ((i = 0; i < N; i++)); do count[i]=$((count[i] + base)); done
			for ((i = 0; i < extra; i++)); do count[(c + i) % N]=$((count[(c + i) % N] + 1)); done
		done
		local min=${count[0]} max=${count[0]}
		for ((i = 1; i < N; i++)); do
			[ "${count[i]}" -lt "$min" ] && min=${count[i]}
			[ "${count[i]}" -gt "$max" ] && max=${count[i]}
		done
		echo "$min..$max"
	}
	split_check() { # split_check <knob> <total> <cells> <containers-per-driver> — prints the per-broker range
		local name="$1" total="$2" cells="$3" per_driver="$4" range
		if [ $((total % cells)) -ne 0 ] || [ "$total" -lt "$cells" ]; then
			die "$name=$total does not split evenly over $D drivers x $per_driver containers ($cells cells) — set ${name}_OVERRIDE to a multiple of $cells, or change the container count / DRIVER_COUNT"
		fi
		range=$(spread_range "$total" "$cells")
		# A skew of one client is the granularity of the split; beyond that it is
		# refused once it exceeds 1% of a broker's share (600 publishers over 25
		# containers on 10 brokers: 58..62 — refused; 12000 on 7 brokers: 1713..1716
		# — accepted and printed).
		local skew=$((${range#*..} - ${range%..*})) min=${range%..*}
		if [ "$skew" -gt 1 ] && [ $((skew * 100)) -gt "$min" ]; then
			die "$name=$total spreads unevenly over $N brokers ($range per broker): $((total / cells)) clients per container with $cells containers — make N divide the per-container count, or the container count a multiple of N"
		fi
		echo "$range"
	}
	local pub_range sub_range
	pub_range=$(split_check LANE_B_PUBS "$LANE_B_PUBS" "$pub_cells" "$LANE_B_PUB_CONTAINERS")
	sub_range=$(split_check LANE_B_SUBS "$LANE_B_SUBS" "$sub_cells" "$LANE_B_SUB_CONTAINERS")
	# The spread above assumes ONE emqtt-bench worker per container: the tool
	# runs one worker per Erlang scheduler and every worker round-robins from
	# host 0, so with the VM's default scheduler count the spread skews.
	case " $BENCH_ERL_FLAGS " in
	*" +S 1:1 "* | *" +S 1 "*) ;;
	*) warn "BENCH_ERL_FLAGS has no +S 1:1 — emqtt-bench runs one worker per scheduler and each worker round-robins from the first broker, so the per-broker spread will NOT be the range printed" ;;
	esac
	echo "brokers=$N drivers=$D pub_containers_per_driver=$LANE_B_PUB_CONTAINERS sub_containers_per_driver=$LANE_B_SUB_CONTAINERS"
	echo "publishers=$LANE_B_PUBS per_container=$((LANE_B_PUBS / pub_cells)) per_broker=$pub_range"
	# fanout containers = floor(pct x total sub containers / 100); each carries
	# subs_per connections that receive EVERY publish. The rest share one copy.
	local fo_containers=$((LANE_B_FANOUT * sub_cells / 100)) sper=$((LANE_B_SUBS / sub_cells))
	local fo_subs=$((fo_containers * sper)) sh_subs=$((LANE_B_SUBS - fo_containers * sper))
	local amp=$((fo_subs + (sh_subs > 0 ? 1 : 0)))
	echo "subscribers=$LANE_B_SUBS per_container=$sper per_broker=$sub_range"
	echo "fanout_pct=$LANE_B_FANOUT fanout_containers=$fo_containers/$sub_cells fanout_subs=$fo_subs shared_subs=$sh_subs (delivered ~= offered x $amp)"
	local rungs=("${LANE_B_RUNGS[@]}") rate interval
	if [ "${SMOKE:-0}" != 1 ] && [ "${STANDARD:-0}" != 1 ]; then rungs+=("$LANE_B_REF_RUNG"); fi
	for rate in "${rungs[@]}"; do
		interval=$(lane_b_interval "$rate")
		echo "rung=$rate interval_ms=$interval per_pub_container_msg_s=$((rate / pub_cells))"
	done
	echo "qos=$LANE_B_QOS inflight=$LANE_B_INFLIGHT connect_rate=$LANE_B_CONNECT_RATE settle_s=$LANE_B_SETTLE measure_s=$LANE_B_SECS min_interval_ms=$LANE_B_MIN_INTERVAL"
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
# ── batched container control: one ssh per driver per phase, drivers in parallel
# Every container used to cost its own ssh round-trip (~1.3s from a laptop to
# fsn1). At one container per driver-broker pair that was 50 per phase; at
# three per pair it was 150, which
# turned a 65-second rung into ~20 minutes, let the 60s CPU sample expire before
# a publisher existed, and put the summarizer's "last 60s" inside the stop
# loop. Each phase is now ONE script per driver, fed on stdin to `bash -s` so
# the docker lines keep exactly the quoting drun used, and the drivers run in
# parallel. Scrapes and log captures come back as one '@@@ <name>'-delimited
# stream per driver and are split locally into the files the summarizer reads.
lane_b_batch() { # lane_b_batch <driver-index> <script> — run the script on the driver; its stdout
	rssh "$(driver_pub_ip "$1")" "bash -s" <<<"$2"
}
lane_b_split() { # lane_b_split <dir> <suffix> <stream-file> — '@@@ name' chunks -> <dir>/<name><suffix>
	awk -v dir="$1" -v sfx="$2" '
		/^@@@ / { if (f) close(f); f = dir "/" $2 sfx; printf "" > f; next }
		f { print > f }' "$3"
}
lane_b_rung() { # lane_b_rung <total-rate> <posture:plain|mtls>
	local rate="$1" posture="$2"
	local rdir="$OUT/laneB/rung-$rate-$posture"
	mkdir -p "$rdir/.batch"
	# The timer and both per-container splits were verified exact by
	# lane_b_shape at the top of the run; nothing here can floor.
	local interval
	interval=$(lane_b_interval "$rate") # ms between msgs per publisher
	local P=$LANE_B_PUB_CONTAINERS S=$LANE_B_SUB_CONTAINERS
	local subs_per=$((LANE_B_SUBS / (D * S))) pubs_per=$((LANE_B_PUBS / (D * P)))
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
	local port
	port=$([ "$posture" = mtls ] && echo 8883 || echo 1883)
	# Every container spans every broker: client i lands on host i mod N, and
	# each container's list is rotated by its index so a remainder lands on
	# different brokers from one container to the next (lane_b_shape proved
	# the resulting per-broker totals).
	local -a HOSTS
	IFS=, read -r -a HOSTS <<<"$(brokers_csv '.private_ip')"
	rotated_hosts() { # rotated_hosts <container-index>
		local rot=$(($1 % N)) i out=""
		for ((i = 0; i < N; i++)); do out+="${HOSTS[(rot + i) % N]},"; done
		echo "${out%,}"
	}
	# Compose every driver's four scripts up front: subscriber starts, publisher
	# starts, histogram scrape (used twice), and log capture + stop.
	local di j seq_base hosts
	local -a subs pubs scrape stop names
	for ((di = 0; di < D; di++)); do
		subs[di]="set -e"$'\n'
		pubs[di]="set -e"$'\n'
		scrape[di]=""
		stop[di]=""
		names[di]=""
		for ((j = 0; j < S; j++)); do
			hosts=$(rotated_hosts $((di * S + j)))
			# The first LANE_B_FANOUT% of sub containers (by global index) take a
			# plain wildcard — every publish reaches every one of their
			# connections; the rest join the one $share group and split one copy.
			if [ $((di * S + j)) -lt $((LANE_B_FANOUT * D * S / 100)) ]; then sub_filter="bench/#"; else sub_filter="\$share/g1/bench/#"; fi
			subs[di]+="$DOCKER_RUN --name sub-$di-$j -v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG sub -h $hosts -p $port -c $subs_per -R $LANE_B_CONNECT_RATE -t '$sub_filter' -q $LANE_B_QOS $active --payload-hdrs ts --prometheus --restapi $((port_base + j)) $args >/dev/null"$'\n'
			scrape[di]+="printf '\\n@@@ sub-$di-$j\\n'; curl -s http://localhost:$((port_base + j))/metrics"$'\n'
			stop[di]+="printf '\\n@@@ sub-$di-$j\\n'; docker logs sub-$di-$j 2>&1"$'\n'
			names[di]+=" sub-$di-$j"
		done
		for ((j = 0; j < P; j++)); do
			# `-t bench/%i` numbers topics by CLIENT SEQNO, which restarts at
			# --startnumber in every container; without an offset every container
			# would publish the same `bench/1..pubs_per` set and the topic count
			# would change with fleet shape (ADR 0048 §2 forbids exactly that).
			# emqtt-bench documents `-n` for this; each container gets its own
			# block, so the topic count IS LANE_B_PUBS by construction.
			seq_base=$(((di * P + j) * pubs_per))
			hosts=$(rotated_hosts $((di * P + j)))
			pubs[di]+="$DOCKER_RUN --name pub-$di-$j -v /opt/bench-certs:/opt/bench-certs:ro $BENCH_IMG pub -h $hosts -p $port -c $pubs_per -R $LANE_B_CONNECT_RATE -t 'bench/%i' -q $LANE_B_QOS -s 256 $active -n $seq_base -I $interval -F $LANE_B_INFLIGHT --payload-hdrs ts $args >/dev/null"$'\n'
			stop[di]+="printf '\\n@@@ pub-$di-$j\\n'; docker logs pub-$di-$j 2>&1"$'\n'
			names[di]+=" pub-$di-$j"
		done
	done
	local -a pids
	local p
	snapshot_metrics "$rdir" before
	# subscribers first (they expose the e2e histogram), then publishers
	pids=()
	for ((di = 0; di < D; di++)); do lane_b_batch "$di" "${subs[di]}" & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || die "starting subscriber containers failed on a driver (rung $rate $posture)"; done
	sleep 5
	pids=()
	for ((di = 0; di < D; di++)); do lane_b_batch "$di" "${pubs[di]}" & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || die "starting publisher containers failed on a driver (rung $rate $posture)"; done
	# CPU is sampled over exactly the settle + measure window, now that the
	# publishers exist — started any earlier it measures the container ramp.
	start_cpu_sampling "$rdir/cpu" $((LANE_B_SETTLE + LANE_B_SECS))
	# Let the publisher ramp finish BEFORE the latency measurement starts. The
	# subscriber histograms are CUMULATIVE over the container's life, so a
	# single end-of-rung scrape bakes every message delivered while the
	# publishers were still connecting into the published percentiles — a fine
	# median with a heavy tail, for reasons that have nothing to do with the
	# broker's steady state. Baseline here, scrape again at the end, and the
	# summarizer reports the DIFFERENCE: the same steady-window discipline the
	# throughput numbers already use.
	sleep "$LANE_B_SETTLE"
	pids=()
	for ((di = 0; di < D; di++)); do lane_b_batch "$di" "${scrape[di]}" >"$rdir/.batch/base-$di" 2>/dev/null & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	for ((di = 0; di < D; di++)); do lane_b_split "$rdir" "-base.prom" "$rdir/.batch/base-$di"; done
	sleep "$LANE_B_SECS"
	# scrape each subscriber's histogram BEFORE stopping anything
	pids=()
	for ((di = 0; di < D; di++)); do lane_b_batch "$di" "${scrape[di]}" >"$rdir/.batch/final-$di" 2>/dev/null & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	for ((di = 0; di < D; di++)); do lane_b_split "$rdir" ".prom" "$rdir/.batch/final-$di"; done
	# logs off every container, then one docker rm per driver
	pids=()
	for ((di = 0; di < D; di++)); do lane_b_batch "$di" "${stop[di]}docker rm -f${names[di]} >/dev/null 2>&1" >"$rdir/.batch/stop-$di" 2>/dev/null & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	for ((di = 0; di < D; di++)); do lane_b_split "$rdir" ".log" "$rdir/.batch/stop-$di"; done
	rm -rf "$rdir/.batch"
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
