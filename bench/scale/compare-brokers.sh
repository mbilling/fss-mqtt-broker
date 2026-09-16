#!/usr/bin/env bash
# compare-brokers.sh <run-dir> <inventory.json>
#
# Single-node broker comparison on cloud hardware (ADR 0048 T4). One broker at a
# time on ONE provisioned broker host, driven by emqtt-bench from separate driver
# hosts, laddered until the knee.
#
# WHY ONE HOST FOR EVERY BROKER. bench/docker-compose.yml already runs one broker
# at a time so they never contend; what it cannot do is separate the driver from
# the broker, which is why bench/README.md calls its own numbers dev-grade. This
# lane fixes that — and reuses the SAME provisioning for every broker, because
# this rig has measured two provisionings of nominally identical hardware 40%
# apart (ADR 0077 T4). Across a comparison that spread would be indistinguishable
# from a broker difference, so hardware is held fixed and the brokers move.
#
# WHAT KEEPS THE SEQUENCE HONEST.
#   * the broker host is REBOOTED between arms — page cache, socket state and any
#     kernel drift from the previous broker's overload rungs go with it;
#   * the first broker is repeated as a CONTROL arm at the end; if the closing
#     arm does not match its opening arm, the sequence measured drift, not
#     brokers, and summarize-compare.py says so instead of publishing a table;
#   * COMPARE_SEQUENCE=reverse runs the order backwards, so a repeat cancels any
#     remaining order effect.
#
# MEASUREMENT IS DRIVER-SIDE ONLY. Offered/sent/received come from emqtt-bench's
# counters and the p99 from its own histogram of timestamped payloads; the broker
# host contributes only mpstat and `docker stats`. No broker's internal counters
# are read — mqttd exports plenty and reading them here would be home-field
# advantage, which ADR 0048 §3 exists to refuse.
#
# --shape validates knobs and the container budget offline and exits; run.sh's
# preflight uses it before any cloud call.
set -euo pipefail
. "$(dirname "$0")/lib.sh"
. "$SCALE_DIR/cpu.sh"

# ── the brokers, pinned by digest ────────────────────────────────────────────
# Digests, not tags: a tag is a moving target and a comparison republished six
# months later against a moved tag is not the comparison that was reviewed.
# Configs live beside this script in compare/ and are each broker's documented
# reasonable minimum for this posture — anonymous, plaintext, sessions in memory,
# quiet logs. mqttd runs the SAME published image users pull, with its durable
# default explicitly off (ADR 0048 §4: never buy "fast" by silently dropping a
# guarantee). EMQX 5.8.6 is the last Apache-2.0 line; HiveMQ is the CE image.
broker_image() { # broker_image <broker>
	case "$1" in
	mqttd) echo "ghcr.io/mbilling/fss-mqtt-broker@sha256:db226efa1c18af3f3937b477c2398f231ed0bae458806392c5e96601c6146ab5" ;;
	mosquitto) echo "eclipse-mosquitto@sha256:21421af7b32bf9ce508e9090c8eb13bb81f410ca778dc205506180a6f862d0eb" ;;
	emqx) echo "emqx/emqx@sha256:a1e3d10fa1dcc8c94325b815a93b5b5974253a97cb36470b44e47d5d3ef9cd13" ;;
	hivemq) echo "hivemq/hivemq-ce@sha256:5f440cd2e286a3810001939767e3d91bd056a5687611344e929b5198090567d5" ;;
	*) return 1 ;;
	esac
}
broker_version() { # broker_version <broker> — what the digest above is
	case "$1" in
	mqttd) echo "1.0.17" ;;
	mosquitto) echo "2.0.20" ;;
	emqx) echo "5.8.6" ;;
	hivemq) echo "ce-2024.3" ;;
	esac
}

RUN="${1:?usage: compare-brokers.sh <run-dir> <inventory.json>}"
INVENTORY="${2:?inventory.json}"
case "$RUN" in /*) ;; *) RUN="$SCALE_DIR/$RUN" ;; esac
case "$INVENTORY" in /*) ;; *) INVENTORY="$PWD/$INVENTORY" ;; esac

COMPARE_BROKERS="${COMPARE_BROKERS:-mqttd mosquitto emqx hivemq}"
# The ladder. Every rate must divide the per-container rate exactly, or a rung
# would offer something other than its own label — the failure mode lane B's
# shape check exists for.
COMPARE_RATES="${COMPARE_RATES:-30000 60000 90000 120000 150000 180000 210000 240000}"
COMPARE_PUBS_PER_CONTAINER="${COMPARE_PUBS_PER_CONTAINER:-600}"
COMPARE_RATE_PER_PUB="${COMPARE_RATE_PER_PUB:-25}" # whole-ms timer: -I 40
COMPARE_PAYLOAD="${COMPARE_PAYLOAD:-200}"
COMPARE_QOS="${COMPARE_QOS:-0}"
COMPARE_SECS="${COMPARE_SECS:-60}"
COMPARE_SETTLE="${COMPARE_SETTLE:-20}"
COMPARE_SETTLE_BUDGET="${COMPARE_SETTLE_BUDGET:-180}"
COMPARE_DRAIN_SECS="${COMPARE_DRAIN_SECS:-60}"
COMPARE_DRAIN_POLL="${COMPARE_DRAIN_POLL:-5}"
COMPARE_FLAT_POLLS="${COMPARE_FLAT_POLLS:-3}"
COMPARE_CONNECT_RATE="${COMPARE_CONNECT_RATE:-500}"
COMPARE_SEQUENCE="${COMPARE_SEQUENCE:-forward}"
COMPARE_CONTROL="${COMPARE_CONTROL:-1}"
COMPARE_REBOOT_BETWEEN_ARMS="${COMPARE_REBOOT_BETWEEN_ARMS:-1}"
BENCH_IMG="${BENCH_IMG:-emqx/emqtt-bench:0.6.3}"

PER_CONTAINER_RATE=$((COMPARE_PUBS_PER_CONTAINER * COMPARE_RATE_PER_PUB))
PUB_INTERVAL_MS=$((1000 / COMPARE_RATE_PER_PUB))

positive_int() { case "$2" in '' | *[!0-9]*) die "$1 must be a positive integer (got '$2')" ;; 0) die "$1 must be > 0" ;; esac; }
positive_int COMPARE_PUBS_PER_CONTAINER "$COMPARE_PUBS_PER_CONTAINER"
positive_int COMPARE_RATE_PER_PUB "$COMPARE_RATE_PER_PUB"
positive_int COMPARE_SECS "$COMPARE_SECS"
[ $((1000 % COMPARE_RATE_PER_PUB)) -eq 0 ] ||
	die "COMPARE_RATE_PER_PUB=$COMPARE_RATE_PER_PUB needs a whole-millisecond timer (1000/rate); emqtt-bench -I floors, so the rung would offer more than its label"
for b in $COMPARE_BROKERS; do broker_image "$b" >/dev/null || die "unknown broker '$b' (known: mqttd mosquitto emqx hivemq)"; done

D=$(driver_count)
N=$(broker_count)
[ "$N" = 1 ] || die "compare-brokers.sh measures ONE broker host; this inventory has $N (use run.sh compare, which applies node_count=1)"
DRIVER_VCPUS=$(inv '[.drivers[] | (.vcpus // ({"ccx13":2,"ccx23":4,"ccx33":8,"ccx43":16,"ccx53":32,"cpx32":4,"cpx42":8,"cpx41":8,"cpx51":16}[.server_type // ""]) // 8)] | min')
OUT="$RUN/results/compare"

# ── the shape, refused offline ───────────────────────────────────────────────
# Each container is pinned to one vCPU, and a rung runs a publisher container AND
# a subscriber container per slice of the rate (1:1 topics: every publisher owns
# `bench/<i>` and exactly one subscriber holds it). Crossing the budget does not
# fail loudly at run time — it quietly under-offers, and the broker gets the
# blame. Same rule, same arithmetic, as lane E.
shape() {
	local rate containers per_driver verdict=0
	{
		echo "brokers under test: $COMPARE_BROKERS (sequence: $COMPARE_SEQUENCE, control: $COMPARE_CONTROL)"
		for b in $COMPARE_BROKERS; do printf '  %-10s %s (%s)\n' "$b" "$(broker_version "$b")" "$(broker_image "$b")"; done
		echo "broker host: 1 x $(inv '.brokers[0].server_type // "?"'), drivers: $D x $(inv '.drivers[0].server_type // "?"') ($DRIVER_VCPUS vCPU budget each)"
		echo "per container: $COMPARE_PUBS_PER_CONTAINER publishers x $COMPARE_RATE_PER_PUB msg/s x ${COMPARE_PAYLOAD}B qos $COMPARE_QOS = $PER_CONTAINER_RATE msg/s (-I ${PUB_INTERVAL_MS}ms)"
		echo "1:1 topics: bench/<i>, one subscriber per publisher; measurement is driver-side only"
		echo "window ${COMPARE_SECS}s after a ${COMPARE_SETTLE}s settle; drain budget ${COMPARE_DRAIN_SECS}s"
		printf '\n%8s | %10s | %10s | %10s | %s\n' "offered/s" "containers" "per driver" "clients" "verdict"
		for rate in $COMPARE_RATES; do
			[ $((rate % PER_CONTAINER_RATE)) -eq 0 ] ||
				die "COMPARE_RATES entry $rate is not a multiple of $PER_CONTAINER_RATE msg/s per container — the rung would offer something other than its label"
			containers=$((2 * rate / PER_CONTAINER_RATE))
			per_driver=$(((containers + D - 1) / D))
			if [ "$per_driver" -gt "$DRIVER_VCPUS" ]; then
				verdict=1
				printf '%8s | %10s | %10s | %10s | REFUSED: %s containers on the busiest driver > %s\n' \
					"$rate" "$containers" "$per_driver" "$((2 * rate / COMPARE_RATE_PER_PUB))" "$per_driver" "$DRIVER_VCPUS"
			else
				printf '%8s | %10s | %10s | %10s | ok\n' "$rate" "$containers" "$per_driver" "$((2 * rate / COMPARE_RATE_PER_PUB))"
			fi
		done
	} >"${SHAPE_OUT:-/dev/stdout}"
	[ "$verdict" -eq 0 ] ||
		die "compare: a rung needs more containers per driver than $DRIVER_VCPUS (one vCPU each). Raise DRIVER_COUNT, use a bigger DRIVER_TYPE, or shorten COMPARE_RATES"
}

if [ "${1:-}" = --shape ] || [ "${COMPARE_SHAPE_ONLY:-0}" = 1 ]; then
	shape
	say "COMPARE_SHAPE_ONLY — knobs and container budget verified; touching no host"
	exit 0
fi

mkdir -p "$OUT"
SHAPE_OUT="$OUT/shape.txt" shape
sed 's/^/    /' "$OUT/shape.txt" >&2

BROKER_IP=$(broker_pub_ip 0)
BROKER_PRIV=$(broker_priv_ip 0)

# ── driver-side plumbing (same pattern as lane E) ────────────────────────────
DOCKER_RUN="docker run -d --network host --ulimit nofile=1048576:1048576"
driver_batch() { rssh "$(driver_pub_ip "$1")" "bash -s" <<<"$2"; }
batch_split() { awk -v dir="$1" -v sfx="$2" '/^@@@ /{if(f)close(f); f=dir "/" $2 sfx; printf "" > f; next} f{print > f}' "$3"; }

# The host's own mqttd, installed by cloud-init, would compete for the port and
# the CPU with every arm — including its own, which runs as a container so all
# four brokers share one runtime.
prepare_host() {
	# Fail here, not three minutes into the first arm. The broker host only has a
	# container runtime when it was provisioned for this lane (broker_docker,
	# which `run.sh compare` sets); a measurement host deliberately has nothing
	# but the shipped binary, and the first arm's `docker run` would otherwise die
	# as "command not found" with a fleet already billing (2026-09-16).
	rssh "$BROKER_IP" "command -v docker >/dev/null" ||
		die "the broker host has no docker — provision this fleet with run.sh compare (it sets broker_docker=true); a plain measurement host cannot run the comparison arms"
	rssh "$BROKER_IP" "systemctl disable --now mqttd >/dev/null 2>&1 || true; docker rm -f compare-broker >/dev/null 2>&1 || true"
}

start_broker() { # start_broker <broker> <arm-dir>
	local broker="$1" dir="$2" image extra=()
	image=$(broker_image "$broker")
	case "$broker" in
	mqttd) rscp "$SCALE_DIR/compare/mqttd.env" "root@$BROKER_IP:/opt/compare.env"; extra=(--env-file /opt/compare.env) ;;
	emqx) rscp "$SCALE_DIR/compare/emqx.env" "root@$BROKER_IP:/opt/compare.env"; extra=(--env-file /opt/compare.env) ;;
	mosquitto)
		rscp "$SCALE_DIR/compare/mosquitto.conf" "root@$BROKER_IP:/opt/mosquitto.conf"
		extra=(-v /opt/mosquitto.conf:/mosquitto/config/mosquitto.conf:ro)
		;;
	hivemq)
		# The JVM sizes its heap as a fraction of host RAM, so an implicit heap
		# would hand HiveMQ a different share of a 4 vCPU host than of a 32 vCPU
		# one and the vertical-scaling arm would measure that, not the broker.
		# Half of host RAM, stated in the record.
		local half
		half=$(rssh "$BROKER_IP" "awk '/MemTotal/{printf \"%d\", \$2/1024/2}' /proc/meminfo")
		rscp "$SCALE_DIR/compare/hivemq.env" "root@$BROKER_IP:/opt/compare.env"
		extra=(--env-file /opt/compare.env -e "HIVEMQ_HEAPSIZE=${half}m")
		;;
	esac
	rssh "$BROKER_IP" "docker pull -q $image >/dev/null && $DOCKER_RUN --name compare-broker ${extra[*]} $image" >/dev/null
	wait_for "$broker accepting MQTT on $BROKER_PRIV:1883" 180 \
		rssh "$(driver_pub_ip 0)" "timeout 2 bash -c '</dev/tcp/$BROKER_PRIV/1883'"
	{
		echo "broker=$broker"
		echo "version=$(broker_version "$broker")"
		echo "image=$image"
		# The digest the host actually resolved, read back off the host rather
		# than echoed from the pin above: a table that names a digest must name
		# the bytes that ran, not the bytes that were asked for.
		echo "digest=$(rssh "$BROKER_IP" "docker image inspect --format '{{index .RepoDigests 0}}' $image" 2>/dev/null || echo unknown)"
		echo "config_sha256=$(sha256sum "$SCALE_DIR/compare/$broker".* 2>/dev/null | awk '{print $1}' | head -1)"
		echo "started_unix=$(date +%s)"
	} >"$dir/broker.txt"
	{
		# The instance type is the single most important fact about the host and
		# `lscpu` does not carry it; take it from the inventory the fleet was
		# provisioned from.
		echo "server_type=$(inv '.brokers[0].server_type // "unknown"')"
		echo "drivers=$D x $(inv '.drivers[0].server_type // "unknown"')"
		rssh "$BROKER_IP" "uname -a; lscpu | head -15; free -h; docker --version" 2>/dev/null || true
	} >"$dir/host.txt"
}

stop_broker() { # stop_broker <arm-dir>
	rssh "$BROKER_IP" "docker logs compare-broker 2>&1 | tail -200" >"$1/broker-container.log" 2>&1 || true
	rssh "$BROKER_IP" "docker rm -f compare-broker >/dev/null 2>&1; docker volume prune -f >/dev/null 2>&1" || true
	echo "stopped_unix=$(date +%s)" >>"$1/broker.txt"
}

# A reboot is the cheapest way to give every arm the same kernel: page cache,
# socket tables and any sysctl drift from the last arm's overload rungs go with
# it. ~60s against arms that cost 20 minutes each.
reboot_broker_host() {
	[ "$COMPARE_REBOOT_BETWEEN_ARMS" = 1 ] || return 0
	local old
	old=$(rssh "$BROKER_IP" "cat /proc/sys/kernel/random/boot_id" 2>/dev/null || echo unknown)
	rssh "$BROKER_IP" "reboot" >/dev/null 2>&1 || true
	wait_for "broker host reboot (new boot id)" 300 bash -c "
		now=\$(ssh ${SSH_OPTS[*]} -o UserKnownHostsFile='$RUN/known_hosts' root@$BROKER_IP 'cat /proc/sys/kernel/random/boot_id' 2>/dev/null) || exit 1
		[ -n \"\$now\" ] && [ \"\$now\" != '$old' ]"
	wait_for "docker back after reboot" 180 rssh "$BROKER_IP" "docker info >/dev/null 2>&1"
	prepare_host
}

# ── one rung ─────────────────────────────────────────────────────────────────
rung() { # rung <broker> <arm-dir> <offered>
	local broker="$1" adir="$2" rate="$3"
	local rdir="$adir/rung-$rate" containers=$((rate / PER_CONTAINER_RATE))
	mkdir -p "$rdir/.batch" "$rdir/cpu"
	local -a subs pubs scrape stop subnames pubnames subdump
	local di c seq_base
	for ((di = 0; di < D; di++)); do subs[di]="set -e"$'\n'; pubs[di]="set -e"$'\n'; scrape[di]=""; stop[di]=""; subnames[di]=""; pubnames[di]=""; subdump[di]=""; done
	for ((c = 0; c < containers; c++)); do
		di=$((c % D))
		seq_base=$((c * COMPARE_PUBS_PER_CONTAINER))
		# 1:1 topics. `-n` offsets the topic index per container so the whole rung
		# covers bench/0..bench/<clients-1> exactly once on each side, whatever the
		# container count is — the same reason lane B offsets its populations.
		subs[di]+="$DOCKER_RUN --name sub-$c $BENCH_IMG sub -h $BROKER_PRIV -p 1883 -c $COMPARE_PUBS_PER_CONTAINER -R $COMPARE_CONNECT_RATE -t 'bench/%i' -n $seq_base -q $COMPARE_QOS --payload-hdrs ts --prometheus --restapi $((9400 + c / D)) >/dev/null"$'\n'
		pubs[di]+="$DOCKER_RUN --name pub-$c $BENCH_IMG pub -h $BROKER_PRIV -p 1883 -c $COMPARE_PUBS_PER_CONTAINER -R $COMPARE_CONNECT_RATE -t 'bench/%i' -n $seq_base -q $COMPARE_QOS -s $COMPARE_PAYLOAD -I $PUB_INTERVAL_MS --payload-hdrs ts >/dev/null"$'\n'
		scrape[di]+="printf '\\n@@@ sub-$c\\n'; curl -s http://localhost:$((9400 + c / D))/metrics"$'\n'
		stop[di]+="printf '\\n@@@ pub-$c\\n'; docker logs pub-$c 2>&1"$'\n'
		subdump[di]+="printf '\\n@@@ sub-$c\\n'; docker logs sub-$c 2>&1"$'\n'
		subnames[di]+=" sub-$c"
		pubnames[di]+=" pub-$c"
	done
	local -a pids=()
	for ((di = 0; di < D; di++)); do driver_batch "$di" "${subs[di]}" & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || die "compare: starting subscriber containers failed ($broker, $rate msg/s)"; done
	sleep 5
	pids=()
	for ((di = 0; di < D; di++)); do driver_batch "$di" "${pubs[di]}" & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || die "compare: starting publisher containers failed ($broker, $rate msg/s)"; done
	sleep "$COMPARE_SETTLE"

	# The population must have ARRIVED before the window opens, or the rung
	# measures a broker still accepting connections. Broker-side connection
	# counts are off limits here (they are exactly the home-field metric this
	# lane refuses), so the drivers answer it themselves: emqtt-bench's own
	# `connect_succ` across every container of the rung, polled until it reaches
	# the population or the budget runs out. A rung that opens its window early
	# says so in rung.txt rather than being quietly comparable to one that did not.
	local expect=$((2 * rate / COMPARE_RATE_PER_PUB)) settled=no settled_conns=0 waited=0
	while :; do
		pids=()
		for ((di = 0; di < D; di++)); do driver_batch "$di" "${scrape[di]}" >"$rdir/.batch/settle-$di" 2>/dev/null & pids+=($!); done
		for p in "${pids[@]}"; do wait "$p" || true; done
		settled_conns=$(cat "$rdir"/.batch/settle-* 2>/dev/null | awk '/^connect_succ /{s += $2} END{print s + 0}')
		# Publisher containers expose no REST endpoint, so the scrape sees the
		# subscriber half; half the population is the whole of what it can see.
		if [ "$settled_conns" -ge $((expect / 2)) ]; then settled=yes; break; fi
		[ "$waited" -lt "$COMPARE_SETTLE_BUDGET" ] || {
			warn "compare: $broker at $rate msg/s opened its window with $settled_conns/$((expect / 2)) subscriber connections after ${waited}s — rung flagged UNSETTLED"
			break
		}
		sleep "$COMPARE_DRAIN_POLL"
		waited=$((waited + COMPARE_DRAIN_POLL))
	done

	# The window: baseline the histograms, hold, scrape again. Same shape as lane
	# E's aligned window — a single end-of-rung scrape would bake the connect ramp
	# into the published tail.
	window() {
		pids=()
		for ((di = 0; di < D; di++)); do driver_batch "$di" "${scrape[di]}" >"$rdir/.batch/base-$di" 2>/dev/null & pids+=($!); done
		for p in "${pids[@]}"; do wait "$p" || true; done
		for ((di = 0; di < D; di++)); do batch_split "$rdir" "-base.prom" "$rdir/.batch/base-$di"; done
		sleep "$COMPARE_SECS"
		pids=()
		for ((di = 0; di < D; di++)); do driver_batch "$di" "${scrape[di]}" >"$rdir/.batch/final-$di" 2>/dev/null & pids+=($!); done
		for p in "${pids[@]}"; do wait "$p" || true; done
		for ((di = 0; di < D; di++)); do batch_split "$rdir" ".prom" "$rdir/.batch/final-$di"; done
	}
	local cpu_window=aligned
	if ! with_cpu_sampling "$rdir/cpu" window; then
		local rc=$?
		[ "$rc" -lt 128 ] || exit "$rc"
		cpu_window=missing
		warn "compare: CPU samplers failed for $broker at $rate msg/s — the rung still measures its window"
		window
	fi
	rssh "$BROKER_IP" "docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' compare-broker" >"$rdir/mem-broker.txt" 2>/dev/null || true

	pids=()
	for ((di = 0; di < D; di++)); do driver_batch "$di" "${stop[di]}" >"$rdir/.batch/stop-$di" 2>/dev/null & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	for ((di = 0; di < D; di++)); do batch_split "$rdir" ".log" "$rdir/.batch/stop-$di"; done

	# Publishers stop; consumers stay up to take whatever the broker still owes,
	# so a shortfall reads as loss instead of "we stopped watching too early".
	local drained=no elapsed=0 flat=0 prev=-1 cur t0
	pids=()
	for ((di = 0; di < D; di++)); do [ -n "${pubnames[di]}" ] && driver_batch "$di" "docker rm -f${pubnames[di]} >/dev/null 2>&1" >/dev/null 2>&1 & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	t0=$(date +%s)
	while :; do
		sleep "$COMPARE_DRAIN_POLL"
		elapsed=$(($(date +%s) - t0))
		pids=()
		for ((di = 0; di < D; di++)); do driver_batch "$di" "${scrape[di]}" >"$rdir/.batch/poll-$di" 2>/dev/null & pids+=($!); done
		for p in "${pids[@]}"; do wait "$p" || true; done
		cur=$(cat "$rdir"/.batch/poll-* 2>/dev/null | awk '/^recv /{s += $2} END{print s + 0}')
		if [ "$cur" -gt 0 ] && [ "$cur" -le "$prev" ]; then
			flat=$((flat + 1))
			[ "$flat" -lt "$COMPARE_FLAT_POLLS" ] || { drained=yes; break; }
		else flat=0; fi
		prev=$cur
		[ "$elapsed" -lt "$COMPARE_DRAIN_SECS" ] || { warn "compare: drain budget elapsed with the backlog moving ($broker, $rate) — rung reports UNRESOLVED"; break; }
	done
	pids=()
	for ((di = 0; di < D; di++)); do driver_batch "$di" "${subdump[di]}" >"$rdir/.batch/drain-$di" 2>/dev/null & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	for ((di = 0; di < D; di++)); do batch_split "$rdir" ".drain" "$rdir/.batch/drain-$di"; done
	pids=()
	for ((di = 0; di < D; di++)); do [ -n "${subnames[di]}" ] && driver_batch "$di" "docker rm -f${subnames[di]} >/dev/null 2>&1" >/dev/null 2>&1 & pids+=($!); done
	for p in "${pids[@]}"; do wait "$p" || true; done
	rm -rf "$rdir/.batch"
	echo "broker=$broker offered=$rate publishers=$((rate / COMPARE_RATE_PER_PUB)) subscribers=$((rate / COMPARE_RATE_PER_PUB)) payload=$COMPARE_PAYLOAD qos=$COMPARE_QOS window_secs=$COMPARE_SECS settle_s=$((COMPARE_SETTLE + waited)) settled=$settled settled_conns=$settled_conns expected_conns=$((expect / 2)) drained=$drained drain_secs=$elapsed containers=$((2 * containers)) cpu_window=$cpu_window" >"$rdir/rung.txt"
	say "  $broker: $rate msg/s offered — window done (drained=$drained)"
}

# ── the sequence ─────────────────────────────────────────────────────────────
order() {
	case "$COMPARE_SEQUENCE" in
	forward) echo "$COMPARE_BROKERS" ;;
	reverse) tr ' ' '\n' <<<"$COMPARE_BROKERS" | tac | tr '\n' ' ' ;;
	*) echo "$COMPARE_SEQUENCE" ;;
	esac
}
SEQ=$(order)
FIRST=$(awk '{print $1}' <<<"$SEQ")
[ "$COMPARE_CONTROL" = 1 ] && SEQ="$SEQ $FIRST"
echo "sequence=$SEQ" >"$OUT/sequence.txt"

prepare_host
idx=0
for broker in $SEQ; do
	idx=$((idx + 1))
	control=no
	[ "$COMPARE_CONTROL" = 1 ] && [ "$idx" -eq "$(wc -w <<<"$SEQ")" ] && control=yes
	adir="$OUT/$idx-$broker"
	[ "$control" = yes ] && adir="$OUT/$idx-$broker-control"
	mkdir -p "$adir"
	say "════ arm $idx: $broker $(broker_version "$broker")${control:+ (control: $control)} ════"
	start_broker "$broker" "$adir"
	echo "arm=$idx control=$control" >>"$adir/broker.txt"
	for rate in $COMPARE_RATES; do rung "$broker" "$adir" "$rate"; done
	stop_broker "$adir"
	[ "$idx" -lt "$(wc -w <<<"$SEQ")" ] && reboot_broker_host
done
{
	echo "harness_rev=$(git -C "$SCALE_DIR" rev-parse HEAD 2>/dev/null || echo unknown)"
	echo "harness_dirty=$(git -C "$SCALE_DIR" diff --quiet HEAD 2>/dev/null && echo no || echo YES)"
	echo "driver_image=$BENCH_IMG"
	echo "rates=$COMPARE_RATES"
	echo "sequence=$SEQ"
	echo "run_stamp=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
} >"$OUT/provenance.txt"
cp "$INVENTORY" "$OUT/inventory.json"
say "comparison complete -> $OUT"
