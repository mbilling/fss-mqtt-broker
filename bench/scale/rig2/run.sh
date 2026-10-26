#!/usr/bin/env bash
# The ADR 0090 rig: a msg/s ramp per workload, judged on the broker only (T3), on the
# local, 3-node and 10-node tiers (T5). Read bench/scale/rig2/README.md first.
#
#   TIER=local ./run.sh                                   # free: 3 local nodes, a short ramp
#   PRICE_ONLY=1 TIER=n3 WORKLOADS="durable-qos1 clean-qos1 qos0" ./run.sh   # the quote, no cloud call
#   HCLOUD_TOKEN=... MQTTD_VERSION=1.3.0 TIER=n3 ./run.sh                 # PAID
#   HCLOUD_TOKEN=... MQTTD_URL=... MQTTD_SHA256=... TIER=n10 WORKLOADS="durable-qos1" ./run.sh
#
# One provisioning carries every workload in WORKLOADS, one ladder after the other, on
# the same cluster. A ladder is a sequence of steps; a step is ONE mqtt-load process per
# generator host (its own ledger), all started together, the total msg/s split evenly.
# Every step scrapes every broker's /metrics on the broker's own host for the whole
# step, and verdict.py judges it from those counters and the generators' summaries.
#
# Teardown (bench/scale/teardown.sh: tofu destroy, then the label audit) is trapped on
# EXIT/INT/TERM. KEEP_INFRA=1 skips it and says so. Hetzner only (CLOUD=hcloud).

set -euo pipefail
RIG2="$(cd "$(dirname "$0")" && pwd)"
. "$RIG2/../lib.sh"
require_modern_bash
. "$SCALE_DIR/cloud.sh"

TIER="${TIER:-local}"
WORKLOADS="${WORKLOADS:-durable-qos1}"
ARM="${ARM:-A}"
[ -z "${MQTTD_URL_B:-}" ] || die "MQTTD_URL_B is set: the same-hosts A/B (ADR 0090 D5) is task 0090-T4 and not implemented yet"

# ── the tier's shape (ADR 0090 D7), every count overridable ─────────────────────────
case "$TIER" in
local)
	BROKERS="${BROKERS:-3}" GENERATORS=1
	DEF_WARMUP=5 DEF_MEASURE=10 DEF_WINDOW=5 DEF_SCRAPE=1 DEF_MARGIN=0.5 DEF_MAX_STEPS=3
	;;
n3)
	BROKERS="${BROKERS:-3}" GENERATORS="${GENERATORS:-1}"
	DEF_WARMUP=20 DEF_MEASURE=60 DEF_WINDOW=10 DEF_SCRAPE=2 DEF_MARGIN=1 DEF_MAX_STEPS=14
	;;
n10)
	BROKERS="${BROKERS:-10}" GENERATORS="${GENERATORS:-3}"
	DEF_WARMUP=20 DEF_MEASURE=60 DEF_WINDOW=10 DEF_SCRAPE=2 DEF_MARGIN=1 DEF_MAX_STEPS=14
	;;
*) die "TIER must be local, n3 or n10, not '$TIER'" ;;
esac
BROKER_TYPE="${BROKER_TYPE:-ccx23}"
GEN_TYPE="${GEN_TYPE:-ccx33}"

# ── the ramp (ADR 0090 D4) ──────────────────────────────────────────────────────────
# A step: WARMUP s, then MEASURE s measured in WINDOW s windows, then the drain.
WARMUP="${WARMUP:-$DEF_WARMUP}"
MEASURE="${MEASURE:-$DEF_MEASURE}"
WINDOW="${WINDOW:-$DEF_WINDOW}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-30}"
# Coarse steps multiply by FACTOR while steps are CARRIED (lossless, whatever their
# latency); after the first FAILED / OFFER NOT TAKEN / DISCONNECTED step, bisection until
# the gap between the last carried and the first bad step is <= GAP_PCT %.
FACTOR="${FACTOR:-1.5}"
GAP_PCT="${GAP_PCT:-3}"
GRANULARITY="${GRANULARITY:-100}"
MIN_RATE="${MIN_RATE:-$GRANULARITY}"
MAX_RATE="${MAX_RATE:-2000000}"
MAX_STEPS="${MAX_STEPS:-$DEF_MAX_STEPS}"
# A VOID step (the harness's fault) is rerun this many times before the ladder stops.
VOID_RETRIES="${VOID_RETRIES:-1}"
# A FAILED step (loss) ends the ladder; BISECT_ON_FAIL=1 keeps bisecting below it.
BISECT_ON_FAIL="${BISECT_ON_FAIL:-0}"
# Broker scrapes: every SCRAPE_INTERVAL s on the broker's host; the window edges shrink
# by MARGIN_S each side before the first/last scrape inside them is taken.
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-$DEF_SCRAPE}"
MARGIN_S="${MARGIN_S:-$DEF_MARGIN}"
# Between steps: every broker's connections and sessions <= IDLE_FLOOR and no pending
# publish, within RESET_BUDGET s, or the next step is VOID (unreset).
IDLE_FLOOR="${IDLE_FLOOR:-0}"
RESET_BUDGET="${RESET_BUDGET:-120}"
# For the price quote: steps a ladder is expected to take (coarse climb + bisection).
EST_STEPS="${EST_STEPS:-9}"

for w in $WORKLOADS; do
	[ -f "$RIG2/workloads/$w.env" ] || die "no workload $w (have: $(cd "$RIG2/workloads" && ls ./*.env | sed 's|./||; s|\.env||' | tr '\n' ' '))"
done
NWORK=$(wc -w <<<"$WORKLOADS" | tr -d ' ')
STEP_SECS=$((WARMUP + MEASURE + 45))

# ── the quote, before anything is provisioned ───────────────────────────────────────
if [ "$TIER" != local ]; then
	say "$(python3 "$RIG2/verdict.py" cost --tier "$TIER" --brokers "$BROKERS" --generators "$GENERATORS" \
		--broker-type "$BROKER_TYPE" --gen-type "$GEN_TYPE" --workloads "$NWORK" --steps "$EST_STEPS" \
		--step-secs "$STEP_SECS")"
	[ "${PRICE_ONLY:-0}" != 1 ] || exit 0
fi

STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN="${RUN_DIR:-$RIG2/.runs/$TIER-$STAMP}"
case "$RUN" in /*) ;; *) RUN="$RIG2/$RUN" ;; esac
mkdir -p "$RUN"
export RUN
say "run dir: $RUN"

# ── host plumbing: the same verbs for local processes and cloud hosts ───────────────
# on_gen <g> <cmd> / on_broker <i> <cmd>: run <cmd> on that host with $H = its rig dir.
# get_gen / get_broker <i> <remote-rel> <local>: copy a file or directory back.
if [ "$TIER" = local ]; then
	host_dir() { echo "$RUN/hosts/$1"; }
	on_gen() { H="$(host_dir "gen-$1")" bash -c "$2"; }
	on_broker() { H="$(host_dir "broker-$1")" bash -c "$2"; }
	get_gen() { cp -R "$(host_dir "gen-$1")/$2" "$3"; }
	get_broker() { cp -R "$(host_dir "broker-$1")/$2" "$3"; }
	broker_metrics_url() { echo "http://$(sed -n "$(($1 + 1))p" "$RUN/local/brokers.tsv" | cut -f3)/metrics"; }
else
	REMOTE_H=/root/rig2
	on_gen() { rssh "$(driver_pub_ip "$1")" "H=$REMOTE_H; $2"; }
	on_broker() { rssh "$(broker_pub_ip "$1")" "H=$REMOTE_H; $2"; }
	get_gen() { rscp -r "root@$(driver_pub_ip "$1"):$REMOTE_H/$2" "$3"; }
	get_broker() { rscp -r "root@$(broker_pub_ip "$1"):$REMOTE_H/$2" "$3"; }
	broker_metrics_url() { echo "http://localhost:8080/metrics"; }
fi

# ── bring-up ────────────────────────────────────────────────────────────────────────
BINARY_DESC=""
if [ "$TIER" = local ]; then
	MQTTD_BIN="${MQTTD_BIN:-$REPO_ROOT/target/release/mqttd}"
	LOAD_BIN="${MQTT_LOAD_BIN:-$REPO_ROOT/target/release/mqtt-load}"
	if [ -z "${SKIP_BUILD:-}" ] && { [ ! -x "$MQTTD_BIN" ] || [ ! -x "$LOAD_BIN" ]; }; then
		say "building mqttd and mqtt-load (release)"
		(cd "$REPO_ROOT" && cargo build --release -p mqttd -p mqtt-load)
	fi
	[ -x "$MQTTD_BIN" ] && [ -x "$LOAD_BIN" ] || die "need $MQTTD_BIN and $LOAD_BIN"
	BINARY_DESC="local build $MQTTD_BIN ($("$MQTTD_BIN" --version 2>/dev/null | head -1), git $(git -C "$REPO_ROOT" rev-parse --short HEAD))"
	stop_local() {
		local rc=$?
		trap - EXIT INT TERM
		MQTTD_BIN="$MQTTD_BIN" "$RIG2/local-cluster.sh" stop "$RUN/local" || true
		exit "$rc"
	}
	trap stop_local EXIT INT TERM
	MQTTD_BIN="$MQTTD_BIN" "$RIG2/local-cluster.sh" start "$RUN/local" "$BROKERS"
	ENDPOINTS=$(cut -f2 "$RUN/local/brokers.tsv" | paste -sd, -)
	for i in $(seq 0 $((BROKERS - 1))); do mkdir -p "$(host_dir "broker-$i")"; cp "$RIG2/scrape-loop.py" "$(host_dir "broker-$i")/"; done
	mkdir -p "$(host_dir gen-0)"
	cp "$LOAD_BIN" "$(host_dir gen-0)/mqtt-load"
else
	select_scale_cloud
	[ "$CLOUD" = hcloud ] || die "rig2 runs on CLOUD=hcloud only"
	require_scale_token
	TF=$(command -v tofu) || die "OpenTofu (tofu) is required"
	command -v jq >/dev/null || die "jq not installed"
	if [ -n "${SSH_KEY:-}" ]; then [ -f "${SSH_KEY}.pub" ] || die "${SSH_KEY}.pub does not exist"; fi
	# The binary under test is named every time (bench/scale/run.sh's rule).
	if [ -n "${MQTTD_URL:-}" ]; then
		[ -n "${MQTTD_SHA256:-}" ] || die "MQTTD_URL needs MQTTD_SHA256 (an arbitrary URL publishes no .sha256)"
		MQTTD_VERSION="${MQTTD_VERSION:-candidate}"
		BINARY_DESC="UNRELEASED $MQTTD_URL sha256=$MQTTD_SHA256"
		printf 'UNRELEASED\nurl=%s\nsha256=%s\n' "$MQTTD_URL" "$MQTTD_SHA256" >"$RUN/UNRELEASED-BINARY.txt"
	else
		[ -n "${MQTTD_VERSION:-}" ] || die "set MQTTD_VERSION (a release) or MQTTD_URL + MQTTD_SHA256 (a candidate)"
		BINARY_DESC="release v$MQTTD_VERSION"
	fi
	# A state file that still tracks servers belongs to another run (or a leak): never
	# apply over it, and never let this run's trap destroy it either.
	if [ -f "$SCALE_DIR/terraform/terraform.tfstate" ] && [ "$(jq '[.resources[]? | select(.type == "hcloud_server")] | length' "$SCALE_DIR/terraform/terraform.tfstate")" != 0 ]; then
		die "bench/scale/terraform's state still tracks servers — another run is up or leaked; run bench/scale/teardown.sh first"
	fi
	LOAD_BIN="${MQTT_LOAD_BIN:-}"
	if [ -z "$LOAD_BIN" ]; then
		LOAD_BIN=$("$RIG2/build-mqtt-load.sh" | tail -1)
	fi
	file "$LOAD_BIN" 2>/dev/null | grep -q "x86-64" || die "$LOAD_BIN is not an x86-64 binary (build-mqtt-load.sh builds one)"
	if [ -t 0 ] && [ "${RIG2_YES:-0}" != 1 ]; then
		read -r -p "Provision PAID servers for this run? [y/N] " ok
		[ "$ok" = y ] || [ "$ok" = Y ] || die "not confirmed; nothing provisioned"
	elif [ "${RIG2_YES:-0}" != 1 ]; then
		die "not a terminal: set RIG2_YES=1 to provision without the prompt"
	fi
	cloud_teardown() {
		local rc=$?
		trap - EXIT INT TERM
		if [ "${KEEP_INFRA:-0}" = 1 ]; then
			printf '\033[1;31m!! KEEP_INFRA=1 — PAID SERVERS ARE STILL RUNNING. Destroy with: bench/scale/teardown.sh\033[0m\n' >&2
			exit "$rc"
		fi
		if [ "$rc" -ne 0 ] && [ -f "$RUN/inventory.json" ]; then
			"$SCALE_DIR/collect.sh" "$RUN" "$RUN/inventory.json" >>"$RUN/collect.log" 2>&1 || true
		fi
		[ -f "$RUN/report.md" ] || python3 "$RIG2/verdict.py" report "$RUN" >/dev/null 2>&1 || true
		say "tearing down (trap; exit code was $rc): destroy, then the label audit"
		"$SCALE_DIR/teardown.sh" 2>&1 | tee -a "$RUN/teardown.log" >&2 || {
			warn "teardown or its leak audit FAILED — run bench/scale/teardown.sh (--force) and check 'hcloud server list'"
			exit 1
		}
		exit "$rc"
	}
	trap cloud_teardown EXIT INT TERM
	(cd "$TFDIR" && "$TF" init -input=false >"$RUN/tf-init.log" 2>&1) || { cat "$RUN/tf-init.log" >&2; die "tofu init failed"; }
	TF_APPLY_ARGS=(-var node_count="$BROKERS" -var driver_count="$GENERATORS"
		-var broker_server_type="$BROKER_TYPE" -var driver_server_type="$GEN_TYPE"
		-var run_label="rig2-$STAMP" -var mqttd_version="$MQTTD_VERSION"
		-var build_bench=false -var driver_emqtt_bench=false)
	[ -z "${MQTTD_URL:-}" ] || TF_APPLY_ARGS+=(-var mqttd_url="$MQTTD_URL")
	[ -z "${MQTTD_SHA256:-}" ] || TF_APPLY_ARGS+=(-var mqttd_sha256="$MQTTD_SHA256")
	TF_APPLY_ARGS+=(${TOFU_SSH_PUBKEY_ARGS[@]+"${TOFU_SSH_PUBKEY_ARGS[@]}"})
	declare -p TF_APPLY_ARGS >"$RUN/tf-apply-args.sh"
	say "provisioning $BROKERS x $BROKER_TYPE + $GENERATORS x $GEN_TYPE"
	(cd "$TFDIR" && "$TF" apply -auto-approve -input=false "${TF_APPLY_ARGS[@]}" >"$RUN/tf-apply.log" 2>&1) ||
		{ tail -30 "$RUN/tf-apply.log" >&2; die "tofu apply failed"; }
	INVENTORY="$RUN/inventory.json"
	(cd "$TFDIR" && "$TF" output -json inventory) >"$INVENTORY"
	: >"$RUN/known_hosts"
	await_fleet
	"$SCALE_DIR/bootstrap-cluster.sh" "$RUN" "$INVENTORY" durable
	ENDPOINTS=$(jq -r '[.brokers[].mqtt_plain] | join(",")' "$INVENTORY")
	for i in $(seq 0 $((BROKERS - 1))); do
		on_broker "$i" 'mkdir -p $H'
		rscp "$RIG2/scrape-loop.py" "root@$(broker_pub_ip "$i"):$REMOTE_H/"
	done
	for g in $(seq 0 $((GENERATORS - 1))); do
		on_gen "$g" 'mkdir -p $H'
		rscp "$LOAD_BIN" "root@$(driver_pub_ip "$g"):$REMOTE_H/mqtt-load"
		on_gen "$g" 'chmod +x $H/mqtt-load && $H/mqtt-load --version'
	done
fi

python3 - "$RUN/run.json" <<PY
import json, sys
json.dump({"tier": "$TIER", "brokers": $BROKERS, "broker_type": "$BROKER_TYPE" if "$TIER" != "local" else "local",
           "generators": $GENERATORS, "gen_type": "$GEN_TYPE" if "$TIER" != "local" else "local",
           "binary": """$BINARY_DESC""", "unreleased": bool("${MQTTD_URL:-}"), "workloads": "$WORKLOADS".split(),
           "started_utc": "$STAMP", "warmup_s": $WARMUP, "measure_s": $MEASURE, "window_s": $WINDOW,
           "git": "$(git -C "$REPO_ROOT" rev-parse HEAD)"}, open(sys.argv[1], "w"), indent=2)
PY

# ── one step ────────────────────────────────────────────────────────────────────────
scrape_once_all() { # scrape_once_all <dir>: one /metrics per broker into <dir>/broker-<i>.prom
	local d="$1" i
	mkdir -p "$d"
	for ((i = 0; i < BROKERS; i++)); do
		if [ "$TIER" = local ]; then
			curl -s -m 10 "$(broker_metrics_url "$i")" >"$d/broker-$i.prom" || return 1
		else
			rssh "$(broker_pub_ip "$i")" "curl -s -m 10 http://localhost:8080/metrics" >"$d/broker-$i.prom" || return 1
		fi
	done
}

await_idle() { # await_idle <step-dir>: prints the idle record as JSON
	local d="$1/pre" waited=0 state
	while :; do
		if scrape_once_all "$d" && state=$(python3 "$RIG2/verdict.py" idle --floor "$IDLE_FLOOR" "$d"/broker-*.prom); then
			echo "{\"idle\": true, \"waited_s\": $waited, \"state\": $state}"
			return 0
		fi
		if [ "$waited" -ge "$RESET_BUDGET" ]; then
			warn "cluster not idle after ${RESET_BUDGET}s: ${state:-unreadable} — the step will be VOID (unreset)"
			echo "{\"idle\": false, \"waited_s\": $waited, \"state\": ${state:-null}}"
			return 0
		fi
		sleep 5
		waited=$((waited + 5))
	done
}

run_step() { # run_step <ladder-dir> <index> <rate>
	local ladder="$1" idx="$2" rate="$3" id hid step g i per_gen idle bound pids=() attempt
	attempt=$(find "$ladder/steps" -maxdepth 1 \( -name "[0-9]*-$rate" -o -name "[0-9]*-$rate-a*" \) 2>/dev/null | wc -l | tr -d ' ')
	attempt=$((attempt + 1))
	id=$(printf '%02d-%s' "$idx" "$rate")
	[ "$attempt" = 1 ] || id="$id-a$attempt"
	step="$ladder/steps/$id"
	# Unique on the hosts across arms and ladders: step ids repeat between workloads.
	hid="$ARM-$WORKLOAD-$id"
	mkdir -p "$step/scrapes"
	idle=$(await_idle "$step")
	per_gen=$(python3 -c "print(f'{$rate / $GENERATORS:.3f}')")
	python3 - "$step/step.json" <<PY
import json, sys
json.dump({"index": $idx, "id": "$id", "rate": float($rate), "attempt": $attempt, "generators": $GENERATORS,
           "per_generator_rate": float($per_gen), "workload": "$WORKLOAD", "arm": "$ARM",
           "idle_before": json.loads(r"""$idle"""),
           "judge": {"margin_s": $MARGIN_S, "latency_gate": "$LATENCY_GATE", "idle_floor": $IDLE_FLOOR}},
          open(sys.argv[1], "w"), indent=2)
PY
	say "[$WORKLOAD] step $id: $rate msg/s offered ($GENERATORS generator(s) x $per_gen)"
	for ((i = 0; i < BROKERS; i++)); do
		on_broker "$i" "rm -rf \$H/steps/$hid; mkdir -p \$H/steps/$hid; nohup python3 \$H/scrape-loop.py --url '$(broker_metrics_url "$i")' \
			--out \$H/steps/$hid/scrapes --interval $SCRAPE_INTERVAL --max-secs 1800 >/dev/null 2>&1 </dev/null & echo \$! >\$H/steps/$hid/scrape.pid"
	done
	sleep "$SCRAPE_INTERVAL"
	local -a args=(--broker "$ENDPOINTS" --rate "$per_gen" --qos "$QOS" --payload "$PAYLOAD"
		--topics "$TOPICS" --shape "$SHAPE" --consumers "$CONSUMERS_N" --groups "$SHARE_GROUPS"
		--sessions "$SESSIONS" --rate-per-publisher "$RATE_PER_PUBLISHER" --inflight "$INFLIGHT"
		--warmup "$WARMUP" --duration "$MEASURE" --window "$WINDOW" --drain-timeout "$DRAIN_TIMEOUT" --quiet)
	[ "$SESSIONS" != durable ] || args+=(--session-expiry "$SESSION_EXPIRY")
	bound=$((WARMUP + MEASURE + DRAIN_TIMEOUT + 180))
	for ((g = 0; g < GENERATORS; g++)); do
		run_bounded "$bound" on_gen "$g" "mkdir -p \$H/steps/$hid && cd \$H && { ulimit -n 262144 2>/dev/null || ulimit -n \$(ulimit -Hn); } 2>/dev/null; ./mqtt-load ${args[*]@Q} \
			--topic-prefix rig2/$hid/g$g --client-prefix r2-$hid-g$g --out steps/$hid/gen.json \
			2>steps/$hid/gen.stderr; echo \$? >steps/$hid/gen.exit" &
		pids+=($!)
	done
	for pid in "${pids[@]}"; do wait "$pid" || warn "a generator session ended with $? (its exit code is in gen-*.exit)"; done
	sleep "$SCRAPE_INTERVAL"
	for ((i = 0; i < BROKERS; i++)); do
		on_broker "$i" "kill \$(cat \$H/steps/$hid/scrape.pid) 2>/dev/null; true"
	done
	sleep 1
	for ((i = 0; i < BROKERS; i++)); do
		get_broker "$i" "steps/$hid/scrapes" "$step/scrapes/broker-$i" || warn "could not fetch broker $i's scrapes"
	done
	for ((g = 0; g < GENERATORS; g++)); do
		get_gen "$g" "steps/$hid/gen.json" "$step/gen-$g.json" || warn "generator $g left no summary"
		get_gen "$g" "steps/$hid/gen.stderr" "$step/gen-$g.stderr" || true
		get_gen "$g" "steps/$hid/gen.exit" "$step/gen-$g.exit" || true
	done
	python3 "$RIG2/verdict.py" judge "$step" | while IFS= read -r line; do say "  $line"; done
}

run_ladder() { # run_ladder <workload>, in a subshell so one workload's values never leak into the next
	WORKLOAD="$1"
	# shellcheck disable=SC1090
	. "$RIG2/workloads/$WORKLOAD.env"
	if [ "$CONSUMERS" = auto ]; then CONSUMERS_N="$BROKERS"; else CONSUMERS_N="$CONSUMERS"; fi
	local start_var="START_RATE_$TIER" ladder="$RUN/$ARM/$WORKLOAD" idx=0 rate
	START_RATE="${START_RATE:-${!start_var}}"
	mkdir -p "$ladder/steps"
	python3 - "$ladder/ladder.json" <<PY
import json, sys
json.dump({"workload": "$WORKLOAD", "arm": "$ARM", "binary": """$BINARY_DESC""",
           "ramp": {"start_rate": $START_RATE, "factor": $FACTOR, "gap_pct": $GAP_PCT, "granularity": $GRANULARITY,
                    "min_rate": $MIN_RATE, "max_rate": $MAX_RATE, "void_retries": $VOID_RETRIES,
                    "bisect_on_fail": bool($BISECT_ON_FAIL), "latency_gate": "$LATENCY_GATE"},
           "mqtt_load": {"qos": $QOS, "shape": "$SHAPE", "sessions": "$SESSIONS", "session_expiry_s": $SESSION_EXPIRY,
                         "payload": $PAYLOAD, "topics_per_generator": $TOPICS, "consumers": $CONSUMERS_N,
                         "groups": $SHARE_GROUPS, "rate_per_publisher": $RATE_PER_PUBLISHER, "inflight": $INFLIGHT,
                         "warmup_s": $WARMUP, "measure_s": $MEASURE, "window_s": $WINDOW, "drain_timeout_s": $DRAIN_TIMEOUT}},
          open(sys.argv[1], "w"), indent=2)
PY
	say "════ ladder: $WORKLOAD (start $START_RATE msg/s, x$FACTOR, bisect to $GAP_PCT%) ════"
	while [ "$idx" -lt "$MAX_STEPS" ]; do
		rate=$(python3 "$RIG2/verdict.py" next "$ladder" 2>"$ladder/next.log")
		say "  next: $(cat "$ladder/next.log")"
		[ -n "$rate" ] || break
		idx=$((idx + 1))
		run_step "$ladder" "$idx" "$rate"
	done
	[ "$idx" -lt "$MAX_STEPS" ] || warn "[$WORKLOAD] stopped at MAX_STEPS=$MAX_STEPS"
}

for w in $WORKLOADS; do
	(run_ladder "$w")
done

python3 "$RIG2/verdict.py" report "$RUN" >/dev/null
say "report: $RUN/report.md"
cat "$RUN/report.md" >&2
