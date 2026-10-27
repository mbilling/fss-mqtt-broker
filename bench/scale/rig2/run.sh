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
# the same cluster. A ladder is a sequence of steps; a step is ROUNDS rounds at its msg/s
# (3 on n3 and n10, 1 locally), each round ONE mqtt-load process per generator host (its
# own ledger), all started together, the total msg/s split evenly. Every round scrapes
# every broker's /metrics on the broker's own host for the whole round, and verdict.py
# judges it from those counters and the generators' summaries, then the step from its
# rounds (verdict.py round-next decides which round runs next, verdict.py step judges).
#
# Teardown (bench/scale/teardown.sh: tofu destroy, then the label audit) is trapped on
# EXIT/INT/TERM. KEEP_INFRA=1 skips it and says so. Hetzner only (CLOUD=hcloud).
# Every long child (tofu, the bootstrap, each ladder) runs in the background under `wait`
# (run_fg), so a TERM or Ctrl-C reaches the trap at once: bash defers a trap until a
# FOREGROUND child returns, and on the first 10-node run (2026-10-26) a TERM sat
# unhandled behind a ladder until the fleet was killed and torn down by hand. The trap
# stops this run's children first, then tears down; a second INT/TERM does not cut the
# teardown short.
#
# Live Grafana is ON by default (OBSERVE=0 disables): bench/scale/observe.sh runs
# Grafana, Prometheus and Alloy in Docker on THIS machine; generator host 0 runs the old
# rig's curl bridge (curl + python3 only, no Docker) over every broker's /metrics and
# every generator's mqtt-load --metrics, reached through an ssh tunnel.
# http://localhost:3000 — see the README's "Live Grafana".

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
	DEF_WARMUP=5 DEF_MEASURE=10 DEF_WINDOW=5 DEF_SCRAPE=1 DEF_MARGIN=0.5 DEF_MAX_STEPS=3 DEF_PRIME=0 DEF_ROUNDS=1
	;;
n3)
	BROKERS="${BROKERS:-3}" GENERATORS="${GENERATORS:-1}"
	DEF_WARMUP=20 DEF_MEASURE=60 DEF_WINDOW=10 DEF_SCRAPE=2 DEF_MARGIN=1 DEF_MAX_STEPS=14 DEF_PRIME=90 DEF_ROUNDS=3
	;;
n10)
	BROKERS="${BROKERS:-10}" GENERATORS="${GENERATORS:-3}"
	DEF_WARMUP=20 DEF_MEASURE=60 DEF_WINDOW=10 DEF_SCRAPE=2 DEF_MARGIN=1 DEF_MAX_STEPS=14 DEF_PRIME=90 DEF_ROUNDS=3
	;;
*) die "TIER must be local, n3 or n10, not '$TIER'" ;;
esac
BROKER_TYPE="${BROKER_TYPE:-ccx23}"
GEN_TYPE="${GEN_TYPE:-ccx33}"

# ── the ramp (ADR 0090 D4) ──────────────────────────────────────────────────────────
# A step: ROUNDS rounds at the same msg/s, each a settle, then WARMUP s, then MEASURE s
# measured in WINDOW s windows, then the drain. Three rounds a step on the cloud tiers
# (2026-10-27: one round's disturbance must not decide a step); the report gives their
# mean, spread and coefficient of variation.
ROUNDS="${ROUNDS:-$DEF_ROUNDS}"
[[ "$ROUNDS" =~ ^[1-9][0-9]*$ ]] || die "ROUNDS must be a positive integer, not '$ROUNDS'"
WARMUP="${WARMUP:-$DEF_WARMUP}"
MEASURE="${MEASURE:-$DEF_MEASURE}"
WINDOW="${WINDOW:-$DEF_WINDOW}"
# Before a ladder's first step: PRIME_SECS s of unmeasured load at the start rate (its own
# mqtt-load process, never judged), then the full settle. Run 2 (2026-10-26) showed the
# first steps' latency excursions 70-80 s into the step, so a longer warm-up inside the
# step would have measured them; a separate priming run keeps them out. 0 skips it.
PRIME_SECS="${PRIME_SECS:-$DEF_PRIME}"
# The drain: it ends when every expected delivery and ack is in, or after a quiet window
# with nothing arriving: DRAIN_QUIET s, or DRAIN_QUIET_FACTOR x the slowest measured p99
# if longer (mqtt-load --drain-quiet-factor), or at the hard DRAIN_TIMEOUT. A step whose
# deliveries ran 17.9 s behind was cut off after 5 s of silence in run 2.
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-120}"
DRAIN_QUIET="${DRAIN_QUIET:-10}"
DRAIN_QUIET_FACTOR="${DRAIN_QUIET_FACTOR:-2}"
# Coarse steps multiply by FACTOR while steps are CARRIED (lossless, whatever their
# latency); after the first FAILED / OFFER NOT TAKEN / DISCONNECTED / NOT DRAINED step, bisection until
# the gap between the last carried and the first bad step is <= GAP_PCT %.
FACTOR="${FACTOR:-1.5}"
GAP_PCT="${GAP_PCT:-3}"
GRANULARITY="${GRANULARITY:-100}"
MIN_RATE="${MIN_RATE:-$GRANULARITY}"
MAX_RATE="${MAX_RATE:-2000000}"
MAX_STEPS="${MAX_STEPS:-$DEF_MAX_STEPS}"
# A VOID round (the harness's fault) is rerun this many times before its step is VOID
# and the ladder stops; the step's other rounds stand.
VOID_RETRIES="${VOID_RETRIES:-1}"
# A FAILED step (loss) ends the ladder; BISECT_ON_FAIL=1 keeps bisecting below it.
BISECT_ON_FAIL="${BISECT_ON_FAIL:-0}"
# A FAILED / OFFER NOT TAKEN / DISCONNECTED / NOT DRAINED round of a step whose nearest
# carried step below had its gated p99 under RETRY_P99_MS is retried once after a full
# settle; only a second bad attempt counts. One round a step, at most RETRY_MAX per
# ladder (verdict.py plan_rounds, retry_eligible).
RETRY_P99_MS="${RETRY_P99_MS:-50}"
RETRY_MAX="${RETRY_MAX:-2}"
# Broker scrapes: every SCRAPE_INTERVAL s on the broker's host; the window edges shrink
# by MARGIN_S each side before the first/last scrape inside them is taken.
SCRAPE_INTERVAL="${SCRAPE_INTERVAL:-$DEF_SCRAPE}"
MARGIN_S="${MARGIN_S:-$DEF_MARGIN}"
# Between steps, the settle (verdict.py idle): no connections (<= IDLE_FLOOR), sessions
# back to the run's baseline, nothing in the append lanes, the pending-publish table,
# the delivery inflight or backlog, or on the peer links, no placement group under-
# replicated, replication not still catching up, routing settled (a periodic session scan
# is not settling; one still running on the next pass is); then SETTLE_QUIET s more of the
# same with the traffic and durable-writer counters standing still, or ticking no faster
# than a background allowance (verdict.py QUIET_FRACTION of the step's msg/s, at least
# QUIET_FLOOR per second). Within RESET_BUDGET s, or the step runs and is VOID (unreset),
# whatever it measured, and the reasons are recorded. After a step that was not carried, and
# before a ladder's first step, the budget is SETTLE_BUDGET_LONG and the quiet period
# SETTLE_QUIET_LONG (sessions expiring, replication catching up).
IDLE_FLOOR="${IDLE_FLOOR:-0}"
RESET_BUDGET="${RESET_BUDGET:-120}"
SETTLE_QUIET="${SETTLE_QUIET:-10}"
SETTLE_BUDGET_LONG="${SETTLE_BUDGET_LONG:-300}"
SETTLE_QUIET_LONG="${SETTLE_QUIET_LONG:-30}"
# One start for every generator: each mqtt-load writes a ready file once its clients are
# connected and waits for a start file; when every generator is ready (or START_SYNC_WAIT
# s have passed) the rig writes one start, START_LEAD_MS ahead on generator host 0's
# clock, into every start file. A generator that connected 8.6 s after the others on the
# 10-node run (clean-qos1 303,800) left windows overlapping 51.6 s of 60: VOID. One that
# is not ready in time starts on its own when it is (mqtt-load --start-wait), and the
# judge's overlap check decides.
START_SYNC_WAIT="${START_SYNC_WAIT:-30}"
if [ "$TIER" = local ]; then START_LEAD_MS="${START_LEAD_MS:-500}"; else START_LEAD_MS="${START_LEAD_MS:-3000}"; fi
# For the price quote: steps a ladder is expected to take (coarse climb + bisection), and
# the long settles it waits through besides the first (after a step that was not carried,
# before a transient retry), each about SETTLE_QUIET_LONG + 30 s.
EST_STEPS="${EST_STEPS:-9}"
EST_LONG_SETTLES="${EST_LONG_SETTLES:-3}"

# Live Grafana (bench/scale/observe.sh). mqtt-load serves its --metrics on this port: on
# 127.0.0.1 locally, on the generator's private address in the cloud (the public
# interface admits SSH only), where generator host 0's bridge reads it.
OBSERVE="${OBSERVE:-1}"
LOAD_METRICS_PORT="${LOAD_METRICS_PORT:-9464}"
phase() { # phase <name> [detail]: the run's phase on the dashboards; best-effort
	[ "$OBSERVE" = 1 ] || return 0
	"$SCALE_DIR/observe.sh" phase "$1" "${2:-}" 2>/dev/null || true
}

# ── signals: the trap must run while a child is still working ───────────────────────
# run_fg <cmd...>: run <cmd> (a command or a function) as a background job and wait for
# it. `wait` returns as soon as a trapped signal arrives, so the trap runs at once; a
# foreground child would hold the trap back until it exits. Never inside $(...): the
# shell then waits for the substitution, which is just as deaf.
run_fg() {
	"$@" &
	local pid=$! rc=0
	wait "$pid" || rc=$?
	return "$rc"
}
descendants() { # descendants <pid>: every process below <pid>, parents first
	local c
	for c in $(pgrep -P "$1" 2>/dev/null); do
		echo "$c"
		descendants "$c"
	done
}
# stop_children: TERM every process this run started (ladders, generators, ssh, tofu),
# wait up to CHILD_GRACE s for them to go (a TERMed tofu writes its state before it
# exits, and the destroy that follows needs that state and its lock), then KILL. The
# keep-awake assertion stays: the teardown is exactly when the laptop must not sleep.
CHILD_GRACE="${CHILD_GRACE:-60}"
stop_children() {
	local pids="" p t alive
	for p in $(descendants $$); do
		[ "$p" != "${BENCH_KEEP_AWAKE_PID:-}" ] || continue
		kill -0 "$p" 2>/dev/null && pids="$pids $p"
	done
	[ -n "$pids" ] || return 0
	# shellcheck disable=SC2086 # a list of pids
	kill -TERM $pids 2>/dev/null || true
	for ((t = 0; t < CHILD_GRACE * 5; t++)); do
		alive=0
		for p in $pids; do
			# A child already gone but not yet reaped is a zombie: gone.
			case "$(ps -o stat= -p "$p" 2>/dev/null)" in "" | Z*) ;; *) alive=1 ;; esac
		done
		[ "$alive" = 1 ] || return 0
		sleep 0.2
	done
	warn "children still running after ${CHILD_GRACE}s — killing them"
	# shellcheck disable=SC2086
	kill -KILL $pids 2>/dev/null || true
}

for w in $WORKLOADS; do
	[ -f "$RIG2/workloads/$w.env" ] || die "no workload $w (have: $(cd "$RIG2/workloads" && ls ./*.env | sed 's|./||; s|\.env||' | tr '\n' ' '))"
done
NWORK=$(wc -w <<<"$WORKLOADS" | tr -d ' ')
# One round: ~20 s of connect, start lead, drain and scrape fetch around WARMUP + MEASURE
# (the 10-node runs of 2026-10-27: 97-111 s a round at 20 + 60), plus its short settle.
ROUND_SECS=$((WARMUP + MEASURE + 20 + SETTLE_QUIET + 25))
# A ladder outside its rounds: the prime (and its overhead) and its long settles.
LADDER_SECS=$((PRIME_SECS + 20 + (1 + EST_LONG_SETTLES) * (SETTLE_QUIET_LONG + 30)))

# ── the quote, before anything is provisioned ───────────────────────────────────────
if [ "$TIER" != local ]; then
	say "$(python3 "$RIG2/verdict.py" cost --tier "$TIER" --brokers "$BROKERS" --generators "$GENERATORS" \
		--broker-type "$BROKER_TYPE" --gen-type "$GEN_TYPE" --workloads "$NWORK" --steps "$EST_STEPS" \
		--rounds "$ROUNDS" --step-secs "$ROUND_SECS" --ladder-secs "$LADDER_SECS")"
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
# tar_gen <g> / tar_broker <i> <remote-rel-dir> [file...]: that directory's contents (or
# the files named) as ONE tar stream on stdout. A step's ~60 scrapes came back one scp
# round trip each, ~15 s a broker, on the 10-node run.
if [ "$TIER" = local ]; then
	host_dir() { echo "$RUN/hosts/$1"; }
	on_gen() { H="$(host_dir "gen-$1")" bash -c "$2"; }
	on_broker() { H="$(host_dir "broker-$1")" bash -c "$2"; }
	tar_gen() { local g="$1" d="$2"; shift 2; tar -C "$(host_dir "gen-$g")/$d" -cf - "${@:-.}"; }
	export COPYFILE_DISABLE=1 # macOS tar: no ._ AppleDouble entries among the scrapes
	tar_broker() { tar -C "$(host_dir "broker-$1")/$2" -cf - .; }
	broker_metrics_url() { echo "http://$(sed -n "$(($1 + 1))p" "$RUN/local/brokers.tsv" | cut -f3)/metrics"; }
	gen_metrics_addr() { echo "127.0.0.1:$LOAD_METRICS_PORT"; }
else
	REMOTE_H=/root/rig2
	on_gen() { rssh "$(driver_pub_ip "$1")" "H=$REMOTE_H; $2"; }
	on_broker() { rssh "$(broker_pub_ip "$1")" "H=$REMOTE_H; $2"; }
	# A file of the list that is not there (no summary) still lets tar send the rest.
	tar_gen() { local g="$1" d="$2"; shift 2; run_bounded 60 rssh "$(driver_pub_ip "$g")" "tar -C $REMOTE_H/$d -cf - ${*:-.} 2>/dev/null; true"; }
	tar_broker() { run_bounded 60 rssh "$(broker_pub_ip "$1")" "tar -C $REMOTE_H/$2 -cf - ."; }
	broker_metrics_url() { echo "http://localhost:8080/metrics"; }
	gen_metrics_addr() { echo "$(driver_priv_ip "$1"):$LOAD_METRICS_PORT"; }
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
		trap - EXIT
		trap '' INT TERM
		[ "$rc" -eq 0 ] || say "stopping (trap; exit code was $rc): this run's processes, then the local cluster"
		stop_children
		# A step's scrape loops are detached (nohup), so no child of this run: stop them by
		# their pid files (in the cloud they go with the hosts).
		# Only a pid that still is a scrape loop: a stopped one's pid may be reused.
		local pidf pid
		for pidf in "$RUN"/hosts/broker-*/steps/*/scrape.pid; do
			[ -f "$pidf" ] || continue
			pid=$(cat "$pidf")
			ps -o command= -p "$pid" 2>/dev/null | grep -q scrape-loop.py && kill "$pid" 2>/dev/null || true
		done
		phase idle "local run ended"
		MQTTD_BIN="$MQTTD_BIN" "$RIG2/local-cluster.sh" stop "$RUN/local" || true
		exit "$rc"
	}
	trap stop_local EXIT INT TERM
	MQTTD_BIN="$MQTTD_BIN" run_fg "$RIG2/local-cluster.sh" start "$RUN/local" "$BROKERS"
	ENDPOINTS=$(cut -f2 "$RUN/local/brokers.tsv" | paste -sd, -)
	if [ "$OBSERVE" = 1 ]; then
		run_fg "$SCALE_DIR/observe.sh" attach-local "$RUN" "$RUN/local/brokers.tsv" "$(gen_metrics_addr 0)" ||
			warn "observe attach failed (is Docker running?) — continuing unobserved; OBSERVE=0 skips it"
	fi
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
		run_fg "$RIG2/build-mqtt-load.sh" >"$RUN/mqtt-load-path"
		LOAD_BIN=$(tail -1 "$RUN/mqtt-load-path")
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
		trap - EXIT
		# The fleet must go even if the operator presses Ctrl-C again.
		trap '' INT TERM
		stop_children
		# The tunnel is a local ssh process; the bridge dies with generator host 0.
		[ "$OBSERVE" != 1 ] || "$SCALE_DIR/observe.sh" detach 2>/dev/null || true
		phase teardown "rig2 $TIER"
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
	tf() { cd "$TFDIR" && "$TF" "$@"; } # run under run_fg: a background job, so the cd stays there
	run_fg tf init -input=false >"$RUN/tf-init.log" 2>&1 || { cat "$RUN/tf-init.log" >&2; die "tofu init failed"; }
	TF_APPLY_ARGS=(-var node_count="$BROKERS" -var driver_count="$GENERATORS"
		-var broker_server_type="$BROKER_TYPE" -var driver_server_type="$GEN_TYPE"
		-var run_label="rig2-$STAMP" -var mqttd_version="$MQTTD_VERSION"
		-var build_bench=false -var driver_emqtt_bench=false)
	[ -z "${MQTTD_URL:-}" ] || TF_APPLY_ARGS+=(-var mqttd_url="$MQTTD_URL")
	[ -z "${MQTTD_SHA256:-}" ] || TF_APPLY_ARGS+=(-var mqttd_sha256="$MQTTD_SHA256")
	TF_APPLY_ARGS+=(${TOFU_SSH_PUBKEY_ARGS[@]+"${TOFU_SSH_PUBKEY_ARGS[@]}"})
	declare -p TF_APPLY_ARGS >"$RUN/tf-apply-args.sh"
	phase provisioning "rig2 $TIER: $BROKERS x $BROKER_TYPE + $GENERATORS x $GEN_TYPE"
	say "provisioning $BROKERS x $BROKER_TYPE + $GENERATORS x $GEN_TYPE"
	run_fg tf apply -auto-approve -input=false "${TF_APPLY_ARGS[@]}" >"$RUN/tf-apply.log" 2>&1 ||
		{ tail -30 "$RUN/tf-apply.log" >&2; die "tofu apply failed"; }
	INVENTORY="$RUN/inventory.json"
	run_fg tf output -json inventory >"$INVENTORY"
	: >"$RUN/known_hosts"
	run_fg await_fleet
	phase bootstrapping "rig2 $TIER: $BROKERS nodes"
	run_fg "$SCALE_DIR/bootstrap-cluster.sh" "$RUN" "$INVENTORY" durable
	if [ "$OBSERVE" = 1 ]; then
		OBSERVE_GEN_METRICS_PORT="$LOAD_METRICS_PORT" run_fg "$SCALE_DIR/observe.sh" attach "$RUN" "$INVENTORY" ||
			warn "observe attach failed — continuing unobserved (OBSERVE=0 skips it)"
	fi
	ENDPOINTS=$(jq -r '[.brokers[].mqtt_plain] | join(",")' "$INVENTORY")
	mapfile -t BROKER_PRIV < <(jq -r '.brokers[].private_ip' "$INVENTORY")
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
           "prime_s": $PRIME_SECS, "drain_timeout_s": $DRAIN_TIMEOUT, "drain_quiet_s": $DRAIN_QUIET,
           "settle_quiet_s": $SETTLE_QUIET, "rounds": $ROUNDS,
           "observe": "$OBSERVE" == "1",
           "git": "$(git -C "$REPO_ROOT" rev-parse HEAD)"}, open(sys.argv[1], "w"), indent=2)
PY

# ── one step ────────────────────────────────────────────────────────────────────────
# Fan-out to the brokers: at most SSH_PAR ssh sessions at once (sshd's MaxSessions and
# MaxStartups, and this machine's fd limit, which an earlier run hit), every call tried
# up to SSH_TRIES times. A settle scrape is one pass over every broker: in the cloud ONE
# ssh to generator host 0, which curls every broker's /metrics over the private network
# in parallel (the health port is private-only); a broker that pass misses is asked
# again directly over its own ssh. Ten scrapes in a row from the laptop took 10-15 s a
# pass on the first 10-node run, and one failure reset the whole settle.
SSH_PAR="${SSH_PAR:-5}"
SSH_TRIES="${SSH_TRIES:-2}"
SCRAPE_TRIES="${SCRAPE_TRIES:-2}"
SETTLE_POLL="${SETTLE_POLL:-5}"
for_brokers() { # for_brokers <fn> [broker...]: <fn> <i> for each broker (default: all), SSH_PAR at a time, each tried SSH_TRIES times; prints the brokers that failed every try
	local fn="$1" k n i t
	shift
	local -a list=("$@") failed=() pids who
	[ "${#list[@]}" -gt 0 ] || for ((i = 0; i < BROKERS; i++)); do list+=("$i"); done
	for ((k = 0; k < ${#list[@]}; k += SSH_PAR)); do
		pids=() who=()
		for i in "${list[@]:k:SSH_PAR}"; do
			(
				for ((t = 1; t <= SSH_TRIES; t++)); do
					"$fn" "$i" >/dev/null && exit 0
					[ "$t" -ge "$SSH_TRIES" ] || sleep 1
				done
				exit 1
			) &
			pids+=($!) who+=("$i")
		done
		for n in "${!pids[@]}"; do wait "${pids[n]}" || failed+=("${who[n]}"); done
	done
	echo "${failed[*]-}"
}

prom_complete() { [ -s "$1" ] && [ "$(tail -n 1 "$1")" = "# EOF" ]; } # mqttd ends every exposition with # EOF

# Fetchers: broker <i>'s /metrics into $SCRAPE_TMP/broker-<i>.prom (a .part file renamed
# when complete, so nobody ever reads half of one).
fetch_direct() {
	local out="$SCRAPE_TMP/broker-$1.prom"
	if [ "$TIER" = local ]; then
		curl -sf -m 10 "$(broker_metrics_url "$1")" -o "$out.part" || { rm -f "$out.part"; return 1; }
	else
		run_bounded 30 rssh "$(broker_pub_ip "$1")" "curl -sf -m 10 http://localhost:8080/metrics" >"$out.part" 2>/dev/null ||
			{ rm -f "$out.part"; return 1; }
	fi
	prom_complete "$out.part" && mv -f "$out.part" "$out" || { rm -f "$out.part"; return 1; }
}
fetch_via_gen0() { # fetch_via_gen0 <broker...>: one ssh to generator host 0 for all of them
	local s='t=$(mktemp -d) || exit 1; ' i
	for i; do
		s+="(curl -sf -m 10 http://${BROKER_PRIV[i]}:8080/metrics -o \$t/broker-$i.prom || rm -f \$t/broker-$i.prom) & "
	done
	s+='wait; tar -C "$t" -cf - . ; rm -rf "$t"'
	run_bounded 30 rssh "$(driver_pub_ip 0)" "$s" 2>/dev/null | tar -xf - -C "$SCRAPE_TMP" 2>/dev/null || true
}

# scrape_once_all <dir>: one pass, every broker's /metrics into <dir>/broker-<i>.prom.
# 0 when every broker answered: the files are then all from this pass, each renamed
# into place. 1 when some did not after SCRAPE_TRIES rounds: <dir> keeps the previous
# complete pass, and <dir>/.missing names the brokers this one missed.
scrape_once_all() {
	local d="$1" i try
	local -a missing=() still
	mkdir -p "$d"
	SCRAPE_TMP="$d/.pass"
	rm -rf "$SCRAPE_TMP"
	mkdir -p "$SCRAPE_TMP"
	for ((i = 0; i < BROKERS; i++)); do missing+=("$i"); done
	for ((try = 1; try <= SCRAPE_TRIES && ${#missing[@]} > 0; try++)); do
		if [ "$TIER" != local ] && [ "$try" = 1 ]; then
			fetch_via_gen0 "${missing[@]}"
		else
			SSH_TRIES=1 for_brokers fetch_direct "${missing[@]}" >/dev/null
		fi
		still=()
		for i in "${missing[@]}"; do
			prom_complete "$SCRAPE_TMP/broker-$i.prom" || { rm -f "$SCRAPE_TMP/broker-$i.prom"; still+=("$i"); }
		done
		missing=(${still[@]+"${still[@]}"})
	done
	if [ "${#missing[@]}" -gt 0 ]; then
		echo "${missing[*]}" >"$d/.missing"
		rm -rf "$SCRAPE_TMP"
		return 1
	fi
	for ((i = 0; i < BROKERS; i++)); do mv -f "$SCRAPE_TMP/broker-$i.prom" "$d/broker-$i.prom"; done
	rm -rf "$SCRAPE_TMP" "$d/.missing"
}

# await_idle <step-dir> <budget-s> <quiet-s> <step msg/s> <on-fail>: settle, then print
# the record as JSON. Budget and quiet are wall-clock seconds since the call. A pass that
# missed a broker is UNKNOWN: it neither confirms the quiet period nor restarts it, and
# the next complete pass is compared with the last complete one, so a counter that moved
# in between still shows. Only a complete pass that is not idle restarts the quiet
# period. A counter ticking below verdict.py's background allowance (0.1 % of <step
# msg/s>, at least 20/s) does not. Every pass is logged to <step-dir>/pre/passes.jsonl,
# and a settle that runs out of budget names what held it: the last pass's reasons,
# or, when that pass was idle, the last pass that restarted the quiet period. A broker
# that never answered within the budget is named in the VOID's reasons. <on-fail> ends
# the warning: what happens next.
await_idle() {
	local d="$1/pre" budget="$2" quiet="$3" step_rate="$4" on_fail="$5" t0=$SECONDS t state since=-1 prev="" rc i passes=0 unknown=0
	local -a answered=() missed=()
	for ((i = 0; i < BROKERS; i++)); do answered+=(0) missed+=(0); done
	mkdir -p "$d"
	: >"$d/passes.jsonl"
	while :; do
		t=$((SECONDS - t0))
		passes=$((passes + 1))
		state=""
		if scrape_once_all "$d"; then
			for ((i = 0; i < BROKERS; i++)); do answered[i]=1; done
			rc=0
			state=$(python3 "$RIG2/verdict.py" idle --floor "$IDLE_FLOOR" --baseline-sessions "$BASELINE_SESSIONS" \
				--step-rate "$step_rate" ${prev:+--prev "$prev"} "$d"/broker-*.prom) || rc=$?
			if [ -z "$state" ]; then
				warn "verdict.py idle printed nothing (exit $rc) — this pass counts as unknown"
				unknown=$((unknown + 1))
				printf '{"t": %d, "unknown": "verdict.py idle printed nothing (exit %d)"}\n' "$t" "$rc" >>"$d/passes.jsonl"
			else
				if [ "$rc" = 0 ]; then
					[ "$since" -ge 0 ] || since=$t
				else
					since=-1
				fi
				prev="$d/state.json"
				printf '%s\n' "$state" >"$prev.tmp" && mv -f "$prev.tmp" "$prev"
				printf '{"t": %d, "idle": %s, "state": %s}\n' "$t" "$([ "$rc" = 0 ] && echo true || echo false)" "$state" >>"$d/passes.jsonl"
			fi
		else
			unknown=$((unknown + 1))
			for i in $(cat "$d/.missing"); do missed[i]=$((missed[i] + 1)); done
			for ((i = 0; i < BROKERS; i++)); do
				case " $(cat "$d/.missing") " in *" $i "*) ;; *) answered[i]=1 ;; esac
			done
			printf '{"t": %d, "unknown": "no scrape from broker(s) %s"}\n' "$t" "$(cat "$d/.missing")" >>"$d/passes.jsonl"
		fi
		if [ -n "$state" ] && [ "$since" -ge 0 ] && [ $((t - since)) -ge "$quiet" ]; then
			settle_record true "$((SECONDS - t0))" "$quiet" "$budget" "$passes" "$unknown" "$prev" "${answered[*]}" "${missed[*]}"
			return 0
		fi
		if [ $((SECONDS - t0)) -ge "$budget" ]; then
			state=$(settle_record false "$((SECONDS - t0))" "$quiet" "$budget" "$passes" "$unknown" "$prev" "${answered[*]}" "${missed[*]}")
			warn "cluster not settled after $((SECONDS - t0))s (budget ${budget}s, ${quiet}s quiet needed): $(python3 -c 'import json,sys; print("; ".join(json.loads(sys.argv[1])["state"]["why_not"]))' "$state") — $on_fail"
			echo "$state"
			return 0
		fi
		sleep "$SETTLE_POLL"
	done
}
settle_record() { # settle_record <idle> <waited> <quiet> <budget> <passes> <unknown> <state-file|""> <answered...> <missed...>: the JSON await_idle prints
	python3 - "$@" <<'PY'
import json, sys
from pathlib import Path
idle, waited, quiet, budget, passes, unknown, state_file, answered, missed = sys.argv[1:10]
answered, missed = [int(x) for x in answered.split()], [int(x) for x in missed.split()]
state = None
if state_file:
    try:
        state = json.load(open(state_file))
    except (OSError, ValueError):
        state = None
log = []
if state_file:
    try:
        log = [json.loads(x) for x in (Path(state_file).parent / "passes.jsonl").read_text().splitlines() if x.strip()]
    except (OSError, ValueError):
        log = []
blocked = [x for x in log if x.get("idle") is False]
rec = {"idle": idle == "true", "waited_s": int(waited), "quiet_s": int(quiet), "budget_s": int(budget),
       "passes": int(passes), "unknown_passes": int(unknown), "blocked_passes": len(blocked),
       "scrape_misses": {f"broker-{i}": n for i, n in enumerate(missed) if n}, "state": state}
if blocked:
    rec["last_blocked"] = {"t_s": blocked[-1]["t"], "why_not": blocked[-1]["state"].get("why_not") or []}
if not rec["idle"]:
    why = list((state or {}).get("why_not") or [])
    never = [f"broker-{i}" for i, a in enumerate(answered) if not a]
    if never:
        why.append(f"no scrape of {', '.join(never)} answered in {waited} s")
    elif not why and blocked:
        # The last pass was idle, but the quiet period it was counting had not run its
        # course: name the pass that restarted it.
        last = blocked[-1]
        why.append(f"the {quiet} s quiet period last restarted at {last['t']} s of {waited}, on: "
                   + "; ".join(last["state"].get("why_not") or ["?"])
                   + f" ({len(blocked)} of {passes} passes were not idle)")
    elif not why and int(unknown):
        why.append(f"{unknown} of {passes} scrape passes incomplete ("
                   + ", ".join(f"{b} missed {n}" for b, n in rec["scrape_misses"].items()) + ")")
    elif not why:
        why.append(f"idle on every pass but not yet {quiet} s quiet when the budget ran out")
    if state is None:
        state = rec["state"] = {}
    state["why_not"] = why
print(json.dumps(rec))
PY
}

launch_gens() { # launch_gens <local-dir> <host-id> <per-gen rate> <warmup> <measure>: one mqtt-load per generator, one start
	local dir="$1" hid="$2" per_gen="$3" warmup="$4" measure="$5" g bound metrics pids=()
	local -a args=(--broker "$ENDPOINTS" --rate "$per_gen" --qos "$QOS" --payload "$PAYLOAD"
		--topics "$TOPICS" --shape "$SHAPE" --consumers "$CONSUMERS_N" --groups "$SHARE_GROUPS"
		--sessions "$SESSIONS" --rate-per-publisher "$RATE_PER_PUBLISHER" --inflight "$INFLIGHT"
		--warmup "$warmup" --duration "$measure" --window "$WINDOW" --drain-timeout "$DRAIN_TIMEOUT"
		--drain-quiet "$DRAIN_QUIET" --drain-quiet-factor "$DRAIN_QUIET_FACTOR" --quiet
		--ready-file "steps/$hid/gen.ready" --start-file "steps/$hid/gen.start" --start-wait $((START_SYNC_WAIT + 15)))
	[ "$SESSIONS" != durable ] || args+=(--session-expiry "$SESSION_EXPIRY")
	bound=$((START_SYNC_WAIT + 15 + warmup + measure + DRAIN_TIMEOUT + 180))
	for ((g = 0; g < GENERATORS; g++)); do
		metrics=""
		[ "$OBSERVE" != 1 ] || metrics="--metrics $(gen_metrics_addr "$g")"
		run_bounded "$bound" on_gen "$g" "mkdir -p \$H/steps/$hid && cd \$H && rm -f steps/$hid/gen.ready steps/$hid/gen.start steps/$hid/gen.exit && { ulimit -n 262144 2>/dev/null || ulimit -n \$(ulimit -Hn); } 2>/dev/null; ./mqtt-load ${args[*]@Q} \
			--topic-prefix rig2/$hid/g$g --client-prefix r2-$hid-g$g --out steps/$hid/gen.json $metrics \
			2>steps/$hid/gen.stderr; echo \$? >steps/$hid/gen.exit" &
		pids+=($!)
	done
	start_gens "$hid"
	for pid in "${pids[@]}"; do wait "$pid" || warn "a generator session ended with $? (its exit code is in gen-*.exit)"; done
	local f part
	for ((g = 0; g < GENERATORS; g++)); do
		part="$dir/.fetch-gen-$g"
		rm -rf "$part"
		mkdir -p "$part"
		tar_gen "$g" "steps/$hid" gen.json gen.stderr gen.exit 2>/dev/null | tar -C "$part" -xf - 2>/dev/null || true
		for f in json stderr exit; do
			[ ! -f "$part/gen.$f" ] || mv -f "$part/gen.$f" "$dir/gen-$g.$f"
		done
		rm -rf "$part"
		[ -f "$dir/gen-$g.json" ] || warn "generator $g left no summary"
	done
}
# gens_ready <host-id>: 0 when every generator has connected its clients (its ready file)
# or is already gone (its exit file: it failed to start, and waiting is pointless).
gens_ready() {
	local hid="$1" g rc=0
	local -a pids=()
	for ((g = 0; g < GENERATORS; g++)); do
		run_bounded 20 on_gen "$g" "test -f \$H/steps/$hid/gen.ready || test -f \$H/steps/$hid/gen.exit" >/dev/null 2>&1 &
		pids+=($!)
	done
	for pid in "${pids[@]}"; do wait "$pid" || rc=1; done
	return "$rc"
}
# start_gens <host-id>: wait for every generator to be ready, then write one start time
# into every start file (atomically: a temporary file renamed into place).
start_gens() {
	local hid="$1" deadline=$((SECONDS + START_SYNC_WAIT)) at g missed_start=""
	local -a pids=()
	until gens_ready "$hid"; do
		if [ "$SECONDS" -ge "$deadline" ]; then
			warn "not every generator connected within ${START_SYNC_WAIT}s; starting the ready ones (a late one starts on its own, and the judge checks the overlap)"
			break
		fi
		sleep 0.5
	done
	at=$(run_bounded 20 on_gen 0 "python3 -c 'import time; print(int(time.time() * 1000) + $START_LEAD_MS)'" 2>/dev/null) || at=""
	[[ "$at" =~ ^[0-9]+$ ]] || at=$(python3 -c "import time; print(int(time.time() * 1000) + $START_LEAD_MS)")
	for ((g = 0; g < GENERATORS; g++)); do
		run_bounded 20 on_gen "$g" "printf '%s\\n' $at >\$H/steps/$hid/gen.start.part && mv -f \$H/steps/$hid/gen.start.part \$H/steps/$hid/gen.start" >/dev/null 2>&1 &
		pids+=($!)
	done
	for g in "${!pids[@]}"; do wait "${pids[g]}" || missed_start+=" $g"; done
	[ -z "$missed_start" ] || warn "could not write the start to generator(s)$missed_start (they start on their own after the wait)"
}

prime_ladder() { # prime_ladder <ladder-dir>: unmeasured load at the start rate before the first step
	local ladder="$1" per_gen
	[ "${PRIME_SECS%.*}" -gt "$WINDOW" ] 2>/dev/null || return 0
	mkdir -p "$ladder/prime"
	# The same full settle as before a first step: the prime must not start on residue
	# either (a prime started on a cluster seconds old was refused with 0x88 locally).
	await_idle "$ladder/prime" "$SETTLE_BUDGET_LONG" "$SETTLE_QUIET_LONG" "$START_RATE" \
		"priming anyway (the prime is not judged; the first step settles again before it starts)" >"$ladder/prime/idle_before.json"
	per_gen=$(python3 -c "print(f'{$START_RATE / $GENERATORS:.3f}')")
	say "[$WORKLOAD] priming: ${PRIME_SECS}s at $START_RATE msg/s, not measured"
	phase running "$WORKLOAD priming: $START_RATE msg/s"
	launch_gens "$ladder/prime" "$ARM-$WORKLOAD-prime" "$per_gen" $((PRIME_SECS - WINDOW)) "$WINDOW"
}

# A round's scrape loop on broker <i>, for the round $hid (run_round's): started (any loop
# left by an earlier try of the same start is stopped first), stopped, fetched.
scrape_loop_start() {
	on_broker "$1" "[ -f \$H/steps/$hid/scrape.pid ] && kill \$(cat \$H/steps/$hid/scrape.pid) 2>/dev/null; rm -rf \$H/steps/$hid; mkdir -p \$H/steps/$hid; nohup python3 \$H/scrape-loop.py --url '$(broker_metrics_url "$1")' \
		--out \$H/steps/$hid/scrapes --interval $SCRAPE_INTERVAL --max-secs 1800 >/dev/null 2>&1 </dev/null & echo \$! >\$H/steps/$hid/scrape.pid"
}
scrape_loop_stop() { on_broker "$1" "kill \$(cat \$H/steps/$hid/scrape.pid) 2>/dev/null; true"; }
# One tar stream per broker, unpacked beside the target and renamed into place only when
# the whole stream arrived: a cut connection leaves no directory (the judge then names
# the broker and VOIDs the step), never a partial one that passes for a broker's step.
scrape_loop_fetch() {
	local dst="$step/scrapes/broker-$1" part="$step/.fetch-broker-$1"
	rm -rf "$dst" "$part"
	mkdir -p "$part"
	tar_broker "$1" "steps/$hid/scrapes" | tar -C "$part" -xf - || { rm -rf "$part"; return 1; }
	mv "$part" "$dst"
}

run_step() { # run_step <ladder-dir> <index> <rate> <why the ramp chose it>: its rounds, then the step's verdict
	local ladder="$1" idx="$2" rate="$3" why="$4" id step_dir attempt prev_v plan rdir settle reason n=0
	attempt=$(find "$ladder/steps" -maxdepth 1 \( -name "[0-9]*-$rate" -o -name "[0-9]*-$rate-a*" \) 2>/dev/null | wc -l | tr -d ' ')
	attempt=$((attempt + 1))
	id=$(printf '%02d-%s' "$idx" "$rate")
	[ "$attempt" = 1 ] || id="$id-a$attempt"
	step_dir="$ladder/steps/$id"
	mkdir -p "$step_dir"
	prev_v=$(python3 - "$ladder" <<'PY'
import json, sys
from pathlib import Path
vs = sorted(Path(sys.argv[1], "steps").glob("*/verdict.json"))
print(json.loads(vs[-1].read_text())["verdict"] if vs else "none")
PY
)
	python3 - "$step_dir/step.json" <<PY
import json, sys
json.dump({"index": $idx, "id": "$id", "rate": float($rate), "attempt": $attempt, "rounds": $ROUNDS,
           "generators": $GENERATORS, "brokers": $BROKERS, "workload": "$WORKLOAD", "arm": "$ARM",
           "after": "$prev_v", "ramp_reason": r"""$why"""}, open(sys.argv[1], "w"), indent=2)
PY
	say "[$WORKLOAD] step $id: $rate msg/s offered, $ROUNDS round(s)"
	# verdict.py round-next names the next round attempt (a new round, a VOID round's rerun,
	# or a bad round's transient retry) and its settle, or nothing when the step is decided.
	# Bounded: no plan can need more attempts than this.
	while [ "$n" -lt $((ROUNDS * (VOID_RETRIES + 1) * 2)) ]; do
		plan=$(python3 "$RIG2/verdict.py" round-next "$step_dir")
		[ -n "$plan" ] || break
		read -r rdir settle reason <<<"$plan"
		n=$((n + 1))
		run_round "$step_dir" "$idx" "$rate" "$rdir" "$settle" "$reason"
	done
	python3 "$RIG2/verdict.py" step "$step_dir" | while IFS= read -r line; do say "  $line"; done
}

run_round() { # run_round <step-dir> <index> <rate> <round-dir name> <short|long settle> <why>: one round of the step
	local step_dir="$1" idx="$2" rate="$3" rname="$4" settle="$5" why="$6" id hid step per_gen idle budget quiet failed
	id="$(basename "$step_dir")/$rname"
	step="$step_dir/$rname"
	# Unique on the hosts across arms, ladders and rounds: step ids repeat between workloads.
	hid="$ARM-$WORKLOAD-$(basename "$step_dir")-$rname"
	mkdir -p "$step/scrapes"
	# The full settle before a ladder's first round and after any round that was not
	# carried; the short one between the rounds of a carried step.
	if [ "$settle" = short ]; then budget=$RESET_BUDGET quiet=$SETTLE_QUIET; else budget=$SETTLE_BUDGET_LONG quiet=$SETTLE_QUIET_LONG; fi
	idle=$(await_idle "$step" "$budget" "$quiet" "$rate" "the round runs and is VOID (unreset)")
	per_gen=$(python3 -c "print(f'{$rate / $GENERATORS:.3f}')")
	python3 - "$step/step.json" <<PY
import json, re, sys
m = re.match(r"round-(\d+)(?:-a(\d+))?$", "$rname")
json.dump({"index": $idx, "id": "$id", "rate": float($rate), "round": int(m.group(1)), "attempt": int(m.group(2) or 1),
           "generators": $GENERATORS, "brokers": $BROKERS,
           "per_generator_rate": float($per_gen), "workload": "$WORKLOAD", "arm": "$ARM",
           "idle_before": json.loads(r"""$idle"""), "settle": "$settle", "ramp_reason": r"""$why""",
           "judge": {"margin_s": $MARGIN_S, "latency_gate": "$LATENCY_GATE", "idle_floor": $IDLE_FLOOR}},
          open(sys.argv[1], "w"), indent=2)
PY
	say "[$WORKLOAD] step $id: $rate msg/s ($GENERATORS generator(s) x $per_gen) — $why"
	phase running "$WORKLOAD step $id: $rate msg/s"
	# In parallel (SSH_PAR at a time) and retried: a broker whose loop did not start, or
	# whose scrapes did not come back, leaves the judge short of that broker (VOID), not
	# the run dead under set -e.
	failed=$(for_brokers scrape_loop_start)
	[ -z "$failed" ] || warn "scrape loop did not start on broker(s) $failed"
	sleep "$SCRAPE_INTERVAL"
	launch_gens "$step" "$hid" "$per_gen" "$WARMUP" "$MEASURE"
	sleep "$SCRAPE_INTERVAL"
	failed=$(for_brokers scrape_loop_stop)
	[ -z "$failed" ] || warn "could not stop the scrape loop on broker(s) $failed (it stops itself after 1800 s)"
	sleep 1
	failed=$(for_brokers scrape_loop_fetch)
	[ -z "$failed" ] || warn "could not fetch the scrapes of broker(s) $failed"
	python3 "$RIG2/verdict.py" judge "$step" | while IFS= read -r line; do say "  $line"; done
}

run_ladder() { # run_ladder <workload>, as a background job (run_fg): its own subshell
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
                    "bisect_on_fail": bool($BISECT_ON_FAIL), "latency_gate": "$LATENCY_GATE",
                    "retry_p99_ms": $RETRY_P99_MS, "retry_max": $RETRY_MAX, "rounds": $ROUNDS},
           "mqtt_load": {"qos": $QOS, "shape": "$SHAPE", "sessions": "$SESSIONS", "session_expiry_s": $SESSION_EXPIRY,
                         "payload": $PAYLOAD, "topics_per_generator": $TOPICS, "consumers": $CONSUMERS_N,
                         "groups": $SHARE_GROUPS, "rate_per_publisher": $RATE_PER_PUBLISHER, "inflight": $INFLIGHT,
                         "warmup_s": $WARMUP, "measure_s": $MEASURE, "window_s": $WINDOW, "drain_timeout_s": $DRAIN_TIMEOUT,
                         "drain_quiet_s": $DRAIN_QUIET, "drain_quiet_factor": $DRAIN_QUIET_FACTOR, "prime_s": $PRIME_SECS}},
          open(sys.argv[1], "w"), indent=2)
PY
	say "════ ladder: $WORKLOAD (start $START_RATE msg/s, x$FACTOR, bisect to $GAP_PCT%) ════"
	prime_ladder "$ladder"
	while [ "$idx" -lt "$MAX_STEPS" ]; do
		rate=$(python3 "$RIG2/verdict.py" next "$ladder" 2>"$ladder/next.log")
		say "  next: $(cat "$ladder/next.log")"
		[ -n "$rate" ] || break
		idx=$((idx + 1))
		run_step "$ladder" "$idx" "$rate" "$(cat "$ladder/next.log")"
	done
	[ "$idx" -lt "$MAX_STEPS" ] || warn "[$WORKLOAD] stopped at MAX_STEPS=$MAX_STEPS"
}

# The run's baseline: what the fresh cluster holds before any step (normally no sessions).
# Every settle waits for the session count to be back to it.
mkdir -p "$RUN/baseline"
BASELINE_SESSIONS=0
if scrape_once_all "$RUN/baseline"; then
	python3 "$RIG2/verdict.py" idle "$RUN/baseline"/broker-*.prom >"$RUN/baseline/state.json" || true
	BASELINE_SESSIONS=$(python3 -c 'import json,sys; print(int(json.load(open(sys.argv[1]))["mqttd_sessions"]))' "$RUN/baseline/state.json" 2>/dev/null || echo 0)
fi
say "baseline: $BASELINE_SESSIONS session(s) on the fresh cluster"

for w in $WORKLOADS; do
	# A background job (its own subshell: one workload's values never leak into the
	# next) under `wait`, so a TERM reaches the trap mid-ladder.
	run_fg run_ladder "$w"
done

phase collecting "rig2 report"
python3 "$RIG2/verdict.py" report "$RUN" >/dev/null
say "report: $RUN/report.md"
cat "$RUN/report.md" >&2
