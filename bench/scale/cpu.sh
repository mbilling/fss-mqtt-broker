#!/usr/bin/env bash
# Source after lib.sh. Lane A samplers follow the actual driver lifetime (#595),
# not a guessed duration that can expire during readiness/setup or between arms.
with_cpu_sampling() (
	# __cpu_ names, not pids/files: bash locals are dynamically scoped, so the
	# WRAPPED command sees them. The comparison lane's window function opened its
	# driver scrapes with `pids=()`, which emptied this array before stop_streams
	# could use it — every sampler survived its rung, kept appending to a finished
	# rung's CPU file, and only died when its host rebooted, while the harness
	# reported "ended before the driver" on every rung (2026-09-16 rehearsal).
	local dir="$1" rc=0 sampling_rc=0 i ip role
	shift
	local -a __cpu_pids=() __cpu_files=()
	mkdir -p "$dir"
	stop_streams() {
		local pid failed=0
		for pid in "${__cpu_pids[@]}"; do
			if kill -0 "$pid" 2>/dev/null; then
				kill "$pid" 2>/dev/null || true
				wait "$pid" 2>/dev/null || true
			else
				wait "$pid" 2>/dev/null || true
				warn "CPU sampler $pid ended before the driver; coverage is incomplete"
				failed=1
			fi
		done
		__cpu_pids=()
		return "$failed"
	}
	trap 'stop_streams || true' EXIT
	trap 'exit 130' INT
	trap 'exit 143' TERM
	: > "$dir/samplers.tsv"
	for role in broker driver; do
		local count="$N"
		[ "$role" != driver ] || count="$D"
		for ((i = 0; i < count; i++)); do
			if [ "$role" = broker ]; then ip=$(broker_pub_ip "$i"); else ip=$(driver_pub_ip "$i"); fi
			local file="$dir/cpu-$role$i.txt"
			# exec is essential: track/kill the SSH process, not an intermediate
			# shell which could leave an orphaned SSH session/remote sampler.
			(
				# -n: the sampler must not read the CALLER's stdin. Without it ssh
				# inherits whatever the harness was started with, and a script run
				# from a heredoc hands it an already-closed stdin: ssh sees EOF,
				# tears the session down, and every stream dies a second after it
				# starts ("CPU sampler ... ended before the driver" on every rung
				# of the 2026-09-16 comparison). exec stays: the pid we track and
				# kill must be the ssh itself, not an intermediate shell.
				exec ssh -n "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" "root@$ip" \
					'command -v mpstat >/dev/null || exit 127; printf "CPU_STREAM_START_UTC %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; exec env LC_ALL=C TZ=UTC mpstat -P ALL 1'
			) > "$file" 2> "$dir/cpu-$role$i.stderr" &
			__cpu_pids+=("$!")
			__cpu_files+=("$file")
			printf '%s\t%s\n' "$!" "$role$i" >> "$dir/samplers.tsv"
		done
	done
	# Every stream must be established before the driver starts. The stream's
	# UTC marker and the driver's MEASUREMENT_WINDOW JSON permit alignment;
	# do not average idle/preflight samples into the measurement window.
	for file in "${__cpu_files[@]}"; do
		wait_for "CPU stream $file" 30 grep -q '^CPU_STREAM_START_UTC ' "$file" || exit 1
	done
	if "$@"; then rc=0; else rc=$?; fi
	stop_streams || sampling_rc=$?
	[ "$rc" -eq 0 ] || exit "$rc"
	exit "$sampling_rc"
)
