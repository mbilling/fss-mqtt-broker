#!/usr/bin/env bash
# Source after lib.sh. Lane A samplers follow the actual driver lifetime (#595),
# not a guessed duration that can expire during readiness/setup or between arms.
with_cpu_sampling() (
	local dir="$1" rc=0 sampling_rc=0 i ip role
	shift
	local -a pids=() files=()
	mkdir -p "$dir"
	stop_streams() {
		local pid failed=0
		for pid in "${pids[@]}"; do
			if kill -0 "$pid" 2>/dev/null; then
				kill "$pid" 2>/dev/null || true
				wait "$pid" 2>/dev/null || true
			else
				wait "$pid" 2>/dev/null || true
				warn "CPU sampler $pid ended before the driver; coverage is incomplete"
				failed=1
			fi
		done
		pids=()
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
				exec ssh "${SSH_OPTS[@]}" -o UserKnownHostsFile="$RUN/known_hosts" "root@$ip" \
					'command -v mpstat >/dev/null || exit 127; printf "CPU_STREAM_START_UTC %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)"; exec env LC_ALL=C TZ=UTC mpstat -P ALL 1'
			) > "$file" 2> "$dir/cpu-$role$i.stderr" &
			pids+=("$!")
			files+=("$file")
			printf '%s\t%s\n' "$!" "$role$i" >> "$dir/samplers.tsv"
		done
	done
	# Every stream must be established before the driver starts. The stream's
	# UTC marker and the driver's MEASUREMENT_WINDOW JSON permit alignment;
	# do not average idle/preflight samples into the measurement window.
	for file in "${files[@]}"; do
		wait_for "CPU stream $file" 30 grep -q '^CPU_STREAM_START_UTC ' "$file" || exit 1
	done
	if "$@"; then rc=0; else rc=$?; fi
	stop_streams || sampling_rc=$?
	[ "$rc" -eq 0 ] || exit "$rc"
	exit "$sampling_rc"
)
