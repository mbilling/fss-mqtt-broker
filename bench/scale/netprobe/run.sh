#!/usr/bin/env bash
# netprobe: the packet rate two Hetzner hosts sustain on one private network, with no
# broker involved. Two hosts (TYPE, default ccx53) are created with the hcloud CLI,
# iperf3 and neper are installed, and three tests sweep up to where the path breaks:
#
#   udp   iperf3 -u, 64 B datagrams, 8 streams, at each of UDP_PPS packets/s in all:
#         the bare packet-rate limit and its loss, no TCP
#   rr    neper tcp_rr over RR_FLOWS connections, 200 B request and response, closed
#         loop: transactions/s, latency, retransmits (the mqttd rig's 20,000-client shape)
#   bulk  iperf3 TCP, 8 streams: bandwidth, as a sanity check
#
# Around every test both hosts record every counter that can drop or delay a packet
# (host.sh snap: nstat, tc -s qdisc, ethtool -S/-g/-l, interface and softnet stats,
# per-CPU time); summarize.py prints one table per test from their differences.
# Everything created carries the labels purpose=mqttd-netprobe and run=<stamp> and is
# deleted on exit (a trap), then audited by both, so two probes can run at once.
#
#   HCLOUD_TOKEN=... bench/scale/netprobe/run.sh          # asks before creating
#   NETPROBE_YES=1 ...                                     # no prompt (not a terminal)
#   PRICE_ONLY=1 ...                                       # the quote, nothing created
#   TYPE=ccx33 LOCATION=nbg1 TESTS="udp rr" ...            # other shapes
#   QDISCS="default fq fq_codel-big" TESTS=rr ...           # each test once per qdisc
#
# QDISCS: the root qdisc of the private interface on both hosts, set before each pass:
# `default` leaves the image's (fq_codel, limit 10240p, 1024 flows); `fq` paces each
# connection with its own queue; `fq_codel-big` is fq_codel with limit 100000p and
# 65536 flows. Each test's directory is named <test>[@<qdisc>].
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
TYPE="${TYPE:-ccx53}"
LOCATION="${LOCATION:-fsn1}"
IMAGE="${IMAGE:-ubuntu-24.04}"
TESTS="${TESTS:-udp rr bulk}"
UDP_PPS="${UDP_PPS:-50000 100000 150000 200000 300000 400000 600000 800000}"
UDP_SECS="${UDP_SECS:-15}"
RR_FLOWS="${RR_FLOWS:-1000 10000 20000}"
RR_THREADS="${RR_THREADS:-}"
RR_SECS="${RR_SECS:-20}"
QDISCS="${QDISCS:-default}"
LABEL="purpose=mqttd-netprobe"
STAMP=$(date -u +%Y%m%dT%H%M%SZ)
RUN="$HERE/.runs/$STAMP"
NAME=$(echo "netprobe-$STAMP" | tr "[:upper:]" "[:lower:]")

say() { printf '\033[1;34m==>\033[0m %s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
die() { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

[ -n "${HCLOUD_TOKEN:-}" ] || die "set HCLOUD_TOKEN"
command -v hcloud >/dev/null || die "the hcloud CLI is required"
command -v jq >/dev/null || die "jq is required"

price=$(hcloud server-type describe "$TYPE" -o json | jq -r --arg l "$LOCATION" \
	'.prices[] | select(.location == $l) | .price_hourly.gross')
[ -n "$price" ] || die "no price for $TYPE in $LOCATION"
say "2 x $TYPE in $LOCATION = EUR $(python3 -c "print(f'{2 * $price:.4f}')")/h; ~25 min = EUR $(python3 -c "print(f'{2 * $price * 25 / 60:.2f}')") prorated"
[ "${PRICE_ONLY:-0}" = 1 ] && exit 0
if [ "${NETPROBE_YES:-0}" != 1 ]; then
	[ -t 0 ] || die "not a terminal: set NETPROBE_YES=1 to create the hosts without the prompt"
	read -r -p "create them? [y/N] " a
	[ "$a" = y ] || exit 0
fi

mkdir -p "$RUN"
say "run dir: $RUN"
KEY="$RUN/id_ed25519"
ssh-keygen -q -t ed25519 -N "" -C "$NAME" -f "$KEY"
SSH=(ssh -i "$KEY" -o UserKnownHostsFile="$RUN/known_hosts" -o StrictHostKeyChecking=accept-new
	-o ConnectTimeout=10 -o ServerAliveInterval=15 -o BatchMode=yes)
on() { "${SSH[@]}" "root@$1" "$2"; }

teardown() {
	local rc=$?
	trap - EXIT INT TERM
	say "tearing down (exit code $rc)"
	for s in $(hcloud server list -l "$LABEL,run=$STAMP" -o noheader -o columns=id 2>/dev/null); do
		hcloud server delete "$s" >/dev/null || true
	done
	hcloud firewall delete "$NAME" >/dev/null 2>&1 || true
	hcloud network delete "$NAME" >/dev/null 2>&1 || true
	hcloud ssh-key delete "$NAME" >/dev/null 2>&1 || true
	local left
	# This run's own resources (run=STAMP): two probes may run at once, each audits its own.
	local sel="$LABEL,run=$STAMP"
	left=$( { hcloud server list -l "$sel" -o noheader; hcloud firewall list -l "$sel" -o noheader
		hcloud network list -l "$sel" -o noheader; hcloud ssh-key list -l "$sel" -o noheader; } 2>&1 || true)
	if [ -n "$left" ]; then
		printf '\033[1;31mLEAKED\033[0m:\n%s\n' "$left" >&2
		exit 1
	fi
	say "nothing leaked — you are not paying for anything"
	exit "$rc"
}
trap teardown EXIT INT TERM

hcloud ssh-key create --name "$NAME" --public-key-from-file "$KEY.pub" --label "$LABEL" --label "run=$STAMP" >/dev/null
hcloud network create --name "$NAME" --ip-range 10.98.0.0/16 --label "$LABEL" --label "run=$STAMP" >/dev/null
hcloud network add-subnet "$NAME" --type cloud --network-zone eu-central --ip-range 10.98.1.0/24 >/dev/null
cat >"$RUN/fw.json" <<'JSON'
[{"direction":"in","protocol":"tcp","port":"22","source_ips":["0.0.0.0/0","::/0"]},
 {"direction":"in","protocol":"icmp","source_ips":["0.0.0.0/0","::/0"]}]
JSON
hcloud firewall create --name "$NAME" --rules-file "$RUN/fw.json" --label "$LABEL" --label "run=$STAMP" >/dev/null
say "creating 2 x $TYPE"
for h in a b; do
	hcloud server create --name "$NAME-$h" --type "$TYPE" --image "$IMAGE" --location "$LOCATION" \
		--ssh-key "$NAME" --network "$NAME" --firewall "$NAME" \
		--label "$LABEL" --label "run=$STAMP" -o json >"$RUN/server-$h.json" &
done
wait
A=$(jq -r '.server.public_net.ipv4.ip' "$RUN/server-a.json")
B=$(jq -r '.server.public_net.ipv4.ip' "$RUN/server-b.json")
BPRIV=$(hcloud server describe "$NAME-b" -o json | jq -r '.private_net[0].ip')
[ -n "$A" ] && [ -n "$B" ] && [ "$BPRIV" != null ] || die "servers have no addresses: a=$A b=$B b-private=$BPRIV"
say "client a $A, server b $B (private $BPRIV)"
for ip in "$A" "$B"; do
	for _ in $(seq 60); do on "$ip" true 2>/dev/null && break; sleep 5; done
	on "$ip" "cloud-init status --wait >/dev/null 2>&1 || true; mkdir -p /root/netprobe"
	scp -q -i "$KEY" -o UserKnownHostsFile="$RUN/known_hosts" -o BatchMode=yes "$HERE/host.sh" "root@$ip:/root/netprobe/"
done
for h in a b; do
	ip=$([ $h = a ] && echo "$A" || echo "$B")
	on "$ip" "bash /root/netprobe/host.sh setup" >"$RUN/setup-$h.txt" 2>&1 &
done
wait
say "a: $(tail -1 "$RUN/setup-a.txt")"
say "b: $(tail -1 "$RUN/setup-b.txt")"
NCPU=$(on "$A" nproc)
THREADS="${RR_THREADS:-$NCPU}"
# neper opens one socket per flow on each side, and iperf3 one per stream.
for ip in "$A" "$B"; do on "$ip" "sysctl -qw net.core.somaxconn=65535 net.ipv4.tcp_max_syn_backlog=65535 net.ipv4.ip_local_port_range='1024 65535'"; done

# test <id> <client command> [<server command>]: counters on both hosts before and after,
# the client's output kept, its sockets' RTTs read half-way through a TCP test.
test_one() {
	local id="$1" client="$2" server="${3:-}" dir="$RUN/$1"
	mkdir -p "$dir"
	if [ -n "$server" ]; then
		on "$B" "ulimit -n \$(ulimit -Hn); nohup $server >/root/netprobe/$id.server.txt 2>&1 </dev/null & echo \$! >/root/netprobe/server.pid"
		sleep 2
	fi
	on "$A" "bash /root/netprobe/host.sh snap /root/netprobe/a.before" &
	on "$B" "bash /root/netprobe/host.sh snap /root/netprobe/b.before" &
	wait
	on "$A" "ulimit -n \$(ulimit -Hn); $client" >"$dir/client.txt" 2>&1 &
	local pid=$!
	if [[ "$id" == rr-* ]]; then
		sleep $((RR_SECS / 2))
		on "$A" "bash /root/netprobe/host.sh rtt /root/netprobe/a.rtt" || true
		on "$B" "bash /root/netprobe/host.sh rtt /root/netprobe/b.rtt" || true
	fi
	wait "$pid" || say "  $id: the client exited non-zero (kept in $dir/client.txt)"
	on "$A" "bash /root/netprobe/host.sh snap /root/netprobe/a.after" &
	on "$B" "bash /root/netprobe/host.sh snap /root/netprobe/b.after" &
	wait
	if [ -n "$server" ]; then
		on "$B" "kill \$(cat /root/netprobe/server.pid) 2>/dev/null; sleep 1; cat /root/netprobe/$id.server.txt" >"$dir/server.txt" 2>&1 || true
	fi
	for h in a b; do
		ip=$([ $h = a ] && echo "$A" || echo "$B")
		for f in before after rtt; do
			on "$ip" "cat /root/netprobe/$h.$f 2>/dev/null; rm -f /root/netprobe/$h.$f" >"$dir/$h.$f" || true
		done
	done
	say "  $id done"
}

set_qdisc() { # set_qdisc <name>: the root qdisc of both hosts' private interface
	local q
	case "$1" in
	default) q="fq_codel" ;;
	fq) q="fq" ;;
	fq_codel-big) q="fq_codel limit 100000 flows 65536" ;;
	*) die "unknown qdisc $1" ;;
	esac
	for ip in "$A" "$B"; do on "$ip" "IF=\$(bash /root/netprobe/host.sh iface); tc qdisc replace dev \$IF root $q"; done
	say "qdisc: $1 ($q)"
}

for QD in $QDISCS; do
set_qdisc "$QD"
SFX=$([ "$QD" = default ] && echo "" || echo "@$QD")
for t in $TESTS; do
	case "$t" in
	udp)
		on "$B" "pkill iperf3; nohup iperf3 -s -D >/dev/null 2>&1" || true
		for pps in $UDP_PPS; do
			bps=$((pps / 8 * 64 * 8))
			say "udp: $pps packets/s (8 streams of 64 B)"
			test_one "udp-$pps$SFX" "iperf3 -c $BPRIV -u -l 64 -b $bps -P 8 -t $UDP_SECS -J"
		done
		on "$B" "pkill iperf3" || true
		;;
	rr)
		for f in $RR_FLOWS; do
			say "rr: $f connections, $THREADS threads, 200 B each way"
			test_one "rr-$f$SFX" \
				"/root/neper/tcp_rr -c -H $BPRIV -F $f -T $THREADS -l $RR_SECS -Q 200 -R 200 -p 50,90,99,99.9" \
				"/root/neper/tcp_rr -F $f -T $THREADS -l $((RR_SECS + 15)) -Q 200 -R 200"
		done
		;;
	bulk)
		on "$B" "pkill iperf3; nohup iperf3 -s -D >/dev/null 2>&1" || true
		say "bulk: TCP, 8 streams"
		test_one "bulk$SFX" "iperf3 -c $BPRIV -P 8 -t 10 -J"
		on "$B" "pkill iperf3" || true
		;;
	*) die "unknown test $t" ;;
	esac
done
done

python3 "$HERE/summarize.py" "$RUN" | tee "$RUN/report.txt"
