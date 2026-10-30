#!/usr/bin/env bash
# host.sh <command> [args]: the netprobe's side on one host (copied to /root/netprobe).
#
#   host.sh setup           install iperf3 and build neper (google/neper; its revision is printed)
#   host.sh iface           the private network's interface name
#   host.sh snap <file>     one reading of every counter that can drop or delay a packet
#   host.sh rtt <file>      the established TCP sockets' RTTs, one per line (ms)
#
# The reading is plain text sections, each `== <name>` and then the tool's own output:
# nstat (absolute, every counter), tc -s qdisc, ethtool -S / -g / -l, the interface's
# statistics, softnet_stat, and /proc/stat's per-CPU lines. summarize.py takes the
# differences of two readings.
set -euo pipefail

iface() { ip -o -4 addr show | awk '$4 ~ /^10\./ {print $2; exit}'; }

case "${1:?command}" in
setup)
	export DEBIAN_FRONTEND=noninteractive
	apt-get update -qq
	apt-get install -y -qq iperf3 build-essential git ethtool iproute2 >/dev/null
	if [ ! -x /root/neper/tcp_rr ]; then
		git clone -q --depth 1 https://github.com/google/neper /root/neper
		make -C /root/neper -s tcp_rr >/dev/null
	fi
	echo "iperf3 $(iperf3 --version | head -1 | awk '{print $2}'), neper $(git -C /root/neper rev-parse --short HEAD), kernel $(uname -r), iface $(iface), cpus $(nproc)"
	;;
iface) iface ;;
snap)
	IF=$(iface)
	{
		echo "== nstat"; nstat -az 2>/dev/null
		echo "== qdisc"; tc -s qdisc show dev "$IF"
		echo "== ethtool-S"; ethtool -S "$IF" 2>&1 || true
		echo "== ethtool-g"; ethtool -g "$IF" 2>&1 || true
		echo "== ethtool-l"; ethtool -l "$IF" 2>&1 || true
		echo "== ifstat"; for f in /sys/class/net/"$IF"/statistics/*; do echo "$(basename "$f") $(cat "$f")"; done
		echo "== softnet"; cat /proc/net/softnet_stat
		echo "== cpu"; grep '^cpu[0-9]' /proc/stat
		echo "== time"; date +%s.%N
	} >"$2"
	;;
rtt) ss -tinH state established | grep -oE 'rtt:[0-9.]+' | cut -d: -f2 >"$2" ;;
*) echo "unknown command $1" >&2; exit 2 ;;
esac
