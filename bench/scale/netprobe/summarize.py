#!/usr/bin/env python3
"""One table per netprobe test, from its client output and the counters before/after.

    summarize.py <run-dir>

Per test, both hosts (a = client, b = server): packets/s on the private interface,
TCP retransmits and the drop counters that never reach the interface's own (qdisc,
socket receive queue and backlog, UDP buffers, the virtio queues' drops), softnet
drops and squeezes, and the busiest CPU's softirq share.
"""
import json
import re
import sys
from pathlib import Path

NSTAT = ["TcpRetransSegs", "TcpExtTCPTimeouts", "TcpExtTCPLossProbes", "TcpExtTCPRcvQDrop",
         "TcpExtTCPBacklogDrop", "TcpExtListenDrops", "UdpRcvbufErrors", "UdpSndbufErrors",
         "UdpInErrors", "IpInDiscards", "IpOutDiscards"]


def sections(text: str) -> dict:
    out, cur = {}, None
    for line in text.splitlines():
        if line.startswith("== "):
            cur = line[3:].strip()
            out[cur] = []
        elif cur:
            out[cur].append(line)
    return out


def reading(path: Path) -> dict | None:
    if not path.exists() or not path.read_text().strip():
        return None
    s = sections(path.read_text())
    r = {"t": float(s["time"][0]), "nstat": {}, "qdisc_dropped": 0, "qdisc_overlimits": 0,
         "virtio_drops": 0, "if": {}, "softnet": [], "softirq": []}
    for line in s.get("nstat", []):
        p = line.split()
        if len(p) >= 2 and p[1].isdigit():
            r["nstat"][p[0]] = int(p[1])
    for m in re.finditer(r"dropped (\d+), overlimits (\d+)", "\n".join(s.get("qdisc", []))):
        r["qdisc_dropped"] += int(m.group(1))
        r["qdisc_overlimits"] += int(m.group(2))
    for line in s.get("ethtool-S", []):
        m = re.match(r"\s*(\S+):\s*(\d+)$", line)
        if m and re.search(r"drop|err|timeout|full", m.group(1)):
            r["virtio_drops"] += int(m.group(2))
    for line in s.get("ifstat", []):
        k, _, v = line.partition(" ")
        if v.strip().isdigit():
            r["if"][k] = int(v)
    r["softnet"] = [[int(x, 16) for x in line.split()[:3]] for line in s.get("softnet", []) if line.strip()]
    r["softirq"] = [int(line.split()[7]) for line in s.get("cpu", [])]
    r["ring"] = " ".join(x.split()[-1] for x in s.get("ethtool-g", []) if x.startswith(("RX:", "TX:")))[:40]
    return r


def delta(a: dict, b: dict) -> dict:
    dt = b["t"] - a["t"]
    d = {"dt": dt}
    d["rx_pps"] = (b["if"].get("rx_packets", 0) - a["if"].get("rx_packets", 0)) / dt
    d["tx_pps"] = (b["if"].get("tx_packets", 0) - a["if"].get("tx_packets", 0)) / dt
    d["if_drops"] = sum(b["if"].get(k, 0) - a["if"].get(k, 0) for k in ("rx_dropped", "tx_dropped", "rx_missed_errors"))
    for k in NSTAT:
        d[k] = (b["nstat"].get(k, 0) - a["nstat"].get(k, 0)) / dt
    d["qdisc_drop"] = (b["qdisc_dropped"] - a["qdisc_dropped"]) / dt
    d["virtio_drop"] = (b["virtio_drops"] - a["virtio_drops"]) / dt
    d["sn_drop"] = sum(y[1] - x[1] for x, y in zip(a["softnet"], b["softnet"])) / dt
    d["sn_squeeze"] = sum(y[2] - x[2] for x, y in zip(a["softnet"], b["softnet"])) / dt
    d["softirq_max"] = max((y - x for x, y in zip(a["softirq"], b["softirq"])), default=0) / dt  # 100 = one core
    return d


def host(dirp: Path, h: str) -> dict | None:
    a, b = reading(dirp / f"{h}.before"), reading(dirp / f"{h}.after")
    return delta(a, b) if a and b else None


def rtts(dirp: Path, h: str) -> str:
    p = dirp / f"{h}.rtt"
    xs = sorted(float(x) for x in p.read_text().split()) if p.exists() else []
    if not xs:
        return "—"
    q = lambda f: xs[min(len(xs) - 1, int(f * len(xs)))]  # noqa: E731
    return f"{q(0.5):.2f}/{q(0.99):.1f}"


def net_cols(d: dict | None) -> str:
    if not d:
        return "(no counters)"
    return (f"{d['rx_pps']:>8.0f} {d['tx_pps']:>8.0f} | {d['TcpRetransSegs']:>8.0f} {d['TcpExtTCPTimeouts']:>6.0f} | "
            f"{d['qdisc_drop']:>6.0f} {d['virtio_drop']:>6.0f} {d['if_drops']:>5.0f} {d['TcpExtTCPRcvQDrop'] + d['TcpExtTCPBacklogDrop']:>6.0f} "
            f"{d['UdpRcvbufErrors'] + d['UdpInErrors']:>7.0f} {d['sn_drop'] + d['sn_squeeze']:>6.0f} | {d['softirq_max']:>5.1f}")


NET_HDR = (f"{'rx pps':>8} {'tx pps':>8} | {'retr/s':>8} {'RTO/s':>6} | {'qdisc':>6} {'virtio':>6} {'ifdrp':>5} "
           f"{'tcpQ':>6} {'udpErr':>7} {'snet':>6} | {'sirq%':>5}")


def main(run: Path) -> None:
    for s in sorted(run.glob("setup-*.txt")):
        print(f"{s.stem}: {s.read_text().strip().splitlines()[-1] if s.read_text().strip() else '—'}")
    b0 = reading(next(iter(sorted(run.glob("*/a.before"))), Path("/nonexistent")))
    if b0:
        print(f"client NIC rings (ethtool -g, max then current, RX TX): {b0['ring']}")
    print()

    num = lambda p: int(p.name.split("-")[1].split("@")[0])  # noqa: E731
    arm = lambda p: p.name.partition("@")[2] or "default"  # noqa: E731
    udp = sorted(run.glob("udp-*"), key=lambda p: (arm(p), num(p)))
    if udp:
        print("UDP, 64 B datagrams, 8 streams: offered packets/s, delivered, lost")
        print(f"{'offered':>8} {'sent':>8} {'lost %':>7} | host  {NET_HDR}")
        for d in udp:
            try:
                j = json.loads((d / "client.txt").read_text())
                s = j["end"]["sum"]
                sent = s["packets"] / s["seconds"]
                lost = s.get("lost_percent", float("nan"))
            except Exception:  # noqa: BLE001 — a test whose output did not parse is shown as such
                sent, lost = float("nan"), float("nan")
            for h in ("a", "b"):
                lead = f"{num(d):>8} {sent:>8.0f} {lost:>7.2f}" if h == "a" else " " * 25
                print(f"{lead} | {h}     {net_cols(host(d, h))}")
        print()

    rr = sorted(run.glob("rr-*"), key=lambda p: (arm(p), num(p)))
    if rr:
        print("TCP request/response (neper tcp_rr), 200 B each way, closed loop")
        print(f"{'qdisc':>12} {'flows':>6} {'trans/s':>9} {'p50 ms':>7} {'p99 ms':>7} {'p99.9':>7} | host {'rtt p50/p99':>11}  {NET_HDR}")
        for d in rr:
            t = (d / "client.txt").read_text()
            val = lambda k: float(m.group(1)) if (m := re.search(rf"^{k}=([0-9.eE+-]+)", t, re.M)) else float("nan")  # noqa: E731
            lat = lambda k: val(k) * 1e3  # noqa: E731 — neper reports seconds
            flows = num
            for h in ("a", "b"):
                lead = (f"{arm(d):>12} {flows(d):>6} {val('throughput'):>9.0f} {lat('latency_p50'):>7.2f} "
                        f"{lat('latency_p99'):>7.2f} {lat('latency_p99.9'):>7.2f}") if h == "a" else " " * 52
                print(f"{lead} | {h}    {rtts(d, h):>11}  {net_cols(host(d, h))}")
        print()

    for bulk in sorted(run.glob("bulk*")):
        try:
            j = json.loads((bulk / "client.txt").read_text())
            gbps = j["end"]["sum_received"]["bits_per_second"] / 1e9
            retr = j["end"]["sum_sent"].get("retransmits", "—")
            print(f"TCP bulk ({arm(bulk)}), 8 streams: {gbps:.2f} Gbit/s received, {retr} retransmits")
        except Exception as e:  # noqa: BLE001
            print(f"TCP bulk ({arm(bulk)}): output did not parse ({e!r})")
    print()
    print("pps: the private interface's packets/s; retr/s: TCP retransmitted segments; RTO/s: retransmit timeouts;")
    print("qdisc/virtio/ifdrp/tcpQ/udpErr/snet: drops per second at the qdisc, the virtio queues (ethtool -S),")
    print("the interface, TCP's receive queue and backlog, UDP's buffers, softnet (drops + squeezes);")
    print("sirq%: the busiest CPU's softirq time (100 = one core). a = client, b = server.")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
