#!/usr/bin/env python3
"""Scrape one broker's /metrics on a fixed tick, each scrape to its own file.

    scrape-loop.py --url http://localhost:8080/metrics --out DIR [--interval 2] [--max-secs 3600]
    scrape-loop.py --url ... --out DIR --once

Runs on the broker's own host (bench/scale/rig2/run.sh starts it before a step's
generators and stops it after they exit), so every timestamp is the broker host's own
clock: rates come from differences of one host's readings, never across hosts.

Each complete exposition (mqttd ends every one with `# EOF`) is written to
`DIR/<unix_ms>.prom`, where `unix_ms` is the midpoint of the request. A scrape that
failed or came back truncated is not written; it is counted in `DIR/failed`. Writes go
to a temporary name first, so a reader never sees half a file. Stops on SIGTERM or
after --max-secs (a loop whose orchestrator vanished must not run forever).

With `--net PORT` each tick also writes `DIR/<unix_ms>.net.json`: the host's network
path, which the broker's own metrics cannot see. TCP round-trip times and retransmits
of the established sockets on PORT (`ss -ti`), softnet's per-CPU processed, dropped and
squeezed counts, per-CPU softirq time, and every interface's packet counters, queue
count and RPS mask. Counters are cumulative; a reader takes differences.

With `--host NAME` each tick also writes `DIR/<unix_ms>.host.json`, read right after the
scrape: the host's CPUs and disks, which the broker's metrics cannot see either.
/proc/stat's jiffies for the machine and per CPU (user, nice, system, idle, iowait, irq,
softirq, steal), its context switches and runnable tasks; the CPU of the newest process
named NAME (utime and stime from /proc/<pid>/stat, and its thread count); and every whole
disk's counters from /proc/diskstats (reads, writes, sectors, write and busy ms, flushes).
That is three small /proc reads a tick, beside `--net`'s listing of every socket. Counters
are cumulative; a reader takes differences by the sample's own `unix_ms`, in `clk_tck`
jiffies a second. A reading that fails is written as `{"error": ...}`: it never stops the
loop, whose scrapes the judge reads.
"""
import argparse
import json
import os
import re
import signal
import subprocess
import sys
import time
import urllib.request

RTT = re.compile(rb"\brtt:([0-9.]+)/")
RETRANS = re.compile(rb"\bretrans:\d+/(\d+)")
NOTSENT = re.compile(rb"\bnotsent:(\d+)")


def quantiles(xs: list[float]) -> dict:
    if not xs:
        return {"n": 0}
    xs = sorted(xs)
    at = lambda q: xs[min(len(xs) - 1, int(q * len(xs)))]  # noqa: E731
    return {"n": len(xs), "p50": at(0.5), "p90": at(0.9), "p99": at(0.99), "max": xs[-1]}


def read(path: str) -> str:
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def net_sample(port: int) -> dict:
    """One reading of the host's network path (see the module docstring)."""
    t0 = time.time()
    try:
        out = subprocess.run(["ss", "-tinH", "state", "established", f"( sport = :{port} )"],
                             capture_output=True, timeout=10).stdout
    except Exception:  # noqa: BLE001 — a missing reading, not a failed run
        out = b""
    rtts = [float(m.group(1)) for m in RTT.finditer(out)]
    retrans = sum(int(m.group(1)) for m in RETRANS.finditer(out))
    notsent = [int(m.group(1)) for m in NOTSENT.finditer(out)]
    softnet = [[int(x, 16) for x in line.split()[:3]] for line in read("/proc/net/softnet_stat").splitlines()]
    softirq_jiffies = [int(line.split()[7]) for line in read("/proc/stat").splitlines()
                       if re.match(r"cpu\d+ ", line)]
    ifaces = {}
    for i in sorted(os.listdir("/sys/class/net")) if os.path.isdir("/sys/class/net") else []:
        base = f"/sys/class/net/{i}"
        if i == "lo" or not os.path.isdir(f"{base}/statistics"):
            continue
        stat = {k: int(read(f"{base}/statistics/{k}") or 0)
                for k in ("rx_packets", "tx_packets", "rx_dropped", "tx_dropped", "rx_bytes", "tx_bytes")}
        qs = os.listdir(f"{base}/queues") if os.path.isdir(f"{base}/queues") else []
        stat["rx_queues"] = sum(q.startswith("rx-") for q in qs)
        stat["tx_queues"] = sum(q.startswith("tx-") for q in qs)
        stat["rps_cpus"] = read(f"{base}/queues/rx-0/rps_cpus").strip()
        ifaces[i] = stat
    return {"sockets": len(rtts), "rtt_ms": quantiles(rtts), "retrans_total": retrans,
            "notsent_bytes": quantiles([float(n) for n in notsent]),
            "softnet": softnet, "softirq_jiffies": softirq_jiffies, "ifaces": ifaces,
            "sample_s": round(time.time() - t0, 3)}


# /proc/stat's CPU columns, in order (proc(5)); guest time is already inside user.
CPU_FIELDS = ("user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal")


def find_process(name: str, proc: str = "/proc") -> int | None:
    """The pid of the newest process whose comm is `name`, or None."""
    best = None
    try:
        entries = os.listdir(proc)
    except OSError:
        return None
    for d in entries:
        if not d.isdigit() or read(f"{proc}/{d}/comm").strip() != name:
            continue
        st = read(f"{proc}/{d}/stat").rpartition(")")[2].split()
        if len(st) > 19 and (best is None or int(st[19]) > best[0]):
            best = (int(st[19]), int(d))
    return best[1] if best else None


def host_sample(name: str, pids: dict, proc: str = "/proc", sys_block: str = "/sys/block") -> dict:
    """One reading of the host's CPUs and disks and of process `name`'s CPU (see the
    module docstring). `pids` keeps the process's pid from one tick to the next; it is
    looked up again when that pid is gone or no longer `name` (a restarted broker)."""
    t0 = time.time()
    total, per_cpu, counts = [], [], {}
    for line in read(f"{proc}/stat").splitlines():
        f = line.split()
        if not f:
            continue
        if f[0] == "cpu":
            total = [int(x) for x in f[1:1 + len(CPU_FIELDS)]]
        elif re.fullmatch(r"cpu\d+", f[0]):
            per_cpu.append([int(x) for x in f[1:1 + len(CPU_FIELDS)]])
        elif f[0] in ("ctxt", "procs_running", "procs_blocked"):
            counts[f[0]] = int(f[1])
    pid = pids.get(name)
    if pid is None or read(f"{proc}/{pid}/comm").strip() != name:
        pid = pids[name] = find_process(name, proc)
    process = None
    if pid is not None:
        # proc(5)'s fields 14 (utime), 15 (stime), 20 (num_threads) and 22 (starttime),
        # counted from field 3 here: the comm before it may hold spaces and parentheses.
        st = read(f"{proc}/{pid}/stat").rpartition(")")[2].split()
        if len(st) > 19:
            process = {"pid": pid, "utime": int(st[11]), "stime": int(st[12]),
                       "threads": int(st[17]), "starttime": int(st[19])}
    disks = {}
    for line in read(f"{proc}/diskstats").splitlines():
        f = line.split()
        # Whole disks only (a partition's I/O is inside its disk's), never loop or ram.
        if len(f) < 14 or f[2].startswith(("loop", "ram")) or not os.path.isdir(f"{sys_block}/{f[2]}"):
            continue
        d = {"reads": int(f[3]), "read_sectors": int(f[5]), "writes": int(f[7]),
             "write_sectors": int(f[9]), "write_ms": int(f[10]), "io_ms": int(f[12])}
        if len(f) >= 20:  # Linux 5.5+: the flush requests an fsync sends a disk with a write cache
            d["flushes"], d["flush_ms"] = int(f[18]), int(f[19])
        disks[f[2]] = d
    return {"unix_ms": int(t0 * 1000), "clk_tck": os.sysconf("SC_CLK_TCK"), "cpu_fields": list(CPU_FIELDS),
            "cpu": total, "per_cpu": per_cpu, **counts, "process": process, "disks": disks,
            "sample_s": round(time.time() - t0, 4)}


def scrape(url: str, timeout: float) -> tuple[int, bytes | None]:
    t0 = time.time()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            body = r.read()
    except Exception:  # noqa: BLE001 — any failure is a missing sample, counted
        body = None
    t1 = time.time()
    if body is not None and not body.rstrip().endswith(b"# EOF"):
        body = None
    return int((t0 + t1) / 2 * 1000), body


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--max-secs", type=float, default=3600.0)
    ap.add_argument("--timeout", type=float, default=5.0)
    ap.add_argument("--once", action="store_true", help="one scrape, then exit (1 if it failed)")
    ap.add_argument("--net", type=int, metavar="PORT", help="also sample the host's network path for PORT")
    ap.add_argument("--host", metavar="NAME", help="also sample the host's CPUs and disks, and process NAME's CPU")
    a = ap.parse_args(argv)
    os.makedirs(a.out, exist_ok=True)
    stop = False

    def on_term(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, on_term)
    signal.signal(signal.SIGINT, on_term)
    failed = 0
    pids: dict = {}
    start = time.monotonic()
    k = 0
    while not stop:
        t_ms, body = scrape(a.url, a.timeout)
        if body is None:
            failed += 1
            with open(os.path.join(a.out, "failed"), "w") as f:
                f.write(f"{failed}\n")
        else:
            tmp = os.path.join(a.out, f".{t_ms}.tmp")
            with open(tmp, "wb") as f:
                f.write(body)
            os.replace(tmp, os.path.join(a.out, f"{t_ms}.prom"))
        if a.host:
            try:
                sample = host_sample(a.host, pids)
            except Exception as e:  # noqa: BLE001 — a missing reading, never a stopped scrape
                sample = {"error": repr(e)}
            tmp = os.path.join(a.out, f".{t_ms}.host.tmp")
            with open(tmp, "w") as f:
                json.dump(sample, f)
            os.replace(tmp, os.path.join(a.out, f"{t_ms}.host.json"))
        if a.net:
            tmp = os.path.join(a.out, f".{t_ms}.net.tmp")
            with open(tmp, "w") as f:
                json.dump(net_sample(a.net), f)
            os.replace(tmp, os.path.join(a.out, f"{t_ms}.net.json"))
        if a.once:
            return 0 if body is not None else 1
        k += 1
        nxt = start + k * a.interval
        if nxt - start > a.max_secs:
            break
        delay = nxt - time.monotonic()
        if delay > 0:
            time.sleep(delay)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
