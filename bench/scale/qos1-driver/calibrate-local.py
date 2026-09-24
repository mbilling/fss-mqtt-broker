#!/usr/bin/env python3
"""Same-host driver calibration against isolated FSS processes, not cloud capacity."""

import argparse
import hashlib
import importlib.util
import json
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


base = Path(__file__).resolve().parent.parent
e = module("evidence", base / "lane-e-evidence.py")
c = module("canary", base / "forward-canary.py")
p = argparse.ArgumentParser()
p.add_argument("--mqttd", required=True, type=Path)
p.add_argument("--output", required=True, type=Path)
p.add_argument("--image", default="fss-qos1-audit:local")
p.add_argument("--publishers", type=int, default=3000)
p.add_argument("--containers", type=int, default=2)
p.add_argument("--seconds", type=int, default=30)
p.add_argument("--publisher-brokers", type=int, choices=[1, 2, 3], default=2)
a = p.parse_args()
if a.publishers % a.containers or min(a.publishers, a.containers, a.seconds) <= 0:
    p.error("positive parameters and evenly divided publishers required")
a.output.mkdir(parents=True, exist_ok=False)
root = a.output
image_id = subprocess.check_output(
    ["docker", "image", "inspect", "--format", "{{.Id}}", a.image], text=True
).strip()
(root / "config.json").write_text(
    json.dumps(
        {
            **vars(a),
            "image_id": image_id,
            "broker_sha256": hashlib.sha256(a.mqttd.read_bytes()).hexdigest(),
        },
        default=str,
        indent=2,
    )
)
(root / "brokers").mkdir()
cluster = c.LocalCluster(a.mqttd.resolve(), 3, root / "brokers", None)
prefix = "qos1-cal-" + uuid.uuid4().hex[:8]
endpoints = []
ports = c._free_ports(3 + a.containers)


def request(ep, path="/metrics", method="GET"):
    start = time.monotonic()
    text = (
        urllib.request.urlopen(
            urllib.request.Request(
                f"http://127.0.0.1:{ep['port']}{path}", method=method
            ),
            timeout=30,
        )
        .read()
        .decode()
    )
    return text, start, time.monotonic()


def snapshot(ep, label):
    raw, start, end = request(ep)
    (root / f"{ep['name']}-{label}.prom").write_text(raw)
    return {
        "start": start,
        "end": end,
        "at": (start + end) / 2,
        "metrics": e.metric_text(raw, ep["name"]),
    }


def until(fn):
    end = time.monotonic() + 60
    while time.monotonic() < end:
        try:
            if fn():
                return
        except OSError:
            pass
        time.sleep(0.2)
    raise RuntimeError("calibration deadline exceeded")


def launch(role, index, clients, node, offset=0):
    ep = {
        "name": f"{role}{index}",
        "role": role,
        "clients": clients,
        "port": ports[len(endpoints)],
    }
    endpoints.append(ep)
    command = [
        "docker",
        "run",
        "-d",
        "--name",
        prefix + "-" + ep["name"],
        "--network",
        "host",
        "--ulimit",
        "nofile=1048576:1048576",
        "-e",
        "QOS1_AUDIT=1",
        "-e",
        "ERL_FLAGS=+S 1:1 +sbwt none +sbwtdcpu none +sbwtdio none",
        image_id,
        role,
        "-h",
        "127.0.0.1",
        "-p",
        str(cluster.mqtt(node)),
        "-V",
        "5",
        "-c",
        str(clients),
        "-R",
        "500",
        "-q",
        "1",
        "-A",
        "true",
        "--prometheus",
        "--restapi",
        f"127.0.0.1:{ep['port']}",
    ]
    if role == "sub":
        command += ["-t", "$share/cal/site/0/#", "--payload-hdrs", "ts"]
    else:
        command += [
            "-t",
            "site/0/%i",
            "-n",
            str(offset),
            "-s",
            "200",
            "-I",
            "100",
            "--payload-hdrs",
            "ts,cnt64",
            "--keep-connected",
            "true",
        ]
    (root / (ep["name"] + "-command.json")).write_text(json.dumps(command))
    subprocess.run(command, check=True, stdout=subprocess.DEVNULL)
    return ep


try:
    cluster.start()
    for i, count in enumerate([4, 3, 3]):
        ep = launch("sub", i, count, i)
        until(
            lambda ep=ep: (
                e.value(snapshot(ep, "ready")["metrics"], "sub") == ep["clients"]
            )
        )
    for i in range(a.containers):
        launch(
            "pub",
            i,
            a.publishers // a.containers,
            i % a.publisher_brokers,
            i * (a.publishers // a.containers),
        )
    for ep in endpoints:
        if ep["role"] == "pub":
            until(
                lambda ep=ep: (
                    e.value(snapshot(ep, "ready")["metrics"], "connect_succ")
                    == ep["clients"]
                )
            )
    time.sleep(10)
    samples = []
    start = time.monotonic()
    while True:
        samples.append(
            {ep["name"]: snapshot(ep, f"sample{len(samples)}") for ep in endpoints}
        )
        if time.monotonic() - start >= a.seconds:
            break
        time.sleep(5)
    pubs = [v for v in endpoints if v["role"] == "pub"]
    subs = [v for v in endpoints if v["role"] == "sub"]
    for ep in pubs:
        request(ep, "/audit/pause", "POST")
    for ep in pubs:
        until(
            lambda ep=ep: (
                e.value(snapshot(ep, "paused")["metrics"], "audit_paused_workers")
                == ep["clients"]
            )
        )
    finals = {ep["name"]: snapshot(ep, "terminal")["metrics"] for ep in pubs}
    sent = sum(e.value(v, "audit_sent") for v in finals.values())
    assert sent == sum(e.value(v, "audit_acked") for v in finals.values())
    until(
        lambda: (
            sum(e.value(snapshot(ep, "terminal")["metrics"], "recv") for ep in subs)
            == sent
        )
    )
    for ep in endpoints:
        raw, _, _ = request(ep, "/audit/ledger")
        (root / f"{ep['name']}.ledger").write_text(raw)
    sent_bits, st = e.ledger([root / f"{v['name']}.ledger" for v in pubs], "sent")
    ack_bits, at = e.ledger([root / f"{v['name']}.ledger" for v in pubs], "acked")
    recv_bits, rt = e.ledger([root / f"{v['name']}.ledger" for v in subs], "received")
    accounting = e.reconcile(sent_bits, ack_bits, recv_bits)
    assert st == at == rt == sent == accounting["unique_delivered"]
    assert all(
        accounting[k] == 0
        for k in ["missing", "unexpected", "unacked", "unexpected_acks"]
    )
    rates = {"sent": 0.0, "acked": 0.0, "received": 0.0, "late": 0.0}
    for ep in endpoints:
        first, last = samples[0][ep["name"]], samples[-1][ep["name"]]
        secs = last["at"] - first["at"]
        pairs = (
            [("received", "recv")]
            if ep["role"] == "sub"
            else [
                ("sent", "audit_sent"),
                ("acked", "audit_acked"),
                ("late", "pub_overrun"),
            ]
        )
        for label, metric in pairs:
            rates[label] += e.delta(first["metrics"], last["metrics"], metric) / secs
    durations = [v["end"] - v["start"] for sample in samples for v in sample.values()]
    buckets = {}
    histogram_count = 0
    for ep in subs:
        h, count = e.histogram(
            samples[0][ep["name"]]["metrics"],
            samples[-1][ep["name"]]["metrics"],
            "e2e_latency",
        )
        histogram_count += count
        for bound, value in h.items():
            buckets[bound] = buckets.get(bound, 0) + value
    p99 = next(
        (
            bound
            for bound in sorted(buckets)
            if buckets[bound] >= histogram_count * 0.99
        ),
        float("inf"),
    )
    result = {
        "scope": "local calibration only",
        "publisher_containers": a.containers,
        "publisher_brokers": a.publisher_brokers,
        "p99_upper_ms": p99,
        "requested": a.publishers * 10,
        "image": a.image,
        "image_id": image_id,
        "rates": rates,
        "late_share": rates["late"] / rates["sent"],
        "max_scrape_seconds": max(durations),
        "lifetime_sent": sent,
        "accounting": accounting,
    }
    result["pass"] = (
        min(rates[k] for k in ["sent", "acked", "received"]) >= a.publishers * 10 * 0.97
        and result["late_share"] <= 0.05
        and result["max_scrape_seconds"] <= a.seconds * 0.02
        and p99 <= 1000
    )
    (root / "sample-times.json").write_text(
        json.dumps(
            [
                {
                    name: {
                        key: value for key, value in snap.items() if key != "metrics"
                    }
                    for name, snap in sample.items()
                }
                for sample in samples
            ],
            indent=2,
        )
    )
    (root / "endpoints.json").write_text(json.dumps(endpoints, indent=2))
    (root / "result.json").write_text(json.dumps(result, indent=2))
    (root / "scrape-durations.json").write_text(json.dumps(durations))
    print(json.dumps(result, indent=2))
finally:
    for ep in endpoints:
        name = prefix + "-" + ep["name"]
        with (root / f"{ep['name']}.log").open("w") as f:
            subprocess.run(
                ["docker", "logs", name],
                stdout=f,
                stderr=subprocess.STDOUT,
                check=False,
            )
        subprocess.run(
            ["docker", "rm", "-f", name], stdout=subprocess.DEVNULL, check=False
        )
    cluster.stop_all()
