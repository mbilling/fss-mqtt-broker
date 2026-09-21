#!/usr/bin/env python3
"""Real MQTT5/QoS1 smoke against local Mosquitto, with two shared consumers.

Uses isolated containers/loopback ports, removes only its own containers.
"""

import argparse
import importlib.util
import json
import subprocess
import tempfile
import time
import urllib.request
import uuid
from pathlib import Path

sp = importlib.util.spec_from_file_location(
    "e", Path(__file__).resolve().parent.parent / "lane-e-evidence.py"
)
e = importlib.util.module_from_spec(sp)
sp.loader.exec_module(e)
parser = argparse.ArgumentParser()
parser.add_argument("--image", default="fss-qos1-audit:local")
parser.add_argument(
    "--mqttd",
    type=Path,
    help="Use three isolated local FSS brokers instead of Mosquitto",
)
args = parser.parse_args()
cluster = None
cluster_tmp = None
prefix = "qos1-proof-" + uuid.uuid4().hex[:8]
names = []


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def start(suffix, *args):
    name = prefix + "-" + suffix
    names.append(name)
    docker("run", "-d", "--name", name, *args)


def http(port, path, method="GET"):
    return (
        urllib.request.urlopen(
            urllib.request.Request(f"http://127.0.0.1:{port}{path}", method=method),
            timeout=5,
        )
        .read()
        .decode()
    )


def metrics(port, p):
    p.write_text(http(port, "/metrics"))
    return e.metric(p)


def until(fn):
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            if fn():
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.1)
    raise RuntimeError("local test deadline exceeded")


try:
    if args.mqttd:
        cluster_tmp = tempfile.TemporaryDirectory()
        spec = importlib.util.spec_from_file_location(
            "canary", Path(__file__).resolve().parent.parent / "forward-canary.py"
        )
        canary = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(canary)
        cluster = canary.LocalCluster(
            args.mqttd.resolve(), 3, Path(cluster_tmp.name), None
        )
        cluster.start()
    else:
        start(
            "broker",
            "-p",
            "127.0.0.1:29883:1883",
            "eclipse-mosquitto:2",
            "sh",
            "-c",
            'printf "listener 1883\\nallow_anonymous true\\n" >/tmp/test.conf; exec mosquitto -c /tmp/test.conf',
        )
    with tempfile.TemporaryDirectory() as t:
        p = Path(t)
        for i in range(2):
            start(
                f"sub{i}",
                "--network",
                "host",
                "-e",
                "QOS1_AUDIT=1",
                "-e",
                "ERL_FLAGS=+S 1:1",
                args.image,
                "sub",
                "-h",
                "127.0.0.1",
                "-p",
                str(cluster.mqtt(i + 1) if cluster else 29883),
                "-V",
                "5",
                "-c",
                "2",
                "-t",
                "$share/test/site/0/#",
                "-q",
                "1",
                "-A",
                "true",
                "--payload-hdrs",
                "ts",
                "--prometheus",
                "--restapi",
                f"127.0.0.1:{29500 + i}",
            )
            until(
                lambda i=i: e.value(metrics(29500 + i, p / f"sub{i}.prom"), "sub") == 2
            )
        start(
            "pub",
            "--network",
            "host",
            "-e",
            "QOS1_AUDIT=1",
            "-e",
            "ERL_FLAGS=+S 1:1",
            args.image,
            "pub",
            "-h",
            "127.0.0.1",
            "-p",
            str(cluster.mqtt(0) if cluster else 29883),
            "-V",
            "5",
            "-c",
            "20",
            "-t",
            "site/0/%i",
            "-q",
            "1",
            "-I",
            "20",
            "-s",
            "200",
            "-A",
            "true",
            "--payload-hdrs",
            "ts,cnt64",
            "--keep-connected",
            "true",
            "--prometheus",
            "--restapi",
            "127.0.0.1:29502",
        )
        until(lambda: e.value(metrics(29502, p / "pub.prom"), "audit_acked") > 2000)
        poll_dir = p / "timed-poll"
        (poll_dir / ".poll").mkdir(parents=True)
        (poll_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "poll_timestamps": True,
                    "endpoints": [
                        {"name": f"sub{i}", "role": "sub", "site": "0"}
                        for i in range(2)
                    ],
                }
            )
        )
        for sample in range(2):
            for i in range(2):
                start_ms = time.time_ns() // 1_000_000
                raw = http(29500 + i, "/metrics")
                end_ms = time.time_ns() // 1_000_000
                (poll_dir / ".poll" / f"sub{i}.prom").write_text(
                    f"# POLL_STAMP_MS {start_ms}\n"
                    + raw
                    + f"\n# POLL_STAMP_MS {end_ms}\n"
                )
            e.poll(poll_dir, 1000)
            if sample == 0:
                time.sleep(0.25)
        bounds = json.loads((poll_dir / "poll-state.json").read_text())["rate_bounds"][
            "0"
        ]
        assert 0 < bounds[0] <= bounds[1]
        http(29502, "/audit/pause", "POST")
        until(
            lambda: (
                e.value(metrics(29502, p / "pub.prom"), "audit_paused_workers") == 20
            )
        )
        terminal = metrics(29502, p / "pub.prom")
        assert "erlang_vm_" not in (p / "pub.prom").read_text()
        sent = e.value(terminal, "audit_sent")
        assert sent == e.value(terminal, "audit_acked")
        until(
            lambda: (
                sum(
                    e.value(metrics(29500 + i, p / f"sub{i}.prom"), "recv")
                    for i in range(2)
                )
                == sent
            )
        )
        for i, n in [(0, "sub0"), (1, "sub1"), (2, "pub")]:
            (p / f"{n}.ledger").write_text(http(29500 + i, "/audit/ledger"))
        s, st = e.ledger([p / "pub.ledger"], "sent")
        a, at = e.ledger([p / "pub.ledger"], "acked")
        r, rt = e.ledger([p / "sub0.ledger", p / "sub1.ledger"], "received")
        result = e.reconcile(s, a, r)
        assert st == at == rt == sent
        assert all(
            result[k] == 0
            for k in ["missing", "unexpected", "unacked", "unexpected_acks"]
        )
        assert e.value(terminal, "puback_latency_count") == sent
        time.sleep(0.25)
        assert e.value(metrics(29502, p / "pub.prom"), "audit_sent") == sent
        print(
            json.dumps(
                {
                    "status": "PASS",
                    "broker": "FSS three-node cross-node shared group"
                    if cluster
                    else "Mosquitto",
                    "sent": sent,
                    "shared_containers": 2,
                    "timed_poll_rate_bounds": bounds,
                    "ledger": result,
                }
            )
        )
finally:
    if names:
        subprocess.run(
            ["docker", "rm", "-f", *names], stdout=subprocess.DEVNULL, check=False
        )
    if cluster is not None:
        cluster.stop_all()
    if cluster_tmp is not None:
        cluster_tmp.cleanup()
