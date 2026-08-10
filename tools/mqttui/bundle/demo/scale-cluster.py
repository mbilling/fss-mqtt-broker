#!/usr/bin/env python3
"""Scale the demo cluster to N mqttd nodes (EXPERIMENT — branch experiment/7-node-demo).

Docker Compose can't loop to create N distinct, stateful services (each node needs its own
NODE_ID / PEER_BIND / SWIM_BIND / data volume / host ports), so the per-node topology is
*generated* instead of hand-maintained. This script is the single source of truth: it rewrites
the marked regions of

  - demo/docker-compose.yml   (follower services mqttd-2..N, data volumes d1..dN, loadgen list,
                               the header comment, and MQTTD_READY_MIN_MEMBERS = majority)
  - demo/alloy/config.alloy   (the Prometheus scrape targets)
  - demo/quic/gen-certs.sh    (the server cert SAN list)

mqttd-1 (the founder / playground / QUIC entry point, with its extra host ports) is hand-written
and left untouched; only the homogeneous followers mqttd-2..N are generated.

Usage:
    python3 demo/scale-cluster.py [N]     # default N = 7

Then rebuild the demo:
    cd demo && docker compose down -v && docker compose up --build

Host ports are assigned deterministically, skipping ones already taken by other services
(partner-broker 1886/8083, playground 8088, mqttd-1 8089, bridge 8090), so the mapping is stable
and collision-free as N changes.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DEMO = Path(__file__).resolve().parent

# Host ports already claimed by non-mqttd-follower services; never assign these.
RESERVED_MQTT_HOST = {1886, 1890}  # 1886 = partner-broker; 1890 = mqttd-1 internal WS port number
RESERVED_HEALTH_HOST = {8083, 8088, 8089, 8090}  # partner-broker, playground, mqttd-1 WS, bridge

MIN_NODES, MAX_NODES = 1, 20


def next_free(counter: int, reserved: set[int]) -> int:
    while counter in reserved:
        counter += 1
    return counter


def assign_ports(n: int) -> dict[int, tuple[int, int]]:
    """Map follower node index (2..N) -> (mqtt_host_port, health_host_port)."""
    mqtt, health = 1884, 8081
    ports: dict[int, tuple[int, int]] = {}
    for node in range(2, n + 1):
        mqtt = next_free(mqtt, RESERVED_MQTT_HOST)
        health = next_free(health, RESERVED_HEALTH_HOST)
        ports[node] = (mqtt, health)
        mqtt += 1
        health += 1
    return ports


def follower_service(node: int, mqtt_port: int, health_port: int) -> str:
    return f"""  mqttd-{node}:
    image: mqttd-demo
    hostname: mqttd-{node}
    environment:
      <<: *broker-env
      MQTTD_NODE_ID: mqttd-{node}
      MQTTD_PEER_BIND: mqttd-{node}:7001
      MQTTD_SWIM_BIND: mqttd-{node}:7946
      MQTTD_SWIM_SEEDS: mqttd-1:7946
    ports: ["{mqtt_port}:1883", "{health_port}:8080"]
    volumes: ["d{node}:/data", "quic-certs:/certs:ro"]
    depends_on:
      mqttd-1:
        condition: service_started
      quic-certs:
        condition: service_completed_successfully
"""


def replace_region(text: str, tag: str, comment: str, body: str) -> str:
    """Swap the content between the `>>> generated: <tag>` / `<<< generated: <tag>` markers.

    `comment` is the marker line prefix (`#` for YAML/sh, `//` for Alloy). The marker lines are
    re-emitted so the region stays self-describing and idempotent across re-runs.
    """
    start = re.escape(f"{comment} >>> generated: {tag}")
    end = re.escape(f"{comment} <<< generated: {tag} <<<")
    pattern = re.compile(rf"[ \t]*{start}.*?{end}", re.DOTALL)
    if not pattern.search(text):
        sys.exit(f"error: markers for '{tag}' not found (comment {comment!r})")
    # Replace via a function so backslashes in `body` (e.g. printf's literal \n) are NOT
    # interpreted as re.sub escape sequences.
    return pattern.sub(lambda _m: body.rstrip("\n"), text)


def scale(n: int) -> None:
    majority = n // 2 + 1
    ports = assign_ports(n)
    mqtt_ports = [1883] + [ports[i][0] for i in range(2, n + 1)]

    # --- docker-compose.yml --------------------------------------------------------------
    compose_path = DEMO / "docker-compose.yml"
    compose = compose_path.read_text()

    followers = (
        "  # >>> generated: follower nodes mqttd-2..N — edit demo/scale-cluster.py, not here >>>\n"
        + "\n".join(follower_service(i, *ports[i]) for i in range(2, n + 1))
        + "  # <<< generated: follower nodes <<<"
    )
    compose = replace_region(compose, "follower nodes", "#", followers)

    volumes = (
        "  # >>> generated: per-node data volumes d1..dN — edit demo/scale-cluster.py, not here >>>\n"
        + "".join(f"  d{i}:\n" for i in range(1, n + 1))
        + "  # <<< generated: per-node data volumes <<<"
    )
    compose = replace_region(compose, "per-node data volumes", "#", volumes)

    node_list = " ".join(f"mqttd-{i}" for i in range(1, n + 1))
    loadgen = (
        "    # >>> generated: loadgen node list — edit demo/scale-cluster.py, not here >>>\n"
        "    environment:\n"
        f'      LOADGEN_NODES: "{node_list}"\n'
        f"    depends_on: [{', '.join(f'mqttd-{i}' for i in range(1, n + 1))}]\n"
        "    # <<< generated: loadgen node list <<<"
    )
    compose = replace_region(compose, "loadgen node list", "#", loadgen)

    # Keyed single-line patches: header + readiness quorum.
    compose = re.sub(r"# A \d+-node durable", f"# A {n}-node durable", compose)
    mqtt_hosts = " / ".join(str(p) for p in mqtt_ports)
    compose = re.sub(
        r"(#   MQTT:\s+localhost:).*",
        rf"\g<1>{mqtt_hosts}  (mqttd-1..{n})",
        compose,
    )
    compose = re.sub(
        r'(MQTTD_READY_MIN_MEMBERS:\s*)"\d+"',
        rf'\g<1>"{majority}"',
        compose,
    )
    compose_path.write_text(compose)

    # --- alloy/config.alloy --------------------------------------------------------------
    alloy_path = DEMO / "alloy" / "config.alloy"
    alloy = alloy_path.read_text()
    targets = (
        "\t\t// >>> generated: broker scrape targets — edit demo/scale-cluster.py, not here >>>\n"
        + "".join(
            f'\t\t{{ __address__ = "mqttd-{i}:8080", instance = "mqttd-{i}" }},\n'
            for i in range(1, n + 1)
        )
        + "\t\t// <<< generated: broker scrape targets <<<"
    )
    alloy = replace_region(alloy, "broker scrape targets", "//", targets)
    alloy_path.write_text(alloy)

    # --- quic/gen-certs.sh (server SAN) --------------------------------------------------
    certs_path = DEMO / "quic" / "gen-certs.sh"
    certs = certs_path.read_text()
    san = ",".join(f"DNS:mqttd-{i}" for i in range(1, n + 1)) + ",DNS:localhost,IP:127.0.0.1"
    san_block = (
        "# >>> generated: server-cert SAN — edit demo/scale-cluster.py, not here >>>\n"
        f"printf 'subjectAltName={san}\\nextendedKeyUsage=serverAuth\\n' > \"$server_ext\"\n"
        "# <<< generated: server-cert SAN <<<"
    )
    certs = replace_region(certs, "server-cert SAN", "#", san_block)
    certs_path.write_text(certs)

    print(f"scaled demo cluster to {n} node(s):")
    print(f"  founder     mqttd-1  (host 1883, /metrics 8080, WS 8089, QUIC 8094/udp)")
    for i in range(2, n + 1):
        mp, hp = ports[i]
        print(f"  follower    mqttd-{i}  (host {mp}, /metrics {hp})")
    print(f"  readiness   MQTTD_READY_MIN_MEMBERS = {majority}  (majority of {n})")
    print("\nnext: cd demo && docker compose down -v && docker compose up --build")


def main() -> None:
    n = 7
    if len(sys.argv) > 1:
        try:
            n = int(sys.argv[1])
        except ValueError:
            sys.exit(f"error: node count must be an integer, got {sys.argv[1]!r}")
    if not MIN_NODES <= n <= MAX_NODES:
        sys.exit(f"error: node count must be between {MIN_NODES} and {MAX_NODES}, got {n}")
    scale(n)


if __name__ == "__main__":
    main()
