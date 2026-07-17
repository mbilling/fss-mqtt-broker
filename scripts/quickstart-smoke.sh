#!/usr/bin/env bash
# Quickstart-as-test (ADR 0044 P7): the README's own two-node cluster commands,
# executed verbatim, so the documented getting-started path can never silently
# rot. If the README's quickstart stops working, this fails.
#
# The ONLY deviations from the README block are mechanical, not semantic:
#   - a prebuilt $MQTTD_BIN instead of `cargo run --bin mqttd` (speed; same binary);
#   - ephemeral ports instead of the literal :1883/:1884/:7001/… (so the smoke
#     runs on a busy CI box) — the env-var *shape* the README documents is
#     exactly what is exercised;
#   - a MQTTD_HEALTH_BIND per node, so readiness is polled instead of slept on.
# Everything else — gossip discovery via SWIM seeds, no static peer list, a
# publish on node B delivered to a subscriber on node A — is the README's flow.
#
# Needs `mosquitto-clients`, `python3`, `curl` on PATH. Set MQTTD_BIN to skip the
# build. Exits non-zero if the documented quickstart does not work.
set -euo pipefail
cd "$(dirname "$0")/.."

for tool in mosquitto_pub mosquitto_sub python3 curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

MQTTD_BIN="${MQTTD_BIN:-}"
if [[ -z "$MQTTD_BIN" ]]; then
  echo "building mqttd (set MQTTD_BIN to skip)…"
  cargo build --quiet -p mqttd
  MQTTD_BIN="target/debug/mqttd"
fi
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN"; exit 2; }

WORK="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT

read -r A_CLIENT B_CLIENT A_PEER B_PEER A_SWIM B_SWIM A_HEALTH B_HEALTH \
  < <(python3 -c "
import socket
ss=[socket.socket() for _ in range(8)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]")

wait_ready() { # <health_port> <name>
  for _ in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$1/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "FATAL: $2 never became ready on health port $1"; exit 1
}

echo "── The README quickstart: two-node cluster via gossip discovery ──"

# Node A — the seed (README: client :1883, peer :7001, gossip :7946).
MQTTD_NODE_ID=node-a \
MQTTD_PLAINTEXT_BIND="127.0.0.1:$A_CLIENT" \
MQTTD_PEER_BIND="127.0.0.1:$A_PEER" \
MQTTD_SWIM_BIND="127.0.0.1:$A_SWIM" \
MQTTD_ALLOW_ANONYMOUS=1 \
MQTTD_HEALTH_BIND="127.0.0.1:$A_HEALTH" \
RUST_LOG=off "$MQTTD_BIN" &
PIDS+=("$!")

# Node B — seeds off A (README: client :1884, peer :7002, gossip :7947,
# MQTTD_SWIM_SEEDS pointing at A's gossip address).
MQTTD_NODE_ID=node-b \
MQTTD_PLAINTEXT_BIND="127.0.0.1:$B_CLIENT" \
MQTTD_PEER_BIND="127.0.0.1:$B_PEER" \
MQTTD_SWIM_BIND="127.0.0.1:$B_SWIM" \
MQTTD_SWIM_SEEDS="127.0.0.1:$A_SWIM" \
MQTTD_ALLOW_ANONYMOUS=1 \
MQTTD_HEALTH_BIND="127.0.0.1:$B_HEALTH" \
RUST_LOG=off "$MQTTD_BIN" &
PIDS+=("$!")

wait_ready "$A_HEALTH" node-a
wait_ready "$B_HEALTH" node-b

# The mesh forms through gossip (no static peer list): wait until each node's
# /readyz reports both members before crossing the cluster.
mesh_ready() { # <health_port>
  curl -fsS "http://127.0.0.1:$1/readyz" 2>/dev/null | grep -q '"members":2'
}
for _ in $(seq 1 150); do
  if mesh_ready "$A_HEALTH" && mesh_ready "$B_HEALTH"; then break; fi
  sleep 0.2
done
mesh_ready "$A_HEALTH" || { echo "FATAL: the two nodes never discovered each other via gossip"; exit 1; }
echo "  ok   — the two nodes formed a mesh through SWIM gossip (no static peer list)"

# The README's cross-node flow: subscribe on A, publish on B, expect delivery.
OUT="$WORK/telemetry.txt"
mosquitto_sub -h 127.0.0.1 -p "$A_CLIENT" -i qs-sub -t 'fleet/+/telemetry' -C 1 -W 8 >"$OUT" 2>/dev/null &
SUB=$!
sleep 1  # let the subscription + cross-node interest gossip settle
mosquitto_pub -h 127.0.0.1 -p "$B_CLIENT" -i qs-pub -t 'fleet/truck7/telemetry' -m 'hi' 2>/dev/null || true
wait "$SUB" 2>/dev/null || true

GOT="$(cat "$OUT")"
if [[ "$GOT" == "hi" ]]; then
  echo "  ok   — a publish on node B was delivered to a subscriber on node A"
  echo "QUICKSTART OK"
else
  echo "  FAIL — cross-node delivery: expected [hi] got [$GOT]"
  echo "QUICKSTART FAILED"
  exit 1
fi
