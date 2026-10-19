#!/usr/bin/env bash
# Will (Last Will and Testament) scenarios against the real binary, in both session modes.
#
# demo/rules-live/feeds/wills.py plays sixteen ways for a connection to end (MQTT 3.1.1 and 5:
# a dropped socket, a silent keep-alive, a protocol error, DISCONNECT with and without
# reason 0x04, Will Delay, a session that ends first, a resume inside the delay, three kinds
# of takeover, a retained Will, Will properties in order) and checks each Will against the
# spec: when it must arrive, what it carries, and when it must not arrive at all. The demo
# stack runs the same scenarios round after round; here each runs once, against mqttd with
# the durable session store and with the in-memory one (MQTTD_DURABLE_SESSIONS=0).
#
# Where mqttd differs from MQTT today the scenario is listed as expected to fail, with its
# issue; the run then fails when such a scenario starts passing, so the list stays honest.
#
# Needs python3 and curl. Set MQTTD_BIN to skip the build.
set -euo pipefail
cd "$(dirname "$0")/.."

for tool in python3 curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

MQTTD_BIN="${MQTTD_BIN:-}"
if [[ -z "$MQTTD_BIN" ]]; then
  echo "building mqttd (set MQTTD_BIN to reuse an existing build)…"
  cargo build --quiet -p mqttd
  MQTTD_BIN="target/debug/mqttd"
fi
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN"; exit 2; }

WORK="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT

# Known differences, by session mode: #1033 (Will Message Expiry, takeover inside the
# Will Delay) in both; #1034 (an empty in-memory session is not kept) without the store.
EXPECT_DURABLE="v5-drop-props,v5-takeover-resumed"
EXPECT_MEMORY="$EXPECT_DURABLE,v5-resumed"

run() {
  local mode="$1" expect="$2"
  local port health
  read -r port health < <(python3 -c "
import socket
ss=[socket.socket() for _ in range(2)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]")
  local env=(MQTTD_NODE_ID="wills-$mode" MQTTD_PLAINTEXT_BIND="127.0.0.1:$port"
             MQTTD_ALLOW_ANONYMOUS=1 MQTTD_HEALTH_BIND="127.0.0.1:$health")
  if [[ "$mode" == memory ]]; then
    env+=(MQTTD_DURABLE_SESSIONS=0)
  else
    mkdir -p "$WORK/data"
    env+=(MQTTD_DATA_DIR="$WORK/data")
  fi
  env "${env[@]}" RUST_LOG=off "$MQTTD_BIN" &
  local pid=$!
  PIDS+=("$pid")
  for _ in $(seq 1 100); do
    curl -fsS "http://127.0.0.1:$health/readyz" >/dev/null 2>&1 && break
    sleep 0.2
  done
  curl -fsS "http://127.0.0.1:$health/readyz" >/dev/null || { echo "FATAL: mqttd ($mode) never became ready"; exit 1; }
  # A node founding a new cluster reports ready a second or two before it can commit
  # (#1035): wait until a retained message comes back, so the scenarios test Wills.
  PYTHONPATH=demo/rules python3 - "$port" <<'PY' || { echo "FATAL: mqttd ($mode) never kept a retained message"; exit 1; }
import sys, time
from sim.mqtt import Client, MqttError
port, deadline = int(sys.argv[1]), time.monotonic() + 30
while time.monotonic() < deadline:
    try:
        c = Client("127.0.0.1", port, "wills-smoke-probe", timeout=2)
        c.connect()
        c.publish("wills/probe", b"ok", qos=1, retain=True)
        c.disconnect()
        r = Client("127.0.0.1", port, "wills-smoke-probe-read", timeout=2)
        r.connect()
        r.subscribe([("wills/probe", 1)])
        got = r.poll(1.0)
        r.publish("wills/probe", b"", qos=1, retain=True)
        r.disconnect()
        if got is not None and got.retain:
            sys.exit(0)
    except (MqttError, OSError):
        pass
    time.sleep(0.5)
sys.exit(1)
PY
  echo "== session store: $mode"
  python3 demo/rules-live/feeds/wills.py --once --quiet --port "$port" --expect-fail "$expect"
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

run durable "$EXPECT_DURABLE"
run memory "$EXPECT_MEMORY"
echo "Will scenarios: as expected in both session modes"
