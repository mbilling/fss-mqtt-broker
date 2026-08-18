#!/usr/bin/env bash
# The external-consumer blueprint, executed (docs/INTEGRATION.md, ADR 0063 / issue #251).
#
# docs/INTEGRATION.md is the rule-engine replacement story: a Kafka/webhook/DB
# sink is an ordinary MQTT consumer group — `$share` + QoS 1 + a durable
# persistent session — and the broker, not the sink, is the buffer. This script
# runs the document's load-bearing claims against the real binary with stock
# mosquitto clients, so the blueprint cannot silently rot:
#
#   1. GROUP SINGLE DELIVERY — two members of one `$share` group (one MQTT 5,
#      one 3.1.1, pinning the "3.1.1 members join the same group" claim) split
#      a QoS 1 stream: every message delivered, none delivered twice, both
#      members participate.
#   2. RETAINED SKIPS THE GROUP — a retained value is NOT replayed to a new
#      shared subscription (MQTT-3.8.4); the documented bootstrap — a plain
#      subscribe first — receives it.
#   3. QUEUE-WHILE-DOWN — with every member disconnected, QoS 1 publishes are
#      accepted and queued to the surviving persistent session, and replayed
#      IN ORDER when the member returns. This is the claim that makes the
#      pattern a buffer you do not build.
#
# Needs `mosquitto-clients`, `python3`, `curl` on PATH. Set MQTTD_BIN to skip
# the build. Exits non-zero if the documented pattern does not work.
set -euo pipefail
cd "$(dirname "$0")/.."

for tool in mosquitto_pub mosquitto_sub python3 curl; do
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

read -r PORT HEALTH < <(python3 -c "
import socket
ss=[socket.socket() for _ in range(2)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]")

MQTTD_NODE_ID=integration \
MQTTD_PLAINTEXT_BIND="127.0.0.1:$PORT" \
MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
MQTTD_ALLOW_ANONYMOUS=1 \
MQTTD_HEALTH_BIND="127.0.0.1:$HEALTH" \
RUST_LOG=off "$MQTTD_BIN" &
PIDS+=("$!")

for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$HEALTH/readyz" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$HEALTH/readyz" >/dev/null 2>&1 \
  || { echo "FATAL: broker never became ready"; exit 1; }

sub() { mosquitto_sub -h 127.0.0.1 -p "$PORT" "$@"; }
pub() { mosquitto_pub -h 127.0.0.1 -p "$PORT" "$@"; }

# The blueprint's contract, verbatim (docs/INTEGRATION.md "The contract"):
# QoS 1, clean_start=false, a long session expiry, a distinct stable client id
# per member. sink-a is MQTT 5; sink-b is 3.1.1 (CleanSession=0), pinning the
# claim that a 3.1.1 connector joins the same `$share` group.
#
# Background members invoke mosquitto_sub DIRECTLY (not via the sub() wrapper):
# backgrounding a function call forks a wrapper subshell, so `$!` would name the
# subshell and a later kill would orphan the still-connected client — which then
# fights its own successor in the takeover loop BRIDGE.md documents.
GROUP_FILTER='$share/sink/telemetry/#'
A_OUT="$WORK/a.txt"; B_OUT="$WORK/b.txt"
mosquitto_sub -h 127.0.0.1 -p "$PORT" -V mqttv5   -q 1 -c -x 3600 -i sink-a \
  -t "$GROUP_FILTER" -v >"$A_OUT" 2>/dev/null &
A_PID=$!; PIDS+=("$A_PID")
mosquitto_sub -h 127.0.0.1 -p "$PORT" -V mqttv311 -q 1 -c -i sink-b \
  -t "$GROUP_FILTER" -v >"$B_OUT" 2>/dev/null &
B_PID=$!; PIDS+=("$B_PID")
sleep 1  # let both subscriptions land before the stream starts

echo "── 1. group single delivery: two members split a QoS 1 stream ──"
N=10
for i in $(seq 1 "$N"); do
  pub -q 1 -i producer -t "telemetry/dev$i/state" -m "m$i" 2>/dev/null
done

lines() { cat "$A_OUT" "$B_OUT" 2>/dev/null | grep -c . || true; }
for _ in $(seq 1 50); do [[ "$(lines)" -ge "$N" ]] && break; sleep 0.2; done

TOTAL="$(lines)"
if [[ "$TOTAL" -ne "$N" ]]; then
  echo "  FAIL — published $N, the group received $TOTAL (single delivery means exactly once across members)"
  exit 1
fi
for i in $(seq 1 "$N"); do
  grep -q "^telemetry/dev$i/state m$i\$" "$A_OUT" "$B_OUT" || {
    echo "  FAIL — m$i was never delivered to any group member"; exit 1; }
done
A_GOT="$(grep -c . "$A_OUT" || true)"; B_GOT="$(grep -c . "$B_OUT" || true)"
if [[ "$A_GOT" -lt 1 || "$B_GOT" -lt 1 ]]; then
  echo "  FAIL — round-robin did not reach both members (v5: $A_GOT, 3.1.1: $B_GOT)"
  exit 1
fi
echo "  ok   — $N published, $N delivered exactly once across the group (v5 member: $A_GOT, 3.1.1 member: $B_GOT)"

echo "── 2. retained state skips the group; the documented plain-subscribe bootstrap sees it ──"
# Outside the group's filter on purpose: this probes the retained rule, not the group.
pub -q 1 -r -i producer -t 'retained/config' -m 'R1' 2>/dev/null
SHARED_R="$(sub -V mqttv5 -q 1 -i probe-shared -t '$share/probe/retained/config' -C 1 -W 2 2>/dev/null || true)"
if [[ -n "$SHARED_R" ]]; then
  echo "  FAIL — a NEW shared subscription received retained state [$SHARED_R]; MQTT-3.8.4 says it must not"
  exit 1
fi
PLAIN_R="$(sub -V mqttv5 -q 1 -i probe-plain -t 'retained/config' -C 1 -W 5 2>/dev/null || true)"
if [[ "$PLAIN_R" != "R1" ]]; then
  echo "  FAIL — the plain-subscribe bootstrap expected the retained value [R1], got [$PLAIN_R]"
  exit 1
fi
echo "  ok   — retained value invisible to a new \$share subscription, delivered to the plain bootstrap"

echo "── 3. queue-while-down: the whole group disconnects; QoS 1 messages wait in the durable session ──"
# Take the whole group offline. sink-b's session is REMOVED (a clean-start
# reconnect wipes it) so the queued fallback has exactly one persistent member
# to land on and the assertion below is deterministic.
kill "$A_PID" 2>/dev/null || true; wait "$A_PID" 2>/dev/null || true
kill "$B_PID" 2>/dev/null || true; wait "$B_PID" 2>/dev/null || true
sub -V mqttv311 -i sink-b -t 'noop' -C 1 -W 1 >/dev/null 2>&1 || true  # CleanSession=1 takeover: sink-b's session is gone

M=5
for i in $(seq 1 "$M"); do
  pub -q 1 -i producer -t 'telemetry/offline/q' -m "q$i" 2>/dev/null
done

# The member returns — same client id, clean_start=false — and the queue replays.
R_OUT="$WORK/replay.txt"
mosquitto_sub -h 127.0.0.1 -p "$PORT" -V mqttv5 -q 1 -c -x 3600 -i sink-a \
  -t "$GROUP_FILTER" -v >"$R_OUT" 2>/dev/null &
PIDS+=("$!")
for _ in $(seq 1 50); do [[ "$(grep -c . "$R_OUT" || true)" -ge "$M" ]] && break; sleep 0.2; done

GOT="$(grep '^telemetry/offline/q ' "$R_OUT" | sed 's/^telemetry\/offline\/q //' | paste -sd, - || true)"
WANT="$(seq -f 'q%g' 1 "$M" | paste -sd, -)"
if [[ "$GOT" != "$WANT" ]]; then
  echo "  FAIL — expected the offline stream replayed in order [$WANT], got [$GOT]"
  exit 1
fi
echo "  ok   — $M messages published with no member connected were queued and replayed in order on reconnect"

echo
echo "INTEGRATION BLUEPRINT SMOKE PASSED"
