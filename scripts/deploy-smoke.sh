#!/usr/bin/env bash
# The non-Kubernetes reference deployment, as a test (ADR 0047 T9).
#
# `deploy/compose/` and `deploy/systemd/` are the supported way to run mqttd off
# Kubernetes. An untested deployment artifact is a promise, not a deliverable — and the
# specific way these rot is silent: a renamed environment variable, a seed list that no
# longer forms a cluster, a readiness floor that lets a minority node serve traffic. None
# of that shows up in a unit test.
#
# So this boots a real THREE-NODE cluster using the values from the SHIPPED files —
# deploy/systemd/mqttd.env.example is parsed, not re-typed here — and proves:
#
#   1. authentication is actually on (anonymous is refused);
#   2. the generated password file authenticates (mqttd --hash-password end to end);
#   3. the ACL is enforced (a device cannot read another device's topic);
#   4. the cluster forms and routes ACROSS nodes;
#   5. an acknowledged QoS 1 message survives the loss of the node that accepted it;
#   6. a minority node reports NOT ready (so a load balancer drops it).
#
# The deviations from the shipped file are mechanical, not semantic, and are exactly the
# four edits it tells operators to make (identity, seeds, readiness floor, paths) plus
# ephemeral ports so this runs on a busy CI box.
#
# The compose file and the systemd unit are additionally validated with their own tools
# when those are available (`docker compose config`, `systemd-analyze verify`); both are
# SKIPPED LOUDLY rather than silently when they are not.
#
# Needs: mosquitto-clients, python3, curl. Set MQTTD_BIN to skip the build.
set -euo pipefail
cd "$(dirname "$0")/.."

for tool in mosquitto_pub mosquitto_sub python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

MQTTD_BIN="${MQTTD_BIN:-}"
if [[ -z "$MQTTD_BIN" ]]; then
  echo "building mqttd (set MQTTD_BIN to skip)…"
  cargo build --quiet -p mqttd
  MQTTD_BIN="target/debug/mqttd"
fi
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN"; exit 2; }
MQTTD_BIN="$(cd "$(dirname "$MQTTD_BIN")" && pwd)/$(basename "$MQTTD_BIN")"

ENV_FILE="deploy/systemd/mqttd.env.example"
COMPOSE_FILE="deploy/compose/compose.yaml"
ACL_FILE="deploy/compose/acl.toml"
for f in "$ENV_FILE" "$COMPOSE_FILE" "$ACL_FILE"; do
  [[ -r "$f" ]] || { echo "FATAL: missing deployment artifact: $f"; exit 2; }
done

WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────────
# 0. The shipped env file is the source of truth for everything except the four
#    per-node values it tells you to edit. Read it rather than restating it, so a
#    setting renamed in the artifact and not here fails loudly.
# ─────────────────────────────────────────────────────────────────────────────────
shipped() { # <VAR> -> its uncommented value in the example env file, or ""
  sed -n "s/^$1=//p" "$ENV_FILE" | tail -1
}

for required in MQTTD_DATA_DIR MQTTD_PASSWORD_FILE MQTTD_ACL_FILE MQTTD_SWIM_KEY_FILE \
                MQTTD_READY_MIN_MEMBERS MQTTD_SHUTDOWN_GRACE MQTTD_PLAINTEXT_BIND \
                MQTTD_HEALTH_BIND MQTTD_PEER_BIND MQTTD_SWIM_BIND; do
  [[ -n "$(shipped "$required")" ]] \
    || fail "$ENV_FILE no longer sets $required — the reference deployment and this test have drifted"
done
pass "$ENV_FILE sets every variable the reference deployment needs"

# The example must NOT enable anonymous access; a reference deployment that did would
# teach the wrong thing to everyone who copies it.
grep -qE '^MQTTD_ALLOW_ANONYMOUS' "$ENV_FILE" \
  && fail "$ENV_FILE enables anonymous access"
grep -qE '^\s*MQTTD_ALLOW_ANONYMOUS' "$COMPOSE_FILE" \
  && fail "$COMPOSE_FILE enables anonymous access"
pass "neither artifact enables anonymous access"

# ─────────────────────────────────────────────────────────────────────────────────
# 1. Secrets, made the way bootstrap.sh makes them — with the broker's own hasher.
# ─────────────────────────────────────────────────────────────────────────────────
SWIM_KEY="$WORK/swim.key"
python3 -c "import secrets;print(secrets.token_hex(32))" > "$SWIM_KEY"

PW_FILE="$WORK/passwd"
: > "$PW_FILE"
DEVICE_A_PW='device-a-secret'
DEVICE_B_PW='device-b-secret'
BACKEND_PW='backend-secret'
printf %s "$DEVICE_A_PW" | "$MQTTD_BIN" --hash-password device-a >> "$PW_FILE"
printf %s "$DEVICE_B_PW" | "$MQTTD_BIN" --hash-password device-b >> "$PW_FILE"
printf %s "$BACKEND_PW"  | "$MQTTD_BIN" --hash-password backend  >> "$PW_FILE"
[[ $(wc -l < "$PW_FILE") -eq 3 ]] || fail "the password file should have three lines"
pass "mqttd --hash-password produced a three-user password file"

# ─────────────────────────────────────────────────────────────────────────────────
# 2. Three nodes, configured from the shipped file.
# ─────────────────────────────────────────────────────────────────────────────────
read -r P1 P2 P3 H1 H2 H3 PB1 PB2 PB3 SW1 SW2 SW3 < <(python3 -c "
import socket
ss=[socket.socket() for _ in range(12)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]")

start_node() { # <n> <client_port> <health_port> <peer_port> <swim_port> <seeds> <ready_min>
  local n="$1" cport="$2" hport="$3" pport="$4" sport="$5" seeds="$6" ready="$7"
  local dir="$WORK/node$n"
  mkdir -p "$dir"
  env -i \
    PATH="$PATH" HOME="$HOME" \
    MQTTD_NODE_ID="mqttd-$n" \
    MQTTD_DATA_DIR="$dir" \
    MQTTD_PLAINTEXT_BIND="127.0.0.1:$cport" \
    MQTTD_HEALTH_BIND="127.0.0.1:$hport" \
    MQTTD_PEER_BIND="127.0.0.1:$pport" \
    MQTTD_PEER_ADVERTISE="127.0.0.1:$pport" \
    MQTTD_SWIM_BIND="127.0.0.1:$sport" \
    MQTTD_SWIM_SEEDS="$seeds" \
    MQTTD_SWIM_KEY_FILE="$SWIM_KEY" \
    MQTTD_PASSWORD_FILE="$PW_FILE" \
    MQTTD_ACL_FILE="$ACL_FILE" \
    MQTTD_READY_MIN_MEMBERS="$ready" \
    MQTTD_SHUTDOWN_GRACE="$(shipped MQTTD_SHUTDOWN_GRACE)" \
    RUST_LOG=warn \
    "$MQTTD_BIN" > "$dir/log" 2>&1 &
  PIDS+=($!)
  echo $!
}

# Node 1 founds the cluster BECAUSE it has no seeds, and needs a floor of 1 to come up
# alone — exactly what the shipped env file documents.
N1=$(start_node 1 "$P1" "$H1" "$PB1" "$SW1" "" 1)
N2=$(start_node 2 "$P2" "$H2" "$PB2" "$SW2" "127.0.0.1:$SW1" "$(shipped MQTTD_READY_MIN_MEMBERS)")
N3=$(start_node 3 "$P3" "$H3" "$PB3" "$SW3" "127.0.0.1:$SW1,127.0.0.1:$SW2" "$(shipped MQTTD_READY_MIN_MEMBERS)")

# Readiness via the broker's own probe — the same command the compose healthcheck runs,
# so this covers that too.
wait_ready() { # <health_port> <name>
  local port="$1" name="$2"
  for _ in $(seq 1 300); do
    if MQTTD_HEALTH_BIND="127.0.0.1:$port" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "--- $name log ---"; tail -30 "$WORK/node${name#mqttd-}/log" 2>/dev/null || true
  fail "$name never became ready on health port $port"
}

wait_ready "$H1" mqttd-1
wait_ready "$H2" mqttd-2
wait_ready "$H3" mqttd-3
pass "three nodes formed a cluster and all report /readyz 200"

# ─────────────────────────────────────────────────────────────────────────────────
# 3. The security posture the artifacts claim.
# ─────────────────────────────────────────────────────────────────────────────────
if mosquitto_pub -h 127.0.0.1 -p "$P1" -t 'devices/device-a/up/t' -m x -q 1 \
     -i anon-probe >/dev/null 2>&1; then
  fail "an ANONYMOUS client was accepted — the reference deployment is not secure by default"
fi
pass "anonymous clients are refused"

if mosquitto_pub -h 127.0.0.1 -p "$P1" -t 'devices/device-a/up/t' -m x -q 1 \
     -u device-a -P 'wrong-password' -i bad-pw >/dev/null 2>&1; then
  fail "a WRONG password was accepted"
fi
pass "a wrong password is refused"

mosquitto_pub -h 127.0.0.1 -p "$P1" -t 'devices/device-a/up/hello' -m 'hi' -q 1 \
  -u device-a -P "$DEVICE_A_PW" -i pw-ok >/dev/null 2>&1 \
  || fail "the generated password file did not authenticate a legitimate user"
pass "a hashed password from --hash-password authenticates against a running broker"

# The ACL: device-b must not reach device-a's subtree. A denied SUBSCRIBE is answered
# with SUBACK 0x80 (128) — and mosquitto_sub still EXITS 0 in that case, so the exit
# code is not the signal. The SUBACK code is, and `-d` is what prints it.
suback_code() { # <port> <topic> <user> <password> -> the granted-QoS byte, or "none"
  # `|| true`: a subscription that is granted but never receives a message exits 27
  # (timed out via -W), which `set -o pipefail` would otherwise treat as a script error.
  # The SUBACK code is the answer here, not the exit status.
  local out
  out="$(mosquitto_sub -h 127.0.0.1 -p "$1" -t "$2" -C 1 -W 2 -u "$3" -P "$4" \
    -i "acl-probe-$$-$RANDOM" -d 2>&1 || true)"
  sed -n 's/^Subscribed (mid: [0-9]*): //p' <<<"$out" | head -1
}

denied="$(suback_code "$P1" 'devices/device-a/down/#' device-b "$DEVICE_B_PW")"
[[ "$denied" == "128" ]] \
  || fail "device-b's SUBSCRIBE to device-a's topics returned '${denied:-none}', expected 128 (denied) — the ACL is not enforced"

# The positive half: without it, a broker that denied EVERY subscription would pass.
granted="$(suback_code "$P1" 'devices/device-b/down/#' device-b "$DEVICE_B_PW")"
[[ "$granted" == "0" || "$granted" == "1" ]] \
  || fail "device-b's SUBSCRIBE to its OWN topics returned '${granted:-none}', expected a granted QoS — the ACL is too tight"
pass "the ACL confines a device to its own subtree (128 for another's, granted for its own)"

# ─────────────────────────────────────────────────────────────────────────────────
# 4. Cross-node routing: subscribe on node 3, publish on node 1.
# ─────────────────────────────────────────────────────────────────────────────────
OUT="$WORK/xnode.out"
mosquitto_sub -h 127.0.0.1 -p "$P3" -t 'devices/+/up/#' -C 1 -W 20 \
  -u backend -P "$BACKEND_PW" -i xnode-sub > "$OUT" 2>/dev/null &
SUB_PID=$!
sleep 2
mosquitto_pub -h 127.0.0.1 -p "$P1" -t 'devices/device-a/up/temp' -m 'crossed' -q 1 \
  -u device-a -P "$DEVICE_A_PW" -i xnode-pub >/dev/null 2>&1 \
  || fail "the cross-node publish was not accepted"
wait "$SUB_PID" 2>/dev/null || true
grep -q crossed "$OUT" || fail "a message published on node 1 never reached a subscriber on node 3"
pass "a publish on node 1 reaches a subscriber on node 3"

# ─────────────────────────────────────────────────────────────────────────────────
# 5. An acknowledged QoS 1 message survives losing the node that accepted it.
#    A persistent subscriber is OFFLINE; the publisher is acked only once the message
#    is durably enqueued; the accepting node is then killed outright.
# ─────────────────────────────────────────────────────────────────────────────────
mosquitto_sub -h 127.0.0.1 -p "$P2" -t 'devices/device-b/down/#' -q 1 \
  -u device-b -P "$DEVICE_B_PW" -i durable-sub -c -C 1 -W 3 >/dev/null 2>&1 || true
pass "a persistent session is established, then disconnected"

mosquitto_pub -h 127.0.0.1 -p "$P1" -t 'devices/device-b/down/cmd' -m 'survives' -q 1 \
  -u backend -P "$BACKEND_PW" -i durable-pub >/dev/null 2>&1 \
  || fail "the QoS 1 publish was never acknowledged"
pass "the publisher was acknowledged (the broker has taken responsibility)"

kill -9 "$N1" 2>/dev/null || true
sleep 5
pass "node 1 was SIGKILLed"

RESUMED="$WORK/resumed.out"
mosquitto_sub -h 127.0.0.1 -p "$P2" -t 'devices/device-b/down/#' -q 1 \
  -u device-b -P "$DEVICE_B_PW" -i durable-sub -c -C 1 -W 25 > "$RESUMED" 2>/dev/null || true
grep -q survives "$RESUMED" \
  || fail "an ACKNOWLEDGED message was lost when its node died — the durability claim is false here"
pass "the acknowledged message survived the loss of the node that accepted it"

# ─────────────────────────────────────────────────────────────────────────────────
# 6. The readiness floor does what the artifacts say. With node 1 already gone, kill
#    node 2 as well: node 3 is then a minority of three and must drop OUT of rotation
#    while staying alive. This is the setting an operator is most likely to leave at
#    its default without checking, and the cost of it being wrong is a lone node
#    serving clients from a store it cannot write.
# ─────────────────────────────────────────────────────────────────────────────────
kill -9 "$N2" 2>/dev/null || true
for _ in $(seq 1 60); do
  MQTTD_HEALTH_BIND="127.0.0.1:$H3" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1 || break
  sleep 1
done

MQTTD_HEALTH_BIND="127.0.0.1:$H3" "$MQTTD_BIN" --probe /livez >/dev/null 2>&1 \
  || fail "the surviving node stopped serving /livez — a minority node must stay ALIVE, only unready"
if MQTTD_HEALTH_BIND="127.0.0.1:$H3" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
  fail "the last surviving node of three still reports READY — MQTTD_READY_MIN_MEMBERS is not doing its job"
fi
pass "a minority node reports live-but-not-ready (drops from rotation, is not restarted)"

# ─────────────────────────────────────────────────────────────────────────────────
# 7. The remaining artifacts, checked with their own tools where available.
# ─────────────────────────────────────────────────────────────────────────────────
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  ( cd deploy/compose && mkdir -p secrets && : > secrets/mqttd-passwd \
    && : > secrets/mqttd-swim-key && docker compose config >/dev/null ) \
    || fail "deploy/compose/compose.yaml is not a valid compose file"
  rm -rf deploy/compose/secrets
  pass "deploy/compose/compose.yaml validates with 'docker compose config'"
else
  echo "  SKIP — docker compose not available; compose.yaml was NOT validated here"
fi

if command -v systemd-analyze >/dev/null 2>&1; then
  # `verify` cannot be gated on its exit code alone here: on a machine where mqttd is not
  # installed it legitimately complains about the absent EnvironmentFile and the absent
  # `mqttd` user, neither of which says anything about the unit's correctness. What DOES
  # matter is a directive systemd does not recognise — a typo'd hardening option is
  # silently ignored at runtime, so the unit would look hardened and not be.
  verify_out="$(systemd-analyze verify deploy/systemd/mqttd.service 2>&1 || true)"
  real_errors="$(grep -vE "EnvironmentFile|/etc/mqttd|Unknown user|Failed to (open|read)" <<<"$verify_out" \
                 | grep -iE "unknown lvalue|unknown key|unknown section|invalid|failed to parse" || true)"
  if [[ -n "$real_errors" ]]; then
    echo "$real_errors"
    fail "deploy/systemd/mqttd.service has an unknown or invalid directive (above)"
  fi
  pass "deploy/systemd/mqttd.service passes systemd-analyze verify"
else
  echo "  SKIP — systemd-analyze not available; the unit was NOT verified here"
fi

echo
echo "DEPLOY SMOKE OK"
