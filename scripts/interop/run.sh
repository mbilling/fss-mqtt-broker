#!/usr/bin/env bash
# Foreign-client interop conformance suite (ADR 0034).
#
# Drives the REAL mqttd binary with the Eclipse Mosquitto CLI — a non-Rust client that
# shares no code with the broker's codec, so a passing round-trip is independent evidence
# the broker frames MQTT the way the ecosystem expects (not merely self-consistently). The
# foreign client is an external process, NOT a cargo dependency: nothing is added to the
# broker's supply chain.
#
# Runs locally (needs `mosquitto-clients`, `openssl`, `python3`, `curl` on PATH) and in CI.
# Set MQTTD_BIN to a prebuilt binary to skip the build.
#
# Exit non-zero on any framing/feature mismatch.
set -euo pipefail

cd "$(dirname "$0")/../.."

# --- prerequisites ----------------------------------------------------------
for tool in mosquitto_pub mosquitto_sub openssl python3 curl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

MQTTD_BIN="${MQTTD_BIN:-}"
if [[ -z "$MQTTD_BIN" ]]; then
  echo "building mqttd (set MQTTD_BIN to skip)…"
  cargo build --quiet -p mqttd
  MQTTD_BIN="target/debug/mqttd"
fi
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN"; exit 2; }
echo "broker:   $MQTTD_BIN"
echo "client:   $(mosquitto_pub --help 2>&1 | head -1)"

WORK="$(mktemp -d)"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$WORK"; }
trap cleanup EXIT

PASS=0
FAIL=0
ok()   { echo "  ok   — $1"; PASS=$((PASS + 1)); }
bad()  { echo "  FAIL — $1"; FAIL=$((FAIL + 1)); }

# Compare actual vs expected; pass/fail a named check.
expect() { # <name> <expected> <actual>
  if [[ "$2" == "$3" ]]; then ok "$1"; else bad "$1 — expected [$2] got [$3]"; fi
}

# --- helpers ----------------------------------------------------------------
# Grab N free localhost TCP ports (one shot, no race-prone reuse within the batch).
free_ports() { python3 -c "
import socket
n=$1
ss=[socket.socket() for _ in range(n)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]"; }

# Block until the broker's /readyz returns 200, or fail after ~6s.
wait_ready() { # <health_port>
  for _ in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$1/readyz" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  echo "FATAL: broker never became ready on health port $1"; exit 2
}

SETTLE="${INTEROP_SETTLE:-0.5}"  # subscription-establish wait before publishing
TMO="${INTEROP_TIMEOUT:-5}"      # per-subscriber receive timeout (seconds)
N=0                              # unique-client-id counter

# A concurrent pub→sub round-trip; asserts the subscriber receives `expected`.
# Extra mosquitto args (e.g. -V mqttv5, --cafile, -F) are passed through to BOTH sides via
# the SUB_ARGS / PUB_ARGS arrays set by the caller.
roundtrip() { # <name> <port> <topic> <payload> <expected>
  local name="$1" port="$2" topic="$3" payload="$4" expected="$5"
  local out="$WORK/sub.$N.txt"; N=$((N + 1))
  mosquitto_sub -h 127.0.0.1 -p "$port" -i "iop-sub-$N" -t "$topic" -C 1 -W "$TMO" \
    "${SUB_ARGS[@]}" >"$out" 2>"$WORK/sub.$N.err" &
  local sp=$!
  sleep "$SETTLE"
  mosquitto_pub -h 127.0.0.1 -p "$port" -i "iop-pub-$N" -t "$topic" -m "$payload" \
    "${PUB_ARGS[@]}" 2>"$WORK/pub.$N.err" || true
  wait "$sp" 2>/dev/null || true
  expect "$name" "$expected" "$(cat "$out")"
}

# Mint a CA + a 127.0.0.1 server leaf + a client leaf under $WORK/pki.
gen_pki() {
  local d="$WORK/pki"; mkdir -p "$d"
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$d/ca.key" -out "$d/ca.crt" \
    -subj '/CN=interop-ca' -days 1 >/dev/null 2>&1
  # server leaf, SAN IP:127.0.0.1
  openssl req -newkey rsa:2048 -nodes -keyout "$d/server.key" -out "$d/server.csr" \
    -subj '/CN=127.0.0.1' >/dev/null 2>&1
  openssl x509 -req -in "$d/server.csr" -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial \
    -out "$d/server.crt" -days 1 \
    -extfile <(printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth') >/dev/null 2>&1
  # client leaf (for mTLS). The clientAuth EKU is REQUIRED: rustls/webpki rejects a client
  # cert without it ("certificate unknown"). OpenSSL's `x509 -req` does not add it by default,
  # so set it explicitly (matches the broker's own rcgen client certs in tests/tls.rs).
  openssl req -newkey rsa:2048 -nodes -keyout "$d/client.key" -out "$d/client.csr" \
    -subj '/CN=interop-client' >/dev/null 2>&1
  openssl x509 -req -in "$d/client.csr" -CA "$d/ca.crt" -CAkey "$d/ca.key" -CAcreateserial \
    -out "$d/client.crt" -days 1 \
    -extfile <(printf 'extendedKeyUsage=clientAuth') >/dev/null 2>&1
}

# ===========================================================================
echo
echo "── Phase A: plaintext + server-TLS ────────────────────────────────────"
gen_pki
read -r MQTT TLSP HEALTH < <(free_ports 3)
MQTTD_NODE_ID=interop-a \
MQTTD_PLAINTEXT_BIND="127.0.0.1:$MQTT" \
MQTTD_TLS_BIND="127.0.0.1:$TLSP" \
MQTTD_TLS_CERT="$WORK/pki/server.crt" \
MQTTD_TLS_KEY="$WORK/pki/server.key" \
MQTTD_ALLOW_ANONYMOUS=1 \
MQTTD_HEALTH_BIND="127.0.0.1:$HEALTH" \
RUST_LOG=off "$MQTTD_BIN" &
PIDS+=("$!")
wait_ready "$HEALTH"

# v3.1.1 payload integrity at every QoS.
for q in 0 1 2; do
  SUB_ARGS=(-q "$q"); PUB_ARGS=(-q "$q")
  roundtrip "v3.1.1 QoS$q round-trip" "$MQTT" "iop/q$q" "payload-q$q" "payload-q$q"
done

# Retained delivered to a LATE subscriber (publish retained first, then subscribe).
mosquitto_pub -h 127.0.0.1 -p "$MQTT" -i iop-ret-pub -t 'iop/retained' -q 1 -r -m 'kept' 2>/dev/null || true
sleep "$SETTLE"
got="$(mosquitto_sub -h 127.0.0.1 -p "$MQTT" -i iop-ret-sub -t 'iop/retained' -C 1 -W "$TMO" 2>/dev/null || true)"
expect "retained delivered to a late subscriber" "kept" "$got"
mosquitto_pub -h 127.0.0.1 -p "$MQTT" -i iop-ret-clr -t 'iop/retained' -q 1 -r -m '' 2>/dev/null || true  # clear

# MQTT v5 round-trip with a User Property that must survive to the subscriber (ADR 0030).
SUB_ARGS=(-V mqttv5 -q 1 -F '%p|%P')
PUB_ARGS=(-V mqttv5 -q 1 -D publish user-property zone kitchen)
roundtrip "v5 round-trip + User Property survives" "$MQTT" "iop5/a" "hello-v5" "hello-v5|zone:kitchen"

# Server-auth TLS 1.3: OpenSSL client ↔ rustls server.
SUB_ARGS=(--cafile "$WORK/pki/ca.crt" --tls-version tlsv1.3 -q 1)
PUB_ARGS=(--cafile "$WORK/pki/ca.crt" --tls-version tlsv1.3 -q 1)
roundtrip "TLS 1.3 round-trip (OpenSSL↔rustls)" "$TLSP" "tls/a" "over-tls" "over-tls"

# ===========================================================================
echo
echo "── Phase B: mutual TLS ────────────────────────────────────────────────"
read -r MTLSP HEALTHB < <(free_ports 2)
MQTTD_NODE_ID=interop-b \
MQTTD_TLS_BIND="127.0.0.1:$MTLSP" \
MQTTD_TLS_CERT="$WORK/pki/server.crt" \
MQTTD_TLS_KEY="$WORK/pki/server.key" \
MQTTD_TLS_CLIENT_CA="$WORK/pki/ca.crt" \
MQTTD_ALLOW_ANONYMOUS=1 \
MQTTD_HEALTH_BIND="127.0.0.1:$HEALTHB" \
RUST_LOG=off "$MQTTD_BIN" &
PIDS+=("$!")
wait_ready "$HEALTHB"

# A client presenting a CA-signed cert completes mTLS and round-trips.
CL=(--cafile "$WORK/pki/ca.crt" --cert "$WORK/pki/client.crt" --key "$WORK/pki/client.key" --tls-version tlsv1.3)
SUB_ARGS=("${CL[@]}" -q 1); PUB_ARGS=("${CL[@]}" -q 1)
roundtrip "mTLS round-trip (client cert accepted)" "$MTLSP" "mtls/a" "with-cert" "with-cert"

# A client WITHOUT a cert must be refused (no message gets through).
got="$(mosquitto_sub -h 127.0.0.1 -p "$MTLSP" --cafile "$WORK/pki/ca.crt" --tls-version tlsv1.3 \
  -t 'mtls/b' -C 1 -W 2 2>/dev/null || true)"
expect "mTLS refuses a client with no cert" "" "$got"

# ===========================================================================
echo
echo "── Summary ────────────────────────────────────────────────────────────"
echo "  passed: $PASS   failed: $FAIL"
[[ "$FAIL" -eq 0 ]] || { echo "INTEROP FAILED"; exit 1; }
echo "INTEROP OK"
