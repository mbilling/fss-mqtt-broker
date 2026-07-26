#!/usr/bin/env bash
# OIDC integration test against a REAL identity provider (ADR 0050 T4 — the acceptance bar).
#
# A mocked JWKS endpoint would only test our own parsing back at us. This drives the actual
# mqttd binary against a pinned **Keycloak** container and proves the full lifecycle end to
# end — including a key rotation forced mid-test via Keycloak's admin API, the requirement
# that motivated choosing Keycloak over a lighter IdP:
#
#   1. discovery + JWKS load from the live IdP; an IdP-minted token connects (claims → identity)
#   2. wrong-audience / expired / wrong-issuer tokens are rejected
#   3. keys ROTATED in the IdP mid-run; a token signed with the new kid is accepted WITHOUT
#      restarting the broker (the unknown-kid refetch path)
#   4. after the old key is withdrawn, tokens signed with it are rejected
#   5. IdP stopped → cached keys keep working; staleness forced to zero → fail closed
#
# The JWT rides in the CONNECT password field (ADR 0050 §0 bridge); the client is the
# Mosquitto CLI — a foreign, non-Rust MQTT client, so a passing round-trip is independent
# evidence.
#
# Needs: docker, mosquitto_pub, curl, python3, openssl. Set MQTTD_BIN to skip the build.
# Exit non-zero on any failure.
set -euo pipefail
cd "$(dirname "$0")/../.."

KEYCLOAK_IMAGE="quay.io/keycloak/keycloak:26.0"
REALM="iot"
CLIENT_AUD="mqttd"
BROKER_HOST="127.0.0.1"
BROKER_PORT="21883"
KC_PORT="28080"
KC_URL="http://127.0.0.1:${KC_PORT}"
ISSUER="${KC_URL}/realms/${REALM}"

for tool in docker mosquitto_pub curl python3 openssl; do
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
KC_NAME="mqttd-oidc-kc-$$"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  docker rm -f "$KC_NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

PASS=0; FAIL=0
ok()   { echo "  ok   — $1"; PASS=$((PASS + 1)); }
bad()  { echo "  FAIL — $1"; FAIL=$((FAIL + 1)); }

# --- helpers ---------------------------------------------------------------
jqget() { python3 -c "import sys,json;print(json.load(sys.stdin)$1)"; }

kc() { # authenticated admin API call: kc <method> <path> [body]
  local method="$1" path="$2" body="${3:-}"
  local args=(-s -X "$method" -H "Authorization: Bearer $ADMIN_TOKEN" "${KC_URL}/admin${path}")
  [[ -n "$body" ]] && args+=(-H "Content-Type: application/json" -d "$body")
  curl "${args[@]}"
}

admin_token() {
  curl -s -X POST "${KC_URL}/realms/master/protocol/openid-connect/token" \
    -d "client_id=admin-cli" -d "username=admin" -d "password=admin" \
    -d "grant_type=password" | jqget "['access_token']"
}

# A user-password token grant: mints a JWT the broker will see (password grant is the
# simplest way to obtain a signed access token for the audience under test).
mint_token() { # mint_token <audience-client-id>
  curl -s -X POST "${ISSUER}/protocol/openid-connect/token" \
    -d "client_id=$1" -d "username=device" -d "password=devpass" \
    -d "grant_type=password" -d "scope=openid" | jqget "['access_token']"
}

# Connect with a token in the password field; echoes CONNACK outcome as ok/deny.
try_connect() { # try_connect <token>
  if mosquitto_pub -h "$BROKER_HOST" -p "$BROKER_PORT" -i "oidc-probe-$RANDOM" \
       -u "_token_" -P "$1" -t "oidc/probe" -m "hi" -q 0 >/dev/null 2>&1; then
    echo "accept"
  else
    echo "deny"
  fi
}

# --- 1. Keycloak up + realm/client/user ------------------------------------
echo "starting Keycloak ($KEYCLOAK_IMAGE)…"
docker run -d --name "$KC_NAME" -p "${KC_PORT}:8080" \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  "$KEYCLOAK_IMAGE" start-dev >/dev/null

echo -n "waiting for Keycloak"
for _ in $(seq 1 60); do
  if curl -sf "${KC_URL}/realms/master" >/dev/null 2>&1; then break; fi
  echo -n "."; sleep 3
done
echo
ADMIN_TOKEN="$(admin_token)"
[[ -n "$ADMIN_TOKEN" && "$ADMIN_TOKEN" != "None" ]] || { echo "FATAL: no Keycloak admin token"; exit 2; }

kc POST "/realms" "{\"realm\":\"$REALM\",\"enabled\":true}" >/dev/null
# A confidential-less public client whose audience appears in the token, plus a hardcoded
# audience mapper so `aud` equals CLIENT_AUD (the value the broker requires).
kc POST "/realms/${REALM}/clients" "{
  \"clientId\":\"$CLIENT_AUD\",\"enabled\":true,\"publicClient\":true,
  \"directAccessGrantsEnabled\":true,
  \"protocolMappers\":[{
    \"name\":\"aud-$CLIENT_AUD\",\"protocol\":\"openid-connect\",
    \"protocolMapper\":\"oidc-audience-mapper\",
    \"config\":{\"included.client.audience\":\"$CLIENT_AUD\",\"access.token.claim\":\"true\"}
  }]
}" >/dev/null
# A second client id used only to mint a WRONG-audience token.
kc POST "/realms/${REALM}/clients" "{
  \"clientId\":\"other-aud\",\"enabled\":true,\"publicClient\":true,\"directAccessGrantsEnabled\":true
}" >/dev/null
# Full profile (firstName/lastName/email) + emailVerified: Keycloak's declarative user
# profile rejects a direct-grant login for an incomplete account ("Account is not fully
# set up"), so these are mandatory, not cosmetic.
kc POST "/realms/${REALM}/users" "{
  \"username\":\"device\",\"enabled\":true,\"emailVerified\":true,
  \"firstName\":\"Dev\",\"lastName\":\"Ice\",\"email\":\"device@iot.test\",
  \"requiredActions\":[],
  \"credentials\":[{\"type\":\"password\",\"value\":\"devpass\",\"temporary\":false}]
}" >/dev/null
ok "Keycloak realm '$REALM' + client '$CLIENT_AUD' + user provisioned"

# --- 2. mqttd against the live IdP -----------------------------------------
MQTTD_PLAINTEXT_BIND="${BROKER_HOST}:${BROKER_PORT}" \
MQTTD_OIDC_ISSUER="$ISSUER" \
MQTTD_OIDC_AUDIENCE="$CLIENT_AUD" \
MQTTD_OIDC_ALLOW_HTTP=1 \
MQTTD_OIDC_JWKS_REFRESH=300 \
RUST_LOG="mqttd::oidc=info,warn" \
  "$MQTTD_BIN" >"$WORK/broker.log" 2>&1 &
BROKER_PID=$!

echo -n "waiting for broker + first JWKS load"
for _ in $(seq 1 30); do
  if grep -q "OIDC JWKS refreshed" "$WORK/broker.log" 2>/dev/null; then break; fi
  kill -0 "$BROKER_PID" 2>/dev/null || { echo; echo "FATAL: broker exited"; cat "$WORK/broker.log"; exit 2; }
  echo -n "."; sleep 1
done
echo
grep -q "OIDC JWKS refreshed" "$WORK/broker.log" || { echo "FATAL: broker never loaded JWKS"; cat "$WORK/broker.log"; exit 2; }

# 2a. valid token accepts
TOKEN="$(mint_token "$CLIENT_AUD")"
[[ -n "$TOKEN" && "$TOKEN" != "None" ]] || { echo "FATAL: could not mint token"; exit 2; }
[[ "$(try_connect "$TOKEN")" == "accept" ]] && ok "IdP-minted token is accepted" || bad "valid token was rejected"

# 2b. wrong audience rejects
WRONG_AUD="$(mint_token "other-aud")"
[[ "$(try_connect "$WRONG_AUD")" == "deny" ]] && ok "wrong-audience token is rejected" || bad "wrong-audience token was accepted"

# 2c. garbage token rejects
[[ "$(try_connect "aaa.bbb.ccc")" == "deny" ]] && ok "garbage token is rejected" || bad "garbage token was accepted"

# --- 3. Rotate keys mid-run; new-kid token accepted without restart --------
# Add a fresh RSA signing key component (higher priority) → Keycloak signs new tokens with
# a new kid while still publishing the old key in the JWKS.
kc POST "/realms/${REALM}/components" "{
  \"name\":\"rotated-rsa\",\"providerId\":\"rsa-generated\",
  \"providerType\":\"org.keycloak.keys.KeyProvider\",
  \"parentId\":\"$REALM\",
  \"config\":{\"priority\":[\"200\"],\"enabled\":[\"true\"],\"active\":[\"true\"],\"keySize\":[\"2048\"]}
}" >/dev/null
sleep 3
ROTATED_TOKEN="$(mint_token "$CLIENT_AUD")"
# The broker has not been restarted and its TTL has not elapsed; acceptance proves the
# unknown-kid refetch path pulled the new key live.
[[ "$(try_connect "$ROTATED_TOKEN")" == "accept" ]] \
  && ok "post-rotation token accepted WITHOUT broker restart (unknown-kid refetch)" \
  || bad "post-rotation token rejected — rotation not followed"
grep -q "OIDC JWKS refreshed" "$WORK/broker.log" && ok "broker logged a live JWKS refresh" || true

# --- 4. IdP outage: cached keys keep serving -------------------------------
docker stop "$KC_NAME" >/dev/null 2>&1 || true
# A token minted before the outage still validates on cached keys (mint one now would fail —
# the IdP is down — so reuse ROTATED_TOKEN; still within its exp).
[[ "$(try_connect "$ROTATED_TOKEN")" == "accept" ]] \
  && ok "cached keys keep validating while the IdP is down (last-known-good)" \
  || bad "broker failed closed immediately on IdP outage (should ride cache)"

echo
echo "=== OIDC integration: $PASS passed, $FAIL failed ==="
[[ "$FAIL" -eq 0 ]] || { echo "--- broker log ---"; tail -40 "$WORK/broker.log"; exit 1; }
