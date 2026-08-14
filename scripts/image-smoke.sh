#!/usr/bin/env bash
# The container image, exercised as a user runs it (ADR 0045 T4).
#
# Every other test in this repo drives the BINARY. That leaves the image itself
# untested, and the gap was not theoretical: the published v0.9.0-rc image ran as
# uid 65532 with no directory that uid could write, so durable sessions — the
# headline feature, on by default — could not be enabled at all. `MQTTD_DATA_DIR=/data`,
# a named volume, and a subdirectory all failed with `Permission denied`, leaving
# `--user 0:0` (discarding the non-root posture) as the way through. No test noticed,
# because no test ran the image.
#
# So this asserts the properties that only exist at the image layer:
#   1. the documented data dir is writable BY THE NONROOT USER, with a volume;
#   2. durable state actually survives a container restart;
#   3. the broker runs under a READ-ONLY root filesystem with all capabilities
#      dropped and no-new-privileges — the hardened posture the docs advertise;
#   4. it still runs as nonroot (a regression to root would pass 1-3 silently);
#   5. the README's SECURED container invocation (issue #257) works as written:
#      mounted certs + ACL, TLS 1.3 + mTLS round-trip inside the grant, a client
#      with no certificate refused at the handshake, and no INSECURE log line.
#
# Needs docker + mosquitto-clients + openssl. Set MQTTD_IMAGE to test a prebuilt
# image (e.g. a published tag) instead of building one from the working tree.
set -euo pipefail
cd "$(dirname "$0")/.."

for tool in docker mosquitto_pub mosquitto_sub openssl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

IMAGE="${MQTTD_IMAGE:-}"
VOL_A=mqttd-imgsmoke-a
VOL_B=mqttd-imgsmoke-b
VOL_C=mqttd-imgsmoke-c
PORT_A=31883
PORT_B=31884
PORT_C=38883
WORK="$(mktemp -d)"

cleanup() {
  docker rm -f mqttd-imgsmoke-a mqttd-imgsmoke-b mqttd-imgsmoke-c >/dev/null 2>&1 || true
  docker volume rm -f "$VOL_A" "$VOL_B" "$VOL_C" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT
cleanup

if [[ -z "$IMAGE" ]]; then
  IMAGE=mqttd:image-smoke
  echo "building $IMAGE from the working tree…"
  ARCH="$(uname -m)"
  TARGET="${ARCH}-unknown-linux-musl"
  # The Dockerfile copies a prebuilt static binary; the release pipeline stages it
  # the same way, so this exercises the real image recipe rather than a variant.
  if [[ ! -x dist/mqttd ]]; then
    rustup target add "$TARGET" >/dev/null 2>&1 || true
    cargo build --release -p mqttd --target "$TARGET"
    mkdir -p dist && cp "target/${TARGET}/release/mqttd" dist/mqttd
  fi
  docker build -q -t "$IMAGE" . >/dev/null
else
  # A registry reference has to be pulled before `docker inspect` can read it —
  # inspect only ever looks at LOCAL images, so without this the run dies on the
  # nonroot check with a bare "No such object", which reads like the image is
  # broken rather than absent.
  echo "pulling ${IMAGE} ..."
  docker pull -q "$IMAGE" >/dev/null
fi
echo "image under test: $IMAGE"

# --- 4. it runs as nonroot -------------------------------------------------
USER_CFG="$(docker inspect "$IMAGE" --format '{{.Config.User}}')"
case "$USER_CFG" in
  nonroot*|65532*) echo "  ok   — image runs as '$USER_CFG', not root" ;;
  *) echo "  FAIL — image runs as '$USER_CFG'; it must not run as root"; exit 1 ;;
esac

# --- 1 + 2. the data dir is writable, and state survives a restart ---------
docker volume create "$VOL_A" >/dev/null
docker run -d --name mqttd-imgsmoke-a \
  -v "$VOL_A":/var/lib/mqttd -p "$PORT_A":1883 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 \
  -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd \
  -e MQTTD_HEALTH_BIND=0.0.0.0:8080 \
  "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
  [[ "$(docker inspect -f '{{.State.Running}}' mqttd-imgsmoke-a 2>/dev/null)" == "true" ]] || break
  mosquitto_pub -h 127.0.0.1 -p "$PORT_A" -t 'imgsmoke/ping' -m x >/dev/null 2>&1 && break
  sleep 1
done
if [[ "$(docker inspect -f '{{.State.Running}}' mqttd-imgsmoke-a 2>/dev/null)" != "true" ]]; then
  echo "  FAIL — the broker did not stay up with MQTTD_DATA_DIR on a mounted volume:"
  docker logs mqttd-imgsmoke-a 2>&1 | tail -5 | sed 's/^/         /'
  exit 1
fi
echo "  ok   — MQTTD_DATA_DIR on a mounted volume is writable by the nonroot user"

mosquitto_pub -h 127.0.0.1 -p "$PORT_A" -t 'imgsmoke/kept' -m 'survives-restart' -r -q 1 2>/dev/null
docker restart mqttd-imgsmoke-a >/dev/null
for _ in $(seq 1 60); do
  mosquitto_pub -h 127.0.0.1 -p "$PORT_A" -t 'imgsmoke/ping' -m x >/dev/null 2>&1 && break
  sleep 1
done
GOT="$(mosquitto_sub -h 127.0.0.1 -p "$PORT_A" -t 'imgsmoke/kept' -C 1 -W 8 2>/dev/null || true)"
if [[ "$GOT" != "survives-restart" ]]; then
  echo "  FAIL — durable state did not survive a container restart: expected [survives-restart] got [$GOT]"
  exit 1
fi
echo "  ok   — durable state survived a container restart"

# --- 3. the hardened posture the docs advertise ----------------------------
docker volume create "$VOL_B" >/dev/null
docker run -d --name mqttd-imgsmoke-b \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v "$VOL_B":/var/lib/mqttd -p "$PORT_B":1883 \
  -e MQTTD_PLAINTEXT_BIND=0.0.0.0:1883 \
  -e MQTTD_ALLOW_ANONYMOUS=1 \
  -e MQTTD_DATA_DIR=/var/lib/mqttd \
  -e MQTTD_HEALTH_BIND=0.0.0.0:8080 \
  "$IMAGE" >/dev/null

for _ in $(seq 1 60); do
  mosquitto_pub -h 127.0.0.1 -p "$PORT_B" -t 'imgsmoke/ro' -m 'ok' -r -q 1 >/dev/null 2>&1 && break
  sleep 1
done
if [[ "$(docker inspect -f '{{.State.Running}}' mqttd-imgsmoke-b 2>/dev/null)" != "true" ]]; then
  echo "  FAIL — the broker did not run under --read-only --cap-drop ALL:"
  docker logs mqttd-imgsmoke-b 2>&1 | tail -5 | sed 's/^/         /'
  exit 1
fi
RO="$(mosquitto_sub -h 127.0.0.1 -p "$PORT_B" -t 'imgsmoke/ro' -C 1 -W 8 2>/dev/null || true)"
if [[ "$RO" != "ok" ]]; then
  echo "  FAIL — served nothing under a read-only root filesystem: got [$RO]"
  exit 1
fi
echo "  ok   — serves under --read-only --cap-drop ALL --security-opt no-new-privileges"

# --- 5. the secured container invocation the README documents ---------------
# README "Single node, secured, in a container" (issue #257): the same TLS 1.3 +
# mTLS + deny-by-default-ACL posture as the cargo walkthrough, as one hardened
# `docker run` with the PKI and ACL mounted read-only. Exercised here so the
# documented block cannot rot. Deviations are mechanical only: a host port from
# the 3xxxx range, and this script's own throwaway PKI (same shape as the
# README's steps 1–2, one client leaf being enough to prove the round-trip).
PKI="$WORK/pki"; mkdir -p "$PKI"
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -keyout "$PKI/ca.key" -out "$PKI/ca.crt" -subj '/CN=mqttd-imgsmoke-ca' >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$PKI/server.key" -out "$PKI/server.csr" \
  -subj '/CN=127.0.0.1' >/dev/null 2>&1
openssl x509 -req -in "$PKI/server.csr" -CA "$PKI/ca.crt" -CAkey "$PKI/ca.key" \
  -CAcreateserial -out "$PKI/server.crt" -days 365 \
  -extfile <(printf 'subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth') >/dev/null 2>&1
openssl req -newkey rsa:2048 -nodes -keyout "$PKI/sensor-1.key" -out "$PKI/sensor-1.csr" \
  -subj '/CN=sensor-1' >/dev/null 2>&1
openssl x509 -req -in "$PKI/sensor-1.csr" -CA "$PKI/ca.crt" -CAkey "$PKI/ca.key" \
  -CAcreateserial -out "$PKI/sensor-1.crt" -days 365 \
  -extfile <(printf 'extendedKeyUsage=clientAuth') >/dev/null 2>&1

cat > "$WORK/acl.toml" <<'ACL'
default = "deny"

[[rules]]
identities = ["sensor-1", "sensor-2"]
actions = ["publish", "subscribe"]
effect = "allow"
topics = ["sensors/%i/#"]
ACL

# The image runs as uid 65532; the bind-mounted material must be readable to it.
# Exactly what the README block tells the reader to do (throwaway PKI only).
chmod 0755 "$WORK" "$PKI"
chmod 0644 "$PKI/server.key" "$WORK/acl.toml"

docker volume create "$VOL_C" >/dev/null
docker run -d --name mqttd-imgsmoke-c \
  --read-only --cap-drop ALL --security-opt no-new-privileges \
  -v "$PKI":/etc/mqttd/pki:ro -v "$WORK/acl.toml":/etc/mqttd/acl.toml:ro \
  -v "$VOL_C":/var/lib/mqttd -p "$PORT_C":8883 \
  -e MQTTD_TLS_BIND=0.0.0.0:8883 \
  -e MQTTD_TLS_CERT=/etc/mqttd/pki/server.crt \
  -e MQTTD_TLS_KEY=/etc/mqttd/pki/server.key \
  -e MQTTD_TLS_CLIENT_CA=/etc/mqttd/pki/ca.crt \
  -e MQTTD_ACL_FILE=/etc/mqttd/acl.toml \
  -e MQTTD_DATA_DIR=/var/lib/mqttd \
  "$IMAGE" >/dev/null

MOSQ_TLS=(--cafile "$PKI/ca.crt" --cert "$PKI/sensor-1.crt" --key "$PKI/sensor-1.key")
for _ in $(seq 1 60); do
  [[ "$(docker inspect -f '{{.State.Running}}' mqttd-imgsmoke-c 2>/dev/null)" == "true" ]] || break
  mosquitto_pub -h 127.0.0.1 -p "$PORT_C" "${MOSQ_TLS[@]}" -i imgsmoke-sec-ping \
    -t 'sensors/sensor-1/ping' -m x >/dev/null 2>&1 && break
  sleep 1
done
if [[ "$(docker inspect -f '{{.State.Running}}' mqttd-imgsmoke-c 2>/dev/null)" != "true" ]]; then
  echo "  FAIL — the secured invocation did not stay up with mounted certs + ACL:"
  docker logs mqttd-imgsmoke-c 2>&1 | tail -5 | sed 's/^/         /'
  exit 1
fi

# The round-trip inside the grant, judged by delivery (retained, so no race).
mosquitto_pub -h 127.0.0.1 -p "$PORT_C" "${MOSQ_TLS[@]}" -i imgsmoke-sec-pub \
  -t 'sensors/sensor-1/temp' -m 'secured-ok' -r -q 1 2>/dev/null
SEC_GOT="$(mosquitto_sub -h 127.0.0.1 -p "$PORT_C" "${MOSQ_TLS[@]}" -i imgsmoke-sec-sub \
  -t 'sensors/sensor-1/#' -C 1 -W 8 2>/dev/null || true)"
if [[ "$SEC_GOT" != "secured-ok" ]]; then
  echo "  FAIL — secured container: mTLS round-trip inside the grant: expected [secured-ok] got [$SEC_GOT]"
  exit 1
fi
echo "  ok   — secured container: an mTLS client round-trips inside its ACL grant"

# A client with no certificate must fail at the TLS handshake — nothing reaches
# the MQTT layer at all.
if mosquitto_pub -h 127.0.0.1 -p "$PORT_C" --cafile "$PKI/ca.crt" -i imgsmoke-sec-nocert \
     -t 'sensors/sensor-1/temp' -m 'nope' >/dev/null 2>&1; then
  echo "  FAIL — secured container: a client with NO certificate was accepted"
  exit 1
fi
echo "  ok   — secured container: a client with no certificate is refused at the handshake"

# The README's claim about this configuration: it logs no INSECURE warning.
# Every opt-out of a secure default is loudly logged, so absence is a signal.
if docker logs mqttd-imgsmoke-c 2>&1 | grep -q "INSECURE"; then
  echo "  FAIL — the secured invocation logged an INSECURE warning:"
  docker logs mqttd-imgsmoke-c 2>&1 | grep "INSECURE" | sed 's/^/         /'
  exit 1
fi
echo "  ok   — secured container: the configuration logs no INSECURE warning"

echo "IMAGE SMOKE OK"
