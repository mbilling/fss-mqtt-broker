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
#   4. it still runs as nonroot (a regression to root would pass 1-3 silently).
#
# Needs docker + mosquitto-clients. Set MQTTD_IMAGE to test a prebuilt image
# (e.g. a published tag) instead of building one from the working tree.
set -euo pipefail
cd "$(dirname "$0")/.."

for tool in docker mosquitto_pub mosquitto_sub; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

IMAGE="${MQTTD_IMAGE:-}"
VOL_A=mqttd-imgsmoke-a
VOL_B=mqttd-imgsmoke-b
PORT_A=31883
PORT_B=31884

cleanup() {
  docker rm -f mqttd-imgsmoke-a mqttd-imgsmoke-b >/dev/null 2>&1 || true
  docker volume rm -f "$VOL_A" "$VOL_B" >/dev/null 2>&1 || true
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

echo "IMAGE SMOKE OK"
