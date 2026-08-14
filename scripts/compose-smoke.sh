#!/usr/bin/env bash
# The compose reference deployment, brought up in REAL CONTAINERS from the shipped file.
#
# scripts/deploy-smoke.sh proves the broker's behaviour with the shipped *values*, but it
# runs three local processes and only ever `docker compose config`s the file. Everything
# that lives strictly between the YAML and a running container is therefore unproven by it:
# whether the one-shot ordering actually gates the brokers, whether the staged secrets are
# readable by uid 65532 under a read-only rootfs, whether the health check passes inside the
# start_period, whether the published TLS ports work from the host. Those are exactly the
# things a reader hits on their first `docker compose up -d`, and issue #254 was a case of
# the file confidently claiming a posture nothing had ever run.
#
# So this runs the reader's commands, on the shipped `deploy/compose/compose.yaml`:
#
#   1. with secrets/ absent, `up -d` FAILS, no broker container is created, and the init
#      one-shot's message names ./bootstrap.sh;
#   2. after ./bootstrap.sh, `up -d` brings all three brokers to HEALTHY;
#   3. a TLS subscriber on node 1 receives a QoS 1 publish made on node 3 (cross-node
#      routing over TLS, through a mutually-authenticated cluster bus);
#   4. a client with no --cafile is refused, and a wrong password is refused;
#   5. no container logs INSECURE, and each of the three logs signed per-node gossip;
#   6. compose publishes no host port 1883 — and, when 1883 was free before this run, that
#      nothing accepts a cleartext connection there afterwards either;
#   7. each broker's /etc/mqttd/tls holds its OWN key and no other node's, and no CA key:
#      the custody rule deploy/README.md states, checked on the running containers;
#   8. with the explicit -f overlay, plaintext comes back on 127.0.0.1:1883 AND every
#      broker logs "INSECURE: starting PLAINTEXT MQTT listener" — checked PER BROKER, so
#      one noisy container cannot satisfy it for the other two.
#
# ── TWO MODES, both nightly ───────────────────────────────────────────────────────────
# Default mode exports MQTTD_IMAGE (an image built from this working tree, or whatever
# prebuilt tag you passed in) before every compose invocation — it proves the ARTIFACTS
# against today's code. With MQTTD_SMOKE_DEFAULT_IMAGE=1 nothing is exported and nothing
# is built: compose.yaml's pinned default tag resolves exactly as it does for a reader,
# `./bootstrap.sh` falls back to that same published tag, and the run proves the artifacts
# against THE IMAGE USERS ACTUALLY PULL — the lane whose absence let issue #263 ship a
# default image lacking `--probe` and `--hash-password`. If the pinned tag is not
# published yet (a forward pin awaiting its release; scripts/check-deploy-image-pin.sh
# verifies that shape on every PR), the default-image mode SKIPS LOUDLY rather than
# reporting the pending release as a defect in these files.
#
# Assertion (2) carries a second one implicitly and it is the load-bearing one on native
# Linux: a container only reaches healthy if uid 65532 could read /run/secrets/mqttd-passwd
# and the gossip key. A bind-mounted 0700 `./secrets` cannot be traversed by that uid at
# all — Docker Desktop's VM hides this behind uid mapping, which is exactly how a compose
# file ships broken to Linux users. The staging step in deploy/compose/init.sh is what
# makes it pass, and this is the only lane that runs on a native-Linux docker host.
#
# Needs docker (+ compose) and mosquitto-clients; SKIPS LOUDLY when docker is absent.
# Set MQTTD_IMAGE to test a prebuilt image instead of building one from the working tree —
# and note that whichever you do, the compose default tag stays unexercised (above).
#
# Host ports 8883-8885 (and 1883-1885 during the overlay phase) must be free.
set -euo pipefail
cd "$(dirname "$0")/.."

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  echo "SKIP — docker compose is not available; the compose reference deployment was NOT brought up"
  exit 0
fi
for tool in mosquitto_pub mosquitto_sub; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

COMPOSE_DIR="deploy/compose"
SECRETS="$COMPOSE_DIR/secrets"
PROJECT=mqttd-compose-smoke
PLAIN=(-f compose.yaml -f compose.plaintext.yaml)

# bootstrap.sh writes a reader's ONLY copy of their generated passwords into secrets/, and
# the teardown here deletes that directory. Refuse rather than destroy it.
[[ -e "$SECRETS" ]] && {
  echo "FATAL: $SECRETS already exists — move it aside; this smoke needs a clean directory"
  exit 2
}

pass() { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }

compose() { ( cd "$COMPOSE_DIR" && docker compose -p "$PROJECT" "$@" ); }
compose_plain() { ( cd "$COMPOSE_DIR" && docker compose -p "$PROJECT" "${PLAIN[@]}" "$@" ); }

cleanup() {
  # `down -v` on both -f combinations: the overlay phase's containers are the same project,
  # but tearing down with the same file set that created them keeps compose from warning
  # about orphans, and -v is what removes the PKI + staged-secret volumes.
  compose_plain down -v --remove-orphans >/dev/null 2>&1 || true
  compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$SECRETS"
}
trap cleanup EXIT
cleanup

# ── the image under test ────────────────────────────────────────────────────────────
if [[ "${MQTTD_SMOKE_DEFAULT_IMAGE:-0}" == 1 ]]; then
  # The reader's path: no override, no build. compose.yaml and bootstrap.sh resolve
  # their own pinned default; this block only decides whether that tag is pullable yet.
  [[ -z "${MQTTD_IMAGE:-}" ]] \
    || { echo "FATAL: MQTTD_SMOKE_DEFAULT_IMAGE=1 with MQTTD_IMAGE set — pick one"; exit 2; }
  DEFAULT_REF="$(grep -oE 'MQTTD_IMAGE:-[^}]+' "$COMPOSE_DIR/compose.yaml" | head -1 | cut -d- -f2-)"
  [[ -n "$DEFAULT_REF" ]] || { echo "FATAL: no MQTTD_IMAGE default in compose.yaml"; exit 2; }
  if ! docker manifest inspect "$DEFAULT_REF" >/dev/null 2>&1; then
    echo "SKIP — the pinned default image $DEFAULT_REF is not published yet; the"
    echo "       default-image lane engages on the first nightly after that release tag"
    echo "       is pushed (the per-PR pin gate proves the pin is forward-looking)."
    exit 0
  fi
  echo "image under test: $DEFAULT_REF — compose.yaml's OWN default, no override (issue #263)"
  # The exact invocation bootstrap.sh's no-toolchain fallback uses. Asserted directly,
  # because a binary without the flag does not fail — it BOOTS A BROKER, and the garbage
  # lands in the password file (the corrupt-passwd half of issue #263). A booted broker
  # never exits, so the call is bounded where `timeout` exists (Linux CI always has it).
  TIMEOUT=(); command -v timeout >/dev/null 2>&1 && TIMEOUT=(timeout 60)
  # (The guarded expansion keeps macOS's bash 3.2 happy under set -u when TIMEOUT is empty.)
  probe_hash="$(printf %s smoke | ${TIMEOUT[@]+"${TIMEOUT[@]}"} docker run --rm -i "$DEFAULT_REF" --hash-password probe-user 2>/dev/null || true)"
  grep -qE '^probe-user:\$argon2id\$' <<<"$probe_hash" \
    || fail "the published $DEFAULT_REF does not hash passwords ('--hash-password' output: ${probe_hash:0:60}…)"
  pass "the published image's --hash-password emits one valid argon2id line"
else
# Built exactly as scripts/image-smoke.sh builds it (musl release binary staged into
# dist/, then `docker build`), which is also how the release pipeline stages it. Keep the
# two recipes together: if one changes, change both.
IMAGE="${MQTTD_IMAGE:-}"
if [[ -z "$IMAGE" ]]; then
  IMAGE=mqttd:compose-smoke
  echo "building $IMAGE from the working tree…"
  # `arm64` is what macOS calls it and `aarch64` is what Rust calls it, so the raw
  # `uname -m` that scripts/image-smoke.sh uses names no real target on Apple Silicon.
  # Translated here because this script is one a maintainer runs locally before merging a
  # compose change (per-PR CI never brings containers up).
  ARCH="$(uname -m)"
  [[ "$ARCH" == arm64 ]] && ARCH=aarch64
  TARGET="${ARCH}-unknown-linux-musl"
  if [[ ! -x dist/mqttd ]]; then
    rustup target add "$TARGET" >/dev/null 2>&1 || true
    cargo build --release -p mqttd --target "$TARGET"
    mkdir -p dist && cp "target/${TARGET}/release/mqttd" dist/mqttd
  fi
  docker build -q -t "$IMAGE" . >/dev/null
elif docker image inspect "$IMAGE" >/dev/null 2>&1; then
  # Already local — a tag built by hand for exactly this check. Pulling unconditionally
  # (as scripts/image-smoke.sh does) fails on such a tag with "pull access denied", which
  # reads like an auth problem rather than "that image only exists here".
  echo "using the local image ${IMAGE}"
else
  echo "pulling ${IMAGE} ..."
  docker pull -q "$IMAGE" >/dev/null
fi
# Exported for `./bootstrap.sh` as well as for compose: when it has no local build to hash
# with it falls back to `docker run $MQTTD_IMAGE --hash-password`, and that must be this
# image rather than the published default. The default tag is exercised by the OTHER mode
# (MQTTD_SMOKE_DEFAULT_IMAGE=1) — see "TWO MODES" in the header.
export MQTTD_IMAGE="$IMAGE"
echo "image under test: $IMAGE (override mode; the pinned default tag is the other lane's job)"
fi

# Is something already on host 1883? If so the "nothing on 1883" and plaintext-overlay
# assertions cannot distinguish our stack from theirs, so they are skipped loudly rather
# than reported as a failure of this repo's artifacts.
HOST_1883_BUSY=0
if mosquitto_pub -h 127.0.0.1 -p 1883 -t compose-smoke/probe -m x -q 0 \
     --quiet >/dev/null 2>&1; then
  HOST_1883_BUSY=1
fi

# ─────────────────────────────────────────────────────────────────────────────────
# 1. Without secrets, the bring-up fails and says what to run.
# ─────────────────────────────────────────────────────────────────────────────────
up_out="$(compose up -d 2>&1)" && fail "\`docker compose up -d\` SUCCEEDED with no secrets/ — \
the init one-shot must gate the brokers so the failure names ./bootstrap.sh"
init_log="$(compose logs init 2>&1 || true)"
grep -q 'run ./bootstrap.sh' <<<"$init_log$up_out" \
  || { echo "$up_out"; echo "$init_log"; fail "the missing-secrets failure does not name ./bootstrap.sh"; }
# Compose *creates* the broker containers up front and then never starts them, so the
# property to assert is that none of them ever RAN — a container object with a zero
# StartedAt has served nothing and read no secret. Checking `ps -aq` would fail here for a
# reason that has nothing to do with the gate.
for svc in mqttd-1 mqttd-2 mqttd-3; do
  cid="$(compose ps -aq "$svc" 2>/dev/null || true)"
  [[ -n "$cid" ]] || continue
  started="$(docker inspect --format '{{.State.StartedAt}}' "$cid" 2>/dev/null || echo unknown)"
  [[ "$started" == 0001-01-01T00:00:00Z ]] \
    || fail "$svc STARTED (at $started) despite the init one-shot failing — the dependency gate is not holding"
done
pass "with no secrets/, up -d fails, no broker ever starts, and the failure names ./bootstrap.sh"
cleanup

# ─────────────────────────────────────────────────────────────────────────────────
# 2. The reader's two commands.
# ─────────────────────────────────────────────────────────────────────────────────
( cd "$COMPOSE_DIR" && ./bootstrap.sh >/dev/null ) || fail "./bootstrap.sh failed"
[[ -s "$SECRETS/mqttd-passwd" && -s "$SECRETS/mqttd-swim-key" ]] \
  || fail "./bootstrap.sh did not write the secrets"
BACKEND_PW="$(awk -F'\t' '$1=="backend"{print $2}' "$SECRETS/PASSWORDS.txt")"
DEVICE_A_PW="$(awk -F'\t' '$1=="device-a"{print $2}' "$SECRETS/PASSWORDS.txt")"
[[ -n "$BACKEND_PW" && -n "$DEVICE_A_PW" ]] \
  || fail "could not read the generated passwords out of $SECRETS/PASSWORDS.txt"
# Every line must be one `user:$argon2id$…` record. Issue #263's corrupt-passwd failure
# looked exactly like success from here — the file existed and was non-empty, but held a
# broker's boot log — so the SHAPE is asserted, not just the presence.
if grep -qvE '^[A-Za-z0-9_-]+:\$argon2id\$' "$SECRETS/mqttd-passwd"; then
  fail "$SECRETS/mqttd-passwd holds something that is not a user:argon2id line: $(grep -vE '^[A-Za-z0-9_-]+:\$argon2id\$' "$SECRETS/mqttd-passwd" | head -1)"
fi
pass "./bootstrap.sh minted the gossip key and a well-formed Argon2id password file"

compose up -d >/dev/null 2>&1 || { compose logs; fail "docker compose up -d failed"; }

# The one-shot must have run to completion BEFORE the brokers — that is the whole point of
# the dependency, and it is also what produced secrets/ca.pem for the client calls below.
[[ -s "$SECRETS/ca.pem" ]] \
  || { compose logs init; fail "the init one-shot did not write secrets/ca.pem for host clients"; }
pass "the init one-shot completed first and published the trust anchor"

# Healthy within the compose healthcheck's own budget (start_period 60s + retries).
deadline=$((SECONDS + 180))
while :; do
  unhealthy=""
  for svc in mqttd-1 mqttd-2 mqttd-3; do
    cid="$(compose ps -q "$svc")"
    [[ -n "$cid" ]] || { unhealthy="$svc(missing)"; break; }
    state="$(docker inspect --format '{{.State.Health.Status}}' "$cid" 2>/dev/null || echo unknown)"
    [[ "$state" == healthy ]] || unhealthy="$svc($state)"
  done
  [[ -z "$unhealthy" ]] && break
  (( SECONDS < deadline )) || { compose ps; compose logs --tail 40; \
    fail "not all brokers became healthy within 180s (last: $unhealthy)"; }
  sleep 3
done
pass "all three brokers are HEALTHY (so uid 65532 could read the staged secrets)"

CA="$SECRETS/ca.pem"

# ─────────────────────────────────────────────────────────────────────────────────
# 3. Cross-node routing, over TLS, from the host.
# ─────────────────────────────────────────────────────────────────────────────────
OUT="$(mktemp)"
mosquitto_sub -h 127.0.0.1 -p 8883 --cafile "$CA" -t 'devices/+/up/#' -C 1 -W 30 \
  -u backend -P "$BACKEND_PW" -i cs-sub >"$OUT" 2>/dev/null &
SUB=$!
sleep 3
mosquitto_pub -h 127.0.0.1 -p 8885 --cafile "$CA" -t 'devices/device-a/up/temp' \
  -m 'crossed' -q 1 -u device-a -P "$DEVICE_A_PW" -i cs-pub >/dev/null 2>&1 \
  || fail "the TLS publish on node 3 (host port 8885) was not accepted"
wait "$SUB" 2>/dev/null || true
grep -q crossed "$OUT" || { compose logs --tail 40; \
  fail "a publish on node 3 never reached a TLS subscriber on node 1"; }
rm -f "$OUT"
pass "a TLS publish on node 3 reaches a TLS subscriber on node 1"

# ─────────────────────────────────────────────────────────────────────────────────
# 4. The refusals.
# ─────────────────────────────────────────────────────────────────────────────────
if mosquitto_pub -h 127.0.0.1 -p 8883 -t 'devices/device-a/up/t' -m x \
     -u device-a -P "$DEVICE_A_PW" -i cs-cleartext >/dev/null 2>&1; then
  fail "a client with no --cafile was accepted on 8883 — the listener is not TLS"
fi
pass "a client that does not speak TLS is refused"

if mosquitto_pub -h 127.0.0.1 -p 8883 --cafile "$CA" -t 'devices/device-a/up/t' -m x \
     -u device-a -P 'wrong-password' -i cs-badpw >/dev/null 2>&1; then
  fail "a WRONG password was accepted"
fi
pass "a wrong password is refused"

# ─────────────────────────────────────────────────────────────────────────────────
# 5. What the containers say about themselves.
# ─────────────────────────────────────────────────────────────────────────────────
# Per service rather than over the combined log, so the result cannot be satisfied by two
# nodes logging it twice, and a restart loop cannot inflate the count.
for svc in mqttd-1 mqttd-2 mqttd-3; do
  svc_log="$(compose logs --no-color "$svc" 2>&1)"
  if grep -q INSECURE <<<"$svc_log"; then
    grep INSECURE <<<"$svc_log" | sed 's/^/         /'
    fail "$svc logged an INSECURE warning — the shipped compose file is running a plaintext listener"
  fi
  grep -q 'SWIM gossip is SIGNED per-node' <<<"$svc_log" \
    || fail "$svc did not log 'SWIM gossip is SIGNED per-node' — the cluster-bus material is \
not reaching the gossip layer (ADR 0022), so gossip is shared-key only"
done
pass "no container logs INSECURE, and each of the three signs its gossip per-node"

# ─────────────────────────────────────────────────────────────────────────────────
# 6. Nothing on host 1883.
# ─────────────────────────────────────────────────────────────────────────────────
# Two different things, and the second is the one a reader cares about: `compose port`
# reads the rendered configuration (no 1883 is PUBLISHED), and the connect attempt reads
# the host's actual sockets. The socket half is only meaningful if 1883 was free before
# this run — otherwise it would be testing somebody else's broker — so it is skipped
# loudly in that case rather than quietly dropped.
for svc in mqttd-1 mqttd-2 mqttd-3; do
  if compose port "$svc" 1883 >/dev/null 2>&1; then
    fail "compose publishes container port 1883 for $svc — the default must not"
  fi
done
if (( HOST_1883_BUSY )); then
  pass "the default bring-up publishes no plaintext port for any broker (host 1883 was \
already in use before this run, so the socket itself was NOT probed)"
else
  if mosquitto_pub -h 127.0.0.1 -p 1883 -t compose-smoke/probe -m x -q 0 --quiet \
       >/dev/null 2>&1; then
    fail "something accepts cleartext MQTT on 127.0.0.1:1883 with the default bring-up up"
  fi
  pass "the default bring-up publishes no plaintext port, and nothing accepts cleartext on \
127.0.0.1:1883"
fi

# ─────────────────────────────────────────────────────────────────────────────────
# 7. Key custody, on the running containers.
# ─────────────────────────────────────────────────────────────────────────────────
# All three brokers run as uid 65532, so file modes cannot keep them apart: the boundary is
# the mount list, and this asserts it where it is real rather than in the rendered YAML
# (scripts/deploy-smoke.sh does that half on every PR). For each broker: find the volume
# behind /etc/mqttd/tls, then list it — it must hold that node's key and no other node's,
# and no CA private key.
#
# The listing runs in whatever image the `init` one-shot just ran, read off that container
# rather than named again here: any second copy of compose.yaml's pinned certgen tag would
# be a pin that can drift from the one that matters.
CERTGEN="$(docker inspect --format '{{.Config.Image}}' "$(compose ps -aq init)")" \
  || fail "could not read the init one-shot's image for the custody check"
for n in 1 2 3; do
  cid="$(compose ps -q "mqttd-$n")"
  [[ -n "$cid" ]] || fail "mqttd-$n is not running for the custody check"
  vol="$(docker inspect \
    --format '{{range .Mounts}}{{if eq .Destination "/etc/mqttd/tls"}}{{.Name}}{{end}}{{end}}' \
    "$cid")"
  [[ -n "$vol" ]] || fail "mqttd-$n has no named volume at /etc/mqttd/tls"
  listing="$(docker run --rm --entrypoint ls -v "$vol:/t:ro" "$CERTGEN" -1 /t)" \
    || fail "could not list mqttd-$n's TLS volume ($vol)"
  grep -qx "mqttd-$n.key" <<<"$listing" \
    || { echo "$listing" | sed 's/^/         /'; fail "mqttd-$n's TLS volume has no mqttd-$n.key"; }
  for other in 1 2 3; do
    (( other == n )) && continue
    grep -qx "mqttd-$other.key" <<<"$listing" \
      && { echo "$listing" | sed 's/^/         /'
           fail "mqttd-$n can read mqttd-$other.key — one broker holds another node's peer \
identity, and the bus binds identity to the certificate CN"; }
  done
  grep -qx 'ca.key' <<<"$listing" \
    && { echo "$listing" | sed 's/^/         /'
         fail "mqttd-$n's TLS volume contains the CA PRIVATE key — anything that can read it \
can mint a leaf for any node id"; }
done
pass "each broker's TLS volume holds only its own key, and none holds the CA private key"

# ─────────────────────────────────────────────────────────────────────────────────
# 8. The opt-in overlay puts plaintext back, loudly.
# ─────────────────────────────────────────────────────────────────────────────────
if (( HOST_1883_BUSY )); then
  echo "  SKIP — something else is already listening on 127.0.0.1:1883; the plaintext"
  echo "         overlay was NOT exercised (it would test that broker, not this one)"
else
  compose_plain up -d >/dev/null 2>&1 || { compose_plain logs --tail 40; \
    fail "the plaintext overlay failed to bring the cluster up"; }
  ok=0
  for _ in $(seq 1 30); do
    if mosquitto_pub -h 127.0.0.1 -p 1883 -t 'devices/device-a/up/plain' -m x -q 1 \
         -u device-a -P "$DEVICE_A_PW" -i cs-plain >/dev/null 2>&1; then ok=1; break; fi
    sleep 2
  done
  (( ok )) || { compose_plain logs --tail 40; \
    fail "the plaintext overlay did not accept a cleartext client on 127.0.0.1:1883"; }
  # PER SERVICE, like section 5: over the combined log a single noisy broker satisfies the
  # pattern, and the claim being checked is that EVERY broker labels its own listener.
  # Captured, not piped: `grep -q` exits at the first match and closes the pipe, so
  # `docker compose logs` dies of SIGPIPE (141) and `set -o pipefail` reports the pipeline
  # as failed even though the pattern matched.
  for svc in mqttd-1 mqttd-2 mqttd-3; do
    plain_log="$(compose_plain logs --no-color "$svc" 2>&1)"
    grep -q 'INSECURE: starting PLAINTEXT MQTT listener' <<<"$plain_log" \
      || fail "$svc did not log 'INSECURE: starting PLAINTEXT MQTT listener' under the overlay \
— the warning at the point of use is not real for that broker"
  done
  pass "the overlay restores plaintext on loopback and EACH of the three brokers logs INSECURE for it"
fi

echo
echo "COMPOSE SMOKE OK — with MQTTD_IMAGE=$IMAGE"
echo "NOT covered: compose.yaml's \${MQTTD_IMAGE:-ghcr.io/mbilling/fss-mqtt-broker:latest}"
echo "default. The published :latest is v0.9.0, which predates both --hash-password and the"
echo "--probe early exit, so bootstrap.sh and the healthcheck cannot work against it and this"
echo "lane deliberately does not try. Issue #263."
