#!/usr/bin/env bash
# ADR 0047 T5 — kind/k3d runtime smoke for the Helm chart.
#
# Stands up a real 3-node cluster from the chart in a kind cluster, proves it FORMS (founder
# bootstrap + self-forming gossip mesh + per-pod PV + rendered config + check-config init +
# readiness), then exercises the two operations the chart exists to make safe:
#   - scale-down is a decommission DRAIN (preStop → `mqttd --decommission`), not a crash;
#   - a rolling restart is quorum-safe and loses no durable (retained) state.
#
# Requires: docker, kind, kubectl, helm. Builds the broker + a test image itself. Designed to run
# in CI (nightly) and by hand. Verbose + self-diagnosing: on failure it dumps pod state and logs.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${CLUSTER:-mqttd-smoke}"
NS="${NS:-mqttd-smoke}"
# The release name must NOT contain the chart name "mqttd", or Helm's fullname collapses to just
# the release name (mqttd.fullname), breaking the "<release>-mqttd" object names this script uses.
RELEASE="${RELEASE:-smoke}"
IMAGE="${IMAGE:-mqttd:smoke}"
CHART="$REPO_ROOT/deploy/helm/mqttd"
SMOKE_VALUES="$CHART/ci/values-smoke.yaml"
STS="$RELEASE-mqttd"
READY_TIMEOUT="${READY_TIMEOUT:-360s}"

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

dump() {
  echo "::group::diagnostics"
  kubectl -n "$NS" get pods -o wide || true
  kubectl -n "$NS" get pvc,statefulset,svc || true
  kubectl -n "$NS" describe pods || true
  for p in $(kubectl -n "$NS" get pods -o name 2>/dev/null); do
    echo "---- logs $p (all containers, tail) ----"
    kubectl -n "$NS" logs "$p" --all-containers --tail=120 || true
    # The membership/gossip timeline lives at FORMATION (log start), which the tail
    # above misses under retained-commit spam. Capture it compactly so a failed run
    # explains any SWIM isolation (2026-07-20 post-mortem, follow-up 2).
    echo "---- $p membership/gossip timeline ----"
    kubectl -n "$NS" logs "$p" -c mqttd 2>/dev/null \
      | grep -Ei 'membership|swim|gossip|peer link|establishing|alive|suspect|dead|drop|isolat|voter|lease assign|DIAG' \
      | head -250 || true
  done
  echo "::endgroup::"
}

cleanup() { kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap 'rc=$?; if [ $rc -ne 0 ]; then dump; fi; cleanup; exit $rc' EXIT

# A throwaway mosquitto client pod runs pub/sub against the client Service.
#
# NEVER use `kubectl run -i` here. `-i` attaches to the container, and a client
# that finishes BEFORE the attach completes has its output dropped on the floor
# ("couldn't attach …: container not found in pod", then a fallback log stream
# that has already missed the write). A fast retained replay loses that race
# routinely — observed 3/6 on an arm64 Docker Desktop VM against a cluster that
# was demonstrably serving the value every time (issue #86). Instead: run the pod
# to completion, then read its logs. Deterministic, attach-free, same cost.
mqtt() { # mqtt <pub|sub> <args...> — returns the client's stdout
  local verb="$1"; shift
  local pod="mqtt-$verb-$RANDOM"
  kubectl -n "$NS" run "$pod" --restart=Never --image=eclipse-mosquitto:2 \
    --command -- "mosquitto_$verb" "$@" >/dev/null 2>&1
  # Wait for the client to finish (either outcome is fine — a `sub` that times
  # out without a message exits non-zero and its logs are simply empty).
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded "pod/$pod" \
    --timeout=60s >/dev/null 2>&1 ||
    kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Failed "pod/$pod" \
      --timeout=5s >/dev/null 2>&1 || true
  kubectl -n "$NS" logs "$pod" 2>/dev/null || true
  kubectl -n "$NS" delete pod "$pod" --wait=false >/dev/null 2>&1 || true
}

# Read one retained message from a topic and return ONLY the payload.
mqtt_read() { # mqtt_read <topic>
  mqtt sub -h "$RELEASE-mqttd.$NS.svc" -t "$1" -C 1 -W 15 | tr -d '\r\n'
}

log "Build broker + test image ($IMAGE)"
if [ "$(uname -s)" = "Darwin" ]; then
  # macOS host: the kind node runs Linux, so a host build would produce a Mach-O
  # binary the pod cannot exec. Build inside a Linux container instead (native
  # arch under Docker Desktop; rustup picks up the repo's pinned toolchain).
  # A named volume caches the cargo registry, and the target dir lives under the
  # repo's gitignored target/ so re-runs are incremental.
  docker volume create mqttd-smoke-cargo >/dev/null
  docker run --rm -v "$REPO_ROOT":/src -w /src \
    -v mqttd-smoke-cargo:/usr/local/cargo/registry \
    -e CARGO_TARGET_DIR=/src/target/smoke-linux \
    rust:slim sh -ec 'apt-get update -qq >/dev/null && \
      apt-get install -y -qq cmake gcc g++ make perl pkg-config >/dev/null && \
      cargo build --release -p mqttd' 
  cp "$REPO_ROOT/target/smoke-linux/release/mqttd" "$REPO_ROOT/dist-smoke-mqttd"
else
  cargo build --release -p mqttd --manifest-path "$REPO_ROOT/Cargo.toml"
  cp "$REPO_ROOT/target/release/mqttd" "$REPO_ROOT/dist-smoke-mqttd"
fi
docker build -t "$IMAGE" -f - "$REPO_ROOT" <<'DOCKERFILE'
FROM debian:stable-slim
COPY dist-smoke-mqttd /usr/local/bin/mqttd
ENTRYPOINT ["/usr/local/bin/mqttd"]
DOCKERFILE
rm -f "$REPO_ROOT/dist-smoke-mqttd"

log "Create kind cluster '$CLUSTER' + load images"
kind create cluster --name "$CLUSTER" --wait 120s
kind load docker-image "$IMAGE" --name "$CLUSTER"
# Preload the busybox init image so the render init container needs no registry pull.
docker pull busybox:1.36 && kind load docker-image busybox:1.36 --name "$CLUSTER"

log "helm install the chart (smoke values)"
kubectl create namespace "$NS"
helm install "$RELEASE" "$CHART" -n "$NS" -f "$SMOKE_VALUES" \
  --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE#*:}" \
  --set image.pullPolicy=Never --set initImage.pullPolicy=IfNotPresent

log "Wait for the StatefulSet to roll out (all 3 pods Ready = mesh + lease group formed)"
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout="$READY_TIMEOUT"
kubectl -n "$NS" get pods -o wide

log "Connectivity + durable retained publish"
# Publish a RETAINED message; a fresh subscriber must receive it (retained state is durable).
mqtt pub -h "$RELEASE-mqttd.$NS.svc" -t smoke/state -m "hello-v1" -q 1 -r
got="$(mqtt_read smoke/state)"
echo "retained read back: '$got'"
[ "$got" = "hello-v1" ] || { echo "FAIL: retained message not delivered"; exit 1; }

log "Scale down 3 -> 2 (must DRAIN via preStop --decommission, not crash)"
kubectl -n "$NS" scale "statefulset/$STS" --replicas=2
# The departing pod is ordinal 2. Give the drain + graceful shutdown time.
kubectl -n "$NS" wait --for=delete "pod/$STS-2" --timeout=120s
# The remaining pods stay Ready (quorum held); the retained state survives the shrink.
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout=120s
got2="$(mqtt_read smoke/state)"
[ "$got2" = "hello-v1" ] || { echo "FAIL: retained state lost across scale-down"; exit 1; }
echo "retained state survived scale-down: '$got2'"

log "Rolling restart (quorum-safe, one at a time) — durable state must survive"
kubectl -n "$NS" rollout restart "statefulset/$STS"
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout="$READY_TIMEOUT"
got3="$(mqtt_read smoke/state)"
[ "$got3" = "hello-v1" ] || { echo "FAIL: retained state lost across rolling restart"; exit 1; }
echo "retained state survived rolling restart: '$got3'"

# The roll must still hold a minute LATER (issue #92). `rollout status` returns the
# instant every pod is Ready, and this smoke used to read its value and tear the
# cluster down right then — which is why it stayed green through a bug that left a
# rolled pod NotReady forever: a stale Dead claim about the pod's previous life
# re-killed it ~30s after the roll (dead_ttl_ms), every time.
log "Post-roll stability — every pod must STILL be Ready after the membership settles"
sleep 90
not_ready="$(kubectl -n "$NS" get pods -l "app.kubernetes.io/name=mqttd" \
  -o 'jsonpath={range .items[*]}{.metadata.name}={.status.conditions[?(@.type=="Ready")].status} {end}')"
case "$not_ready" in
  *=False*|*=Unknown*)
    kubectl -n "$NS" get pods
    echo "FAIL: a pod fell out of Ready after the roll settled: $not_ready"
    exit 1
    ;;
esac
echo "all pods still Ready 90s after the roll: $not_ready"

log "SMOKE PASSED: cluster formed, drained on scale-down, and survived a quorum-safe roll that STAYED healthy"
