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
    echo "---- logs $p (all containers) ----"
    kubectl -n "$NS" logs "$p" --all-containers --tail=120 || true
  done
  echo "::endgroup::"
}

cleanup() { kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap 'rc=$?; if [ $rc -ne 0 ]; then dump; fi; cleanup; exit $rc' EXIT

# A throwaway mosquitto client pod runs pub/sub against the client Service.
mqtt() { # mqtt <pub|sub> <args...>
  local verb="$1"; shift
  kubectl -n "$NS" run "mqtt-$verb-$RANDOM" --rm -i --restart=Never \
    --image=eclipse-mosquitto:2 --command -- "mosquitto_$verb" "$@"
}

log "Build broker + test image ($IMAGE)"
cargo build --release -p mqttd --manifest-path "$REPO_ROOT/Cargo.toml"
cp "$REPO_ROOT/target/release/mqttd" "$REPO_ROOT/dist-smoke-mqttd"
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
got="$(mqtt sub -h "$RELEASE-mqttd.$NS.svc" -t smoke/state -C 1 -W 15 | tr -d '\r\n')"
echo "retained read back: '$got'"
[ "$got" = "hello-v1" ] || { echo "FAIL: retained message not delivered"; exit 1; }

log "Scale down 3 -> 2 (must DRAIN via preStop --decommission, not crash)"
kubectl -n "$NS" scale "statefulset/$STS" --replicas=2
# The departing pod is ordinal 2. Give the drain + graceful shutdown time.
kubectl -n "$NS" wait --for=delete "pod/$STS-2" --timeout=120s
# The remaining pods stay Ready (quorum held); the retained state survives the shrink.
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout=120s
got2="$(mqtt sub -h "$RELEASE-mqttd.$NS.svc" -t smoke/state -C 1 -W 15 | tr -d '\r\n')"
[ "$got2" = "hello-v1" ] || { echo "FAIL: retained state lost across scale-down"; exit 1; }
echo "retained state survived scale-down: '$got2'"

log "Rolling restart (quorum-safe, one at a time) — durable state must survive"
kubectl -n "$NS" rollout restart "statefulset/$STS"
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout="$READY_TIMEOUT"
got3="$(mqtt sub -h "$RELEASE-mqttd.$NS.svc" -t smoke/state -C 1 -W 15 | tr -d '\r\n')"
[ "$got3" = "hello-v1" ] || { echo "FAIL: retained state lost across rolling restart"; exit 1; }
echo "retained state survived rolling restart: '$got3'"

log "SMOKE PASSED: cluster formed, drained on scale-down, and survived a quorum-safe roll"
