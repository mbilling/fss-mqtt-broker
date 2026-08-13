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

cleanup() {
  kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true
  # The throwaway cluster PKI, CA private key included.
  [ -n "${PKI_DIR:-}" ] && rm -rf "$PKI_DIR"
  return 0
}
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

log "Mint the cluster-bus PKI with the SHIPPED bootstrap.sh (issue #262)"
kubectl create namespace "$NS"
# The recipe is EXECUTED, not restated: bootstrap.sh mints one CA plus ONE LEAF PER NODE
# (CN = pod name, SAN = that pod's advertise host, both EKUs, ECDSA P-256) and creates the
# Secrets. A shipped recipe this smoke does not run is a shipped recipe nothing checks —
# which is exactly how it came to ship a single shared RSA leaf that could not work.
# REPLICAS must match the chart's replicaCount, or an ordinal comes up with no leaf.
PKI_DIR="$(mktemp -d)"
NS="$NS" RELEASE="$RELEASE" REPLICAS=3 PKI_DIR="$PKI_DIR" "$CHART/bootstrap.sh"
# The CA private key must never enter the cluster — only ca.crt does. Assert it, because the
# whole trust model of the bus rests on it: anything holding this key can mint a leaf with
# any CN, and the bus binds node identity to the CN.
if kubectl -n "$NS" get secret mqttd-peer-tls -o jsonpath='{.data}' | grep -q 'cluster-ca.key'; then
  echo "FAIL: the cluster CA PRIVATE key was put into the peer-bus Secret"; exit 1
fi
# One leaf per pod, keyed by pod name — the property that makes a per-node CN possible.
for i in 0 1 2; do
  kubectl -n "$NS" get secret mqttd-peer-tls -o "jsonpath={.data.$STS-$i\\.crt}" | grep -q . \
    || { echo "FAIL: the peer-bus Secret has no leaf for pod $STS-$i"; exit 1; }
done
echo "peer-bus Secret carries ca.crt + one leaf per pod, and no CA private key"

log "helm install the chart (smoke values, cluster bus ON)"
helm install "$RELEASE" "$CHART" -n "$NS" -f "$SMOKE_VALUES" \
  --set image.repository="${IMAGE%:*}" --set image.tag="${IMAGE#*:}" \
  --set image.pullPolicy=Never --set initImage.pullPolicy=IfNotPresent

log "Wait for the StatefulSet to roll out (all 3 pods Ready = mesh + lease group formed)"
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout="$READY_TIMEOUT"
kubectl -n "$NS" get pods -o wide

# The cluster bus is the thing issue #262 left unexercised, so assert its posture directly
# rather than inferring it from readiness. Each pod must have a MUTUALLY AUTHENTICATED peer
# listener, signed per-node gossip, and — the leg that a shared certificate would fail —
# no link dropped on the node-id/CN binding. Tracing writes ANSI escapes between a field
# name and its value even through `kubectl logs`, so strip them before matching: a naive
# `grep mtls=true` silently never matches and the assertion passes vacuously.
log "Cluster-bus posture: mTLS peer links + signed per-node gossip on every pod"
nocolor() { sed $'s/\033\\[[0-9;]*m//g'; }
for i in 0 1 2; do
  plog="$(kubectl -n "$NS" logs "$STS-$i" -c mqttd 2>/dev/null | nocolor)"
  printf '%s' "$plog" | grep -Eq 'accepting cluster peer links.*mtls=true' \
    || { echo "FAIL: $STS-$i did not bring up a mutually-authenticated peer listener"
         printf '%s\n' "$plog" | grep -E 'peer|mtls' | head; exit 1; }
  printf '%s' "$plog" | grep -q 'SWIM gossip is SIGNED per-node' \
    || { echo "FAIL: $STS-$i did not arm signed per-node gossip"; exit 1; }
  printf '%s' "$plog" | grep -q 'INSECURE: starting PLAINTEXT peer listener' \
    && { echo "FAIL: $STS-$i ran a PLAINTEXT peer bus despite a peer-TLS Secret"; exit 1; }
  printf '%s' "$plog" | grep -q 'does not match its certificate Common Name' \
    && { echo "FAIL: $STS-$i dropped a peer link on the node-id/CN binding — the shared-leaf defect"
         printf '%s\n' "$plog" | grep 'Common Name'; exit 1; }
done
echo "all 3 pods: peer listener mtls=true, gossip SIGNED per-node, no CN-binding drops"

log "Connectivity + durable retained publish"
# Publish a RETAINED message; a fresh subscriber must receive it (retained state is durable).
mqtt pub -h "$RELEASE-mqttd.$NS.svc" -t smoke/state -m "hello-v1" -q 1 -r
got="$(mqtt_read smoke/state)"
echo "retained read back: '$got'"
[ "$got" = "hello-v1" ] || { echo "FAIL: retained message not delivered"; exit 1; }

log "Scale down 3 -> 2 (must DRAIN via preStop --decommission, not crash)"
# Scale through HELM, not `kubectl scale`: kubectl would take server-side-apply
# ownership of .spec.replicas and every later `helm upgrade` would fail with a field
# conflict (observed). It is also the more faithful gesture — a chart user scales by
# changing the value they installed with.
helm upgrade "$RELEASE" "$CHART" -n "$NS" -f "$SMOKE_VALUES" --reuse-values \
  --set replicaCount=2 >/dev/null
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

# Both split-brain mechanisms ship in the CHART as well as the operator, and until now
# only the operator path was proven end to end — which is backwards: a chart-only
# deployment has no controller to compensate, so it is the path that needs the proof
# most. The cluster is at 2 replicas here (the scale-down above), which is enough: the
# survivor is the majority and pod-0 is the one that re-founds.
#
# An attach-free reader (never `kubectl run --rm`: --attach is implied and hangs).
statusz() { # statusz <ordinal> [jq-ish field]
  kubectl -n "$NS" delete pod szread --ignore-not-found >/dev/null 2>&1
  kubectl -n "$NS" run szread --image=busybox:1.36 --restart=Never --command -- \
    sh -c "wget -qO- http://$STS-$1.$STS-headless.$NS.svc.cluster.local:8080/statusz 2>/dev/null" >/dev/null 2>&1
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Succeeded pod/szread --timeout=60s >/dev/null 2>&1
  kubectl -n "$NS" logs szread 2>/dev/null
  kubectl -n "$NS" delete pod szread --ignore-not-found >/dev/null 2>&1
}

log "Founder guard — arming it must make a wiped pod-0 REJOIN (ADR 0055 T9)"
# Order matters, and it cost a run to learn why. This step runs FIRST, on a healthy
# cluster: a StatefulSet rolls highest-ordinal-first, so if pod-0 were already
# NotReady (as the self-quarantine step below deliberately leaves it) the roll would
# never reach ordinal 0 and the controller would recreate it at the OLD revision —
# without the env, rendering empty seeds, and the test would fail for a reason that
# has nothing to do with the guard.
helm upgrade "$RELEASE" "$CHART" -n "$NS" -f "$SMOKE_VALUES" --reuse-values \
  --set clusterEstablished=true >/dev/null
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout="$READY_TIMEOUT"
kubectl -n "$NS" delete pod "$STS-0" --wait=true
kubectl -n "$NS" delete pvc "data-$STS-0" --wait=false
kubectl -n "$NS" delete pod "$STS-0" --grace-period=0 --force >/dev/null 2>&1 || true
for _ in $(seq 1 60); do
  [ "$(kubectl -n "$NS" get pod "$STS-0" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = "True" ] && break
  sleep 5
done
[ "$(kubectl -n "$NS" get pod "$STS-0" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null)" = "True" ] \
  || { echo "FAIL: pod-0 never became Ready with the guard armed — it did not rejoin"; exit 1; }
kubectl -n "$NS" logs "$STS-0" -c render-config 2>/dev/null | grep -E '^seeds = \[".+"\]' >/dev/null \
  || { echo "FAIL: pod-0 rendered an EMPTY seed list — the chart's founder guard did not apply"; exit 1; }
id0="$(statusz 0 | sed 's/.*"cluster_id":"\([^"]*\)".*/\1/')"
id1="$(statusz 1 | sed 's/.*"cluster_id":"\([^"]*\)".*/\1/')"
[ -n "$id0" ] && [ "$id0" = "$id1" ] \
  || { echo "FAIL: pod-0 did not rejoin — identities differ (pod-0=$id0 pod-1=$id1)"; exit 1; }
echo "founder guard held on the CHART path: pod-0 rejoined the same cluster ($id0)"

log "Self-quarantine — with the guard OFF, a re-founded pod-0 must refuse to serve"
# Terminal step: it deliberately leaves one pod permanently NotReady, so nothing may
# follow it. Disarm the guard first — with it armed the divergence this observes
# cannot happen at all, which is the whole point of the step above.
helm upgrade "$RELEASE" "$CHART" -n "$NS" -f "$SMOKE_VALUES" --reuse-values \
  --set clusterEstablished=false >/dev/null
kubectl -n "$NS" rollout status "statefulset/$STS" --timeout="$READY_TIMEOUT"
kubectl -n "$NS" delete pod "$STS-0" --wait=true
kubectl -n "$NS" delete pvc "data-$STS-0" --wait=false
kubectl -n "$NS" delete pod "$STS-0" --grace-period=0 --force >/dev/null 2>&1 || true
for _ in $(seq 1 40); do
  kubectl -n "$NS" get pod "$STS-0" >/dev/null 2>&1 && break
  sleep 3
done
sleep 45   # let it boot, mint its own identity, and hear the survivor's gossip
sz="$(statusz 0)"
case "$sz" in
  *'"quarantine":{"active":true'*) : ;;
  *) echo "FAIL: the re-founded pod-0 did not self-quarantine; /statusz was: $sz"; exit 1;;
esac
ready0="$(kubectl -n "$NS" get pod "$STS-0" -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
[ "$ready0" = "True" ] && { echo "FAIL: the re-founded pod-0 is Ready — it is serving an empty store"; exit 1; }
eps="$(kubectl -n "$NS" get endpoints "$RELEASE-mqttd" -o jsonpath='{.subsets[*].addresses[*].targetRef.name}' 2>/dev/null || true)"
case "$eps" in
  *"$STS-0"*) echo "FAIL: pod-0 is still in the client Service endpoints: $eps"; exit 1;;
esac
echo "self-quarantine held: pod-0 refuses readiness and is out of the Service endpoints"

log "SMOKE PASSED: cluster formed over a MUTUALLY AUTHENTICATED bus (per-node certs from the shipped bootstrap.sh, signed per-node gossip), drained on scale-down, survived a quorum-safe roll that STAYED healthy, self-quarantined a re-founder, and rejoined a wiped pod-0 once the founder guard was armed"
