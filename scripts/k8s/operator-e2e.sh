#!/usr/bin/env bash
# Operator end-to-end (ADR 0055 T7) — the runtime proof that the reconciler does
# what its unit tests claim, against a real API server.
#
# Four things are asserted, in order of how much they matter:
#
#   1. The operator CREATES a working cluster from a CR (apply → StatefulSet →
#      3/3 Ready → status.phase=Ready with one clusterId).
#   2. A routine ROLLING RESTART provokes NO fence. This is the safety property:
#      unreachable pods must read as absent evidence, never as a split brain.
#      A controller that fails this would attack healthy nodes during ordinary
#      operations, so it is asserted BEFORE the fence is proven to work at all.
#   3. A SCALE cycle (3->4->3) resizes the set, keeps the cluster identity, raises
#      no fence, and leaves EVERY claim intact — the departed pod's included, since
#      no scale may destroy the only copy of anything (issue #97).
#   4. An INDUCED SPLIT BRAIN (wipe the founder's volume so it re-founds) is
#      detected and, with remediation opt-in, fenced — PVC labelled, pod deleted,
#      DATA NEVER DELETED — and the verdict HOLDS: the fence fires once, so the
#      incident stays visible instead of flickering behind a restart loop. The
#      re-founded pod must ALSO have quarantined itself, so it serves no clients
#      while the incident stands.
#
# Requires: docker, kind, kubectl, cargo. Builds both images itself. Nightly tier.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CLUSTER="${CLUSTER:-mqttd-op-e2e}"
NS="${NS:-mqttd-e2e}"
BROKER_IMAGE="mqttd:e2e"
OPERATOR_IMAGE="mqttd-operator:e2e"
READY_TIMEOUT="${READY_TIMEOUT:-300s}"

log() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }
fail() { printf '\033[1;31mFAIL: %s\033[0m\n' "$*"; exit 1; }

dump() {
  echo "::group::diagnostics"
  kubectl -n "$NS" get pods,pvc,statefulset,mqttdclusters -o wide || true
  kubectl -n "$NS" describe mqttdclusters || true
  kubectl -n "$NS" logs deploy/mqttd-operator --tail=80 || true
  # Broker-side logs, per pod and per container. Without these a failure looks like
  # "the operator did not converge" when the real story is in the pod: a first run
  # died because a FULL DISK made the render init container write a 0-byte config
  # and the broker booted on defaults, which no operator-side log could show.
  for pod in $(kubectl -n "$NS" get pods -l app.kubernetes.io/name=mqttd -o name 2>/dev/null); do
    echo "--- $pod render-config ---"
    kubectl -n "$NS" logs "$pod" -c render-config --tail=40 || true
    echo "--- $pod mqttd (current) ---"
    kubectl -n "$NS" logs "$pod" -c mqttd --tail=200 || true
    echo "--- $pod mqttd (previous) ---"
    kubectl -n "$NS" logs "$pod" -c mqttd --previous --tail=40 2>/dev/null || true
  done
  kubectl -n "$NS" get events --sort-by=.lastTimestamp | tail -30 || true
  # Node pressure is invisible in every object above and breaks the run in ways that
  # read as broker bugs.
  kubectl describe nodes | grep -A 6 "Conditions:" | head -20 || true
  echo "::endgroup::"
}
cleanup() { kind delete cluster --name "$CLUSTER" >/dev/null 2>&1 || true; }
trap 'rc=$?; if [ $rc -ne 0 ]; then dump; fi; cleanup; exit $rc' EXIT

# The CR's status field, or empty.
cr_status() { # cr_status <jsonpath-under-.status>
  kubectl -n "$NS" get mqttdcluster e2e -o "jsonpath={.status.$1}" 2>/dev/null || true
}
# The status of one condition type, or empty.
cr_condition() { # cr_condition <type>
  kubectl -n "$NS" get mqttdcluster e2e \
    -o "jsonpath={.status.conditions[?(@.type=='$1')].status}" 2>/dev/null || true
}
# Poll until `cond` holds or the deadline passes.
wait_for() { # wait_for <seconds> <description> <command...>
  local deadline=$(( $(date +%s) + $1 )); local what="$2"; shift 2
  while [ "$(date +%s)" -lt "$deadline" ]; do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 3
  done
  fail "timed out waiting for: $what"
}

log "Build broker + operator images"
build_linux() { # build_linux <package> <output path>
  if [ "$(uname -s)" = "Darwin" ]; then
    docker volume create mqttd-e2e-cargo >/dev/null
    docker run --rm -v "$REPO_ROOT":/src -w /src \
      -v mqttd-e2e-cargo:/usr/local/cargo/registry \
      -e CARGO_TARGET_DIR=/src/target/e2e-linux \
      rust:slim sh -ec "apt-get update -qq >/dev/null && \
        apt-get install -y -qq cmake gcc g++ make perl pkg-config >/dev/null && \
        cargo build --release -p $1"
    cp "$REPO_ROOT/target/e2e-linux/release/$1" "$2"
  else
    cargo build --release -p "$1" --manifest-path "$REPO_ROOT/Cargo.toml"
    cp "$REPO_ROOT/target/release/$1" "$2"
  fi
}
build_linux mqttd "$REPO_ROOT/dist-e2e-mqttd"
build_linux mqttd-operator "$REPO_ROOT/dist-e2e-operator"
docker build -q -t "$BROKER_IMAGE" -f - "$REPO_ROOT" <<'DOCKERFILE' >/dev/null
FROM debian:stable-slim
COPY dist-e2e-mqttd /usr/local/bin/mqttd
ENTRYPOINT ["/usr/local/bin/mqttd"]
DOCKERFILE
docker build -q -t "$OPERATOR_IMAGE" -f - "$REPO_ROOT" <<'DOCKERFILE' >/dev/null
FROM debian:stable-slim
COPY dist-e2e-operator /usr/local/bin/mqttd-operator
ENTRYPOINT ["/usr/local/bin/mqttd-operator"]
DOCKERFILE
rm -f "$REPO_ROOT/dist-e2e-mqttd" "$REPO_ROOT/dist-e2e-operator"

log "Create kind cluster + load images"
kind create cluster --name "$CLUSTER" --wait 120s
kind load docker-image "$BROKER_IMAGE" "$OPERATOR_IMAGE" --name "$CLUSTER"
docker pull -q busybox:1.36 >/dev/null && kind load docker-image busybox:1.36 --name "$CLUSTER"

log "Install the CRD + operator"
kubectl create namespace "$NS"
kubectl apply -f "$REPO_ROOT/deploy/crds/mqttd.io_mqttdclusters.json"
kubectl -n "$NS" apply -f "$REPO_ROOT/deploy/operator/operator.yaml"
kubectl -n "$NS" rollout status deploy/mqttd-operator --timeout=120s

log "1/4 — apply a CR; the operator must CREATE a working cluster"
# Fence is enabled from the start: assertion 2 proves it does NOT fire on a
# routine roll, which is only meaningful if it was armed the whole time.
kubectl -n "$NS" apply -f - <<EOF
apiVersion: mqttd.io/v1alpha1
kind: MqttdCluster
metadata:
  name: e2e
spec:
  replicas: 3
  image: $BROKER_IMAGE
  remediation:
    splitBrain: Fence
  config: |
    [node]
    id = "__NODE_ID__"
    data_dir = "/var/lib/mqttd"

    [listeners]
    plaintext_bind = "0.0.0.0:1883"
    health_bind = "0.0.0.0:8080"

    [security]
    allow_anonymous = true

    [cluster]
    peer_bind = "0.0.0.0:7001"
    peer_advertise = "__PEER_ADVERTISE__"

    [cluster.swim]
    bind = "0.0.0.0:7946"
    seeds = [__SEEDS__]

    [durable]
    enabled = true
    # 5, matching the chart default — NOT the replica count. Readiness requires that
    # this node be a lease VOTER, so a node beyond the voter cap can never become
    # Ready; with `lease_voters = 3` the scale-up below produced a 4th pod that was
    # correctly, permanently NotReady, and OrderedReady then stalled forever.
    lease_voters = 5

    [runtime]
    ready_min_members = __READY_MIN__
EOF

wait_for 120 "the operator to create the StatefulSet" \
  kubectl -n "$NS" get statefulset e2e
kubectl -n "$NS" rollout status statefulset/e2e --timeout="$READY_TIMEOUT"
wait_for 180 "status.phase=Ready" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath='{.status.phase}')\" = Ready ]"

CLUSTER_ID="$(cr_status clusterId)"
[ -n "$CLUSTER_ID" ] || fail "status.clusterId is empty — the operator did not observe an identity"
[ "$(cr_status readyReplicas)" = "3" ] || fail "expected readyReplicas=3, got '$(cr_status readyReplicas)'"
[ "$(cr_condition SplitBrain)" = "False" ] || fail "SplitBrain should be False on a healthy cluster"
# The objects must be OWNED: deleting the CR would garbage-collect them.
kubectl -n "$NS" get statefulset e2e -o jsonpath='{.metadata.ownerReferences[0].kind}' \
  | grep -q MqttdCluster || fail "StatefulSet is not owned by the CR"
echo "cluster created by the operator; clusterId=$CLUSTER_ID, ownership confirmed"

log "2/4 — a rolling restart must provoke NO fence (the safety property)"
BEFORE_PVCS="$(kubectl -n "$NS" get pvc -l "$(printf 'mqttd.io/quarantined=true')" -o name | wc -l | tr -d ' ')"
kubectl -n "$NS" rollout restart statefulset/e2e
kubectl -n "$NS" rollout status statefulset/e2e --timeout="$READY_TIMEOUT"
# Give the operator several reconciles over the churn.
sleep 45
[ "$(cr_condition SplitBrain)" = "False" ] \
  || fail "a rolling restart was mistaken for a split brain — the fence would attack healthy nodes"
AFTER_PVCS="$(kubectl -n "$NS" get pvc -l mqttd.io/quarantined=true -o name | wc -l | tr -d ' ')"
[ "$BEFORE_PVCS" = "$AFTER_PVCS" ] || fail "a PVC was quarantined during a routine roll"
[ "$(cr_status clusterId)" = "$CLUSTER_ID" ] || fail "cluster identity changed across a roll"
echo "rolling restart completed with no fence and a stable identity"

log "3/4 — scale up then down; the operator must resize the set and keep data"
# The rendered config carries REPLICAS (the readiness-floor math), so a scale is also
# a config change: the operator must roll the set to the new shape, not just edit
# .spec.replicas. Withheld until issue #92 was fixed — the roll a scale implies left
# a pod NotReady forever, so OrderedReady never reached the new ordinal.
kubectl -n "$NS" patch mqttdcluster e2e --type=merge -p '{"spec":{"replicas":4}}'
wait_for 240 "the operator to scale the StatefulSet to 4" \
  bash -c "[ \"\$(kubectl -n $NS get statefulset e2e -o jsonpath='{.spec.replicas}')\" = 4 ]"
kubectl -n "$NS" rollout status statefulset/e2e --timeout="$READY_TIMEOUT"
wait_for 240 "status.readyReplicas=4" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath='{.status.readyReplicas}')\" = 4 ]"

kubectl -n "$NS" patch mqttdcluster e2e --type=merge -p '{"spec":{"replicas":3}}'
wait_for 240 "the departing pod to be gone" \
  bash -c "! kubectl -n $NS get pod e2e-3 >/dev/null 2>&1"
# EVERY claim must survive, the departed pod's included: both retention policies are
# now Retain (issue #97). `whenScaled: Delete` is only safe while a survivor remains to
# receive the ADR 0043 drain, and Kubernetes applies the policy uniformly — at
# replicas=0 the same setting erased the only copy of everything. An orphaned volume is
# visible and reversible; silent total loss is not.
for ord in 0 1 2 3; do
  kubectl -n "$NS" get "pvc/data-e2e-$ord" >/dev/null \
    || fail "PVC data-e2e-$ord was destroyed by a scale cycle — no scale may delete data"
done
[ "$(cr_status clusterId)" = "$CLUSTER_ID" ] || fail "cluster identity changed across a scale cycle"
[ "$(cr_condition SplitBrain)" = "False" ] || fail "a scale cycle was mistaken for a split brain"
echo "scaled 3->4->3; identity stable, no fence, EVERY PVC intact (incl. the departed pod's)"

log "4/4 — induce a split brain; it must be detected and fenced"
# Wipe the founder's volume: pod-0 renders with no seeds, so an empty data dir
# makes it found a NEW cluster beside the survivors — the exact scenario
# OPERATIONS.md warns about.
kubectl -n "$NS" delete pod e2e-0 --wait=true
kubectl -n "$NS" delete pvc data-e2e-0 --wait=false
kubectl -n "$NS" delete pod e2e-0 --grace-period=0 --force >/dev/null 2>&1 || true
wait_for 240 "the re-founded pod to come back" \
  bash -c "[ \"\$(kubectl -n $NS get pod e2e-0 -o jsonpath='{.status.phase}' 2>/dev/null)\" = Running ]"

# Remediation: the minority founder's PVC is quarantined by LABEL, never deleted.
wait_for 240 "the re-founder's PVC to be quarantined" \
  bash -c "kubectl -n $NS get pvc data-e2e-0 -o jsonpath='{.metadata.labels.mqttd\\.io/quarantined}' | grep -q true"
kubectl -n "$NS" get pvc data-e2e-0 >/dev/null || fail "the PVC was DELETED — data must never be destroyed"
echo "split brain FENCED: PVC quarantined by label, still present (data intact)"

# Detection must PERSIST. A fenced pod comes straight back and re-founds (its
# volume still holds the divergent state), so the divergence is real until a human
# runs the documented wipe. The operator fences ONCE: it must not keep killing the
# pod, because every kill removes the divergent node from the observation and drops
# SplitBrain back to False. An earlier build did exactly that — fence every 30s,
# condition True for ~25ms per cycle — which no alert and no human would ever see.
wait_for 240 "SplitBrain=True" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath=\"{.status.conditions[?(@.type=='SplitBrain')].status}\")\" = True ]"
echo "split brain DETECTED; checking the verdict HOLDS (no re-fence loop)"
held=0
for _ in $(seq 1 20); do
  [ "$(cr_condition SplitBrain)" = "True" ] && held=$((held + 1))
  sleep 5
done
[ "$held" -ge 18 ] \
  || fail "SplitBrain held for only $held/20 samples — the operator is re-fencing and hiding the incident"

# The re-founder must have taken ITSELF out of rotation (issue #92 follow-up): it minted
# a second identity and then heard the surviving cluster's gossip, so it latches NotReady
# rather than serve clients an empty session and retained store. Without this the pod is
# Ready — the founder readiness floor is 1 — and takes a share of client connections.
ready0="$(kubectl -n "$NS" get pod e2e-0 \
  -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
[ "$ready0" = "True" ] \
  && fail "the re-founded pod-0 is READY — it is serving clients from an empty store"
echo "the re-founded pod-0 self-quarantined (Ready=$ready0), so it serves no clients"
kubectl -n "$NS" get pvc data-e2e-0 >/dev/null || fail "the PVC was DELETED — data must never be destroyed"
echo "split brain verdict STABLE ($held/20 samples) with the PVC intact"

log "OPERATOR E2E PASSED: created a cluster, ignored a routine roll, scaled up and down without data loss, detected and fenced a real split brain exactly once"
