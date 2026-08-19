#!/usr/bin/env bash
# Operator end-to-end (ADR 0055 T7) — the runtime proof that the reconciler does
# what its unit tests claim, against a real API server.
#
# Nine things are asserted, in order of how much they matter:
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
#   5. WIPING THE FOUNDER'S VOLUME no longer splits the cluster: with the founder
#      guard armed, ordinal 0 renders seeds, so an empty data dir makes it REJOIN
#      the surviving cluster instead of minting a second one (ADR 0055 T9).
#   6. BREAK GLASS — with re-founding explicitly re-permitted, a real split brain
#      is still detected and fenced (PVC labelled, pod deleted, DATA NEVER
#      DELETED), the verdict HOLDS because the fence fires once, and the
#      re-founder has ALSO quarantined itself so it serves no clients.
#   4. BROWNOUT is detected and surfaced (ExpandPvc had no runtime coverage at all);
#      expansion itself is asserted only where the StorageClass can expand, and the
#      run says which case it exercised rather than passing silently. It runs here,
#      while the cluster is HEALTHY, because it changes the config — and a config
#      roll cannot complete once a later step has left a pod permanently unready.
#   7. THE OPERATOR RESTARTING mid-incident does not re-fence — fence-once is
#      enforced by cluster state (the PVC label), not by operator memory.
#   8. LEADER FAILOVER: with two replicas, killing the Lease holder hands over and
#      the survivor actually reconciles.
#   9. DELETING THE CR collects every owned object and keeps every volume.
#
# Plus, before any of it: the operator's RBAC is minimal, not merely sufficient.
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

# The operator's RBAC must be MINIMAL, not merely sufficient. The e2e passing proves
# sufficiency; nothing proved the limits, and the most load-bearing of them — "the
# absence of a PVC delete verb is the never-destroy-data guarantee" — lived only in a
# comment. Assert it, and assert a verb it DOES need, so a blanket deny (a broken
# ServiceAccount reference, say) cannot make the negatives pass vacuously.
sa="system:serviceaccount:$NS:mqttd-operator"
can() { kubectl auth can-i "$1" "$2" --as="$sa" -n "$NS" 2>/dev/null; }
[ "$(can delete persistentvolumeclaims)" = "no" ] \
  || fail "the operator CAN delete PVCs — the never-destroy-data guarantee is gone"
[ "$(can get secrets)" = "no" ] \
  || fail "the operator can READ Secrets; it references them by name and must never read material"
[ "$(can delete secrets)" = "no" ] || fail "the operator can delete Secrets"
[ "$(can delete pods)" = "yes" ] \
  || fail "the operator cannot delete pods — the fence needs that, so these can-i checks are vacuous"
echo "RBAC is minimal: no PVC deletion, no Secret access, pod deletion (the fence) intact"

log "1/9 — apply a CR; the operator must CREATE a working cluster"
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

# The FOUNDER GUARD arms itself (ADR 0055 T9): having observed one identity, the
# operator latches status.bootstrapped and re-renders ordinal 0 WITH seeds, so a
# pod-0 that later loses its volume rejoins instead of founding a second cluster.
wait_for 180 "status.bootstrapped to latch" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath='{.status.bootstrapped}')\" = true ]"
wait_for 180 "the StatefulSet to re-render with CLUSTER_ESTABLISHED" \
  bash -c "kubectl -n $NS get statefulset e2e -o jsonpath='{.spec.template.spec.initContainers[0].env[*].name}' | grep -q CLUSTER_ESTABLISHED"
kubectl -n "$NS" rollout status statefulset/e2e --timeout="$READY_TIMEOUT"
echo "founder guard armed: ordinal 0 can no longer found"

log "2/9 — a rolling restart must provoke NO fence (the safety property)"
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

log "3/9 — scale up then down; the operator must resize the set and keep data"
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

log "4/9 — brownout must be DETECTED (the remediation with no runtime coverage)"
# Placed HERE, while the cluster is healthy, and that placement is load-bearing: this
# step changes the config, a config change rolls the StatefulSet, and a roll cannot
# complete while any pod is unready — which the steps below deliberately arrange. Run
# after them and the patch never reaches a single broker (observed: pods untouched,
# no "disk watermark active" anywhere, brownout never raised).
#
# ExpandPvc has been unit-tested at plan() only and never run against a cluster.
# store_max_bytes = 1 browns out immediately (a positive value is accepted; only 0 is
# rejected), so no publish load is needed to reach the state.
kubectl -n "$NS" get mqttdcluster e2e -o jsonpath='{.spec.config}' > /tmp/e2e-config-orig.toml
kubectl -n "$NS" patch mqttdcluster e2e --type=merge \
  -p '{"spec":{"remediation":{"brownout":"ExpandPvc"}}}'
# INSERT under [durable] — do not append. TOML is section-scoped, the config ends with
# [runtime], and the structs are deny_unknown_fields, so an appended key lands in the
# wrong table and `--check-config` rejects the whole file. (It did: the check-config
# init container CrashLoopBackOff'd, which is that gate working exactly as intended.)
python3 -c "
import sys
src, dst = sys.argv[1], sys.argv[2]
lines = open(src).read().splitlines()
out = []
for line in lines:
    out.append(line)
    if line.strip() == '[durable]':
        out.append('store_max_bytes = 1')
assert any(l == 'store_max_bytes = 1' for l in out), 'no [durable] section to patch'
open(dst, 'w').write('\n'.join(out) + '\n')
" /tmp/e2e-config-orig.toml /tmp/e2e-config-brownout.toml
grep -A 1 '^\[durable\]' /tmp/e2e-config-brownout.toml
set_config() { # set_config <file>
  kubectl -n "$NS" patch mqttdcluster e2e --type=merge \
    -p "$(python3 -c "
import json,sys
print(json.dumps({'spec': {'config': open(sys.argv[1]).read()}}))" "$1")"
}
set_config /tmp/e2e-config-brownout.toml
# Wait for DIRECT evidence the config reached a broker, rather than for `rollout status`.
# Whether a browned-out node stays Ready is precisely one of the things not yet
# established, so gating on readiness here would couple this assertion to an unknown.
wait_for 300 "a broker to load the watermark" \
  bash -c "kubectl -n $NS logs -l app.kubernetes.io/name=mqttd -c mqttd --tail=-1 2>/dev/null | grep -q 'disk watermark active'"
wait_for 240 "the Brownout condition to be raised" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath=\"{.status.conditions[?(@.type=='Brownout')].status}\")\" = True ]"
echo "brownout DETECTED and surfaced as a condition"
ready_in_brownout="$(kubectl -n "$NS" get pods -l app.kubernetes.io/name=mqttd \
  -o 'jsonpath={range .items[*]}{.metadata.name}={.status.conditions[?(@.type=="Ready")].status} {end}' 2>/dev/null || true)"
echo "readiness while browned out: $ready_in_brownout"

# Expansion needs an expansion-capable StorageClass. kind's local-path provisioner is
# usually not one, and a test that silently passes on a cluster that cannot expand
# proves nothing — so say which case this run exercised.
sc="$(kubectl -n "$NS" get pvc data-e2e-1 -o jsonpath='{.spec.storageClassName}' 2>/dev/null || true)"
expandable="$(kubectl get storageclass "$sc" -o jsonpath='{.allowVolumeExpansion}' 2>/dev/null || true)"
if [ "$expandable" = "true" ]; then
  wait_for 300 "the PVC to be expanded" \
    bash -c "[ \"\$(kubectl -n $NS get pvc data-e2e-1 -o jsonpath='{.spec.resources.requests.storage}')\" != '1Gi' ]"
  echo "brownout REMEDIATED: PVC expanded on an expansion-capable StorageClass ($sc)"
else
  echo "NOTE: StorageClass '$sc' has allowVolumeExpansion='$expandable' — expansion is NOT"
  echo "      exercised on this cluster. Detection is asserted; the ExpandPvc patch path"
  echo "      still needs a run against an expansion-capable CSI."
fi

# Revert, so everything below runs on a broker that is not refusing growth-writes.
set_config /tmp/e2e-config-orig.toml
kubectl -n "$NS" patch mqttdcluster e2e --type=merge \
  -p '{"spec":{"remediation":{"brownout":"Alert"}}}'
kubectl -n "$NS" rollout status statefulset/e2e --timeout="$READY_TIMEOUT"
wait_for 240 "the Brownout condition to clear" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath=\"{.status.conditions[?(@.type=='Brownout')].status}\")\" != True ]"
echo "watermark reverted; the cluster is out of brownout"

log "5/9 — wipe the founder's volume; the guard must make it REJOIN, not re-found"
# The same induction that used to create a split brain: destroy pod-0's data dir while
# the cluster is live. With the founder guard armed (assertion 1) ordinal 0 renders
# seeds, so the empty volume makes it a JOINER — it adopts the surviving identity and
# back-fills (ADR 0043) instead of minting a second cluster beside the survivors.
kubectl -n "$NS" delete pod e2e-0 --wait=true
kubectl -n "$NS" delete pvc data-e2e-0 --wait=false
kubectl -n "$NS" delete pod e2e-0 --grace-period=0 --force >/dev/null 2>&1 || true
wait_for 240 "pod-0 to come back" \
  bash -c "[ \"\$(kubectl -n $NS get pod e2e-0 -o jsonpath='{.status.phase}' 2>/dev/null)\" = Running ]"

# It must have rendered seeds — the mechanism, checked directly rather than inferred.
kubectl -n "$NS" logs e2e-0 -c render-config 2>/dev/null | grep -E '^seeds = \[".+"\]' >/dev/null \
  || fail "pod-0 rendered an EMPTY seed list — the founder guard did not apply, so it will re-found"

# No split brain, and it STAYS that way: the identity is unchanged and nothing is fenced.
held=0
for _ in $(seq 1 20); do
  [ "$(cr_condition SplitBrain)" = "False" ] && held=$((held + 1))
  sleep 5
done
[ "$held" -ge 18 ] \
  || fail "a split brain appeared after wiping pod-0 ($held/20 samples clean) — the guard failed"
[ "$(cr_status clusterId)" = "$CLUSTER_ID" ] \
  || fail "the cluster identity changed — pod-0 founded instead of joining"
kubectl -n "$NS" get pvc -l mqttd.io/quarantined=true -o name | grep -q . \
  && fail "a PVC was quarantined — the operator fenced a node that should have rejoined"
echo "founder guard held: pod-0 rejoined the SAME cluster ($CLUSTER_ID), no split brain, no fence"

log "6/9 — break glass: allow re-founding, and the fence must still work"
# The guard is the prevention; the fence is the containment behind it. Proving the fence
# still fires needs a split brain, and with the guard armed one can no longer be induced —
# so ask for founding back, exactly as an operator would after total volume loss.
kubectl -n "$NS" patch mqttdcluster e2e --type=merge -p '{"spec":{"bootstrapPolicy":"AllowRebootstrap"}}'
wait_for 180 "the guard to disarm" \
  bash -c "! kubectl -n $NS get statefulset e2e -o jsonpath='{.spec.template.spec.initContainers[0].env[*].name}' | grep -q CLUSTER_ESTABLISHED"
kubectl -n "$NS" rollout status statefulset/e2e --timeout="$READY_TIMEOUT"

kubectl -n "$NS" delete pod e2e-0 --wait=true
kubectl -n "$NS" delete pvc data-e2e-0 --wait=false
kubectl -n "$NS" delete pod e2e-0 --grace-period=0 --force >/dev/null 2>&1 || true
wait_for 240 "the re-founded pod to come back" \
  bash -c "[ \"\$(kubectl -n $NS get pod e2e-0 -o jsonpath='{.status.phase}' 2>/dev/null)\" = Running ]"

# Containment: PVC quarantined by LABEL, never deleted.
wait_for 300 "the re-founder's PVC to be quarantined" \
  bash -c "kubectl -n $NS get pvc data-e2e-0 -o go-template='{{index .metadata.labels \"mqttd.io/quarantined\"}}' | grep -q true"
kubectl -n "$NS" get pvc data-e2e-0 >/dev/null || fail "the PVC was DELETED — data must never be destroyed"

# And the verdict holds: the fence fires ONCE, so the incident stays visible instead of
# flickering behind a restart loop.
wait_for 240 "SplitBrain=True" \
  bash -c "[ \"\$(kubectl -n $NS get mqttdcluster e2e -o jsonpath=\"{.status.conditions[?(@.type=='SplitBrain')].status}\")\" = True ]"
held=0
for _ in $(seq 1 20); do
  [ "$(cr_condition SplitBrain)" = "True" ] && held=$((held + 1))
  sleep 5
done
[ "$held" -ge 18 ] \
  || fail "SplitBrain held for only $held/20 samples — the operator is re-fencing and hiding the incident"

# The re-founder must ALSO have taken itself out of rotation (ADR 0054 amendment): it is
# alone and hearing the surviving cluster, so it refuses to serve an empty store.
ready0="$(kubectl -n "$NS" get pod e2e-0 \
  -o 'jsonpath={.status.conditions[?(@.type=="Ready")].status}' 2>/dev/null || true)"
[ "$ready0" = "True" ] \
  && fail "the re-founded pod-0 is READY — it is serving clients from an empty store"
echo "break glass works: re-founding permitted, fenced once, self-quarantined (Ready=$ready0), data intact"

log "7/9 — the operator restarting mid-incident must not re-fence"
# Fence-once is enforced by CLUSTER state (the PVC's quarantine label), not by anything
# the operator remembers. If it were in-memory, a restart — a crash, an eviction, a
# rolling upgrade of the operator itself — would re-arm the fence and the pod would be
# killed again, which is the loop that made the incident invisible in the first place.
# The cluster is split-brained right now (step 5), so this is the interleaving that
# matters.
pod0_uid_before="$(kubectl -n "$NS" get pod e2e-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
quarantined_before="$(kubectl -n "$NS" get pvc -l mqttd.io/quarantined=true -o name | wc -l | tr -d ' ')"
kubectl -n "$NS" delete pod -l app.kubernetes.io/name=mqttd-operator --wait=true
kubectl -n "$NS" rollout status deploy/mqttd-operator --timeout=120s
# Give the restarted operator several reconciles over the standing incident.
sleep 75
quarantined_after="$(kubectl -n "$NS" get pvc -l mqttd.io/quarantined=true -o name | wc -l | tr -d ' ')"
[ "$quarantined_before" = "$quarantined_after" ] \
  || fail "a restarted operator quarantined more PVCs ($quarantined_before -> $quarantined_after)"
pod0_uid_after="$(kubectl -n "$NS" get pod e2e-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
[ "$pod0_uid_before" = "$pod0_uid_after" ] \
  || fail "a restarted operator DELETED the fenced pod again (uid changed) — fence-once is in memory, not in cluster state"
[ "$(cr_condition SplitBrain)" = "True" ] \
  || fail "the restarted operator stopped reporting the standing split brain"
echo "operator restart survived: no re-fence, pod untouched, verdict still raised"

log "8/9 — leader election: killing the holder must not stop reconciliation"
# The operator ships one replica, so the Lease path has never run with a contender —
# including the microsecond MicroTime fix that a 500 from the API server once exposed.
kubectl -n "$NS" scale deploy/mqttd-operator --replicas=2
kubectl -n "$NS" rollout status deploy/mqttd-operator --timeout=180s
wait_for 120 "a Lease holder to be recorded" \
  bash -c "[ -n \"\$(kubectl -n $NS get lease mqttd-operator -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)\" ]"
holder_before="$(kubectl -n "$NS" get lease mqttd-operator -o jsonpath='{.spec.holderIdentity}')"
kubectl -n "$NS" delete pod "$holder_before" --wait=true 2>/dev/null || true
# LEASE_SECS is 30, so the survivor may take that long to take over.
wait_for 180 "the Lease to move to the surviving replica" \
  bash -c "[ \"\$(kubectl -n $NS get lease mqttd-operator -o jsonpath='{.spec.holderIdentity}' 2>/dev/null)\" != \"$holder_before\" ]"
# Taking the Lease is not the same as USING it: require a reconcile after the handover.
reconciled=""
for _ in $(seq 1 24); do
  if kubectl -n "$NS" logs -l app.kubernetes.io/name=mqttd-operator --since=30s --tail=-1 2>/dev/null | grep -q "reconciled"; then
    reconciled=yes; break
  fi
  sleep 5
done
[ -n "$reconciled" ] || fail "the new Lease holder took over but never reconciled"
kubectl -n "$NS" scale deploy/mqttd-operator --replicas=1
kubectl -n "$NS" rollout status deploy/mqttd-operator --timeout=120s
echo "leader failover works: the Lease moved and the survivor reconciled"

log "9/9 — deleting the CR must collect the cluster but KEEP the data"
# Ownership is asserted at creation (assertion 1); collection never was. Deleting the CR
# must garbage-collect every object the operator owns — and must NOT take the volumes
# with it, because volumeClaimTemplate PVCs are not owned by the CR and the retention
# policy is Retain (issue #97). "Delete the CR" is a plausible way to rebuild a cluster,
# and it must not be a way to lose one.
pvcs_before="$(kubectl -n "$NS" get pvc --no-headers 2>/dev/null | wc -l | tr -d ' ')"
kubectl -n "$NS" delete mqttdcluster e2e --wait=true
wait_for 180 "the StatefulSet to be garbage-collected" \
  bash -c "! kubectl -n $NS get statefulset e2e >/dev/null 2>&1"
wait_for 120 "the Services to be garbage-collected" \
  bash -c "! kubectl -n $NS get service e2e >/dev/null 2>&1"
pvcs_after="$(kubectl -n "$NS" get pvc --no-headers 2>/dev/null | wc -l | tr -d ' ')"
[ "$pvcs_before" = "$pvcs_after" ] \
  || fail "deleting the CR destroyed volumes ($pvcs_before -> $pvcs_after) — data must survive"
echo "CR deleted: owned objects collected, all $pvcs_after volumes retained"

log "OPERATOR E2E PASSED: minimal RBAC, created a cluster, ignored a routine roll, scaled without data loss, kept a wiped founder from splitting the cluster, fenced one when re-founding was allowed, and collected the cluster on delete without losing a volume"
