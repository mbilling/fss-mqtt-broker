#!/usr/bin/env bash
# The chart's CLUSTER BUS, checked at PR time — no cluster required (issue #262, 0047-T11).
#
# The defect this exists to prevent: deploy/helm/mqttd/bootstrap.sh minted ONE shared
# cluster-bus leaf (shared CN, no SANs, RSA key) and printed it as a delivered Secret with
# the `helm install` flag to wire it. Every one of those three properties is independently
# fatal on the bus, and nothing in CI had ever executed the recipe or wired the Secret — so
# the artifact and the project's own words about it drifted apart completely.
#
# The kind lane (scripts/k8s/kind-smoke.sh, nightly) now runs with the bus ON and is the
# end-to-end proof. This is the PR-tier half: it runs offline, in seconds, and covers the
# things that do not need a Kubernetes API server —
#
#   1. the SHIPPED mint is executed, and its own readback proves the four rules;
#   2. the mint AGREES WITH THE CHART: every leaf's CN is a pod name the chart actually
#      renders, and its SAN is the advertise host the chart's OWN init script computes —
#      both read out of `helm template` rather than restated here, so a change to the
#      fullname rule, the headless name or the advertise formula fails HERE;
#   3. the chart CONSUMES what it mounts: the rendered StatefulSet must carry the three
#      MQTTD_PEER_TLS_* paths (per-pod, via $(POD_NAME)) and MQTTD_SWIM_KEY_FILE. This is
#      the regression test for the trap class — a mounted-but-unread Secret meant the bus
#      was PLAINTEXT while the deployment looked mutually authenticated;
#   4. a REAL broker boots on a shipped leaf: mTLS peer listener, signed per-node gossip
#      (which arms itself once peer TLS + a gossip key are both present), and readiness;
#   5. the ORIGINAL #262 certificate is refused — by the mint's verifier AND, if it somehow
#      reached a node, by the broker at startup. A negative control, so the check cannot
#      pass by asserting nothing;
#   6. two real nodes LINK and reach a 2-member readiness floor over per-node certificates,
#      which is what exercises rules 1 and 2 (CN binding, SAN name verification) rather
#      than merely inspecting them. Needs an /etc/hosts entry to resolve the pod FQDNs to
#      loopback, so it SKIPS LOUDLY where that is not writable (and runs in CI, which has
#      passwordless sudo).
#
# Requires: helm, openssl, python3. Set MQTTD_BIN to skip building the broker.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="$REPO_ROOT/deploy/helm/mqttd"
BOOTSTRAP="$CHART/bootstrap.sh"
# The kind smoke's identities, so both lanes prove the same names. RELEASE must NOT contain
# "mqttd" or helm's fullname collapses (templates/_helpers.tpl) — the trap that would make
# every CN wrong while every certificate still verified.
RELEASE="${RELEASE:-smoke}"
NS="${NS:-mqttd-smoke}"
REPLICAS="${REPLICAS:-3}"

for tool in helm openssl python3; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: need $tool on PATH" >&2; exit 2; }
done

WORK="$(mktemp -d)"
PIDS=()
HOSTS_ADDED=0
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  if [ "$HOSTS_ADDED" = "1" ]; then
    sudo -n sed -i.mqttdbak '/# mqttd-peer-tls-check/d' /etc/hosts 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }

# The broker's tracing output carries ANSI escapes even when redirected to a file, and they
# sit BETWEEN the field name and its value ("mtls" ESC "=" ESC "true"). A naive
# `grep mtls=true` therefore never matches and every log assertion below would pass
# vacuously — the failure mode this whole script exists to prevent. Strip first, then grep.
logq() { # logq <file> <extended-regex>
  sed $'s/\033\\[[0-9;]*m//g' "$1" | grep -Eq "$2"
}
logshow() { sed $'s/\033\\[[0-9;]*m//g' "$1"; }

MQTTD_BIN="${MQTTD_BIN:-}"
if [ -z "$MQTTD_BIN" ]; then
  echo "building mqttd (set MQTTD_BIN to skip)…"
  cargo build --quiet -p mqttd --manifest-path "$REPO_ROOT/Cargo.toml"
  MQTTD_BIN="$REPO_ROOT/target/debug/mqttd"
fi
[ -x "$MQTTD_BIN" ] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN" >&2; exit 2; }

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }

# ─────────────────────────────────────────────────────────────────────────────────
# 1. The SHIPPED mint, executed. Its own readback is the assertion: it verifies every
#    property it prints and refuses to install anything that fails, so a zero exit
#    means all four rules held for all $REPLICAS leaves.
# ─────────────────────────────────────────────────────────────────────────────────
echo "── 1. the shipped mint runs, and self-verifies ──"
PKI="$WORK/pki"
RELEASE="$RELEASE" NS="$NS" REPLICAS="$REPLICAS" PKI_DIR="$PKI" \
  bash "$BOOTSTRAP" --mint-only >"$WORK/mint.log" 2>&1 \
  || { cat "$WORK/mint.log"; fail "$BOOTSTRAP could not mint a conforming PKI"; }
pass "$REPLICAS per-node leaves + a CA minted and read back (see $WORK/mint.log)"

# The CA private key must never be among the files that go into the cluster. The mint keeps
# it in $PKI/ca/, and only ca/cluster-ca.pem is put in the Secret; assert the custody rule
# rather than trusting the comment that states it.
[ -s "$PKI/ca/cluster-ca.key" ] || fail "no CA private key was produced"
for f in "$PKI"/nodes/*; do
  case "$(basename "$f")" in
    *cluster-ca*) fail "the CA key/cert leaked into the per-node directory: $f" ;;
  esac
done
pass "the CA private key stays in $PKI/ca and is not part of any node's material"

# Re-running must RE-VERIFY and keep, never silently re-mint: a second CA is a second trust
# root, and a rotated gossip key partitions a running cluster.
KEY_BEFORE="$(cat "$PKI/gossip/swim-key")"
CA_BEFORE="$(openssl x509 -in "$PKI/ca/cluster-ca.pem" -noout -serial)"
RELEASE="$RELEASE" NS="$NS" REPLICAS="$REPLICAS" PKI_DIR="$PKI" \
  bash "$BOOTSTRAP" --mint-only >"$WORK/mint2.log" 2>&1 \
  || { cat "$WORK/mint2.log"; fail "a second mint run failed against its own output"; }
[ "$KEY_BEFORE" = "$(cat "$PKI/gossip/swim-key")" ] \
  || fail "the gossip key was rotated by a re-run — that partitions a running cluster"
[ "$CA_BEFORE" = "$(openssl x509 -in "$PKI/ca/cluster-ca.pem" -noout -serial)" ] \
  || fail "the CA was re-minted by a re-run — a second trust root forms no mesh"
grep -q 'RE-VERIFIED' "$WORK/mint2.log" \
  || fail "the re-run did not re-verify the existing CA (it must validate, not stat)"
pass "a re-run keeps and RE-VERIFIES the CA and gossip key instead of re-minting"

# ─────────────────────────────────────────────────────────────────────────────────
# 2. The mint agrees with the CHART. Both sides are read out of the artifacts: the pod
#    names and the advertise hosts come from `helm template` and from RUNNING the chart's
#    own init-container script, never from a formula retyped here. A shared-CN leaf, a
#    collapsed fullname or a changed advertise shape all fail at this step.
# ─────────────────────────────────────────────────────────────────────────────────
echo "── 2. every CN is a real pod name, every SAN is that pod's advertise host ──"
helm template "$RELEASE" "$CHART" --namespace "$NS" \
  --set replicaCount="$REPLICAS" \
  --set secrets.peerTls.secretName=mqttd-peer-tls \
  --set secrets.gossipKey.secretName=mqttd-gossip > "$WORK/render.yaml"

PY_BIN=python3
if ! python3 -c "import yaml" >/dev/null 2>&1; then
  echo "  (provisioning a throwaway venv for PyYAML)"
  python3 -m venv "$WORK/venv" >/dev/null
  "$WORK/venv/bin/pip" install --quiet pyyaml
  PY_BIN="$WORK/venv/bin/python"
fi

# Pull the init script, its env, the config template and the broker env out of the render.
"$PY_BIN" - "$WORK/render.yaml" "$WORK" <<'PY'
import sys, yaml, json, pathlib
render, out = sys.argv[1], pathlib.Path(sys.argv[2])
docs = [d for d in yaml.safe_load_all(open(render)) if d]
sts = next(d for d in docs if d["kind"] == "StatefulSet")
cm = next(d for d in docs if d["kind"] == "ConfigMap")
pod = sts["spec"]["template"]["spec"]
init = next(c for c in pod["initContainers"] if c["name"] == "render-config")
broker = next(c for c in pod["containers"] if c["name"] == "mqttd")
(out / "init.sh").write_text(init["args"][0])
(out / "mqttd.toml.tmpl").write_text(next(iter(cm["data"].values())))
env = {e["name"]: e.get("value", "") for e in init.get("env", [])}
json.dump({
    "sts_name": sts["metadata"]["name"],
    "replicas": sts["spec"]["replicas"],
    "headless_domain": env.get("HEADLESS_DOMAIN", ""),
    "init_env": env,
    "init_mounts": [m["mountPath"] for m in init.get("volumeMounts", [])],
    "broker_env": {e["name"]: e.get("value", "") for e in broker.get("env", [])},
    "broker_env_order": [e["name"] for e in broker.get("env", [])],
    "broker_mounts": [m["mountPath"] for m in broker.get("volumeMounts", [])],
}, open(out / "facts.json", "w"))
PY

STS_NAME="$("$PY_BIN" -c 'import json,sys;print(json.load(open(sys.argv[1]))["sts_name"])' "$WORK/facts.json")"
HEADLESS_DOMAIN="$("$PY_BIN" -c 'import json,sys;print(json.load(open(sys.argv[1]))["headless_domain"])' "$WORK/facts.json")"
[ -n "$HEADLESS_DOMAIN" ] || fail "the render carries no HEADLESS_DOMAIN — the advertise name cannot be derived"

# Run the chart's OWN init script per ordinal and read the advertise address it renders.
ADV=()
for ((i = 0; i < REPLICAS; i++)); do
  d="$WORK/ord$i"; mkdir -p "$d"
  cp "$WORK/mqttd.toml.tmpl" "$d/mqttd.toml.tmpl"
  ( cd "$d" && POD_NAME="$STS_NAME-$i" REPLICAS="$REPLICAS" HEADLESS_DOMAIN="$HEADLESS_DOMAIN" \
      TMPL_DIR="$d" OUT_DIR="$d" sh "$WORK/init.sh" >/dev/null 2>&1 ) \
    || fail "the chart's init script failed for ordinal $i"
  adv="$(sed -n 's/^peer_advertise = "\(.*\)"$/\1/p' "$d/mqttd.toml")"
  [ -n "$adv" ] || fail "ordinal $i rendered no peer_advertise"
  ADV+=("$adv")
done

for ((i = 0; i < REPLICAS; i++)); do
  node="$STS_NAME-$i"
  crt="$PKI/nodes/$node.pem"
  [ -s "$crt" ] || fail "the mint produced no leaf for pod $node (the chart renders that pod)"
  # Rule 1, against the name the CHART gives the pod.
  cn="$(openssl x509 -in "$crt" -noout -subject | sed 's/.*CN *= *//; s/[/,].*//')"
  [ "$cn" = "$node" ] || fail "leaf CN '$cn' is not the pod name '$node' (rule 1)"
  # Rule 2, against the advertise host the CHART renders (port stripped, as
  # crates/mqtt-net/src/tls.rs::server_name does before verification).
  host="${ADV[$i]%:*}"
  openssl x509 -in "$crt" -noout -text | grep -q "DNS:$host" \
    || fail "leaf for $node has no DNS:$host SAN — that is the name a dialing peer verifies (rule 2)"
done
pass "CN == pod name and SAN == rendered advertise host, for all $REPLICAS ordinals"
pass "  (pods $STS_NAME-0..$((REPLICAS - 1)), advertise ${ADV[0]})"

# ─────────────────────────────────────────────────────────────────────────────────
# 3. The chart CONSUMES what it mounts. This is the assertion the original defect
#    would have failed even after a perfect mint: the Secret was mounted and nothing
#    referenced it, so the bus stayed plaintext.
# ─────────────────────────────────────────────────────────────────────────────────
echo "── 3. a mounted peer-bus Secret is always CONSUMED ──"
"$PY_BIN" - "$WORK/facts.json" <<'PY'
import json, sys
f = json.load(open(sys.argv[1]))
env, order = f["broker_env"], f["broker_env_order"]
problems = []
want = {
    "MQTTD_PEER_TLS_CA": "/etc/mqttd/cluster/ca.crt",
    "MQTTD_PEER_TLS_CERT": "/etc/mqttd/cluster/$(POD_NAME).crt",
    "MQTTD_PEER_TLS_KEY": "/etc/mqttd/cluster/$(POD_NAME).key",
    "MQTTD_SWIM_KEY_FILE": "/etc/mqttd/gossip/swim-key",
}
for k, v in want.items():
    if k not in env:
        problems.append(f"the broker container does not set {k} — a mounted cluster-bus "
                        "Secret that nothing references leaves the bus PLAINTEXT (issue #262)")
    elif env[k] != v:
        problems.append(f"{k} is {env[k]!r}, expected {v!r}")
# Per-pod, not shared: the cert/key paths MUST vary by pod, and POD_NAME must be defined
# BEFORE them or Kubernetes leaves the reference unexpanded and the path is literal.
for k in ("MQTTD_PEER_TLS_CERT", "MQTTD_PEER_TLS_KEY"):
    if k in env and "$(POD_NAME)" not in env[k]:
        problems.append(f"{k} does not vary by pod ({env[k]!r}): one shared leaf drops every "
                        "peer link, because the bus binds node identity to the certificate CN")
if "POD_NAME" not in order:
    problems.append("POD_NAME is not in the broker env, so $(POD_NAME) cannot expand")
else:
    for k in ("MQTTD_PEER_TLS_CERT", "MQTTD_PEER_TLS_KEY"):
        if k in order and order.index("POD_NAME") > order.index(k):
            problems.append(f"POD_NAME is declared AFTER {k}; Kubernetes only expands "
                            "references to variables defined earlier in the list")
if "/etc/mqttd/cluster" not in f["broker_mounts"]:
    problems.append("the peer-bus Secret is not mounted into the broker container")
if "/etc/mqttd/cluster" not in f["init_mounts"]:
    problems.append("the peer-bus Secret is not mounted into the render init container, so "
                    "the per-pod-leaf preflight cannot see it")
if f["init_env"].get("PEER_TLS_DIR") != "/etc/mqttd/cluster":
    problems.append("PEER_TLS_DIR is not set on the init container, so the missing-leaf "
                    "preflight is inert and a scale-up past the PKI fails obscurely")
if problems:
    for p in problems:
        print(f"  FAIL — {p}")
    raise SystemExit(1)
PY
pass "the render sets MQTTD_PEER_TLS_{CA,CERT,KEY} per-pod and MQTTD_SWIM_KEY_FILE"
pass "the peer-bus Secret is mounted into both containers, preflight armed"

# The default render must NOT carry any of it — the bus is opt-in, and a chart that always
# demanded certificates would break every plaintext-mesh install.
helm template "$RELEASE" "$CHART" --namespace "$NS" | grep -q 'MQTTD_PEER_TLS' \
  && fail "the DEFAULT render references MQTTD_PEER_TLS_* — the cluster bus must stay opt-in"
pass "the default render carries no peer-bus env (opt-in preserved)"

if command -v kubeconform >/dev/null 2>&1; then
  kubeconform -strict -summary -ignore-missing-schemas -schema-location default \
    < "$WORK/render.yaml" >/dev/null || fail "the bus-on render is not schema-valid"
  pass "the bus-on render passes kubeconform"
else
  echo "  SKIP — kubeconform not installed (CI installs it; schema check not run here)"
fi

# The preflight must actually fire for an ordinal with no leaf — the scale-up case.
d="$WORK/preflight"; mkdir -p "$d/certs"
cp "$WORK/mqttd.toml.tmpl" "$d/mqttd.toml.tmpl"
if ( cd "$d" && POD_NAME="$STS_NAME-9" REPLICAS=10 HEADLESS_DOMAIN="$HEADLESS_DOMAIN" \
       PEER_TLS_DIR="$d/certs" TMPL_DIR="$d" OUT_DIR="$d" sh "$WORK/init.sh" >"$d/out" 2>&1 ); then
  fail "an ordinal with NO peer-bus leaf rendered happily — it must fail with the mint command"
fi
grep -q 'no cluster-bus certificate' "$d/out" \
  || { cat "$d/out"; fail "the missing-leaf failure does not name the cause"; }
grep -q 'bootstrap.sh' "$d/out" \
  || fail "the missing-leaf failure does not name the command that fixes it"
pass "an ordinal with no leaf fails the init container, naming the mint command (scale-up)"

# ─────────────────────────────────────────────────────────────────────────────────
# 4. A REAL broker on a shipped leaf. Signed gossip arms itself once peer TLS and a
#    gossip key are both present, so this covers rule 4 (the ECDSA/PKCS#8 signing key)
#    for free: an RSA leaf cannot get past here, as section 5 proves.
# ─────────────────────────────────────────────────────────────────────────────────
echo "── 4. a real broker boots on a shipped per-node leaf ──"
boot_node() { # boot_node <ordinal> <peer-port> <swim-port> <health-port> <advertise> <seeds> <ready-min>
  local ord="$1" pport="$2" sport="$3" hport="$4" adv="$5" seeds="$6" readymin="$7"
  local node="$STS_NAME-$ord" dir="$WORK/run$ord"
  mkdir -p "$dir/data"
  MQTTD_NODE_ID="$node" MQTTD_DATA_DIR="$dir/data" \
  MQTTD_PLAINTEXT_BIND="127.0.0.1:$(free_port)" MQTTD_HEALTH_BIND="127.0.0.1:$hport" \
  MQTTD_ALLOW_ANONYMOUS=true MQTTD_DURABLE_SESSIONS=false \
  MQTTD_PEER_BIND="127.0.0.1:$pport" MQTTD_PEER_ADVERTISE="$adv" \
  MQTTD_SWIM_BIND="127.0.0.1:$sport" MQTTD_SWIM_SEEDS="$seeds" \
  MQTTD_SWIM_KEY_FILE="$PKI/gossip/swim-key" \
  MQTTD_PEER_TLS_CA="$PKI/ca/cluster-ca.pem" \
  MQTTD_PEER_TLS_CERT="$PKI/nodes/$node.pem" \
  MQTTD_PEER_TLS_KEY="$PKI/nodes/$node.key" \
  MQTTD_READY_MIN_MEMBERS="$readymin" \
    "$MQTTD_BIN" >"$WORK/node$ord.log" 2>&1 &
  LAST_PID=$!
  PIDS+=("$LAST_PID")
}

await_ready() { # await_ready <health-port> <tries>
  local port="$1" tries="${2:-40}" i
  for ((i = 0; i < tries; i++)); do
    if MQTTD_HEALTH_BIND="127.0.0.1:$port" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

H0="$(free_port)"
boot_node 0 "$(free_port)" "$(free_port)" "$H0" "${ADV[0]}" "" 1
await_ready "$H0" || { logshow "$WORK/node0.log" | tail -30; fail "the broker never became ready on a shipped leaf"; }
logq "$WORK/node0.log" 'accepting cluster peer links.*mtls=true' \
  || { logshow "$WORK/node0.log" | tail -20; fail "the peer listener is not mutually authenticated (mtls=true absent)"; }
logq "$WORK/node0.log" 'SWIM gossip is SIGNED per-node' \
  || fail "signed per-node gossip did not arm (ADR 0022) — it should, given peer TLS + a gossip key"
logq "$WORK/node0.log" 'INSECURE: starting PLAINTEXT peer listener' \
  && fail "the peer bus came up PLAINTEXT while a peer-TLS Secret was configured — issue #262's real payload"
pass "peer listener is mTLS, per-node gossip is SIGNED, /readyz passes"
kill "$LAST_PID" 2>/dev/null || true; wait "$LAST_PID" 2>/dev/null || true; PIDS=()

# ─────────────────────────────────────────────────────────────────────────────────
# 5. NEGATIVE CONTROL — the certificate issue #262 actually shipped. Without this the
#    section above could pass while the mint quietly produced something unusable.
# ─────────────────────────────────────────────────────────────────────────────────
echo "── 5. the original #262 certificate is refused ──"
BADPKI="$WORK/badpki"
cp -R "$PKI" "$BADPKI"; chmod -R u+w "$BADPKI"
# One shared leaf for the whole cluster: RSA key, CN=<release>-node, no SANs, both EKUs —
# the exact three-legged shape, minted the exact way the old script did.
openssl req -newkey rsa:2048 -nodes -keyout "$BADPKI/shared.key" -out "$BADPKI/shared.csr" \
  -subj "/CN=$RELEASE-node" >/dev/null 2>&1
openssl x509 -req -in "$BADPKI/shared.csr" -CA "$BADPKI/ca/cluster-ca.pem" \
  -CAkey "$BADPKI/ca/cluster-ca.key" -CAcreateserial -days 365 -out "$BADPKI/shared.pem" \
  -extfile <(printf 'extendedKeyUsage=serverAuth,clientAuth\n') >/dev/null 2>&1
for ((i = 0; i < REPLICAS; i++)); do
  cp "$BADPKI/shared.pem" "$BADPKI/nodes/$STS_NAME-$i.pem"
  cp "$BADPKI/shared.key" "$BADPKI/nodes/$STS_NAME-$i.key"
done
# (a) the mint's verifier must refuse to bless it on a re-run, and say why.
if RELEASE="$RELEASE" NS="$NS" REPLICAS="$REPLICAS" PKI_DIR="$BADPKI" \
     bash "$BOOTSTRAP" --mint-only >"$WORK/bad-mint.log" 2>&1; then
  fail "the mint BLESSED the #262 certificate on a re-run — a rejected artifact must never be reused"
fi
for expect in "rule 1" "rule 2" "rule 4"; do
  grep -q "$expect" "$WORK/bad-mint.log" \
    || { cat "$WORK/bad-mint.log"; fail "the refusal does not cite $expect"; }
done
pass "the mint refuses the shared-CN/no-SAN/RSA leaf, citing rules 1, 2 and 4"
# (b) and if it reached a node anyway, the broker must refuse to start on it.
H1="$(free_port)"
node="$STS_NAME-0"
mkdir -p "$WORK/runbad/data"
set +e
MQTTD_NODE_ID="$node" MQTTD_DATA_DIR="$WORK/runbad/data" \
MQTTD_PLAINTEXT_BIND="127.0.0.1:$(free_port)" MQTTD_HEALTH_BIND="127.0.0.1:$H1" \
MQTTD_ALLOW_ANONYMOUS=true MQTTD_DURABLE_SESSIONS=false \
MQTTD_PEER_BIND="127.0.0.1:$(free_port)" MQTTD_PEER_ADVERTISE="${ADV[0]}" \
MQTTD_SWIM_BIND="127.0.0.1:$(free_port)" MQTTD_SWIM_KEY_FILE="$PKI/gossip/swim-key" \
MQTTD_PEER_TLS_CA="$BADPKI/ca/cluster-ca.pem" \
MQTTD_PEER_TLS_CERT="$BADPKI/nodes/$node.pem" \
MQTTD_PEER_TLS_KEY="$BADPKI/nodes/$node.key" \
  "$MQTTD_BIN" >"$WORK/badnode.log" 2>&1
rc=$?
set -e
[ "$rc" -ne 0 ] || fail "the broker STARTED on an RSA cluster-bus leaf — the gossip signer must refuse it"
logq "$WORK/badnode.log" 'gossip signing key' \
  || { logshow "$WORK/badnode.log" | tail -5; fail "the startup failure does not name the signing key"; }
pass "a broker refuses to start on the RSA leaf (\"gossip signing key\"), exit $rc"

# ─────────────────────────────────────────────────────────────────────────────────
# 6. Two nodes must LINK over per-node certificates. This is what exercises rule 1
#    (the CN↔node-id binding) and rule 2 (SAN name verification) for real: the leaves
#    carry DIFFERENT CNs and different SANs, and a dialing peer verifies the advertise
#    host it was told. Needs the pod FQDNs to resolve to loopback.
# ─────────────────────────────────────────────────────────────────────────────────
echo "── 6. two nodes link and reach a 2-member readiness floor ──"
if [ "$REPLICAS" -lt 2 ]; then
  echo "  SKIP — REPLICAS=$REPLICAS, nothing to link"
elif ! sudo -n true 2>/dev/null; then
  echo "  SKIP LOUDLY — no passwordless sudo, so the pod FQDNs cannot be pointed at"
  echo "                loopback in /etc/hosts. Rules 1 and 2 are checked statically in"
  echo "                section 2 and end-to-end by scripts/k8s/kind-smoke.sh (nightly),"
  echo "                which has real per-pod DNS. CI runs this section."
else
  {
    printf '127.0.0.1 %s # mqttd-peer-tls-check\n' "${ADV[0]%:*}"
    printf '127.0.0.1 %s # mqttd-peer-tls-check\n' "${ADV[1]%:*}"
  } | sudo -n tee -a /etc/hosts >/dev/null
  HOSTS_ADDED=1
  P0="$(free_port)"; S0="$(free_port)"; H0="$(free_port)"
  P1="$(free_port)"; S1="$(free_port)"; H1="$(free_port)"
  # Advertise the pod FQDN (SAN-covered) on the loopback port each node really binds.
  boot_node 0 "$P0" "$S0" "$H0" "${ADV[0]%:*}:$P0" "" 2
  boot_node 1 "$P1" "$S1" "$H1" "${ADV[1]%:*}:$P1" "127.0.0.1:$S0" 2
  # A 2-member floor cannot be met by a node talking to itself, so readiness here IS
  # proof that the mutually-authenticated link formed and gossip verified.
  await_ready "$H0" 60 || { logshow "$WORK/node0.log" | tail -40; logshow "$WORK/node1.log" | tail -40
    fail "node 0 never reached the 2-member readiness floor — the peer link did not form"; }
  await_ready "$H1" 60 || { logshow "$WORK/node1.log" | tail -40
    fail "node 1 never reached the 2-member readiness floor"; }
  for l in "$WORK/node0.log" "$WORK/node1.log"; do
    logq "$l" 'does not match its certificate Common Name' \
      && { logshow "$l" | grep 'Common Name'; fail "a peer link was dropped on the CN binding ($l)"; }
    logq "$l" 'SWIM gossip is SIGNED per-node' \
      || fail "signed per-node gossip did not arm on $l"
  done
  pass "both nodes reached a 2-member floor over per-node mTLS, no CN mismatch, gossip signed"
fi

echo
echo "PEER TLS CHECK OK — the shipped mint conforms, matches the chart's pod identities,"
echo "is consumed by the render, boots a real broker, and refuses the #262 certificate."
