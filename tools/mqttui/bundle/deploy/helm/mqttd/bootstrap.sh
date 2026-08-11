#!/usr/bin/env bash
# Mint the Secrets/ConfigMap the mqttd chart needs, into a namespace, then print the
# `helm install` flags that wire them (#170). The chart references secret material BY
# PATH (ADR 0046 T5) from existing Secrets you manage; a bare `helm install` points at
# mounts that do not exist, which is the "missing middle" the compose path had a
# bootstrap.sh for and Kubernetes did not.
#
#   ./bootstrap.sh                        # namespace "mqttd", sample ACL, users alice/bob
#   NS=iot ./bootstrap.sh sensor-1 sensor-2
#
# Creates, in $NS:
#   Secret mqttd-tls        server TLS keypair          (kubernetes.io/tls: tls.crt/tls.key)
#   Secret mqttd-peer-tls   cluster-bus mTLS material    (ca.crt + tls.crt + tls.key)
#   Secret mqttd-gossip     gossip authentication key    (key: swim-key)
#   Secret mqttd-passwd     Argon2id password file       (key: passwd)   [if users given]
#   ConfigMap mqttd-acl     a starter deny-by-default ACL (key: acl.toml)
#
# It is idempotent per run only in that it overwrites (kubectl apply). Losing the CA key
# it mints for the cluster bus means re-issuing every node cert — the correct property for
# key material. This is a STARTER: review the ACL, rotate the demo certs for real ones
# (e.g. cert-manager), and keep the printed passwords somewhere safe, then delete them.
set -euo pipefail

NS="${NS:-mqttd}"
RELEASE="${RELEASE:-mqttd}"
IMAGE="${MQTTD_IMAGE:-ghcr.io/mbilling/fss-mqtt-broker:latest}"
USERS=("$@")

for tool in kubectl openssl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: need $tool on PATH" >&2; exit 2; }
done

# The broker binary hashes passwords (--hash-password); reuse a local build or the image,
# exactly as the compose bootstrap does, so a host with no Rust toolchain still works.
if [[ -n "${MQTTD_BIN:-}" ]]; then
  hash_password() { printf %s "$2" | "$MQTTD_BIN" --hash-password "$1"; }
elif command -v docker >/dev/null 2>&1; then
  hash_password() { printf %s "$2" | docker run --rm -i "$IMAGE" --hash-password "$1"; }
else
  hash_password() { echo "FATAL: need MQTTD_BIN or docker to hash passwords" >&2; exit 2; }
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
cd "$WORK"

echo "== namespace $NS =="
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

# --- gossip key (ADR 0003) --------------------------------------------------------------
openssl rand -hex 32 > swim-key
kubectl create secret generic mqttd-gossip -n "$NS" \
  --from-file=swim-key=swim-key --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  Secret mqttd-gossip (swim-key)"

# --- one CA, two leaves: a server cert (serverAuth) and a node cert (server+clientAuth for
#     the mutually-authenticated cluster bus) ---------------------------------------------
openssl req -x509 -newkey rsa:2048 -nodes -days 3650 -keyout ca.key -out ca.crt \
  -subj '/CN=mqttd-bootstrap-ca' >/dev/null 2>&1

# Server TLS: SANs cover the in-cluster service and localhost; serverAuth EKU.
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
  -subj "/CN=$RELEASE.$NS.svc" >/dev/null 2>&1
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
  -out server.crt -extfile <(printf 'subjectAltName=DNS:%s.%s.svc,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' "$RELEASE" "$NS") \
  >/dev/null 2>&1
kubectl create secret tls mqttd-tls -n "$NS" --cert=server.crt --key=server.key \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  Secret mqttd-tls (server keypair)"

# Cluster-bus node cert: BOTH serverAuth and clientAuth — every node dials and is dialed,
# and rustls rejects a client cert without the clientAuth EKU (the interop lesson).
openssl req -newkey rsa:2048 -nodes -keyout node.key -out node.csr \
  -subj "/CN=$RELEASE-node" >/dev/null 2>&1
openssl x509 -req -in node.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 365 \
  -out node.crt -extfile <(printf 'extendedKeyUsage=serverAuth,clientAuth\n') >/dev/null 2>&1
kubectl create secret generic mqttd-peer-tls -n "$NS" \
  --from-file=ca.crt=ca.crt --from-file=tls.crt=node.crt --from-file=tls.key=node.key \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  Secret mqttd-peer-tls (cluster-bus mTLS: ca.crt/tls.crt/tls.key)"

# --- a starter deny-by-default ACL ------------------------------------------------------
cat > acl.toml <<'ACL'
# Starter ACL — deny by default, edit before production (ADR 0004).
default = "deny"

# Example: a device may write its own telemetry and read its own commands.
# [[rules]]
# subject = "sensor-1"
# allow = ["write:telemetry/sensor-1/#", "read:commands/sensor-1/#"]
ACL
kubectl create configmap mqttd-acl -n "$NS" --from-file=acl.toml=acl.toml \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  ConfigMap mqttd-acl (deny-by-default starter)"

# --- passwords (optional) ---------------------------------------------------------------
PW_FLAGS=""
if [[ ${#USERS[@]} -gt 0 ]]; then
  : > passwd
  echo "  generated passwords (save these, then delete):"
  for u in "${USERS[@]}"; do
    pw="$(openssl rand -base64 18)"
    echo "$u:$(hash_password "$u" "$pw")" >> passwd
    echo "    $u  $pw"
  done
  kubectl create secret generic mqttd-passwd -n "$NS" --from-file=passwd=passwd \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  echo "  Secret mqttd-passwd (Argon2id lines)"
  PW_FLAGS='  # password file: mount mqttd-passwd and set MQTTD_PASSWORD_FILE via extraEnv'
fi

cat <<EOF

Done. Install the chart wired to these:

  helm install $RELEASE deploy/helm/mqttd -n $NS \\
    --set secrets.tls.secretName=mqttd-tls \\
    --set secrets.acl.configMapName=mqttd-acl \\
    --set secrets.peerTls.secretName=mqttd-peer-tls \\
    --set secrets.gossipKey.secretName=mqttd-gossip \\
    --set-string extraEnv[0].name=MQTTD_SWIM_KEY_FILE \\
    --set-string extraEnv[0].value=/etc/mqttd/gossip/swim-key
$PW_FLAGS

First install only: leave clusterEstablished=false (the default). Turn it on once the
cluster has formed:  helm upgrade $RELEASE deploy/helm/mqttd --reuse-values --set clusterEstablished=true

The CA and server certs here are throwaway. For production, issue real certs (cert-manager)
and replace the mqttd-tls / mqttd-peer-tls Secrets.
EOF
