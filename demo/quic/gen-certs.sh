#!/bin/sh
# Generate throwaway demo PKI for the MQTT-over-QUIC listener (ADR 0036): a CA, a server cert
# (valid for the cluster node names + localhost), and a client cert for the quic-demo service.
# DEMO ONLY — these are disposable, regenerated on every `up`. Never use in production.
# POSIX sh (alpine): no bash process substitution; extension configs go through temp files.
set -eu

D="${CERT_DIR:-/certs}"
if [ -f "$D/ca.pem" ] && [ -f "$D/server.pem" ] && [ -f "$D/client.pem" ]; then
  echo "quic certs already present in $D"
  exit 0
fi
mkdir -p "$D"

# CA
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.pem" \
  -subj '/CN=mqttd-demo-quic-ca' -days 3650 >/dev/null 2>&1

# Server leaf — SAN covers every cluster node + localhost/127.0.0.1.
server_ext="$(mktemp)"
# >>> generated: server-cert SAN — edit demo/scale-cluster.py, not here >>>
printf 'subjectAltName=DNS:mqttd-1,DNS:mqttd-2,DNS:mqttd-3,DNS:mqttd-4,DNS:mqttd-5,DNS:mqttd-6,DNS:mqttd-7,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$server_ext"
# <<< generated: server-cert SAN <<<
openssl req -newkey rsa:2048 -nodes -keyout "$D/server.key" -out "$D/server.csr" \
  -subj '/CN=mqttd-quic' >/dev/null 2>&1
openssl x509 -req -in "$D/server.csr" -CA "$D/ca.pem" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/server.pem" -days 3650 -extfile "$server_ext" >/dev/null 2>&1

# Client leaf — the quic-demo service's mTLS identity (CN becomes the session identity).
client_ext="$(mktemp)"
printf 'extendedKeyUsage=clientAuth\n' > "$client_ext"
openssl req -newkey rsa:2048 -nodes -keyout "$D/client.key" -out "$D/client.csr" \
  -subj '/CN=quic-demo' >/dev/null 2>&1
openssl x509 -req -in "$D/client.csr" -CA "$D/ca.pem" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/client.pem" -days 3650 -extfile "$client_ext" >/dev/null 2>&1

rm -f "$D"/*.csr "$D"/*.srl "$server_ext" "$client_ext"
echo "generated demo QUIC PKI in $D (ca.pem, server.pem/key, client.pem/key)"
