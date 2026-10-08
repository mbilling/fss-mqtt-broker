#!/bin/sh
# Mint the live rules demo's throwaway PKI. compose.yaml's one-shot `pki` service runs this
# with no network: a CA, the admin listener's server certificate, and the CN=rules-ui client
# certificate that the UI and the `admin` service present to the admin API (mTLS only, ADR
# 0081). The recipe is demo/quic/gen-certs.sh's: v3 leaves through -extfile.
# DEMO ONLY. Never use these anywhere else.
#
# Three volumes, so each private key is only where it is used:
#   $CA_DIR      ca.key, ca.crt                      the `pki` service alone mounts it
#   $SERVER_DIR  ca.crt, server.crt, server.key      mqttd
#   $CLIENT_DIR  ca.crt, rules-ui.crt, rules-ui.key  ui, admin
#
# Idempotent: the CA is kept, and a certificate is issued only when it is missing or about
# to expire. A new CA reissues both leaves.
set -eu

CA="${CA_DIR:-/pki/ca}"
SERVER="${SERVER_DIR:-/pki/server}"
CLIENT="${CLIENT_DIR:-/pki/client}"
umask 077
mkdir -p "$CA" "$SERVER" "$CLIENT"

if [ ! -f "$CA/ca.key" ] || [ ! -f "$CA/ca.crt" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CA/ca.key" -out "$CA/ca.crt" \
    -subj '/CN=mqttd-rules-live-demo-ca' -days 3650 \
    -addext 'basicConstraints=critical,CA:TRUE' \
    -addext 'keyUsage=critical,keyCertSign,cRLSign' >/dev/null 2>&1
  rm -f "$SERVER"/* "$CLIENT"/*
  echo "generated the demo CA"
fi

# issue <dir> <name> <CN> <extensions, one per line>: a new certificate, unless the one
# there is good for another day.
# 825 days: the longest validity macOS accepts for a TLS server certificate, so curl on a
# Mac trusts the server certificate too.
issue() {
  if [ -f "$1/$2.key" ] && [ -f "$1/$2.crt" ] \
    && openssl x509 -checkend 86400 -noout -in "$1/$2.crt" >/dev/null 2>&1; then
    echo "kept $2.crt (CN=$3)"
  else
    ext="$(mktemp)"
    printf '%s\nbasicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n' \
      "$4" > "$ext"
    openssl req -newkey rsa:2048 -nodes -keyout "$1/$2.key" -out "$1/$2.csr" \
      -subj "/CN=$3" >/dev/null 2>&1
    openssl x509 -req -in "$1/$2.csr" -CA "$CA/ca.crt" -CAkey "$CA/ca.key" -CAcreateserial \
      -out "$1/$2.crt" -days 825 -extfile "$ext" >/dev/null 2>&1
    rm -f "$1/$2.csr" "$ext"
    echo "issued $2.crt (CN=$3)"
  fi
}

issue "$SERVER" server mqttd \
  "$(printf 'subjectAltName=DNS:mqttd,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth')"
issue "$CLIENT" rules-ui rules-ui 'extendedKeyUsage=clientAuth'

cp "$CA/ca.crt" "$SERVER/ca.crt"
cp "$CA/ca.crt" "$CLIENT/ca.crt"
chmod 644 "$SERVER"/*.crt "$CLIENT"/*.crt
echo "demo PKI ready (CN=rules-ui is the admin API operator)"
