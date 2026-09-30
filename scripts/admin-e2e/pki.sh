#!/usr/bin/env bash
# PKI for the admin-API end-to-end cluster (scripts/admin-e2e.sh): a cluster CA with one
# cert per node (CN = node id, SAN = its hostname; it serves the peer bus AND the admin
# listener), and an admin CA with three client identities: oncall (viewer), root
# (operator), stranger (in no role list). Throwaway, 3-day validity.
# Usage: pki.sh <output dir>
set -eu
D="${1:?usage: pki.sh <output dir>}"
rm -rf "$D"; mkdir -p "$D"; cd "$D"

ca() { # name
  openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout "$1-ca.key" \
    -out "$1-ca.pem" -subj "/CN=$1 CA" -days 3 \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    2>/dev/null
}
leaf() { # ca name subject san
  openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -keyout "$2.key" -out "$2.csr" \
    -subj "$3" 2>/dev/null
  printf "subjectAltName=%s\nextendedKeyUsage=serverAuth,clientAuth\n" "$4" > "$2.ext"
  openssl x509 -req -in "$2.csr" -CA "$1-ca.pem" -CAkey "$1-ca.key" -CAcreateserial \
    -out "$2.pem" -days 3 -extfile "$2.ext" 2>/dev/null
  rm -f "$2.csr" "$2.ext"
}

ca cluster
ca admin
for n in 1 2 3; do
  leaf cluster "mqttd-$n" "/CN=mqttd-$n" "DNS:mqttd-$n,DNS:localhost,IP:127.0.0.1"
done
leaf admin oncall "/CN=oncall/O=example" "DNS:oncall"
leaf admin root "/CN=root/O=example" "DNS:root"
leaf admin stranger "/CN=stranger/O=example" "DNS:stranger"
chmod 644 ./*.key ./*.pem
