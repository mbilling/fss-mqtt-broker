#!/bin/sh
# Throwaway PKI for the benchmark TLS/mTLS posture (ADR 0048 T2): CA + server cert
# (SAN: broker) + client cert. Same recipe as the demo QUIC PKI; EKUs matter — the
# client cert carries clientAuth and the server cert serverAuth (rustls/webpki
# enforce them; learned the hard way in the interop suite).
set -eu
D="$(dirname "$0")/certs"
mkdir -p "$D"

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$D/ca.key" -out "$D/ca.pem" \
  -subj '/CN=mqttd-bench-ca' -days 365 >/dev/null 2>&1

server_ext="$(mktemp)"
printf 'subjectAltName=DNS:broker,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' > "$server_ext"
openssl req -newkey rsa:2048 -nodes -keyout "$D/server.key" -out "$D/server.csr" \
  -subj '/CN=broker' >/dev/null 2>&1
openssl x509 -req -in "$D/server.csr" -CA "$D/ca.pem" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/server.pem" -days 365 -extfile "$server_ext" >/dev/null 2>&1

client_ext="$(mktemp)"
printf 'extendedKeyUsage=clientAuth\n' > "$client_ext"
openssl req -newkey rsa:2048 -nodes -keyout "$D/client.key" -out "$D/client.csr" \
  -subj '/CN=bench-client' >/dev/null 2>&1
openssl x509 -req -in "$D/client.csr" -CA "$D/ca.pem" -CAkey "$D/ca.key" -CAcreateserial \
  -out "$D/client.pem" -days 365 -extfile "$client_ext" >/dev/null 2>&1

rm -f "$D"/*.csr "$server_ext" "$client_ext"
# Throwaway PKI, world-readable so every containerized broker user can load it.
chmod 644 "$D"/*
echo "bench PKI written to $D"
