#!/bin/sh
# Mint the reference cluster's TLS material and stage its secrets, once, at bring-up.
#
# THROWAWAY STARTER PKI. Self-signed, no revocation, a 10-year CA sitting in a Docker
# volume. It exists so that `docker compose up -d` is TLS on the first run instead of
# plaintext with a TODO — not so that you ship it. Replace it before production
# (deploy/compose/README.md, "Before production"). To re-mint from scratch:
#
#   docker compose down && docker volume rm mqttd_mqttd-ca \
#     mqttd_mqttd-tls-1 mqttd_mqttd-tls-2 mqttd_mqttd-tls-3 && docker compose up -d
#
# This runs as a one-shot `init` service inside `docker compose up`, as ROOT, before any
# broker starts. Root is the point: the brokers run as uid 65532 under a read-only root
# filesystem, so somebody has to hand them files that uid can read. It does four things:
#
#   1. refuses to continue if ./bootstrap.sh has not been run (so the failure names the fix);
#   2. mints a CA and ONE LEAF PER NODE, idempotently, VERIFYING everything it prints —
#      it cannot assume a particular openssl, because deploy-smoke.sh runs it on the host
#      and MQTTD_CERTGEN_IMAGE can point the compose service anywhere;
#   3. stages the password file + gossip key into a volume owned by 65532;
#   4. drops the CA certificate where a client on the host can find it.
#
# ── KEY CUSTODY: ONE DIRECTORY PER NODE, AND THE CA KEY IN NEITHER ────────────────────
# $CA_DIR holds the CA (cert AND private key) and is mounted into THIS container only —
# no broker mounts it, so no broker can read the cluster's trust root even by accident.
# Each node's leaf goes into its OWN directory, $CERT_DIR/<node-id>, which compose backs
# with a per-node volume mounted read-only into that broker alone. All three brokers run as
# the same uid (65532), so file modes cannot separate them: only separate mounts can, and
# that is why the layout is what it is. deploy/README.md states the rule ("its private key
# on none of the broker hosts"); this is the compose packaging keeping it, and
# scripts/deploy-smoke.sh asserts both halves on the rendered compose config and on this
# script's output.
#
# Each per-node directory must ALREADY EXIST when this runs — compose creates it as a
# volume mountpoint. A node listed in MQTTD_NODES with no volume mounted for it is a
# configuration mistake that would otherwise mint a leaf into this container's ephemeral
# filesystem and vanish, so it is a hard failure that names the missing volume.
#
# WHY PER-NODE LEAVES, not one shared cert:
#   - the cluster bus enforces a node-id ↔ certificate binding: a peer may only claim the
#     node id its Subject CN attests to (crates/mqttd/src/peer.rs, "peer Hello node id does
#     not match its certificate Common Name"). So CN MUST equal MQTTD_NODE_ID.
#   - a dialing node verifies the peer's certificate against the host part of that peer's
#     MQTTD_PEER_ADVERTISE (crates/mqtt-net/src/tls.rs::server_name). So the SAN MUST cover
#     it — here DNS:<node> for in-network dialing, plus DNS:localhost and IP:127.0.0.1 so a
#     client on the Docker host can verify the published 8883/8884/8885 ports.
#
# WHY BOTH EKUs: every node both dials and is dialed on the cluster bus, and rustls rejects
# a client certificate without the clientAuth EKU. serverAuth alone forms no mesh.
#
# WHY ECDSA P-256 AND NOT RSA: the cluster-bus key is ALSO the per-node gossip signing key
# (ADR 0022), and that signer accepts only PKCS#8 ECDSA P-256/P-384 or Ed25519. An RSA leaf
# gets you a working TLS handshake and then a hard startup failure — "unsupported or
# unparseable gossip signing key" — which is a confusing way to learn this. P-256 because
# it is the intersection of what the signer accepts and what every TLS stack here supports.
#
# POSIX sh (the certgen image is alpine — no bash), so extension configs go through temp
# files rather than process substitution. Env-driven so scripts/deploy-smoke.sh can call
# THIS file instead of retyping the recipe; it skips the chown/chmod step when not root.
#
# This CANNOT assume one openssl, and an earlier version of this header wrongly said it
# could ("the pinned alpine/openssl:3.5.7, so it does not need gen-certs.sh's conformance
# checks"). Two callers break that assumption: scripts/deploy-smoke.sh runs this file
# directly on the host with whatever `openssl` is on PATH, and compose.yaml's certgen image
# is overridable (`${MQTTD_CERTGEN_IMAGE:-alpine/openssl:3.5.7}`). Under LibreSSL — stock
# macOS — the naive recipe silently yields explicit ECC parameters and a CA with no
# basicConstraints, i.e. material the broker refuses, and the old code printed a full success
# banner for it. So this script mints into a staging directory, VERIFIES every property it is
# about to claim, and installs only what verified. scripts/deploy-smoke.sh additionally boots
# a three-node cluster from the result.
#
# openssl's STDERR IS LEFT ALONE on every call below — only stdout goes to /dev/null. It
# costs a few lines of banner noise ("-----", "Certificate request self-signature ok") and
# buys the only explanation an operator gets when the PKI fails: `set -eu` aborts, the
# brokers then refuse to start on the depends_on gate, and a bare non-zero exit from this
# container would be the whole cluster's failure message.
set -eu

CERT_DIR="${CERT_DIR:-/certs}"          # one SUBDIRECTORY PER NODE, each its own volume
CA_DIR="${CA_DIR:-/ca}"                 # the CA cert + PRIVATE KEY; mounted into init only
SECRETS_SRC="${SECRETS_SRC:-/in}"       # ./secrets from the host, where bootstrap.sh writes
SECRETS_OUT="${SECRETS_OUT:-/secrets}"  # the staged copy (mqttd-secrets volume, ro in the brokers)
MQTTD_NODES="${MQTTD_NODES:-mqttd-1 mqttd-2 mqttd-3}"
BROKER_UID="${BROKER_UID:-65532}"
BROKER_GID="${BROKER_GID:-65532}"
CERT_DAYS="${CERT_DAYS:-3650}"

# ── 1. Preflight: the secrets bootstrap.sh mints are a precondition, not an option ─────
# The brokers depend on this service completing, so a clear failure here is the whole
# cluster's failure message. Without it, three containers each die with a bare path error.
for f in mqttd-passwd mqttd-swim-key; do
  if [ ! -s "$SECRETS_SRC/$f" ]; then
    echo "FATAL: deploy/compose/secrets/$f is missing or empty." >&2
    echo "       run ./bootstrap.sh in deploy/compose first, then: docker compose up -d" >&2
    exit 1
  fi
done

# A per-node directory that is not there is a missing volume, not something to create: a
# leaf minted into this container's own filesystem disappears when the one-shot exits, and
# the broker then fails closed on a path that "was minted" a moment ago.
for n in $MQTTD_NODES; do
  if [ ! -d "$CERT_DIR/$n" ]; then
    echo "FATAL: no directory $CERT_DIR/$n — nothing is mounted for node '$n'." >&2
    echo "       In compose that means the mqttd-tls-<n> volume for it is missing: declare" >&2
    echo "       it under 'volumes:' and mount it BOTH into 'init' (at $CERT_DIR/$n) and" >&2
    echo "       into that broker (at /etc/mqttd/tls, read-only). See deploy/compose/" >&2
    echo "       README.md, 'Adding a node' — it is the third edit point." >&2
    exit 1
  fi
done

# ── 2. The PKI, idempotently ───────────────────────────────────────────────────────────
#
# Every property this section PRINTS is read back out of the certificate after signing, and
# nothing is installed that did not verify. That is not belt-and-braces: this script runs on
# whatever `openssl` its caller has — scripts/deploy-smoke.sh runs it directly on the host,
# and compose.yaml's image is overridable via MQTTD_CERTGEN_IMAGE — and LibreSSL (stock
# macOS) silently produces material the broker refuses: `req -newkey ec -pkeyopt` yields
# explicit ECC parameters, and `req -x509` adds no basicConstraints at all. The minting shape
# below (`ecparam -param_enc named_curve` + `pkcs8 -topk8`, extensions from a file, explicit
# -sha256) is the intersection both implementations get right.
mkdir -p "$CA_DIR"

CURVE=prime256v1
BAD=''
bad() { BAD="$BAD  - $1
"; }
cert_text() { openssl x509 -in "$1" -noout -text 2>/dev/null || true; }
# openssl's -subject printing differs per build (LibreSSL "subject= /CN=x", OpenSSL 3
# "subject=CN=x" or "subject=CN = x"), so extract the VALUE rather than matching a spelling.
cert_cn() { openssl x509 -in "$1" -noout -subject 2>/dev/null | sed 's/.*CN *= *//; s/[ ,/].*//'; }

check_key() { # <key>
  head -n 1 "$1" 2>/dev/null | grep -q -- '-----BEGIN PRIVATE KEY-----' \
    || bad "$(basename "$1") is not an unencrypted PKCS#8 key — the gossip signer reads PKCS#8 only"
  openssl pkey -in "$1" -noout -text 2>/dev/null | grep -q "ASN1 OID: $CURVE" \
    || bad "$(basename "$1") is not a named-curve $CURVE key"
}
check_core() { # <cert>
  t="$(cert_text "$1")"
  [ -n "$t" ] || { bad "$(basename "$1") is not a readable X.509 certificate"; return 0; }
  case "$t" in *"ASN1 OID: $CURVE"*) ;; *) bad "$(basename "$1") has no named-curve $CURVE public key — OpenSSL 3 and rustls both reject explicit ECC parameters" ;; esac
  case "$t" in *'Signature Algorithm: ecdsa-with-SHA256'*) ;; *) bad "$(basename "$1") is not signed ecdsa-with-SHA256" ;; esac
}
check_ca_usable() { # <ca cert> <ca key>
  [ -s "$1" ] && [ -s "$2" ] || { bad "the CA is missing or empty"; return 0; }
  check_core "$1"
  case "$(cert_text "$1")" in *'CA:TRUE'*) ;; *) bad "$(basename "$1") is not a CA certificate (no basicConstraints CA:TRUE) — nothing it signs will verify" ;; esac
  openssl verify -CAfile "$1" "$1" >/dev/null 2>&1 || bad "$(basename "$1") does not verify against itself"
  check_key "$2"
}
check_leaf_usable() { # <cert> <key> <node id>
  [ -s "$1" ] && [ -s "$2" ] || { bad "$3's leaf is missing or empty"; return 0; }
  check_core "$1"
  check_key "$2"
  t="$(cert_text "$1")"
  [ "$(cert_cn "$1")" = "$3" ] || bad "$(basename "$1") has CN '$(cert_cn "$1")', not '$3' — the peer Hello binds the node id to this CN, so every link would be dropped"
  case "$t" in *'CA:FALSE'*) ;; *) bad "$(basename "$1") is not marked CA:FALSE" ;; esac
  case "$t" in *"DNS:$3"*) ;; *) bad "$(basename "$1") has no DNS:$3 SAN" ;; esac
  case "$t" in *'IP Address:127.0.0.1'*) ;; *) bad "$(basename "$1") has no IP:127.0.0.1 SAN — a client on the Docker host verifies against it" ;; esac
  case "$t" in *'TLS Web Server Authentication'*) ;; *) bad "$(basename "$1") has no serverAuth EKU" ;; esac
  case "$t" in *'TLS Web Client Authentication'*) ;; *) bad "$(basename "$1") has no clientAuth EKU — rustls rejects a client certificate without it, and every node dials as well as being dialed" ;; esac
  openssl verify -CAfile "$CA_DIR/ca.pem" "$1" >/dev/null 2>&1 || bad "$(basename "$1") does not verify against ca.pem"
}
die_nonconforming() { # <what>
  echo "" >&2
  echo "FATAL: the $1 this openssl produced does not satisfy the rules the broker enforces," >&2
  echo "       so it was DISCARDED instead of installed:" >&2
  printf '%s' "$BAD" >&2
  echo "       openssl in use: $(openssl version)" >&2
  echo "       Use an OpenSSL 3 build. In compose that means overriding the certgen image:" >&2
  echo "         MQTTD_CERTGEN_IMAGE=alpine/openssl:3.5.7 docker compose up -d" >&2
  echo "       Nothing was written by this run." >&2
  exit 1
}

STAGE="$(mktemp -d)"
cleanup_stage() { rm -rf "$STAGE"; }
trap cleanup_stage EXIT

mint_key() { # <dest .key>
  openssl ecparam -name "$CURVE" -genkey -noout -param_enc named_curve -out "$STAGE/k.sec1" >/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "$STAGE/k.sec1" -out "$1" >/dev/null
  rm -f "$STAGE/k.sec1"
}

# The CA: VALIDATE an existing one rather than stat it. Blessing a CA merely because two
# files exist is how a volume seeded by a LibreSSL run (or any earlier broken run) gets
# leaves minted under a trust root that cannot verify them, with a success banner.
if [ -f "$CA_DIR/ca.pem" ] && [ -f "$CA_DIR/ca.key" ]; then
  BAD=''
  check_ca_usable "$CA_DIR/ca.pem" "$CA_DIR/ca.key"
  if [ -n "$BAD" ]; then
    echo "" >&2
    echo "FATAL: $CA_DIR already holds CA material, and it is NOT usable:" >&2
    printf '%s' "$BAD" >&2
    echo "       Leaves signed under it fail every peer-bus handshake. Discard the CA volume" >&2
    echo "       and let this run mint a fresh one:" >&2
    echo "         docker compose down && docker volume rm mqttd_mqttd-ca \\" >&2
    echo "           mqttd_mqttd-tls-1 mqttd_mqttd-tls-2 mqttd_mqttd-tls-3 && docker compose up -d" >&2
    exit 1
  fi
  echo "keeping the existing cluster CA ($CA_DIR/ca.pem) — re-verified, not merely present"
else
  echo "minting a throwaway cluster CA in $CA_DIR (self-signed, replace before production)"
  rm -f "$CA_DIR"/ca.pem "$CA_DIR"/ca.key "$CA_DIR"/*.srl 2>/dev/null || true
  for n in $MQTTD_NODES; do
    rm -f "$CERT_DIR/$n"/*.pem "$CERT_DIR/$n"/*.key "$CERT_DIR/$n"/*.csr 2>/dev/null || true
  done
  # basicConstraints from THIS file: LibreSSL's `req -x509` adds no extensions at all.
  cat > "$STAGE/ca.cnf" <<EOF
[req]
distinguished_name=dn
prompt=no
x509_extensions=v3_ca
[dn]
CN=mqttd-compose-ca
[v3_ca]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
EOF
  mint_key "$STAGE/ca.key"
  openssl req -x509 -new -key "$STAGE/ca.key" -days "$CERT_DAYS" -sha256 \
    -config "$STAGE/ca.cnf" -extensions v3_ca -subj '/CN=mqttd-compose-ca' \
    -out "$STAGE/ca.pem" >/dev/null
  BAD=''
  # Validate in the staging directory, using the path check_leaf_usable will later expect.
  cp "$STAGE/ca.pem" "$STAGE/verify-ca.pem"
  check_ca_usable "$STAGE/ca.pem" "$STAGE/ca.key"
  [ -z "$BAD" ] || die_nonconforming "cluster CA"
  install -m 0644 "$STAGE/ca.pem" "$CA_DIR/ca.pem"
  install -m 0400 "$STAGE/ca.key" "$CA_DIR/ca.key"
  echo "  + ca.pem (CN=$(cert_cn "$CA_DIR/ca.pem"), CA:TRUE, verified against itself), and"
  echo "    ca.key — which no broker mounts"
fi

for n in $MQTTD_NODES; do
  # The trust anchor goes into every node's directory; the CA PRIVATE key into none of them.
  cp "$CA_DIR/ca.pem" "$CERT_DIR/$n/ca.pem"
  if [ -f "$CERT_DIR/$n/$n.pem" ] && [ -f "$CERT_DIR/$n/$n.key" ]; then
    BAD=''
    check_leaf_usable "$CERT_DIR/$n/$n.pem" "$CERT_DIR/$n/$n.key" "$n"
    if [ -z "$BAD" ]; then
      echo "  = $n/$n.pem (already present, re-verified)"
      continue
    fi
    echo "  ! $n/$n.pem is present but unusable; re-minting. Reasons:" >&2
    printf '%s' "$BAD" >&2
    rm -f "$CERT_DIR/$n"/"$n".pem "$CERT_DIR/$n"/"$n".key
  fi
  # CN=$n is load-bearing (the peer Hello binding); the SANs are what let peers AND a
  # client on the Docker host verify the same leaf.
  printf 'subjectAltName=DNS:%s,DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth,clientAuth\nbasicConstraints=critical,CA:FALSE\n' \
    "$n" > "$STAGE/leaf.ext"
  printf '[req]\ndistinguished_name=dn\nprompt=no\n[dn]\nCN=%s\n' "$n" > "$STAGE/leaf.cnf"
  mint_key "$STAGE/leaf.key"
  openssl req -new -key "$STAGE/leaf.key" -sha256 -config "$STAGE/leaf.cnf" \
    -subj "/CN=$n" -out "$STAGE/leaf.csr" >/dev/null
  openssl x509 -req -in "$STAGE/leaf.csr" -CA "$CA_DIR/ca.pem" -CAkey "$CA_DIR/ca.key" \
    -CAcreateserial -days "$CERT_DAYS" -sha256 -out "$STAGE/leaf.pem" \
    -extfile "$STAGE/leaf.ext" >/dev/null
  BAD=''
  check_leaf_usable "$STAGE/leaf.pem" "$STAGE/leaf.key" "$n"
  [ -z "$BAD" ] || die_nonconforming "leaf for $n"
  install -m 0644 "$STAGE/leaf.pem" "$CERT_DIR/$n/$n.pem"
  install -m 0400 "$STAGE/leaf.key" "$CERT_DIR/$n/$n.key"
  rm -f "$STAGE/leaf.csr" "$STAGE/leaf.ext" "$STAGE/leaf.cnf" "$STAGE/leaf.pem" "$STAGE/leaf.key"
  # Printed from the certificate, not from the request.
  echo "  + $n/$n.pem / $n.key (CN=$(cert_cn "$CERT_DIR/$n/$n.pem"), \
SAN DNS:$n,DNS:localhost,IP:127.0.0.1, EKU serverAuth+clientAuth, verified against ca.pem)"
done
rm -f "$CA_DIR"/*.srl 2>/dev/null || true

# ── 3. Stage the secrets where uid 65532 can read them ─────────────────────────────────
# NOT a bind mount of ./secrets: bootstrap.sh makes that directory 0700 owned by the
# invoking host user, and on a native-Linux Docker host uid 65532 cannot even traverse it.
# (Docker Desktop hides this behind its VM's uid mapping, which is how it ships broken.)
mkdir -p "$SECRETS_OUT"
cp "$SECRETS_SRC/mqttd-passwd"   "$SECRETS_OUT/mqttd-passwd"
cp "$SECRETS_SRC/mqttd-swim-key" "$SECRETS_OUT/mqttd-swim-key"
echo "staged mqttd-passwd + mqttd-swim-key for the brokers"

if [ "$(id -u)" = "0" ]; then
  chown "$BROKER_UID:$BROKER_GID" "$SECRETS_OUT"
  chmod 0755 "$SECRETS_OUT"
  for n in $MQTTD_NODES; do
    chown "$BROKER_UID:$BROKER_GID" "$CERT_DIR/$n" \
      "$CERT_DIR/$n/$n.pem" "$CERT_DIR/$n/$n.key" "$CERT_DIR/$n/ca.pem"
    chmod 0755 "$CERT_DIR/$n"
    chmod 0444 "$CERT_DIR/$n/$n.pem" "$CERT_DIR/$n/ca.pem"
    chmod 0400 "$CERT_DIR/$n/$n.key"
  done
  # The CA PRIVATE key stays root-only AND stays in this volume, which no broker mounts.
  # Anything that could read it could mint any node identity in the cluster.
  chown 0:0 "$CA_DIR" "$CA_DIR/ca.key" "$CA_DIR/ca.pem"
  chmod 0700 "$CA_DIR"
  chmod 0400 "$CA_DIR/ca.key"
  chmod 0444 "$CA_DIR/ca.pem"
  chown "$BROKER_UID:$BROKER_GID" "$SECRETS_OUT/mqttd-passwd" "$SECRETS_OUT/mqttd-swim-key"
  chmod 0400 "$SECRETS_OUT/mqttd-passwd" "$SECRETS_OUT/mqttd-swim-key"
fi

# ── 4. The trust anchor, where a client on the host can reach it ───────────────────────
# 0644 and not 0400: this is a public certificate, and it lands in a directory owned by
# the host user while THIS process is root — they must be able to read it without sudo.
#
# A FAILURE HERE IS FATAL, not skipped. The banner below tells the reader to pass
# `--cafile secrets/ca.pem`, and every client command in README.md does the same, so a
# silently-skipped copy would leave the success message naming a file that does not exist
# and every documented client invocation failing on a missing trust anchor.
if ! cp "$CA_DIR/ca.pem" "$SECRETS_SRC/ca.pem"; then
  echo "FATAL: could not write $SECRETS_SRC/ca.pem." >&2
  echo "       That file is the trust anchor every client command in the README passes to" >&2
  echo "       --cafile, so this cannot be skipped. In compose, ./secrets is bind-mounted" >&2
  echo "       read-write for exactly this; check that mount and the directory's ownership." >&2
  exit 1
fi
chmod 0644 "$SECRETS_SRC/ca.pem"
echo "wrote secrets/ca.pem — the trust anchor for clients on this host"

cat <<'EOF'

TLS is on. There is no plaintext listener. From deploy/compose, with a password out of
secrets/PASSWORDS.txt:

  mosquitto_sub -h 127.0.0.1 -p 8883 --cafile secrets/ca.pem \
                -t 'devices/+/up/#' -u backend -P '<password>'

  mosquitto_pub -h 127.0.0.1 -p 8885 --cafile secrets/ca.pem \
                -t 'devices/device-a/up/temp' -m 21.5 -q 1 -u device-a -P '<password>'

EOF
