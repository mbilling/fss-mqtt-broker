#!/bin/sh
# Mint the systemd cluster's TLS material: one CA for the whole cluster, then one set of
# leaves per node. Run this on an ADMIN WORKSTATION, not on a broker host — see "CA KEY
# CUSTODY" below, which is the reason this is a script and not a paste-able comment.
#
#   ./gen-certs.sh ca                                             # ONCE, for the cluster
#   ./gen-certs.sh node mqttd-1 mqttd-1.internal.example.com  mqtt.example.com
#   ./gen-certs.sh node mqttd-2 mqttd-2.internal.example.com  mqtt.example.com
#   ./gen-certs.sh node mqttd-3 mqttd-3.internal.example.com  mqtt.example.com
#
#   arg 1  the node's MQTTD_NODE_ID           — becomes the peer certificate's Subject CN
#   arg 2  the host part of MQTTD_PEER_ADVERTISE — becomes the peer certificate's SAN
#   arg 3+ the names YOUR CLIENTS dial (optional) — the server certificate's SANs;
#          defaults to arg 2 when you give none
#
# Everything lands under $PKI_DIR (default ./mqttd-pki). The script prints, per node, the
# exact per-file `install` commands to run on that host. It is idempotent: an existing CA is
# kept, never re-minted.
#
# THROWAWAY STARTER PKI. Self-signed, no revocation, a 10-year CA in a local directory. It
# exists so the shipped mqttd.env.example — which enables TLS and the cluster bus, and so
# fails CLOSED without certificates — has a recipe that RUNS. Replace it before production;
# the four rules below are the part that carries over to your own CA.
#
# ── ONE CA FOR THE CLUSTER, NOT ONE PER HOST ──────────────────────────────────────────
# Run `ca` exactly once, here, and copy only peer-ca.pem to the nodes. Three hosts that each
# self-sign their own CA get three mutually-untrusting trust roots: every peer-bus mTLS
# handshake fails, per-node gossip verification fails with it (MQTTD_SWIM_SIGNED defaults to
# `require` once this material is present), membership never forms, and no node ever passes
# the majority readiness floor. What the operator sees is three brokers that start, log
# "SWIM gossip is SIGNED per-node", and never become ready — with no message naming the cause.
#
# ── CA KEY CUSTODY ────────────────────────────────────────────────────────────────────
# $PKI_DIR/ca/peer-ca.key is the cluster's trust root and MUST NOT reach a broker host, and
# in particular must never be readable by the broker's `mqttd` service account. Anything that
# can read it can mint a leaf with any CN, and the cluster bus binds node identity to exactly
# that CN — so it can claim any node's identity and forge that node's gossip signatures,
# while the READMEs advertise the bus as mutually authenticated. This script therefore never
# writes the CA key into a per-node directory, and the install commands it prints never
# mention it. Move it to offline/HSM storage once the leaves are minted; you need it again
# only to add a node.
#
# ── FOUR RULES the PEER certificate must satisfy ──────────────────────────────────────
# Each is easy to get wrong exactly once, and three of the four fail at RUNTIME rather than
# at issue time. This script satisfies all four AND CHECKS THAT IT DID (see VERIFIED, NOT
# ASSERTED below); they are stated because your own CA must satisfy them too.
#   1. Subject CN == the node's MQTTD_NODE_ID. A peer may only claim the node id its
#      certificate attests to; a mismatch drops the link with "peer Hello node id does not
#      match its certificate Common Name".
#   2. A SAN covering the HOST PART of MQTTD_PEER_ADVERTISE — that is the name a dialing
#      peer verifies against (crates/mqtt-net/src/tls.rs::server_name).
#   3. BOTH serverAuth and clientAuth EKUs: every node dials and is dialed, and rustls
#      rejects a client certificate without clientAuth. serverAuth alone forms no mesh.
#   4. An ECDSA P-256/P-384 or Ed25519 key, NOT RSA, in PKCS#8. This key is ALSO the
#      per-node gossip signing key (ADR 0022) and that signer accepts nothing else: an RSA
#      leaf gives you a working TLS handshake and then a hard startup failure reading
#      "unsupported or unparseable gossip signing key". P-256 here — the intersection of what
#      the signer accepts and what every TLS stack in this project supports.
# The client-facing (server.pem) keypair has none of these constraints; it is minted the same
# way for one less thing to explain, with serverAuth only.
#
# ── VERIFIED, NOT ASSERTED ────────────────────────────────────────────────────────────
# Every certificate property this script prints is read back out of the file with openssl
# after minting, and nothing is installed until the readback passes:
#   - the CA carries basicConstraints CA:TRUE and verifies against itself;
#   - each leaf verifies against the CA (`openssl verify`), and carries CA:FALSE, the CN,
#     the SANs and the EKUs claimed for it — and the server leaf carries NO clientAuth;
#   - every key is an unencrypted PKCS#8 NAMED-CURVE prime256v1 key that matches its
#     certificate's public key, and every signature is ecdsa-with-SHA256.
# Material is minted into a TEMPORARY directory and moved into place only once it passes, so
# a rejected mint leaves nothing behind — and the `ca` idempotence branch and the `node`
# precondition RE-VERIFY the CA on disk rather than testing that two files exist. A run that
# says "keeping the existing cluster CA" has re-checked it; a bad CA is never blessed by a
# second run.
#
# WHY THAT MATTERS, CONCRETELY: `openssl` is not one program. LibreSSL 3.3.6 — the
# /usr/bin/openssl of macOS — ignores `-pkeyopt ec_paramgen_curve`, emits a version-1
# certificate with NO X509v3 extensions at all (so no basicConstraints, so no CA), and
# encodes explicit ECC parameters instead of a named curve, which OpenSSL 3 and rustls both
# refuse ("Certificate public key has explicit ECC parameters"). The recipe below is written
# to the intersection that both implementations get right (`ecparam -param_enc named_curve` +
# `pkcs8 -topk8`, extensions through -extfile/-extensions, an explicit -sha256), and verified
# on both. If your openssl still cannot produce conforming output, the failure names the
# remedy: set OPENSSL to a conforming binary, e.g.
#   OPENSSL="$(brew --prefix openssl@3)/bin/openssl" ./gen-certs.sh ca
#
# POSIX sh, and extensions go through temp files rather than process substitution, so this
# runs under /bin/sh on any host and matches deploy/compose/init.sh's proven shape.
# Executed by scripts/deploy-smoke.sh on every CI run — it boots two real nodes from this
# script's output — so it cannot rot into a recipe nobody runs.
set -eu

INVOCATION="$*"         # echoed back in the remedy, so the suggested re-run is THIS run
PKI_DIR="${PKI_DIR:-./mqttd-pki}"
CA_DIR="$PKI_DIR/ca"
CA_DAYS="${CA_DAYS:-3650}"
LEAF_DAYS="${LEAF_DAYS:-365}"
CURVE=prime256v1        # NIST P-256; see rule 4
OPENSSL="${OPENSSL:-openssl}"   # override to point at a conforming build; see above

command -v "$OPENSSL" >/dev/null 2>&1 || {
  echo "FATAL: no openssl found at '$OPENSSL'." >&2
  echo "       Install one, or set OPENSSL to its path." >&2
  exit 1
}

# Everything is staged here first and moved into place only after it verifies.
STAGE=''
cleanup() { [ -n "$STAGE" ] || return 0; rm -rf "$STAGE"; return 0; }
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

usage() {
  sed -n '2,14p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

# DNS: or IP: — a starter PKI's SAN list is hostnames and literal addresses, and openssl
# needs them tagged differently. Anything that is only digits and dots, or contains a colon
# (IPv6), is an address; everything else is a name.
san_for() {
  case "$1" in
    *:*) printf 'IP:%s' "$1" ;;
    *[!0-9.]*) printf 'DNS:%s' "$1" ;;
    *) printf 'IP:%s' "$1" ;;
  esac
}

# ── the readback checks ────────────────────────────────────────────────────────────────
# Failures accumulate in BAD so one run reports every property that is wrong, rather than
# stopping at the first and sending the operator round the loop four times.
BAD=''
bad() { BAD="$BAD
  - $1"; }

cert_text() { "$OPENSSL" x509 -in "$1" -noout -text 2>/dev/null; }

# LibreSSL prints `subject=/CN=x`, OpenSSL 3 prints `subject=CN = x`. Normalise both.
cert_cn() { "$OPENSSL" x509 -in "$1" -noout -subject 2>/dev/null | sed 's/.*CN *= *//; s/[/,].*//'; }

# openssl prints IP SANs as "IP Address:", while the -extfile syntax is "IP:".
san_printed_form() { printf '%s' "$1" | sed 's/^IP:/IP Address:/'; }

# Does this openssl support semantic SAN matching? OpenSSL 1.1+ does; LibreSSL 3.3.6
# (stock macOS) does not, which is why the fallback below exists at all.
openssl_can_checkip() { "$OPENSSL" x509 -help 2>&1 | grep -q -- '-checkip'; }

# Verify ONE requested SAN is really in the certificate.
#
# A literal substring match is wrong for IPv6: openssl prints `IP:fd00::5` as
# `IP Address:FD00:0:0:0:0:0:0:5` — expanded and upper-cased — so comparing the
# requested spelling never matched and the script blamed a missing SAN, with a
# LibreSSL remedy that could not possibly help (every openssl prints it that way).
# Prefer `-checkip`, which compares addresses rather than spellings; where it is
# unavailable and the address is IPv6, say we could not verify it instead of
# asserting it is absent.
check_one_san() { # <cert> <cert text> <requested SAN, e.g. DNS:host or IP:addr>
  _cf="$1"; _txt="$2"; _san="$3"
  case "$_san" in
    IP:*)
      _ip="${_san#IP:}"
      if openssl_can_checkip; then
        "$OPENSSL" x509 -in "$_cf" -noout -checkip "$_ip" 2>/dev/null | grep -qi 'does match' \
          || bad "$(basename "$_cf") does not match IP $_ip (openssl -checkip): a dialing peer verifies the host part of MQTTD_PEER_ADVERTISE against the SAN (rule 2)"
        return 0
      fi
      case "$_ip" in
        *:*)  # IPv6 without -checkip: do not claim absence we cannot establish.
          case "$_txt" in
            *'IP Address:'*) warn_san "$_cf" "$_ip" ;;
            *) bad "$(basename "$_cf") carries no IP SAN at all, though IP:$_ip was requested (rule 2)" ;;
          esac
          return 0 ;;
      esac ;;
  esac
  case "$_txt" in
    *"$(san_printed_form "$_san")"*) ;;
    *) bad "$(basename "$_cf") has no $_san SAN — a dialing peer verifies the host part of MQTTD_PEER_ADVERTISE against it (rule 2)" ;;
  esac
}

warn_san() { # <cert> <ip>
  printf '  WARNING: this openssl (%s) has no -checkip, so the IPv6 SAN for %s in %s\n' \
    "$("$OPENSSL" version)" "$2" "$(basename "$1")" >&2
  printf '           could not be verified semantically. It was requested and the extension\n' >&2
  printf '           was accepted; verify with an OpenSSL 3 build if this host is IPv6-only.\n' >&2
}

check_key() { # <key file> — rule 4, on the key itself
  key="$1"
  head -n 1 "$key" 2>/dev/null | grep -q -- '-----BEGIN PRIVATE KEY-----' \
    || bad "$(basename "$key") is not an unencrypted PKCS#8 key (no \"BEGIN PRIVATE KEY\" header): the ADR 0022 gossip signer reads PKCS#8 only"
  "$OPENSSL" pkey -in "$key" -noout -text 2>/dev/null | grep -q "ASN1 OID: $CURVE" \
    || bad "$(basename "$key") is not a named-curve $CURVE key"
}

check_pair() { # <cert> <key> — the key actually belongs to the certificate
  cpub="$("$OPENSSL" x509 -in "$1" -noout -pubkey 2>/dev/null || true)"
  kpub="$("$OPENSSL" pkey -in "$2" -pubout 2>/dev/null || true)"
  [ -n "$cpub" ] && [ "$cpub" = "$kpub" ] \
    || bad "$(basename "$2") is not the private key of $(basename "$1")"
}

check_cert_core() { # <cert> <expected CN>
  crt="$1"; want_cn="$2"
  txt="$(cert_text "$crt")"
  [ -n "$txt" ] || { bad "$(basename "$crt") is not a readable X.509 certificate"; return 0; }
  case "$txt" in
    *"ASN1 OID: $CURVE"*) ;;
    *) bad "$(basename "$crt") does not carry a NAMED-CURVE $CURVE public key — explicit ECC parameters are what OpenSSL 3 rejects with 'Certificate public key has explicit ECC parameters', and rustls cannot verify either" ;;
  esac
  case "$txt" in
    *'Signature Algorithm: ecdsa-with-SHA256'*) ;;
    *) bad "$(basename "$crt") is not signed ecdsa-with-SHA256 (SHA-1 signatures are rejected by modern verifiers)" ;;
  esac
  # An empty expected CN means "any": used for the CA, whose name is the operator's
  # business. The leaf CN is the one the cluster bus actually binds (rule 1).
  if [ -n "$want_cn" ]; then
    got_cn="$(cert_cn "$crt")"
    [ "$got_cn" = "$want_cn" ] \
      || bad "$(basename "$crt") has Subject CN '$got_cn', not '$want_cn' (rule 1)"
  fi
}

check_ca() { # <ca cert> <ca key>
  ca_crt="$1"; ca_key="$2"
  [ -s "$ca_crt" ] || { bad "$ca_crt is missing or empty"; }
  [ -s "$ca_key" ] || { bad "$ca_key is missing or empty"; }
  [ -s "$ca_crt" ] && [ -s "$ca_key" ] || return 0
  # NOT the CA's Subject CN: the bus binds the LEAF's CN to the node id
  # (crates/mqttd/src/peer.rs), and says nothing about the issuer's name. An operator
  # bringing their own conforming CA called anything at all is fine, and telling them to
  # `rm -rf` it was both wrong and destructive.
  check_cert_core "$ca_crt" ""
  case "$(cert_text "$ca_crt")" in
    *'CA:TRUE'*) ;;
    *) bad "$(basename "$ca_crt") is not a CA certificate (no basicConstraints CA:TRUE) — nothing it signs will verify" ;;
  esac
  "$OPENSSL" verify -CAfile "$ca_crt" "$ca_crt" >/dev/null 2>&1 \
    || bad "$(basename "$ca_crt") does not verify against itself (\`openssl verify\` refuses the trust root)"
  check_key "$ca_key"
  check_pair "$ca_crt" "$ca_key"
}

check_leaf() { # <cert> <key> <CN> <SAN config list> <EKU config list>
  lf="$1"; lk="$2"; lcn="$3"; lsans="$4"; lekus="$5"
  [ -s "$lf" ] && [ -s "$lk" ] || { bad "$lf / $lk were not produced"; return 0; }
  check_cert_core "$lf" "$lcn"
  check_key "$lk"
  check_pair "$lf" "$lk"
  txt="$(cert_text "$lf")"
  case "$txt" in
    *'CA:FALSE'*) ;;
    *) bad "$(basename "$lf") is not marked CA:FALSE" ;;
  esac
  # Rule 2: every SAN asked for is really in the certificate. (SAN entries never contain
  # spaces, so a comma-to-space swap is enough splitting and needs no IFS games.)
  for s in $(printf '%s' "$lsans" | tr ',' ' '); do
    check_one_san "$lf" "$txt" "$s"
  done
  # Rule 3: both EKUs where both were asked for, and NO clientAuth on the client-facing leaf.
  case "$lekus" in
    *serverAuth*)
      case "$txt" in
        *'TLS Web Server Authentication'*) ;;
        *) bad "$(basename "$lf") has no serverAuth EKU" ;;
      esac ;;
  esac
  case "$lekus" in
    *clientAuth*)
      case "$txt" in
        *'TLS Web Client Authentication'*) ;;
        *) bad "$(basename "$lf") has no clientAuth EKU — rustls rejects a client certificate without it, and every node dials as well as being dialed (rule 3)" ;;
      esac ;;
    *)
      case "$txt" in
        *'TLS Web Client Authentication'*)
          bad "$(basename "$lf") carries clientAuth, and the client-facing leaf is minted serverAuth-only" ;;
      esac ;;
  esac
  "$OPENSSL" verify -CAfile "$CA_DIR/peer-ca.pem" "$lf" >/dev/null 2>&1 \
    || bad "$(basename "$lf") does not verify against $CA_DIR/peer-ca.pem"
}

# The properties, READ BACK out of the file — this is what gets printed, so the banner
# cannot drift from the artifact.
cert_summary() { # <cert>
  txt="$(cert_text "$1")"
  s_san="$(printf '%s\n' "$txt" | grep -A1 'Subject Alternative Name' | tail -1 | sed 's/^ *//')"
  s_eku="$(printf '%s\n' "$txt" | grep -A1 'Extended Key Usage' | tail -1 | sed 's/^ *//')"
  s_alg="$(printf '%s\n' "$txt" | sed -n 's/^ *Signature Algorithm: *//p' | head -1)"
  s_crv="$(printf '%s\n' "$txt" | sed -n 's/^ *NIST CURVE: *//p' | head -1)"
  printf 'CN=%s' "$(cert_cn "$1")"
  [ -z "$s_san" ] || printf ', SAN %s' "$s_san"    # the CA has neither, and printing
  [ -z "$s_eku" ] || printf ', EKU %s' "$s_eku"    # "SAN , EKU ," would be a claim about nothing
  printf ', %s, ECDSA %s' "$s_alg" "$s_crv"
}

die_nonconforming() { # <what was being minted>
  {
    echo "FATAL: the $1 this openssl produced does not satisfy the rules the cluster bus"
    echo "       enforces, so it was DISCARDED instead of installed:"
    echo "$BAD" | sed '/^$/d'
    echo
    echo "       openssl in use: $OPENSSL"
    "$OPENSSL" version 2>&1 | sed 's/^/         /'
    echo
    echo "       LibreSSL (the /usr/bin/openssl of macOS) is the usual cause: it emits"
    echo "       certificates with no X509v3 extensions and keys with explicit ECC"
    echo "       parameters, and OpenSSL 3 and rustls both refuse the resulting chain."
    echo "       REMEDY — point this script at an OpenSSL 3 build and re-run:"
    echo "         OPENSSL=\"\$(brew --prefix openssl@3)/bin/openssl\" $0 $INVOCATION"
    echo "       (any OpenSSL 3.x binary works; on Linux distributions /usr/bin/openssl"
    echo "       already is one.)"
    echo
    echo "       Nothing was written to $PKI_DIR by this run."
  } >&2
  exit 1
}

# ── minting ───────────────────────────────────────────────────────────────────────────
# `ecparam -param_enc named_curve` + `pkcs8 -topk8`, rather than `req -newkey ec -pkeyopt`:
# LibreSSL ignores that -pkeyopt and hands back a key with explicit ECC parameters, which
# OpenSSL 3 and rustls both reject. This shape is verified on both implementations.
mint_key() { # <dest .key>
  "$OPENSSL" ecparam -name "$CURVE" -genkey -noout -param_enc named_curve \
    -out "$STAGE/key.sec1" >/dev/null
  "$OPENSSL" pkcs8 -topk8 -nocrypt -in "$STAGE/key.sec1" -out "$1" >/dev/null
  rm -f "$STAGE/key.sec1"
}

# <out-basename> <CN> <EKU> <SAN-list>
mint_leaf() {
  leaf="$1"; cn="$2"; eku="$3"; sans="$4"
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\nbasicConstraints=critical,CA:FALSE\n' \
    "$sans" "$eku" > "$STAGE/leaf.ext"
  printf '[req]\ndistinguished_name=dn\nprompt=no\n[dn]\nCN=%s\n' "$cn" > "$STAGE/leaf.cnf"
  mint_key "$leaf.key"
  # -sha256 explicitly: LibreSSL's default signing digest here is SHA-1.
  "$OPENSSL" req -new -key "$leaf.key" -sha256 -config "$STAGE/leaf.cnf" \
    -subj "/CN=$cn" -out "$STAGE/leaf.csr" >/dev/null
  "$OPENSSL" x509 -req -in "$STAGE/leaf.csr" -CA "$CA_DIR/peer-ca.pem" \
    -CAkey "$CA_DIR/peer-ca.key" -CAcreateserial -days "$LEAF_DAYS" -sha256 \
    -out "$leaf.pem" -extfile "$STAGE/leaf.ext" >/dev/null
  rm -f "$STAGE/leaf.csr" "$STAGE/leaf.ext" "$STAGE/leaf.cnf" "$CA_DIR/peer-ca.srl"
  chmod 0600 "$leaf.key"
  chmod 0644 "$leaf.pem"
}

cmd_ca() {
  mkdir -p "$CA_DIR"
  # VALIDATE, do not stat. A CA that exists but is unusable is the case that matters: the
  # previous shape of this branch tested only that two files were present, so a mint this
  # script had itself rejected (leaving both files behind) was blessed by the next run and
  # every leaf was then signed under it.
  if [ -e "$CA_DIR/peer-ca.pem" ] || [ -e "$CA_DIR/peer-ca.key" ]; then
    BAD=''
    check_ca "$CA_DIR/peer-ca.pem" "$CA_DIR/peer-ca.key"
    [ -z "$BAD" ] || {
      {
        echo "FATAL: $CA_DIR already holds CA material, and it is NOT usable:"
        echo "$BAD" | sed '/^$/d'
        echo
        echo "       This is refused rather than reused: every leaf signed under it would be"
        echo "       worthless in the same way, and the brokers would start, log \"SWIM gossip"
        echo "       is SIGNED per-node\" and never form a mesh."
        echo "       REMEDY — throw the directory away and mint again with an OpenSSL 3 build:"
        echo "         rm -rf $PKI_DIR"
        echo "         OPENSSL=\"\$(brew --prefix openssl@3)/bin/openssl\" $0 ca   # macOS"
        echo "       Delete the whole $PKI_DIR, not just $CA_DIR: any per-node leaves already"
        echo "       under this CA must be re-minted too."
      } >&2
      exit 1
    }
    echo "keeping the existing cluster CA ($CA_DIR/peer-ca.pem) — RE-VERIFIED just now, not"
    echo "assumed from the file being there:"
    echo "  $(cert_summary "$CA_DIR/peer-ca.pem"), CA:TRUE, verifies against itself"
    echo "NOT re-minting: a second CA is a second trust root, and a cluster whose nodes hold"
    echo "two of them forms no mesh. Delete $PKI_DIR by hand if you truly want a new cluster."
    return 0
  fi

  STAGE="$(mktemp -d)"
  # basicConstraints comes from THIS file, not from whatever openssl's default config says:
  # LibreSSL's `req -x509` adds no extensions at all, which is how a "CA" ends up unable to
  # sign anything. keyUsage is stated for the same reason.
  cat > "$STAGE/ca.cnf" <<EOF
[req]
distinguished_name=dn
prompt=no
x509_extensions=v3_ca
[dn]
CN=mqttd-cluster-ca
[v3_ca]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
EOF
  mint_key "$STAGE/peer-ca.key"
  "$OPENSSL" req -x509 -new -key "$STAGE/peer-ca.key" -days "$CA_DAYS" -sha256 \
    -config "$STAGE/ca.cnf" -extensions v3_ca -subj '/CN=mqttd-cluster-ca' \
    -out "$STAGE/peer-ca.pem" >/dev/null

  BAD=''
  check_ca "$STAGE/peer-ca.pem" "$STAGE/peer-ca.key"
  [ -z "$BAD" ] || die_nonconforming "cluster CA"

  chmod 0400 "$STAGE/peer-ca.key"
  chmod 0644 "$STAGE/peer-ca.pem"
  mv "$STAGE/peer-ca.key" "$CA_DIR/peer-ca.key"
  mv "$STAGE/peer-ca.pem" "$CA_DIR/peer-ca.pem"

  cat <<EOF
minted the cluster CA, ONCE, for ALL nodes — and verified the result before installing it:
  $CA_DIR/peer-ca.pem   the trust anchor — copy this to every node
    $(cert_summary "$CA_DIR/peer-ca.pem"), CA:TRUE, verifies against itself
  $CA_DIR/peer-ca.key   the cluster's PRIVATE trust root — copy this NOWHERE

Keep peer-ca.key on this machine (or move it to offline storage) and off every broker host.
A broker host that holds it can mint a leaf with any CN, and the cluster bus binds node
identity to the CN — so it can impersonate any node and forge that node's gossip signatures.

Next, once per node:
  $0 node <MQTTD_NODE_ID> <MQTTD_PEER_ADVERTISE host> [client dns name ...]
EOF
}

cmd_node() {
  [ $# -ge 2 ] || usage
  node_id="$1"; peer_host="$2"; shift 2
  if [ ! -e "$CA_DIR/peer-ca.pem" ] && [ ! -e "$CA_DIR/peer-ca.key" ]; then
    echo "FATAL: no cluster CA at $CA_DIR/peer-ca.pem" >&2
    echo "       run '$0 ca' first — ONCE, on this machine, for the whole cluster." >&2
    echo "       If you already ran it on another machine, copy that machine's" >&2
    echo "       $CA_DIR/ over to this one (peer-ca.key included, it is needed to sign)" >&2
    echo "       rather than minting a second CA here: two trust roots form no mesh." >&2
    exit 1
  fi
  # The precondition VALIDATES the CA — it does not merely stat it. Signing leaves under a
  # CA this script would have refused to mint is how a rejected CA becomes a cluster that
  # never forms a mesh, with a success banner as the only evidence.
  BAD=''
  check_ca "$CA_DIR/peer-ca.pem" "$CA_DIR/peer-ca.key"
  [ -z "$BAD" ] || {
    {
      echo "FATAL: the cluster CA in $CA_DIR is not usable, so no leaf was minted for $node_id:"
      echo "$BAD" | sed '/^$/d'
      echo
      echo "       Leaves signed under it would fail every peer-bus handshake. Throw the"
      echo "       directory away and mint the CA again with an OpenSSL 3 build:"
      echo "         rm -rf $PKI_DIR"
      echo "         OPENSSL=\"\$(brew --prefix openssl@3)/bin/openssl\" $0 ca   # macOS"
    } >&2
    exit 1
  }

  out="$PKI_DIR/$node_id"
  STAGE="$(mktemp -d)"

  # Rule 1: CN is the node id. Rule 2: the SAN covers the advertise host. Rule 3: both EKUs.
  peer_sans="$(san_for "$peer_host")"
  mint_leaf "$STAGE/peer" "$node_id" 'serverAuth,clientAuth' "$peer_sans"

  # The client-facing leaf: the names YOUR clients dial, which are usually not the name
  # peers dial. serverAuth only — clients are not certificate-authenticated unless you also
  # set MQTTD_TLS_CLIENT_CA, and that MUST be a different CA from this one (see
  # mqttd.env.example: a leaf under the cluster CA is a cluster-bus credential).
  [ $# -gt 0 ] || set -- "$peer_host"
  client_sans="$(san_for "$1")"; shift
  for name in "$@"; do client_sans="$client_sans,$(san_for "$name")"; done
  mint_leaf "$STAGE/server" "$node_id" 'serverAuth' "$client_sans"

  BAD=''
  check_leaf "$STAGE/peer.pem"   "$STAGE/peer.key"   "$node_id" "$peer_sans"   'serverAuth,clientAuth'
  check_leaf "$STAGE/server.pem" "$STAGE/server.key" "$node_id" "$client_sans" 'serverAuth'
  [ -z "$BAD" ] || die_nonconforming "leaf set for $node_id"

  mkdir -p "$out"
  cp "$CA_DIR/peer-ca.pem" "$STAGE/peer-ca.pem"
  chmod 0644 "$STAGE/peer-ca.pem"
  for f in peer-ca.pem peer.pem peer.key server.pem server.key; do
    mv "$STAGE/$f" "$out/$f"
  done

  cat <<EOF
minted $node_id under $out — every property below was read back out of the certificate
after signing, and the leaves were installed only because they verified against the CA:
  peer-ca.pem   the cluster trust anchor (a copy — NOT the CA private key)
  peer.pem/key  $(cert_summary "$out/peer.pem")
                verifies against peer-ca.pem
  server.pem/key  $(cert_summary "$out/server.pem")
                verifies against peer-ca.pem

Copy these FIVE files to $peer_host and install them individually — there is deliberately no
chmod glob here, because a glob over /etc/mqttd/tls would hand every key in the directory to
the mqttd group, and the cluster CA key must never be one of them:

  scp $out/peer-ca.pem $out/peer.pem $out/peer.key $out/server.pem $out/server.key $peer_host:/tmp/
  # then, on $peer_host:
  sudo install -d -m 0750 -o root -g mqttd /etc/mqttd/tls
  sudo install -m 0644 -o root -g mqttd /tmp/peer-ca.pem /etc/mqttd/tls/peer-ca.pem
  sudo install -m 0644 -o root -g mqttd /tmp/peer.pem    /etc/mqttd/tls/peer.pem
  sudo install -m 0640 -o root -g mqttd /tmp/peer.key    /etc/mqttd/tls/peer.key
  sudo install -m 0644 -o root -g mqttd /tmp/server.pem  /etc/mqttd/tls/server.pem
  sudo install -m 0640 -o root -g mqttd /tmp/server.key  /etc/mqttd/tls/server.key
  rm -f /tmp/peer-ca.pem /tmp/peer.pem /tmp/peer.key /tmp/server.pem /tmp/server.key

$CA_DIR/peer-ca.key is NOT in that list. Keep it here, or offline.
EOF
}

case "${1:-}" in
  ca) shift; cmd_ca "$@" ;;
  node) shift; cmd_node "$@" ;;
  -h|--help|help) usage 0 ;;
  *) usage ;;
esac
