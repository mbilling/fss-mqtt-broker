#!/bin/sh
# Mint, renew and revoke the systemd cluster's TLS material: one CA for the whole cluster,
# then one set of leaves per node. Run this on an ADMIN WORKSTATION, not on a broker host —
# see "CA KEY CUSTODY" below, which is the reason this is a script and not a paste-able comment.
#
#   ./gen-certs.sh ca                                             # ONCE, for the cluster
#   ./gen-certs.sh node   --lifetime 24h mqttd-1 mqttd-1.internal.example.com mqtt.example.com
#   ./gen-certs.sh renew  --lifetime 24h mqttd-1                  # before the renew-by date
#   ./gen-certs.sh revoke mqttd-1                                 # the node has left for good
#   ./gen-certs.sh crl                                            # (re)issue the revocation list
#
#   node    arg 1  the node's MQTTD_NODE_ID          — becomes the peer certificate's Subject CN
#           arg 2  the host part of MQTTD_PEER_ADVERTISE — becomes the peer certificate's SAN
#           arg 3+ the names YOUR CLIENTS dial (optional) — the server certificate's SANs;
#                  defaults to arg 2 when you give none
#   renew   new leaves, and new keys, for a node `node` already minted: same CN, same SANs
#           (read back from its current certificates). --reuse-key keeps the keys.
#   revoke  every certificate this CA issued to that node id goes on the revocation list
#   crl     the revocation list as it stands, with a fresh nextUpdate (empty before any revoke)
#
#   --lifetime <N>h | <N>d | <N>m   how long a leaf lives. REQUIRED by `node` and `renew`:
#           there is NO default (see A STATED LIFETIME). LEAF_LIFETIME in the environment
#           states it too.
#   --crl-lifetime <N>h | <N>d      the list's nextUpdate (CRL_LIFETIME; 30d unless stated)
#
# Everything lands under $PKI_DIR (default ./mqttd-pki). The script prints, per node, the
# exact per-file `install` commands to run on that host. `ca` is idempotent: an existing CA
# is kept, never re-minted.
#
# ── A STATED LIFETIME ─────────────────────────────────────────────────────────────────
# How long a node's certificate lives is decided when the cluster is deployed, by whoever
# deploys it, and whoever operates the cluster renews in time (ADR 0088 D1). So `node` and
# `renew` mint NOTHING without a lifetime: there is no default to fall back to, the refusal
# names the option, and the exit status is 2. What is minted is printed with its end and
# with the date to RENEW BY, which is where a broker starts warning about it: two thirds of
# the lifetime, the default of tls.expiry_warn_permille (667). If your brokers run another
# threshold, set RENEW_AT_PERMILLE to it and the printed date follows.
#
# Short is the aim (hours to a day): a certificate is good until it ends whether or not the
# node it names is still yours, and a short lifetime is what bounds that without anyone
# acting. Expiry is strict. A node whose certificate has ended is out of the cluster until
# it is given a new one, so a lifetime you state is a renewal you owe.
#
# The CA and the revocation list are not asked about, they are ANNOUNCED: the CA lives
# CA_DAYS (3650) and its end is printed by every `ca` run; the list's nextUpdate is 30 days
# unless --crl-lifetime says otherwise, and is printed with its own re-issue-by date.
#
# ── RENEW AND REVOKE ──────────────────────────────────────────────────────────────────
# `renew` replaces a node's four leaf files. Install them over the old ones and the broker
# picks them up WITHOUT a restart (its file watcher, or `systemctl reload mqttd`): the next
# peer handshake serves the new certificate and the next gossip datagram is signed with it.
# A node on its new certificate and one on its old accept each other, so renew one node at a
# time, in any order.
#
# `revoke` is how a node is put out BEFORE its certificate ends, and it is the last step of
# decommissioning one: `mqttd --decommission` hands the node's data over and leaves its
# certificate valid. The list is $PKI_DIR/ca/peer-crl.pem; copy it to every REMAINING node as
# the file MQTTD_PEER_TLS_CRL names and the broker reloads it live, ending the revoked node's
# established links. Set MQTTD_PEER_TLS_CRL when the cluster is deployed, pointing at the
# empty list `crl` issues: the path is configuration (read at startup), the file is not.
#
# The CA keeps a ledger for this next to its key: $PKI_DIR/ca/index.txt (what was issued,
# and what is revoked), ca/issued/ (the certificates, never a key) and ca/crlnumber. A
# certificate minted before the ledger existed is revoked from the copy in the node's
# directory.
#
# THROWAWAY STARTER PKI. Self-signed, a 10-year CA in a local directory. It
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
# second run. The same holds for time: each leaf's notAfter is checked against the lifetime
# that was stated, and a revocation list is installed only once it verifies against the CA
# and names every serial that was revoked.
#
# WHY THAT MATTERS, CONCRETELY: `openssl` is not one program. LibreSSL 3.3.6 — the
# /usr/bin/openssl of macOS — ignores `-pkeyopt ec_paramgen_curve`, emits a version-1
# certificate with NO X509v3 extensions at all (so no basicConstraints, so no CA), and
# encodes explicit ECC parameters instead of a named curve, which OpenSSL 3 and rustls both
# refuse ("Certificate public key has explicit ECC parameters"). The recipe below is written
# to the intersection that both implementations get right (`ecparam -param_enc named_curve` +
# `pkcs8 -topk8`, extensions through -extfile/-extensions, an explicit -sha256, leaves signed
# by `openssl ca` with explicit -startdate/-enddate because `x509 -req` counts in whole days
# only), and verified on both. If your openssl still cannot produce conforming output, the failure names the
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
# Where a broker starts warning about a certificate, in thousandths of its lifetime: the
# default of tls.expiry_warn_permille. The renew-by date printed below is this point.
RENEW_AT_PERMILLE="${RENEW_AT_PERMILLE:-667}"
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
  sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'
  exit "${1:-2}"
}

# ── options ───────────────────────────────────────────────────────────────────────────
# Accepted anywhere on the command line, so `node --lifetime 24h mqttd-1 …` and
# `--lifetime 24h node mqttd-1 …` are the same run. What is left is the positional list.
LIFETIME=''; LIFETIME_FROM=''
CRL_LIFETIME_OPT=''
REUSE_KEY=0
need_value() { # <option> <args left>
  [ "$2" -gt 0 ] || { echo "FATAL: $1 needs a value, e.g. $1 24h" >&2; exit 2; }
}
_left=$#
while [ "$_left" -gt 0 ]; do
  _arg="$1"; shift; _left=$((_left - 1))
  case "$_arg" in
    --lifetime) need_value "$_arg" "$_left"; LIFETIME="$1"; LIFETIME_FROM="--lifetime $1"; shift; _left=$((_left - 1)) ;;
    --lifetime=*) LIFETIME="${_arg#--lifetime=}"; LIFETIME_FROM="$_arg" ;;
    --crl-lifetime) need_value "$_arg" "$_left"; CRL_LIFETIME_OPT="$1"; shift; _left=$((_left - 1)) ;;
    --crl-lifetime=*) CRL_LIFETIME_OPT="${_arg#--crl-lifetime=}" ;;
    --reuse-key) REUSE_KEY=1 ;;
    *) set -- "$@" "$_arg" ;;
  esac
done

# ── time ──────────────────────────────────────────────────────────────────────────────
# <N>m | <N>h | <N>d -> seconds. Nothing else is a lifetime: no bare number (days? hours?),
# no zero, no leading zero.
lifetime_secs() {
  _n="${1%[mhd]}"; _u="${1#"$_n"}"
  case "$_n" in ''|0*|*[!0-9]*) return 1 ;; esac
  case "$_u" in
    m) echo $((_n * 60)) ;;
    h) echo $((_n * 3600)) ;;
    d) echo $((_n * 86400)) ;;
    *) return 1 ;;
  esac
}

# GNU and busybox take `-d @<epoch>`, BSD (macOS) takes `-r <epoch>`.
fmt_epoch() { # <epoch> <strftime format>
  date -u -d "@$1" "+$2" 2>/dev/null || date -u -r "$1" "+$2"
}
iso() { fmt_epoch "$1" '%Y-%m-%dT%H:%M:%SZ'; }
# The form `openssl ca -startdate/-enddate` takes. RFC 5280: UTCTime (two-digit year)
# through 2049, GeneralizedTime from 2050 — and LibreSSL documents only the former.
asn1_time() {
  if [ "$(fmt_epoch "$1" '%Y')" -lt 2050 ]; then fmt_epoch "$1" '%y%m%d%H%M%SZ'
  else fmt_epoch "$1" '%Y%m%d%H%M%SZ'; fi
}
NOW="$(date -u +%s)"
case "$RENEW_AT_PERMILLE" in
  ''|0*|*[!0-9]*) RENEW_AT_PERMILLE=0 ;;
esac
[ "$RENEW_AT_PERMILLE" -ge 1 ] && [ "$RENEW_AT_PERMILLE" -le 1000 ] || {
  echo "FATAL: RENEW_AT_PERMILLE must be 1..1000 (thousandths of the lifetime, as" >&2
  echo "       tls.expiry_warn_permille is); 667 is the broker's default." >&2
  exit 2
}

# D1 / O1 of ADR 0088: no lifetime, no certificate. Sets LEAF_SECS.
require_leaf_lifetime() { # <subcommand> <its positional args>, for the remedy line
  if [ -z "$LIFETIME" ] && [ -n "${LEAF_LIFETIME:-}" ]; then
    LIFETIME="$LEAF_LIFETIME"; LIFETIME_FROM="LEAF_LIFETIME=$LEAF_LIFETIME"
  elif [ -z "$LIFETIME" ] && [ -n "${LEAF_DAYS:-}" ]; then
    # The variable this script read before it had --lifetime. Setting it is stating it.
    LIFETIME="${LEAF_DAYS}d"; LIFETIME_FROM="LEAF_DAYS=$LEAF_DAYS"
  fi
  if [ -z "$LIFETIME" ]; then
    {
      echo "FATAL: no certificate lifetime was stated, so nothing was minted."
      echo "       There is no default. How long a node's certificate lives is decided when"
      echo "       the cluster is deployed, and whoever operates it renews before that ends"
      echo "       (ADR 0088). State it and re-run:"
      _sub="$1"; shift
      _keep=''; [ "$REUSE_KEY" = 0 ] || _keep=' --reuse-key'
      echo "         $0 $_sub --lifetime 24h$_keep $*"
      echo "       <N>h, <N>d or <N>m; LEAF_LIFETIME=24h in the environment states it too."
      echo "       Hours to a day is the aim: a certificate stays good until it ends, whether"
      echo "       or not the node it names is still yours. The run prints the date to renew"
      echo "       by, and '$0 renew' mints the replacement."
    } >&2
    exit 2
  fi
  LEAF_SECS="$(lifetime_secs "$LIFETIME")" || {
    echo "FATAL: '$LIFETIME' ($LIFETIME_FROM) is not a lifetime. Use <N>h, <N>d or <N>m," >&2
    echo "       e.g. --lifetime 24h or --lifetime 90d. Nothing was minted." >&2
    exit 2
  }
  # notBefore a minute in the past: a peer whose clock is a few seconds behind this machine
  # would otherwise refuse a freshly renewed node until it caught up. The END is the
  # stated lifetime from now; the renew-by point is measured over the whole window, as
  # the broker measures the fraction spent (notBefore..notAfter).
  NOT_BEFORE=$((NOW - 60))
  NOT_AFTER=$((NOW + LEAF_SECS))
  RENEW_BY=$((NOT_BEFORE + (NOT_AFTER - NOT_BEFORE) * RENEW_AT_PERMILLE / 1000))
}

# The revocation list's nextUpdate. Announced, not asked for: 30 days unless stated.
# `openssl ca -gencrl` counts in whole hours on every implementation, so no minutes here.
crl_lifetime() {
  CRL_LIFETIME_SET="${CRL_LIFETIME_OPT:-${CRL_LIFETIME:-30d}}"
  case "$CRL_LIFETIME_SET" in
    *m) _s='' ;;
    *) _s="$(lifetime_secs "$CRL_LIFETIME_SET")" || _s='' ;;
  esac
  [ -n "$_s" ] || {
    echo "FATAL: '$CRL_LIFETIME_SET' is not a revocation-list lifetime. Use <N>h or <N>d," >&2
    echo "       e.g. --crl-lifetime 7d. Nothing was written." >&2
    exit 2
  }
  CRL_HOURS=$((_s / 3600))
  CRL_NEXT=$((NOW + _s))
  CRL_REISSUE_BY=$((NOW + _s * RENEW_AT_PERMILLE / 1000))
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
  check_window "$lf"
}

# The lifetime that was STATED is the lifetime the certificate HAS: still valid two minutes
# short of it, ended two minutes past it. `-checkend` is the one question about time that
# every openssl answers the same way, and it needs no date parsing.
check_window() { # <cert>
  _short=$((LEAF_SECS - 120)); [ "$_short" -ge 0 ] || _short=0
  "$OPENSSL" x509 -in "$1" -noout -checkend "$_short" >/dev/null 2>&1 \
    || bad "$(basename "$1") ends before the stated lifetime ($LIFETIME) is up"
  if "$OPENSSL" x509 -in "$1" -noout -checkend $((LEAF_SECS + 120)) >/dev/null 2>&1; then
    bad "$(basename "$1") outlives the stated lifetime ($LIFETIME)"
  fi
}

cert_not_after() { "$OPENSSL" x509 -in "$1" -noout -enddate 2>/dev/null | sed 's/^notAfter=//'; }
cert_serial() { "$OPENSSL" x509 -in "$1" -noout -serial 2>/dev/null | sed 's/^serial=//' | tr 'a-f' 'A-F'; }

# The SANs of an existing certificate, in the form -extfile takes (DNS:a,IP:1.2.3.4): what
# `renew` signs again, so a renewed node answers to exactly the names it answered to.
cert_sans() { # <cert>
  cert_text "$1" | grep -A1 'Subject Alternative Name' | tail -1 \
    | sed 's/^ *//; s/IP Address:/IP:/g; s/, */,/g'
}

# ── the CA's ledger ───────────────────────────────────────────────────────────────────
# index.txt is `openssl ca`'s database: one line per certificate issued, V(alid) or
# R(evoked). It is what a revocation list is generated FROM, so it is kept from the first
# leaf on. Like everything else it is worked on in the stage and copied back only once the
# run has verified what it made: a rejected mint leaves no line behind either.
stage_db() {
  mkdir -p "$STAGE/db/issued"
  for _f in index.txt crlnumber; do
    [ ! -e "$CA_DIR/$_f" ] || cp "$CA_DIR/$_f" "$STAGE/db/$_f"
  done
  [ -e "$STAGE/db/index.txt" ] || : > "$STAGE/db/index.txt"
  [ -s "$STAGE/db/crlnumber" ] || echo 01 > "$STAGE/db/crlnumber"
  # Two leaves per node share one CN, and every renewal repeats it.
  printf 'unique_subject = no\n' > "$STAGE/db/index.txt.attr"
  cat > "$STAGE/db/ca.cnf" <<EOF
[ca]
default_ca=mqttd
[mqttd]
database=$STAGE/db/index.txt
new_certs_dir=$STAGE/db/issued
serial=$STAGE/db/serial
crlnumber=$STAGE/db/crlnumber
default_md=sha256
policy=pol
unique_subject=no
copy_extensions=none
crl_extensions=crl_ext
[pol]
commonName=supplied
[crl_ext]
authorityKeyIdentifier=keyid,issuer
EOF
}

commit_db() {
  mkdir -p "$CA_DIR/issued"
  for _f in "$STAGE"/db/issued/*.pem; do
    [ -e "$_f" ] || continue
    chmod 0644 "$_f"
    mv "$_f" "$CA_DIR/issued/"
  done
  for _f in index.txt index.txt.attr crlnumber; do
    cp "$STAGE/db/$_f" "$CA_DIR/$_f.new"
    mv "$CA_DIR/$_f.new" "$CA_DIR/$_f"
  done
}

# `openssl ca` narrates on stderr whether or not it worked. Keep it for the failure.
run_ca() {
  "$OPENSSL" ca -batch -config "$STAGE/db/ca.cnf" -cert "$CA_DIR/peer-ca.pem" \
    -keyfile "$CA_DIR/peer-ca.key" "$@" >"$STAGE/ca.log" 2>&1 || {
    {
      echo "FATAL: '$OPENSSL ca' failed, and nothing was written to $PKI_DIR by this run:"
      sed 's/^/         /' "$STAGE/ca.log"
    } >&2
    exit 1
  }
}

ledger_serials() { # <status V|R> <node id> -> the serials the ledger holds for that CN
  awk -F'\t' -v st="$1" -v subj="/CN=$2" '$1 == st && $6 == subj { print $4 }' \
    "$STAGE/db/index.txt"
}
ledger_has() { # <status> <serial>
  awk -F'\t' -v st="$1" -v ser="$2" '$1 == st && $4 == ser { found = 1 } END { exit !found }' \
    "$STAGE/db/index.txt"
}

# The precondition of everything that signs: the CA is there, and it is USABLE.
require_ca() { # <what could not be done, for the message>
  if [ ! -e "$CA_DIR/peer-ca.pem" ] && [ ! -e "$CA_DIR/peer-ca.key" ]; then
    echo "FATAL: no cluster CA at $CA_DIR/peer-ca.pem" >&2
    echo "       run '$0 ca' first — ONCE, on this machine, for the whole cluster." >&2
    echo "       If you already ran it on another machine, copy that machine's" >&2
    echo "       $CA_DIR/ over to this one (peer-ca.key included, it is needed to sign)" >&2
    echo "       rather than minting a second CA here: two trust roots form no mesh." >&2
    exit 1
  fi
  # VALIDATED, not merely stat'ed. Signing under a CA this script would have refused to
  # mint is how a rejected CA becomes a cluster that never forms a mesh, with a success
  # banner as the only evidence.
  BAD=''
  check_ca "$CA_DIR/peer-ca.pem" "$CA_DIR/peer-ca.key"
  [ -z "$BAD" ] || {
    {
      echo "FATAL: the cluster CA in $CA_DIR is not usable, so $1:"
      echo "$BAD" | sed '/^$/d'
      echo
      echo "       Leaves signed under it would fail every peer-bus handshake. Throw the"
      echo "       directory away and mint the CA again with an OpenSSL 3 build:"
      echo "         rm -rf $PKI_DIR"
      echo "         OPENSSL=\"\$(brew --prefix openssl@3)/bin/openssl\" $0 ca   # macOS"
    } >&2
    exit 1
  }
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

# <out-basename> <CN> <EKU> <SAN-list> [<existing key to keep>]
mint_leaf() {
  leaf="$1"; cn="$2"; eku="$3"; sans="$4"; keep_key="${5:-}"
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\nbasicConstraints=critical,CA:FALSE\n' \
    "$sans" "$eku" > "$STAGE/leaf.ext"
  printf '[req]\ndistinguished_name=dn\nprompt=no\n[dn]\nCN=%s\n' "$cn" > "$STAGE/leaf.cnf"
  if [ -n "$keep_key" ]; then cp "$keep_key" "$leaf.key"; else mint_key "$leaf.key"; fi
  # -sha256 explicitly: LibreSSL's default signing digest here is SHA-1.
  "$OPENSSL" req -new -key "$leaf.key" -sha256 -config "$STAGE/leaf.cnf" \
    -subj "/CN=$cn" -out "$STAGE/leaf.csr" >/dev/null
  # A random 127-bit serial per certificate, positive (leading nibble 4): `-create_serial`
  # is not in every openssl, and a counter would say how many leaves a cluster has had.
  "$OPENSSL" rand -hex 16 | sed 's/^./4/' > "$STAGE/db/serial"
  # `openssl ca`, not `x509 -req`: it takes the END of the validity as a time, so a
  # lifetime of hours is what it says, and it writes the ledger a revocation needs.
  run_ca -notext -in "$STAGE/leaf.csr" -out "$leaf.pem" -extfile "$STAGE/leaf.ext" \
    -startdate "$(asn1_time "$NOT_BEFORE")" -enddate "$(asn1_time "$NOT_AFTER")"
  rm -f "$STAGE/leaf.csr" "$STAGE/leaf.ext" "$STAGE/leaf.cnf"
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
    echo "  valid until $(cert_not_after "$CA_DIR/peer-ca.pem") — when it ends, every leaf under it does"
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
    valid until $(cert_not_after "$CA_DIR/peer-ca.pem") ($CA_DAYS days; CA_DAYS sets it)
  $CA_DIR/peer-ca.key   the cluster's PRIVATE trust root — copy this NOWHERE

Keep peer-ca.key on this machine (or move it to offline storage) and off every broker host.
A broker host that holds it can mint a leaf with any CN, and the cluster bus binds node
identity to the CN — so it can impersonate any node and forge that node's gossip signatures.

The CA's lifetime is announced, not asked for: replacing a CA is the one rotation that is
not live (every node restarts), so it is long. A LEAF's lifetime is yours to state, and
there is no default.

Next, once per node:
  $0 node --lifetime <N>h|<N>d <MQTTD_NODE_ID> <MQTTD_PEER_ADVERTISE host> [client dns name ...]
and, to be able to put a node out later without a restart, the (empty) revocation list:
  $0 crl
EOF
}

# Mint, verify and install one node's leaf set. The lifetime, the CA and the stage are
# the caller's; this is the part `node` and `renew` share.
#   <node id> <peer SANs> <client SANs> <peer key to keep, or ''> <server key to keep, or ''>
mint_node_set() {
  node_id="$1"; peer_sans="$2"; client_sans="$3"; keep_peer="$4"; keep_server="$5"
  out="$PKI_DIR/$node_id"

  # What this replaces is kept in the ledger's issued/ first. A node minted before the
  # ledger existed has its only copy here, and once it is overwritten `revoke` could not
  # find it, while it stays valid until it ends.
  for _old in "$out/peer.pem" "$out/server.pem"; do
    [ -s "$_old" ] || continue
    _ser="$(cert_serial "$_old")"
    [ -n "$_ser" ] || continue
    if ! ledger_has V "$_ser" && ! ledger_has R "$_ser" && [ ! -e "$CA_DIR/issued/$_ser.pem" ]; then
      cp "$_old" "$STAGE/db/issued/$_ser.pem"
    fi
  done

  # Rule 1: CN is the node id. Rule 2: the SAN covers the advertise host. Rule 3: both EKUs.
  mint_leaf "$STAGE/peer" "$node_id" 'serverAuth,clientAuth' "$peer_sans" "$keep_peer"
  # The client-facing leaf: the names YOUR clients dial, which are usually not the name
  # peers dial. serverAuth only — clients are not certificate-authenticated unless you also
  # set MQTTD_TLS_CLIENT_CA, and that MUST be a different CA from this one (see
  # mqttd.env.example: a leaf under the cluster CA is a cluster-bus credential).
  mint_leaf "$STAGE/server" "$node_id" 'serverAuth' "$client_sans" "$keep_server"

  BAD=''
  check_leaf "$STAGE/peer.pem"   "$STAGE/peer.key"   "$node_id" "$peer_sans"   'serverAuth,clientAuth'
  check_leaf "$STAGE/server.pem" "$STAGE/server.key" "$node_id" "$client_sans" 'serverAuth'
  [ -z "$BAD" ] || die_nonconforming "leaf set for $node_id"

  commit_db
  mkdir -p "$out"
  cp "$CA_DIR/peer-ca.pem" "$STAGE/peer-ca.pem"
  chmod 0644 "$STAGE/peer-ca.pem"
  for f in peer-ca.pem peer.pem peer.key server.pem server.key; do
    mv "$STAGE/$f" "$out/$f"
  done
}

# What was minted, when it ends, and when to act. Printed by `node` and `renew`.
window_report() { # <node id>
  cat <<EOF
  lifetime      $LIFETIME from now, as stated ($LIFETIME_FROM) — checked against both
                certificates, which are valid from a minute before they were minted
  valid until   $(iso "$NOT_AFTER")   ($(cert_not_after "$PKI_DIR/$1/peer.pem"), read back from peer.pem)
  RENEW BY      $(iso "$RENEW_BY")   — $RENEW_AT_PERMILLE thousandths of the lifetime, where a broker
                starts warning about it (tls.expiry_warn_permille). The command is
                  $0 renew --lifetime $LIFETIME $1
                and at $(iso "$NOT_AFTER") the node is OUT of the cluster until it has run:
                there is no grace period.
EOF
}

cmd_node() {
  [ $# -ge 2 ] || usage
  require_leaf_lifetime node "$@"
  node_id="$1"; peer_host="$2"; shift 2
  require_ca "no leaf was minted for $node_id"

  STAGE="$(mktemp -d)"
  stage_db
  peer_sans="$(san_for "$peer_host")"
  [ $# -gt 0 ] || set -- "$peer_host"
  client_sans="$(san_for "$1")"; shift
  for name in "$@"; do client_sans="$client_sans,$(san_for "$name")"; done
  mint_node_set "$node_id" "$peer_sans" "$client_sans" '' ''
  out="$PKI_DIR/$node_id"

  cat <<EOF
minted $node_id under $out — every property below was read back out of the certificate
after signing, and the leaves were installed only because they verified against the CA:
  peer-ca.pem   the cluster trust anchor (a copy — NOT the CA private key)
  peer.pem/key  $(cert_summary "$out/peer.pem")
                verifies against peer-ca.pem
  server.pem/key  $(cert_summary "$out/server.pem")
                verifies against peer-ca.pem

$(window_report "$node_id")

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

cmd_renew() {
  [ $# -eq 1 ] || usage
  require_leaf_lifetime renew "$@"
  node_id="$1"
  out="$PKI_DIR/$node_id"
  for f in peer.pem peer.key server.pem server.key; do
    [ -s "$out/$f" ] || {
      echo "FATAL: nothing to renew for '$node_id': $out/$f is missing." >&2
      echo "       'renew' is for a node this PKI directory already holds. A new node is" >&2
      echo "         $0 node --lifetime $LIFETIME $node_id <MQTTD_PEER_ADVERTISE host> [client dns name ...]" >&2
      exit 1
    }
  done
  require_ca "no leaf was renewed for $node_id"

  # The identity is read from what is being replaced, not asked for again: a renewal that
  # could change the CN or drop a SAN would be a different node, or one its peers cannot dial.
  # The OLD certificates are not verified against the clock on purpose: an expired one is
  # exactly the node that most needs this command.
  BAD=''
  for f in peer.pem server.pem; do
    [ "$(cert_cn "$out/$f")" = "$node_id" ] \
      || bad "$out/$f has Subject CN '$(cert_cn "$out/$f")', not '$node_id'"
  done
  peer_sans="$(cert_sans "$out/peer.pem")"
  client_sans="$(cert_sans "$out/server.pem")"
  [ -n "$peer_sans" ] || bad "$out/peer.pem has no SAN to carry over"
  [ -n "$client_sans" ] || bad "$out/server.pem has no SAN to carry over"
  [ -z "$BAD" ] || {
    {
      echo "FATAL: $out does not hold the identity '$node_id', so nothing was renewed:"
      echo "$BAD" | sed '/^$/d'
    } >&2
    exit 1
  }

  STAGE="$(mktemp -d)"
  stage_db
  if [ "$REUSE_KEY" = 1 ]; then
    cp "$out/peer.key" "$STAGE/old-peer.key"; cp "$out/server.key" "$STAGE/old-server.key"
    mint_node_set "$node_id" "$peer_sans" "$client_sans" "$STAGE/old-peer.key" "$STAGE/old-server.key"
    keys='the SAME keys (--reuse-key)'
  else
    mint_node_set "$node_id" "$peer_sans" "$client_sans" '' ''
    keys='NEW keys'
  fi

  cat <<EOF
renewed $node_id under $out — new certificates with $keys, the same CN and the same SANs,
read back and verified against the CA before they replaced the old ones:
  peer.pem/key  $(cert_summary "$out/peer.pem")
  server.pem/key  $(cert_summary "$out/server.pem")

$(window_report "$node_id")

Install the FOUR leaf files over the old ones on that node. No restart: the broker re-reads
them (its file watcher, or the reload below), serves the new certificate on the next
handshake and signs its next gossip datagram with the new key. A node on its new
certificate and one on its old accept each other, so renew one node at a time.

  scp $out/peer.pem $out/peer.key $out/server.pem $out/server.key <host>:/tmp/
  # then, on that host:
  sudo install -m 0644 -o root -g mqttd /tmp/peer.pem    /etc/mqttd/tls/peer.pem
  sudo install -m 0640 -o root -g mqttd /tmp/peer.key    /etc/mqttd/tls/peer.key
  sudo install -m 0644 -o root -g mqttd /tmp/server.pem  /etc/mqttd/tls/server.pem
  sudo install -m 0640 -o root -g mqttd /tmp/server.key  /etc/mqttd/tls/server.key
  rm -f /tmp/peer.pem /tmp/peer.key /tmp/server.pem /tmp/server.key
  sudo systemctl reload mqttd       # or wait for the file watcher
  mqttd --check-tls                 # the new window, as the broker reads it

The certificates this replaces stay valid until they end. If the reason for renewing is
that a key may have leaked, that is '$0 revoke $node_id' FIRST, then '$0 renew'.
EOF
}

# Issue the list from the staged ledger and prove it before it goes anywhere: it must verify
# against the CA, and it must name every serial it was issued to revoke.
issue_crl() { # <serials that must be on it, space separated>
  crl_lifetime
  run_ca -gencrl -crlhours "$CRL_HOURS" -out "$STAGE/peer-crl.pem"
  BAD=''
  "$OPENSSL" crl -in "$STAGE/peer-crl.pem" -CAfile "$CA_DIR/peer-ca.pem" -noout 2>&1 \
    | grep -q 'verify OK' || bad "peer-crl.pem does not verify against $CA_DIR/peer-ca.pem"
  _crl_txt="$("$OPENSSL" crl -in "$STAGE/peer-crl.pem" -noout -text 2>/dev/null | tr '[:lower:]' '[:upper:]')"
  for _ser in $1; do
    case "$_crl_txt" in
      *"SERIAL NUMBER: $_ser"*) ;;
      *) bad "peer-crl.pem does not list serial $_ser, which was revoked" ;;
    esac
  done
  [ -z "$BAD" ] || die_nonconforming "revocation list"
  commit_db
  chmod 0644 "$STAGE/peer-crl.pem"
  mv "$STAGE/peer-crl.pem" "$CA_DIR/peer-crl.pem"
}

crl_report() {
  _count="$("$OPENSSL" crl -in "$CA_DIR/peer-crl.pem" -noout -text 2>/dev/null | grep -c 'Serial Number:' || true)"
  cat <<EOF
  $CA_DIR/peer-crl.pem   signed by the cluster CA, verified against it; $_count certificate(s) revoked
  nextUpdate    $(iso "$CRL_NEXT")   ($CRL_LIFETIME_SET; --crl-lifetime sets it, 30d unless stated)
  RE-ISSUE BY   $(iso "$CRL_REISSUE_BY")   with '$0 crl' — a list past its nextUpdate still loads and is
                enforced as it is, and the broker warns about it as it does about a certificate.

Copy it to EVERY REMAINING node, as the file MQTTD_PEER_TLS_CRL names:

  scp $CA_DIR/peer-crl.pem <host>:/tmp/
  # then, on that host:
  sudo install -m 0644 -o root -g mqttd /tmp/peer-crl.pem /etc/mqttd/tls/peer-crl.pem
  rm -f /tmp/peer-crl.pem
  sudo systemctl reload mqttd       # or wait for the file watcher

A broker that already runs with MQTTD_PEER_TLS_CRL re-reads the file without a restart. One
that does not has to be given the setting and restarted ONCE: the path is configuration.
EOF
}

cmd_crl() {
  [ $# -eq 0 ] || usage
  require_ca "no revocation list was issued"
  STAGE="$(mktemp -d)"
  stage_db
  issue_crl ''
  echo "issued the cluster's revocation list:"
  crl_report
}

cmd_revoke() {
  [ $# -eq 1 ] || usage
  node_id="$1"
  require_ca "nothing was revoked for $node_id"
  STAGE="$(mktemp -d)"
  stage_db

  # Every certificate still good for that identity: what the ledger holds as valid, and
  # what is in the node's directory (which is all there is for a PKI older than the ledger).
  : > "$STAGE/to-revoke"
  for _ser in $(ledger_serials V "$node_id"); do
    echo "$CA_DIR/issued/$_ser.pem" >> "$STAGE/to-revoke"
  done
  # ... and what issued/ holds for that CN without a ledger line: certificates from before
  # the ledger, kept there by the `renew` or `node` run that replaced them.
  for _crt in "$CA_DIR"/issued/*.pem; do
    [ -s "$_crt" ] || continue
    [ "$(cert_cn "$_crt")" = "$node_id" ] || continue
    _ser="$(cert_serial "$_crt")"
    ledger_has V "$_ser" || ledger_has R "$_ser" || echo "$_crt" >> "$STAGE/to-revoke"
  done
  for f in peer.pem server.pem; do
    [ ! -s "$PKI_DIR/$node_id/$f" ] || echo "$PKI_DIR/$node_id/$f" >> "$STAGE/to-revoke"
  done

  BAD=''
  revoked=''
  ended=0
  while IFS= read -r _crt; do
    [ -s "$_crt" ] || { bad "$_crt is in the CA's ledger and missing on disk: it cannot be revoked without the certificate"; continue; }
    _ser="$(cert_serial "$_crt")"
    [ -n "$_ser" ] || { bad "$_crt is not a readable certificate"; continue; }
    [ "$(cert_cn "$_crt")" = "$node_id" ] || { bad "$_crt has Subject CN '$(cert_cn "$_crt")', not '$node_id'"; continue; }
    if ledger_has R "$_ser"; then continue; fi          # both lists name it, or a second run
    if ! "$OPENSSL" verify -CAfile "$CA_DIR/peer-ca.pem" -no_check_time "$_crt" >/dev/null 2>&1 \
      && ! "$OPENSSL" verify -CAfile "$CA_DIR/peer-ca.pem" "$_crt" >/dev/null 2>&1; then
      # An openssl without -no_check_time cannot tell "ended" from "not ours". An ended
      # certificate opens nothing and needs no revoking, so that is the one case let through.
      if ! "$OPENSSL" x509 -in "$_crt" -noout -checkend 0 >/dev/null 2>&1; then
        ended=$((ended + 1)); continue
      fi
      bad "$_crt was not issued by $CA_DIR/peer-ca.pem — this CA's list cannot revoke it"; continue
    fi
    run_ca -revoke "$_crt"
    revoked="$revoked $_ser"
  done < "$STAGE/to-revoke"
  [ -z "$BAD" ] || {
    {
      echo "FATAL: not every certificate of '$node_id' could be revoked, so NO list was written:"
      echo "$BAD" | sed '/^$/d'
    } >&2
    exit 1
  }
  already="$(ledger_serials R "$node_id" | tr '\n' ' ')"
  if [ -z "$already" ] && [ "$ended" -gt 0 ]; then
    echo "nothing to revoke for $node_id: every certificate it holds has already ended, and an"
    echo "ended certificate opens nothing. No list was written."
    return 0
  fi
  [ -n "$already" ] || {
    echo "FATAL: this CA knows no certificate for '$node_id': nothing in its ledger" >&2
    echo "       ($CA_DIR/index.txt) and nothing under $PKI_DIR/$node_id. Nothing was revoked." >&2
    echo "       The id is the MQTTD_NODE_ID the certificate was minted for (its Subject CN)." >&2
    exit 1
  }

  issue_crl "$already"
  if [ -n "$revoked" ]; then
    echo "revoked $node_id — every certificate this CA issued to that node id is on the list:"
  else
    echo "$node_id was already revoked; the list was re-issued with a fresh nextUpdate:"
  fi
  for _ser in $already; do echo "  serial $_ser"; done
  crl_report
  cat <<EOF

Once the list is on a node, that node refuses $node_id: its established links are ended, new
handshakes fail and its gossip is dropped. $PKI_DIR/$node_id still holds the revoked
material; delete it when the node is gone for good. To bring the SAME id back later,
'$0 renew --lifetime <N>h $node_id' mints it certificates that are not on the list.
EOF
}

case "${1:-}" in
  ca) shift; cmd_ca "$@" ;;
  node) shift; cmd_node "$@" ;;
  renew) shift; cmd_renew "$@" ;;
  revoke) shift; cmd_revoke "$@" ;;
  crl) shift; cmd_crl "$@" ;;
  -h|--help|help) usage 0 ;;
  *) usage ;;
esac
