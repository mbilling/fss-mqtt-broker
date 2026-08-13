#!/usr/bin/env bash
# Mint the Secrets/ConfigMap the mqttd chart needs, into a namespace, then print the
# `helm install` flags that wire them (#170). The chart references secret material BY
# PATH (ADR 0046 T5) from existing Secrets you manage; a bare `helm install` points at
# mounts that do not exist, which is the "missing middle" the compose path had a
# bootstrap.sh for and Kubernetes did not.
#
#   ./bootstrap.sh                        # namespace "mqttd", 3 nodes, NO password users
#   ./bootstrap.sh alice bob              # ...same, plus an Argon2id passwd Secret for these users
#   NS=iot ./bootstrap.sh sensor-1 sensor-2
#   REPLICAS=5 ./bootstrap.sh             # after a scale-up — see SCALING below
#   ./bootstrap.sh --mint-only            # mint + verify the PKI, touch no cluster
#
# Creates, in $NS:
#   Secret mqttd-tls        server TLS keypair          (kubernetes.io/tls: tls.crt/tls.key)
#   Secret mqttd-peer-tls   cluster-bus mTLS material    (ca.crt + ONE LEAF PER NODE)
#   Secret mqttd-gossip     gossip authentication key    (key: swim-key)
#   Secret mqttd-passwd     Argon2id password file       (key: passwd)   [if users given]
#   ConfigMap mqttd-acl     a starter deny-by-default ACL (key: acl.toml)
#
# THROWAWAY STARTER PKI. Self-signed, no revocation, a 10-year CA in a local directory. It
# exists so the chart's cluster bus has a recipe that RUNS. Replace it before production
# (cert-manager — see CERT-MANAGER below); the four rules below are what carries over.
#
# ── ONE LEAF PER NODE, NOT ONE FOR THE CLUSTER ────────────────────────────────────────
# The cluster bus binds node identity to the certificate, so a single shared leaf cannot
# work no matter how it is minted (issue #262 — this script used to ship one). Each pod
# gets its OWN leaf, keyed in the Secret by the pod name, and the chart hands each pod its
# own by path. The StatefulSet makes the identities knowable in advance: node id = pod name
# = <fullname>-<ordinal>, so this loops over ordinals 0..REPLICAS-1.
#
# ── FOUR RULES the PEER certificate must satisfy ──────────────────────────────────────
# Each is easy to get wrong exactly once, and three of the four fail at RUNTIME rather than
# at issue time. This script satisfies all four AND CHECKS THAT IT DID (see VERIFIED, NOT
# ASSERTED below); they are stated because your own CA must satisfy them too.
#   1. Subject CN == the node id == the POD NAME. A peer may only claim the node id its
#      certificate attests to; a mismatch drops the link with "peer Hello node id does not
#      match its certificate Common Name" (crates/mqttd/src/peer.rs) and the same CN must
#      equal the gossip sender (crates/mqtt-cluster/src/swim_driver.rs). One leaf shared by
#      every pod therefore drops EVERY link.
#   2. A SAN covering the pod's PEER_ADVERTISE host — <pod>.<headless>.<ns>.svc.cluster.local,
#      the per-pod DNS name the chart's init container renders. That is the name a dialing
#      peer verifies (crates/mqtt-net/src/tls.rs::server_name), and rustls verifies SANs
#      only: the CN above satisfies rule 1 and NOTHING about name verification.
#   3. BOTH serverAuth and clientAuth EKUs: every node dials and is dialed, and rustls
#      rejects a client certificate without clientAuth. serverAuth alone forms no mesh.
#   4. An ECDSA P-256/P-384 or Ed25519 key, NOT RSA, in PKCS#8. This key is ALSO the
#      per-node gossip signing key (ADR 0022) and that signer accepts nothing else: an RSA
#      leaf gives a working TLS handshake and then a hard startup failure reading
#      "unsupported or unparseable gossip signing key". Signed gossip arms ITSELF once peer
#      TLS and a gossip key are both present, so this is not opt-in. P-256 here.
# The client-facing (mqttd-tls) keypair has none of these constraints; it is minted the same
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
# a rejected mint leaves nothing behind — and REUSE re-verifies what is on disk rather than
# testing that files exist. A run that says "keeping" has re-checked; a bad mint is never
# blessed by a second run. The previous version of this script printed a delivered-secret
# banner for a certificate that could not work, which is exactly what this prevents.
#
# WHY THAT MATTERS, CONCRETELY: `openssl` is not one program, and this one runs on YOUR
# workstation rather than in a pinned image. LibreSSL 3.3.6 — the /usr/bin/openssl of
# macOS — ignores `-pkeyopt ec_paramgen_curve`, emits a version-1 certificate with NO
# X509v3 extensions at all (so no basicConstraints, no SAN, no EKU), encodes explicit ECC
# parameters instead of a named curve (OpenSSL 3 and rustls both refuse: "Certificate
# public key has explicit ECC parameters"), and defaults to SHA-1 signatures. The recipe
# below is written to the intersection both implementations get right (`ecparam
# -param_enc named_curve` + `pkcs8 -topk8`, extensions through -extfile, an explicit
# -sha256) and is verified on both. If your openssl still cannot produce conforming
# output the failure names the remedy: set OPENSSL to a conforming binary, e.g.
#   OPENSSL="$(brew --prefix openssl@3)/bin/openssl" ./bootstrap.sh
#
# ── CA KEY CUSTODY ────────────────────────────────────────────────────────────────────
# $PKI_DIR/ca/cluster-ca.key is the cluster's trust root and NEVER enters the cluster: only
# cluster-ca.pem goes into the Secret. Anything that can read the key can mint a leaf with
# any CN, and the bus binds node identity to exactly that CN — so it could claim any node's
# identity and forge that node's gossip signatures. Keep $PKI_DIR (or move the key to
# offline storage); you need it again only to add nodes.
#
# RESIDUAL, on this starter path: one Secret holds every node's leaf key, and Kubernetes
# gives a pod the whole Secret — so every broker pod can read every OTHER node's peer key.
# The CN binding therefore stops an outsider, not a compromised pod. Per-pod key isolation
# needs a per-pod issuer; see CERT-MANAGER below.
#
# ── RENEWAL ───────────────────────────────────────────────────────────────────────────
# Leaves live LEAF_DAYS (365 by default) and a re-run KEEPS whatever verifies, so renewal
# is an explicit act: delete the leaf files, re-run, roll the pods (rotation is NOT hot —
# the gossip signer reads the key once at startup; see docs/OPERATIONS.md).
#
#   rm mqttd-pki/nodes/<release>-*.pem mqttd-pki/nodes/<release>-*.key
#   REPLICAS=<n> ./bootstrap.sh           # mints fresh leaves under the SAME CA
#   kubectl -n <ns> rollout restart statefulset/<release>
#
# An EXPIRED leaf fails re-verification ("does not verify"), which is this same procedure,
# not an openssl problem. The CA lives CA_DAYS (3650); renewing IT invalidates every leaf,
# so delete the whole $PKI_DIR and re-run, then roll.
#
# ── SCALING ───────────────────────────────────────────────────────────────────────────
# A new ordinal is a new node id and a new DNS name, so it needs its OWN leaf: re-run with
# the new size BEFORE the upgrade, then raise replicaCount.
#
#   REPLICAS=5 ./bootstrap.sh --mint-only   # mints ordinals 3 and 4, keeps 0-2 and the CA
#   REPLICAS=5 ./bootstrap.sh               # ... and re-applies the Secret
#   helm upgrade <release> deploy/helm/mqttd --reuse-values --set replicaCount=5
#
# Existing leaves and the CA are REUSED (re-verified, not re-minted), so this neither
# re-roots the cluster nor rotates the gossip key under running pods. A pod whose ordinal
# has no leaf fails its init container with a message naming this command rather than
# crash-looping on a missing file. Scale-DOWN needs nothing: unused leaves are inert.
#
# ── CERT-MANAGER (the production path) ────────────────────────────────────────────────
# Nothing here is required — the chart needs EITHER a Secret with ca.crt plus <pod>.crt /
# <pod>.key per pod (this script's output), OR a per-pod directory via secrets.peerTls.dir
# + extraVolumes (the cert-manager csi-driver path — wired in values.yaml, see the
# annotated extraVolumes example there). csi is the one option that satisfies all four
# rules AND keeps each pod to its own key (minted into an ephemeral volume, never a
# shared Secret):
#   csi.cert-manager.io/common-name: "${POD_NAME}"        # rule 1
#   csi.cert-manager.io/dns-names: "${POD_NAME}.<headless>.${POD_NAMESPACE}.svc.cluster.local"
#   csi.cert-manager.io/key-usages: "server auth,client auth"   # rule 3
#   csi.cert-manager.io/key-algorithm: ECDSA                    # rule 4
#   csi.cert-manager.io/key-encoding: PKCS8                     # rule 4
# A single cert-manager Certificate with all pods' DNS names in it does NOT work here: it
# has one CN, and rule 1 is per-pod. (That shape is what RabbitMQ's inter-node example
# uses; Erlang distribution does not bind identity to the CN, and this bus does.)
set -euo pipefail

INVOCATION="$*"   # echoed back in the remedy, so the suggested re-run is THIS run
NS="${NS:-mqttd}"
RELEASE="${RELEASE:-mqttd}"
REPLICAS="${REPLICAS:-3}"
IMAGE="${MQTTD_IMAGE:-ghcr.io/mbilling/fss-mqtt-broker:latest}"
PKI_DIR="${PKI_DIR:-./mqttd-pki}"
CA_DAYS="${CA_DAYS:-3650}"
LEAF_DAYS="${LEAF_DAYS:-365}"
CURVE=prime256v1                 # NIST P-256; see rule 4
OPENSSL="${OPENSSL:-openssl}"    # override to point at a conforming build; see above

MINT_ONLY=0
USERS=()
for arg in "$@"; do
  case "$arg" in
    --mint-only) MINT_ONLY=1 ;;
    -h|--help) awk 'NR>1 && /^#/ {sub(/^# ?/,""); print; next} NR>1 {exit}' "$0"; exit 0 ;;
    -*) echo "FATAL: unknown flag $arg" >&2; exit 2 ;;
    *) USERS+=("$arg") ;;
  esac
done

# The chart's own fullname rule (templates/_helpers.tpl): the release name alone when it
# already contains the chart name, else "<release>-mqttd". Pod names — and therefore every
# CN and SAN below — derive from it, so a wrong guess here reproduces issue #262 exactly:
# certificates that verify, for identities no pod ever claims. Override FULLNAME if you
# install with nameOverride/fullnameOverride, and CHECK the printed list against
# `kubectl get pods`.
if [[ -z "${FULLNAME:-}" ]]; then
  if [[ "$RELEASE" == *mqttd* ]]; then FULLNAME="$RELEASE"; else FULLNAME="$RELEASE-mqttd"; fi
fi
HEADLESS_DOMAIN="$FULLNAME-headless.$NS.svc.cluster.local"

REQUIRED_TOOLS=("$OPENSSL")
[[ $MINT_ONLY -eq 1 ]] || REQUIRED_TOOLS+=(kubectl)
for tool in "${REQUIRED_TOOLS[@]}"; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: need $tool on PATH" >&2; exit 2; }
done
[[ "$REPLICAS" =~ ^[0-9]+$ ]] && [[ "$REPLICAS" -ge 1 ]] \
  || { echo "FATAL: REPLICAS must be a positive integer (got '$REPLICAS')" >&2; exit 2; }

# The broker binary hashes passwords (--hash-password); reuse a local build or the image,
# exactly as the compose bootstrap does, so a host with no Rust toolchain still works.
if [[ -n "${MQTTD_BIN:-}" ]]; then
  hash_password() { printf %s "$2" | "$MQTTD_BIN" --hash-password "$1"; }
elif command -v docker >/dev/null 2>&1; then
  hash_password() { printf %s "$2" | docker run --rm -i "$IMAGE" --hash-password "$1"; }
else
  hash_password() { echo "FATAL: need MQTTD_BIN or docker to hash passwords" >&2; exit 2; }
fi

CA_DIR="$PKI_DIR/ca"
NODES_DIR="$PKI_DIR/nodes"
SERVER_DIR="$PKI_DIR/server"
GOSSIP_DIR="$PKI_DIR/gossip"

# Everything is minted here first and moved into place only after it verifies.
STAGE=''
WORK="$(mktemp -d)"
cleanup() { rm -rf "$WORK"; [ -n "$STAGE" ] && rm -rf "$STAGE"; return 0; }
trap cleanup EXIT
trap 'cleanup; exit 130' INT
trap 'cleanup; exit 143' TERM

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

check_key() { # <key file> — rule 4, on the key itself
  local key="$1" txt
  head -n 1 "$key" 2>/dev/null | grep -q -- '-----BEGIN PRIVATE KEY-----' \
    || bad "$(basename "$key") is not an unencrypted PKCS#8 key (no \"BEGIN PRIVATE KEY\" header): the ADR 0022 gossip signer reads PKCS#8 only, and a SEC1 \"BEGIN EC PRIVATE KEY\" file gives a WORKING TLS handshake and then a startup failure"
  txt="$("$OPENSSL" pkey -in "$key" -noout -text 2>/dev/null || true)"
  case "$txt" in
    *"ASN1 OID: $CURVE"*) ;;
    *'rsaEncryption'*|*'RSA Private-Key'*|*'Private-Key: ('*'bit, 2 primes)'*)
      bad "$(basename "$key") is an RSA key, and the peer-bus key doubles as the per-node GOSSIP SIGNING key, which accepts only ECDSA P-256/P-384 or Ed25519 (rule 4). RSA gives a working TLS handshake and then a hard startup failure — this is exactly the defect in issue #262" ;;
    *) bad "$(basename "$key") is not a named-curve $CURVE key (rule 4)" ;;
  esac
}

check_pair() { # <cert> <key> — the key actually belongs to the certificate
  local cpub kpub
  cpub="$("$OPENSSL" x509 -in "$1" -noout -pubkey 2>/dev/null || true)"
  kpub="$("$OPENSSL" pkey -in "$2" -pubout 2>/dev/null || true)"
  [[ -n "$cpub" && "$cpub" == "$kpub" ]] \
    || bad "$(basename "$2") is not the private key of $(basename "$1")"
}

check_cert_core() { # <cert> <expected CN>
  local crt="$1" want_cn="$2" txt got_cn
  txt="$(cert_text "$crt")"
  [[ -n "$txt" ]] || { bad "$(basename "$crt") is not a readable X.509 certificate"; return 0; }
  case "$txt" in
    *"ASN1 OID: $CURVE"*) ;;
    *'rsaEncryption'*)
      bad "$(basename "$crt") carries an RSA public key; the peer-bus leaf must be ECDSA $CURVE because its key is also the gossip signing key (rule 4, issue #262)" ;;
    *) bad "$(basename "$crt") does not carry a NAMED-CURVE $CURVE public key — explicit ECC parameters are what OpenSSL 3 rejects with 'Certificate public key has explicit ECC parameters', and rustls cannot verify either" ;;
  esac
  case "$txt" in
    *'Signature Algorithm: ecdsa-with-SHA256'*) ;;
    *) bad "$(basename "$crt") is not signed ecdsa-with-SHA256 (SHA-1 signatures are rejected by modern verifiers)" ;;
  esac
  got_cn="$(cert_cn "$crt")"
  [[ "$got_cn" == "$want_cn" ]] \
    || bad "$(basename "$crt") has Subject CN '$got_cn', not '$want_cn' (rule 1)"
}

check_ca() { # <ca cert> <ca key>
  local ca_crt="$1" ca_key="$2"
  [[ -s "$ca_crt" ]] || bad "$ca_crt is missing or empty"
  [[ -s "$ca_key" ]] || bad "$ca_key is missing or empty"
  [[ -s "$ca_crt" && -s "$ca_key" ]] || return 0
  check_cert_core "$ca_crt" mqttd-cluster-ca
  case "$(cert_text "$ca_crt")" in
    *'CA:TRUE'*) ;;
    *) bad "$(basename "$ca_crt") is not a CA certificate (no basicConstraints CA:TRUE) — nothing it signs will verify" ;;
  esac
  "$OPENSSL" verify -CAfile "$ca_crt" "$ca_crt" >/dev/null 2>&1 \
    || bad "$(basename "$ca_crt") does not verify against itself (\`openssl verify\` refuses the trust root)"
  check_key "$ca_key"
  check_pair "$ca_crt" "$ca_key"
}

check_leaf() { # <cert> <key> <CN> <SAN list> <EKU list>
  local lf="$1" lk="$2" lcn="$3" lsans="$4" lekus="$5" txt s
  [[ -s "$lf" && -s "$lk" ]] || { bad "$lf / $lk were not produced"; return 0; }
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
    case "$txt" in
      *"$(san_printed_form "$s")"*) ;;
      *) bad "$(basename "$lf") has no $s SAN — a dialing peer verifies the pod's advertise host against it (rule 2)" ;;
    esac
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
  "$OPENSSL" verify -CAfile "$CA_DIR/cluster-ca.pem" "$lf" >/dev/null 2>&1 \
    || bad "$(basename "$lf") does not verify against $CA_DIR/cluster-ca.pem"
}

# The properties, READ BACK out of the file — this is what gets printed, so the banner
# cannot drift from the artifact.
cert_summary() { # <cert>
  local txt s_san s_eku s_alg s_crv
  txt="$(cert_text "$1")"
  s_san="$(printf '%s\n' "$txt" | grep -A1 'Subject Alternative Name' | tail -1 | sed 's/^ *//')"
  s_eku="$(printf '%s\n' "$txt" | grep -A1 'Extended Key Usage' | tail -1 | sed 's/^ *//')"
  s_alg="$(printf '%s\n' "$txt" | sed -n 's/^ *Signature Algorithm: *//p' | head -1)"
  s_crv="$(printf '%s\n' "$txt" | sed -n 's/^ *NIST CURVE: *//p' | head -1)"
  printf 'CN=%s' "$(cert_cn "$1")"
  [[ -z "$s_san" ]] || printf ', SAN %s' "$s_san"   # the CA has neither, and printing
  [[ -z "$s_eku" ]] || printf ', EKU %s' "$s_eku"   # "SAN , EKU ," would claim nothing
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
    echo "       Nothing was installed by this run."
  } >&2
  exit 1
}

die_unusable() { # <what> <remedy-path>
  {
    echo "FATAL: $1 already exists under $PKI_DIR, and it is NOT usable:"
    echo "$BAD" | sed '/^$/d'
    echo
    echo "       This is refused rather than reused: certificates minted under it would"
    echo "       fail the same way at runtime, and the brokers would start, log \"SWIM"
    echo "       gossip is SIGNED per-node\", and never form a mesh."
    echo "       If a reason above is 'does not verify', check the dates first — an EXPIRED"
    echo "       leaf fails verification too, and the remedy for that is simply renewal"
    echo "       (delete the named files, re-run, roll the pods; see RENEWAL in --help),"
    echo "       not a different openssl."
    echo "       REMEDY — delete it and mint again with an OpenSSL 3 build:"
    echo "         rm -rf $2"
    echo "         OPENSSL=\"\$(brew --prefix openssl@3)/bin/openssl\" $0"
    if [[ "$2" != "$PKI_DIR" ]]; then
      echo "       If the CA itself is the problem, delete the whole $PKI_DIR: every leaf"
      echo "       under it must be re-minted too."
    fi
  } >&2
  exit 1
}

# ── minting ───────────────────────────────────────────────────────────────────────────
# openssl chatters on stderr even when it succeeds ("Signature ok", "Getting CA Private
# Key"), and that noise buries the verified summaries this script exists to print. So each
# invocation is quiet on success and DUMPS ITS OUTPUT ON FAILURE — never silently discarded,
# which would hide the one message that explains a broken mint.
ssl() {
  "$OPENSSL" "$@" >"$WORK/openssl.log" 2>&1 || {
    echo "FATAL: '$OPENSSL $1' failed:" >&2
    sed 's/^/  /' "$WORK/openssl.log" >&2
    exit 1
  }
}

# `ecparam -param_enc named_curve` + `pkcs8 -topk8`, rather than `req -newkey ec -pkeyopt`:
# LibreSSL ignores that -pkeyopt and hands back a key with explicit ECC parameters, which
# OpenSSL 3 and rustls both reject. This shape is verified on both implementations.
mint_key() { # <dest .key>
  ssl ecparam -name "$CURVE" -genkey -noout -param_enc named_curve -out "$STAGE/key.sec1"
  ssl pkcs8 -topk8 -nocrypt -in "$STAGE/key.sec1" -out "$1"
  rm -f "$STAGE/key.sec1"
}

mint_leaf() { # <out-basename> <CN> <EKU list> <SAN list>
  local leaf="$1" cn="$2" eku="$3" sans="$4"
  # SKI/AKI: webpki/rustls ignore them (paths are built by DN), but RFC 5280 requires
  # them and `openssl verify -x509_strict` fails without them — and external tooling an
  # operator points at this PKI may be strict.
  printf 'subjectAltName=%s\nextendedKeyUsage=%s\nbasicConstraints=critical,CA:FALSE\nsubjectKeyIdentifier=hash\nauthorityKeyIdentifier=keyid,issuer\n' \
    "$sans" "$eku" > "$STAGE/leaf.ext"
  printf '[req]\ndistinguished_name=dn\nprompt=no\n[dn]\nCN=%s\n' "$cn" > "$STAGE/leaf.cnf"
  mint_key "$leaf.key"
  # -sha256 explicitly: LibreSSL's default signing digest here is SHA-1.
  ssl req -new -key "$leaf.key" -sha256 -config "$STAGE/leaf.cnf" \
    -subj "/CN=$cn" -out "$STAGE/leaf.csr"
  ssl x509 -req -in "$STAGE/leaf.csr" -CA "$CA_DIR/cluster-ca.pem" \
    -CAkey "$CA_DIR/cluster-ca.key" -CAcreateserial -days "$LEAF_DAYS" -sha256 \
    -out "$leaf.pem" -extfile "$STAGE/leaf.ext"
  rm -f "$STAGE/leaf.csr" "$STAGE/leaf.ext" "$STAGE/leaf.cnf" "$CA_DIR/cluster-ca.srl"
  chmod 0600 "$leaf.key"
  chmod 0644 "$leaf.pem"
}

echo "== cluster PKI under $PKI_DIR =="

# --- the cluster CA: minted ONCE, re-verified on every reuse -----------------------------
mkdir -p "$CA_DIR"
if [[ -e "$CA_DIR/cluster-ca.pem" || -e "$CA_DIR/cluster-ca.key" ]]; then
  # VALIDATE, do not stat. A CA that exists but is unusable is the case that matters: a
  # mint this script had itself rejected must not be blessed by the next run, with every
  # leaf then signed under it.
  BAD=''
  check_ca "$CA_DIR/cluster-ca.pem" "$CA_DIR/cluster-ca.key"
  [[ -z "$BAD" ]] || die_unusable "a cluster CA" "$PKI_DIR"
  echo "  CA: keeping $CA_DIR/cluster-ca.pem — RE-VERIFIED just now, not assumed"
  echo "      $(cert_summary "$CA_DIR/cluster-ca.pem"), CA:TRUE, verifies against itself"
else
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
subjectKeyIdentifier=hash
EOF
  mint_key "$STAGE/cluster-ca.key"
  ssl req -x509 -new -key "$STAGE/cluster-ca.key" -days "$CA_DAYS" -sha256 \
    -config "$STAGE/ca.cnf" -extensions v3_ca -subj '/CN=mqttd-cluster-ca' \
    -out "$STAGE/cluster-ca.pem"
  BAD=''
  check_ca "$STAGE/cluster-ca.pem" "$STAGE/cluster-ca.key"
  [[ -z "$BAD" ]] || die_nonconforming "cluster CA"
  chmod 0400 "$STAGE/cluster-ca.key"; chmod 0644 "$STAGE/cluster-ca.pem"
  mv "$STAGE/cluster-ca.key" "$CA_DIR/cluster-ca.key"
  mv "$STAGE/cluster-ca.pem" "$CA_DIR/cluster-ca.pem"
  rm -rf "$STAGE"; STAGE=''
  echo "  CA: minted $CA_DIR/cluster-ca.pem (once, for ALL nodes) — verified before install"
  echo "      $(cert_summary "$CA_DIR/cluster-ca.pem"), CA:TRUE, verifies against itself"
fi
echo "      $CA_DIR/cluster-ca.key is the PRIVATE trust root: it is NOT copied into the"
echo "      cluster, and must not be. Keep it here or move it offline."

# --- one leaf per node: CN = pod name, SAN = that pod's advertise host --------------------
STAGE="$(mktemp -d)"
mkdir -p "$NODES_DIR"
PEER_FILES=()
MINTED=() ; KEPT=()
for ((i = 0; i < REPLICAS; i++)); do
  node="$FULLNAME-$i"
  sans="DNS:$node.$HEADLESS_DOMAIN"
  ekus='serverAuth,clientAuth'
  if [[ -s "$NODES_DIR/$node.pem" || -s "$NODES_DIR/$node.key" ]]; then
    # Reuse is re-verification: an existing leaf must still satisfy all four rules for the
    # identity and DNS name it is being reused FOR (NS/RELEASE may have changed since).
    BAD=''
    check_leaf "$NODES_DIR/$node.pem" "$NODES_DIR/$node.key" "$node" "$sans" "$ekus"
    [[ -z "$BAD" ]] || die_unusable "the leaf for $node" "$NODES_DIR/$node.pem $NODES_DIR/$node.key"
    KEPT+=("$node")
  else
    mint_leaf "$STAGE/$node" "$node" "$ekus" "$sans"
    BAD=''
    # Verify at the staged path, then install: a rejected leaf leaves nothing behind.
    mv "$STAGE/$node.pem" "$NODES_DIR/$node.pem"
    mv "$STAGE/$node.key" "$NODES_DIR/$node.key"
    check_leaf "$NODES_DIR/$node.pem" "$NODES_DIR/$node.key" "$node" "$sans" "$ekus"
    [[ -z "$BAD" ]] || { rm -f "$NODES_DIR/$node.pem" "$NODES_DIR/$node.key"
      die_nonconforming "peer leaf for $node"; }
    MINTED+=("$node")
  fi
  PEER_FILES+=("--from-file=$node.crt=$NODES_DIR/$node.pem"
               "--from-file=$node.key=$NODES_DIR/$node.key")
done

# Leaves BEYOND the requested count stay in the Secret: `kubectl create --dry-run | apply`
# REPLACES the object, so omitting them would prune a still-running pod's leaf on a re-run
# with a smaller REPLICAS — contradicting the header's "scale-down needs nothing; unused
# leaves are inert". They are kept as long as the files exist under $NODES_DIR.
EXTRA_KEPT=()
for pem in "$NODES_DIR/$FULLNAME"-*.pem; do
  [[ -e "$pem" ]] || continue
  node="$(basename "$pem" .pem)"
  ord="${node##*-}"
  [[ "$ord" =~ ^[0-9]+$ ]] && (( ord >= REPLICAS )) || continue
  [[ -s "$NODES_DIR/$node.key" ]] || continue
  PEER_FILES+=("--from-file=$node.crt=$NODES_DIR/$node.pem"
               "--from-file=$node.key=$NODES_DIR/$node.key")
  EXTRA_KEPT+=("$node")
done
if (( ${#EXTRA_KEPT[@]} )); then
  echo "  kept ${#EXTRA_KEPT[@]} leaf/leaves beyond REPLICAS=$REPLICAS in the Secret (${EXTRA_KEPT[*]}):"
  echo "  a re-apply REPLACES the Secret, and a still-running higher-ordinal pod must not lose its leaf."
fi

echo "  peer leaves for $REPLICAS node(s) — every property below read back from the file:"
for ((i = 0; i < REPLICAS; i++)); do
  node="$FULLNAME-$i"
  echo "    $node.crt/.key  $(cert_summary "$NODES_DIR/$node.pem")"
done
[[ ${#KEPT[@]} -eq 0 ]] || echo "    (kept + re-verified: ${KEPT[*]})"
[[ ${#MINTED[@]} -eq 0 ]] || echo "    (newly minted: ${MINTED[*]})"
echo "    CHECK THIS LIST against \`kubectl -n $NS get pods\`: the CN must be the POD NAME."
echo "    A leaf for an identity no pod claims is issue #262 all over again."

# --- the client-facing server keypair (serverAuth only) ----------------------------------
mkdir -p "$SERVER_DIR"
server_cn="$FULLNAME.$NS.svc"
server_sans="DNS:$FULLNAME.$NS.svc,DNS:$FULLNAME.$NS.svc.cluster.local,DNS:localhost,IP:127.0.0.1"
if [[ -s "$SERVER_DIR/server.pem" || -s "$SERVER_DIR/server.key" ]]; then
  BAD=''
  check_leaf "$SERVER_DIR/server.pem" "$SERVER_DIR/server.key" "$server_cn" "$server_sans" 'serverAuth'
  [[ -z "$BAD" ]] || die_unusable "the server keypair" "$SERVER_DIR"
  echo "  server keypair: kept + re-verified"
else
  mint_leaf "$STAGE/server" "$server_cn" 'serverAuth' "$server_sans"
  mv "$STAGE/server.pem" "$SERVER_DIR/server.pem"
  mv "$STAGE/server.key" "$SERVER_DIR/server.key"
  BAD=''
  check_leaf "$SERVER_DIR/server.pem" "$SERVER_DIR/server.key" "$server_cn" "$server_sans" 'serverAuth'
  [[ -z "$BAD" ]] || { rm -f "$SERVER_DIR/server.pem" "$SERVER_DIR/server.key"
    die_nonconforming "server keypair"; }
  echo "  server keypair: minted"
fi
echo "    $(cert_summary "$SERVER_DIR/server.pem")"

# --- gossip key (ADR 0003) — persisted, never silently rotated ---------------------------
mkdir -p "$GOSSIP_DIR"
if [[ -s "$GOSSIP_DIR/swim-key" ]] && grep -Eq '^[0-9a-f]{64}$' "$GOSSIP_DIR/swim-key"; then
  echo "  gossip key: keeping $GOSSIP_DIR/swim-key (re-minting it mid-life partitions a"
  echo "              running cluster: nodes with different keys cannot gossip)"
else
  [[ -e "$GOSSIP_DIR/swim-key" ]] && { echo "FATAL: $GOSSIP_DIR/swim-key is not a 64-hex key; delete it to mint a new one" >&2; exit 1; }
  # `num` is positional and must come LAST (openssl rand [options] num).
  ssl rand -hex -out "$GOSSIP_DIR/swim-key" 32
  grep -Eq '^[0-9a-f]{64}$' "$GOSSIP_DIR/swim-key" \
    || { echo "FATAL: $OPENSSL did not produce a 64-hex gossip key" >&2; exit 1; }
  chmod 0600 "$GOSSIP_DIR/swim-key"
  echo "  gossip key: minted (64 hex, verified)"
fi

rm -rf "$STAGE"; STAGE=''

# --- a starter deny-by-default ACL ------------------------------------------------------
cat > "$WORK/acl.toml" <<'ACL'
# Starter ACL — deny by default, edit before production (ADR 0004).
default = "deny"

# Example: a device may write its own telemetry and read its own commands.
# [[rules]]
# subject = "sensor-1"
# allow = ["write:telemetry/sensor-1/#", "read:commands/sensor-1/#"]
ACL

if [[ $MINT_ONLY -eq 1 ]]; then
  cat <<EOF

MINT ONLY — verified the PKI above and touched no cluster. Re-run without --mint-only
to create the Secrets in namespace $NS.
EOF
  exit 0
fi

echo
echo "== namespace $NS =="
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f - >/dev/null

kubectl create secret generic mqttd-gossip -n "$NS" \
  --from-file=swim-key="$GOSSIP_DIR/swim-key" --dry-run=client -o yaml \
  | kubectl apply -f - >/dev/null
echo "  Secret mqttd-gossip (swim-key)"

kubectl create secret tls mqttd-tls -n "$NS" \
  --cert="$SERVER_DIR/server.pem" --key="$SERVER_DIR/server.key" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  Secret mqttd-tls (server keypair)"

# ca.crt plus ONE LEAF PER POD, keyed by pod name. The chart points each pod at its own by
# path ($(POD_NAME).crt / .key) — which is what makes a per-node CN possible at all, since a
# StatefulSet pod template cannot select a different Secret per ordinal. The CA PRIVATE key
# is deliberately absent.
kubectl create secret generic mqttd-peer-tls -n "$NS" \
  --from-file=ca.crt="$CA_DIR/cluster-ca.pem" "${PEER_FILES[@]}" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  Secret mqttd-peer-tls (ca.crt + $REPLICAS per-node leaves, keyed by pod name)"

kubectl create configmap mqttd-acl -n "$NS" --from-file=acl.toml="$WORK/acl.toml" \
  --dry-run=client -o yaml | kubectl apply -f - >/dev/null
echo "  ConfigMap mqttd-acl (deny-by-default starter)"

# --- passwords (optional) ---------------------------------------------------------------
PW_FLAGS=""
if [[ ${#USERS[@]} -gt 0 ]]; then
  : > "$WORK/passwd"
  echo "  generated passwords (save these, then delete):"
  for u in "${USERS[@]}"; do
    pw="$($OPENSSL rand -base64 18)"
    echo "$u:$(hash_password "$u" "$pw")" >> "$WORK/passwd"
    echo "    $u  $pw"
  done
  kubectl create secret generic mqttd-passwd -n "$NS" --from-file=passwd="$WORK/passwd" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
  echo "  Secret mqttd-passwd (Argon2id lines)"
  PW_FLAGS='  # password file: mount mqttd-passwd and set MQTTD_PASSWORD_FILE via extraEnv'
fi

cat <<EOF

Done. Install the chart wired to these:

  helm install $RELEASE deploy/helm/mqttd -n $NS \\
    --set replicaCount=$REPLICAS \\
    --set secrets.tls.secretName=mqttd-tls \\
    --set secrets.acl.configMapName=mqttd-acl \\
    --set secrets.peerTls.secretName=mqttd-peer-tls \\
    --set secrets.gossipKey.secretName=mqttd-gossip
$PW_FLAGS
Setting those two cluster-bus names is ALL that is needed: the chart derives
MQTTD_PEER_TLS_CA/CERT/KEY (each pod's own leaf, by pod name) and MQTTD_SWIM_KEY_FILE
from them itself, so a mounted secret cannot sit unread. Signed per-node gossip then
arms automatically — expect "SWIM gossip is SIGNED per-node" in every pod's log, and
no "does not match its certificate Common Name".

replicaCount MUST NOT exceed $REPLICAS here: ordinals above that have no leaf, and
their init container will say so. To grow, mint first — REPLICAS=<n> $0 — then upgrade.

First install only: leave clusterEstablished=false (the default). Turn it on once the
cluster has formed:  helm upgrade $RELEASE deploy/helm/mqttd --reuse-values --set clusterEstablished=true

THROWAWAY PKI: $CA_DIR/cluster-ca.key is this cluster's trust root. Keep $PKI_DIR (you
need it to add nodes), keep the key off every broker host, and replace the whole thing
with cert-manager for production — see CERT-MANAGER at the top of this script.
EOF
