#!/usr/bin/env bash
# The non-Kubernetes reference deployment, as a test (ADR 0047 T9).
#
# `deploy/compose/` and `deploy/systemd/` are the supported way to run mqttd off
# Kubernetes. An untested deployment artifact is a promise, not a deliverable — and the
# specific way these rot is silent: a renamed environment variable, a seed list that no
# longer forms a cluster, a readiness floor that lets a minority node serve traffic. None
# of that shows up in a unit test.
#
# So this boots a real THREE-NODE cluster using the values from the SHIPPED files —
# deploy/systemd/mqttd.env.example is parsed, not re-typed here — and proves:
#
#   1. authentication is actually on (anonymous is refused);
#   2. the generated password file authenticates (mqttd --hash-password end to end);
#   3. the ACL is enforced (a device cannot read another device's topic);
#   4. the cluster forms and routes ACROSS nodes;
#   5. an acknowledged QoS 1 message survives the loss of the node that accepted it;
#   6. a minority node reports NOT ready (so a load balancer drops it).
#
# Every client connection here is over TLS and the cluster bus is mutually authenticated,
# because that is what the shipped artifacts now configure (issue #254). A test that ran
# the plaintext path would be testing a configuration nobody is given. So it additionally
# proves the security posture the READMEs claim:
#
#   7. neither artifact ships a plaintext listener, and compose.yaml sets the TLS and
#      cluster-bus variables (a grep on the artifacts, so drift fails here);
#   8. no node logs INSECURE — which covers a plaintext client listener AND a plaintext
#      peer bus in one assertion — and every node logs signed per-node gossip;
#   9. a cleartext client is REFUSED (there is no plaintext listener to fall back to);
#  10. plaintext comes back only through the explicit overlay, on loopback.
#
# BOTH shipped PKI recipes are CALLED here rather than restated, for the same reason the env
# file is parsed: a shipped recipe this test does not use is a shipped recipe nothing checks.
#
#   - deploy/compose/init.sh mints the three-node cluster's PKI below.
#   - deploy/systemd/gen-certs.sh — what the systemd README and env file tell an operator to
#     run — mints ONE cluster CA and two per-node leaf sets, and TWO MORE NODES ARE BOOTED
#     from that output and made to route to each other (section 1c). Two nodes under one CA
#     is the assertion that catches a per-host CA, which forms no mesh at all; and the pair
#     coming up at all is what proves the ECDSA-not-RSA rule, since the per-node gossip
#     signer refuses anything else and the process then never starts.
#
# So the four peer-certificate constraints that are easy to get wrong — CN equals the node
# id, the SAN covers the MQTTD_PEER_ADVERTISE host, both EKUs, an ECDSA/Ed25519 key — are
# exercised, not merely documented: get any of them wrong and a cross-node assertion fails,
# because the peer links get dropped.
#
# The deviations from the shipped file are mechanical, not semantic, and are exactly the
# five edits it tells operators to make (identity, seeds, readiness floor, secret paths,
# TLS paths) plus ephemeral ports so this runs on a busy CI box.
#
# The compose file and the systemd unit are additionally validated with their own tools
# when those are available (`docker compose config`, `systemd-analyze verify`); both are
# SKIPPED LOUDLY rather than silently when they are not.
#
# Needs: mosquitto-clients, python3, openssl, curl. Set MQTTD_BIN to skip the build.
set -euo pipefail
cd "$(dirname "$0")/.."

# --- CI-fatal skips (issue #260) -------------------------------------------------------
# A skip that prints a note and exits 0 is indistinguishable from a pass, so coverage can
# vanish on the platform that gates merges without anything going red. Allowed locally,
# fatal under CI (GitHub Actions sets CI=true on every runner). `skip_permitted` is the one
# deliberate exception: a lane that genuinely cannot run in CI stays green and says why.
skip_or_fail() {
  if [ "${CI:-}" = "true" ]; then
    echo "FATAL: environmental skip taken under CI — coverage would silently vanish: $1" >&2
    exit 1
  fi
  echo "  SKIP (local only; fatal under CI) — $1"
}
skip_permitted() { echo "  SKIP (permitted in CI by design) — $1"; }

# openssl: the shipped init.sh mints the PKI with it. The interop job already installs it
# (scripts/quickstart-smoke.sh requires it too), so this is not a new CI dependency.
for tool in mosquitto_pub mosquitto_sub python3 openssl; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

MQTTD_BIN="${MQTTD_BIN:-}"
if [[ -z "$MQTTD_BIN" ]]; then
  echo "building mqttd (set MQTTD_BIN to reuse an existing build)…"
  cargo build --quiet -p mqttd
  MQTTD_BIN="target/debug/mqttd"
fi
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN"; exit 2; }
MQTTD_BIN="$(cd "$(dirname "$MQTTD_BIN")" && pwd)/$(basename "$MQTTD_BIN")"

ENV_FILE="deploy/systemd/mqttd.env.example"
COMPOSE_FILE="deploy/compose/compose.yaml"
PLAINTEXT_FILE="deploy/compose/compose.plaintext.yaml"
ACL_FILE="deploy/compose/acl.toml"
INIT_SCRIPT="deploy/compose/init.sh"
GEN_CERTS="deploy/systemd/gen-certs.sh"
for f in "$ENV_FILE" "$COMPOSE_FILE" "$PLAINTEXT_FILE" "$ACL_FILE" "$INIT_SCRIPT" "$GEN_CERTS"; do
  [[ -r "$f" ]] || { echo "FATAL: missing deployment artifact: $f"; exit 2; }
done

WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }

# ─────────────────────────────────────────────────────────────────────────────────
# 0. The shipped env file is the source of truth for everything except the four
#    per-node values it tells you to edit. Read it rather than restating it, so a
#    setting renamed in the artifact and not here fails loudly.
# ─────────────────────────────────────────────────────────────────────────────────
shipped() { # <VAR> -> its uncommented value in the example env file, or ""
  sed -n "s/^$1=//p" "$ENV_FILE" | tail -1
}

# MQTTD_TLS_BIND and the cluster-bus trio are in this list, not MQTTD_PLAINTEXT_BIND:
# the shipped env file enables TLS by default (issue #254), so an edit that commented the
# TLS lines back out — or uncommented the plaintext one — must fail here.
for required in MQTTD_DATA_DIR MQTTD_PASSWORD_FILE MQTTD_ACL_FILE MQTTD_SWIM_KEY_FILE \
                MQTTD_READY_MIN_MEMBERS MQTTD_SHUTDOWN_GRACE \
                MQTTD_TLS_BIND MQTTD_TLS_CERT MQTTD_TLS_KEY \
                MQTTD_PEER_TLS_CA MQTTD_PEER_TLS_CERT MQTTD_PEER_TLS_KEY \
                MQTTD_HEALTH_BIND MQTTD_PEER_BIND MQTTD_SWIM_BIND; do
  [[ -n "$(shipped "$required")" ]] \
    || fail "$ENV_FILE no longer sets $required — the reference deployment and this test have drifted"
done
pass "$ENV_FILE sets every variable the reference deployment needs, TLS included"

# The example must NOT enable anonymous access; a reference deployment that did would
# teach the wrong thing to everyone who copies it.
grep -qE '^MQTTD_ALLOW_ANONYMOUS' "$ENV_FILE" \
  && fail "$ENV_FILE enables anonymous access"
grep -qE '^\s*MQTTD_ALLOW_ANONYMOUS' "$COMPOSE_FILE" \
  && fail "$COMPOSE_FILE enables anonymous access"
pass "neither artifact enables anonymous access"

# ── The plaintext posture, as a property of the ARTIFACTS ────────────────────────
# This is the assertion issue #254 exists for. The README claims the reference deployments
# are TLS by default; the only thing that can make that claim durable is a check on the
# files themselves, because a reader copies the FILE, not the test.
grep -qE '^[[:space:]]*MQTTD_PLAINTEXT_BIND' "$COMPOSE_FILE" \
  && fail "$COMPOSE_FILE ships a plaintext listener — the compose reference deployment must not. \
Plaintext lives in $PLAINTEXT_FILE, behind an explicit -f overlay"
grep -qE '^MQTTD_PLAINTEXT_BIND' "$ENV_FILE" \
  && fail "$ENV_FILE enables a plaintext listener by default — it must be commented and labelled"
for required in MQTTD_TLS_BIND MQTTD_TLS_CERT MQTTD_TLS_KEY \
                MQTTD_PEER_TLS_CA MQTTD_PEER_TLS_CERT MQTTD_PEER_TLS_KEY; do
  grep -qE "^[[:space:]]*$required:" "$COMPOSE_FILE" \
    || fail "$COMPOSE_FILE does not set $required — the compose reference deployment must be TLS by default"
done
grep -qE '^[[:space:]]*MQTTD_PLAINTEXT_BIND' "$PLAINTEXT_FILE" \
  || fail "$PLAINTEXT_FILE does not set MQTTD_PLAINTEXT_BIND — the opt-in overlay opts into nothing"
pass "neither artifact ships a plaintext listener; both configure TLS + a mutually-authenticated bus"

# ─────────────────────────────────────────────────────────────────────────────────
# 1. Secrets, made the way bootstrap.sh makes them — with the broker's own hasher.
# ─────────────────────────────────────────────────────────────────────────────────
# bootstrap.sh's half: the gossip key and an Argon2id password file, in the layout
# init.sh expects to find them (it is the compose `init` one-shot's preflight).
BOOT="$WORK/in"
mkdir -p "$BOOT"
python3 -c "import secrets;print(secrets.token_hex(32))" > "$BOOT/mqttd-swim-key"

DEVICE_A_PW='device-a-secret'
DEVICE_B_PW='device-b-secret'
BACKEND_PW='backend-secret'
: > "$BOOT/mqttd-passwd"
printf %s "$DEVICE_A_PW" | "$MQTTD_BIN" --hash-password device-a >> "$BOOT/mqttd-passwd"
printf %s "$DEVICE_B_PW" | "$MQTTD_BIN" --hash-password device-b >> "$BOOT/mqttd-passwd"
printf %s "$BACKEND_PW"  | "$MQTTD_BIN" --hash-password backend  >> "$BOOT/mqttd-passwd"
[[ $(wc -l < "$BOOT/mqttd-passwd") -eq 3 ]] || fail "the password file should have three lines"
pass "mqttd --hash-password produced a three-user password file"

# ─────────────────────────────────────────────────────────────────────────────────
# 1b. The PKI, minted by the SHIPPED recipe. Not a copy of it: deploy/compose/init.sh
#     is what a reader's `docker compose up` runs, so running it here is what keeps the
#     CN==node-id and SAN-covers-advertise rules from rotting. It skips its chown/chmod
#     step when not root, which is why it works unprivileged here.
# ─────────────────────────────────────────────────────────────────────────────────
PKI="$WORK/pki"
PKI_CA="$WORK/pki-ca"
STAGED="$WORK/staged"
# One directory per node, as compose gives it one VOLUME per node, and the CA (with its
# private key) in a directory of its own that no node's directory contains. init.sh refuses
# to mint for a node whose directory is absent, so creating them here is also a check that
# the compose mount list and this list stay the same shape.
mkdir -p "$PKI_CA" "$PKI/mqttd-1" "$PKI/mqttd-2" "$PKI/mqttd-3"
CERT_DIR="$PKI" CA_DIR="$PKI_CA" SECRETS_SRC="$BOOT" SECRETS_OUT="$STAGED" \
  MQTTD_NODES='mqttd-1 mqttd-2 mqttd-3' \
  sh "$INIT_SCRIPT" >"$WORK/init.log" 2>&1 \
  || { cat "$WORK/init.log"; fail "$INIT_SCRIPT could not mint the cluster PKI"; }

[[ -s "$PKI_CA/ca.pem" && -s "$PKI_CA/ca.key" ]] \
  || { cat "$WORK/init.log"; fail "$INIT_SCRIPT did not produce a CA in $PKI_CA"; }
for n in mqttd-1 mqttd-2 mqttd-3; do
  for f in ca.pem "$n.pem" "$n.key"; do
    [[ -s "$PKI/$n/$f" ]] || fail "$INIT_SCRIPT did not produce $n/$f"
  done
  # KEY CUSTODY, asserted on the artifact — the same rule deploy/README.md states and the
  # systemd packaging is checked against below. A node's directory is a whole volume in
  # compose, mounted into that broker: the CA private key must not be in it, and neither
  # must any OTHER node's key, because all three brokers run as the same uid.
  [[ -e "$PKI/$n/ca.key" ]] \
    && fail "$INIT_SCRIPT put the CA PRIVATE key in $n's directory — that directory is a \
volume mounted into that broker, and anything that can read the CA key can mint any node's identity"
  for other in mqttd-1 mqttd-2 mqttd-3; do
    [[ "$other" == "$n" ]] && continue
    [[ -e "$PKI/$n/$other.key" ]] \
      && fail "$INIT_SCRIPT put $other's private key in $n's directory — one volume per node \
is the only boundary between three brokers that share a uid"
  done
done
# openssl's -subject printing differs per build — LibreSSL: "subject= /CN=x",
# OpenSSL 3 (Homebrew): "subject=CN=x", OpenSSL 3 (Ubuntu): "subject=CN = x" — so a literal
# substring match on "CN=x" passes on macOS and fails on the CI runner (found by exactly
# that). Compare the extracted CN VALUE instead of a spelling.
cert_cn() { openssl x509 -in "$1" -noout -subject | sed 's/.*CN *= *//; s/[ ,\/].*//'; }

# The binding the cluster bus enforces, asserted on the artifact's OUTPUT rather than
# inferred from a passing mesh: a shared leaf would form no cluster at all, and the
# failure would read as a routing bug.
for n in mqttd-1 mqttd-2 mqttd-3; do
  got_cn="$(cert_cn "$PKI/$n/$n.pem")"
  [[ "$got_cn" == "$n" ]] \
    || fail "$n.pem has CN '$got_cn'; the cluster bus requires CN=$n (the node id)"
  ext="$(openssl x509 -in "$PKI/$n/$n.pem" -noout -text)"
  [[ "$ext" == *"TLS Web Server Authentication"* && "$ext" == *"TLS Web Client Authentication"* ]] \
    || fail "$n.pem lacks serverAuth+clientAuth; every node both dials and is dialed"
  [[ "$ext" == *"IP Address:127.0.0.1"* ]] \
    || fail "$n.pem has no IP:127.0.0.1 SAN; peers dial MQTTD_PEER_ADVERTISE and would not verify it"
  openssl verify -CAfile "$PKI_CA/ca.pem" "$PKI/$n/$n.pem" >/dev/null 2>&1 \
    || fail "$n.pem does not verify against the CA in $PKI_CA — one CA for the cluster is the rule"
done
# The staged copies are what the brokers read in compose, so read them here too.
SWIM_KEY="$STAGED/mqttd-swim-key"
PW_FILE="$STAGED/mqttd-passwd"
[[ -s "$SWIM_KEY" && -s "$PW_FILE" ]] || fail "$INIT_SCRIPT did not stage the secrets"
pass "$INIT_SCRIPT minted a per-node PKI (CN=node id, SAN covers the advertise host, both \
EKUs), one CA for all three, and put no CA key and no other node's key in any node's directory"

# The tool the compose README hands the reader for the same job.
MOSQ_TLS=(--cafile "$PKI_CA/ca.pem")

# ─────────────────────────────────────────────────────────────────────────────────
# 1c. The SYSTEMD packaging's PKI recipe, EXECUTED. deploy/systemd/gen-certs.sh is what
#     deploy/systemd/README.md and mqttd.env.example now tell an operator to run, and the
#     previous shape of that instruction was openssl commands pasted into a comment — which
#     did not run at all (an undefined $PEER_HOST produced an empty SAN and openssl refused
#     to sign). A shipped recipe nothing executes is a shipped recipe nobody has checked, so
#     this runs it and then BOOTS TWO NODES from its output.
#
#     Two nodes, not one, and one `ca` invocation for both: that is the per-host-CA trap the
#     script exists to prevent. Three hosts that each self-sign their own CA form no mesh, and
#     the only assertion that can tell the difference is a peer link between two leaves that
#     share ONE issuer. If these two route to each other, all four peer-certificate rules hold
#     at once — CN=node id (or the Hello is dropped), SAN covers the advertise host (or the
#     dialer refuses), both EKUs (or rustls rejects the client cert), and an ECDSA key (or the
#     gossip signer refuses the key and the process never starts, since SIGNED=require here).
# ─────────────────────────────────────────────────────────────────────────────────
SYSPKI="$WORK/syspki"
PKI_DIR="$SYSPKI" sh "$GEN_CERTS" ca >"$WORK/gen-certs.log" 2>&1 \
  || { cat "$WORK/gen-certs.log"; fail "$GEN_CERTS could not mint the cluster CA"; }
for n in mqttd-a mqttd-b; do
  PKI_DIR="$SYSPKI" sh "$GEN_CERTS" node "$n" 127.0.0.1 >>"$WORK/gen-certs.log" 2>&1 \
    || { cat "$WORK/gen-certs.log"; fail "$GEN_CERTS could not mint a leaf set for $n"; }
done

# CA KEY CUSTODY, asserted on the artifact: the cluster's trust root must not appear in any
# per-node directory, because those directories are exactly what gets copied to a broker host
# — and the bus binds node identity to the certificate CN, so a host holding this key can mint
# a leaf claiming any node.
[[ -s "$SYSPKI/ca/peer-ca.key" ]] || fail "$GEN_CERTS did not mint a CA private key"
for n in mqttd-a mqttd-b; do
  [[ -e "$SYSPKI/$n/peer-ca.key" ]] \
    && fail "$GEN_CERTS put the CA PRIVATE key in $n's directory — that directory is copied \
to a broker host, and anything that can read the CA key can claim any node's identity"
  for f in peer-ca.pem peer.pem peer.key server.pem server.key; do
    [[ -s "$SYSPKI/$n/$f" ]] || fail "$GEN_CERTS did not produce $n/$f"
  done
  got_cn="$(cert_cn "$SYSPKI/$n/peer.pem")"
  [[ "$got_cn" == "$n" ]] \
    || fail "$n/peer.pem has CN '$got_cn'; the cluster bus requires CN=$n (the node id)"
  # ECDSA, not RSA: this key doubles as the gossip signing key (ADR 0022) and that signer
  # takes nothing else. Asserted on the key as well as via the boot below, so the reason a
  # regression fails is named rather than inferred from a cluster that never forms.
  openssl pkey -in "$SYSPKI/$n/peer.key" -noout -text 2>/dev/null | grep -q 'prime256v1\|P-256' \
    || fail "$n/peer.key is not an ECDSA P-256 key — the per-node gossip signer accepts only \
PKCS#8 ECDSA P-256/P-384 or Ed25519, so an RSA leaf gives a working TLS handshake and then \
'unsupported or unparseable gossip signing key' at startup"
done
# One issuer for both nodes, verified by the CA rather than assumed from one `ca` run.
for n in mqttd-a mqttd-b; do
  openssl verify -CAfile "$SYSPKI/ca/peer-ca.pem" "$SYSPKI/$n/peer.pem" >/dev/null 2>&1 \
    || fail "$n/peer.pem is not issued by the single cluster CA — a per-host CA forms no mesh"
done
pass "$GEN_CERTS minted ONE cluster CA and per-node leaves (CN=node id, ECDSA P-256), and \
kept the CA private key out of every per-node directory"

# Its own gossip key, so these two can never be confused with the three-node cluster below
# even if a process outlived its kill.
SYS_SWIM_KEY="$WORK/sys-swim-key"
python3 -c "import secrets;print(secrets.token_hex(32))" > "$SYS_SWIM_KEY"

read -r SA_C SA_H SA_P SA_S SB_C SB_H SB_P SB_S < <(python3 -c "
import socket
ss=[socket.socket() for _ in range(8)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]")

start_gen_node() { # <node-id> <client_port> <health_port> <peer_port> <swim_port> <seeds>
  local n="$1" cport="$2" hport="$3" pport="$4" sport="$5" seeds="$6"
  local dir="$WORK/gen-$n"
  mkdir -p "$dir"
  env -i \
    PATH="$PATH" HOME="$HOME" \
    MQTTD_NODE_ID="$n" \
    MQTTD_DATA_DIR="$dir" \
    MQTTD_TLS_BIND="127.0.0.1:$cport" \
    MQTTD_TLS_CERT="$SYSPKI/$n/server.pem" \
    MQTTD_TLS_KEY="$SYSPKI/$n/server.key" \
    MQTTD_HEALTH_BIND="127.0.0.1:$hport" \
    MQTTD_PEER_BIND="127.0.0.1:$pport" \
    MQTTD_PEER_ADVERTISE="127.0.0.1:$pport" \
    MQTTD_PEER_TLS_CA="$SYSPKI/$n/peer-ca.pem" \
    MQTTD_PEER_TLS_CERT="$SYSPKI/$n/peer.pem" \
    MQTTD_PEER_TLS_KEY="$SYSPKI/$n/peer.key" \
    MQTTD_SWIM_BIND="127.0.0.1:$sport" \
    MQTTD_SWIM_SEEDS="$seeds" \
    MQTTD_SWIM_KEY_FILE="$SYS_SWIM_KEY" \
    MQTTD_SWIM_SIGNED=require \
    MQTTD_PASSWORD_FILE="$PW_FILE" \
    MQTTD_ACL_FILE="$ACL_FILE" \
    MQTTD_READY_MIN_MEMBERS=1 \
    RUST_LOG=info \
    "$MQTTD_BIN" > "$dir/log" 2>&1 &
  PIDS+=($!)
  echo $!
}

GA=$(start_gen_node mqttd-a "$SA_C" "$SA_H" "$SA_P" "$SA_S" "")
GB=$(start_gen_node mqttd-b "$SB_C" "$SB_H" "$SB_P" "$SB_S" "127.0.0.1:$SA_S")

gen_wait_ready() { # <health_port> <node-id>
  local port="$1" n="$2"
  for _ in $(seq 1 240); do
    if MQTTD_HEALTH_BIND="127.0.0.1:$port" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "--- $n log ---"; tail -30 "$WORK/gen-$n/log" 2>/dev/null || true
  fail "$n, booted from $GEN_CERTS material, never became ready"
}
gen_wait_ready "$SA_H" mqttd-a
gen_wait_ready "$SB_H" mqttd-b

for n in mqttd-a mqttd-b; do
  if grep -q INSECURE "$WORK/gen-$n/log"; then
    grep INSECURE "$WORK/gen-$n/log" | sed 's/^/         /'
    fail "$n logged INSECURE while configured from $GEN_CERTS output"
  fi
  grep -q 'SWIM gossip is SIGNED per-node' "$WORK/gen-$n/log" \
    || fail "$n did not log 'SWIM gossip is SIGNED per-node' — $GEN_CERTS produced a key the \
per-node gossip signer will not take (ADR 0022)"
done

# The assertion the four rules actually ride on: a peer link between two DIFFERENT leaves
# under ONE CA, carrying a publish across nodes.
GEN_OUT="$WORK/gen-xnode.out"
mosquitto_sub -h 127.0.0.1 -p "$SB_C" --cafile "$SYSPKI/ca/peer-ca.pem" \
  -t 'devices/+/up/#' -C 1 -W 20 -u backend -P "$BACKEND_PW" -i gen-sub > "$GEN_OUT" 2>/dev/null &
GEN_SUB=$!
sleep 2
mosquitto_pub -h 127.0.0.1 -p "$SA_C" --cafile "$SYSPKI/ca/peer-ca.pem" \
  -t 'devices/device-a/up/temp' -m 'gen-crossed' -q 1 \
  -u device-a -P "$DEVICE_A_PW" -i gen-pub >/dev/null 2>&1 \
  || fail "a TLS publish against $GEN_CERTS's server certificate was not accepted"
wait "$GEN_SUB" 2>/dev/null || true
grep -q gen-crossed "$GEN_OUT" || { tail -20 "$WORK/gen-mqttd-a/log"; tail -20 "$WORK/gen-mqttd-b/log"
  fail "two nodes configured from $GEN_CERTS did not route between them — a peer certificate \
rule is broken (CN, SAN, EKU) or the two leaves do not share one CA"; }
pass "two nodes boot from $GEN_CERTS material, sign gossip per-node, and route across a \
mutually-authenticated bus (so all four peer-certificate rules hold)"

# Down again before the three-node cluster: these hold ports and a gossip mesh of their own.
kill "$GA" "$GB" 2>/dev/null || true
sleep 1

# ─────────────────────────────────────────────────────────────────────────────────
# 2. Three nodes, configured from the shipped file.
# ─────────────────────────────────────────────────────────────────────────────────
read -r P1 P2 P3 H1 H2 H3 PB1 PB2 PB3 SW1 SW2 SW3 < <(python3 -c "
import socket
ss=[socket.socket() for _ in range(12)]
[s.bind(('127.0.0.1',0)) for s in ss]
print(*[s.getsockname()[1] for s in ss])
[s.close() for s in ss]")

start_node() { # <n> <client_port> <health_port> <peer_port> <swim_port> <seeds> <ready_min>
  local n="$1" cport="$2" hport="$3" pport="$4" sport="$5" seeds="$6" ready="$7"
  local dir="$WORK/node$n"
  mkdir -p "$dir"
  env -i \
    PATH="$PATH" HOME="$HOME" \
    MQTTD_NODE_ID="mqttd-$n" \
    MQTTD_DATA_DIR="$dir" \
    MQTTD_TLS_BIND="127.0.0.1:$cport" \
    MQTTD_TLS_CERT="$PKI/mqttd-$n/mqttd-$n.pem" \
    MQTTD_TLS_KEY="$PKI/mqttd-$n/mqttd-$n.key" \
    MQTTD_HEALTH_BIND="127.0.0.1:$hport" \
    MQTTD_PEER_BIND="127.0.0.1:$pport" \
    MQTTD_PEER_ADVERTISE="127.0.0.1:$pport" \
    MQTTD_PEER_TLS_CA="$PKI/mqttd-$n/ca.pem" \
    MQTTD_PEER_TLS_CERT="$PKI/mqttd-$n/mqttd-$n.pem" \
    MQTTD_PEER_TLS_KEY="$PKI/mqttd-$n/mqttd-$n.key" \
    MQTTD_SWIM_BIND="127.0.0.1:$sport" \
    MQTTD_SWIM_SEEDS="$seeds" \
    MQTTD_SWIM_KEY_FILE="$SWIM_KEY" \
    MQTTD_SWIM_SIGNED=require \
    MQTTD_PASSWORD_FILE="$PW_FILE" \
    MQTTD_ACL_FILE="$ACL_FILE" \
    MQTTD_READY_MIN_MEMBERS="$ready" \
    MQTTD_SHUTDOWN_GRACE="$(shipped MQTTD_SHUTDOWN_GRACE)" \
    RUST_LOG=info \
    "$MQTTD_BIN" > "$dir/log" 2>&1 &
  PIDS+=($!)
  echo $!
}

# Node 1 founds the cluster BECAUSE it has no seeds, and needs a floor of 1 to come up
# alone — exactly what the shipped env file documents.
N1=$(start_node 1 "$P1" "$H1" "$PB1" "$SW1" "" 1)
N2=$(start_node 2 "$P2" "$H2" "$PB2" "$SW2" "127.0.0.1:$SW1" "$(shipped MQTTD_READY_MIN_MEMBERS)")
N3=$(start_node 3 "$P3" "$H3" "$PB3" "$SW3" "127.0.0.1:$SW1,127.0.0.1:$SW2" "$(shipped MQTTD_READY_MIN_MEMBERS)")

# Readiness via the broker's own probe — the same command the compose healthcheck runs,
# so this covers that too.
wait_ready() { # <health_port> <name>
  local port="$1" name="$2"
  for _ in $(seq 1 300); do
    if MQTTD_HEALTH_BIND="127.0.0.1:$port" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
      return 0
    fi
    sleep 0.5
  done
  echo "--- $name log ---"; tail -30 "$WORK/node${name#mqttd-}/log" 2>/dev/null || true
  fail "$name never became ready on health port $port"
}

wait_ready "$H1" mqttd-1
wait_ready "$H2" mqttd-2
wait_ready "$H3" mqttd-3
pass "three nodes formed a cluster over TLS and all report /readyz 200"

# ─────────────────────────────────────────────────────────────────────────────────
# 3. The security posture the artifacts claim.
# ─────────────────────────────────────────────────────────────────────────────────
# One grep covers both plaintext paths: the client listener logs "INSECURE: starting
# PLAINTEXT MQTT listener" and the cluster bus logs "INSECURE: starting PLAINTEXT peer
# listener". Absence is a real signal because every opt-out of a secure default is loudly
# logged (the same register as scripts/quickstart-smoke.sh).
for n in 1 2 3; do
  if grep -q INSECURE "$WORK/node$n/log"; then
    grep INSECURE "$WORK/node$n/log" | sed 's/^/         /'
    fail "node $n logged an INSECURE warning — the reference deployment is running a plaintext listener"
  fi
  grep -q 'SWIM gossip is SIGNED per-node' "$WORK/node$n/log" \
    || fail "node $n did not log 'SWIM gossip is SIGNED per-node' — the cluster-bus material \
is not reaching the gossip layer (ADR 0022), so gossip is shared-key only"
done
pass "no node logs INSECURE, and every node signs its gossip per-node (ADR 0022)"

# The other half of "there is no plaintext listener": a client that does not offer TLS gets
# nowhere. Without this, a stray MQTTD_PLAINTEXT_BIND would pass every assertion below.
if mosquitto_pub -h 127.0.0.1 -p "$P1" -t 'devices/device-a/up/t' -m x -q 1 \
     -u device-a -P "$DEVICE_A_PW" -i cleartext-probe >/dev/null 2>&1; then
  fail "a CLEARTEXT client was accepted on the TLS port — there must be no plaintext fallback"
fi
pass "a cleartext client is refused (no plaintext listener to fall back to)"

if mosquitto_pub -h 127.0.0.1 -p "$P1" "${MOSQ_TLS[@]}" -t 'devices/device-a/up/t' -m x -q 1 \
     -i anon-probe >/dev/null 2>&1; then
  fail "an ANONYMOUS client was accepted — the reference deployment is not secure by default"
fi
pass "anonymous clients are refused"

if mosquitto_pub -h 127.0.0.1 -p "$P1" "${MOSQ_TLS[@]}" -t 'devices/device-a/up/t' -m x -q 1 \
     -u device-a -P 'wrong-password' -i bad-pw >/dev/null 2>&1; then
  fail "a WRONG password was accepted"
fi
pass "a wrong password is refused"

mosquitto_pub -h 127.0.0.1 -p "$P1" "${MOSQ_TLS[@]}" -t 'devices/device-a/up/hello' -m 'hi' -q 1 \
  -u device-a -P "$DEVICE_A_PW" -i pw-ok >/dev/null 2>&1 \
  || fail "the generated password file did not authenticate a legitimate user over TLS"
pass "a hashed password from --hash-password authenticates against a running broker"

# The ACL: device-b must not reach device-a's subtree. A denied SUBSCRIBE is answered
# with SUBACK 0x80 (128) — and mosquitto_sub still EXITS 0 in that case, so the exit
# code is not the signal. The SUBACK code is, and `-d` is what prints it.
suback_code() { # <port> <topic> <user> <password> -> the granted-QoS byte, or "none"
  # `|| true`: a subscription that is granted but never receives a message exits 27
  # (timed out via -W), which `set -o pipefail` would otherwise treat as a script error.
  # The SUBACK code is the answer here, not the exit status.
  local out
  out="$(mosquitto_sub -h 127.0.0.1 -p "$1" "${MOSQ_TLS[@]}" -t "$2" -C 1 -W 2 -u "$3" -P "$4" \
    -i "acl-probe-$$-$RANDOM" -d 2>&1 || true)"
  sed -n 's/^Subscribed (mid: [0-9]*): //p' <<<"$out" | head -1
}

denied="$(suback_code "$P1" 'devices/device-a/down/#' device-b "$DEVICE_B_PW")"
[[ "$denied" == "128" ]] \
  || fail "device-b's SUBSCRIBE to device-a's topics returned '${denied:-none}', expected 128 (denied) — the ACL is not enforced"

# The positive half: without it, a broker that denied EVERY subscription would pass.
granted="$(suback_code "$P1" 'devices/device-b/down/#' device-b "$DEVICE_B_PW")"
[[ "$granted" == "0" || "$granted" == "1" ]] \
  || fail "device-b's SUBSCRIBE to its OWN topics returned '${granted:-none}', expected a granted QoS — the ACL is too tight"
pass "the ACL confines a device to its own subtree (128 for another's, granted for its own)"

# ─────────────────────────────────────────────────────────────────────────────────
# 4. Cross-node routing: subscribe on node 3, publish on node 1.
# ─────────────────────────────────────────────────────────────────────────────────
#    This is also the assertion that proves the cluster bus is configured correctly: with a
#    shared leaf, a missing clientAuth EKU, or a CN that is not the node id, the peer links
#    are dropped and nothing crosses.
OUT="$WORK/xnode.out"
mosquitto_sub -h 127.0.0.1 -p "$P3" "${MOSQ_TLS[@]}" -t 'devices/+/up/#' -C 1 -W 20 \
  -u backend -P "$BACKEND_PW" -i xnode-sub > "$OUT" 2>/dev/null &
SUB_PID=$!
sleep 2
mosquitto_pub -h 127.0.0.1 -p "$P1" "${MOSQ_TLS[@]}" -t 'devices/device-a/up/temp' -m 'crossed' -q 1 \
  -u device-a -P "$DEVICE_A_PW" -i xnode-pub >/dev/null 2>&1 \
  || fail "the cross-node publish was not accepted"
wait "$SUB_PID" 2>/dev/null || true
grep -q crossed "$OUT" || fail "a message published on node 1 never reached a subscriber on node 3 \
(with peer mTLS on, this is also how a wrongly-shaped cluster-bus certificate presents)"
pass "a publish on node 1 reaches a subscriber on node 3, over a mutually-authenticated bus"

# ─────────────────────────────────────────────────────────────────────────────────
# 5. An acknowledged QoS 1 message survives losing the node that accepted it.
#    A persistent subscriber is OFFLINE; the publisher is acked only once the message
#    is durably enqueued; the accepting node is then killed outright.
# ─────────────────────────────────────────────────────────────────────────────────
mosquitto_sub -h 127.0.0.1 -p "$P2" "${MOSQ_TLS[@]}" -t 'devices/device-b/down/#' -q 1 \
  -u device-b -P "$DEVICE_B_PW" -i durable-sub -c -C 1 -W 3 >/dev/null 2>&1 || true
pass "a persistent session is established, then disconnected"

mosquitto_pub -h 127.0.0.1 -p "$P1" "${MOSQ_TLS[@]}" -t 'devices/device-b/down/cmd' -m 'survives' -q 1 \
  -u backend -P "$BACKEND_PW" -i durable-pub >/dev/null 2>&1 \
  || fail "the QoS 1 publish was never acknowledged"
pass "the publisher was acknowledged (the broker has taken responsibility)"

kill -9 "$N1" 2>/dev/null || true
sleep 5
pass "node 1 was SIGKILLed"

# Redelivery here waits on a TAKEOVER, not just a reconnect: SWIM has to notice node 1 is
# gone, the lease group has to re-elect if node 1 held it, and a survivor has to promote
# the session before it can replay. That runs on production timings and is load-sensitive,
# so a single fixed window makes this assertion flaky — and a flaky assertion on the
# headline durability claim is dangerous in both directions: it cries wolf on unrelated
# changes, and a real regression gets dismissed as "the flaky one". (Seen for real: this
# failed once on a PR touching only a config converter, then passed on re-run.)
#
# So: poll within a generous total budget, and report WHICH failure it was.
RESUMED="$WORK/resumed.out"
DURABLE_BUDGET=90
durable_started=$SECONDS
: > "$RESUMED"
while (( SECONDS - durable_started < DURABLE_BUDGET )); do
  mosquitto_sub -h 127.0.0.1 -p "$P2" "${MOSQ_TLS[@]}" -t 'devices/device-b/down/#' -q 1 \
    -u device-b -P "$DEVICE_B_PW" -i durable-sub -c -C 1 -W 10 >> "$RESUMED" 2>/dev/null || true
  grep -q survives "$RESUMED" && break
done
waited=$(( SECONDS - durable_started ))

if ! grep -q survives "$RESUMED"; then
  # Distinguish "the message is gone" from "the cluster never recovered enough to say".
  # Without this the next occurrence costs another investigation from scratch.
  echo "--- after ${waited}s, surviving nodes report:"
  for hp in "$H2" "$H3"; do
    if MQTTD_HEALTH_BIND="127.0.0.1:$hp" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
      echo "    health $hp: READY"
    else
      echo "    health $hp: NOT ready (takeover may still be in progress)"
    fi
  done
  fail "an ACKNOWLEDGED message was not redelivered within ${waited}s of its node dying. \
If the survivors are READY above, this is a genuine durability failure; if they are NOT, \
the cluster had not finished taking over and the budget needs raising rather than the \
claim doubting"
fi
pass "the acknowledged message survived the loss of the node that accepted it (${waited}s)"

# ─────────────────────────────────────────────────────────────────────────────────
# 6. The readiness floor does what the artifacts say. With node 1 already gone, kill
#    node 2 as well: node 3 is then a minority of three and must drop OUT of rotation
#    while staying alive. This is the setting an operator is most likely to leave at
#    its default without checking, and the cost of it being wrong is a lone node
#    serving clients from a store it cannot write.
# ─────────────────────────────────────────────────────────────────────────────────
kill -9 "$N2" 2>/dev/null || true
for _ in $(seq 1 60); do
  MQTTD_HEALTH_BIND="127.0.0.1:$H3" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1 || break
  sleep 1
done

MQTTD_HEALTH_BIND="127.0.0.1:$H3" "$MQTTD_BIN" --probe /livez >/dev/null 2>&1 \
  || fail "the surviving node stopped serving /livez — a minority node must stay ALIVE, only unready"
if MQTTD_HEALTH_BIND="127.0.0.1:$H3" "$MQTTD_BIN" --probe /readyz >/dev/null 2>&1; then
  fail "the last surviving node of three still reports READY — MQTTD_READY_MIN_MEMBERS is not doing its job"
fi
pass "a minority node reports live-but-not-ready (drops from rotation, is not restarted)"

# ─────────────────────────────────────────────────────────────────────────────────
# 7. The remaining artifacts, checked with their own tools where available.
# ─────────────────────────────────────────────────────────────────────────────────
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  # A pre-existing secrets/ directory would be clobbered by the teardown below, and it
  # holds the reader's only copy of their passwords. Refuse rather than delete.
  [[ -e deploy/compose/secrets ]] \
    && fail "deploy/compose/secrets exists; move it aside — this step needs a clean directory"
  compose_cfg="$WORK/compose-config.yaml"
  overlay_cfg="$WORK/compose-config-plaintext.yaml"
  compose_json="$WORK/compose-config.json"
  armed_json="$WORK/compose-config-armed.json"
  (
    cd deploy/compose && mkdir -p secrets && : > secrets/mqttd-passwd \
      && : > secrets/mqttd-swim-key \
      && docker compose config >"$compose_cfg" \
      && docker compose -f compose.yaml -f compose.plaintext.yaml config >"$overlay_cfg" \
      && docker compose config --format json >"$compose_json" \
      && MQTTD_1_SEEDS=mqttd-2:7946,mqttd-3:7946 MQTTD_1_READY_MIN_MEMBERS=2 \
         docker compose config --format json >"$armed_json"
  ) || { rm -rf deploy/compose/secrets; fail "deploy/compose/*.yaml is not a valid compose file"; }
  rm -rf deploy/compose/secrets
  pass "deploy/compose/compose.yaml validates with 'docker compose config', with and without the overlay"

  # The rendered default must contain no plaintext MQTT anywhere — not the variable and not
  # a published 1883 — and the overlay must add both, on loopback. This is the assertion
  # that makes "plaintext is opt-in" a property of the files rather than a claim about them.
  grep -q 1883 "$compose_cfg" \
    && fail "the rendered default compose config mentions 1883 — plaintext must be opt-in only"
  grep -q MQTTD_PLAINTEXT_BIND "$compose_cfg" \
    && fail "the rendered default compose config sets MQTTD_PLAINTEXT_BIND"
  grep -q MQTTD_PLAINTEXT_BIND "$overlay_cfg" \
    || fail "the plaintext overlay renders no MQTTD_PLAINTEXT_BIND — it opts into nothing"
  grep -q 'published: "1883"' "$overlay_cfg" \
    || fail "the plaintext overlay publishes no 1883 — compose merge semantics may have changed"
  grep -q 'host_ip: 127.0.0.1' "$overlay_cfg" \
    || fail "the plaintext overlay does not bind 1883 to loopback"
  pass "the default renders no plaintext at all; the overlay adds it, published on loopback"

  # ── The two compose properties that are claims about the FILE, checked on the file ──
  # (a) KEY CUSTODY. The three brokers run as one uid, so the only boundary between them
  #     is the mount list: each must mount its own mqttd-tls-N and nothing else, and the
  #     CA-key volume must reach the `init` one-shot alone. deploy/README.md states this
  #     rule for both packagings; the systemd half is asserted on gen-certs.sh's output
  #     above, and this is the compose half.
  # (b) THE FOUNDER'S READINESS FLOOR. compose.yaml exempts mqttd-1 with a floor of 1 so
  #     ordered bring-up can start, and README.md's "arm the founder" step raises it to 2
  #     along with the seeds. Both renderings are asserted, so the documented step cannot
  #     quietly stop working — the majority behaviour it buys is proven in section 6 above.
  python3 - "$compose_json" "$armed_json" <<'PY' || fail "the rendered compose config breaks a documented property (above)"
import json, sys

with open(sys.argv[1]) as f: cfg = json.load(f)
with open(sys.argv[2]) as f: armed = json.load(f)
svcs = cfg["services"]
bad = []

def vols(svc):
    return {v.get("source") for v in svcs[svc].get("volumes", []) if v.get("type") == "volume"}

for n in (1, 2, 3):
    svc, mine = f"mqttd-{n}", {f"mqttd-tls-{n}"}
    tls = {v for v in vols(svc) if v.startswith("mqttd-tls")}
    if tls != mine:
        bad.append(f"{svc} mounts TLS volumes {sorted(tls)}; it must mount exactly {sorted(mine)} — "
                   "all three brokers share a uid, so another node's volume is another node's key")
    if "mqttd-ca" in vols(svc):
        bad.append(f"{svc} mounts mqttd-ca — the CA PRIVATE key must reach the init one-shot only")

init = vols("init")
if "mqttd-ca" not in init:
    bad.append("the init one-shot does not mount mqttd-ca, so the CA key would live in its "
               "container filesystem and vanish, re-minting the whole PKI on every up")
for n in (1, 2, 3):
    if f"mqttd-tls-{n}" not in init:
        bad.append(f"the init one-shot does not mount mqttd-tls-{n}; init.sh fails closed on it")

def floor(cfg, svc):
    return cfg["services"][svc]["environment"].get("MQTTD_READY_MIN_MEMBERS")

if floor(cfg, "mqttd-1") != "1":
    bad.append(f"the founder renders MQTTD_READY_MIN_MEMBERS={floor(cfg,'mqttd-1')!r} by default; "
               "it must be 1 or the first bring-up can never start")
if floor(armed, "mqttd-1") != "2":
    bad.append("with MQTTD_1_READY_MIN_MEMBERS=2 in the environment the founder still renders "
               f"{floor(armed,'mqttd-1')!r} — README.md's 'arm the founder' step cannot raise the floor, "
               "so node 1 stays exempt from the majority rule the docs advertise")
for n in (2, 3):
    if floor(cfg, f"mqttd-{n}") != "2":
        bad.append(f"mqttd-{n} renders a readiness floor of {floor(cfg,f'mqttd-{n}')!r}, not 2 (a majority of three)")

for b in bad:
    print("      " + b)
sys.exit(1 if bad else 0)
PY
  pass "each broker mounts only its own TLS volume, the CA key only reaches init, and the \
founder's floor is 1 by default and 2 once armed"
else
  skip_or_fail "docker compose not available; compose.yaml was NOT validated here"
fi

if command -v systemd-analyze >/dev/null 2>&1; then
  # `verify` cannot be gated on its exit code alone here: on a machine where mqttd is not
  # installed it legitimately complains about the absent EnvironmentFile and the absent
  # `mqttd` user, neither of which says anything about the unit's correctness. What DOES
  # matter is a directive systemd does not recognise — a typo'd hardening option is
  # silently ignored at runtime, so the unit would look hardened and not be.
  verify_out="$(systemd-analyze verify deploy/systemd/mqttd.service 2>&1 || true)"
  real_errors="$(grep -vE "EnvironmentFile|/etc/mqttd|Unknown user|Failed to (open|read)" <<<"$verify_out" \
                 | grep -iE "unknown lvalue|unknown key|unknown section|invalid|failed to parse" || true)"
  if [[ -n "$real_errors" ]]; then
    echo "$real_errors"
    fail "deploy/systemd/mqttd.service has an unknown or invalid directive (above)"
  fi
  pass "deploy/systemd/mqttd.service passes systemd-analyze verify"
else
  skip_or_fail "systemd-analyze not available; the unit was NOT verified here"
fi

echo
echo "DEPLOY SMOKE OK"
