#!/usr/bin/env bash
# The SEMANTIC ORACLE, Mosquitto lane (issue #297).
#
# Every misreading the five review rounds found had the same root: the converter's claims
# about what a directive MEANS were checked against our reading of the vendor's docs, by
# more reading. The provenance gate cannot catch that class — a misread value carries an
# honest `# from:` — and inspection demonstrably does not converge on it. What closes it is
# differential testing against the vendor itself: boot the REAL broker on the source
# config, boot mqttd on the converted config, and compare OBSERVABLE BEHAVIOUR, not config
# text. This is the same move the benchmarks made for performance claims (#244) and the
# history check made for consistency claims (#231): stop asserting, start measuring.
#
# What is compared, per probe, with the SAME client binaries against both brokers:
#   1. an anonymous client        (allow_anonymous false must mean REFUSED on both)
#   2. a wrong password           (REFUSED on both)
#   3. a valid credential         (ACCEPTED on both)
#   4. a permitted publish        (DELIVERED to an authorized subscriber on both)
#   5. a publish outside the ACL  (NOT delivered on both)
#   6. a permitted subscription   (DELIVERED on both)
#   7. a subscription outside it  (NOT delivered on both)
#
# Verdicts are compared at the ACCEPTED/REFUSED and DELIVERED/NOT_DELIVERED level
# deliberately: reason codes and error strings differ legitimately between brokers, but
# whether a client gets in and whether a message crosses is exactly the meaning the ACL
# and auth mappings claim to preserve. The brokers run SEQUENTIALLY on the same address,
# because the converted config carries the source's own bind — which puts the converter's
# bind translation under test too.
#
# The mqttd side runs the converted config AS WRITTEN, finished exactly as its own TODOs
# and NOTEs instruct an operator to finish it (each env overlay below names the line in
# the draft that demands it). If a finishing step beyond those is ever needed here, that
# is itself a converter defect: the draft failed to name a decision.
#
# What this does NOT prove: coverage. Seven probes over one config exercise the auth and
# ACL mappings, not mosquitto.conf(5). A mapping this file never touches is exactly as
# unverified as before — docs/MIGRATION.md says so where it matters.
#
# Needs the mosquitto BROKER (same requirement and exit-2 convention as
# scripts/migrate/dual-run-smoke.sh; CI installs it for that lane).
set -euo pipefail
cd "$(dirname "$0")/../.."

MQTTD_BIN="${MQTTD_BIN:-target/debug/mqttd}"
[[ -x "$MQTTD_BIN" ]]  || { echo "FATAL: $MQTTD_BIN not built";  exit 2; }
command -v mosquitto        >/dev/null || { echo "FATAL: the mosquitto BROKER is not installed"; exit 2; }
command -v mosquitto_passwd >/dev/null || { echo "FATAL: mosquitto_passwd is not installed"; exit 2; }
command -v mosquitto_pub    >/dev/null || { echo "FATAL: mosquitto_pub is not installed"; exit 2; }
command -v mosquitto_sub    >/dev/null || { echo "FATAL: mosquitto_sub is not installed"; exit 2; }

WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done
  rm -rf "$WORK"
}
trap cleanup EXIT

ok()   { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }
port() { python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()"; }

PORT=$(port); HPORT=$(port)

LEGACY_VERSION="$(mosquitto -h 2>&1 | grep -m1 -oE 'mosquitto version [0-9.]+' || true)"
MQTTD_VERSION="$("$MQTTD_BIN" --version 2>&1 || true)"
echo "versions under test:"
echo "  vendor oracle: $LEGACY_VERSION"
echo "  mqttd:         ${MQTTD_VERSION%%$'\n'*}"

# ── the SOURCE: a small but real mosquitto.conf + password_file + acl_file ────────────
# One scoped writer (sensor-1: write its own data topic, read its own command tree), one
# admin (readwrite #), anonymous access off. Small enough to reason about, wide enough
# that every probe above lands on a distinct mapping claim.
cat > "$WORK/mosquitto.conf" <<CONF
listener $PORT 127.0.0.1
allow_anonymous false
persistence false
password_file $WORK/mosq-passwd
acl_file $WORK/aclfile
CONF
cat > "$WORK/aclfile" <<'ACL'
user sensor-1
topic write sensors/sensor-1/data
topic read commands/sensor-1/#

user admin
topic readwrite #
ACL
mosquitto_passwd -c -b "$WORK/mosq-passwd" sensor-1 s3cret  >/dev/null 2>&1
mosquitto_passwd    -b "$WORK/mosq-passwd" admin    adminpw >/dev/null 2>&1

# ── convert ───────────────────────────────────────────────────────────────────────────
python3 scripts/migrate/from-mosquitto.py "$WORK/mosquitto.conf" \
  --out-config "$WORK/mqttd.toml" --out-acl "$WORK/acl.toml" >/dev/null \
  || fail "the converter refused the source config"
"$MQTTD_BIN" --check-config --config "$WORK/mqttd.toml" >/dev/null 2>&1 \
  || fail "the converted config does not pass mqttd --check-config"

# ── finish the draft, exactly as the draft itself instructs ───────────────────────────
# password_file: the config's TODO says mosquitto_passwd hashes cannot be converted and
# gives the recipe verbatim — `printf %s '<password>' | mqttd --hash-password <username>`.
printf %s 's3cret'  | "$MQTTD_BIN" --hash-password sensor-1 >  "$WORK/mqttd-passwd"
printf %s 'adminpw' | "$MQTTD_BIN" --hash-password admin    >> "$WORK/mqttd-passwd"
# acl_file: the config's NOTE says its /etc/mqttd/acl.toml is the converter's own
# deployment default — "CHANGE IT if you write the translated policy elsewhere".
# data_dir: the config's NOTE says /var/lib/mqttd is a packaged default to be replaced
# with a real volume. Both are supplied as env overlays (`--help`: "env still overlays").
FINISH=(
  MQTTD_PASSWORD_FILE="$WORK/mqttd-passwd"
  MQTTD_ACL_FILE="$WORK/acl.toml"
  MQTTD_DATA_DIR="$WORK/data"
  MQTTD_HEALTH_BIND="127.0.0.1:$HPORT"
)
mkdir -p "$WORK/data"

# ── the probe battery ─────────────────────────────────────────────────────────────────
# mosquitto_sub exit codes observed and relied on: 27 = -W timeout with the connection UP
# (the subscribe was accepted and nothing arrived), non-zero otherwise on a refused
# CONNACK. A connection verdict never inspects broker-specific error text.
connect_verdict() {  # extra client args...
  local rc=0
  mosquitto_sub -h 127.0.0.1 -p "$PORT" "$@" -t 'probe/none' -C 1 -W 2 >/dev/null 2>&1 || rc=$?
  if [[ $rc -eq 0 || $rc -eq 27 ]]; then echo ACCEPTED; else echo REFUSED; fi
}
deliver_verdict() {  # $1 sub user, $2 sub pass, $3 filter, $4 pub user, $5 pub pass, $6 topic
  local got="$WORK/got.$$"
  : > "$got"
  mosquitto_sub -h 127.0.0.1 -p "$PORT" -u "$1" -P "$2" -t "$3" -C 1 -W 4 > "$got" 2>/dev/null &
  local sub=$!
  sleep 0.4
  # `|| true`: a broker may refuse the denied publish at the protocol level (a v5 PUBACK
  # 0x87, a disconnect) or accept-and-drop it; both are legitimate spellings of DENIED and
  # the verdict is whether the message CROSSED, observed at the subscriber.
  mosquitto_pub -h 127.0.0.1 -p "$PORT" -u "$4" -P "$5" -q 1 -t "$6" -m crossed >/dev/null 2>&1 || true
  wait "$sub" 2>/dev/null || true
  if [[ -s "$got" ]]; then echo DELIVERED; else echo NOT_DELIVERED; fi
}
battery() {  # $1 = verdict file
  {
    echo "anonymous-connect      $(connect_verdict)"
    echo "wrong-password         $(connect_verdict -u sensor-1 -P wrong)"
    echo "valid-credential       $(connect_verdict -u sensor-1 -P s3cret)"
    echo "permitted-publish      $(deliver_verdict admin adminpw 'sensors/#'           sensor-1 s3cret sensors/sensor-1/data)"
    echo "publish-outside-acl    $(deliver_verdict admin adminpw 'telemetry/#'         sensor-1 s3cret telemetry/other)"
    echo "permitted-subscribe    $(deliver_verdict sensor-1 s3cret 'commands/sensor-1/#' admin adminpw commands/sensor-1/reboot)"
    echo "subscribe-outside-acl  $(deliver_verdict sensor-1 s3cret 'secret/#'            admin adminpw secret/x)"
  } > "$1"
}

# ── the vendor, on the source config ──────────────────────────────────────────────────
mosquitto -c "$WORK/mosquitto.conf" > "$WORK/mosquitto.log" 2>&1 &
MOSQ=$!; PIDS+=("$MOSQ")
for _ in $(seq 1 50); do
  mosquitto_pub -h 127.0.0.1 -p "$PORT" -u admin -P adminpw -t probe/up -m x >/dev/null 2>&1 && break
  sleep 0.1
done
battery "$WORK/verdicts.mosquitto"
kill "$MOSQ" 2>/dev/null || true; wait "$MOSQ" 2>/dev/null || true

# HARNESS SANITY, before any comparison: if the oracle side cannot even show the two
# anchor behaviours the source config plainly demands, agreement below would be vacuous
# (two broken probes agree perfectly).
grep -q '^anonymous-connect      REFUSED' "$WORK/verdicts.mosquitto" \
  || { cat "$WORK/verdicts.mosquitto"; fail "harness: the vendor accepted an anonymous client under allow_anonymous false — the probe is broken"; }
grep -q '^permitted-publish      DELIVERED' "$WORK/verdicts.mosquitto" \
  || { cat "$WORK/verdicts.mosquitto"; fail "harness: the vendor did not deliver the plainly-permitted publish — the probe is broken"; }
ok "vendor verdicts recorded ($LEGACY_VERSION), and the harness anchors hold"

# ── mqttd, on the CONVERTED config ────────────────────────────────────────────────────
env "${FINISH[@]}" RUST_LOG=warn "$MQTTD_BIN" --config "$WORK/mqttd.toml" \
  > "$WORK/mqttd.log" 2>&1 &
MQTTD=$!; PIDS+=("$MQTTD")
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$HPORT/readyz" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$HPORT/readyz" >/dev/null 2>&1 \
  || { tail -5 "$WORK/mqttd.log" | sed 's/^/         /'; fail "mqttd did not become ready on the converted config"; }
battery "$WORK/verdicts.mqttd"
kill "$MQTTD" 2>/dev/null || true; wait "$MQTTD" 2>/dev/null || true
ok "mqttd verdicts recorded, booted from the converted config + its own finishing steps"

# ── THE COMPARISON ────────────────────────────────────────────────────────────────────
if ! diff "$WORK/verdicts.mosquitto" "$WORK/verdicts.mqttd" > "$WORK/verdicts.diff"; then
  echo "  FAIL — mqttd on the CONVERTED config behaves differently from the vendor on the SOURCE:"
  echo "         (<) $LEGACY_VERSION on the source config   (>) mqttd on the converted config"
  sed 's/^/         /' "$WORK/verdicts.diff"
  echo "         A divergence here is a semantic misread: the converter carried the words and lost the meaning."
  exit 1
fi
sed 's/^/         /' "$WORK/verdicts.mosquitto"
ok "all 7 verdicts identical: the converted config means what the source config meant"
echo "DIFFERENTIAL OK"
