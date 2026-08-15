#!/usr/bin/env bash
# The dual-run cutover playbook's bridge step, actually run (ADR 0051 T18).
#
# docs/MIGRATION.md tells an operator to bridge their incumbent broker to mqttd, move
# clients across in cohorts, verify, then cut. That instruction is worth nothing if the
# bridge config in the document does not work, so this runs the document's own bridge
# shape against a REAL third-party broker: Mosquitto standing in for "the incumbent".
#
# Mosquitto is the stand-in because it is the third-party broker this repository already
# depends on for interop, and because the property being tested is a PROTOCOL property —
# whether the far broker honours the MQTT 5 No Local subscription option, which is what
# stops a bidirectional bridge amplifying. EMQX and HiveMQ document the same behaviour
# (EMQX's own `bridge_mode` docs call it "equivalent to No Local = 1"), but they are NOT
# exercised here and docs/MIGRATION.md says so where it matters.
#
# The assertions are the claims the playbook makes:
#   1. incumbent -> mqttd crosses          (the inbound leg)
#   2. mqttd -> incumbent crosses          (the outbound leg, so a moved client can still
#                                           talk to one that has not moved)
#   3. RETAINED state crosses by itself    (the one piece of state that DOES migrate)
#   4. a `both` rule does NOT amplify inbound   — MQTTD's No Local cuts this one
#   5. a `both` rule does NOT amplify outbound  — the FOREIGN broker's No Local cuts it
#   6. a retained value pruned while the bridge is UP stays pruned on both sides
#      (the documented mitigation for the resurrection hazard in docs/MIGRATION.md)
#
# 4 and 5 exist as separate assertions because they are different claims about different
# brokers, and the first draft of this harness conflated them. Measured with the bridge's
# own counters: publishing on the incumbent moves fss_bridge_forwarded_total{direction=in}
# and leaves {direction=out} alone — the message never travelled back toward Mosquitto, so
# the loop was cut on the MQTTD side by mqttd's own No Local, and Mosquitto's behaviour was
# never consulted. Only assertion 5 (publish on mqttd, COUNT on the incumbent) puts the
# foreign broker's No Local on the hook, and it is the only thing in this file that
# supports the "at least the protocol property was checked against a third-party broker"
# claim docs/MIGRATION.md makes.
set -euo pipefail
cd "$(dirname "$0")/../.."

MQTTD_BIN="${MQTTD_BIN:-target/debug/mqttd}"
BRIDGE_BIN="${MQTT_BRIDGE_BIN:-target/debug/mqtt-bridge}"
[[ -x "$MQTTD_BIN" ]]  || { echo "FATAL: $MQTTD_BIN not built";  exit 2; }
[[ -x "$BRIDGE_BIN" ]] || { echo "FATAL: $BRIDGE_BIN not built"; exit 2; }
command -v mosquitto     >/dev/null || { echo "FATAL: the mosquitto BROKER is not installed"; exit 2; }
command -v mosquitto_pub >/dev/null || { echo "FATAL: mosquitto_pub is not installed"; exit 2; }
command -v mosquitto_sub >/dev/null || { echo "FATAL: mosquitto_sub is not installed"; exit 2; }

WORK="$(mktemp -d)"
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  for p in "${PIDS[@]:-}"; do wait "$p" 2>/dev/null || true; done   # reap: an unreaped
  rm -rf "$WORK"                                                    # job prints "Terminated"
}
trap cleanup EXIT

ok()   { echo "  ok   — $1"; }
fail() { echo "  FAIL — $1"; exit 1; }
port() { python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()"; }

LEGACY_PORT=$(port); MQTTD_PORT=$(port); HPORT=$(port); BMETRICS=$(port)
mkdir -p "$WORK/spool" "$WORK/data"

# Versions are recorded because the claims below are about SPECIFIC binaries. Captured
# into variables rather than piped: `| head -1` kills the producer with SIGPIPE, which
# `set -o pipefail` turns into a spurious exit 141.
LEGACY_VERSION="$(mosquitto -h 2>&1 | grep -m1 -oE 'mosquitto version [0-9.]+' || true)"
MQTTD_VERSION="$("$MQTTD_BIN" --version 2>&1 || true)"
echo "versions under test:"
echo "  incumbent stand-in: $LEGACY_VERSION"
echo "  mqttd:              ${MQTTD_VERSION%%$'\n'*}"

# ── the incumbent ────────────────────────────────────────────────────────────────────
cat > "$WORK/mosquitto.conf" <<CONF
listener $LEGACY_PORT 127.0.0.1
allow_anonymous true
persistence false
CONF
mosquitto -c "$WORK/mosquitto.conf" > "$WORK/legacy.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 50); do
  mosquitto_pub -h 127.0.0.1 -p "$LEGACY_PORT" -t probe -m x >/dev/null 2>&1 && break
  sleep 0.1
done
mosquitto_pub -h 127.0.0.1 -p "$LEGACY_PORT" -t probe -m x >/dev/null 2>&1 \
  || fail "the incumbent stand-in (mosquitto) did not come up"
ok "the incumbent broker is up on 127.0.0.1:$LEGACY_PORT"

# Retained state exists on the incumbent BEFORE the bridge starts — that is the honest
# shape of a cutover, and it is what makes assertion 3 meaningful.
mosquitto_pub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/truck7/config' -m 'interval=30' -r
ok "a RETAINED message pre-exists on the incumbent (fleet/truck7/config)"

# ── mqttd, empty, beside it ──────────────────────────────────────────────────────────
MQTTD_PLAINTEXT_BIND="127.0.0.1:$MQTTD_PORT" MQTTD_HEALTH_BIND="127.0.0.1:$HPORT" \
  MQTTD_DATA_DIR="$WORK/data" MQTTD_ALLOW_ANONYMOUS=1 RUST_LOG=warn \
  "$MQTTD_BIN" > "$WORK/mqttd.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$HPORT/readyz" >/dev/null 2>&1 && break
  sleep 0.1
done
curl -fsS "http://127.0.0.1:$HPORT/readyz" >/dev/null 2>&1 \
  || { echo "  FAIL — mqttd did not become ready:"; tail -5 "$WORK/mqttd.log" | sed 's/^/         /'; exit 1; }
ok "mqttd is up and READY on 127.0.0.1:$MQTTD_PORT (empty: no sessions, no retained set)"

# ── the cutover bridge — the exact shape docs/MIGRATION.md documents ─────────────────
# `both` with NO remap over the shared namespace (a remap on `both` is REFUSED, #192),
# qos = 1 explicitly (the default 0 would silently downgrade the migrated traffic), a
# durable spool (a QoS>=1 rule is refused without one), one instance with sharing off.
cat > "$WORK/bridge.toml" <<CONF
hop_count_limit = 8
share_group = ""
metrics_bind = "127.0.0.1:$BMETRICS"

[local]
url = "127.0.0.1:$MQTTD_PORT"
client_id = "cutover-bridge-local"

[spool]
dir = "$WORK/spool"
max_messages = 10000

[[upstreams]]
name = "incumbent"
url = "127.0.0.1:$LEGACY_PORT"
client_id = "cutover-bridge-incumbent"

[[upstreams.rules]]
direction = "both"
filter = "fleet/#"
qos = 1
CONF
RUST_LOG=info "$BRIDGE_BIN" "$WORK/bridge.toml" > "$WORK/bridge.log" 2>&1 &
PIDS+=($!)
for _ in $(seq 1 100); do
  grep -q 'starting mqtt-bridge' "$WORK/bridge.log" && break
  sleep 0.1
done
grep -q 'starting mqtt-bridge' "$WORK/bridge.log" \
  || { echo "  FAIL — mqtt-bridge rejected the playbook's config:";
       tail -5 "$WORK/bridge.log" | sed 's/^/         /'; exit 1; }
sleep 2   # let both sides connect and subscribe
ok "mqtt-bridge accepted the playbook's config and connected to both brokers"

# ── 1. incumbent -> mqttd ────────────────────────────────────────────────────────────
# `fleet/+/telemetry`, not `fleet/#`: the pre-existing RETAINED fleet/truck7/config would
# otherwise satisfy `-C 1` before the live publish arrived, and this assertion would pass
# on the wrong message.
mosquitto_sub -h 127.0.0.1 -p "$MQTTD_PORT" -t 'fleet/+/telemetry' -C 1 -W 8 > "$WORK/in.txt" 2>&1 &
SUB=$!; PIDS+=("$SUB")
sleep 1
mosquitto_pub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/truck7/telemetry' -m 'from-incumbent' -q 1
wait $SUB 2>/dev/null || true
grep -q 'from-incumbent' "$WORK/in.txt" \
  || { echo "  FAIL — a publish on the incumbent did not reach mqttd. Got:";
       sed 's/^/         /' "$WORK/in.txt"; exit 1; }
ok "1. incumbent -> mqttd: a QoS 1 publish crosses inbound"

# ── 2. mqttd -> incumbent ────────────────────────────────────────────────────────────
mosquitto_sub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/+/telemetry' -C 1 -W 8 > "$WORK/out.txt" 2>&1 &
SUB=$!; PIDS+=("$SUB")
sleep 1
mosquitto_pub -h 127.0.0.1 -p "$MQTTD_PORT" -t 'fleet/truck9/telemetry' -m 'from-mqttd' -q 1
wait $SUB 2>/dev/null || true
grep -q 'from-mqttd' "$WORK/out.txt" \
  || { echo "  FAIL — a publish on mqttd did not reach the incumbent. Got:";
       sed 's/^/         /' "$WORK/out.txt"; exit 1; }
ok "2. mqttd -> incumbent: a QoS 1 publish crosses outbound (a moved client can still reach an unmoved one)"

# ── 3. RETAINED state crossed by itself ──────────────────────────────────────────────
# The bridge subscribes with retain_as_published = true and retain_handling = 0, so on
# connect it receives the far side's retained SET and republishes it with RETAIN intact.
# This is the one piece of state that DOES migrate — an idempotent re-sync, not a replay.
# It is NOT pure upside, and the same sentence without this clause is the one docs/BRIDGE.md,
# docs/MIGRATION.md, docs/OPERATIONS.md and README.md each had to have it added to: the
# re-sync runs in BOTH directions on every reconnect, so a retained value deleted while the
# bridge is down is RESURRECTED from the other side. Assertion 6 below pins the mitigation
# (prune with the bridge UP and the tombstone crosses).
mosquitto_sub -h 127.0.0.1 -p "$MQTTD_PORT" -t 'fleet/truck7/config' -C 1 -W 8 > "$WORK/ret.txt" 2>&1 || true
grep -q 'interval=30' "$WORK/ret.txt" \
  || { echo "  FAIL — the incumbent's RETAINED message did not cross to mqttd. Got:";
       sed 's/^/         /' "$WORK/ret.txt"; exit 1; }
ok "3. RETAINED state crossed on its own and is retained on mqttd (a fresh subscriber gets it)"

# ── 4. a `both` rule does not amplify INBOUND (mqttd's own No Local) ─────────────────
# Publish on the incumbent, count on mqttd. If the loop were open, the message would go
# incumbent -> mqttd -> incumbent -> mqttd … until the hop counter cut it at 8, and this
# subscriber would see several copies. It sees one — but the cut happened on MQTTD's side:
# mqttd did not echo the message back to the bridge's own subscription. That is a claim
# about mqttd, not about Mosquitto. Assertion 5 is the one about Mosquitto.
fwd_in()  { curl -fsS "http://127.0.0.1:$BMETRICS/metrics" \
              | sed -n 's/^fss_bridge_forwarded_total{upstream="incumbent",direction="in"} //p'; }
fwd_out() { curl -fsS "http://127.0.0.1:$BMETRICS/metrics" \
              | sed -n 's/^fss_bridge_forwarded_total{upstream="incumbent",direction="out"} //p'; }
IN0=$(fwd_in); OUT0=$(fwd_out)
mosquitto_sub -h 127.0.0.1 -p "$MQTTD_PORT" -t 'fleet/loop/probe' -W 5 > "$WORK/loop.txt" 2>&1 &
SUB=$!; PIDS+=("$SUB")
sleep 1
mosquitto_pub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/loop/probe' -m 'once' -q 1
wait $SUB 2>/dev/null || true
COUNT=$(grep -c 'once' "$WORK/loop.txt" || true)
[[ "$COUNT" == "1" ]] \
  || fail "4. ONE publish was delivered $COUNT time(s) — a bidirectional rule is amplifying"
IN1=$(fwd_in); OUT1=$(fwd_out)
# The counters are what make the attribution checkable rather than asserted: the message
# crossed inbound once and never travelled back out, which is what "mqttd cut it" means.
[[ "$IN1" == "$((IN0 + 1))" ]] \
  || fail "4. the inbound counter went $IN0 -> $IN1; the probe did not cross inbound exactly once"
[[ "$OUT1" == "$OUT0" ]] \
  || fail "4. the OUTBOUND counter moved ($OUT0 -> $OUT1) — mqttd echoed the bridge's own publish back, so No Local is not holding on the mqttd side"
ok "4. inbound: one publish, exactly one delivery — and the outbound counter never moved, so MQTTD's No Local made the cut"

# ── 5. a `both` rule does not amplify OUTBOUND (the FOREIGN broker's No Local) ────────
# The mirror image, and the only assertion here that tests the third-party broker: publish
# on mqttd, count on the incumbent. Mosquitto delivers to the test subscriber AND would
# deliver back to the bridge's own incumbent-side subscription if it ignored No Local —
# which would come back to mqttd, be forwarded out again, and land a second copy on this
# subscriber. A count of exactly 1 here IS a claim about Mosquitto 2.1.2's behaviour.
mosquitto_sub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/rloop/probe' -W 5 > "$WORK/rloop.txt" 2>&1 &
SUB=$!; PIDS+=("$SUB")
sleep 1
mosquitto_pub -h 127.0.0.1 -p "$MQTTD_PORT" -t 'fleet/rloop/probe' -m 'rev' -q 1
wait $SUB 2>/dev/null || true
RCOUNT=$(grep -c 'rev' "$WORK/rloop.txt" || true)
[[ "$RCOUNT" == "1" ]] \
  || fail "5. ONE publish on mqttd was delivered $RCOUNT time(s) on the incumbent — the FOREIGN broker is echoing the bridge's own publish back, so No Local is not holding there"
IN2=$(fwd_in); OUT2=$(fwd_out)
[[ "$OUT2" == "$((OUT1 + 1))" ]] \
  || fail "5. the outbound counter went $OUT1 -> $OUT2; the probe did not cross outbound exactly once"
[[ "$IN2" == "$IN1" ]] \
  || fail "5. the INBOUND counter moved ($IN1 -> $IN2) — the incumbent echoed the message back to the bridge, which is exactly the amplification No Local is meant to prevent"
ok "5. outbound: one publish, exactly one delivery on the incumbent, and the inbound counter never moved — the FOREIGN broker honoured No Local"

# ── 6. the retained-resurrection mitigation ──────────────────────────────────────────
# Bidirectional retained re-sync has a hazard the playbook must not present as pure
# upside: a retained value deleted on one side while the bridge is DOWN is republished
# from the surviving side on the next reconnect (measured — the delete on the incumbent
# came back as `interval=30` from mqttd). The documented mitigation is to prune while the
# bridge is RUNNING, so the zero-length retained publish crosses as a tombstone. This
# asserts the mitigation, because a mitigation nobody ran is not a mitigation.
mosquitto_pub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/truck7/config' -r -n
sleep 2
mosquitto_sub -h 127.0.0.1 -p "$LEGACY_PORT" -t 'fleet/truck7/config' -C 1 -W 3 > "$WORK/pruned-legacy.txt" 2>&1 || true
mosquitto_sub -h 127.0.0.1 -p "$MQTTD_PORT"  -t 'fleet/truck7/config' -C 1 -W 3 > "$WORK/pruned-mqttd.txt" 2>&1 || true
grep -q 'interval=30' "$WORK/pruned-legacy.txt" \
  && fail "6. the retained value survived its own deletion on the incumbent"
grep -q 'interval=30' "$WORK/pruned-mqttd.txt" \
  && { echo "  FAIL — 6. the retained CLEAR did not cross the bridge: mqttd still serves the old value,";
       echo "         so pruning on one side leaves the other side able to resurrect it."; exit 1; }
ok "6. a retained value pruned with the bridge UP is gone on BOTH sides (the tombstone crossed)"

# ── the bridge's own signals are scrapeable, which is how step 5 is verified ──────────
# Asserting on the substring `bridge_` was the bug that let docs/MIGRATION.md cite five
# series that do not exist: `bridge_connected` matches inside `fss_bridge_connected`, so
# the harness agreed with a document naming the wrong metrics. Every name the playbook's
# verification table tells an operator to alert on is now matched ANCHORED, with its
# registry prefix and its real label set.
curl -fsS "http://127.0.0.1:$BMETRICS/metrics" > "$WORK/metrics.txt" 2>/dev/null \
  || fail "the bridge's Prometheus endpoint did not answer"
for series in \
  'fss_bridge_connected{side="local"}' \
  'fss_bridge_connected{side="incumbent"}' \
  'fss_bridge_forwarded_total{upstream="incumbent",direction="in"}' \
  'fss_bridge_forwarded_total{upstream="incumbent",direction="out"}' \
  'fss_bridge_dropped_total{reason="hop-limit"}' \
  'fss_bridge_dropped_total{reason="spool-full",side="local"}' \
  'fss_bridge_reconnects_total{side="local"}' \
  'fss_bridge_spool_depth{side="local"}' \
  'fss_bridge_spool_capacity{side="local"}' ; do
  grep -qF "$series " "$WORK/metrics.txt" \
    || fail "the playbook cites $series but the bridge does not export it"
done
# The other half of the same guard: an UNPREFIXED name must not appear, because that is
# what the document used to tell operators to alert on.
if grep -qE '^bridge_' "$WORK/metrics.txt"; then
  fail "an UNPREFIXED bridge_* series appeared; the docs' metric names would need revisiting"
fi
ok "every fss_bridge_* series the playbook's verification table cites exists, anchored and labelled"

echo "DUAL RUN SMOKE OK"
