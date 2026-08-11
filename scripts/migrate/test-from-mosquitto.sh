#!/usr/bin/env bash
# The converter's output must be accepted by the broker, not merely well-formed.
#
# A migration tool that emits plausible-looking TOML the broker then rejects is
# worse than none: it burns the evaluation it was meant to enable. So this feeds a
# realistic mosquitto.conf + acl_file through the converter and boots the real
# binary on the result.
set -euo pipefail
cd "$(dirname "$0")/../.."

MQTTD_BIN="${MQTTD_BIN:-target/debug/mqttd}"
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: $MQTTD_BIN not built"; exit 2; }

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

cat > "$WORK/mosquitto.conf" <<'CONF'
persistence true
persistence_location /var/lib/mosquitto/
max_connections 5000
max_queued_messages 1000
allow_anonymous false
acl_file ACLPATH
password_file /etc/mosquitto/passwd
listener 1883 127.0.0.1
listener 8883 0.0.0.0
certfile /etc/certs/server.crt
keyfile /etc/certs/server.key
cafile /etc/certs/ca.crt
sys_interval 10
CONF
sed -i.bak "s|ACLPATH|$WORK/aclfile|" "$WORK/mosquitto.conf"

cat > "$WORK/aclfile" <<'ACL'
user sensor-1
topic write sensors/sensor-1/#
topic read commands/sensor-1/#
user admin
topic readwrite #
pattern read devices/%u/status
pattern write devices/%u/telemetry
ACL

python3 scripts/migrate/from-mosquitto.py "$WORK/mosquitto.conf" \
  --out-config "$WORK/mqttd.toml" --out-acl "$WORK/acl.toml" >/dev/null

[[ -s "$WORK/acl.toml" ]] || { echo "  FAIL — no ACL produced"; exit 1; }
# The converted CONFIG must be valid TOML. The fixture has two listeners on purpose:
# the converter once emitted [listeners] per listener, which tomllib rejects — and this
# harness never noticed, because the broker below is booted with env vars and the
# converted ACL only. Found by the 2026-08-11 review panel.
python3 - "$WORK/mqttd.toml" <<'PYEOF' || { echo "  FAIL — converted config is not valid TOML"; exit 1; }
import sys, tomllib
tomllib.load(open(sys.argv[1], "rb"))
PYEOF
echo "  ok   — converted config parses as TOML"

grep -q 'default = "deny"' "$WORK/acl.toml" || { echo "  FAIL — translated ACL is not deny-by-default"; exit 1; }
# %u must become mqttd's %i, or every pattern rule silently matches nothing.
grep -q '%i' "$WORK/acl.toml" || { echo "  FAIL — mosquitto's %u was not translated to %i"; exit 1; }
echo "  ok   — ACL is deny-by-default and %u became %i"

# THE assertion: the real broker accepts it.
PORT=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
HPORT=$(python3 -c "import socket;s=socket.socket();s.bind(('127.0.0.1',0));print(s.getsockname()[1]);s.close()")
MQTTD_ACL_FILE="$WORK/acl.toml" MQTTD_PLAINTEXT_BIND="127.0.0.1:$PORT" \
  MQTTD_ALLOW_ANONYMOUS=1 MQTTD_HEALTH_BIND="127.0.0.1:$HPORT" RUST_LOG=warn \
  "$MQTTD_BIN" > "$WORK/boot.log" 2>&1 &
BROKER=$!
trap 'kill $BROKER 2>/dev/null || true; rm -rf "$WORK"' EXIT

for _ in $(seq 1 100); do
  curl -fsS "http://127.0.0.1:$HPORT/readyz" >/dev/null 2>&1 && break
  sleep 0.1
done
if ! curl -fsS "http://127.0.0.1:$HPORT/readyz" >/dev/null 2>&1; then
  echo "  FAIL — the broker REJECTED the converted ACL:"
  tail -5 "$WORK/boot.log" | sed 's/^/         /'
  exit 1
fi
echo "  ok   — the broker booted on the converted ACL"
echo "MIGRATE OK"
