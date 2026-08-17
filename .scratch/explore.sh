#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
W="$PWD/w"
rm -rf "$W"; mkdir -p "$W"
cat > "$W/mosquitto.conf" <<CONF
per_listener_settings false
persistence false
listener 11991 127.0.0.1
allow_anonymous false
password_file $W/passwd
acl_file $W/aclfile
CONF
cat > "$W/aclfile" <<'ACL'
user sensor-1
topic write sensors/sensor-1/data
topic read commands/sensor-1/#

user admin
topic readwrite #
ACL
mosquitto_passwd -b -c "$W/passwd" sensor-1 s3cret
mosquitto_passwd -b "$W/passwd" admin adminpw
python3 ../scripts/migrate/from-mosquitto.py "$W/mosquitto.conf" \
  --out-config "$W/mqttd.toml" --out-acl "$W/acl.toml"
echo "=== CONFIG ==="; cat "$W/mqttd.toml"
echo "=== ACL ==="; cat "$W/acl.toml"
