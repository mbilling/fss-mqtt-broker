#!/usr/bin/env bash
# Start the live rules demo (README.md beside this file): build mqttd from this checkout,
# start the stack, wait until the broker is healthy, and print where to look.
#
#   demo/rules-live/up.sh
#   MQTT_PORT=1884 UI_PORT=8071 demo/rules-live/up.sh    # when a port is taken
#
# Always `up --build`: an image left from an older checkout would ignore the MQTTD_RULES_*
# settings without a word (an unknown variable is not an error), and an unchanged checkout
# rebuilds from the cache in seconds. The first build compiles mqttd: 5-15 minutes.
set -euo pipefail
cd "$(dirname "$0")"

if ! docker compose version >/dev/null 2>&1; then
  echo "up.sh: needs Docker with Compose v2 (\`docker compose\`)" >&2
  exit 1
fi

mqtt_bind="${MQTT_BIND:-127.0.0.1}"
mqtt_port="${MQTT_PORT:-1883}"
ui_port="${UI_PORT:-8070}"
health_port="${HEALTH_PORT:-8080}"
admin_port="${ADMIN_PORT:-9443}"

# A port something else already holds fails `up` halfway, with an error about the
# container. Say which one, and how to move it. On a re-run this stack holds them itself.
if [ -z "$(docker compose ps -q mqttd ui 2>/dev/null)" ]; then
  taken=0
  for check in "MQTT_PORT $mqtt_port" "UI_PORT $ui_port" "HEALTH_PORT $health_port" \
    "ADMIN_PORT $admin_port"; do
    read -r name port <<<"$check"
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      echo "up.sh: port $port is in use; pick another, e.g. $name=$((port + 1)) $0" >&2
      taken=1
    fi
  done
  [ "$taken" = 0 ] || exit 1
fi

if ! docker compose up --build -d; then
  docker compose logs --tail 40 mqttd >&2
  exit 1
fi

# `up -d` already waited for mqttd's healthcheck (the simulators and the UI depend on
# it); this says so, and stops with the broker's log if it never got there.
for _ in $(seq 1 60); do
  health="$(docker inspect -f '{{.State.Health.Status}}' "$(docker compose ps -q mqttd)" 2>/dev/null || true)"
  [ "$health" = healthy ] && break
  if [ "$health" = unhealthy ]; then
    docker compose logs --tail 40 mqttd >&2
    echo "up.sh: mqttd is unhealthy (its log is above)" >&2
    exit 1
  fi
  sleep 2
done
if [ "$health" != healthy ]; then
  echo "up.sh: mqttd is not healthy after 2 minutes: docker compose logs mqttd" >&2
  exit 1
fi

host=127.0.0.1
[ "$mqtt_bind" = 127.0.0.1 ] || host="$mqtt_bind"
sub="mosquitto_sub -h $host -p $mqtt_port -v"
cat <<EOF

The live rules demo is up.

  UI       http://localhost:$ui_port
  MQTT     $host:$mqtt_port (plaintext, anonymous)
  Health   http://localhost:$health_port/metrics
  Admin    https://localhost:$admin_port (mTLS; certificates in the pki-client volume)

Point any MQTT client at it. '#' never matches \$SYS topics, so those are named:

  rule statistics   $sub -t '\$SYS/brokers/+/rules/#'
  rule trace        $sub -t '\$SYS/brokers/+/trace/rules/+'
  derived messages  $sub -t 'alerts/#' -t 'kpi/#' -t 'normalized/#' -t 'analytics/#' -t 'state/#' -t 'events/#'
  device messages   $sub -t 'plant/#' -t 'home/#' -t 'vehicle/#'

From $(pwd):
  docker compose run --rm admin rules    the admin API (\`run --rm admin help\` lists the verbs)
  docker compose logs -f                 what every service says
  docker compose down                    stop (add -v to reset the rules and the PKI)
EOF
