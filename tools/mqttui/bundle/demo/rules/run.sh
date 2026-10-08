#!/usr/bin/env bash
# The rule-engine demo: simulated power plants, homes and cars through mqttd's rules.
#
#   demo/rules/run.sh                          10 simulated minutes at 10x real time (~1 minute)
#   demo/rules/run.sh --speed 1                the same at real time
#   demo/rules/run.sh --domains cars --quiet   one domain, derived messages only
#
# Starts mqttd on free localhost ports with demo/rules/rules.toml, plays the simulated
# devices into it, prints every device message (→) and every message the rules derive (⇒),
# then the broker's own per-rule counters, and stops the broker. Every option is passed to
# simulate.py (`python3 demo/rules/simulate.py --help`). README.md explains the devices,
# the rules and why each makes the data more useful.
#
# Needs a build of mqttd that has the rule engine: $MQTTD if set, else this checkout's
# target/release or target/debug build, else it runs `cargo build --release -p mqttd`.
# A released binary on PATH is not used: no release has the rule engine yet, and one would
# ignore the rules file without a word (docs/RULES.md, "Get a build that has the rule engine").
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

command -v python3 >/dev/null || { echo "run.sh: python3 is required" >&2; exit 2; }

MQTTD="${MQTTD:-}"
if [ -z "$MQTTD" ]; then
  if [ -x "$ROOT/target/release/mqttd" ]; then
    MQTTD="$ROOT/target/release/mqttd"
  elif [ -x "$ROOT/target/debug/mqttd" ]; then
    MQTTD="$ROOT/target/debug/mqttd"
  elif [ -f "$ROOT/Cargo.toml" ]; then
    echo "run.sh: building mqttd (first build takes a few minutes)…" >&2
    (cd "$ROOT" && cargo build --release -p mqttd)
    MQTTD="$ROOT/target/release/mqttd"
  else
    echo "run.sh: set MQTTD to an mqttd built from a checkout that has the rule engine" >&2
    exit 2
  fi
fi

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}
PORT="$(free_port)"
HEALTH="$(free_port)"
LOG="$(mktemp -t mqttd-rules-demo.XXXXXX)"

MQTTD_PLAINTEXT_BIND="127.0.0.1:$PORT" MQTTD_ALLOW_ANONYMOUS=1 MQTTD_DURABLE_SESSIONS=0 \
  MQTTD_HEALTH_BIND="127.0.0.1:$HEALTH" MQTTD_RULES_FILE="$HERE/rules.toml" \
  NO_COLOR=1 RUST_LOG=mqttd=info "$MQTTD" >"$LOG" 2>&1 &
BROKER=$!
cleanup() {
  kill "$BROKER" 2>/dev/null || true
  wait "$BROKER" 2>/dev/null || true
  rm -f "$LOG"
}
trap cleanup EXIT

for _ in $(seq 1 100); do
  grep -q "listening" "$LOG" 2>/dev/null && break
  kill -0 "$BROKER" 2>/dev/null || { cat "$LOG" >&2; echo "run.sh: mqttd exited" >&2; exit 1; }
  sleep 0.1
done
if ! grep -q "rules loaded" "$LOG"; then
  cat "$LOG" >&2
  echo "run.sh: $MQTTD did not load the rules: it has no rule engine, or the file failed to load" >&2
  exit 1
fi
grep "rules loaded" "$LOG" | sed 's/^.*INFO //'
echo "mqttd is on 127.0.0.1:$PORT; watch it yourself with: mosquitto_sub -p $PORT -t 'alerts/#' -v"
echo

python3 "$HERE/simulate.py" --port "$PORT" "$@"

echo
echo "The broker's own counters (GET /metrics on 127.0.0.1:$HEALTH):"
python3 - "$HEALTH" <<'EOF'
import sys, urllib.request
text = urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/metrics", timeout=5).read().decode()
rows = {}
for line in text.splitlines():
    if line.startswith("mqttd_rule_actions_total{"):
        labels, value = line.rsplit(" ", 1)
        rule = labels.split('rule="', 1)[1].split('"', 1)[0]
        result = labels.split('result="', 1)[1].split('"', 1)[0]
        rows.setdefault(rule, {})[result] = int(float(value))
for rule in sorted(rows):
    r = rows[rule]
    failed = f"  failed {r['failed']}" if r.get("failed") else ""
    print(f"  {rule:<34} {r.get('ok', 0):>6} published{failed}")
EOF
