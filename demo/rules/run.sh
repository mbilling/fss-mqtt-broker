#!/usr/bin/env bash
# The rule-engine demo: simulated power plants, homes and cars through mqttd's rules.
#
#   demo/rules/run.sh                          the README's ten minutes, at 10x real time (~1 minute)
#   demo/rules/run.sh --speed 1                the same at real time
#   demo/rules/run.sh --domains cars --quiet   one domain, derived messages only
#   demo/rules/run.sh --start now              from the current time instead
#
# Starts mqttd on free localhost ports with demo/rules/rules.toml, plays the simulated
# devices into it, prints every device message (→) and every message the rules derive (⇒),
# then the broker's own per-rule counters, and stops the broker. Every option is passed to
# simulate.py (`demo/rules/run.sh --help` lists them). README.md explains the devices, the
# rules and why each makes the data more useful.
#
# Needs a build of mqttd that has the rule engine: $MQTTD if set, else this checkout's
# target/release or target/debug build, else it runs `cargo build --release -p mqttd`.
# A released binary on PATH is not used: no release has the rule engine yet, and one would
# ignore the rules file without a word (docs/RULES.md, "Get a build that has the rule engine").
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

command -v python3 >/dev/null || { echo "run.sh: python3 is required" >&2; exit 2; }

# Options that need no broker go straight to the simulator.
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
      echo
      exec python3 "$HERE/simulate.py" --help
      ;;
    --dry-run)
      exec python3 "$HERE/simulate.py" "$@"
      ;;
  esac
done

MQTTD="${MQTTD:-}"
if [ -n "$MQTTD" ]; then
  [ -x "$MQTTD" ] || { echo "run.sh: MQTTD=$MQTTD is not an executable" >&2; exit 2; }
elif [ -x "$ROOT/target/release/mqttd" ]; then
  MQTTD="$ROOT/target/release/mqttd"
elif [ -x "$ROOT/target/debug/mqttd" ]; then
  MQTTD="$ROOT/target/debug/mqttd"
elif [ -f "$ROOT/Cargo.toml" ]; then
  command -v cargo >/dev/null || {
    echo "run.sh: no mqttd build in $ROOT/target, and no cargo to make one." >&2
    echo "  Install Rust 1.88 or later (https://rustup.rs) and run this again, or set MQTTD to" >&2
    echo "  an mqttd built from a checkout that has the rule engine (docs/RULES.md, step 1)." >&2
    exit 2
  }
  echo "run.sh: building mqttd (the first build takes a few minutes)…" >&2
  (cd "$ROOT" && cargo build --release -p mqttd)
  MQTTD="$ROOT/target/release/mqttd"
else
  echo "run.sh: set MQTTD to an mqttd built from a checkout that has the rule engine" >&2
  exit 2
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

# The rules load before the listener starts, so once it accepts clients the log says
# whether they loaded.
for _ in $(seq 1 300); do
  grep -q "accepting MQTT" "$LOG" 2>/dev/null && break
  kill -0 "$BROKER" 2>/dev/null || {
    echo "run.sh: $MQTTD exited at startup:" >&2
    grep -E "WARN|ERROR|error" "$LOG" | tail -5 >&2 || tail -5 "$LOG" >&2
    exit 1
  }
  sleep 0.1
done
if ! grep -q "accepting MQTT" "$LOG"; then
  echo "run.sh: $MQTTD did not start within 30 s; the end of its log:" >&2
  tail -5 "$LOG" >&2
  exit 1
fi
if ! grep -q "rules loaded" "$LOG"; then
  echo "run.sh: $MQTTD started without loading $HERE/rules.toml." >&2
  echo "  It has no rule engine (a release, or a build from before it), or it ignored the" >&2
  echo "  file. Build this checkout's (cargo build --release -p mqttd) or set MQTTD to one." >&2
  grep -E "ERROR|rule engine|rules" "$LOG" | tail -5 >&2 || true
  exit 1
fi
grep "rules loaded" "$LOG" | sed 's/^.*INFO //'
echo "mqttd is on 127.0.0.1:$PORT; watch it yourself with: mosquitto_sub -p $PORT -t 'alerts/#' -v"
echo

python3 "$HERE/simulate.py" --port "$PORT" "$@"

echo
echo "The broker's own counters (GET /metrics on 127.0.0.1:$HEALTH):"
python3 - "$HEALTH" "$HERE/rules.toml" <<'EOF'
import re, sys, urllib.request
text = urllib.request.urlopen(f"http://127.0.0.1:{sys.argv[1]}/metrics", timeout=5).read().decode()
rules = re.findall(r"^\[rules\.([A-Za-z0-9_-]+)\]", open(sys.argv[2], encoding="utf-8").read(), re.M)
rows = {rule: {} for rule in rules}
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
