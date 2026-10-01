#!/usr/bin/env bash
# Eclipse `paho.mqtt.testing` — the INDEPENDENT MQTT 5 conformance oracle.
#
# Our own tests share this implementation's reading of the spec; the Eclipse suite does
# not. It was written against a reference broker by people who were not looking at our
# code, so a failure here is evidence in a way our own green suite cannot be.
#
# The suite is cloned at a PINNED commit and run as an external process against the real
# mqttd binary — nothing enters the broker's cargo supply chain, matching how run.sh and
# paho_conformance.py already treat Mosquitto and Paho.
#
# ## The expected-failure list is a ledger, not a mute button
#
# Three of the suite's 27 tests do not pass, for two different reasons, and every one is
# named in EXPECTED below with its reason. The script fails if:
#
#   * a test outside that list fails      — a regression, or a new deviation
#   * a test INSIDE that list passes      — the reason has expired; delete the entry
#   * the set of tests the suite runs changes — the pin moved under us
#
# The second rule is what stops this from becoming an ignore list: an entry cannot
# outlive the defect it describes, because the day the defect is fixed this script goes
# red and says so.
#
# Runs locally (needs `python3` and `git` on PATH) and in CI.
# Set MQTTD_BIN to a prebuilt binary to skip the build.
set -euo pipefail

cd "$(dirname "$0")/../.."

# The pin. Bump deliberately — a floating clone would make this suite's verdict depend on
# whatever upstream merged today, which is the opposite of an independent FIXED oracle.
PAHO_REPO="${PAHO_REPO:-https://github.com/eclipse-paho/paho.mqtt.testing}"
PAHO_REF="${PAHO_REF:-9d7bb80bb8b9d9cfc0b52f8cb4c1916401281103}"

for tool in python3 git; do
  command -v "$tool" >/dev/null 2>&1 || { echo "FATAL: '$tool' not found on PATH"; exit 2; }
done

MQTTD_BIN="${MQTTD_BIN:-}"
if [[ -z "$MQTTD_BIN" ]]; then
  echo "building mqttd (set MQTTD_BIN to skip)…"
  cargo build --quiet -p mqttd
  MQTTD_BIN="target/debug/mqttd"
fi
MQTTD_BIN="$(cd "$(dirname "$MQTTD_BIN")" && pwd)/$(basename "$MQTTD_BIN")"
[[ -x "$MQTTD_BIN" ]] || { echo "FATAL: mqttd binary not executable: $MQTTD_BIN"; exit 2; }

WORK="$(mktemp -d)"
BROKER_PID=""
cleanup() {
  [[ -n "$BROKER_PID" ]] && kill "$BROKER_PID" 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

# --- the suite --------------------------------------------------------------
CACHE="${PAHO_CACHE:-$WORK/paho.mqtt.testing}"
if [[ ! -d "$CACHE/.git" ]]; then
  echo "cloning $PAHO_REPO @ ${PAHO_REF:0:12}…"
  git clone --quiet "$PAHO_REPO" "$CACHE"
fi
git -C "$CACHE" fetch --quiet origin "$PAHO_REF" 2>/dev/null || git -C "$CACHE" fetch --quiet origin
# --force: a cached checkout still carries the correction below from its last run.
git -C "$CACHE" checkout --quiet --force "$PAHO_REF"
echo "suite:    $CACHE @ $(git -C "$CACHE" rev-parse --short HEAD)"

# One correction to the pinned suite (#777). test_subscribe_options (its noLocal step)
# and test_request_response both have bclient subscribe, then wait on
# `callback.subscribeds`: aclient's queue, which already holds aclient's SUBACK, so they
# never wait for bclient's. aclient publishes at once, and MQTT promises a subscription
# only once its SUBACK is sent [MQTT-3.8.4-1], so on a slow runner bclient misses the
# message and the test reads 0 != 1. Measured with test_subscribe_options alone on one
# CPU-starved core: 13 of 30 runs failed as shipped, 0 of 30 with the wait on bclient's
# own queue. The edit fails loudly if a new pin no longer has exactly these two.
python3 - "$CACHE/interoperability/client_test5.py" <<'PY'
import sys
path = sys.argv[1]
src = open(path, encoding="utf-8").read()
racy = (
    "      bclient.subscribe([topics[0]], [MQTTV5.SubscribeOptions(2, noLocal=True)])\n"
    "      self.waitfor(callback.subscribeds, 1, 3)\n"
)
fixed = racy.replace("callback.subscribeds", "callback2.subscribeds")
if src.count(racy) != 2:
    sys.exit("paho suite correction (#777): the two racy waits are not where they were; re-check them against this pin")
open(path, "w", encoding="utf-8").write(src.replace(racy, fixed))
PY
echo "broker:   $MQTTD_BIN"

# --- the broker -------------------------------------------------------------
# Deliberately run with NO ACL, which costs us test_subscribe_failure (declared below).
#
# The obvious move is to deny `test/nosubscribe` with a real ACL — the suite's reference
# broker special-cases that one filter, so the test is really asking "do you return a
# SUBACK failure where you are meant to". It was tried, and it fails in an instructive
# way: our deny rules match by filter OVERLAP (a deny on a specific topic blocks any
# wildcard that could reach it, `acl.rs`), and the suite's own `cleanRetained()`
# subscribes to bare `#` between test classes. Denying `test/nosubscribe` therefore denies
# the cleanup subscription, retained state survives from one test into the next, and FIVE
# unrelated tests fail — measured, not guessed: 9 failures with the ACL, 4 without.
#
# The ACL semantics are right and the suite's cleanup is reasonable; they are simply
# incompatible. Running unconfigured keeps the other 26 verdicts trustworthy, which is
# worth more than converting one expected failure into a pass.
read -r MQTT HEALTH < <(python3 - <<'PY'
import socket
ss = [socket.socket() for _ in range(2)]
for s in ss:
    s.bind(("127.0.0.1", 0))
print(" ".join(str(s.getsockname()[1]) for s in ss))
for s in ss:
    s.close()
PY
)

mkdir -p "$WORK/data"
MQTTD_NODE_ID=paho-testing \
MQTTD_ALLOW_EPHEMERAL_DURABILITY=1 \
MQTTD_PLAINTEXT_BIND="127.0.0.1:$MQTT" \
MQTTD_HEALTH_BIND="127.0.0.1:$HEALTH" \
MQTTD_ALLOW_ANONYMOUS=1 \
MQTTD_DATA_DIR="$WORK/data" \
RUST_LOG=off "$MQTTD_BIN" > "$WORK/broker.log" 2>&1 &
BROKER_PID=$!

# Ready means READY, not "the port accepts" (#487). The suite's setUpClass connects and
# waits for a CONNACK with a short socket timeout. A broker whose listener is bound but
# whose lease group is still forming accepts the TCP connection and answers late, and the
# suite then dies in setup with `0 tests run`, which reads like a pin change. So wait for
# /readyz to say ready AND for a real MQTT 5 CONNECT to get its CONNACK. If neither comes
# within the budget, fail here with the broker's log, rather than falling through to a
# suite run that can only time out.
probe() { # probe <timeout s> — exit 0 when a CONNECT is answered by a CONNACK in time
  python3 - "$MQTT" "$1" <<'PY'
import socket, sys
port, timeout = int(sys.argv[1]), float(sys.argv[2])
# MQTT 5 CONNECT: clean start, keep alive 60, no properties, client id "paho-ready".
cid = b"paho-ready"
var = b"\x00\x04MQTT\x05\x02\x00\x3c\x00"
payload = len(cid).to_bytes(2, "big") + cid
pkt = bytes([0x10, len(var) + len(payload)]) + var + payload
try:
    s = socket.create_connection(("127.0.0.1", port), timeout)
    s.settimeout(timeout)
    s.sendall(pkt)
    head = s.recv(1)
    s.sendall(b"\xe0\x00")  # DISCONNECT
    s.close()
except OSError:
    sys.exit(1)
sys.exit(0 if head == b"\x20" else 1)
PY
}
readyz() {
  python3 -c "
import json, sys, urllib.request
try:
    body = urllib.request.urlopen('http://127.0.0.1:$HEALTH/readyz', timeout=1).read()
    sys.exit(0 if json.loads(body).get('ready') is True else 1)
except Exception:
    sys.exit(1)
" 2>/dev/null
}
READY=""
for _ in $(seq 1 120); do
  if readyz && probe 2; then READY=1; break; fi
  sleep 0.5
done
if [[ -z "$READY" ]]; then
  echo "FAIL: the broker was not ready within 60 s (/readyz ready and a CONNACK to a probe CONNECT)."
  echo "      Nothing was run against it; this is a startup problem, not a conformance verdict."
  echo "--- broker log ---"; tail -50 "$WORK/broker.log"
  exit 1
fi
echo "listener: 127.0.0.1:$MQTT (ready: /readyz and a CONNECT answered)"

# --- run --------------------------------------------------------------------
# unittest writes its per-test verdicts to stderr; keep both streams.
run_suite() {
  set +e
  (cd "$CACHE/interoperability" && python3 client_test5.py -p "$MQTT") > "$WORK/out.txt" 2>&1
  set -e
}
run_suite
# A run that did not COMPLETE gets a diagnosis and, only if the broker is demonstrably
# healthy, ONE re-run. Incomplete means no "Ran N tests" line at all, OR "Ran 0 tests":
# unittest still prints that line when setUpClass fails, which is exactly the #487 case
# (setUpClass timing out waiting for a CONNACK). Counting 0 as complete let that case
# skip the re-run and fail as a "pin moved" (PR #808, 2026-10-01). A dead or unresponsive
# broker is never re-run into a pass: it fails below with the evidence.
if ! grep -qE '^Ran [1-9][0-9]* tests?' "$WORK/out.txt"; then
  alive=no; kill -0 "$BROKER_PID" 2>/dev/null && alive=yes
  t0=$(python3 -c 'import time; print(time.monotonic())')
  if probe 5; then answer=yes; else answer=no; fi
  ms=$(python3 -c "import time; print(int((time.monotonic() - $t0) * 1000))")
  echo "suite did not complete; broker alive=$alive, probe CONNECT answered=$answer in ${ms} ms"
  sed -n '/^ERROR: setUpClass/,/^---/p' "$WORK/out.txt" | head -20
  if [[ $alive == yes && $answer == yes ]] && readyz; then
    echo "the broker is healthy now: re-running the suite ONCE (a second incomplete run fails)"
    cp "$WORK/out.txt" "$WORK/out.first.txt"
    run_suite
  fi
fi

python3 - "$WORK/out.txt" "$CACHE/interoperability" "$MQTT" <<'PY'
import re, subprocess, sys

# name -> why it does not pass. Each entry is a claim that must stay true. The script
# fails if one of these PASSES, so an entry cannot quietly outlive its reason.
EXPECTED = {
    "test_subscribe_failure": (
        "SUITE CONFIGURATION we decline to make: the test wants a SUBACK failure for "
        "`test/nosubscribe`, which the suite's reference broker hardcodes. Our answer "
        "would be a real ACL — but our deny rules match by filter OVERLAP, so denying "
        "that topic also denies the suite's own `cleanRetained()` subscription to `#`, "
        "leaking retained state between tests and failing FIVE unrelated ones (measured: "
        "9 failures with the ACL, 4 without). Trading 26 trustworthy verdicts for 1 is a "
        "bad trade. The broker's SUBACK-failure path is covered directly by "
        "crates/mqttd/tests (0x87 on an ACL-denied filter)."
    ),
    "test_server_keep_alive": (
        "LEGAL DIFFERENCE + missing feature: the suite requires a broker that caps keep "
        "alive at 60 and returns Server Keep Alive. Ours accepts the client's value "
        "verbatim and has no cap to advertise. §3.2.2.3.14 makes the property OPTIONAL — "
        "a server only sends it to override — so declining is conformant; the suite is "
        "asserting its reference broker's configuration, not a spec requirement."
    ),
}

text = open(sys.argv[1], encoding="utf-8", errors="replace").read()
suite_dir, port = sys.argv[2], sys.argv[3]

ran = re.search(r"^Ran (\d+) tests?", text, re.M)
failed = set(re.findall(r"^(?:FAIL|ERROR): (\w+) ", text, re.M))

print(f"\npaho.mqtt.testing (MQTT 5): {ran.group(1) if ran else '?'} tests ran, "
      f"{len(failed)} failed, {len(EXPECTED)} expected.")
for name, why in sorted(EXPECTED.items()):
    print(f"  expected-fail {name}: {why}")

problems = []

# A suite that did not run to completion is a TERMINAL condition, reported alone. It must
# never fall through to the pass/fail diff below: with zero tests run, `failed` is empty,
# so every EXPECTED entry looks like it "now passes" and a broker that failed to start
# reads as a broker that fixed four bugs. That exact output was this script's first run.
EXPECTED_TESTS = 27
if ran and int(ran.group(1)) == 0:
    problems.append(
        "the suite ran 0 tests: its setUpClass failed (the suite's own connection setup, "
        "not a conformance verdict), and did so again on the one re-run, so NOTHING was "
        "verified. See the setUpClass error in the output below."
    )
elif not ran:
    problems.append(
        "the suite printed no 'Ran N tests' line — it did not complete, so NOTHING was "
        "verified. The broker most likely failed to start; see the output below."
    )
elif int(ran.group(1)) != EXPECTED_TESTS:
    problems.append(
        f"the suite ran {ran.group(1)} tests, not the {EXPECTED_TESTS} this list was "
        "written against — the pin moved, so every entry above needs re-checking before "
        "any verdict here means anything."
    )
else:
    # An undeclared failure gets ONE targeted re-run before it counts.
    #
    # Not a way to wish failures away — the re-run is a second observation, and a real
    # regression fails it too. It is here because some of these tests race the broker by
    # design: `test_will_message` calls disconnect() the instant the message arrives, and
    # the client asserts no QoS 2 receive flow is still half-open, so a PUBREL that lands
    # a moment later fails it. Measured: 8/8 passes run alone, but it fell over once
    # inside the full 27-test run. Without this, the gate would cry wolf often enough to
    # get ignored — which is the only way a gate truly fails.
    for name in sorted(failed - set(EXPECTED)):
        proc = subprocess.run(
            [sys.executable, "client_test5.py", "-p", port, f"Test.{name}"],
            cwd=suite_dir, capture_output=True, text=True,
        )
        if proc.returncode == 0:
            print(f"  flaky   {name}: failed in the full run, PASSED alone — suite race, not a regression.")
        else:
            problems.append(
                f"UNEXPECTED failure: {name} — failed in the full run AND on a targeted "
                "re-run, so it is a regression or a new deviation to triage."
            )
    for name in sorted(set(EXPECTED) - failed):
        problems.append(
            f"{name} now PASSES — delete its EXPECTED entry; the reason it names has expired."
        )

if problems:
    print("\nFAIL:")
    for p in problems:
        print(f"  {p}")
    print("\n--- suite output ---")
    print(text[-8000:])
    sys.exit(1)

print("\nOK: every failure is a declared one, and every declared one still fails.")
PY
