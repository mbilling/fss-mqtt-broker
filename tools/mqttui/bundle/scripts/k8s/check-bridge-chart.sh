#!/usr/bin/env bash
# The chart's bridge deployment, checked for the properties that make it correct
# rather than merely renderable (ADR 0025 T12).
#
# The one that matters: every `client_id` in the bridge config must carry the
# __POD_NAME__ placeholder. MQTT client ids must be unique, and the bridge's
# built-in default is the CONSTANT `fss-bridge-local` — so replicas sharing an id
# take over each other's session in a reconnect loop, and the HA that a shared
# subscription is supposed to buy you becomes an outage instead.
#
# A first version of this check grepped the whole rendered manifest for
# __POD_NAME__ and passed even with a hard-coded client id, because it matched the
# init container's own `sed` pattern. It now reads the ConfigMap's TOML.
set -euo pipefail
cd "$(dirname "$0")/../.."

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

CHART=deploy/helm/mqttd
fail() { echo "  FAIL — $*"; exit 1; }

echo "── bridge is opt-in ──"
if helm template mqttd "$CHART" | grep -qi 'bridge'; then
  fail "the default render mentions the bridge; it must be opt-in"
fi
echo "  ok   — the default render contains no bridge objects"

echo "── bridge renders, and renders correctly ──"
helm template mqttd "$CHART" \
  --set bridge.enabled=true --set bridge.replicaCount=2 > /tmp/bridge-render.yaml

python3 - /tmp/bridge-render.yaml <<'PY'
import re, sys

# Deliberately not PyYAML: the CI runner has python3 but no guaranteed yaml
# module, and the two facts we need are simple enough to read textually.
text = open(sys.argv[1], encoding="utf-8").read()

problems = []

# 1. Every client_id in the bridge ConfigMap must be per-replica.
#    Grab the ConfigMap's embedded TOML: the block after "bridge.toml: |".
m = re.search(r"bridge\.toml: \|\n(.*?)(?=\n---|\Z)", text, re.S)
if not m:
    problems.append("no bridge.toml found in the rendered ConfigMap")
else:
    toml = m.group(1)
    ids = [
        line.strip()
        for line in toml.splitlines()
        if "client_id" in line and not line.strip().startswith("#")
    ]
    if not ids:
        problems.append("the bridge config sets no client_id at all")
    for line in ids:
        if "__POD_NAME__" not in line:
            problems.append(
                f"client_id without __POD_NAME__: {line!r} — replicas would share an "
                "MQTT client id and take over each other's session"
            )

# 2. The init container must actually perform the substitution.
if "s/__POD_NAME__/" not in text:
    problems.append("the init container no longer substitutes the pod name")

# 3. The spool must be mounted where the image makes it writable.
if "/var/lib/mqtt-bridge" not in text:
    problems.append("the spool is not mounted at /var/lib/mqtt-bridge")

if problems:
    for p in problems:
        print(f"  FAIL — {p}")
    raise SystemExit(1)
print("  ok   — every client_id is per-replica, substitution is wired, spool mounted")
PY

if command -v kubeconform >/dev/null 2>&1; then
  kubeconform -strict -summary -ignore-missing-schemas -schema-location default \
    < /tmp/bridge-render.yaml
else
  skip_or_fail "kubeconform not installed, so the rendered bridge manifests were NOT schema-validated"
fi

echo "BRIDGE CHART OK"
