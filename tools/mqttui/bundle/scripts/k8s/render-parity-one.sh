#!/usr/bin/env bash
# Render parity: the operator and the Helm chart must produce the SAME objects
# (ADR 0055 T2).
#
# Two supported deployment paths that disagree are worse than one — a fix applied
# to the chart but not the operator (or vice versa) is exactly the drift this gate
# exists to make impossible. It renders both sides for equivalent inputs, strips
# the differences that are *by construction* (see NORMALIZE below), and diffs what
# remains: any load-bearing divergence fails the build.
#
# Requires: helm, python3, cargo. Runs offline — no cluster.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CHART="$REPO_ROOT/deploy/helm/mqttd"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# The chart and the operator are compared in BOTH founder-guard states (ADR 0055 T9).
# Comparing only the default would leave the armed branch — the one that decides
# whether a wiped pod-0 founds a second cluster — unchecked until the nightly e2e.
PASS_LABEL="${1:-default (founder guard off)}"
CHART_ARGS="${2:-}"
OPERATOR_ARGS="${3:-}"

echo "== rendering the chart (helm template) — $PASS_LABEL =="
# Release name "mqttd" so helm's fullname collapses to "mqttd" — the operator's
# object naming from a CR named "mqttd" (mqttd.fullname vs metadata.name).
# shellcheck disable=SC2086 # deliberate word-splitting of the per-pass flags
helm template mqttd "$CHART" \
  --namespace default \
  --set image.repository=ghcr.io/mbilling/fss-mqtt-broker \
  --set image.tag=0.9.0 \
  --set-file config="$CHART/parity-config.toml" \
  $CHART_ARGS \
  > "$WORK/chart.yaml"

echo "== rendering the operator (cargo run --bin render_sample) — $PASS_LABEL =="
# shellcheck disable=SC2086
cargo run --quiet -p mqttd-operator --bin render_sample -- $OPERATOR_ARGS > "$WORK/operator.json"

# helm emits multi-document YAML, so the comparison needs a YAML reader. Most CI
# images ship PyYAML; a machine that does not gets a throwaway venv (never the
# system interpreter — PEP 668 makes that an error on modern macOS/Debian).
PY_BIN=python3
if ! python3 -c "import yaml" >/dev/null 2>&1; then
  echo "== provisioning a throwaway venv for PyYAML =="
  python3 -m venv "$WORK/venv" >/dev/null
  "$WORK/venv/bin/pip" install --quiet pyyaml
  PY_BIN="$WORK/venv/bin/python"
fi

echo "== comparing =="
"$PY_BIN" - "$WORK/chart.yaml" "$WORK/operator.json" <<'PY'
import json, sys, yaml

chart_path, operator_path = sys.argv[1], sys.argv[2]

chart_docs = [d for d in yaml.safe_load_all(open(chart_path)) if d]
operator_docs = json.load(open(operator_path))

# ---- NORMALIZE: differences that are correct by construction ----------------
#
#  * helm.sh/chart, app.kubernetes.io/managed-by, app.kubernetes.io/version:
#    provenance labels — Helm stamps its chart/release, the operator stamps
#    itself. Their VALUES must differ; their presence is not load-bearing.
#  * metadata.namespace: helm template omits it unless set on the object; the
#    operator always sets it explicitly from the CR.
#  * ServiceMonitor: chart-optional (Prometheus-Operator only), not owned by the
#    operator. ServiceAccount IS compared — the StatefulSet references it, so an
#    operator that failed to create one could not start a pod (found by the T7 e2e).
PROVENANCE = {
    "helm.sh/chart",
    "app.kubernetes.io/managed-by",
    "app.kubernetes.io/version",
}
IGNORED_KINDS = {"ServiceMonitor"}

def strip(node):
    """Recursively drop provenance labels and namespace so the comparison is
    about the SHAPE both paths must agree on."""
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k in PROVENANCE or k == "namespace":
                continue
            out[k] = strip(v)
        return out
    if isinstance(node, list):
        return [strip(v) for v in node]
    return node

def key(doc):
    return (doc["kind"], doc["metadata"]["name"])

chart = {key(d): strip(d) for d in chart_docs if d["kind"] not in IGNORED_KINDS}
operator = {key(d): strip(d) for d in operator_docs if d["kind"] not in IGNORED_KINDS}

failures = []

only_chart = sorted(set(chart) - set(operator))
only_operator = sorted(set(operator) - set(chart))
if only_chart:
    failures.append(f"objects the CHART renders but the operator does not: {only_chart}")
if only_operator:
    failures.append(f"objects the OPERATOR renders but the chart does not: {only_operator}")

for k in sorted(set(chart) & set(operator)):
    a = json.dumps(chart[k], indent=2, sort_keys=True).splitlines()
    b = json.dumps(operator[k], indent=2, sort_keys=True).splitlines()
    if a != b:
        import difflib
        diff = "\n".join(difflib.unified_diff(a, b, fromfile=f"chart {k}", tofile=f"operator {k}", lineterm=""))
        failures.append(f"{k[0]}/{k[1]} differs:\n{diff}")

if failures:
    print("RENDER PARITY FAILED — the chart and the operator disagree.\n")
    for f in failures:
        print(f)
        print()
    print("Fix BOTH paths (deploy/helm/mqttd/templates/* and")
    print("crates/mqttd-operator/src/render.rs), or narrow the normalization in")
    print("scripts/k8s/render-parity.sh if the difference is genuinely by construction.")
    sys.exit(1)

print(f"RENDER PARITY OK — {len(chart)} objects identical "
      f"({', '.join(sorted(f'{k[0]}/{k[1]}' for k in chart))})")
PY
