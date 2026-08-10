#!/usr/bin/env bash
# Run the parity comparison in BOTH founder-guard states (ADR 0055 T9): the default
# (bootstrap-capable) render, and the armed render where ordinal 0 seeds to its peers.
# A drift in either is a drift.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
"$HERE/render-parity-one.sh" "default (founder guard off)" "" ""
"$HERE/render-parity-one.sh" "founder guard ARMED" "--set clusterEstablished=true" "--established"
echo
echo "RENDER PARITY OK IN BOTH FOUNDER-GUARD STATES"
