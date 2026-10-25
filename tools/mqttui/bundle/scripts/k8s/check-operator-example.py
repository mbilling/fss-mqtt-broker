#!/usr/bin/env python3
"""The operator's example MqttdCluster names only objects the chart's bootstrap script creates.

The README, docs/KUBERNETES.md and the operator README all say: run the mqttd chart's
bootstrap script, then apply deploy/helm/mqttd-operator/example-mqttdcluster.yaml. Until
2026-10-25 the example named `mqttd-server-tls` and `mqttd-gossip-key`, which the script
never created, so that path could not start a pod.

Usage: scripts/k8s/check-operator-example.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
EXAMPLE = ROOT / "deploy/helm/mqttd-operator/example-mqttdcluster.yaml"
BOOTSTRAP = ROOT / "deploy/helm/mqttd/bootstrap.sh"


def created() -> set[str]:
    """`kubectl create secret <type> <name>` and `kubectl create configmap <name>`."""
    text = BOOTSTRAP.read_text(encoding="utf-8")
    return set(re.findall(r"kubectl create (?:secret (?:generic|tls)|configmap) ([a-z0-9-]+)", text))


def named() -> set[str]:
    """The uncommented `key: name` lines of the example's `spec.secrets` block."""
    names, inside = set(), False
    for line in EXAMPLE.read_text(encoding="utf-8").splitlines():
        if line.startswith("  secrets:"):
            inside = True
            continue
        if inside and re.match(r"^  \S", line):
            break
        if inside and (m := re.match(r"^    [A-Za-z]+: ([a-z0-9-]+)\s*$", line)):
            names.add(m.group(1))
    return names


def main() -> int:
    want, have = named(), created()
    if not want:
        print(f"FATAL: found no spec.secrets names in {EXAMPLE.relative_to(ROOT)}")
        return 1
    if missing := sorted(want - have):
        print(f"FATAL: {EXAMPLE.relative_to(ROOT)} names objects the bootstrap script does not create:")
        for name in missing:
            print(f"  {name}")
        print(f"it creates: {' '.join(sorted(have))}")
        return 1
    print(f"operator example OK: {' '.join(sorted(want))} are all created by the bootstrap script")
    return 0


if __name__ == "__main__":
    sys.exit(main())
