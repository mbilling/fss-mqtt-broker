#!/usr/bin/env python3
"""Reconcile terminal identities independently of throughput window validity."""

import argparse
import importlib.util
import json
import pathlib

p = pathlib.Path(__file__).resolve().parent.parent / "lane-e-evidence.py"
sp = importlib.util.spec_from_file_location("e", p)
e = importlib.util.module_from_spec(sp)
sp.loader.exec_module(e)
parser = argparse.ArgumentParser()
parser.add_argument("run", type=pathlib.Path)
root = parser.parse_args().run
rows = []
for d in sorted((root / "results").glob("nodes=*/laneE/sites-*")):
    if not (d / "rung.txt").exists():
        continue
    try:
        # The reconciliation itself is lane-e-evidence.py's, shared with
        # summarize-curve.py's broker-counter reading: one ledger, two readers.
        v = e.identity_ledger(d)
        v.update(nodes=int(d.parent.parent.name.split("=")[1]), rung=d.name)
    except (OSError, ValueError, KeyError) as ex:
        v = {"rung": d.name, "error": str(ex)}
    rows.append(v)
(root / "ledger-only.json").write_text(json.dumps(rows, indent=2))
print(json.dumps(rows, indent=2))
