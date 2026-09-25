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
        m = json.loads((d / "manifest.json").read_text())
        pub = [
            d / (v["name"] + ".ledger") for v in m["endpoints"] if v["role"] == "pub"
        ]
        sub = [
            d / (v["name"] + ".ledger") for v in m["endpoints"] if v["role"] == "sub"
        ]
        s, st = e.ledger(pub, "sent")
        a, at = e.ledger(pub, "acked")
        r, rt = e.ledger(sub, "received")
        v = e.reconcile(s, a, r)
        v.update(
            nodes=int(d.parent.parent.name.split("=")[1]),
            rung=d.name,
            sent=st,
            acked=at,
            received=rt,
            duplicates=rt - v["unique_delivered"],
        )
        for paths, metric, total in [
            (pub, "audit_sent", st),
            (pub, "audit_acked", at),
            (sub, "recv", rt),
        ]:
            observed = sum(
                e.value(e.metric(d / (p.stem + "-terminal.prom")), metric)
                for p in paths
            )
            if observed != total:
                raise ValueError(
                    f"{metric} terminal/ledger mismatch: {observed} != {total}"
                )
        v["terminal_counters_match"] = True
        v["identities_close"] = (
            bool(st)
            and st == sum(bits.bit_count() for bits in s.values())
            and not any(
                v[k] for k in ("missing", "unexpected", "unacked", "unexpected_acks")
            )
        )
    except (OSError, ValueError, KeyError) as ex:
        v = {"rung": d.name, "error": str(ex)}
    rows.append(v)
(root / "ledger-only.json").write_text(json.dumps(rows, indent=2))
print(json.dumps(rows, indent=2))
