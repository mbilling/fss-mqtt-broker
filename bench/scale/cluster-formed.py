#!/usr/bin/env python3
"""Does one broker see a fully formed cluster of N members?

    cluster-formed.py <members> <voter-cap> < view     exit 0 when formed, else 1 and why
    cluster-formed.py --self-test

A cluster growing by a join (bootstrap-cluster.sh JOIN_FROM, rig2/local-cluster.sh join)
starts its next joiner, and counts as grown, only when every member's view is formed. A
joiner's own /readyz is not that: it answers ready at its majority floor, seconds before
the other members have it in their membership, and a second joiner started then overlaps
the first one's join.

`view` is the broker's `/readyz?verbose=1` body on its first line, then its
`mqttd_members`, `mqttd_peer_links` and `mqttd_voters` samples from /metrics. The view is
formed when the broker:

- is ready, with a ready lease group;
- sees <members> SWIM members alive, and none suspect or dead;
- has a peer link to each of the other <members> - 1;
- counts min(<members>, <voter-cap>) lease voters (MQTTD_LEASE_VOTERS, 5 by default).
"""
from __future__ import annotations

import json
import sys


def why_not(members: int, voter_cap: int, view: str) -> list[str]:
    """What keeps this broker's view from being formed; empty when it is."""
    lines = view.splitlines()
    try:
        ready = json.loads(lines[0]) if lines and lines[0].strip() else {}
    except ValueError:
        ready = {}
    samples: dict[str, float] = {}
    for line in lines[1:]:
        key, _, value = line.strip().rpartition(" ")
        try:
            samples[key] = float(value)
        except ValueError:
            continue
    out = []
    if ready.get("ready") is not True:
        out.append("not ready")
    if ready.get("lease_group_ready") is False:
        out.append("lease group not ready")
    alive = samples.get('mqttd_members{state="alive"}', 0.0)
    if alive < members:
        out.append(f"{alive:.0f} of {members} members alive")
    for state in ("suspect", "dead"):
        n = samples.get(f'mqttd_members{{state="{state}"}}', 0.0)
        if n:
            out.append(f"{n:.0f} member(s) {state}")
    links = samples.get("mqttd_peer_links", 0.0)
    if links < members - 1:
        out.append(f"{links:.0f} of {members - 1} peer links")
    want = min(members, voter_cap)
    voters = samples.get("mqttd_voters", 0.0)
    if voters < want:
        out.append(f"{voters:.0f} of {want} voters")
    return out


def self_test() -> int:
    formed = ('{"ready":true,"members":5,"lease_group_ready":true,"voters":5}\n'
              'mqttd_members{state="alive"} 5\nmqttd_members{state="suspect"} 0\n'
              'mqttd_members{state="dead"} 0\nmqttd_peer_links 4\nmqttd_voters 5\n')
    cases = [
        (5, 5, formed, []),
        (3, 5, formed, []),  # more than enough: a view of 5 holds a cluster of 3
        (5, 5, formed.replace("mqttd_peer_links 4", "mqttd_peer_links 3"), ["3 of 4 peer links"]),
        (5, 5, formed.replace('suspect"} 0', 'suspect"} 1'), ["1 member(s) suspect"]),
        (5, 5, formed.replace('alive"} 5', 'alive"} 4'), ["4 of 5 members alive"]),
        (5, 5, formed.replace("mqttd_voters 5", "mqttd_voters 3"), ["3 of 5 voters"]),
        (7, 5, formed.replace('alive"} 5', 'alive"} 7').replace("mqttd_peer_links 4", "mqttd_peer_links 6"), []),
        (5, 5, formed.replace('"ready":true', '"ready":false'), ["not ready"]),
        (5, 5, "", ["not ready", "0 of 5 members alive", "0 of 4 peer links", "0 of 5 voters"]),
    ]
    bad = 0
    for members, cap, view, want in cases:
        got = why_not(members, cap, view)
        if got != want:
            print(f"cluster-formed self-test: {members}/{cap}: got {got}, want {want}", file=sys.stderr)
            bad += 1
    if bad:
        return 1
    print(f"cluster-formed self-test: {len(cases)} cases OK")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["--self-test"]:
        return self_test()
    if len(argv) != 2:
        print(__doc__.strip().splitlines()[2], file=sys.stderr)
        return 2
    why = why_not(int(argv[0]), int(argv[1]), sys.stdin.read())
    if why:
        print("; ".join(why))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
