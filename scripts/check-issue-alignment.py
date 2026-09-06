#!/usr/bin/env python3
"""Check the delivery records and GitHub issues still agree.

`gen-status.py` enforces the half that needs no network: every open task names
an issue. This script enforces the other half, which does — that the issue is
actually open, and that a closed issue is not still carrying an open task.

The two together are the whole contract: work that needs doing exists in exactly
one place as a plan (the issue) and one place as a status (the delivery task),
and neither can move without the other being wrong and saying so.

Usage:
    python3 scripts/check-issue-alignment.py           # report and exit 1 on drift
    python3 scripts/check-issue-alignment.py --quiet   # only print problems

Requires `gh` authenticated against the repo. Skips cleanly (exit 0) when `gh`
is unavailable, so a local run without it is not a hard stop; CI has it.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DELIVERY_DIR = ROOT / "docs" / "delivery"
OPEN_STATUSES = ("planned", "in-progress", "blocked")


def tasks() -> list[dict]:
    """Every task carrying an `issue:`, with its doc, id and status."""
    out = []
    for path in sorted(DELIVERY_DIR.glob("0*.md")):
        text = path.read_text()
        if not text.startswith("---\n"):
            continue
        block = text[4 : text.find("\n---", 4)]
        cur: dict | None = None
        for line in block.split("\n"):
            m = re.match(r"^  - id:\s*(\S+)", line)
            if m:
                cur = {"id": m.group(1), "doc": path.name}
                out.append(cur)
                continue
            m = re.match(r"^    (\w+):\s*(.*)$", line)
            if m and cur is not None:
                cur[m.group(1)] = m.group(2).strip()
    return [t for t in out if t.get("issue")]


def issue_states(numbers: list[str]) -> dict[str, str]:
    """Map issue number -> OPEN/CLOSED, in one API call."""
    if not numbers:
        return {}
    proc = subprocess.run(
        ["gh", "issue", "list", "--state", "all", "--limit", "1000",
         "--json", "number,state"],
        capture_output=True, text=True, check=True,
    )
    return {str(r["number"]): r["state"] for r in json.loads(proc.stdout)}


def main() -> int:
    quiet = "--quiet" in sys.argv
    ts = tasks()
    try:
        states = issue_states([t["issue"] for t in ts])
    except (FileNotFoundError, subprocess.CalledProcessError) as e:
        print(f"skipping issue alignment: gh unavailable ({e})", file=sys.stderr)
        return 0

    problems = []
    for t in ts:
        num, status = t["issue"], t["status"]
        state = states.get(num)
        if state is None:
            problems.append(
                f"{t['doc']}: task {t['id']} names issue #{num}, which does not exist "
                "in this repo"
            )
        elif status in OPEN_STATUSES and state == "CLOSED":
            problems.append(
                f"{t['doc']}: task {t['id']} is {status} but issue #{num} is CLOSED — "
                "either the task shipped (set it done + evidence) or the issue was "
                "closed too early"
            )
        elif status == "done" and state == "OPEN":
            problems.append(
                f"{t['doc']}: task {t['id']} is done but issue #{num} is still OPEN — "
                "close the issue, or the task is not actually done"
            )

    if problems:
        print("delivery/issue alignment drift:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1
    if not quiet:
        print(f"aligned: {len(ts)} tasks reference issues, all consistent")
    return 0


if __name__ == "__main__":
    sys.exit(main())
