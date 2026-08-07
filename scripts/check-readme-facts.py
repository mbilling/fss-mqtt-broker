#!/usr/bin/env python3
"""Guard the README's *derivable* facts against the tree they describe.

Some README statements are not opinions or descriptions — they are counts and
lists with exactly one correct value, readable from the filesystem. Those drift
silently: nothing fails when a crate is added or an ADR is written, so the
number in the prose quietly becomes false. The README claimed "44 ADRs" while
55 existed, and omitted two crates entirely.

This checks only facts with a mechanical source of truth. Prose stays a human
judgement; a wrong *number* is now a failed build.

Usage: scripts/check-readme-facts.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"


def fail(problems: list[str]) -> None:
    print("README states facts the tree contradicts:\n", file=sys.stderr)
    for p in problems:
        print(f"  - {p}", file=sys.stderr)
    print(
        "\nThese are derivable from the repository, so the repository wins.",
        file=sys.stderr,
    )
    sys.exit(1)


def main() -> int:
    text = README.read_text(encoding="utf-8")
    problems: list[str] = []

    # --- ADR count -------------------------------------------------------
    # Numbered decision records only; docs/adr/README.md is the index, not one.
    adrs = sorted(p for p in (ROOT / "docs" / "adr").glob("[0-9][0-9][0-9][0-9]-*.md"))
    match = re.search(r"\((\d+) ADRs, per-task status\)", text)
    if not match:
        problems.append(
            "the '(N ADRs, per-task status)' phrase is gone — this guard cannot "
            "check the count any more; restore it or update this script"
        )
    elif int(match.group(1)) != len(adrs):
        problems.append(
            f"claims {match.group(1)} ADRs; docs/adr/ holds {len(adrs)}"
        )

    # --- crate table -----------------------------------------------------
    # Every workspace crate must appear in the Workspace layout table. A crate
    # nobody mentions is one nobody can evaluate.
    crates = sorted(
        p.name for p in (ROOT / "crates").iterdir() if (p / "Cargo.toml").exists()
    )
    listed = set(re.findall(r"^\| `([a-z0-9-]+)` \|", text, re.MULTILINE))
    for crate in crates:
        if crate not in listed:
            problems.append(f"crate `{crate}` is missing from the Workspace layout table")
    for name in sorted(listed - set(crates)):
        problems.append(
            f"the Workspace layout table lists `{name}`, which is not a crate in crates/"
        )

    if problems:
        fail(problems)

    print(f"README facts check: {len(adrs)} ADRs, {len(crates)} crates — all match.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
