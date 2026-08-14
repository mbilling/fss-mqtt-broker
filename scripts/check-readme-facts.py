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
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
COMPARISON = ROOT / "docs" / "COMPARISON.md"


def tracked_files() -> set[str]:
    """Every path git tracks, repo-relative with forward slashes."""
    out = subprocess.run(
        ["git", "ls-files", "-z"],
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    ).stdout
    return {p for p in out.split("\0") if p}


def github_anchor(heading: str) -> str:
    """The fragment GitHub generates for a heading.

    Lowercase, strip everything that is not a word character, space or hyphen,
    then spaces become hyphens. `TLS 1.3 + mTLS` → `tls-13--mtls` — the dropped
    `+` leaves the double hyphen, which is easy to get wrong by hand and is
    exactly why this is computed rather than typed.
    """
    text = heading.strip().lower()
    text = re.sub(r"[^\w\s-]", "", text)
    return re.sub(r"\s", "-", text)


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

    # --- runnable-script count -------------------------------------------
    # "There are N runnable scripts here" must match the mqttui manifest, which
    # is itself CI-gated against the tree — so this chains the README to the
    # same source of truth instead of trusting whoever last counted.
    declared = (ROOT / "tools" / "mqttui" / "tasks.toml").read_text(encoding="utf-8")
    task_count = declared.count("[[task]]")
    match = re.search(r"There are (\d+) runnable scripts here", text)
    if not match:
        problems.append(
            "the 'There are N runnable scripts here' phrase is gone — restore it "
            "or update this script"
        )
    elif int(match.group(1)) != task_count:
        problems.append(
            f"claims {match.group(1)} runnable scripts; tools/mqttui/tasks.toml "
            f"declares {task_count}"
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

    # --- intra-document anchors ------------------------------------------
    # A `#section` link that points at a renamed heading does not error, it just
    # silently does nothing — the reader clicks and stays put. The README now
    # leans on them (a jump list, and the Status paragraph pointing at
    # Limitations), so they are worth holding to the same standard as the counts.
    headings = re.findall(r"^#{2,6}\s+(.+?)\s*$", text, re.MULTILINE)
    anchors = {github_anchor(h) for h in headings}
    for link in sorted(set(re.findall(r"\]\(#([^)]+)\)", text))):
        if link not in anchors:
            problems.append(f"link to #{link} matches no heading in README.md")

    # --- COMPARISON citations resolve to TRACKED files (issue #253) ------
    # COMPARISON.md is the document that promises trust-through-checkability, and
    # it cited `bench/results/results.md` — a path its own .gitignore keeps out of
    # the repository. A citation of an untracked path is a claim the reader cannot
    # check, which is the one defect that file must not have. Both citation forms
    # are held to it: backticked file paths and relative markdown link targets.
    # Resolution tries the file's own directory (docs/) then the repo root, since
    # both conventions appear; a directory citation counts if any tracked file
    # lives under it.
    tracked = tracked_files()
    comparison_text = COMPARISON.read_text(encoding="utf-8")

    def resolves_tracked(target: str) -> bool:
        import posixpath

        for base in ("docs", ""):
            candidate = posixpath.normpath(posixpath.join(base, target))
            if candidate in tracked:
                return True
            if any(t.startswith(candidate.rstrip("/") + "/") for t in tracked):
                return True  # a directory with tracked contents
        return False

    citations = set(
        re.findall(r"`([A-Za-z0-9_./-]+/[A-Za-z0-9_.-]+\.[a-z]{2,4})`", comparison_text)
    )
    citations |= {
        t for t in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", comparison_text)
        if "://" not in t
    }
    for target in sorted(citations):
        if not resolves_tracked(target):
            problems.append(
                f"docs/COMPARISON.md cites `{target}`, which is not a tracked file — "
                "cite a tracked artifact or state that the evidence is unpublished"
            )

    # --- README's stated COMPARISON date matches the file header ---------
    # The README quoted a date the comparison's own header contradicted (#253
    # item 3); both values are mechanical, so the files must agree.
    header = re.search(r"\*\*Dated (\d{4}-\d{2}-\d{2})\.\*\*", comparison_text)
    stated = re.search(r"docs/COMPARISON\.md\)\s*\(dated (\d{4}-\d{2}-\d{2})\)", text)
    if not header:
        problems.append(
            "docs/COMPARISON.md's '**Dated YYYY-MM-DD.**' header is gone — restore "
            "it or update this script"
        )
    if not stated:
        problems.append(
            "the README's 'docs/COMPARISON.md) (dated YYYY-MM-DD)' phrase is gone — "
            "restore it or update this script"
        )
    if header and stated and header.group(1) != stated.group(1):
        problems.append(
            f"README says COMPARISON is dated {stated.group(1)}; the file's own "
            f"header says {header.group(1)}"
        )

    if problems:
        fail(problems)

    print(
        f"README facts check: {len(adrs)} ADRs, {len(crates)} crates, "
        f"{task_count} scripts, {len(anchors)} anchors, "
        f"{len(citations)} COMPARISON citations, dates in sync — all match."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
