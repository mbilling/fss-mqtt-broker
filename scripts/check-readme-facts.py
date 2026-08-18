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
    """Every path that is in the repository, or on its way in.

    `--cached` is the tracked set; `--others --exclude-standard` adds files that
    exist and are **not** ignored — i.e. a file added in the same change but not yet
    committed. That distinction is deliberate: in CI the checkout contains only
    committed files, so `--others` is empty and the citation check is strict, while
    locally it does not fail an author for citing the artifact they just wrote. What
    it never admits is the defect this guards against — a path that is gitignored
    (`bench/results/results.md`) or does not exist at all.
    """
    out = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
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

    # --- evidence citations resolve to TRACKED files (issues #253, #244) --
    # COMPARISON.md is the document that promises trust-through-checkability, and
    # it cited `bench/results/results.md` — a path its own .gitignore keeps out of
    # the repository. A citation of an untracked path is a claim the reader cannot
    # check, which is the one defect that file must not have. Both citation forms
    # are held to it: backticked file paths and relative markdown link targets.
    # Resolution tries the citing file's own directory, then docs/, then the repo
    # root, since all three conventions appear; a directory citation counts if any
    # tracked file lives under it.
    #
    # Issue #244 widened the guarded set from COMPARISON alone to the three
    # surfaces where a dangling evidence citation is fatal rather than untidy:
    # the README (the front door), COMPARISON (the migrator's comparison), and
    # every published benchmark record (numbers with no reachable method are
    # exactly the failure #244 was filed about). `docs/delivery/` is deliberately
    # NOT in the set: those files are dated evidence prose about what was true
    # when a task closed, and rewriting history to satisfy a path check would be
    # the dishonest fix.
    tracked = tracked_files()

    def resolves_tracked(target: str, own_dir: str) -> bool:
        import posixpath

        for base in (own_dir, "docs", ""):
            candidate = posixpath.normpath(posixpath.join(base, target))
            if candidate in tracked:
                return True
            if any(t.startswith(candidate.rstrip("/") + "/") for t in tracked):
                return True  # a directory with tracked contents
        return False

    cited_docs = [README, COMPARISON] + sorted((ROOT / "docs" / "benchmarks").glob("*.md"))
    citation_count = 0
    for doc in cited_docs:
        doc_text = doc.read_text(encoding="utf-8")
        rel = doc.relative_to(ROOT).as_posix()
        own_dir = doc.parent.relative_to(ROOT).as_posix()
        own_dir = "" if own_dir == "." else own_dir
        citations = set(
            re.findall(r"`([A-Za-z0-9_./-]+/[A-Za-z0-9_.-]+\.[a-z]{2,4})`", doc_text)
        )
        citations |= {
            t for t in re.findall(r"\]\(([^)#\s]+)(?:#[^)]*)?\)", doc_text)
            if "://" not in t
        }
        citation_count += len(citations)
        for target in sorted(citations):
            if not resolves_tracked(target, own_dir):
                problems.append(
                    f"{rel} cites `{target}`, which is not a tracked file — cite a "
                    "tracked artifact or state that the evidence is unpublished"
                )

    comparison_text = COMPARISON.read_text(encoding="utf-8")

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

    # --- the 1.0 freeze is not un-flippable by prose (issue #247) --------
    # The compatibility promise's *content* is prose, but whether the pre-1.0
    # escape clause may still appear is derivable: the workspace version says
    # which regime is in force. From 1.0.0 on, the phrases that granted the
    # pre-1.0 reshape window must not survive anywhere an operator would read
    # the policy — a stale sentence would promise the right to break formats
    # that ADR 0039 has already revoked.
    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    ver = re.search(r'^version = "(\d+)\.(\d+)\.(\d+)"', cargo, re.MULTILINE)
    if ver and int(ver.group(1)) >= 1:
        stale_phrases = [
            "reshapes are permitted",
            "no cross-version compatibility is promised",
            "formats may change freely",
            "applies from 1.0.0",
            "this is pre-1.0",
        ]
        policy_docs = [README, ROOT / "docs" / "OPERATIONS.md",
                       ROOT / "CHANGELOG.md", ROOT / "RELEASING.md"]
        for doc in policy_docs:
            # Collapse all whitespace: prose wraps mid-phrase ("reshapes are\n
            # permitted"), and a phrase ban that a line break defeats is no ban.
            body = re.sub(r"\s+", " ", doc.read_text(encoding="utf-8"))
            for phrase in stale_phrases:
                if phrase in body:
                    problems.append(
                        f"{doc.relative_to(ROOT)} still says '{phrase}' — the "
                        f"workspace is {ver.group(0).split('\"')[1]}, so the "
                        "pre-1.0 reshape window is closed (ADR 0039 in force)"
                    )

    if problems:
        fail(problems)

    print(
        f"README facts check: {len(adrs)} ADRs, {len(crates)} crates, "
        f"{task_count} scripts, {len(anchors)} anchors, "
        f"{citation_count} evidence citations across {len(cited_docs)} cited docs, "
        f"dates in sync — all match."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
