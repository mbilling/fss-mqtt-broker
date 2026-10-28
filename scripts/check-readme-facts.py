#!/usr/bin/env python3
"""Guard the README's *derivable* facts against the tree they describe.

Some README statements are not opinions or descriptions — they are counts and
lists with exactly one correct value, readable from the filesystem. Those drift
silently: nothing fails when a crate is added or an ADR is written, so the
number in the prose quietly becomes false. The README claimed "44 ADRs" while
55 existed, and omitted two crates entirely.

This checks only facts with a mechanical source of truth. Prose stays a human
judgement; a wrong *number* is now a failed build.

Since 2026-10-25 the README is a short front page: its numbers sections live in
docs/benchmarks/EVIDENCE.md and its crate table in docs/ARCHITECTURE.md, and the
checks on them moved along. The figures the README itself states are held to
docs/benchmarks/data/headline.csv, and that file to the cards it cites.

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
EVIDENCE = ROOT / "docs" / "benchmarks" / "EVIDENCE.md"
ARCHITECTURE = ROOT / "docs" / "ARCHITECTURE.md"
HEADLINE = ROOT / "docs" / "benchmarks" / "data" / "headline.csv"


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


def anchors_of(doc: Path) -> set[str]:
    """Every fragment GitHub generates for a file's headings, duplicates suffixed -1, -2."""
    body = re.sub(r"^```.*?^```", "", doc.read_text(encoding="utf-8"), flags=re.MULTILINE | re.DOTALL)
    seen: dict[str, int] = {}
    out = set()
    for h in re.findall(r"^#{1,6}\s+(.+?)\s*#*\s*$", body, re.MULTILINE):
        a = github_anchor(re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", h).replace("`", ""))
        n = seen.get(a, 0)
        out.add(a if n == 0 else f"{a}-{n}")
        seen[a] = n + 1
    return out


def digits(text: str) -> set[int]:
    """Every integer a text states, with thousands written as 1,234 or 1 234 or plain."""
    out = set()
    for m in re.findall(r"(?<![\d.])\d{1,3}(?:[,\u202f ]\d{3})+(?![\d])|(?<![\d.,])\d+(?![\d,])", text):
        out.add(int(re.sub(r"[,\u202f ]", "", m)))
    return out


def headline_rows() -> list[dict]:
    import csv

    with HEADLINE.open(encoding="utf-8") as f:
        return list(csv.DictReader(line for line in f if not line.startswith("#")))


def compact(v: float) -> str:
    """chart_style.compact: 90003 -> 90k, 1140000 -> 1.14M."""
    if v >= 1_000_000:
        return f"{v / 1e6:.2f}".rstrip("0").rstrip(".") + "M"
    return f"{v / 1000:.0f}k" if v >= 1000 else f"{v:.0f}"


def headline_figures(rows: list[dict]) -> set[str]:
    """Every way the README may write a headline.csv figure: 90,003 · 90k · 30.0k per node
    · 7,500 per vCPU · and the ratios the charts print (2.7×, 1.7×)."""
    out: set[str] = set()
    by_chart: dict[str, dict[str, float]] = {}
    for r in rows:
        v, n, vcpu = int(r["msg_per_s"]), int(r["nodes"]), int(r["vcpu"])
        pn = v / n
        out |= {f"{v:,}", compact(v), f"{pn / 1000:.1f}k", f"{pn / 1000:.0f}k", f"{v / vcpu:,.0f}"}
        by_chart.setdefault(r["chart"], {})[r["series"]] = v / vcpu if r["chart"] == "durable-per-vcpu" else v
    for vals in by_chart.values():
        for a in vals.values():
            for b in vals.values():
                if a > b:
                    out.add(f"{a / b:.1f}×")
    # The cost chart's ratios: HiveMQ's published per-vCPU rate against each measured
    # durable QoS 1 point (the highest step at ack p99 <= 1 s), at the same vCPU price.
    hive = [r for r in rows if r["chart"] == "durable-per-vcpu" and r["series"] == "hivemq"]
    if hive:
        hv = int(hive[0]["msg_per_s"]) / int(hive[0]["vcpu"])
        for r in rows:
            if r["chart"] == "scale-out" and r["series"] == "durable" and r["state"] == "green":
                out.add(f"{int(r['msg_per_s']) / int(r['vcpu']) / hv:.1f}×")
    return out


def scale_figures(text: str) -> set[str]:
    """The figures a scale section or card states, in a comparable form.

    Thousands-separated counts (`233,716,926`), per-node rates (`20.7k`) and
    percentages (`0.00%`). A range is one figure, kept whole (`19.2–21.1k`,
    `13–17%`): a card full of percentages contains most single numbers somewhere,
    so a range checked end by end would pass with either end changed.
    """
    out = set(re.findall(r"(?<![\d.,])\d{1,3}(?:,\d{3})+(?![\d,]*\.\d)", text))
    out |= set(re.findall(r"(?<![\d.,–])(?:\d+\.\d+–)?\d+\.\d+k\b", text))
    out |= set(re.findall(r"(?<![\d.,–])(?:\d+(?:\.\d+)?–)?\d+(?:\.\d+)?%", text))
    return out


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
    for doc in (README, ARCHITECTURE):
        for stated_adrs in re.findall(r"\((\d+) ADRs, per-task status\)", doc.read_text(encoding="utf-8")):
            if int(stated_adrs) != len(adrs):
                problems.append(
                    f"{doc.relative_to(ROOT)} claims {stated_adrs} ADRs; docs/adr/ holds {len(adrs)}"
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
    # Every workspace crate must appear in docs/ARCHITECTURE.md's Workspace crates
    # table (the README's crate table moved there on 2026-10-25). A crate nobody
    # mentions is one nobody can evaluate.
    crates = sorted(
        p.name for p in (ROOT / "crates").iterdir() if (p / "Cargo.toml").exists()
    )
    arch = ARCHITECTURE.read_text(encoding="utf-8")
    crate_section = re.search(r"^## Workspace crates\n(.*?)(?=^## )", arch, re.MULTILINE | re.DOTALL)
    if not crate_section:
        problems.append(
            "docs/ARCHITECTURE.md's '## Workspace crates' section is gone — restore it "
            "or update this script"
        )
    listed = set(re.findall(r"^\| `([a-z0-9-]+)` \|",
                            crate_section.group(1) if crate_section else "", re.MULTILINE))
    for crate in crates:
        if crate not in listed:
            problems.append(f"crate `{crate}` is missing from docs/ARCHITECTURE.md's Workspace crates table")
    for name in sorted(listed - set(crates)):
        problems.append(
            f"docs/ARCHITECTURE.md's Workspace crates table lists `{name}`, which is not a crate in crates/"
        )

    # --- intra-document anchors ------------------------------------------
    # A `#section` link that points at a renamed heading does not error, it just
    # silently does nothing — the reader clicks and stays put. The README now
    # leans on them (a jump list, and the Status paragraph pointing at
    # Limitations), so they are worth holding to the same standard as the counts.
    # Held for the README and for docs/benchmarks/EVIDENCE.md (which took the README's
    # numbers sections and their anchors), both for `#section` links inside the file
    # and for `other.md#section` links into another tracked markdown file.
    import posixpath

    anchor_count = 0
    for doc in (README, EVIDENCE):
        doc_text = doc.read_text(encoding="utf-8")
        rel = doc.relative_to(ROOT).as_posix()
        own = anchors_of(doc)
        anchor_count += len(own)
        for link in sorted(set(re.findall(r"\]\(#([^)]+)\)", doc_text))):
            if link not in own:
                problems.append(f"link to #{link} matches no heading in {rel}")
        for target, frag in sorted(set(re.findall(r"\]\(([^)#\s:]+\.md)#([^)\s]+)\)", doc_text))):
            path = ROOT / posixpath.normpath(posixpath.join(doc.parent.relative_to(ROOT).as_posix(), target))
            if path.is_file() and frag not in anchors_of(path):
                problems.append(f"{rel} links to {target}#{frag}, which matches no heading there")

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

    # --- ADR 0070 T8: docs/README.md is a complete, stamped index --------------
    # Every top-level docs/*.md must appear as a markdown link in docs/README.md
    # (the orphan class dies). Stakeholder docs carry the version-stamp
    # convention THREAT-MODEL/HARDENING established; generated files stamp
    # themselves with the GENERATED header.
    docs_index = ROOT / "docs" / "README.md"
    if not docs_index.exists():
        problems.append("docs/README.md is missing — ADR 0070 T1/T8")
    else:
        index_text = docs_index.read_text(encoding="utf-8")
        top_level = sorted(
            p for p in (ROOT / "docs").glob("*.md") if p.is_file()
        )
        for doc in top_level:
            if doc.name == "README.md":
                continue
            name = doc.name
            if f"]({name})" not in index_text and f"](./{name})" not in index_text:
                problems.append(
                    f"docs/{name} is not listed in docs/README.md — "
                    "an unlisted doc is the orphan class ADR 0070 closes"
                )
        if not re.search(
            r"\|\s*Document\s*\|\s*Stakeholder\s*\|\s*One line\s*\|",
            index_text,
            re.I,
        ):
            problems.append(
                "docs/README.md is missing the Document / Stakeholder / One line table"
            )

        stamp = re.compile(
            r"(\*\*Verified against|\*\*Dated |\*\*Generated against|<!-- GENERATED)",
        )
        skip_stamp = {"README.md"}  # the index itself; it links the stamped set
        for doc in top_level:
            if doc.name in skip_stamp:
                continue
            head = "\n".join(doc.read_text(encoding="utf-8").splitlines()[:12])
            if not stamp.search(head):
                problems.append(
                    f"docs/{doc.name} has no version stamp in its header "
                    "(**Verified against**, **Dated**, **Generated against**, "
                    "or a GENERATED banner) — ADR 0070 T8"
                )

    # --- EVIDENCE.md's figures are the results card's, figure for figure ---------
    # "The figures" publishes the benchmark rig's capacity per tier and workload, and the
    # per-node rates derived from them. The card (docs/benchmarks/RIG2-RESULTS.md) is the
    # record (the raw runs are untracked), so every figure in the section must be one the
    # card states: a number edited here alone, or left behind when the card is corrected,
    # is a claim with no source. Thousands-separated counts (`292,200`) and per-node rates
    # (`39.3k`) are held to it; prose numbers (node counts, `p99 ≤ 1 s`) are left to the
    # reader. This replaced the 1.2.0 scale section's check when the page moved to the
    # ADR 0090 rig on 2026-10-28.
    evidence_text = EVIDENCE.read_text(encoding="utf-8")
    section = re.search(r"^## The figures\n(.*?)(?=^#{2,3} )", evidence_text, re.MULTILINE | re.DOTALL)
    figure_count = 0
    card = ROOT / "docs" / "benchmarks" / "RIG2-RESULTS.md"
    if not section:
        problems.append("docs/benchmarks/EVIDENCE.md's '## The figures' section is gone — restore it or update this script")
    elif not card.is_file():
        problems.append("docs/benchmarks/RIG2-RESULTS.md, the card EVIDENCE.md's figures come from, is missing")
    else:
        stated = scale_figures(card.read_text(encoding="utf-8"))
        stated |= set(re.findall(r"(?<![\d.,])\d+k\b", card.read_text(encoding="utf-8")))
        body = section.group(1)
        quoted = scale_figures(body) | set(re.findall(r"(?<![\d.,])\d+k\b", body))
        figure_count = len(quoted)
        for figure in sorted(quoted - stated):
            problems.append(
                f"EVIDENCE.md's '## The figures' says {figure}, which docs/benchmarks/RIG2-RESULTS.md does not state"
            )
        if not quoted:
            problems.append("EVIDENCE.md's '## The figures' quotes no figure at all")

    # --- the README's evidence figures are headline.csv's, and its rows are their cards' --
    # The README's header, Why mqttd and Evidence sections quote throughput figures
    # (90,003 · 270k · 27.0k · 7,500 · 2.7×). Each must be one headline.csv derives,
    # so a number edited in the prose alone fails; and each CSV row's msg_per_s must be
    # stated in the card it cites, so the CSV cannot drift from the record either. The
    # charts drawn from the CSV must match what is committed.
    rows = headline_rows()
    allowed = headline_figures(rows)
    front = re.split(r"^## Feature matrix", text, maxsplit=1, flags=re.MULTILINE)[0]
    stated_front = set(re.findall(r"(?<![\w.,])\d{1,3}(?:,\d{3})+(?![\d,])", front))
    stated_front |= set(re.findall(r"(?<![\w.,])\d+(?:\.\d+)?[kM](?![\w])", front))
    stated_front |= set(re.findall(r"(?<![\w.,])\d+(?:\.\d+)?×", front))
    headline_count = len(stated_front)
    for figure in sorted(stated_front - allowed):
        problems.append(
            f"the README states {figure} before its Feature matrix, which "
            "docs/benchmarks/data/headline.csv does not derive — add the row (citing its card) "
            "or correct the figure"
        )
    import csv as _csv

    per_vcpu_rows = list(_csv.DictReader(
        line for line in (ROOT / "docs/benchmarks/data/per-vcpu.csv").open(encoding="utf-8")
        if not line.startswith("#")))
    for r in rows:
        src = ROOT / r["source"]
        if not src.is_file():
            problems.append(f"headline.csv row {r['label']!r} cites {r['source']}, which does not exist")
            continue
        if src.suffix == ".csv":
            ok = any(p["class"] == r["section"] and p["broker"] + " " + p["version"] in r["label"]
                     and int(p["msg_per_s"]) == int(r["msg_per_s"]) and int(p["vcpu"]) == int(r["vcpu"])
                     for p in per_vcpu_rows)
        else:
            # The figure must be in the section the row names: a heading path such as
            # "10 nodes / Durable QoS 1" (each part a heading, nested in order), so a figure
            # that appears elsewhere in the card does not vouch for this row.
            body = src.read_text(encoding="utf-8")
            scope, found = body, True
            for part in [p.strip() for p in r["section"].split(" / ")]:
                m = re.search(rf"^(#+) {re.escape(part)}\s*$", scope, re.MULTILINE)
                if not m:
                    found = False
                    break
                level = len(m.group(1))
                rest = scope[m.end():]
                end = re.search(rf"^#{{1,{level}}} ", rest, re.MULTILINE)
                scope = rest[: end.start()] if end else rest
            if not found:
                problems.append(f"headline.csv row {r['label']!r} cites section {r['section']!r}, "
                                f"which {r['source']} has no heading for")
                continue
            ok = int(r["msg_per_s"]) in digits(scope)
        if not ok:
            problems.append(
                f"headline.csv row {r['label']!r} ({r['nodes']} nodes) says {int(r['msg_per_s']):,} msg/s, "
                f"which {r['source']} ({r['section']}) does not state"
            )
    import runpy

    chart = runpy.run_path(str(ROOT / "bench/scale/chart-readme.py"))
    for name, fn in chart["CHARTS"].items():
        committed = ROOT / "docs/benchmarks/img" / name
        if not committed.is_file() or committed.read_text(encoding="utf-8") != fn(chart["load"]()):
            problems.append(
                f"docs/benchmarks/img/{name} is not what bench/scale/chart-readme.py draws from "
                "headline.csv — run it and commit the result"
            )

    if problems:
        fail(problems)

    print(
        f"README facts check: {len(adrs)} ADRs, {len(crates)} crates, "
        f"{task_count} scripts, {anchor_count} anchors (README + EVIDENCE.md), "
        f"{citation_count} evidence citations across {len(cited_docs)} cited docs, "
        f"{figure_count} EVIDENCE figures found in the results card, "
        f"{headline_count} README evidence figures and {len(rows)} headline.csv rows checked, "
        f"dates in sync — all match."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
