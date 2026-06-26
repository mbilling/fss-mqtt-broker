#!/usr/bin/env python3
"""Generate the delivery dashboard (docs/delivery/STATUS.md) and refresh the
per-doc progress tables, from the frontmatter in each docs/delivery/NNNN-*.md.

Frontmatter is the single source of truth (see docs/delivery/README.md). This
script never invents status; it only renders what the delivery docs declare, and
lists ADRs that have no delivery doc yet as "not migrated" using their ADR Status
line. Run it after editing any delivery doc; CI checks the output is committed.

Usage:
    python3 scripts/gen-status.py            # write files
    python3 scripts/gen-status.py --check    # exit 1 if anything would change
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ADR_DIR = ROOT / "docs" / "adr"
DELIVERY_DIR = ROOT / "docs" / "delivery"
STATUS_FILE = DELIVERY_DIR / "STATUS.md"

# Controlled task-status vocabulary -> display label. Order is the dashboard sort
# order for the "open work" view (most actionable first).
STATUS_LABEL = {
    "in-progress": "🚧 in-progress",
    "blocked": "⛔ blocked",
    "planned": "⬜ planned",
    "done": "✅ done",
    "deferred": "💤 deferred",
    "cut": "✂️ cut",
}
OPEN_STATUSES = ("in-progress", "blocked", "planned")
KNOWN_ADR_STATUS = {"Proposed", "Accepted", "Superseded", "Deprecated"}


class DocError(Exception):
    pass


def parse_frontmatter(text: str, path: Path) -> dict:
    """Parse the constrained YAML subset the schema allows: flat `key: value`
    scalars plus a `tasks:` list of `- id:`-led items with indented fields."""
    if not text.startswith("---\n"):
        raise DocError(f"{path.name}: missing frontmatter")
    end = text.find("\n---", 4)
    if end == -1:
        raise DocError(f"{path.name}: unterminated frontmatter")
    block = text[4:end].split("\n")

    meta: dict = {"tasks": []}
    in_tasks = False
    cur: dict | None = None
    for raw in block:
        if not raw.strip():
            continue
        if raw == "tasks:":
            in_tasks = True
            continue
        if not in_tasks:
            key, _, val = raw.partition(":")
            meta[key.strip()] = val.strip()
            continue
        # task list
        m = re.match(r"^  - (\w+):\s*(.*)$", raw)
        if m:
            cur = {m.group(1): m.group(2).strip()}
            meta["tasks"].append(cur)
            continue
        m = re.match(r"^    (\w+):\s*(.*)$", raw)
        if m and cur is not None:
            cur[m.group(1)] = m.group(2).strip()
            continue
        raise DocError(f"{path.name}: cannot parse frontmatter line: {raw!r}")

    for req in ("adr", "title", "adr_status"):
        if req not in meta:
            raise DocError(f"{path.name}: frontmatter missing '{req}'")
    for t in meta["tasks"]:
        if "id" not in t or "title" not in t or "status" not in t:
            raise DocError(f"{path.name}: a task is missing id/title/status")
        if t["status"] not in STATUS_LABEL:
            raise DocError(f"{path.name}: task {t.get('id')} has unknown status {t['status']!r}")
        if t["status"] == "done" and not t.get("evidence"):
            raise DocError(f"{path.name}: done task {t['id']} needs 'evidence'")
        if t["status"] in ("blocked", "deferred") and not t.get("notes"):
            raise DocError(f"{path.name}: {t['status']} task {t['id']} needs 'notes'")
    return meta


def task_table(tasks: list[dict]) -> str:
    rows = ["| Task | Status | When | Evidence / notes |", "|------|--------|------|------------------|"]
    for t in tasks:
        when = t.get("date", "—") or "—"
        info = t.get("evidence") or t.get("notes") or ""
        rows.append(f"| {t['id']} | {STATUS_LABEL[t['status']]} | {when} | {info} |")
    return "\n".join(rows)


def refresh_doc_table(path: Path, meta: dict) -> str:
    """Return `path`'s text with the region between the status-table markers
    replaced by the rendered table."""
    text = path.read_text()
    adr = meta["adr"].split(",")[0].strip().strip('"')
    begin = f"<!-- status-table:{adr} -->"
    finish = f"<!-- /status-table:{adr} -->"
    if begin not in text or finish not in text:
        raise DocError(f"{path.name}: missing status-table markers for {adr}")
    pre = text[: text.index(begin) + len(begin)]
    post = text[text.index(finish):]
    return f"{pre}\n{task_table(meta['tasks'])}\n{post}"


def adr_status_line(num: str) -> str:
    for p in ADR_DIR.glob(f"{num}-*.md"):
        for line in p.read_text().splitlines():
            m = re.match(r"^- \*\*Status:\*\*\s*(.*)$", line)
            if m:
                return m.group(1).strip()
    return "—"


def adr_title(num: str) -> str:
    for p in ADR_DIR.glob(f"{num}-*.md"):
        first = p.read_text().splitlines()[0]
        return re.sub(r"^# ADR \d+ —\s*", "", first).strip()
    return "?"


def adr_file(num: str) -> str | None:
    """The ADR markdown filename for `num`, or None if there is no ADR file."""
    for p in ADR_DIR.glob(f"{num}-*.md"):
        return p.name
    return None


def build_dashboard(docs: list[dict]) -> str:
    by_adr = {d["adr"].split(",")[0].strip().strip('"'): d for d in docs}
    all_nums = sorted(p.name[:4] for p in ADR_DIR.glob("0*.md"))

    out = [
        "# Delivery status",
        "",
        "> **Generated** by `scripts/gen-status.py` from the frontmatter in each",
        "> `docs/delivery/NNNN-*.md`. Do not edit by hand. See",
        "> [README.md](README.md) for the artifact model and status vocabulary.",
        "",
        "## Decisions and their build progress",
        "",
        "| ADR | Title | Decision | Tasks | Open / deferred |",
        "|-----|-------|----------|-------|-----------------|",
    ]
    for num in all_nums:
        title = adr_title(num)
        # Link the ADR number to its decision record; this dashboard is the canonical ADR
        # catalogue, so the navigation the old hand-maintained index gave lives here now.
        af = adr_file(num)
        num_cell = f"[{num}](../adr/{af})" if af else num
        d = by_adr.get(num)
        if d is None:
            out.append(f"| {num_cell} | {title} | {adr_status_line(num)} | _not migrated_ | — |")
            continue
        tasks = d["tasks"]
        done = sum(1 for t in tasks if t["status"] == "done")
        opened = sum(1 for t in tasks if t["status"] in OPEN_STATUSES)
        deferred = sum(1 for t in tasks if t["status"] == "deferred")
        prog = f"{done}/{len(tasks)} done" if tasks else "—"
        # Link the progress to the delivery doc (same dir as this file) for the task detail.
        prog_cell = f"[{prog}]({d['_path'].name})"
        tail = []
        if opened:
            tail.append(f"{opened} open")
        if deferred:
            tail.append(f"{deferred} deferred")
        out.append(f"| {num_cell} | {title} | {d['adr_status']} | {prog_cell} | {', '.join(tail) or '—'} |")

    # Open + deferred detail across all migrated docs.
    out += ["", "## Open and deferred work", ""]
    any_item = False
    for num in all_nums:
        d = by_adr.get(num)
        if not d:
            continue
        items = [t for t in d["tasks"] if t["status"] in OPEN_STATUSES or t["status"] == "deferred"]
        if not items:
            continue
        any_item = True
        out.append(f"**{num} — {adr_title(num)}**")
        out.append("")
        for t in items:
            note = t.get("notes") or t.get("evidence") or ""
            suffix = f" — {note}" if note else ""
            out.append(f"- `{t['id']}` {STATUS_LABEL[t['status']]}: {t['title']}{suffix}")
        out.append("")
    if not any_item:
        out += ["_None — every migrated task is done or cut._", ""]

    return "\n".join(out).rstrip() + "\n"


def main() -> int:
    check = "--check" in sys.argv
    docs = []
    for p in sorted(DELIVERY_DIR.glob("0*.md")):
        docs.append(parse_frontmatter(p.read_text(), p))
        docs[-1]["_path"] = p

    changed = []
    for d in docs:
        p = d["_path"]
        new = refresh_doc_table(p, d)
        if new != p.read_text():
            changed.append(p)
            if not check:
                p.write_text(new)

    dashboard = build_dashboard(docs)
    if not STATUS_FILE.exists() or STATUS_FILE.read_text() != dashboard:
        changed.append(STATUS_FILE)
        if not check:
            STATUS_FILE.write_text(dashboard)

    if check and changed:
        names = ", ".join(c.name for c in changed)
        print(f"out of date (run scripts/gen-status.py): {names}", file=sys.stderr)
        return 1
    if not check:
        print(f"wrote {STATUS_FILE.relative_to(ROOT)} ({len(docs)} delivery docs)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
