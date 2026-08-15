#!/usr/bin/env python3
"""Two ways a test suite silently stops testing, made into build failures.

Issue #260. The test suite is this project's primary trust asset in the absence of
production miles, and it degrades in two ways that no test run reports:

1. **A skip that reports success.** A test that returns early because the
   environment is missing something prints a note nobody reads and exits green.
   There is no way to tell a suite that passed from one that did not run.
2. **A naked wall-clock wait.** `sleep(500ms)` in place of a condition is a latent
   flake *and* a latent vacuity — when the thing being waited for stops happening,
   a sleep still returns, and the assertion after it may be reachable anyway.

Both are invisible to `cargo test`, so they are checked here. The repository already
gates derivable docs facts this way (`check-readme-facts.py`, `gen-status.py`); this
is the same shape applied to test hygiene.

## The taxonomy (docs/TEST-PLAN.md § Conventions is the prose home)

Every wall-clock wait in test code is one of:

  (a) a **bounded poll**: a loop with a state-dependent exit AND a bound, so it
      stops early when the condition holds and fails loudly when it never does;
  (b) a **deliberate settling delay** whose reason a skeptic would accept, written
      at the site as `// SETTLE(<slug>): <reason>` and listed in
      docs/test-settling-delays.md so that adding one is a reviewable diff line;
  (d) a **virtual-clock advance** inside `#[tokio::test(start_paused = true)]`,
      where `sleep` costs no wall time and is exactly deterministic;
  (c) a **naked wait** — everything else. These fail this check.

(a) and (d) pass for free: they need no annotation because their shape *is* the
argument. (b) costs a comment and a census line. (c) is the defect.

## Why this is not a grep

A naked wait and a bounded poll's pacing sleep are lexically identical:
`tokio::time::sleep(Duration::from_millis(100)).await;` is both. The whole
distinction lives in the enclosing control flow, so the checker tracks block
structure. It does not need a Rust parser — brace depth over source with comments,
string literals and char literals blanked is sufficient, and the first two attempts
at this check failed *because* they skipped that blanking step: a line-oriented
regex over unblanked source finds 4 of the 7 self-skips in this tree and reports
success, because the giveaway token sits on a continuation line of a multi-line
macro. A check that cannot see the defect is worse than no check, so the shapes
here are span-based and structure-aware, and `--audit` prints the full
classification so the classification itself can be reviewed rather than trusted.

**Every structural check reads a comment-stripped view of the source, without
exception.** That is not a style preference, it is the lesson of the first version:
check B3 — whose entire job is keeping the skip macro fatal under CI — searched the
file's RAW text for `var_os("CI")` and `assert!`, so deleting the assertion and
leaving a two-line comment quoting it passed the gate while `CI=true cargo test` ran
the self-skip green. Prose about a check is not the check. The one exception is
narrow and deliberate: B4 keeps string literals, because `cfg!(target_os = "linux")`
is a condition whose meaning IS a string — and it still blanks comments.

## And because text is porous no matter how careful it is

Three rounds of adversarial review found working bypasses of this file — eleven, then seven
more, then eight more — and each closure is named at the check that closes it. The general
lesson is that a rule can only see a shape someone thought of, so two checks here are not text
rules at all:

  * `--check-inventory` asks the compiled binaries what tests they CONTAIN
    (`cargo test -- --list`, plus `--list --ignored`) and compares that to a generated,
    checked-in docs/test-inventory.md. That catches a test `cfg`-gated out of existence, a file
    that compiled to zero tests, a silent deletion, and a test retired with `#[ignore]`.
  * `--check-results` asks what actually RAN AND PASSED. A test can be present in the binary
    and absent from every run (`#[ignore]`), and a whole binary's results can be discarded in
    silence (`std::process::exit(0)` in one test: `running 6 tests` and then nothing — no
    per-test lines, no summary — with `cargo test` exiting 0). Both are invisible to any rule
    over source text and both are obvious in the run's own output, so the run's own output is
    checked: a complete summary per binary, no failures, nothing filtered out, the recorded
    passed count, and an ignored set that matches an allowlist whose tiers are verified.

What this gate still cannot detect is enumerated in docs/TEST-PLAN.md
§ "What this gate detects, and what it cannot", because a limit that is not written down gets
trusted past. Nothing here claims more than it checks — that discipline is the point, and
where a claim was found to outrun its check the claim was narrowed, not the finding.

Usage:
  scripts/check-test-hygiene.py                     # the gate (CI's docs job)
  scripts/check-test-hygiene.py --audit             # every wait site and its class
  scripts/check-test-hygiene.py --write             # regenerate the (b) census
  scripts/check-test-hygiene.py --write-inventory   # regenerate the test inventory
  scripts/check-test-hygiene.py --check-inventory   # compare it (needs cargo; CI's
                                                    # test and mqttui jobs)
  scripts/check-test-hygiene.py --check-results LOG # what ran and passed, from a
                                                    # `cargo test` log (CI tees one);
                                                    # with no LOG, runs the suite here
"""

from __future__ import annotations

import argparse
import functools
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CENSUS = ROOT / "docs" / "test-settling-delays.md"

# The canonical CI-fatal skip macro. mqttui is a separate workspace with no lib target (ADR
# 0056 §1 keeps ratatui out of the broker's dependency graph), so its tests cannot import the
# broker's copy — the text is duplicated. Copies are DISCOVERED rather than listed, so a
# fourth workspace that needs one is covered by the drift check the moment it exists.
SKIP_MACRO_CANONICAL = Path("crates/mqttd/tests/common/skip.rs")
SKIP_MACRO_NAME = "skip_locally_or_fail_in_ci"
SKIP_MACRO_MARKER = f"macro_rules! {SKIP_MACRO_NAME}"

# Where the compile-time-vanishing suites are accounted for (check B4).
PLATFORM_COVERAGE = Path("crates/mqttd/tests/platform_coverage.rs")

MIN_REASON_CHARS = 60


# --------------------------------------------------------------------------------------
# Rust surface scanning: blank everything that is not code, then track brace depth.
# --------------------------------------------------------------------------------------

_RAW_STR = re.compile(r"b?r(#*)\"")
_CHAR_LIT = re.compile(r"'(?:\\.[^'\n]*|[^\\'\n])'")


def blank_noncode(src: str, *, strings: bool = True) -> str:
    """Return `src` with comments and literals replaced by spaces, offsets preserved.

    Newlines survive so line numbers still work, and the result is the same length as
    the input so every offset computed on it indexes the original text too. Blanking
    (rather than deleting) is what lets a check say "this token is code" and separately
    read the human-readable message at the same offsets out of the original.

    `strings=False` keeps string literals and blanks only comments. Exactly one check wants
    that — B4 reads `assert!` CONDITIONS, and `cfg!(target_os = "linux")` is a condition that
    legitimately contains a string — and it must still not read *prose*, because a comment
    that quotes a predicate is not that predicate (issue #260 round 2, finding 1: check B3
    searched raw text and a two-line comment quoting the deleted assertion satisfied it).
    Every structural check in this file therefore runs over one of these two views, never
    over raw source.
    """
    out = list(src)
    n = len(src)

    def blank(a: int, b: int) -> None:
        for k in range(a, min(b, n)):
            if out[k] != "\n":
                out[k] = " "

    i = 0
    while i < n:
        c = src[i]
        if c == "/" and src.startswith("//", i):
            j = src.find("\n", i)
            j = n if j < 0 else j
            blank(i, j)
            i = j
        elif c == "/" and src.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if src.startswith("/*", j):
                    depth += 1
                    j += 2
                elif src.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif not strings:
            i += 1
        elif (c in "br") and (m := _RAW_STR.match(src, i)):
            # Raw string: no escapes, terminated by `"` plus the opening run of `#`.
            close = '"' + m.group(1)
            j = src.find(close, m.end())
            j = n if j < 0 else j + len(close)
            blank(i, j)
            i = j
        elif c == '"':
            j = i + 1
            while j < n and src[j] != '"':
                j += 2 if src[j] == "\\" else 1
            j = min(j + 1, n)
            blank(i, j)
            i = j
        elif c == "'":
            # `'a'` is a char literal; `'a` is a lifetime. Only the former is blanked,
            # or every `&'a str` in the tree would swallow the code after it.
            if m := _CHAR_LIT.match(src, i):
                blank(i, m.end())
                i = m.end()
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


@dataclass
class Block:
    """A `{ … }` and the code immediately before it, which says what it is."""

    open: int
    close: int
    header: str
    parent: int | None
    index: int

    @property
    def is_loop(self) -> bool:
        return bool(re.search(r"\b(loop|while|for)\b", self.header))

    @property
    def fn_name(self) -> str | None:
        m = re.search(r"\bfn\s+(\w+)", self.header)
        return m.group(1) if m else None

    @property
    def is_async_fn(self) -> bool:
        return bool(re.search(r"\basync\s+fn\b", self.header))

    @property
    def is_test_fn(self) -> bool:
        return self.fn_name is not None and bool(
            re.search(r"#\[\s*(?:tokio::)?test\b", self.header)
        )

    @property
    def is_paused_test(self) -> bool:
        return bool(
            re.search(r"#\[\s*tokio::test\s*\([^\]]*start_paused\s*=\s*true", self.header)
        )

    @property
    def is_cfg_test_mod(self) -> bool:
        """A `mod` or `fn` gated on `test` — in ANY spelling of the predicate.

        The first version matched the literal `#[cfg(test)]` only, and every in-src gate in
        this tree happens to be spelled that way — which is exactly why nobody would notice
        the first `#[cfg(all(test, not(miri)))] mod`, or a `#[cfg(test)] #[tokio::test] fn`
        with no enclosing mod at all. Both were proven to leave a module's waits and skips
        unpoliced (issue #260 round 2, minor finding on `mqtt-observability`), so the rule is
        now "a cfg predicate mentioning `test`", not one exact string.
        """
        if not re.search(r"\b(mod|fn)\s+\w+", self.header):
            return False
        for m in re.finditer(r"#\[\s*cfg\s*\(", self.header):
            end = balanced_span(self.header, self.header.index("(", m.end() - 1))
            if re.search(r"\btest\b", self.header[m.start() : end]):
                return True
        return False

    @property
    def is_macro_def(self) -> bool:
        return bool(re.search(r"\bmacro_rules!\s*\w+", self.header))

    @property
    def macro_name(self) -> str | None:
        m = re.search(r"\bmacro_rules!\s*(\w+)", self.header)
        return m.group(1) if m else None

    @property
    def is_closure_or_async_block(self) -> bool:
        """A `|args| {` or `async {` / `async move {` body — a scope of its own."""
        tail = self.header.rstrip()
        return bool(re.search(r"\|[^|]*\|\s*$", tail)) or bool(
            re.search(r"\basync\b(\s+move)?\s*$", tail)
        )


@dataclass
class RustFile:
    path: Path
    rel: str
    src: str
    code: str
    blocks: list[Block] = field(default_factory=list)
    line_starts: list[int] = field(default_factory=list)

    def line_of(self, off: int) -> int:
        lo, hi = 0, len(self.line_starts) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if self.line_starts[mid] <= off:
                lo = mid
            else:
                hi = mid - 1
        return lo + 1

    def chain(self, off: int) -> list[Block]:
        """Enclosing blocks, outermost first."""
        return [b for b in self.blocks if b.open < off < b.close]

    def body(self, b: Block) -> str:
        return self.code[b.open : b.close]

    def children(self, b: Block) -> list[Block]:
        """The blocks directly inside `b`, in source order."""
        return [c for c in self.blocks if c.parent == b.index]

    def is_comment_line(self, line: int) -> bool:
        """True if 1-indexed `line` is a `//` comment in CODE, not inside a string literal.

        A line that merely *starts* with `//` in the raw text may be the middle of a
        multi-line string literal, which is how a fake `// SETTLE(...)` marker could be
        smuggled past the wait check without any comment existing.
        """
        a = self.line_starts[line - 1]
        b = self.line_starts[line] if line < len(self.line_starts) else len(self.src)
        return self.src[a:b].lstrip().startswith("//") and not self.code[a:b].strip()


def parse(path: Path) -> RustFile:
    src = path.read_text(encoding="utf-8")
    code = blank_noncode(src)
    f = RustFile(path=path, rel=str(path.relative_to(ROOT)), src=src, code=code)
    f.line_starts = [0] + [i + 1 for i, ch in enumerate(src) if ch == "\n"]

    stack: list[Block] = []
    boundary = 0  # start of the text that will become the next block's header
    for i, ch in enumerate(code):
        if ch == "{":
            b = Block(
                open=i,
                close=len(code),
                header=code[boundary:i],
                parent=stack[-1].index if stack else None,
                index=len(f.blocks),
            )
            f.blocks.append(b)
            stack.append(b)
            boundary = i + 1
        elif ch == "}":
            if stack:
                stack.pop().close = i
            boundary = i + 1
        elif ch == ";":
            boundary = i + 1
    return f


def rust_files() -> list[RustFile]:
    paths: list[Path] = []
    for pattern in ("crates/*/tests/**/*.rs", "crates/*/src/**/*.rs", "tools/mqttui/**/*.rs"):
        paths += sorted(ROOT.glob(pattern))
    seen, out = set(), []
    for p in paths:
        if "/target/" in str(p) or "/bundle/" in str(p) or p in seen:
            continue
        seen.add(p)
        out.append(parse(p))
    return out


def is_test_code(f: RustFile, chain: list[Block]) -> bool:
    """Test code is: anything under a `tests/` directory, or inside a `#[cfg(test)] mod`.

    Production timers under `src/` are deliberately out of scope. A retry backoff in
    `main.rs` is not a test wait, and a check that conflated the two would be argued
    with instead of obeyed.
    """
    if "/tests/" in f.rel:
        return True
    return any(b.is_cfg_test_mod for b in chain)


# --------------------------------------------------------------------------------------
# Check A — every wall-clock wait in test code is (a), (b) or (d)
# --------------------------------------------------------------------------------------

# The ways test code BLOCKS ON A CLOCK. `sleep(` was the whole vocabulary until round 2, and
# six other spellings each really burned 250 ms with the gate reporting `all bounded polls` —
# because it had counted none of them (finding 4). What is covered here is the set of calls whose
# argument IS a duration and whose effect is to wait for it:
#
#   sleep / sleep_until    tokio and std
#   park_timeout           the std thread version
#   recv_timeout           the idiomatic way to wait for something that will not come
#   recv_deadline          the same with an Instant
#   .tick()                `interval(250ms).tick().await` — a sleep with a phase
#   timeout(d, pending())  a duration with no future in it at all
#
# This list can never be complete: a busy-wait can be written with arithmetic and a `yield_now`,
# and `check_temporal_burn_loops` covers only the shape that consumes a duration while doing
# nothing. docs/TEST-PLAN.md § "What this gate detects, and what it cannot" says so rather than implying that
# a wait-site count is a census of waits.
WAIT = re.compile(
    r"\bsleep(?:_until)?\s*\("
    r"|\bpark_timeout\s*\("
    r"|\brecv_timeout\s*\("
    r"|\brecv_deadline\s*\("
    r"|\.\s*tick\s*\(\s*\)"
    r"|\btimeout\s*\([^;]{0,200}?\bpending\s*(?:::<[^>]*>)?\s*\("
)
# `use tokio::time::sleep as pause;` renames the wait, and `pause(d).await` then carries no
# `sleep` token at all — a proven, working bypass of the whole of check A (issue #260 round 2,
# finding 4). Renames are resolved rather than banned, because banning an import is a rule
# authors route around and resolving one is a rule they cannot. Resolution is REPO-WIDE, not
# per file: `pub use tokio::time::sleep as settle;` in a helper module puts the rename in one
# file and the call in another, which is a two-line move that defeated the per-file version
# (round 2, finding 4, shape 6). The cost is that the name is a wait everywhere — an
# over-approximation, and the safe direction.
SLEEP_ALIAS = re.compile(r"\bsleep(?:_until)?\s+as\s+(\w+)")
SETTLE = re.compile(r"//\s*SETTLE\(([A-Za-z0-9][A-Za-z0-9._-]*)\)\s*:\s*(.*)")
DURATION = re.compile(
    r"\bDuration::from_(millis|secs|micros|secs_f64)\s*\(\s*([0-9_.]+)"
    r"|\b([A-Z][A-Z0-9_]{2,})\b"
)


# A condition that consults only the clock. `while start.elapsed() < D { sleep(5ms).await }` is
# a pure wall-clock wait wearing a poll's clothes: it cannot stop early, because there is no
# state in it to stop on. Accepting it (the first version did) would have let any naked wait be
# relabelled (a) by wrapping it in a temporal `while` — the gate training the tree toward the
# shape it exists to ban (issue #260 round 2, finding 5).
TEMPORAL_ONLY = re.compile(
    r"""^\s*(?:
          [\w.]*\.elapsed\s*\(\s*\)\s*[<>]=?\s*[\w:.()_+*\s-]+
        | Instant::now\s*\(\s*\)\s*[<>]=?\s*[\w:.()_+*\s-]+
        | [\w.]+\s*[<>]=?\s*(?:deadline|until|[\w.]*\.elapsed\s*\(\s*\))[\w:.()_+*\s-]*
        )\s*$""",
    re.X,
)

# A `for` header whose iterator cannot run out. `for _ in 0..` and `iter::repeat()` are loops
# with no bound; every other `for` is bounded by its iterator, including `for p in &peers`,
# which the first version rejected because it only accepted range syntax.
UNBOUNDED_ITER = re.compile(r"\.\.\s*$|\brepeat(?:_with)?\s*\(|\bcycle\s*\(")


def _while_condition(header: str) -> str | None:
    m = re.search(r"\bwhile\b(.*)$", header, re.S)
    return m.group(1) if m else None


def _exits_are_temporal_only(f: RustFile, loop_block: Block) -> bool:
    """Every way out of this loop consults the clock and nothing else.

    Two shapes: a temporal `while` header, and `loop { if deadline_passed { break } }`. Both
    are durations, not conditions — the loop cannot finish early because nothing it looks at
    can change the answer sooner.
    """
    cond = _while_condition(loop_block.header)
    if cond is not None and not TEMPORAL_ONLY.match(cond):
        return False
    body = f.body(loop_block)
    guards: list[str] = []
    for m in re.finditer(r"\b(break|return)\b", body):
        off = loop_block.open + m.start()
        inner = [b for b in f.chain(off) if b.open > loop_block.open]
        guard = next(
            (b.header for b in reversed(inner) if re.search(r"\bif\b", b.header)), None
        )
        if guard is None:
            # An unguarded `break`/`return`, or one guarded by a `match` arm: not analysable
            # as temporal, so give it the benefit of the doubt and call the exit stateful.
            return False
        guards.append(re.sub(r"^.*\bif\b", "", guard, count=1))
    if cond is None and not guards:
        return False  # `loop {}` with no exit at all — not our business here
    return all(TEMPORAL_ONLY.match(g) for g in guards)


def loop_is_bounded_poll(f: RustFile, loop_block: Block, outer: list[Block]) -> tuple[bool, bool]:
    """(has a state-dependent exit, has a bound) for one loop.

    A poll earns its sleep by being able to *stop early* (so it is a condition, not a
    duration) and by being unable to run forever (so a condition that never holds is a
    named failure rather than a hung job). `outer` is the enclosing block chain, because a
    loop's bound is often the `timeout(...)` WRAPPING it rather than anything inside it.
    """
    header, body = loop_block.header, f.body(loop_block)
    exits = bool(re.search(r"\bwhile\b", header)) or bool(
        re.search(r"\b(break|return)\b", body)
    )
    if exits and _exits_are_temporal_only(f, loop_block):
        exits = False
    is_for = bool(re.search(r"\bfor\b[^{]*\bin\b", header))
    bound = (
        (is_for and not UNBOUNDED_ITER.search(header))
        or bool(re.search(r"\bwhile\b[^{]*\b(deadline|elapsed|Instant::now)\b", header))
        or bool(re.search(r"\bdeadline\b", body))
        or bool(re.search(r"\.elapsed\s*\(\s*\)", body))
        or bool(re.search(r"\bInstant::now\s*\(\s*\)", body))
        or bool(re.search(r"\btimeout\s*\(", body))
        or bool(re.search(r"\bassert\w*!\s*\([^;]*[<>]", body))
        or _counter_bound(body)
        # The idiomatic sound shape: `timeout(d, async { loop { … sleep … } }).await.expect(…)`.
        # The bound is OUTSIDE the loop, and rejecting it (the first version did) teaches
        # authors to work around the gate, which is worse than a gap.
        or any(
            re.search(r"\b(timeout|select!|select\s*!)\s*[({]", b.header) for b in outer
        )
    )
    return exits, bound


def _counter_bound(body: str) -> bool:
    """A counter incremented every pass and compared against a literal limit.

    `Err(e) if attempt < 40 => { attempt += 1; sleep(25ms) }` is a bounded retry with no
    deadline and no range header, and the first version of this check called it naked. The
    increment is what separates it from an incidental `if depth > 0`: the same name must both
    advance and be capped.
    """
    bumped = set(re.findall(r"\b(\w+)\s*\+=", body)) | {
        m.group(1) for m in re.finditer(r"\b(\w+)\s*=\s*\1\s*\+", body)
    }
    for name in bumped:
        # `attempt < 40` and `if attempts > 100 { panic!(…) }` are the same bound written from
        # opposite ends; the first version only accepted the former.
        if re.search(rf"\b{re.escape(name)}\s*(?:<=?|>=?)\s*[0-9_]+", body):
            return True
        if re.search(rf"[0-9_]+\s*(?:<=?|>=?)\s*\b{re.escape(name)}\b", body):
            return True
    return False


def _comment_block_above(f: RustFile, line: int) -> range:
    """The contiguous `//` comment block immediately above 1-indexed `line`.

    "Comment" means a comment in code: a line inside a multi-line string literal can start
    with `//` too, and a marker forged there would license a wait without any comment
    existing (the same class as finding 1 — never read raw text structurally).
    """
    top = line
    while top - 1 >= 1 and f.is_comment_line(top - 1):
        top -= 1
    return range(top, line)


def settle_marker(f: RustFile, line: int, block: Block | None) -> tuple[str, str] | None:
    """Find the `// SETTLE(slug): reason` that documents the wait at `line`.

    Two places count, and only two: the comment block immediately above the wait, and the
    comment block immediately above its innermost enclosing block's opening line — because
    a *bounded window* is documented at the loop that bounds it, not at the sleep inside.
    Anywhere else and one marker would silently vouch for a whole function.

    The reason continues over following `//` lines. A real argument usually needs more than
    one line, and cutting it at the first newline would reward one-liners.
    """
    lines = f.src.splitlines()
    candidates = list(_comment_block_above(f, line))
    if block is not None:
        candidates += list(_comment_block_above(f, f.line_of(block.open)))
    for probe in sorted(set(candidates), reverse=True):
        m = SETTLE.search(lines[probe - 1])
        if not m or not f.is_comment_line(probe):
            continue
        slug, first = m.group(1), m.group(2).strip()
        parts = [first]
        for k in range(probe, len(lines)):
            nxt = lines[k].strip()
            if nxt.startswith("//") and not SETTLE.search(nxt):
                parts.append(nxt.lstrip("/").strip())
            else:
                break
        return slug, " ".join(p for p in parts if p).strip()
    return None


def duration_of(f: RustFile, off: int) -> str:
    """The wait's magnitude as written, for the census. Best-effort and never fatal."""
    span = f.code[off : off + 200]
    end = span.find(";")
    span = span[: end if end > 0 else len(span)]
    m = DURATION.search(span)
    if not m:
        return "?"
    if m.group(1):
        unit = {"millis": "ms", "secs": "s", "micros": "us", "secs_f64": "s"}[m.group(1)]
        return f"{m.group(2).replace('_', '')}{unit}"
    return m.group(3)


@dataclass
class Site:
    rel: str
    line: int
    fn: str
    cls: str  # "a" | "b" | "d" | "c"
    slug: str = ""
    reason: str = ""
    duration: str = ""
    note: str = ""
    callers: int = -1  # -1: the wait is inside a test fn, so it has exactly one caller


def sleep_re(files: list[RustFile]) -> re.Pattern[str]:
    """The wait pattern for the whole tree: every blocking-on-a-clock call and every rename."""
    names = {n for f in files for n in SLEEP_ALIAS.findall(f.code)}
    if not names:
        return WAIT
    alt = "|".join(sorted(re.escape(n) for n in names))
    return re.compile(WAIT.pattern + rf"|\b(?:{alt})\s*\(")


def call_sites(files: list[RustFile], name: str) -> int:
    """How many times `name(` is called in test code, not counting its own definition.

    A helper that sleeps is one marker vouching for however many callers it has, and the
    census cannot even state the magnitude when the duration is a parameter. Counting the
    callers is what turns "add a third call site" into a regenerated, reviewable diff line
    (issue #260 round 2, finding 4c: one marker licensed three naked waits).
    """
    n = 0
    pat = re.compile(rf"\b{re.escape(name)}\s*\(")
    for f in files:
        for m in pat.finditer(f.code):
            if re.search(r"\bfn\s+$", f.code[max(0, m.start() - 12) : m.start()]):
                continue
            if is_test_code(f, f.chain(m.start())):
                n += 1
    return n


def scan_waits(files: list[RustFile]) -> list[Site]:
    sites: list[Site] = []
    waits = sleep_re(files)
    for f in files:
        for m in waits.finditer(f.code):
            off = m.start()
            # `fn sleep(` / `fn sleep_until(` are definitions, not waits.
            if re.search(r"\bfn\s+$", f.code[max(0, off - 12) : off]):
                continue
            chain = f.chain(off)
            if not is_test_code(f, chain):
                continue
            line = f.line_of(off)
            fn = next((b.fn_name for b in reversed(chain) if b.fn_name), None)
            # A `macro_rules!` body used to be exempt outright, and `settle!(250)` at the call
            # site carries no `sleep` token — a working bypass that really burned wall clock at
            # exit 0. The wait is now classified where it is WRITTEN, with the macro's own name
            # standing in for a function name (issue #260 round 2, finding 4a).
            macros = [b for b in chain if b.is_macro_def]
            if fn is None and macros:
                fn = f"{macros[-1].macro_name}!"
            dur = duration_of(f, off)

            if any(b.is_paused_test for b in chain):
                sites.append(Site(f.rel, line, fn or "<file>", "d", duration=dur))
                continue

            loops = [b for b in chain if b.is_loop]
            if loops:
                outer = [b for b in chain if b.open < loops[-1].open]
                exits, bound = loop_is_bounded_poll(f, loops[-1], outer)
                if exits and bound:
                    sites.append(Site(f.rel, line, fn or "<file>", "a", duration=dur))
                    continue
                why = []
                if not exits:
                    why.append(
                        "its loop has no state-dependent exit (a purely temporal condition is "
                        "a duration, not a poll)"
                    )
                if not bound:
                    why.append("its loop has no deadline or iteration bound")
                note = " and ".join(why)
            else:
                note = "not inside a loop"

            # A wait inside something other than a `#[test]` fn is a helper's wait: one
            # marker, many callers. The count goes in the census so growth is diff-visible.
            fns = [b for b in chain if b.fn_name]
            host = fns[-1] if fns else None
            callers = -1
            if host is None or not host.is_test_fn:
                callers = call_sites(files, (host.fn_name if host else None) or fn or "")

            marker = settle_marker(f, line, chain[-1] if chain else None)
            if marker:
                slug, reason = marker
                sites.append(
                    Site(
                        f.rel,
                        line,
                        fn or "<file>",
                        "b",
                        slug=slug,
                        reason=reason,
                        duration=dur,
                        note=note,
                        callers=callers,
                    )
                )
            else:
                sites.append(
                    Site(f.rel, line, fn or "<file>", "c", duration=dur, note=note, callers=callers)
                )
    return sites


def check_a(files: list[RustFile], sites: list[Site]) -> list[str]:
    errs: list[str] = []

    # A3 — naked waits.
    for s in (x for x in sites if x.cls == "c"):
        errs.append(
            f"{s.rel}:{s.line}: naked wall-clock wait ({s.duration}) in `{s.fn}` — "
            f"{s.note}, and there is no `// SETTLE(<slug>): <reason>` marker. Convert it to "
            f"a bounded poll on observable state (a loop with a condition AND a deadline "
            f"whose failure message says what never happened), or, if no observable exists, "
            f"add the marker with a reason a skeptic would accept and run "
            f"`scripts/check-test-hygiene.py --write`."
        )

    # A3 — a (b) needs a real reason, not a shrug.
    for s in (x for x in sites if x.cls == "b"):
        if len(s.reason) < MIN_REASON_CHARS:
            errs.append(
                f"{s.rel}:{s.line}: SETTLE({s.slug}) reason is {len(s.reason)} chars; a "
                f"deliberate wall-clock wait needs at least {MIN_REASON_CHARS} — what state "
                f"is settling, why no observable condition exists for it, and what happens "
                f"on a slow machine."
            )

    # A3 — one marker vouches for one wait. Sharing a slug between two waits is how a
    # second, undocumented wall-clock wait would ride in on the first one's argument.
    seen: dict[tuple[str, str], Site] = {}
    for s in (x for x in sites if x.cls == "b"):
        key = (s.rel, s.slug)
        if key in seen:
            errs.append(
                f"{s.rel}:{s.line}: SETTLE({s.slug}) is already claimed by the wait at line "
                f"{seen[key].line}. Give each deliberate wait its own slug and its own reason "
                f"— one marker must not vouch for two."
            )
        seen[key] = s

    # A3 — the census. A marker alone can be added in a diff nobody reads; a checked-in
    # table makes every deliberate wall-clock wait a visible line in a reviewed file.
    errs += check_census([s for s in sites if s.cls == "b"])

    # A5 — a wall-clock wait with no wait CALL in it at all: `while Instant::now() < deadline
    # { yield_now().await }` consumes its duration exactly like a sleep and carries no token any
    # vocabulary could list (round 2, finding 4, shape 2). The rule is narrow on purpose: a loop
    # whose every exit is temporal AND whose body does nothing but burn. A temporal loop that
    # does real work — `while start.elapsed() < 5s { publish(…) }`, the shape of every load
    # generator here — is a duration-bounded workload, not a wait, and flagging it would be
    # flagging correct code.
    for f in files:
        for b in f.blocks:
            if not b.is_loop or not is_test_code(f, f.chain(b.open) + [b]):
                continue
            body = f.body(b)
            if not _exits_are_temporal_only(f, b):
                continue
            stripped = re.sub(
                r"\b(?:\w+\s*::\s*)*(?:yield_now|spin_loop|park|sleep)\s*\([^)]*\)"
                r"|\.\s*await|;|\{|\}|\(\s*\)",
                " ",
                body,
            )
            if stripped.strip():
                continue
            errs.append(
                f"{f.rel}:{f.line_of(b.open)}: this loop is a wall-clock wait spelled as a "
                f"loop — every way out of it consults only the clock, and its body does nothing "
                f"but yield. It consumes its duration exactly as `sleep` would, so it is a (c) "
                f"naked wait: poll an observable and fail loudly at a deadline, or mark it "
                f"`// SETTLE(<slug>): <reason>` and record it in the census."
            )

    # A4 — a blocking sleep inside an async test body parks a runtime worker, so the
    # thing being waited for may be the very task that cannot now run.
    for f in files:
        for m in re.finditer(r"\b(?:std::)?thread::sleep\s*\(", f.code):
            chain = f.chain(m.start())
            if not is_test_code(f, chain):
                continue
            fns = [b for b in chain if b.fn_name]
            if fns and fns[-1].is_async_fn:
                errs.append(
                    f"{f.rel}:{f.line_of(m.start())}: `thread::sleep` inside async fn "
                    f"`{fns[-1].fn_name}` blocks a runtime worker — use "
                    f"`tokio::time::sleep(...).await` in a bounded poll."
                )
    return errs


def census_rows(bs: list[Site]) -> list[str]:
    return [
        f"| `{s.rel}` | `{s.fn}` | `{s.slug}` | {s.duration} | "
        f"{'in the test itself' if s.callers < 0 else f'helper, {s.callers} call site(s)'} |"
        for s in sorted(bs, key=lambda s: (s.rel, s.line))
    ]


def render_census(bs: list[Site]) -> str:
    header = """<!-- GENERATED by scripts/check-test-hygiene.py --write. Do not edit by hand. -->
# Deliberate settling delays in test code

Every wall-clock wait the gate can SEE in this repository's test code is one of four shapes
(docs/TEST-PLAN.md § Conventions). What it can see is a list of calls that block on a clock —
`sleep`, `sleep_until`, `park_timeout`, `recv_timeout`, `recv_deadline`, `interval().tick()`,
`timeout(d, pending())`, any local or cross-file rename of `sleep`, and a loop whose only exit is
temporal and whose body only yields. A list is not a census: a busy-wait that computes is a wait
this file does not know about, and TEST-PLAN § "What this gate detects, and what it cannot" says
so rather than leaving the count to imply completeness.

- **(a) a bounded poll** — a loop with a state-dependent exit *and* a deadline. Needs no
  entry here: its shape is the argument.
- **(d) a virtual-clock advance** — `sleep` inside `#[tokio::test(start_paused = true)]`,
  which costs no wall time and is exactly deterministic. Also needs no entry.
- **(b) a deliberate settling delay** — a real wall-clock wait kept on purpose, because the
  state being settled has no observable, or observing it would destroy the subject. Every
  one is listed below and carries a `// SETTLE(<slug>): <reason>` comment at the site.
- **(c) a naked wait** — banned. `scripts/check-test-hygiene.py` fails the build on one.

This file exists so that (b) is a **reviewable diff line** rather than a comment in a
1500-line test file. `scripts/check-test-hygiene.py` fails if a marked site is missing
here, if an entry here names a site that no longer exists, or if a reason is shorter than
60 characters. It does not — cannot — judge whether a reason is *good*; that is the
reviewer's job, and the point of putting the list where a reviewer will see it.

Regenerate with `scripts/check-test-hygiene.py --write`.

The **reach** column is why a helper cannot launder a wait: a `settle(ms)` helper with one
marker would otherwise vouch for any number of waits at any number of call sites, and the
census could not even state their magnitudes. The call-site count is derived, so adding a
caller changes this file and the gate fails until it is regenerated and re-read.

| site | test | slug | wait | reach |
| --- | --- | --- | --- | --- |
"""
    return header + "\n".join(census_rows(bs)) + "\n"


def check_census(bs: list[Site]) -> list[str]:
    if not CENSUS.is_file():
        return [
            f"{CENSUS.relative_to(ROOT)} is missing; run "
            f"`scripts/check-test-hygiene.py --write`."
        ]
    have = {ln.strip() for ln in CENSUS.read_text(encoding="utf-8").splitlines()}
    want = census_rows(bs)
    errs = []
    for row in want:
        if row not in have:
            errs.append(
                f"{CENSUS.relative_to(ROOT)}: missing census row for a SETTLE marker: "
                f"{row} — run `scripts/check-test-hygiene.py --write`."
            )
    wantset = set(want)
    for ln in have:
        if ln.startswith("| `") and ln.endswith("|") and "---" not in ln and ln not in wantset:
            errs.append(
                f"{CENSUS.relative_to(ROOT)}: stale census row (the site is gone or "
                f"changed): {ln} — run `scripts/check-test-hygiene.py --write`."
            )
    return errs


# --------------------------------------------------------------------------------------
# Check B — a skip must never report success on the platform that gates merges
# --------------------------------------------------------------------------------------

# A return that carries no information. Every terminator matters:
#   `return;`            the multi-line form
#   `if cond { return }` no semicolon — the one-liner, and the shape a developer writes first
#   `let Some(x) = f() else { return };`   also `return` then `}`
#   `return Ok(())`      a Result-returning test: `Ok\s*\(\s*\)` cannot match `Ok(())`
#   `A => return,`       a match arm
# The first version required the semicolon and spelled the unit as `Ok()`, so a one-line
# early return and a `return Ok(());` both passed the gate AND passed green under CI=true —
# i.e. the exact self-skip issue #260 exists to prevent (round 2, findings 2 and 3).
BARE_RETURN = re.compile(
    r"\breturn\b[ \t]*(?:Ok\s*\(\s*\(\s*\)\s*\)|\(\s*\))?[ \t]*(?=[;},]|\n[ \t]*[})])"
)
PRINTLN = re.compile(r"\b(e?println)\s*!\s*\(")
SKIP_WORDS = re.compile(r"\b(skip|skipped|skipping|not\s+run)\b", re.IGNORECASE)


LOUD = re.compile(r"\b(assert\w*!|panic!|unreachable!|todo!|unimplemented!)")
# Case (2) of `poll_exhaustion_is_loud` needs a stronger thing than LOUD: an UNCONDITIONAL
# failure. An `assert_eq!` after a loop can pass, so it cannot be the statement that makes
# exhaustion fatal — and treating it as one exempted `for _ in 0..1 { if probe() { return } }
# assert_eq!(a, b);`, a self-skip whose fig-leaf loop is followed by the test's ordinary
# assertion (round 3; adjacency alone did not separate them, because that assertion IS
# adjacent). A real poll's tail diverges: `panic!("never arrived")`.
DIVERGES = re.compile(r"\b(panic!|unreachable!|todo!|unimplemented!)")


def poll_exhaustion_is_loud(f: RustFile, test: Block, loop_block: Block) -> bool:
    """Does this loop FAIL when it finishes without taking its early `return`?

    B1 exempts a bare `return` inside a loop, because a bounded poll's success exit is a
    `return` and every poll in this tree writes one. The exemption used to be unconditional —
    any enclosing loop at all — so `for _ in 0..1 { if probe().is_err() { return } assert…! }`
    passed the gate and passed green under `CI=true`: a self-skip wearing a loop as a fig leaf
    (issue #260 round 2, finding 3). What separates the twelve real polls in this tree from that
    is not the loop, it is what happens when the loop RUNS OUT:

      * `for attempt in 0..50 { … return; assert!(attempt < 49, "never arrived") }` — the
        progress bound is asserted against the loop's own induction variable, so exhaustion is a
        named failure;
      * `for _ in 0..200 { if ready { return } sleep } panic!("was never counted: …")` — the
        failure is after the loop.

    Both mean the same thing: not returning is a failure, so returning is a success exit rather
    than an escape. A trivial loop has neither. The residual — a return taken on a probe FAILURE
    inside a loop that is otherwise a real poll — is named in docs/TEST-PLAN.md, because telling
    "returned because the state arrived" from "returned because the environment is missing"
    needs meaning, not structure.
    """
    body = f.body(loop_block)
    # (1) the loop's own bound, asserted: an assert/panic mentioning the induction variable or
    #     the counter the loop advances.
    names = set(re.findall(r"\bfor\s+(?:mut\s+)?(\w+)\s+in\b", loop_block.header))
    names |= set(re.findall(r"\b(\w+)\s*\+=", body))
    for m in LOUD.finditer(body):
        end = balanced_span(body, body.index("(", m.end() - 1)) if "(" in body[m.end() - 1 :] else 0
        args = body[m.start() : end]
        if any(re.search(rf"\b{re.escape(n)}\b", args) for n in names if n != "_"):
            return True
    # (2) a loud statement that is the loop's IMMEDIATE consequence: the first statement after
    #     it, so falling out of the loop fails before anything else can happen.
    #
    #     "Anywhere after the loop" was too weak — it matched the test's own ordinary
    #     assertions, so `for _ in 0..1 { if probe() { return } } assert_eq!(a, b);` was
    #     exempt (round 3). A real poll writes its failure adjacent to the loop, because that
    #     is the only place it means "the state never arrived"; an assertion further down is
    #     about something else entirely and says nothing about exhaustion.
    # `close` is the index OF the loop's closing brace, so the tail begins with it; strip that
    # and any separator before asking what the first real statement is.
    tail = f.code[loop_block.close : test.close].lstrip("} \t\r\n;")
    m = DIVERGES.search(tail)
    return bool(m and m.start() == 0)


def balanced_span(code: str, open_paren: int) -> int:
    depth, i, n = 0, open_paren, len(code)
    while i < n:
        if code[i] == "(":
            depth += 1
        elif code[i] == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return n


def check_b(files: list[RustFile]) -> list[str]:
    errs: list[str] = []

    # B1 — the structural rule. A test function has no legitimate reason to return early: it
    # asserts. There is no allowlist and no exemption comment, because the sanctioned form hides
    # the `return` inside a macro and a compliant test therefore has no visible bare return at
    # all. It is not airtight — it has exactly TWO exemptions, and this comment names them
    # because the previous one called itself "airtight" while carrying one that needed no comment
    # to invoke (round 2, finding 8):
    #
    #   1. a bounded poll's success exit — a `return` inside a loop WHOSE EXHAUSTION FAILS
    #      (`poll_exhaustion_is_loud`). The unconditional version of this exemption ("any
    #      enclosing loop") was a one-line bypass: `for _ in 0..1 { if probe() { return } … }`.
    #   2. a `return` inside a closure or async block handed to `spawn` — that returns from the
    #      task, not from the test.
    #
    # A return taken on a probe FAILURE inside a loop that is otherwise a real poll still passes;
    # that is in docs/TEST-PLAN.md, because separating it from a success exit needs meaning.
    for f in files:
        for m in BARE_RETURN.finditer(f.code):
            chain = f.chain(m.start())
            if not is_test_code(f, chain):
                continue
            # Only the ONE sanctioned macro may hide a `return` — it is the macro whose whole
            # purpose is to make that return fatal under CI. Exempting every `macro_rules!`
            # body (the first version did) means a `macro_rules! skip_if { … return; … }` is a
            # silent self-skip generator that the gate never looks at.
            if any(b.macro_name == SKIP_MACRO_NAME for b in chain):
                continue
            macros = [b for b in chain if b.is_macro_def]
            tests = [b for b in chain if b.is_test_fn]
            if not tests:
                if macros and not any(b.is_loop for b in chain if b.open > macros[-1].open):
                    errs.append(
                        f"{f.rel}:{f.line_of(m.start())}: bare early `return` inside "
                        f"`macro_rules! {macros[-1].macro_name}` in test code — expanded in a "
                        f"test it returns from that test, which reports success without "
                        f"testing anything, and the call site carries no visible `return` at "
                        f"all. `{SKIP_MACRO_NAME}!` is the only macro allowed to do this, "
                        f"because it is fatal when CI=true."
                    )
                continue
            inner = tests[-1]
            after = [b for b in chain if b.open > inner.open]
            # A `return` inside a closure or async block passed to `spawn` returns from that
            # TASK, not from the test — the one legitimate non-poll early return in this tree
            # (`frame.rs`'s writer task stops when the reader hangs up). `block_on` is
            # deliberately not in this list: an async block a test *awaits its own body in* is
            # the test body, and exempting it would hand back the whole hole.
            if any(
                b.is_closure_or_async_block
                and re.search(r"\b(spawn|spawn_blocking|spawn_local|scope)\s*\(", b.header)
                for b in after
            ):
                continue
            loops = [b for b in after if b.is_loop]
            if loops and poll_exhaustion_is_loud(f, inner, loops[-1]):
                continue  # a bounded poll's success exit: running out of the loop FAILS
            why = (
                " Its enclosing loop does not make it one: a poll's `return` is a success exit "
                "only because exhausting the loop is a named failure (`assert!(attempt < 49, "
                "\"…\")` against the loop's own counter, or a `panic!` after the loop). This "
                "loop can simply finish, and then the test passes having done nothing."
                if loops
                else ""
            )
            errs.append(
                f"{f.rel}:{f.line_of(m.start())}: bare early `return` in test "
                f"`{inner.fn_name}` — a test that returns instead of asserting reports "
                f"success without testing anything.{why} If this is an environmental skip, use "
                f"`skip_locally_or_fail_in_ci!(\"…\")`, which is fatal when CI=true so the "
                f"coverage cannot quietly vanish on the platform that gates merges."
            )

    # B2 — a word-level backstop for skips that are not shaped like an early return
    # (a skip that falls through to a weaker assertion, say). Spans are balanced-paren,
    # not per-line: the message that gives the game away is often on a continuation line,
    # and a line-oriented version of this check found 4 of 7 and reported success.
    for f in files:
        for m in PRINTLN.finditer(f.code):
            chain = f.chain(m.start())
            if not is_test_code(f, chain):
                continue
            if any(b.macro_name == SKIP_MACRO_NAME for b in chain):
                continue
            end = balanced_span(f.code, f.code.index("(", m.end() - 1))
            if SKIP_WORDS.search(f.src[m.start() : end]):
                errs.append(
                    f"{f.rel}:{f.line_of(m.start())}: a `{m.group(1)}!` in test code "
                    f"announces a skip. Announcing it is not enough — the run still exits "
                    f"green. Route it through `skip_locally_or_fail_in_ci!(\"…\")`."
                )

    # B5 — the skip that has no `return` and prints nothing: a test whose assertions all sit
    # inside an `if` the environment can fail. Nothing about it is lexically a skip, so the
    # only question that finds it is the one that matters anyway — can this test finish
    # without asserting?
    for f in files:
        for b in f.blocks:
            if not b.is_test_fn or not is_test_code(f, f.chain(b.open) + [b]):
                continue
            if not every_action_is_conditional(f, b):
                continue
            errs.append(
                f"{f.rel}:{f.line_of(b.open)}: test `{b.fn_name}` does nothing at all unless a "
                f"condition holds — every statement it has beyond a `let` binding is inside an "
                f"`if` that can simply not be taken. That is an environmental self-skip with no "
                f"`return`, no message and no macro: it reports success on the platform that "
                f"gates merges. Run the test unconditionally, or make the environment check a "
                f"`{SKIP_MACRO_NAME}!(\"…\")` so it is fatal under CI."
            )

    # B7 — the same skip as B5 with the `if` spelled as a short-circuit. `let _ = probe() && {
    # assert_eq!(1, 2, "the real assertion"); true };` puts the assertion inside a binding, and
    # B5 skips statements that start with `let` (deliberately: most tests bind their subject
    # first). It is the shape an author reaches for when the `if` form is rejected by name, and
    # it does not exist anywhere in this tree, so it costs nothing to refuse (round 2, finding 2,
    # third shape). A closure `|| {` is not a short-circuit: the operator must have a real left
    # operand.
    for f in files:
        for m in re.finditer(r"(?<=[)\]\w\"'?])\s*(?:&&|\|\|)\s*\{", f.code):
            chain = f.chain(m.start())
            tests = [b for b in chain if b.is_test_fn]
            if not tests or not is_test_code(f, chain):
                continue
            end = brace_span(f.code, m.end() - 1)
            if not re.search(r"\bassert\w*!", f.code[m.end() - 1 : end]):
                continue
            errs.append(
                f"{f.rel}:{f.line_of(m.start())}: test `{tests[-1].fn_name}` asserts inside the "
                f"right operand of a `&&`/`||`, so the assertion runs only if the left operand "
                f"says so — an `if`-shaped skip written as an expression, and one that hides "
                f"inside a `let` binding where B5 does not look. Assert unconditionally, or make "
                f"the environment check a `{SKIP_MACRO_NAME}!(\"…\")`."
            )

    # B6 — `std::process::exit` in test code takes the whole BINARY out of the run, not just its
    # own test: the harness leaves mid-suite, so `running 6 tests` is followed by no per-test
    # lines and no `test result:` summary, and `cargo test` exits 0 (round 2, finding 7). This is
    # the text half of the close — cheap, and it names the one shape a source rule can see. The
    # structural half is `--check-results`, which notices any binary whose results vanished, for
    # this reason or any other.
    for f in files:
        for m in re.finditer(r"\bprocess::(exit|abort)\s*\(", f.code):
            chain = f.chain(m.start())
            if not is_test_code(f, chain):
                continue
            errs.append(
                f"{f.rel}:{f.line_of(m.start())}: `process::{m.group(1)}` in test code ends the "
                f"whole test BINARY, discarding every result in it — no per-test lines, no "
                f"`test result:` summary, and `cargo test` still exits 0. A test that must stop "
                f"early either asserts or uses `{SKIP_MACRO_NAME}!(\"…\")`; a child process that "
                f"must exit belongs in a child process."
            )

    # B3 — every copy of the macro must be byte-identical to the canonical one, and the
    # canonical one must still be fatal. A duplicated safety net with one frayed copy is
    # worse than an obviously missing one.
    canon = ROOT / SKIP_MACRO_CANONICAL
    if not canon.is_file():
        errs.append(f"{SKIP_MACRO_CANONICAL}: the CI-fatal skip macro is missing.")
    else:
        want = canon.read_text(encoding="utf-8")
        errs += check_skip_macro_is_fatal(want)
        for f in files:
            if SKIP_MACRO_MARKER not in f.code or f.path == canon:
                continue
            if f.src != want:
                errs.append(
                    f"{f.rel} defines `skip_locally_or_fail_in_ci!` but has drifted from "
                    f"{SKIP_MACRO_CANONICAL}. The macro is duplicated because mqttui is a "
                    f"separate workspace with no lib target; every copy must be byte-identical "
                    f"so one of them cannot quietly stop being fatal."
                )

    # B4 — the hole a runtime check can never see. A `#![cfg(...)]` at the top of a test
    # file compiles the WHOLE FILE to zero tests off-platform, and the binary reports
    # success; an assertion inside that file would be excluded by the same gate. So the
    # predicates are mirrored in an always-compiled file.
    gated = platform_gates(files)
    covp = ROOT / PLATFORM_COVERAGE
    cov = blank_noncode(covp.read_text(encoding="utf-8"), strings=False) if covp.is_file() else ""
    if gated and not cov:
        errs.append(
            f"{PLATFORM_COVERAGE} is missing, but these suites vanish at compile time "
            f"off-platform: {sorted(gated)}."
        )
    # Only the CONDITIONS of the assertions count — never their messages. The first version of
    # this compared the predicate against the whole file, and its own mutation proof caught it
    # being vacuous: an assertion whose MESSAGE quotes the predicate ("memory_watermark.rs is
    # #![cfg(target_os = \"linux\")] ...") satisfied the search, so deleting the real `cfg!` from
    # the condition left the check green. Prose about a check is not the check.
    conditions = " ".join(assert_conditions(cov)).replace(" ", "")
    for name, preds in sorted(gated.items()):
        for pred, inner in preds:
            attr = f"#![cfg({pred})]" if inner else f"#[cfg({pred})]"
            scope = "that file compiles to zero tests" if inner else "those tests do not exist"
            if pred.replace(" ", "") not in conditions:
                errs.append(
                    f"{PLATFORM_COVERAGE}: no assertion CONDITION covers `{name}`'s "
                    f"`{attr}`. Off-platform {scope} and the run is green; the assertion must "
                    f"live in an always-compiled file, because an inner one would be excluded "
                    f"by the same cfg."
                )
            elif name not in cov:
                errs.append(
                    f"{PLATFORM_COVERAGE}: something asserts `{attr}` but the file "
                    f"never names `{name}`, so a failure would not say which suite vanished."
                )
    return errs


# Predicates that make code vanish on a platform, as opposed to `cfg(test)` or a feature.
PLATFORMISH = re.compile(r"\b(target_os|target_family|target_arch|target_env|unix|windows|miri)\b")


def platform_gates(files: list[RustFile]) -> dict[str, list[tuple[str, bool]]]:
    """Every platform `cfg` under `tests/`: `{file name: [(predicate, is_file_level)]}`.

    File-level `#![cfg(…)]` was the only form the first version looked for. One character
    less — `#[cfg(target_os = "windows")] #[test] fn …` — and a whole suite of tests reports
    `0 passed … ok` with the gate green (round 2, minor finding on B4). Both forms vanish the
    same way, so both are accounted for here.
    """
    gated: dict[str, list[tuple[str, bool]]] = {}
    for f in files:
        if "/tests/" not in f.rel or f.path.name == PLATFORM_COVERAGE.name:
            continue
        for m in re.finditer(r"#(!?)\[\s*cfg\s*\(", f.code):
            inner = m.group(1) == "!"
            lp = f.code.index("(", m.end() - 1)
            end = balanced_span(f.code, lp)
            pred = " ".join(f.src[lp + 1 : end - 1].split())
            if not PLATFORMISH.search(pred):
                continue
            if not inner:
                # Only an attribute on a test item, not on a helper fn or a use statement:
                # a gated helper cannot silently shrink the suite, a gated test can.
                after = f.code[end:]
                run = r"\s*\]\s*(?:#\[[^\]]*\]\s*)*"
                if not re.match(rf"{run}#\[\s*(?:tokio::)?test\b", after) and not re.match(
                    rf"{run}(?:pub\s+)?mod\b", after
                ):
                    continue
            gated.setdefault(f.path.name, []).append((pred, inner))
    return gated


# The guard, in the only two spellings that mean it. The condition must BE the CI check and
# nothing else — no disjunct, no `cfg!`, no extra term. A condition that can be satisfied
# ANOTHER WAY is not a guard: `assert!(std::env::var_os("CI").is_none() || cfg!(debug_assertions),
# …)` reads the environment variable, mentions no other environment, passes any token search —
# and never fires, because `debug_assertions` is on in every `cargo test` profile. That bypass
# was proven against the previous version of this check, in both copies of the macro, with the
# gate reporting success (issue #260 round 2, finding 1).
CI_GUARD = re.compile(
    r"""^(?:
          std::env::var_os\("CI"\)\.is_none\(\)
        | std::env::var\("CI"\)\.is_err\(\)
        )$""",
    re.X,
)


def check_skip_macro_is_fatal(src: str) -> list[str]:
    """The macro must ASSERT on `CI` — and the assertion must be able to FAIL.

    Two rounds of bypass live in this one function, and they are different in kind:

    1. The first version searched the file's RAW text for `var_os("CI")` and `assert!`, so
       deleting the assertion and leaving a comment quoting it passed the gate while
       `CI=true cargo test` ran the self-skip green. Fixed by reading only the macro's own
       `{ … }` body, located structurally, and only `assert!` CONDITIONS within it, comments
       blanked. Prose about a check is not the check.
    2. The second version asked whether a TOKEN appears in a condition — not whether the
       condition can be false. One always-true disjunct (`|| cfg!(debug_assertions)`) satisfied
       it and made the macro non-fatal everywhere. So the condition is now matched WHOLE
       against the two spellings that mean "CI is unset": anything else — a disjunction, a
       `cfg!`, an extra term — is rejected by name even if it also reads `CI`.

    What is still *not* verified here is the macro's behaviour: nothing in this repository runs
    the macro under `CI=true` and observes the failure. That residual is named in
    docs/TEST-PLAN.md § "What this gate detects, and what it cannot"; the check below is structural.

    `debug_assert!` cannot satisfy it either (`\\bassert` does not match inside it), because it
    compiles out of a release-profile test run.
    """
    code = blank_noncode(src, strings=False)
    m = re.search(rf"macro_rules!\s*{SKIP_MACRO_NAME}\b", code)
    if not m:
        return [
            f"{SKIP_MACRO_CANONICAL}: `macro_rules! {SKIP_MACRO_NAME}` is gone. Every "
            f"environmental skip in the tree routes through it; without it a skip is silent "
            f"again, which is issue #260 reopened."
        ]
    body = code[m.end() : brace_span(code, m.end())]
    conds = [c.replace(" ", "") for c in assert_conditions(body)]
    if any(CI_GUARD.match(c) for c in conds):
        # …and it must be REACHABLE. A correct condition inside `if false { … }` is a guard
        # that never runs, which both round-3 verifiers used to make every environmental skip
        # in the tree non-fatal while leaving the condition byte-identical. Nothing about the
        # condition can see that, so ask about position instead: the guard may not sit inside
        # a conditional. (Executed proof is the sibling test
        # `the_skip_macro_is_fatal_under_ci`, which runs the macro with CI=true in a
        # subprocess and observes the panic — the residual this check used to only name.)
        if guard_is_conditional(body):
            return [
                f"{SKIP_MACRO_CANONICAL}: the `CI` guard in `macro_rules! "
                f"{SKIP_MACRO_NAME}` is nested inside a conditional, so it can be skipped "
                f"without changing its condition — `if false {{ assert!(…) }}` keeps this "
                f"check's own text byte-identical and makes every environmental skip in the "
                f"tree silent under CI. The guard must be a statement of the macro body."
            ]
        return []
    near = [c for c in conds if re.search(r"\bvar(?:_os)?\(\"CI\"", c)]
    if near:
        return [
            f"{SKIP_MACRO_CANONICAL}: the `assert!` in `macro_rules! {SKIP_MACRO_NAME}` reads "
            f"the `CI` variable but its condition is not ONLY that check: {near[0]!r}. A "
            f"condition that can be satisfied another way is not a guard — one always-true "
            f"disjunct (`|| cfg!(debug_assertions)`, true in every `cargo test` profile) makes "
            f"every environmental skip in the tree silent again under CI, which is issue #260 "
            f"reopened. Write it as exactly `std::env::var_os(\"CI\").is_none()` (or "
            f"`std::env::var(\"CI\").is_err()`) and put any nuance in the message."
        ]
    return [
        f"{SKIP_MACRO_CANONICAL}: no `assert!` inside `macro_rules! {SKIP_MACRO_NAME}` has a "
        f"CONDITION that reads the `CI` environment variable, so an environmental skip is "
        f"silent again on the platform that gates merges — which is issue #260 reopened. A "
        f"comment quoting the assertion is not the assertion."
    ]


def guard_is_conditional(body: str) -> bool:
    """Is the `CI` assertion nested inside an `if`/`match` within the macro body?

    Position, not text: a condition cannot reveal that it never runs. Depth is counted over
    braces, and any `if`/`match` opened before the assertion and still open at it means the
    guard is skippable.
    """
    idx = body.find("assert!")
    if idx < 0:
        return False
    depth_stack: list[bool] = []  # one entry per open brace: True if opened by if/match
    i = 0
    while i < idx:
        ch = body[i]
        if ch == "{":
            head = body[max(0, i - 80) : i]
            depth_stack.append(bool(re.search(r"\b(if|match)\b[^;{}]*$", head)))
        elif ch == "}":
            if depth_stack:
                depth_stack.pop()
        i += 1
    return any(depth_stack)


def brace_span(code: str, start: int) -> int:
    """Offset just past the `{ … }` that begins at or after `start`."""
    i = code.find("{", start)
    if i < 0:
        return len(code)
    depth = 0
    while i < len(code):
        if code[i] == "{":
            depth += 1
        elif code[i] == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return len(code)


# A statement that does nothing at all: a discard binding of a literal, a unit expression, a
# stray semicolon. One of these in an `else` is what defeated the first version of B5 —
# `if probe() { assert_eq!(1, 2, "the real assertion"); } else { let _unused = (); }` made the
# chain look "closed", so the chain was not skippable, so the test's every-action-is-conditional
# question was never asked (issue #260 round 2, finding 2). `let _ = probe();` is NOT a no-op:
# it calls something, and a rule that guessed otherwise would flag correct code.
# A statement that does nothing. The RHS list is deliberately "any literal", not a menu of
# literal spellings: round 3 defeated the menu with `let _unused = 0u8;`, `= "";` and `= 1.0;`
# — three one-character variants of an integer that was already listed. Anything whose value
# is a literal (suffixed integer, float, string, byte string, char, bool, unit, None, or
# Default::default()) is a no-op; a CALL is not, because `let _ = probe();` has an effect.
NOOP_STMT = re.compile(
    r"""^(?:
          \(\s*\)
        | let\s+(?:mut\s+)?_\w*(?:\s*:\s*[\w:<>,&'\[\] ]+)?\s*=\s*
          (?:
              \(\s*\)                                   # unit
            | [0-9][0-9_]*(?:\.[0-9_]+)?(?:[iuf](?:8|16|32|64|128|size))?  # int/float, any suffix
            | 0[xob][0-9a-fA-F_]+(?:[iu](?:8|16|32|64|128|size))?          # hex/oct/bin
            | b?"(?:[^"\\]|\\.)*"                        # string / byte string
            | b?'(?:[^'\\]|\\.)*'                        # char / byte char
            | true|false|None
            | Default::default\(\s*\)
          )
        )$""",
    re.X,
)


def _does_nothing(text: str) -> bool:
    """True if `text` (a block body, code view) contains no statement that does anything."""
    for stmt in text.split(";"):
        s = " ".join(stmt.strip().strip("{}").split())
        if not s or s in {"else", "}"} or s.startswith("#["):
            continue
        if NOOP_STMT.match(s):
            continue
        return False
    return True


def skippable_if_chains(f: RustFile, block: Block) -> list[tuple[int, int]]:
    """Spans of the `if …` chains directly inside `block` that may be skipped entirely.

    "May be skipped" means: no final `else` (so the whole chain can be a no-op), or a branch
    that DOES NOTHING — lexically empty, or filled with no-op statements only, which is the
    same thing wearing an else's clothes.
    """
    kids = f.children(block)
    spans: list[tuple[int, int]] = []
    i = 0
    while i < len(kids):
        c = kids[i]
        head = c.header.rstrip()
        if not re.search(r"\bif\b", head) or re.search(r"\b(while|for|match)\b", head):
            i += 1
            continue
        branches, j, closed = [c], i + 1, False
        while j < len(kids):
            if not re.match(r"^\s*else\b", f.code[kids[j - 1].close + 1 : kids[j].open]):
                break
            branches.append(kids[j])
            j += 1
            if not re.search(r"\bif\b", branches[-1].header):
                closed = True
                break
        empty = any(_does_nothing(f.body(b)) for b in branches)
        if not closed or empty:
            hstart = c.open - len(c.header) + [m.start() for m in re.finditer(r"\bif\b", c.header)][-1]
            spans.append((hstart, branches[-1].close))
        i = j
    return spans


def every_action_is_conditional(f: RustFile, block: Block) -> bool:
    """Does this test body do NOTHING unless a condition holds?

    The proven attack this exists to stop is a test whose whole body is wrapped:

        #[tokio::test] async fn t() {
            if second_loopback_bindable() { assert_eq!(1, 2, "the real assertion"); }
        }

    No `return`, no `println!`, no macro — nothing for B1 or B2 to see — and green under
    `CI=true` (round 2, finding 2). B1's structural insight was that a test has no legitimate
    reason to *return* early; the same insight one level up is that a test has no legitimate
    reason to be *entirely* optional.

    The question deliberately is not "does every path assert?" — most tests here assert
    through helpers (`sub.subscribe(1, t, QoS::AtLeastOnce)` asserts the SUBACK; `roundtrip(p)`
    asserts the decode), and a rule that could not see through a call would have flagged ~70
    correct tests, which is how a gate gets worked around. The question is narrower and
    answerable from structure alone: outside the skippable `if` chains, does this test *do*
    anything at all besides bind names?
    """
    spans = skippable_if_chains(f, block)
    if not spans:
        return False
    outside = "".join(f.code[a:b] for a, b in _gaps(block.open + 1, block.close, spans))
    for stmt in outside.split(";"):
        s = " ".join(stmt.strip().strip("{}").split())
        if not s or s.startswith("let ") or s.startswith("#[") or s in {"else", "}"}:
            continue
        if NOOP_STMT.match(s):
            continue  # `()` and `let _unused = ()` are not work anywhere, not just in an else
        return False  # unconditional work: this test is not merely optional
    return True


def _gaps(start: int, end: int, spans: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """`[start, end)` minus `spans` — the text of a block that is not inside a child block."""
    out, cur = [], start
    for a, b in sorted(spans):
        if a > cur:
            out.append((cur, min(a, end)))
        cur = max(cur, b + 1)
    if cur < end:
        out.append((cur, end))
    return out


def assert_conditions(src: str) -> list[str]:
    """The first argument of every `assert!`-family macro in `src` — the condition only.

    Balanced on the RAW text rather than the blanked copy, because a condition legitimately
    contains a string literal: `cfg!(target_os = "linux")` is the entire point here. The scan
    stops at the first comma at depth 1, which is the boundary between condition and message.
    """
    out = []
    for m in re.finditer(r"\bassert\w*!\s*\(", src):
        i = start = m.end()
        depth = 1
        while i < len(src) and depth:
            c = src[i]
            if c == "(":
                depth += 1
            elif c == ")":
                depth -= 1
                if depth == 0:
                    break
            elif c == "," and depth == 1:
                break
            i += 1
        out.append(" ".join(src[start:i].split()))
    return out


# --------------------------------------------------------------------------------------
# Check C — the same rule for shell, where CI was being trusted by comment
# --------------------------------------------------------------------------------------

# Case-INSENSITIVE deliberately. The first version of this matched `SKIP` in capitals and
# reported success on `echo "  (kubeconform not installed — skipping schema validation)"` —
# the same silent skip, spelled in lower case. A check that only finds the shouted ones is
# the vacuous half of a gate.
# The announcement forms only. "the nightly lane skips loudly" is prose ABOUT another lane's
# documented skip, not this script announcing one, and a gate that flags prose gets reworded
# around rather than obeyed.
SH_SKIP = re.compile(r"\bskip(?:ped|ping)?\b", re.I)
# Output, or a one-line local wrapper for it (`note "…"`). The message may be anywhere on the
# line: `|| { echo "tool missing, skipping"; }` defeated an `^`-anchored rule.
SH_ANNOUNCES = re.compile(r"\b(?:echo|printf)\b")
# "this cannot be skipped", "must not be skipped" — prose in a FAILURE message, the opposite
# of an announcement that something was skipped.
SH_NOT_A_SKIP = re.compile(r"(?:cannot|can't|must\s+not|will\s+not|never|no[tn]e?)\s+(?:be\s+)?skip", re.I)
SH_HELPERS = ("skip_or_fail", "skip_permitted")

# A capability probe. The dangerous vacuous CI pass is not the one that announces itself in the
# wrong case — it is `command -v tool >/dev/null 2>&1 || exit 0`, which prints NOTHING and exits
# green, and which no message matcher can ever see (issue #260 round 2, finding 6).
SH_PROBE = re.compile(
    r"\bcommand\s+-v\b|\bwhich\s+\w|\btype\s+-[pP]\b|\bhash\s+\w+\s*(?:>|2>|\|\||$)"
    r"|\[\[?\s+-[xefd]\s",
)
SH_SILENT_OK = re.compile(r"\b(?:exit|return)\s+0\b")
# A top-level `exit 0` that is not the end of the script. Whatever probe led to it, a gate that
# leaves with a success status before it has finished is the defect — so this is a whitelist of
# SANCTIONED success exits rather than a blacklist of probe spellings, which is the only form of
# this rule that does not lose to the next spelling (round 2, finding 6: a `have()` helper, an
# env-var test, a `uname` test and seven lines of distance each defeated the vocabulary). The
# sanctioned forms are: a declared skip (`skip_or_fail` / `skip_permitted` immediately above),
# the last statement of the script, or an at-site annotation saying why this success is real.
SH_EXIT_OK = re.compile(r"\bexit\s+0\b")
SH_NOT_A_SKIP_MARK = re.compile(r"#\s*NOT-A-SKIP\s*:\s*(\S.*)")
SH_MARK_MIN_CHARS = 30

# The same hole in the Python gates a workflow runs. Narrow on purpose: a capability probe
# whose failure path leaves with a success code. (A message rule would have to distinguish a
# gate's own error prose about skips from an actual skip, which it cannot.)
# Not only an EXECUTABLE probe: `if not os.environ.get("MQTT_PORT"): sys.exit(0)` and
# `except ImportError: raise SystemExit(0)` are the two spellings that got a Python gate to
# report success having imported nothing and run nothing (round 2, finding 6). Neither costs a
# false positive in the gates CI runs today, which is the test for whether a rule can be added.
PY_PROBE = re.compile(
    r"\bshutil\.which\s*\(|\bos\.access\s*\(|\bfind_executable\s*\("
    r"|\bos\.environ\.get\s*\(|\bos\.getenv\s*\(|except\s+ImportError"
)
PY_SILENT_OK = re.compile(
    r"\bsys\.exit\s*\(\s*0?\s*\)|\braise\s+SystemExit\s*\(\s*0?\s*\)|^\s*return\s*(?:0\s*)?$",
    re.M,
)

# The shell helper is inlined rather than sourced from scripts/lib/: mqttui's manifest
# (ADR 0056 §3) walks every .sh under scripts/, so a shared file would need its own task
# row and README count bump for five lines. Inlined copies drift, so they are compared.
SH_HELPER_TEXT = """\
# --- CI-fatal skips (issue #260) -------------------------------------------------------
# A skip that prints a note and exits 0 is indistinguishable from a pass, so coverage can
# vanish on the platform that gates merges without anything going red. Allowed locally,
# fatal under CI (GitHub Actions sets CI=true on every runner). `skip_permitted` is the one
# deliberate exception: a lane that genuinely cannot run in CI stays green and says why.
skip_or_fail() {
  if [ "${CI:-}" = "true" ]; then
    echo "FATAL: environmental skip taken under CI — coverage would silently vanish: $1" >&2
    exit 1
  fi
  echo "  SKIP (local only; fatal under CI) — $1"
}
skip_permitted() { echo "  SKIP (permitted in CI by design) — $1"; }
"""


SCRIPT_REF = re.compile(r"[A-Za-z0-9_./-]*\.(?:sh|py)\b")


def resolve_script(ref: str) -> Path | None:
    """A script reference in a workflow or script, resolved to a file in this tree.

    References are not always repo-relative: `cd scripts && ./run.sh` and a bare `run.sh` both
    name a real gate, and the first version's `scripts/…\\.sh` pattern silently dropped both —
    the same derivation that auto-INCLUDES a newly wired script auto-EXCLUDED a differently
    spelled one (round 2, minor finding on scope). A basename with exactly one match under
    `scripts/` resolves; an ambiguous one does not (guessing would be worse than the gap).
    """
    ref = ref.lstrip("./")
    p = ROOT / ref
    if p.is_file():
        return p
    hits = [q for q in (ROOT / "scripts").rglob(Path(ref).name) if q.is_file()]
    return hits[0] if len(hits) == 1 else None


def ci_run_scripts() -> set[Path]:
    """Every shell script a workflow runs, transitively — DERIVED, not listed.

    The property being checked is narrow and specific: *a script CI runs as a gate must not
    report success without running its checks*. A script CI never invokes cannot produce a
    vacuous CI pass, and `scripts/migrate/cert-audit.sh` — a user-facing audit tool whose
    per-certificate "skip a CA, it is not a client credential" is a documented, correct
    decision — must not be dragged into a rule written for gates. Deriving the set from
    `.github/workflows/` rather than writing it down means adding a script to a job brings it
    under the rule automatically, which an allowlist would not.
    """
    frontier: set[Path] = set()
    wf_dir = ROOT / ".github" / "workflows"
    # `*.yml` AND `*.yaml`: GitHub accepts both, and a workflow saved with the other spelling
    # would have left every script it runs outside the rule.
    for wf in sorted(list(wf_dir.glob("*.yml")) + list(wf_dir.glob("*.yaml"))):
        for ref in SCRIPT_REF.findall(sh_code(wf.read_text(encoding="utf-8"))):
            if p := resolve_script(ref):
                frontier.add(p)
    seen: set[Path] = set()
    while frontier:
        p = frontier.pop()
        if p in seen or not p.is_file():
            continue
        seen.add(p)
        # A `.py` gate is a leaf of the derivation. Following references OUT of one drags in
        # whatever it merely *mentions*: `from-emqx.py` advises the reader to run
        # `scripts/migrate/cert-audit.sh`, and that pulled a user-facing audit tool — whose
        # per-certificate "skip a CA, it is not a client credential" is a documented, correct
        # decision — under a rule written for gates.
        if p.suffix != ".sh":
            continue
        for ref in SCRIPT_REF.findall(sh_code(p.read_text(encoding="utf-8"))):
            if q := resolve_script(ref):
                frontier.add(q)
    return seen


def sh_code(src: str) -> str:
    """`src` with comments blanked, offsets and line numbers preserved.

    Trailing comments count: `foo || exit 0   # documented above` is code with a comment on
    it, and the first version only dropped whole-line comments. A `#` inside quotes is not a
    comment, so quoting is tracked.
    """
    out: list[str] = []
    for ln in src.split("\n"):
        if ln.lstrip().startswith("#"):
            out.append(" " * len(ln))
            continue
        # Quote state is tracked within the line only. An apostrophe in prose ("the
        # directory's owner") would otherwise open a quote that swallows every following
        # line — which is exactly how the first version left 14 comments unstripped and
        # reported them as skips.
        q: str | None = None
        cut = len(ln)
        for i, c in enumerate(ln):
            if q:
                if c == q:
                    q = None
                continue
            if c in "'\"":
                q = c
            elif c == "#" and (i == 0 or ln[i - 1] in " \t;|&()"):
                cut = i
                break
        out.append(ln[:cut] + " " * (len(ln) - cut))
    return "\n".join(out)


def logical_lines(code: str) -> list[tuple[int, str]]:
    """(1-indexed start line, text) with backslash continuations joined."""
    out: list[tuple[int, str]] = []
    buf, start = "", 1
    for n, ln in enumerate(code.splitlines(), 1):
        if not buf:
            start = n
        buf += ln
        if ln.rstrip().endswith("\\"):
            buf = buf.rstrip()[:-1] + " "
            continue
        out.append((start, buf))
        buf = ""
    if buf:
        out.append((start, buf))
    return out


def enclosing_fns(lines: list[str]) -> list[str | None]:
    """Per line, the shell function it is inside — brace-tracked, not "the nearest `name() {`".

    The backwards search this replaces could not see a function CLOSING: a one-line
    `skip_permitted() { echo …; }` made every line after it look like part of the sanctioned
    helper, which exempted the two silent-skip attacks injected directly below it. A rule that
    stops looking after the first thing that resembles an answer is how a check goes vacuous.
    """
    out: list[str | None] = []
    stack: list[tuple[str | None, int]] = []
    for ln in lines:
        m = re.match(r"\s*(?:function\s+)?(\w+)\s*(?:\(\s*\))?\s*\{", ln)
        # The defining line belongs to the function it defines — a one-line
        # `skip_permitted() { echo "SKIP …"; }` is the helper, not a script-level skip.
        out.append(m.group(1) if m else (stack[-1][0] if stack else None))
        depth = ln.count("{") - ln.count("}")
        if m and depth > 0:
            stack.append((m.group(1), depth))
        elif stack:
            stack[-1] = (stack[-1][0], stack[-1][1] + depth)
        while stack and stack[-1][1] <= 0:
            stack.pop()
    return out


def sh_unquoted(text: str) -> str:
    """`text` with quoted runs blanked — for rules about CODE, not about messages.

    `ok "an empty config reports itself (exit 0)"` is a test script saying what the converter
    does, not a script exiting; the success-exit rule reads this view, while the skip-message
    rules deliberately read the quoted text.
    """
    out, q = [], None
    for c in text:
        if q:
            out.append(" ")
            if c == q:
                q = None
            continue
        if c in "'\"":
            q = c
            out.append(" ")
            continue
        out.append(c)
    return "".join(out)


def probe_fns(lines: list[str], fns: list[str | None]) -> set[str]:
    """Shell functions whose body runs a capability probe — one level of indirection.

    `have() { command -v "$1" >/dev/null 2>&1; }` used far below as `have kubeconform || exit 0`
    defeats a rule that looks for probe TOKENS: the use site has no probe in it and the
    definition has no success exit near it (round 2, finding 6, shape 1). Resolving one level
    closes that; a helper that calls a helper that probes is not resolved, and the
    sanctioned-success-exit rule below is what covers the general case.
    """
    out: set[str] = set()
    for ln, fn in zip(lines, fns):
        if fn and SH_PROBE.search(ln):
            out.add(fn)
    return out


def check_shell() -> list[str]:
    errs: list[str] = []
    for p in sorted(ci_run_scripts()):
        src = p.read_text(encoding="utf-8")
        rel = p.relative_to(ROOT)
        if p.suffix == ".py":
            errs += check_py_gate(rel, src)
            continue
        errs += check_sh_script(rel, src)
    return errs


def check_sh_script(rel: Path, src: str) -> list[str]:
    """Every shell rule, against one script's text.

    Split out from `check_shell` so `--self-test` can drive the rules with fixtures. A rule
    that can only be exercised by committing a real script to the repository is a rule whose
    boundaries nobody can check, which is how this gate came to have three defects of its own
    (issue #260): the boundary cases are exactly what goes wrong, and they are exactly what a
    fixture can state and a real script cannot.
    """
    errs: list[str] = []
    if any(f"{h}()" in src or f"{h} " in src for h in SH_HELPERS):
        if SH_HELPER_TEXT not in src:
            errs.append(
                f"{rel}: uses the CI-fatal skip helpers but its inlined "
                f"copy is not byte-identical to the canonical text in "
                f"scripts/check-test-hygiene.py (SH_HELPER_TEXT). One drifted copy is one "
                f"script where a skip is silent again."
            )
    code = sh_code(src)
    raw_lines = code.splitlines()
    src_lines = src.splitlines()
    fns = enclosing_fns(raw_lines)
    helpers = probe_fns(raw_lines, fns)
    # A call to a probing helper IS a probe, at the call site: `have kubeconform || exit 0`.
    probe_call = (
        re.compile(r"(?:^|[|&;({!]|\bthen\b|\bif\b|\bdo\b)\s*(?:!\s*)?(?:%s)\b" % "|".join(sorted(map(re.escape, helpers))))
        if helpers
        else None
    )

    # The sanctioned-success-exit rule. `exit 0` inside a function is a function's own
    # success (`wait_ready`), not the script leaving; only top-level exits are gate exits.
    last_code_line = max(
        (i + 1 for i, ln in enumerate(raw_lines) if ln.strip()), default=0
    )
    for line, text in logical_lines(code):
        if not SH_EXIT_OK.search(sh_unquoted(text)) or (
            line - 1 < len(fns) and fns[line - 1] is not None
        ):
            continue
        if line >= last_code_line:
            continue  # the script's own final `exit 0`
        window = "\n".join(raw_lines[max(0, line - 4) : line])
        if any(h in window for h in SH_HELPERS):
            continue  # a declared skip, fatal or permitted, said so immediately above
        mark = next(
            (
                mk.group(1).strip()
                for probe in range(max(0, line - 5), min(line, len(src_lines)))
                if (mk := SH_NOT_A_SKIP_MARK.search(src_lines[probe]))
            ),
            None,
        )
        if mark is not None and len(mark) >= SH_MARK_MIN_CHARS:
            continue
        errs.append(
            f"{rel}:{line}: this script leaves with a SUCCESS status before its end, and "
            f"nothing says the checks ran. Whatever led here — a `command -v`, a helper that "
            f"probes, an unset variable, a `uname` — a gate that exits 0 early is "
            f"indistinguishable from one that passed, which is the whole of issue #260 in "
            f"shell. Route it through `skip_or_fail` (fatal under CI) or `skip_permitted`, "
            f"or, if this success is real, annotate the line "
            f"`# NOT-A-SKIP: <why, {SH_MARK_MIN_CHARS}+ chars>`"
            + (f" (found only {len(mark)} chars)" if mark is not None else "")
            + "."
        )

    reported: set[int] = set()
    for line, text in logical_lines(code):
        fn_of = fns[line - 1] if line - 1 < len(fns) else None
        if fn_of in SH_HELPERS or any(f"{h} " in text for h in SH_HELPERS):
            continue
        # Any mention of skipping, anywhere on the line — not only at the start of an
        # `echo`. The three spellings that got through were `|| { echo "…skipping…"; }`
        # (an echo after `|| {`), a `note "skipping…"` wrapper, and an anchor-defeating
        # mid-line echo. The mechanism, not the spelling, is what has to be covered.
        probes = SH_PROBE.search(text) or (probe_call and probe_call.search(text))
        announces = SH_ANNOUNCES.search(text) or probes
        # …but a mention of skipping is only an ANNOUNCEMENT if the script then actually
        # stops. `echo "building mqttd (set MQTTD_BIN to skip)…"` is prose about a
        # configuration option, immediately followed by the build it describes — flagging
        # it would train authors to reword honest messages to appease the gate, which is
        # how a gate stops being read. Require a real exit/return within a few lines.
        follows_with_stop = bool(
            re.search(
                r"\b(exit|return)\b",
                "\n".join(code.splitlines()[line - 1 : line + 4]),
            )
        )
        if (
            announces
            and SH_SKIP.search(text)
            and follows_with_stop
            and not SH_NOT_A_SKIP.search(text)
        ):
            reported.add(line)
            errs.append(
                f"{rel}:{line}: announces a skip outside the sanctioned helpers. Use "
                f"`skip_or_fail \"<reason>\"` (fatal when CI=true) or, for a lane that is "
                f"legitimately unrunnable in CI by design, `skip_permitted \"<reason>\"` — "
                f"so a skip is either impossible in CI or a deliberate, named exception."
            )
        elif probes and SH_SILENT_OK.search(text):
            reported.add(line)
            errs.append(
                f"{rel}:{line}: a capability probe leaves with a SUCCESS status and no "
                f"message at all (`|| exit 0`) — the one vacuous CI pass no message rule "
                f"can ever see, because there is no message. Route it through "
                f"`skip_or_fail`/`skip_permitted`, which say what did not run and which is "
                f"fatal under CI."
            )
    # A probe on one line and a silent success exit a few lines below, with nothing in
    # between saying so: the multi-line spelling of the same thing.
    multi = (
        re.compile(f"{SH_PROBE.pattern}|{probe_call.pattern}") if probe_call else SH_PROBE
    )
    for m in multi.finditer(code):
        start = code[: m.start()].count("\n")
        if start + 1 in reported:
            continue
        window = raw_lines[start : start + 6]
        if not any(SH_SILENT_OK.search(w) for w in window):
            continue
        upto = "\n".join(window[: 1 + next(i for i, w in enumerate(window) if SH_SILENT_OK.search(w))])
        if any(h in upto for h in SH_HELPERS) or (
            start < len(fns) and fns[start] in SH_HELPERS
        ):
            continue
        if SH_SKIP.search(upto):
            continue  # already reported above by the message rule
        errs.append(
            f"{rel}:{start + 1}: a capability probe here reaches `exit 0`/`return 0` "
            f"within {len(window)} lines without announcing anything — a silent green "
            f"pass for a check that did not run. Use `skip_or_fail`/`skip_permitted`."
        )
    return errs


def py_code(src: str) -> str:
    """`src` with comments and string literals blanked, offsets and line numbers preserved.

    The same rule as everywhere else in this file, for the same reason: a probe INSIDE A STRING
    is not a probe. This one bit immediately — the moment `os.environ.get(` joined `PY_PROBE`,
    the pattern's own source text made this file trip its own rule twice. Python's tokenizer is
    used rather than a hand-rolled scanner, because f-strings and triple quotes are exactly where
    a hand-rolled one would be wrong.
    """
    import io
    import tokenize

    lines = src.splitlines(keepends=True)
    try:
        toks = list(tokenize.generate_tokens(io.StringIO(src).readline))
    except (tokenize.TokenError, IndentationError, SyntaxError):
        return src  # unparseable: read it raw rather than silently checking nothing
    out = [list(ln) for ln in lines]
    for tok in toks:
        if tok.type not in (tokenize.STRING, tokenize.COMMENT):
            continue
        (r1, c1), (r2, c2) = tok.start, tok.end
        for r in range(r1, r2 + 1):
            row = out[r - 1]
            a = c1 if r == r1 else 0
            b = c2 if r == r2 else len(row)
            for k in range(a, min(b, len(row))):
                if row[k] != "\n":
                    row[k] = " "
    return "".join("".join(r) for r in out)


def check_py_gate(rel: Path, src: str) -> list[str]:
    """The Python gates a workflow runs, held to the same rule as the shell ones.

    Check C was `.sh`-only while `scripts/interop/paho_conformance.py` runs as the interop
    conformance gate (round 2, minor finding on scope). Only the capability-probe hole is
    checked here, deliberately: a message rule cannot tell a gate's own error prose about
    skipping from an actual skip, and this very file would trip it.
    """
    errs: list[str] = []
    src = py_code(src)
    lines = src.splitlines()
    for m in PY_PROBE.finditer(src):
        start = src[: m.start()].count("\n")
        window = lines[start : start + 6]
        if any(PY_SILENT_OK.search(w) for w in window):
            errs.append(
                f"{rel}:{start + 1}: a capability probe reaches a success exit within "
                f"{len(window)} lines — a gate that reports success without running its "
                f"checks. Fail instead when `CI` is set (GitHub Actions sets `CI=true` on "
                f"every runner), the way `skip_or_fail` does in the shell gates."
            )
    return errs


# --------------------------------------------------------------------------------------
# Check D — the test INVENTORY: what the binaries actually contain
#
# Everything above reads source text, and text is porous by construction: a rule can only see
# a shape it was told about, and this file has now been through two rounds of finding shapes it
# was not. The inventory is a different KIND of check — it asks the compiled artifact what
# tests it contains, and compares that against a checked-in list. That catches, with no shape
# knowledge at all:
#
#   * a test `cfg`-gated out of existence (the hole check B4 can only account for, never see);
#   * a whole file that compiled to zero tests and reported success;
#   * a test silently deleted or renamed in a diff nobody read.
#
# `cargo test -- --list` is the source of truth, so the check cannot be talked out of it. The
# list is GENERATED (`--write-inventory`), the way `gen-status.py` generates the dashboard:
# adding a test is one regeneration, not a hand-edited manifest, or the manifest becomes a tax
# and then a lie.
# --------------------------------------------------------------------------------------

INVENTORY = ROOT / "docs" / "test-inventory.md"
WORKSPACES = {"root": ["--workspace"], "mqttui": ["--manifest-path", "tools/mqttui/Cargo.toml"]}


def host_cfg(pred: str) -> bool:
    """Evaluate a `cfg` predicate for THIS host — enough of one to know if code vanishes here.

    A manifest generated on a Mac and checked on a Linux runner would otherwise disagree about
    every `#![cfg(target_os = "linux")]` suite, and the check would be turned off within a week.
    """
    import platform

    osname = {"darwin": "macos", "linux": "linux", "win32": "windows"}.get(
        sys.platform, sys.platform
    )
    facts = {
        "unix": osname in {"macos", "linux"},
        "windows": osname == "windows",
        "miri": False,
        "test": True,
        "debug_assertions": True,
    }
    kv = {
        "target_os": osname,
        "target_family": "unix" if facts["unix"] else "windows",
        "target_arch": {"arm64": "aarch64"}.get(platform.machine(), platform.machine()),
    }
    p = pred.strip()
    for op, fold in (("all", all), ("any", any)):
        if p.startswith(f"{op}("):
            return fold(host_cfg(a) for a in _split_args(p[len(op) + 1 : -1]))
    if p.startswith("not("):
        return not host_cfg(p[4:-1])
    if m := re.match(r"(\w+)\s*=\s*\"([^\"]*)\"", p):
        return kv.get(m.group(1)) == m.group(2)
    return facts.get(p, True)  # an unknown predicate is assumed to hold: fail loud, not silent


def _split_args(s: str) -> list[str]:
    out, depth, cur = [], 0, ""
    for c in s:
        if c == "," and depth == 0:
            out.append(cur)
            cur = ""
            continue
        depth += (c == "(") - (c == ")")
        cur += c
    if cur.strip():
        out.append(cur)
    return [a.strip() for a in out if a.strip()]


def file_level_cfg(path: Path) -> str | None:
    """The `#![cfg(…)]` predicate at the top of a test file, if it has one."""
    if not path.is_file():
        return None
    code = blank_noncode(path.read_text(encoding="utf-8"))
    m = re.search(r"#!\[\s*cfg\s*\(", code)
    if not m:
        return None
    lp = code.index("(", m.end() - 1)
    src = path.read_text(encoding="utf-8")
    return " ".join(src[lp + 1 : balanced_span(code, lp) - 1].split())


def module_path(rel: str, root: str) -> list[str]:
    """The Rust module path of `rel` within the target rooted at `root`.

    The target root is the crate root: an integration test's own file names its tests bare,
    while `src/foo.rs` under a lib root prefixes them `foo::`. Getting this wrong would put a
    phantom prefix on every name and fail CI on the first run, so it is derived from the
    target cargo reported rather than guessed from the directory name.
    """
    if rel == root:
        return []
    parts = list(Path(rel).relative_to(Path(root).parent).parts)
    if parts and parts[-1] == "mod.rs":
        parts.pop()
    elif parts:
        parts[-1] = parts[-1].removesuffix(".rs")
    return parts


def tests_in_file(f: RustFile, root: str) -> list[tuple[str, str | None, bool]]:
    """[(full test path, the platform `cfg` gating it, is it `#[ignore]`d)] for one file."""
    out = []
    for b in f.blocks:
        if not b.is_test_fn:
            continue
        ignored = bool(re.search(r"#\[\s*ignore\b", b.header))
        mods = [
            m.group(1)
            for blk in f.chain(b.open) + [b]
            if (m := re.search(r"\bmod\s+(\w+)\s*$", blk.header.rstrip()))
        ]
        name = "::".join(module_path(f.rel, root) + mods + [b.fn_name or "?"])
        pred = None
        base = b.open - len(b.header)  # the header's offset in BOTH views
        for m in re.finditer(r"#\[\s*cfg\s*\(", b.header):
            lp = b.header.index("(", m.end() - 1)
            end = balanced_span(b.header, lp)
            # The predicate is read from the RAW text: `cfg(target_os = "linux")` is one of
            # the few places a string literal IS the meaning, and the blanked view has
            # nothing but `target_os =` left.
            p = " ".join(f.src[base + lp + 1 : base + end - 1].split())
            if PLATFORMISH.search(p):
                pred = p
        out.append((name, pred, ignored))
    return out


def tests_from_source(path: Path, root: str) -> list[tuple[str, str | None, bool]]:
    """Test names read from the source — the fallback for a suite this host cannot compile.

    Used when a file-level `cfg` excludes the file here, so that the manifest can still be
    regenerated (and still checked, against the source) off the platform that runs it.
    """
    return sorted(tests_in_file(parse(path), root))


def cargo_test_binaries(which: str) -> list[tuple[str, Path]]:
    """[(source path relative to the repo, test executable)] for one workspace."""
    import json
    import subprocess

    out = subprocess.run(
        ["cargo", "test", *WORKSPACES[which], "--no-run", "--message-format=json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if out.returncode != 0:
        raise SystemExit(
            f"check-test-hygiene: `cargo test --no-run` failed for the {which} workspace, so "
            f"the test inventory cannot be taken:\n{out.stderr[-4000:]}"
        )
    found = []
    for ln in out.stdout.splitlines():
        try:
            msg = json.loads(ln)
        except ValueError:
            continue
        if msg.get("reason") != "compiler-artifact" or not msg.get("executable"):
            continue
        if not msg.get("profile", {}).get("test"):
            continue
        src = Path(msg["target"]["src_path"])
        try:
            rel = str(src.relative_to(ROOT))
        except ValueError:
            continue
        found.append((rel, Path(msg["executable"])))
    return sorted(set(found))


def target_sources(rel: str) -> list[Path]:
    """The source files that could contribute tests to the target rooted at `rel`.

    Approximate on purpose, and only ever used to find tests that this host cannot compile:
    a lib target owns its crate's `src/` (minus the binaries), everything else owns its own
    file. Over-attribution would show up immediately as an inventory mismatch, not silently.
    """
    p = ROOT / rel
    if p.name == "lib.rs":
        return sorted(
            q
            for q in p.parent.rglob("*.rs")
            if q.name != "main.rs" and "bin" not in q.relative_to(p.parent).parts
        )
    return [p]


@functools.lru_cache(maxsize=None)
def take_inventory(
    only: str | None,
) -> dict[str, tuple[list[tuple[str, str | None, bool]], str | None]]:
    """{source path: ([(test name, the platform cfg gating it, `#[ignore]`d)], file-level cfg)}.

    A test gated on a platform is recorded WITH its predicate rather than left out, so one
    inventory serves every host: a Mac and a Linux runner disagree about which tests exist,
    and a manifest that could not say so would either be regenerated per platform (useless)
    or turned off (worse). `memory_watch.rs`'s Linux-only RSS test is the live case.

    `#[ignore]`d status is recorded for the same reason, and it is asked of the BINARY
    (`--list --ignored`) rather than read from source: one attribute removes a test from every
    run while `--list` still prints it, so an inventory that recorded only names was
    byte-identical before and after a test was retired (issue #260 round 2, finding 4).
    """
    import subprocess

    inv: dict[str, tuple[list[tuple[str, str | None, bool]], str | None]] = {}
    for which in WORKSPACES if only is None else [only]:
        for rel, exe in cargo_test_binaries(which):
            filecfg = file_level_cfg(ROOT / rel)
            if filecfg is not None and not host_cfg(filecfg):
                inv[rel] = (tests_from_source(ROOT / rel, rel), filecfg)
                continue

            def listed(args: list[str]) -> set[str]:
                out = subprocess.run(
                    [str(exe), *args], capture_output=True, text=True, check=False
                )
                return {
                    ln.rsplit(":", 1)[0] for ln in out.stdout.splitlines() if ln.endswith(": test")
                }

            here = listed(["--list"])
            skipped = listed(["--list", "--ignored"])
            names = [(n, None, n in skipped) for n in here]
            # Tests this host cannot see because a `cfg` excluded them: recorded with the
            # predicate, so the platform that DOES compile them still checks them.
            for src in target_sources(rel):
                for name, pred, ign in tests_in_file(parse(src), rel):
                    if pred and not host_cfg(pred) and name not in here:
                        names.append((name, pred, ign))
            inv[rel] = (sorted(names), filecfg)
    return inv


def render_inventory(inv: dict[str, tuple[list[tuple[str, str | None, bool]], str | None]]) -> str:
    head = """<!-- GENERATED by scripts/check-test-hygiene.py --write-inventory. Do not edit by hand. -->
# Test inventory

Every test each binary in this repository actually contains, as reported by
`cargo test -- --list`, compared in CI by `scripts/check-test-hygiene.py --check-inventory`.

Issue #260's two defects — a test that skips itself and a wait that hides a broken observable
— are both visible in source text, so the rest of that gate reads source text. This file
exists for the third kind, which is not: a test that **is not there**. A `#[cfg]` that excludes
it, a file that compiles to zero tests, a rename, a deletion in a large diff — each of them
leaves `cargo test` reporting success over a smaller suite, and no amount of pattern matching
over source can reliably tell a suite that shrank from one that passed.

Regenerate with `scripts/check-test-hygiene.py --write-inventory` after adding, renaming or
deliberately removing a test. The diff is the review: a line disappearing here is a test
disappearing from CI.

A test annotated *only when `cfg(...)`* does not exist on every platform. It is recorded with
its predicate rather than left out, so one inventory serves a Mac and a Linux runner alike:
each host expects exactly the tests its own `cfg` evaluation says it should have. Suites whose
WHOLE file is gated are read from source when this host cannot compile them, which is how they
stay checkable off the platform that runs them.

A test annotated **`#[ignore]`d** is in the binary and out of every run. That is the cheapest
way to retire a test, and it used to change nothing here (`--list` still prints an ignored
test), so it is recorded: `--check-inventory` fails when a test becomes ignored without this
file changing, every ignored test must be declared in `IGNORE_ALLOWLIST` with the tier that
runs it, and `--check-results` fails if the ignored set in an actual run differs from this one.
"""
    body = []
    for rel, (names, filecfg) in sorted(inv.items()):
        tag = f" — the whole file is `cfg({filecfg})`" if filecfg else ""
        body.append(f"\n## `{rel}` — {len(names)} test(s){tag}\n")
        for n, p, ign in names:
            notes = ([f"only when `cfg({p})`"] if p else []) + (["`#[ignore]`d"] if ign else [])
            body.append(f"- `{n}`" + (f" — {', '.join(notes)}" if notes else ""))
    return head + "\n".join(body) + "\n"


def parse_inventory(text: str) -> dict[str, tuple[list[tuple[str, str | None, bool]], str | None]]:
    """The checked-in inventory, read back: {rel: ([(name, cfg, ignored)], file-level cfg)}."""
    inv: dict[str, tuple[list[tuple[str, str | None, bool]], str | None]] = {}
    cur: str | None = None
    for ln in text.splitlines():
        if m := re.match(r"##\s+`([^`]+)`", ln):
            cur = m.group(1)
            fc = re.search(r"the whole file is `cfg\((.*)\)`", ln)
            inv[cur] = ([], fc.group(1) if fc else None)
        elif cur and (m := re.match(r"-\s+`([^`]+)`(?:\s+—\s+(.*))?$", ln)):
            notes = m.group(2) or ""
            pred = c.group(1) if (c := re.search(r"only when `cfg\((.*?)\)`", notes)) else None
            inv[cur][0].append((m.group(1), pred, "`#[ignore]`d" in notes))
    return inv


def check_inventory(only: str | None) -> list[str]:
    if not INVENTORY.is_file():
        return [
            f"{INVENTORY.relative_to(ROOT)} is missing; run "
            f"`scripts/check-test-hygiene.py --write-inventory`."
        ]
    recorded = parse_inventory(INVENTORY.read_text(encoding="utf-8"))
    actual = take_inventory(only)
    errs: list[str] = check_ignore_allowlist(recorded)
    for rel, (names, filecfg) in sorted(actual.items()):
        if rel not in recorded:
            errs.append(
                f"{INVENTORY.relative_to(ROOT)}: `{rel}` is not in the inventory ({len(names)} "
                f"test(s)). A new test binary must be recorded: run "
                f"`scripts/check-test-hygiene.py --write-inventory`."
            )
            continue
        # Only what this host actually compiled counts as "have": `take_inventory` also
        # reports the tests it knows exist elsewhere, and those are not evidence about here.
        have = {n for n, p, _ in names if p is None}
        want = {n for n, p, _ in recorded[rel][0] if p is None or host_cfg(p)}
        elsewhere = {n for n, p, _ in recorded[rel][0] if p is not None and not host_cfg(p)}
        # `#[ignore]` is the one edit that removes a test from every run while leaving it in
        # the binary, so the binary is asked and the answer compared (round 2, finding 4).
        skipped_now = {n for n, _, ign in names if ign}
        skipped_rec = {n for n, _, ign in recorded[rel][0] if ign}
        for n in sorted((skipped_now - skipped_rec) & have):
            errs.append(
                f"{rel}: test `{n}` is `#[ignore]`d, which the inventory does not record. One "
                f"attribute removes a test from every run while the binary still LISTS it, so "
                f"nothing else here can see the loss: regenerate with `--write-inventory` (the "
                f"diff is the review) and declare it in `IGNORE_ALLOWLIST` with the tier that "
                f"does run it."
            )
        for n in sorted((skipped_rec - skipped_now) & have):
            errs.append(
                f"{rel}: test `{n}` is recorded as `#[ignore]`d but the binary runs it — good "
                f"news that must still be regenerated (`--write-inventory`) and removed from "
                f"`IGNORE_ALLOWLIST`, so the allowlist never outlives its reason."
            )
        for gone in sorted(want - have):
            errs.append(
                f"{rel}: test `{gone}` is in the inventory but the binary does not contain it — "
                f"`cargo test` reports success over a suite that is missing this test. Either "
                f"it was deleted or renamed on purpose (regenerate with `--write-inventory`, "
                f"and the diff shows the loss), or a `#[cfg]` has quietly excluded it"
                + (f" along with the rest of the file (`cfg({filecfg})`)" if filecfg else "")
                + "."
            )
        for extra in sorted(have - want - elsewhere):
            errs.append(
                f"{rel}: test `{extra}` exists but is not in the inventory — run "
                f"`scripts/check-test-hygiene.py --write-inventory`."
            )
        for surprise in sorted(have & elsewhere):
            errs.append(
                f"{rel}: test `{surprise}` is recorded as platform-conditional but this host "
                f"compiled it. The recorded `cfg` predicate is wrong; regenerate."
            )
    for rel in sorted(set(recorded) - set(actual)):
        # `--only` builds one workspace, so the other's targets are legitimately absent. The
        # test is "does it belong to the workspace we did not build", not "is it under
        # crates/" — a future root-workspace member outside crates/ must not be excused
        # silently by a prefix that happened to fit.
        mqttui = rel.startswith("tools/mqttui/")
        if only == "root" and mqttui or only == "mqttui" and not mqttui:
            continue
        errs.append(
            f"{INVENTORY.relative_to(ROOT)}: `{rel}` records {len(recorded[rel][0])} test(s) but "
            f"no such test binary was built. A whole suite has left the build — deliberately "
            f"(regenerate) or because a manifest entry or file disappeared."
        )
    return errs


# --------------------------------------------------------------------------------------
# Check E — the RESULTS: what actually ran and passed
#
# The inventory answers "what does the binary CONTAIN". Two ways of losing coverage answer that
# question identically before and after, and both were proven against round 2:
#
#   * `#[ignore]` — the test is in the binary, `--list` still prints it, and it runs nowhere;
#   * `std::process::exit(0)` inside ONE test — the harness leaves mid-suite, so `running 6
#     tests` is followed by no per-test lines and no `test result:` summary at all, and
#     `cargo test` exits 0. Every result in that binary is discarded, silently.
#
# Neither is visible to any rule over source text (the second one is not even a distinctive
# token — `process::exit` is ordinary code), and both are trivially visible in the RUN's own
# output. So this check reads the results: for every binary the inventory says this host has, it
# requires a complete `test result:` line, `failed == 0`, `filtered out == 0`, a passed count
# equal to the inventory's expectation under this host's `cfg` evaluation, and an ignored set
# equal to the recorded one — every member of which must be declared in `IGNORE_ALLOWLIST`.
#
# It consumes the log CI already produces (`cargo test --all --no-fail-fast | tee
# test-output.txt`), so it costs the run nothing; with no log it runs the suite itself.
# --------------------------------------------------------------------------------------

# Every `#[ignore]`d test, why, and the tier that runs it — `None` meaning NO tier does.
#
# An ignored test is coverage the per-PR gate does not have, so it may not be a silent local
# decision: it is declared here, and the tier is VERIFIED against `.github/workflows/` rather
# than believed (`workflow_ignored_tiers()`). Adding `#[ignore]` to anything else fails the
# gate by name, in two places: `--check-inventory` (the binary says it is ignored and the
# checked-in inventory does not) and `--check-results` (it was ignored in the run).
#
# `None` is the honest category, not an escape hatch. The five `durable_bench.rs` benchmarks are
# run by NO tier — not per-PR, not nightly, not release — so they are coverage that exists only
# on paper. The gate prints that on every run, docs/TEST-PLAN.md names it, and it is drafted as
# a follow-up issue; what it is not is quietly allowlisted, which is how a manifest becomes a
# lie. `--write-inventory` cannot add entries here: growth is a hand-written, reviewed line.
IGNORE_ALLOWLIST: dict[str, tuple[str, str | None]] = {
    "crates/mqttd/tests/cluster_upgrade.rs::a_rolling_upgrade_and_rollback_lose_no_acked_fact": (
        "builds a second broker binary (minutes), which the per-PR profile cannot afford",
        "nightly",
    ),
    "crates/mqttd/tests/cluster_soak.rs::a_soak_under_sustained_load_shows_no_drift": (
        "an hour of sustained load by design (MQTTD_SOAK_SECS=3600)",
        "nightly",
    ),
    "crates/mqttd/tests/durable_bench.rs::durable_path_floor": (
        "macro-benchmark: minutes long, and only meaningful in --release",
        None,
    ),
    "crates/mqttd/tests/durable_bench.rs::degraded_group_does_not_delay_other_groups": (
        "macro-benchmark: minutes long, and only meaningful in --release",
        None,
    ),
    "crates/mqttd/tests/durable_bench.rs::multi_host_preflight": (
        "requires an operator-provisioned multi-host cluster, which no runner has",
        None,
    ),
    "crates/mqttd/tests/durable_bench.rs::store_append_floor": (
        "micro-benchmark for the durable store; only meaningful in --release",
        None,
    ),
    "crates/mqttd/tests/durable_bench.rs::device_barrier_floor": (
        "micro-benchmark for the host's durability barrier; --release only",
        None,
    ),
}


def workflow_ignored_tiers() -> dict[str, set[str]]:
    """{test-binary stem: workflows that run its `#[ignore]`d tests} — derived, not declared.

    A tier claim in `IGNORE_ALLOWLIST` is only worth having if it is checked: "the nightly tier
    runs it" is exactly the kind of sentence that stays in a file for a year after the step is
    deleted. So the claim is matched against a workflow command that really passes `--ignored`
    for that binary.
    """
    tiers: dict[str, set[str]] = {}
    wf_dir = ROOT / ".github" / "workflows"
    for wf in sorted(list(wf_dir.glob("*.yml")) + list(wf_dir.glob("*.yaml"))):
        for ln in sh_code(wf.read_text(encoding="utf-8")).splitlines():
            if "--ignored" not in ln or "cargo test" not in ln:
                continue
            for m in re.finditer(r"--test\s+([\w-]+)", ln):
                tiers.setdefault(m.group(1), set()).add(wf.stem)
    return tiers


def check_ignore_allowlist(
    recorded: dict[str, tuple[list[tuple[str, str | None, bool]], str | None]],
) -> list[str]:
    """Every ignored test is declared, and every declaration is still true."""
    errs: list[str] = []
    if not recorded:
        return [
            f"{INVENTORY.relative_to(ROOT)} is missing or unreadable, so the ignored-test "
            f"allowlist cannot be checked against anything; run "
            f"`scripts/check-test-hygiene.py --write-inventory`."
        ]
    tiers = workflow_ignored_tiers()
    ignored = {
        f"{rel}::{n}" for rel, (names, _) in recorded.items() for n, _, ign in names if ign
    }
    for key in sorted(ignored - set(IGNORE_ALLOWLIST)):
        errs.append(
            f"{key.replace('::', ': test `', 1)}` is `#[ignore]`d but not declared in "
            f"`IGNORE_ALLOWLIST` (scripts/check-test-hygiene.py). An ignored test runs in no "
            f"per-PR job while `cargo test` still says ok, so retiring one must be a reviewed "
            f"line: give it a reason and the tier that runs it — and if no tier does, say so "
            f"with `None`, which the gate then reports on every run as coverage on paper."
        )
    for key in sorted(set(IGNORE_ALLOWLIST) - ignored):
        errs.append(
            f"scripts/check-test-hygiene.py: `IGNORE_ALLOWLIST` declares `{key}` ignored, but "
            f"the inventory does not record it as `#[ignore]`d (it may have been renamed, "
            f"deleted or un-ignored). A stale allowlist entry is a licence nobody asked for; "
            f"remove it."
        )
    for key, (why, tier) in sorted(IGNORE_ALLOWLIST.items()):
        if tier is None or key not in ignored:
            continue
        stem = Path(key.split("::", 1)[0]).stem
        if tier not in tiers.get(stem, set()):
            errs.append(
                f"scripts/check-test-hygiene.py: `IGNORE_ALLOWLIST` says the `{tier}` tier runs "
                f"`{key}` ({why}), but no `.github/workflows/{tier}.yml` command runs "
                f"`--test {stem} -- --ignored`. Either the step was removed — in which case "
                f"this is coverage nobody runs and the entry must say `None` — or the tier is "
                f"misnamed."
            )
    return errs


def unrun_ignored(
    recorded: dict[str, tuple[list[tuple[str, str | None, bool]], str | None]],
    only: str | None = None,
) -> list[str]:
    """The ignored tests no tier runs. Printed on every successful run, deliberately."""
    ignored = {
        f"{rel}::{n}"
        for rel, (names, _) in recorded.items()
        for n, _, ign in names
        if ign and in_workspace(rel, only)
    }
    return sorted(k for k in ignored if IGNORE_ALLOWLIST.get(k, ("", "?"))[1] is None)


def in_workspace(rel: str, only: str | None) -> bool:
    """Does this target belong to the workspace `--only` selected?"""
    mqttui = rel.startswith("tools/mqttui/")
    return not (only == "root" and mqttui or only == "mqttui" and not mqttui)


@dataclass
class RunSection:
    """One test binary's block in a `cargo test` log."""

    label: str
    exe: str
    started: int | None = None
    outcome: str | None = None  # the `test result: <outcome>.` word; None: no summary at all
    passed: int = 0
    failed: int = 0
    ignored: int = 0
    filtered: int = 0
    per_test: dict[str, str] = field(default_factory=dict)


RUN_HEADER = re.compile(r"^\s+Running\s+(.*?)\s+\((\S+)\)\s*$")
OTHER_HEADER = re.compile(r"^\s+(Doc-tests|Compiling|Finished|Fresh)\b")
RUNNING_N = re.compile(r"^running (\d+) tests?$")
TEST_LINE = re.compile(r"^test (\S+) \.\.\. (ok|FAILED|ignored)\b")
RESULT_LINE = re.compile(
    r"^test result: (\w+)\. (\d+) passed; (\d+) failed; (\d+) ignored; \d+ measured; "
    r"(\d+) filtered out"
)


# cargo emits SGR colour whenever it thinks it is on a terminal — and CI forces it:
# `.github/workflows/ci.yml` sets `CARGO_TERM_COLOR: always` at file scope, so the tee'd
# `test-output.txt` carries `\x1b[1m\x1b[92m     Running\x1b[0m tests/foo.rs (…)`. Every
# anchored pattern below would then match nothing and the check would report a fully GREEN
# run as a failure — i.e. the mechanism could not pass in the one place it must run. Strip
# the escapes rather than loosen the anchors: an anchor that tolerates arbitrary junk before
# `Running` would also accept a line that merely mentions it.
ANSI_SGR = re.compile(r"\x1b\[[0-9;]*m")


def parse_run_log(text: str) -> list[RunSection]:
    """Every `Running <target> (<exe>)` block in a `cargo test` log, with its results."""
    out: list[RunSection] = []
    cur: RunSection | None = None
    for ln in text.splitlines():
        ln = ANSI_SGR.sub("", ln.rstrip("\r"))
        if m := RUN_HEADER.match(ln):
            cur = RunSection(label=m.group(1), exe=m.group(2))
            out.append(cur)
            continue
        if OTHER_HEADER.match(ln):
            cur = None
            continue
        if cur is None:
            continue
        if m := RUNNING_N.match(ln):
            cur.started = int(m.group(1))
        elif m := TEST_LINE.match(ln):
            cur.per_test[m.group(1)] = m.group(2)
        elif m := RESULT_LINE.match(ln):
            cur.outcome = m.group(1)
            cur.passed, cur.failed, cur.ignored, cur.filtered = (int(g) for g in m.groups()[1:])
    return out


def run_the_suite(only: str | None) -> str:
    """Run the tests and return the log, for when no log was handed in."""
    import subprocess

    log = []
    for which in WORKSPACES if only is None else [only]:
        # `2>&1`, not two captured pipes: cargo prints `Running <target> (<exe>)` on stderr and
        # the harness prints `running N tests` / `test result:` on stdout, so capturing them
        # separately puts every section header after every result — which reads exactly like
        # every binary having lost its summary. The interleaving IS the data.
        out = subprocess.run(
            ["cargo", "test", *WORKSPACES[which], "--no-fail-fast"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=False,
        )
        log.append(out.stdout)
    return "\n".join(log)


def check_results(only: str | None, log_path: str | None) -> list[str]:
    """The tests that RAN and PASSED are exactly the ones the inventory accounts for."""
    if not INVENTORY.is_file():
        return [
            f"{INVENTORY.relative_to(ROOT)} is missing; run "
            f"`scripts/check-test-hygiene.py --write-inventory`."
        ]
    recorded = parse_inventory(INVENTORY.read_text(encoding="utf-8"))
    errs: list[str] = check_ignore_allowlist(recorded)

    text = (
        Path(log_path).read_text(encoding="utf-8", errors="replace")
        if log_path
        else run_the_suite(only)
    )
    sections = parse_run_log(text)
    # The log names executables; the inventory is keyed by source path. cargo knows the mapping.
    # An exact basename match ties the log to THIS build; the stem fallback keeps the check
    # usable with a log from a build whose hashes have since changed, and says which it used.
    by_exe: dict[str, str] = {}
    by_stem: dict[str, list[str]] = {}
    for which in WORKSPACES if only is None else [only]:
        for rel, exe in cargo_test_binaries(which):
            by_exe[exe.name] = rel
            by_stem.setdefault(exe.name.rsplit("-", 1)[0], []).append(rel)
    found: dict[str, RunSection] = {}
    by_stem_only: list[str] = []
    for sec in sections:
        name = Path(sec.exe).name
        rel = by_exe.get(name)
        if rel is None:
            stem = name.rsplit("-", 1)[0]
            hits = by_stem.get(stem, [])
            rel = hits[0] if len(hits) == 1 else None
            if rel is not None:
                by_stem_only.append(name)
        if rel is not None:
            found[rel] = sec
    if by_stem_only:
        # Say it rather than quietly accept it: a log whose executable hashes no longer exist was
        # produced by a DIFFERENT build, and "these results are about this tree" is the one thing
        # this check assumes. In CI the same job builds and runs, so this never fires there.
        print(
            f"check-test-hygiene: NOTE — {len(by_stem_only)} binaries in this log no longer exist "
            f"under those exact names ({by_stem_only[:3]}…), so they were matched by target name. "
            f"The log is from an earlier build than the current one.",
            file=sys.stderr,
        )

    for rel, (names, filecfg) in sorted(recorded.items()):
        if not in_workspace(rel, only):
            continue
        if filecfg is not None and not host_cfg(filecfg):
            continue  # the whole suite is cfg'd out here: it was never built, so it never ran
        want_pass = {n for n, p, ign in names if (p is None or host_cfg(p)) and not ign}
        want_skip = {n for n, p, ign in names if (p is None or host_cfg(p)) and ign}
        sec = found.get(rel)
        if sec is None:
            errs.append(
                f"{rel}: no results at all in the run — {len(want_pass)} test(s) are expected to "
                f"run in this binary and it reported nothing. A suite that produced no result "
                f"line cannot have passed: it was filtered out of the invocation, or it never "
                f"started."
            )
            continue
        if sec.outcome is None:
            errs.append(
                f"{rel}: the binary started {sec.started if sec.started is not None else '?'} "
                f"test(s) and never printed a `test result:` summary — every result in it is "
                f"discarded and `cargo test` still exits 0. That is what `std::process::exit` "
                f"(or `abort`, or a harness-killing signal) inside ONE test does to the whole "
                f"binary: {len(sec.per_test)} test(s) reported individually, "
                f"{len(want_pass)} expected."
            )
            continue
        if sec.failed:
            errs.append(f"{rel}: {sec.failed} test(s) FAILED in the run this check was given.")
        if sec.filtered:
            errs.append(
                f"{rel}: {sec.filtered} test(s) were filtered out of this run, so it cannot "
                f"certify the suite. `--check-results` must be given a full, unfiltered run "
                f"(CI's `cargo test --all --no-fail-fast` log)."
            )
        if sec.passed != len(want_pass):
            missing = sorted(n for n in want_pass if sec.per_test.get(n) != "ok")
            errs.append(
                f"{rel}: {sec.passed} test(s) passed but the inventory accounts for "
                f"{len(want_pass)} on this host"
                + (f" — no `ok` line for {missing}" if missing else "")
                + ". A green run over a smaller suite is the defect this exists to catch."
            )
        ran_skipped = {n for n, v in sec.per_test.items() if v == "ignored"}
        for n in sorted(ran_skipped - want_skip):
            errs.append(
                f"{rel}: test `{n}` was IGNORED in the run and the inventory does not record it "
                f"as `#[ignore]`d. One attribute takes a test out of every run while the binary "
                f"still contains and lists it — regenerate the inventory and declare it in "
                f"`IGNORE_ALLOWLIST` with the tier that runs it."
            )
        for n in sorted(want_skip - ran_skipped):
            if sec.per_test:
                errs.append(
                    f"{rel}: test `{n}` is recorded as `#[ignore]`d but the run does not report "
                    f"it as ignored. Regenerate the inventory (`--write-inventory`)."
                )
        if not sec.per_test and sec.ignored != len(want_skip):
            errs.append(
                f"{rel}: the run reports {sec.ignored} ignored test(s), the inventory records "
                f"{len(want_skip)}."
            )
    return errs


# --------------------------------------------------------------------------------------


# --------------------------------------------------------------------------------------------
# The gate's own tests.
#
# This gate shipped with three defects of its own (issue #260), and every one was a BOUNDARY:
# a rule that fired on prose it should have ignored, a check whose evidence was its own source
# line, a wiring assertion that `cargo fmt` silently broke. None of them were visible from the
# rule's code — they were visible only from an input the rule got wrong. So each rule here is
# pinned by a pair: an input it MUST flag, and the nearest input it must NOT. The pair is the
# test. A rule with only positive fixtures passes by firing on everything.
FIXTURES: list[tuple[str, str, str, bool, str]] = [
    # (rule, name, source, should_flag, why this input is the boundary)
    (
        "sh-announce",
        "an announced skip that stops",
        'if ! command -v cosign >/dev/null; then\n  echo "skipping: no cosign"\n  exit 0\nfi\n',
        True,
        "announces and then stops: the coverage is gone and the script reports success",
    ),
    (
        "sh-announce",
        "prose about a build option",
        'echo "building mqttd (set MQTTD_BIN to skip)…"\ncargo build --quiet -p mqttd\n'
        'MQTTD_BIN="target/debug/mqttd"\n',
        False,
        "the word `skip` names an env var, and the next line does the work it describes — "
        "flagging this teaches authors to reword honest messages, which is how a gate stops "
        "being read",
    ),
    (
        "sh-announce",
        "a skip mentioned in a comment about the rule itself",
        '# this lane cannot be skipped: it gates merges\nrun_the_lane\n',
        False,
        "prose that mentions skipping in order to deny it",
    ),
]

RUST_FIXTURES: list[tuple[str, str, bool, str]] = [
    (
        "a bare early return with no loop",
        "#[test]\nfn t() {\n    if !have_it() { return; }\n    assert!(check());\n}\n",
        True,
        "returns success having asserted nothing — the exact silent-coverage-loss shape",
    ),
    (
        "a poll whose exhaustion panics",
        "#[test]\nfn t() {\n    for _ in 0..50 {\n        if ready() { return; }\n    }\n"
        '    panic!("never became ready");\n}\n',
        False,
        "the `return` is a success exit only because falling out of the loop diverges "
        "unconditionally; this is the sanctioned poll",
    ),
    (
        "a poll whose exhaustion merely asserts a truth",
        "#[test]\nfn t() {\n    for _ in 0..50 {\n        if ready() { return; }\n    }\n"
        "    assert!(1 == 1);\n}\n",
        True,
        "adjacency is not loudness: a trailing assertion that cannot fail lets the loop finish "
        "and the test pass having done nothing — the fig leaf B1 was tightened to reject",
    ),
]


def self_test() -> list[str]:
    """Run every fixture. Returns the failures, each naming the boundary it lost."""
    import tempfile

    bad: list[str] = []
    for rule, name, src, should, why in FIXTURES:
        got = check_sh_script(Path(f"fixture/{rule}.sh"), "#!/usr/bin/env bash\n" + src)
        flagged = bool(got)
        if flagged != should:
            bad.append(
                f"{rule}: {name!r} was {'not flagged' if should else 'flagged'} but must be "
                f"{'flagged' if should else 'left alone'} — {why}"
                + (f"\n      the gate said: {got[0]}" if got else "")
            )

    # Under ROOT, because `parse` derives a file's repo-relative name and a fixture outside the
    # tree has none. `target/` is not scanned, so a fixture cannot be mistaken for real source.
    scratch = ROOT / "target"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=scratch, prefix="hygiene-fixtures-") as td:
        for name, src, should, why in RUST_FIXTURES:
            f = Path(td) / "fixture.rs"
            f.write_text(src, encoding="utf-8")
            rf = parse(f)
            # `/tests/` in the path is what makes B1 look at all: `is_test_code` scopes the
            # rule to test code so production timers stay out of it. A fixture that forgets this
            # is silently never examined, and every negative fixture would "pass".
            rf.rel = "crates/fixture/tests/fixture.rs"
            got = check_b([rf])
            flagged = bool(got)
            if flagged != should:
                bad.append(
                    f"B1: {name!r} was {'not flagged' if should else 'flagged'} but must be "
                    f"{'flagged' if should else 'left alone'} — {why}"
                    + (f"\n      the gate said: {got[0]}" if got else "")
                )
    return bad


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--audit", action="store_true", help="print every wait site and class")
    ap.add_argument(
        "--self-test", action="store_true", help="check this gate's own rules against fixtures"
    )
    ap.add_argument("--write", action="store_true", help="regenerate the (b) census")
    ap.add_argument(
        "--write-inventory", action="store_true", help="regenerate docs/test-inventory.md"
    )
    ap.add_argument(
        "--check-inventory",
        action="store_true",
        help="compare the built test binaries against docs/test-inventory.md (needs cargo)",
    )
    ap.add_argument(
        "--check-results",
        nargs="?",
        const="",
        metavar="LOG",
        help="compare what actually RAN and PASSED against docs/test-inventory.md. LOG is a "
        "`cargo test` log (CI already tees one); with no LOG the suite is run here.",
    )
    ap.add_argument(
        "--only", choices=sorted(WORKSPACES), help="restrict --*-inventory to one workspace"
    )
    args = ap.parse_args()

    if args.self_test:
        bad = self_test()
        for b in bad:
            print(f"  FAIL {b}", file=sys.stderr)
        n = len(FIXTURES) + len(RUST_FIXTURES)
        if bad:
            print(
                f"\ncheck-test-hygiene --self-test: {len(bad)} of {n} fixture(s) failed — a rule "
                f"of this gate no longer draws the line where it is documented to.",
                file=sys.stderr,
            )
            return 1
        print(f"check-test-hygiene --self-test: OK — {n} fixtures, each a should-flag/must-not pair.")
        return 0

    if args.check_results is not None:
        recorded = (
            parse_inventory(INVENTORY.read_text(encoding="utf-8")) if INVENTORY.is_file() else {}
        )
        errs = check_results(args.only, args.check_results or None)
        if errs:
            print(
                f"\ncheck-test-hygiene: {len(errs)} result problem(s) — the tests that actually "
                f"RAN AND PASSED are not the ones {INVENTORY.relative_to(ROOT)} accounts for\n",
                file=sys.stderr,
            )
            for e in errs:
                print(f"  FAIL {e}", file=sys.stderr)
            return 1
        paper = unrun_ignored(recorded, args.only)
        print(
            f"check-test-hygiene: results OK — every "
            f"{'' if args.only is None else args.only + ' '}test binary the inventory accounts "
            f"for reported a complete summary, with no failures, nothing filtered out, and its "
            f"recorded passed and ignored counts."
        )
        if paper:
            print(
                f"  NOTE: {len(paper)} `#[ignore]`d test(s) are run by NO tier — coverage that "
                f"exists only on paper (docs/TEST-PLAN.md § What this gate detects, and what it cannot):"
            )
            for k in paper:
                print(f"    {k} — {IGNORE_ALLOWLIST[k][0]}")
        return 0

    if args.write_inventory:
        inv = take_inventory(None)
        INVENTORY.write_text(render_inventory(inv), encoding="utf-8")
        print(
            f"wrote {INVENTORY.relative_to(ROOT)} — {len(inv)} test binaries, "
            f"{sum(len(v[0]) for v in inv.values())} tests"
        )
        return 0

    if args.check_inventory:
        errs = check_inventory(args.only)
        if errs:
            print(
                f"\ncheck-test-hygiene: {len(errs)} inventory problem(s) — the tests a binary "
                f"CONTAINS have changed without the checked-in inventory changing\n",
                file=sys.stderr,
            )
            for e in errs:
                print(f"  FAIL {e}", file=sys.stderr)
            return 1
        # Report what was actually compared, not what the file contains: with `--only`, one
        # workspace's binaries were never built, and saying "every test binary" would be the
        # same kind of overclaim this script exists to catch.
        checked = take_inventory(args.only)
        print(
            f"check-test-hygiene: inventory OK — the "
            f"{sum(len(v[0]) for v in checked.values())} tests in {len(checked)} "
            f"{'' if args.only is None else args.only + ' '}test binaries are exactly the ones "
            f"recorded in {INVENTORY.relative_to(ROOT)}."
        )
        return 0

    files = rust_files()
    sites = scan_waits(files)

    if args.write:
        CENSUS.write_text(render_census([s for s in sites if s.cls == "b"]), encoding="utf-8")
        print(f"wrote {CENSUS.relative_to(ROOT)}")
        return 0

    if args.audit:
        counts = {c: sum(1 for s in sites if s.cls == c) for c in "abcd"}
        print(
            f"test-code wait sites: {len(sites)}  "
            f"(a) bounded poll {counts['a']}  (b) documented settle {counts['b']}  "
            f"(c) NAKED {counts['c']}  (d) virtual clock {counts['d']}"
        )
        for s in sorted(sites, key=lambda s: (s.cls, s.rel, s.line)):
            extra = f"  [{s.slug}]" if s.slug else (f"  ({s.note})" if s.cls == "c" else "")
            print(f"  ({s.cls}) {s.rel}:{s.line} {s.duration:>7}  {s.fn}{extra}")

    # The ignored-test allowlist is enforced here too, from the checked-in inventory, so the
    # cargo-less docs job also fails when a declaration goes stale or a tier stops existing.
    recorded = parse_inventory(INVENTORY.read_text(encoding="utf-8")) if INVENTORY.is_file() else {}
    errs = check_a(files, sites) + check_b(files) + check_shell() + check_ignore_allowlist(recorded)
    if errs:
        print(
            f"\ncheck-test-hygiene: {len(errs)} problem(s) "
            f"(see docs/TEST-PLAN.md § Conventions for the taxonomy)\n",
            file=sys.stderr,
        )
        for e in errs:
            print(f"  FAIL {e}", file=sys.stderr)
        return 1
    # What was verified, not what is hoped. The previous line said "no self-skip can pass
    # silently under CI" while seven shapes did (issue #260 round 2, finding 8) — and the next
    # reviewer reads the claim instead of re-testing, which makes an overclaim worse than the gap
    # it hides. Everything this line does not say is in docs/TEST-PLAN.md § "What this gate does
    # not detect".
    counts = {c: sum(1 for s in sites if s.cls == c) for c in "abd"}
    print(
        f"check-test-hygiene: OK — {len(sites)} wall-clock wait sites classified "
        f"({counts['a']} bounded polls, {counts['b']} documented settling delays, "
        f"{counts['d']} virtual-clock advances, 0 naked); no bare early `return` outside a "
        f"loop whose exhaustion fails, no test whose every action is optional, no announced "
        f"skip, no `process::exit`; the CI-fatal macro's guard is exactly the `CI` check and "
        f"every copy is byte-identical; {len(ci_run_scripts())} CI-run scripts have no "
        f"unsanctioned success exit. Limits: docs/TEST-PLAN.md § What this gate detects, and what it cannot."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
