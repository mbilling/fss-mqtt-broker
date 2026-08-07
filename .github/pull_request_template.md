<!--
Describe why, not just what — the diff already shows the what. If this closes an
issue, say "Closes #N".
-->

## What this changes, and why

## How it was verified

<!--
The strongest form is a test. If you fixed a bug, a test that FAILS against the
old behaviour is worth more than one that merely passes against the new — say so
if you checked, and say what it printed.
-->

## Checklist

- [ ] `cargo fmt --all` and `cargo clippy --all-targets -- -D warnings` are clean
      (CI builds with `-D warnings`, so a warning fails the build)
- [ ] Tests pass, and any new behaviour has one
- [ ] If a delivery task changed: frontmatter updated and
      `python3 scripts/gen-status.py` re-run
- [ ] **No claim here is broader than what was built.** If scope shrank, the
      title/doc was narrowed and the remainder has a task — not deleted quietly
- [ ] If a known problem was found and not fixed, an issue exists for it
