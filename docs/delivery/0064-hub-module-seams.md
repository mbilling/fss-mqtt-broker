---
adr: "0064"
title: "The hub's module seams"
adr_status: Accepted
tasks:
  - id: 0064-T1
    title: "Issue #258: five move-only slices extract the correctness-critical seams from hub/mod.rs, each landing green with zero logic edits"
    status: done
    date: 2026-08-18
    evidence: "Slice 0 reshaped hub.rs into hub/mod.rs (zero movement). Slices 1-5 moved, verbatim: retained authority/convergence (34 items, ~1,400 lines), brownout/refusal policy (8 items — with quota_full caught mid-slice belonging to impl Inflight and returned), the ADR 0061 append lanes (25 items, ~900 lines), the delivery plane (20 methods, ~1,100 lines), and the pending-publish/forwarding ledger (17 items, ~660 lines). Every slice: pub(super) visibility and path requalification ONLY, full lib suite 330/330 between slices, test edits limited to two qualified paths (super::retained_digest -> super::retained::retained_digest and chunk_retained likewise). hub/mod.rs 19,893 -> 15,784 lines; production portion ~9,700 -> ~5,600 (42% cut); each module's doc comment states the single invariant it owns and ADR 0064's table is the routing rule for future work."
    notes: "Field ownership is documented, not compiler-enforced (the Hub struct is one item and stays in mod.rs). The test module deliberately stays in mod.rs — its unchanged location is part of the no-behaviour-change evidence. Remaining bulk in mod.rs is the actor loop, session lifecycle, sweep, and honesty gates, which belong there per the table."
---

# Delivery — ADR 0064: The hub's module seams

Decision: [docs/adr/0064-hub-module-seams.md](../adr/0064-hub-module-seams.md).

<!-- status-table:0064 -->
| Task | Status | When | Evidence / notes |
|------|--------|------|------------------|
| 0064-T1 | ✅ done | 2026-08-18 | "Slice 0 reshaped hub.rs into hub/mod.rs (zero movement). Slices 1-5 moved, verbatim: retained authority/convergence (34 items, ~1,400 lines), brownout/refusal policy (8 items — with quota_full caught mid-slice belonging to impl Inflight and returned), the ADR 0061 append lanes (25 items, ~900 lines), the delivery plane (20 methods, ~1,100 lines), and the pending-publish/forwarding ledger (17 items, ~660 lines). Every slice: pub(super) visibility and path requalification ONLY, full lib suite 330/330 between slices, test edits limited to two qualified paths (super::retained_digest -> super::retained::retained_digest and chunk_retained likewise). hub/mod.rs 19,893 -> 15,784 lines; production portion ~9,700 -> ~5,600 (42% cut); each module's doc comment states the single invariant it owns and ADR 0064's table is the routing rule for future work." |
<!-- /status-table:0064 -->
