# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: its context, the
choice, and the trade-offs accepted. Numbered sequentially; superseded ADRs are
kept (marked `Superseded by NNNN`) rather than deleted.

An ADR records **the decision only**, and its `Status` is just the lifecycle
(`Proposed | Accepted | Superseded | Deprecated`). **How** a decision is being built
and **how far along** it is live in its delivery doc under
[`docs/delivery/`](../delivery/) — start with the
[**delivery dashboard**](../delivery/STATUS.md) for the at-a-glance, whole-project
overview, and see [`docs/delivery/README.md`](../delivery/README.md) for the model and
conventions.

## The ADR catalogue

The list of every ADR — its title, lifecycle status, and a link to both the decision
record and its delivery progress — is the [**delivery dashboard**](../delivery/STATUS.md).
That table is **generated** from the ADR files and delivery-doc frontmatter by
`scripts/gen-status.py` and **CI-checked** (`gen-status.py --check`), so it cannot drift.

There is deliberately **no second, hand-maintained list here**: a manual table duplicated the
dashboard and silently fell out of date (it drifted three ADRs behind before anyone noticed).
One generated, gated catalogue is the single source of truth. To read a record directly,
browse the `NNNN-*.md` files in this directory.
