# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: its context, the
choice, and the trade-offs accepted. Numbered sequentially; superseded ADRs are
kept (marked `Superseded by NNNN`) rather than deleted.

| # | Title | Status |
|---|-------|--------|
| [0001](0001-session-durability.md) | Session durability in a horizontally-scalable cluster | Accepted (design) |
| [0002](0002-transport-security.md) | Transport security: TLS 1.3 everywhere, mTLS on the cluster bus | Accepted |
| [0003](0003-gossip-authentication.md) | Gossip-plane authentication: keyed MAC on SWIM datagrams | Accepted |
| [0004](0004-identity-and-authentication.md) | Identity model: mTLS Common Name first, deny by default | Accepted |
| [0005](0005-session-affinity.md) | Session affinity: relocate persistent sessions to their owner | Accepted |
