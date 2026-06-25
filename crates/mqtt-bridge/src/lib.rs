//! `mqtt-bridge` — a standalone boundary MQTT bridge
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md)).
//!
//! The bridge forwards a *configured* set of topics between this broker (the local
//! cluster) and one or more brokers in **other security zones**, with per-rule direction
//! and **enforced** unidirectional flow as the headline security control. It is an MQTT
//! *client* to both sides — not an in-process broker plugin — so the boundary crossing is a
//! small, isolated, auditable unit with its own identity, credentials, and failure domain.
//!
//! Layers:
//! - [`client`] — a minimal async MQTT client (TCP/TLS) over `mqtt-codec`/`mqtt-net`.
//! - the config model, forwarding engine, spool, and observability land in their own
//!   modules as the delivery tasks (0025-T2…T9) complete.

pub mod client;
pub mod config;
pub mod engine;
pub mod forward;
pub mod metrics;
