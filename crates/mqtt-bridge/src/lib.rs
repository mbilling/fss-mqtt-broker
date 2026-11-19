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
//! - [`config`] — the configuration model and its validation.
//! - [`forward`] — the pure forwarding policy: direction, remap, loop bounding.
//! - [`engine`] — builds one link per upstream from the configuration; each link owns its
//!   local session and its upstream connection and moves messages both ways
//!   ([ADR 0060](../../../docs/adr/0060-bridge-durability-and-ack-contract.md) Amendment 1).
//! - [`spool`] — the bounded store-and-forward spool for traffic coming in from an upstream.
//! - [`metrics`] — Prometheus text and the audit log.

pub mod client;
pub mod config;
pub mod engine;
pub mod forward;
mod link;
pub mod metrics;
pub mod spool;
