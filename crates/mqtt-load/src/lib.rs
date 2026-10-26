//! `mqtt-load` — the benchmark rig's load generator
//! ([ADR 0090](../../../docs/adr/0090-benchmark-rig-redesign.md) D2).
//!
//! Paced, open-loop MQTT 5 publishers with several `QoS` 1 messages in flight, and the
//! consumers they publish to, in one process on one monotonic clock — so ack latency
//! (PUBLISH → PUBACK) and end-to-end latency (PUBLISH → delivery) are both exact. Every
//! message carries an identity; the ledger reconciles sent, acked and received at the
//! end. The generator reports its own health, so a run it could not drive is VOID
//! rather than counted against the broker.
//!
//! Layers:
//! - [`schedule`] — the open-loop pacing math (pure).
//! - [`payload`] — the identity header and the allocation-free PUBLISH encoder.
//! - [`ledger`] — the identity ledger.
//! - [`hist`], [`stats`] — HDR latency histograms and the sharded counters.
//! - [`client`], [`publisher`], [`subscriber`] — the connections, on `mqtt-codec`.
//! - [`health`] — the generator's own CPU.
//! - [`metrics`], [`summary`] — Prometheus during the run; the JSON summary after it.
//! - [`run`] — one run end to end.
//!
//! The JSON schema and the CLI are documented in `crates/mqtt-load/README.md`.

pub mod client;
pub mod clock;
pub mod config;
pub mod ctx;
pub mod health;
pub mod hist;
pub mod ledger;
pub mod metrics;
pub mod payload;
pub mod publisher;
pub mod run;
pub mod schedule;
pub mod stats;
pub mod subscriber;
pub mod summary;

pub use config::{Config, Sessions, Shape, Thresholds};
pub use run::{run, RunError};
pub use schedule::Phase;
pub use summary::{Outcome, Summary};
