//! Broker server library: the routing hub and per-connection handling.
//!
//! Exposed as a library so the connection logic can be driven by integration
//! tests over real TCP sockets (see `tests/`), with the `mqttd` binary as a thin
//! wrapper that wires in listeners and configuration.

pub mod cluster;
pub mod conn;
pub mod health;
pub mod hub;
pub mod peer;

pub use hub::{Hub, HubCommand};
