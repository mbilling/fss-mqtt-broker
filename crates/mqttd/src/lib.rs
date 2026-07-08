//! Broker server library: the routing hub and per-connection handling.
//!
//! Exposed as a library so the connection logic can be driven by integration
//! tests over real TCP sockets (see `tests/`), with the `mqttd` binary as a thin
//! wrapper that wires in listeners and configuration.

pub mod admission;
pub mod aliases;
pub mod clock;
pub mod cluster;
pub mod config_watch;
pub mod conn;
pub mod health;
pub mod hub;
pub mod peer;
pub mod reload;

pub use hub::{Hub, HubCommand};
