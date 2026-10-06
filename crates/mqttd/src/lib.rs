//! Broker server library: the routing hub and per-connection handling.
//!
//! Exposed as a library so the connection logic can be driven by integration
//! tests over real TCP sockets (see `tests/`), with the `mqttd` binary as a thin
//! wrapper that wires in listeners and configuration.

pub mod accept;
pub mod admin;
pub mod admission;
pub mod aliases;
pub mod backpressure;
pub mod backup;
pub mod clock;
pub mod cluster;
pub mod config_view;
pub mod config_watch;
pub mod conn;
pub mod health;
pub mod http_auth;
pub mod hub;
pub mod ingress;
pub mod log_filter;
pub mod memory_watch;
pub mod oidc;
pub mod peer;
pub mod reload;
pub mod runtime_probe;
pub mod store_probe;
pub mod store_watch;
pub mod tls_check;

pub use hub::{Hub, HubCommand};
