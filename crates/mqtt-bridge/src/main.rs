//! `mqtt-bridge` binary entry point ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md)).
//!
//! Usage: `mqtt-bridge <config.toml>`. The config declares the local-cluster connection and
//! the external upstreams with their per-rule topic mappings; the engine connects to every
//! side and forwards according to the rules. Wired up incrementally across 0025-T2…T11.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let path = std::env::args()
        .nth(1)
        .ok_or("usage: mqtt-bridge <config.toml>")?;
    eprintln!("mqtt-bridge: config {path} (engine wiring lands in 0025-T2+)");
    Ok(())
}
