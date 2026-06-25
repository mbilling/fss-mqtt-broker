//! `mqtt-bridge` binary entry point ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md)).
//!
//! Usage: `mqtt-bridge <config.toml>`. The config declares the local-cluster connection and
//! the external upstreams with their per-rule topic mappings; the engine connects to every
//! side and forwards according to the rules. Runs until SIGINT/SIGTERM, then shuts down.

use std::sync::Arc;

use mqtt_bridge::config::BridgeConfig;
use mqtt_bridge::engine::Bridge;
use mqtt_bridge::metrics::BridgeMetrics;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

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
    let toml = std::fs::read_to_string(&path).map_err(|e| format!("read {path}: {e}"))?;
    let cfg = BridgeConfig::parse_toml(&toml)?;
    info!(
        upstreams = cfg.upstreams.len(),
        hop_count_limit = cfg.hop_count_limit,
        "starting mqtt-bridge"
    );

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    rt.block_on(async move {
        let metrics_bind = cfg.metrics_bind.clone();
        let bridge = Bridge::start(cfg);
        if let Some(bind) = metrics_bind {
            serve_metrics(bind, bridge.metrics());
        }
        // Run until a shutdown signal, then stop every connection cleanly.
        tokio::signal::ctrl_c().await.ok();
        info!("shutting down mqtt-bridge");
        bridge.shutdown();
    });
    Ok(())
}

/// Serve the bridge's Prometheus metrics text at `GET /metrics` on `bind` (a tiny raw
/// HTTP responder — the bridge has no need for a full HTTP stack).
fn serve_metrics(bind: String, metrics: Arc<BridgeMetrics>) {
    tokio::spawn(async move {
        let listener = match TcpListener::bind(&bind).await {
            Ok(l) => {
                info!(%bind, "serving bridge metrics");
                l
            }
            Err(e) => {
                warn!(%bind, error = %e, "could not bind metrics endpoint");
                return;
            }
        };
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                continue;
            };
            let metrics = metrics.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await; // drain the request line; we only GET
                let body = metrics.render();
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });
}
