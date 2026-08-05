//! The operator binary: leader gate, then the controller (ADR 0055 T1).

use kube::Client;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    let client = Client::try_default().await?;
    // Pod identity + namespace from the Downward-API conventions; sane
    // fallbacks for `kubectl`-context development runs.
    let identity = std::env::var("POD_NAME")
        .unwrap_or_else(|_| format!("mqttd-operator-{}", std::process::id()));
    let ns = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".into());
    info!(%identity, %ns, "mqttd-operator starting (ADR 0055 T1 scaffold)");
    mqttd_operator::leader::acquire_and_hold(client.clone(), &ns, identity).await;
    mqttd_operator::controller::run(client).await;
    Ok(())
}
