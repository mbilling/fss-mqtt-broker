//! The MQTT broker server binary.
//!
//! Milestone: a clustered, QoS-0 MQTT 3.1.1 broker with transport security
//! (ADR 0002). Clients connect over TLS 1.3; peer links run mutual TLS against
//! a dedicated cluster CA; peers are discovered dynamically via SWIM gossip
//! (preferred) or configured statically. Auth/authz arrive in later milestones.
//!
//! Secure-by-default: no listener runs unless explicitly enabled, and every
//! plaintext option is loudly logged as insecure.
//!
//! Dev environment shims (until config-file loading lands):
//! - `MQTTD_NODE_ID`        — this node's id (default `node-local`)
//! - `MQTTD_TLS_BIND`       — TLS client listener bind, e.g. `0.0.0.0:8883`
//!   (requires `MQTTD_TLS_CERT` + `MQTTD_TLS_KEY`, PEM paths)
//! - `MQTTD_TLS_CLIENT_CA`  — PEM CA bundle; when set, clients must present a
//!   certificate it issued (mTLS)
//! - `MQTTD_PLAINTEXT_BIND` — insecure client listener bind, e.g. `127.0.0.1:1883`
//! - `MQTTD_PEER_BIND`      — inter-node listener bind, e.g. `127.0.0.1:7001`
//! - `MQTTD_PEER_TLS_CA` / `MQTTD_PEER_TLS_CERT` / `MQTTD_PEER_TLS_KEY` —
//!   cluster-bus mTLS material (set all three); without them peer links are
//!   plaintext and loudly logged
//! - `MQTTD_PEERS`          — comma-separated peer addresses to dial (static mesh)
//! - `MQTTD_SWIM_BIND`      — SWIM gossip UDP bind, e.g. `127.0.0.1:7946`
//!   (requires `MQTTD_PEER_BIND`; peer links are then established from
//!   membership, no `MQTTD_PEERS` needed)
//! - `MQTTD_SWIM_SEEDS`     — comma-separated SWIM addresses of existing members

use mqtt_cluster::swim::Swim;
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_config::Config;
use mqtt_net::tls;
use mqtt_storage::MemorySessionStore;
use mqttd::{cluster, conn, hub, peer};
use std::path::Path;
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

/// SWIM driver tick; must stay below the ack timeout (250ms default config).
const SWIM_TICK: Duration = Duration::from_millis(100);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let node_id = NodeId(std::env::var("MQTTD_NODE_ID").unwrap_or_else(|_| "node-local".into()));
    info!(version = env!("CARGO_PKG_VERSION"), node = %node_id.0, "starting mqttd");

    let config = Config::default();
    config.validate()?;
    info!(
        require_client_cert = config.security.require_client_cert,
        allow_anonymous = config.security.allow_anonymous,
        "configuration validated (secure defaults)"
    );

    // Start the routing hub.
    let (hub, hub_tx) = hub::Hub::with_config(node_id.clone(), Box::new(MemorySessionStore::new()));
    tokio::spawn(hub.run());

    // Cluster-bus mTLS context (ADR 0002): one CA + node cert pair secures both
    // the accepting and dialing side of every peer link.
    let peer_tls = peer_tls_from_env()?;

    // Cluster peer mesh (opt-in).
    let peer_bind = non_empty_env("MQTTD_PEER_BIND");
    if let Some(bind) = &peer_bind {
        if peer_tls.is_none() {
            warn!(%bind, "INSECURE: starting PLAINTEXT peer listener (no mTLS) — testing use only");
        }
        let listener = TcpListener::bind(bind).await?;
        info!(%bind, mtls = peer_tls.is_some(), "accepting cluster peer links");
        tokio::spawn(peer::serve_listener(
            listener,
            node_id.clone(),
            hub_tx.clone(),
            peer_tls.clone(),
        ));
    }
    if let Some(peers) = non_empty_env("MQTTD_PEERS") {
        for addr in peers.split(',').map(str::trim).filter(|a| !a.is_empty()) {
            info!(%addr, "dialing cluster peer (static)");
            tokio::spawn(peer::dial_forever(
                addr.to_string(),
                node_id.clone(),
                hub_tx.clone(),
                peer_tls.clone(),
            ));
        }
    }

    // SWIM gossip membership (opt-in): discovers peers and drives the peer mesh,
    // replacing the need for a static MQTTD_PEERS list.
    start_swim_from_env(&node_id, peer_bind, &hub_tx, peer_tls.as_ref()).await?;

    // Client listeners. TLS is the intended path; plaintext is a loudly-logged
    // local-testing escape hatch.
    let mut listeners = Vec::new();
    if let Some(bind) = non_empty_env("MQTTD_TLS_BIND") {
        let (Some(cert), Some(key)) = (
            non_empty_env("MQTTD_TLS_CERT"),
            non_empty_env("MQTTD_TLS_KEY"),
        ) else {
            return Err("MQTTD_TLS_BIND requires MQTTD_TLS_CERT and MQTTD_TLS_KEY".into());
        };
        let client_ca = non_empty_env("MQTTD_TLS_CLIENT_CA");
        let acceptor = tls::server_acceptor(
            Path::new(&cert),
            Path::new(&key),
            client_ca.as_deref().map(Path::new),
        )?;
        let listener = TcpListener::bind(&bind).await?;
        info!(%bind, mtls = client_ca.is_some(), "accepting MQTT 3.1.1 clients over TLS 1.3");
        listeners.push(tokio::spawn(serve_tls_clients(
            listener,
            acceptor,
            hub_tx.clone(),
        )));
    }
    if let Some(addr) = non_empty_env("MQTTD_PLAINTEXT_BIND") {
        warn!(%addr, "INSECURE: starting PLAINTEXT MQTT listener (no TLS) — testing use only");
        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "accepting MQTT 3.1.1 clients (QoS 0 delivery)");
        listeners.push(tokio::spawn(serve_plaintext_clients(listener, hub_tx)));
    }
    if listeners.is_empty() {
        warn!(
            "No client listener active. Set MQTTD_TLS_BIND (with MQTTD_TLS_CERT \
             and MQTTD_TLS_KEY) for the TLS listener, or MQTTD_PLAINTEXT_BIND for \
             insecure local testing."
        );
    }
    for l in listeners {
        let _ = l.await;
    }
    Ok(())
}

/// Build the cluster-bus mTLS context from `MQTTD_PEER_TLS_{CA,CERT,KEY}`.
/// All three must be set together; none means a (loudly logged) plaintext mesh.
fn peer_tls_from_env() -> Result<Option<peer::PeerTls>, Box<dyn std::error::Error>> {
    match (
        non_empty_env("MQTTD_PEER_TLS_CA"),
        non_empty_env("MQTTD_PEER_TLS_CERT"),
        non_empty_env("MQTTD_PEER_TLS_KEY"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let (ca, cert, key) = (Path::new(&ca), Path::new(&cert), Path::new(&key));
            Ok(Some(peer::PeerTls {
                acceptor: tls::server_acceptor(cert, key, Some(ca))?,
                connector: tls::client_connector(ca, cert, key)?,
            }))
        }
        (None, None, None) => Ok(None),
        _ => Err(
            "MQTTD_PEER_TLS_CA, MQTTD_PEER_TLS_CERT and MQTTD_PEER_TLS_KEY \
             must be set together"
                .into(),
        ),
    }
}

/// Start SWIM membership from `MQTTD_SWIM_{BIND,SEEDS}` (no-op when unset) and
/// hand its events to the peer-link manager.
async fn start_swim_from_env(
    node_id: &NodeId,
    peer_bind: Option<String>,
    hub_tx: &mpsc::UnboundedSender<hub::HubCommand>,
    peer_tls: Option<&peer::PeerTls>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(bind) = non_empty_env("MQTTD_SWIM_BIND") else {
        return Ok(());
    };
    let Some(peer_addr) = peer_bind else {
        return Err("MQTTD_SWIM_BIND requires MQTTD_PEER_BIND: membership \
                    gossips the peer-link address so other nodes can dial us"
            .into());
    };
    let socket = UdpSocket::bind(&bind).await?;
    let seeds: Vec<String> = non_empty_env("MQTTD_SWIM_SEEDS")
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|a| !a.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    info!(%bind, seeds = seeds.len(), "starting SWIM gossip membership");
    let swim = Swim::new(
        node_id.clone(),
        bind,
        peer_addr,
        mqtt_cluster::swim::Config::default(),
        seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    tokio::spawn(swim_driver::run(socket, swim, SWIM_TICK, event_tx));
    tokio::spawn(cluster::maintain_peer_links(
        event_rx,
        node_id.clone(),
        hub_tx.clone(),
        peer_tls.cloned(),
    ));
    Ok(())
}

/// Accept TLS clients forever: per-connection handshake (off the accept loop so
/// a slow handshake cannot stall other clients), then normal MQTT handling.
async fn serve_tls_clients(
    listener: TcpListener,
    acceptor: TlsAcceptor,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!(error = %e, "TLS listener accept failed");
                return;
            }
        };
        debug!(%peer, "accepted TLS connection");
        let acceptor = acceptor.clone();
        let hub = hub_tx.clone();
        tokio::spawn(async move {
            let _ = stream.set_nodelay(true);
            match acceptor.accept(stream).await {
                Ok(tls_stream) => conn::handle_stream(tls_stream, Some(peer), hub).await,
                Err(e) => debug!(%peer, error = %e, "TLS handshake failed"),
            }
        });
    }
}

/// Accept plaintext clients forever (insecure; explicitly opted into).
async fn serve_plaintext_clients(
    listener: TcpListener,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
) {
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(accepted) => accepted,
            Err(e) => {
                warn!(error = %e, "plaintext listener accept failed");
                return;
            }
        };
        debug!(%peer, "accepted connection");
        let _ = stream.set_nodelay(true);
        tokio::spawn(conn::handle(stream, hub_tx.clone()));
    }
}

/// Read an environment variable, treating unset or empty as absent.
fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
