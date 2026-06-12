//! The MQTT broker server binary.
//!
//! Milestone: a clustered MQTT 3.1.1 broker — `QoS` 0/1/2 delivery, retained
//! messages, wills, keepalive — with transport security
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
//! - `MQTTD_ACL_FILE`       — TOML topic-ACL policy (deny by default); without
//!   it authorization is not enforced and loudly logged
//! - `MQTTD_PLAINTEXT_BIND` — insecure client listener bind, e.g. `127.0.0.1:1883`
//! - `MQTTD_ALLOW_ANONYMOUS` — any non-empty value permits clients that present
//!   no credentials at all; default-off and loudly logged as insecure
//! - `MQTTD_PASSWORD_FILE`  — Argon2id `username:phc-hash` file (ADR 0004 step 6)
//! - `MQTTD_JWT_HS256_SECRET` / `MQTTD_JWT_RS256_PEM` — JWT verification key;
//!   optional `MQTTD_JWT_ISSUER` / `MQTTD_JWT_AUDIENCE` constraints
//! - `MQTTD_PEER_BIND`      — inter-node listener bind, e.g. `127.0.0.1:7001`
//! - `MQTTD_PEER_TLS_CA` / `MQTTD_PEER_TLS_CERT` / `MQTTD_PEER_TLS_KEY` —
//!   cluster-bus mTLS material (set all three); without them peer links are
//!   plaintext and loudly logged
//! - `MQTTD_PEERS`          — comma-separated peer addresses to dial (static mesh)
//! - `MQTTD_SWIM_BIND`      — SWIM gossip UDP bind, e.g. `127.0.0.1:7946`
//!   (requires `MQTTD_PEER_BIND`; peer links are then established from
//!   membership, no `MQTTD_PEERS` needed)
//! - `MQTTD_SWIM_SEEDS`     — comma-separated SWIM addresses of existing members
//! - `MQTTD_SWIM_KEY`       — 64-hex-char cluster gossip key (ADR 0003), e.g.
//!   from `openssl rand -hex 32`; without it gossip is unauthenticated and
//!   loudly logged

use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{Authenticator, Authorizer};
use mqtt_cluster::swim::Swim;
use mqtt_cluster::swim_auth::SwimAuth;
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_config::Config;
use mqtt_net::tls;
use mqtt_observability::AuditLog;
use mqtt_storage::MemorySessionStore;
use mqttd::{cluster, conn, hub, peer};
use std::path::Path;
use std::sync::Arc;
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

    // Client authentication + topic authorization + audit policy (ADR 0004),
    // shared by both client listeners.
    let policy = client_policy_from_env()?;

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
            policy.clone(),
        )));
    }
    if let Some(addr) = non_empty_env("MQTTD_PLAINTEXT_BIND") {
        warn!(%addr, "INSECURE: starting PLAINTEXT MQTT listener (no TLS) — testing use only");
        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "accepting MQTT 3.1.1 clients");
        listeners.push(tokio::spawn(serve_plaintext_clients(
            listener, hub_tx, policy,
        )));
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

/// Build the connection policy — authentication, topic authorization, and
/// auditing — from the `MQTTD_*` shims (ADR 0004). Everything is deny-by-default;
/// the insecure fallbacks are explicit and loudly logged.
fn client_policy_from_env() -> Result<Arc<conn::ConnPolicy>, Box<dyn std::error::Error>> {
    let auth = authenticator_from_env()?;

    // A TOML ACL file gives deny-by-default per-identity topic policy; without
    // one, authorization is not enforced — loudly.
    let authz: Arc<dyn Authorizer> = if let Some(path) = non_empty_env("MQTTD_ACL_FILE") {
        let text = std::fs::read_to_string(&path)?;
        let policy = mqtt_auth::acl::AclPolicy::from_toml_str(&text)?;
        info!(%path, "topic ACL policy loaded (deny by default)");
        Arc::new(policy)
    } else {
        warn!(
            "INSECURE: no MQTTD_ACL_FILE configured — topic authorization is \
             NOT enforced (every authenticated client may publish/subscribe \
             anywhere)"
        );
        Arc::new(mqtt_auth::AllowAll)
    };

    Ok(Arc::new(conn::ConnPolicy {
        auth,
        authz,
        audit: Arc::new(AuditLog::new()),
    }))
}

/// Build the CONNECT authenticator (ADR 0004 steps 2 + 6): a certificate /
/// anonymous baseline, then — when configured — an Argon2id password file
/// (`MQTTD_PASSWORD_FILE`) and a JWT verifier (`MQTTD_JWT_HS256_SECRET` or
/// `MQTTD_JWT_RS256_PEM`, with optional `MQTTD_JWT_ISSUER`/`MQTTD_JWT_AUDIENCE`).
/// Credentials are tried cert → password → token via a chain.
fn authenticator_from_env() -> Result<Arc<dyn Authenticator>, Box<dyn std::error::Error>> {
    let allow_anonymous = non_empty_env("MQTTD_ALLOW_ANONYMOUS").is_some();
    if allow_anonymous {
        warn!(
            "INSECURE: anonymous MQTT clients are PERMITTED (MQTTD_ALLOW_ANONYMOUS) — \
             testing use only"
        );
    }
    let mut members: Vec<Arc<dyn Authenticator>> =
        vec![Arc::new(BasicAuthenticator { allow_anonymous })];

    if let Some(path) = non_empty_env("MQTTD_PASSWORD_FILE") {
        let text = std::fs::read_to_string(&path)?;
        let pw = mqtt_auth::password::PasswordAuthenticator::from_file_contents(&text)?;
        info!(%path, "Argon2id password file loaded");
        members.push(Arc::new(pw));
    }

    if let Some(secret) = non_empty_env("MQTTD_JWT_HS256_SECRET") {
        info!("JWT HS256 verification enabled");
        members.push(Arc::new(mqtt_auth::token::TokenAuthenticator::hs256(
            secret.as_bytes(),
            jwt_config_from_env(),
        )));
    } else if let Some(pem_path) = non_empty_env("MQTTD_JWT_RS256_PEM") {
        let pem = std::fs::read(&pem_path)?;
        let tok = mqtt_auth::token::TokenAuthenticator::rs256_pem(&pem, jwt_config_from_env())?;
        info!(%pem_path, "JWT RS256 verification enabled");
        members.push(Arc::new(tok));
    }

    Ok(Arc::new(mqtt_auth::chain::ChainAuthenticator::new(members)))
}

/// Assemble JWT validation options from the optional issuer/audience shims.
fn jwt_config_from_env() -> mqtt_auth::token::TokenConfig {
    mqtt_auth::token::TokenConfig {
        issuer: non_empty_env("MQTTD_JWT_ISSUER"),
        audience: non_empty_env("MQTTD_JWT_AUDIENCE"),
        ..Default::default()
    }
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
    // Gossip authentication (ADR 0003): keyed = membership claims require the
    // cluster key; unkeyed is possible but loudly insecure.
    let auth = if let Some(hex) = non_empty_env("MQTTD_SWIM_KEY") {
        Some(SwimAuth::from_hex_key(&hex)?)
    } else {
        warn!(
            "INSECURE: SWIM gossip is UNAUTHENTICATED (no MQTTD_SWIM_KEY) — \
             anyone reaching the gossip port can inject membership claims, \
             including Dead claims that tear down routing"
        );
        None
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
    info!(%bind, seeds = seeds.len(), authenticated = auth.is_some(), "starting SWIM gossip membership");
    let swim = Swim::new(
        node_id.clone(),
        bind,
        peer_addr,
        mqtt_cluster::swim::Config::default(),
        seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    tokio::spawn(swim_driver::run(socket, swim, SWIM_TICK, event_tx, auth));
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
    policy: Arc<conn::ConnPolicy>,
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
        let policy = policy.clone();
        tokio::spawn(async move {
            let _ = stream.set_nodelay(true);
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    // mTLS identity (ADR 0004): the verified leaf cert's CN.
                    let identity = conn::tls_identity(&tls_stream);
                    conn::handle_stream(tls_stream, Some(peer), identity, policy, hub).await;
                }
                Err(e) => debug!(%peer, error = %e, "TLS handshake failed"),
            }
        });
    }
}

/// Accept plaintext clients forever (insecure; explicitly opted into).
async fn serve_plaintext_clients(
    listener: TcpListener,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
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
        tokio::spawn(conn::handle_stream(
            stream,
            Some(peer),
            None,
            policy.clone(),
            hub_tx.clone(),
        ));
    }
}

/// Read an environment variable, treating unset or empty as absent.
fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}
