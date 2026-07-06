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
//! - `MQTTD_MAX_QUEUED_MESSAGES` — per-session offline-queue cap (default 100000)
//! - `MQTTD_QUEUE_OVERFLOW` — `drop-oldest` (default) or `reject-newest`
//! - `MQTTD_TOPIC_ALIAS_MAX` — Topic Alias Maximum advertised to v5 clients (ADR 0011;
//!   default 16, `0` disables inbound topic aliases)
//! - `MQTTD_RECEIVE_MAXIMUM` — Receive Maximum advertised to v5 clients (ADR 0012;
//!   default 256, floored at 1). A client exceeding it is sent DISCONNECT `0x93`.
//! - `MQTTD_AUTH_TIMEOUT` — per-round enhanced-auth reply timeout in seconds (ADR 0013;
//!   default 10, floored at 1)
//! - `MQTTD_DURABLE_SESSIONS` — the durable, consensus-backed session store
//!   (ADR 0006/0007), replicating persistent sessions across the peer mesh, is the
//!   **default** (ADR 0029). Opt out with `0`/`false`/`off`/`no` for the lightweight
//!   in-memory store. A node with no `MQTTD_SWIM_SEEDS` is the cluster founder that
//!   bootstraps the lease group (exactly one per cluster).
//! - `MQTTD_DATA_DIR`        — directory for on-disk session persistence (ADR 0018),
//!   orthogonal to durability. With durable on (the default) it makes the lease group
//!   and replicated log on-disk, so sessions survive a full-cluster restart (the
//!   recommended production setup). With durable opted out, it stores single-node
//!   sessions in `<dir>/sessions.redb` (restart-safe, not replicated). Unset → in-memory.
//! - `MQTTD_FAILURE_DOMAIN`  — this node's own failure-domain label (ADR 0016 T5), e.g.
//!   `rack-a`. Advertised over the authenticated SWIM gossip payload so the cluster's
//!   failure-domain topology **self-assembles** (the bounded lease-voter set spreads across
//!   racks/zones without a static map). The preferred mechanism — each node sets only its own
//!   label. Unset → this node is unlabelled (its own singleton domain) unless a peer or the
//!   static map below supplies one. When the cluster-bus certificate **attests** a label
//!   (ADR 0016 T6, see `MQTTD_PEER_TLS_*`), the certificate is authoritative: this value
//!   must match it or peers reject this node's gossip, and it may be omitted entirely
//!   (the cert alone labels the node).
//! - `MQTTD_FAILURE_DOMAINS` — static failure-domain topology (ADR 0016 T4): `node-id=domain`
//!   pairs (e.g. `n1=rack-a,n2=rack-a,n3=rack-b`) so the bounded lease-voter set is spread
//!   across racks/zones and one domain's loss cannot take quorum. A cluster-uniform seed/
//!   fallback; gossip-advertised labels (`MQTTD_FAILURE_DOMAIN`) override it per node.
//!   Unset → no static spread (id-ordered voter selection unless labels are gossiped).
//! - `MQTTD_TLS_BIND`       — TLS client listener bind, e.g. `0.0.0.0:8883`
//!   (requires `MQTTD_TLS_CERT` + `MQTTD_TLS_KEY`, PEM paths)
//! - `MQTTD_TLS_CLIENT_CA`  — PEM CA bundle; when set, clients must present a
//!   certificate it issued (mTLS)
//! - `MQTTD_TLS_CRL`        — PEM certificate revocation list (requires
//!   `MQTTD_TLS_CLIENT_CA`); a client whose cert is listed is refused at the TLS
//!   handshake. Re-read on `SIGHUP`, so a published CRL applies without a restart
//! - `MQTTD_ACL_FILE`       — TOML topic-ACL policy (deny by default); without
//!   it authorization is not enforced and loudly logged
//! - `MQTTD_PLAINTEXT_BIND` — insecure client listener bind, e.g. `127.0.0.1:1883`
//! - `MQTTD_ALLOW_ANONYMOUS` — any non-empty value permits clients that present
//!   no credentials at all; default-off and loudly logged as insecure
//! - `MQTTD_PASSWORD_FILE`  — Argon2id `username:phc-hash` file (ADR 0004 step 6)
//! - `MQTTD_JWT_HS256_SECRET` / `MQTTD_JWT_RS256_PEM` — JWT verification key;
//!   optional `MQTTD_JWT_ISSUER` / `MQTTD_JWT_AUDIENCE` constraints
//! - `MQTTD_CONFIG_WATCH`   — opt-in filesystem auto-reload (ADR 0033): poll interval in
//!   seconds; when a configured policy file (ACL, password, JWT PEM, TLS cert/key/CA/CRL)
//!   changes on disk, reload through the same fail-safe routine as `SIGHUP` (no restart).
//!   Unset/`0` = disabled (signal-only, the default). For declarative/Kubernetes-ConfigMap use
//! - `MQTTD_PEER_BIND`      — inter-node listener bind, e.g. `127.0.0.1:7001`
//! - `MQTTD_PEER_TLS_CA` / `MQTTD_PEER_TLS_CERT` / `MQTTD_PEER_TLS_KEY` —
//!   cluster-bus mTLS material (set all three); without them peer links are
//!   plaintext and loudly logged. A leaf whose SANs carry
//!   `URI:urn:fss:failure-domain:<label>` has its failure domain **CA-attested**
//!   (ADR 0016 T6): the label is authoritative on the gossip plane and a
//!   disagreeing self-claim is rejected.
//! - `MQTTD_PEER_TLS_CRL`   — PEM CRL for the **cluster bus** (ADR 0022 T7; requires the
//!   three above): signed gossip from a revoked certificate is dropped. The CRL must be
//!   signed by the cluster CA; it hot-reloads via SIGHUP / `MQTTD_CONFIG_WATCH` (ADR
//!   0032/0033), so publishing a new CRL evicts a compromised node without a restart.
//!   Expired/not-yet-valid certificates are rejected on the gossip plane regardless.
//! - `MQTTD_PEERS`          — comma-separated peer addresses to dial (static mesh)
//! - `MQTTD_SWIM_BIND`      — SWIM gossip UDP bind, e.g. `127.0.0.1:7946`
//!   (requires `MQTTD_PEER_BIND`; peer links are then established from
//!   membership, no `MQTTD_PEERS` needed)
//! - `MQTTD_SWIM_SEEDS`     — comma-separated SWIM addresses of existing members
//! - `MQTTD_SWIM_KEY`       — 64-hex-char cluster gossip key (ADR 0003), e.g.
//!   from `openssl rand -hex 32`; without it gossip is unauthenticated and
//!   loudly logged
//! - `MQTTD_SWIM_KEY_ACCEPT` — comma-separated extra 64-hex keys that incoming
//!   gossip may also be sealed with (ADR 0003 zero-downtime rotation): datagrams
//!   are sealed with `MQTTD_SWIM_KEY` but opened with it *or* any of these. Rotate
//!   by staging the new key here cluster-wide, promoting it to `MQTTD_SWIM_KEY`,
//!   then dropping the old one. Requires `MQTTD_SWIM_KEY`.
//! - `MQTTD_SWIM_SIGNED`    — per-node gossip signatures (ADR 0022): `require`
//!   (sign + reject unsigned) or `off`. Defaults to `require` when both
//!   `MQTTD_SWIM_KEY` and the peer-TLS material are present, else `off`. `require`
//!   needs both; otherwise a startup error. Signs with the cluster-bus leaf key,
//!   verified against the CA. A signed node accepts only signed gossip — each
//!   posture is strict (no mixed-version coexistence).
//! - `MQTTD_SWIM_REPLAY`    — gossip anti-replay (ADR 0023): `require` (sequence +
//!   reject un-sequenced) or `off` (default). Needs `MQTTD_SWIM_SIGNED=require`
//!   (the sequence binds to the per-node signature) and `MQTTD_DATA_DIR` (a
//!   restart-safe, clock-free sequence counter persists in `<dir>/gossip-seq`).
//!   Otherwise a startup error. A sequenced node accepts only sequenced gossip.
//! - `MQTTD_HEALTH_BIND`    — HTTP health-probe bind for orchestrators, e.g.
//!   `0.0.0.0:8080`; serves `GET /livez` (hub responsive), `GET /readyz`
//!   (mesh + durable-store ready), and `GET /metrics` (Prometheus, ADR 0020).
//!   Unset = no health server.
//! - `MQTTD_METRICS_BIND`   — optional separate bind for `GET /metrics` (ADR 0020),
//!   to isolate the metrics scrape from the health probes. Plaintext, internal/ops
//!   network only — do not expose publicly.
//! - `MQTTD_OTLP_ENDPOINT`  — OTLP/HTTP base URL of an OpenTelemetry Collector (e.g.
//!   `http://collector:4318`); when set, the same metrics are pushed via OTLP in
//!   addition to the Prometheus endpoint (ADR 0020 T9). `/v1/metrics` is appended.
//! - `MQTTD_OTLP_INTERVAL`  — OTLP push interval in seconds (default 10).
//! - `MQTTD_READY_MIN_MEMBERS` — smallest mesh size `/readyz` accepts (default 1;
//!   raise it to hold a node out of rotation until it has joined its peers)
//! - `MQTTD_SHUTDOWN_GRACE` — seconds to drain live client connections after a
//!   `SIGTERM`/`SIGINT` before forcing shutdown (ADR 0019; default 30). `/readyz`
//!   flips to draining immediately so orchestrators stop routing new connections.

use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{Authenticator, Authorizer};
use mqtt_cluster::placement::{self, Placement};
use mqtt_cluster::swim::Swim;
use mqtt_cluster::swim_auth::SwimAuth;
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_config::Config;
use mqtt_net::tls;
use mqtt_observability::{AuditLog, AuditSink};
use mqtt_storage::logged::ReplicatedSessionStore;
use mqtt_storage::persistent_log::PersistentLog;
use mqtt_storage::persistent_retained::PersistentRetainedStore;
use mqtt_storage::{MemorySessionStore, OverflowPolicy, QueueLimits, RetainedStore, SessionStore};
use mqttd::{cluster, config_watch, conn, hub, peer, reload};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

/// SWIM driver tick; must stay below the ack timeout (250ms default config).
const SWIM_TICK: Duration = Duration::from_millis(100);

// Startup is a linear wiring sequence; splitting it would only scatter the order it
// documents.
#[allow(clippy::too_many_lines)]
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

    // Server-wide MQTT 5 wire limits (ADR 0011/0012/0013), configurable via env, set once
    // before any connection is served.
    conn::set_wire_limits(wire_limits_from_env()?);

    // Session-placement ring (ADR 0005), kept in step with SWIM membership and
    // read by the hub to identify each persistent session's owner node.
    let placement = Arc::new(RwLock::new(
        Placement::new(node_id.clone(), placement::DEFAULT_REPLICAS)
            // This node's own failure-domain label (ADR 0016 T5), so placement reports it
            // in the topology map without waiting for gossip to round-trip.
            .with_local_domain(this_node_failure_domain()),
    ));

    // Graceful-shutdown plumbing (ADR 0019): a cancellation token that stops the accept
    // loops and drains live connections, and a tracker that lets us wait for them.
    let shutdown = tokio_util::sync::CancellationToken::new();
    let connections = tokio_util::task::TaskTracker::new();

    // Metrics (ADR 0020), built once and shared (Arc) into the hub (publish/deliver
    // counts), the connections, the listeners, the gossip driver, and the health server's
    // /metrics endpoint. With MQTTD_OTLP_ENDPOINT set, the same measurements are also
    // pushed via OTLP/HTTP (ADR 0020 T9); otherwise it is the Prometheus endpoint only.
    let version = env!("CARGO_PKG_VERSION");
    let metrics = Arc::new(
        if let Some(endpoint) = non_empty_env("MQTTD_OTLP_ENDPOINT") {
            let interval = non_empty_env("MQTTD_OTLP_INTERVAL")
                .and_then(|v| v.parse().ok())
                .map_or(Duration::from_secs(10), Duration::from_secs);
            // node_id becomes service.instance.id so each cluster node's OTLP series are
            // distinct at the backend (otherwise all nodes collide into one series).
            let m = mqtt_observability::metrics::Metrics::with_otlp(
                version, &endpoint, interval, &node_id.0,
            )?;
            info!(%endpoint, interval_s = interval.as_secs(), "OTLP/HTTP metric export enabled");
            m
        } else {
            mqtt_observability::metrics::Metrics::new(version)
        },
    );

    // Build and spawn the routing hub with its session store (durable opt-in, or
    // the bounded in-memory default). The store is shared with connections for the
    // QoS-2 dedup window (ADR 0007 §5).
    let (hub_tx, store, durable_plane, lease_driver) =
        start_hub(&node_id, &placement, &metrics).await?;

    // Health endpoints for orchestrators (opt-in via MQTTD_HEALTH_BIND), serving
    // /livez (hub responsive) and /readyz (mesh + durable-store ready). Keep a plane
    // handle to stop openraft cleanly on shutdown.
    let plane_for_shutdown = durable_plane.clone();
    let draining =
        start_health_from_env(&hub_tx, &placement, durable_plane, metrics.clone()).await?;

    // Cluster-bus mTLS context (ADR 0002): one CA + node cert pair secures both
    // the accepting and dialing side of every peer link.
    let peer_tls = peer_tls_from_env()?;

    // Client policy (ADR 0004 auth/authz/audit + ADR 0005 session relocation),
    // built before the peer listener so the latter can serve sessions relocated
    // here by other nodes. The same policy serves the client listeners below.
    let proxy = conn::ProxyContext {
        node: node_id.clone(),
        placement: placement.clone(),
        connector: peer_tls.as_ref().map(|t| t.connector.clone()),
    };
    let (policy, mut reloader) =
        client_policy_from_env(Some(proxy), store, shutdown.clone(), metrics.clone())?;

    // Fold the cluster-bus gossip CRL (ADR 0022 T7) into the same validate-before-swap
    // reload as the client policy: a republished CRL revokes a node's gossip on the next
    // datagram after SIGHUP (or the ADR 0033 watcher), with no restart.
    if let Some(tls) = &peer_tls {
        if let Some(path) = tls.crl_path.clone() {
            let ca_der = tls.ca_der.clone();
            reloader.attach_swim_crl(tls.gossip_crl.clone(), move || {
                load_gossip_crl(&path, &ca_der)
            });
        }
    }

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
            Some(policy.clone()),
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
    start_swim_from_env(
        &node_id,
        peer_bind,
        &hub_tx,
        peer_tls.as_ref(),
        placement,
        &shutdown,
        metrics.clone(),
    )
    .await?;

    // Client listeners. TLS is the intended path; plaintext is a loudly-logged
    // local-testing escape hatch. The serve loops stop themselves on `shutdown`. The TLS
    // branch registers its acceptor with the reloader so SIGHUP also rotates cert/key/CA.
    start_client_listeners(hub_tx, policy, &mut reloader, &shutdown, &connections).await?;

    // Share the (now fully-configured) reloader between the SIGHUP handler and the optional
    // filesystem watcher; both drive the same validate-before-swap routine.
    let reloader = std::sync::Arc::new(reloader);

    // SIGHUP reloads the security policy (ACL + authenticator + TLS material) in place
    // (ADR 0032) — no restart, no dropped connections; a bad file keeps the running policy.
    spawn_reload_handler(reloader.clone());

    // Optional filesystem watcher (ADR 0033): MQTTD_CONFIG_WATCH=<seconds> auto-reloads when a
    // configured policy file changes on disk (the Kubernetes ConfigMap case), through the same
    // fail-safe reload. Off by default — signal-driven reload stays the default.
    spawn_config_watcher(reloader, &shutdown)?;

    // Run until a shutdown signal, then drain gracefully (ADR 0019).
    graceful_shutdown(
        &shutdown,
        &connections,
        &draining,
        plane_for_shutdown,
        lease_driver,
    )
    .await;
    // Push a final OTLP batch so the last counters are not lost on exit (no-op without
    // OTLP; the provider also flushes when the last Arc<Metrics> drops).
    metrics.flush();
    Ok(())
}

/// Bind and spawn the MQTT client listeners (TLS, WSS, QUIC, plaintext, WS) selected by the
/// `MQTTD_*_BIND` shims. Each accept loop owns its `shutdown` clone and stops itself when the
/// token fires, so the join handles are intentionally dropped.
// A flat sequence of per-listener setup blocks: long by count (one per transport), not by
// branching complexity — like `Metrics::build`'s registration list.
#[allow(clippy::too_many_lines)]
async fn start_client_listeners(
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    reloader: &mut reload::Reloader,
    shutdown: &tokio_util::sync::CancellationToken,
    connections: &tokio_util::task::TaskTracker,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut any = false;
    let tls_bind = non_empty_env("MQTTD_TLS_BIND");
    let wss_bind = non_empty_env("MQTTD_WSS_BIND");

    // A single reloadable client-TLS acceptor, shared by the TLS and WSS listeners (ADR 0035
    // WSS reuses the ADR 0002 TLS stack + the ADR 0032 reloadable acceptor — one TLS path).
    let acceptor_rx = if tls_bind.is_some() || wss_bind.is_some() {
        let (Some(cert), Some(key)) = (
            non_empty_env("MQTTD_TLS_CERT"),
            non_empty_env("MQTTD_TLS_KEY"),
        ) else {
            return Err(
                "MQTTD_TLS_BIND / MQTTD_WSS_BIND require MQTTD_TLS_CERT and MQTTD_TLS_KEY".into(),
            );
        };
        let client_ca = non_empty_env("MQTTD_TLS_CLIENT_CA");
        // Optional certificate revocation list (ADR 0002 T8): a client whose cert is listed is
        // rejected at the TLS handshake. Reloadable on SIGHUP via the same closure below, so a
        // freshly-published CRL takes effect on the next handshake with no restart (ADR 0032 §5).
        let crl = non_empty_env("MQTTD_TLS_CRL");
        let acceptor = tls::server_acceptor_with_crl(
            Path::new(&cert),
            Path::new(&key),
            client_ca.as_deref().map(Path::new),
            crl.as_deref().map(Path::new),
        )?;
        // Register the acceptor for SIGHUP reload (ADR 0032 T6): the closure re-reads the
        // same paths so a renewed cert/key/client-CA — and an updated CRL — is served on the
        // next handshake.
        Some(reloader.attach_tls(acceptor, move || {
            tls::server_acceptor_with_crl(
                Path::new(&cert),
                Path::new(&key),
                client_ca.as_deref().map(Path::new),
                crl.as_deref().map(Path::new),
            )
            .map_err(|e| e.to_string())
        }))
    } else {
        None
    };

    if let Some(bind) = tls_bind {
        let listener = TcpListener::bind(&bind).await?;
        info!(%bind, "accepting MQTT 3.1.1 clients over TLS 1.3");
        tokio::spawn(serve_tls_clients(
            listener,
            acceptor_rx
                .clone()
                .expect("acceptor built when tls_bind set"),
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if let Some(bind) = wss_bind {
        let listener = TcpListener::bind(&bind).await?;
        info!(%bind, "accepting MQTT clients over WebSocket + TLS 1.3 (wss, ADR 0035)");
        tokio::spawn(serve_wss_clients(
            listener,
            acceptor_rx
                .clone()
                .expect("acceptor built when wss_bind set"),
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if let Some(addr) = non_empty_env("MQTTD_PLAINTEXT_BIND") {
        warn!(%addr, "INSECURE: starting PLAINTEXT MQTT listener (no TLS) — testing use only");
        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "accepting MQTT 3.1.1 clients");
        tokio::spawn(serve_plaintext_clients(
            listener,
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if let Some(addr) = non_empty_env("MQTTD_WS_BIND") {
        warn!(%addr, "INSECURE: starting PLAINTEXT WebSocket listener (no TLS) — testing use only");
        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "accepting MQTT clients over WebSocket (ws, ADR 0035)");
        tokio::spawn(serve_ws_clients(
            listener,
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if let Some(bind) = non_empty_env("MQTTD_QUIC_BIND") {
        // QUIC mandates TLS 1.3 (no plaintext mode); it reuses the same cert material as the
        // TLS listener. The endpoint is built once (cert hot-reload is a follow-on, ADR 0036).
        let (Some(cert), Some(key)) = (
            non_empty_env("MQTTD_TLS_CERT"),
            non_empty_env("MQTTD_TLS_KEY"),
        ) else {
            return Err("MQTTD_QUIC_BIND requires MQTTD_TLS_CERT and MQTTD_TLS_KEY".into());
        };
        let client_ca = non_empty_env("MQTTD_TLS_CLIENT_CA");
        let udp: std::net::SocketAddr = bind
            .parse()
            .map_err(|e| format!("MQTTD_QUIC_BIND is not a UDP socket address ({bind}): {e}"))?;
        let endpoint = mqtt_net::quic::server_endpoint(
            udp,
            Path::new(&cert),
            Path::new(&key),
            client_ca.as_deref().map(Path::new),
        )?;
        info!(%bind, "accepting MQTT clients over QUIC + TLS 1.3 (ADR 0036)");
        tokio::spawn(serve_quic_clients(
            endpoint,
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if !any {
        warn!(
            "No client listener active. Set MQTTD_TLS_BIND, MQTTD_WSS_BIND, or \
             MQTTD_QUIC_BIND (with MQTTD_TLS_CERT and MQTTD_TLS_KEY) for the secure \
             TLS / WebSocket-TLS / QUIC listeners, or MQTTD_PLAINTEXT_BIND / MQTTD_WS_BIND \
             for insecure local testing."
        );
    }
    Ok(())
}

/// Build the connection policy — authentication, topic authorization, and
/// auditing — from the `MQTTD_*` shims (ADR 0004). Everything is deny-by-default;
/// the insecure fallbacks are explicit and loudly logged.
fn client_policy_from_env(
    proxy: Option<conn::ProxyContext>,
    store: Arc<dyn SessionStore>,
    shutdown: tokio_util::sync::CancellationToken,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
) -> Result<(Arc<conn::ConnPolicy>, reload::Reloader), Box<dyn std::error::Error>> {
    let audit: Arc<dyn AuditSink> = Arc::new(AuditLog::new());
    // Build the initial policy, and a closure that re-reads the configured files on reload
    // (ADR 0032). The closure returns the freshly-built (authorizer, authenticator) or an
    // error string that aborts the swap — validate-before-swap lives in `reload::Reloader`.
    let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) =
        (authorizer_from_env()?, authenticator_from_env()?);
    let build = || -> reload::BuildResult {
        let authz = authorizer_from_env().map_err(|e| e.to_string())?;
        let auth = authenticator_from_env().map_err(|e| e.to_string())?;
        Ok((authz, auth))
    };
    let (reloader, handles) =
        reload::Reloader::with_metrics(initial, audit.clone(), Some(metrics.clone()), build);

    let policy = Arc::new(conn::ConnPolicy {
        auth: handles.auth,
        authz: handles.authz,
        audit,
        proxy,
        store: Some(store),
        connect_timeout: conn::DEFAULT_CONNECT_TIMEOUT,
        enhanced: None,
        shutdown: Some(shutdown),
        metrics: Some(metrics),
    });
    Ok((policy, reloader))
}

/// Build the topic authorizer (ADR 0004 step 3): a TOML ACL file gives deny-by-default
/// per-identity topic policy; without one, authorization is not enforced — loudly. Reads
/// the file fresh each call, so it is reusable at startup *and* on a SIGHUP reload (ADR 0032).
fn authorizer_from_env() -> Result<Arc<dyn Authorizer>, Box<dyn std::error::Error>> {
    if let Some(path) = non_empty_env("MQTTD_ACL_FILE") {
        let text = std::fs::read_to_string(&path)?;
        let policy = mqtt_auth::acl::AclPolicy::from_toml_str(&text)?;
        info!(%path, "topic ACL policy loaded (deny by default)");
        Ok(Arc::new(policy))
    } else {
        warn!(
            "INSECURE: no MQTTD_ACL_FILE configured — topic authorization is \
             NOT enforced (every authenticated client may publish/subscribe \
             anywhere)"
        );
        Ok(Arc::new(mqtt_auth::AllowAll))
    }
}

/// Install the SIGHUP handler that drives [`reload::Reloader::reload`] for the process
/// lifetime (ADR 0032). Non-Unix has no SIGHUP, so reload is unavailable there (logged).
#[cfg(unix)]
fn spawn_reload_handler(reloader: std::sync::Arc<reload::Reloader>) {
    use tokio::signal::unix::{signal, SignalKind};
    tokio::spawn(async move {
        let mut hup = match signal(SignalKind::hangup()) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "cannot install SIGHUP handler; security reload disabled");
                return;
            }
        };
        while hup.recv().await.is_some() {
            info!("SIGHUP received — reloading security policy");
            reloader.reload("signal");
        }
    });
}

#[cfg(not(unix))]
fn spawn_reload_handler(_reloader: std::sync::Arc<reload::Reloader>) {
    warn!("security policy reload (SIGHUP) is unavailable on this platform");
}

/// The configured policy file paths the reload closures read — the set the filesystem watcher
/// stats (ADR 0033 T1). Only file-backed material: the JWT HS256 secret is an inline env value,
/// not a file, so it is not watchable. A path is included only when its env var is set.
fn watched_policy_paths() -> Vec<std::path::PathBuf> {
    [
        "MQTTD_ACL_FILE",
        "MQTTD_PASSWORD_FILE",
        "MQTTD_JWT_RS256_PEM",
        "MQTTD_TLS_CERT",
        "MQTTD_TLS_KEY",
        "MQTTD_TLS_CLIENT_CA",
        "MQTTD_TLS_CRL",
        "MQTTD_PEER_TLS_CRL",
    ]
    .iter()
    .filter_map(|var| non_empty_env(var))
    .map(std::path::PathBuf::from)
    .collect()
}

/// Spawn the opt-in filesystem watcher (ADR 0033) when `MQTTD_CONFIG_WATCH=<seconds>` is set
/// (unset / `0` = disabled, the signal-only default). Polls the configured policy files and
/// auto-reloads through the same fail-safe routine as `SIGHUP`.
///
/// # Errors
/// Returns an error if `MQTTD_CONFIG_WATCH` is set but not a non-negative integer.
fn spawn_config_watcher(
    reloader: std::sync::Arc<reload::Reloader>,
    shutdown: &tokio_util::sync::CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(raw) = non_empty_env("MQTTD_CONFIG_WATCH") else {
        return Ok(());
    };
    let secs: u64 = raw
        .parse()
        .map_err(|_| format!("MQTTD_CONFIG_WATCH must be a number of seconds (got {raw:?})"))?;
    if secs == 0 {
        return Ok(()); // explicitly disabled
    }
    let paths = watched_policy_paths();
    if paths.is_empty() {
        warn!(
            "MQTTD_CONFIG_WATCH is set but no watchable policy files are configured — watcher idle"
        );
        return Ok(());
    }
    info!(
        interval_secs = secs,
        files = paths.len(),
        "config-file watcher enabled (ADR 0033): auto-reload on change"
    );
    tokio::spawn(config_watch::watch(
        reloader,
        paths,
        std::time::Duration::from_secs(secs),
        shutdown.clone(),
    ));
    Ok(())
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

/// Per-session offline-queue bounds (ADR 0001 §6) from `MQTTD_MAX_QUEUED_MESSAGES`
/// and `MQTTD_QUEUE_OVERFLOW`. Bounded by default; an unparseable value is a
/// startup error rather than a silent fallback.
/// Build and spawn the routing hub with its session store, returning the command
/// sender. The store is the **durable, consensus-backed** backend by default
/// (ADR 0006/0007/0029): a lease group over the peer mesh replicates each persistent
/// session's log. `MQTTD_DURABLE_SESSIONS=0|false|off|no` opts out to the bounded
/// in-memory backend (ADR 0001 §6). The effective mode is loudly logged.
type HubHandle = (
    mpsc::UnboundedSender<hub::HubCommand>,
    Arc<dyn SessionStore>,
    Option<mqtt_cluster::durable_plane::DurablePlane>,
    // The lease-group driver task (durable mode only), so graceful shutdown can stop it
    // rather than leave it spinning against a shut-down raft (ADR 0019).
    Option<tokio::task::JoinHandle<()>>,
);

/// Whether durable sessions are enabled given the `MQTTD_DURABLE_SESSIONS` value
/// (ADR 0029). Durable is the **default**: unset → on; `0`/`false`/`off`/`no`
/// (case-insensitive) opts out to the in-memory store; anything else → on.
fn durable_enabled(val: Option<&str>) -> bool {
    val.is_none_or(|v| {
        !matches!(
            v.to_ascii_lowercase().as_str(),
            "0" | "false" | "off" | "no"
        )
    })
}

/// The bounded lease-consensus voter-set size `N` from `MQTTD_LEASE_VOTERS` (ADR 0021).
/// Default `5` (recommend odd). An unparseable or zero value is a startup error rather
/// than a silent fallback — a zero-voter group would be un-electable.
fn lease_voters() -> Result<usize, Box<dyn std::error::Error>> {
    match non_empty_env("MQTTD_LEASE_VOTERS") {
        None => Ok(5),
        Some(raw) => {
            let n: usize = raw
                .parse()
                .map_err(|_| format!("MQTTD_LEASE_VOTERS is not a number: {raw:?}"))?;
            if n == 0 {
                return Err("MQTTD_LEASE_VOTERS must be at least 1".into());
            }
            Ok(n)
        }
    }
}

/// The failure-domain topology from `MQTTD_FAILURE_DOMAINS` (ADR 0016 T4): a comma-separated
/// list of `node-id=domain` pairs (e.g. `n1=rack-a,n2=rack-a,n3=rack-b`) mapping each cluster
/// node to its rack/zone label, so the bounded lease-voter set is spread across domains and one
/// domain's loss cannot take quorum. **Must be cluster-uniform** — every node needs the same map
/// so each successive leader's reconciler computes the same voter target. Empty/unset disables
/// the spread (every node is its own singleton domain — the prior id-ordered selection). A
/// malformed entry is a startup error, not a silent skip — a deny-by-default broker does not
/// quietly ignore a misconfigured security/availability control.
fn failure_domains(
) -> Result<std::collections::BTreeMap<NodeId, String>, Box<dyn std::error::Error>> {
    let mut map = std::collections::BTreeMap::new();
    let Some(raw) = non_empty_env("MQTTD_FAILURE_DOMAINS") else {
        return Ok(map);
    };
    for entry in raw.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (node, domain) = entry.split_once('=').ok_or_else(|| {
            format!("MQTTD_FAILURE_DOMAINS entry {entry:?} is not `node-id=domain`")
        })?;
        let (node, domain) = (node.trim(), domain.trim());
        if node.is_empty() || domain.is_empty() {
            return Err(format!("MQTTD_FAILURE_DOMAINS entry {entry:?} has an empty side").into());
        }
        map.insert(NodeId(node.to_string()), domain.to_string());
    }
    Ok(map)
}

/// This node's own failure-domain label from `MQTTD_FAILURE_DOMAIN` (singular, ADR 0016 T5):
/// the node advertises it over the authenticated SWIM gossip payload so the cluster topology
/// **self-assembles** — no static, cluster-uniform `MQTTD_FAILURE_DOMAINS` map required. Unset
/// leaves this node unlabelled (its own singleton domain, unless a peer/static map supplies a
/// label). When both are set, the self-advertised label wins for this node.
fn this_node_failure_domain() -> Option<String> {
    non_empty_env("MQTTD_FAILURE_DOMAIN")
}

async fn start_hub(
    node_id: &NodeId,
    placement: &Arc<RwLock<Placement>>,
    metrics: &Arc<mqtt_observability::metrics::Metrics>,
) -> Result<HubHandle, Box<dyn std::error::Error>> {
    // Claim the data directory for this node (ADR 0018 phase 5): refuse to open another
    // node's persistent state, before any store touches disk.
    if let Some(dir) = non_empty_env("MQTTD_DATA_DIR") {
        mqtt_storage::data_dir::guard_data_dir(&dir, &node_id.0)?;
    }
    // Durable is the **default** (ADR 0029): the consensus-backed replicated store is on
    // unless explicitly opted out. `0/false/off/no` selects the lightweight in-memory store.
    let durable = durable_enabled(non_empty_env("MQTTD_DURABLE_SESSIONS").as_deref());
    if durable {
        // A node started with no SWIM seeds is the cluster founder — only it
        // bootstraps the lease group (ADR 0007 §2). Exactly one founder per cluster.
        let founder = non_empty_env("MQTTD_SWIM_SEEDS").is_none();
        // Persist the lease store and follower replica copy on disk when MQTTD_DATA_DIR
        // is set (ADR 0018 phases 2–3): the lease vote/assignments and the replicated
        // session log survive a restart (restoring Raft safety and full-cluster-restart
        // durability). Without it the durable plane is in-memory (rebuilds from peers).
        let data_dir = non_empty_env("MQTTD_DATA_DIR");
        // Bound the lease-consensus voter set (ADR 0021): at most `N` members vote, the
        // rest join as learners that still receive the lease log. Default 5 (recommend
        // odd); decouples consensus cost from cluster size.
        let voter_cap = lease_voters()?;
        // Failure-domain topology (ADR 0016 T4): spread the bounded voter set across racks/zones.
        let domains = failure_domains()?;
        info!(
            founder,
            persistent = data_dir.is_some(),
            voter_cap,
            failure_domains = domains.len(),
            "DURABLE sessions enabled: consensus-backed replicated store"
        );
        let (store, durable_retained, plane, driver) =
            mqtt_cluster::durable_node::build_durable_node(
                node_id.clone(),
                placement.clone(),
                founder,
                voter_cap,
                &domains,
                data_dir.as_deref().map(Path::new),
                None, // no commit-latency fault injection in production (ADR 0026)
            )
            .await;
        let (mut hub, hub_tx) = hub::Hub::with_config_and_placement(
            node_id.clone(),
            store.clone(),
            Some(placement.clone()),
        );
        // Keep a plane clone for the health endpoint's lease-group readiness signal.
        let plane_for_health = plane.clone();
        hub.attach_durable_plane(plane);
        // Durable retained (ADR 0037): retained mutations also commit through the
        // topic's group lease-owner, so retained state converges instead of diverging.
        hub.attach_durable_retained(durable_retained);
        if let Some(dir) = &data_dir {
            hub.attach_retained_store(persistent_retained(dir)?); // ADR 0018 phase 4
        }
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());
        Ok((hub_tx, store, Some(plane_for_health), Some(driver)))
    } else if let Some(dir) = non_empty_env("MQTTD_DATA_DIR") {
        // Single-node **persistent** sessions (ADR 0018 phase 1): the session log is
        // backed by an on-disk redb database, so sessions, subscriptions, the QoS-2
        // dedup window and offline queues survive a restart. Not replicated — use
        // MQTTD_DURABLE_SESSIONS for cluster (quorum) durability.
        let path = std::path::Path::new(&dir).join("sessions.redb");
        info!(
            path = %path.display(),
            "PERSISTENT sessions: on-disk durable store (ADR 0018; single-node, not replicated)"
        );
        let log = PersistentLog::open(&path)?;
        let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::with_limits(
            log,
            queue_limits_from_env()?,
        ));
        let (mut hub, hub_tx) = hub::Hub::with_config_and_placement(
            node_id.clone(),
            store.clone(),
            Some(placement.clone()),
        );
        hub.attach_retained_store(persistent_retained(&dir)?); // ADR 0018 phase 4
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());
        Ok((hub_tx, store, None, None))
    } else {
        let store: Arc<dyn SessionStore> =
            Arc::new(MemorySessionStore::with_limits(queue_limits_from_env()?));
        let (mut hub, hub_tx) = hub::Hub::with_config_and_placement(
            node_id.clone(),
            store.clone(),
            Some(placement.clone()),
        );
        hub.attach_metrics(metrics.clone());
        tokio::spawn(hub.run());
        Ok((hub_tx, store, None, None))
    }
}

/// Build the on-disk retained-message store at `<dir>/retained.redb` (ADR 0018 phase 4),
/// so retained messages survive a restart.
fn persistent_retained(dir: &str) -> Result<Box<dyn RetainedStore>, Box<dyn std::error::Error>> {
    let path = Path::new(dir).join("retained.redb");
    Ok(Box::new(PersistentRetainedStore::open(path)?))
}

/// Start the health endpoint server from `MQTTD_HEALTH_BIND` (no-op when unset).
/// `/livez` reports hub liveness; `/readyz` additionally requires the mesh to have
/// at least `MQTTD_READY_MIN_MEMBERS` members (default 1) and, when durable sessions
/// are on, the lease group to be ready (a leader exists and this node is a voter).
async fn start_health_from_env(
    hub_tx: &mpsc::UnboundedSender<hub::HubCommand>,
    placement: &Arc<RwLock<Placement>>,
    durable_plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
) -> Result<Arc<std::sync::atomic::AtomicBool>, Box<dyn std::error::Error>> {
    let health_bind = non_empty_env("MQTTD_HEALTH_BIND");
    let metrics_bind = non_empty_env("MQTTD_METRICS_BIND");
    if health_bind.is_none() && metrics_bind.is_none() {
        // Neither server: hand back a standalone flag so the caller's shutdown path is
        // uniform (nothing reads it).
        return Ok(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    }
    let min_members = match non_empty_env("MQTTD_READY_MIN_MEMBERS") {
        Some(raw) => raw
            .parse()
            .map_err(|_| format!("MQTTD_READY_MIN_MEMBERS is not a number: {raw:?}"))?,
        None => 1,
    };
    // One state serves both binds: health endpoints plus `/metrics` (ADR 0020).
    let state = mqttd::health::HealthState::new(
        hub_tx.clone(),
        Some(placement.clone()),
        durable_plane,
        min_members,
    )
    .with_metrics(metrics);
    let draining = state.draining_handle();
    if let Some(bind) = &health_bind {
        let listener = TcpListener::bind(bind).await?;
        info!(%bind, min_members, "serving health endpoints (/livez, /readyz, /healthz, /metrics)");
        tokio::spawn(mqttd::health::serve(listener, state.clone()));
    }
    // An optional separate bind to isolate the metrics scrape from the health probes.
    if let Some(bind) = &metrics_bind {
        if Some(bind) != health_bind.as_ref() {
            let listener = TcpListener::bind(bind).await?;
            info!(%bind, "serving /metrics on a separate bind (ADR 0020)");
            tokio::spawn(mqttd::health::serve(listener, state));
        }
    }
    Ok(draining)
}

fn queue_limits_from_env() -> Result<QueueLimits, Box<dyn std::error::Error>> {
    let mut limits = QueueLimits::default();
    if let Some(raw) = non_empty_env("MQTTD_MAX_QUEUED_MESSAGES") {
        limits.max_messages = raw
            .parse()
            .map_err(|_| format!("MQTTD_MAX_QUEUED_MESSAGES is not a number: {raw:?}"))?;
    }
    if let Some(raw) = non_empty_env("MQTTD_QUEUE_OVERFLOW") {
        limits.overflow = match raw.as_str() {
            "drop-oldest" => OverflowPolicy::DropOldest,
            "reject-newest" => OverflowPolicy::RejectNewest,
            other => {
                return Err(format!(
                    "MQTTD_QUEUE_OVERFLOW must be drop-oldest or reject-newest, got {other:?}"
                )
                .into())
            }
        };
    }
    info!(
        max_queued_messages = limits.max_messages,
        overflow = ?limits.overflow,
        "offline session queues bounded"
    );
    Ok(limits)
}

/// Build the cluster-bus mTLS context from `MQTTD_PEER_TLS_{CA,CERT,KEY}`.
/// All three must be set together; none means a (loudly logged) plaintext mesh.
/// `MQTTD_PEER_TLS_CRL` (optional, requires the other three) loads a cluster-CA-signed
/// CRL checked on every inbound signed-gossip datagram (ADR 0022 T7).
fn peer_tls_from_env() -> Result<Option<peer::PeerTls>, Box<dyn std::error::Error>> {
    let crl_path = non_empty_env("MQTTD_PEER_TLS_CRL");
    match (
        non_empty_env("MQTTD_PEER_TLS_CA"),
        non_empty_env("MQTTD_PEER_TLS_CERT"),
        non_empty_env("MQTTD_PEER_TLS_KEY"),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            let (ca, cert, key) = (Path::new(&ca), Path::new(&cert), Path::new(&key));
            let ca_der = tls::first_cert_der(ca)?;
            // Cluster-bus CRL (ADR 0022 T7): parsed and CA-verified up front — a bad CRL
            // is a startup error, not a silently-skipped revocation check.
            let crl_path = crl_path.map(std::path::PathBuf::from);
            let gossip_crl = match &crl_path {
                Some(p) => {
                    let list = load_gossip_crl(p, &ca_der)?;
                    info!(path = %p.display(), revoked = list.len(),
                        "cluster-bus CRL loaded: revoked certs are rejected on the gossip plane");
                    Some(list)
                }
                None => None,
            };
            Ok(Some(peer::PeerTls {
                acceptor: tls::server_acceptor(cert, key, Some(ca))?,
                connector: tls::client_connector(ca, cert, key)?,
                // Raw DER kept for signed gossip (ADR 0022): the CA verifies inbound certs,
                // and our leaf + key sign outbound datagrams.
                cert_der: tls::first_cert_der(cert)?,
                key_der: tls::private_key_der(key)?,
                ca_der,
                gossip_crl: Arc::new(std::sync::RwLock::new(gossip_crl)),
                crl_path,
            }))
        }
        (None, None, None) if crl_path.is_none() => Ok(None),
        (None, None, None) => Err(
            "MQTTD_PEER_TLS_CRL requires MQTTD_PEER_TLS_CA/CERT/KEY: a CRL revokes \
             cluster-bus certificates, so there must be a cluster bus to revoke from"
                .into(),
        ),
        _ => Err(
            "MQTTD_PEER_TLS_CA, MQTTD_PEER_TLS_CERT and MQTTD_PEER_TLS_KEY \
             must be set together"
                .into(),
        ),
    }
}

/// Read + parse + CA-verify the cluster-bus CRL (ADR 0022 T7). Used at startup and by the
/// reload closure, so a republished CRL takes effect without a restart.
fn load_gossip_crl(
    path: &Path,
    ca_der: &[u8],
) -> Result<mqtt_auth::signed_gossip::RevocationList, String> {
    let der = tls::first_crl_der(path).map_err(|e| format!("cluster-bus CRL: {e}"))?;
    mqtt_auth::signed_gossip::RevocationList::from_der(&der, ca_der)
        .map_err(|e| format!("cluster-bus CRL {}: {e}", path.display()))
}

/// Signs outgoing gossip with this node's cluster-bus key, embedding its leaf cert so
/// receivers can chain-verify it (ADR 0022).
struct NodeGossipSigner {
    cert_der: Vec<u8>,
    signer: mqtt_auth::signed_gossip::GossipSigner,
}

impl mqtt_cluster::swim_auth::GossipSign for NodeGossipSigner {
    fn cert_der(&self) -> &[u8] {
        &self.cert_der
    }
    fn sign(&self, payload: &[u8]) -> Vec<u8> {
        self.signer.sign(payload)
    }
}

/// Verifies an inbound gossip cert chains to the cluster CA and its signature is valid,
/// returning the authenticated Common Name (ADR 0022).
struct CaGossipVerifier {
    ca_der: Vec<u8>,
    /// The live revocation list (ADR 0022 T7), shared with the reloader so a republished
    /// CRL revokes a node's gossip on the very next datagram — no restart.
    crl: reload::SwimCrlSlot,
}

impl mqtt_cluster::swim_auth::GossipVerify for CaGossipVerifier {
    fn verify(
        &self,
        cert_der: &[u8],
        payload: &[u8],
        sig: &[u8],
    ) -> Result<mqtt_cluster::swim_auth::VerifiedIdentity, mqtt_cluster::swim_auth::OpenReject>
    {
        use mqtt_auth::signed_gossip::VerifyError;
        use mqtt_cluster::swim_auth::{OpenReject, VerifiedIdentity};
        // Real wall-clock time, like rustls' own validity checks on the TLS paths; an
        // unrepresentable clock fails closed inside `verify`.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| i64::try_from(d.as_secs()).unwrap_or(i64::MAX));
        let crl = self
            .crl
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match mqtt_auth::signed_gossip::verify(
            &self.ca_der,
            cert_der,
            payload,
            sig,
            now,
            crl.as_ref(),
        ) {
            Ok(v) => Ok(VerifiedIdentity {
                cn: v.cn,
                failure_domain: v.failure_domain,
            }),
            Err(VerifyError::Expired) => Err(OpenReject::Expired),
            Err(VerifyError::Revoked) => Err(OpenReject::Revoked),
            Err(_) => Err(OpenReject::Auth),
        }
    }
}

/// Signed-gossip posture (ADR 0022), from `MQTTD_SWIM_SIGNED`. A strict on/off choice: a
/// signed node signs outgoing gossip and accepts only signed gossip (no mixed-version
/// coexistence — the pre-release rollout `prefer` mode was removed).
#[derive(Clone, Copy, PartialEq, Eq)]
enum SignedGossip {
    /// Shared-key MAC only (ADR 0003).
    Off,
    /// Sign outgoing and reject any unsigned v1 datagram.
    Require,
}

/// Resolve the signed-gossip mode. Defaults to `Require` when both the shared key and the
/// cluster-bus TLS material are present (the security win), else `Off`.
fn signed_gossip_from_env(
    has_tls: bool,
    has_key: bool,
) -> Result<SignedGossip, Box<dyn std::error::Error>> {
    Ok(match non_empty_env("MQTTD_SWIM_SIGNED").as_deref() {
        Some("require") => SignedGossip::Require,
        Some("off") => SignedGossip::Off,
        Some(other) => {
            return Err(
                format!("MQTTD_SWIM_SIGNED must be one of require|off (got {other:?})").into(),
            );
        }
        None if has_tls && has_key => SignedGossip::Require,
        None => SignedGossip::Off,
    })
}

/// Layer per-node signatures (ADR 0022) onto the shared-key `auth` when configured. Signed
/// gossip needs both the shared key (the HMAC base) and cluster-bus TLS material (to sign
/// and verify); a requested mode without them is a startup error, not a silent downgrade.
fn apply_signed_gossip(
    auth: Option<SwimAuth>,
    peer_tls: Option<&peer::PeerTls>,
    mode: SignedGossip,
) -> Result<Option<SwimAuth>, Box<dyn std::error::Error>> {
    if mode == SignedGossip::Off {
        return Ok(auth);
    }
    let Some(base) = auth else {
        return Err(
            "MQTTD_SWIM_SIGNED requires MQTTD_SWIM_KEY: signed gossip layers a \
                    per-node signature on top of the shared-key MAC"
                .into(),
        );
    };
    let Some(tls) = peer_tls else {
        return Err("MQTTD_SWIM_SIGNED requires cluster-bus TLS material \
                    (MQTTD_PEER_TLS_CA/CERT/KEY) to sign and verify gossip"
            .into());
    };
    let signer = mqtt_auth::signed_gossip::GossipSigner::from_pkcs8_der(&tls.key_der)
        .map_err(|e| format!("signed gossip signing key: {e}"))?;
    let signer = Arc::new(NodeGossipSigner {
        cert_der: tls.cert_der.clone(),
        signer,
    });
    let verifier = Arc::new(CaGossipVerifier {
        ca_der: tls.ca_der.clone(),
        crl: tls.gossip_crl.clone(),
    });
    info!("SWIM gossip is SIGNED per-node (ADR 0022)");
    Ok(Some(base.with_signing(signer, verifier)))
}

/// How many gossip sequence numbers to reserve per fsync (ADR 0023). At gossip's
/// few-datagrams-per-second this is one durable write every several minutes.
const SEQ_BLOCK: u64 = 1024;

/// On-disk persistence for the gossip sequence high-water (ADR 0023): an 8-byte little-endian
/// counter in `<data dir>/gossip-seq`, fsync'd on every reservation so the sequence is never
/// reused across a restart. A persist failure is fatal — silently reusing a sequence would
/// reopen the replay window.
struct FileSeqStore {
    path: std::path::PathBuf,
    reserved: u64,
}

impl FileSeqStore {
    fn open(path: std::path::PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let reserved = match std::fs::read(&path) {
            Ok(b) if b.len() == 8 => u64::from_le_bytes(b.try_into().unwrap()),
            Ok(b) if b.is_empty() => 0,
            Ok(_) => return Err(format!("corrupt gossip sequence file {}", path.display()).into()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(format!("reading {}: {e}", path.display()).into()),
        };
        Ok(Self { path, reserved })
    }
}

impl mqtt_cluster::replay::SeqStore for FileSeqStore {
    fn reserved(&self) -> u64 {
        self.reserved
    }
    fn persist(&mut self, reserved_until: u64) {
        use std::io::Write as _;
        // Fail-stop on any write/fsync error: continuing could reuse a sequence (ADR 0023).
        let result = std::fs::File::create(&self.path).and_then(|mut f| {
            f.write_all(&reserved_until.to_le_bytes())?;
            f.sync_all()
        });
        assert!(
            result.is_ok(),
            "persisting the gossip sequence to {} failed ({:?}); refusing to risk sequence reuse",
            self.path.display(),
            result.err()
        );
        self.reserved = reserved_until;
    }
}

/// Anti-replay posture (ADR 0023), from `MQTTD_SWIM_REPLAY`. A strict on/off choice: a
/// sequenced node sequences outgoing gossip and accepts only sequenced gossip (the
/// pre-release rollout `prefer` mode was removed).
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplayMode {
    Off,
    Require,
}

/// Layer anti-replay (ADR 0023) onto the signed `auth` when configured, returning the auth
/// plus the per-node sequence allocator the driver uses to sequence outgoing datagrams.
/// Anti-replay binds to the per-node signature, so it requires signed gossip; it persists a
/// sequence counter, so it requires a data dir. A requested mode without them is a startup
/// error. Defaults to `off` (opt-in).
fn apply_anti_replay(
    auth: Option<SwimAuth>,
    signed: SignedGossip,
) -> Result<(Option<SwimAuth>, Option<swim_driver::SeqAlloc>), Box<dyn std::error::Error>> {
    let mode = match non_empty_env("MQTTD_SWIM_REPLAY").as_deref() {
        Some("require") => ReplayMode::Require,
        Some("off") | None => ReplayMode::Off,
        Some(other) => {
            return Err(
                format!("MQTTD_SWIM_REPLAY must be one of require|off (got {other:?})").into(),
            );
        }
    };
    if mode == ReplayMode::Off {
        return Ok((auth, None));
    }
    let Some(auth) = auth else {
        return Err("MQTTD_SWIM_REPLAY requires MQTTD_SWIM_KEY".into());
    };
    if signed == SignedGossip::Off {
        return Err(
            "MQTTD_SWIM_REPLAY requires MQTTD_SWIM_SIGNED=require: anti-replay binds the \
                    sequence to the per-node signature"
                .into(),
        );
    }
    let Some(dir) = non_empty_env("MQTTD_DATA_DIR") else {
        return Err(
            "MQTTD_SWIM_REPLAY requires MQTTD_DATA_DIR for the persisted, restart-safe \
                    sequence counter"
                .into(),
        );
    };
    let store = FileSeqStore::open(Path::new(&dir).join("gossip-seq"))?;
    let alloc = mqtt_cluster::replay::SequenceAllocator::open(
        Box::new(store) as Box<dyn mqtt_cluster::replay::SeqStore>,
        SEQ_BLOCK,
    );
    info!("SWIM gossip anti-replay enabled (ADR 0023)");
    Ok((Some(auth.with_sequencing()), Some(alloc)))
}

/// Start SWIM membership from `MQTTD_SWIM_{BIND,SEEDS}` (no-op when unset) and
/// hand its events to the peer-link manager.
async fn start_swim_from_env(
    node_id: &NodeId,
    peer_bind: Option<String>,
    hub_tx: &mpsc::UnboundedSender<hub::HubCommand>,
    peer_tls: Option<&peer::PeerTls>,
    placement: Arc<RwLock<Placement>>,
    shutdown: &tokio_util::sync::CancellationToken,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
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
        let mut auth = SwimAuth::from_hex_key(&hex)?;
        // Additional keys accepted (but not used to seal) during a rotation window (ADR
        // 0003): an old key still opens peers' datagrams while the cluster migrates to the
        // new primary, so the gossip key rotates without downtime.
        let mut rotation = 0;
        for k in non_empty_env("MQTTD_SWIM_KEY_ACCEPT")
            .iter()
            .flat_map(|s| {
                s.split(',')
                    .map(str::trim)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .filter(|k| !k.is_empty())
        {
            auth = auth.accept_also_hex(&k)?;
            rotation += 1;
        }
        if rotation > 0 {
            info!(
                rotation_keys = rotation,
                "SWIM gossip accepts additional rotation keys (ADR 0003)"
            );
        }
        Some(auth)
    } else {
        if non_empty_env("MQTTD_SWIM_KEY_ACCEPT").is_some() {
            return Err(
                "MQTTD_SWIM_KEY_ACCEPT requires MQTTD_SWIM_KEY: rotation keys are \
                        accepted in addition to a primary key, not on their own"
                    .into(),
            );
        }
        warn!(
            "INSECURE: SWIM gossip is UNAUTHENTICATED (no MQTTD_SWIM_KEY) — \
             anyone reaching the gossip port can inject membership claims, \
             including Dead claims that tear down routing"
        );
        None
    };
    // Layer per-node signatures (ADR 0022) then anti-replay sequencing (ADR 0023) on top of
    // the shared-key MAC when configured.
    let signed = signed_gossip_from_env(peer_tls.is_some(), auth.is_some())?;
    let auth = apply_signed_gossip(auth, peer_tls, signed)?;
    let (auth, seq_alloc) = apply_anti_replay(auth, signed)?;
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
        // Advertise this node's own failure-domain label over gossip (ADR 0016 T5).
        this_node_failure_domain(),
        mqtt_cluster::swim::Config::default(),
        seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    // Count dropped gossip datagrams by reason on the metrics registry (ADR 0003-T6).
    let reject: swim_driver::RejectCounter = {
        let m = metrics.clone();
        Arc::new(move |reason: &'static str| m.gossip_rejected(reason))
    };
    // On graceful shutdown (ADR 0019) the driver announces a SWIM leave so peers drop
    // this node from the ring immediately, instead of waiting out failure detection.
    tokio::spawn(swim_driver::run(
        socket,
        swim,
        SWIM_TICK,
        event_tx,
        auth,
        seq_alloc,
        Some(reject),
        shutdown.clone().cancelled_owned(),
    ));
    tokio::spawn(cluster::maintain_peer_links(
        event_rx,
        node_id.clone(),
        hub_tx.clone(),
        peer_tls.cloned(),
        Some(placement),
        Some(metrics),
    ));
    Ok(())
}

/// Accept TLS clients forever: per-connection handshake (off the accept loop so
/// a slow handshake cannot stall other clients), then normal MQTT handling.
async fn serve_tls_clients(
    listener: TcpListener,
    acceptor_rx: tokio::sync::watch::Receiver<TlsAcceptor>,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    loop {
        let (stream, peer) = tokio::select! {
            // Graceful shutdown (ADR 0019): stop accepting; refuse new connections fast.
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = %e, "TLS listener accept failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("accept");
                    }
                    return;
                }
            },
        };
        debug!(%peer, "accepted TLS connection");
        if let Some(m) = &policy.metrics {
            m.connection_accepted("tls");
        }
        // Read the *current* acceptor per accept, so a SIGHUP cert/key/CA reload is served
        // on the next handshake (ADR 0032 T6); in-flight TLS sessions are undisturbed.
        let acceptor = acceptor_rx.borrow().clone();
        let hub = hub_tx.clone();
        let policy = policy.clone();
        connections.spawn(async move {
            let _ = stream.set_nodelay(true);
            match acceptor.accept(stream).await {
                Ok(tls_stream) => {
                    // mTLS admission (ADR 0004/0040): the verified leaf cert's CN + serial.
                    let cert = conn::tls_admission(&tls_stream);
                    conn::handle_stream(tls_stream, Some(peer), cert, policy, hub).await;
                }
                Err(e) => {
                    debug!(%peer, error = %e, "TLS handshake failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("tls");
                    }
                }
            }
        });
    }
}

/// Accept plaintext clients forever (insecure; explicitly opted into).
async fn serve_plaintext_clients(
    listener: TcpListener,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    loop {
        let (stream, peer) = tokio::select! {
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = %e, "plaintext listener accept failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("accept");
                    }
                    return;
                }
            },
        };
        debug!(%peer, "accepted connection");
        if let Some(m) = &policy.metrics {
            m.connection_accepted("plaintext");
        }
        let _ = stream.set_nodelay(true);
        connections.spawn(conn::handle_stream(
            stream,
            Some(peer),
            None,
            policy.clone(),
            hub_tx.clone(),
        ));
    }
}

/// Accept MQTT-over-WebSocket clients over plaintext (insecure; explicitly opted into).
/// The WebSocket handshake (per connection, off the accept loop) yields a byte stream that
/// the MQTT engine reads exactly like a TCP socket (ADR 0035).
async fn serve_ws_clients(
    listener: TcpListener,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    loop {
        let (stream, peer) = tokio::select! {
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = %e, "ws listener accept failed");
                    if let Some(m) = &policy.metrics { m.connection_error("accept"); }
                    return;
                }
            },
        };
        debug!(%peer, "accepted ws connection");
        if let Some(m) = &policy.metrics {
            m.connection_accepted("ws");
        }
        let hub = hub_tx.clone();
        let policy = policy.clone();
        connections.spawn(async move {
            let _ = stream.set_nodelay(true);
            match mqtt_net::ws::accept(stream).await {
                Ok(ws) => conn::handle_stream(ws, Some(peer), None, policy, hub).await,
                Err(e) => {
                    debug!(%peer, error = %e, "websocket handshake failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("ws");
                    }
                }
            }
        });
    }
}

/// Accept MQTT-over-WebSocket clients over TLS (`wss://`, ADR 0035). TLS is done first with
/// the (reloadable) ADR 0002 acceptor — so the mTLS client-cert **identity** is extracted from
/// the TLS stream exactly as for a TCP TLS client (ADR 0004) — then the WebSocket handshake
/// runs over the TLS stream.
async fn serve_wss_clients(
    listener: TcpListener,
    acceptor_rx: tokio::sync::watch::Receiver<TlsAcceptor>,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    loop {
        let (stream, peer) = tokio::select! {
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = %e, "wss listener accept failed");
                    if let Some(m) = &policy.metrics { m.connection_error("accept"); }
                    return;
                }
            },
        };
        debug!(%peer, "accepted wss connection");
        if let Some(m) = &policy.metrics {
            m.connection_accepted("wss");
        }
        // Read the current acceptor per accept so a SIGHUP cert reload is served next handshake.
        let acceptor = acceptor_rx.borrow().clone();
        let hub = hub_tx.clone();
        let policy = policy.clone();
        connections.spawn(async move {
            let _ = stream.set_nodelay(true);
            match acceptor.accept(stream).await {
                Ok(tls) => {
                    // mTLS admission (ADR 0004/0040): the verified leaf cert's CN + serial —
                    // read before the TLS stream is consumed by the WebSocket adapter.
                    let cert = conn::tls_admission(&tls);
                    match mqtt_net::ws::accept(tls).await {
                        Ok(ws) => conn::handle_stream(ws, Some(peer), cert, policy, hub).await,
                        Err(e) => {
                            debug!(%peer, error = %e, "websocket handshake failed");
                            if let Some(m) = &policy.metrics {
                                m.connection_error("ws");
                            }
                        }
                    }
                }
                Err(e) => {
                    debug!(%peer, error = %e, "TLS handshake failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("tls");
                    }
                }
            }
        });
    }
}

/// Accept MQTT-over-QUIC clients (ADR 0036). QUIC mandates TLS 1.3, so the mTLS **identity** is
/// the verified leaf-cert CN read from the connection (ADR 0004), exactly as for a TCP TLS
/// client. The MQTT session runs over the connection's first **bidirectional** stream (the
/// control stream) — multi-stream data streams layer on this foundation.
async fn serve_quic_clients(
    endpoint: quinn::Endpoint,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    loop {
        let incoming = tokio::select! {
            () = shutdown.cancelled() => {
                endpoint.close(0u32.into(), b"shutdown");
                return;
            }
            inc = endpoint.accept() => match inc {
                Some(inc) => inc,
                None => return, // endpoint closed
            },
        };
        let hub = hub_tx.clone();
        let policy = policy.clone();
        connections.spawn(async move {
            let conn = match incoming.await {
                Ok(conn) => conn,
                Err(e) => {
                    debug!(error = %e, "QUIC handshake failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("tls");
                    }
                    return;
                }
            };
            let peer = conn.remote_address();
            debug!(%peer, "accepted QUIC connection");
            if let Some(m) = &policy.metrics {
                m.connection_accepted("quic");
            }
            // mTLS admission (ADR 0004/0040): the verified leaf cert's CN + serial, from
            // the QUIC handshake.
            let cert = mqtt_net::quic::peer_leaf_cert(&conn).and_then(|c| conn::cert_admission(&c));
            let identity = cert.as_ref().map(|c| c.identity.clone());
            // Connection-migration observation (ADR 0036 §3b): QUIC keeps a connection alive
            // across a client path change (Wi-Fi↔cellular, NAT rebind). Watch the remote address
            // on the *same* connection — a change is a migration, not a reconnect — and log +
            // count it. The session, streams, and identity are untouched.
            spawn_quic_migration_watch(conn.clone(), identity.clone(), policy.metrics.clone());
            // Multi-stream mux (ADR 0036): the control stream carries the session; any data
            // streams the client opens feed PUBLISH into the same session, no HoL blocking.
            match mqtt_net::quic::accept_mux(conn).await {
                Ok(mux) => conn::handle_stream(mux, Some(peer), cert, policy, hub).await,
                Err(e) => {
                    debug!(%peer, error = %e, "QUIC connection opened no control stream");
                }
            }
        });
    }
}

/// Watch one QUIC connection for **path migration** (ADR 0036 §3b). QUIC identifies a connection
/// by its connection ID, not the 4-tuple, so it survives a client address change (Wi-Fi↔cellular,
/// NAT rebind) on the *same* connection — no reconnect, no new handshake. Observing
/// `remote_address()` change is how the broker sees it: on a change we log `from → to` for the
/// identity and bump `mqttd_quic_path_migrations_total`. The session, streams, and mTLS identity
/// are untouched. One slow timer per QUIC connection; it does nothing until the path actually moves
/// and stops when the connection closes.
fn spawn_quic_migration_watch(
    conn: quinn::Connection,
    identity: Option<mqtt_auth::Identity>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
) {
    const POLL: Duration = Duration::from_millis(500);
    let subject = identity.map_or_else(|| "<anonymous>".to_string(), |i| i.subject);
    tokio::spawn(async move {
        let mut last = conn.remote_address();
        loop {
            tokio::select! {
                _ = conn.closed() => return,
                () = tokio::time::sleep(POLL) => {
                    let cur = conn.remote_address();
                    if cur != last {
                        info!(identity = %subject, from = %last, to = %cur,
                            "QUIC connection migrated to a new client path");
                        if let Some(m) = &metrics {
                            m.quic_path_migrated();
                        }
                        last = cur;
                    }
                }
            }
        }
    });
}

/// Read an environment variable, treating unset or empty as absent.
fn non_empty_env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|v| !v.is_empty())
}

/// Parse an env var as `T`, falling back to `default` when unset/empty. An unparseable
/// value is a startup error rather than a silent fallback.
fn parse_env<T>(key: &str, default: T) -> Result<T, Box<dyn std::error::Error>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match non_empty_env(key) {
        None => Ok(default),
        Some(v) => v
            .parse::<T>()
            .map_err(|e| format!("{key}: invalid value {v:?}: {e}").into()),
    }
}

/// Build the server-wide MQTT 5 wire limits from env (ADR 0011/0012/0013), each with a
/// spec-sensible default. `MQTTD_TOPIC_ALIAS_MAX` (default 16; `0` disables inbound
/// aliases), `MQTTD_RECEIVE_MAXIMUM` (default 256; floored at 1 — a Receive Maximum of 0
/// is a Protocol Error), `MQTTD_AUTH_TIMEOUT` seconds (default 10; floored at 1).
fn wire_limits_from_env() -> Result<conn::WireLimits, Box<dyn std::error::Error>> {
    let d = conn::WireLimits::default();
    Ok(conn::WireLimits {
        topic_alias_max: parse_env("MQTTD_TOPIC_ALIAS_MAX", d.topic_alias_max)?,
        receive_maximum: parse_env("MQTTD_RECEIVE_MAXIMUM", d.receive_maximum)?.max(1),
        auth_round_timeout: Duration::from_secs(
            parse_env("MQTTD_AUTH_TIMEOUT", d.auth_round_timeout.as_secs())?.max(1),
        ),
    })
}

/// Default graceful-shutdown drain deadline (ADR 0019), aligned with a typical
/// Kubernetes `terminationGracePeriodSeconds`.
const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// The graceful-shutdown drain deadline from `MQTTD_SHUTDOWN_GRACE` (seconds).
fn shutdown_grace_from_env() -> Duration {
    non_empty_env("MQTTD_SHUTDOWN_GRACE")
        .and_then(|v| v.parse().ok())
        .map_or(DEFAULT_SHUTDOWN_GRACE, Duration::from_secs)
}

/// Run until a shutdown signal, then drain gracefully (ADR 0019): fail readiness, stop
/// accepting and drain live connections, all bounded by the grace deadline (or a second
/// signal), then stop the lease consensus core cleanly.
async fn graceful_shutdown(
    shutdown: &tokio_util::sync::CancellationToken,
    connections: &tokio_util::task::TaskTracker,
    draining: &std::sync::atomic::AtomicBool,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    lease_driver: Option<tokio::task::JoinHandle<()>>,
) {
    connections.close(); // no more spawns once the accept loops stop
    wait_for_shutdown_signal().await;
    let grace = shutdown_grace_from_env();
    warn!(
        grace_secs = grace.as_secs(),
        "shutdown signal received; draining"
    );

    // 1. Fail readiness so orchestrators stop routing new traffic (liveness stays up).
    draining.store(true, std::sync::atomic::Ordering::Release);
    // 2. Stop accepting and tell live connections to finish their current packet and
    //    close (without firing wills — the client is not gone, its session is retained).
    shutdown.cancel();
    // 3. Wait for connections to drain, bounded by the grace deadline; a second signal
    //    escalates to immediate exit.
    tokio::select! {
        () = connections.wait() => info!("all client connections drained"),
        () = tokio::time::sleep(grace) => {
            warn!("drain grace elapsed; forcing shutdown with connections still open");
        }
        () = wait_for_shutdown_signal() => warn!("second signal; forcing immediate shutdown"),
    }
    // 4. Stop the lease-group driver loop, then the consensus core, cleanly (in-flight
    //    durable writes are already fsync'd). Stopping the driver first avoids it issuing
    //    lease RPCs against a raft that is shutting down.
    if let Some(driver) = lease_driver {
        driver.abort();
        let _ = driver.await;
    }
    if let Some(plane) = plane {
        let _ = plane.raft().shutdown().await;
    }
    info!("shutdown complete");
}

/// Resolve once a shutdown signal arrives: `SIGTERM` (the orchestrator stop signal) or
/// `SIGINT` (Ctrl-C). Called again during drain so a *second* signal can escalate to an
/// immediate exit.
async fn wait_for_shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = term.recv() => {}
                }
            }
            Err(e) => {
                warn!(error = %e, "cannot install SIGTERM handler; only Ctrl-C stops the broker");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

#[cfg(test)]
mod tests {
    use super::durable_enabled;

    #[test]
    fn durable_is_the_default_and_opts_out_explicitly() {
        // Default (unset) → durable on (ADR 0029).
        assert!(durable_enabled(None));
        // Explicit falsey values opt out, case-insensitively.
        for v in ["0", "false", "False", "FALSE", "off", "OFF", "no", "No"] {
            assert!(!durable_enabled(Some(v)), "{v} should opt out");
        }
        // Truthy / anything-else stays on.
        for v in ["1", "true", "TRUE", "on", "yes", "anything"] {
            assert!(durable_enabled(Some(v)), "{v} should stay durable");
        }
    }
}
