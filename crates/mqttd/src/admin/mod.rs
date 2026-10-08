//! The authenticated admin API ([ADR 0081](../../../../docs/adr/0081-admin-api.md)).
//!
//! A listener separate from the health server, off unless `admin.bind` is set:
//!
//! - **TLS 1.3 with a required client certificate.** The handshake admits certificates
//!   from the admin client CA (and the cluster CA, so nodes can query each other); the
//!   verified subject then maps to a [`Role`] from the live `admin.viewers` /
//!   `admin.operators` lists. A subject in no list is refused.
//! - **Every request is audited** (`admin.request`): subject, role, method, target and
//!   status. Reads are audited too, because client and session detail identify people.
//! - **Versioned JSON** under `/admin/v1/`. A refusal carries a stable reason code in
//!   `{"error":{"code":…,"message":…}}`.
//! - **Bounded.** One request per connection, a deadline to send it, capped sizes, a cap
//!   on concurrent connections, and paged lists.
//!
//! The configuration itself is never written through this API (ADR 0081 §5): the file
//! stays the only source. The one exception is the rules file, and only for the subjects
//! `[rules] admin_writers` lists (ADR 0084): a write replaces the file and runs the
//! ordinary reload, so the file stays the only source of the rules too.

pub mod actions;
pub mod authz;
pub mod cli;
pub mod client;
pub mod cluster;
pub mod config;
pub mod http;
pub mod roles;
mod routes;
pub mod rules;
pub mod scope;
pub mod sessions;

pub use roles::{Caller, Role};

use crate::health::HealthState;
use mqtt_observability::AuditSink;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio_rustls::TlsAcceptor;
use tracing::debug;

/// Most admin connections served at once; more wait in the accept backlog.
const MAX_CONCURRENT: usize = 32;
/// Upper bound on answering one request once it has been read.
const HANDLER_DEADLINE: Duration = Duration::from_secs(30);

/// The cluster-CA check behind [`Role::Peer`], swappable so a reload that rotates the
/// cluster CA (ADR 0081 T18) re-maps node certificates on the next request.
pub type ClusterCaSlot = Arc<RwLock<Option<Arc<mqtt_net::tls::ChainCheck>>>>;

/// What the admin handlers read. Cheap to clone: every field is shared.
#[derive(Clone)]
pub struct AdminState {
    node_id: String,
    health: HealthState,
    live_config: Arc<RwLock<mqtt_config::Config>>,
    audit: Arc<dyn AuditSink>,
    /// Tells a certificate the cluster CA issued apart from an admin one, for
    /// [`Role::Peer`]. Holds `None` when the node has no cluster TLS.
    cluster_ca: ClusterCaSlot,
    /// How to reach the other nodes' admin listeners for the cluster view. `None` when
    /// this node has no cluster TLS: its peers are then listed as not queryable.
    peers: Option<Arc<cluster::PeerAccess>>,
    /// The hub and stores behind the client and session endpoints (T4).
    sessions: Option<Arc<sessions::SessionAccess>>,
    /// The live authorizer, for the dry run (T5).
    authz: Option<authz::LiveAuthorizer>,
    /// The reloader and config stamp, for the config and reload endpoints (T6).
    reload: Option<Arc<config::ReloadAccess>>,
    /// The cordon flag the admission gate and `/readyz` read (T8).
    cordon: Option<Arc<std::sync::atomic::AtomicBool>>,
    /// The running rules and the last reload, for the rules endpoints (ADR 0084).
    rules: Option<Arc<rules::RulesAccess>>,
}

impl std::fmt::Debug for AdminState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminState")
            .field("node_id", &self.node_id)
            .finish_non_exhaustive()
    }
}

impl AdminState {
    /// The state for node `node_id`: `health` supplies the node's own status,
    /// `live_config` the role lists (re-read per request), `audit` the sink every request
    /// is recorded to.
    #[must_use]
    pub fn new(
        node_id: String,
        health: HealthState,
        live_config: Arc<RwLock<mqtt_config::Config>>,
        audit: Arc<dyn AuditSink>,
    ) -> Self {
        Self {
            node_id,
            health,
            live_config,
            audit,
            cluster_ca: Arc::new(RwLock::new(None)),
            peers: None,
            sessions: None,
            authz: None,
            reload: None,
            cordon: None,
            rules: None,
        }
    }

    /// Serve the rules endpoints (ADR 0084). Their writes also need
    /// [`with_reload`](Self::with_reload): a write runs the ordinary reload.
    #[must_use]
    pub fn with_rules(mut self, access: rules::RulesAccess) -> Self {
        self.rules = Some(Arc::new(access));
        self
    }

    /// Serve cordon / uncordon over `flag`, the one the admission gate and health read.
    #[must_use]
    pub fn with_cordon(mut self, flag: Arc<std::sync::atomic::AtomicBool>) -> Self {
        self.cordon = Some(flag);
        self
    }

    /// Serve the effective config and the reload action.
    #[must_use]
    pub fn with_reload(mut self, access: config::ReloadAccess) -> Self {
        self.reload = Some(Arc::new(access));
        self
    }

    /// Serve the authorization dry run against the live policy.
    #[must_use]
    pub fn with_authorizer(mut self, live: authz::LiveAuthorizer) -> Self {
        self.authz = Some(live);
        self
    }

    /// Serve the client, session, subscriber, backlog and retained endpoints.
    #[must_use]
    pub fn with_sessions(mut self, access: sessions::SessionAccess) -> Self {
        self.sessions = Some(Arc::new(access));
        self
    }

    /// Let this node answer for the cluster by asking its peers' admin listeners.
    #[must_use]
    pub fn with_peers(mut self, peers: cluster::PeerAccess) -> Self {
        self.peers = Some(Arc::new(peers));
        self
    }

    /// This node's own state: the `/statusz` body (ADR 0054), or its identity alone when
    /// the status block is not wired.
    async fn local_status(&self) -> serde_json::Value {
        match self.health.statusz().await {
            Some(s) => serde_json::from_str(&s).unwrap_or_else(|_| serde_json::json!({})),
            None => serde_json::json!({
                "node_id": self.node_id,
                "version": env!("CARGO_PKG_VERSION"),
            }),
        }
    }

    /// Grant [`Role::Peer`] to certificates that verify against the cluster CA.
    #[must_use]
    pub fn with_cluster_ca(self, check: mqtt_net::tls::ChainCheck) -> Self {
        *self
            .cluster_ca
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::new(check));
        self
    }

    /// Read the cluster-CA check from `slot`, which a reload swaps (T18).
    #[must_use]
    pub fn with_cluster_ca_slot(mut self, slot: ClusterCaSlot) -> Self {
        self.cluster_ca = slot;
        self
    }

    /// The live cluster-CA check, for a reload to swap (T18).
    #[must_use]
    pub fn cluster_ca_slot(&self) -> ClusterCaSlot {
        self.cluster_ca.clone()
    }

    /// Identify the caller from the verified certificate chain (leaf first).
    fn caller(&self, chain: &[tokio_rustls::rustls::pki_types::CertificateDer<'_>]) -> Caller {
        let Some((subject, cn)) = chain
            .first()
            .and_then(|leaf| mqtt_auth::mtls::subject_from_cert(leaf.as_ref()))
        else {
            return Caller {
                subject: String::from("<unparseable certificate>"),
                cn: None,
                role: None,
            };
        };
        let is_peer = self
            .cluster_ca
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|c| c.verifies(chain));
        let role = {
            let live = self
                .live_config
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            roles::resolve(&live.admin, &subject, cn.as_deref(), is_peer)
        };
        Caller { subject, cn, role }
    }
}

/// Serve the admin API on `listener` until it errors, with a fixed acceptor.
pub async fn serve(listener: TcpListener, acceptor: TlsAcceptor, state: AdminState) {
    let (_keep, acceptor) = tokio::sync::watch::channel(acceptor);
    serve_reloadable(listener, acceptor, state).await;
}

/// Serve the admin API on `listener` until it errors. Each handshake uses the acceptor
/// current at accept, so a reload that rebuilds it (T18) serves the renewed certificate
/// and client CA on the next connection; connections already accepted are undisturbed.
pub async fn serve_reloadable(
    listener: TcpListener,
    acceptor: tokio::sync::watch::Receiver<TlsAcceptor>,
    state: AdminState,
) {
    let slots = Arc::new(Semaphore::new(MAX_CONCURRENT));
    loop {
        let Ok(permit) = slots.clone().acquire_owned().await else {
            return;
        };
        match listener.accept().await {
            Ok((stream, _)) => {
                let (acceptor, state) = (acceptor.borrow().clone(), state.clone());
                tokio::spawn(async move {
                    handle(stream, acceptor, state).await;
                    drop(permit);
                });
            }
            // Issue #504: pause and accept again; never end the listener.
            Err(e) => crate::accept::pause_after_error(&e, "admin").await,
        }
    }
}

/// One connection: handshake, identify the caller, read the request, authorize, answer,
/// audit, close.
async fn handle(stream: TcpStream, acceptor: TlsAcceptor, state: AdminState) {
    let read = tokio::time::timeout(http::REQUEST_DEADLINE, async {
        let mut tls = acceptor.accept(stream).await.ok()?;
        // The caller is known from the handshake, before the body is read: a large body is
        // read only for a route this caller's role may call (ADR 0084).
        let caller = {
            let chain = tls.get_ref().1.peer_certificates().unwrap_or_default();
            state.caller(chain)
        };
        let request = http::read_request(&mut tls, |method, path| {
            routes::body_limit(caller.role, method, path)
        })
        .await;
        Some((tls, caller, request))
    })
    .await;
    let Ok(Some((mut tls, caller, request))) = read else {
        debug!("admin connection closed before a complete request");
        return;
    };
    let (status, body, target) = match request {
        Err(e) => {
            let (status, code) = match e {
                http::ReadError::TooLarge => (413, "too-large"),
                http::ReadError::Malformed | http::ReadError::Closed => (400, "bad-request"),
            };
            let (status, body) = routes::error(status, code, "the request could not be read");
            (status, body, String::from("-"))
        }
        Ok(request) => {
            let target = audit_target(&request);
            let (status, body) = match caller.role {
                None => routes::error(
                    403,
                    "forbidden",
                    "this certificate's subject is not listed in admin.viewers or admin.operators",
                ),
                Some(role) => {
                    match tokio::time::timeout(
                        HANDLER_DEADLINE,
                        routes::route(&state, &caller, role, &request),
                    )
                    .await
                    {
                        Ok(answer) => answer,
                        Err(_) => routes::error(503, "timeout", "the request took too long"),
                    }
                }
            };
            (status, body, target)
        }
    };
    state.audit.record(
        "admin.request",
        Some(&caller.subject),
        &format!(
            "role={} {target} -> {status}",
            caller.role.map_or("none", Role::as_str)
        ),
    );
    if let Err(e) = http::write_response(&mut tls, status, &body).await {
        debug!(error = %e, "admin response write failed");
    }
}

/// `METHOD /path?query` for the audit record. The path and every query component are
/// re-encoded after decoding, so a `%0A` in the request cannot split the record: the audit
/// line stays one line whatever the client sent.
fn audit_target(request: &http::Request) -> String {
    let path: Vec<String> = request.path.split('/').map(http::percent_encode).collect();
    let mut target = format!("{} {}", request.method, path.join("/"));
    for (i, (k, v)) in request.query.iter().enumerate() {
        target.push(if i == 0 { '?' } else { '&' });
        target.push_str(&http::percent_encode(k));
        target.push('=');
        target.push_str(&http::percent_encode(v));
    }
    target
}
