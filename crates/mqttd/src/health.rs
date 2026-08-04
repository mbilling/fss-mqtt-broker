//! Kubernetes-style health endpoints over a tiny hand-rolled HTTP/1.1 server.
//!
//! Orchestrators (k8s liveness/readiness probes, load-balancer health checks) need
//! an HTTP signal the MQTT protocol does not provide. This module serves two:
//!
//! - **`GET /livez`** (alias `GET /healthz`) — **liveness**: the process is up and
//!   the hub actor loop is draining commands (it answers a [`HubCommand::Ping`]
//!   within [`LIVE_TIMEOUT`]). A failing `/livez` means the broker is wedged and
//!   should be restarted.
//! - **`GET /readyz`** — **readiness**: it is safe to send this node client traffic.
//!   Ready = live **and** the mesh has at least `min_members` members **and**, when
//!   durable sessions are enabled, the lease group is ready
//!   ([`DurablePlane::lease_group_ready`]) — there is a leader and this node is a
//!   voter, so it can durably own the sessions it would be handed. A failing
//!   `/readyz` should pull the node from the Service endpoints **without** killing it
//!   (correct during a rolling restart or a transient lease blip).
//!
//! No HTTP framework is pulled in: the server parses the request line, routes on the
//! path, and writes a small JSON body with `Connection: close`. It is deliberately
//! minimal and strictly **read-only** (`GET`/`HEAD` only). `/readyz` carries the
//! liveness/readiness booleans, member count, lease-group-ready flag, and (ADR 0049)
//! durable-serviceability detail — voter count and quorum-ack age — without those
//! details ever changing the ready/NotReady status code. **`GET /statusz`**
//! (ADR 0054) is the wider, structured operator-facing state body: identity,
//! membership view, lease detail, decommission, brownout, store sizes, and protocol
//! range — everything a controller (or a human with `curl`) needs to act, none of it
//! secret material, all of it on the same unauthenticated ops-network trust model as
//! the probes (ADR 0020 §2).

use crate::hub::HubCommand;
use mqtt_cluster::decommission::DrainStatus;
use mqtt_cluster::durable_plane::DurablePlane;
use mqtt_cluster::placement::Placement;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

/// How long the liveness probe waits for the hub to answer its ping before
/// reporting the broker wedged.
const LIVE_TIMEOUT: Duration = Duration::from_secs(2);

/// Shared brownout state (ADR 0054): flipped by the hub on
/// [`HubCommand::SetBrownout`] transitions, read by the `/statusz` body. The
/// rejection counters record symptoms; this is the condition itself.
#[derive(Debug, Default)]
pub struct BrownoutStatus {
    on: std::sync::atomic::AtomicBool,
    since_unix: std::sync::atomic::AtomicU64,
}

impl BrownoutStatus {
    /// Record a brownout transition (`on` = entered, `!on` = lifted).
    pub fn set(&self, on: bool) {
        use std::sync::atomic::Ordering::Relaxed;
        self.on.store(on, Relaxed);
        let since = if on {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs())
        } else {
            0
        };
        self.since_unix.store(since, Relaxed);
    }

    /// `(active, since-unix-seconds)`; `since` is `None` when not browned out.
    #[must_use]
    pub fn read(&self) -> (bool, Option<u64>) {
        use std::sync::atomic::Ordering::Relaxed;
        let on = self.on.load(Relaxed);
        let since = self.since_unix.load(Relaxed);
        (on, (on && since > 0).then_some(since))
    }
}

/// Cap on the bytes read while looking for the request line — a crude guard against
/// a client that streams forever without sending `\r\n`.
const MAX_REQUEST_LINE: usize = 8 * 1024;

/// What the health server needs to answer probes: a handle to ping the hub, and the
/// readiness inputs (live membership and, when durable, the lease-group endpoint).
#[derive(Clone)]
pub struct HealthState {
    hub: mpsc::UnboundedSender<HubCommand>,
    placement: Option<Arc<RwLock<Placement>>>,
    durable: Option<DurablePlane>,
    min_members: usize,
    /// Set on graceful shutdown (ADR 0019): `/readyz` reports not-ready while draining
    /// so orchestrators stop routing new traffic, but `/livez` stays up so we are not
    /// killed mid-drain.
    draining: Arc<std::sync::atomic::AtomicBool>,
    /// Set when a decommission drain starts (ADR 0043 P3): `/readyz` then reports
    /// the drain's progress — pending hand-offs, rounds, completion — so an
    /// operator can watch the departure instead of guessing.
    decommission: Arc<OnceLock<Arc<DrainStatus>>>,
    /// When set, `GET /metrics` serves Prometheus exposition (ADR 0020); otherwise it 404s.
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    /// Identity block for `/statusz` (ADR 0054): `(node id, founder flag)`.
    /// `None` = `/statusz` 404s (the route is opt-in via [`with_status`]).
    identity: Option<(String, bool)>,
    /// Brownout state for `/statusz` (ADR 0054).
    brownout: Option<Arc<BrownoutStatus>>,
    /// Store-size snapshot for `/statusz` (ADR 0054).
    stores: Option<Arc<crate::store_watch::StoreSnapshot>>,
}

impl std::fmt::Debug for HealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthState")
            .field("min_members", &self.min_members)
            .field("durable", &self.durable.is_some())
            .finish_non_exhaustive()
    }
}

/// A readiness snapshot, serialized into the `/readyz` JSON body.
struct Report {
    live: bool,
    ready: bool,
    members: Option<usize>,
    lease_group_ready: Option<bool>,
    /// `(pending hand-offs, rounds, complete)` when a decommission drain is
    /// active (ADR 0043 P3).
    decommission: Option<(usize, u64, bool)>,
    /// Durable-serviceability signals (ADR 0049), reported in the body so an operator
    /// probing a suspect node can see a *green-but-degraded* durable plane — without
    /// changing the ready/NotReady contract the plain `/readyz` status code carries.
    /// `voters`: current lease-group voter count; `quorum_ack_age_ms`: ms since the
    /// leader last had a quorum ack (a growing value is the fsync-bound degradation
    /// behind the 2026-07-14 incident).
    voters: Option<usize>,
    quorum_ack_age_ms: Option<u64>,
}

impl Report {
    fn to_json(&self) -> String {
        use std::fmt::Write;
        let status = if self.ready { "ok" } else { "unavailable" };
        let mut s = format!(
            "{{\"status\":\"{status}\",\"live\":{},\"ready\":{}",
            self.live, self.ready
        );
        if let Some(m) = self.members {
            let _ = write!(s, ",\"members\":{m}");
        }
        if let Some(l) = self.lease_group_ready {
            let _ = write!(s, ",\"lease_group_ready\":{l}");
        }
        if let Some((pending, rounds, complete)) = self.decommission {
            let _ = write!(
                s,
                ",\"decommission\":{{\"pending\":{pending},\"rounds\":{rounds},\"complete\":{complete}}}"
            );
        }
        if let Some(v) = self.voters {
            let _ = write!(s, ",\"voters\":{v}");
        }
        if let Some(ms) = self.quorum_ack_age_ms {
            let _ = write!(s, ",\"quorum_ack_age_ms\":{ms}");
        }
        s.push('}');
        s
    }
}

impl HealthState {
    /// Build the health state. `placement` (when clustered) supplies the member
    /// count; `durable` (when durable sessions are on) supplies lease-group
    /// readiness; `min_members` is the smallest mesh size `/readyz` accepts (1 = no
    /// membership gate).
    #[must_use]
    pub fn new(
        hub: mpsc::UnboundedSender<HubCommand>,
        placement: Option<Arc<RwLock<Placement>>>,
        durable: Option<DurablePlane>,
        min_members: usize,
    ) -> Self {
        Self {
            hub,
            placement,
            durable,
            min_members,
            draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            decommission: Arc::new(OnceLock::new()),
            metrics: None,
            identity: None,
            brownout: None,
            stores: None,
        }
    }

    /// Enable `GET /statusz` (ADR 0054): the structured operator-facing state body.
    /// `node_id`/`founder` form the identity block; `brownout` and `stores` are the
    /// shared snapshots the hub and store watcher keep current.
    #[must_use]
    pub fn with_status(
        mut self,
        node_id: String,
        founder: bool,
        brownout: Arc<BrownoutStatus>,
        stores: Option<Arc<crate::store_watch::StoreSnapshot>>,
    ) -> Self {
        self.identity = Some((node_id, founder));
        self.brownout = Some(brownout);
        self.stores = stores;
        self
    }

    /// Serve Prometheus metrics on `GET /metrics` from this health server (ADR 0020).
    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<mqtt_observability::metrics::Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// A handle to this state's draining flag (ADR 0019). Setting it makes `/readyz`
    /// report not-ready (while `/livez` stays up), so an orchestrator drains traffic.
    #[must_use]
    pub fn draining_handle(&self) -> Arc<std::sync::atomic::AtomicBool> {
        self.draining.clone()
    }

    /// The slot a starting decommission drain (ADR 0043 P3) publishes its
    /// [`DrainStatus`] into, making the drain's progress visible on `/readyz`.
    #[must_use]
    pub fn decommission_slot(&self) -> Arc<OnceLock<Arc<DrainStatus>>> {
        self.decommission.clone()
    }

    /// Whether the hub actor loop is draining: it answers a ping within
    /// [`LIVE_TIMEOUT`]. A closed channel (hub gone) or a timeout reads as not-live.
    async fn live(&self) -> bool {
        let (tx, rx) = oneshot::channel();
        if self.hub.send(HubCommand::Ping { reply: tx }).is_err() {
            return false;
        }
        matches!(tokio::time::timeout(LIVE_TIMEOUT, rx).await, Ok(Ok(())))
    }

    /// The current member count, if this node is part of a cluster.
    fn member_count(&self) -> Option<usize> {
        self.placement.as_ref().map(|p| {
            p.read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .member_count()
        })
    }

    /// Evaluate readiness: live, mesh at/above `min_members`, and (when durable) the
    /// lease group ready.
    async fn readiness(&self) -> Report {
        let live = self.live().await;
        let members = self.member_count();
        let lease_group_ready = self.durable.as_ref().map(DurablePlane::lease_group_ready);
        let draining = self.draining.load(std::sync::atomic::Ordering::Acquire);
        let ready = !draining
            && live
            && members.is_none_or(|n| n >= self.min_members)
            && lease_group_ready.unwrap_or(true);
        let decommission = self.decommission.get().map(|d| {
            use std::sync::atomic::Ordering::Acquire;
            (
                d.pending.load(Acquire),
                d.rounds.load(Acquire),
                d.complete.load(Acquire),
            )
        });
        // Durable-serviceability detail (ADR 0049): voter count + quorum-ack age, so a
        // degraded-but-green durable plane is visible in the body (status code unchanged).
        let voters = self.durable.as_ref().map(DurablePlane::voter_count);
        let quorum_ack_age_ms = self
            .durable
            .as_ref()
            .and_then(DurablePlane::quorum_ack_age_ms);
        Report {
            live,
            ready,
            members,
            lease_group_ready,
            decommission,
            voters,
            quorum_ack_age_ms,
        }
    }
}

/// Minimal JSON string escaping for operator-supplied values (node ids,
/// addresses, domains): quotes, backslashes, and control bytes.
fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => {
                use std::fmt::Write;
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out
}

impl HealthState {
    /// Build the `/statusz` body (ADR 0054): the structured, read-only,
    /// operator-facing state of this node. Unbounded detail (member lists, voter
    /// ids) lives HERE, never in metric labels — the ADR 0020 cardinality rule.
    /// Every value is broker state an ops-network reader may see; no secret
    /// material is ever included.
    async fn statusz(&self) -> Option<String> {
        use std::fmt::Write;
        let (node_id, founder) = self.identity.as_ref()?;
        let report = self.readiness().await;
        let mut s = format!(
            "{{\"node_id\":\"{}\",\"version\":\"{}\",\"founder\":{founder},\"ready\":{},\"live\":{}",
            json_escape(node_id),
            env!("CARGO_PKG_VERSION"),
            report.ready,
            report.live,
        );
        // Membership: the placement view (self + non-dead peers). Deterministic order.
        if let Some(p) = &self.placement {
            let members = p
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .members_snapshot();
            s.push_str(",\"members\":[");
            for (i, (id, addr, domain)) in members.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                let _ = write!(s, "{{\"id\":\"{}\"", json_escape(&id.0));
                if let Some(a) = addr {
                    let _ = write!(s, ",\"addr\":\"{}\"", json_escape(a));
                }
                if let Some(d) = domain {
                    let _ = write!(s, ",\"failure_domain\":\"{}\"", json_escape(d));
                }
                s.push('}');
            }
            s.push(']');
        }
        // Lease group (durable mode): role, epoch, voter identities, readiness.
        if let Some(plane) = &self.durable {
            let (is_leader, epoch) = plane.lease_role();
            let _ = write!(
                s,
                ",\"lease\":{{\"leader\":{is_leader},\"epoch\":{epoch},\"group_ready\":{},\"voters\":[",
                plane.lease_group_ready()
            );
            for (i, v) in plane.voter_ids().iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                let _ = write!(s, "\"{}\"", json_escape(&v.0));
            }
            s.push(']');
            if let Some(ms) = plane.quorum_ack_age_ms() {
                let _ = write!(s, ",\"quorum_ack_age_ms\":{ms}");
            }
            let (current, tracked) = plane.caught_up_summary();
            let _ = write!(
                s,
                ",\"replica_groups\":{{\"current\":{current},\"tracked\":{tracked}}}}}"
            );
        }
        // Decommission drain (ADR 0043), including the previously unsurfaced
        // `active` flag — an operator distinguishes a draining shrink from a roll.
        if let Some(d) = self.decommission.get() {
            use std::sync::atomic::Ordering::Acquire;
            let _ = write!(
                s,
                ",\"decommission\":{{\"active\":{},\"pending\":{},\"rounds\":{},\"complete\":{}}}",
                d.active.load(Acquire),
                d.pending.load(Acquire),
                d.rounds.load(Acquire),
                d.complete.load(Acquire)
            );
        }
        // Brownout state (ADR 0041 §5): the condition, not the symptoms.
        if let Some(b) = &self.brownout {
            let (on, since) = b.read();
            let _ = write!(s, ",\"brownout\":{{\"disk\":{on}");
            if let Some(t) = since {
                let _ = write!(s, ",\"since_unix\":{t}");
            }
            s.push('}');
        }
        // Store sizes + watermark, from the store watcher's shared snapshot.
        if let Some(stores) = &self.stores {
            let (sizes, max) = stores.read();
            s.push_str(",\"store\":{");
            for (i, (name, bytes)) in sizes.iter().enumerate() {
                if i > 0 {
                    s.push(',');
                }
                let _ = write!(s, "\"{name}\":{bytes}");
            }
            if let Some(max) = max {
                let _ = write!(s, ",\"max_bytes\":{max}");
            }
            s.push('}');
        }
        // Peer-bus protocol range: a mixed-version fleet is visible per node.
        let _ = write!(
            s,
            ",\"proto\":{{\"min\":{},\"max\":{}}}",
            mqtt_cluster::peer::PROTO_MIN,
            mqtt_cluster::peer::PROTO_MAX
        );
        s.push('}');
        Some(s)
    }
}

/// Serve health endpoints on `listener` until it errors. Each connection is handled
/// off the accept loop so a slow client cannot stall probes.
pub async fn serve(listener: TcpListener, state: HealthState) {
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let state = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle(stream, state).await {
                        debug!(error = %e, "health connection ended with an error");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "health listener accept failed");
                return;
            }
        }
    }
}

/// Read one request, route it, write the response, close.
async fn handle(mut stream: TcpStream, state: HealthState) -> std::io::Result<()> {
    let target = read_request_target(&mut stream).await?;
    let (status, body, content_type) = match target {
        Some(path) => route(&state, &path).await,
        None => (400, "{\"error\":\"bad request\"}".to_string(), JSON),
    };
    write_response(&mut stream, status, &body, content_type).await
}

/// JSON content type for the health endpoints.
const JSON: &str = "application/json";
/// `OpenMetrics` content type for `/metrics` (the format `prometheus-client` emits).
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// Map a request path to an HTTP status, body, and content type.
async fn route(state: &HealthState, path: &str) -> (u16, String, &'static str) {
    match path {
        "/livez" | "/healthz" => {
            if state.live().await {
                (200, "{\"status\":\"ok\",\"live\":true}".to_string(), JSON)
            } else {
                (
                    503,
                    "{\"status\":\"unavailable\",\"live\":false}".to_string(),
                    JSON,
                )
            }
        }
        "/readyz" => {
            let report = state.readiness().await;
            (if report.ready { 200 } else { 503 }, report.to_json(), JSON)
        }
        // Metrics exposition (ADR 0020); 404 when metrics are not enabled.
        "/metrics" => match &state.metrics {
            Some(m) => (200, m.render(), OPENMETRICS),
            None => (404, "{\"error\":\"not found\"}".to_string(), JSON),
        },
        // Structured operator-facing state (ADR 0054); 404 until wired via
        // `with_status`. Always 200 when wired — /statusz reports state, only
        // /readyz carries the ready/NotReady contract in its status code.
        "/statusz" => match state.statusz().await {
            Some(body) => (200, body, JSON),
            None => (404, "{\"error\":\"not found\"}".to_string(), JSON),
        },
        _ => (404, "{\"error\":\"not found\"}".to_string(), JSON),
    }
}

/// Read the request line and return its path (query string stripped). `None` for a
/// malformed line, a non-`GET`/`HEAD` method, a connection that closes first, or an
/// over-long line.
async fn read_request_target(stream: &mut TcpStream) -> std::io::Result<Option<String>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 256];
    loop {
        if let Some(pos) = buf.windows(2).position(|w| w == b"\r\n") {
            return Ok(parse_request_line(&buf[..pos]));
        }
        if buf.len() > MAX_REQUEST_LINE {
            return Ok(None);
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(None); // closed before a full request line
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// Parse `GET /path?query HTTP/1.1` into its path, accepting only `GET`/`HEAD`.
fn parse_request_line(line: &[u8]) -> Option<String> {
    let line = std::str::from_utf8(line).ok()?;
    let mut parts = line.split(' ');
    let method = parts.next()?;
    let target = parts.next()?;
    if method != "GET" && method != "HEAD" {
        return None;
    }
    let path = target.split('?').next().unwrap_or(target);
    Some(path.to_string())
}

/// Write a minimal HTTP/1.1 response with a JSON body and `Connection: close`.
async fn write_response(
    stream: &mut TcpStream,
    status: u16,
    body: &str,
    content_type: &str,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        503 => "Service Unavailable",
        _ => "",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await
}

#[cfg(test)]
mod tests {
    use super::{parse_request_line, HealthState, LIVE_TIMEOUT};
    use crate::hub::HubCommand;
    use mqtt_cluster::placement::{Placement, DEFAULT_REPLICAS};
    use mqtt_cluster::NodeId;
    use std::sync::{Arc, RwLock};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    /// A hub stand-in that answers pings, so `live()` succeeds.
    fn spawn_live_hub() -> mpsc::UnboundedSender<HubCommand> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                if let HubCommand::Ping { reply } = cmd {
                    let _ = reply.send(());
                }
            }
        });
        tx
    }

    fn placement(members: usize) -> Arc<RwLock<Placement>> {
        let mut p = Placement::new(NodeId("self".into()), DEFAULT_REPLICAS);
        for i in 1..members {
            p.observe(
                &NodeId(format!("peer-{i}")),
                mqtt_cluster::swim::MemberState::Alive,
                &format!("peer-{i}:7000"),
                None,
            );
        }
        Arc::new(RwLock::new(p))
    }

    #[test]
    fn parses_get_and_head_with_query_strings() {
        assert_eq!(
            parse_request_line(b"GET /readyz HTTP/1.1").as_deref(),
            Some("/readyz")
        );
        assert_eq!(
            parse_request_line(b"HEAD /livez HTTP/1.1").as_deref(),
            Some("/livez")
        );
        assert_eq!(
            parse_request_line(b"GET /readyz?verbose=1 HTTP/1.1").as_deref(),
            Some("/readyz")
        );
        // A write method is refused (these endpoints are read-only).
        assert_eq!(parse_request_line(b"POST /readyz HTTP/1.1"), None);
        assert_eq!(parse_request_line(b"garbage"), None);
    }

    #[tokio::test]
    async fn livez_is_ok_when_the_hub_answers_and_unavailable_when_it_is_gone() {
        let live = HealthState::new(spawn_live_hub(), None, None, 1);
        assert_eq!(super::route(&live, "/livez").await.0, 200);
        assert_eq!(super::route(&live, "/healthz").await.0, 200);

        // A hub whose receiver is dropped is not live.
        let (tx, rx) = mpsc::unbounded_channel();
        drop(rx);
        let dead = HealthState::new(tx, None, None, 1);
        assert_eq!(super::route(&dead, "/livez").await.0, 503);
    }

    #[tokio::test]
    async fn readyz_gates_on_min_members() {
        // A single-member mesh with min_members=1 is ready; raising the floor to 2
        // makes it not-ready until a peer is seen.
        let ready = HealthState::new(spawn_live_hub(), Some(placement(1)), None, 1);
        assert_eq!(super::route(&ready, "/readyz").await.0, 200);

        let waiting = HealthState::new(spawn_live_hub(), Some(placement(1)), None, 2);
        let (status, body, _) = super::route(&waiting, "/readyz").await;
        assert_eq!(status, 503);
        assert!(body.contains("\"ready\":false"));
        assert!(body.contains("\"members\":1"));

        let quorate = HealthState::new(spawn_live_hub(), Some(placement(2)), None, 2);
        assert_eq!(super::route(&quorate, "/readyz").await.0, 200);
    }

    #[tokio::test]
    async fn unknown_paths_are_404() {
        let state = HealthState::new(spawn_live_hub(), None, None, 1);
        assert_eq!(super::route(&state, "/").await.0, 404);
        // /metrics 404s when metrics are not enabled (no `with_metrics`).
        assert_eq!(super::route(&state, "/metrics").await.0, 404);
        // /statusz 404s until wired via with_status (ADR 0054).
        assert_eq!(super::route(&state, "/statusz").await.0, 404);
    }

    /// ADR 0054: `/statusz` reports the structured operator-facing state —
    /// identity, membership view, brownout state (with a since-timestamp while
    /// active), and the peer protocol range — and always answers 200 (only
    /// `/readyz` carries readiness in its status code).
    #[tokio::test]
    async fn statusz_reports_identity_members_brownout_and_proto() {
        let brownout = Arc::new(super::BrownoutStatus::default());
        let state = HealthState::new(spawn_live_hub(), Some(placement(2)), None, 1).with_status(
            "node-a".into(),
            true,
            brownout.clone(),
            None,
        );

        let (status, body, _) = super::route(&state, "/statusz").await;
        assert_eq!(status, 200);
        assert!(body.contains("\"node_id\":\"node-a\""), "{body}");
        assert!(body.contains("\"founder\":true"), "{body}");
        assert!(body.contains("\"members\":["), "{body}");
        assert!(body.contains("\"id\":\"peer-1\""), "{body}");
        assert!(body.contains("\"brownout\":{\"disk\":false}"), "{body}");
        assert!(
            body.contains(&format!(
                "\"proto\":{{\"min\":{},\"max\":{}}}",
                mqtt_cluster::peer::PROTO_MIN,
                mqtt_cluster::peer::PROTO_MAX
            )),
            "{body}"
        );

        // A brownout transition is visible as STATE, with its onset timestamp.
        brownout.set(true);
        let (status, body, _) = super::route(&state, "/statusz").await;
        assert_eq!(status, 200, "statusz reports state; it never turns 503");
        assert!(body.contains("\"brownout\":{\"disk\":true,\"since_unix\":"), "{body}");
        brownout.set(false);
        let (_, body, _) = super::route(&state, "/statusz").await;
        assert!(body.contains("\"brownout\":{\"disk\":false}"), "{body}");
    }

    /// Operator-supplied strings (node ids) are JSON-escaped in the body.
    #[test]
    fn json_escape_handles_quotes_backslashes_and_control_bytes() {
        assert_eq!(super::json_escape("plain-id"), "plain-id");
        assert_eq!(super::json_escape("a\"b\\c"), "a\\\"b\\\\c");
        assert_eq!(super::json_escape("nl\n"), "nl\\u000a");
    }

    #[tokio::test]
    async fn metrics_endpoint_serves_exposition_when_enabled() {
        let state = HealthState::new(spawn_live_hub(), None, None, 1)
            .with_metrics(Arc::new(mqtt_observability::metrics::Metrics::new("test")));
        let (status, body, content_type) = super::route(&state, "/metrics").await;
        assert_eq!(status, 200);
        assert!(
            body.contains("mqttd_build_info{version=\"test\"}"),
            "{body}"
        );
        assert!(content_type.contains("openmetrics-text"));
    }

    /// End to end over a real TCP socket: a GET to /livez returns a 200 with a JSON
    /// body — the hand-rolled HTTP path works against a real client.
    #[tokio::test]
    async fn serves_a_real_http_request_over_tcp() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = HealthState::new(spawn_live_hub(), Some(placement(1)), None, 1);
        tokio::spawn(super::serve(listener, state));

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /readyz HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        // Read to EOF (the server sends Connection: close).
        tokio::time::timeout(
            LIVE_TIMEOUT + Duration::from_secs(1),
            stream.read_to_end(&mut response),
        )
        .await
        .expect("response within timeout")
        .unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "got: {response}");
        assert!(response.contains("\"status\":\"ok\""));
        assert!(response.contains("\"members\":1"));
    }
}
