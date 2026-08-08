//! The bridge runtime — connect every side, subscribe per the rules, and route
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §3, T3).
//!
//! One supervised connection per side (the local cluster + each upstream): connect, issue
//! the side's subscriptions, pump it ([`MqttClient::run`]), and on any disconnect reconnect
//! with bounded backoff. Inbound publishes from every side funnel into one **router** task
//! that applies the pure [`crate::forward`] policy and pushes the resulting publishes to the
//! destination sides — so all the security-relevant decisions live in the tested pure core,
//! and this layer is just plumbing.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use mqtt_codec::packet::Publish;
use mqtt_codec::{ProtocolVersion, QoS};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::client::{Command, ConnectOptions, MqttClient, Transport};
use crate::config::{BridgeConfig, Endpoint};
use crate::forward::{plan_forwards, read_hop_count, set_hop_count, Side};
use crate::metrics::{BridgeMetrics, CrossDirection};
use crate::spool::{Spool, SpooledMessage};

/// A fixed keepalive (seconds) for every bridge connection; the client pings at half this.
const KEEP_ALIVE: u16 = 30;
/// Reconnect backoff bounds.
const BACKOFF_START: Duration = Duration::from_millis(200);
const BACKOFF_MAX: Duration = Duration::from_secs(10);

/// A running bridge. Dropping it (or calling [`Bridge::shutdown`]) stops every connection.
#[derive(Debug)]
pub struct Bridge {
    tasks: Vec<JoinHandle<()>>,
    metrics: Arc<BridgeMetrics>,
}

impl Bridge {
    /// Start the bridge from a validated configuration: spawn a supervised connection per
    /// side and the router. Returns immediately; the connections come up in the background.
    #[must_use]
    pub fn start(cfg: BridgeConfig) -> Self {
        let cfg = Arc::new(cfg);
        // Register the label values up front, from config: metric cardinality is then
        // bounded by the configuration rather than by traffic.
        let upstream_names: Vec<String> = cfg.upstreams.iter().map(|u| u.name.clone()).collect();
        let metrics = Arc::new(BridgeMetrics::for_upstreams(&upstream_names));
        let n_sides = 1 + cfg.upstreams.len();

        // One command channel per side (index 0 = local, i+1 = upstream i). The router keeps
        // every sender to dispatch forwards; each supervisor owns its receiver + a sender
        // clone (to inject its own (re)subscribes after each connect).
        let mut cmd_tx = Vec::with_capacity(n_sides);
        let mut cmd_rx = Vec::with_capacity(n_sides);
        for _ in 0..n_sides {
            let (tx, rx) = mpsc::unbounded_channel::<Command>();
            cmd_tx.push(tx);
            cmd_rx.push(rx);
        }

        // One inbound channel into the router, tagged with the originating side.
        let (in_tx, in_rx) = mpsc::unbounded_channel::<(Side, Publish)>();

        // Per-side store-and-forward spool + a connected flag the router consults: a forward
        // to a connected side goes straight to its channel; a forward to a disconnected side
        // is spooled (bounded) and replayed when the supervisor reconnects (§7).
        let mut spools = Vec::with_capacity(n_sides);
        let mut connected = Vec::with_capacity(n_sides);
        spools.push(build_spool(&cfg, Side::Local));
        connected.push(Arc::new(AtomicBool::new(false)));
        for i in 0..cfg.upstreams.len() {
            spools.push(build_spool(&cfg, Side::Upstream(i)));
            connected.push(Arc::new(AtomicBool::new(false)));
        }

        let mut tasks = Vec::new();

        // Local supervisor (side index 0).
        tasks.push(spawn_supervisor(
            Side::Local,
            cfg.clone(),
            cmd_rx.remove(0),
            cmd_tx[0].clone(),
            in_tx.clone(),
            metrics.clone(),
            spools[0].clone(),
            connected[0].clone(),
        ));
        // Upstream supervisors. `cmd_rx.remove(0)` keeps popping the front as sides advance.
        for i in 0..cfg.upstreams.len() {
            tasks.push(spawn_supervisor(
                Side::Upstream(i),
                cfg.clone(),
                cmd_rx.remove(0),
                cmd_tx[i + 1].clone(),
                in_tx.clone(),
                metrics.clone(),
                spools[i + 1].clone(),
                connected[i + 1].clone(),
            ));
        }

        // Publish the live per-side state the gauges read. These are clones of the very
        // flags and spools the engine uses, so `fss_bridge_connected` and
        // `fss_bridge_spool_depth` cannot drift from the state they describe.
        let mut side_names = Vec::with_capacity(n_sides);
        side_names.push("local".to_string());
        for u in &cfg.upstreams {
            side_names.push(u.name.clone());
        }
        metrics.register_sides(side_names, connected.clone(), spools.clone());

        tasks.push(spawn_router(
            cfg,
            cmd_tx,
            in_rx,
            metrics.clone(),
            spools,
            connected,
        ));
        Self { tasks, metrics }
    }

    /// The bridge's metrics handle (forwarded/dropped/reconnect counters, Prometheus text).
    #[must_use]
    pub fn metrics(&self) -> Arc<BridgeMetrics> {
        self.metrics.clone()
    }

    /// Stop every connection and the router.
    pub fn shutdown(self) {
        for t in self.tasks {
            t.abort();
        }
    }
}

/// The per-side connection supervisor: (re)connect, subscribe, pump, backoff.
#[allow(clippy::too_many_arguments)] // a supervisor wires together one side's channels + state
fn spawn_supervisor(
    side: Side,
    cfg: Arc<BridgeConfig>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    self_tx: mpsc::UnboundedSender<Command>,
    inbound_central: mpsc::UnboundedSender<(Side, Publish)>,
    metrics: Arc<BridgeMetrics>,
    spool: Arc<Spool>,
    connected: Arc<AtomicBool>,
) -> JoinHandle<()> {
    // Tag each inbound PUBLISH with this side before it reaches the router.
    let (in_tx, mut in_rx) = mpsc::unbounded_channel::<Publish>();
    {
        let central = inbound_central;
        tokio::spawn(async move {
            while let Some(p) = in_rx.recv().await {
                if central.send((side, p)).is_err() {
                    break;
                }
            }
        });
    }

    tokio::spawn(async move {
        let endpoint = endpoint_for(&cfg, side);
        let subs = subscriptions_for(&cfg, side);
        let default_id = default_client_id(side);
        // The local side uses a **persistent** session (§5): a brief bridge restart resumes
        // its shared subscription and buffered messages. Upstreams stay clean.
        let persistent = matches!(side, Side::Local);
        let mut backoff = BACKOFF_START;
        loop {
            let opts = match connect_options(endpoint, &default_id, persistent) {
                Ok(o) => o,
                Err(e) => {
                    warn!(?side, error = %e, "bridge endpoint misconfigured; not connecting");
                    return; // a config error will not fix itself by retrying
                }
            };
            match MqttClient::connect(&opts).await {
                Ok(client) => {
                    backoff = BACKOFF_START;
                    metrics.reconnect(side_index(side));
                    info!(?side, addr = %opts.addr, subs = subs.len(), "bridge side connected");
                    // (Re)subscribe for this side's open direction(s) only.
                    let mut pkid: u16 = 1;
                    for (filter, qos) in &subs {
                        let _ = self_tx.send(Command::Subscribe {
                            pkid,
                            filter: filter.clone(),
                            qos: qos_from_u8(*qos),
                        });
                        pkid = pkid.wrapping_add(1).max(1);
                    }
                    // Replay anything spooled while this side was down (§7), oldest first,
                    // before opening the live fast path so order is preserved.
                    replay_spool(&spool, &self_tx, side);
                    connected.store(true, Ordering::SeqCst);
                    let err = client.run(&mut commands, &in_tx, KEEP_ALIVE).await;
                    connected.store(false, Ordering::SeqCst);
                    warn!(?side, error = %err, "bridge side disconnected; reconnecting");
                }
                Err(e) => warn!(?side, addr = %opts.addr, error = %e, "bridge connect failed"),
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(BACKOFF_MAX);
        }
    })
}

/// The router: apply the pure forwarding policy to each inbound message and dispatch.
fn spawn_router(
    cfg: Arc<BridgeConfig>,
    cmd_tx: Vec<mpsc::UnboundedSender<Command>>,
    mut inbound: mpsc::UnboundedReceiver<(Side, Publish)>,
    metrics: Arc<BridgeMetrics>,
    spools: Vec<Arc<Spool>>,
    connected: Vec<Arc<AtomicBool>>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while let Some((side, publish)) = inbound.recv().await {
            let hop = read_hop_count(&publish.properties);
            let forwards = plan_forwards(&cfg, side, &publish.topic, hop);
            if forwards.is_empty() && hop >= cfg.hop_count_limit {
                metrics.dropped_hop_limit();
                debug!(?side, topic = %publish.topic, hop, "hop limit reached; dropped");
            }
            for f in forwards {
                let idx = side_index(f.dest);
                // Record + audit the crossing (the upstream involved is the non-local side).
                let (dir, up_name) = match side {
                    Side::Local => (CrossDirection::Out, upstream_name(&cfg, f.dest)),
                    Side::Upstream(_) => (CrossDirection::In, upstream_name(&cfg, side)),
                };
                metrics.forwarded(
                    up_name,
                    dir,
                    &publish.topic,
                    &f.topic,
                    publish.payload.len(),
                );
                // Forward the publisher's user properties, with the hop count incremented.
                let properties = set_hop_count(&publish.properties, hop + 1);
                if connected[idx].load(Ordering::SeqCst) {
                    // Fast path: the destination is up — the run loop assigns the packet id.
                    let _ = cmd_tx[idx].send(Command::Publish {
                        topic: f.topic,
                        payload: publish.payload.clone(),
                        qos: qos_from_u8(f.qos),
                        pkid: None,
                        properties,
                    });
                } else {
                    // The destination is down: spool it (bounded) for replay on reconnect (§7).
                    let user_properties = properties
                        .0
                        .iter()
                        .filter_map(|p| match p {
                            mqtt_codec::Property::UserProperty(k, v) => {
                                Some((k.clone(), v.clone()))
                            }
                            _ => None,
                        })
                        .collect();
                    let spooled = SpooledMessage {
                        topic: f.topic,
                        payload: publish.payload.to_vec(),
                        qos: f.qos,
                        user_properties,
                    };
                    if let Err(e) = spools[idx].push(&spooled) {
                        warn!(dest = idx, error = %e, "failed to spool a message for a down side");
                    }
                }
            }
        }
    })
}

/// Build a side's spool: disk-backed under `spool.dir` when configured (so it survives a
/// bridge restart), else in-memory — both bounded to `spool.max_messages` (§7).
fn build_spool(cfg: &BridgeConfig, side: Side) -> Arc<Spool> {
    let cap = cfg.spool.max_messages;
    match &cfg.spool.dir {
        Some(dir) => {
            let file = match side {
                Side::Local => "spool-local.redb".to_string(),
                Side::Upstream(i) => format!("spool-upstream-{i}.redb"),
            };
            let path = std::path::Path::new(dir).join(file);
            match Spool::on_disk(&path, cap) {
                Ok(s) => Arc::new(s),
                Err(e) => {
                    warn!(?side, error = %e, "disk spool unavailable; using an in-memory spool");
                    Arc::new(Spool::in_memory(cap))
                }
            }
        }
        None => Arc::new(Spool::in_memory(cap)),
    }
}

/// Replay every spooled message for a side onto its command channel, oldest first.
fn replay_spool(spool: &Spool, self_tx: &mpsc::UnboundedSender<Command>, side: Side) {
    match spool.drain() {
        Ok(msgs) => {
            if !msgs.is_empty() {
                info!(
                    ?side,
                    count = msgs.len(),
                    "replaying spooled messages on reconnect"
                );
            }
            for m in msgs {
                let properties = mqtt_codec::Properties(
                    m.user_properties
                        .into_iter()
                        .map(|(k, v)| mqtt_codec::Property::UserProperty(k, v))
                        .collect(),
                );
                let _ = self_tx.send(Command::Publish {
                    topic: m.topic,
                    payload: Bytes::from(m.payload),
                    qos: qos_from_u8(m.qos),
                    pkid: None,
                    properties,
                });
            }
        }
        Err(e) => warn!(?side, error = %e, "failed to drain the spool on reconnect"),
    }
}

/// The configured name of the upstream involved in a side (for audit/metrics labels).
fn upstream_name(cfg: &BridgeConfig, side: Side) -> &str {
    match side {
        Side::Local => "local",
        Side::Upstream(i) => cfg.upstreams.get(i).map_or("unknown", |u| u.name.as_str()),
    }
}

fn side_index(side: Side) -> usize {
    match side {
        Side::Local => 0,
        Side::Upstream(i) => i + 1,
    }
}

fn endpoint_for(cfg: &BridgeConfig, side: Side) -> &Endpoint {
    match side {
        Side::Local => &cfg.local,
        Side::Upstream(i) => &cfg.upstreams[i].endpoint,
    }
}

fn subscriptions_for(cfg: &BridgeConfig, side: Side) -> Vec<(String, u8)> {
    match side {
        Side::Local => crate::forward::local_subscriptions(cfg),
        Side::Upstream(i) => crate::forward::upstream_subscriptions(cfg, i),
    }
}

/// This host's name, as the runtime sees it — the stable, per-instance part of a
/// default client id.
///
/// Two properties are required and both matter:
///
/// - **Distinct per instance**, or replicas collide (see [`default_client_id`]).
/// - **Stable across restarts**, because the local side uses a persistent session
///   (`clean_session=false`). An id that changed on every start would orphan the
///   previous session — and its queued messages — on the broker each time.
///
/// A hostname satisfies both: Kubernetes sets it to the pod name (stable for a
/// `StatefulSet` ordinal), and on a normal host it is the machine name. Checked in
/// order because neither source is universal: `HOSTNAME` is set by container
/// runtimes and shells, `/etc/hostname` is bind-mounted into containers by the
/// runtime and so is present even in a distroless image with no shell.
fn host_identity() -> Option<String> {
    if let Ok(h) = std::env::var("HOSTNAME") {
        let h = h.trim().to_string();
        if !h.is_empty() {
            return Some(h);
        }
    }
    if let Ok(h) = std::fs::read_to_string("/etc/hostname") {
        let h = h.trim().to_string();
        if !h.is_empty() {
            return Some(h);
        }
    }
    None
}

/// The client id used for a side when the config does not set one.
///
/// **Unique per instance, by default.** This used to return the constant
/// `fss-bridge-local`, which is safe for exactly one bridge and quietly
/// catastrophic for two: MQTT keys a session by client id and requires the broker
/// to disconnect the existing client when a new one connects with the same id, so
/// two replicas evicted each other in a permanent reconnect loop — turning the HA
/// that a shared subscription is supposed to buy into an outage. Nothing warned;
/// the config comment said "each instance still needs a distinct `client_id`" and
/// the default made it easy to never notice.
///
/// Deriving from the hostname makes replicas distinct without configuration, and
/// keeps each id stable across restarts so a persistent session resumes rather
/// than being orphaned.
///
/// Note on length: MQTT guarantees server support only for ids up to 23 bytes,
/// and a long hostname can exceed that. The id is deliberately **not** truncated —
/// truncation could map two hosts onto one id, which is the very failure this
/// exists to prevent. A strict upstream is handled by setting `client_id`
/// explicitly for that endpoint, which is a loud, immediate connect error rather
/// than a silent takeover.
fn default_client_id(side: Side) -> String {
    client_id_for(host_identity().as_deref(), side)
}

/// The pure half of [`default_client_id`]: given a host identity (or none), the id.
///
/// Split out so the naming rules are testable without mutating process-wide
/// environment — which, when a test did it, raced every other test reading it.
fn client_id_for(host: Option<&str>, side: Side) -> String {
    let Some(host) = host else {
        // No identity available: fall back to the old constants and say so. Any
        // second instance sharing this fallback WILL fight over the session, so
        // this is a real warning, not noise.
        warn!(
            "could not determine this host's name (no HOSTNAME, no /etc/hostname); \
             falling back to a FIXED bridge client id. Running more than one bridge \
             instance with this default will make them evict each other's MQTT \
             session — set `client_id` explicitly on each side."
        );
        return match side {
            Side::Local => "fss-bridge-local".to_string(),
            Side::Upstream(i) => format!("fss-bridge-upstream-{i}"),
        };
    };
    match side {
        Side::Local => format!("bridge-{host}"),
        Side::Upstream(i) => format!("bridge-{host}-{i}"),
    }
}

fn qos_from_u8(v: u8) -> QoS {
    QoS::from_u8(v).unwrap_or(QoS::AtMostOnce)
}

/// Build the client connect options for an endpoint.
///
/// # Errors
/// A string error if the endpoint requests TLS without an mTLS identity (cert+key), which
/// the client cannot yet honour, or a password file that cannot be read.
fn connect_options(
    ep: &Endpoint,
    default_id: &str,
    persistent: bool,
) -> Result<ConnectOptions, String> {
    let transport = match &ep.tls {
        None => Transport::Plain,
        Some(tls) => match (&tls.cert, &tls.key) {
            (Some(cert), Some(key)) => Transport::Tls {
                ca: tls.ca.clone().into(),
                cert: cert.clone().into(),
                key: key.clone().into(),
            },
            _ => {
                return Err(
                    "TLS without a client cert+key (server-auth-only) is not yet supported"
                        .to_string(),
                )
            }
        },
    };
    let password = match (&ep.password, &ep.password_file) {
        (Some(p), _) => Some(Bytes::from(p.clone())),
        (None, Some(path)) => {
            let raw = std::fs::read_to_string(path)
                .map_err(|e| format!("read password_file {path:?}: {e}"))?;
            Some(Bytes::from(raw.trim().to_string()))
        }
        (None, None) => None,
    };
    let client_id = if ep.client_id.is_empty() {
        default_id.to_string()
    } else {
        ep.client_id.clone()
    };
    Ok(ConnectOptions {
        addr: ep.url.clone(),
        transport,
        version: ProtocolVersion::V5, // v5 so the hop-count user property travels (§6)
        client_id,
        username: ep.username.clone(),
        password,
        keep_alive: KEEP_ALIVE,
        clean_start: !persistent,
    })
}

#[cfg(test)]
mod default_id_tests {
    use super::{client_id_for, default_client_id, host_identity, Side};

    #[test]
    fn a_host_identity_makes_replicas_distinct() {
        // THE regression: the default was the CONSTANT `fss-bridge-local`, so two
        // replicas claimed one session and MQTT made them evict each other in a
        // permanent loop — the HA a shared subscription offers, inverted.
        let a = client_id_for(Some("mqttd-bridge-0"), Side::Local);
        let b = client_id_for(Some("mqttd-bridge-1"), Side::Local);
        assert_ne!(a, b, "two pods must not share a client id");
        assert_eq!(a, "bridge-mqttd-bridge-0");
        assert_ne!(a, "fss-bridge-local", "still the colliding constant");
    }

    #[test]
    fn every_side_of_one_bridge_is_distinct_too() {
        let host = Some("h1");
        let ids = [
            client_id_for(host, Side::Local),
            client_id_for(host, Side::Upstream(0)),
            client_id_for(host, Side::Upstream(1)),
        ];
        let unique: std::collections::BTreeSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "sides collided: {ids:?}");
    }

    #[test]
    fn the_id_is_stable_for_a_given_host() {
        // The local side keeps a PERSISTENT session, so an id that varied between
        // starts would orphan the previous session and everything queued on it.
        assert_eq!(
            client_id_for(Some("h1"), Side::Local),
            client_id_for(Some("h1"), Side::Local)
        );
    }

    #[test]
    fn without_a_host_it_falls_back_and_is_honest_about_it() {
        // No identity available: the old constants come back, and the caller warns
        // loudly that a second instance will fight over the session.
        assert_eq!(client_id_for(None, Side::Local), "fss-bridge-local");
        assert_eq!(
            client_id_for(None, Side::Upstream(3)),
            "fss-bridge-upstream-3"
        );
    }

    #[test]
    fn the_live_default_uses_whatever_identity_this_machine_has() {
        // Reads the real environment rather than mutating it, so it cannot race
        // other tests. Where an identity exists the id must carry it.
        let id = default_client_id(Side::Local);
        match host_identity() {
            Some(host) => assert!(
                id.contains(&host),
                "default id {id:?} does not carry host {host:?}; replicas would collide"
            ),
            None => assert_eq!(id, "fss-bridge-local"),
        }
    }
}
