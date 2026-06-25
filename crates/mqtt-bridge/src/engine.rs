//! The bridge runtime — connect every side, subscribe per the rules, and route
//! ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §3, T3).
//!
//! One supervised connection per side (the local cluster + each upstream): connect, issue
//! the side's subscriptions, pump it ([`MqttClient::run`]), and on any disconnect reconnect
//! with bounded backoff. Inbound publishes from every side funnel into one **router** task
//! that applies the pure [`crate::forward`] policy and pushes the resulting publishes to the
//! destination sides — so all the security-relevant decisions live in the tested pure core,
//! and this layer is just plumbing.

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
        let metrics = Arc::new(BridgeMetrics::new());
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

        let mut tasks = Vec::new();

        // Local supervisor (side index 0).
        tasks.push(spawn_supervisor(
            Side::Local,
            cfg.clone(),
            cmd_rx.remove(0),
            cmd_tx[0].clone(),
            in_tx.clone(),
            metrics.clone(),
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
            ));
        }

        tasks.push(spawn_router(cfg, cmd_tx, in_rx, metrics.clone()));
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
fn spawn_supervisor(
    side: Side,
    cfg: Arc<BridgeConfig>,
    mut commands: mpsc::UnboundedReceiver<Command>,
    self_tx: mpsc::UnboundedSender<Command>,
    inbound_central: mpsc::UnboundedSender<(Side, Publish)>,
    metrics: Arc<BridgeMetrics>,
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
                    metrics.reconnect();
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
                    let err = client.run(&mut commands, &in_tx, KEEP_ALIVE).await;
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
) -> JoinHandle<()> {
    tokio::spawn(async move {
        // A per-side outbound packet-id cursor (each side's own QoS-1 id space).
        let mut pkids = vec![0u16; cmd_tx.len()];
        while let Some((side, publish)) = inbound.recv().await {
            let hop = read_hop_count(&publish.properties);
            let forwards = plan_forwards(&cfg, side, &publish.topic, hop);
            if forwards.is_empty() && hop >= cfg.hop_count_limit {
                metrics.dropped_hop_limit();
                debug!(?side, topic = %publish.topic, hop, "hop limit reached; dropped");
            }
            for f in forwards {
                let idx = side_index(f.dest);
                let qos = qos_from_u8(f.qos);
                let pkid = if qos == QoS::AtMostOnce {
                    None
                } else {
                    pkids[idx] = pkids[idx].wrapping_add(1).max(1);
                    Some(pkids[idx])
                };
                // Record + audit the crossing (the upstream involved is the non-local side).
                let (dir, up_name) = match side {
                    Side::Local => (CrossDirection::Out, upstream_name(&cfg, f.dest)),
                    Side::Upstream(_) => (CrossDirection::In, upstream_name(&cfg, side)),
                };
                metrics.forwarded(up_name, dir, &publish.topic, &f.topic);
                // Forward the publisher's user properties, with the hop count incremented.
                let properties = set_hop_count(&publish.properties, hop + 1);
                let _ = cmd_tx[idx].send(Command::Publish {
                    topic: f.topic,
                    payload: publish.payload.clone(),
                    qos,
                    pkid,
                    properties,
                });
            }
        }
    })
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

fn default_client_id(side: Side) -> String {
    match side {
        Side::Local => "fss-bridge-local".to_string(),
        Side::Upstream(i) => format!("fss-bridge-upstream-{i}"),
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
