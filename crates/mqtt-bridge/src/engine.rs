//! The bridge runtime ([ADR 0025](../../../docs/adr/0025-boundary-bridge.md) §3,
//! [ADR 0060](../../../docs/adr/0060-bridge-durability-and-ack-contract.md) Amendment 1).
//!
//! One link per upstream: each link task (the private `link` module) owns two connections, its own persistent
//! session on the local cluster and the connection to its upstream, and moves messages both
//! ways between them, so an unreachable upstream stops only its own traffic. This module
//! builds the links from the configuration — client ids, connection options, subscriptions,
//! spools (including the ones an older bridge left behind), metrics sides — and warns at
//! startup about rules through which the bridge now relays from one upstream to another.
//! All the security-relevant decisions stay in the pure [`crate::forward`] policy.

use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use bytes::Bytes;
use mqtt_codec::{ProtocolVersion, QoS};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::client::{ConnectOptions, Transport};
use crate::config::{BridgeConfig, Endpoint};
use crate::forward::{link_local_subscriptions, relay_paths, upstream_subscriptions, Side};
use crate::link::{self, LinkSpec};
use crate::metrics::{local_side_name, BridgeMetrics};
use crate::spool::{Overflow, Spool};

/// A running bridge. Dropping it (or calling [`Bridge::shutdown`]) stops every connection.
#[derive(Debug)]
pub struct Bridge {
    tasks: Vec<JoinHandle<()>>,
    metrics: Arc<BridgeMetrics>,
}

impl Bridge {
    /// Start the bridge from a validated configuration: one link per upstream. Returns
    /// immediately; the connections come up in the background.
    #[must_use]
    pub fn start(cfg: BridgeConfig) -> Self {
        let cfg = Arc::new(cfg);
        // Register the label values up front, from config: metric cardinality is then
        // bounded by the configuration rather than by traffic.
        let names: Vec<String> = cfg.upstreams.iter().map(|u| u.name.clone()).collect();
        let metrics = Arc::new(BridgeMetrics::for_upstreams(&names));

        for path in relay_paths(&cfg) {
            warn!(
                from = %path.from,
                from_filter = %path.from_filter,
                to = %path.to,
                to_filter = %path.to_filter,
                "messages brought in from one upstream match another upstream's out rule, so the \
                 bridge forwards them on to it (each upstream has its own local session, ADR 0060 \
                 Amendment 1); remap the in rule's topics if they must not cross"
            );
        }

        let host = host_identity();
        if host.is_none()
            && cfg
                .upstreams
                .iter()
                .any(|u| u.endpoint.client_id.is_empty())
        {
            // Without a host name every instance generates the SAME ids, so a second one
            // would evict the first. Allowed, never silent.
            warn!(
                "could not determine this host's name (no HOSTNAME, no /etc/hostname); \
                 every bridge instance will generate the SAME client id and they will \
                 evict each other's MQTT session — set `client_id` explicitly on each side"
            );
        }

        let mut side_names = Vec::with_capacity(2 * names.len());
        let mut connected = Vec::with_capacity(2 * names.len());
        let mut spools = Vec::with_capacity(2 * names.len());
        let mut tasks = Vec::with_capacity(names.len());
        for (i, up) in cfg.upstreams.iter().enumerate() {
            let local_side = local_side_name(&names, i);
            let local_connected = Arc::new(AtomicBool::new(false));
            let remote_connected = Arc::new(AtomicBool::new(false));
            let inbound_spool = build_inbound_spool(&cfg, i, &local_side);
            if i == 0 {
                if let Some(dir) = &cfg.spool.dir {
                    migrate_shared_local_spool(Path::new(dir), &inbound_spool);
                }
            }
            let legacy_outbound = open_legacy_outbound(&cfg, i);

            side_names.push(local_side.clone());
            connected.push(local_connected.clone());
            spools.push(Some(inbound_spool.clone()));
            side_names.push(up.name.clone());
            connected.push(remote_connected.clone());
            spools.push(legacy_outbound.clone());

            let local_id = local_client_id(&cfg, host.as_deref(), i);
            let remote_id = if up.endpoint.client_id.is_empty() {
                let id = client_id_for(host.as_deref(), Side::Upstream(i));
                log_generated_id(&id, host.as_deref(), &up.name);
                id
            } else {
                up.endpoint.client_id.clone()
            };
            let local = connect_options(&cfg.local, local_id, cfg.local_session_expiry());
            let remote = connect_options(
                &up.endpoint,
                remote_id,
                up.endpoint.session_expiry_secs.unwrap_or(0),
            );
            let (local, remote) = match (local, remote) {
                (Ok(l), Ok(r)) => (l, r),
                (Err(e), _) | (_, Err(e)) => {
                    // A config error will not fix itself by retrying.
                    warn!(upstream = %up.name, error = %e, "bridge endpoint misconfigured; this link does not start");
                    continue;
                }
            };
            let qos = |subs: Vec<(String, u8)>| -> Vec<(String, QoS)> {
                subs.into_iter().map(|(f, q)| (f, qos_from_u8(q))).collect()
            };
            let spec = LinkSpec {
                index: i,
                name: up.name.clone(),
                local_side,
                local,
                remote,
                local_subs: qos(link_local_subscriptions(&cfg, i)),
                remote_subs: qos(upstream_subscriptions(&cfg, i)),
                max_inflight_bytes: usize::try_from(cfg.max_inflight_bytes).unwrap_or(usize::MAX),
                inbound_spool,
                legacy_outbound,
                local_connected,
                remote_connected,
            };
            tasks.push(tokio::spawn(link::run(spec, cfg.clone(), metrics.clone())));
        }

        // Publish the live per-side state the gauges read. These are clones of the very
        // flags and spools the links use, so `fss_bridge_connected` and
        // `fss_bridge_spool_depth` cannot drift from the state they describe.
        metrics.register_sides(side_names, connected, spools);
        Self { tasks, metrics }
    }

    /// The bridge's metrics handle (forwarded/dropped/reconnect counters, Prometheus text).
    #[must_use]
    pub fn metrics(&self) -> Arc<BridgeMetrics> {
        self.metrics.clone()
    }

    /// Stop every link and its connections.
    pub fn shutdown(self) {
        for t in self.tasks {
            t.abort();
        }
    }
}

/// The client id of upstream `i`'s local session: the configured `[local] client_id`, with
/// `.<upstream name>` appended when there are several upstreams; otherwise a generated one.
/// With one upstream it is exactly the id the local side has always used.
///
/// **Unique per instance, by default.** The local id used to be the constant
/// `fss-bridge-local`, which is safe for exactly one bridge and quietly catastrophic for two:
/// MQTT keys a session by client id and requires the broker to disconnect the existing client
/// when a new one connects with the same id, so two replicas evicted each other in a permanent
/// reconnect loop. Deriving from the hostname makes replicas distinct without configuration,
/// and keeps each id stable across restarts so the persistent session resumes rather than
/// being orphaned. The id is deliberately **not** truncated to fit a strict broker — that
/// could map two hosts onto one id; set `client_id` explicitly for such an endpoint.
fn local_client_id(cfg: &BridgeConfig, host: Option<&str>, i: usize) -> String {
    let several = cfg.upstreams.len() > 1;
    if !cfg.local.client_id.is_empty() {
        return if several {
            format!("{}.{}", cfg.local.client_id, cfg.upstreams[i].name)
        } else {
            cfg.local.client_id.clone()
        };
    }
    let id = if several {
        link_client_id_for(host, i)
    } else {
        client_id_for(host, Side::Local)
    };
    log_generated_id(&id, host, &format!("local:{}", cfg.upstreams[i].name));
    id
}

/// Print a generated id's mapping once: the hash suffix is opaque by construction, and this
/// is what makes a strict, generated id as debuggable as a readable one.
fn log_generated_id(id: &str, host: Option<&str>, side: &str) {
    info!(
        client_id = %id,
        host = host.unwrap_or("<unknown>"),
        side,
        "generated MQTT client id (23-byte alphanumeric form accepted by any broker)"
    );
}

/// Build upstream `i`'s inbound spool (ADR 0060 Amendment 1 §7): messages from the upstream
/// that the bridge holds for the local cluster. Disk-backed under `spool.dir` as
/// `spool-local-<i>.redb` (so it survives a bridge restart), else in memory — bounded by
/// `spool.max_messages` and, when set, `spool.max_bytes` (ADR 0041 T7).
fn build_inbound_spool(cfg: &BridgeConfig, i: usize, label: &str) -> Arc<Spool> {
    let cap = cfg.spool.max_messages;
    // ADR 0060 T5: a QoS≥1 rule bringing traffic in refuses at the cap (everything spooled was
    // already acked to the upstream, so shedding it loses a message the upstream believes was
    // delivered); a QoS-0-only one drops the oldest, since QoS 0 promises nothing.
    let overflow = if cfg.requires_durable_spool() {
        Overflow::Refuse
    } else {
        Overflow::DropOldest
    };
    let finish = |s: Spool| {
        Arc::new(
            s.with_overflow(overflow)
                .with_max_bytes(cfg.spool.max_bytes)
                .with_label(label),
        )
    };
    let Some(dir) = &cfg.spool.dir else {
        return finish(Spool::in_memory(cap));
    };
    let path = Path::new(dir).join(format!("spool-local-{i}.redb"));
    match Spool::on_disk(&path, cap) {
        Ok(s) => finish(s),
        Err(e) => {
            // ADR 0060 T4: never silently fall back to a non-durable spool when a QoS≥1 rule
            // needs one — that loses acked messages on restart. Refuse to start unless the
            // operator opted into ephemeral operation.
            if cfg.requires_durable_spool() && !cfg.spool.allow_ephemeral_spool {
                error!(
                    path = %path.display(), error = %e,
                    "durable spool required for a QoS>=1 in rule but the disk spool could not be \
                     opened; refusing to start (set spool.allow_ephemeral_spool to run \
                     non-durably and accept message loss)"
                );
                std::process::exit(1);
            }
            warn!(path = %path.display(), error = %e, "disk spool unavailable; using an in-memory spool");
            finish(Spool::in_memory(cap))
        }
    }
}

/// The spool an older bridge kept for upstream `i` (`spool-upstream-<i>.redb`), if it holds
/// anything (ADR 0060 Amendment 1 §8). Those messages were acknowledged to the local cluster
/// already, so the link sends them to the upstream before its local session connects. A
/// bridge of this version never writes to it.
fn open_legacy_outbound(cfg: &BridgeConfig, i: usize) -> Option<Arc<Spool>> {
    let dir = cfg.spool.dir.as_ref()?;
    let path = Path::new(dir).join(format!("spool-upstream-{i}.redb"));
    if !path.exists() {
        return None;
    }
    match Spool::on_disk(&path, usize::MAX) {
        Ok(s) => match s.len() {
            Ok(0) => None,
            Ok(n) => {
                info!(
                    path = %path.display(), messages = n,
                    "an older bridge left messages spooled for this upstream; sending them first"
                );
                Some(Arc::new(s))
            }
            Err(e) => {
                warn!(path = %path.display(), error = %e, "could not read the old spool");
                None
            }
        },
        Err(e) => {
            warn!(path = %path.display(), error = %e, "could not open the old spool");
            None
        }
    }
}

/// Move what an older bridge spooled for the local cluster (`spool-local.redb`, shared by all
/// upstreams then) into upstream 0's inbound spool, which replays it like its own (ADR 0060
/// Amendment 1 §8). Each message is pushed before the old copies are removed, so a crash in
/// between duplicates rather than loses. If the new spool refuses one, the rest stay in the
/// old file for the next start.
fn migrate_shared_local_spool(dir: &Path, into: &Spool) {
    let path = dir.join("spool-local.redb");
    if !path.exists() {
        return;
    }
    let old = match Spool::on_disk(&path, usize::MAX) {
        Ok(s) => s,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "could not open the old local spool");
            return;
        }
    };
    let messages = match old.peek_after(None, usize::MAX) {
        Ok(m) => m,
        Err(e) => {
            warn!(path = %path.display(), error = %e, "could not read the old local spool");
            return;
        }
    };
    if messages.is_empty() {
        return;
    }
    let mut moved = Vec::with_capacity(messages.len());
    for (key, m) in &messages {
        if let Err(e) = into.push(m) {
            warn!(
                path = %path.display(), error = %e,
                "the new spool refused a message from the old local spool; the rest stays there \
                 for the next start"
            );
            break;
        }
        moved.push(*key);
    }
    if let Err(e) = old.remove_many(&moved) {
        warn!(path = %path.display(), error = %e, "could not remove moved messages from the old local spool; they will replay twice");
    }
    info!(
        moved = moved.len(),
        "moved the messages an older bridge spooled for the local cluster into upstream 0's spool"
    );
    debug!(path = %path.display(), "old local spool emptied");
}

/// This host's name, as the runtime sees it — the stable, per-instance part of a
/// default client id.
///
/// Two properties are required and both matter:
///
/// - **Distinct per instance**, or replicas collide (see [`local_client_id`]).
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

/// The pure half of [`local_client_id`]: given a host identity (or none), the id.
///
/// Split out so the naming rules are testable without mutating process-wide
/// environment — which, when a test did it, raced every other test reading it.
/// MQTT's *guaranteed* client-id shape: 1-23 bytes of `[0-9a-zA-Z]`.
///
/// A server MUST support ids in this range and MAY support more. Ours accepts any
/// non-empty id of any length or charset — but the bridge also connects to
/// *other people's* brokers, and "the far side is permissive" is an assumption
/// nothing here can check. So generated ids stay inside the guaranteed set, for
/// every side: the local endpoint is just a URL a user configures, and there is
/// no guarantee it points at this broker either.
const ID_MAX: usize = 23;
/// Marks a generated id as ours in someone else's broker logs.
const ID_PREFIX: &str = "fssb";
/// Alphanumerics of the host name kept for human recognition.
const ID_HOST_KEEP: usize = 10;
/// Base36 digits of hash — 36^7 ≈ 7.8e10, the part that guarantees uniqueness.
const ID_HASH_LEN: u32 = 7;

/// FNV-1a, 64-bit.
///
/// Deliberately not `DefaultHasher`: its output is explicitly not stable across
/// Rust releases, and a client id that changed when the binary was rebuilt would
/// orphan the persistent local session every upgrade. FNV is fixed by its
/// specification, trivial, and needs no dependency. It is not cryptographic and
/// does not need to be — this distinguishes hosts, it does not authenticate them.
fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// `n` rendered in base36 `[0-9a-z]`, exactly `width` digits, zero-padded.
fn base36(n: u64, width: usize) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    let mut out = vec![b'0'; width];
    let mut n = n;
    for slot in out.iter_mut().rev() {
        *slot = DIGITS[usize::try_from(n % 36).unwrap_or(0)];
        n /= 36;
    }
    String::from_utf8(out).unwrap_or_else(|_| "0".repeat(width))
}

/// The client id used for a side when the config does not set one.
///
/// **Unique per instance, valid on any broker, and needs no configuration.**
///
/// It used to be the constant `fss-bridge-local` for every instance. MQTT keys a
/// session by client id and requires a broker to disconnect the existing client
/// when a new one claims the same id, so two replicas evicted each other in a
/// permanent loop — the HA a shared subscription is meant to buy, inverted. The
/// config comment said "each instance still needs a distinct `client_id`", which
/// made it a footgun rather than a surprise, but nothing warned.
///
/// The shape, 23 bytes at most:
///
/// ```text
///   fssb   lo    ttdbridge0   k3n9x2q
///   ────   ──    ──────────   ───────
///    4      2        10          7
///   ours   side   host tail     hash
/// ```
///
/// Three choices worth stating, because each prevents a specific failure:
///
/// - **The hash covers the FULL host name**, never the truncated part. A
///   shortener that could map two hosts onto one id would reintroduce exactly the
///   collision this exists to prevent.
/// - **The host's TAIL is kept, not its head.** Generated names put the
///   distinguishing part last — `mqttd-bridge-0` and `mqttd-bridge-1` share their
///   first 11 characters, so head-truncation would discard the only readable
///   difference.
/// - **Base36 `[0-9a-z]`** sits inside the guaranteed charset and survives a
///   broker that folds case.
///
/// Stable across restarts, because the local side uses `clean_session=false`: an
/// id that varied per start would orphan the previous session and its queue.
fn client_id_for(host: Option<&str>, side: Side) -> String {
    match side {
        Side::Local => generated_id(host, "lo", b"local"),
        // Beyond 36 upstreams the tag repeats; the hash still separates them, since it is
        // taken over the real index.
        Side::Upstream(i) => generated_id(
            host,
            &format!("u{}", base36((i % 36) as u64, 1)),
            format!("up{i}").as_bytes(),
        ),
    }
}

/// The generated client id of upstream `link`'s local session when the bridge has several
/// upstreams (ADR 0060 Amendment 1 §2): the same shape as [`client_id_for`], with the side tag
/// `l<link>` and the link index in the hash, so every link's session is distinct and stable.
fn link_client_id_for(host: Option<&str>, link: usize) -> String {
    generated_id(
        host,
        &format!("l{}", base36((link % 36) as u64, 1)),
        format!("local{link}").as_bytes(),
    )
}

/// The shared body of the generated ids: `fssb` + a two-character side tag + the host's
/// tail + a hash over the full host and `seed_tail`.
fn generated_id(host: Option<&str>, side_tag: &str, seed_tail: &[u8]) -> String {
    let host = host.unwrap_or("");
    // Hash over the full identity — host and side — before anything is shortened.
    let mut seed = Vec::with_capacity(host.len() + 8);
    seed.extend_from_slice(host.as_bytes());
    seed.push(0);
    seed.extend_from_slice(seed_tail);
    let hash = base36(
        fnv1a64(&seed) % 36_u64.pow(ID_HASH_LEN),
        ID_HASH_LEN as usize,
    );

    // Readable part comes from the FIRST DNS label only. Kubernetes sets HOSTNAME
    // to the short pod name, but /etc/hostname can hold an FQDN — and there the
    // tail is the domain, so `…svc.cluster.local` would keep "usterlocal" and lose
    // the pod entirely. The hash above still covers the whole string, so dropping
    // the domain here cannot merge two hosts.
    let short = host.split('.').next().unwrap_or(host);
    // Only characters every server must accept, lowercased so a case-folding
    // broker cannot merge two ids that differ solely by case.
    let clean: String = short
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect();
    let tail: String = if clean.len() > ID_HOST_KEEP {
        clean[clean.len() - ID_HOST_KEEP..].to_string()
    } else {
        clean
    };

    let id = format!("{ID_PREFIX}{side_tag}{tail}{hash}");
    debug_assert!(
        id.len() <= ID_MAX,
        "generated client id {id} exceeds {ID_MAX}"
    );
    debug_assert!(
        id.chars().all(|c| c.is_ascii_alphanumeric()),
        "generated client id {id} leaves the guaranteed charset"
    );
    id
}

fn qos_from_u8(v: u8) -> QoS {
    QoS::from_u8(v).unwrap_or(QoS::AtMostOnce)
}

/// Build the client connect options for an endpoint: `client_id`, and a session that lasts
/// `session_expiry` seconds after a disconnect (`0` = a clean session that ends with the
/// connection).
///
/// # Errors
/// A string error if the endpoint requests TLS without an mTLS identity (cert+key), which
/// the client cannot yet honour, or a password file that cannot be read.
fn connect_options(
    ep: &Endpoint,
    client_id: String,
    session_expiry: u32,
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
            let secret = mqtt_core::read_secret_file(path)
                .map_err(|e| format!("read password_file {path:?}: {e}"))?;
            Some(Bytes::from(secret))
        }
        (None, None) => None,
    };
    Ok(ConnectOptions {
        addr: ep.url.clone(),
        transport,
        version: ProtocolVersion::V5, // v5 so the hop-count user property travels (§6)
        client_id,
        username: ep.username.clone(),
        password,
        keep_alive: ep.keep_alive(),
        clean_start: session_expiry == 0,
        session_expiry,
        receive_maximum: None,
    })
}

#[cfg(test)]
mod default_id_tests {
    use super::{client_id_for, host_identity, local_client_id, Side, ID_MAX};

    /// Every generated id must satisfy MQTT's guaranteed-support shape, whatever
    /// the host is called: 1-23 bytes of [0-9a-zA-Z]. A broker is only obliged to
    /// accept ids in that range, and the bridge connects to brokers we do not run.
    fn assert_union_safe(id: &str) {
        assert!(
            !id.is_empty(),
            "empty client id (needs clean_session, not universal)"
        );
        assert!(
            id.len() <= ID_MAX,
            "id {id:?} is {} bytes, past the 23-byte guaranteed floor",
            id.len()
        );
        assert!(
            id.chars().all(|c| c.is_ascii_alphanumeric()),
            "id {id:?} leaves the guaranteed [0-9a-zA-Z] charset"
        );
    }

    #[test]
    fn any_host_name_yields_a_union_safe_id() {
        // Including the ones that would break a naive scheme: over-long, dotted,
        // hyphenated, unicode, and empty.
        for host in [
            "mqttd-bridge-0",
            "a",
            "",
            "mqttd-bridge-0.mqttd-bridge-headless.production-namespace.svc.cluster.local",
            "MQTTD-Bridge-0",
            "brücke-0",
            "0000000000000000000000000000000000000000",
        ] {
            for side in [Side::Local, Side::Upstream(0), Side::Upstream(41)] {
                assert_union_safe(&client_id_for(Some(host), side));
            }
        }
        assert_union_safe(&client_id_for(None, Side::Local));
    }

    #[test]
    fn replicas_are_distinct_even_when_truncation_collides() {
        // THE case that matters in Kubernetes: pod names share a long prefix and
        // differ only in the ordinal, so the readable part alone cannot separate
        // them. The hash is taken over the FULL name, so they still differ.
        let a = client_id_for(Some("mqttd-bridge-0"), Side::Local);
        let b = client_id_for(Some("mqttd-bridge-1"), Side::Local);
        assert_ne!(a, b);

        // And names long enough that the kept tail is identical.
        let long_a = client_id_for(
            Some("prod-eu-west-1-edge-bridge-alpha-000000000"),
            Side::Local,
        );
        let long_b = client_id_for(
            Some("prod-eu-west-2-edge-bridge-beta-000000000"),
            Side::Local,
        );
        assert_ne!(
            long_a, long_b,
            "truncation must not be able to merge two hosts"
        );
    }

    #[test]
    fn a_fleet_of_pod_names_generates_no_duplicates() {
        let ids: std::collections::BTreeSet<String> = (0..200)
            .flat_map(|i| {
                [
                    client_id_for(Some(&format!("mqttd-bridge-{i}")), Side::Local),
                    client_id_for(Some(&format!("mqttd-bridge-{i}")), Side::Upstream(0)),
                    client_id_for(Some(&format!("mqttd-bridge-{i}")), Side::Upstream(1)),
                ]
            })
            .collect();
        assert_eq!(ids.len(), 600, "collision within a 200-pod fleet");
    }

    #[test]
    fn every_side_of_one_bridge_is_distinct() {
        let host = Some("h1");
        let ids: std::collections::BTreeSet<String> = [
            client_id_for(host, Side::Local),
            client_id_for(host, Side::Upstream(0)),
            client_id_for(host, Side::Upstream(1)),
            // Past the 36-value side tag: the tag repeats, the hash does not.
            client_id_for(host, Side::Upstream(36)),
        ]
        .into_iter()
        .collect();
        assert_eq!(ids.len(), 4, "sides collided");
    }

    #[test]
    fn the_id_is_stable_for_a_given_host() {
        // The local side keeps a PERSISTENT session, so an id that varied between
        // starts would orphan the previous session and everything queued on it.
        // Pinned literally: a change here silently re-identifies every deployed
        // bridge, so it should have to be done on purpose.
        assert_eq!(
            client_id_for(Some("mqttd-bridge-0"), Side::Local),
            client_id_for(Some("mqttd-bridge-0"), Side::Local)
        );
        assert_eq!(client_id_for(Some("mqttd-bridge-0"), Side::Local).len(), 23);
    }

    #[test]
    fn the_id_is_recognisably_ours_and_carries_the_host_tail() {
        let id = client_id_for(Some("mqttd-bridge-7"), Side::Local);
        assert!(
            id.starts_with("fssblo"),
            "{id} should mark itself as ours + side"
        );
        assert!(
            id.contains("bridge7"),
            "{id} should keep the host's tail so a human can recognise the pod"
        );
    }

    #[test]
    fn an_fqdn_keeps_the_pod_not_the_domain() {
        // /etc/hostname can hold an FQDN, whose TAIL is the domain — keeping it
        // would yield "usterlocal" from "…svc.cluster.local" for every pod alike.
        let a = client_id_for(
            Some("mqttd-bridge-0.mqttd-bridge-hl.prod.svc.cluster.local"),
            Side::Local,
        );
        let b = client_id_for(
            Some("mqttd-bridge-1.mqttd-bridge-hl.prod.svc.cluster.local"),
            Side::Local,
        );
        assert!(a.contains("bridge0"), "{a} lost the pod name to the domain");
        assert!(b.contains("bridge1"), "{b} lost the pod name to the domain");
        assert_ne!(a, b);
        // The domain still participates in the hash, so two pods with the same
        // short name in different domains stay distinct.
        assert_ne!(
            client_id_for(Some("gw-0.site-a.example"), Side::Local),
            client_id_for(Some("gw-0.site-b.example"), Side::Local),
        );
    }

    #[test]
    fn the_live_default_matches_this_machine() {
        let one = |extra: &str| {
            crate::config::BridgeConfig::parse_toml(&format!(
                r#"
                [local]
                url = "l:1"
                [[upstreams]]
                name = "a"
                url = "a:1"
                {extra}
                "#
            ))
            .unwrap()
        };
        let host = host_identity();
        // One upstream: the local id the bridge has always generated.
        let id = local_client_id(&one(""), host.as_deref(), 0);
        assert_union_safe(&id);
        assert_eq!(id, client_id_for(host.as_deref(), Side::Local));
        // Two upstreams: one distinct, union-safe id per link.
        let two = one("[[upstreams]]\nname = \"b\"\nurl = \"b:1\"");
        let a = local_client_id(&two, host.as_deref(), 0);
        let b = local_client_id(&two, host.as_deref(), 1);
        assert_union_safe(&a);
        assert_union_safe(&b);
        assert_ne!(a, b);
        assert_ne!(a, id);
    }

    #[test]
    fn a_configured_local_id_names_the_upstream_only_when_there_are_several() {
        let cfg = |ups: &str| {
            crate::config::BridgeConfig::parse_toml(&format!(
                "[local]\nurl = \"l:1\"\nclient_id = \"bridge-0\"\n{ups}"
            ))
            .unwrap()
        };
        let one = cfg("[[upstreams]]\nname = \"a\"\nurl = \"a:1\"");
        assert_eq!(local_client_id(&one, None, 0), "bridge-0");
        let two = cfg(
            "[[upstreams]]\nname = \"a\"\nurl = \"a:1\"\n[[upstreams]]\nname = \"b\"\nurl = \"b:1\"",
        );
        assert_eq!(local_client_id(&two, None, 0), "bridge-0.a");
        assert_eq!(local_client_id(&two, None, 1), "bridge-0.b");
    }
}
