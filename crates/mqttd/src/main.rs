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
//! Configuration (ADR 0046): every setting below is loaded in precedence order
//! **defaults < TOML file < `MQTTD_*` env var** (CLI flags on top) into a typed
//! `mqtt_config::Config` — the `MQTTD_*` variables are the env layer of that config, not
//! read directly here. The TOML file is named by `--config <path>` or `MQTTD_CONFIG`; with
//! neither, config is defaults + the env overlay (fully backward compatible). Each
//! `MQTTD_*` variable maps to exactly one config key (see `mqtt_config::ENV_VARS`).
//! `mqttd --check-config` (ADR 0046 T3) validates the effective config and exits without
//! binding any port — the GitOps/pre-rollout gate. `SIGHUP` (and the `MQTTD_CONFIG_WATCH`
//! filesystem watcher) reload the whole config file through the ADR 0032 validate-before-swap
//! path (ADR 0046 T4): a bad edit is rejected and the running config kept; live-swappable
//! settings (policy files, `allow_anonymous`, quotas) change without a restart; every other
//! change is logged + audited as requires-restart.
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
//! - `MQTTD_JWT_HS256_SECRET_FILE` / `MQTTD_JWT_RS256_PEM` — JWT verification key, read from a
//!   file (ADR 0046 T5 secret-by-reference); optional `MQTTD_JWT_ISSUER` / `MQTTD_JWT_AUDIENCE`
//! - `MQTTD_OIDC_ISSUER` — OIDC-mode token auth (ADR 0050): discovery + JWKS rotation from the
//!   issuer; requires `MQTTD_OIDC_AUDIENCE`; optional `MQTTD_OIDC_JWKS_REFRESH` (secs, 300),
//!   `MQTTD_OIDC_MAX_STALE` (secs, 86400 — fail-closed beyond), `MQTTD_OIDC_GROUPS_CLAIM`
//!   (`groups`), `MQTTD_OIDC_ALLOW_HTTP` (INSECURE, tests only). Mutually exclusive with the
//!   static `MQTTD_JWT_*` verifier; OIDC settings are read at startup (not hot-reloaded)
//! - `MQTTD_CONFIG_WATCH`   — opt-in filesystem auto-reload (ADR 0033): poll interval in
//!   seconds; when a configured policy file (ACL, password, JWT PEM, TLS cert/key/CA/CRL)
//!   changes on disk, reload through the same fail-safe routine as `SIGHUP` (no restart).
//!   Unset/`0` = disabled (signal-only, the default). For declarative/Kubernetes-ConfigMap use
//! - `MQTTD_PEER_BIND`      — inter-node listener bind, e.g. `127.0.0.1:7001`
//! - `MQTTD_PEER_ADVERTISE` — the peer-link address gossip advertises to other
//!   nodes (default: the `MQTTD_PEER_BIND` value). Set it when the address
//!   peers can dial differs from the bound one — NAT, container port mapping,
//!   or a fronting proxy/relay.
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
//! - `MQTTD_SWIM_KEY`       — 64-hex-char cluster gossip key (ADR 0003), inline, e.g.
//!   from `openssl rand -hex 32`; without it (or `MQTTD_SWIM_KEY_FILE`) gossip is
//!   unauthenticated and loudly logged
//! - `MQTTD_SWIM_KEY_FILE`  — path to a file holding the 64-hex gossip key (ADR 0046 T5
//!   secret-by-reference); mutually exclusive with the inline `MQTTD_SWIM_KEY`
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
//!
//! Signals: `SIGTERM`/`SIGINT` begin the ADR 0019 graceful shutdown (a second one
//! forces immediate exit). **`SIGUSR1` begins a DECOMMISSION** (ADR 0043 P3): the
//! node first hands every durable key it holds to each group's post-departure
//! replica set and verifies the copies landed (progress on `/readyz` as
//! `decommission{pending,rounds,complete}`), and only then runs the graceful
//! leave — so a planned removal loses nothing, unlike pulling the plug. A
//! `SIGTERM` during the drain escalates to a plain shutdown (crash semantics).
//!
//! Subcommands (each validates + exits, binding nothing): `--check-config [--config <path>]`
//! (ADR 0046 T3) validates the effective config; **`--decommission [--pid <n>] [--timeout <secs>]`**
//! (ADR 0047 T4) sends `SIGUSR1` to the running broker (`--pid`, default 1 — the container
//! entrypoint) to begin the decommission drain and **blocks until it exits**, so a Kubernetes
//! `preStop` holds the pod open for the whole drain even though the distroless image has no shell.

use mqtt_auth::basic::BasicAuthenticator;
use mqtt_auth::{Authenticator, Authorizer};
use mqtt_cluster::placement::{self, Placement};
use mqtt_cluster::swim::Swim;
use mqtt_cluster::swim_auth::SwimAuth;
use mqtt_cluster::{swim_driver, NodeId};
use mqtt_config::{Config, ConfigError, Jwt};
use mqtt_net::tls;
use mqtt_observability::{AuditLog, AuditSink};
use mqtt_storage::logged::ReplicatedSessionStore;
use mqtt_storage::persistent_log::PersistentLog;
use mqtt_storage::persistent_retained::PersistentRetainedStore;
use mqtt_storage::{MemorySessionStore, OverflowPolicy, QueueLimits, RetainedStore, SessionStore};
use mqttd::{admission, cluster, config_watch, conn, hub, peer, reload};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

/// SWIM driver tick; must stay below the ack timeout (250ms default config).
const SWIM_TICK: Duration = Duration::from_millis(100);

// Startup is a linear wiring sequence; splitting it would only scatter the order it
// documents.
#[allow(clippy::too_many_lines)]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Process-default crypto provider (aws-lc-rs, ADR 0053): reqwest's rustls-no-provider
    // build (the OIDC JWKS fetcher, ADR 0050) resolves its TLS provider from here. With a
    // single provider compiled into the build this is belt-and-braces determinism — no
    // binary or test can silently resolve a different stack.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    // `--version` / `-V` and `--help` / `-h`: local, print-and-exit, before any subcommand
    // or config work (#169). `--version` in particular MUST exist — an operator typing it
    // expecting a version once silently BOOTED A BROKER, because unrecognised flags fell
    // through to startup (the reject-unknown-flags check below now closes that).
    if std::env::args()
        .skip(1)
        .any(|a| a == "--version" || a == "-V")
    {
        println!("mqttd {}", env!("CARGO_PKG_VERSION"));
        std::process::exit(0);
    }
    if std::env::args().skip(1).any(|a| a == "--help" || a == "-h") {
        print_usage();
        std::process::exit(0);
    }

    // #169: a flag the broker does not recognise must be an ERROR, not a silent boot. Three
    // reviewers independently hit `mqttd --version` (or a typo) quietly starting a real
    // broker. Checked before any subcommand dispatch or resource acquisition, so a
    // mistyped flag never reaches startup.
    reject_unknown_flags();

    // ADR 0046 T3: `--check-config` validates the config the broker would boot with and exits,
    // without binding a port or starting the hub — the GitOps/pre-rollout gate. Handled here,
    // before any resource is acquired.
    if std::env::args().skip(1).any(|a| a == "--check-config") {
        check_config();
    }

    // `--hash-password [<username>]` prints an Argon2id password-file line and exits.
    // Before the broker configures anything: it is a local text utility, not a server mode.
    if std::env::args().skip(1).any(|a| a == "--hash-password") {
        hash_password_cli();
    }

    // `--probe [/readyz|/livez]` asks the RUNNING broker's health endpoint and exits with
    // its verdict — the health check for orchestrators that run a command (Compose,
    // Podman, systemd) rather than performing the HTTP GET themselves, on an image with
    // no shell and no curl.
    if std::env::args().skip(1).any(|a| a == "--probe") {
        probe_health().await;
    }

    // ADR 0047 T4: `--decommission` sends SIGUSR1 to the running broker (PID 1 in a distroless
    // container, which has no shell/`kill`) and waits for the drain + graceful shutdown to
    // complete — the Kubernetes `preStop` hook. Handled here so it never touches the network.
    if std::env::args().skip(1).any(|a| a == "--decommission") {
        run_decommission();
    }

    // ADR 0046 T2: assemble the effective configuration in precedence order —
    // defaults < TOML file < `MQTTD_*` env. The file path comes from `--config <path>` or
    // `MQTTD_CONFIG` (the flag wins); with neither, the config is defaults + the env overlay,
    // fully backward-compatible with the env-only deployments that predate file config. CLI
    // flags are ADR 0046's top layer; today `--config` is the only one, so env is the highest
    // value layer. Every `MQTTD_*` setting below is now read from this typed `Config`, never
    // from `std::env` directly — the env surface is mapped once, in `mqtt_config`.
    let config = Arc::new(load_config()?);
    let node_id = NodeId(config.node.id.clone());
    info!(version = env!("CARGO_PKG_VERSION"), node = %node_id.0, "starting mqttd");
    log_effective_config(&config);

    // ADR 0046 T4: the running config, shared with the policy reload closures so a `SIGHUP` /
    // watch re-load of the file reaches them (a changed ACL path, a changed quota) through the
    // ADR 0032 validate-before-swap path. The one-time startup wiring below reads the immutable
    // `config` snapshot (those settings are requires-restart); only the reload path reads `live`.
    let live_config = Arc::new(RwLock::new((*config).clone()));

    // Server-wide MQTT 5 wire limits (ADR 0011/0012/0013), configurable via env, set once
    // before any connection is served.
    let wire_limits = wire_limits_from_config(&config)?;
    // The same ceiling governs the transport frame reader (ADR 0041 T4): the
    // advertised Maximum Packet Size and the enforced cap cannot drift apart.
    mqtt_net::set_max_packet_bytes(wire_limits.max_packet_size as usize);
    conn::set_wire_limits(wire_limits);

    // Session-placement ring (ADR 0005), kept in step with SWIM membership and
    // read by the hub to identify each persistent session's owner node.
    // The min-replicas write floor (issue #167): checked against the replication
    // factor HERE, where the factor is known — a floor above R can never be met and
    // would refuse every durable write forever, so it fails fast like any other
    // invalid configuration.
    let min_replicas = config.durable.min_replicas as usize;
    if min_replicas > placement::DEFAULT_REPLICAS {
        return Err(format!(
            "durable.min_replicas ({min_replicas}) exceeds the replication factor \
             ({}) — no group can ever satisfy that floor",
            placement::DEFAULT_REPLICAS
        )
        .into());
    }
    let placement = Arc::new(RwLock::new(
        Placement::new(node_id.clone(), placement::DEFAULT_REPLICAS)
            // This node's own failure-domain label (ADR 0016 T5), so placement reports it
            // in the topology map without waiting for gossip to round-trip.
            .with_local_domain(config.node.failure_domain.clone())
            .with_min_replicas(min_replicas),
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
        if let Some(endpoint) = &config.observability.otlp_endpoint {
            let interval = Duration::from_secs(config.observability.otlp_interval_secs);
            // node_id becomes service.instance.id so each cluster node's OTLP series are
            // distinct at the backend (otherwise all nodes collide into one series).
            let m = mqtt_observability::metrics::Metrics::with_otlp(
                version, endpoint, interval, &node_id.0,
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
    // Shared operator-state snapshots (ADR 0054): the hub flips brownout, the store
    // watcher fills sizes, /statusz reads both.
    let brownout_status = Arc::new(mqttd::health::BrownoutStatus::default());
    let store_snapshot = Arc::new(mqttd::store_watch::StoreSnapshot::default());
    // Cluster identity (ADR 0054 T2): the founder (seedless) mints it, joiners adopt
    // it over gossip, and gossip from a separately-founded cluster is dropped —
    // split-brain becomes detectable (compare /statusz across nodes) AND contained.
    let cluster_identity = Arc::new(
        mqtt_cluster::cluster_identity::ClusterIdentity::load_or_mint(
            config.cluster.swim.seeds.is_empty(),
            config
                .node
                .data_dir
                .as_ref()
                .map(|d| std::path::Path::new(d).join("cluster-id")),
        )
        // Flattened to its message so the operator gets the same one-line shape as every
        // other startup refusal. Boxing the `io::Error` itself renders as
        // `Custom { kind: NotFound, error: "…" }`, which buries the path it now carries.
        .map_err(|e| e.to_string())?,
    );
    // Evidence for the re-found self-quarantine (issue #92 follow-up): set the first
    // time the gossip driver drops a datagram from a FOREIGN cluster. Created here
    // because health starts long before the gossip plane, and both need the handle.
    //
    // Cluster-configured is the same predicate the hub is told (peer/gossip networking
    // is set up), hoisted so readiness can be armed before the cluster plane exists.
    let cluster_configured = config.cluster.peer_bind.is_some()
        || !config.cluster.peers.is_empty()
        || config.cluster.swim.bind.is_some()
        || !config.cluster.swim.seeds.is_empty();
    let foreign_cluster_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    // Config stamp (ADR 0054 T3): checksum + generation of the applied config,
    // recorded at startup and on every successful reload.
    let config_stamp = Arc::new(mqttd::reload::ConfigStamp::default());
    {
        let bytes = config_path()
            .ok()
            .flatten()
            .and_then(|p| std::fs::read(p).ok())
            .unwrap_or_default();
        config_stamp.record(&bytes);
        let (sum, _) = config_stamp.read();
        metrics.set_config_info(&sum);
    }
    // SWIM key-rotation posture (ADR 0054 T3): filled by start_swim once auth is built.
    let swim_key_fps = Arc::new(std::sync::OnceLock::new());
    metrics.set_peer_proto(mqtt_cluster::peer::PROTO_MIN, mqtt_cluster::peer::PROTO_MAX);
    metrics.set_founder(cluster_identity.founder());
    if cluster_identity.minted() {
        // A founding event. Expected exactly once, on a brand-new cluster's first
        // boot; any later founding (a founder restarted over a lost data dir) is
        // the split-brain alarm the foundings counter exists for.
        metrics.founding();
        info!(
            cluster_id = cluster_identity.get().as_deref().unwrap_or(""),
            "founded a NEW cluster identity (ADR 0054)"
        );
    }
    // Export cluster_info{cluster_id} once the identity is known (immediately for
    // the founder; after gossip adoption for a joiner).
    {
        let (m, id) = (metrics.clone(), cluster_identity.clone());
        tokio::spawn(async move {
            loop {
                if let Some(v) = id.get() {
                    m.set_cluster_info(&v);
                    return;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }
    let (hub_tx, store, durable_plane, lease_driver) =
        start_hub(&config, &node_id, &placement, &metrics, &brownout_status).await?;

    // Health endpoints for orchestrators (opt-in via MQTTD_HEALTH_BIND), serving
    // /livez (hub responsive) and /readyz (mesh + durable-store ready). Keep a plane
    // handle to stop openraft cleanly on shutdown.
    let plane_for_shutdown = durable_plane.clone();
    let (draining, decommission_slot) = start_health(
        &config,
        &hub_tx,
        &placement,
        durable_plane.clone(),
        metrics.clone(),
        &node_id,
        &cluster_identity,
        &brownout_status,
        &store_snapshot,
        &config_stamp,
        &swim_key_fps,
        // Arm the guard only for a cluster node that has not opted out.
        (cluster_configured && config.cluster.refound_guard).then(|| foreign_cluster_seen.clone()),
    )
    .await?;

    // Cluster-bus mTLS context (ADR 0002): one CA + node cert pair secures both
    // the accepting and dialing side of every peer link.
    let peer_tls_parts = peer_tls_from_config(&config)?;
    let (peer_tls, peer_tls_reload) = match peer_tls_parts {
        Some((tls, reload_parts)) => (Some(tls), Some(reload_parts)),
        None => (None, None),
    };

    // Client policy (ADR 0004 auth/authz/audit + ADR 0005 session relocation),
    // built before the peer listener so the latter can serve sessions relocated
    // here by other nodes. The same policy serves the client listeners below.
    let proxy = conn::ProxyContext {
        node: node_id.clone(),
        placement: placement.clone(),
        connector: peer_tls.as_ref().map(|t| t.connector.clone()),
    };
    let (policy, mut reloader) = client_policy(
        &live_config,
        &node_id,
        Some(proxy),
        store,
        shutdown.clone(),
        metrics.clone(),
    )?;
    reloader.attach_config_stamp(config_stamp.clone());

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
    // Peer-bus TLS reload (ADR 0040 T4, paying the ADR 0032 deferred item): the
    // acceptor/connector are rebuilt in the same validate-before-swap reload, so a
    // rotated cluster cert/key/CA is served on the next peer handshake.
    if let Some(parts) = peer_tls_reload {
        reloader.attach_peer_tls(parts.acceptor_tx, parts.connector_tx, parts.build);
    }

    // A cluster whose nodes all answer to the same name is broken in ways that
    // surface late and confusingly: session placement is keyed by node id (the HRW
    // ring), gossip identifies members by it, and a server-assigned client id is
    // qualified with it. Kubernetes users never meet this because the chart sets
    // MQTTD_NODE_ID from the pod name — which is exactly why it is worth saying out
    // loud for everyone deploying a cluster any other way.
    let clustered = config.cluster.peer_bind.is_some()
        || config.cluster.swim.bind.is_some()
        || !config.cluster.peers.is_empty()
        || !config.cluster.swim.seeds.is_empty();
    if clustered && config.node.id == mqtt_config::DEFAULT_NODE_ID {
        warn!(
            node_id = %config.node.id,
            "clustering is configured but MQTTD_NODE_ID is still the default — every \
             node will answer to the same id, which breaks session placement, gossip \
             identity, and server-assigned client ids. Set a UNIQUE MQTTD_NODE_ID per node."
        );
    }

    // Cluster peer mesh (opt-in).
    let peer_bind = config.cluster.peer_bind.clone();
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
            durable_plane.clone(),
        ));
    }
    for addr in &config.cluster.peers {
        info!(%addr, "dialing cluster peer (static)");
        tokio::spawn(peer::dial_forever(
            addr.clone(),
            node_id.clone(),
            hub_tx.clone(),
            peer_tls.clone(),
            durable_plane.clone(),
        ));
    }

    // SWIM gossip membership (opt-in): discovers peers and drives the peer mesh,
    // replacing the need for a static MQTTD_PEERS list.
    start_swim(
        &config,
        &node_id,
        peer_bind,
        &hub_tx,
        peer_tls.as_ref(),
        placement,
        &shutdown,
        metrics.clone(),
        durable_plane.clone(),
        cluster_identity.clone(),
        swim_key_fps.clone(),
        foreign_cluster_seen.clone(),
    )
    .await?;

    // Client listeners. TLS is the intended path; plaintext is a loudly-logged
    // local-testing escape hatch. The serve loops stop themselves on `shutdown`. The TLS
    // branch registers its acceptor with the reloader so SIGHUP also rotates cert/key/CA.
    // Revocation reaches live state (ADR 0040 T2): after every successful reload the
    // hub sweeps online sessions against the new policy — a CRL'd certificate, a
    // removed password user, or a connect-ACL deny evicts the live session. The
    // client-CRL serials mirror the same MQTTD_TLS_CRL file the TLS verifier enforces
    // per handshake; parsing it is part of the same validate-before-swap reload.
    let client_crl_build = config.tls.crl.clone().map(|path| {
        Box::new(move || {
            let bytes = std::fs::read(&path).map_err(|e| format!("read client crl {path}: {e}"))?;
            mqtt_auth::signed_gossip::RevocationList::from_bytes_unverified(&bytes)
                .map_err(|e| format!("parse client crl {path}: {e}"))
        }) as Box<dyn Fn() -> reload::ClientCrlBuildResult + Send + Sync>
    });
    reloader.attach_identity_sweep(hub_tx.clone(), client_crl_build);
    // ADR 0046 T4: whole-config hot reload. On SIGHUP / watch the reloader now re-loads the
    // config file, validates it (validate-before-swap), swaps `live_config`, rebuilds the policy
    // from it, pushes the live-swappable settings (quotas) to the hub, and logs every changed
    // non-live section as requires-restart. A bad edit is rejected and the running config kept.
    {
        let hub_for_apply = hub_tx.clone();
        let audit_for_apply = policy.audit.clone();
        reloader.attach_config_source(reload::ConfigSource {
            live: live_config.clone(),
            path: config_path()?,
            precheck: Box::new(runtime_precheck),
            apply: Box::new(move |old, new| {
                apply_live_config(old, new, &hub_for_apply, &audit_for_apply);
            }),
        });
    }
    // Disk visibility + watermark brownout (ADR 0041 T5): stat the redb stores
    // periodically, export store_bytes{store}, and drive the hub's brownout flag
    // when MQTTD_STORE_MAX_BYTES is configured.
    if let Some(dir) = &config.node.data_dir {
        // ADR 0041 T5: the disk high-water cap. A configured zero is meaningless (it would
        // brown out immediately), so reject it as the env path did.
        let max_bytes = match config.durable.store_max_bytes {
            Some(0) => return Err("store_max_bytes must be a positive integer".into()),
            other => other,
        };
        if let Some(max) = max_bytes {
            info!(max, "disk watermark active (ADR 0041): brownout above it");
        }
        tokio::spawn(mqttd::store_watch::watch(
            std::path::PathBuf::from(dir),
            max_bytes,
            hub_tx.clone(),
            Some(metrics.clone()),
            None,
            Some(store_snapshot.clone()),
        ));
    }

    // Process-memory visibility + watermark brownout (ADR 0041 T8): sample RSS
    // periodically, export process_resident_bytes, and drive the hub's brownout flag on
    // the memory axis when MQTTD_MEMORY_MAX_BYTES is configured. Runs regardless of a
    // data dir — memory pressure is not a durable-store concern.
    let memory_max_bytes = match config.limits.memory_max_bytes {
        // A configured zero would brown out immediately and never recover; reject it
        // rather than accept an instruction that cannot have been meant.
        Some(0) => return Err("memory_max_bytes must be a positive integer".into()),
        other => other,
    };
    if let Some(max) = memory_max_bytes {
        info!(
            max,
            "memory watermark active (ADR 0041 T8): brownout above it. This is a \
             watermark, not a ceiling — keep the container/cgroup memory limit as the \
             hard bound"
        );
    }
    tokio::spawn(mqttd::memory_watch::watch(
        memory_max_bytes,
        hub_tx.clone(),
        Some(metrics.clone()),
        None,
        None,
    ));

    // Per-client and global state quotas (ADR 0041 T3/T4), configured once before
    // any listener accepts. Unset = uncapped; a non-positive or unparseable value
    // is a startup error. Live-swappable on config reload (ADR 0046 T4).
    let quotas = quotas_from_config(&config)?;
    if quotas.max_subscriptions_per_client.is_some()
        || quotas.max_retained_messages.is_some()
        || quotas.max_sessions.is_some()
    {
        info!(?quotas, "state quotas active (ADR 0041)");
        let _ = hub_tx.send(mqttd::hub::HubCommand::SetQuotas(quotas));
    }

    // The hub consults the live authorizer when a persistent session resumes
    // (ADR 0040 T3): grants a tightening reload revoked while the session slept are
    // removed at resume, before any replay. Sent before any listener accepts.
    let _ = hub_tx.send(mqttd::hub::HubCommand::AttachAuthorizer(
        mqttd::hub::AuthzWatch(policy.authz.clone()),
    ));

    start_client_listeners(
        &config,
        hub_tx,
        policy,
        &mut reloader,
        &shutdown,
        &connections,
    )
    .await?;

    // Share the (now fully-configured) reloader between the SIGHUP handler and the optional
    // filesystem watcher; both drive the same validate-before-swap routine.
    let reloader = std::sync::Arc::new(reloader);

    // SIGHUP reloads the security policy (ACL + authenticator + TLS material) in place
    // (ADR 0032) — no restart, no dropped connections; a bad file keeps the running policy.
    spawn_reload_handler(reloader.clone());

    // Optional filesystem watcher (ADR 0033): MQTTD_CONFIG_WATCH=<seconds> auto-reloads when a
    // configured policy file changes on disk (the Kubernetes ConfigMap case), through the same
    // fail-safe reload. Off by default — signal-driven reload stays the default.
    spawn_config_watcher(&config, reloader, &shutdown);

    // Run until a shutdown signal, then drain gracefully (ADR 0019).
    graceful_shutdown(
        Duration::from_secs(config.runtime.shutdown_grace_secs),
        &shutdown,
        &connections,
        &draining,
        plane_for_shutdown,
        lease_driver,
        node_id,
        decommission_slot,
        Some(metrics.clone()),
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
/// Build the connection-admission gate (ADR 0041 T1) from
/// `MQTTD_MAX_CONNECTIONS` / `MQTTD_MAX_CONNECTIONS_PER_IP`. Unset = uncapped
/// (today's behavior); a value that does not parse as a positive integer is a
/// startup error, not a silent misconfiguration.
fn admission_gate(
    config: &Config,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    audit: Option<Arc<dyn mqtt_observability::AuditSink>>,
) -> Result<admission::AdmissionGate, Box<dyn std::error::Error>> {
    let max_connections = positive_cap("limits.max_connections", config.limits.max_connections)?;
    let max_per_ip = positive_cap(
        "limits.max_connections_per_ip",
        config.limits.max_connections_per_ip,
    )?;
    // Auth-failure penalty box (ADR 0041 T2): threshold enables it; the decay is
    // how long one strike takes to age away (default 60s).
    let penalty = match config.security.auth_penalty.threshold {
        None | Some(0) => None,
        Some(threshold) => {
            let decay_secs = config.security.auth_penalty.decay_secs.unwrap_or(60);
            Some(admission::PenaltyConfig {
                threshold,
                decay: std::time::Duration::from_secs(decay_secs),
            })
        }
    };
    if max_connections.is_some() || max_per_ip.is_some() || penalty.is_some() {
        info!(
            ?max_connections,
            ?max_per_ip,
            ?penalty,
            "connection admission caps active (ADR 0041): over-cap or penalized \
             connections are closed at accept, before any TLS work"
        );
    }
    Ok(admission::AdmissionGate::with_penalty(
        max_connections,
        max_per_ip,
        penalty,
        metrics,
        audit,
    ))
}

/// Prove a TLS material file is readable *before* it reaches rustls, so the failure names the
/// **environment variable** that pointed at it as well as the path.
///
/// The loaders in `mqtt_net::tls` name the file they could not read, but they cannot know
/// which setting chose it — and one acceptor is built from up to four paths (cert, key, client
/// CA, CRL) while the cluster-bus context is built from another three. An operator who has
/// just been told to edit five marked lines in `mqttd.env` needs to know *which* line is
/// wrong, and on a host where the file exists but the broker's service account cannot read it
/// the filename alone says nothing. Checked in the order the env file lists them, so the
/// message names the first setting to fix rather than an arbitrary one.
fn tls_path_readable(var: &str, path: &str) -> Result<(), String> {
    std::fs::File::open(path)
        .map(|_| ())
        .map_err(|e| format!("cannot read {var} ({path}): {e}"))
}

// One linear listener-wiring flow; splitting it would scatter the env-var reads.
#[allow(clippy::too_many_lines)]
async fn start_client_listeners(
    config: &Config,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    reloader: &mut reload::Reloader,
    shutdown: &tokio_util::sync::CancellationToken,
    connections: &tokio_util::task::TaskTracker,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut any = false;
    // Connection admission caps (ADR 0041 T1), shared by every client listener.
    let gate = admission_gate(config, policy.metrics.clone(), Some(policy.audit.clone()))?;
    let tls_bind = config.listeners.tls_bind.clone();
    let wss_bind = config.listeners.wss_bind.clone();

    // A single reloadable client-TLS acceptor, shared by the TLS and WSS listeners (ADR 0035
    // WSS reuses the ADR 0002 TLS stack + the ADR 0032 reloadable acceptor — one TLS path).
    let acceptor_rx = if tls_bind.is_some() || wss_bind.is_some() {
        let (Some(cert), Some(key)) = (config.tls.cert.clone(), config.tls.key.clone()) else {
            return Err(
                "tls_bind / wss_bind require a TLS cert and key (MQTTD_TLS_CERT / MQTTD_TLS_KEY)"
                    .into(),
            );
        };
        let client_ca = config.tls.client_ca.clone();
        // Optional certificate revocation list (ADR 0002 T8): a client whose cert is listed is
        // rejected at the TLS handshake. Reloadable on SIGHUP via the same closure below, so a
        // freshly-published CRL takes effect on the next handshake with no restart (ADR 0032 §5).
        let crl = config.tls.crl.clone();
        // Named-variable readability check (see `tls_path_readable`): all four of these come
        // from different lines of the operator's environment file, and rustls would report
        // only the filename.
        tls_path_readable("MQTTD_TLS_CERT", &cert)?;
        tls_path_readable("MQTTD_TLS_KEY", &key)?;
        if let Some(p) = &client_ca {
            tls_path_readable("MQTTD_TLS_CLIENT_CA", p)?;
        }
        if let Some(p) = &crl {
            tls_path_readable("MQTTD_TLS_CRL", p)?;
        }
        // Resumption cache sized for the fleet (MQTTD_TLS_SESSION_CACHE; 0 disables) —
        // rustls' own 256-entry default is no resumption at all once more devices than
        // that reconnect, and battery-powered clients pay a full handshake every time.
        let session_cache = config
            .tls
            .session_cache
            .unwrap_or(tls::DEFAULT_SESSION_CACHE);
        // A relaxation of a thing that is off cannot mean anything — refuse the
        // combination rather than silently ignoring half of it (the CRL-without-CA rule).
        if config.tls.allow_unsafe_tls12_features && !config.tls.allow_tls12 {
            return Err(
                "MQTTD_TLS_ALLOW_UNSAFE_TLS12_FEATURES requires MQTTD_TLS_ALLOW_TLS12 — \
                 there is no TLS 1.2 posture to relax while TLS 1.2 is off"
                    .into(),
            );
        }
        let tls12 = match (
            config.tls.allow_tls12,
            config.tls.allow_unsafe_tls12_features,
        ) {
            (false, _) => tls::Tls12::Off,
            (true, false) => tls::Tls12::Hardened,
            (true, true) => tls::Tls12::UnsafeLegacyFeatures,
        };
        if config.tls.allow_tls12 {
            // The same register as the other posture reductions: impossible to miss in
            // the log, stated at every start, never silent. The README advertises
            // 1.3-only as the default posture, and this is the one sanctioned exception.
            warn!(
                "REDUCED TLS POSTURE: TLS 1.2 clients are admitted on the TLS listener \
                 (MQTTD_TLS_ALLOW_TLS12) — hardened: ECDHE+AEAD suites only, Extended \
                 Master Secret required. Intended only for device fleets that cannot \
                 negotiate TLS 1.3. The cluster bus and QUIC remain 1.3-only."
            );
        }
        if config.tls.allow_unsafe_tls12_features {
            warn!(
                "UNSAFE TLS 1.2 FEATURES ENABLED (MQTTD_TLS_ALLOW_UNSAFE_TLS12_FEATURES): \
                 Extended Master Secret is no longer required, reopening the \
                 triple-handshake surface for clients that do not offer it. Use only for \
                 legacy firmware that predates RFC 7627, and plan its retirement."
            );
        }
        let acceptor = tls::server_acceptor_versions(
            Path::new(&cert),
            Path::new(&key),
            client_ca.as_deref().map(Path::new),
            crl.as_deref().map(Path::new),
            session_cache,
            tls12,
        )?;
        // Register the acceptor for SIGHUP reload (ADR 0032 T6): the closure re-reads the
        // same paths so a renewed cert/key/client-CA — and an updated CRL — is served on the
        // next handshake.
        Some(reloader.attach_tls(acceptor, move || {
            tls::server_acceptor_versions(
                Path::new(&cert),
                Path::new(&key),
                client_ca.as_deref().map(Path::new),
                crl.as_deref().map(Path::new),
                session_cache,
                tls12,
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
            gate.clone(),
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
            gate.clone(),
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
    if let Some(addr) = config.listeners.plaintext_bind.clone() {
        warn!(%addr, "INSECURE: starting PLAINTEXT MQTT listener (no TLS) — testing use only");
        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "accepting MQTT 3.1.1 clients");
        tokio::spawn(serve_plaintext_clients(
            gate.clone(),
            listener,
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if let Some(addr) = config.listeners.ws_bind.clone() {
        warn!(%addr, "INSECURE: starting PLAINTEXT WebSocket listener (no TLS) — testing use only");
        let listener = TcpListener::bind(&addr).await?;
        info!(%addr, "accepting MQTT clients over WebSocket (ws, ADR 0035)");
        tokio::spawn(serve_ws_clients(
            gate.clone(),
            listener,
            hub_tx.clone(),
            policy.clone(),
            shutdown.clone(),
            connections.clone(),
        ));
        any = true;
    }
    if let Some(bind) = config.listeners.quic_bind.clone() {
        // QUIC mandates TLS 1.3 (no plaintext mode); it reuses the same cert material as the
        // TLS listener. The endpoint is built once (cert hot-reload is a follow-on, ADR 0036).
        let (Some(cert), Some(key)) = (config.tls.cert.clone(), config.tls.key.clone()) else {
            return Err(
                "quic_bind requires a TLS cert and key (MQTTD_TLS_CERT / MQTTD_TLS_KEY)".into(),
            );
        };
        let client_ca = config.tls.client_ca.clone();
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
            gate.clone(),
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
fn client_policy(
    live: &Arc<RwLock<Config>>,
    // This node's id, passed explicitly rather than read off `proxy`: what this
    // node is called and whether session relocation is configured are two separate
    // facts, and a server-assigned client id must stay unique across the cluster
    // even if they ever stop coinciding.
    node: &NodeId,
    proxy: Option<conn::ProxyContext>,
    store: Arc<dyn SessionStore>,
    shutdown: tokio_util::sync::CancellationToken,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
) -> Result<(Arc<conn::ConnPolicy>, reload::Reloader), Box<dyn std::error::Error>> {
    let audit: Arc<dyn AuditSink> = Arc::new(AuditLog::new());
    // Build the initial policy, and a closure that re-reads the configured files on reload
    // (ADR 0032). The closure returns the freshly-built (authorizer, authenticator) or an
    // error string that aborts the swap — validate-before-swap lives in `reload::Reloader`.
    // The closure reads the *current* config snapshot from `live` each call (ADR 0046 T4): a
    // config-file reload swaps `live` first, so a changed ACL/password/JWT path — not just the
    // file contents at a fixed path — is picked up here.
    // OIDC-mode token auth (ADR 0050) is built ONCE, outside the reload closure: its JWKS
    // cache (last-known-good keys) and single fetch loop must survive policy hot-reloads —
    // a reload rebuilds the chain around the same Arc. OIDC settings are start-time.
    let oidc_auth = {
        let snap = live
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        oidc_from_config(&snap, shutdown.clone())?
    };
    let initial: (Arc<dyn Authorizer>, Arc<dyn Authenticator>) = {
        let snap = live
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (
            authorizer_from_config(&snap)?,
            authenticator_from_config(&snap, oidc_auth.clone(), Some(&metrics))?,
        )
    };
    let build = {
        let live = live.clone();
        let oidc_auth = oidc_auth.clone();
        let metrics = metrics.clone();
        move || -> reload::BuildResult {
            let snap = live
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let authz = authorizer_from_config(&snap).map_err(|e| e.to_string())?;
            let auth = authenticator_from_config(&snap, oidc_auth.clone(), Some(&metrics))
                .map_err(|e| e.to_string())?;
            Ok((authz, auth))
        }
    };
    let (reloader, handles) =
        reload::Reloader::with_metrics(initial, audit.clone(), Some(metrics.clone()), build);

    let policy = Arc::new(conn::ConnPolicy {
        auth: handles.auth,
        authz: handles.authz,
        // Start-time, like the OIDC settings above: re-keying every ACL under live
        // sessions is a restart-level change, and `requires_restart` reports an edit as
        // such (ADR 0046 T4). Validated at config load, so `parse` cannot fail here.
        identity_source: identity_source_from_config(
            &live
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        ),
        audit,
        proxy,
        node: Some(node.clone()),
        store: Some(store),
        connect_timeout: conn::DEFAULT_CONNECT_TIMEOUT,
        enhanced: None,
        shutdown: Some(shutdown),
        metrics: Some(metrics),
    });
    Ok((policy, reloader))
}

/// Which field of a verified client certificate is the identity (ADR 0004 T11).
///
/// `Config::validate` has already rejected an unrecognised spelling at load, so an
/// unparseable value here cannot come from a validated config; it is treated as the
/// secure-by-default CN and logged rather than panicking in the connection path.
fn identity_source_from_config(config: &Config) -> mqtt_auth::mtls::IdentitySource {
    let Some(raw) = config.security.mtls_identity_source.as_deref() else {
        return mqtt_auth::mtls::IdentitySource::default();
    };
    match mqtt_auth::mtls::IdentitySource::parse(raw) {
        Ok(source) => {
            if source != mqtt_auth::mtls::IdentitySource::default() {
                info!(
                    source = %source,
                    "mTLS identity is read from a Subject Alternative Name, not the Common Name (ADR 0004 T11)"
                );
            }
            source
        }
        Err(bad) => {
            error!(
                value = %bad,
                "unrecognised security.mtls_identity_source; falling back to the Common Name"
            );
            mqtt_auth::mtls::IdentitySource::default()
        }
    }
}

/// Build the topic authorizer (ADR 0004 step 3): a TOML ACL file gives deny-by-default
/// per-identity topic policy; without one, authorization is not enforced — loudly. Reads
/// the file fresh each call, so it is reusable at startup *and* on a SIGHUP reload (ADR 0032).
fn authorizer_from_config(
    config: &Config,
) -> Result<Arc<dyn Authorizer>, Box<dyn std::error::Error>> {
    if let Some(path) = &config.security.acl_file {
        tls_path_readable("MQTTD_ACL_FILE", path)?;
        let text = std::fs::read_to_string(path)?;
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
            info!("SIGHUP received — reloading configuration (ADR 0046 T4) + security policy");
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
fn watched_policy_paths(config: &Config) -> Vec<std::path::PathBuf> {
    [
        config.security.acl_file.as_ref(),
        config.security.password_file.as_ref(),
        config.security.jwt.hs256_secret_file.as_ref(),
        config.security.jwt.rs256_pem_file.as_ref(),
        config.tls.cert.as_ref(),
        config.tls.key.as_ref(),
        config.tls.client_ca.as_ref(),
        config.tls.crl.as_ref(),
        config.cluster.peer_tls.crl.as_ref(),
    ]
    .into_iter()
    .flatten()
    .map(std::path::PathBuf::from)
    .collect()
}

/// Spawn the opt-in filesystem watcher (ADR 0033) when `runtime.config_watch_secs`
/// (`MQTTD_CONFIG_WATCH`) is non-zero (0 = disabled, the signal-only default). Polls the
/// configured policy files and auto-reloads through the same fail-safe routine as `SIGHUP`.
fn spawn_config_watcher(
    config: &Config,
    reloader: std::sync::Arc<reload::Reloader>,
    shutdown: &tokio_util::sync::CancellationToken,
) {
    let secs = config.runtime.config_watch_secs;
    if secs == 0 {
        return; // unset / explicitly disabled — signal-only default
    }
    let mut paths = watched_policy_paths(config);
    // ADR 0046 T4: also watch the config file itself, so editing it (a Kubernetes ConfigMap
    // update) auto-reloads the whole config through the same fail-safe path as SIGHUP.
    if let Ok(Some(p)) = config_path() {
        paths.push(p);
    }
    if paths.is_empty() {
        warn!("config_watch is set but no watchable files are configured — watcher idle");
        return;
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
}

/// Build the CONNECT authenticator (ADR 0004 steps 2 + 6): a certificate /
/// anonymous baseline, then — when configured — an Argon2id password file
/// (`MQTTD_PASSWORD_FILE`) and a JWT verifier (`MQTTD_JWT_HS256_SECRET` or
/// `MQTTD_JWT_RS256_PEM`, with optional `MQTTD_JWT_ISSUER`/`MQTTD_JWT_AUDIENCE`).
/// Credentials are tried cert → password → token via a chain.
fn authenticator_from_config(
    config: &Config,
    oidc: Option<Arc<mqtt_auth::oidc::OidcAuthenticator>>,
    metrics: Option<&Arc<mqtt_observability::metrics::Metrics>>,
) -> Result<Arc<dyn Authenticator>, Box<dyn std::error::Error>> {
    let allow_anonymous = config.security.allow_anonymous;
    if allow_anonymous {
        warn!(
            "INSECURE: anonymous MQTT clients are PERMITTED (allow_anonymous / \
             MQTTD_ALLOW_ANONYMOUS) — testing use only"
        );
    }
    let mut members: Vec<Arc<dyn Authenticator>> =
        vec![Arc::new(BasicAuthenticator { allow_anonymous })];

    if let Some(path) = &config.security.password_file {
        tls_path_readable("MQTTD_PASSWORD_FILE", path)?;
        let text = std::fs::read_to_string(path)?;
        let pw = mqtt_auth::password::PasswordAuthenticator::from_file_contents(&text)?;
        info!(%path, "Argon2id password file loaded");
        members.push(Arc::new(pw));
    }

    // Secrets by reference (ADR 0046 T5): the HS256 shared secret is read from the file at
    // `hs256_secret_file` (like the RS256 PEM), so the HMAC key is mounted from a Secret, never
    // inlined in the config. Whitespace trimming follows the one shared rule in
    // `mqtt_core::secrets` so the on-disk file and an inline value agree.
    if let Some(path) = &config.security.jwt.hs256_secret_file {
        let secret = mqtt_core::read_secret_file(path)?;
        info!(%path, "JWT HS256 verification enabled (secret from file)");
        members.push(Arc::new(mqtt_auth::token::TokenAuthenticator::hs256(
            &secret,
            jwt_config(config),
        )));
    } else if let Some(pem_path) = &config.security.jwt.rs256_pem_file {
        let pem = std::fs::read(pem_path)?;
        let tok = mqtt_auth::token::TokenAuthenticator::rs256_pem(&pem, jwt_config(config))?;
        info!(%pem_path, "JWT RS256 verification enabled");
        members.push(Arc::new(tok));
    }

    // The remote hook (ADR 0004 T16) goes LAST among password verifiers: a local password
    // file is cheaper and does not depend on a network, so a user present in both is
    // answered without a round trip. The chain stops at the first real verdict, so a local
    // file that REJECTS also stops here — which is why the hook is for users the file does
    // not contain, not a fallback for ones it refuses.
    if let Some(http_cfg) =
        mqttd::http_auth::HttpAuthConfig::from_config(&config.security.http_auth)?
    {
        info!(
            url = %http_cfg.url,
            timeout_s = http_cfg.timeout.as_secs_f64(),
            cache_s = http_cfg.cache_ttl.as_secs_f64(),
            "HTTP authentication hook enabled — an unreachable hook DENIES (fail closed)"
        );
        members.push(Arc::new(mqttd::http_auth::HttpAuthenticator::new(
            http_cfg,
            metrics.cloned(),
        )?));
    }

    if let Some(oidc) = oidc {
        // The chain stops at the first real verdict on a credential kind, so a static JWT
        // verifier ahead of OIDC would shadow it: the two are mutually exclusive (ADR 0050
        // §1 — no silent fallback between key sources).
        if config.security.jwt.hs256_secret_file.is_some()
            || config.security.jwt.rs256_pem_file.is_some()
        {
            return Err(
                "MQTTD_OIDC_ISSUER and MQTTD_JWT_* are mutually exclusive: configure one                  token verifier"
                    .into(),
            );
        }
        members.push(oidc);
    }
    Ok(Arc::new(mqtt_auth::chain::ChainAuthenticator::new(members)))
}

/// Build the OIDC-mode authenticator (ADR 0050) and spawn its JWKS fetch loop, once per
/// process. `None` when OIDC is not configured. Startup errors on a non-https issuer
/// (without the loud test override) or a missing audience — fail closed at config time.
fn oidc_from_config(
    config: &Config,
    shutdown: tokio_util::sync::CancellationToken,
) -> Result<Option<Arc<mqtt_auth::oidc::OidcAuthenticator>>, Box<dyn std::error::Error>> {
    let Some(issuer) = config.security.oidc.issuer.clone() else {
        return Ok(None);
    };
    let allow_http = config.security.oidc.allow_http;
    if !issuer.starts_with("https://") {
        if !allow_http {
            return Err(format!(
                "MQTTD_OIDC_ISSUER must be https ({issuer}); MQTTD_OIDC_ALLOW_HTTP overrides                  for tests only"
            )
            .into());
        }
        warn!(%issuer, "INSECURE: OIDC issuer over plaintext http (MQTTD_OIDC_ALLOW_HTTP) — testing use only");
    }
    let Some(audience) = config.security.oidc.audience.clone() else {
        return Err("MQTTD_OIDC_AUDIENCE is required with MQTTD_OIDC_ISSUER (ADR 0050:                     audience validation is not optional in OIDC mode)"
            .into());
    };
    let mut cfg = mqtt_auth::oidc::OidcConfig::new(issuer.clone(), audience);
    if let Some(s) = config.security.oidc.max_stale_secs {
        cfg.max_stale = Duration::from_secs(s);
    }
    if let Some(c) = config.security.oidc.groups_claim.clone() {
        cfg.groups_claim = c;
    }
    let refresh = Duration::from_secs(config.security.oidc.jwks_refresh_secs.unwrap_or(300).max(5));
    let (auth, hints) = mqtt_auth::oidc::OidcAuthenticator::new(cfg);
    info!(%issuer, refresh_s = refresh.as_secs(), "OIDC token authentication enabled (ADR 0050); fail-closed until the first JWKS load");
    tokio::spawn(mqttd::oidc::run_fetch_loop(
        auth.clone(),
        issuer,
        allow_http,
        refresh,
        hints,
        shutdown,
    ));
    Ok(Some(auth))
}

/// Assemble JWT validation options from the optional issuer/audience config.
fn jwt_config(config: &Config) -> mqtt_auth::token::TokenConfig {
    mqtt_auth::token::TokenConfig {
        issuer: config.security.jwt.issuer.clone(),
        audience: config.security.jwt.audience.clone(),
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

/// Log the durability posture at startup (#166). Ephemeral durability — durable ON with
/// no `MQTTD_DATA_DIR`, so the replicated state is only in RAM — is announced in the same
/// loud register as the other degraded modes (plaintext, unauthenticated gossip), because
/// logging it as a plain "DURABLE sessions enabled" line is a durability lie an operator
/// reads and trusts.
fn log_durability_mode(config: &Config, founder: bool, voter_cap: usize, failure_domains: usize) {
    if config.durability_is_ephemeral() {
        warn!(
            founder,
            voter_cap,
            "EPHEMERAL durability: durable sessions are ON but no MQTTD_DATA_DIR is set — \
             the replicated state lives only in MEMORY. A single node's loss is survived \
             (peers hold it), but a correlated restart of a quorum LOSES acknowledged \
             facts. Set MQTTD_DATA_DIR and mount a volume for real durability; this mode \
             is for development and tests."
        );
    } else {
        info!(
            founder,
            voter_cap,
            failure_domains,
            min_replicas = config.durable.min_replicas,
            "DURABLE sessions enabled: consensus-backed replicated store (on disk)"
        );
    }
    if config.durable.min_replicas > 1 {
        info!(
            floor = config.durable.min_replicas,
            "min-replicas write floor active (issue #167): a placement group whose \
             replica set shrinks below the floor REFUSES durable writes (QoS>=1 acks \
             withheld, retained mutations queue) until capacity returns"
        );
    }
}

async fn start_hub(
    config: &Config,
    node_id: &NodeId,
    placement: &Arc<RwLock<Placement>>,
    metrics: &Arc<mqtt_observability::metrics::Metrics>,
    brownout_status: &Arc<mqttd::health::BrownoutStatus>,
) -> Result<HubHandle, Box<dyn std::error::Error>> {
    // Claim the data directory for this node (ADR 0018 phase 5): refuse to open another
    // node's persistent state, before any store touches disk.
    if let Some(dir) = &config.node.data_dir {
        mqtt_storage::data_dir::guard_data_dir(dir, &node_id.0)?;
    }
    // Durable is the **default** (ADR 0029): the consensus-backed replicated store is on
    // unless explicitly opted out. `0/false/off/no` selects the lightweight in-memory store.
    let durable = config.durable.enabled;
    // Cluster-configured = peer networking is set up. Told to the hub explicitly
    // (0043-P4 exhibit ②): a restarted cluster node sees a single-member ring for
    // its first moments, and judging "clustered" by live membership would switch
    // every cluster honesty gate off exactly while its view is most incomplete.
    let cluster_configured = config.cluster.peer_bind.is_some()
        || !config.cluster.peers.is_empty()
        || config.cluster.swim.bind.is_some()
        || !config.cluster.swim.seeds.is_empty();
    if durable {
        // A node started with no SWIM seeds is the cluster founder — only it
        // bootstraps the lease group (ADR 0007 §2). Exactly one founder per cluster.
        let founder = config.cluster.swim.seeds.is_empty();
        // Persist the lease store and follower replica copy on disk when a data dir
        // is set (ADR 0018 phases 2–3): the lease vote/assignments and the replicated
        // session log survive a restart (restoring Raft safety and full-cluster-restart
        // durability). Without it the durable plane is in-memory (rebuilds from peers).
        let data_dir = config.node.data_dir.clone();
        // Bound the lease-consensus voter set (ADR 0021): at most `N` members vote, the
        // rest join as learners that still receive the lease log. Default 5 (recommend
        // odd); decouples consensus cost from cluster size. `validate()` guarantees ≥ 1.
        let voter_cap = config.durable.lease_voters as usize;
        // Failure-domain topology (ADR 0016 T4): spread the bounded voter set across racks/zones.
        let domains: std::collections::BTreeMap<NodeId, String> = config
            .node
            .failure_domains
            .iter()
            .map(|(node, domain)| (NodeId(node.clone()), domain.clone()))
            .collect();
        log_durability_mode(config, founder, voter_cap, domains.len());
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
        if cluster_configured {
            hub.set_cluster_configured();
        }
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
        hub.attach_brownout_status(brownout_status.clone());
        tokio::spawn(hub.run());
        Ok((hub_tx, store, Some(plane_for_health), Some(driver)))
    } else if let Some(dir) = config.node.data_dir.clone() {
        // Single-node **persistent** sessions (ADR 0018 phase 1): the session log is
        // backed by an on-disk redb database, so sessions, subscriptions, the QoS-2
        // dedup window and offline queues survive a restart. Not replicated — use
        // durable sessions for cluster (quorum) durability.
        let path = std::path::Path::new(&dir).join("sessions.redb");
        info!(
            path = %path.display(),
            "PERSISTENT sessions: on-disk durable store (ADR 0018; single-node, not replicated)"
        );
        let log = PersistentLog::open(&path)?;
        let store: Arc<dyn SessionStore> = Arc::new(ReplicatedSessionStore::with_limits(
            log,
            queue_limits_from_config(config)?,
        ));
        let (mut hub, hub_tx) = hub::Hub::with_config_and_placement(
            node_id.clone(),
            store.clone(),
            Some(placement.clone()),
        );
        if cluster_configured {
            hub.set_cluster_configured();
        }
        hub.attach_retained_store(persistent_retained(&dir)?); // ADR 0018 phase 4
        hub.attach_metrics(metrics.clone());
        hub.attach_brownout_status(brownout_status.clone());
        tokio::spawn(hub.run());
        Ok((hub_tx, store, None, None))
    } else {
        let store: Arc<dyn SessionStore> = Arc::new(MemorySessionStore::with_limits(
            queue_limits_from_config(config)?,
        ));
        let (mut hub, hub_tx) = hub::Hub::with_config_and_placement(
            node_id.clone(),
            store.clone(),
            Some(placement.clone()),
        );
        if cluster_configured {
            hub.set_cluster_configured();
        }
        hub.attach_metrics(metrics.clone());
        hub.attach_brownout_status(brownout_status.clone());
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
#[allow(clippy::too_many_arguments)] // a wiring seam: one call site, named handles
async fn start_health(
    config: &Config,
    hub_tx: &mpsc::UnboundedSender<hub::HubCommand>,
    placement: &Arc<RwLock<Placement>>,
    durable_plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    node_id: &NodeId,
    cluster_identity: &Arc<mqtt_cluster::cluster_identity::ClusterIdentity>,
    brownout_status: &Arc<mqttd::health::BrownoutStatus>,
    store_snapshot: &Arc<mqttd::store_watch::StoreSnapshot>,
    config_stamp: &Arc<mqttd::reload::ConfigStamp>,
    swim_key_fps: &Arc<std::sync::OnceLock<Vec<String>>>,
    refound_evidence: Option<Arc<std::sync::atomic::AtomicBool>>,
) -> Result<
    (
        Arc<std::sync::atomic::AtomicBool>,
        Arc<std::sync::OnceLock<Arc<mqtt_cluster::decommission::DrainStatus>>>,
    ),
    Box<dyn std::error::Error>,
> {
    let health_bind = config.listeners.health_bind.clone();
    let metrics_bind = config.listeners.metrics_bind.clone();
    if health_bind.is_none() && metrics_bind.is_none() {
        // Neither server: hand back standalone handles so the caller's shutdown
        // path is uniform (nothing reads them).
        return Ok((
            Arc::new(std::sync::atomic::AtomicBool::new(false)),
            Arc::new(std::sync::OnceLock::new()),
        ));
    }
    // `validate()` guarantees ready_min_members ≥ 1.
    let min_members = config.runtime.ready_min_members;
    // One state serves both binds: health endpoints plus `/metrics` (ADR 0020).
    let state = mqttd::health::HealthState::new(
        hub_tx.clone(),
        Some(placement.clone()),
        durable_plane,
        min_members,
    )
    .with_metrics(metrics)
    // /statusz (ADR 0054): the structured operator-facing state body; the cluster
    // identity fills its founder flag and (once known) cluster_id.
    .with_status(
        node_id.0.clone(),
        cluster_identity.clone(),
        brownout_status.clone(),
        config_stamp.clone(),
        swim_key_fps.clone(),
        config
            .node
            .data_dir
            .as_ref()
            .map(|_| store_snapshot.clone()),
    );
    // Re-found self-quarantine (issue #92 follow-up): a node that minted an identity
    // this boot AND then hears gossip from another cluster refuses to serve.
    let state = match refound_evidence {
        Some(evidence) => state.with_refound_guard(evidence),
        None => state,
    };
    let draining = state.draining_handle();
    let decommission = state.decommission_slot();
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
    Ok((draining, decommission))
}

fn queue_limits_from_config(config: &Config) -> Result<QueueLimits, Box<dyn std::error::Error>> {
    let mut limits = QueueLimits::default();
    if let Some(max) = config.limits.max_queued_messages {
        limits.max_messages = usize::try_from(max)
            .map_err(|_| format!("limits.max_queued_messages is too large: {max}"))?;
    }
    // `validate()` already constrains the enum to drop-oldest|reject-newest; the fallthrough
    // stays as defence in depth.
    if let Some(raw) = &config.limits.queue_overflow {
        limits.overflow = match raw.as_str() {
            "drop-oldest" => OverflowPolicy::DropOldest,
            "reject-newest" => OverflowPolicy::RejectNewest,
            other => {
                return Err(format!(
                    "limits.queue_overflow must be drop-oldest or reject-newest, got {other:?}"
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

/// The reload half of the peer-bus TLS context (ADR 0040 T4): the `watch` senders
/// behind [`peer::PeerTls`]'s acceptor/connector plus the closure that re-reads the
/// PEM files — handed to [`reload::Reloader::attach_peer_tls`] once the reloader
/// exists, so a rotated cluster cert/key/CA is served on the next peer handshake.
struct PeerTlsReload {
    acceptor_tx: tokio::sync::watch::Sender<tokio_rustls::TlsAcceptor>,
    connector_tx: tokio::sync::watch::Sender<tokio_rustls::TlsConnector>,
    build: Box<dyn Fn() -> reload::PeerTlsBuildResult + Send + Sync>,
}

/// Build the cluster-bus mTLS context from `MQTTD_PEER_TLS_{CA,CERT,KEY}`.
/// All three must be set together; none means a (loudly logged) plaintext mesh.
/// `MQTTD_PEER_TLS_CRL` (optional, requires the other three) loads a cluster-CA-signed
/// CRL checked on every inbound signed-gossip datagram (ADR 0022 T7).
fn peer_tls_from_config(
    config: &Config,
) -> Result<Option<(peer::PeerTls, PeerTlsReload)>, Box<dyn std::error::Error>> {
    let crl_path = config.cluster.peer_tls.crl.clone();
    match (
        config.cluster.peer_tls.ca.clone(),
        config.cluster.peer_tls.cert.clone(),
        config.cluster.peer_tls.key.clone(),
    ) {
        (Some(ca), Some(cert), Some(key)) => {
            // Named-variable readability check (see `tls_path_readable`). This is the FIRST
            // material an unedited `mqttd.env.example` install reaches, so this message is
            // the one the systemd README quotes as the fail-closed symptom.
            tls_path_readable("MQTTD_PEER_TLS_CA", &ca)?;
            tls_path_readable("MQTTD_PEER_TLS_CERT", &cert)?;
            tls_path_readable("MQTTD_PEER_TLS_KEY", &key)?;
            if let Some(p) = &crl_path {
                tls_path_readable("MQTTD_PEER_TLS_CRL", p)?;
            }
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
            // The acceptor/connector live behind `watch` channels (ADR 0040 T4): the
            // senders + rebuild closure go to the reloader once it exists, so a
            // rotated cluster cert is served on the next peer handshake.
            let (acceptor_tx, acceptor) =
                tokio::sync::watch::channel(tls::server_acceptor(cert, key, Some(ca))?);
            let (connector_tx, connector) =
                tokio::sync::watch::channel(tls::client_connector(ca, cert, key)?);
            let build = {
                let (ca, cert, key) = (ca.to_path_buf(), cert.to_path_buf(), key.to_path_buf());
                Box::new(move || {
                    let acceptor =
                        tls::server_acceptor(&cert, &key, Some(&ca)).map_err(|e| e.to_string())?;
                    let connector =
                        tls::client_connector(&ca, &cert, &key).map_err(|e| e.to_string())?;
                    Ok((acceptor, connector))
                }) as Box<dyn Fn() -> reload::PeerTlsBuildResult + Send + Sync>
            };
            Ok(Some((
                peer::PeerTls {
                    acceptor,
                    connector,
                    // Raw DER kept for signed gossip (ADR 0022): the CA verifies inbound
                    // certs, and our leaf + key sign outbound datagrams.
                    cert_der: tls::first_cert_der(cert)?,
                    key_der: tls::private_key_der(key)?,
                    ca_der,
                    gossip_crl: Arc::new(std::sync::RwLock::new(gossip_crl)),
                    crl_path,
                },
                PeerTlsReload {
                    acceptor_tx,
                    connector_tx,
                    build,
                },
            )))
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
fn signed_gossip_from_config(
    config: &Config,
    has_tls: bool,
    has_key: bool,
) -> Result<SignedGossip, Box<dyn std::error::Error>> {
    Ok(match config.cluster.swim.signed.as_deref() {
        Some("require") => SignedGossip::Require,
        Some("off") => SignedGossip::Off,
        Some(other) => {
            return Err(
                format!("cluster.swim.signed must be one of require|off (got {other:?})").into(),
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
    config: &Config,
    auth: Option<SwimAuth>,
    signed: SignedGossip,
) -> Result<(Option<SwimAuth>, Option<swim_driver::SeqAlloc>), Box<dyn std::error::Error>> {
    let mode = match config.cluster.swim.replay.as_deref() {
        Some("require") => ReplayMode::Require,
        Some("off") | None => ReplayMode::Off,
        Some(other) => {
            return Err(
                format!("cluster.swim.replay must be one of require|off (got {other:?})").into(),
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
            "cluster.swim.replay requires cluster.swim.signed=require: anti-replay binds the \
                    sequence to the per-node signature"
                .into(),
        );
    }
    let Some(dir) = &config.node.data_dir else {
        return Err(
            "cluster.swim.replay requires a data dir (node.data_dir) for the persisted, \
                    restart-safe sequence counter"
                .into(),
        );
    };
    let store = FileSeqStore::open(Path::new(dir).join("gossip-seq"))?;
    let alloc = mqtt_cluster::replay::SequenceAllocator::open(
        Box::new(store) as Box<dyn mqtt_cluster::replay::SeqStore>,
        SEQ_BLOCK,
    );
    info!("SWIM gossip anti-replay enabled (ADR 0023)");
    Ok((Some(auth.with_sequencing()), Some(alloc)))
}

/// Start SWIM membership from `MQTTD_SWIM_{BIND,SEEDS}` (no-op when unset) and
/// hand its events to the peer-link manager.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)] // one wiring seam
async fn start_swim(
    config: &Config,
    node_id: &NodeId,
    peer_bind: Option<String>,
    hub_tx: &mpsc::UnboundedSender<hub::HubCommand>,
    peer_tls: Option<&peer::PeerTls>,
    placement: Arc<RwLock<Placement>>,
    shutdown: &tokio_util::sync::CancellationToken,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    cluster_identity: Arc<mqtt_cluster::cluster_identity::ClusterIdentity>,
    swim_key_fps: Arc<std::sync::OnceLock<Vec<String>>>,
    // Set on the first FOREIGN-cluster datagram — the evidence the re-found
    // self-quarantine keys on (issue #92 follow-up).
    foreign_cluster_seen: Arc<std::sync::atomic::AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(bind) = config.cluster.swim.bind.clone() else {
        return Ok(());
    };
    let Some(peer_addr) = peer_bind else {
        return Err(
            "swim.bind requires peer_bind (MQTTD_PEER_BIND): membership \
                    gossips the peer-link address so other nodes can dial us"
                .into(),
        );
    };
    // The address gossip ADVERTISES for this node's peer links (ADR 0044 P1):
    // defaults to the bind, overridable where the dialable address differs from
    // the bound one — NAT, container port mapping, or a fronting relay (the
    // out-of-process harness fronts each peer listener with one).
    let peer_addr = config.cluster.peer_advertise.clone().unwrap_or(peer_addr);
    // Gossip authentication (ADR 0003): keyed = membership claims require the
    // cluster key; unkeyed is possible but loudly insecure. The key is either inline
    // (`swim.key`) or read from a file (`swim.key_file`, ADR 0046 T5 secret-by-reference);
    // `validate()` guarantees at most one is set.
    let primary_key: Option<String> =
        match (&config.cluster.swim.key, &config.cluster.swim.key_file) {
            (Some(hex), _) => Some(hex.clone()),
            (None, Some(path)) => {
                // Named-variable readability check, same reason as `tls_path_readable`:
                // a bare `Os { code: 2 }` here is indistinguishable from an unreadable
                // password or ACL file, and the operator was just told to edit three
                // secrets-by-path lines (issue #254 round 3).
                tls_path_readable("MQTTD_SWIM_KEY_FILE", path)?;
                Some(String::from_utf8_lossy(&mqtt_core::read_secret_file(path)?).to_string())
            }
            (None, None) => None,
        };
    let auth = if let Some(hex) = &primary_key {
        let mut auth = SwimAuth::from_hex_key(hex)?;
        // Additional keys accepted (but not used to seal) during a rotation window (ADR
        // 0003): an old key still opens peers' datagrams while the cluster migrates to the
        // new primary, so the gossip key rotates without downtime.
        let mut rotation = 0;
        for k in config
            .cluster
            .swim
            .key_accept
            .iter()
            .filter(|k| !k.is_empty())
        {
            auth = auth.accept_also_hex(k)?;
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
        if !config.cluster.swim.key_accept.is_empty() {
            return Err(
                "swim.key_accept requires swim.key (MQTTD_SWIM_KEY): rotation keys are \
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
    let signed = signed_gossip_from_config(config, peer_tls.is_some(), auth.is_some())?;
    let auth = apply_signed_gossip(auth, peer_tls, signed)?;
    let (auth, seq_alloc) = apply_anti_replay(config, auth, signed)?;
    let socket = UdpSocket::bind(&bind).await?;
    let seeds: Vec<String> = config.cluster.swim.seeds.clone();
    info!(%bind, seeds = seeds.len(), authenticated = auth.is_some(), "starting SWIM gossip membership");
    // This process's gossip GENERATION (issue #92). A node id outlives the process
    // that holds it — a Kubernetes pod keeps its name across every restart — while
    // incarnation numbers do not survive a restart, so peers could not tell a
    // returning node from the one they had just buried: a `Dead` claim about the
    // previous life outranked the new process's `Alive` and killed it on a loop.
    // Wall-clock milliseconds at start orders the lives without any persisted state.
    // A clock that steps BACKWARDS across a restart would let the old life's claims
    // win again — no worse than the behaviour this replaces, and NTP-stepped hosts
    // do not do it between two pod starts.
    let generation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX));
    let swim = Swim::new(
        node_id.clone(),
        bind,
        peer_addr,
        // Advertise this node's own failure-domain label over gossip (ADR 0016 T5).
        config.node.failure_domain.clone(),
        generation,
        mqtt_cluster::swim::Config::default(),
        seeds,
    );
    let (event_tx, event_rx) = mpsc::unbounded_channel();
    // Count dropped gossip datagrams by reason on the metrics registry (ADR 0003-T6).
    let reject: swim_driver::RejectCounter = {
        let m = metrics.clone();
        let seen = foreign_cluster_seen.clone();
        Arc::new(move |reason: &'static str| {
            m.gossip_rejected(reason);
            // Foreign-cluster gossip is the evidence the self-quarantine keys on: another
            // cluster is answering for this address space. Log ONCE, on the transition —
            // this is the line that explains an otherwise inexplicable NotReady node.
            if reason == "cluster-mismatch" && !seen.swap(true, std::sync::atomic::Ordering::AcqRel)
            {
                warn!(
                    "gossip from a DIFFERENT cluster is arriving: if this node minted its \
                     own identity at boot it has re-founded beside a live cluster and will \
                     refuse to serve (see docs/OPERATIONS.md split-brain recovery); set \
                     MQTTD_REFOUND_GUARD=false only to re-bootstrap deliberately"
                );
            }
        })
    };
    // Rotation posture (ADR 0054 T3): the accepted-key fingerprints, for /statusz
    // and the swim_keys_accepted gauge (1 steady; 2 = a rotation window is open).
    let fps = auth
        .as_ref()
        .map(|a| a.key_fingerprints().to_vec())
        .unwrap_or_default();
    metrics.set_swim_keys_accepted(fps.len());
    let _ = swim_key_fps.set(fps);
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
        Some(cluster_identity),
        shutdown.clone().cancelled_owned(),
    ));
    tokio::spawn(cluster::maintain_peer_links(
        event_rx,
        node_id.clone(),
        hub_tx.clone(),
        peer_tls.cloned(),
        Some(placement),
        Some(metrics),
        plane,
    ));
    Ok(())
}

/// The shared accept loop behind every TCP-based client listener (TLS, plaintext,
/// WS, WSS): shutdown-select accept (ADR 0019), the admission gate BEFORE any
/// per-connection work (ADR 0041 T1), the accepted-connection metric under `label`,
/// `TCP_NODELAY`, and a tracked spawn that holds the admission permit for the
/// connection's lifetime. Everything protocol-specific — TLS/WebSocket handshakes,
/// mTLS identity extraction, the MQTT engine — happens in `per_conn`, which runs
/// inside the spawned task and returns `None` when the connection died before
/// reaching the engine (it logs/counts its own handshake failures). Auth-failure
/// bookkeeping happens HERE, once: an outcome with `auth_failed` feeds the gate's
/// penalty box (ADR 0041 T2), so no listener variant can forget it. QUIC keeps its
/// own loop below — quinn's accept/refuse handshake shape is not a `TcpListener` —
/// and must uphold this same contract by hand.
async fn serve_tcp_clients<F, Fut>(
    gate: admission::AdmissionGate,
    listener: TcpListener,
    label: &'static str,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
    per_conn: F,
) where
    F: Fn(tokio::net::TcpStream, std::net::SocketAddr) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Option<conn::ConnOutcome>> + Send + 'static,
{
    loop {
        let (stream, peer) = tokio::select! {
            // Graceful shutdown (ADR 0019): stop accepting; refuse new connections fast.
            () = shutdown.cancelled() => return,
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(e) => {
                    warn!(error = %e, listener = label, "listener accept failed");
                    if let Some(m) = &policy.metrics {
                        m.connection_error("accept");
                    }
                    return;
                }
            },
        };
        // Admission caps (ADR 0041 T1): refuse BEFORE any handshake or per-connection
        // work; dropping the stream closes it. Counted + logged by the gate.
        let Some(permit) = gate.try_admit(Some(peer.ip())) else {
            continue;
        };
        debug!(%peer, listener = label, "accepted connection");
        if let Some(m) = &policy.metrics {
            m.connection_accepted(label);
        }
        let _ = stream.set_nodelay(true);
        let conn_fut = per_conn(stream, peer);
        let gate = gate.clone();
        connections.spawn(async move {
            let _permit = permit; // slot freed when the connection task ends
            if let Some(outcome) = conn_fut.await {
                if outcome.auth_failed {
                    gate.record_auth_failure(Some(peer.ip()));
                }
            }
        });
    }
}

/// Accept TLS clients forever: per-connection handshake (off the accept loop so
/// a slow handshake cannot stall other clients), then normal MQTT handling.
async fn serve_tls_clients(
    gate: admission::AdmissionGate,
    listener: TcpListener,
    acceptor_rx: tokio::sync::watch::Receiver<TlsAcceptor>,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    let per_conn_policy = policy.clone();
    serve_tcp_clients(
        gate,
        listener,
        "tls",
        policy,
        shutdown,
        connections,
        move |stream, peer| {
            // Read the *current* acceptor per accept, so a SIGHUP cert/key/CA reload is
            // served on the next handshake (ADR 0032 T6); in-flight sessions are undisturbed.
            let acceptor = acceptor_rx.borrow().clone();
            let hub = hub_tx.clone();
            let policy = per_conn_policy.clone();
            async move {
                match acceptor.accept(stream).await {
                    Ok(tls_stream) => {
                        // mTLS admission (ADR 0004/0040): the verified leaf cert's CN + serial.
                        let cert = conn::tls_admission(&tls_stream, policy.identity_source);
                        Some(conn::handle_stream(tls_stream, Some(peer), cert, policy, hub).await)
                    }
                    Err(e) => {
                        debug!(%peer, error = %e, "TLS handshake failed");
                        if let Some(m) = &policy.metrics {
                            m.connection_error("tls");
                        }
                        None
                    }
                }
            }
        },
    )
    .await;
}

/// Accept plaintext clients forever (insecure; explicitly opted into).
async fn serve_plaintext_clients(
    gate: admission::AdmissionGate,
    listener: TcpListener,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    let per_conn_policy = policy.clone();
    serve_tcp_clients(
        gate,
        listener,
        "plaintext",
        policy,
        shutdown,
        connections,
        move |stream, peer| {
            let hub = hub_tx.clone();
            let policy = per_conn_policy.clone();
            async move { Some(conn::handle_stream(stream, Some(peer), None, policy, hub).await) }
        },
    )
    .await;
}

/// Accept MQTT-over-WebSocket clients over plaintext (insecure; explicitly opted into).
/// The WebSocket handshake (per connection, off the accept loop) yields a byte stream that
/// the MQTT engine reads exactly like a TCP socket (ADR 0035).
async fn serve_ws_clients(
    gate: admission::AdmissionGate,
    listener: TcpListener,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    let per_conn_policy = policy.clone();
    serve_tcp_clients(
        gate,
        listener,
        "ws",
        policy,
        shutdown,
        connections,
        move |stream, peer| {
            let hub = hub_tx.clone();
            let policy = per_conn_policy.clone();
            async move {
                match mqtt_net::ws::accept(stream).await {
                    Ok(ws) => Some(conn::handle_stream(ws, Some(peer), None, policy, hub).await),
                    Err(e) => {
                        debug!(%peer, error = %e, "websocket handshake failed");
                        if let Some(m) = &policy.metrics {
                            m.connection_error("ws");
                        }
                        None
                    }
                }
            }
        },
    )
    .await;
}

/// Accept MQTT-over-WebSocket clients over TLS (`wss://`, ADR 0035). TLS is done first with
/// the (reloadable) ADR 0002 acceptor — so the mTLS client-cert **identity** is extracted from
/// the TLS stream exactly as for a TCP TLS client (ADR 0004) — then the WebSocket handshake
/// runs over the TLS stream.
async fn serve_wss_clients(
    gate: admission::AdmissionGate,
    listener: TcpListener,
    acceptor_rx: tokio::sync::watch::Receiver<TlsAcceptor>,
    hub_tx: mpsc::UnboundedSender<hub::HubCommand>,
    policy: Arc<conn::ConnPolicy>,
    shutdown: tokio_util::sync::CancellationToken,
    connections: tokio_util::task::TaskTracker,
) {
    let per_conn_policy = policy.clone();
    serve_tcp_clients(
        gate,
        listener,
        "wss",
        policy,
        shutdown,
        connections,
        move |stream, peer| {
            // Read the current acceptor per accept so a SIGHUP cert reload is served
            // on the next handshake (ADR 0032 T6).
            let acceptor = acceptor_rx.borrow().clone();
            let hub = hub_tx.clone();
            let policy = per_conn_policy.clone();
            async move {
                match acceptor.accept(stream).await {
                    Ok(tls) => {
                        // mTLS admission (ADR 0004/0040): the verified leaf cert's CN + serial —
                        // read before the TLS stream is consumed by the WebSocket adapter.
                        let cert = conn::tls_admission(&tls, policy.identity_source);
                        match mqtt_net::ws::accept(tls).await {
                            Ok(ws) => {
                                Some(conn::handle_stream(ws, Some(peer), cert, policy, hub).await)
                            }
                            Err(e) => {
                                debug!(%peer, error = %e, "websocket handshake failed");
                                if let Some(m) = &policy.metrics {
                                    m.connection_error("ws");
                                }
                                None
                            }
                        }
                    }
                    Err(e) => {
                        debug!(%peer, error = %e, "TLS handshake failed");
                        if let Some(m) = &policy.metrics {
                            m.connection_error("tls");
                        }
                        None
                    }
                }
            }
        },
    )
    .await;
}

/// Accept MQTT-over-QUIC clients (ADR 0036). QUIC mandates TLS 1.3, so the mTLS **identity** is
/// the verified leaf-cert CN read from the connection (ADR 0004), exactly as for a TCP TLS
/// client. The MQTT session runs over the connection's first **bidirectional** stream (the
/// control stream) — multi-stream data streams layer on this foundation.
async fn serve_quic_clients(
    gate: admission::AdmissionGate,
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
        // Admission caps (ADR 0041 T1): refuse BEFORE the QUIC/TLS handshake —
        // the remote address is known from the initial datagram.
        let Some(permit) = gate.try_admit(Some(incoming.remote_address().ip())) else {
            incoming.refuse();
            continue;
        };
        let hub = hub_tx.clone();
        let policy = policy.clone();
        let gate = gate.clone();
        connections.spawn(async move {
            let _permit = permit; // slot freed when the connection task ends
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
            let cert = mqtt_net::quic::peer_leaf_cert(&conn)
                .and_then(|c| conn::cert_admission(&c, policy.identity_source));
            let identity = cert.as_ref().map(|c| c.identity.clone());
            // Connection-migration observation (ADR 0036 §3b): QUIC keeps a connection alive
            // across a client path change (Wi-Fi↔cellular, NAT rebind). Watch the remote address
            // on the *same* connection — a change is a migration, not a reconnect — and log +
            // count it. The session, streams, and identity are untouched.
            spawn_quic_migration_watch(conn.clone(), identity.clone(), policy.metrics.clone());
            // Multi-stream mux (ADR 0036): the control stream carries the session; any data
            // streams the client opens feed PUBLISH into the same session, no HoL blocking.
            match mqtt_net::quic::accept_mux(conn).await {
                Ok(mux) => {
                    let outcome = conn::handle_stream(mux, Some(peer), cert, policy, hub).await;
                    if outcome.auth_failed {
                        gate.record_auth_failure(Some(peer.ip()));
                    }
                }
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

/// Convert a config `Option<u64>` cap into the `Option<usize>` the runtime uses, rejecting a
/// configured `0` (a zero cap is meaningless — unset it to mean "unbounded"). `field` names the
/// config key for the error message.
fn positive_cap(
    field: &str,
    value: Option<u64>,
) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    match value {
        None => Ok(None),
        Some(0) => {
            Err(format!("{field} must be a positive integer (unset it for unbounded)").into())
        }
        Some(v) => Ok(Some(
            usize::try_from(v).map_err(|_| format!("{field} is too large: {v}"))?,
        )),
    }
}

/// The per-client / global state quotas (ADR 0041 T3/T4) from the config. Shared by startup and
/// the config-reload apply path (ADR 0046 T4), so both derive the same live-swappable quotas.
fn quotas_from_config(config: &Config) -> Result<hub::Quotas, Box<dyn std::error::Error>> {
    Ok(hub::Quotas {
        max_subscriptions_per_client: positive_cap(
            "limits.max_subscriptions_per_client",
            config.limits.max_subscriptions_per_client,
        )?,
        max_retained_messages: positive_cap(
            "limits.max_retained_messages",
            config.limits.max_retained_messages,
        )?,
        max_sessions: positive_cap("limits.max_sessions", config.limits.max_sessions)?,
    })
}

/// The runtime acceptance gate (ADR 0046 T4): can this config be built into every derived
/// runtime value the broker boots with? Runs the same fallible conversions startup does — wire
/// limits (packet-size fits u32, publish-rate positive), queue limits (queued-messages fits
/// usize, valid overflow enum), the state quotas, the admission caps, and the disk watermark —
/// so a config that could not start is rejected *before* it is swapped in live. Returns a
/// human-readable reason on the first failure.
fn runtime_precheck(config: &Config) -> Result<(), String> {
    fn ok<T>(r: Result<T, Box<dyn std::error::Error>>) -> Result<(), String> {
        r.map(|_| ()).map_err(|e| e.to_string())
    }
    ok(wire_limits_from_config(config))?;
    ok(queue_limits_from_config(config))?;
    ok(quotas_from_config(config))?;
    ok(positive_cap(
        "limits.max_connections",
        config.limits.max_connections,
    ))?;
    ok(positive_cap(
        "limits.max_connections_per_ip",
        config.limits.max_connections_per_ip,
    ))?;
    if config.durable.store_max_bytes == Some(0) {
        return Err("durable.store_max_bytes must be a positive integer".to_string());
    }
    Ok(())
}

/// The config sections that changed between `old` and `new` **and are not live-swappable** — i.e.
/// changes that only take effect after a restart (ADR 0046 T4). The live-swappable fields are
/// masked out before comparing: the policy paths + `allow_anonymous` (rebuilt by the reloader)
/// and the state quotas (pushed to the hub). Everything else — listeners, cluster/SWIM, durable,
/// wire limits, observability, runtime, node identity, and the TLS *paths* — requires a restart.
fn requires_restart(old: &Config, new: &Config) -> Vec<&'static str> {
    // Blank the live-swappable fields so only restart-relevant differences remain.
    let mask = |c: &Config| {
        let mut c = c.clone();
        c.security.allow_anonymous = false;
        c.security.password_file = None;
        c.security.acl_file = None;
        c.security.jwt = Jwt::default();
        c.limits.max_subscriptions_per_client = None;
        c.limits.max_retained_messages = None;
        c.limits.max_sessions = None;
        c
    };
    let (o, n) = (mask(old), mask(new));
    let mut changed = Vec::new();
    if o.node != n.node {
        changed.push("node");
    }
    if o.listeners != n.listeners {
        changed.push("listeners");
    }
    if o.tls != n.tls {
        changed.push("tls");
    }
    if o.security != n.security {
        changed.push("security");
    }
    if o.cluster != n.cluster {
        changed.push("cluster");
    }
    if o.durable != n.durable {
        changed.push("durable");
    }
    if o.limits != n.limits {
        changed.push("limits");
    }
    if o.observability != n.observability {
        changed.push("observability");
    }
    if o.runtime != n.runtime {
        changed.push("runtime");
    }
    changed
}

/// Apply the live-swappable half of a committed config reload (ADR 0046 T4): push the (possibly
/// changed) state quotas to the hub, and log + audit every changed non-live section as
/// requires-restart so an operator knows those edits are staged but not yet in effect. The policy
/// half (ACL/auth/TLS/CRL) is applied by the reloader itself, before this runs.
fn apply_live_config(
    old: &Config,
    new: &Config,
    hub: &mpsc::UnboundedSender<hub::HubCommand>,
    audit: &Arc<dyn AuditSink>,
) {
    // Quotas are live: push the new set (idempotent when unchanged). precheck guaranteed they
    // build, so this does not error.
    if let Ok(quotas) = quotas_from_config(new) {
        let _ = hub.send(hub::HubCommand::SetQuotas(quotas));
    }
    let restart = requires_restart(old, new);
    if !restart.is_empty() {
        let sections = restart.join(", ");
        warn!(
            sections = %sections,
            "config reload: settings changed that require a RESTART to take effect — the running \
             values are kept for now"
        );
        audit.record(
            "config.reload",
            None,
            &format!("requires-restart sections: {sections}"),
        );
    }
}

/// Resolve the config-file path (`--config <path>` / `--config=<path>`, else `MQTTD_CONFIG`)
/// and load the layered configuration — defaults < TOML file < `MQTTD_*` env — then validate it
/// (ADR 0046 T2). The command-line flag wins over the env var.
///
/// # Errors
/// A `--config` flag with no value, an unreadable/malformed file, an unparseable env value, or a
/// config that fails validation.
fn load_config() -> Result<Config, Box<dyn std::error::Error>> {
    let path = config_path()?;
    if let Some(p) = &path {
        info!(path = %p.display(), "loading configuration file (ADR 0046)");
    }
    let cfg = Config::load(path.as_deref())?;
    // The warn posture (issue #230, ADR 0058 T4) ignores keys a NEWER config
    // carries — one loud line per key, every boot, so a long-forgotten skew
    // setting cannot silently eat a typo forever.
    for key in &cfg.ignored_keys {
        warn!(
            key = %key,
            "config key IGNORED (runtime.config_unknown_keys = \"warn\"): unknown \
             to this broker version — a config written for a newer broker, or a typo \
             this posture cannot catch (ADR 0058 T4)"
        );
    }
    Ok(cfg)
}

/// Every flag `mqttd` recognises (#169). A dash-prefixed argument not in this list is a
/// mistake, not a silent boot. Values (a `--config` path, a `--hash-password` username, a
/// `--probe` path) do not start with `-`, so they are never mistaken for flags.
const KNOWN_FLAGS: &[&str] = &[
    "--check-config",
    "--config",
    "--hash-password",
    "--probe",
    "--url",
    "--decommission",
    "--pid",
    "--timeout",
    "--version",
    "-V",
    "--help",
    "-h",
];

/// The dash-prefixed arguments `mqttd` does not recognise. Pure over the argument list so
/// it is unit-testable without spawning the binary (#169).
fn unknown_flags<I: IntoIterator<Item = String>>(args: I) -> Vec<String> {
    args.into_iter()
        .filter(|a| a.starts_with('-') && !KNOWN_FLAGS.contains(&a.as_str()))
        .collect()
}

/// Reject any unrecognised flag with a clear error and exit `2`, rather than falling
/// through to boot a broker (#169 — the footgun three review reviewers hit with
/// `mqttd --version`). Recognised subcommands are dispatched by the callers above.
fn reject_unknown_flags() {
    let unknown = unknown_flags(std::env::args().skip(1));
    if !unknown.is_empty() {
        eprintln!("mqttd: unrecognised argument(s): {}", unknown.join(", "));
        eprintln!("Try 'mqttd --help' for the list of flags.");
        std::process::exit(2);
    }
}

/// One-screen usage for `--help`.
fn print_usage() {
    println!(
        "mqttd {} — a security-first, cluster-native MQTT broker\n\n\
         USAGE:\n  \
           mqttd                     start the broker (configured by MQTTD_* env / --config)\n  \
           mqttd --config <path>     start with a TOML config file (env still overlays)\n  \
           mqttd --check-config      validate the effective config and exit (no ports bound)\n  \
           mqttd --hash-password [u] print an Argon2id password-file line and exit\n  \
           mqttd --probe [/readyz]   query the running broker's health endpoint and exit\n  \
           mqttd --decommission      drain and gracefully stop the running broker\n  \
           mqttd --version           print the version and exit\n  \
           mqttd --help              print this help and exit\n\n\
         Configuration is via MQTTD_* environment variables and/or a --config TOML file;\n\
         see docs/mqttd.example.toml and the README.",
        env!("CARGO_PKG_VERSION")
    );
}

/// Validate the config the broker would boot with (file from `--config` / `MQTTD_CONFIG`,
/// layered under the `MQTTD_*` env), then exit — **without binding any port or starting the
/// hub** (ADR 0046 T3). For CI gates and pre-rollout operator checks: `mqttd --check-config`
/// (optionally with `--config <path>`). Exit `0` + `config OK` on success; exit `1` + a clear,
/// located error on failure; exit `2` if the invocation itself is malformed.
fn check_config() -> ! {
    match check_config_inner() {
        Ok(Some(path)) => {
            println!(
                "config OK: {} + MQTTD_* env overlay validates",
                path.display()
            );
            std::process::exit(0);
        }
        Ok(None) => {
            println!("config OK: defaults + MQTTD_* env overlay validates (no config file set)");
            std::process::exit(0);
        }
        Err(CheckError::Usage(e)) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
        Err(CheckError::Invalid { path, error }) => {
            match path {
                Some(p) => eprintln!("config INVALID ({}): {error}", p.display()),
                None => eprintln!("config INVALID: {error}"),
            }
            std::process::exit(1);
        }
    }
}

/// `mqttd --probe [/readyz|/livez] [--url <host:port>]`: ask this node's own health
/// endpoint and exit `0` only on `200 OK`.
///
/// The image is distroless — no shell, no `curl`, no `wget`. Kubernetes does not care
/// (an `httpGet` probe is performed by the kubelet, outside the container), but Docker
/// Compose, Podman and systemd all express health as *a command the container or unit
/// runs*, and there was no command to run. Compose files were therefore reduced to
/// health checks that could not fail — which is worse than none, because the orchestrator
/// then reports a wedged broker as healthy.
///
/// Defaults to `/readyz` on the configured `MQTTD_HEALTH_BIND`, with `0.0.0.0` rewritten
/// to `127.0.0.1` (you cannot connect *to* a wildcard). Speaks just enough HTTP/1.0 to
/// ask the question, using the same tokio runtime the broker already links.
async fn probe_health() -> ! {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let idx = args.iter().position(|a| a == "--probe").unwrap_or(0);
    // `--probe /livez` — a path argument, if the next token is one (not another flag).
    let path = args
        .get(idx + 1)
        .filter(|a| a.starts_with('/'))
        .cloned()
        .unwrap_or_else(|| "/readyz".to_string());
    let explicit_url = args
        .iter()
        .position(|a| a == "--url")
        .and_then(|i| args.get(i + 1))
        .cloned();

    let Some(target) = explicit_url.or_else(health_bind_from_config) else {
        eprintln!(
            "error: no health endpoint to probe. Set MQTTD_HEALTH_BIND (or pass \
             --url <host:port>) — the broker serves /livez and /readyz there."
        );
        std::process::exit(2);
    };
    // A wildcard bind is not an address you can dial.
    let target = target
        .replace("0.0.0.0:", "127.0.0.1:")
        .replace("[::]:", "[::1]:");

    // `main` is already `#[tokio::main]`, so this awaits on the existing runtime rather
    // than building a nested one (which panics).
    match probe_once(&target, &path).await {
        Ok(200) => {
            println!("{path} 200");
            std::process::exit(0);
        }
        Ok(status) => {
            eprintln!("{path} {status} (not ready)");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("{path} unreachable at {target}: {e}");
            std::process::exit(1);
        }
    }
}

/// The `MQTTD_HEALTH_BIND` the broker would use, read through the ordinary config path so
/// a probe honours a config file exactly as the running broker does.
fn health_bind_from_config() -> Option<String> {
    let path = config_path().ok()?;
    Config::load(path.as_deref())
        .ok()?
        .listeners
        .health_bind
        .clone()
}

/// One HTTP/1.0 `GET`, returning the numeric status. Hand-rolled to match the equally
/// hand-rolled server (`health.rs`) and to add no dependency for a three-line request.
async fn probe_once(target: &str, path: &str) -> std::io::Result<u16> {
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

    let connect = tokio::net::TcpStream::connect(target);
    let mut stream = tokio::time::timeout(std::time::Duration::from_secs(5), connect)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out"))??;
    // HTTP/1.0: the server closes when done, so there is no keep-alive framing to parse.
    let request = format!("GET {path} HTTP/1.0\r\nHost: localhost\r\n\r\n");
    stream.write_all(request.as_bytes()).await?;

    let mut response = Vec::new();
    let read = stream.read_to_end(&mut response);
    tokio::time::timeout(std::time::Duration::from_secs(5), read)
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "read timed out"))??;

    // "HTTP/1.1 200 OK" -> 200
    let head = String::from_utf8_lossy(&response);
    head.split_whitespace()
        .nth(1)
        .and_then(|c| c.parse::<u16>().ok())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "no HTTP status in the response: {:?}",
                    head.chars().take(60).collect::<String>()
                ),
            )
        })
}

/// `mqttd --hash-password [<username>]`: read a password from **stdin** and print the
/// Argon2id PHC hash the broker verifies against — a whole `username:hash` password-file
/// line when a username is given, the bare hash otherwise.
///
/// This exists because password authentication was documented but unreachable. The broker
/// verifies Argon2id hashes and `MQTTD_PASSWORD_FILE` wants `username:hash` lines, yet
/// nothing shipped could produce one, and `mosquitto_passwd` output is a different format
/// entirely. Setting up the broker's own auth required writing an Argon2id hasher first.
///
/// Reads stdin rather than taking the password as an argument on purpose: an argument
/// lands in the shell history, in `ps` output, and in any process-listing an unprivileged
/// user on the box can read.
fn hash_password_cli() -> ! {
    use std::io::Read as _;

    let mut args = std::env::args()
        .skip(1)
        .skip_while(|a| a != "--hash-password");
    args.next(); // the flag itself
    let username = args.next().filter(|a| !a.starts_with("--"));

    let mut password = String::new();
    if let Err(e) = std::io::stdin().read_to_string(&mut password) {
        eprintln!("error: failed to read the password from stdin: {e}");
        std::process::exit(2);
    }
    // One trailing newline is the shell adding it (`echo`, a heredoc, a pipe), not part of
    // the password. Anything else is left alone — a password may legitimately contain
    // spaces, and silently trimming them would make the hash fail to verify later.
    let password = password.strip_suffix('\n').unwrap_or(&password);
    let password = password.strip_suffix('\r').unwrap_or(password);
    if password.is_empty() {
        eprintln!(
            "error: refusing to hash an empty password. Pipe one in, e.g.:\n  \
             printf %s 'correct horse battery staple' | mqttd --hash-password alice"
        );
        std::process::exit(2);
    }

    match mqtt_auth::password::hash_password(password) {
        Ok(hash) => {
            match username {
                Some(u) => println!("{u}:{hash}"),
                None => println!("{hash}"),
            }
            std::process::exit(0);
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}

/// `mqttd --decommission [--pid <n>] [--timeout <secs>]` (ADR 0047 T4): send `SIGUSR1` to the
/// running broker to begin the ADR 0043 decommission drain (hand every held key to its
/// post-departure replica set, verify, then leave gracefully), and **block until that process
/// exits** so a Kubernetes `preStop` holds the pod open for the whole drain. The broker image is
/// distroless (no shell, no `kill`), so this is how the `preStop` hook reaches it.
///
/// Target defaults to **PID 1** — the broker is the container entrypoint. Exit `0` when the
/// broker exits (drain complete), `1` on timeout (`preStop` then yields to the grace period /
/// `SIGTERM`), `2` on a usage or signal error.
#[cfg(unix)]
fn run_decommission() -> ! {
    use rustix::process::{kill_process, Pid, Signal};

    let (raw_pid, timeout) = match decommission_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(2);
        }
    };
    let Some(pid) = Pid::from_raw(raw_pid) else {
        eprintln!("error: --pid must be a positive pid, got {raw_pid}");
        std::process::exit(2);
    };
    if let Err(e) = kill_process(pid, Signal::USR1) {
        eprintln!("decommission: cannot signal pid {raw_pid}: {e}");
        std::process::exit(2);
    }
    println!("decommission: sent SIGUSR1 to pid {raw_pid}; waiting for drain + graceful shutdown");
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if broker_exited(raw_pid) {
            println!("decommission: pid {raw_pid} exited — drain complete");
            std::process::exit(0);
        }
        if std::time::Instant::now() >= deadline {
            eprintln!(
                "decommission: timed out after {}s waiting for pid {raw_pid} to exit",
                timeout.as_secs()
            );
            std::process::exit(1);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// Has the broker process exited (drain complete)? On Linux we read `/proc/<pid>/stat` and treat
/// a **zombie** (`Z`) or dead (`X`) state — or a missing entry — as exited; a bare `kill(pid, 0)`
/// would call a not-yet-reaped zombie "alive" and never return. macOS answers the same question
/// through `sysctl(KERN_PROC_PID)` (issue #217); the remaining unixes keep the signal-0 probe.
#[cfg(target_os = "linux")]
fn broker_exited(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Err(_) => true, // no /proc entry — reaped / gone
        // `stat` is `pid (comm) state …`; comm may contain spaces/parens, so the state char is the
        // first non-space after the LAST ')'.
        Ok(s) => s
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.trim_start().chars().next())
            .is_none_or(|st| st == 'Z' || st == 'X' || st == 'x'),
    }
}

/// No `/proc` off Linux. A bare `kill(pid, 0)` calls a not-yet-reaped ZOMBIE
/// "alive", so `--decommission` timed out AFTER the drain had actually completed
/// whenever the supervisor was slow to reap (issue #217; `preStop`-shaped
/// supervisors reap on their own schedule). Ask `ps` for the process state and
/// treat `Z…` (exited, awaiting reap) — or no row at all — as exited. The
/// workspace forbids `unsafe`, which rules the `sysctl(KERN_PROC_PID)` answer
/// out; this path is dev-box only (production containers are Linux and use the
/// `/proc` check above) and polls twice a second from a CLI tool, so a `ps`
/// spawn is proportionate. If `ps` itself cannot run, fall back to the signal-0
/// probe rather than inventing an exit.
#[cfg(all(unix, not(target_os = "linux")))]
fn broker_exited(pid: i32) -> bool {
    use rustix::process::{test_kill_process, Pid};
    match std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
    {
        Ok(out) if out.status.success() => {
            let stat = String::from_utf8_lossy(&out.stdout);
            let stat = stat.trim();
            stat.is_empty() || stat.starts_with('Z')
        }
        // `ps` ran and found no such process: reaped / gone.
        Ok(_) => true,
        Err(_) => Pid::from_raw(pid).is_none_or(|p| test_kill_process(p).is_err()),
    }
}

#[cfg(not(unix))]
fn run_decommission() -> ! {
    eprintln!("error: --decommission is only supported on Unix");
    std::process::exit(2);
}

/// Parse `--pid <n>` (default 1, the container entrypoint) and `--timeout <secs>` (default 3600;
/// the effective bound is the pod's `terminationGracePeriodSeconds`) from `--decommission`.
#[cfg(unix)]
fn decommission_args() -> Result<(i32, Duration), String> {
    let mut pid: i32 = 1;
    let mut timeout = Duration::from_secs(3600);
    let mut args = std::env::args().skip(1);
    while let Some(a) = args.next() {
        match a.as_str() {
            "--pid" => {
                let v = args.next().ok_or("--pid requires a value")?;
                pid = v.parse().map_err(|_| format!("--pid: not a pid: {v:?}"))?;
            }
            "--timeout" => {
                let v = args.next().ok_or("--timeout requires a value")?;
                let secs: u64 = v
                    .parse()
                    .map_err(|_| format!("--timeout: not seconds: {v:?}"))?;
                timeout = Duration::from_secs(secs);
            }
            _ => {}
        }
    }
    Ok((pid, timeout))
}

/// Distinguishes a malformed *invocation* (exit 2) from a well-formed one that produced an
/// *invalid config* (exit 1) — CI can tell "you called me wrong" from "the config is bad".
enum CheckError {
    Usage(Box<dyn std::error::Error>),
    Invalid {
        path: Option<std::path::PathBuf>,
        error: ConfigError,
    },
}

/// The testable core of [`check_config`]: resolve the path and load+validate, returning the
/// resolved path on success (so the caller can report it) or a classified error.
fn check_config_inner() -> Result<Option<std::path::PathBuf>, CheckError> {
    let path = config_path().map_err(CheckError::Usage)?;
    match Config::load(path.as_deref()) {
        Ok(_) => Ok(path),
        Err(error) => Err(CheckError::Invalid { path, error }),
    }
}

/// The config-file path from `--config <path>` / `--config=<path>` (highest precedence) or the
/// `MQTTD_CONFIG` env var, or `None`. A `--config` without a value is a startup error.
fn config_path() -> Result<Option<std::path::PathBuf>, Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if let Some(inline) = arg.strip_prefix("--config=") {
            return Ok(Some(std::path::PathBuf::from(inline)));
        }
        if arg == "--config" {
            let value = args
                .next()
                .ok_or("--config requires a file path argument")?;
            return Ok(Some(std::path::PathBuf::from(value)));
        }
    }
    Ok(std::env::var("MQTTD_CONFIG")
        .ok()
        .filter(|v| !v.is_empty())
        .map(std::path::PathBuf::from))
}

/// Log the effective configuration at startup (ADR 0046 T2) with secret material redacted: the
/// inline gossip keys (`swim.key`, `swim.key_accept`) and the inline JWT HS256 secret never
/// reach the logs. File *paths* are safe to log and are retained for operability.
fn log_effective_config(config: &Config) {
    let mut redacted = config.clone();
    if redacted.cluster.swim.key.is_some() {
        redacted.cluster.swim.key = Some("<redacted>".to_string());
    }
    if !redacted.cluster.swim.key_accept.is_empty() {
        let n = redacted.cluster.swim.key_accept.len();
        redacted.cluster.swim.key_accept = vec![format!("<{n} key(s) redacted>")];
    }
    // Every other secret is now referenced by path (ADR 0046 T5) — paths are safe to log; only
    // the inline gossip key(s) above are raw secrets. The HS256 secret is a file path.
    info!(config = ?redacted, "effective configuration (ADR 0046; secrets redacted)");
}

/// Build the server-wide MQTT 5 wire limits from env (ADR 0011/0012/0013), each with a
/// spec-sensible default. `MQTTD_TOPIC_ALIAS_MAX` (default 16; `0` disables inbound
/// aliases), `MQTTD_RECEIVE_MAXIMUM` (default 256; floored at 1 — a Receive Maximum of 0
/// is a Protocol Error), `MQTTD_AUTH_TIMEOUT` seconds (default 10; floored at 1).
fn wire_limits_from_config(
    config: &Config,
) -> Result<conn::WireLimits, Box<dyn std::error::Error>> {
    let d = conn::WireLimits::default();
    let l = &config.limits;
    // The config carries these as serialization-friendly widths; convert to the transport's
    // internal widths, preserving the historical floors (a Receive Maximum of 0 is a Protocol
    // Error; a sub-1 KiB packet ceiling would refuse CONNECT itself).
    let max_packet_size = match l.max_packet_size {
        None => d.max_packet_size,
        Some(v) => u32::try_from(v)
            .map_err(|_| format!("limits.max_packet_size is too large for u32: {v}"))?,
    }
    .max(1024);
    let publish_rate = match l.max_publish_rate {
        None => None,
        Some(0) => return Err("limits.max_publish_rate must be a positive integer".into()),
        Some(v) => Some(
            u32::try_from(v)
                .map_err(|_| format!("limits.max_publish_rate is too large for u32: {v}"))?,
        ),
    };
    Ok(conn::WireLimits {
        topic_alias_max: l.topic_alias_max.unwrap_or(d.topic_alias_max),
        receive_maximum: l.receive_maximum.unwrap_or(d.receive_maximum).max(1),
        auth_round_timeout: Duration::from_secs(
            config
                .security
                .auth_timeout_secs
                .unwrap_or(d.auth_round_timeout.as_secs())
                .max(1),
        ),
        // The inbound packet ceiling (ADR 0041 T4), advertised as the MQTT 5 Maximum Packet
        // Size; installed into the transport below. Floor 1 KiB.
        max_packet_size,
        // Per-connection publish-rate throttle (ADR 0041 T3); unset = unlimited.
        publish_rate,
    })
}

/// Run until a shutdown signal, then drain gracefully (ADR 0019): fail readiness, stop
/// accepting and drain live connections, all bounded by the grace deadline (or a second
/// signal), then stop the lease consensus core cleanly.
///
/// `SIGUSR1` instead begins a **decommission** (ADR 0043 P3): readiness fails
/// immediately, the drain hands every held key to its group's post-departure
/// replica set and verifies it landed, and only then does the ordinary graceful
/// shutdown (whose SWIM leave moves ownership and triggers voter demotion) run.
/// A `SIGTERM`/`SIGINT` during the drain escalates to a plain shutdown — a
/// crash mid-decommission is just a crash, handled by the survivors.
#[allow(clippy::too_many_arguments)]
async fn graceful_shutdown(
    grace: Duration,
    shutdown: &tokio_util::sync::CancellationToken,
    connections: &tokio_util::task::TaskTracker,
    draining: &std::sync::atomic::AtomicBool,
    plane: Option<mqtt_cluster::durable_plane::DurablePlane>,
    lease_driver: Option<tokio::task::JoinHandle<()>>,
    node_id: NodeId,
    decommission_slot: Arc<std::sync::OnceLock<Arc<mqtt_cluster::decommission::DrainStatus>>>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
) {
    connections.close(); // no more spawns once the accept loops stop
    tokio::select! {
        () = wait_for_shutdown_signal() => {}
        () = wait_for_decommission_signal() => {
            // Fail readiness for the whole drain: orchestrators steer new
            // traffic elsewhere while this node hands its data off.
            draining.store(true, std::sync::atomic::Ordering::Release);
            if let Some(plane) = &plane {
                warn!("SIGUSR1: decommission requested; draining data to the post-departure replica sets (ADR 0043 P3)");
                let drain = plane.decommission_drain(node_id);
                let _ = decommission_slot.set(drain.status());
                // ADR 0054: mirror the drain into the decommission gauges so a
                // scrape-only observer (operator, alert rule) sees it too — the
                // /readyz|/statusz bodies alone require a direct probe.
                if let (Some(m), Some(status)) = (&metrics, decommission_slot.get()) {
                    let (m, status) = (m.clone(), status.clone());
                    tokio::spawn(async move {
                        use std::sync::atomic::Ordering::Acquire;
                        loop {
                            let complete = status.complete.load(Acquire);
                            let state = if complete { 2 } else { 1 };
                            m.set_decommission(state, status.pending.load(Acquire));
                            if complete {
                                return;
                            }
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    });
                }
                tokio::select! {
                    () = drain.run() => {
                        warn!("decommission drain complete; proceeding with the graceful leave");
                    }
                    () = wait_for_shutdown_signal() => {
                        warn!("shutdown signal during decommission drain; leaving with crash semantics (survivors recover)");
                    }
                }
            } else {
                warn!("SIGUSR1: decommission requested without a durable plane; proceeding as a plain graceful shutdown");
            }
        }
    }
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

/// Resolve once a decommission is requested: `SIGUSR1` (ADR 0043 P3). Pends
/// forever on platforms without it (or if the handler cannot install) — plain
/// shutdown signals still work.
async fn wait_for_decommission_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::user_defined1()) {
            Ok(mut usr1) => {
                let _ = usr1.recv().await;
                return;
            }
            Err(e) => {
                warn!(error = %e, "cannot install SIGUSR1 handler; decommission unavailable");
            }
        }
    }
    std::future::pending::<()>().await;
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
    use super::{
        identity_source_from_config, positive_cap, queue_limits_from_config, requires_restart,
        runtime_precheck, unknown_flags, wire_limits_from_config,
    };
    use mqtt_config::Config;
    use mqtt_storage::OverflowPolicy;

    /// #169 self-guard: every double-dash flag this source compares against must be in
    /// `KNOWN_FLAGS`, or the strict-rejection check would refuse a flag the code accepts.
    /// A new subcommand flag added without listing it here fails this test — which is
    /// exactly how a missing `--pid`/`--timeout` was caught in CI the first time.
    #[test]
    fn every_flag_the_code_compares_is_in_the_known_set() {
        let src = include_str!("main.rs");
        let mut missing = Vec::new();
        // Each occurrence of `== "` opens a compared string literal; keep the ones that
        // start with `--` (the double-dash flags) and check membership.
        for (i, _) in src.match_indices("== \"") {
            let rest = &src[i + 4..];
            let Some(end) = rest.find('"') else { continue };
            let tok = &rest[..end];
            if tok.starts_with("--")
                && !super::KNOWN_FLAGS.contains(&tok)
                && !missing.contains(&tok.to_string())
            {
                missing.push(tok.to_string());
            }
        }
        assert!(
            missing.is_empty(),
            "these flags are compared in main.rs but absent from KNOWN_FLAGS, so the strict \
             check would reject them: {missing:?}"
        );
    }

    /// #169 — a mistyped or unrecognised flag is reported, not silently booted; known
    /// flags and their (dash-less) values pass through clean.
    #[test]
    fn unknown_flags_are_caught_and_known_ones_pass() {
        let v = |a: &[&str]| unknown_flags(a.iter().map(|s| (*s).to_string()));

        assert_eq!(v(&["--verison"]), vec!["--verison"], "a typo is caught");
        assert_eq!(v(&["--nope", "-x"]), vec!["--nope", "-x"], "both reported");
        // Known flags and their values are clean — no false positives.
        assert!(v(&["--config", "/etc/mqttd.toml"]).is_empty());
        assert!(v(&["--check-config"]).is_empty());
        assert!(v(&["--probe", "/readyz", "--url", "http://x:8080"]).is_empty());
        assert!(v(&["--version"]).is_empty() && v(&["-V"]).is_empty());
        assert!(v(&["--hash-password", "alice"]).is_empty());
        // No flags at all (normal boot): nothing rejected.
        assert!(v(&[]).is_empty());
    }

    #[test]
    fn positive_cap_rejects_zero_and_converts() {
        assert_eq!(positive_cap("x", None).unwrap(), None);
        assert_eq!(positive_cap("x", Some(7)).unwrap(), Some(7));
        assert!(positive_cap("x", Some(0)).is_err());
    }

    #[test]
    fn wire_limits_default_config_matches_the_spec_floors() {
        // Durable is on by default, so validate() is satisfied by the default config.
        let cfg = Config::default();
        let d = super::conn::WireLimits::default();
        let w = wire_limits_from_config(&cfg).unwrap();
        // Unset limits fall back to the transport defaults (no drift from the env path).
        assert_eq!(w.topic_alias_max, d.topic_alias_max);
        assert_eq!(w.receive_maximum, d.receive_maximum);
        assert_eq!(w.max_packet_size, d.max_packet_size.max(1024));
        assert_eq!(w.publish_rate, None);
    }

    #[test]
    fn wire_limits_apply_config_and_preserve_floors() {
        let mut cfg = Config::default();
        cfg.limits.receive_maximum = Some(0); // floored to 1 (0 is a Protocol Error)
        cfg.limits.max_packet_size = Some(10); // floored to 1024
        cfg.security.auth_timeout_secs = Some(0); // floored to 1
        let w = wire_limits_from_config(&cfg).unwrap();
        assert_eq!(w.receive_maximum, 1);
        assert_eq!(w.max_packet_size, 1024);
        assert_eq!(w.auth_round_timeout.as_secs(), 1);
        // A zero publish rate is a hard error, not a silent unlimited.
        cfg.limits.max_publish_rate = Some(0);
        assert!(wire_limits_from_config(&cfg).is_err());
    }

    #[test]
    fn queue_limits_read_overflow_policy_from_config() {
        let mut cfg = Config::default();
        cfg.limits.max_queued_messages = Some(42);
        cfg.limits.queue_overflow = Some("reject-newest".to_string());
        let q = queue_limits_from_config(&cfg).unwrap();
        assert_eq!(q.max_messages, 42);
        assert_eq!(q.overflow, OverflowPolicy::RejectNewest);
    }

    #[test]
    fn requires_restart_masks_the_live_swappable_fields() {
        let base = Config::default();
        // No change → nothing requires a restart.
        assert!(requires_restart(&base, &base).is_empty());

        // Live-swappable edits (ADR 0046 T4) report NO restart: quotas, allow_anonymous, and the
        // policy file paths are applied in place by the reloader.
        let mut live = base.clone();
        live.security.allow_anonymous = true;
        live.security.acl_file = Some("/etc/acl.toml".into());
        live.limits.max_sessions = Some(1000);
        assert!(
            requires_restart(&base, &live).is_empty(),
            "quotas / allow_anonymous / ACL path are live"
        );

        // Non-live edits DO require a restart, reported by section.
        let mut restart = base.clone();
        restart.listeners.tls_bind = Some("0.0.0.0:8883".into());
        restart.cluster.peer_bind = Some("127.0.0.1:7001".into());
        restart.durable.lease_voters = 7;
        let sections = requires_restart(&base, &restart);
        assert!(sections.contains(&"listeners"));
        assert!(sections.contains(&"cluster"));
        assert!(sections.contains(&"durable"));
        assert!(!sections.contains(&"security"));
    }

    /// The config crate validates the spelling; `mqtt_auth` decides what it means. The two
    /// lists live in different crates on purpose (mqtt-config has no broker dependencies),
    /// so this is where they are pinned together: a value the config accepts must parse,
    /// and one it rejects must not.
    #[test]
    fn the_config_and_the_authenticator_agree_on_identity_source_spellings() {
        use mqtt_auth::mtls::IdentitySource;
        for good in ["cn", "san-dns", "san-uri", "san-email"] {
            let toml = format!("[security]\nmtls_identity_source = \"{good}\"\n");
            let cfg = Config::from_toml(&toml).expect("config accepts it");
            assert_eq!(
                IdentitySource::parse(good)
                    .expect("mqtt-auth accepts it")
                    .as_str(),
                good,
                "spelling must round-trip"
            );
            // ...and the broker reads back exactly that source, never the default.
            assert_eq!(
                identity_source_from_config(&cfg),
                IdentitySource::parse(good).unwrap()
            );
        }
        for bad in ["san", "dns", "common-name", "san_dns", ""] {
            assert!(
                Config::from_toml(&format!("[security]\nmtls_identity_source = \"{bad}\"\n"))
                    .is_err(),
                "config must reject {bad:?}"
            );
            assert!(
                IdentitySource::parse(bad).is_err(),
                "mqtt-auth must reject {bad:?}"
            );
        }
        // Unset means the historical Common Name, with no logging noise.
        assert_eq!(
            identity_source_from_config(&Config::default()),
            IdentitySource::CommonName
        );
    }

    /// Re-keying every ACL under live sessions is not a hot-swap: an edit to the identity
    /// source must be reported as requires-restart, not half-applied (ADR 0046 T4).
    #[test]
    fn changing_the_identity_source_requires_a_restart() {
        let base = Config::default();
        let mut changed = base.clone();
        changed.security.mtls_identity_source = Some("san-dns".into());
        assert!(requires_restart(&base, &changed).contains(&"security"));
    }

    #[test]
    fn runtime_precheck_rejects_what_startup_would_reject() {
        assert!(runtime_precheck(&Config::default()).is_ok());
        // A zero publish rate / zero quota / zero watermark are all rejected before a live swap.
        let mut c = Config::default();
        c.limits.max_publish_rate = Some(0);
        assert!(runtime_precheck(&c).is_err());
        let mut c = Config::default();
        c.limits.max_sessions = Some(0);
        assert!(runtime_precheck(&c).is_err());
        let mut c = Config::default();
        c.durable.store_max_bytes = Some(0);
        assert!(runtime_precheck(&c).is_err());
    }
}
