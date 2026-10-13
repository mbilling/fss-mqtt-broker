//! How long the TLS material this node has loaded is still good for
//! ([ADR 0088](../../../docs/adr/0088-credential-lifetimes-and-rotation.md) D2).
//!
//! Rotating in time is the operator's duty; this module is the broker's half of it, which
//! is that the operator is not surprised. For every certificate file, CA bundle and
//! revocation list the node has loaded it keeps the validity window, and from it come:
//!
//! - two gauges per [`Role`], `mqttd_tls_expiry_seconds` and
//!   `mqttd_tls_lifetime_spent_ratio`, computed when the metrics are read;
//! - a warning in the log once [`Tls::expiry_warn_permille`](mqtt_config::Tls) of the
//!   lifetime is spent and an error once it is over, each repeated at most once every
//!   [`REPEAT_SECS`] per role ([`TlsExpiry::tick`]);
//! - the same threshold in `mqttd --check-tls` ([`crate::tls_check`]) and
//!   `--check-config` ([`check_config_lines`]).
//!
//! **One series per file, decided by the certificate in it that expires first.** A chain
//! file whose intermediate runs out before its leaf is reported by the intermediate, a CA
//! bundle by its earliest CA. Both numbers of a series come from that one certificate.
//! A revocation list is reported by its `nextUpdate`, over its `thisUpdate..nextUpdate`
//! window; one without a `nextUpdate` has no window and no series.
//!
//! Nothing here refuses, ends or delays anything: enforcement is ADR 0088 D3.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

use mqtt_config::Config;
use mqtt_observability::metrics::Metrics;
pub use mqtt_observability::validity::{unix_secs, ValidityWindow};
use x509_parser::certificate::X509Certificate;
use x509_parser::prelude::FromDer;
use x509_parser::revocation_list::CertificateRevocationList;

/// The least time between two log lines about the same role while nothing about it
/// changes: one hour.
pub const REPEAT_SECS: i64 = 3_600;

/// How often the running broker looks at the clock ([`run`]): a threshold or an expiry
/// is noticed within this of being crossed.
pub const TICK: Duration = Duration::from_mins(1);

/// What kind of file a [`Role`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// A certificate chain file: the leaf first, then any intermediates.
    Certificate,
    /// A bundle of CA certificates.
    CaBundle,
    /// A certificate revocation list.
    Crl,
}

/// Which loaded TLS material a series, a log line or a finding is about. A small fixed
/// set, and the only label the gauges carry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// `tls.cert`: the certificate the client-facing TLS, WSS and QUIC listeners serve.
    Listener,
    /// `tls.client_ca`: the CAs client certificates are verified against.
    ClientCa,
    /// `tls.crl`: the client-certificate revocation list.
    ClientCrl,
    /// `cluster.peer_tls.cert`: this node's own cluster certificate.
    Peer,
    /// `cluster.peer_tls.ca`: the cluster CA.
    PeerCa,
    /// `cluster.peer_tls.crl`: the cluster's revocation list.
    PeerCrl,
    /// `admin.cert`: the certificate the admin listener serves.
    Admin,
    /// `admin.client_ca`: the CAs admin client certificates are verified against.
    AdminCa,
}

impl Role {
    /// Every role.
    pub const ALL: [Role; 8] = [
        Role::Listener,
        Role::ClientCa,
        Role::ClientCrl,
        Role::Peer,
        Role::PeerCa,
        Role::PeerCrl,
        Role::Admin,
        Role::AdminCa,
    ];

    /// The `role` label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Role::Listener => "listener",
            Role::ClientCa => "client_ca",
            Role::ClientCrl => "client_crl",
            Role::Peer => "peer",
            Role::PeerCa => "peer_ca",
            Role::PeerCrl => "peer_crl",
            Role::Admin => "admin",
            Role::AdminCa => "admin_ca",
        }
    }

    /// The config key that names the file.
    #[must_use]
    pub const fn setting(self) -> &'static str {
        match self {
            Role::Listener => "tls.cert",
            Role::ClientCa => "tls.client_ca",
            Role::ClientCrl => "tls.crl",
            Role::Peer => "cluster.peer_tls.cert",
            Role::PeerCa => "cluster.peer_tls.ca",
            Role::PeerCrl => "cluster.peer_tls.crl",
            Role::Admin => "admin.cert",
            Role::AdminCa => "admin.client_ca",
        }
    }

    /// What kind of file the role names.
    #[must_use]
    pub const fn kind(self) -> Kind {
        match self {
            Role::Listener | Role::Peer | Role::Admin => Kind::Certificate,
            Role::ClientCa | Role::PeerCa | Role::AdminCa => Kind::CaBundle,
            Role::ClientCrl | Role::PeerCrl => Kind::Crl,
        }
    }
}

/// One file a role is loaded from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Source {
    /// The role.
    pub role: Role,
    /// The file.
    pub path: PathBuf,
}

/// The TLS material a broker running `startup` has loaded, as a reload leaves it when the
/// live config is `live`.
///
/// The `[tls]` and `[cluster.peer_tls]` paths are fixed at startup (an edit is reported
/// as needing a restart), so they come from `startup`; the admin listener's are re-read
/// from the live config by every reload, so they come from `live`. A role is listed only
/// if something serves it: `[tls]` needs a TLS, WSS or QUIC listener, the cluster set
/// needs all three of its files, the admin set needs `admin.bind`.
#[must_use]
pub fn loaded_sources(startup: &Config, live: &Config) -> Vec<Source> {
    let mut out = Vec::new();
    let mut push = |role, path: Option<&String>| {
        if let Some(path) = path {
            out.push(Source {
                role,
                path: PathBuf::from(path),
            });
        }
    };
    let listeners = &startup.listeners;
    let handshake_listener = listeners.tls_bind.is_some() || listeners.wss_bind.is_some();
    let tls = &startup.tls;
    if (handshake_listener || listeners.quic_bind.is_some()) && tls.cert.is_some() {
        push(Role::Listener, tls.cert.as_ref());
        push(Role::ClientCa, tls.client_ca.as_ref());
        // QUIC takes no revocation list; only the TLS and WSS acceptor does.
        if handshake_listener && tls.client_ca.is_some() {
            push(Role::ClientCrl, tls.crl.as_ref());
        }
    }
    let peer = &startup.cluster.peer_tls;
    if peer.ca.is_some() && peer.cert.is_some() && peer.key.is_some() {
        push(Role::Peer, peer.cert.as_ref());
        push(Role::PeerCa, peer.ca.as_ref());
        push(Role::PeerCrl, peer.crl.as_ref());
    }
    if startup.admin.bind.is_some() {
        push(Role::Admin, live.admin.cert.as_ref());
        push(Role::AdminCa, live.admin.client_ca.as_ref());
    }
    out
}

/// The roles a reload re-reads. The client-facing set is rebuilt only for the TLS and WSS
/// acceptor; a node whose only client-facing TLS is QUIC keeps serving what it loaded at
/// startup (ADR 0036), so its series keep describing that.
#[must_use]
pub fn reloaded_roles(startup: &Config) -> Vec<Role> {
    let rebuilt = startup.listeners.tls_bind.is_some() || startup.listeners.wss_bind.is_some();
    Role::ALL
        .into_iter()
        .filter(|role| {
            rebuilt || !matches!(role, Role::Listener | Role::ClientCa | Role::ClientCrl)
        })
        .collect()
}

/// The validity window that decides a file's expiry, and what it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deciding {
    /// The window.
    pub window: ValidityWindow,
    /// Which certificate or list in the file it is, e.g. `leaf CN=node-1`.
    pub what: String,
    /// The end of the window as the file states it (`notAfter` or `nextUpdate`).
    pub until: String,
}

/// Every PEM block labelled `label` in the file at `path`, as DER.
///
/// # Errors
/// The file cannot be read or is not PEM.
pub fn read_pem_blocks(path: &Path, label: &str) -> Result<Vec<Vec<u8>>, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let mut out = Vec::new();
    for pem in x509_parser::pem::Pem::iter_from_buffer(&bytes) {
        let pem = pem.map_err(|e| format!("{} is not valid PEM: {e}", path.display()))?;
        if pem.label == label {
            out.push(pem.contents);
        }
    }
    Ok(out)
}

/// A certificate's validity window.
#[must_use]
pub fn certificate_window(cert: &X509Certificate<'_>) -> ValidityWindow {
    let validity = cert.validity();
    ValidityWindow {
        not_before: validity.not_before.timestamp(),
        not_after: validity.not_after.timestamp(),
    }
}

/// A revocation list's `thisUpdate..nextUpdate` window, if it states a `nextUpdate`.
#[must_use]
pub fn crl_window(crl: &CertificateRevocationList<'_>) -> Option<ValidityWindow> {
    crl.next_update().map(|next| ValidityWindow {
        not_before: crl.last_update().timestamp(),
        not_after: next.timestamp(),
    })
}

/// The certificate in a chain file or CA bundle that expires first. On a tie the earlier
/// one in the file decides, so a leaf is named ahead of an issuer ending with it.
fn earliest_certificate(path: &Path, kind: Kind) -> Result<Deciding, String> {
    let ders = read_pem_blocks(path, "CERTIFICATE")?;
    let mut earliest: Option<Deciding> = None;
    for (i, der) in ders.iter().enumerate() {
        let (_, cert) = X509Certificate::from_der(der).map_err(|e| {
            format!(
                "certificate #{} in {} does not parse: {e}",
                i + 1,
                path.display()
            )
        })?;
        let window = certificate_window(&cert);
        if earliest
            .as_ref()
            .is_none_or(|e| window.not_after < e.window.not_after)
        {
            let position = match (kind, i) {
                (Kind::CaBundle, _) => "CA",
                (_, 0) => "leaf",
                _ => "chain certificate",
            };
            earliest = Some(Deciding {
                window,
                what: format!("{position} {}", cert.subject()),
                until: cert.validity().not_after.to_string(),
            });
        }
    }
    earliest.ok_or_else(|| format!("no certificates found in {}", path.display()))
}

/// The revocation list in a file whose `nextUpdate` comes first; `None` when none of
/// them states one.
fn earliest_crl(path: &Path) -> Result<Option<Deciding>, String> {
    let ders = read_pem_blocks(path, "X509 CRL")?;
    if ders.is_empty() {
        return Err(format!("no CRL found in {}", path.display()));
    }
    let mut earliest: Option<Deciding> = None;
    for der in &ders {
        let (_, crl) = CertificateRevocationList::from_der(der)
            .map_err(|e| format!("{} does not parse: {e}", path.display()))?;
        let (Some(window), Some(next)) = (crl_window(&crl), crl.next_update()) else {
            continue;
        };
        if earliest
            .as_ref()
            .is_none_or(|e| window.not_after < e.window.not_after)
        {
            earliest = Some(Deciding {
                window,
                what: format!("CRL issued by {}", crl.issuer()),
                until: next.to_string(),
            });
        }
    }
    Ok(earliest)
}

/// Read what decides `source`'s expiry. `Ok(None)` is a revocation list without a
/// `nextUpdate`: it has no window to report.
///
/// # Errors
/// The file cannot be read, holds none of what it should, or does not parse.
pub fn read(source: &Source) -> Result<Option<Deciding>, String> {
    match source.role.kind() {
        Kind::Crl => earliest_crl(&source.path),
        kind => earliest_certificate(&source.path, kind).map(Some),
    }
}

/// Where a window stands against the warning threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    /// The window has not begun: whoever checks it against the clock refuses it.
    NotYetValid,
    /// The window runs and less than the threshold is spent.
    Healthy,
    /// The threshold is spent and the window still runs.
    Warn,
    /// The window is over.
    Expired,
}

/// Where `window` stands at `now` against a threshold of `warn_permille` thousandths.
#[must_use]
pub fn status(window: &ValidityWindow, warn_permille: u16, now: i64) -> Status {
    if window.not_yet_begun(now) {
        Status::NotYetValid
    } else if window.ended(now) {
        Status::Expired
    } else if window.past(warn_permille, now) {
        Status::Warn
    } else {
        Status::Healthy
    }
}

/// A fraction as a percentage with one decimal, `66.7%`.
#[must_use]
pub fn percent(fraction: f64) -> String {
    format!("{:.1}%", fraction * 100.0)
}

/// The threshold as a percentage, `66.7%`.
#[must_use]
pub fn threshold_percent(warn_permille: u16) -> String {
    percent(f64::from(warn_permille) / 1000.0)
}

/// A length of time in its two largest units: `12d 3h`, `5h 12m`, `4m 10s`, `42s`. The
/// sign is dropped; the caller says whether it is left or gone.
#[must_use]
pub fn span(secs: i64) -> String {
    let s = secs.unsigned_abs();
    let (d, h, m) = (s / 86_400, s % 86_400 / 3_600, s % 3_600 / 60);
    if d > 0 {
        format!("{d}d {h}h")
    } else if h > 0 {
        format!("{h}h {m}m")
    } else if m > 0 {
        format!("{m}m {}s", s % 60)
    } else {
        format!("{s}s")
    }
}

/// What to say about a window that has not begun, is past the threshold or is over, for
/// `kind`; `None` while it is healthy. The one wording the log, `--check-tls` and `--check-config` share.
#[must_use]
pub fn describe(kind: Kind, deciding: &Deciding, warn_permille: u16, now: i64) -> Option<String> {
    let Deciding {
        window,
        what,
        until,
    } = deciding;
    let left = span(window.seconds_left(now));
    let spent = percent(window.spent(now));
    let threshold = threshold_percent(warn_permille);
    match (status(window, warn_permille, now), kind) {
        (Status::Healthy, _) => None,
        // A revocation list dated ahead is not refused for it; only a certificate is.
        (Status::NotYetValid, Kind::Crl) => None,
        (Status::NotYetValid, _) => Some(format!(
            "{what} is not valid yet: its validity begins in {}",
            span(window.not_before.saturating_sub(now))
        )),
        (Status::Warn, Kind::Crl) => Some(format!(
            "{what} has spent {spent} of its thisUpdate..nextUpdate window (warning \
             threshold {threshold}): its next update is due in {left}, on {until}"
        )),
        (Status::Warn, _) => Some(format!(
            "{what} has spent {spent} of its lifetime (warning threshold {threshold}): it \
             expires in {left}, on {until}"
        )),
        (Status::Expired, Kind::Crl) => Some(format!(
            "{what} is {left} past its nextUpdate ({until}): revocations published since \
             are not in it"
        )),
        (Status::Expired, _) => Some(format!("{what} EXPIRED {left} ago, on {until}")),
    }
}

/// How serious a [`Notice`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeLevel {
    /// Past the threshold, or a file whose expiry could not be read.
    Warn,
    /// Expired.
    Error,
}

/// One log line about one role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    /// WARN or ERROR.
    pub level: NoticeLevel,
    /// The role.
    pub role: Role,
    /// The file.
    pub path: PathBuf,
    /// What is wrong, with the fraction spent and the time left.
    pub message: String,
}

/// Write `notices` to the log, each naming its role, its setting and its file.
pub fn emit(notices: &[Notice]) {
    for n in notices {
        let (role, setting, path) = (n.role.as_str(), n.role.setting(), n.path.display());
        match n.level {
            NoticeLevel::Warn => tracing::warn!(
                role, setting, %path,
                "TLS material: {} — rotate it in time (ADR 0088)", n.message
            ),
            NoticeLevel::Error => tracing::error!(
                role, setting, %path,
                "TLS material: {} — replace it now (ADR 0088)", n.message
            ),
        }
    }
}

/// One loaded file and when it was last logged about.
#[derive(Debug)]
struct Held {
    source: Source,
    deciding: Deciding,
    /// When a notice was last written for it, and at which status.
    logged: Option<(i64, Status)>,
}

/// The validity windows of everything this node has loaded, the gauges fed from them and
/// the rate-bounded log lines about them.
#[derive(Debug)]
pub struct TlsExpiry {
    warn_permille: u16,
    metrics: Option<Arc<Metrics>>,
    held: Mutex<Vec<Held>>,
}

impl TlsExpiry {
    /// An empty set warning at `warn_permille` thousandths of a lifetime, exporting to
    /// `metrics` when given.
    #[must_use]
    pub fn new(warn_permille: u16, metrics: Option<Arc<Metrics>>) -> Self {
        if let Some(m) = &metrics {
            m.set_tls_expiry_warn_permille(warn_permille);
        }
        Self {
            warn_permille,
            metrics,
            held: Mutex::new(Vec::new()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<Held>> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Read `sources` and make them what is held for the roles in `scope`; a role in
    /// `scope` with no source stops being held, and roles outside it are left alone.
    /// Called at startup with every role and after each reload that committed with the
    /// roles it rebuilt.
    ///
    /// A file that is the same file with the same window keeps its place in the log's
    /// schedule, so reloads do not repeat a warning; a new certificate starts afresh and
    /// is spoken of at the next [`tick`](Self::tick) if it is already past the threshold.
    ///
    /// Returns a warning for each file whose expiry could not be read. Such a role is
    /// not held, and so not exported, until a later load reads it.
    pub fn load(&self, scope: &[Role], sources: &[Source]) -> Vec<Notice> {
        let mut notices = Vec::new();
        let mut held = self.lock();
        let mut previous: Vec<Held> = Vec::new();
        let mut kept: Vec<Held> = Vec::new();
        for h in held.drain(..) {
            if scope.contains(&h.source.role) {
                previous.push(h);
            } else {
                kept.push(h);
            }
        }
        for source in sources.iter().filter(|s| scope.contains(&s.role)) {
            match read(source) {
                Ok(Some(deciding)) => {
                    let logged = previous
                        .iter()
                        .find(|p| p.source == *source && p.deciding.window == deciding.window)
                        .and_then(|p| p.logged);
                    kept.push(Held {
                        source: source.clone(),
                        deciding,
                        logged,
                    });
                }
                // A revocation list with no nextUpdate: nothing to count down to.
                Ok(None) => {}
                Err(e) => notices.push(Notice {
                    level: NoticeLevel::Warn,
                    role: source.role,
                    path: source.path.clone(),
                    message: format!("its time to expiry cannot be read and is not exported ({e})"),
                }),
            }
        }
        kept.sort_by_key(|h| h.source.role);
        *held = kept;
        if let Some(m) = &self.metrics {
            m.set_tls_validity(
                held.iter()
                    .map(|h| (h.source.role.as_str(), h.deciding.window))
                    .collect(),
            );
        }
        notices
    }

    /// The log lines due at `now`: one for each held role that is not valid yet or past
    /// the threshold (WARN) or expired (ERROR), the first time it is seen so — at load, or when it
    /// crosses over — and then at most once every [`REPEAT_SECS`] while it stays so.
    /// Nothing for a healthy role.
    pub fn tick(&self, now: SystemTime) -> Vec<Notice> {
        let now = unix_secs(now);
        let mut notices = Vec::new();
        for h in self.lock().iter_mut() {
            let status = status(&h.deciding.window, self.warn_permille, now);
            let due = match h.logged {
                None => true,
                Some((at, was)) => was != status || now.saturating_sub(at) >= REPEAT_SECS,
            };
            let Some(message) =
                describe(h.source.role.kind(), &h.deciding, self.warn_permille, now)
            else {
                // Healthy: silent, and forgotten, so a later crossing is said at once.
                h.logged = None;
                continue;
            };
            if !due {
                continue;
            }
            h.logged = Some((now, status));
            notices.push(Notice {
                level: if status == Status::Expired {
                    NoticeLevel::Error
                } else {
                    NoticeLevel::Warn
                },
                role: h.source.role,
                path: h.source.path.clone(),
                message,
            });
        }
        notices
    }

    /// What is held: each role, its file and the window that decides it.
    #[must_use]
    pub fn held(&self) -> Vec<(Role, PathBuf, ValidityWindow)> {
        self.lock()
            .iter()
            .map(|h| (h.source.role, h.source.path.clone(), h.deciding.window))
            .collect()
    }
}

/// Look at the clock every [`TICK`] until `shutdown`, writing what [`TlsExpiry::tick`]
/// finds due.
pub async fn run(expiry: Arc<TlsExpiry>, shutdown: tokio_util::sync::CancellationToken) {
    let mut every = tokio::time::interval(TICK);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = every.tick() => emit(&expiry.tick(SystemTime::now())),
        }
    }
}

/// What `--check-config` says about the TLS material `config` would load, at `now`: the
/// threshold, then one line for each file past it, expired or not valid yet. Empty when the config loads
/// none. A file that cannot be read here is passed over: the check runs where configs are
/// written, which need not hold the files (`--preflight` is what opens them).
#[must_use]
pub fn check_config_lines(config: &Config, now: SystemTime) -> Vec<String> {
    let sources = loaded_sources(config, config);
    if sources.is_empty() {
        return Vec::new();
    }
    let permille = config.tls.expiry_warn_permille;
    let now = unix_secs(now);
    let mut lines = vec![format!(
        "TLS expiry: the broker warns once {} of a certificate's, CA's or revocation list's \
         lifetime is spent (tls.expiry_warn_permille = {permille})",
        threshold_percent(permille)
    )];
    for source in &sources {
        let Ok(Some(deciding)) = read(source) else {
            continue;
        };
        if let Some(message) = describe(source.role.kind(), &deciding, permille, now) {
            lines.push(format!(
                "warning: {} ({}): {message}",
                source.role.setting(),
                source.path.display()
            ));
        }
    }
    lines
}

#[cfg(test)]
pub(crate) mod testing {
    //! Certificates and revocation lists with chosen validity windows, minted in the test.

    use std::path::{Path, PathBuf};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    /// A fixed instant every test here measures from: 2026-01-01T00:00:00Z.
    pub(crate) const T0: u64 = 1_767_225_600;
    pub(crate) const HOUR: u64 = 3_600;
    pub(crate) const DAY: u64 = 86_400;

    pub(crate) fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    /// A CA valid over `[from, to]` (Unix seconds), able to issue.
    pub(crate) fn ca(
        cn: &str,
        from: u64,
        to: u64,
    ) -> rcgen::CertifiedIssuer<'static, rcgen::KeyPair> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.key_usages = vec![
            rcgen::KeyUsagePurpose::KeyCertSign,
            rcgen::KeyUsagePurpose::CrlSign,
        ];
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.not_before = at(from).into();
        params.not_after = at(to).into();
        rcgen::CertifiedIssuer::self_signed(params, rcgen::KeyPair::generate().unwrap()).unwrap()
    }

    /// An intermediate CA valid over `[from, to]`, issued by `issuer`.
    pub(crate) fn intermediate(
        cn: &str,
        from: u64,
        to: u64,
        issuer: &rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    ) -> rcgen::CertifiedIssuer<'static, rcgen::KeyPair> {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.not_before = at(from).into();
        params.not_after = at(to).into();
        rcgen::CertifiedIssuer::signed_by(params, rcgen::KeyPair::generate().unwrap(), issuer)
            .unwrap()
    }

    /// A leaf valid over `[from, to]` issued by `issuer`: its certificate and key, as PEM.
    pub(crate) fn leaf(
        cn: &str,
        from: u64,
        to: u64,
        issuer: &rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
    ) -> (String, String) {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(vec![format!("{cn}.example")]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        params.not_before = at(from).into();
        params.not_after = at(to).into();
        (
            params.signed_by(&key, issuer).unwrap().pem(),
            key.serialize_pem(),
        )
    }

    /// A revocation list issued by `issuer` at `this_update`, due again at `next_update`.
    pub(crate) fn crl(
        issuer: &rcgen::CertifiedIssuer<'static, rcgen::KeyPair>,
        this_update: u64,
        next_update: u64,
    ) -> String {
        rcgen::CertificateRevocationListParams {
            this_update: at(this_update).into(),
            next_update: at(next_update).into(),
            crl_number: rcgen::SerialNumber::from(1u64),
            issuing_distribution_point: None,
            revoked_certs: Vec::new(),
            key_identifier_method: rcgen::KeyIdMethod::Sha256,
        }
        .signed_by(issuer)
        .unwrap()
        .pem()
        .unwrap()
    }

    pub(crate) fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        path
    }
}

#[cfg(test)]
mod tests {
    use super::testing::{at, ca, crl, intermediate, leaf, write, DAY, HOUR, T0};
    use super::{
        check_config_lines, loaded_sources, read, reloaded_roles, span, NoticeLevel, Role, Source,
        TlsExpiry, REPEAT_SECS,
    };
    use mqtt_config::Config;
    use mqtt_observability::metrics::Metrics;
    use std::sync::Arc;

    fn source(role: Role, path: &std::path::Path) -> Source {
        Source {
            role,
            path: path.to_path_buf(),
        }
    }

    fn secs(n: u64) -> i64 {
        i64::try_from(n).unwrap()
    }

    #[test]
    fn a_leaf_is_read_with_its_own_window_whatever_its_lifetime() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        for (name, from, to) in [
            ("an hour", T0, T0 + HOUR),
            ("a year", T0 - DAY, T0 + 365 * DAY),
            ("expired", T0 - 30 * DAY, T0 - DAY),
            ("not yet valid", T0 + DAY, T0 + 2 * DAY),
        ] {
            let (cert, _) = leaf("node-1", from, to, &root);
            let path = write(dir.path(), "leaf.pem", &cert);
            let d = read(&source(Role::Peer, &path)).unwrap().unwrap();
            assert_eq!(d.window.not_before, secs(from), "{name}");
            assert_eq!(d.window.not_after, secs(to), "{name}");
            assert!(d.what.starts_with("leaf "), "{name}: {}", d.what);
            assert!(d.what.contains("node-1"), "{name}: {}", d.what);
        }
    }

    #[test]
    fn a_chain_is_decided_by_whichever_certificate_expires_first() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        // The usual case: the intermediate outlives the leaf, and the leaf decides.
        let long = intermediate("issuing-long", T0 - 10 * DAY, T0 + 900 * DAY, &root);
        let (cert, _) = leaf("node-1", T0, T0 + 30 * DAY, &long);
        let path = write(dir.path(), "chain.pem", &format!("{cert}{}", long.pem()));
        let d = read(&source(Role::Listener, &path)).unwrap().unwrap();
        assert_eq!(d.window.not_after, secs(T0 + 30 * DAY));
        assert!(d.what.starts_with("leaf "), "{}", d.what);

        // An intermediate that runs out before the leaf it issued is what the file is
        // reported by: both numbers come from the intermediate.
        let short = intermediate("issuing-short", T0 - 80 * DAY, T0 + 10 * DAY, &root);
        let (cert, _) = leaf("node-1", T0, T0 + 30 * DAY, &short);
        let path = write(dir.path(), "chain.pem", &format!("{cert}{}", short.pem()));
        let d = read(&source(Role::Listener, &path)).unwrap().unwrap();
        assert_eq!(d.window.not_before, secs(T0 - 80 * DAY));
        assert_eq!(d.window.not_after, secs(T0 + 10 * DAY));
        assert!(
            d.what.starts_with("chain certificate ") && d.what.contains("issuing-short"),
            "{}",
            d.what
        );
    }

    #[test]
    fn a_ca_bundle_is_decided_by_its_earliest_ca() {
        let dir = tempfile::tempdir().unwrap();
        let old = ca("old-root", T0 - 3000 * DAY, T0 + 20 * DAY);
        let new = ca("new-root", T0 - DAY, T0 + 3650 * DAY);
        for bundle in [
            format!("{}{}", old.pem(), new.pem()),
            format!("{}{}", new.pem(), old.pem()),
        ] {
            let path = write(dir.path(), "ca.pem", &bundle);
            let d = read(&source(Role::PeerCa, &path)).unwrap().unwrap();
            assert_eq!(d.window.not_after, secs(T0 + 20 * DAY));
            assert_eq!(d.what, "CA CN=old-root");
        }
        // No certificate in the file is an error, not a healthy bundle.
        let path = write(dir.path(), "empty.pem", "");
        assert!(read(&source(Role::PeerCa, &path))
            .unwrap_err()
            .contains("no certificates"));
    }

    #[test]
    fn a_revocation_list_is_decided_by_its_next_update() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        let path = write(dir.path(), "bus.crl", &crl(&root, T0, T0 + 7 * DAY));
        let d = read(&source(Role::PeerCrl, &path)).unwrap().unwrap();
        assert_eq!(d.window.not_before, secs(T0));
        assert_eq!(d.window.not_after, secs(T0 + 7 * DAY));
        assert_eq!(d.what, "CRL issued by CN=root");
        // A certificate where a revocation list should be.
        let path = write(dir.path(), "not.crl", &root.pem());
        assert!(read(&source(Role::PeerCrl, &path))
            .unwrap_err()
            .contains("no CRL"));
    }

    /// The whole life of one certificate under a driven clock: silent while healthy, a
    /// WARN the minute the threshold is crossed, again an hour later and no sooner, an
    /// ERROR the minute it expires and hourly after that; and every tick moves the gauges.
    #[test]
    fn the_log_warns_past_the_threshold_and_errors_at_expiry_at_a_bounded_rate() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        // 30 hours: two thirds spent at T0 + 20h.
        let (cert, _) = leaf("node-1", T0, T0 + 30 * HOUR, &root);
        let path = write(dir.path(), "node.crt", &cert);
        let metrics = Arc::new(Metrics::new("t"));
        let expiry = TlsExpiry::new(667, Some(metrics.clone()));
        assert!(expiry
            .load(&Role::ALL, &[source(Role::Peer, &path)])
            .is_empty());

        // Healthy: nothing, however often it is asked.
        for h in [0, 1, 10, 19] {
            assert!(expiry.tick(at(T0 + h * HOUR)).is_empty(), "hour {h}");
        }
        let out = metrics.render_at(at(T0 + 15 * HOUR));
        assert!(
            out.contains("mqttd_tls_expiry_seconds{role=\"peer\"} 54000"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_tls_lifetime_spent_ratio{role=\"peer\"} 0.5"),
            "{out}"
        );

        // Across the threshold (20h01m of 30h = 66.72%).
        let crossed = T0 + 20 * HOUR + 60;
        let notices = expiry.tick(at(crossed));
        assert_eq!(notices.len(), 1, "{notices:?}");
        let n = &notices[0];
        assert_eq!(n.level, NoticeLevel::Warn);
        assert_eq!(n.role, Role::Peer);
        assert_eq!(n.path, path);
        assert!(
            n.message
                .contains("leaf CN=node-1 has spent 66.7% of its lifetime")
                && n.message.contains("warning threshold 66.7%")
                && n.message.contains("expires in 9h 59m"),
            "{}",
            n.message
        );
        // Bounded: a tick a minute for the rest of the hour says nothing more.
        for minute in 1..60 {
            assert!(
                expiry.tick(at(crossed + minute * 60)).is_empty(),
                "minute {minute}"
            );
        }
        let again = expiry.tick(at(crossed + u64::try_from(REPEAT_SECS).unwrap()));
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].level, NoticeLevel::Warn);
        assert!(again[0].message.contains("expires in 8h 59m"));

        // The last second is still a warning; the one after it is an error, at once,
        // whatever the hour's schedule says.
        let last = expiry.tick(at(T0 + 30 * HOUR));
        assert_eq!(last.len(), 1, "{last:?}");
        assert_eq!(last[0].level, NoticeLevel::Warn);
        assert!(last[0].message.contains("expires in 0s"));
        let expired = expiry.tick(at(T0 + 30 * HOUR + 1));
        assert_eq!(expired.len(), 1, "{expired:?}");
        assert_eq!(expired[0].level, NoticeLevel::Error);
        assert!(
            expired[0].message.contains("leaf CN=node-1 EXPIRED 1s ago"),
            "{}",
            expired[0].message
        );
        assert!(expiry.tick(at(T0 + 30 * HOUR + 1800)).is_empty());
        let still = expiry.tick(at(T0 + 31 * HOUR + 1));
        assert_eq!(still.len(), 1);
        assert_eq!(still[0].level, NoticeLevel::Error);
        assert!(still[0].message.contains("EXPIRED 1h 0m ago"));

        let out = metrics.render_at(at(T0 + 33 * HOUR));
        assert!(
            out.contains("mqttd_tls_expiry_seconds{role=\"peer\"} -10800"),
            "{out}"
        );
        assert!(
            out.contains("mqttd_tls_lifetime_spent_ratio{role=\"peer\"} 1.1"),
            "{out}"
        );
    }

    /// A certificate already past the threshold when it is loaded is spoken of at the
    /// first tick; reloading the same file does not repeat it; a new certificate starts
    /// afresh, and each role keeps its own schedule.
    #[test]
    fn a_reload_follows_the_new_certificate_and_does_not_repeat_the_old_warning() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 3000 * DAY, T0 + 650 * DAY);
        let ca_path = write(dir.path(), "ca.crt", &root.pem());
        let (old, _) = leaf("node-1", T0 - 29 * DAY, T0 + DAY, &root);
        let path = write(dir.path(), "node.crt", &old);
        let metrics = Arc::new(Metrics::new("t"));
        let expiry = TlsExpiry::new(900, Some(metrics.clone()));
        let sources = [source(Role::Peer, &path), source(Role::PeerCa, &ca_path)];
        expiry.load(&Role::ALL, &sources);

        // The leaf is 96.7% spent, the CA 82.2%: only the leaf is past 90%.
        let first = expiry.tick(at(T0));
        assert_eq!(first.len(), 1, "{first:?}");
        assert_eq!(first[0].role, Role::Peer);
        // The watcher reloads for an unrelated edit: same file, same window, no repeat.
        expiry.load(&Role::ALL, &sources);
        assert!(expiry.tick(at(T0 + 60)).is_empty());

        // The certificate is renewed and the reload commits.
        let (new, _) = leaf("node-1", T0, T0 + 30 * DAY, &root);
        write(dir.path(), "node.crt", &new);
        expiry.load(&Role::ALL, &sources);
        assert!(
            expiry.tick(at(T0 + 120)).is_empty(),
            "the new one is healthy"
        );
        let out = metrics.render_at(at(T0 + 120));
        assert!(
            out.contains(&format!(
                "mqttd_tls_expiry_seconds{{role=\"peer\"}} {}",
                30 * DAY - 120
            )),
            "{out}"
        );
        assert_eq!(
            expiry.held()[0].2.not_after,
            secs(T0 + 30 * DAY),
            "the held window is the new certificate's"
        );

        // A reload that rebuilt nothing for a role leaves it alone...
        expiry.load(&[Role::Admin, Role::AdminCa], &[]);
        assert_eq!(expiry.held().len(), 2);
        // ...and one that no longer has it drops its series.
        expiry.load(&Role::ALL, &[source(Role::Peer, &path)]);
        assert!(!metrics.render_at(at(T0 + 120)).contains("peer_ca"));
    }

    /// `nextUpdate` is optional in a revocation list (RFC 5280 §5.1.2.5). One without it
    /// has no window, so nothing is exported for it. rcgen always writes the field, so the
    /// list here is one of its own with the field cut out (the signature no longer
    /// matches, which the parser does not check).
    #[test]
    fn a_revocation_list_without_a_next_update_has_no_window() {
        use x509_parser::prelude::FromDer;
        use x509_parser::revocation_list::CertificateRevocationList;

        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        let pem = crl(&root, T0, T0 + 7 * DAY);
        let (_, parsed) = x509_parser::pem::parse_x509_pem(pem.as_bytes()).unwrap();
        let der = parsed.contents;
        let (_, whole) = CertificateRevocationList::from_der(&der).unwrap();
        assert!(super::crl_window(&whole).is_some());

        // Two UTCTime values side by side: thisUpdate, then nextUpdate (15 bytes each).
        let times = der
            .windows(17)
            .position(|w| w[..2] == [0x17, 0x0d] && w[15..] == [0x17, 0x0d])
            .expect("thisUpdate followed by nextUpdate");
        let mut cut = der.clone();
        cut.drain(times + 15..times + 30);
        // The outer SEQUENCE and the TBSCertList inside it are each 15 bytes shorter.
        // A length is one byte below 128, else `0x81 n` or `0x82 n n`.
        let mut at = 0;
        for _ in 0..2 {
            assert_eq!(cut[at], 0x30, "a SEQUENCE header");
            match cut[at + 1] {
                short if short < 0x80 => {
                    cut[at + 1] = short - 15;
                    at += 2;
                }
                0x81 => {
                    cut[at + 2] -= 15;
                    at += 3;
                }
                0x82 => {
                    let len = u16::from_be_bytes([cut[at + 2], cut[at + 3]]) - 15;
                    cut[at + 2..at + 4].copy_from_slice(&len.to_be_bytes());
                    at += 4;
                }
                other => panic!("unexpected length form {other:#x}"),
            }
        }
        let (_, without) = CertificateRevocationList::from_der(&cut).unwrap();
        assert!(without.next_update().is_none(), "the field is gone");
        assert_eq!(super::crl_window(&without), None);
    }

    #[test]
    fn a_revocation_list_past_its_next_update_is_an_error_and_an_unreadable_one_is_not_held() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        let path = write(dir.path(), "bus.crl", &crl(&root, T0, T0 + 6 * DAY));
        let metrics = Arc::new(Metrics::new("t"));
        let expiry = TlsExpiry::new(667, Some(metrics.clone()));
        expiry.load(&Role::ALL, &[source(Role::PeerCrl, &path)]);
        assert!(expiry.tick(at(T0 + 3 * DAY)).is_empty());
        let warn = expiry.tick(at(T0 + 5 * DAY));
        assert_eq!(warn[0].level, NoticeLevel::Warn);
        assert!(
            warn[0]
                .message
                .contains("has spent 83.3% of its thisUpdate..nextUpdate window")
                && warn[0].message.contains("next update is due in 1d 0h"),
            "{}",
            warn[0].message
        );
        let stale = expiry.tick(at(T0 + 8 * DAY));
        assert_eq!(stale[0].level, NoticeLevel::Error);
        assert!(
            stale[0].message.contains("is 2d 0h past its nextUpdate"),
            "{}",
            stale[0].message
        );
        let out = metrics.render_at(at(T0 + 8 * DAY));
        assert!(
            out.contains(&format!(
                "mqttd_tls_expiry_seconds{{role=\"peer_crl\"}} -{}",
                2 * DAY
            )),
            "{out}"
        );

        // An unreadable file is said once per load and not exported.
        let missing = dir.path().join("gone.crl");
        let notices = expiry.load(&Role::ALL, &[source(Role::PeerCrl, &missing)]);
        assert_eq!(notices.len(), 1);
        assert_eq!(notices[0].level, NoticeLevel::Warn);
        assert!(notices[0].message.contains("cannot be read"));
        assert!(expiry.held().is_empty());
        assert!(!metrics.render_at(at(T0)).contains("peer_crl"));
    }

    #[test]
    fn what_is_loaded_follows_the_listeners_and_the_admin_paths_follow_the_live_config() {
        let mut startup = Config::default();
        startup.tls.cert = Some("/pki/server.crt".into());
        startup.tls.key = Some("/pki/server.key".into());
        startup.tls.client_ca = Some("/pki/clients.crt".into());
        startup.tls.crl = Some("/pki/clients.crl".into());
        assert!(
            loaded_sources(&startup, &startup).is_empty(),
            "no listener serves [tls], so nothing is loaded"
        );
        startup.listeners.quic_bind = Some("0.0.0.0:14567".into());
        let roles = |c: &Config, live: &Config| -> Vec<Role> {
            loaded_sources(c, live)
                .into_iter()
                .map(|s| s.role)
                .collect()
        };
        assert_eq!(
            roles(&startup, &startup),
            [Role::Listener, Role::ClientCa],
            "QUIC takes no revocation list"
        );
        assert!(
            !reloaded_roles(&startup).contains(&Role::Listener),
            "QUIC serves what it loaded at startup"
        );
        startup.listeners.tls_bind = Some("0.0.0.0:8883".into());
        assert!(reloaded_roles(&startup).contains(&Role::Listener));
        startup.cluster.peer_tls.ca = Some("/pki/ca.crt".into());
        startup.cluster.peer_tls.cert = Some("/pki/node.crt".into());
        assert_eq!(
            roles(&startup, &startup),
            [Role::Listener, Role::ClientCa, Role::ClientCrl],
            "an incomplete cluster set is not loaded"
        );
        startup.cluster.peer_tls.key = Some("/pki/node.key".into());
        startup.admin.bind = Some("127.0.0.1:9443".into());
        startup.admin.cert = Some("/pki/admin.crt".into());
        startup.admin.client_ca = Some("/pki/admin-ca.crt".into());
        let mut live = startup.clone();
        live.admin.cert = Some("/pki/admin-new.crt".into());
        live.tls.cert = Some("/pki/ignored-until-restart.crt".into());
        let sources = loaded_sources(&startup, &live);
        let path = |role| {
            sources
                .iter()
                .find(|s| s.role == role)
                .map(|s| s.path.display().to_string())
        };
        assert_eq!(path(Role::Listener).as_deref(), Some("/pki/server.crt"));
        assert_eq!(path(Role::Peer).as_deref(), Some("/pki/node.crt"));
        assert_eq!(path(Role::PeerCa).as_deref(), Some("/pki/ca.crt"));
        assert_eq!(path(Role::PeerCrl), None);
        assert_eq!(path(Role::Admin).as_deref(), Some("/pki/admin-new.crt"));
        assert_eq!(path(Role::AdminCa).as_deref(), Some("/pki/admin-ca.crt"));
        // Every label value is distinct and from the documented set.
        let mut labels: Vec<&str> = Role::ALL.iter().map(|r| r.as_str()).collect();
        labels.sort_unstable();
        labels.dedup();
        assert_eq!(labels.len(), Role::ALL.len());
    }

    #[test]
    fn check_config_states_the_threshold_and_warns_past_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        let (cert, _) = leaf("broker", T0, T0 + 90 * DAY, &root);
        let cert_path = write(dir.path(), "server.crt", &cert);
        let mut config = Config::default();
        assert!(
            check_config_lines(&config, at(T0)).is_empty(),
            "no TLS material, nothing to say"
        );
        config.listeners.tls_bind = Some("127.0.0.1:8883".into());
        config.tls.cert = Some(cert_path.display().to_string());
        config.tls.key = Some("/not/opened/by/this/check.key".into());
        // A file this host does not hold is passed over, not refused.
        config.tls.client_ca = Some("/not/on/this/host/ca.crt".into());

        let healthy = check_config_lines(&config, at(T0 + 30 * DAY));
        assert_eq!(healthy.len(), 1, "{healthy:?}");
        assert!(
            healthy[0].contains("66.7%") && healthy[0].contains("tls.expiry_warn_permille = 667"),
            "{}",
            healthy[0]
        );
        let late = check_config_lines(&config, at(T0 + 72 * DAY));
        assert_eq!(late.len(), 2, "{late:?}");
        assert!(
            late[1].starts_with("warning: tls.cert (")
                && late[1].contains("has spent 80.0% of its lifetime")
                && late[1].contains("expires in 18d 0h"),
            "{}",
            late[1]
        );
        let over = check_config_lines(&config, at(T0 + 91 * DAY));
        assert!(over[1].contains("EXPIRED 1d 0h ago"), "{}", over[1]);
        // The threshold is the configured one.
        config.tls.expiry_warn_permille = 900;
        let lines = check_config_lines(&config, at(T0 + 72 * DAY));
        assert_eq!(lines.len(), 1, "{lines:?}");
        assert!(lines[0].contains("90.0%"));
    }

    /// A certificate loaded ahead of its `notBefore` (a pre-dated rotation, a clock that
    /// is behind) is refused by whoever checks it, and reads as brand new on the gauges:
    /// the log says so, at the same bounded rate, until its validity begins.
    #[test]
    fn a_certificate_that_is_not_valid_yet_is_warned_about_until_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let root = ca("root", T0 - 365 * DAY, T0 + 3650 * DAY);
        let (cert, _) = leaf("node-1", T0 + 2 * HOUR, T0 + 26 * HOUR, &root);
        let path = write(dir.path(), "node.crt", &cert);
        let expiry = TlsExpiry::new(667, None);
        expiry.load(&Role::ALL, &[source(Role::Peer, &path)]);
        let early = expiry.tick(at(T0));
        assert_eq!(early.len(), 1, "{early:?}");
        assert_eq!(early[0].level, NoticeLevel::Warn);
        assert!(
            early[0]
                .message
                .contains("leaf CN=node-1 is not valid yet: its validity begins in 2h 0m"),
            "{}",
            early[0].message
        );
        assert!(expiry.tick(at(T0 + 1800)).is_empty(), "bounded");
        assert_eq!(expiry.tick(at(T0 + HOUR)).len(), 1);
        // Valid: nothing more, until the threshold.
        assert!(expiry.tick(at(T0 + 2 * HOUR)).is_empty());
        assert!(expiry.tick(at(T0 + 10 * HOUR)).is_empty());
        assert_eq!(expiry.tick(at(T0 + 19 * HOUR)).len(), 1);
    }

    #[test]
    fn a_span_is_written_in_its_two_largest_units() {
        assert_eq!(span(42), "42s");
        assert_eq!(span(250), "4m 10s");
        assert_eq!(span(5 * 3_600 + 12 * 60 + 7), "5h 12m");
        assert_eq!(span(12 * 86_400 + 3 * 3_600 + 59), "12d 3h");
        assert_eq!(span(-90), "1m 30s");
    }
}
