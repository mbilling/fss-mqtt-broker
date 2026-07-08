//! Admission caps ([ADR 0041](../../../docs/adr/0041-resource-governance.md) T1):
//! global and per-source-IP connection limits, enforced **at accept, before any
//! TLS handshake work** — completing a handshake (or an MQTT exchange) just to
//! refuse a connection would spend exactly the CPU the cap exists to protect.
//!
//! The gate hands out RAII [`AdmissionPermit`]s: the accept loop asks
//! [`try_admit`](AdmissionGate::try_admit) before spawning the connection task,
//! moves the permit into the task, and the slot frees itself when the task ends
//! (however it ends). Unconfigured caps admit everything — today's behavior.
//!
//! The per-IP table cannot become the resource leak it guards against: an entry
//! exists only while at least one **live** connection from that address holds a
//! permit (removed at zero), so the table is bounded by the live-connection count
//! — each entry is backed by a real socket, itself bounded by the global cap and
//! the process's descriptor limit.
//!
//! The gate also runs the **auth-failure penalty box** (T2): repeated
//! authentication failures from a source address put it in a decaying penalty —
//! while penalized, its connections are closed at accept, before any Argon2 work,
//! converting a brute-force or CPU-burn attempt into a self-limiting trickle. The
//! penalty keys on the attacker's **address only**, never on a username (a
//! username-keyed lockout would be a denial-of-service lever aimed at a victim's
//! credentials), and its table is hard-bounded with evict-oldest so address
//! spraying cannot grow it.

use mqtt_observability::metrics::Metrics;
use mqtt_observability::AuditSink;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, warn};

/// Hard bound on the penalty table (T2): under an address-spraying attack the
/// oldest entry is evicted — the accounting can never outgrow this.
const PENALTY_TABLE_MAX: usize = 4096;

/// The auth-failure penalty policy (ADR 0041 T2); see [`AdmissionGate::record_auth_failure`].
#[derive(Debug, Clone, Copy)]
pub struct PenaltyConfig {
    /// Strikes (failed authentications) at which an address becomes penalized.
    pub threshold: u32,
    /// How long one strike takes to decay away. An address with `threshold`
    /// strikes is refused for roughly `decay` per strike above the line.
    pub decay: Duration,
}

/// Shared connection-admission state; cheap to clone (one `Arc`).
#[derive(Clone)]
pub struct AdmissionGate {
    inner: Arc<Inner>,
}

struct Inner {
    /// Global concurrent-connection ceiling; `None` = uncapped.
    max_connections: Option<usize>,
    /// Per-source-IP concurrent-connection ceiling; `None` = uncapped.
    max_per_ip: Option<usize>,
    /// The auth-failure penalty policy; `None` = penalty box disabled.
    penalty: Option<PenaltyConfig>,
    /// Live counts. One mutex over both — admission is an accept-rate operation,
    /// not a per-message one, and a single lock keeps the two counts atomic.
    state: Mutex<State>,
    metrics: Option<Arc<Metrics>>,
    /// Audit sink for `security.penalty` records (an address crossing into the
    /// penalized state is a security-relevant event); `None` in tests.
    audit: Option<Arc<dyn AuditSink>>,
}

#[derive(Default)]
struct State {
    active: usize,
    per_ip: HashMap<IpAddr, usize>,
    /// Auth-failure strikes per source address (T2); decayed lazily on access,
    /// hard-bounded at [`PENALTY_TABLE_MAX`] with evict-oldest.
    penalties: HashMap<IpAddr, Strikes>,
}

/// One address's decaying strike count.
struct Strikes {
    /// Strike level as of `at` (fractional: decay is continuous).
    level: f64,
    /// When `level` was last recomputed.
    at: Instant,
}

impl Strikes {
    /// The strike level now, after decay.
    fn current(&self, now: Instant, decay: Duration) -> f64 {
        let elapsed = now.saturating_duration_since(self.at).as_secs_f64();
        (self.level - elapsed / decay.as_secs_f64()).max(0.0)
    }
}

impl std::fmt::Debug for AdmissionGate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdmissionGate")
            .field("max_connections", &self.inner.max_connections)
            .field("max_per_ip", &self.inner.max_per_ip)
            .finish_non_exhaustive()
    }
}

impl AdmissionGate {
    /// A gate with the given caps (`None` = uncapped). Caps of `0` are the
    /// caller's startup-validation problem — this type treats them literally
    /// (admit nothing).
    #[must_use]
    pub fn new(
        max_connections: Option<usize>,
        max_per_ip: Option<usize>,
        metrics: Option<Arc<Metrics>>,
    ) -> Self {
        Self::with_penalty(max_connections, max_per_ip, None, metrics, None)
    }

    /// A gate with connection caps **and** the auth-failure penalty box
    /// (ADR 0041 T2). `penalty: None` disables the box.
    #[must_use]
    pub fn with_penalty(
        max_connections: Option<usize>,
        max_per_ip: Option<usize>,
        penalty: Option<PenaltyConfig>,
        metrics: Option<Arc<Metrics>>,
        audit: Option<Arc<dyn AuditSink>>,
    ) -> Self {
        AdmissionGate {
            inner: Arc::new(Inner {
                max_connections,
                max_per_ip,
                penalty,
                state: Mutex::new(State::default()),
                metrics,
                audit,
            }),
        }
    }

    /// A gate that admits everything — for setups and tests without caps.
    #[must_use]
    pub fn unlimited() -> Self {
        Self::new(None, None, None)
    }

    /// Try to admit one connection from `ip` (`None` when the transport has no
    /// usable peer address). Returns the RAII permit, or `None` when a cap is
    /// hit — the caller drops the socket immediately, counted and logged, before
    /// any further work.
    #[must_use]
    pub fn try_admit(&self, ip: Option<IpAddr>) -> Option<AdmissionPermit> {
        let mut state = self.lock();
        // Penalty box first (T2): a penalized address must not even consume a
        // global slot's worth of consideration.
        if let (Some(cfg), Some(ip)) = (self.inner.penalty, ip) {
            // Penalized once the threshold was crossed, until a full strike has
            // decayed (`> threshold - 1`): strikes are integral additions on a
            // continuous decay, so comparing `>= threshold` would un-penalize an
            // epsilon after the crossing.
            let penalized = state.penalties.get(&ip).is_some_and(|s| {
                s.current(Instant::now(), cfg.decay) > f64::from(cfg.threshold - 1)
            });
            if penalized {
                drop(state);
                debug!(%ip, "connection refused at accept: auth-failure penalty");
                self.reject("penalty");
                return None;
            }
        }
        if let Some(max) = self.inner.max_connections {
            if state.active >= max {
                drop(state);
                debug!(
                    ?ip,
                    max, "connection refused at accept: max-connections cap"
                );
                self.reject("max-connections");
                return None;
            }
        }
        if let (Some(max), Some(ip)) = (self.inner.max_per_ip, ip) {
            if state.per_ip.get(&ip).copied().unwrap_or(0) >= max {
                drop(state);
                debug!(%ip, max, "connection refused at accept: per-ip cap");
                self.reject("per-ip");
                return None;
            }
        }
        state.active += 1;
        if let Some(ip) = ip {
            if self.inner.max_per_ip.is_some() {
                *state.per_ip.entry(ip).or_insert(0) += 1;
            }
        }
        drop(state);
        Some(AdmissionPermit {
            inner: self.inner.clone(),
            ip,
        })
    }

    /// Record one failed authentication from `ip` (ADR 0041 T2). Called by the
    /// accept-loop wrapper when a connection's CONNECT failed authentication —
    /// never for authorization denials, and never keyed by username. Crossing the
    /// threshold is audited (`security.penalty`); while over it, the address's
    /// connections are refused at accept until the strikes decay.
    pub fn record_auth_failure(&self, ip: Option<IpAddr>) {
        let (Some(cfg), Some(ip)) = (self.inner.penalty, ip) else {
            return;
        };
        let now = Instant::now();
        let mut state = self.lock();
        // Hard-bound the table: a fresh address evicts the oldest entry when full,
        // so address spraying cycles the table instead of growing it.
        if !state.penalties.contains_key(&ip) && state.penalties.len() >= PENALTY_TABLE_MAX {
            if let Some(oldest) = state
                .penalties
                .iter()
                .min_by_key(|(_, s)| s.at)
                .map(|(ip, _)| *ip)
            {
                state.penalties.remove(&oldest);
            }
        }
        let entry = state.penalties.entry(ip).or_insert(Strikes {
            level: 0.0,
            at: now,
        });
        let before = entry.current(now, cfg.decay);
        let after = before + 1.0;
        *entry = Strikes {
            level: after,
            at: now,
        };
        drop(state);
        if before < f64::from(cfg.threshold) && after >= f64::from(cfg.threshold) {
            warn!(%ip, threshold = cfg.threshold,
                  "address penalized: repeated authentication failures (ADR 0041)");
            if let Some(a) = &self.inner.audit {
                a.record(
                    "security.penalty",
                    None,
                    &format!("address {ip} penalized after repeated auth failures"),
                );
            }
        }
    }

    fn reject(&self, reason: &str) {
        if let Some(m) = &self.inner.metrics {
            m.admission_rejected(reason);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// One admitted connection's slot; dropping it frees the slot (global and
/// per-IP), however the connection ended.
#[derive(Debug)]
pub struct AdmissionPermit {
    inner: Arc<Inner>,
    ip: Option<IpAddr>,
}

impl std::fmt::Debug for Inner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Inner").finish_non_exhaustive()
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut state = self
            .inner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.active = state.active.saturating_sub(1);
        if let Some(ip) = self.ip {
            if let Some(count) = state.per_ip.get_mut(&ip) {
                *count -= 1;
                if *count == 0 {
                    // Remove-at-zero keeps the table bounded by live connections.
                    state.per_ip.remove(&ip);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(last: u8) -> IpAddr {
        IpAddr::from([127, 0, 0, last])
    }

    /// The global cap admits up to N, refuses the N+1-th, and a freed slot is
    /// immediately reusable.
    #[test]
    fn the_global_cap_refuses_at_the_bound_and_recovers() {
        let gate = AdmissionGate::new(Some(2), None, None);
        let a = gate.try_admit(Some(ip(1))).expect("first admitted");
        let _b = gate.try_admit(Some(ip(2))).expect("second admitted");
        assert!(
            gate.try_admit(Some(ip(3))).is_none(),
            "the cap must refuse the third concurrent connection"
        );
        drop(a);
        assert!(
            gate.try_admit(Some(ip(3))).is_some(),
            "a freed slot must be reusable"
        );
    }

    /// The per-IP cap counts each address separately, and its table shrinks back
    /// to empty when the last connection from an address ends.
    #[test]
    fn the_per_ip_cap_is_independent_and_the_table_shrinks_to_empty() {
        let gate = AdmissionGate::new(None, Some(1), None);
        let a = gate.try_admit(Some(ip(1))).expect("first from .1");
        assert!(
            gate.try_admit(Some(ip(1))).is_none(),
            "a second connection from the same address must be refused"
        );
        let b = gate
            .try_admit(Some(ip(2)))
            .expect("a different address is unaffected");
        drop(a);
        let a2 = gate
            .try_admit(Some(ip(1)))
            .expect("the freed per-ip slot must be reusable");
        drop(a2);
        drop(b);
        assert!(
            gate.lock().per_ip.is_empty(),
            "the per-ip table must shrink to empty (bounded by live connections)"
        );
    }

    /// ADR 0041 T2: crossing the failure threshold penalizes the address — its
    /// connections are refused at accept — while a different address is
    /// unaffected, and the penalty decays back to admission.
    #[test]
    fn the_penalty_box_refuses_after_the_threshold_and_decays() {
        let gate = AdmissionGate::with_penalty(
            None,
            None,
            Some(PenaltyConfig {
                threshold: 2,
                decay: Duration::from_millis(50),
            }),
            None,
            None,
        );
        // One failure: still admitted.
        gate.record_auth_failure(Some(ip(1)));
        assert!(gate.try_admit(Some(ip(1))).is_some());
        // Second failure crosses the threshold: refused...
        gate.record_auth_failure(Some(ip(1)));
        assert!(
            gate.try_admit(Some(ip(1))).is_none(),
            "the penalized address must be refused at accept"
        );
        // ...while a different address still connects.
        assert!(
            gate.try_admit(Some(ip(2))).is_some(),
            "the penalty must key on the failing address only"
        );
        // The strikes decay: after ~2 decay periods the level drops below the
        // threshold and the address is admitted again.
        std::thread::sleep(Duration::from_millis(120));
        assert!(
            gate.try_admit(Some(ip(1))).is_some(),
            "the penalty must decay back to admission"
        );
    }

    /// ADR 0041 T2: the penalty table is hard-bounded — spraying failures from
    /// many addresses cycles the table (evict-oldest) instead of growing it.
    #[test]
    fn the_penalty_table_is_bounded_under_address_spraying() {
        let gate = AdmissionGate::with_penalty(
            None,
            None,
            Some(PenaltyConfig {
                threshold: 100, // never actually penalize; we only exercise the table
                decay: Duration::from_secs(3600),
            }),
            None,
            None,
        );
        // Spray failures from far more addresses than the table bound.
        for a in 0..=255u8 {
            for b in 0..=31u8 {
                gate.record_auth_failure(Some(IpAddr::from([10, 0, a, b])));
            }
        }
        let len = gate.lock().penalties.len();
        assert!(
            len <= PENALTY_TABLE_MAX,
            "the penalty table must never exceed its bound (len={len})"
        );
    }

    /// No caps configured = today's behavior; an address-less transport is
    /// admitted and never touches the per-ip table.
    #[test]
    fn unconfigured_caps_admit_everything() {
        let gate = AdmissionGate::unlimited();
        let permits: Vec<_> = (0..1000).map(|_| gate.try_admit(None).unwrap()).collect();
        assert_eq!(gate.lock().active, 1000);
        assert!(gate.lock().per_ip.is_empty());
        drop(permits);
        assert_eq!(gate.lock().active, 0);
    }
}
