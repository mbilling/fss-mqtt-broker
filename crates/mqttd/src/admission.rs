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

use mqtt_observability::metrics::Metrics;
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use tracing::debug;

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
    /// Live counts. One mutex over both — admission is an accept-rate operation,
    /// not a per-message one, and a single lock keeps the two counts atomic.
    state: Mutex<State>,
    metrics: Option<Arc<Metrics>>,
}

#[derive(Default)]
struct State {
    active: usize,
    per_ip: HashMap<IpAddr, usize>,
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
        AdmissionGate {
            inner: Arc::new(Inner {
                max_connections,
                max_per_ip,
                state: Mutex::new(State::default()),
                metrics,
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
