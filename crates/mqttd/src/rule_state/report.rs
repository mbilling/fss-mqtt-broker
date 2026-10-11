//! What the store reports of itself (ADR 0085 D10): each namespace's settings and
//! counts, as the admin API, `$SYS` and `/metrics` show them.
//!
//! One reading serves all three ([`namespaces`]), and none of them carries a key or a
//! value: a key is built from client ids, topic levels and payload fields, and a value
//! is payload data. Those are an operator's to read, one key at a time, over the admin
//! API.

use std::collections::HashMap;

use mqtt_observability::metrics::Metrics;
use mqtt_rules::state::{Consistency, Eviction, NamespaceSettings, KV_STORE_NAMESPACE};
use mqtt_rules::RuleSet;
use serde_json::{json, Value};

use super::{NamespaceCounts, Refused, Replication, Store, OPS, RESULTS};

/// One namespace the running rules hold: its declaration and what it holds now.
#[derive(Debug, Clone)]
pub struct NamespaceReport {
    /// A declared name, `kv_store`, or `rule:<id>`.
    pub name: String,
    /// Its settings, as the running rules file declares them.
    pub settings: NamespaceSettings,
    /// Its counts.
    pub counts: NamespaceCounts,
    /// How its replication stands, for an `eventual` namespace.
    pub replication: Option<Replication>,
}

/// The namespaces the running rules `set` holds in `store`, in name order.
#[must_use]
pub fn namespaces(store: &Store, set: &RuleSet) -> Vec<NamespaceReport> {
    set.state_namespaces()
        .filter_map(|(name, settings)| {
            Some(NamespaceReport {
                name: name.to_string(),
                settings: settings.clone(),
                counts: store.counts(name)?,
                replication: store.replication(name),
            })
        })
        .collect()
}

/// What a namespace is: one the file declares, EMQX's table, or a rule's own.
#[must_use]
pub fn kind(name: &str) -> &'static str {
    if name == KV_STORE_NAMESPACE {
        "kv_store"
    } else if name.starts_with("rule:") {
        "rule"
    } else {
        "declared"
    }
}

impl NamespaceReport {
    /// The namespace as the admin API and `$SYS` show it: its mode, its bounds and its
    /// counts. No key and no value.
    #[must_use]
    pub fn json(&self) -> Value {
        let (s, c) = (&self.settings, &self.counts);
        let refused = c.refused();
        let by_reason: serde_json::Map<String, Value> = RESULTS[1..]
            .iter()
            .zip(refused)
            .map(|(reason, n)| ((*reason).to_string(), json!(n)))
            .collect();
        let mut doc = json!({
            "namespace": self.name,
            "kind": kind(&self.name),
            "consistency": match s.consistency {
                Consistency::Local => "local",
                Consistency::Eventual => "eventual",
            },
            "eviction": match s.eviction {
                Eviction::Fail => "fail",
                Eviction::Lru => "lru",
            },
            "max_keys": s.max_keys,
            "max_bytes": s.max_bytes,
            "default_ttl_secs": s.default_ttl_secs,
            "max_ttl_secs": s.max_ttl_secs,
            "keys": c.keys,
            "bytes": c.bytes,
            "calls": c.calls_ok(),
            "refused": refused.iter().sum::<u64>(),
            "refused_by": by_reason,
            "expired": c.expired,
            "expiry_backlog": c.expiry_backlog,
            "evicted": c.evicted,
            "deleted": c.deleted,
        });
        // An `eventual` namespace: how its replication stands.
        if let Some(r) = &self.replication {
            let refused: serde_json::Map<String, Value> = Refused::ALL
                .iter()
                .map(|reason| (reason.as_str().to_string(), json!(r.refused(*reason))))
                .collect();
            doc["tombstone_window_secs"] = json!(s.tombstone_window_secs);
            doc["replication"] = json!({
                "links": r.links,
                "pending": r.pending,
                "lag_ms": r.lag_ms,
                "merged": r.merged,
                "refused_by": refused,
                "expiry_members": r.expiry_members,
            });
        }
        doc
    }
}

/// Moves the store's counts into the Prometheus series (ADR 0085 D10).
///
/// The store counts its calls where they happen, under locks they already hold, and
/// this reads those counts when someone looks: at a scrape, and on a timer for the
/// OTLP export. It remembers what it exported, so a counter moves by what happened
/// since, and a namespace that was dropped and declared again (whose counts start over)
/// carries on from where the series was.
#[derive(Debug, Default)]
pub struct Exporter {
    seen: HashMap<String, NamespaceCounts>,
    dictionary_refused: u64,
}

impl Exporter {
    /// Export what `store` holds for the running rules `set`.
    pub fn export(&mut self, store: &Store, set: &RuleSet, metrics: &Metrics) {
        let reports = namespaces(store, set);
        self.seen.retain(|name, _| {
            let held = reports.iter().any(|r| r.name == *name);
            if !held {
                metrics.forget_rule_state(name);
            }
            held
        });
        for report in reports {
            let now = report.counts;
            let before = self
                .seen
                .get(&report.name)
                .filter(|before| before.instance == now.instance)
                .copied()
                .unwrap_or_default();
            metrics.set_rule_state(&report.name, now.keys, now.bytes, now.expiry_backlog);
            for (op, name) in OPS.iter().enumerate() {
                let ok = now.ok[op].saturating_sub(before.ok[op]);
                if ok > 0 {
                    metrics.rule_state_ops(&report.name, name, RESULTS[0], ok);
                }
                for (reason, result) in RESULTS[1..].iter().enumerate() {
                    let n = now.failed[op][reason].saturating_sub(before.failed[op][reason]);
                    if n > 0 {
                        metrics.rule_state_ops(&report.name, name, result, n);
                    }
                }
            }
            let expired = now.expired.saturating_sub(before.expired);
            if expired > 0 {
                metrics.rule_state_expired(&report.name, expired);
            }
            self.seen.insert(report.name, now);
        }
        let totals = store.totals();
        metrics.rule_state_dictionaries(
            totals.dictionary_bytes,
            totals
                .dictionary_refused
                .saturating_sub(self.dictionary_refused),
        );
        self.dictionary_refused = totals.dictionary_refused;
    }
}
