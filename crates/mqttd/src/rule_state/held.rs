//! What a store holds, key by key: for a replayed sequence, which shows what each step
//! changed ([ADR 0085](../../../../docs/adr/0085-rule-state-store.md) D11).

use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;

use mqtt_rules::Value;

use super::{lock, Store};

/// One key of a store, as [`Store::held`] lists it.
#[derive(Debug, Clone)]
pub struct HeldKey {
    /// Its namespace: a declared name, `kv_store`, or `rule:<id>`.
    pub namespace: Arc<str>,
    /// The key: a `kv_*` key's bytes, or the [`term_key`](mqtt_rules::state::term_key)
    /// of a `kv_store_*` key.
    pub key: Arc<[u8]>,
    /// The type its creating operation gave it (`lww`, `first`, `max`, `min`, `sum`,
    /// `union`, `hll`).
    pub kind: &'static str,
    /// What `kv_get` would read: the value, a counter's count, a set's elements, a
    /// sketch's estimate.
    pub value: Value,
    /// When it expires, in the store's milliseconds; `0`: never.
    pub expires_ms: u64,
}

impl Store {
    /// Every key the store holds now, by namespace and then by key. A key past its
    /// expiry is not listed: it reads as absent. Listing is not a use (the order an
    /// `lru` namespace evicts in is as it was) and removes nothing.
    ///
    /// One shard's lock at a time, each for as long as its keys take to copy: for a
    /// dry run's store, which nothing else calls. The running store is not listed
    /// this way.
    #[must_use]
    pub fn held(&self) -> Vec<HeldKey> {
        let now_ms = self.shared.clock.now_ms();
        let namespaces: Vec<_> = lock(&self.namespaces)
            .values()
            .filter_map(std::sync::Weak::upgrade)
            .collect();
        let mut held = Vec::new();
        for namespace in namespaces {
            if namespace.keys.load(Relaxed) == 0 {
                continue;
            }
            for shard in &*namespace.shards {
                let shard = lock(shard);
                held.extend(
                    shard
                        .map
                        .values()
                        .filter(|entry| !entry.expired(now_ms))
                        .map(|entry| HeldKey {
                            namespace: namespace.name.clone(),
                            key: entry.key.clone(),
                            kind: entry.data.type_name(),
                            value: entry.data.read(),
                            expires_ms: entry.expires_ms,
                        }),
                );
            }
        }
        held.sort_unstable_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
        held
    }
}
