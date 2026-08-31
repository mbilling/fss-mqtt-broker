//! On-disk [`RetainedStore`] backed by `redb` (ADR 0018 phase 4).
//!
//! The persistent counterpart to
//! [`MemoryRetainedStore`](crate::MemoryRetainedStore): an in-memory
//! [`RetainedState`] — the topic → message map plus its match index — serves reads
//! (`matching`/`all`, on the subscribe hot path), and every `set` is
//! **write-through fsync'd** to a `redb` database before it returns, so retained
//! messages survive a restart. On `open` that state is rebuilt from disk, index
//! included, so a reopened database indexes exactly the rows it holds; cross-node
//! back-fill (ADR 0014 §3) still reconciles any divergence afterwards.
//!
//! The index is a purely in-memory accelerator: nothing about it is persisted, and
//! the on-disk layout below is untouched by it.
//!
//! ## On-disk layout
//!
//! One table, `retained`, keyed by topic; the value is
//! `qos(1) ++ retain(1) ++ props_len(4) ++ props ++ payload` (the topic is the key, so
//! it is not repeated in the value; the properties block is
//! [`AppProps::encode`](crate::app_props::AppProps::encode)'s output, length-prefixed —
//! ADR 0038 T3, so a retained replay after restart carries the publisher's application
//! properties). An empty-payload `set` deletes the topic's row (MQTT zero-length
//! retained-PUBLISH semantics).

use crate::app_props::AppProps;
use crate::{RetainedState, RetainedStore, StorageError};
use async_trait::async_trait;
use bytes::Bytes;
use mqtt_core::{Message, QoS};
use redb::{Database, Durability, TableDefinition};
use std::fmt::Display;
use std::path::Path;
use std::sync::{Arc, Mutex};

/// The retained store's on-disk layout version (ADR 0038 T2). Reset to 1 with the
/// postcard codec succession (ADR 0052; the retired pre-release history fails closed
/// at the gate — wipe-and-rejoin). Bumped to 2 for the absolute expiry deadline in
/// the value (issue #227, MQTT 5 Message Expiry on retained copies) — pre-1.0, so
/// the bump is another fail-closed reshape, no migration (ADR 0039/0058).
pub const SCHEMA_VERSION: u32 = 2;

/// In-place migrations for `retained.redb` (ADR 0058). Empty by design at 1.0 — the first
/// post-1.0 schema bump lands its `MigrationStep` here in the same PR, or the coverage
/// test fails.
const RETAINED_MIGRATIONS: &[crate::schema::MigrationStep] = &[];
/// Oldest `retained.redb` version the contract migrates from (ADR 0058). Pinned to the
/// layout the v1.0.0 tag shipped — a literal, not [`SCHEMA_VERSION`], so a version
/// raise without its `MigrationStep` fails the coverage test rather than moving the
/// floor with the ceiling. Raised only when a release retires migrations (ADR 0039).
#[cfg(test)]
const RETAINED_MIGRATE_FLOOR: u32 = 2;

const RETAINED: TableDefinition<&str, &[u8]> = TableDefinition::new("retained");

fn backend<E: Display>(e: E) -> StorageError {
    StorageError::Backend(e.to_string())
}

/// Encode a retained message's value:
/// `qos ++ retain ++ expires_at ++ props_len ++ props ++ payload` (the topic is the
/// key; `expires_at` is Unix epoch seconds, `0` = never — issue #227).
fn encode(m: &Message) -> Vec<u8> {
    let props = AppProps::from(&m.app).encode();
    let mut out = Vec::with_capacity(14 + props.len() + m.payload.len());
    out.push(m.qos as u8);
    out.push(u8::from(m.retain));
    out.extend_from_slice(&m.expires_at.unwrap_or(0).to_be_bytes());
    out.extend_from_slice(&u32::try_from(props.len()).unwrap_or(u32::MAX).to_be_bytes());
    out.extend_from_slice(&props);
    out.extend_from_slice(&m.payload);
    out
}

/// Decode a value back into a [`Message`] for `topic`; `None` (row treated as absent,
/// fail-closed) on a malformed value.
fn decode(topic: &str, bytes: &[u8]) -> Option<Message> {
    let qos = QoS::from_u8(*bytes.first()?)?;
    let retain = *bytes.get(1)? != 0;
    let expires = u64::from_be_bytes(bytes.get(2..10)?.try_into().ok()?);
    let props_len = u32::from_be_bytes(bytes.get(10..14)?.try_into().ok()?) as usize;
    let props = AppProps::decode(bytes.get(14..14 + props_len)?)?;
    Some(Message {
        topic: topic.to_string(),
        payload: Bytes::copy_from_slice(bytes.get(14 + props_len..)?),
        qos,
        retain,
        app: props.into(),
        expires_at: (expires > 0).then_some(expires),
    })
}

/// A durable [`RetainedStore`] persisting to a `redb` file (ADR 0018 phase 4).
#[derive(Debug)]
pub struct PersistentRetainedStore {
    /// In-memory cache (source of truth for reads) and its derived match index,
    /// behind one lock — see [`RetainedState`].
    state: Mutex<RetainedState>,
    db: Arc<Database>,
}

impl PersistentRetainedStore {
    /// Open (creating if absent) the retained store at `path`, recovering its topics.
    ///
    /// # Errors
    /// [`StorageError::Backend`] if the database cannot be opened or decoded.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let db = crate::open::create_with_lock_retry(path).map_err(backend)?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        crate::schema::gate_or_migrate(&db, "retained.redb", SCHEMA_VERSION, RETAINED_MIGRATIONS)
            .map_err(backend)?;
        let txn = db.begin_write().map_err(backend)?;
        {
            let _ = txn.open_table(RETAINED).map_err(backend)?;
        }
        txn.commit().map_err(backend)?;

        // Rebuild the cache AND its match index from the rows on disk: the index
        // is in-memory only, so this reload is the single place a restart could
        // otherwise leave `matching` blind to a persisted topic. Both go through
        // `RetainedState::insert`, so they cannot be rebuilt out of step.
        let mut state = RetainedState::default();
        let rtxn = db.begin_read().map_err(backend)?;
        let table = rtxn.open_table(RETAINED).map_err(backend)?;
        for item in table.range::<&str>(..).map_err(backend)? {
            let (k, v) = item.map_err(backend)?;
            if let Some(m) = decode(k.value(), v.value()) {
                state.insert(k.value(), m);
            }
        }
        Ok(Self {
            state: Mutex::new(state),
            db: Arc::new(db),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RetainedState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Durably set (or, with `None`, clear) a topic's retained row in one fsync'd write.
fn persist(db: &Database, topic: &str, value: Option<&[u8]>) -> Result<(), StorageError> {
    let mut txn = db.begin_write().map_err(backend)?;
    txn.set_durability(Durability::Immediate); // fsync on commit (ADR 0018)
    {
        let mut table = txn.open_table(RETAINED).map_err(backend)?;
        match value {
            Some(v) => {
                table.insert(topic, v).map_err(backend)?;
            }
            None => {
                table.remove(topic).map_err(backend)?;
            }
        }
    }
    txn.commit().map_err(backend)?;
    Ok(())
}

#[async_trait]
impl RetainedStore for PersistentRetainedStore {
    async fn set(&self, message: &Message) -> Result<(), StorageError> {
        // An empty-payload retained PUBLISH clears the topic (MQTT semantics).
        let value = if message.payload.is_empty() {
            None
        } else {
            Some(encode(message))
        };

        // Persist (fsync) before updating the cache, off the async worker.
        let db = self.db.clone();
        let topic_for_persist = message.topic.clone();
        tokio::task::spawn_blocking(move || persist(&db, &topic_for_persist, value.as_deref()))
            .await
            .map_err(backend)??;

        // Cache and index move together, under one lock, after the durable write —
        // the ordering (and its fsync) is exactly as it was.
        self.lock().set(message);
        Ok(())
    }

    async fn matching(&self, filter: &str) -> Result<Vec<Message>, StorageError> {
        Ok(self.lock().matching(filter))
    }

    async fn all(&self) -> Result<Vec<Message>, StorageError> {
        Ok(self.lock().all())
    }

    async fn count(&self) -> Result<usize, StorageError> {
        Ok(self.lock().len())
    }

    async fn contains(&self, topic: &str) -> Result<bool, StorageError> {
        Ok(self.lock().contains(topic))
    }
}

#[cfg(test)]
mod tests {
    /// ADR 0058 T2: the retained.redb migration registry must cover the contract range, so a
    /// future schema bump without its migration fails here, not at an operator's upgrade.
    #[test]
    fn the_migration_registry_covers_the_contract_range() {
        crate::schema::assert_migrations_cover(
            super::RETAINED_MIGRATE_FLOOR,
            super::SCHEMA_VERSION,
            super::RETAINED_MIGRATIONS,
        )
        .expect("retained.redb migration registry has a gap");
    }

    /// ADR 0038 T2: a retained store stamped by a foreign layout version refuses to
    /// open, naming both versions.
    #[test]
    fn a_foreign_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retained.redb");
        drop(super::PersistentRetainedStore::open(&path).unwrap()); // stamped current
        {
            let db = redb::Database::create(&path).unwrap();
            crate::schema::force_version(&db, 999).unwrap();
        }
        let err = super::PersistentRetainedStore::open(&path)
            .unwrap_err()
            .to_string();
        assert!(err.contains("v999") && err.contains("expects v2"), "{err}");
    }

    use super::PersistentRetainedStore;
    use crate::RetainedStore;
    use bytes::Bytes;
    use mqtt_core::{Message, QoS};

    fn msg(topic: &str, payload: &[u8]) -> Message {
        Message::new(
            topic.to_string(),
            Bytes::copy_from_slice(payload),
            QoS::AtLeastOnce,
            true,
        )
    }

    /// Retained messages survive the database being closed and reopened, an empty
    /// payload clears the topic durably, and wildcard matching works after reopen.
    #[tokio::test]
    async fn retained_survives_reopen_and_clear_persists() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retained.redb");
        {
            let store = PersistentRetainedStore::open(&path).unwrap();
            let mut with_props = msg("home/a", b"1");
            with_props.app = mqtt_core::AppProperties {
                payload_format: Some(1),
                content_type: Some("application/json".into()),
                response_topic: Some("replies/a".into()),
                correlation_data: Some(Bytes::from_static(&[9, 9])),
                user_properties: vec![("origin".into(), "sensor-7".into())],
            };
            store.set(&with_props).await.unwrap();
            store.set(&msg("home/b", b"2")).await.unwrap();
            store.set(&msg("away/c", b"3")).await.unwrap();
            // Clear home/b with an empty payload.
            store.set(&msg("home/b", b"")).await.unwrap();
        }

        let store = PersistentRetainedStore::open(&path).unwrap();
        // home/b stayed cleared across the reopen; home/a and away/c survived.
        let mut all: Vec<_> = store
            .all()
            .await
            .unwrap()
            .into_iter()
            .map(|m| m.topic)
            .collect();
        all.sort();
        assert_eq!(all, vec!["away/c".to_string(), "home/a".to_string()]);

        // Wildcard matching and payload/qos fidelity after reopen.
        let matched = store.matching("home/+").await.unwrap();
        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].topic, "home/a");
        assert_eq!(&matched[0].payload[..], b"1");
        assert_eq!(matched[0].qos, QoS::AtLeastOnce);
        assert!(matched[0].retain);
        // Application properties replay exactly as published across the restart
        // (ADR 0038 T3).
        let app = &matched[0].app;
        assert_eq!(app.payload_format, Some(1));
        assert_eq!(app.content_type.as_deref(), Some("application/json"));
        assert_eq!(app.response_topic.as_deref(), Some("replies/a"));
        assert_eq!(app.correlation_data.as_deref(), Some(&[9u8, 9][..]));
        assert_eq!(
            app.user_properties,
            vec![("origin".to_string(), "sensor-7".to_string())]
        );
    }

    /// Corpus for the reopen check: wildcards at every position, empty levels, and
    /// `$`-rooted topics — kept small because every `set` here is an fsync.
    fn reopen_corpus() -> (Vec<String>, Vec<String>) {
        let alphabet = ["a", "b", ""];
        let mut topics: Vec<String> = Vec::new();
        for d1 in alphabet {
            topics.push(d1.to_string());
            for d2 in alphabet {
                topics.push(format!("{d1}/{d2}"));
                for d3 in alphabet {
                    topics.push(format!("{d1}/{d2}/{d3}"));
                }
            }
        }
        for extra in ["$SYS", "$SYS/broker", "$share/g/a"] {
            topics.push(extra.to_string());
        }
        let mut filters = topics.clone();
        for extra in [
            "#",
            "+",
            "+/#",
            "a/#",
            "a/+",
            "+/b",
            "+/+/+",
            "a/+/b",
            "$SYS/#",
            "$SYS/+",
            "$SYS/broker",
            "a//b",
            "/a",
        ] {
            filters.push(extra.to_string());
        }
        (topics, filters)
    }

    /// The match index is an in-memory accelerator over the redb rows, so `open` must
    /// REBUILD it from disk: a store that reloaded the map but not the index would
    /// answer every wildcard subscribe with nothing after a restart, silently. Checked
    /// the only way that proves it — against the linear `topic_matches` scan the index
    /// replaced, over the whole corpus, after the reopen.
    #[tokio::test]
    async fn the_match_index_is_rebuilt_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retained.redb");
        let (mut topics, filters) = reopen_corpus();
        {
            let store = PersistentRetainedStore::open(&path).unwrap();
            for t in &topics {
                store
                    .set(&msg(t, format!("v:{t}").as_bytes()))
                    .await
                    .unwrap();
            }
            // One cleared before the close: the reopened index must not resurrect it.
            store.set(&msg("a/b", b"")).await.unwrap();
        }
        topics.retain(|t| t != "a/b");

        let store = PersistentRetainedStore::open(&path).unwrap();
        assert_eq!(store.count().await.unwrap(), topics.len());
        for f in &filters {
            let mut got: Vec<String> = store
                .matching(f)
                .await
                .unwrap()
                .into_iter()
                .map(|m| m.topic)
                .collect();
            got.sort();
            let mut want: Vec<String> = topics
                .iter()
                .filter(|t| mqtt_core::topic_matches(f, t))
                .cloned()
                .collect();
            want.sort();
            assert_eq!(got, want, "rebuilt index disagrees with the scan on {f:?}");
        }

        // `contains` is answered off the same rebuilt state.
        assert!(store.contains("a/a/b").await.unwrap());
        assert!(!store.contains("a/b").await.unwrap());
        assert!(!store.contains("never/stored").await.unwrap());

        // And the rebuilt index stays live: a set and a clear after the reopen move
        // map and index together, durably.
        store.set(&msg("a/b", b"back")).await.unwrap();
        assert!(store.contains("a/b").await.unwrap());
        // "a/b", "b/b" and "/b" — the re-inserted topic is back in the index.
        assert_eq!(store.matching("+/b").await.unwrap().len(), 3);
        store.set(&msg("a/a/b", b"")).await.unwrap();
        // "a/b/b" and "a//b" survive the sibling's removal.
        assert_eq!(store.matching("a/+/b").await.unwrap().len(), 2);
        drop(store);

        let store = PersistentRetainedStore::open(&path).unwrap();
        assert!(store.contains("a/b").await.unwrap());
        assert!(!store.contains("a/a/b").await.unwrap());
        assert_eq!(store.matching("+/b").await.unwrap().len(), 3);
        assert_eq!(store.matching("a/+/b").await.unwrap().len(), 2);
    }
}
