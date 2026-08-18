//! On-disk [`RetainedStore`] backed by `redb` (ADR 0018 phase 4).
//!
//! The persistent counterpart to
//! [`MemoryRetainedStore`](crate::MemoryRetainedStore): an in-memory topic → message
//! map serves reads (`matching`/`all`, on the subscribe hot path), and every `set` is
//! **write-through fsync'd** to a `redb` database before it returns, so retained
//! messages survive a restart. On `open` the map is reloaded from disk; cross-node
//! back-fill (ADR 0014 §3) still reconciles any divergence afterwards.
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
use crate::{RetainedStore, StorageError};
use async_trait::async_trait;
use bytes::Bytes;
use mqtt_core::{topic_matches, Message, QoS};
use redb::{Database, Durability, TableDefinition};
use std::collections::HashMap;
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
    /// In-memory cache (source of truth for reads).
    by_topic: Mutex<HashMap<String, Message>>,
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

        let mut by_topic = HashMap::new();
        let rtxn = db.begin_read().map_err(backend)?;
        let table = rtxn.open_table(RETAINED).map_err(backend)?;
        for item in table.range::<&str>(..).map_err(backend)? {
            let (k, v) = item.map_err(backend)?;
            if let Some(m) = decode(k.value(), v.value()) {
                by_topic.insert(k.value().to_string(), m);
            }
        }
        Ok(Self {
            by_topic: Mutex::new(by_topic),
            db: Arc::new(db),
        })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Message>> {
        self.by_topic
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
        let topic = message.topic.clone();
        // An empty-payload retained PUBLISH clears the topic (MQTT semantics).
        let value = if message.payload.is_empty() {
            None
        } else {
            Some(encode(message))
        };

        // Persist (fsync) before updating the cache, off the async worker.
        let db = self.db.clone();
        let topic_for_persist = topic.clone();
        tokio::task::spawn_blocking(move || persist(&db, &topic_for_persist, value.as_deref()))
            .await
            .map_err(backend)??;

        let mut map = self.lock();
        if message.payload.is_empty() {
            map.remove(&topic);
        } else {
            map.insert(topic, message.clone());
        }
        Ok(())
    }

    async fn matching(&self, filter: &str) -> Result<Vec<Message>, StorageError> {
        Ok(self
            .lock()
            .values()
            .filter(|m| topic_matches(filter, &m.topic))
            .cloned()
            .collect())
    }

    async fn all(&self) -> Result<Vec<Message>, StorageError> {
        Ok(self.lock().values().cloned().collect())
    }

    async fn count(&self) -> Result<usize, StorageError> {
        Ok(self.lock().len())
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
}
