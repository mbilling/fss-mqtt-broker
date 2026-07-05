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

/// The retained store's on-disk layout version (ADR 0038 T2). v2: application
/// properties in the value (ADR 0038 T3) — pre-1.0, a v1 file fails closed at the
/// gate (wipe-and-rejoin) rather than silently decoding payload bytes as properties.
const SCHEMA_VERSION: u32 = 2;

const RETAINED: TableDefinition<&str, &[u8]> = TableDefinition::new("retained");

fn backend<E: Display>(e: E) -> StorageError {
    StorageError::Backend(e.to_string())
}

/// Encode a retained message's value: `qos ++ retain ++ props_len ++ props ++ payload`
/// (the topic is the key).
fn encode(m: &Message) -> Vec<u8> {
    let props = AppProps::from(&m.app).encode();
    let mut out = Vec::with_capacity(6 + props.len() + m.payload.len());
    out.push(m.qos as u8);
    out.push(u8::from(m.retain));
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
    let props_len = u32::from_be_bytes(bytes.get(2..6)?.try_into().ok()?) as usize;
    let props = AppProps::decode(bytes.get(6..6 + props_len)?)?;
    Some(Message {
        topic: topic.to_string(),
        payload: Bytes::copy_from_slice(bytes.get(6 + props_len..)?),
        qos,
        retain,
        app: props.into(),
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
        let db = Database::create(path).map_err(backend)?;
        // Layout version gate (ADR 0038 T2): stamp fresh, fail closed on foreign.
        crate::schema::gate(&db, "retained.redb", SCHEMA_VERSION).map_err(backend)?;
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
    /// ADR 0038 T2: a retained store stamped by a foreign layout version refuses to
    /// open, naming both versions.
    #[test]
    fn a_foreign_schema_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("retained.redb");
        drop(super::PersistentRetainedStore::open(&path).unwrap()); // stamped v2
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
