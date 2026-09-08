//! #577: a completion's snapshot must not erase another delivery's durable ID.
use super::*;
use crate::repl::{InMemoryReplicatedLog, LogEntry, ReplError};
use std::{
    future::{poll_fn, Future},
    sync::{Arc, Mutex},
    task::Poll,
};
use tokio::sync::oneshot;

#[derive(Debug, Default)]
struct GatedLog {
    inner: InMemoryReplicatedLog,
    gate: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait]
impl ReplicatedLog for GatedLog {
    type Key = String;
    async fn append(&self, key: &String, record: Vec<u8>) -> Result<u64, ReplError> {
        let gate = self.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.await
                .expect("test releases the parked metadata append");
        }
        self.inner.append(key, record).await
    }
    async fn read(
        &self,
        key: &String,
        after: u64,
        limit: usize,
    ) -> Result<Vec<LogEntry>, ReplError> {
        self.inner.read(key, after, limit).await
    }
    async fn truncate(&self, key: &String, up_to: u64) -> Result<(), ReplError> {
        self.inner.truncate(key, up_to).await
    }
    async fn remove(&self, key: &String) -> Result<(), ReplError> {
        self.inner.remove(key).await
    }
}

#[tokio::test]
async fn clearing_one_id_cannot_erase_a_concurrent_delivery_identity() {
    let log = Arc::new(GatedLog::default());
    let store = ReplicatedSessionStore::new(log.clone());
    let client = ClientId("metadata-race".into());
    store.record_outbound(&client, 7, 41).await.unwrap();
    let (release, gate) = oneshot::channel();
    *log.gate.lock().unwrap() = Some(gate);
    let mut clear = Box::pin(store.clear_outbound(&client, 7));
    // Poll exactly once: the old snapshot has been read and its replacement is
    // now parked before append. No sleeps or scheduler-luck race reproduction.
    poll_fn(|cx| {
        assert!(clear.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let other = ClientId("unrelated-metadata".into());
    let mut unrelated = Box::pin(store.record_outbound(&other, 9, 43));
    let unrelated_poll = poll_fn(|cx| Poll::Ready(unrelated.as_mut().poll(cx))).await;
    assert!(
        matches!(unrelated_poll, Poll::Ready(Ok(()))),
        "a parked session must not hold an unrelated session's metadata lock"
    );
    let mut record = Box::pin(store.record_outbound(&client, 8, 42));
    let record_poll = poll_fn(|cx| Poll::Ready(record.as_mut().poll(cx))).await;
    release.send(()).unwrap();
    clear.await.unwrap();
    match record_poll {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => record.await.unwrap(),
    }
    let ids = store.outbound(&client).await.unwrap();
    assert_eq!(
        ids.len(),
        1,
        "clear must not overwrite a newer delivery snapshot"
    );
    assert_eq!(ids[0].packet_id, 8);
    assert_eq!(ids[0].offset, 42);
}

#[tokio::test]
async fn metadata_lock_pruning_and_waiter_cancellation_preserve_exclusion() {
    let store = ReplicatedSessionStore::new(InMemoryReplicatedLog::default());
    let client = ClientId("held".into());
    let held = store.lock_meta(&client).await;
    let mut waiter = Box::pin(store.lock_meta(&client));
    poll_fn(|cx| {
        assert!(waiter.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    for id in 0..2050 {
        drop(
            store
                .lock_meta(&ClientId(format!("expired-{id}").into()))
                .await,
        );
    }
    assert!(
        store.metadata_locks.lock().unwrap().len() <= 1024,
        "completed clients must not grow the lock registry forever"
    );
    drop(waiter);
    let mut next = Box::pin(store.lock_meta(&client));
    poll_fn(|cx| {
        assert!(
            next.as_mut().poll(cx).is_pending(),
            "pruning/cancellation must not create a second lock for a held client"
        );
        Poll::Ready(())
    })
    .await;
    drop(held);
    drop(
        tokio::time::timeout(std::time::Duration::from_secs(1), next)
            .await
            .unwrap(),
    );
}
