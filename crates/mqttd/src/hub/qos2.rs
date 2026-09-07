//! Durable `QoS` 2 retirement (#577). A prefix submission is not a per-message
//! deletion certificate. IDs remain reserved until both queue and ID writes have
//! completed; this deliberately does not detach or pipeline the `QoS` 2 path.
#[allow(clippy::wildcard_imports)]
use super::*;

impl Hub {
    /// Retry only sessions carrying completed-but-not-retired deliveries. The
    /// subscriber owes nothing after PUBCOMP, so recovery cannot depend on another
    /// client packet. The existing sweep is the bounded retry clock.
    pub(super) async fn retry_qos2_cleanup(&mut self) {
        let clients: Vec<_> = self.qos2_cleanup.iter().cloned().collect();
        for client in clients {
            self.retire_completed_qos2(&client).await;
            self.drain_backlog(&client);
        }
    }

    pub(super) async fn retire_completed_qos2(&mut self, client: &ClientId) {
        let Some(inf) = self.inflight.get(client) else {
            self.qos2_cleanup.remove(client);
            return;
        };
        let safe = inf.safe_ack();
        let candidates: Vec<_> = inf
            .pending
            .iter()
            .filter_map(|(&pkid, p)| {
                (p.state == OutState::CompletedQos2 && p.offset.is_none_or(|o| o <= safe))
                    .then_some((pkid, p.offset))
            })
            .collect();
        // Never infer durability from acked_through: it also records detached QoS 1
        // submissions. Even an equal prefix must be retried after a failed write.
        let durable = if candidates.iter().any(|(_, offset)| offset.is_some()) {
            self.truncate_acked_now(client).await
        } else {
            None
        };
        for (pkid, offset) in candidates {
            if let Some(offset) = offset {
                if durable.is_none_or(|through| offset > through) {
                    continue;
                }
                if let Err(error) = self.store.clear_outbound(client, pkid).await {
                    warn!(client = %client.0, pkid, %error,
                        "QoS2 ID clearance failed; keeping the ID reserved for retry");
                    continue;
                }
            }
            if let Some(inf) = self.inflight.get_mut(client) {
                inf.pending.remove(&pkid);
            }
        }
        if !self.inflight.get(client).is_some_and(|inf| {
            inf.pending
                .values()
                .any(|p| p.state == OutState::CompletedQos2)
        }) {
            self.qos2_cleanup.remove(client);
        }
    }

    /// A durable ID outside the replay window may be a real orphan, or may point
    /// to an entry beyond that window. Only an authoritative read proving the
    /// whole prefix absent permits retirement. A failed read/write keeps it reserved.
    pub(super) async fn clear_orphaned_qos2(
        &mut self,
        client: &ClientId,
        pkid: u16,
        offset: Offset,
    ) {
        match self.store.pending(client, 0, 1).await {
            Ok(entries) if entries.first().is_none_or(|entry| entry.offset > offset) => {}
            Ok(_) => return,
            Err(error) => {
                warn!(client = %client.0, pkid, %error, "cannot verify QoS2 orphan retirement");
                return;
            }
        }
        // A local read can reflect a previous LAZY QoS 1 truncate whose followers
        // still retain the entry. Establish a durable prefix before retiring its ID.
        if let Err(error) = self.store.ack_durable(client, offset).await {
            warn!(client = %client.0, pkid, %error, "orphaned QoS2 retirement is not durable");
            return;
        }
        if let Err(error) = self.store.clear_outbound(client, pkid).await {
            warn!(client = %client.0, pkid, %error, "orphaned QoS2 ID clearance failed");
            return;
        }
        if let Some(inf) = self.inflight.get_mut(client) {
            inf.orphaned_qos2.remove(&pkid);
        }
    }
}
