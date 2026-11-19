//! The resume replay, a page at a time (issue #1235).
//!
//! A reconnecting persistent session used to get one read of its queue, at most 10,000
//! entries. The rest waited for the next reconnect while live traffic went past it, and
//! once the subscriber had acknowledged that traffic the watermark covered the unread
//! entries and the truncation flusher deleted them undelivered: `append_done` sends a
//! live append whose offset is above the session's high-water, and `safe_ack` returns
//! the high-water when nothing is owed.
//!
//! Now the attach reads the first page inline, as before, and while the queue is longer
//! than a page it keeps a [`ReplayCursor`]. The next page is read on the session's lane
//! once the subscriber has room: its backlog has drained and its outbound queue is not
//! deep. The lane runs the read as a barrier job, so the read sees every append admitted
//! before it, and its result reaches the loop before the completion of any append
//! admitted after it. While a cursor is active:
//!
//! - a live delivery to the session is written, not sent, and the cursor reaches it in the
//!   queue's order: `append_done` holds the send, write elision is off, and `QoS` 0 is
//!   written too;
//! - truncation never passes the last offset the replay has read
//!   ([`Inflight::replay_ceiling`](super::Inflight)), whatever raises the high-water.
//!
//! A short page ends the replay. Everything appended before that page's read was in it,
//! and everything appended after reaches the loop afterwards, when the session is live
//! again.

use std::collections::{BTreeMap, HashSet};

use mqtt_core::{parse_shared, topic_matches, ClientId};
use mqtt_storage::{Offset, QueuedMessage, StorageError};
use tracing::{debug, warn};

use super::lanes::LaneJob;
use super::{publish_packet, Hub, OutState, Outbound, Packet, PendingOut, MAX_OUTBOUND_QUEUE};

/// Queued messages read per page. It bounds one read, not what a session keeps: the
/// whole queue replays, a page at a time, as the subscriber makes room.
pub(super) const REPLAY_PAGE: usize = 1_000;

/// What each page's entries need from the attach that started the replay.
#[derive(Debug, Default)]
pub(super) struct ReplayState {
    /// Durable `QoS` 2 identities (ADR 0057) not yet married to their message, by offset.
    pub(super) restored: BTreeMap<Offset, (u16, bool)>,
    /// Offsets the attach resumed from the in-memory in-flight table with DUP.
    pub(super) resumed_offsets: HashSet<Offset>,
    /// Grants the current policy revoked at attach (ADR 0040 T3).
    pub(super) revoked_grants: Vec<String>,
}

/// A replay that has not reached the end of the session's queue yet.
#[derive(Debug)]
pub(super) struct ReplayCursor {
    /// The connection the replay delivers to; a newer connection starts its own.
    pub(super) conn_id: u64,
    /// The last offset read.
    pub(super) after: Offset,
    /// A page read is on the session's lane.
    pub(super) reading: bool,
    /// Carried from page to page.
    pub(super) state: ReplayState,
}

impl Hub {
    /// Replay one queued entry to `client`'s connection: skip what is already on the
    /// wire or settled, resume a durable `QoS` 2 handshake under its original id, drop
    /// what a revoked grant alone admits or what has expired, send the rest.
    pub(super) fn replay_entry(
        &mut self,
        client: &ClientId,
        outbound: &Outbound,
        qm: &QueuedMessage,
        now: u64,
        state: &mut ReplayState,
    ) {
        // Raise the truncation ceiling for every entry read, so the ones dropped below
        // are let go of even though nothing will deliver them (#124). Entries that ARE
        // sent additionally become owed, in `send_to_client`, and hold the ceiling down
        // until acknowledged.
        self.inflight
            .entry(client.clone())
            .or_default()
            .note_offset(qm.offset);
        // Still in the in-flight table means this is a client reconnect, not a broker
        // restart: the DUP resume at attach already re-sent it under its original packet
        // id — and for `QoS` 2 possibly as a bare PUBREL, because the client has
        // acknowledged receipt and only owes the release. Replaying the PUBLISH here
        // would deliver it twice. A write-elision fallback linked to a resumed entry
        // since the attach is added to the set too (`link_elided_offset`). Merely being
        // owed is not enough: an entry parked in the old connection's backlog is owed
        // and was never sent.
        if state.resumed_offsets.contains(&qm.offset) {
            debug!(client = %client.0, offset = qm.offset,
                   "queued message is still in flight in memory; not replaying");
            state.restored.remove(&qm.offset);
            return;
        }
        // Acknowledged on an earlier connection, kept in the queue only because an older
        // entry still owed blocks truncation (issue #1056): settled, not owed, and a copy
        // here would be a new message to the client.
        if self
            .inflight
            .get(client)
            .is_some_and(|inf| inf.settled_above_watermark(qm.offset))
        {
            debug!(client = %client.0, offset = qm.offset,
                   "queued message was acknowledged before this connection and awaits \
                    truncation; not replaying (#1056)");
            return;
        }
        // A broker restart with a durably-recorded id (ADR 0057): resume the handshake
        // mid-phrase UNDER THE ORIGINAL ID, never through a fresh allocation. Before
        // PUBREC: the PUBLISH goes out again with DUP and the id the subscriber may
        // already hold. After PUBREC: a bare PUBREL — the subscriber has the message;
        // re-publishing it is the #130 duplicate this table exists to prevent.
        if let Some((pkid, pubrec_seen)) = state.restored.remove(&qm.offset) {
            let inf = self.inflight.entry(client.clone()).or_default();
            inf.orphaned_qos2.remove(&pkid);
            inf.orphaned_qos2_cleanup.remove(&pkid);
            let out_state = if pubrec_seen {
                OutState::AwaitingPubComp
            } else {
                OutState::AwaitingPubRec
            };
            // Restored handshakes still owe their queued delivery. Without this, the
            // replay's prefix truncate can delete an earlier unacked restored message
            // while completing a later one (#577).
            inf.track(qm.offset);
            inf.pending.insert(
                pkid,
                PendingOut {
                    message: qm.message.clone(),
                    state: out_state,
                    offset: Some(qm.offset),
                },
            );
            let packet = if pubrec_seen {
                Packet::PubRel(pkid.into())
            } else {
                publish_packet(
                    &qm.message.topic,
                    qm.message.payload.clone(),
                    qm.message.qos,
                    Some(pkid),
                    true,
                    false,
                    None,
                    &qm.message.app,
                    &self.matching_sub_ids(client, &qm.message.topic),
                )
            };
            let _ = outbound.send_from(packet, self.delivery_origin(&qm.message.app));
            return;
        }
        // A queued message that only a revoked grant admits is dropped (ADR 0040 T3):
        // delivering it would leak data the new policy denies. A topic a surviving grant
        // also matches still replays.
        if !state.revoked_grants.is_empty() {
            let topic = &qm.message.topic;
            let admits = |f: &str| {
                let f = parse_shared(f).map_or(f, |(_, inner)| inner);
                topic_matches(f, topic)
            };
            let survives = self
                .subs
                .get(client)
                .is_some_and(|subs| subs.iter().any(|e| admits(&e.filter)));
            if state.revoked_grants.iter().map(String::as_str).any(admits) && !survives {
                debug!(client = %client.0, offset = qm.offset, %topic,
                       "dropping queued message for a revoked grant (ADR 0040 T3)");
                return;
            }
        }
        match qm.expiry_at {
            Some(deadline) if deadline <= now => {
                debug!(client = %client.0, offset = qm.offset, "dropping expired queued message");
                // EMQX's `expired`. Its interval has run out: none is left.
                self.note_delivery_dropped(client, "expired", &qm.message, false, Some(0));
            }
            Some(deadline) => {
                let remaining = u32::try_from(deadline - now).unwrap_or(u32::MAX);
                self.send_to_client(
                    client,
                    outbound,
                    &qm.message,
                    false,
                    Some(remaining),
                    Some(qm.offset),
                );
            }
            None => {
                self.send_to_client(client, outbound, &qm.message, false, None, Some(qm.offset));
            }
        }
    }

    /// The replay has read the whole queue: the durable `QoS` 2 ids it never married to
    /// a message are only POSSIBLE orphans. Released phase resumes PUBREL; PUBCOMP makes
    /// cleanup eligible. Unreleased phase can attempt cleanup immediately. Both paths
    /// must prove durable prefix retirement, not infer absence from the replay.
    pub(super) fn finish_replay(
        &mut self,
        client: &ClientId,
        outbound: &Outbound,
        restored: BTreeMap<Offset, (u16, bool)>,
    ) {
        for (offset, (pkid, pubrec_seen)) in restored {
            self.inflight
                .entry(client.clone())
                .or_default()
                .orphaned_qos2
                .insert(pkid, offset);
            if pubrec_seen {
                debug!(client = %client.0, pkid, offset,
                       "orphaned outbound QoS2 id in released phase; sending PUBREL");
                let _ = outbound.send(Packet::PubRel(pkid.into()));
            } else {
                // Absence from the replay is not proof of absence from storage. Check
                // this exact offset before retiring its ID. The store waits run on the
                // session lane (#575) so attach of an unrelated session is not parked
                // behind this proof.
                self.queue_unreleased_qos2_orphan(client, pkid, offset);
            }
        }
        self.submit_qos2_retire(client);
    }

    /// Read the replay's next page on `client`'s lane, if a replay is waiting for one
    /// and the subscriber has room: nothing in its backlog or staged behind a record,
    /// and its outbound queue under half of what the hub sheds `QoS` 0 at. Called after
    /// each page, whenever the backlog drains, and from the sweep, which catches a
    /// replay that waited on the outbound queue or on a full lane.
    pub(super) fn read_next_replay_page(&mut self, client: &ClientId) {
        let Some(cursor) = self.replays.get(client) else {
            return;
        };
        if cursor.reading {
            return;
        }
        let (conn_id, after) = (cursor.conn_id, cursor.after);
        let Some(online) = self.online.get(client) else {
            return;
        };
        if online.conn_id != conn_id || online.tx.is_closed() {
            return;
        }
        if online.tx.depth() >= MAX_OUTBOUND_QUEUE / 2
            || self
                .inflight
                .get(client)
                .is_some_and(|inf| !inf.backlog.is_empty() || inf.records_pending > 0)
        {
            return;
        }
        let admitted = self
            .lane_for(client)
            .tx
            .try_send(LaneJob::ReplayPage {
                client: client.clone(),
                conn_id,
                after,
                limit: REPLAY_PAGE,
            })
            .is_ok();
        if !admitted {
            debug!(client = %client.0, "session lane full; the sweep retries the replay page");
            return;
        }
        if let Some(lane) = self.append_lanes.get_mut(client) {
            lane.outstanding += 1;
        }
        if let Some(cursor) = self.replays.get_mut(client) {
            cursor.reading = true;
        }
    }

    /// The sweep's turn: read the next page for every replay that is waiting for room.
    pub(super) fn pump_replays(&mut self) {
        if self.replays.is_empty() {
            return;
        }
        let waiting: Vec<ClientId> = self
            .replays
            .iter()
            .filter(|(_, c)| !c.reading)
            .map(|(client, _)| client.clone())
            .collect();
        for client in waiting {
            self.read_next_replay_page(&client);
        }
    }

    /// A replay page read on `client`'s lane came back.
    pub(super) async fn replay_page_done(
        &mut self,
        client: &ClientId,
        conn_id: u64,
        result: Result<Vec<QueuedMessage>, StorageError>,
    ) {
        if let Some(lane) = self.append_lanes.get_mut(client) {
            lane.outstanding = lane.outstanding.saturating_sub(1);
        }
        // A page read for a connection that has since been replaced or gone: the newer
        // connection's attach starts its own replay from the beginning.
        let current = self.online.get(client).map(|o| o.conn_id) == Some(conn_id)
            && self
                .replays
                .get(client)
                .is_some_and(|c| c.conn_id == conn_id);
        if !current {
            return;
        }
        let entries = match result {
            Ok(entries) => entries,
            Err(error) => {
                // As for the attach's first page: carrying on would leave the client
                // attached with its queue undelivered until it happened to reconnect.
                // Close it instead; its reconnect replays the queue from the start.
                warn!(client = %client.0, %error,
                      "a page of queued messages could not be read for replay; closing \
                       the connection so the client reconnects and receives them");
                self.replays.remove(client);
                self.close_for_retry(client, conn_id).await;
                return;
            }
        };
        let Some(outbound) = self.online.get(client).map(|o| o.tx.clone()) else {
            return;
        };
        let Some(mut cursor) = self.replays.remove(client) else {
            return;
        };
        cursor.reading = false;
        let full = entries.len() >= REPLAY_PAGE;
        let now = self.clock.now_epoch_secs();
        for qm in entries {
            cursor.after = qm.offset;
            self.replay_entry(client, &outbound, &qm, now, &mut cursor.state);
            // A send can find the connection gone; its own detach then ends the replay.
            if self.online.get(client).map(|o| o.conn_id) != Some(conn_id) {
                return;
            }
        }
        if full {
            if let Some(inf) = self.inflight.get_mut(client) {
                inf.replay_ceiling = Some(cursor.after);
            }
            self.replays.insert(client.clone(), cursor);
            self.truncate_acked(client);
            self.read_next_replay_page(client);
            return;
        }
        debug!(client = %client.0, up_to = cursor.after, "replay reached the end of the queue");
        if let Some(inf) = self.inflight.get_mut(client) {
            inf.replay_ceiling = None;
        }
        self.truncate_acked(client);
        self.finish_replay(client, &outbound, cursor.state.restored);
    }
}
