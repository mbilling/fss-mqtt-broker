//! Durable `QoS` 2 retirement (#577) and outbound store-wait isolation (#575).
//!
//! A prefix submission is not a per-message deletion certificate. IDs remain
//! reserved until both queue and ID writes have completed. Those writes stay
//! **ordered** (durable prefix, then ID clearance) and are never fire-and-forget
//! (#533). On the actor path they run on the session's append lane so a stalled
//! `QoS` 2 store does not stall unrelated hub dispatches (#575 / #405 isolation
//! slice). Direct `Hub` method calls — the #577 fixtures, which have no running
//! actor — still await the same sequence inline.
#[allow(clippy::wildcard_imports)]
use super::*;

/// Off-loop `QoS` 2 store work, frozen on-loop. The lane worker sees only this
/// and the store — never `Hub` state (the #242 / #238 contract).
#[derive(Debug)]
pub enum Qos2LaneOp {
    /// PUBREC: `advance_outbound` before PUBREL goes out (ADR 0057).
    Advance {
        pkid: u16,
        /// Bumped on session discard so a late completion cannot move a new session.
        epoch: u64,
    },
    /// PUBCOMP / sweep retirement: durable prefix then ID clearance (#577).
    Retire(Qos2RetirePlan),
}

/// The retirement work one lane job will attempt. Candidates and the truncate
/// watermark are frozen at submit time; a later PUBCOMP submits a follow-up.
#[derive(Debug)]
pub struct Qos2RetirePlan {
    client: ClientId,
    epoch: u64,
    /// Truncate through this watermark when any candidate still has a log offset.
    up_to: Option<Offset>,
    candidates: Vec<(u16, Option<Offset>)>,
    orphans: Vec<(u16, Offset)>,
}

impl Qos2RetirePlan {
    fn is_empty(&self) -> bool {
        self.candidates.is_empty() && self.orphans.is_empty()
    }
}

/// What the lane worker reports back for one [`Qos2LaneOp`].
#[derive(Debug)]
pub enum Qos2OpOutcome {
    Advance { pkid: u16, epoch: u64, ok: bool },
    Retire(Qos2RetireResult),
}

#[derive(Debug)]
pub struct Qos2RetireResult {
    client: ClientId,
    epoch: u64,
    durable: Option<Offset>,
    cleared: Vec<u16>,
    orphans_cleared: Vec<u16>,
}

/// Execute one frozen `QoS` 2 store op. Store only — the on-loop apply owns
/// inflight mutation, PUBREL, and packet-ID retirement.
pub(crate) async fn exec_qos2_lane_op(
    store: &Arc<dyn SessionStore>,
    client: &ClientId,
    op: Qos2LaneOp,
) -> Qos2OpOutcome {
    match op {
        Qos2LaneOp::Advance { pkid, epoch } => {
            let ok = match store.advance_outbound(client, pkid).await {
                Ok(()) => true,
                Err(error) => {
                    warn!(client = %client.0, pkid, %error,
                          "outbound QoS2 phase advance failed; PUBREL withheld");
                    false
                }
            };
            Qos2OpOutcome::Advance { pkid, epoch, ok }
        }
        Qos2LaneOp::Retire(plan) => Qos2OpOutcome::Retire(exec_qos2_retire(store, plan).await),
    }
}

async fn exec_qos2_retire(store: &Arc<dyn SessionStore>, plan: Qos2RetirePlan) -> Qos2RetireResult {
    let durable = if let Some(up_to) = plan.up_to {
        match store.ack_durable(&plan.client, up_to).await {
            Ok(()) => Some(up_to),
            Err(error) => {
                debug!(client = %plan.client.0, up_to, %error,
                       "failed to truncate the acknowledged session log; QoS2 IDs remain reserved");
                None
            }
        }
    } else {
        None
    };
    let mut cleared = Vec::new();
    for (pkid, offset) in plan.candidates {
        if let Some(offset) = offset {
            if durable.is_none_or(|through| offset > through) {
                continue;
            }
            if let Err(error) = store.clear_outbound(&plan.client, pkid).await {
                warn!(client = %plan.client.0, pkid, %error,
                    "QoS2 ID clearance failed; keeping the ID reserved for retry");
                continue;
            }
        }
        cleared.push(pkid);
    }
    let mut orphans_cleared = Vec::new();
    for (pkid, offset) in plan.orphans {
        if exec_orphan_clear(store, &plan.client, pkid, offset).await {
            orphans_cleared.push(pkid);
        }
    }
    Qos2RetireResult {
        client: plan.client,
        epoch: plan.epoch,
        durable,
        cleared,
        orphans_cleared,
    }
}

/// The store half of [`Hub::clear_orphaned_qos2`]: prove the prefix absent,
/// make that proof durable, then clear the ID.
async fn exec_orphan_clear(
    store: &Arc<dyn SessionStore>,
    client: &ClientId,
    pkid: u16,
    offset: Offset,
) -> bool {
    match store.pending(client, 0, 1).await {
        Ok(entries) if entries.first().is_none_or(|entry| entry.offset > offset) => {}
        Ok(_) => return false,
        Err(error) => {
            warn!(client = %client.0, pkid, %error, "cannot verify QoS2 orphan retirement");
            return false;
        }
    }
    if let Err(error) = store.ack_durable(client, offset).await {
        warn!(client = %client.0, pkid, %error, "orphaned QoS2 retirement is not durable");
        return false;
    }
    if let Err(error) = store.clear_outbound(client, pkid).await {
        warn!(client = %client.0, pkid, %error, "orphaned QoS2 ID clearance failed");
        return false;
    }
    true
}

impl Hub {
    /// Retry only sessions carrying completed-but-not-retired deliveries. The
    /// subscriber owes nothing after PUBCOMP, so recovery cannot depend on another
    /// client packet. The existing sweep is the bounded retry clock.
    ///
    /// Inline: used by the #577 fixtures that drive `Hub` without an actor. The
    /// running loop uses [`Self::submit_pending_qos2_cleanup`].
    #[cfg(test)]
    pub(super) async fn retry_qos2_cleanup(&mut self) {
        let clients: Vec<_> = self.qos2_cleanup.iter().cloned().collect();
        for client in clients {
            self.retire_completed_qos2(&client).await;
            self.drain_backlog(&client);
        }
    }

    #[cfg(test)]
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
        let orphans: Vec<_> = self.inflight.get(client).map_or_else(Vec::new, |inf| {
            inf.orphaned_qos2_cleanup
                .iter()
                .filter_map(|pkid| inf.orphaned_qos2.get(pkid).map(|offset| (*pkid, *offset)))
                .collect()
        });
        for (pkid, offset) in orphans {
            self.clear_orphaned_qos2(client, pkid, offset).await;
        }
        if !self.inflight.get(client).is_some_and(|inf| {
            !inf.orphaned_qos2_cleanup.is_empty()
                || inf
                    .pending
                    .values()
                    .any(|p| p.state == OutState::CompletedQos2)
        }) {
            self.qos2_cleanup.remove(client);
        }
    }

    /// A durable ID outside the replay window may be a real orphan, or may point
    /// to an entry beyond that window. Only an authoritative read proving the
    /// whole prefix absent permits retirement. A failed read/write keeps it reserved.
    #[cfg(test)]
    pub(super) async fn clear_orphaned_qos2(
        &mut self,
        client: &ClientId,
        pkid: u16,
        offset: Offset,
    ) {
        if let Some(inf) = self.inflight.get_mut(client) {
            inf.orphaned_qos2_cleanup.insert(pkid);
            self.qos2_cleanup.insert(client.clone());
        }
        if exec_orphan_clear(&self.store, client, pkid, offset).await {
            if let Some(inf) = self.inflight.get_mut(client) {
                inf.orphaned_qos2.remove(&pkid);
                inf.orphaned_qos2_cleanup.remove(&pkid);
            }
        }
    }

    /// Actor-path PUBACK: `QoS` 1 truncate stays detached; outstanding `QoS` 2
    /// retirement is submitted to the session lane rather than awaited.
    pub(super) fn dispatch_pub_ack(&mut self, client: &ClientId, pkid: u16) {
        let completed = self.complete_pending(client, pkid, OutState::AwaitingPubAck);
        if completed {
            self.truncate_acked(client);
            if self.qos2_cleanup.contains(client) {
                self.submit_qos2_retire(client);
            }
            self.drain_backlog(client);
        }
    }

    /// Actor-path PUBREC: the durable phase write runs on the session lane;
    /// PUBREL is sent only when that write succeeds (ADR 0057).
    pub(super) fn dispatch_pub_rec(&mut self, client: &ClientId, pkid: u16) {
        let durable = self
            .inflight
            .get(client)
            .and_then(|inf| inf.pending.get(&pkid))
            .is_some_and(|p| p.state == OutState::AwaitingPubRec && p.offset.is_some());
        if durable {
            let key = (client.clone(), pkid);
            if self.qos2_advance_inflight.contains(&key) {
                return;
            }
            let epoch = self.qos2_epoch(client);
            if self.submit_qos2_op(client, Qos2LaneOp::Advance { pkid, epoch }) {
                self.qos2_advance_inflight.insert(key);
            }
            return;
        }
        self.send_pubrel_after_advance(client, pkid);
    }

    /// Actor-path PUBCOMP: mark completed and submit ordered retirement. A
    /// PUBCOMP that arrives while its PUBREC advance is still in the lane is
    /// held — the in-memory state is still `AwaitingPubRec` until that write
    /// lands, and ignoring it would drop a legal completion that the inline
    /// path used to see in the next dispatch.
    pub(super) fn dispatch_pub_comp(&mut self, client: &ClientId, pkid: u16) {
        let key = (client.clone(), pkid);
        let action = self
            .inflight
            .get(client)
            .and_then(|inf| inf.pending.get(&pkid))
            .map(|p| p.state);
        match action {
            Some(OutState::AwaitingPubComp | OutState::CompletedQos2) => {
                self.mark_qos2_completed(client, pkid);
                self.submit_qos2_retire(client);
            }
            Some(OutState::AwaitingPubRec) if self.qos2_advance_inflight.contains(&key) => {
                self.qos2_held_pubcomp.insert(key);
            }
            Some(_) => {}
            None => {
                if self
                    .inflight
                    .get(client)
                    .is_some_and(|inf| inf.orphaned_qos2.contains_key(&pkid))
                {
                    if let Some(inf) = self.inflight.get_mut(client) {
                        inf.orphaned_qos2_cleanup.insert(pkid);
                    }
                    self.qos2_cleanup.insert(client.clone());
                    self.submit_qos2_retire(client);
                }
            }
        }
    }

    fn mark_qos2_completed(&mut self, client: &ClientId, pkid: u16) {
        if let Some(inf) = self.inflight.get_mut(client) {
            if let Some(pending) = inf.pending.get_mut(&pkid) {
                pending.state = OutState::CompletedQos2;
                if let Some(offset) = pending.offset {
                    inf.release(offset);
                }
            }
        }
        self.qos2_cleanup.insert(client.clone());
    }

    /// Sweep-tick isolation: submit outstanding retirement instead of awaiting
    /// it on the hub loop.
    pub(super) fn submit_pending_qos2_cleanup(&mut self) {
        let clients: Vec<_> = self.qos2_cleanup.iter().cloned().collect();
        for client in clients {
            self.submit_qos2_retire(&client);
            self.drain_backlog(&client);
        }
    }

    /// Mark unreleased restore orphans for lane retirement (attach-time isolation).
    pub(super) fn queue_unreleased_qos2_orphan(
        &mut self,
        client: &ClientId,
        pkid: u16,
        offset: Offset,
    ) {
        let inf = self.inflight.entry(client.clone()).or_default();
        inf.orphaned_qos2.insert(pkid, offset);
        inf.orphaned_qos2_cleanup.insert(pkid);
        self.qos2_cleanup.insert(client.clone());
    }

    pub(super) fn qos2_op_done(&mut self, client: &ClientId, outcome: Qos2OpOutcome) {
        if let Some(lane) = self.append_lanes.get_mut(client) {
            lane.outstanding = lane.outstanding.saturating_sub(1);
        }
        match outcome {
            Qos2OpOutcome::Advance { pkid, epoch, ok } => {
                self.qos2_advance_done(client, pkid, epoch, ok);
            }
            Qos2OpOutcome::Retire(result) => self.apply_qos2_retire(result),
        }
    }

    fn qos2_advance_done(&mut self, client: &ClientId, pkid: u16, epoch: u64, ok: bool) {
        let key = (client.clone(), pkid);
        self.qos2_advance_inflight.remove(&key);
        if epoch != self.qos2_epoch(client) {
            self.qos2_held_pubcomp.remove(&key);
            return;
        }
        if !ok {
            self.qos2_held_pubcomp.remove(&key);
            return;
        }
        self.send_pubrel_after_advance(client, pkid);
        if self.qos2_held_pubcomp.remove(&key) {
            self.dispatch_pub_comp(client, pkid);
        }
    }

    fn apply_qos2_retire(&mut self, result: Qos2RetireResult) {
        let client = result.client;
        self.qos2_retire_inflight.remove(&client);
        if result.epoch != self.qos2_epoch(&client) {
            return;
        }
        if let Some(through) = result.durable {
            if let Some(inf) = self.inflight.get_mut(&client) {
                inf.acked_through = inf.acked_through.max(through);
            }
        }
        for pkid in result.cleared {
            if let Some(inf) = self.inflight.get_mut(&client) {
                if inf
                    .pending
                    .get(&pkid)
                    .is_some_and(|p| p.state == OutState::CompletedQos2)
                {
                    inf.pending.remove(&pkid);
                }
            }
        }
        for pkid in result.orphans_cleared {
            if let Some(inf) = self.inflight.get_mut(&client) {
                inf.orphaned_qos2.remove(&pkid);
                inf.orphaned_qos2_cleanup.remove(&pkid);
            }
        }
        let still = self.inflight.get(&client).is_some_and(|inf| {
            !inf.orphaned_qos2_cleanup.is_empty()
                || inf
                    .pending
                    .values()
                    .any(|p| p.state == OutState::CompletedQos2)
        });
        if still {
            self.qos2_cleanup.insert(client.clone());
            self.submit_qos2_retire(&client);
        } else {
            self.qos2_cleanup.remove(&client);
        }
        self.drain_backlog(&client);
    }

    pub(super) fn send_pubrel_after_advance(&mut self, client: &ClientId, pkid: u16) {
        let advanced =
            self.inflight
                .get_mut(client)
                .is_some_and(|inf| match inf.pending.get_mut(&pkid) {
                    Some(p) if p.state == OutState::AwaitingPubRec => {
                        p.state = OutState::AwaitingPubComp;
                        true
                    }
                    _ => false,
                });
        if advanced {
            if let Some(sess) = self.online.get(client) {
                let _ = sess.tx.send(Packet::PubRel(pkid.into()));
            }
        }
    }

    fn plan_qos2_retire(&self, client: &ClientId) -> Qos2RetirePlan {
        let epoch = self.qos2_epoch(client);
        let Some(inf) = self.inflight.get(client) else {
            return Qos2RetirePlan {
                client: client.clone(),
                epoch,
                up_to: None,
                candidates: Vec::new(),
                orphans: Vec::new(),
            };
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
        let up_to = candidates
            .iter()
            .any(|(_, offset)| offset.is_some())
            .then_some(safe)
            .filter(|&up_to| up_to > 0);
        let orphans: Vec<_> = inf
            .orphaned_qos2_cleanup
            .iter()
            .filter_map(|pkid| inf.orphaned_qos2.get(pkid).map(|offset| (*pkid, *offset)))
            .collect();
        Qos2RetirePlan {
            client: client.clone(),
            epoch,
            up_to,
            candidates,
            orphans,
        }
    }

    pub(super) fn submit_qos2_retire(&mut self, client: &ClientId) {
        if self.qos2_retire_inflight.contains(client) {
            return;
        }
        let plan = self.plan_qos2_retire(client);
        if plan.is_empty() {
            if !self.inflight.get(client).is_some_and(|inf| {
                !inf.orphaned_qos2_cleanup.is_empty()
                    || inf
                        .pending
                        .values()
                        .any(|p| p.state == OutState::CompletedQos2)
            }) {
                self.qos2_cleanup.remove(client);
            }
            return;
        }
        if self.submit_qos2_op(client, Qos2LaneOp::Retire(plan)) {
            self.qos2_retire_inflight.insert(client.clone());
        }
    }

    fn submit_qos2_op(&mut self, client: &ClientId, op: Qos2LaneOp) -> bool {
        let admitted = {
            let lane = self.lane_for(client);
            lane.tx
                .try_send(LaneJob::Qos2Op {
                    client: client.clone(),
                    op,
                })
                .is_ok()
        };
        if !admitted {
            warn!(
                client = %client.0,
                "QoS 2 lane job rejected; the sweep retries retirement and the \
                 subscriber retries PUBREC (issue #575)"
            );
            return false;
        }
        if let Some(lane) = self.append_lanes.get_mut(client) {
            lane.outstanding += 1;
        }
        true
    }

    pub(super) fn qos2_epoch(&self, client: &ClientId) -> u64 {
        self.qos2_epoch.get(client).copied().unwrap_or(0)
    }

    pub(super) fn bump_qos2_epoch(&mut self, client: &ClientId) {
        *self.qos2_epoch.entry(client.clone()).or_insert(0) += 1;
    }

    pub(super) fn forget_qos2_isolation(&mut self, client: &ClientId) {
        self.qos2_cleanup.remove(client);
        self.qos2_retire_inflight.remove(client);
        self.qos2_advance_inflight.retain(|(c, _)| c != client);
        self.qos2_held_pubcomp.retain(|(c, _)| c != client);
        self.bump_qos2_epoch(client);
    }
}
