//! The per-session append lanes — ADR 0061's off-loop durable machinery
//! (issue #258 slice 3: moved verbatim from `hub/mod.rs`, no logic edits).
//!
//! **The invariant this module owns:** nothing the store must answer runs on the
//! hub loop. Every durable append (and the `QoS` 2 outbound records and packet-id
//! reservations that precede an online send) is FROZEN into an [`AppendJob`] at
//! plan time — on-loop, policy decided (`hub/policy.rs`), gate recorded — and
//! executed by a per-session lane worker that may see only the job, the store,
//! metrics, and the command sender, never `Hub` state. Per-session FIFO lanes
//! make append order structural; `LaneOutcome` cannot even express a refusal
//! (policy is on-loop); completions return as `AppendDone` and the on-loop
//! continuation (`AppendThen`) is the only writer of gates and dedup state.
//! Workers spawn via `Hub::spawn_owned` into the hub's owned `JoinSet` — a task
//! holding the store past node stop keeps redb's exclusive lock (ADR 0061 §8).

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258): the five
// siblings share one type/state vocabulary by design, and enumerating it would
// re-couple every future hub change to six import lists. Scoped to these files.
use super::*;

/// The outcome an append lane reports back to the loop (issue #242 / ADR 0061).
///
/// Deliberately has **no `Refused` variant**: a refusal is a POLICY decision, and policy
/// is decided only on-loop, at the plan/submit freeze point, before a job exists
/// (issue #238). A lane worker can only report what the store did — this type is the
/// #238 tripwire's structural successor: a refusal decided off-loop cannot even be
/// expressed.
#[derive(Debug, Clone, Copy)]
pub enum LaneOutcome {
    /// Recorded at this offset; the log is truncated through it once the subscriber
    /// acknowledges.
    Stored(Offset),
    /// The session queue cap under `reject-newest` rejected the newest message
    /// (ADR 0001 §6). Counted and logged, and the publisher IS still acknowledged: this
    /// cap is opt-in and its whole purpose is to shed rather than block, so the drop is
    /// the stated policy rather than a failure to honour one.
    Dropped,
    /// The write failed. The publisher's acknowledgement must be withheld so it retries
    /// (ADR 0041 T5).
    Failed,
    /// A [`LaneWork::Passthrough`] job: no store write was owed — the lane was used
    /// purely so this send cannot overtake an earlier append's post-durable live send.
    Passed,
}

/// The on-loop continuation an [`AppendDone`](HubCommand::AppendDone) runs — WHO is
/// owed what once the store answered. Frozen into the job at submit time.
#[derive(Debug, Clone)]
pub enum AppendThen {
    /// A gated publish's obligation: decrement its `appends_outstanding`, then
    /// complete (or, on `Failed`, withhold) via the pending-publish table.
    Gate(u64),
    /// A peer awaits a durability verdict for forward `seq` (0041-T12): resolve this
    /// job in the `(node, seq)` aggregate and answer when its last job lands.
    Peer(NodeId, u64),
    /// Nobody is owed an answer (a Will, an unanswerable forward, a `QoS` 0 order mark).
    Ungated,
}

/// The store work a lane job carries.
#[derive(Debug)]
pub enum LaneWork {
    /// The durable append itself: `enqueue_with_expiry` with the absolute deadline
    /// FROZEN at plan time from the hub clock (ADR 0009 §3 receipt-time semantics).
    Append {
        /// Absolute expiry deadline (Unix epoch seconds), if the publisher set one.
        expiry_at: Option<u64>,
    },
    /// No store work: a `QoS` 0 send routed through a busy lane purely for per-client
    /// wire order (it must not overtake an earlier append's post-durable live send).
    Passthrough,
    /// The SECOND lane stage (issue #242 finding A): ADR 0057's outbound-id write
    /// for a `QoS` 2 delivery with a durable offset. Staged at send time — the
    /// pending entry parks in [`OutState::AwaitingIdRecord`] — and the completion,
    /// holding the store's own outcome, is the only site that puts the packet on
    /// the wire.
    RecordOutbound {
        /// The packet id being made durable (pinned in the in-flight table).
        pkid: u16,
        /// The delivery's durable log offset.
        offset: Offset,
    },
    /// The detach-time backlog spill (issue #242 finding C): the persistent
    /// session's never-recorded (`offset == None`) flow-control backlog, enqueued
    /// off-loop and — because it rides the lane — strictly BEHIND every in-flight
    /// append, which is exactly the pre-motion order (the inline spill followed
    /// all previously admitted appends).
    Spill {
        /// `(message, absolute expiry deadline)` per spilled entry, frozen at
        /// detach time from the hub clock (ADR 0009 §3).
        entries: Vec<(Message, Option<u64>)>,
    },
}

/// One unit of off-loop durable-append work (issue #242 / ADR 0061).
///
/// **The off-loop contract:** a lane worker may read ONLY this job's own (owned,
/// immutable) fields, the store `Arc`, the metrics `Arc`, and the hub's command sender.
/// It must never see `Hub` state — no brownout flag, no routing table, no shared
/// cursor, no pending-publish table. Every decision is frozen in here at submit time,
/// on-loop, inside the same dispatch that planned the fan-out (issue #238); the worker
/// only executes. All fields are private so only the hub can construct one.
#[derive(Debug)]
pub struct AppendJob {
    /// The subscriber session the append belongs to (the lane key).
    pub(super) client: ClientId,
    /// The message, with `expires_at` already computed at plan time.
    pub(super) message: Message,
    /// What the worker does with the message.
    pub(super) work: LaneWork,
    /// The on-loop continuation.
    pub(super) then: AppendThen,
    /// The `conn_id` of the online connection this delivery was planned against;
    /// `None` when the target was planned offline. The completion handler live-sends
    /// only to this exact connection — or, after a reconnect, only when the attach
    /// replay provably did not cover the stored offset.
    pub(super) planned_conn: Option<u64>,
    /// The RETAIN flag for the wire send (Retain As Published, #198).
    pub(super) retain: bool,
    /// The remaining message-expiry interval to forward on the wire, if any.
    pub(super) message_expiry: Option<u32>,
}

/// What a lane worker receives: append/passthrough work, or a clean-start discard
/// serialized BEHIND every already-admitted append for the session, so a late append
/// can never re-create a queue the discard was supposed to empty (issue #242).
#[derive(Debug)]
pub enum LaneJob {
    /// Run the job's store work and post [`HubCommand::AppendDone`].
    Deliver(Box<AppendJob>),
    /// Run the clean-start durable discard (`store.remove`) and post
    /// `SessionRecovered::Cleaned`, exactly like the spawned path (ADR 0017) — plus a
    /// bookkeeping `AppendDone` so the lane's outstanding count drains.
    Discard(Box<PendingAttach>),
    /// Run the durable session discard (`store.remove`) for a zero-expiry detach or
    /// the expiry sweep, serialized BEHIND the session's admitted appends (issue
    /// #242 finding C) — a direct remove racing an in-flight append would let the
    /// append land after it and re-create the queue with a ghost message. Posts a
    /// bookkeeping `AppendDone` so the lane's outstanding count drains.
    Remove {
        /// The discarded session.
        client: ClientId,
    },
}

/// One session's append lane: a bounded FIFO queue to a dedicated worker task. Keyed
/// by SUBSCRIBER session (never by topic or placement group): all of one session's
/// durable keys live in one group, so per-session lanes give exact failure-domain
/// isolation AND make per-session append order structural (issue #242 / ADR 0061).
#[derive(Debug)]
pub(super) struct AppendLane {
    /// Bounded sender ([`LANE_QUEUE_CAP`]); `try_send` only — the loop never awaits it.
    pub(super) tx: mpsc::Sender<LaneJob>,
    /// Jobs admitted and not yet completed. Maintained purely on-loop (submit
    /// increments, `AppendDone` decrements), so reads need no synchronization.
    pub(super) outstanding: usize,
}

/// A peer verdict aggregate: one `RemotePublishAcked`/`RemoteSharedDeliverAcked`
/// fan-out's lane jobs, keyed `(origin, seq)`. The verdict is answered only when the
/// last job lands — a peer is never told `Stored` before the store actually stored
/// (issue #242).
#[derive(Debug)]
pub(super) struct RemoteAppendGate {
    /// Lane jobs still unresolved for this forward.
    pub(super) awaiting: usize,
    /// The composed verdict so far ([`DurableOutcome::and`] precedence).
    pub(super) worst: DurableOutcome,
}

/// Bound on jobs queued in one session's append lane (issue #242). At the cap the
/// NEWEST job is rejected at submit time — evicting an older job would break the
/// lane's FIFO order and orphan another publish's gate. An answerable rejection
/// withholds the publisher's ack (it retries: fail closed, ADR 0041 T5) and is
/// counted as `publish_dropped{reason="append-backlog-full"}`. Outbound-id record
/// jobs (issue #242 finding A) SHARE this cap with appends — a `QoS` 2-heavy
/// session spends up to half its headroom on records.
pub(super) const LANE_QUEUE_CAP: usize = 256;

/// WHO is told about this fan-out's outcome — threaded from each entry point down to
/// [`Hub::deliver_to_client`], where it becomes the submitted job's [`AppendThen`].
/// Replaces the old bare `answerable` flag: with appends completing asynchronously
/// (issue #242) the fan-out must carry its continuation, not just a bool.
#[derive(Debug, Clone)]
pub(super) enum AppendGate {
    /// A locally-gated publish: the pending-publish id whose ack awaits every append.
    Pending(u64),
    /// A peer awaiting a durability verdict for forward `seq` (0041-T12).
    Peer {
        /// The origin node.
        node: NodeId,
        /// Its forward sequence.
        seq: u64,
    },
    /// Nobody is told (a Will, a plain forward, a retained-window back-fill): a
    /// refused durable copy is a counted drop and the live delivery still happens
    /// (issue #238).
    None,
}

impl AppendGate {
    /// Whether SOMEBODY IS BEING TOLD about a refusal (the old `answerable` flag).
    pub(super) fn answerable(&self) -> bool {
        !matches!(self, Self::None)
    }

    /// The on-loop continuation a job submitted under this gate carries.
    pub(super) fn then(&self) -> AppendThen {
        match self {
            Self::Pending(id) => AppendThen::Gate(*id),
            Self::Peer { node, seq } => AppendThen::Peer(node.clone(), *seq),
            Self::None => AppendThen::Ungated,
        }
    }
}

/// What [`Hub::submit_append`] decided ON-loop, at the freeze point (issue #242).
/// The store's own outcome arrives later, as [`HubCommand::AppendDone`].
#[derive(Debug, Clone, Copy)]
pub(super) enum Submitted {
    /// Admitted into the session's lane; the obligation is recorded.
    Queued,
    /// Refused under a stated policy (brownout) — decided before any effect, so the
    /// refusal is effect-free and the publisher's retry idempotent (issue #238).
    Refused(PublishRefusal),
    /// The lane is at [`LANE_QUEUE_CAP`] (reject-newest): fail closed — the caller
    /// withholds the publisher's ack exactly as for a failed store write.
    Full,
}

/// Recover a persistent session off the hub command loop and post the result back as
/// [`HubCommand::SessionRecovered`] (ADR 0017). Run in a spawned task so the bounded
/// lease/quorum wait never blocks the single-threaded hub.
impl AppendJob {
    /// A CONTROL job (no delivery of its own): the detach spill, or the no-op mark
    /// below. `planned_conn: None` guarantees its completion never sends.
    pub(super) fn control(client: ClientId, work: LaneWork) -> Self {
        Self {
            client,
            message: Message {
                topic: String::new(),
                payload: Bytes::new(),
                qos: QoS::AtMostOnce,
                retain: false,
                app: AppProperties::default(),
                expires_at: None,
            },
            work,
            then: AppendThen::Ungated,
            planned_conn: None,
            retain: false,
            message_expiry: None,
        }
    }

    /// A no-op job a lane worker posts after a lane-serialized discard, purely to
    /// drain the lane's outstanding count on-loop (issue #242).
    pub(super) fn discard_mark(client: ClientId) -> Self {
        Self::control(client, LaneWork::Passthrough)
    }
}

/// One session's append-lane worker (issue #242 / ADR 0061): a pure FIFO executor.
/// Pop a job, run its store call, post the completion back to the loop. It holds ONLY
/// the store, the hub's command sender, and metrics — it can read no hub state by
/// construction, so every policy decision provably stayed on-loop (issue #238). It
/// retries nothing and decides nothing; and it always runs an accepted job to a real
/// store outcome, even if the hub is already gone (the completion send then no-ops and
/// the publisher's pending entry died withheld — fail closed).
pub(super) async fn append_lane_worker(
    store: Arc<dyn SessionStore>,
    self_tx: mpsc::UnboundedSender<HubCommand>,
    mut rx: mpsc::Receiver<LaneJob>,
    metrics: Option<Arc<mqtt_observability::metrics::Metrics>>,
    durable: bool,
) {
    while let Some(job) = rx.recv().await {
        match job {
            LaneJob::Deliver(job) => {
                let outcome = run_lane_job(&store, &job, metrics.as_ref(), durable).await;
                let _ = self_tx.send(HubCommand::AppendDone { job, outcome });
            }
            LaneJob::Discard(pending) => {
                // The clean-start durable discard (ADR 0017), serialized BEHIND every
                // admitted append for this session so a late append cannot re-create
                // the queue it just emptied (issue #242). Best-effort like the
                // spawned path; the in-memory wipe already happened on-loop.
                let client = pending.client.clone();
                let _ = store.remove(&client).await;
                let _ = self_tx.send(HubCommand::SessionRecovered {
                    pending: *pending,
                    recovery: SessionRecovery::Cleaned,
                });
                let _ = self_tx.send(HubCommand::AppendDone {
                    job: Box::new(AppendJob::discard_mark(client)),
                    outcome: LaneOutcome::Passed,
                });
            }
            LaneJob::Remove { client } => {
                // The zero-expiry-detach / expiry-sweep durable discard (issue
                // #242 finding C), serialized BEHIND every admitted append for
                // this session so a late append cannot re-create the queue it
                // just emptied. Best-effort like the spawned path; the in-memory
                // wipe already happened on-loop.
                let _ = store.remove(&client).await;
                let _ = self_tx.send(HubCommand::AppendDone {
                    job: Box::new(AppendJob::discard_mark(client)),
                    outcome: LaneOutcome::Passed,
                });
            }
        }
    }
}

/// The off-loop half of one durable append: exactly the store call, timing, and
/// failure classification (ADR 0020-T6) the loop used to run inline, moved verbatim
/// off it (issue #242). Reads ONLY the job and the store — no hub state, by
/// construction (see [`AppendJob`]).
pub(super) async fn run_lane_job(
    store: &Arc<dyn SessionStore>,
    job: &AppendJob,
    metrics: Option<&Arc<mqtt_observability::metrics::Metrics>>,
    durable: bool,
) -> LaneOutcome {
    let expiry_at = match &job.work {
        LaneWork::Passthrough => return LaneOutcome::Passed,
        // The second lane stage (issue #242 finding A): ADR 0057's outbound-id
        // write, exactly the store call the loop used to await inline at send
        // time. The completion handler owns the wire send and the failure arm.
        LaneWork::RecordOutbound { pkid, offset } => {
            return match store.record_outbound(&job.client, *pkid, *offset).await {
                Ok(()) => LaneOutcome::Stored(*offset),
                Err(e) => {
                    warn!(client = %job.client.0, pkid, error = %e,
                          "outbound QoS2 id write failed in the session lane");
                    LaneOutcome::Failed
                }
            };
        }
        // The detach spill (issue #242 finding C): the loop's old inline loop,
        // moved here verbatim — same per-entry best-effort posture.
        LaneWork::Spill { entries } => {
            for (message, expiry_at) in entries {
                if let Err(e) = store
                    .enqueue_with_expiry(&job.client, message, *expiry_at)
                    .await
                {
                    warn!(client = %job.client.0, error = %e,
                          "failed to spill backlog to store");
                }
            }
            return LaneOutcome::Passed;
        }
        LaneWork::Append { expiry_at } => *expiry_at,
    };
    // Durable (quorum) append: time it and classify any failure (ADR 0020-T6).
    // The latency histogram is only meaningful when the store is the replicated
    // one, so gate it on durable mode; a failure reason is recorded either way.
    let started = Instant::now();
    let result = store
        .enqueue_with_expiry(&job.client, &job.message, expiry_at)
        .await;
    if durable {
        if let Some(m) = metrics {
            m.observe_durable_append_latency(started.elapsed().as_secs_f64());
        }
    }
    match result {
        Ok(Enqueued::Stored { offset, evicted }) => {
            if evicted > 0 {
                warn!(client = %job.client.0, evicted, topic = %job.message.topic,
                      "session queue full: evicted oldest message(s)");
                if let Some(m) = metrics {
                    m.publish_dropped("queue-overflow");
                }
            }
            LaneOutcome::Stored(offset)
        }
        Ok(Enqueued::Rejected) => {
            warn!(client = %job.client.0, topic = %job.message.topic,
                  "session queue full: dropped message (reject-newest)");
            if let Some(m) = metrics {
                m.publish_dropped("queue-overflow");
            }
            LaneOutcome::Dropped
        }
        Err(e) => {
            if let Some(m) = metrics {
                m.durable_append_failed(durable_failure_reason(&e));
            }
            warn!(client = %job.client.0, error = %e,
                  "failed to enqueue message; withholding the publisher's ack (ADR 0041 T5)");
            // Fail closed like the local ack path (ADR 0018): the completion withholds
            // the publisher's acknowledgement so it retries, instead of acking a
            // message a subscriber will never see.
            LaneOutcome::Failed
        }
    }
}

impl Hub {
    /// Submit `message` for append to its subscriber's durable session lane — the
    /// write the publisher's `QoS` ≥ 1 acknowledgement is gated on (ADR 0001), now run
    /// OFF the command loop (issue #242 / ADR 0061) so a degraded placement group
    /// stalls only its own sessions' appends, never every client on the node.
    ///
    /// **The #238 freeze point.** Everything decision-shaped happens HERE, on-loop,
    /// inside the same dispatch that ran the plan pass — and a dispatch's internal
    /// awaits never interleave another command, so the plan pass's `self.brownout`
    /// read and this submission observe one consistent state: the brownout arm below,
    /// the absolute expiry deadline (receipt time plus interval, ADR 0009 §3 — frozen
    /// from `self.clock`, already inside `message.expires_at`), the target, and the
    /// continuation. The lane worker executes only frozen data and can report only
    /// what the store did ([`LaneOutcome`] has no `Refused` variant, by construction).
    /// A `SetBrownout` that lands after this dispatch therefore affects the NEXT
    /// publish, never this one's admitted jobs — every interleaving linearizes to
    /// "publish committed first, then the flag flipped", exactly as it did when the
    /// append was awaited inline.
    pub(super) fn submit_append(
        &mut self,
        client: &ClientId,
        message: &Message,
        message_expiry: Option<u32>,
        gate: &AppendGate,
        planned_conn: Option<u64>,
    ) -> Submitted {
        let answerable = gate.answerable();
        // Brownout (ADR 0041 T5 disk / T8 memory): an enqueue GROWS the store, so it is
        // refused above the watermark. The publisher is REFUSED with it (0041-T11, issue
        // #238) rather than acked for a message that exists nowhere — "acked means
        // durable" is the product claim, and QoS 0 / clean session is the spec-native way
        // to ask for fire-and-forget.
        if self.brownout {
            // Debug, not warn: the brownout entry/exit EDGES already log at warn/info in
            // `set_brownout_axis`, and a sustained brownout must not flood the very log
            // the operator is diagnosing it from.
            debug!(client = %client.0, topic = %message.topic,
                   "brownout: durable enqueue refused (ADR 0041)");
            // An ANSWERABLE fan-out never gets here: `plan_refusal` decided the refusal
            // before this fan-out took any side effect, which is what makes a refusal
            // effect-free and the publisher's retry idempotent (issue #238). Reaching
            // here means a caller with NOBODY TO TELL — `publish_will`,
            // `deliver_to_windowed_subscribers`, `redeliver_pending`'s ungated paths — so
            // the lost durable copy is a genuine drop, counted as one. The assert is the
            // tripwire for a future refusal axis, or an await slipped between the plan
            // pass and this submission (the decide/commit freeze span, issue #242),
            // that breaks the plan/commit invariant.
            debug_assert!(
                !answerable || message.qos == QoS::AtMostOnce,
                "an answerable fan-out that OWED a durable append reached \
                 submit_append's brownout arm: the plan/commit invariant behind an \
                 effect-free refusal is broken (#238)"
            );
            if let Some(m) = &self.metrics {
                m.publish_dropped("brownout");
            }
            return Submitted::Refused(PublishRefusal::Brownout);
        }
        let job = AppendJob {
            client: client.clone(),
            message: message.clone(),
            work: LaneWork::Append {
                // Frozen at plan time (ADR 0009 §3): the same receipt-time deadline
                // `message.expires_at` carries, never re-read off-loop.
                expiry_at: message
                    .expires_at
                    .or_else(|| message_expiry.map(|s| self.clock.now_epoch_secs() + u64::from(s))),
            },
            then: gate.then(),
            planned_conn,
            retain: message.retain,
            message_expiry,
        };
        self.submit_lane_job(job)
    }

    /// Route a no-append send through `client`'s busy lane so it cannot overtake an
    /// earlier append's post-durable live send (issue #242). Never refused — no store
    /// growth is involved — but subject to the same lane bound.
    pub(super) fn submit_passthrough(
        &mut self,
        client: &ClientId,
        message: &Message,
        message_expiry: Option<u32>,
        planned_conn: Option<u64>,
    ) -> Submitted {
        let job = AppendJob {
            client: client.clone(),
            message: message.clone(),
            work: LaneWork::Passthrough,
            then: AppendThen::Ungated,
            planned_conn,
            retain: message.retain,
            message_expiry,
        };
        self.submit_lane_job(job)
    }

    /// This session's append lane, spawning it (bounded channel + worker) on first use.
    pub(super) fn lane_for(&mut self, client: &ClientId) -> &mut AppendLane {
        if !self.append_lanes.contains_key(client) {
            // The channel holds LANE_CONTROL_HEADROOM slots beyond the delivery cap
            // (enforced on `outstanding` below) so a discard/spill control job is
            // admitted even at the cap — see LANE_CONTROL_HEADROOM.
            let (tx, rx) = mpsc::channel(LANE_QUEUE_CAP + LANE_CONTROL_HEADROOM);
            // Spawned INTO the hub's own JoinSet, never bare: the worker holds the
            // store's exclusive handle, so its lifetime must not outlive the hub's
            // (see `owned_tasks`).
            self.owned_tasks.spawn(append_lane_worker(
                self.store.clone(),
                self.self_tx.clone(),
                rx,
                self.metrics.clone(),
                self.durable_plane.is_some(),
            ));
            self.append_lanes
                .insert(client.clone(), AppendLane { tx, outstanding: 0 });
        }
        self.append_lanes
            .get_mut(client)
            .expect("just inserted above")
    }

    /// Spawn a task that holds an `Arc` of the session store, OWNED by the hub.
    ///
    /// Never `tokio::spawn` such a task bare. The store's redb handle is an exclusive
    /// lock, so a task holding it past the node's stop keeps the data dir locked and the
    /// next start fails with "Database already open. Cannot acquire lock." — and at a
    /// full-cluster stop every store call blocks for the replication bound, so "it
    /// finishes quickly" is exactly the assumption that does not hold. Spawning into
    /// `owned_tasks` makes the abort that stops the node cascade here too (see
    /// [`Hub::owned_tasks`]).
    pub(super) fn spawn_owned<F>(&mut self, fut: F)
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        self.owned_tasks.spawn(fut);
    }

    /// Admit one job into its session's lane (spawning the lane on first use), record
    /// the gate obligation, and return. `try_send` only: the loop NEVER awaits lane
    /// capacity — at the cap the NEWEST job is rejected (reject-newest keeps the
    /// lane's FIFO order intact) and the caller fails closed.
    pub(super) fn submit_lane_job(&mut self, job: AppendJob) -> Submitted {
        let client = job.client.clone();
        // A rejected append/passthrough is LOST or withheld — counted as a drop. A
        // rejected record job is only DEFERRED (its message is already durable at
        // its offset; the caller requeues it at the backlog front), so counting it
        // as dropped would be a lie the operator alerts on.
        let deferred_only = matches!(job.work, LaneWork::RecordOutbound { .. });
        let then = job.then.clone();
        let admitted = {
            let lane = self.lane_for(&client);
            // Delivery jobs are admitted only while channel occupancy is under
            // LANE_QUEUE_CAP — the remaining LANE_CONTROL_HEADROOM slots belong to
            // control jobs (discard/spill), which bypass this check.
            lane.tx.capacity() > LANE_CONTROL_HEADROOM
                && lane.tx.try_send(LaneJob::Deliver(Box::new(job))).is_ok()
        };
        if !admitted {
            warn!(
                client = %client.0, cap = LANE_QUEUE_CAP,
                "append lane full: rejecting the newest job (ack withheld / drop / \
                 deferral) — this session's placement group is not keeping up \
                 (issue #242)"
            );
            if !deferred_only {
                if let Some(m) = &self.metrics {
                    m.publish_dropped("append-backlog-full");
                }
            }
            return Submitted::Full;
        }
        self.lane_for(&client).outstanding += 1;
        // Record the obligation SYNCHRONOUSLY, inside the same dispatch that
        // created the gate, so no interleaved command can observe the gate
        // with obligations not yet registered (ACK-AFTER-DURABLE, #124).
        match then {
            AppendThen::Gate(id) => {
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.appends_outstanding += 1;
                }
            }
            AppendThen::Peer(node, seq) => {
                self.remote_append_pending
                    .entry((node, seq))
                    .or_insert(RemoteAppendGate {
                        awaiting: 0,
                        worst: DurableOutcome::Ok,
                    })
                    .awaiting += 1;
            }
            AppendThen::Ungated => {}
        }
        Submitted::Queued
    }

    /// Handle one lane job's completion (issue #242 / ADR 0061): the on-loop second
    /// half of a durable append. Runs on the single-threaded loop like every dispatch,
    /// so all pending-publish, in-flight, and lane mutation stays race-free —
    /// ADR 0017's argument, applied to appends.
    pub(super) async fn append_done(&mut self, job: AppendJob, outcome: LaneOutcome) {
        if let Some(lane) = self.append_lanes.get_mut(&job.client) {
            lane.outstanding = lane.outstanding.saturating_sub(1);
        }
        // The second lane stage — ADR 0057's outbound-id record (issue #242
        // finding A) — has its own completion contract: the post-record wire send.
        if let LaneWork::RecordOutbound { pkid, offset } = job.work {
            return self.outbound_record_done(job, pkid, offset, outcome);
        }
        let offset = match outcome {
            LaneOutcome::Stored(o) => {
                // The one place a durable copy comes into existence — so the one place
                // that can honestly say a publish IS stored somewhere (issue #238).
                self.durable_writes += 1;
                Some(o)
            }
            LaneOutcome::Dropped | LaneOutcome::Failed | LaneOutcome::Passed => None,
        };
        // The post-durable live send (ACK-AFTER-DURABLE's wire half, #124): the packet
        // structurally cannot reach the conn channel before the store answered —
        // this is the only send site for a lane-routed delivery, and it holds the
        // store's own offset. Sent to the exact connection the delivery was planned
        // against; after a reconnect, only when the attach replay provably did not
        // cover the stored offset (every replayed entry raises the session's
        // high-water, and offsets are monotone) — otherwise the durable copy is (or
        // was) the replay's to deliver, and sending again would duplicate. While a
        // connect is mid-recovery (`connecting`), nothing is sent: the old connection
        // is being replaced and the new one's replay owns delivery.
        let mut send = !self.connecting.contains_key(&job.client)
            && match outcome {
                LaneOutcome::Failed => false,
                // No durable copy exists (queue-cap drop) or none was owed
                // (passthrough): the live send is delivery itself, valid only for
                // the exact planned connection.
                LaneOutcome::Dropped | LaneOutcome::Passed => self
                    .online
                    .get(&job.client)
                    .is_some_and(|o| Some(o.conn_id) == job.planned_conn),
                LaneOutcome::Stored(o) => match self.online.get(&job.client) {
                    None => false, // offline: the durable copy replays on reconnect
                    Some(online) if Some(online.conn_id) == job.planned_conn => true,
                    Some(_) => {
                        // (Re)attached mid-flight: deliver only what the replay
                        // could not have seen, into a still-persistent session.
                        self.is_persistent(&job.client)
                            && self
                                .inflight
                                .get(&job.client)
                                .is_none_or(|i| o > i.high_water)
                    }
                },
            };
        if send && self.park_ordering_gated_qos0(&job).await {
            send = false;
        }
        if send {
            if let Some(tx) = self.online.get(&job.client).map(|s| s.tx.clone()) {
                self.send_to_client(
                    &job.client,
                    &tx,
                    &job.message,
                    job.retain,
                    job.message_expiry,
                    offset,
                )
                .await;
                if let Some(m) = &self.metrics {
                    m.publish_delivered(qos_num(job.message.qos));
                }
            }
        }
        match job.then {
            AppendThen::Gate(id) => {
                if let Some(p) = self.pending_publishes.get_mut(&id) {
                    p.appends_outstanding = p.appends_outstanding.saturating_sub(1);
                    if offset.is_some() {
                        // Event-driven successor of the old durable-writes snapshot
                        // trick: a refusal for this publish may now only WITHHOLD
                        // (issue #238).
                        p.stored = true;
                    }
                }
                match outcome {
                    // Fail closed like the inline path did (ADR 0041 T5): withhold.
                    LaneOutcome::Failed => self.drop_pending(id),
                    _ => self.try_complete_pending(id),
                }
            }
            AppendThen::Peer(node, seq) => {
                let out = match outcome {
                    LaneOutcome::Failed => DurableOutcome::Failed,
                    _ => DurableOutcome::Ok,
                };
                if let Some(g) = self.remote_append_pending.get_mut(&(node.clone(), seq)) {
                    g.awaiting = g.awaiting.saturating_sub(1);
                    g.worst = g.worst.and(out);
                    if g.awaiting == 0 {
                        if let Some(g) = self.remote_append_pending.remove(&(node.clone(), seq)) {
                            self.answer_forward(&node, seq, g.worst.to_verdict());
                        }
                    }
                }
            }
            AppendThen::Ungated => {}
        }
    }

    /// A `QoS` 0 passthrough completion must not slip past a delivery that is
    /// ADMITTED but momentarily parked off the wire — an outbound-id record staged
    /// after the passthrough was queued (issue #242 finding A), a packet-id block
    /// reservation in flight, or anything already waiting in the backlog. Returns
    /// `true` after parking such a completion in the backlog — the one per-client
    /// ordering buffer — so the gate's completion drains it in exact FIFO order
    /// (delaying a `QoS` 0 is always legal; overtaking is what nothing permits).
    /// `QoS` > 0 passthroughs need no re-check: their send runs through
    /// `send_to_client`, whose backlog gate already diverts them.
    pub(super) async fn park_ordering_gated_qos0(&mut self, job: &AppendJob) -> bool {
        let gated = matches!(job.work, LaneWork::Passthrough)
            && job.message.qos == QoS::AtMostOnce
            && self.inflight.get(&job.client).is_some_and(|i| {
                i.records_pending > 0 || i.reserve_outstanding || !i.backlog.is_empty()
            });
        if !gated {
            return false;
        }
        let limits = self.subscriber_limits;
        let inf = self.inflight.entry(job.client.clone()).or_default();
        let evicted = inf.push_backlog(
            BacklogEntry::new(job.message.clone(), job.retain, job.message_expiry, None),
            &limits,
        );
        if !evicted.is_empty() {
            let bytes = inf.backlog.bytes();
            for (e, _) in &evicted {
                if let Some(offset) = e.offset {
                    inf.release(offset);
                }
                if let Some(m) = &self.metrics {
                    m.publish_dropped("backlog-overflow");
                }
            }
            warn_backlog_eviction(&job.client, &evicted, bytes, &limits);
            self.truncate_acked(&job.client).await;
        }
        true
    }

    /// Completion of one staged outbound-id record (issue #242 finding A): the ONLY
    /// send site for a `QoS` 2 delivery with a durable offset — it holds the record
    /// write's own outcome, so the packet structurally cannot reach the wire before
    /// the store answered. ADR 0057's ordering, relocated, not weakened.
    pub(super) fn outbound_record_done(
        &mut self,
        job: AppendJob,
        pkid: u16,
        offset: Offset,
        outcome: LaneOutcome,
    ) {
        if let Some(inf) = self.inflight.get_mut(&job.client) {
            inf.records_pending = inf.records_pending.saturating_sub(1);
        }
        // The exact-conn fence (the same rule the append completion applies): a
        // reconnect never re-sends — the staged entry was dropped at
        // detach/takeover and the durable copy is the replay's to deliver; while a
        // connect is mid-recovery, the new connection's replay owns delivery.
        let fence = !self.connecting.contains_key(&job.client)
            && self
                .online
                .get(&job.client)
                .is_some_and(|o| Some(o.conn_id) == job.planned_conn);
        match outcome {
            LaneOutcome::Stored(_) if fence => {
                // Recorded durably: the handshake may now start under this id.
                if let Some(p) = self
                    .inflight
                    .get_mut(&job.client)
                    .and_then(|inf| inf.pending.get_mut(&pkid))
                {
                    p.state = OutState::AwaitingPubRec;
                }
                if let Some(tx) = self.online.get(&job.client).map(|o| o.tx.clone()) {
                    let _ = tx.send(publish_packet(
                        &job.message.topic,
                        job.message.payload.clone(),
                        job.message.qos,
                        Some(pkid),
                        false,
                        job.retain,
                        job.message_expiry,
                        &job.message.app,
                    ));
                    if let Some(m) = &self.metrics {
                        m.publish_delivered(qos_num(job.message.qos));
                    }
                }
                // The gate is open again: release anything it diverted, in order.
                self.drain_backlog(&job.client);
            }
            LaneOutcome::Stored(_) => {
                // Fence failed (reconnect / mid-connect): nothing is sent — the
                // durable copy replays. Clean up a leftover staged entry,
                // state-checked so a replay's rebuilt entry under the same id is
                // never touched; the offset stays owed, holding truncation.
                self.drop_staged_entry(&job.client, pkid);
                self.drain_backlog(&job.client);
            }
            LaneOutcome::Failed | LaneOutcome::Dropped | LaneOutcome::Passed => {
                // The record write failed — ADR 0057's failure arm, relocated
                // verbatim: the PUBLISH is withheld, not sent under an id that
                // would not survive. The message is already durable at `offset`,
                // so nothing is lost — back to the FRONT of the backlog (ordering
                // holds); the next drain retries. Deliberately NO drain here:
                // retrying in this same completion would spin against a store
                // that just refused.
                self.drop_staged_entry(&job.client, pkid);
                warn!(client = %job.client.0, pkid,
                      "outbound QoS2 id could not be made durable; delivery deferred");
                if let Some(m) = &self.metrics {
                    m.publish_dropped("outbound-id-write-failed");
                }
                if fence {
                    self.inflight
                        .entry(job.client.clone())
                        .or_default()
                        .backlog
                        .push_front_admitted(BacklogEntry::new(
                            job.message,
                            job.retain,
                            job.message_expiry,
                            Some(offset),
                        ));
                }
                // Fence failed: the entry is simply dropped — the offset stays
                // owed and the reattach replay owns delivery, the same rule the
                // detach spill applies to offset-carrying backlog entries.
            }
        }
    }

    /// Remove `pkid`'s pending entry only while it is still `AwaitingIdRecord` — a
    /// stale completion must never disturb an entry the attach replay rebuilt under
    /// the same id (ADR 0057's restored table re-inserts original ids).
    pub(super) fn drop_staged_entry(&mut self, client: &ClientId, pkid: u16) {
        if let Some(inf) = self.inflight.get_mut(client) {
            if inf
                .pending
                .get(&pkid)
                .is_some_and(|p| p.state == OutState::AwaitingIdRecord)
            {
                inf.pending.remove(&pkid);
            }
        }
    }

    /// The off-loop packet-id block reservation answered (ADR 0007 T9 / issue #242
    /// finding A): bank the base — `0` (no durable session) and an error both bank
    /// a bare refill, today's in-memory fallback verbatim — and drain the
    /// deliveries that deferred on it.
    pub(super) fn pkid_block_reserved(
        &mut self,
        client: &ClientId,
        result: Result<u16, mqtt_storage::StorageError>,
    ) {
        let inf = self.inflight.entry(client.clone()).or_default();
        inf.reserve_outstanding = false;
        inf.banked_base = Some(match result {
            // base = persisted high-water before the reservation; resume from it.
            Ok(base) => base,
            Err(e) => {
                debug!(client = %client.0, error = %e,
                       "packet-id reservation failed; in-memory fallback");
                0
            }
        });
        self.drain_backlog(client);
    }

    /// The packet-id block is spent and nothing is banked (issue #242 finding A):
    /// park `entry` at the backlog FRONT (ordering holds — the delivery waits in
    /// the backlog, not the loop) and run the durable block reservation in a
    /// spawned single-flight task owning only `(client, store, self_tx)` — the
    /// off-loop contract. [`PkidBlockReserved`](HubCommand::PkidBlockReserved)
    /// banks the result and drains.
    pub(super) fn defer_for_pkid_block(&mut self, client: &ClientId, entry: BacklogEntry) {
        let inf = self.inflight.entry(client.clone()).or_default();
        inf.backlog.push_front_admitted(entry);
        if inf.reserve_outstanding {
            return;
        }
        inf.reserve_outstanding = true;
        let store = self.store.clone();
        let self_tx = self.self_tx.clone();
        let client = client.clone();
        self.spawn_owned(async move {
            let result = store.reserve_packet_ids(&client, PKID_BLOCK).await;
            let _ = self_tx.send(HubCommand::PkidBlockReserved { client, result });
        });
    }
}
