//! Quarantine of a durable session that stays unreadable (issue #1049).
//!
//! **The defect this module closes.** While the last session scan was incomplete,
//! a clustered node held the PUBACK of every gated `QoS` 1 publish that matched no
//! session it knew: the view might be missing a session it owns, and "nobody is
//! owed this" is then not a safe conclusion. A scan is incomplete when ONE owned
//! key cannot be read. So a single session whose group would not recover (the
//! field case: `NoQuorum` for over 90 minutes, across restarts) held every such
//! ack on its node, for as long as that one read kept failing.
//!
//! **The rule.** No timer releases a held ack. Instead the hub remembers, per
//! client, the scans that could not read it in a row. A client that is unread in
//! every scan for at least [`QUARANTINE_AFTER_TICKS`] sweep ticks, and in at least
//! [`QUARANTINE_AFTER_SCANS`] scans, is QUARANTINED: named at WARN with its error,
//! counted on `mqttd_sessions_quarantined`, and listed on `/statusz`
//! (`session_quarantine`). A scan then counts as complete when everything it could
//! not read is quarantined, the store named every unread session (the list was not
//! cut at [`SessionScan::UNREAD_KEPT`]), and the enumeration itself did not fail.
//! A quarantined client that reads again (or is gone: a full scan that does not
//! name it) is released from quarantine on that scan, and the scan lands the usual
//! way, materialising its subscriptions.
//!
//! **Why these numbers.** In the field a transient `scan-incomplete` after churn
//! cleared within about 20 s, and a node retries a scan on every one of its first
//! eight ticks (the boot window) and every [`EXPIRY_RECONCILE_EVERY`] ticks while
//! routing is unsettled. Two minutes is six times that transient, and longer than
//! any takeover window, so a group that is merely slow to recover is never
//! quarantined; the scan floor keeps a single slow scan from deciding it. And it
//! is far shorter than the defect: the field node held its acks for over 13
//! minutes before anyone looked.
//!
//! **The residual, stated precisely.** Quarantine stops one session from holding
//! the acks of publishes it is not owed. It does not make that session deliverable:
//!
//! - A gated publish that matches a subscription of the quarantined session this
//!   node DOES know (materialised by an earlier scan or a live attach before the
//!   key went unreadable) still routes to it; its append fails (`NoQuorum`) and the
//!   publisher's ack is WITHHELD, as before. Nothing changes there.
//! - A gated publish whose only match is a subscription of the quarantined session
//!   this node has NEVER read (the field case: the key was unreadable from boot)
//!   matches nobody here, and once the session is quarantined it is acked
//!   `Accepted` and not stored for it. That is the cost. It is bounded by what the
//!   session could take anyway: its queue refuses every append with the same
//!   `NoQuorum`, so the message could not have been stored for it either, and
//!   this node already acks a publish that reached any live subscriber without
//!   knowing what the unreadable session subscribes to. Before the fix the same
//!   publish was held unacked indefinitely, which a publisher that gives up treats
//!   no better.
//! - The same holds cluster-wide once this node's interest gossip turns
//!   authoritative on a complete-with-quarantine scan: peers forget filters only
//!   the quarantined session had. The interest backstop
//!   ([`INTEREST_AUTHORITATIVE_BACKSTOP_TICKS`]) already forced that after ten
//!   ticks of incomplete scans, so this is not new.
//! - Nothing is re-delivered when the quarantine clears: what was acked during it
//!   was never stored for the session. A publish still HELD at that moment (for
//!   another term of `routing_unsettled()`) is re-delivered by the ordinary settle
//!   pass to the subscriptions the clearing scan materialises.
//! - More unreadable sessions than [`SessionScan::UNREAD_KEPT`] are never
//!   quarantined into a complete scan: the store does not name them all, so the
//!   hub cannot know every one is stuck, and the node keeps holding (loudly).
//!
//! [`SessionScan::UNREAD_KEPT`]: mqtt_storage::SessionScan::UNREAD_KEPT

#[allow(clippy::wildcard_imports)] // an intra-hub module split (#258), as in the siblings.
use super::*;

/// The fewest scans a client must be unread in, in a row, before it is quarantined.
pub(super) const QUARANTINE_AFTER_SCANS: u32 = 3;

/// The fewest sweep ticks (one tick is `SESSION_SWEEP_INTERVAL`, 1 s) between the
/// first and the latest scan that could not read a client before it is quarantined:
/// four reconcile cadences, two minutes. See the module docs for why.
pub(super) const QUARANTINE_AFTER_TICKS: u32 = 4 * EXPIRY_RECONCILE_EVERY;

/// How many quarantined sessions `/statusz` lists (the count is always whole).
pub const QUARANTINE_LISTED: usize = 64;

/// One client's run of scans that could not read it.
#[derive(Debug, Clone, Copy)]
pub(super) struct UnreadStreak {
    /// Scans in a row that named it unread.
    pub(super) scans: u32,
    /// The sweep tick of the first of them.
    pub(super) since: u32,
}

impl Hub {
    /// Account one landed scan's unread sessions against the quarantine (issue
    /// #1049), and answer whether the scan counts as complete: the store read
    /// everything, or every session it could not read is quarantined and it named
    /// all of them, and the enumeration itself did not fail.
    ///
    /// `enumerated` is false when the store could not even list its keys: that
    /// scan says nothing about any one client, so it neither extends nor breaks a
    /// streak, and it is never complete.
    pub(super) fn account_unread(
        &mut self,
        complete: bool,
        enumerated: bool,
        unread: &[(ClientId, String)],
        unread_total: usize,
    ) -> bool {
        if !enumerated {
            return false;
        }
        let listed_all = unread.len() >= unread_total;
        let named: HashSet<&ClientId> = unread.iter().map(|(c, _)| c).collect();
        if listed_all {
            // A full list: whoever it does not name was read (or is gone).
            self.unread_streaks.retain(|c, _| named.contains(c));
            let lifted: Vec<ClientId> = self
                .quarantined_sessions
                .keys()
                .filter(|c| !named.contains(c))
                .cloned()
                .collect();
            for client in lifted {
                self.quarantined_sessions.remove(&client);
                warn!(
                    client = %client.0,
                    "quarantined session is readable again (or gone); quarantine lifted, \
                     and the scan counts it as usual (issue #1049)"
                );
            }
        }
        // A cut list says nothing about the clients it did not name: their streaks
        // and quarantine stand until a full list decides.
        let now = self.expiry_reconcile_tick;
        for (client, error) in unread {
            let streak = self
                .unread_streaks
                .entry(client.clone())
                .or_insert(UnreadStreak {
                    scans: 0,
                    since: now,
                });
            streak.scans = streak.scans.saturating_add(1);
            let (scans, unread_ticks) = (streak.scans, now.wrapping_sub(streak.since));
            if let Some(kept) = self.quarantined_sessions.get_mut(client) {
                kept.clone_from(error);
            } else if scans >= QUARANTINE_AFTER_SCANS && unread_ticks >= QUARANTINE_AFTER_TICKS {
                self.quarantined_sessions
                    .insert(client.clone(), error.clone());
                warn!(
                    client = %client.0,
                    error = %error,
                    scans,
                    unread_secs = unread_ticks,
                    "durable session QUARANTINED: this node owns it and has not been able \
                     to read it for {unread_ticks} s; session scans stop waiting for it, so \
                     QoS 1 publishes no known session matches are acked again. Publishes \
                     to subscriptions of it this node never read are acked and NOT stored \
                     for it until it reads again; its own queue refuses appends meanwhile \
                     (issue #1049)"
                );
            }
        }
        self.publish_quarantine();
        // An incomplete scan that names nobody gives quarantine nothing to stand in
        // for: it stays incomplete.
        complete
            || (unread_total > 0
                && listed_all
                && unread
                    .iter()
                    .all(|(c, _)| self.quarantined_sessions.contains_key(c)))
    }

    /// Mirror the quarantined set to `/statusz` (its gauge is set on the metrics
    /// refresh, with the `routing_unsettled` terms).
    fn publish_quarantine(&self) {
        if let Some(status) = &self.quarantine_status {
            status.set(
                self.quarantined_sessions.len(),
                self.quarantined_sessions
                    .iter()
                    .take(QUARANTINE_LISTED)
                    .map(|(c, e)| (c.0.to_string(), e.clone()))
                    .collect(),
            );
        }
    }

    /// Attach the shared quarantine list the `/statusz` body reads (issue #1049).
    pub fn attach_session_quarantine(&mut self, status: Arc<crate::health::SessionQuarantine>) {
        self.quarantine_status = Some(status);
    }
}
