//! Relay streams and the certificates they were admitted under (ADR 0005, issue #1016).
//!
//! A persistent session relocated to its owning node runs on a relay stream: the landing
//! node dials the owner's peer listener under its own cluster certificate, sends a
//! `ProxyHello`, and splices the client's bytes through. Both ends of that connection
//! passed `admit_peer_link`, and a TLS session keeps the certificates of its handshake, so
//! the stream is held to them as a peer link is: it ends the second after the earlier of
//! the two certificates' ends (ADR 0088 D3), and when the cluster CRL revokes the remote
//! one (ADR 0040 T4). Each node holds its own side: the owner the relocated session, the
//! landing node the splice.
//!
//! Ending a relay stream disconnects its client, which reconnects and is relayed again on
//! the certificates the two nodes hold then. A relay stream cannot move under a live
//! client as a peer link does, so a renewal costs each relayed client one reconnect. What
//! [`Relays`] does about that is spread the reconnects instead of ending every stream in
//! the old certificate's last second, with the timing of the peer links' moves
//! ([`MovePlan`]):
//!
//! - **after this node's own renewal**, a stream whose end is held by this node's
//!   certificate (strictly earlier than the peer's) is ended at a uniformly random moment
//!   within half the time left before the end window's margin, at most five minutes;
//! - **in the end window**, a stream made at least [`RELAY_WINDOW_MIN_LIFE_MS`] before its
//!   end is ended at a uniformly random moment of the window (a quarter of its life, at
//!   most five minutes, closing a minute before the end). This is what moves a stream
//!   whose two certificates end together (nodes minted together), which neither side's
//!   renewal alone makes any later, and one whose peer renewed out of this node's sight. A
//!   stream made later than that — the reconnect of one ended in the window, when nobody
//!   renewed — is held to the hard end only, so a client is not ended over and over as the
//!   end approaches.
//!
//! The hard end stays strict whatever was planned.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};

use mqtt_core::ClientId;
use tokio::sync::oneshot;

use super::LinkCert;
use crate::peer::{MovePlan, MOVE_SPREAD_MAX_MS};

/// The shortest life, at registration, for which a relay stream is given an early end in
/// its end window: four times the window's five minutes, so the window is a full one and a
/// stream remade inside it (which then has less life than this) is never planned again.
pub(crate) const RELAY_WINDOW_MIN_LIFE_MS: i64 = 4 * MOVE_SPREAD_MAX_MS;

/// What holds a relay stream's other end on this node, and so what ending it does.
#[derive(Debug)]
pub(crate) enum RelayHolder {
    /// The owner's side: the relocated session served on the stream, by its connection
    /// id ([`RelayKey`] is that id). Ended through the hub's own close of the session.
    Session(ClientId),
    /// The landing node's side: the splice task, which ends when this fires (or is
    /// dropped).
    Splice(oneshot::Sender<()>),
}

/// Why a relay stream ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RelayEnd {
    /// A certificate of its handshake has passed `notAfter`: the bounded reason label
    /// (`relay-peer-cert-expired` / `relay-own-cert-expired`).
    Expired(&'static str),
    /// Planned ahead of the end, so its client reconnects on renewed certificates and
    /// not in the same second as every other relayed client.
    Moved,
}

/// A relay stream's id: the owner side's session connection id, or the id the landing
/// node drew for its splice from the same counter.
pub(crate) type RelayKey = u64;

#[derive(Debug)]
struct Relay {
    cert: LinkCert,
    made_ms: i64,
    /// When it is to be ended, Unix milliseconds: the hard end, or a planned one before it.
    due_ms: i64,
    /// This node's own certificate end an after-renewal end was last planned for.
    own_seen: Option<i64>,
    holder: RelayHolder,
}

impl Relay {
    fn plan(&self) -> Option<MovePlan> {
        Some(MovePlan {
            end_ms: self.cert.ends.earliest()?.saturating_mul(1000),
            made_ms: self.made_ms,
        })
    }
}

/// The relay streams this node holds an end of, with when each is to be ended.
#[derive(Debug, Default)]
pub(crate) struct Relays {
    streams: HashMap<RelayKey, Relay>,
    /// `(due_ms, key)`, earliest first. An entry whose stream has gone, or whose due has
    /// since moved, is dropped when it surfaces.
    due: BinaryHeap<Reverse<(i64, RelayKey)>>,
}

/// The hard end of a stream whose earliest certificate end is `end` (Unix seconds): the
/// first second past it, in milliseconds.
fn hard_end_ms(end: i64) -> i64 {
    end.saturating_add(1).saturating_mul(1000)
}

impl Relays {
    /// How many relay streams are held.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.streams.len()
    }

    /// Hold a relay stream `key` admitted under `cert`, made at `now_ms`; `jitter` in
    /// `[0, 1)` places a planned end in its end window. A stream with nothing to hold it
    /// to (a plaintext mesh) is not held. Returns when it is due, if held.
    pub(crate) fn open(
        &mut self,
        key: RelayKey,
        cert: LinkCert,
        holder: RelayHolder,
        now_ms: i64,
        jitter: f64,
    ) -> Option<i64> {
        if cert.is_empty() {
            return None;
        }
        let mut relay = Relay {
            own_seen: cert.ends.own,
            cert,
            made_ms: now_ms,
            due_ms: i64::MAX,
            holder,
        };
        if let Some(plan) = relay.plan() {
            let hard = hard_end_ms(relay.cert.ends.earliest().unwrap_or(i64::MAX));
            relay.due_ms = if plan.end_ms.saturating_sub(now_ms) >= RELAY_WINDOW_MIN_LIFE_MS {
                plan.in_end_window(jitter).min(hard)
            } else {
                hard
            };
        }
        let due = relay.due_ms;
        if due != i64::MAX {
            self.due.push(Reverse((due, key)));
        }
        self.streams.insert(key, relay);
        self.compact();
        Some(due)
    }

    /// Let go of relay stream `key` (it has ended by itself); what held it, if anything.
    pub(crate) fn close(&mut self, key: RelayKey) -> Option<RelayHolder> {
        self.streams.remove(&key).map(|r| r.holder)
    }

    /// This node's own certificate now ends at `own` (a committed reload): every stream
    /// whose end is held by this node's earlier certificate is planned to end at a
    /// uniformly random moment soon after `now_ms` ([`MovePlan::after_renewal`]), unless it
    /// is due sooner already. `jitter` is drawn once per such stream.
    pub(crate) fn own_renewed(&mut self, own: i64, now_ms: i64, mut jitter: impl FnMut() -> f64) {
        for (key, relay) in &mut self.streams {
            let ends = relay.cert.ends;
            let Some(held) = ends.own else { continue };
            // Only where this node's certificate is what ends the stream: where the peer's
            // ends as soon or sooner, the stream remade now would end no later.
            if ends.peer.is_some_and(|peer| peer <= held) || Some(own) <= relay.own_seen {
                continue;
            }
            relay.own_seen = Some(own);
            let Some(plan) = relay.plan() else { continue };
            if now_ms > plan.end_ms {
                continue;
            }
            let at = plan.after_renewal(now_ms, jitter());
            if at < relay.due_ms {
                relay.due_ms = at;
                self.due.push(Reverse((at, *key)));
            }
        }
    }

    /// The streams due at `now_ms` (whose `now_secs` is the same moment in seconds), taken
    /// off, with why each ends.
    pub(crate) fn take_due(
        &mut self,
        now_ms: i64,
        now_secs: i64,
    ) -> Vec<(RelayKey, RelayHolder, RelayEnd)> {
        let mut ended = Vec::new();
        while let Some(Reverse((due, key))) = self.due.peek().copied() {
            if due > now_ms {
                break;
            }
            self.due.pop();
            if self.streams.get(&key).is_none_or(|r| r.due_ms != due) {
                continue;
            }
            let Some(relay) = self.streams.remove(&key) else {
                continue;
            };
            let why = match relay.cert.ends.expired(now_secs) {
                Some(reason) => RelayEnd::Expired(relay_reason(reason)),
                None => RelayEnd::Moved,
            };
            ended.push((key, relay.holder, why));
        }
        self.compact();
        ended
    }

    /// The streams whose remote certificate `revoked` names, taken off.
    pub(crate) fn take_revoked(
        &mut self,
        revoked: &mqtt_auth::signed_gossip::RevocationList,
    ) -> Vec<(RelayKey, RelayHolder)> {
        let keys: Vec<RelayKey> = self
            .streams
            .iter()
            .filter(|(_, r)| {
                r.cert
                    .serial
                    .as_ref()
                    .is_some_and(|serial| revoked.contains(serial))
            })
            .map(|(key, _)| *key)
            .collect();
        let taken = keys
            .into_iter()
            .filter_map(|key| self.streams.remove(&key).map(|r| (key, r.holder)))
            .collect();
        self.compact();
        taken
    }

    /// The next moment a stream is due, Unix milliseconds — perhaps one that has since
    /// gone, which only makes the sweep look once for nothing.
    pub(crate) fn next_due_ms(&self) -> Option<i64> {
        self.due.peek().map(|Reverse((due, _))| *due)
    }

    /// Rebuild the heap once entries for streams that have gone or moved outnumber the
    /// streams held.
    fn compact(&mut self) {
        if self.due.len() <= 2 * self.streams.len() + 1024 {
            return;
        }
        self.due = self
            .streams
            .iter()
            .filter(|(_, r)| r.due_ms != i64::MAX)
            .map(|(key, r)| Reverse((r.due_ms, *key)))
            .collect();
    }
}

/// The relay stream's reason label for a link's expiry reason (`peer-cert-expired` /
/// `own-cert-expired`): a relay stream's end is counted apart from a peer link's, since
/// it is one per relayed client, not one per peer.
fn relay_reason(link_reason: &'static str) -> &'static str {
    if link_reason == "own-cert-expired" {
        "relay-own-cert-expired"
    } else {
        "relay-peer-cert-expired"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hub::LinkCertEnds;

    const HOUR_MS: i64 = 3_600_000;

    fn cert(peer: Option<i64>, own: Option<i64>) -> LinkCert {
        LinkCert {
            serial: Some(vec![0x10, 0x16]),
            ends: LinkCertEnds { peer, own },
        }
    }

    fn session(n: u32) -> RelayHolder {
        RelayHolder::Session(ClientId(format!("c{n}").into()))
    }

    #[test]
    fn a_plaintext_stream_is_not_held() {
        let mut relays = Relays::default();
        assert_eq!(
            relays.open(1, LinkCert::default(), session(1), 0, 0.5),
            None
        );
        assert_eq!(relays.len(), 0);
        assert_eq!(relays.next_due_ms(), None);
    }

    #[test]
    fn a_short_lived_stream_ends_the_second_after_its_earlier_end_and_says_whose() {
        let mut relays = Relays::default();
        // 40 s to go: no end window, only the hard end.
        relays.open(
            1,
            cert(Some(1_040), Some(9_999)),
            session(1),
            1_000_000,
            0.5,
        );
        relays.open(
            2,
            cert(Some(9_999), Some(1_040)),
            session(2),
            1_000_000,
            0.5,
        );
        assert_eq!(relays.next_due_ms(), Some(1_041_000));
        // The last second of the window is served.
        assert!(relays.take_due(1_040_999, 1_040).is_empty());
        let mut ended = relays.take_due(1_041_000, 1_041);
        ended.sort_by_key(|(key, _, _)| *key);
        let why: Vec<_> = ended.iter().map(|(key, _, why)| (*key, *why)).collect();
        assert_eq!(
            why,
            vec![
                (1, RelayEnd::Expired("relay-peer-cert-expired")),
                (2, RelayEnd::Expired("relay-own-cert-expired")),
            ]
        );
        assert_eq!(relays.len(), 0);
    }

    #[test]
    fn a_long_lived_stream_ends_in_its_end_window_not_at_the_end() {
        let mut relays = Relays::default();
        let now = 0;
        let day_end = 24 * 3600;
        let at = |jitter| {
            let mut relays = Relays::default();
            relays
                .open(
                    1,
                    cert(Some(day_end), Some(day_end)),
                    session(1),
                    now,
                    jitter,
                )
                .unwrap()
        };
        let end_ms = day_end * 1000;
        // Five minutes, closing a minute before the end.
        assert_eq!(at(0.0), end_ms - 6 * 60_000);
        assert_eq!(at(0.5), end_ms - 6 * 60_000 + 150_000);
        assert!(at(0.999_999) < end_ms - 60_000);
        relays.open(1, cert(Some(day_end), Some(day_end)), session(1), now, 0.5);
        let ended = relays.take_due(end_ms - 210_000, day_end - 210);
        assert_eq!(ended.len(), 1);
        assert_eq!(
            ended[0].2,
            RelayEnd::Moved,
            "ended ahead of the end, not expired"
        );
    }

    #[test]
    fn a_stream_remade_inside_the_window_is_held_to_the_hard_end_only() {
        let mut relays = Relays::default();
        let day_end = 24 * 3600;
        // The reconnect of a stream ended in the window, nobody having renewed: four
        // minutes to go.
        let due = relays
            .open(
                1,
                cert(Some(day_end), Some(day_end)),
                session(1),
                day_end * 1000 - 240_000,
                0.0,
            )
            .unwrap();
        assert_eq!(due, hard_end_ms(day_end));
        // Exactly at the threshold it is planned; below it, not.
        let made = day_end * 1000 - RELAY_WINDOW_MIN_LIFE_MS;
        assert!(
            relays
                .open(2, cert(Some(day_end), None), session(2), made, 0.0)
                .unwrap()
                < due
        );
        assert_eq!(
            relays
                .open(3, cert(Some(day_end), None), session(3), made + 1, 0.0)
                .unwrap(),
            due
        );
    }

    #[test]
    fn this_nodes_renewal_ends_the_streams_its_old_certificate_holds_soon_and_spread() {
        let mut relays = Relays::default();
        let now = 0;
        let own_s = 3 * 3600;
        // Held by this node's certificate.
        relays.open(
            1,
            cert(Some(own_s + 3600), Some(own_s)),
            session(1),
            now,
            0.5,
        );
        // Held as much by the peer's (minted together): a remade stream would end no later.
        relays.open(2, cert(Some(own_s), Some(own_s)), session(2), now, 0.5);
        // Held by the peer's alone.
        relays.open(
            3,
            cert(Some(own_s - 600), Some(own_s)),
            session(3),
            now,
            0.5,
        );
        let window_2 = relays.streams[&2].due_ms;
        let window_3 = relays.streams[&3].due_ms;

        let renewed_at = HOUR_MS;
        let mut draws = [0.25].into_iter();
        relays.own_renewed(own_s + 24 * 3600, renewed_at, || draws.next().unwrap());
        // Within five minutes of the renewal (half the time left is more than that).
        assert_eq!(relays.streams[&1].due_ms, renewed_at + 75_000);
        assert_eq!(relays.streams[&2].due_ms, window_2);
        assert_eq!(relays.streams[&3].due_ms, window_3);
        // A second reload of the same certificate plans nothing new.
        relays.own_renewed(own_s + 24 * 3600, renewed_at + 1000, || 0.0);
        assert_eq!(relays.streams[&1].due_ms, renewed_at + 75_000);

        let ended = relays.take_due(renewed_at + 75_000, (renewed_at + 75_000) / 1000);
        assert_eq!(ended.len(), 1);
        assert_eq!((ended[0].0, ended[0].2), (1, RelayEnd::Moved));
    }

    #[test]
    fn the_relays_of_one_renewal_day_endpread_over_minutes() {
        let mut relays = Relays::default();
        let own_s = 3 * 3600;
        for key in 0..200 {
            relays.open(key, cert(Some(own_s * 2), Some(own_s)), session(1), 0, 0.5);
        }
        relays.own_renewed(own_s * 4, HOUR_MS, crate::peer::jitter);
        let dues: Vec<i64> = relays.streams.values().map(|r| r.due_ms).collect();
        let (lo, hi) = (dues.iter().min().unwrap(), dues.iter().max().unwrap());
        assert!(*lo >= HOUR_MS && *hi <= HOUR_MS + MOVE_SPREAD_MAX_MS);
        assert!(
            hi - lo > 4 * 60_000,
            "spread over more than four minutes: {lo}..{hi}"
        );
    }

    #[test]
    fn a_revoked_serial_takes_its_streams_and_no_other() {
        let mut relays = Relays::default();
        relays.open(1, cert(Some(i64::MAX / 2000), None), session(1), 0, 0.5);
        let other = LinkCert {
            serial: Some(vec![0x09, 0x75]),
            ..cert(Some(i64::MAX / 2000), None)
        };
        relays.open(2, other, session(2), 0, 0.5);
        let list = mqtt_auth::signed_gossip::RevocationList::from_serials([vec![0x10, 0x16]]);
        let taken: Vec<RelayKey> = relays
            .take_revoked(&list)
            .into_iter()
            .map(|(key, _)| key)
            .collect();
        assert_eq!(taken, vec![1]);
        assert_eq!(relays.len(), 1);
    }

    #[test]
    fn a_closed_stream_is_not_ended_and_a_moved_due_is_not_taken_twice() {
        let mut relays = Relays::default();
        relays.open(1, cert(Some(100), Some(100)), session(1), 0, 0.5);
        assert!(relays.close(1).is_some());
        assert!(relays.take_due(1_000_000, 1_000).is_empty());

        // A planned end moved earlier leaves the old entry in the heap; it is skipped.
        relays.open(2, cert(Some(7200 * 2), Some(7200)), session(2), 0, 0.5);
        relays.own_renewed(7200 * 3, 1000, || 0.0);
        let ended = relays.take_due(i64::MAX / 2, i64::MAX / 2000);
        assert_eq!(ended.len(), 1);
        assert!(relays.take_due(i64::MAX / 2, i64::MAX / 2000).is_empty());
    }
}
