//! Independent checking of recorded client-visible histories (issue #231,
//! ADR 0044): the broker's durability promises, verified from what clients
//! actually SAW, by code that shares nothing with the implementation.
//!
//! The out-of-process harness records one JSONL event per client-visible fact —
//! connects (with the session-present answer), durable subscribes, publishes with
//! their acknowledgement outcome, deliveries, retained probes, and the nemesis
//! events (kills/restarts) that frame them. This crate re-derives the promises
//! from those facts alone:
//!
//! 1. **Acked durability** — every payload whose PUBACK was received, on a topic a
//!    durable subscriber held at publish time, is delivered to that subscriber by
//!    the end of the history (duplicates legal, loss not).
//! 2. **Session-present honesty** — a resume of a session that was ever
//!    successfully established answers `session_present = true`; the very first
//!    establishment answers `false` (ADR 0017's recovery-honesty contract, from
//!    the client's chair).
//! 3. **Retained convergence** — the final probe of every node serves a value
//!    at-or-beyond the last acknowledged retained set of its topic, and every
//!    node serves the same one.
//! 4. **Per-publisher order** — deliveries to one subscriber from one publisher
//!    stream never reorder that publisher's acknowledged sequence.
//! 5. **Redelivery is marked** — a `QoS` > 0 payload delivered to the same
//!    subscriber more than once carries `DUP = 1` on every delivery after the
//!    first ([MQTT-4.4.0-1]), so a client can tell a repeat from a new message.
//! 6. **`QoS` 2 is exactly once** — a payload published once and delivered at
//!    `QoS` 2 reaches each subscriber's application at most once, even when its
//!    publisher re-sent it after a fault (\[MQTT-4.3.3\]).
//! 7. **No protocol errors** — the test client saw the broker keep the protocol
//!    (a packet id is never reused while it is in flight, \[MQTT-2.2.1-4\]).
//! 8. **Wills when owed, and only then** — a connection that ends without a
//!    normal DISCONNECT has its Will published once, after its Will Delay or when
//!    its session ends, whichever is first, to every subscriber of its topic that
//!    is owed it; a normal DISCONNECT, or a resume inside the delay, means no Will
//!    at all (\[MQTT-3.1.2-8\], MQTT 5 §3.1.2.5, §3.1.3.2.2, §3.1.4).
//!
//! Check 2 also honours Clean Start and the Session Expiry Interval, a new one sent
//! with DISCONNECT included (MQTT 5 §3.1.2.11, §3.14.2.2.2), and a takeover of a
//! session whose expiry is 0, which ends it. Checks 2 and 8 read the same timeline
//! of each client's connections ([`spans`]).
//!
//! Checks 6 to 8 judge what the recorder gives them: a history without `QoS` 2
//! deliveries, Wills, DISCONNECTs or expiries keeps them trivially.
//!
//! A violation names the events that prove it; the harness prints the offending
//! slice, keeping the #214 discipline: a failure must be diagnosable without a
//! re-run.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// One recorded client-visible fact. `at_ms` is milliseconds since the schedule
/// started — ordering within the recording process is what matters, not wall
/// truth.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "ev", rename_all = "snake_case")]
pub enum Event {
    /// A client's CONNECT completed, with the broker's session-present answer.
    Connect {
        at_ms: u64,
        client: String,
        via_node: String,
        session_present: bool,
        /// Clean Start (MQTT 5) or `CleanSession` (3.1.1). `None` in histories recorded
        /// before it was, whose clients all resumed durable sessions: it reads as
        /// `false`. Every new field below is optional for the same reason as
        /// `Deliver::dup`: the nightly re-checks archived histories.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        clean_start: Option<bool>,
        /// The session's expiry interval from this CONNECT, in seconds. `None`: it
        /// never expires (3.1.1 with `CleanSession` 0, and histories that did not
        /// record it). An MQTT 5 recorder records 0 for a CONNECT without the
        /// property, as the spec reads it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_expiry_s: Option<u32>,
        /// The Will this connection set.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        will: Option<Will>,
        /// The recorder's id for this connection, which its end names. Without ids,
        /// the recorder must record a connection's end before the same client's next
        /// CONNECT completes, except in a takeover, where it records no end at all:
        /// an end recorded after the next CONNECT would be read as the end of the new
        /// connection.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conn: Option<String>,
    },
    /// The client sent DISCONNECT: reason 0 deletes its Will; MQTT 5's 0x04 asks for
    /// it all the same. An MQTT 5 DISCONNECT may set a new session expiry.
    Disconnect {
        at_ms: u64,
        client: String,
        #[serde(default)]
        reason: u8,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_expiry_s: Option<u32>,
        /// The connection that ended (see `Connect::conn`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conn: Option<String>,
    },
    /// The client's connection ended without it sending DISCONNECT: it dropped the
    /// socket or went silent, its node was killed, its link was cut, or the broker
    /// closed it (a takeover included).
    ConnectionLost {
        at_ms: u64,
        client: String,
        why: String,
        /// The connection that ended (see `Connect::conn`). A takeover is recorded
        /// by the next `Connect` alone; a `ConnectionLost` naming the taken-over
        /// connection is then ignored.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conn: Option<String>,
    },
    /// The test client caught the broker breaking the protocol: a packet id reused
    /// while in flight, an acknowledgement for nothing, a malformed packet.
    ProtocolError {
        at_ms: u64,
        client: String,
        what: String,
    },
    /// A durable subscription was granted (SUBACK success) for `client`.
    Subscribe {
        at_ms: u64,
        client: String,
        topic: String,
    },
    /// A publish attempt finished: `acked` is whether the PUBACK arrived. Only
    /// acked publishes create obligations.
    Publish {
        at_ms: u64,
        publisher: String,
        topic: String,
        payload: String,
        acked: bool,
        /// The publisher's own sequence number for order checking.
        seq: u64,
    },
    /// A retained set attempt finished (`acked` as above).
    RetainedSet {
        at_ms: u64,
        topic: String,
        payload: String,
        acked: bool,
    },
    /// A payload was delivered to (and acknowledged by) a subscriber.
    ///
    /// A `QoS` 2 message is recorded when the client hands it to its application,
    /// once per packet id until the PUBREL: a re-sent PUBLISH of an id the client
    /// still holds is the same message, as a real client sees it, and is not
    /// recorded again (check 6 counts application deliveries). The client keeps
    /// those ids across reconnects of the same session, since the broker may
    /// re-send them on the next connection.
    ///
    /// **Every** delivery is recorded, including repeats of a payload the same
    /// subscriber already saw. That is deliberate and load-bearing: the recorder
    /// used to drop duplicates, and no counting promise (redelivery marking,
    /// exactly-once) can be checked against a history that has already thrown
    /// away the counts. `check_acked_durability` and `check_publisher_order` both
    /// tolerate repeats, so the richer history costs them nothing.
    Deliver {
        at_ms: u64,
        client: String,
        topic: String,
        payload: String,
        /// The wire DUP flag as the subscriber saw it ([MQTT-4.4.0-1]).
        ///
        /// `#[serde(default)]` because the nightly archives histories for 14 days
        /// and re-checks them with the standalone binary: `parse` deliberately
        /// refuses what it cannot decode, so a required field would make every
        /// history recorded before this change unreadable.
        #[serde(default)]
        dup: bool,
        /// The delivered `QoS`. Redelivery is a `QoS` > 0 concept only.
        #[serde(default)]
        qos: u8,
    },
    /// A fresh clean-session probe read a node's retained value for `topic`
    /// (`payload = None` = nothing served). The LAST probe per (node, topic)
    /// is the convergence verdict.
    RetainedProbe {
        at_ms: u64,
        node: String,
        topic: String,
        payload: Option<String>,
    },
    /// Nemesis: a node was killed / restarted — recorded so a violation's
    /// context is legible, not used by the checks themselves.
    Nemesis { at_ms: u64, what: String },
}

/// A Will as its connection set it. The recorder makes `payload` unique per
/// connection, so a delivery names the connection it was owed for.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Will {
    pub topic: String,
    pub payload: String,
    /// The Will's `QoS`: a `QoS` 0 Will is not kept for an offline subscriber, and a
    /// `QoS` 2 one is delivered exactly once.
    pub qos: u8,
    /// MQTT 5 Will Delay Interval, in seconds; 0 in 3.1.1.
    #[serde(default)]
    pub delay_s: u32,
}

impl Event {
    fn at(&self) -> u64 {
        match self {
            Event::Connect { at_ms, .. }
            | Event::Disconnect { at_ms, .. }
            | Event::ConnectionLost { at_ms, .. }
            | Event::ProtocolError { at_ms, .. }
            | Event::Subscribe { at_ms, .. }
            | Event::Publish { at_ms, .. }
            | Event::RetainedSet { at_ms, .. }
            | Event::Deliver { at_ms, .. }
            | Event::RetainedProbe { at_ms, .. }
            | Event::Nemesis { at_ms, .. } => *at_ms,
        }
    }
}

/// One violated promise, with the evidence that proves it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    /// Which check found it.
    pub check: &'static str,
    /// The human-readable finding.
    pub detail: String,
}

impl std::fmt::Display for Violation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.check, self.detail)
    }
}

/// Parse a JSONL history. Unknown lines are an error — a checker that skips what
/// it does not understand can be silently blinded by a recorder change.
///
/// # Errors
/// The first malformed line, with its number.
pub fn parse(jsonl: &str) -> Result<Vec<Event>, String> {
    let mut events = Vec::new();
    for (i, line) in jsonl.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let ev: Event = serde_json::from_str(line)
            .map_err(|e| format!("history line {}: {e}: {line}", i + 1))?;
        events.push(ev);
    }
    Ok(events)
}

/// Run every check; the returned list is empty exactly when the history keeps
/// every promise.
#[must_use]
pub fn check(events: &[Event]) -> Vec<Violation> {
    let mut v = Vec::new();
    v.extend(check_acked_durability(events));
    v.extend(check_session_present(events));
    v.extend(check_retained_convergence(events));
    v.extend(check_publisher_order(events));
    v.extend(check_redelivery_marked(events));
    v.extend(check_qos2_once(events));
    v.extend(check_protocol(events));
    v.extend(check_wills(events));
    v
}

/// How far from a computed instant (a Will's due time, a session's expiry) an
/// observation may land and still be judged: broker timers, scheduling and the
/// recorder's own clock all blur the instant.
pub const TIMING_TOLERANCE_MS: u64 = 1_000;

/// Check 1: an acked payload on a topic a durable subscriber held at publish
/// time must be delivered to that subscriber somewhere in the history.
fn check_acked_durability(events: &[Event]) -> Vec<Violation> {
    // Subscription intervals: (client, topic) -> granted-at (the harness never
    // unsubscribes; a re-grant is idempotent).
    let mut granted: BTreeMap<(&str, &str), u64> = BTreeMap::new();
    let mut delivered: BTreeSet<(&str, &str, &str)> = BTreeSet::new();
    for ev in events {
        match ev {
            Event::Subscribe { client, topic, .. } => {
                granted.entry((client, topic)).or_insert(ev.at());
            }
            Event::Deliver {
                client,
                topic,
                payload,
                ..
            } => {
                delivered.insert((client, topic, payload));
            }
            _ => {}
        }
    }
    let mut v = Vec::new();
    for ev in events {
        let Event::Publish {
            at_ms,
            topic,
            payload,
            acked: true,
            ..
        } = ev
        else {
            continue;
        };
        for ((client, t), granted_at) in &granted {
            if t != topic || granted_at > at_ms {
                continue;
            }
            if !delivered.contains(&(client, topic.as_str(), payload.as_str())) {
                v.push(Violation {
                    check: "acked-durability",
                    detail: format!(
                        "payload {payload:?} on {topic} was ACKED at t+{at_ms}ms with \
                         {client} durably subscribed since t+{granted_at}ms, but was \
                         never delivered to it"
                    ),
                });
            }
        }
    }
    v
}

/// How a recorded connection ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndKind {
    /// DISCONNECT 0x00: the Will is deleted.
    Normal,
    /// DISCONNECT with another reason (0x04 asks for the Will), or a lost connection.
    Abnormal,
    /// The same client connected again before an end was recorded.
    TakenOver,
}

/// One connection of one client, as the history recorded it.
struct Span<'a> {
    client: &'a str,
    at: u64,
    via_node: &'a str,
    present: bool,
    clean: bool,
    expiry_s: Option<u32>,
    will: Option<&'a Will>,
    conn: Option<&'a str>,
    /// When and how it ended, and an expiry its DISCONNECT set.
    end: Option<(u64, EndKind, Option<u32>)>,
}

impl Span<'_> {
    /// The session's expiry after this connection, a DISCONNECT's override applied.
    fn expiry_after(&self) -> Option<u32> {
        match self.end {
            Some((_, _, Some(exp))) => Some(exp),
            _ => self.expiry_s,
        }
    }
}

/// Every client's connections in order. An end applies to the client's open
/// connection, unless it names a different one (`conn`), which is a stale end of an
/// earlier connection, taken over already, and is ignored.
fn spans(events: &[Event]) -> Vec<Span<'_>> {
    let mut out: Vec<Span> = Vec::new();
    let mut open: BTreeMap<&str, usize> = BTreeMap::new();
    for ev in events {
        let (at, client, kind, exp, conn) = match ev {
            Event::Connect {
                at_ms,
                client,
                via_node,
                session_present,
                clean_start,
                session_expiry_s,
                will,
                conn,
            } => {
                if let Some(i) = open.remove(client.as_str()) {
                    out[i].end = Some((*at_ms, EndKind::TakenOver, None));
                }
                open.insert(client, out.len());
                out.push(Span {
                    client,
                    at: *at_ms,
                    via_node,
                    present: *session_present,
                    clean: clean_start.unwrap_or(false),
                    expiry_s: *session_expiry_s,
                    will: will.as_ref(),
                    conn: conn.as_deref(),
                    end: None,
                });
                continue;
            }
            Event::Disconnect {
                at_ms,
                client,
                reason,
                session_expiry_s,
                conn,
            } => {
                let kind = if *reason == 0 {
                    EndKind::Normal
                } else {
                    EndKind::Abnormal
                };
                (*at_ms, client, kind, *session_expiry_s, conn)
            }
            Event::ConnectionLost {
                at_ms,
                client,
                conn,
                ..
            } => (*at_ms, client, EndKind::Abnormal, None, conn),
            _ => continue,
        };
        let Some(&i) = open.get(client.as_str()) else {
            continue;
        };
        if conn.is_some() && conn.as_deref() != out[i].conn {
            continue; // the end of an earlier connection
        }
        open.remove(client.as_str());
        out[i].end = Some((at, kind, exp));
    }
    out
}

/// Whether a session that followed `prev` still existed at `at`: `Some(answer)`, or
/// `None` when `at` is within [`TIMING_TOLERANCE_MS`] of its expiry.
fn session_exists(prev: &Span, at: u64) -> Option<bool> {
    let Some((end, kind, _)) = prev.end else {
        return Some(true); // still connected
    };
    let exp = prev.expiry_after();
    if kind == EndKind::TakenOver {
        return Some(exp != Some(0)); // a takeover ends a session of expiry 0 (§3.1.4)
    }
    match exp {
        None => Some(true),
        Some(0) => Some(false),
        Some(exp) => {
            let gone = end + u64::from(exp) * 1_000;
            if at > gone + TIMING_TOLERANCE_MS {
                Some(false)
            } else if at + TIMING_TOLERANCE_MS < gone {
                Some(true)
            } else {
                None
            }
        }
    }
}

/// Check 2: session-present honesty from the client's chair.
///
/// A session exists from its first CONNECT until Clean Start discards it, a
/// takeover ends it (expiry 0), or its expiry interval runs out after its
/// connection ends. A CONNECT that does not start clean must answer
/// `session_present = true` while it exists and `false` once it is gone.
/// Histories without the expiry fields read as before: every session, once
/// established, lasts forever.
fn check_session_present(events: &[Event]) -> Vec<Violation> {
    let all = spans(events);
    let mut last: BTreeMap<&str, &Span> = BTreeMap::new();
    let mut v = Vec::new();
    for span in &all {
        let expected = if span.clean {
            Some(false)
        } else {
            match last.get(span.client) {
                None => Some(false),
                Some(prev) => session_exists(prev, span.at),
            }
        };
        if let Some(expected) = expected {
            if span.present != expected {
                v.push(Violation {
                    check: "session-present",
                    detail: format!(
                        "{} connected via {} at t+{}ms (clean start {}): session_present \
                         was {}, but the session {}",
                        span.client,
                        span.via_node,
                        span.at,
                        span.clean,
                        span.present,
                        if expected {
                            "existed: established before and not expired"
                        } else if last.contains_key(span.client) {
                            "was gone: discarded by clean start, ended, or expired"
                        } else {
                            "was NEVER established before"
                        }
                    ),
                });
            }
        }
        last.insert(span.client, span);
    }
    v
}

/// Check 3: every node's FINAL retained probe for a topic serves the same value,
/// at-or-beyond the last acked set.
fn check_retained_convergence(events: &[Event]) -> Vec<Violation> {
    // Candidate payloads per topic: the last acked set and everything after it
    // (an unacked newer set may legally have landed).
    let mut history: BTreeMap<&str, Vec<(&str, bool)>> = BTreeMap::new();
    let mut final_probe: BTreeMap<(&str, &str), Option<&str>> = BTreeMap::new();
    for ev in events {
        match ev {
            Event::RetainedSet {
                topic,
                payload,
                acked,
                ..
            } => history.entry(topic).or_default().push((payload, *acked)),
            Event::RetainedProbe {
                node,
                topic,
                payload,
                ..
            } => {
                final_probe.insert((node, topic), payload.as_deref());
            }
            _ => {}
        }
    }
    let mut v = Vec::new();
    for (topic, sets) in &history {
        let Some(last_acked) = sets.iter().rposition(|(_, acked)| *acked) else {
            continue; // nothing was ever promised
        };
        let candidates: BTreeSet<&str> = sets[last_acked..].iter().map(|(p, _)| *p).collect();
        let probes: Vec<(&str, Option<&str>)> = final_probe
            .iter()
            .filter(|((_, t), _)| t == topic)
            .map(|((n, _), p)| (*n, *p))
            .collect();
        if probes.is_empty() {
            continue;
        }
        let first = probes[0].1;
        let converged = probes.iter().all(|(_, p)| *p == first)
            && first.is_some_and(|p| candidates.contains(p));
        if !converged {
            v.push(Violation {
                check: "retained-convergence",
                detail: format!(
                    "topic {topic}: final probes {probes:?} did not converge on a \
                     value at-or-beyond the last acked set {candidates:?}"
                ),
            });
        }
    }
    v
}

/// Check 5: a repeat delivery of the same payload to the same subscriber is
/// marked `DUP = 1`.
///
/// [MQTT-4.4.0-1]: when a client reconnects with an existing session, the server
/// resends unacknowledged messages with the DUP flag set. The hub's own doc
/// comment claims this, so the check verifies a stated behaviour rather than
/// inventing one. It matters because an unmarked repeat is precisely how a
/// client silently double-processes a message it has already handled: DUP is the
/// only signal it gets that "you may have seen this before".
///
/// Two restrictions keep it from firing on legal histories, and both are the
/// difference between a check and a nuisance:
///
/// * **`QoS` 0 is skipped.** There is no redelivery concept below `QoS` 1, so a
///   repeat there is a different phenomenon and carries no DUP promise.
/// * **Only payloads the history shows published EXACTLY ONCE are considered.**
///   Two genuinely distinct publishes can carry identical bytes, and the second
///   one's delivery is then a FIRST delivery wearing the same clothes as a
///   repeat. This is not hypothetical: the harness's interest warm-up
///   republishes one payload in a loop until the subscriber observes it, and a
///   recorded history really does contain two same-ms deliveries of those bytes
///   with `DUP = 0` — correctly, because they are two publishes. Without this
///   guard the check reports a violation for correct behaviour, which is worse
///   than not checking at all.
///
///   The rule also skips payloads with NO recorded `Publish` (warm-up traffic
///   is not recorded), which is the same conservatism: if the history cannot say
///   how many times those bytes were sent, it cannot say a delivery is a repeat.
///   Where it DOES apply the check is exact, because the recorded publish stream
///   carries a per-publisher `seq` in every payload and so never repeats bytes.
///
/// **Honest status: this check is LATENT on today's `cluster_proc` workload.** An
/// 8-seed sweep recorded 120 deliveries and *zero* judgeable repeats, so it is
/// proven only against fixtures ([`tests::each_check_bites`]) — it has never yet
/// judged a real one. It is reachable rather than dead: the shape it wants is a
/// `QoS` 1 delivery whose PUBACK is lost to a node death, which the seeded kill
/// can produce but did not in those seeds. Two things follow, and neither is
/// "assume it works": the nightly sweep runs far more seeds than 8 and re-checks
/// 14 days of archived histories, so the odds compound; and a schedule step that
/// manufactures the shape deliberately is the named follow-up. It is recorded
/// here rather than in a commit message because a check nobody knows is latent
/// reads as coverage it has not earned.
fn check_redelivery_marked(events: &[Event]) -> Vec<Violation> {
    let published = publish_counts(events);
    let mut seen: BTreeSet<(&str, &str)> = BTreeSet::new();
    let mut v = Vec::new();
    for ev in events {
        let Event::Deliver {
            at_ms,
            client,
            payload,
            dup,
            qos,
            ..
        } = ev
        else {
            continue;
        };
        if *qos == 0 || published.get(payload.as_str()).copied() != Some(1) {
            continue;
        }
        if !seen.insert((client, payload)) && !dup {
            v.push(Violation {
                check: "redelivery-marked",
                detail: format!(
                    "{client} received payload {payload:?} again at t+{at_ms}ms with \
                     DUP = 0 — a repeat delivery must be marked [MQTT-4.4.0-1], or the \
                     subscriber cannot tell it apart from a new message"
                ),
            });
        }
    }
    v
}

/// How many times each payload was published at all, acked or not: an unacked
/// publish may still have reached the broker and been delivered.
fn publish_counts(events: &[Event]) -> BTreeMap<&str, usize> {
    let mut published: BTreeMap<&str, usize> = BTreeMap::new();
    for ev in events {
        if let Event::Publish { payload, .. } = ev {
            *published.entry(payload).or_default() += 1;
        }
    }
    published
}

/// Check 6: a payload published once and delivered at `QoS` 2 reaches each
/// subscriber at most once (\[MQTT-4.3.3\]: exactly once).
///
/// DUP does not excuse a repeat here: at `QoS` 2 the client hands a message to its
/// application once, and the recorder records exactly that (see
/// [`Event::Deliver`]), so a second record is a second message. Its usual cause
/// is the broker taking the publisher's re-send after a fault as a new message.
/// The same guards as check 5 apply: only payloads the history shows published
/// exactly once are judged.
fn check_qos2_once(events: &[Event]) -> Vec<Violation> {
    let published = publish_counts(events);
    let mut seen: BTreeMap<(&str, &str), u64> = BTreeMap::new();
    let mut v = Vec::new();
    for ev in events {
        let Event::Deliver {
            at_ms,
            client,
            payload,
            qos: 2,
            ..
        } = ev
        else {
            continue;
        };
        if published.get(payload.as_str()).copied() != Some(1) {
            continue;
        }
        if let Some(first) = seen.insert((client, payload), *at_ms) {
            v.push(Violation {
                check: "qos2-once",
                detail: format!(
                    "{client} received QoS 2 payload {payload:?} again at t+{at_ms}ms \
                     (first at t+{first}ms): QoS 2 is delivered exactly once [MQTT-4.3.3]"
                ),
            });
        }
    }
    v
}

/// Check 7: the test client saw no protocol error from the broker.
fn check_protocol(events: &[Event]) -> Vec<Violation> {
    events
        .iter()
        .filter_map(|ev| match ev {
            Event::ProtocolError {
                at_ms,
                client,
                what,
            } => Some(Violation {
                check: "protocol",
                detail: format!("{client} at t+{at_ms}ms: {what}"),
            }),
            _ => None,
        })
        .collect()
}

/// Whether an MQTT topic filter matches a topic (`+` one level, `#` the rest). It
/// does not keep `$`-topics from wildcards or know `$share/`; the recorder uses
/// neither for Wills.
fn filter_matches(filter: &str, topic: &str) -> bool {
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) | (None, None) => return true,
            (Some("+"), Some(_)) => {}
            (Some(a), Some(b)) if a == b => {}
            _ => return false,
        }
    }
}

/// Check 8: Wills are published when owed, once, and never when not.
///
/// For each connection that set a Will, its end decides:
///
/// * a DISCONNECT with reason 0 deletes the Will: it is never owed;
/// * any other end (DISCONNECT 0x04, a lost connection, a takeover) owes it after
///   its Will Delay, or when its session ends if that comes first: the session's
///   expiry interval after the end, or at once for an expiry of 0;
/// * a new connection for the client before the Will is due cancels it, unless
///   that connection starts clean, which ends the session and makes the Will due
///   at that moment. A takeover is the case where the new connection arrives
///   before the old one's end was recorded: the Will is owed at once with a delay
///   of 0, with clean start, or with an expiry of 0, and otherwise cancelled
///   (the note in MQTT 5 §3.1.4).
///
/// An owed Will must reach every client subscribed to a matching filter before it
/// was due that is owed it: one online then, or one whose session still existed
/// for a Will of `QoS` 1 or 2. It must not arrive more than [`TIMING_TOLERANCE_MS`]
/// early, nor more than [`WILL_LATE_MS`] late to a subscriber online when it was
/// due, and only once (below `QoS` 2 a repeat marked DUP is legal). One that is not
/// owed must reach no one. A Will due too close to the history's end is not judged.
fn check_wills(events: &[Event]) -> Vec<Violation> {
    let all = spans(events);
    judge_wills(events, &all, &will_verdicts(&all))
}

/// Each Will's verdict: owed from an instant, or not owed and why.
type WillVerdict<'a> = (&'a Will, Result<u64, String>);

/// When a Will is due after its connection ended at `end`: after its delay, or when
/// its session expires if that is sooner (`None`: the session never expires).
fn due_after(end: u64, delay_s: u32, expiry_s: Option<u32>) -> u64 {
    let delay = u64::from(delay_s) * 1_000;
    match expiry_s {
        Some(exp) => end + delay.min(u64::from(exp) * 1_000),
        None => end + delay,
    }
}

/// How late an owed Will may reach a subscriber that was online when it was due.
pub const WILL_LATE_MS: u64 = 5_000;

/// Check 8's first half: what each connection's end owes (see [`check_wills`]).
fn will_verdicts<'a>(all: &[Span<'a>]) -> Vec<WillVerdict<'a>> {
    let mut v = Vec::new();
    for (i, span) in all.iter().enumerate() {
        let (Some(w), Some((end, kind, _))) = (span.will, span.end) else {
            continue; // no Will, or still connected when the history ended
        };
        let next = all[i + 1..].iter().find(|n| n.client == span.client);
        let verdict = match kind {
            EndKind::Normal => Err(format!(
                "the client sent DISCONNECT 0x00 at t+{end}ms, which deletes it"
            )),
            EndKind::TakenOver => {
                let clean = next.is_some_and(|n| n.clean);
                if w.delay_s == 0 || clean || span.expiry_after() == Some(0) {
                    Ok(end)
                } else {
                    Err(format!(
                        "taken over at t+{end}ms inside its {} s Will Delay by a \
                         connection that resumed the session",
                        w.delay_s
                    ))
                }
            }
            EndKind::Abnormal => {
                let due = due_after(end, w.delay_s, span.expiry_after());
                match next {
                    Some(n) if n.at < due && n.clean => Ok(n.at),
                    Some(n) if n.at < due => Err(format!(
                        "its connection ended at t+{end}ms and the client resumed the \
                         session at t+{}ms, before the Will was due",
                        n.at
                    )),
                    _ => Ok(due),
                }
            }
        };
        v.push((w, verdict));
    }
    v
}

/// Whether `client` is owed a Will due at `due`, and whether it was online then.
/// A client the history never connects (a watcher recorded only by its
/// subscription) is online throughout.
fn watcher_at(all: &[Span], client: &str, due: u64, qos: u8) -> (bool, bool) {
    let mine: Vec<&Span> = all.iter().filter(|s| s.client == client).collect();
    if mine.is_empty() {
        return (true, true);
    }
    let online = mine
        .iter()
        .any(|s| s.at <= due && s.end.is_none_or(|(end, _, _)| end > due));
    let session = online
        || mine
            .iter()
            .rfind(|s| s.at <= due)
            .is_some_and(|prev| session_exists(prev, due) != Some(false));
    (online || (session && qos > 0), online)
}

/// The subscribers of a matching filter granted before `due`, less those a later
/// clean start discarded.
fn watchers_of<'a>(
    granted: &[(&'a str, &str, u64)],
    all: &[Span],
    w: &Will,
    due: u64,
) -> Vec<&'a str> {
    let mut watchers: Vec<&str> = granted
        .iter()
        .filter(|(c, f, at)| {
            let cleaned = all
                .iter()
                .filter(|s| s.client == *c && s.clean && s.at <= due)
                .map(|s| s.at)
                .max();
            *at <= due && cleaned.is_none_or(|c| *at >= c) && filter_matches(f, &w.topic)
        })
        .map(|(c, _, _)| *c)
        .collect();
    watchers.sort_unstable();
    watchers.dedup();
    watchers
}

/// Check 8's second half: whether what arrived matches the verdicts.
fn judge_wills(events: &[Event], all: &[Span], verdicts: &[WillVerdict]) -> Vec<Violation> {
    let end_of_history = events.iter().map(Event::at).max().unwrap_or(0);
    let mut granted: Vec<(&str, &str, u64)> = Vec::new();
    let mut delivered: BTreeMap<&str, Vec<(&str, u64, bool)>> = BTreeMap::new();
    for ev in events {
        match ev {
            Event::Subscribe {
                at_ms,
                client,
                topic,
            } => granted.push((client, topic, *at_ms)),
            Event::Deliver {
                at_ms,
                client,
                payload,
                dup,
                ..
            } => delivered
                .entry(payload)
                .or_default()
                .push((client, *at_ms, *dup)),
            _ => {}
        }
    }
    let mut v = Vec::new();
    let mut say = |w: &Will, detail: String| {
        v.push(Violation {
            check: "will",
            detail: format!("Will {:?} on {}: {detail}", w.payload, w.topic),
        });
    };
    for (w, verdict) in verdicts {
        let got = delivered
            .get(w.payload.as_str())
            .map_or(&[][..], Vec::as_slice);
        let due = match verdict {
            Err(why) => {
                if let Some((client, at, _)) = got.first() {
                    say(
                        w,
                        format!("reached {client} at t+{at}ms, but was not owed: {why}"),
                    );
                }
                continue;
            }
            Ok(due) => *due,
        };
        if due + WILL_LATE_MS + TIMING_TOLERANCE_MS > end_of_history {
            continue; // the history ended before it could be judged
        }
        for watcher in watchers_of(&granted, all, w, due) {
            let (owed, online) = watcher_at(all, watcher, due, w.qos);
            if !owed {
                continue; // offline with no session, or a QoS 0 Will
            }
            let mine: Vec<&(&str, u64, bool)> =
                got.iter().filter(|(c, _, _)| *c == watcher).collect();
            match mine.iter().map(|(_, at, _)| *at).min() {
                None => say(
                    w,
                    format!("owed from t+{due}ms, but never reached {watcher}"),
                ),
                Some(at) if at + TIMING_TOLERANCE_MS < due => say(
                    w,
                    format!(
                        "reached {watcher} at t+{at}ms, before it was due at t+{due}ms \
                         (its Will Delay or session expiry was not honoured)"
                    ),
                ),
                Some(at) if online && at > due + WILL_LATE_MS + TIMING_TOLERANCE_MS => say(
                    w,
                    format!(
                        "reached {watcher}, online, at t+{at}ms, more than {} s after it \
                         was due at t+{due}ms",
                        WILL_LATE_MS / 1_000
                    ),
                ),
                Some(_) => {}
            }
            // At QoS 2 a repeat is a repeat; below, one marked DUP is legal.
            let copies = mine.iter().filter(|(_, _, dup)| w.qos == 2 || !dup).count();
            if copies > 1 {
                say(
                    w,
                    format!("reached {watcher} {copies} times: it is published once"),
                );
            }
        }
    }
    v
}

/// Check 4: deliveries to one subscriber never reorder one publisher's acked
/// sequence.
fn check_publisher_order(events: &[Event]) -> Vec<Violation> {
    // payload -> (publisher, seq) for acked publishes.
    let mut origin: BTreeMap<&str, (&str, u64)> = BTreeMap::new();
    for ev in events {
        if let Event::Publish {
            publisher,
            payload,
            seq,
            acked: true,
            ..
        } = ev
        {
            origin.insert(payload, (publisher, *seq));
        }
    }
    // Per (subscriber, publisher): the seq high-water of FIRST deliveries.
    // Re-deliveries (duplicates) are at-least-once legal and may repeat older
    // seqs; only a FIRST delivery arriving below the high-water is a reorder.
    let mut seen: BTreeMap<(&str, &str), BTreeSet<u64>> = BTreeMap::new();
    let mut v = Vec::new();
    for ev in events {
        let Event::Deliver {
            at_ms,
            client,
            payload,
            ..
        } = ev
        else {
            continue;
        };
        let Some((publisher, seq)) = origin.get(payload.as_str()) else {
            continue; // unacked or foreign payload: no order promise
        };
        let firsts = seen.entry((client, publisher)).or_default();
        if firsts.contains(seq) {
            continue; // a duplicate of an already-delivered seq: legal
        }
        if let Some(max) = firsts.iter().next_back() {
            if seq < max {
                v.push(Violation {
                    check: "publisher-order",
                    detail: format!(
                        "{client} first received {publisher}'s seq {seq} at t+{at_ms}ms \
                         AFTER already receiving seq {max} — per-publisher order broken"
                    ),
                });
            }
        }
        firsts.insert(*seq);
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ok_history() -> Vec<Event> {
        vec![
            Event::Connect {
                at_ms: 1,
                client: "s1".into(),
                via_node: "a".into(),
                session_present: false,
                clean_start: None,
                session_expiry_s: None,
                will: None,
                conn: None,
            },
            Event::Subscribe {
                at_ms: 2,
                client: "s1".into(),
                topic: "t".into(),
            },
            Event::Publish {
                at_ms: 10,
                publisher: "p1".into(),
                topic: "t".into(),
                payload: "m1".into(),
                acked: true,
                seq: 1,
            },
            Event::Deliver {
                at_ms: 11,
                client: "s1".into(),
                topic: "t".into(),
                payload: "m1".into(),
                dup: false,
                qos: 1,
            },
            Event::Nemesis {
                at_ms: 12,
                what: "SIGKILL b".into(),
            },
            Event::Connect {
                at_ms: 20,
                client: "s1".into(),
                via_node: "a".into(),
                session_present: true,
                clean_start: None,
                session_expiry_s: None,
                will: None,
                conn: None,
            },
            Event::RetainedSet {
                at_ms: 30,
                topic: "rt".into(),
                payload: "r1".into(),
                acked: true,
            },
            Event::RetainedProbe {
                at_ms: 40,
                node: "a".into(),
                topic: "rt".into(),
                payload: Some("r1".into()),
            },
            Event::RetainedProbe {
                at_ms: 41,
                node: "b".into(),
                topic: "rt".into(),
                payload: Some("r1".into()),
            },
        ]
    }

    #[test]
    fn a_clean_history_passes_and_roundtrips_jsonl() {
        let events = ok_history();
        assert!(check(&events).is_empty());
        let jsonl: String = events
            .iter()
            .map(|e| serde_json::to_string(e).unwrap() + "\n")
            .collect();
        assert_eq!(parse(&jsonl).unwrap(), events);
        assert!(
            parse("{\"ev\":\"martian\"}\n").is_err(),
            "unknown events fail"
        );
    }

    /// RED: each check must bite on the violation it exists for.
    #[test]
    fn each_check_bites() {
        // 1. Acked but never delivered.
        let mut h = ok_history();
        h.push(Event::Publish {
            at_ms: 50,
            publisher: "p1".into(),
            topic: "t".into(),
            payload: "lost".into(),
            acked: true,
            seq: 2,
        });
        let v = check(&h);
        assert!(
            v.iter().any(|v| v.check == "acked-durability"),
            "lost acked payload must be caught: {v:?}"
        );

        // 2. A resume that lies about the session.
        let mut h = ok_history();
        h.push(Event::Connect {
            at_ms: 60,
            client: "s1".into(),
            via_node: "b".into(),
            session_present: false, // the session WAS established
            clean_start: None,
            session_expiry_s: None,
            will: None,
            conn: None,
        });
        assert!(check(&h).iter().any(|v| v.check == "session-present"));

        // 3. A node stuck behind the last acked retained set.
        let mut h = ok_history();
        h.push(Event::RetainedSet {
            at_ms: 61,
            topic: "rt".into(),
            payload: "r2".into(),
            acked: true,
        });
        h.push(Event::RetainedProbe {
            at_ms: 70,
            node: "a".into(),
            topic: "rt".into(),
            payload: Some("r2".into()),
        });
        h.push(Event::RetainedProbe {
            at_ms: 71,
            node: "b".into(),
            topic: "rt".into(),
            payload: Some("r1".into()), // stale — the #214 shape
        });
        assert!(check(&h).iter().any(|v| v.check == "retained-convergence"));

        // 4. A reordered first delivery.
        let mut h = ok_history();
        h.push(Event::Publish {
            at_ms: 80,
            publisher: "p1".into(),
            topic: "t".into(),
            payload: "m2".into(),
            acked: true,
            seq: 2,
        });
        h.push(Event::Publish {
            at_ms: 81,
            publisher: "p1".into(),
            topic: "t".into(),
            payload: "m3".into(),
            acked: true,
            seq: 3,
        });
        h.push(Event::Deliver {
            at_ms: 90,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m3".into(),
            dup: false,
            qos: 1,
        });
        h.push(Event::Deliver {
            at_ms: 91,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m2".into(), // first delivery of seq 2 AFTER seq 3
            dup: false,
            qos: 1,
        });
        assert!(check(&h).iter().any(|v| v.check == "publisher-order"));

        // 5. A repeat delivery that does not say it is one.
        let mut h = ok_history();
        h.push(Event::Deliver {
            at_ms: 95,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m1".into(), // second delivery of m1, unmarked
            dup: false,
            qos: 1,
        });
        let v = check(&h);
        assert!(
            v.iter().any(|v| v.check == "redelivery-marked"),
            "an unmarked repeat must be caught: {v:?}"
        );
    }

    /// The redelivery check must stay silent on the three legal shapes that look
    /// like violations. Each is a false positive that would fire on ordinary
    /// runs, so these controls are what make the check usable rather than a
    /// source of noise someone eventually silences.
    #[test]
    fn the_redelivery_check_accepts_the_legal_shapes() {
        // Negative control A: the same repeat, correctly marked, is legal — this
        // is the ordinary resume-after-kill path, and a check that flagged it
        // would fail every honest run.
        let mut h = ok_history();
        h.push(Event::Deliver {
            at_ms: 95,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m1".into(),
            dup: true,
            qos: 1,
        });
        assert!(
            check(&h).is_empty(),
            "a MARKED repeat is at-least-once legal"
        );

        // Negative control B: two distinct publishes carrying identical bytes.
        // The second delivery is a FIRST delivery that merely looks like a
        // repeat, so DUP = 0 is correct and the check must stay silent. This is
        // the false positive the "published exactly once" guard exists for — the
        // warm-up path really does reuse payloads.
        let mut h = ok_history();
        h.push(Event::Publish {
            at_ms: 96,
            publisher: "p1".into(),
            topic: "t".into(),
            payload: "m1".into(), // same bytes as the publish in ok_history
            acked: true,
            seq: 2,
        });
        h.push(Event::Deliver {
            at_ms: 97,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m1".into(),
            dup: false,
            qos: 1,
        });
        assert!(
            check(&h).is_empty(),
            "identical bytes from two publishes are not a redelivery"
        );

        // Negative control C: QoS 0 has no redelivery concept, so a repeat there
        // carries no DUP promise.
        let mut h = ok_history();
        h.push(Event::Deliver {
            at_ms: 98,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m1".into(),
            dup: false,
            qos: 0,
        });
        assert!(check(&h).is_empty(), "QoS 0 repeats carry no DUP promise");
    }

    /// A history recorded before `dup`/`qos` existed must still parse and check.
    ///
    /// The nightly archives histories for 14 days and re-checks them with the
    /// standalone binary. `parse` refuses anything it cannot decode — by design,
    /// since a checker that skips what it does not understand can be silently
    /// blinded — so without `#[serde(default)]` this change would make every
    /// archived artifact unreadable. This test is that guarantee, stated as a
    /// literal pre-change line rather than as a round-trip of today's struct.
    #[test]
    fn a_history_recorded_before_dup_and_qos_still_parses() {
        let old = r#"{"ev":"deliver","at_ms":11,"client":"s1","topic":"t","payload":"m1"}"#;
        let events = parse(old).expect("a pre-change history must still parse");
        assert_eq!(
            events[0],
            Event::Deliver {
                at_ms: 11,
                client: "s1".into(),
                topic: "t".into(),
                payload: "m1".into(),
                dup: false,
                qos: 0,
            }
        );
        // qos defaults to 0, so the redelivery check skips archived deliveries
        // rather than reading a missing field as "QoS 1, not marked DUP" and
        // inventing violations in histories recorded before the flag existed.
        assert!(check(&events).is_empty());
    }

    // ---- checks 6 to 9 ---------------------------------------------------

    fn connect(at_ms: u64, client: &str, present: bool) -> Event {
        Event::Connect {
            at_ms,
            client: client.into(),
            via_node: "a".into(),
            session_present: present,
            clean_start: Some(false),
            session_expiry_s: Some(60),
            will: None,
            conn: None,
        }
    }

    fn with_will(ev: Event, payload: &str, delay_s: u32) -> Event {
        let Event::Connect {
            at_ms,
            client,
            via_node,
            session_present,
            clean_start,
            session_expiry_s,
            conn,
            ..
        } = ev
        else {
            unreachable!()
        };
        Event::Connect {
            at_ms,
            will: Some(Will {
                topic: format!("wills/{client}"),
                payload: payload.into(),
                qos: 1,
                delay_s,
            }),
            client,
            via_node,
            session_present,
            clean_start,
            session_expiry_s,
            conn,
        }
    }

    fn will_seen(at_ms: u64, payload: &str, dup: bool) -> Event {
        Event::Deliver {
            at_ms,
            client: "w".into(),
            topic: "wills/c".into(),
            payload: payload.into(),
            dup,
            qos: 1,
        }
    }

    fn lost(at_ms: u64, client: &str) -> Event {
        Event::ConnectionLost {
            at_ms,
            client: client.into(),
            why: "node killed".into(),
            conn: None,
        }
    }

    /// A watcher subscribed to every Will, and the history running on to `end`.
    fn will_history(mut middle: Vec<Event>, end: u64) -> Vec<Event> {
        let mut h = vec![Event::Subscribe {
            at_ms: 0,
            client: "w".into(),
            topic: "wills/#".into(),
        }];
        h.append(&mut middle);
        h.push(Event::Nemesis {
            at_ms: end,
            what: "end of schedule".into(),
        });
        h
    }

    fn checks_of(h: &[Event]) -> Vec<&'static str> {
        check(h).into_iter().map(|v| v.check).collect()
    }

    #[test]
    fn a_qos2_repeat_and_a_protocol_error_are_caught() {
        let mut h = ok_history();
        h.push(Event::Publish {
            at_ms: 50,
            publisher: "p1".into(),
            topic: "t".into(),
            payload: "q2".into(),
            acked: true,
            seq: 2,
        });
        for (at_ms, dup) in [(51, false), (52, true)] {
            h.push(Event::Deliver {
                at_ms,
                client: "s1".into(),
                topic: "t".into(),
                payload: "q2".into(),
                dup,
                qos: 2,
            });
        }
        assert_eq!(
            checks_of(&h),
            ["qos2-once"],
            "DUP does not excuse a QoS 2 repeat"
        );
        h.pop();
        assert!(check(&h).is_empty(), "one QoS 2 delivery is exactly once");

        h.push(Event::ProtocolError {
            at_ms: 60,
            client: "s1".into(),
            what: "PUBLISH reused packet id 7 while it was in flight".into(),
        });
        assert_eq!(checks_of(&h), ["protocol"]);
    }

    #[test]
    fn wills_that_are_owed_must_arrive_once_and_not_early() {
        // Lost connection, no delay: owed at once, never arrives.
        let h = will_history(
            vec![with_will(connect(1, "c", false), "w1", 0), lost(100, "c")],
            10_000,
        );
        assert_eq!(checks_of(&h), ["will"], "a missing Will is caught");
        // Arrives twice, unmarked.
        let h = will_history(
            vec![
                with_will(connect(1, "c", false), "w1", 0),
                lost(100, "c"),
                will_seen(150, "w1", false),
                will_seen(160, "w1", false),
            ],
            10_000,
        );
        assert_eq!(checks_of(&h), ["will"], "a Will published twice is caught");
        // A 10 s Will Delay ignored.
        let h = will_history(
            vec![
                with_will(connect(1, "c", false), "w1", 10),
                lost(100, "c"),
                will_seen(300, "w1", false),
            ],
            20_000,
        );
        assert_eq!(checks_of(&h), ["will"], "an early Will is caught");
    }

    #[test]
    fn wills_that_are_not_owed_must_not_arrive() {
        // DISCONNECT 0x00 deletes it.
        let h = will_history(
            vec![
                with_will(connect(1, "c", false), "w1", 0),
                Event::Disconnect {
                    at_ms: 100,
                    client: "c".into(),
                    reason: 0,
                    session_expiry_s: None,
                    conn: None,
                },
                will_seen(150, "w1", false),
            ],
            10_000,
        );
        assert_eq!(checks_of(&h), ["will"]);
        // Resumed inside a 5 s delay.
        let h = will_history(
            vec![
                with_will(connect(1, "c", false), "w1", 5),
                lost(100, "c"),
                connect(1_000, "c", true),
                will_seen(5_200, "w1", false),
            ],
            10_000,
        );
        assert_eq!(checks_of(&h), ["will"]);
        // Taken over inside an 8 s delay by a connection that resumes the session.
        let h = will_history(
            vec![
                with_will(connect(1, "c", false), "w1", 8),
                connect(200, "c", true),
                will_seen(210, "w1", false),
            ],
            20_000,
        );
        assert_eq!(
            checks_of(&h),
            ["will"],
            "the takeover inside the delay (#1033)"
        );
    }

    #[test]
    fn wills_follow_their_delay_session_and_takeover_rules() {
        let disconnect_with_will = Event::Disconnect {
            at_ms: 100,
            client: "c".into(),
            reason: 0x04,
            session_expiry_s: None,
            conn: None,
        };
        let mut clean = connect(1_000, "c", false);
        if let Event::Connect { clean_start, .. } = &mut clean {
            *clean_start = Some(true);
        }
        let mut short = with_will(connect(1, "c", false), "w1", 30);
        if let Event::Connect {
            session_expiry_s, ..
        } = &mut short
        {
            *session_expiry_s = Some(3);
        }
        for (what, h) in [
            (
                "after its 2 s delay",
                vec![
                    with_will(connect(1, "c", false), "w1", 2),
                    lost(100, "c"),
                    will_seen(2_200, "w1", false),
                ],
            ),
            (
                "DISCONNECT 0x04",
                vec![
                    with_will(connect(1, "c", false), "w1", 0),
                    disconnect_with_will,
                    will_seen(150, "w1", false),
                ],
            ),
            (
                "a clean start ends the session inside the delay",
                vec![
                    with_will(connect(1, "c", false), "w1", 30),
                    lost(100, "c"),
                    clean,
                    will_seen(1_050, "w1", false),
                ],
            ),
            (
                "the session expires before the delay",
                vec![short, lost(100, "c"), will_seen(3_150, "w1", false)],
            ),
            (
                "a takeover with no delay",
                vec![
                    with_will(connect(1, "c", false), "w1", 0),
                    connect(200, "c", true),
                    will_seen(250, "w1", false),
                ],
            ),
            (
                "a repeat marked DUP",
                vec![
                    with_will(connect(1, "c", false), "w1", 0),
                    lost(100, "c"),
                    will_seen(150, "w1", false),
                    will_seen(900, "w1", true),
                ],
            ),
            (
                "due too close to the end to judge",
                vec![with_will(connect(1, "c", false), "w1", 4), lost(100, "c")],
            ),
            (
                "resumed in time, never published",
                vec![
                    with_will(connect(1, "c", false), "w1", 5),
                    lost(100, "c"),
                    connect(1_000, "c", true),
                ],
            ),
        ] {
            let end = if what.starts_with("due too close") {
                4_500
            } else {
                40_000
            };
            let h = will_history(h, end);
            assert!(check(&h).is_empty(), "{what}: {:?}", check(&h));
        }
    }

    #[test]
    fn sessions_end_with_clean_start_and_their_expiry() {
        let disconnect = |at_ms, session_expiry_s| Event::Disconnect {
            at_ms,
            client: "c".into(),
            reason: 0,
            session_expiry_s,
            conn: None,
        };
        // Expiry 0 set at DISCONNECT replaces the CONNECT's 60 s: the session is gone.
        let h = vec![
            connect(1, "c", false),
            disconnect(100, Some(0)),
            connect(200, "c", true),
        ];
        assert_eq!(
            checks_of(&h),
            ["session-present"],
            "a DISCONNECT's expiry is honoured (round 9 #2)"
        );
        let h = vec![
            connect(1, "c", false),
            disconnect(100, Some(0)),
            connect(200, "c", false),
        ];
        assert!(check(&h).is_empty());
        // Within 60 s it exists; after it, it is gone; near the boundary nobody judges.
        let h = vec![
            connect(1, "c", false),
            lost(100, "c"),
            connect(5_000, "c", false),
        ];
        assert_eq!(
            checks_of(&h),
            ["session-present"],
            "an empty session must be kept (#1034)"
        );
        for (at, present) in [
            (5_000, true),
            (70_000, false),
            (60_500, true),
            (60_500, false),
        ] {
            let h = vec![
                connect(1, "c", false),
                lost(100, "c"),
                connect(at, "c", present),
            ];
            assert!(
                check(&h).is_empty(),
                "reconnect at {at} present={present}: {:?}",
                check(&h)
            );
        }
        // Clean start never answers present.
        let mut clean = connect(200, "c", true);
        if let Event::Connect { clean_start, .. } = &mut clean {
            *clean_start = Some(true);
        }
        let h = vec![connect(1, "c", false), lost(100, "c"), clean];
        assert_eq!(checks_of(&h), ["session-present"]);
    }

    #[test]
    fn a_connect_recorded_before_the_session_fields_still_parses() {
        let old =
            r#"{"ev":"connect","at_ms":1,"client":"s1","via_node":"a","session_present":false}"#;
        let events = parse(old).expect("a pre-change history must still parse");
        let Event::Connect {
            clean_start,
            session_expiry_s,
            will,
            ..
        } = &events[0]
        else {
            panic!("not a connect: {events:?}")
        };
        assert_eq!((clean_start, session_expiry_s, will), (&None, &None, &None));
        assert_eq!(
            serde_json::to_string(&events[0]).unwrap(),
            old,
            "and writes back unchanged"
        );
    }

    fn qos_will(mut ev: Event, qos: u8) -> Event {
        if let Event::Connect { will: Some(w), .. } = &mut ev {
            w.qos = qos;
        }
        ev
    }

    fn expiry(mut ev: Event, exp: u32) -> Event {
        if let Event::Connect {
            session_expiry_s, ..
        } = &mut ev
        {
            *session_expiry_s = Some(exp);
        }
        ev
    }

    fn conn_id(mut ev: Event, id: &str) -> Event {
        match &mut ev {
            Event::Connect { conn, .. } | Event::ConnectionLost { conn, .. } => {
                *conn = Some(id.into());
            }
            _ => {}
        }
        ev
    }

    #[test]
    fn a_takeover_of_an_expiry_0_session_ends_it_for_both_checks() {
        let h = will_history(
            vec![
                with_will(expiry(connect(1, "c", false), 0), "w1", 8),
                connect(200, "c", false),
                will_seen(250, "w1", false),
            ],
            20_000,
        );
        assert!(check(&h).is_empty(), "{:?}", check(&h));
        let mut h = h;
        h[2] = connect(200, "c", true);
        assert_eq!(checks_of(&h), ["session-present"]);
    }

    #[test]
    fn a_late_recorded_end_of_a_taken_over_connection_is_ignored() {
        let h = will_history(
            vec![
                conn_id(with_will(connect(1, "c", false), "w1", 8), "c#1"),
                conn_id(connect(200, "c", true), "c#2"),
                conn_id(lost(220, "c"), "c#1"),
                connect(30_000, "c", true),
            ],
            40_000,
        );
        assert!(check(&h).is_empty(), "{:?}", check(&h));
    }

    #[test]
    fn who_is_owed_a_will_and_how_late_it_may_be() {
        let watcher_away = |qos, exp| {
            will_history(
                vec![
                    expiry(connect(1, "w", false), exp),
                    lost(50, "w"),
                    qos_will(with_will(connect(1, "c", false), "w1", 0), qos),
                    lost(100, "c"),
                ],
                30_000,
            )
        };
        assert!(
            check(&watcher_away(0, 600)).is_empty(),
            "a QoS 0 Will skips an offline subscriber"
        );
        assert!(
            check(&watcher_away(1, 0)).is_empty(),
            "a subscriber with no session is owed nothing"
        );
        assert_eq!(
            checks_of(&watcher_away(1, 600)),
            ["will"],
            "an offline session keeps a QoS 1 Will"
        );
        let h = will_history(
            vec![
                with_will(connect(1, "c", false), "w1", 2),
                lost(100, "c"),
                will_seen(60_000, "w1", false),
            ],
            70_000,
        );
        assert_eq!(checks_of(&h), ["will"], "a Will a minute late is caught");
        let h = will_history(
            vec![
                qos_will(with_will(connect(1, "c", false), "w1", 0), 2),
                lost(100, "c"),
                will_seen(150, "w1", false),
                will_seen(900, "w1", true),
            ],
            10_000,
        );
        assert_eq!(
            checks_of(&h),
            ["will"],
            "DUP does not excuse a QoS 2 Will repeat"
        );
    }
}
