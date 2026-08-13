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
    Deliver {
        at_ms: u64,
        client: String,
        topic: String,
        payload: String,
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

impl Event {
    fn at(&self) -> u64 {
        match self {
            Event::Connect { at_ms, .. }
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
/// all four promises.
#[must_use]
pub fn check(events: &[Event]) -> Vec<Violation> {
    let mut v = Vec::new();
    v.extend(check_acked_durability(events));
    v.extend(check_session_present(events));
    v.extend(check_retained_convergence(events));
    v.extend(check_publisher_order(events));
    v
}

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

/// Check 2: session-present honesty from the client's chair.
fn check_session_present(events: &[Event]) -> Vec<Violation> {
    let mut established: BTreeSet<&str> = BTreeSet::new();
    let mut v = Vec::new();
    for ev in events {
        let Event::Connect {
            at_ms,
            client,
            via_node,
            session_present,
        } = ev
        else {
            continue;
        };
        let expected = established.contains(client.as_str());
        if *session_present != expected {
            v.push(Violation {
                check: "session-present",
                detail: format!(
                    "{client} connected via {via_node} at t+{at_ms}ms: session_present \
                     was {session_present}, but the session {} established before",
                    if expected { "WAS" } else { "was NEVER" }
                ),
            });
        }
        established.insert(client);
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
        });
        h.push(Event::Deliver {
            at_ms: 91,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m2".into(), // first delivery of seq 2 AFTER seq 3
        });
        assert!(check(&h).iter().any(|v| v.check == "publisher-order"));

        // A duplicate re-delivery of an older seq is at-least-once legal.
        let mut h = ok_history();
        h.push(Event::Deliver {
            at_ms: 95,
            client: "s1".into(),
            topic: "t".into(),
            payload: "m1".into(), // dup of seq 1: fine
        });
        assert!(check(&h).is_empty());
    }
}
