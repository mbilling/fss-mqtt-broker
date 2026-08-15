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
/// all five promises.
#[must_use]
pub fn check(events: &[Event]) -> Vec<Violation> {
    let mut v = Vec::new();
    v.extend(check_acked_durability(events));
    v.extend(check_session_present(events));
    v.extend(check_retained_convergence(events));
    v.extend(check_publisher_order(events));
    v.extend(check_redelivery_marked(events));
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
    // How many times each payload was published at all (acked or not: an unacked
    // publish may still have reached the broker and been delivered).
    let mut published: BTreeMap<&str, usize> = BTreeMap::new();
    for ev in events {
        if let Event::Publish { payload, .. } = ev {
            *published.entry(payload).or_default() += 1;
        }
    }

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
}
