//! Shared-subscription LOCALITY, proved by an exact delivery-sequence oracle
//! (issue #613 wave D: items 3.4 and 3.5, plus the two paired invariants).
//!
//! Every test here drives a REAL `Hub` — the same `dispatch` the command loop
//! calls — and reads the receipt each message actually produced: a `Publish`
//! packet on a local member's outbound, or a `PeerMessage::SharedDeliver` on the
//! peer link for a remote one. Nothing is timed, nothing sleeps, and no
//! production predicate is consulted to decide what the answer should be.
//!
//! **The oracle is re-implemented here on purpose.** [`predict`] below never
//! calls `Hub::prefer_local_now` or `Hub::next_stored_cursor`; it restates the
//! *specified* schedule from `MQTTD_SHARED_LOCAL_BIAS`'s documented contract, so
//! a change to either of those functions is a disagreement the test can see.
//! Calling the production rule would make every assertion here vacuous.
//!
//! **What is NOT here, and why.** The brief asked for wave C/D proofs of items
//! 3.1 (re-plan a bound-limited `QoS` >= 1 shared target onto another member) and
//! 3.2 (local pressure degrades prefer-local to the global rotation). Neither
//! landed in `hub/delivery.rs`: `deliver_shared` still gates its re-plan on
//! `qos == QoS::AtMostOnce`, and `prefer_local_now` still reads only the knob.
//! `hub/pressure.rs` was DELETED rather than left as scaffolding: its
//! `Hub::pressure` read `self.rx.len()` at call time, which is the very
//! peek/commit divergence item 3.2 was sent back for, so it was scaffolding of
//! the wrong shape. A copy is preserved outside the tree for whoever lands 3.2
//! properly. Tests for 3.1/3.2 would certify code that does not exist, so they
//! are not written — but the peek/commit GUARD below is, because it is what
//! fails the day selection starts reading a quantity other tasks can move.
//!
//! What IS written for that pair is the invariant CORRECTION 2 exists to protect:
//! [`the_peek_and_the_commit_choose_the_same_member_however_loaded_the_hub_is`]
//! fails the moment shared selection starts consulting the inbound queue depth,
//! which is precisely how the rejected 3.2 design would have broken the
//! effect-free-refusal property.

use super::*;

/// The one local group member. Its receipts arrive as `Publish` packets.
const LOCAL: &str = "local-a";
/// The one remote group member, on peer `peer-1`. Its receipts arrive as
/// `PeerMessage::SharedDeliver` frames on that link.
const REMOTE: &str = "remote-a";
const PEER: &str = "peer-1";
const GROUP: &str = "g";
const TOPIC: &str = "t";

/// Per mille, exactly as `MQTTD_SHARED_LOCAL_BIAS` is specified. Restated here
/// rather than imported so a change to the production constant is a visible
/// disagreement instead of a silently-tracking one.
const DEN: usize = 1000;

/// One publish's destination, as the test names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Where {
    Local,
    Remote,
}

/// THE ORACLE. The delivery destination of each of `n` consecutive messages to a
/// group with exactly one online local member and exactly one online remote
/// member, at local bias `permille`.
///
/// Derived from the knob's specified contract, not from the implementation:
///
/// * the group's cursor starts at 0 and advances by one per message;
/// * the message takes the LOCAL preference when `cursor % 1000 < permille`,
///   with `permille == 0` never preferring and `permille >= 1000` always
///   preferring (the two degenerate settings the knob promises reproduce
///   `MQTTD_SHARED_PREFER_LOCAL` off and on);
/// * a preferred message goes to the local member, and the cursor is stored
///   modulo (online local members x the bias denominator);
/// * an unpreferred message falls through to the SAME global round robin the
///   knob-off path uses — candidates are locals first, then remotes, so index
///   `cursor % 2` is the local member on even cursors and the remote one on odd
///   — and the cursor is stored modulo (all candidates x the denominator);
/// * at a degenerate bias there is no phase to keep, so the denominator is 1 and
///   the stored cursor is today's `next % count`.
///
/// The last clause is what makes the two degenerate cases assertable as
/// "byte-for-byte today's behaviour" rather than as a coincidence.
fn predict(permille: u16, n: usize) -> Vec<Where> {
    let permille = usize::from(permille);
    let den = if permille == 0 || permille >= DEN {
        1
    } else {
        DEN
    };
    let mut cursor = 0usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let prefer_local = permille != 0 && (permille >= DEN || cursor % DEN < permille);
        if prefer_local {
            out.push(Where::Local);
            // One online local member.
            cursor = (cursor + 1) % den.max(1);
        } else {
            // Two candidates: the local member at index 0, the remote at index 1.
            out.push(if cursor.is_multiple_of(2) {
                Where::Local
            } else {
                Where::Remote
            });
            cursor = (cursor + 1) % (2 * den);
        }
    }
    out
}

/// THE TEST IS THE LOOP. A clean-start `Attach` wipes the in-memory session and
/// then discards the DURABLE one off the loop (ADR 0017), answering the CONNACK
/// only when the resulting `SessionRecovered` comes back round the hub's own
/// queue. A hub driven through `dispatch` with no spawned `run()` therefore has
/// to run that continuation itself, or the reply never arrives.
///
/// The reply is checked BEFORE each blocking `recv`, so this cannot wait on a
/// command that is never going to be sent.
async fn drive_until<T>(hub: &mut Hub, mut wait: oneshot::Receiver<T>) -> T {
    loop {
        match wait.try_recv() {
            Ok(answer) => return answer,
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(e) => panic!("the hub dropped the reply: {e}"),
        }
        let cmd = hub
            .rx
            .recv()
            .await
            .expect("the test holds a sender, so the hub's queue stays open");
        hub.dispatch(cmd).await;
    }
}

/// A hub driven directly, with no spawned `run()`: `dispatch` is awaited to
/// completion per command, so every assertion reads a settled state and the test
/// needs no barrier at all. It also keeps `hub` in the test's hands, which is
/// what lets the peek/commit test read the hub's inbound queue depth and the
/// internal selection plan.
struct Rig {
    hub: Hub,
    tx: mpsc::UnboundedSender<HubCommand>,
    local: mpsc::UnboundedReceiver<Box<Packet>>,
    peer: mpsc::UnboundedReceiver<PeerMessage>,
    metrics: Arc<mqtt_observability::metrics::Metrics>,
    next_seq: u64,
}

impl Rig {
    /// One local online member and one remote online member of `$share/g/t`.
    async fn new(prefer_local: bool, permille: u16) -> Self {
        let (mut hub, tx) = Hub::with_config(
            NodeId("locality".into()),
            Arc::new(MemorySessionStore::new()),
        );
        hub.set_shared_prefer_local(prefer_local);
        hub.set_shared_local_bias_permille(permille);
        let metrics = Arc::new(mqtt_observability::metrics::Metrics::new("locality"));
        hub.attach_metrics(metrics.clone());

        // The local member.
        let (out_tx, local) = mpsc::unbounded_channel();
        let (outbound, _meter) = Outbound::new(out_tx);
        let (reply, wait) = oneshot::channel();
        hub.dispatch(HubCommand::Attach {
            client: ClientId(LOCAL.into()),
            admission: admission(LOCAL),
            conn_id: 1,
            clean_start: true,
            session_expiry: 0,
            receive_maximum: u16::MAX,
            will: None,
            outbound,
            reply,
        })
        .await;
        assert!(matches!(
            drive_until(&mut hub, wait).await,
            AttachOutcome::Present(false)
        ));
        hub.dispatch(HubCommand::Subscribe {
            client: ClientId(LOCAL.into()),
            filters: vec![(format!("$share/{GROUP}/{TOPIC}"), QoS::AtMostOnce)],
            sub_id: None,
            no_local_filters: Vec::new(),
            rap_filters: Vec::new(),
            retain_handling: Vec::new(),
            reply: None,
        })
        .await;

        // The remote member, on a linked peer so the chosen frame has somewhere
        // to go (`plan_shared` reads the gossiped liveness; `deliver_shared`
        // needs the link).
        let (peer_tx, peer): (PeerOutbound, _) = mpsc::unbounded_channel();
        hub.dispatch(HubCommand::PeerConnected {
            node: NodeId(PEER.into()),
            conn_id: 1,
            ctl: peer_tx.clone(),
            tx: peer_tx,
            cert_serial: None,
            depth: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            proto: mqtt_cluster::peer::PROTO_MAX,
        })
        .await;
        hub.dispatch(HubCommand::RemoteSharedInterest {
            node: NodeId(PEER.into()),
            groups: vec![RemoteSharedGroup {
                group: GROUP.into(),
                filter: TOPIC.into(),
                members: vec![(ClientId(REMOTE.into()), QoS::AtMostOnce, true)],
            }],
        })
        .await;

        Self {
            hub,
            tx,
            local,
            peer,
            metrics,
            next_seq: 0,
        }
    }

    /// Publish one `QoS` 0 message whose payload IS its sequence number, so a
    /// receipt read from either side names the message it answers.
    async fn publish_one(&mut self) -> u64 {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.hub
            .dispatch(HubCommand::Publish {
                topic: TOPIC.into(),
                payload: Bytes::copy_from_slice(&seq.to_be_bytes()),
                qos: QoS::AtMostOnce,
                retain: false,
                message_expiry: None,
                app: AppProperties::default(),
                done: None,
                v5: false,
                publisher: None,
            })
            .await;
        seq
    }

    /// Every receipt produced so far, as `(sequence, destination)`, drained from
    /// BOTH sides. This is the exactly-one-member evidence: the caller asserts
    /// the multiset of sequences is exactly `0..n`, so a message delivered twice
    /// (to two members, or to one member twice) is a duplicate the length and
    /// the per-sequence check both catch.
    fn receipts(&mut self) -> Vec<(u64, Where)> {
        let mut out = Vec::new();
        while let Ok(packet) = self.local.try_recv() {
            let Packet::Publish(p) = *packet else {
                panic!("a local shared member receives PUBLISH and nothing else");
            };
            assert_eq!(p.qos, QoS::AtMostOnce);
            assert!(!p.retain, "shared delivery clears RETAIN (#198)");
            out.push((seq_of_bytes(&p.payload), Where::Local));
        }
        while let Ok(frame) = self.peer.try_recv() {
            // Interest gossip rides the same link; only deliveries are receipts.
            if let PeerMessage::SharedDeliver {
                client, payload, ..
            } = frame
            {
                assert_eq!(
                    client, REMOTE,
                    "the only remote member of this group is {REMOTE}"
                );
                out.push((seq_of_bytes(&payload), Where::Remote));
            }
        }
        out.sort_unstable_by_key(|(seq, _)| *seq);
        out
    }

    fn shared_selected(&self, locality: &str) -> u64 {
        let needle = format!("mqttd_shared_selected_total{{locality=\"{locality}\"}} ");
        self.metrics
            .render()
            .lines()
            .find_map(|l| l.strip_prefix(&needle))
            .and_then(|n| n.trim().parse().ok())
            .unwrap_or(0)
    }
}

fn seq_of_bytes(payload: &[u8]) -> u64 {
    u64::from_be_bytes(payload.try_into().expect("an 8-byte sequence payload"))
}

/// Publish `n` messages and return the destination of each, in order, with the
/// EXACTLY-ONE-MEMBER invariant asserted along the way.
async fn observed(rig: &mut Rig, n: usize) -> Vec<Where> {
    let mut seen: Vec<Option<Where>> = vec![None; n];
    for _ in 0..n {
        let seq = rig.publish_one().await;
        let receipts = rig.receipts();
        assert_eq!(
            receipts.len(),
            1,
            "EXACTLY ONE member of a shared group receives each message; \
             message {seq} produced {receipts:?}"
        );
        assert_eq!(receipts[0].0, seq, "a receipt for a message not just sent");
        seen[usize::try_from(seq).unwrap()] = Some(receipts[0].1);
    }
    seen.into_iter()
        .map(|w| w.expect("every message was delivered"))
        .collect()
}

/// ITEM 3.4, THE HEADLINE. At a NON-DEGENERATE bias the achieved schedule is the
/// specified one, message for message.
///
/// 1005 messages is chosen to cross all three regimes of a 250-per-mille bias:
/// the leading preferred window (cursors 0..250), the fall-through to the global
/// rotation (250..1000), and the wrap back into the window at cursor 1000 — the
/// place a cursor stored at the wrong WIDTH would first disagree.
///
/// The assertion is the sequence, not a statistic, because the schedule contains
/// no RNG and no clock: any "roughly a quarter" assertion would pass against an
/// implementation that picked at random with the right mean.
#[tokio::test]
async fn a_fractional_bias_delivers_the_exact_specified_local_schedule() {
    const N: usize = 1005;
    let mut rig = Rig::new(true, 250).await;
    let got = observed(&mut rig, N).await;
    let want = predict(250, N);
    assert_eq!(
        first_disagreement(&got, &want),
        None,
        "the achieved locality schedule must match the specified one message for message"
    );
    assert_eq!(got, want);
    // Stated so the next reader does not mistake the knob for the achieved
    // fraction: the complement of the bias window falls through to the global
    // rotation, which itself lands locally half the time. A 250-per-mille bias
    // therefore achieves MORE than 250 local deliveries per 1000, by exactly the
    // amount the oracle above predicts.
    let local = got.iter().filter(|w| **w == Where::Local).count();
    assert!(
        local > N / 4,
        "the bias is a floor on locality, not the achieved fraction (got {local}/{N})"
    );
}

/// The first index at which two schedules differ, with enough context to read the
/// failure without a debugger.
fn first_disagreement(got: &[Where], want: &[Where]) -> Option<String> {
    got.iter().zip(want).position(|(g, w)| g != w).map(|i| {
        let lo = i.saturating_sub(3);
        let hi = (i + 4).min(got.len());
        format!(
            "message {i}: got {:?}, want {:?} (got[{lo}..{hi}] = {:?}, want[{lo}..{hi}] = {:?})",
            got[i],
            want[i],
            &got[lo..hi],
            &want[lo..hi]
        )
    })
}

/// ITEM 3.4, DEGENERATE CASE 1. Bias 1000 — the default, and what
/// `MQTTD_SHARED_PREFER_LOCAL=1` has always meant — must be byte-for-byte
/// today's behaviour: the local member takes every message while it is online.
#[tokio::test]
async fn bias_1000_reproduces_todays_prefer_local() {
    const N: usize = 64;
    let mut rig = Rig::new(true, 1000).await;
    let got = observed(&mut rig, N).await;
    assert_eq!(got, vec![Where::Local; N]);
    assert_eq!(got, predict(1000, N));
}

/// ITEM 3.4, DEGENERATE CASE 2. Bias 0 must reproduce `MQTTD_SHARED_PREFER_LOCAL=0`
/// exactly — the plain global round robin — and it is asserted against the OTHER
/// hub rather than against the oracle alone, so "the knob folds in as bias 0"
/// is proved by the two configurations agreeing message for message.
#[tokio::test]
async fn bias_0_reproduces_prefer_local_off() {
    const N: usize = 64;
    let mut biased = Rig::new(true, 0).await;
    let with_bias_zero = observed(&mut biased, N).await;

    let mut knob_off = Rig::new(false, 1000).await;
    let with_knob_off = observed(&mut knob_off, N).await;

    assert_eq!(
        with_bias_zero, with_knob_off,
        "bias 0 and prefer-local off are the same schedule, in one place"
    );
    assert_eq!(with_bias_zero, predict(0, N));
    // And it really is a rotation, not an accident of everything going local.
    assert!(with_bias_zero.contains(&Where::Remote));
}

/// THE PAIRED INVARIANT, over the whole bias matrix: EXACTLY ONE member of a
/// group receives each message.
///
/// Without this, item 3.4's schedule tests would still pass if the fall-through
/// from the local preference to the global rotation delivered the message TWICE
/// — once on each path — because both destinations would contain the expected
/// one. `observed` asserts it per message; this test is what makes the
/// obligation cover every setting rather than the two the schedule tests use.
///
/// The pressure dimension the brief asked for is absent because item 3.2 is not
/// implemented (see this file's header): there is no pressure input to shared
/// selection to vary.
#[tokio::test]
async fn exactly_one_member_receives_each_message_at_every_bias() {
    for (prefer_local, permille) in [
        (false, 0),
        (false, 1000),
        (true, 0),
        (true, 1),
        (true, 250),
        (true, 500),
        (true, 999),
        (true, 1000),
    ] {
        let mut rig = Rig::new(prefer_local, permille).await;
        // `observed` panics on any message with zero or two receipts.
        let got = observed(&mut rig, 48).await;
        assert_eq!(got.len(), 48);
        let want = predict(if prefer_local { permille } else { 0 }, 48);
        assert_eq!(
            got, want,
            "prefer_local={prefer_local} permille={permille}: \
             the knob folds into the bias in exactly one place"
        );
    }
}

/// ITEM 3.5. The locality counter moves EXACTLY ONCE per PLACED message.
///
/// The obvious failure is double counting across the two commit sites — the
/// local arm and the remote arm of `deliver_shared`'s `match chosen.node` — so
/// the assertion is on the TOTAL as well as on each label, and it is checked at
/// a bias that exercises both arms.
#[tokio::test]
async fn the_locality_counter_moves_exactly_once_per_placed_message() {
    const N: usize = 300;
    let mut rig = Rig::new(true, 250).await;
    assert_eq!(rig.shared_selected("local"), 0);
    assert_eq!(rig.shared_selected("remote"), 0);

    let got = observed(&mut rig, N).await;
    let local = got.iter().filter(|w| **w == Where::Local).count();
    let remote = got.len() - local;
    assert!(local > 0 && remote > 0, "both arms must have run");

    assert_eq!(
        rig.shared_selected("local"),
        u64::try_from(local).unwrap(),
        "one local count per locally-placed message, no more and no fewer"
    );
    assert_eq!(
        rig.shared_selected("remote"),
        u64::try_from(remote).unwrap(),
        "one remote count per remotely-placed message"
    );
    assert_eq!(
        rig.shared_selected("local") + rig.shared_selected("remote"),
        u64::try_from(N).unwrap(),
        "the two arms are one match on one `chosen`: exactly one fires per placed message"
    );
}

/// ITEM 3.5, the other half: the counter is moved at COMMIT, never at PEEK.
///
/// `shared_plan_owes_durable` (hub/policy.rs) runs `plan_shared` as a pure
/// brownout peek that deliberately consumes no member's turn. Counting the
/// locality there instead of in `deliver_shared`'s two commit arms would count
/// messages that were never selected at all — and would do it once per publish
/// that merely asked whether it would be refused, which is exactly the kind of
/// inflation an operator would read as a locality regression.
///
/// So: peek a hundred times, assert the counter has not moved; then commit once,
/// and assert it moved by exactly one.
#[tokio::test]
async fn the_locality_counter_is_moved_at_commit_and_never_at_peek() {
    let mut rig = Rig::new(true, 1000).await;
    for _ in 0..100 {
        assert!(!rig.hub.shared_plan_owes_durable(TOPIC, QoS::AtLeastOnce));
    }
    assert_eq!(
        rig.shared_selected("local") + rig.shared_selected("remote"),
        0,
        "a peek that consumes no turn must count no placement"
    );

    let _ = observed(&mut rig, 1).await;
    assert_eq!(
        rig.shared_selected("local") + rig.shared_selected("remote"),
        1,
        "one commit, one count"
    );
}

/// THE PEEK/COMMIT AGREEMENT — the invariant CORRECTION 2 exists to protect, and
/// the reason items 3.1 and 3.2 were sent back for redesign.
///
/// `shared_plan_owes_durable` (hub/policy.rs) PEEKS the selection to decide the
/// brownout refusal; `publish` then COMMITS it through `deliver_shared`. A
/// refusal is only effect-free if those two agree: if the peek says "no local
/// member owes a durable append, do not refuse" and the commit then picks a
/// DIFFERENT member that does, the refusal fires after the retained mutation and
/// the live fan-out have already run.
///
/// The rejected item 3.2 would have made selection read `self.rx.len()` — written
/// by other tasks, and re-read across the awaits between the peek and the commit.
/// So this test does exactly what that would have exploited: it peeks, then
/// drives the hub's inbound queue from empty to deep, then commits, and asserts
/// the same member was chosen and the same refusal decision reached.
///
/// The load is asserted as a RAW queue depth rather than through a quantiser.
/// `hub/pressure.rs` is gone, and depending on its thresholds would have made
/// this guard only as strong as one particular mapping of depth to level — when
/// what it actually guards is that selection ignores the depth at all.
///
/// It is a GUARD, not a regression: it passes on today's tree, and its job is to
/// fail the day selection starts consulting a quantity other tasks can move.
/// Deep enough that no plausible saturation threshold sits above it, so this
/// guard keeps its meaning if anyone later reintroduces a pressure signal with
/// thresholds of their own choosing.
const QUEUE_LOAD: usize = 16_384;

#[tokio::test]
async fn the_peek_and_the_commit_choose_the_same_member_however_loaded_the_hub_is() {
    for permille in [0u16, 1, 250, 999, 1000] {
        let mut rig = Rig::new(true, permille).await;
        // Walk the cursor into the middle of the bias window's complement, so the
        // group is sitting on the global-rotation branch rather than the
        // constant-time local one — the branch a pressure input would have
        // switched between.
        let _ = observed(&mut rig, 3).await;

        // THE PEEK, twice: the plan `shared_plan_owes_durable` reads, and the
        // refusal answer itself.
        let peeked = rig
            .hub
            .plan_shared(TOPIC)
            .into_iter()
            .next()
            .expect("the group matches")
            .chosen
            .expect("a member is choosable")
            .client;
        let owes_idle = rig.hub.shared_plan_owes_durable(TOPIC, QoS::AtLeastOnce);
        assert_eq!(rig.hub.rx.len(), 0, "the hub starts idle");

        // Change the varying input, hard: other tasks pile work onto the hub's
        // inbound queue between the peek and the commit. This is the only
        // quantity in the rejected design that moves without the hub running.
        for _ in 0..QUEUE_LOAD {
            let (reply, _drop) = oneshot::channel();
            rig.tx.send(HubCommand::Ping { reply }).unwrap();
        }
        assert_eq!(
            rig.hub.rx.len(),
            QUEUE_LOAD,
            "the test must actually move the input it claims selection ignores"
        );

        // THE PEEK AGAIN, under load: same answer, or a refusal could be decided
        // against a different member than the one about to be committed.
        assert_eq!(
            rig.hub.shared_plan_owes_durable(TOPIC, QoS::AtLeastOnce),
            owes_idle,
            "permille={permille}: the brownout peek must not depend on the inbound queue"
        );

        // THE COMMIT.
        let seq = rig.publish_one().await;
        let receipts = rig.receipts();
        assert_eq!(receipts.len(), 1, "permille={permille}: still exactly one");
        assert_eq!(receipts[0].0, seq);
        let committed = match receipts[0].1 {
            Where::Local => LOCAL,
            Where::Remote => REMOTE,
        };
        assert_eq!(
            &*peeked.0, committed,
            "permille={permille}: the member the peek refused (or allowed) against \
             must be the member the commit delivers to — otherwise a refusal fires \
             after the fan-out it was supposed to prevent"
        );
    }
}
