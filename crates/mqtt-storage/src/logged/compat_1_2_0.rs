//! Issue #1118's rolling-upgrade guard (ADR 0039, ADR 0058 clause 2/3): the session
//! metadata a NEW build writes — now carrying the Session Expiry Interval tail — must
//! decode, field for field, under the decoder 1.2.0 ships, and a record 1.2.0 wrote must
//! decode here with the interval unknown.
//!
//! `decode_session_meta_v1_2_0` is a VERBATIM copy of `decode_session_meta` at the
//! `v1.2.0` release commit (`0b87f056…`, the `cluster_upgrade.rs` baseline), with only
//! the new struct field filled as `None`. Never edit it to follow the live decoder: it
//! stands for a binary that is already deployed.
use super::*;

fn decode_session_meta_v1_2_0(buf: &[u8]) -> Result<SessionMeta, StorageError> {
    let mut r = Reader::new(buf);
    let n = r.u32()? as usize;
    let mut subscriptions = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let filter = r.string()?;
        let max_qos = qos_from_u8(r.u8()?)?;
        let no_local = r.u8()? != 0;
        subscriptions.push(Subscription {
            filter,
            max_qos,
            no_local,
            // Filled from the sub-id tail below, when one was recorded (issue #266).
            sub_id: None,
        });
    }
    let rn = r.u32()? as usize;
    let mut received_qos2 = BTreeSet::new();
    for _ in 0..rn {
        received_qos2.insert(r.u16()?);
    }
    let last_packet_id = r.u16()?;
    // Backward-compatible: a record written before ADR 0009 phase 3 ends here, so an empty
    // remainder means "no expiry deadline".
    let session_expiry_at = if r.is_empty() {
        None
    } else if r.u8()? == 1 {
        Some(r.u64()?)
    } else {
        None
    };
    // Backward-compatible (ADR 0031): a record written before owner binding ends here, so an
    // empty remainder means "no recorded owner".
    let owner = if r.is_empty() {
        None
    } else if r.u8()? == 1 {
        Some(r.string()?)
    } else {
        None
    };
    // Backward-compatible (ADR 0057): a record written before the outbound window ends
    // here, decoding as empty — the pre-0057 behaviour.
    let mut outbound_qos2 = std::collections::BTreeMap::new();
    if !r.is_empty() {
        let on = r.u32()? as usize;
        for _ in 0..on {
            let id = r.u16()?;
            let offset = r.u64()?;
            let pubrec_seen = r.u8()? != 0;
            outbound_qos2.insert(id, (offset, pubrec_seen));
        }
    }
    // Backward-compatible (issue #238): a record written before the acked bit — or one
    // whose session has nothing unacknowledged — ends here, and every held id decodes as
    // ACKED, which is the pre-#238 behaviour exactly.
    let mut unacked_received_qos2 = BTreeSet::new();
    if !r.is_empty() {
        let un = r.u32()? as usize;
        for _ in 0..un {
            unacked_received_qos2.insert(r.u16()?);
        }
    }
    // The sub-id tail (issue #266): present only when a subscription carried one;
    // the forced-present unacked block above keeps the read order deterministic.
    if !r.is_empty() {
        let sn = r.u32()? as usize;
        for _ in 0..sn {
            let f = r.string()?;
            let id = r.u32()?;
            if let Some(sub) = subscriptions.iter_mut().find(|s| s.filter == f) {
                sub.sub_id = Some(id);
            }
        }
    }
    Ok(SessionMeta {
        subscriptions,
        received_qos2,
        last_packet_id,
        session_expiry_at,
        owner,
        outbound_qos2,
        unacked_received_qos2,
        session_expiry_interval: None,
    })
}

fn sub(filter: &str, sub_id: Option<u32>) -> Subscription {
    Subscription {
        filter: filter.into(),
        max_qos: QoS::AtLeastOnce,
        no_local: false,
        sub_id,
    }
}

/// Every shape of the tails the interval rides behind: none of the conditional blocks,
/// an unacked id, a subscription id, and both.
fn metas(interval: Option<u32>) -> Vec<SessionMeta> {
    let mut out = Vec::new();
    for (unacked, sub_id) in [(false, false), (true, false), (false, true), (true, true)] {
        out.push(SessionMeta {
            subscriptions: vec![sub("a/#", None), sub("b/c", sub_id.then_some(7))],
            received_qos2: [3u16, 9].into_iter().collect(),
            last_packet_id: 41,
            session_expiry_at: None,
            owner: Some("alice".into()),
            outbound_qos2: [(5u16, (77u64, false))].into_iter().collect(),
            unacked_received_qos2: if unacked {
                [9u16].into_iter().collect()
            } else {
                BTreeSet::new()
            },
            session_expiry_interval: interval,
        });
    }
    out
}

fn fields(m: &SessionMeta) -> impl PartialEq + std::fmt::Debug {
    (
        m.subscriptions
            .iter()
            .map(|s| (s.filter.clone(), s.max_qos as u8, s.no_local, s.sub_id))
            .collect::<Vec<_>>(),
        m.received_qos2.clone(),
        m.last_packet_id,
        m.session_expiry_at,
        m.owner.clone(),
        m.outbound_qos2.clone(),
        m.unacked_received_qos2.clone(),
    )
}

/// New → 1.2.0: a record carrying the interval decodes under the 1.2.0 decoder with every
/// field it knows intact, and the interval simply unread.
#[test]
fn a_record_with_the_expiry_interval_decodes_under_1_2_0() {
    for interval in [30, u32::MAX] {
        for meta in metas(Some(interval)) {
            let bytes = encode_session_meta(&meta);
            let old = decode_session_meta_v1_2_0(&bytes).expect("1.2.0 decodes it");
            assert_eq!(fields(&old), fields(&meta));
            let new = decode_session_meta(&bytes).unwrap();
            assert_eq!(fields(&new), fields(&meta));
            assert_eq!(new.session_expiry_interval, Some(interval));
        }
    }
}

/// 1.2.0 → new, and the downgrade's read-modify-write: a record without the tail —
/// what 1.2.0 writes, byte for byte, because the tail is the only thing added — reads
/// with the interval unknown, and 1.2.0 rewriting a new record drops the interval and
/// nothing else.
#[test]
fn a_1_2_0_record_reads_with_the_interval_unknown() {
    for (without, with) in metas(None).into_iter().zip(metas(Some(30))) {
        let old_bytes = encode_session_meta(&without);
        let new_bytes = encode_session_meta(&with);
        assert_eq!(
            &new_bytes[new_bytes.len() - 4..],
            &30u32.to_be_bytes(),
            "the interval is the last four bytes"
        );
        let read = decode_session_meta(&old_bytes).unwrap();
        assert_eq!(read.session_expiry_interval, None);
        assert_eq!(fields(&read), fields(&without));
        // 1.2.0 reads the new record and writes it back (its encoder = today's with no
        // interval, since the tail is the only difference).
        let rewritten = encode_session_meta(&decode_session_meta_v1_2_0(&new_bytes).unwrap());
        let back = decode_session_meta(&rewritten).unwrap();
        assert_eq!(back.session_expiry_interval, None);
        assert_eq!(fields(&back), fields(&with));
    }
}

/// A record with no interval encodes exactly the bytes it always did: the forced blocks
/// appear only when the interval follows them.
#[test]
fn a_record_without_the_interval_is_byte_identical_to_1_2_0() {
    let meta = SessionMeta {
        subscriptions: vec![sub("t", None)],
        last_packet_id: 2,
        ..SessionMeta::default()
    };
    let mut expected = Vec::new();
    expected.extend_from_slice(&1u32.to_be_bytes()); // one subscription
    expected.extend_from_slice(&1u32.to_be_bytes());
    expected.push(b't');
    expected.extend_from_slice(&[1, 0]); // max QoS 1, no_local false
    expected.extend_from_slice(&0u32.to_be_bytes()); // no received ids
    expected.extend_from_slice(&2u16.to_be_bytes()); // last packet id
    expected.push(0); // no deadline
    expected.push(0); // no owner
    expected.extend_from_slice(&0u32.to_be_bytes()); // empty outbound window
    assert_eq!(encode_session_meta(&meta), expected);
    let with = SessionMeta {
        session_expiry_interval: Some(5),
        ..meta
    };
    let mut forced = expected.clone();
    forced.extend_from_slice(&0u32.to_be_bytes()); // forced: no unacked ids
    forced.extend_from_slice(&0u32.to_be_bytes()); // forced: no subscription ids
    forced.extend_from_slice(&5u32.to_be_bytes());
    assert_eq!(encode_session_meta(&with), forced);
}

/// The store end to end: the interval written at connect survives a new store over the
/// same log (a restart, a takeover) and reaches the session scan, beside a deadline that
/// is absent; a detach's write keeps it with the deadline.
#[tokio::test]
async fn the_expiry_interval_reaches_the_session_scan() {
    use crate::repl::InMemoryReplicatedLog;
    let log = Arc::new(InMemoryReplicatedLog::new());
    let c = ClientId("crashed".into());
    let s = ReplicatedSessionStore::new(log.clone());
    s.ensure_session(&c).await.unwrap();
    s.set_subscriptions(&c, &[sub("t/#", None)]).await.unwrap();
    s.set_session_lifetime(&c, 30, None).await.unwrap();

    let reborn = ReplicatedSessionStore::new(log.clone());
    let scan = reborn.all_sessions().await.unwrap();
    assert_eq!(scan.sessions.len(), 1);
    let got = &scan.sessions[0];
    assert_eq!(got.expiry_at, None);
    assert_eq!(got.expiry_interval, Some(30));
    assert_eq!(
        got.subscriptions.len(),
        1,
        "subscriptions survive the write"
    );

    reborn
        .set_session_lifetime(&c, 30, Some(1_800_000_000))
        .await
        .unwrap();
    // A deadline-only write (backup restore) leaves the interval alone.
    reborn
        .set_session_expiry(&c, Some(1_800_000_001))
        .await
        .unwrap();
    let got = &reborn.all_sessions().await.unwrap().sessions[0];
    assert_eq!(got.expiry_at, Some(1_800_000_001));
    assert_eq!(got.expiry_interval, Some(30));
}
