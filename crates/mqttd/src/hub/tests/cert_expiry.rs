//! The expiry sweep ([ADR 0088](../../../../../docs/adr/0088-credential-lifetimes-and-rotation.md)
//! D3, D4, O2): a peer link either of whose handshake certificates has passed `notAfter`
//! is ended as a LOST link — the peer stays a member whose link is down, with what it
//! announced kept (ADR 0087) — and, with `tls.end_sessions_at_cert_expiry`, so is a client
//! session whose certificate has.
//!
//! Driven without the hub's actor and under a clock the test names: the sweep is handed
//! `now` in Unix seconds, so the last second of a window and the first second after it
//! are both asserted exactly.

use super::settle_gate::quiesce;
use super::*;
use crate::hub::{LinkCertEnds, Outgoing, RETIRE_TICKS};

fn peer() -> NodeId {
    NodeId("ce-peer".into())
}

/// A hub with one link to [`peer`], admitted under certificates ending at `ends`; the
/// peer has announced interest in `ce/#`. Returns the hub and the link's outbound lane.
async fn linked(ends: LinkCertEnds) -> (Hub, mpsc::UnboundedReceiver<PeerMessage>) {
    let (mut hub, _) = Hub::with_config(
        NodeId("ce-local".into()),
        Arc::new(MemorySessionStore::new()),
    );
    let (tx, rx) = mpsc::unbounded_channel();
    hub.dispatch(HubCommand::PeerConnected {
        node: peer(),
        conn_id: 1,
        ctl: tx.clone(),
        tx,
        cert_serial: Some(vec![0x01]),
        cert_ends: ends,
        proto: mqtt_cluster::peer::PROTO_MAX,
        depth: Arc::default(),
    })
    .await;
    quiesce(&mut hub).await;
    hub.peer_announced_interest(&peer(), vec!["ce/#".into()]);
    (hub, rx)
}

#[test]
fn a_link_ends_at_the_earlier_of_its_two_certificates_and_names_whose() {
    let ends = |peer, own| LinkCertEnds { peer, own };
    assert_eq!(ends(Some(100), Some(200)).earliest(), Some(100));
    assert_eq!(ends(Some(300), Some(200)).earliest(), Some(200));
    assert_eq!(ends(None, Some(200)).earliest(), Some(200));
    assert_eq!(ends(None, None).earliest(), None);
    // The last second of the window is in it; there is no grace after it.
    assert_eq!(ends(Some(100), Some(200)).expired(100), None);
    assert_eq!(
        ends(Some(100), Some(200)).expired(101),
        Some("peer-cert-expired")
    );
    assert_eq!(
        ends(Some(300), Some(200)).expired(201),
        Some("own-cert-expired")
    );
    assert_eq!(ends(None, None).expired(i64::MAX), None);
}

#[tokio::test]
async fn a_link_whose_peer_certificate_ends_is_lost_and_the_peer_is_held_not_erased() {
    let (mut hub, mut link) = linked(LinkCertEnds {
        peer: Some(1_000),
        own: Some(5_000),
    })
    .await;
    assert_eq!(hub.next_cert_end, 1_000, "the sweep knows when to look");

    hub.end_expired_credentials(1_000).await;
    assert!(
        hub.peers.contains_key(&peer()),
        "the last second of the window is served"
    );

    hub.end_expired_credentials(1_001).await;
    assert!(
        !hub.peers.contains_key(&peer()),
        "the link ends the second after notAfter, with no grace"
    );
    while link.try_recv().is_ok() {}
    assert!(
        matches!(
            link.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ),
        "the link's lanes are closed, which ends its pump and its socket"
    );
    // A lost link, not a verdict and not an erasure (ADR 0087): the peer's interest,
    // its negotiated protocol and its standing are what they were.
    assert!(
        hub.interest.nodes_matching("ce/x").contains(&peer()),
        "what the peer announced is kept: its subscribers are still owed publishes"
    );
    assert!(hub.known_peer_protos.contains_key(&peer()));
    assert!(
        !hub.held_dead.contains_key(&peer()),
        "the link's end is not a Dead verdict"
    );
    assert_eq!(hub.next_cert_end, i64::MAX, "nothing left to look at");
}

#[tokio::test]
async fn a_link_ends_when_this_nodes_own_certificate_does() {
    let (mut hub, _link) = linked(LinkCertEnds {
        peer: Some(5_000),
        own: Some(1_000),
    })
    .await;
    hub.end_expired_credentials(1_001).await;
    assert!(!hub.peers.contains_key(&peer()));
    assert!(hub.interest.nodes_matching("ce/x").contains(&peer()));
}

#[tokio::test]
async fn a_plaintext_link_has_no_end() {
    let (mut hub, _link) = linked(LinkCertEnds::default()).await;
    assert_eq!(hub.next_cert_end, i64::MAX);
    hub.end_expired_credentials(i64::MAX - 1).await;
    assert!(hub.peers.contains_key(&peer()));
}

/// Attach client `id` admitted with a certificate ending at `end`.
async fn attach_with_cert(
    hub: &mut Hub,
    id: &str,
    end: i64,
) -> mpsc::UnboundedReceiver<Box<Outgoing>> {
    let (otx, orx) = mpsc::unbounded_channel::<Box<Outgoing>>();
    let (reply, wait) = oneshot::channel();
    let mut admission = admission(id);
    admission.cert_not_after = Some(end);
    hub.dispatch(HubCommand::Attach {
        client: ClientId(id.into()),
        admission: Box::new(admission),
        conn_id: 1,
        clean_start: true,
        session_expiry: 0,
        receive_maximum: u16::MAX,
        will: None,
        outbound: Outbound::new(otx).0,
        reply,
    })
    .await;
    quiesce(hub).await;
    assert!(matches!(wait.await, Ok(AttachOutcome::Present(false))));
    orx
}

#[tokio::test]
async fn a_client_session_outlives_its_certificate_unless_the_setting_is_on() {
    // Off (the default, O2): the certificate was checked at the handshake, and that is all.
    let (mut hub, _) =
        Hub::with_config(NodeId("ce-off".into()), Arc::new(MemorySessionStore::new()));
    let _rx = attach_with_cert(&mut hub, "device", 1_000).await;
    hub.end_expired_credentials(10_000).await;
    assert!(hub.online.contains_key(&ClientId("device".into())));

    // On: ended the second after notAfter, not before.
    let (mut hub, _) =
        Hub::with_config(NodeId("ce-on".into()), Arc::new(MemorySessionStore::new()));
    hub.set_end_sessions_at_cert_expiry(true);
    let _rx = attach_with_cert(&mut hub, "device", 1_000).await;
    let _other = attach_with_cert(&mut hub, "longer", 9_000).await;
    assert_eq!(hub.next_cert_end, 1_000);
    hub.end_expired_credentials(1_000).await;
    assert!(hub.online.contains_key(&ClientId("device".into())));
    hub.end_expired_credentials(1_001).await;
    assert!(
        !hub.online.contains_key(&ClientId("device".into())),
        "the session ends with its certificate"
    );
    assert!(hub.online.contains_key(&ClientId("longer".into())));
    assert_eq!(
        hub.next_cert_end, 9_000,
        "the next end is the remaining session's"
    );
}

/// A second link to [`peer`] (as a move to renewed certificates makes, issue #1016),
/// admitted under certificates ending at `ends`. Returns its outbound lane.
async fn relink(
    hub: &mut Hub,
    conn_id: u64,
    ends: LinkCertEnds,
) -> mpsc::UnboundedReceiver<PeerMessage> {
    let (tx, rx) = mpsc::unbounded_channel();
    hub.dispatch(HubCommand::PeerConnected {
        node: peer(),
        conn_id,
        ctl: tx.clone(),
        tx,
        cert_serial: Some(vec![0x02]),
        cert_ends: ends,
        proto: mqtt_cluster::peer::PROTO_MAX,
        depth: Arc::default(),
    })
    .await;
    quiesce(hub).await;
    rx
}

/// Whether a link's lanes have been closed by the hub (what ends its pump and socket).
fn closed(lane: &mut mpsc::UnboundedReceiver<PeerMessage>) -> bool {
    while lane.try_recv().is_ok() {}
    matches!(
        lane.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    )
}

/// Issue #1016: a link replaced by a newer one to the same peer is not a lost link. The
/// peer has a link throughout; the old one is no longer used for anything this node sends,
/// but stays open for [`RETIRE_TICKS`] so what was in flight on it still arrives, and is
/// then closed — and its disconnect, when its pump reports it, is a stale one.
#[tokio::test]
async fn a_replaced_link_is_retired_quietly_not_lost() {
    let (mut hub, mut old) = linked(LinkCertEnds {
        peer: Some(1_000),
        own: Some(1_000),
    })
    .await;
    let mut new = relink(
        &mut hub,
        2,
        LinkCertEnds {
            peer: Some(9_000),
            own: Some(9_000),
        },
    )
    .await;
    assert_eq!(hub.peers[&peer()].conn_id, 2, "the new link is the link");
    assert!(
        !closed(&mut old),
        "the old link stays open for its last frames"
    );

    // What this node sends goes on the new link only.
    while new.try_recv().is_ok() {}
    while old.try_recv().is_ok() {}
    hub.interest_authoritative = true;
    hub.gossip_interest();
    assert!(new.try_recv().is_ok(), "the new link carries what is sent");
    assert!(old.try_recv().is_err(), "nothing new goes on the old link");

    // Nothing of a lost link: the peer's interest, its members' reachability and its
    // standing are untouched.
    assert!(hub.interest.nodes_matching("ce/x").contains(&peer()));
    assert!(!hub.held_dead.contains_key(&peer()));

    for _ in 1..RETIRE_TICKS {
        hub.close_retired_links();
    }
    assert!(!closed(&mut old), "kept for RETIRE_TICKS ticks");
    hub.close_retired_links();
    assert!(closed(&mut old), "and then closed");

    // Its pump then reports the disconnect: stale, the peer keeps its link.
    hub.peer_disconnected(&peer(), 1);
    assert_eq!(hub.peers[&peer()].conn_id, 2);
    assert!(!closed(&mut new));
}

/// Issue #1016: the hard end stays strict for a retired link. One kept open for its last
/// frames is closed the second after a certificate it was admitted under ends, whatever
/// ticks it had left; the link that replaced it is not touched.
#[tokio::test]
async fn a_retired_link_still_ends_at_its_certificates_end() {
    let (mut hub, mut old) = linked(LinkCertEnds {
        peer: Some(1_000),
        own: Some(1_000),
    })
    .await;
    let mut new = relink(
        &mut hub,
        2,
        LinkCertEnds {
            peer: Some(9_000),
            own: Some(9_000),
        },
    )
    .await;
    assert_eq!(
        hub.next_cert_end, 1_000,
        "the retired link's end is still looked at"
    );
    hub.end_expired_credentials(1_000).await;
    assert!(!closed(&mut old), "its last second is served");
    hub.end_expired_credentials(1_001).await;
    assert!(closed(&mut old), "closed the second after, with ticks left");
    assert!(!closed(&mut new), "the link that replaced it is untouched");
    assert_eq!(hub.peers[&peer()].conn_id, 2);
    assert_eq!(hub.next_cert_end, 9_000);
}

/// Issue #1016: a Dead verdict ends a retired link along with the link, as it ends the
/// link.
#[tokio::test]
async fn a_dead_verdict_closes_a_retired_link_too() {
    let (mut hub, mut old) = linked(LinkCertEnds::default()).await;
    let mut new = relink(&mut hub, 2, LinkCertEnds::default()).await;
    hub.peer_dead(&peer());
    assert!(closed(&mut old));
    assert!(closed(&mut new));
}

/// Issue #1016: the client certificate ends are indexed, and an entry left by a session
/// that has since been replaced does not end the session that replaced it.
#[tokio::test]
async fn a_replaced_sessions_certificate_end_does_not_end_its_successor() {
    let (mut hub, _) =
        Hub::with_config(NodeId("ce-idx".into()), Arc::new(MemorySessionStore::new()));
    hub.set_end_sessions_at_cert_expiry(true);
    let _first = attach_with_cert(&mut hub, "device", 1_000).await;
    // The device reconnects with a renewed certificate: a new connection, a later end.
    let (otx, _orx) = mpsc::unbounded_channel::<Box<Outgoing>>();
    let (reply, wait) = oneshot::channel();
    let mut admission = admission("device");
    admission.cert_not_after = Some(5_000);
    hub.dispatch(HubCommand::Attach {
        client: ClientId("device".into()),
        admission: Box::new(admission),
        conn_id: 2,
        clean_start: true,
        session_expiry: 0,
        receive_maximum: u16::MAX,
        will: None,
        outbound: Outbound::new(otx).0,
        reply,
    })
    .await;
    quiesce(&mut hub).await;
    assert!(wait.await.is_ok());

    hub.end_expired_credentials(1_001).await;
    assert!(
        hub.online.contains_key(&ClientId("device".into())),
        "the renewed session is not ended at the old certificate's end"
    );
    assert_eq!(hub.next_cert_end, 5_000);
    hub.end_expired_credentials(5_001).await;
    assert!(!hub.online.contains_key(&ClientId("device".into())));
    assert!(hub.client_cert_ends.is_empty());
}
