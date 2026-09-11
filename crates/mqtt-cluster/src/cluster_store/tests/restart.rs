//! A surviving disk copy is data, not permission to resume ownership (#597).
use super::*;
use mqtt_storage::repl::ReplError;

struct CurrentLease(Arc<Mutex<Option<Epoch>>>);

#[async_trait]
impl LeaseSource for CurrentLease {
    async fn epoch_for(&self, _group: GroupId) -> Result<Epoch, ReplError> {
        self.0.lock().unwrap().ok_or(ReplError::NotOwner)
    }
}

fn append(state: &mut ReplicaState, epoch: Epoch, key: &str, offset: u64, value: &[u8]) {
    assert!(state.apply(
        epoch,
        &ReplOp::Append {
            key: key.into(),
            offset,
            seq: 0,
            record: value.to_vec(),
        }
    ));
}

#[tokio::test]
async fn returning_disk_copy_needs_current_authority_and_recovers_newer_history() {
    let owner = nid("owner");
    let mut p = Placement::new(owner.clone(), DEFAULT_REPLICAS);
    p.observe(&nid("f1"), MemberState::Alive, "f1:7000", None);
    p.observe(&nid("f2"), MemberState::Alive, "f2:7000", None);
    let (_, client) = owned_group_and_client(&p);
    let key = format!("q/{}", client.0);
    let placement = Arc::new(RwLock::new(p));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replicas.redb");
    {
        let mut old = ReplicaState::open(&path).unwrap();
        append(&mut old, 2, &key, 1, b"later-acked");
        append(&mut old, 2, &key, 2, b"still-owed");
    }

    // While this disk was offline, the surviving replicas advanced the epoch,
    // retired offset 1 and committed offset 3. No global snapshot is imported.
    let transport = Arc::new(PeerReplicaTransport::new());
    for name in ["f1", "f2"] {
        let mut state = ReplicaState::new();
        append(&mut state, 2, &key, 1, b"later-acked");
        append(&mut state, 2, &key, 2, b"still-owed");
        append(&mut state, 7, &key, 3, b"committed-while-away");
        assert!(state.apply(
            7,
            &ReplOp::Truncate {
                key: key.clone(),
                up_to: 1
            }
        ));
        let (tx, rx) = mpsc::unbounded_channel();
        transport.register(nid(name), tx);
        spawn_follower(transport.clone(), Arc::new(Mutex::new(state)), rx);
    }
    let local = Arc::new(Mutex::new(ReplicaState::open(&path).unwrap()));
    assert_eq!(local.lock().unwrap().entries(&key).len(), 2);
    let authority = Arc::new(Mutex::new(None));
    let log = GroupRoutedLog::new(
        owner,
        placement,
        transport,
        CurrentLease(authority.clone()),
        local.clone(),
    );

    // Even a stale placement ring naming this node is not authority: the
    // committed lease belongs elsewhere. Reads and writes must fail closed.
    assert!(matches!(
        log.read(&key, 0, 10).await,
        Err(ReplError::NotOwner)
    ));
    assert!(matches!(
        log.append(&key, b"unauthorized".to_vec()).await,
        Err(ReplError::NotOwner)
    ));
    assert_eq!(
        local.lock().unwrap().entries(&key).len(),
        2,
        "do not wipe saved data"
    );

    // Even if this node has not yet learned the new lease, the survivors fence
    // its old epoch. It cannot re-commit a recovery base and start serving.
    *authority.lock().unwrap() = Some(2);
    assert!(matches!(
        log.read(&key, 0, 10).await,
        Err(ReplError::NoQuorum)
    ));
    assert!(matches!(
        log.append(&key, b"stale".to_vec()).await,
        Err(ReplError::NoQuorum)
    ));

    // Only a new legitimate grant permits recovery and re-commit. A new epoch
    // does not erase old committed entries or resurrect an acknowledged prefix.
    *authority.lock().unwrap() = Some(8);
    let rows = log.read(&key, 0, 10).await.unwrap();
    assert_eq!(
        rows.iter()
            .map(|e| (e.offset, e.record.as_slice()))
            .collect::<Vec<_>>(),
        vec![
            (2, b"still-owed".as_slice()),
            (3, b"committed-while-away".as_slice())
        ]
    );
    assert_eq!(
        log.append(&key, b"new-owner-write".to_vec()).await.unwrap(),
        4
    );
}
