//! Cluster identity ([ADR 0054](../../../docs/adr/0054-operator-facing-state-surface.md) T2).
//!
//! Before this module, nothing distinguished two separately-founded clusters: the
//! seedless-founder rule was the *sole* split-brain guard, with no post-hoc
//! detector — a founder restarted over a lost data dir would happily found a
//! second cluster beside the survivors, and only a key/CA mismatch would stop the
//! two mixing. The identity closes that: the **founder mints** a random id at
//! first bootstrap, every datagram carries it, **joiners adopt** it on first
//! authenticated contact, and the SWIM driver **drops** (and counts) gossip from
//! a foreign cluster — detection *and* containment.
//!
//! Persistence is a plain file (`cluster-id` under the data dir), deliberately
//! not a store schema change: it works identically for durable and in-memory
//! nodes (the latter simply hold the id in memory and re-learn it on restart).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;

/// The id's byte length before hex encoding (128 bits — collision-proof for any
/// plausible number of accidental foundings).
const ID_BYTES: usize = 16;

/// This node's view of the cluster identity. Shared (`Arc`) between the SWIM
/// driver (stamp + guard), the `/statusz` body, and the metrics wiring.
#[derive(Debug)]
pub struct ClusterIdentity {
    id: RwLock<Option<String>>,
    /// Whether this node is the founder (started seedless).
    founder: bool,
    /// Whether THIS process minted a fresh id at startup — a founding event.
    /// A founding on anything but a brand-new cluster's first boot is the
    /// split-brain alarm (`foundings_total` in the metrics).
    minted: AtomicBool,
    /// Where the id persists (`<data_dir>/cluster-id`); `None` = in-memory only.
    path: Option<PathBuf>,
}

impl ClusterIdentity {
    /// Load the persisted id, or — on a founder with none persisted — mint one.
    /// A joiner with nothing persisted starts unknown and adopts on first
    /// authenticated contact ([`adopt`](Self::adopt)).
    ///
    /// # Errors
    /// I/O errors reading or writing the id file.
    pub fn load_or_mint(founder: bool, path: Option<PathBuf>) -> std::io::Result<Self> {
        let persisted = match &path {
            Some(p) if p.exists() => {
                let raw = std::fs::read_to_string(p)?;
                let id = raw.trim().to_string();
                if id.is_empty() {
                    None
                } else {
                    Some(id)
                }
            }
            _ => None,
        };
        let identity = Self {
            id: RwLock::new(persisted.clone()),
            founder,
            minted: AtomicBool::new(false),
            path,
        };
        if persisted.is_none() && founder {
            let minted = mint();
            identity.persist(&minted)?;
            *identity
                .id
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(minted);
            identity.minted.store(true, Ordering::Release);
        }
        Ok(identity)
    }

    /// The cluster id, once known.
    #[must_use]
    pub fn get(&self) -> Option<String> {
        self.id
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Whether this node founded the cluster (started seedless).
    #[must_use]
    pub fn founder(&self) -> bool {
        self.founder
    }

    /// Whether this process minted a fresh id at startup (a founding event).
    #[must_use]
    pub fn minted(&self) -> bool {
        self.minted.load(Ordering::Acquire)
    }

    /// Adopt `id` as the cluster identity if none is known yet (a joiner's first
    /// authenticated contact). Returns whether the adoption happened; an already
    /// known identity is never overwritten (a differing one is the caller's
    /// `cluster-mismatch` rejection, not an adoption).
    pub fn adopt(&self, id: &str) -> bool {
        let mut guard = self
            .id
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if guard.is_some() {
            return false;
        }
        *guard = Some(id.to_string());
        drop(guard);
        // Best-effort persistence: a failed write means re-adoption on restart,
        // never a wrong identity.
        let _ = self.persist(id);
        true
    }

    fn persist(&self, id: &str) -> std::io::Result<()> {
        if let Some(p) = &self.path {
            std::fs::write(p, format!("{id}\n"))?;
        }
        Ok(())
    }
}

/// Mint a fresh random id (hex).
fn mint() -> String {
    let mut bytes = [0u8; ID_BYTES];
    aws_lc_rs::rand::fill(&mut bytes).expect("system RNG");
    bytes.iter().fold(String::new(), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{b:02x}");
        s
    })
}

#[cfg(test)]
mod tests {
    use super::ClusterIdentity;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("mqttd-cid-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("cluster-id")
    }

    /// A founder mints once and reloads the SAME id — a restart is not a founding.
    #[test]
    fn a_founder_mints_once_and_reloads_stably() {
        let path = temp_path("founder");
        let first = ClusterIdentity::load_or_mint(true, Some(path.clone())).unwrap();
        let id = first.get().expect("founder mints");
        assert_eq!(id.len(), 32, "16 bytes hex");
        assert!(first.minted(), "first boot is a founding event");

        let reloaded = ClusterIdentity::load_or_mint(true, Some(path.clone())).unwrap();
        assert_eq!(reloaded.get().as_deref(), Some(id.as_str()));
        assert!(!reloaded.minted(), "a reload is NOT a founding");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// A joiner starts unknown, adopts on first contact, persists the adoption,
    /// and never overwrites a known identity.
    #[test]
    fn a_joiner_adopts_once_and_persists() {
        let path = temp_path("joiner");
        let joiner = ClusterIdentity::load_or_mint(false, Some(path.clone())).unwrap();
        assert_eq!(joiner.get(), None, "a joiner does not mint");
        assert!(!joiner.minted());

        assert!(joiner.adopt("aaaa"));
        assert_eq!(joiner.get().as_deref(), Some("aaaa"));
        assert!(
            !joiner.adopt("bbbb"),
            "a known identity is never overwritten"
        );
        assert_eq!(joiner.get().as_deref(), Some("aaaa"));

        let reloaded = ClusterIdentity::load_or_mint(false, Some(path.clone())).unwrap();
        assert_eq!(
            reloaded.get().as_deref(),
            Some("aaaa"),
            "adoption persisted"
        );
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// Two founders mint DIFFERENT ids — the property split-brain detection rests on.
    #[test]
    fn two_foundings_yield_distinct_ids() {
        let a = ClusterIdentity::load_or_mint(true, None).unwrap();
        let b = ClusterIdentity::load_or_mint(true, None).unwrap();
        assert_ne!(a.get(), b.get());
    }
}
