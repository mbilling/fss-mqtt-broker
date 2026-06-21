//! Data-directory ownership guard (ADR 0018 phase 5).
//!
//! On-disk persistence ([`persistent_log`](crate::persistent_log),
//! [`persistent_retained`](crate::persistent_retained), and the cluster lease/replica
//! stores) makes a node's data directory **stateful and node-specific**: the lease
//! Raft identity, the committed session/replica logs, and the retained set all belong to
//! one node id. Pointing a *second* node at the same directory — a common operational
//! slip (a mis-templated volume, a copied config) — would corrupt consensus (two Raft
//! identities sharing one log) and silently mix sessions.
//!
//! [`guard_data_dir`] stamps the directory with its owning node id on first use and
//! refuses to open a directory stamped by a *different* node, turning a
//! silent-corruption footgun into a loud startup error.

use crate::StorageError;
use std::path::Path;

/// The file that records which node id owns a data directory.
const STAMP: &str = "node-id";

/// Claim `dir` for `node_id`, creating it if absent. Writes a `node-id` stamp on first
/// use; on later opens it must match.
///
/// # Errors
/// [`StorageError::Backend`] if the directory cannot be created or read, or if it is
/// already stamped by a **different** node id.
pub fn guard_data_dir(dir: impl AsRef<Path>, node_id: &str) -> Result<(), StorageError> {
    let dir = dir.as_ref();
    std::fs::create_dir_all(dir)
        .map_err(|e| StorageError::Backend(format!("create data dir {}: {e}", dir.display())))?;
    let stamp = dir.join(STAMP);
    match std::fs::read_to_string(&stamp) {
        Ok(existing) => {
            let owner = existing.trim();
            if owner != node_id {
                return Err(StorageError::Backend(format!(
                    "data dir {} belongs to node {owner:?}, not {node_id:?}; \
                     refusing to open another node's persistent state",
                    dir.display()
                )));
            }
            Ok(())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => std::fs::write(&stamp, node_id)
            .map_err(|e| StorageError::Backend(format!("stamp data dir {}: {e}", dir.display()))),
        Err(e) => Err(StorageError::Backend(format!(
            "read data-dir stamp {}: {e}",
            stamp.display()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::guard_data_dir;

    #[test]
    fn stamps_on_first_use_and_rejects_a_foreign_node() {
        let dir = tempfile::tempdir().unwrap();
        // First use by node "a" stamps the directory.
        guard_data_dir(dir.path(), "a").expect("first claim");
        // The same node may reopen it.
        guard_data_dir(dir.path(), "a").expect("reopen by owner");
        // A different node is refused (loud error, not silent corruption).
        let err = guard_data_dir(dir.path(), "b").unwrap_err();
        assert!(
            err.to_string().contains("belongs to node"),
            "expected an ownership error, got: {err}"
        );
    }

    #[test]
    fn creates_a_missing_directory() {
        let parent = tempfile::tempdir().unwrap();
        let nested = parent.path().join("does/not/exist/yet");
        guard_data_dir(&nested, "a").expect("creates the dir and stamps it");
        assert!(nested.join("node-id").exists());
    }
}
