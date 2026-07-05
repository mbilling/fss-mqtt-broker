//! Schema-version gate for persistent `redb` stores (ADR 0038 T2).
//!
//! Every store opens through [`gate`]: a fresh file is stamped with the store's
//! current layout version; a matching stamp passes; **any other version refuses to
//! open** with an error naming found-vs-expected. Pre-1.0 there are no migrations —
//! the documented recovery for a version bump is wipe-and-rejoin (the durable plane
//! rebuilds a node's replicated state from its peers) — but the stamp is what makes
//! post-1.0 migrations writable at all: a future build gets a version to dispatch
//! on instead of guessing at bytes.
//!
//! **Release rule (ADR 0039)**: store versions bump only in MAJOR releases, and each
//! major ships migrations from exactly **one** major back — sequential upgrades
//! (1 → 2 → 3, no skipping). A gate mismatch more than one version old means the
//! operator must upgrade through the intermediate major first. The rolling path into
//! a new major additionally starts from the previous major's **gateway minor** (its
//! designated last minor) — enforced by the peer handshake, not this gate.

use redb::{Database, ReadableTable, TableDefinition};

/// The one-row marker table every gated store carries.
const SCHEMA_META: TableDefinition<&str, u32> = TableDefinition::new("schema_meta");
const VERSION_KEY: &str = "version";

/// A schema-gate failure: the store must not be used.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    /// The on-disk layout differs from what this build reads and writes.
    #[error(
        "{store}: on-disk schema is v{found} but this build expects v{expected}; no \
         migration path exists pre-1.0 — wipe the store and let the node rejoin \
         (the durable plane rebuilds replicated state from peers)"
    )]
    Mismatch {
        /// The store being opened (its conventional file name).
        store: &'static str,
        /// The version stamped on disk.
        found: u32,
        /// The version this build writes and reads.
        expected: u32,
    },
    /// The gate itself could not read or write the marker.
    #[error("{0}")]
    Backend(String),
}

/// Stamp a fresh store with `expected`, pass a matching one, and **fail closed** on
/// any other stamped version.
///
/// # Errors
/// [`SchemaError::Mismatch`] when the stamped version differs from `expected`;
/// [`SchemaError::Backend`] if the marker cannot be read or written.
pub fn gate(db: &Database, store: &'static str, expected: u32) -> Result<(), SchemaError> {
    let be = |e: &dyn std::fmt::Display| SchemaError::Backend(format!("{store}: schema gate: {e}"));
    let txn = db.begin_write().map_err(|e| be(&e))?;
    {
        let mut table = txn.open_table(SCHEMA_META).map_err(|e| be(&e))?;
        let found = table
            .get(VERSION_KEY)
            .map_err(|e| be(&e))?
            .map(|g| g.value());
        match found {
            Some(v) if v == expected => {}
            Some(found) => {
                return Err(SchemaError::Mismatch {
                    store,
                    found,
                    expected,
                })
            }
            None => {
                table.insert(VERSION_KEY, expected).map_err(|e| be(&e))?;
            }
        }
    }
    txn.commit().map_err(|e| be(&e))?;
    Ok(())
}

/// Overwrite the stamped version unconditionally — a tool for tests (simulating a
/// foreign-version file) and manual recovery, never called by the broker itself.
///
/// # Errors
/// [`SchemaError::Backend`] if the marker cannot be written.
pub fn force_version(db: &Database, version: u32) -> Result<(), SchemaError> {
    let be = |e: &dyn std::fmt::Display| SchemaError::Backend(format!("schema gate: {e}"));
    let txn = db.begin_write().map_err(|e| be(&e))?;
    {
        let mut table = txn.open_table(SCHEMA_META).map_err(|e| be(&e))?;
        table.insert(VERSION_KEY, version).map_err(|e| be(&e))?;
    }
    txn.commit().map_err(|e| be(&e))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{force_version, gate, SchemaError};
    use redb::Database;

    #[test]
    fn a_fresh_store_is_stamped_and_reopens_clean() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        {
            let db = Database::create(&path).unwrap();
            gate(&db, "s.redb", 1).unwrap(); // fresh: stamps
            gate(&db, "s.redb", 1).unwrap(); // idempotent within one open
        }
        let db = Database::create(&path).unwrap();
        gate(&db, "s.redb", 1).unwrap(); // reopen: matches
    }

    #[test]
    fn a_version_mismatch_fails_closed_naming_both_versions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s.redb");
        {
            let db = Database::create(&path).unwrap();
            gate(&db, "s.redb", 1).unwrap();
            force_version(&db, 999).unwrap(); // a foreign build's file
        }
        let db = Database::create(&path).unwrap();
        let err = gate(&db, "s.redb", 1).unwrap_err();
        match &err {
            SchemaError::Mismatch {
                store,
                found,
                expected,
            } => {
                assert_eq!((*store, *found, *expected), ("s.redb", 999, 1));
            }
            SchemaError::Backend(other) => panic!("expected the mismatch, got {other:?}"),
        }
        let msg = err.to_string();
        assert!(msg.contains("v999") && msg.contains("v1"), "{msg}");
    }
}
