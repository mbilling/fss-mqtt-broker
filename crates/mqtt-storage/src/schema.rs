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

/// One in-place migration step: transforms a store's layout from version `from` to
/// `from + 1` inside the supplied write transaction (ADR 0058). The stamp bump is NOT
/// the step's job — [`gate_or_migrate`] commits the bump in the same transaction, so a
/// crash can never leave transformed data under an untransformed stamp or vice versa.
#[derive(Clone, Copy, Debug)]
pub struct MigrationStep {
    /// The version this step migrates FROM (to `from + 1`).
    pub from: u32,
    /// The transformation itself. Returning an error aborts the transaction — the store
    /// stays at `from`, untouched.
    pub run: fn(&redb::WriteTransaction) -> Result<(), String>,
}

/// [`gate`] with an upgrade path: a store stamped older than `expected` is migrated
/// **one step at a time, each step in its own committed transaction that also bumps the
/// stamp** (ADR 0058 decision A). A crash mid-chain therefore resumes exactly at the
/// stamped step on the next open. Fail-closed in every direction the contract names:
///
/// - stamped **newer** than `expected` → refuse (an older binary must never touch a
///   newer store — that is silent corruption's front door)
/// - stamped older with a **gap in the chain** → refuse, naming the versions; the
///   operator upgrades through the intermediate release (sequential majors, ADR 0039)
/// - a step returning an error → that transaction aborts; the store remains at the
///   stamped version, untouched
///
/// # Errors
/// [`SchemaError::Mismatch`] on refusal; [`SchemaError::Backend`] on I/O or a failed
/// migration step.
pub fn gate_or_migrate(
    db: &Database,
    store: &'static str,
    expected: u32,
    migrations: &[MigrationStep],
) -> Result<(), SchemaError> {
    let be = |e: &dyn std::fmt::Display| SchemaError::Backend(format!("{store}: schema gate: {e}"));
    loop {
        // Read the stamp fresh each iteration: each completed step advanced it in its
        // own committed transaction.
        let found = {
            let txn = db.begin_write().map_err(|e| be(&e))?;
            let v = {
                let mut table = txn.open_table(SCHEMA_META).map_err(|e| be(&e))?;
                // Bind the read to an owned value so the access guard drops before the
                // insert below borrows `table` mutably (a match scrutinee holds its
                // temporaries for the whole match, which would collide otherwise).
                let stamped = table
                    .get(VERSION_KEY)
                    .map_err(|e| be(&e))?
                    .map(|g| g.value());
                if let Some(v) = stamped {
                    v
                } else {
                    // Fresh store: stamp the current layout and be done.
                    table.insert(VERSION_KEY, expected).map_err(|e| be(&e))?;
                    expected
                }
            };
            txn.commit().map_err(|e| be(&e))?;
            v
        };
        if found == expected {
            return Ok(());
        }
        if found > expected {
            return Err(SchemaError::Mismatch {
                store,
                found,
                expected,
            });
        }
        let Some(step) = migrations.iter().find(|m| m.from == found) else {
            // Older than this build reads and no step to bridge it: sequential-upgrade
            // territory, or a registry someone forgot to extend — either way, refuse
            // with both numbers on the table.
            return Err(SchemaError::Mismatch {
                store,
                found,
                expected,
            });
        };
        let txn = db.begin_write().map_err(|e| be(&e))?;
        (step.run)(&txn).map_err(|e| {
            be(&format!(
                "migration v{found} -> v{}: {e} (store left at v{found}, untouched)",
                found + 1
            ))
        })?;
        {
            let mut table = txn.open_table(SCHEMA_META).map_err(|e| be(&e))?;
            table.insert(VERSION_KEY, found + 1).map_err(|e| be(&e))?;
        }
        txn.commit().map_err(|e| be(&e))?;
    }
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

/// Assert a store's migration registry can carry any store from `floor` up to `expected`
/// (ADR 0058 T2). The chain must contain exactly one step for each version in
/// `[floor, expected)` — no gaps (a gap means a store stuck mid-way), no duplicates, no
/// step at or past `expected` (nothing to migrate *to* beyond the current layout).
///
/// `floor` is the oldest version the stability contract supports migrating from: pre-1.0
/// it equals `expected` (no back-compat — the range is empty and an empty registry is
/// correct); at 1.0 each store pins `floor` to its 1.0 schema version, at which point a
/// bump of `expected` without adding the bridging step makes this fail.
///
/// This is the guard that makes "wired now, empty" safe: the day someone raises a store's
/// `SCHEMA_VERSION` past its `floor` without a matching `MigrationStep`, the store's own
/// coverage test fails.
///
/// # Errors
/// A message naming the missing or extraneous versions.
pub fn assert_migrations_cover(
    floor: u32,
    expected: u32,
    migrations: &[MigrationStep],
) -> Result<(), String> {
    let mut froms: Vec<u32> = migrations.iter().map(|m| m.from).collect();
    froms.sort_unstable();
    let want: Vec<u32> = (floor..expected).collect();
    if froms != want {
        return Err(format!(
            "migration registry does not cover [{floor}, {expected}): have steps from \
             {froms:?}, need exactly {want:?}. A schema bump landed without its \
             MigrationStep, or a step is a gap/duplicate."
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{force_version, gate, gate_or_migrate, MigrationStep, SchemaError};
    use redb::{Database, TableDefinition};

    /// A data table migrations write witness rows into, so a test can tell WHICH steps
    /// ran and in what order — a migration chain that "succeeds" without demonstrably
    /// transforming data would be a check that cannot fail.
    const WITNESS: TableDefinition<&str, &str> = TableDefinition::new("witness");

    fn witness(txn: &redb::WriteTransaction, key: &str) -> Result<(), String> {
        let mut t = txn.open_table(WITNESS).map_err(|e| e.to_string())?;
        t.insert(key, "ran").map_err(|e| e.to_string())?;
        Ok(())
    }

    fn step(from: u32, tag: &'static str) -> MigrationStep {
        fn run_v1(txn: &redb::WriteTransaction) -> Result<(), String> {
            witness(txn, "v1->v2")
        }
        fn run_v2(txn: &redb::WriteTransaction) -> Result<(), String> {
            witness(txn, "v2->v3")
        }
        fn run_fail(_txn: &redb::WriteTransaction) -> Result<(), String> {
            Err("injected failure".into())
        }
        let run = match tag {
            "v1" => run_v1,
            "v2" => run_v2,
            _ => run_fail,
        };
        MigrationStep { from, run }
    }

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

    /// ADR 0058 decision A, the happy chain: a v1 store under a v3 binary migrates
    /// v1→v2→v3 with each step's transformation witnessed in the data — the promise is
    /// upgrade-in-place, and "in place" must mean the data moved, not just the stamp.
    #[test]
    fn an_old_store_migrates_step_by_step_to_the_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("m.redb")).unwrap();
        gate(&db, "m.redb", 1).unwrap();

        let chain = [step(1, "v1"), step(2, "v2")];
        gate_or_migrate(&db, "m.redb", 3, &chain).unwrap();

        let txn = db.begin_read().unwrap();
        let t = txn.open_table(WITNESS).unwrap();
        assert_eq!(t.get("v1->v2").unwrap().unwrap().value(), "ran");
        assert_eq!(t.get("v2->v3").unwrap().unwrap().value(), "ran");
        // And the stamp landed at 3: a plain gate at 3 passes, at 2 refuses.
        gate(&db, "m.redb", 3).unwrap();
        assert!(gate(&db, "m.redb", 2).is_err());
    }

    /// A failed step aborts ITS transaction and leaves the store at the stamped version,
    /// untouched — then a later open with a healthy step RESUMES from exactly there.
    /// This is the crash-mid-chain story: per-step commits mean the stamp always
    /// describes the data.
    #[test]
    fn a_failed_step_leaves_the_stamp_honest_and_a_retry_resumes() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("r.redb")).unwrap();
        gate(&db, "r.redb", 1).unwrap();

        // Step 1 succeeds; step 2 fails. The store must end at v2 with step 1's data.
        let broken = [step(1, "v1"), step(2, "boom")];
        let err = gate_or_migrate(&db, "r.redb", 3, &broken).unwrap_err();
        assert!(err.to_string().contains("injected failure"), "{err}");
        assert!(
            err.to_string().contains("untouched"),
            "the error must say the store was not harmed: {err}"
        );
        gate(&db, "r.redb", 2).unwrap_or_else(|e| {
            panic!("store should be stamped v2 (step 1 committed, step 2 aborted): {e}")
        });

        // The "restart with a fixed binary": same chain, healthy step 2 — resumes at v2.
        let fixed = [step(1, "v1"), step(2, "v2")];
        gate_or_migrate(&db, "r.redb", 3, &fixed).unwrap();
        let txn = db.begin_read().unwrap();
        let t = txn.open_table(WITNESS).unwrap();
        assert_eq!(t.get("v2->v3").unwrap().unwrap().value(), "ran");
    }

    /// Contract clause 3: a NEWER store refuses an older binary — loudly, with both
    /// versions named. Migration only ever goes forward.
    #[test]
    fn a_newer_store_refuses_an_older_binary() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("n.redb")).unwrap();
        gate(&db, "n.redb", 5).unwrap();
        let err = gate_or_migrate(&db, "n.redb", 3, &[]).unwrap_err();
        assert!(
            matches!(
                err,
                SchemaError::Mismatch {
                    found: 5,
                    expected: 3,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// A gap in the chain refuses rather than skipping: sequential majors (ADR 0039)
    /// mean a too-old store goes through the intermediate release, not around it.
    #[test]
    fn a_gap_in_the_chain_refuses_rather_than_skips() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("g.redb")).unwrap();
        gate(&db, "g.redb", 1).unwrap();
        // Only v2->v3 exists; nothing bridges v1.
        let err = gate_or_migrate(&db, "g.redb", 3, &[step(2, "v2")]).unwrap_err();
        assert!(
            matches!(
                err,
                SchemaError::Mismatch {
                    found: 1,
                    expected: 3,
                    ..
                }
            ),
            "{err}"
        );
        // And nothing ran: the witness table must not exist.
        let txn = db.begin_read().unwrap();
        assert!(
            txn.open_table(WITNESS).is_err(),
            "no step may run across a gap"
        );
    }
}
