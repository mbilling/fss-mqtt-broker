//! Opening persistent `redb` stores with bounded lock-contention retry.
//!
//! On a fast restart over the same data dir — a rolling restart, or a
//! replica-writer whose predecessor just exited — the previous holder's `redb`
//! file lock may not be released for a few milliseconds. Every production store
//! open goes through [`create_with_lock_retry`] so that window never spuriously
//! fails a startup, and so the retry policy lives in exactly one place instead
//! of being re-invented (or forgotten) per store.

use std::path::Path;
use std::time::Duration;

use redb::Database;

/// Attempts before giving up: 30 × 100 ms ≈ a 3 s window, comfortably beyond
/// any observed lock-release lag, short enough that a genuinely unusable store
/// still fails startup promptly.
const ATTEMPTS: u32 = 30;
/// Pause between attempts.
const RETRY_EVERY: Duration = Duration::from_millis(100);

/// Open (creating if absent) a `redb` database at `path`, retrying any open
/// error over a bounded window before returning the last one.
///
/// Blocking (`std::thread::sleep`): intended for the open-at-startup paths
/// where every store is built, not for hot paths. Retrying *any* error (not
/// just lock contention) keeps the helper honest about redb's error surface;
/// the cost is ~3 s added latency before a genuinely corrupt store fails,
/// which startup can afford.
///
/// # Errors
/// The final [`redb::DatabaseError`] once the retry window is exhausted.
pub fn create_with_lock_retry(path: impl AsRef<Path>) -> Result<Database, redb::DatabaseError> {
    let path = path.as_ref();
    let mut last = Database::create(path);
    for _ in 1..ATTEMPTS {
        if last.is_ok() {
            break;
        }
        std::thread::sleep(RETRY_EVERY);
        last = Database::create(path);
    }
    last
}

#[cfg(test)]
mod tests {
    use super::create_with_lock_retry;

    #[test]
    fn opens_a_fresh_database() {
        let dir = tempfile::tempdir().unwrap();
        let db = create_with_lock_retry(dir.path().join("fresh.redb"));
        assert!(db.is_ok());
    }

    #[test]
    fn reopens_after_the_first_handle_drops() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("reopen.redb");
        drop(create_with_lock_retry(&path).unwrap());
        assert!(create_with_lock_retry(&path).is_ok());
    }
}
