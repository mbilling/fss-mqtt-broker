//! Replacing a file whole: a reader sees the old bytes or the new ones, never a mix, and a
//! power cut leaves one of the two.
//!
//! WRITE a temporary file beside the target, FSYNC it, RENAME it over the target, then
//! fsync the DIRECTORY so the rename itself survives. The temporary file is created with
//! `O_EXCL` under a name nobody can guess (`.<name>.<pid>.<random>.tmp`), so a planted
//! file or symlink of that name is never written through, and it is removed on every
//! path that does not end in the rename.
//!
//! Used where a half-written file would be read as a whole one: the restore stamp a
//! restored node boots from ([`crate::backup`]), and a rules file the admin API writes
//! (ADR 0084), which the broker reloads at once — a truncated rules file that still parses
//! would be applied.

use std::io::Write;
use std::path::{Path, PathBuf};

/// How [`replace`] treats the file it replaces.
#[derive(Debug, Clone, Copy)]
pub struct Replace<'a> {
    /// The mode of a file that did not exist before (Unix). A file that did keeps its
    /// mode, and its group when this process may set it.
    pub new_mode: u32,
    /// What to keep beside the file as `<name>.prev`, written the same way and with the
    /// same mode: the bytes being replaced, so a change can be investigated and put back.
    pub previous: Option<&'a [u8]>,
}

/// The step of [`replace`] that failed. Nothing was replaced: the temporary files are
/// gone and the target holds what it held.
#[derive(Debug)]
pub struct ReplaceError {
    /// What was being done (`create`, `write`, `rename`, …).
    pub step: &'static str,
    /// The file it was being done to.
    pub path: PathBuf,
    /// Why it failed.
    pub error: std::io::Error,
}

impl std::fmt::Display for ReplaceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cannot {} {}: {}",
            self.step,
            self.path.display(),
            self.error
        )
    }
}

impl std::error::Error for ReplaceError {}

fn failed(step: &'static str, path: &Path, error: std::io::Error) -> ReplaceError {
    ReplaceError {
        step,
        path: path.to_path_buf(),
        error,
    }
}

/// The directory a file named `path` lives in: its parent, or `.` for a bare name.
#[must_use]
pub fn dir_of(path: &Path) -> &Path {
    match path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    }
}

/// Replace `path` with `bytes` (see the module docs). `path` is replaced as named: a
/// caller that means a symlink's target, not the link, passes the target.
///
/// # Errors
/// The step that failed; nothing was replaced.
pub fn replace(path: &Path, bytes: &[u8], how: &Replace<'_>) -> Result<(), ReplaceError> {
    let dir = dir_of(path);
    let name = path
        .file_name()
        .ok_or_else(|| {
            failed(
                "replace",
                path,
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "not a file name"),
            )
        })?
        .to_string_lossy()
        .into_owned();
    let existing = std::fs::metadata(path).ok();
    let tmp = write_temp(dir, &name, bytes, existing.as_ref(), how.new_mode)?;
    if let Some(previous) = how.previous {
        let prev = dir.join(format!("{name}.prev"));
        let kept = write_temp(
            dir,
            &format!("{name}.prev"),
            previous,
            existing.as_ref(),
            how.new_mode,
        )
        .and_then(|prev_tmp| {
            std::fs::rename(&prev_tmp, &prev).map_err(|e| {
                let _ = std::fs::remove_file(&prev_tmp);
                failed("rename", &prev, e)
            })
        });
        if let Err(e) = kept {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
    }
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(failed("rename", path, e));
    }
    // The rename must be durable too, or the new file can vanish with an unflushed
    // directory entry. A filesystem that cannot fsync a directory has nothing to flush.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

/// A temporary file in `dir` holding `bytes`, fsynced, with `like`'s mode and group (or
/// `new_mode`); removed again if any step fails.
fn write_temp(
    dir: &Path,
    name: &str,
    bytes: &[u8],
    like: Option<&std::fs::Metadata>,
    new_mode: u32,
) -> Result<PathBuf, ReplaceError> {
    let tmp = dir.join(format!(".{name}.{}.{}.tmp", std::process::id(), suffix()));
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        // Private while it is being written; its own mode is set before it is renamed.
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp).map_err(|e| failed("create", &tmp, e))?;
    let written = file
        .write_all(bytes)
        .map_err(|e| failed("write", &tmp, e))
        .and_then(|()| {
            set_mode(&file, like, new_mode).map_err(|e| failed("set the mode of", &tmp, e))
        })
        .and_then(|()| file.sync_all().map_err(|e| failed("fsync", &tmp, e)));
    match written {
        Ok(()) => Ok(tmp),
        Err(e) => {
            drop(file);
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

/// Give `file` the mode of `like` (and its group, where this process may set it), or
/// `new_mode` when there is nothing to be like. The mode is set explicitly, so the
/// process's umask does not narrow it.
#[cfg(unix)]
fn set_mode(
    file: &std::fs::File,
    like: Option<&std::fs::Metadata>,
    new_mode: u32,
) -> std::io::Result<()> {
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    if let Some(m) = like {
        // Best effort: an unprivileged process may only pick a group it is in. The group
        // first, as a change of owner may clear the set-id bits the mode then restores.
        let _ = std::os::unix::fs::fchown(file, None, Some(m.gid()));
    }
    let mode = like.map_or(new_mode, |m| m.mode() & 0o7777);
    file.set_permissions(std::fs::Permissions::from_mode(mode))
}

#[cfg(not(unix))]
fn set_mode(
    _file: &std::fs::File,
    _like: Option<&std::fs::Metadata>,
    _new_mode: u32,
) -> std::io::Result<()> {
    Ok(())
}

/// 16 random hex digits (the clock's nanoseconds should the system's random source fail).
fn suffix() -> String {
    let mut bytes = [0u8; 8];
    if aws_lc_rs::rand::fill(&mut bytes).is_err() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() & u128::from(u64::MAX));
        bytes = u64::try_from(nanos).unwrap_or_default().to_le_bytes();
    }
    mqtt_core::hex_lower(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(tag: &str) -> tempfile::TempDir {
        tempfile::Builder::new()
            .prefix(&format!("atomic-{tag}-"))
            .tempdir()
            .unwrap()
    }

    fn names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    /// The file is replaced whole, the previous bytes are kept as `<name>.prev`, and no
    /// temporary file is left behind.
    #[test]
    fn a_file_is_replaced_whole_and_its_previous_bytes_kept() {
        let d = dir("whole");
        let path = d.path().join("rules.toml");
        std::fs::write(&path, "old").unwrap();
        let how = Replace {
            new_mode: 0o644,
            previous: Some(b"old"),
        };
        replace(&path, b"new", &how).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(
            std::fs::read_to_string(d.path().join("rules.toml.prev")).unwrap(),
            "old"
        );
        assert_eq!(names(d.path()), ["rules.toml", "rules.toml.prev"]);
        // A bare name lives in the current directory.
        assert_eq!(dir_of(Path::new("rules.toml")), Path::new("."));
    }

    /// An existing file's mode is kept, for the file and its `.prev`; a new one gets the
    /// mode asked for, whatever the umask.
    #[cfg(unix)]
    #[test]
    fn the_mode_is_kept_and_a_new_file_gets_the_one_asked_for() {
        use std::os::unix::fs::PermissionsExt;
        let d = dir("mode");
        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o7777;
        let path = d.path().join("rules.toml");
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let how = Replace {
            new_mode: 0o644,
            previous: Some(b"old"),
        };
        replace(&path, b"new", &how).unwrap();
        assert_eq!(mode(&path), 0o640);
        assert_eq!(mode(&d.path().join("rules.toml.prev")), 0o640);
        let fresh = d.path().join("fresh.toml");
        let how = Replace {
            new_mode: 0o604,
            previous: None,
        };
        replace(&fresh, b"x", &how).unwrap();
        assert_eq!(mode(&fresh), 0o604);
    }

    /// The temporary files' names cannot be guessed: a file or symlink planted at a name
    /// a fixed scheme would use is neither written through nor in the way.
    #[cfg(unix)]
    #[test]
    fn a_planted_file_at_a_guessable_name_is_never_written_through() {
        let d = dir("planted");
        let path = d.path().join("rules.toml");
        std::fs::write(&path, "old").unwrap();
        let victim = d.path().join("victim");
        std::fs::write(&victim, "untouched").unwrap();
        for name in [
            ".rules.toml.tmp".to_string(),
            ".rules.toml.prev.tmp".to_string(),
            format!(".rules.toml.{}.tmp", std::process::id()),
            "rules.toml.partial".to_string(),
        ] {
            std::os::unix::fs::symlink(&victim, d.path().join(name)).unwrap();
        }
        let how = Replace {
            new_mode: 0o644,
            previous: Some(b"old"),
        };
        replace(&path, b"new", &how).unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(std::fs::read_to_string(&victim).unwrap(), "untouched");
    }

    /// A step that fails replaces nothing and leaves no temporary file.
    #[cfg(unix)]
    #[test]
    fn a_failed_replace_changes_nothing_and_cleans_up() {
        let d = dir("fail");
        let path = d.path().join("rules.toml");
        std::fs::write(&path, "old").unwrap();
        // `<name>.prev` is a directory: the rename of the kept copy fails, after both
        // temporary files were written.
        std::fs::create_dir(d.path().join("rules.toml.prev")).unwrap();
        std::fs::write(d.path().join("rules.toml.prev").join("x"), "").unwrap();
        let how = Replace {
            new_mode: 0o644,
            previous: Some(b"old"),
        };
        let e = replace(&path, b"new", &how).unwrap_err();
        assert_eq!(e.step, "rename", "{e}");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old");
        assert_eq!(names(d.path()), ["rules.toml", "rules.toml.prev"]);
        // A directory that does not exist: refused at the first step.
        let e = replace(&d.path().join("nope").join("f"), b"x", &how).unwrap_err();
        assert_eq!(
            (e.step, e.error.kind()),
            ("create", std::io::ErrorKind::NotFound)
        );
    }
}
