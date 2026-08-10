//! Is this task runnable *here*, and what will be skipped if not?
//!
//! The payoff of declaring prerequisites (ADR 0056 §2). Most of these scripts fail with a
//! bare `FATAL: 'x' not found` **after** the user has committed to the run — sometimes
//! minutes in, after building a broker. Knowing beforehand is the difference between a
//! tool and a menu.

use crate::manifest::Task;

/// Which of a task's tools are present.
#[derive(Debug, Default)]
pub struct Report {
    /// `(tool, present)` for tools that must be there.
    pub required: Vec<(String, bool)>,
    /// `(tool, present)` for tools whose absence only degrades the run.
    pub optional: Vec<(String, bool)>,
}

/// Probe every tool a task declares.
#[must_use]
pub fn check(task: &Task) -> Report {
    Report {
        required: task
            .requires
            .iter()
            .map(|t| (t.clone(), on_path(t)))
            .collect(),
        optional: task
            .optional
            .iter()
            .map(|t| (t.clone(), on_path(t)))
            .collect(),
    }
}

/// Just the required tools that are missing — the list that blocks a run.
#[must_use]
pub fn missing_required(task: &Task) -> Vec<String> {
    task.requires
        .iter()
        .filter(|t| !on_path(t))
        .cloned()
        .collect()
}

/// Is `tool` an executable on `PATH`?
///
/// Walking `PATH` rather than shelling out to `which`: it is faster, it works the same
/// everywhere, and it does not depend on a tool being installed to check whether tools are
/// installed.
#[must_use]
pub fn on_path(tool: &str) -> bool {
    // An absolute or relative path is a path, not a name to search for.
    if tool.contains('/') {
        return is_executable(std::path::Path::new(tool));
    }
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    std::env::split_paths(&path).any(|dir| is_executable(&dir.join(tool)))
}

#[cfg(unix)]
fn is_executable(p: &std::path::Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(p: &std::path::Path) -> bool {
    p.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(requires: &[&str], optional: &[&str]) -> Task {
        Task {
            id: "t".into(),
            group: "g".into(),
            name: "n".into(),
            script: "scripts/gen-status.py".into(),
            about: String::new(),
            requires: requires.iter().map(|s| (*s).to_string()).collect(),
            optional: optional.iter().map(|s| (*s).to_string()).collect(),
            duration: String::new(),
            caution: None,
            hidden: false,
            env: Vec::new(),
        }
    }

    /// Something that is certainly on PATH, and something that certainly is not — so the
    /// probe is shown to distinguish them rather than always answering the same way.
    #[test]
    fn the_probe_tells_present_from_absent() {
        assert!(on_path("sh"), "sh must be on PATH");
        assert!(
            !on_path("mqttui-definitely-not-a-real-tool"),
            "a nonexistent tool must not be reported present"
        );
    }

    #[test]
    fn missing_required_lists_only_what_blocks() {
        let t = task(&["sh", "mqttui-absent-tool"], &["also-absent"]);
        let missing = missing_required(&t);
        assert_eq!(missing, vec!["mqttui-absent-tool".to_string()]);

        let report = check(&t);
        assert_eq!(report.required.len(), 2);
        assert!(report.required.iter().any(|(n, ok)| n == "sh" && *ok));
        // An absent OPTIONAL tool must not block: it belongs in the report, not the blocker.
        assert!(!report.optional[0].1);
        assert!(!missing.contains(&"also-absent".to_string()));
    }

    /// A directory on PATH is not a program.
    #[test]
    fn a_directory_is_not_an_executable() {
        assert!(!on_path("/"), "a directory must not count as a tool");
    }

    /// A task with no declared tools always runs — no accidental blocking.
    #[test]
    fn a_task_with_no_requirements_is_never_blocked() {
        assert!(missing_required(&task(&[], &[])).is_empty());
    }
}
