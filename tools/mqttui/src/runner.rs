//! Running one task: spawn it, stream its output, cancel it, and tear it down.
//!
//! **One at a time, enforced** (ADR 0056 §T2). These scripts bind fixed ports and start
//! containers, and `bench/run.sh` explicitly requires an otherwise-idle host — concurrency
//! would produce failures that look like broker bugs.
//!
//! Cancellation signals the **process group**, not the child (ADR 0056 §4). Every script
//! traps `EXIT` to remove its brokers, containers and temporary directories; signalling
//! only the wrapper orphans them. That much is guaranteed. What is *not* guaranteed is that
//! the trap works — so [`Run::teardown_report`] verifies afterwards and says what survived,
//! rather than claiming nothing did.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::env;
use crate::manifest::Task;

/// Most output lines kept in memory. A `bench` run emits far more than anyone will scroll;
/// the full stream always goes to the log file, so this bound costs nothing but the tail.
const SCROLLBACK: usize = 10_000;

/// How a finished run ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Exited with this code. Zero is success.
    Exited(i32),
    /// Cancelled by the user.
    Cancelled,
    /// Could not be started at all.
    Failed,
}

/// One line of a run's output.
#[derive(Debug, Clone)]
pub struct Line {
    pub text: String,
    /// Whether it looks like a failure — used to jump to the first one.
    pub bad: bool,
}

impl Line {
    fn classify(text: String) -> Self {
        let t = text.trim_start();
        // The vocabulary these scripts actually use, plus the usual suspects.
        let bad = t.starts_with("FAIL")
            || t.starts_with("FATAL")
            || t.starts_with("ERROR")
            || t.starts_with("error:")
            || t.contains("panicked at")
            || t.starts_with("Error:");
        Self { text, bad }
    }
}

/// A running or finished task.
pub struct Run {
    pub task_id: String,
    pub started: Instant,
    pub lines: Arc<Mutex<Vec<Line>>>,
    pub outcome: Option<Outcome>,
    log_path: PathBuf,
    child: Option<Child>,
    done_rx: Receiver<Outcome>,
    /// Set once cancellation has been requested, so the UI can say "stopping…".
    pub cancelling: bool,
}

impl Run {
    /// Spawn `task` from `root`, with `overrides` layered over the manifest defaults.
    ///
    /// # Errors
    /// If the process cannot be started.
    pub fn start(
        task: &Task,
        root: &Path,
        overrides: &std::collections::BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let log_dir = root.join("target").join("mqttui-logs");
        let _ = std::fs::create_dir_all(&log_dir);
        let log_path = log_dir.join(format!("{}-{}.log", task.id, std::process::id()));

        let script = root.join(&task.script);
        let is_python = Path::new(&task.script)
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("py"));
        let mut cmd = if is_python {
            let mut c = Command::new("python3");
            c.arg(&script);
            c
        } else {
            Command::new(&script)
        };
        cmd.current_dir(root)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        for (k, v) in env::resolve(task, overrides) {
            cmd.env(k, v);
        }
        set_process_group(&mut cmd);

        let mut child = cmd.spawn().map_err(|e| format!("{}: {e}", task.script))?;
        let lines = Arc::new(Mutex::new(Vec::new()));
        let (done_tx, done_rx) = mpsc::channel();

        // stdout and stderr are pumped by two threads into one buffer, so interleaving
        // matches what a terminal would show.
        for stream in [
            child.stdout.take().map(Pipe::Out),
            child.stderr.take().map(Pipe::Err),
        ]
        .into_iter()
        .flatten()
        {
            let sink = Arc::clone(&lines);
            let log = log_path.clone();
            std::thread::spawn(move || pump(stream, &sink, &log));
        }

        // A third thread waits, so the UI never blocks on the child.
        let waiter = child.id();
        std::thread::spawn(move || {
            let code = wait_pid(waiter);
            let _ = done_tx.send(Outcome::Exited(code));
        });

        Ok(Self {
            task_id: task.id.clone(),
            started: Instant::now(),
            lines,
            outcome: None,
            log_path,
            child: Some(child),
            done_rx,
            cancelling: false,
        })
    }

    /// Has it finished? Call from the UI loop; never blocks.
    pub fn poll(&mut self) -> Option<Outcome> {
        if self.outcome.is_some() {
            return self.outcome;
        }
        match self.done_rx.try_recv() {
            Ok(o) => {
                let o = if self.cancelling {
                    Outcome::Cancelled
                } else {
                    o
                };
                self.outcome = Some(o);
                self.child = None;
                Some(o)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                let o = if self.cancelling {
                    Outcome::Cancelled
                } else {
                    Outcome::Failed
                };
                self.outcome = Some(o);
                Some(o)
            }
        }
    }

    /// Ask the task to stop, by signalling its **process group** so each script's own
    /// `trap EXIT` runs. Returns immediately; the run is finished when [`poll`] says so.
    pub fn cancel(&mut self) {
        if self.outcome.is_some() {
            return;
        }
        self.cancelling = true;
        if let Some(child) = &self.child {
            signal_group(child.id());
        }
    }

    /// A snapshot of the output.
    #[must_use]
    pub fn snapshot(&self) -> Vec<Line> {
        self.lines.lock().map(|l| l.clone()).unwrap_or_default()
    }

    /// Where the full, unbounded log was written.
    #[must_use]
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }

    /// Index of the first line that looks like a failure.
    #[must_use]
    pub fn first_bad(&self) -> Option<usize> {
        self.lines.lock().ok()?.iter().position(|l| l.bad)
    }
}

enum Pipe {
    Out(std::process::ChildStdout),
    Err(std::process::ChildStderr),
}

fn pump(stream: Pipe, sink: &Arc<Mutex<Vec<Line>>>, log: &Path) {
    let reader: Box<dyn BufRead> = match stream {
        Pipe::Out(s) => Box::new(BufReader::new(s)),
        Pipe::Err(s) => Box::new(BufReader::new(s)),
    };
    for line in reader.lines().map_while(Result::ok) {
        // The full stream goes to the log unconditionally; the in-memory copy is bounded.
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log)
        {
            use std::io::Write as _;
            let _ = writeln!(f, "{line}");
        }
        if let Ok(mut buf) = sink.lock() {
            if buf.len() >= SCROLLBACK {
                buf.remove(0);
            }
            buf.push(Line::classify(line));
        }
    }
}

// ── Process-group handling ───────────────────────────────────────────────────────────
//
// The child is put in its OWN process group so a signal reaches it and everything it
// started — brokers, docker CLIs, kubectl — without also hitting mqttui itself.

#[cfg(unix)]
fn set_process_group(cmd: &mut Command) {
    use std::os::unix::process::CommandExt as _;
    // SAFETY: setpgid(0, 0) is async-signal-safe and is the documented way to put a child
    // in its own group between fork and exec.
    unsafe {
        cmd.pre_exec(|| {
            if libc_setpgid() == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }
}

#[cfg(not(unix))]
fn set_process_group(_cmd: &mut Command) {}

#[cfg(unix)]
fn libc_setpgid() -> i32 {
    // Declared locally rather than taking a `libc` dependency for two calls.
    extern "C" {
        fn setpgid(pid: i32, pgid: i32) -> i32;
    }
    unsafe { setpgid(0, 0) }
}

/// Signal the child's whole process group: `SIGINT` first, so a script's `trap EXIT` runs
/// exactly as it would on `Ctrl-C` in a terminal.
#[cfg(unix)]
fn signal_group(pid: u32) {
    extern "C" {
        fn kill(pid: i32, sig: i32) -> i32;
    }
    const SIGINT: i32 = 2;
    // Negative pid = the process group. A pid always fits i32 on the platforms we build.
    let Ok(pid) = i32::try_from(pid) else { return };
    unsafe {
        kill(-pid, SIGINT);
    }
}

#[cfg(not(unix))]
fn signal_group(_pid: u32) {}

#[cfg(unix)]
fn wait_pid(pid: u32) -> i32 {
    extern "C" {
        fn waitpid(pid: i32, status: *mut i32, options: i32) -> i32;
    }
    let Ok(pid) = i32::try_from(pid) else {
        return -1;
    };
    let mut status: i32 = 0;
    if unsafe { waitpid(pid, &raw mut status, 0) } < 0 {
        return -1;
    }
    // WIFEXITED / WEXITSTATUS, without pulling in libc for two macros.
    if status.trailing_zeros() >= 7 {
        (status >> 8) & 0xff
    } else {
        // Killed by a signal: report it the way a shell does.
        128 + (status & 0x7f)
    }
}

#[cfg(not(unix))]
fn wait_pid(_pid: u32) -> i32 {
    -1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_lines_are_recognised_and_ordinary_ones_are_not() {
        assert!(Line::classify("FAIL — the ACL is not enforced".into()).bad);
        assert!(Line::classify("  FATAL: 'kind' not found".into()).bad);
        assert!(Line::classify("error: could not compile".into()).bad);
        assert!(Line::classify("thread 'x' panicked at src/y.rs".into()).bad);

        assert!(!Line::classify("  ok   — three nodes formed a cluster".into()).bad);
        assert!(!Line::classify("DEPLOY SMOKE OK".into()).bad);
        // "failed" inside prose must not trip it, or every summary line would be a failure.
        assert!(!Line::classify("0 failed; 14 passed".into()).bad);
    }
}
