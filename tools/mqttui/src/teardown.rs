//! What is on this machine, and what a run left behind (ADR 0056 §4, §5).
//!
//! Two jobs that are really one: knowing the state of Docker, Kubernetes and this
//! machine's stray processes is what makes a cleanup report meaningful, and what makes the
//! Kubernetes context visible *before* a task targets it.
//!
//! **The context display is a safety feature, not a convenience.** `kind-smoke.sh` and
//! `operator-e2e.sh` run `kubectl` against whatever context is current. Seeing
//! `kube: prod-eu-west` before pressing enter is the difference between a smoke test and an
//! incident.
//!
//! **Probing is read-only and unconditional; acting is never automatic.** Nothing here
//! removes anything unless asked. A tool that kills processes the user did not ask it to
//! kill is worse than one that only shows them.

use std::fmt::Write as _;
use std::process::{Command, Stdio};
use std::time::Duration;

/// A probe's answer. `Unknown` is not `None` — "we could not ask" and "there are none" are
/// different facts, and conflating them is how a dashboard starts lying.
#[derive(Debug, Clone)]
pub enum Probe {
    Value(String),
    None,
    Unavailable(String),
}

impl std::fmt::Display for Probe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Value(v) => write!(f, "{v}"),
            Self::None => write!(f, "none"),
            Self::Unavailable(why) => write!(f, "unavailable ({why})"),
        }
    }
}

/// A snapshot of the machine.
#[derive(Debug, Clone)]
pub struct Environment {
    pub docker: Probe,
    pub kube_context: Probe,
    pub kind_clusters: Probe,
    pub stray_brokers: usize,
    pub compose_projects: Probe,
}

impl Environment {
    /// Probe everything cheap. Measured costs on a warm machine: `docker ps` 0.01s,
    /// `kind get clusters` 0.06s, `kubectl config current-context` 0.22s. `docker compose
    /// ls` is 1.9s and is therefore only run here, on demand, never on a timer.
    #[must_use]
    pub fn probe() -> Self {
        Self {
            docker: run_probe("docker", &["ps", "--format", "{{.Names}}"], |out| {
                let n = out.lines().filter(|l| !l.trim().is_empty()).count();
                if n == 0 {
                    Probe::None
                } else {
                    Probe::Value(format!("{n} container(s) running"))
                }
            }),
            kube_context: run_probe("kubectl", &["config", "current-context"], |out| {
                let ctx = out.trim();
                if ctx.is_empty() {
                    Probe::None
                } else {
                    Probe::Value(ctx.to_string())
                }
            }),
            kind_clusters: run_probe("kind", &["get", "clusters"], |out| {
                let names: Vec<&str> = out
                    .lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .collect();
                if names.is_empty() {
                    Probe::None
                } else {
                    Probe::Value(names.join(", "))
                }
            }),
            stray_brokers: stray_brokers().len(),
            compose_projects: run_probe("docker", &["compose", "ls", "--format", "json"], |out| {
                let n = out.matches("\"Name\"").count();
                if n == 0 {
                    Probe::None
                } else {
                    Probe::Value(format!("{n} stack(s)"))
                }
            }),
        }
    }

    /// Human-readable, for the environment pane.
    #[must_use]
    pub fn render(&self) -> String {
        let mut s = String::new();
        let _ = writeln!(s, "Docker           {}", self.docker);
        let _ = writeln!(s, "  compose        {}", self.compose_projects);
        s.push('\n');
        let _ = writeln!(s, "Kube context     {}", self.kube_context);
        if let Probe::Value(ctx) = &self.kube_context {
            if !ctx.starts_with("kind-") {
                s.push_str("  ! kind-smoke and operator-e2e will target THIS context\n");
            }
        }
        let _ = writeln!(s, "  kind clusters  {}", self.kind_clusters);
        s.push('\n');
        if self.stray_brokers > 0 {
            let _ = writeln!(
                s,
                "Local processes  ! {} stray mqttd process(es)\n                 \
                 not started by mqttui — almost certainly orphans from a panicking test;\n                 \
                 they hold ports and slow later runs. `k` kills them.",
                self.stray_brokers
            );
        } else {
            s.push_str("Local processes  no stray mqttd processes\n");
        }
        s
    }
}

/// Run a probe with a bound, so an unreachable Kubernetes context reports rather than
/// freezing the interface.
fn run_probe(bin: &str, args: &[&str], parse: impl Fn(&str) -> Probe) -> Probe {
    if !crate::preflight::on_path(bin) {
        return Probe::Unavailable("not installed".into());
    }
    let Ok(mut child) = Command::new(bin)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .stdin(Stdio::null())
        .spawn()
    else {
        return Probe::Unavailable("could not start".into());
    };

    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                if !status.success() {
                    return Probe::Unavailable("command failed".into());
                }
                let mut out = String::new();
                if let Some(mut s) = child.stdout.take() {
                    use std::io::Read as _;
                    let _ = s.read_to_string(&mut out);
                }
                return parse(&out);
            }
            Ok(None) if std::time::Instant::now() > deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Probe::Unavailable("timed out".into());
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(25)),
            Err(_) => return Probe::Unavailable("could not wait".into()),
        }
    }
}

/// PIDs of broker processes this machine is running that mqttui did not start.
#[must_use]
pub fn stray_brokers() -> Vec<String> {
    let Ok(out) = Command::new("pgrep")
        .args(["-f", "target/debug/mqttd"])
        .stderr(Stdio::null())
        .output()
    else {
        return Vec::new();
    };
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty())
        .map(ToString::to_string)
        .collect()
}

/// Kill the stray brokers. Only ever called from an explicit key press.
pub fn kill_stray_brokers() -> usize {
    let pids = stray_brokers();
    for pid in &pids {
        let _ = Command::new("kill")
            .args(["-9", pid])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
    pids.len()
}

/// What survived a run, as text.
///
/// The honest half of the teardown promise (ADR 0056 §4): the process group was signalled
/// and waited for, which is what makes each script's own `trap EXIT` run. That much is
/// guaranteed. A script whose trap is buggy — or which was `SIGKILL`ed — can still leak,
/// and mqttui cannot make another program's cleanup correct. So it looks, and says.
#[must_use]
pub fn report(task_id: &str) -> String {
    let strays = stray_brokers();
    let env = Environment::probe();
    let mut s = format!("mqttui: {task_id} stopped.\n");

    if strays.is_empty() {
        s.push_str("  no stray mqttd processes\n");
    } else {
        let _ = writeln!(
            s,
            "  ! {} stray mqttd process(es) remain: {}\n    \
             the script's trap did not remove them. `mqttui` will show these under E.",
            strays.len(),
            strays.join(" ")
        );
    }
    if let Probe::Value(clusters) = &env.kind_clusters {
        let _ = writeln!(
            s,
            "  ! kind cluster(s) still present: {clusters}\n    \
             delete with `kind delete cluster --name <name>` if the run created them."
        );
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    /// "Could not ask" and "there are none" must never render the same, or the panel
    /// starts asserting the absence of things it never checked.
    #[test]
    fn unavailable_and_none_are_distinguishable() {
        assert_eq!(Probe::None.to_string(), "none");
        assert!(Probe::Unavailable("not installed".into())
            .to_string()
            .contains("unavailable"));
        assert_ne!(
            Probe::None.to_string(),
            Probe::Unavailable("x".into()).to_string()
        );
    }

    /// A probe for a tool that is not installed answers Unavailable rather than hanging or
    /// claiming there is nothing there.
    #[test]
    fn a_missing_tool_is_unavailable_not_empty() {
        let p = run_probe("mqttui-no-such-binary", &["--version"], |_| Probe::None);
        assert!(matches!(p, Probe::Unavailable(_)));
    }

    /// A probe that answers is parsed.
    #[test]
    fn a_working_probe_is_parsed() {
        let p = run_probe("echo", &["hello"], |out| Probe::Value(out.trim().into()));
        assert!(matches!(p, Probe::Value(v) if v == "hello"));
    }

    /// The report always states the stray-process finding — silence would read as "nothing
    /// leaked", which is exactly the claim this cannot make.
    #[test]
    fn the_report_always_says_something_about_strays() {
        let r = report("some-task");
        assert!(
            r.contains("stray mqttd"),
            "the report must state what it found either way; got: {r}"
        );
    }
}
