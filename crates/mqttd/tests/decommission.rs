//! `mqttd --decommission` (ADR 0047 T4): the Kubernetes `preStop` primitive — signal the running
//! broker (PID 1 in a distroless container has no shell/`kill`) and block until its drain +
//! graceful shutdown finishes. These drive the real binary against a throwaway target process.

#![cfg(unix)]

use std::io::Read as _;
use std::process::{Command, Stdio};

fn mqttd() -> Command {
    Command::new(env!("CARGO_BIN_EXE_mqttd"))
}

#[test]
fn signalling_a_nonexistent_pid_is_a_clean_error_exit_2() {
    // A pid that does not exist can't be signalled — reported, exit 2 (usage/signal error), and it
    // does not hang waiting.
    let out = mqttd()
        .args(["--decommission", "--pid", "2147483646", "--timeout", "1"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2), "expected a signal error exit");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("cannot signal"), "stderr was: {stderr}");
}

#[test]
fn a_zero_pid_is_a_usage_error() {
    // pid 0 means "the whole process group" to kill(2) — reject it; --decommission targets one pid.
    let out = mqttd()
        .args(["--decommission", "--pid", "0"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn it_signals_the_target_and_waits_for_it_to_exit() {
    // A target that exits on SIGUSR1 (as the broker does after its decommission drain): --decommission
    // must deliver the signal, observe the exit, and return 0 — proving the signal + wait-for-drain
    // contract the preStop relies on.
    let mut target = Command::new("sh")
        .arg("-c")
        // Exit 0 when SIGUSR1 arrives; otherwise linger (the wait would then time out).
        .arg("trap 'exit 0' USR1; for i in $(seq 1 100); do sleep 0.1; done")
        .stdout(Stdio::null())
        .spawn()
        .expect("spawn target");
    let pid = target.id().to_string();

    let mut child = mqttd()
        .args(["--decommission", "--pid", &pid, "--timeout", "10"])
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn mqttd --decommission");
    let status = child.wait().expect("wait decommission");
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .unwrap()
        .read_to_string(&mut stdout)
        .ok();

    assert!(
        status.success(),
        "decommission should exit 0 once the target drains; stdout was: {stdout}"
    );
    assert!(stdout.contains("drain complete"), "stdout was: {stdout}");
    // The target really did exit (reaped); nothing left running.
    let _ = target.wait();
}
