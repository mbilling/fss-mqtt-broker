//! Durable-path stage timings, and a diagnostic stand-in for the device barrier.
//!
//! `mqtt-cluster` knows where a durable append spends its time — the writer's group
//! commit, the data sync, the quorum wait, the in-order release — but does not depend on
//! the metrics crate. So it RECORDS stage durations into a process-wide sink, and the
//! binary installs one that turns them into `mqttd_durable_stage_seconds`. With no sink
//! installed, [`record`] is one uncontended `OnceLock` load.
//!
//! The diagnostic half exists for efficiency analysis on a single machine, where the
//! device barrier is whatever that machine's disk does (`F_FULLFSYNC` on macOS is ~4ms,
//! a datacentre SSD with power-loss protection a few tens of µs). Compiled only with the
//! `diag-simulated-fsync` feature, `MQTTD_DIAG_SIMULATED_FSYNC_US` replaces EVERY data
//! sync in the segment log with a sleep of that length, so a fsync-latency sweep is
//! controlled and repeatable. A build with the feature is not durable and must never
//! ship; without the feature none of it exists.

use std::sync::OnceLock;
use std::time::Duration;

/// One stage of a durable append (the `{stage}` label).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    /// Submit until this node's own copy is durable (writer queue + commit + reply).
    LocalDurable,
    /// Own copy durable until the quorum is met — the followers' commits and the round trip.
    Quorum,
    /// Quorum met until the in-order commit releases the offset (head-of-line wait).
    Order,
    /// One group commit on a shard writer, data sync included.
    Commit,
    /// The data sync alone.
    Fsync,
    /// Leader side, per follower: Replicate queued to the link until its ack is
    /// back — the peer bus both ways plus the follower's whole apply.
    ReplicateRtt,
    /// Follower side: Replicate received until its ack is put on the link — the
    /// follower's writer queue wait plus its group commit. `replicate_rtt` minus
    /// this is the network and the two peer-bus queues.
    ReplicaApply,
}

impl Stage {
    /// The bounded label value.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Stage::LocalDurable => "local_durable",
            Stage::Quorum => "quorum",
            Stage::Order => "order",
            Stage::Commit => "commit",
            Stage::Fsync => "fsync",
            Stage::ReplicateRtt => "replicate_rtt",
            Stage::ReplicaApply => "replica_apply",
        }
    }
}

type Sink = Box<dyn Fn(Stage, Duration) + Send + Sync>;

static SINK: OnceLock<Sink> = OnceLock::new();

/// Install the process's stage sink. The first call wins (one broker per process);
/// returns whether this one was installed.
pub fn set_sink(sink: impl Fn(Stage, Duration) + Send + Sync + 'static) -> bool {
    SINK.set(Box::new(sink)).is_ok()
}

/// Record one stage duration; a no-op until a sink is installed.
#[inline]
pub fn record(stage: Stage, elapsed: Duration) {
    if let Some(sink) = SINK.get() {
        sink(stage, elapsed);
    }
}

/// DIAGNOSTIC: the simulated barrier length, if this build has the feature and
/// `MQTTD_DIAG_SIMULATED_FSYNC_US` is set. Read once.
#[cfg(feature = "diag-simulated-fsync")]
#[must_use]
pub fn simulated_fsync() -> Option<Duration> {
    static SIM: OnceLock<Option<Duration>> = OnceLock::new();
    *SIM.get_or_init(|| {
        let us = std::env::var("MQTTD_DIAG_SIMULATED_FSYNC_US").ok()?;
        let us: u64 = us.trim().parse().ok()?;
        eprintln!(
            "WARNING: MQTTD_DIAG_SIMULATED_FSYNC_US={us}: every segment-log data sync is a \
             {us}us SLEEP — this build is NOT durable (diag-simulated-fsync)"
        );
        Some(Duration::from_micros(us))
    })
}

/// Without the feature there is no simulation, ever.
#[cfg(not(feature = "diag-simulated-fsync"))]
#[must_use]
#[inline]
pub fn simulated_fsync() -> Option<Duration> {
    None
}

#[cfg(test)]
mod tests {
    use super::Stage;

    #[test]
    fn labels_are_the_documented_bounded_set() {
        let all = [
            Stage::LocalDurable,
            Stage::Quorum,
            Stage::Order,
            Stage::Commit,
            Stage::Fsync,
            Stage::ReplicateRtt,
            Stage::ReplicaApply,
        ];
        let labels: Vec<_> = all.iter().map(|s| s.label()).collect();
        assert_eq!(
            labels,
            [
                "local_durable",
                "quorum",
                "order",
                "commit",
                "fsync",
                "replicate_rtt",
                "replica_apply"
            ]
        );
    }

    #[cfg(not(feature = "diag-simulated-fsync"))]
    #[test]
    fn a_normal_build_never_simulates() {
        assert_eq!(super::simulated_fsync(), None);
    }
}
