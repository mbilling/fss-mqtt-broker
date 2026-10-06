//! Async-runtime scheduling delay, measured from inside the runtime (#662).
//!
//! On 8-vCPU brokers the durable knee arrived with ~35% of CPU idle, the peer-link
//! task ~10% busy and the hub task ~50% busy, while every task that waits to be
//! *scheduled* waited longer (lane queue 0.4 -> 9 ms, link transit 1.7 -> 15 ms). This
//! samples that delay directly, 100 times a second per probe:
//!
//! - `yield`: one `yield_now()` round trip, i.e. how long a ready task waits behind the
//!   other ready tasks on the runtime;
//! - `channel`: a message stamped by one task until another wakes to receive it, the
//!   hand-off a durable append makes several times (hub -> lane -> link -> ack).
//!
//! Two tiny tasks and 200 wake-ups a second: negligible against the workload.

use mqtt_observability::metrics::Metrics;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

const PERIOD: Duration = Duration::from_millis(10);

/// Spawn the probes on the current runtime. They run for the process's lifetime.
pub fn spawn(metrics: Arc<Metrics>) {
    let yields = Arc::clone(&metrics);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PERIOD);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            let started = Instant::now();
            tokio::task::yield_now().await;
            yields.observe_runtime_wake("yield", started.elapsed().as_secs_f64());
        }
    });
    let (tx, mut rx) = mpsc::channel::<Instant>(64);
    tokio::spawn(async move {
        while let Some(sent) = rx.recv().await {
            metrics.observe_runtime_wake("channel", sent.elapsed().as_secs_f64());
        }
    });
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PERIOD);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tick.tick().await;
            if tx.send(Instant::now()).await.is_err() {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both probes sample on a real runtime and land in `mqttd_runtime_wake_seconds`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn both_probes_feed_the_wake_histogram() {
        let metrics = Arc::new(Metrics::new("test"));
        spawn(Arc::clone(&metrics));
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let out = metrics.render();
            let count = |probe: &str| {
                out.lines()
                    .find(|l| {
                        l.starts_with(&format!(
                            "mqttd_runtime_wake_seconds_count{{probe=\"{probe}\"}}"
                        ))
                    })
                    .and_then(|l| l.rsplit_once(' '))
                    .and_then(|(_, v)| v.trim().parse::<f64>().ok())
                    .unwrap_or(0.0)
            };
            if count("yield") >= 5.0 && count("channel") >= 5.0 {
                break;
            }
            assert!(Instant::now() < deadline, "probes never sampled:\n{out}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}
