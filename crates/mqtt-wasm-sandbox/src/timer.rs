//! The clock behind a call's deadline: one thread per loaded module that sleeps until
//! the earliest deadline of the calls now running, and then ticks: advances the
//! engine's epoch, which the module's code looks at in every loop and at every
//! function entry.
//!
//! While no call with a deadline runs the thread waits on a condition variable: it
//! does not tick. While a call is overdue (asked to stop and not yet back, or inside a
//! host function) it ticks every [`OVERDUE_TICK`], so no tick can be missed.

use std::collections::BTreeSet;
use std::io;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// The time between two ticks while a call is past its deadline and still running.
const OVERDUE_TICK: Duration = Duration::from_micros(250);

/// A wait no longer than this is taken in one piece.
const FINE_WAIT: Duration = Duration::from_millis(1);

#[derive(Default)]
struct State {
    /// The deadline of every call now running that has one, with a number that keeps
    /// two equal instants apart.
    deadlines: BTreeSet<(Instant, u64)>,
    next: u64,
    closed: bool,
}

#[derive(Default)]
struct Shared {
    state: Mutex<State>,
    changed: Condvar,
}

impl Shared {
    /// The state, also after a panic elsewhere: every update of it is one statement.
    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The timer of one engine. Dropping it ends its thread.
pub(crate) struct Timer {
    shared: Arc<Shared>,
}

/// A deadline registered with the timer for as long as this lives.
pub(crate) struct Armed {
    registered: Option<(Arc<Shared>, (Instant, u64))>,
}

impl Timer {
    /// Starts the thread, which waits until a deadline is registered. `tick` is what
    /// it does when one passes.
    pub(crate) fn start(tick: impl Fn() + Send + 'static) -> io::Result<Self> {
        let shared = Arc::new(Shared::default());
        let theirs = Arc::clone(&shared);
        std::thread::Builder::new()
            .name("mqttd-wasm-timer".to_owned())
            .stack_size(64 * 1024)
            .spawn(move || run(&theirs, &tick))?;
        Ok(Self { shared })
    }

    /// Registers `deadline`: the timer ticks when it passes, and then again every
    /// [`OVERDUE_TICK`] until the returned guard is dropped. `None` registers nothing
    /// and costs nothing.
    pub(crate) fn arm(&self, deadline: Option<Instant>) -> Armed {
        let registered = deadline.map(|at| {
            let mut state = self.shared.state();
            let key = (at, state.next);
            state.next = state.next.wrapping_add(1);
            let earliest = state.deadlines.first().is_none_or(|first| key < *first);
            state.deadlines.insert(key);
            drop(state);
            if earliest {
                self.shared.changed.notify_one();
            }
            (Arc::clone(&self.shared), key)
        });
        Armed { registered }
    }
}

impl Drop for Armed {
    fn drop(&mut self) {
        if let Some((shared, key)) = self.registered.take() {
            shared.state().deadlines.remove(&key);
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.shared.state().closed = true;
        self.shared.changed.notify_one();
    }
}

fn run(shared: &Shared, tick: &impl Fn()) {
    let mut state = shared.state();
    while !state.closed {
        let now = Instant::now();
        let wait = match state.deadlines.first() {
            None => None,
            // A long wait may end late by a fraction of its length (the operating
            // system batches timers); approaching the deadline in shrinking steps
            // keeps the last one, and so the lateness, short.
            Some((at, _)) if *at > now => {
                let left = at.saturating_duration_since(now);
                Some(if left > FINE_WAIT {
                    left.saturating_sub(left / 8)
                } else {
                    left
                })
            }
            Some(_) => {
                drop(state);
                tick();
                state = shared.state();
                Some(OVERDUE_TICK)
            }
        };
        state = match wait {
            None => shared
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner),
            Some(time) => {
                shared
                    .changed
                    .wait_timeout(state, time)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0
            }
        };
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn counting() -> (Timer, Arc<AtomicU64>) {
        let ticks = Arc::new(AtomicU64::new(0));
        let theirs = Arc::clone(&ticks);
        let timer = Timer::start(move || {
            theirs.fetch_add(1, Ordering::SeqCst);
        })
        .expect("a thread");
        (timer, ticks)
    }

    /// Polls until `ticks` has reached `at_least`, for at most ten seconds.
    fn wait_for(ticks: &AtomicU64, at_least: u64) {
        let until = Instant::now() + Duration::from_secs(10);
        while ticks.load(Ordering::SeqCst) < at_least {
            assert!(Instant::now() < until, "no tick within ten seconds");
            std::thread::yield_now();
        }
    }

    #[test]
    fn a_deadline_ticks_when_it_passes_and_not_before() {
        let (timer, ticks) = counting();
        let started = Instant::now();
        let armed = timer.arm(Some(started + Duration::from_millis(40)));
        wait_for(&ticks, 1);
        assert!(
            started.elapsed() >= Duration::from_millis(40),
            "ticked early, after {:?}",
            started.elapsed()
        );
        drop(armed);
    }

    #[test]
    fn an_overdue_call_is_ticked_until_it_is_gone_and_then_nothing_ticks() {
        let (timer, ticks) = counting();
        let armed = timer.arm(Some(Instant::now()));
        wait_for(&ticks, 5);
        drop(armed);
        // The thread may be in the middle of one last tick; after that it has no
        // deadline and waits. A deadline a day away does not wake it either.
        let far = timer.arm(Some(Instant::now() + Duration::from_hours(24)));
        let until = Instant::now() + Duration::from_secs(10);
        let settled = loop {
            let seen = ticks.load(Ordering::SeqCst);
            std::thread::sleep(OVERDUE_TICK * 8);
            if ticks.load(Ordering::SeqCst) == seen {
                break seen;
            }
            assert!(
                Instant::now() < until,
                "the timer still ticks with no overdue call"
            );
        };
        assert_eq!(timer.shared.state().deadlines.len(), 1);
        drop(far);
        assert!(timer.shared.state().deadlines.is_empty());
        assert_eq!(ticks.load(Ordering::SeqCst), settled);
    }

    #[test]
    fn no_deadline_registers_nothing() {
        let (timer, ticks) = counting();
        let armed = timer.arm(None);
        assert!(timer.shared.state().deadlines.is_empty());
        drop(armed);
        assert_eq!(ticks.load(Ordering::SeqCst), 0);
    }
}
