//! The sandbox against a module written out by hand, byte by byte
//! ([`crate::test_module`]): these tests need no Wasm toolchain and no jq. What the
//! module's one function does is chosen by the first letter of the name it is called by.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::jq::{Pool, PoolStats, Tag};
use crate::test_module::{module, Shape};
use crate::{
    Arg, CallError, Execution, Grants, HandOff, Limits, LoadError, Reply, Sandbox, ABI_VERSION,
};

fn sandbox(limits: Limits) -> Sandbox {
    Sandbox::load(&module(&Shape::default()), limits, Grants::default())
        .expect("the test module loads")
}

#[allow(clippy::unnecessary_wraps)] // a deadline, as `call` takes one
fn soon() -> Option<Instant> {
    Some(Instant::now() + Duration::from_secs(30))
}

#[test]
fn a_call_returns_the_modules_value() {
    let sandbox = sandbox(Limits::default());
    assert_eq!(sandbox.description(), b"{}");
    let mut instance = sandbox.instantiate().unwrap();
    let reply = instance.call("true", &[Arg::Json("1"), Arg::Binary(b"\xff\0")], soon());
    assert_eq!(reply, Ok(Reply::Json(b"true".to_vec())));
    assert!(!instance.is_poisoned());
    // No fuel is counted unless a fuel bound was asked for.
    assert_eq!(instance.fuel_used(), 0);
}

#[test]
fn a_modules_own_error_is_its_status_and_message_and_the_instance_lives_on() {
    let mut instance = sandbox(Limits::default()).instantiate().unwrap();
    let error = instance.call("error", &[], soon()).unwrap_err();
    assert_eq!(
        error,
        CallError::Module {
            status: 7,
            message: b"nope".to_vec()
        }
    );
    assert!(!instance.is_poisoned());
    assert_eq!(
        instance.call("t", &[], soon()),
        Ok(Reply::Json(b"true".to_vec()))
    );
}

#[test]
fn the_arguments_arrive_as_one_counted_frame() {
    let mut instance = sandbox(Limits::default()).instantiate().unwrap();
    let args = [Arg::Json("1"), Arg::Binary(b"two"), Arg::Json("[3]")];
    let error = instance.call("n", &args, soon()).unwrap_err();
    assert_eq!(
        error,
        CallError::Module {
            status: 3,
            message: b"nope".to_vec()
        }
    );
}

#[test]
fn an_endless_loop_stops_at_the_deadline_and_poisons_only_its_instance() {
    let sandbox = sandbox(Limits::default());
    let mut instance = sandbox.instantiate().unwrap();
    let started = Instant::now();
    let error = instance.call("loop", &[], Some(started + Duration::from_millis(50)));
    let took = started.elapsed();
    assert_eq!(error, Err(CallError::Timeout));
    assert!(
        took >= Duration::from_millis(50),
        "stopped early, after {took:?}"
    );
    assert!(
        took < Duration::from_secs(5),
        "stopped late, after {took:?}"
    );
    assert!(instance.is_poisoned());
    assert_eq!(instance.call("t", &[], soon()), Err(CallError::Poisoned));
    // The module is whole: the next instance of it answers.
    let mut next = sandbox.instantiate().unwrap();
    assert_eq!(
        next.call("t", &[], soon()),
        Ok(Reply::Json(b"true".to_vec()))
    );
}

#[test]
fn a_fuel_bound_stops_the_same_work_at_the_same_point_every_time() {
    let limits = Limits {
        max_fuel: Some(1_000_000),
        ..Limits::default()
    };
    // Compiled or interpreted, the count is the same: fuel is the module's
    // instructions, not the machine's.
    let burnt: Vec<u64> = [Execution::Compiled, Execution::Interpreted]
        .into_iter()
        .flat_map(|execution| {
            let sandbox = Sandbox::load_as(
                &module(&Shape::default()),
                execution,
                limits.clone(),
                Grants::default(),
            )
            .expect("the test module loads with a fuel bound");
            (0..2).map(move |_| {
                let mut instance = sandbox.instantiate().unwrap();
                assert_eq!(
                    instance.call("t", &[], None),
                    Ok(Reply::Json(b"true".to_vec()))
                );
                assert_eq!(instance.call("loop", &[], None), Err(CallError::Fuel));
                instance.fuel_used()
            })
        })
        .collect();
    assert!(burnt.iter().all(|&used| used == burnt[0]), "{burnt:?}");
    assert!(burnt[0] >= 1_000_000);
}

/// Fuel is for calls. The runtime also bills making an instance to it (16,385 units
/// for this module compiled, 65,537 interpreted), so a bound smaller than that used
/// to refuse every module at load.
#[test]
fn a_small_fuel_bound_still_loads_and_runs_a_module() {
    let limits = Limits {
        max_fuel: Some(200),
        ..Limits::default()
    };
    for execution in [Execution::Compiled, Execution::Interpreted] {
        let sandbox = Sandbox::load_as(
            &module(&Shape::default()),
            execution,
            limits.clone(),
            Grants::default(),
        )
        .unwrap_or_else(|e| panic!("{execution:?}: {e}"));
        let mut instance = sandbox.instantiate().unwrap();
        assert_eq!(
            instance.call("t", &[], None),
            Ok(Reply::Json(b"true".to_vec()))
        );
        assert!(instance.fuel_used() <= 200, "{}", instance.fuel_used());
        assert_eq!(instance.call("loop", &[], None), Err(CallError::Fuel));
    }
}

#[test]
fn memory_growth_stops_at_the_cap() {
    let limits = Limits {
        memory_bytes: 8 << 20,
        ..Limits::default()
    };
    let mut instance = sandbox(limits).instantiate().unwrap();
    assert_eq!(instance.call("grow", &[], soon()), Err(CallError::Memory));
    assert!(instance.memory_bytes() <= 8 << 20);
    assert!(instance.is_poisoned());
}

#[test]
fn endless_recursion_overflows_the_modules_stack_and_not_the_hosts() {
    // A call runs on a stack of its own, as large as the limit: the thread calling
    // here has a quarter of a mebibyte, and the module recurses through four
    // mebibytes before it is stopped. Not one of its frames is on this thread's stack.
    for execution in [Execution::Compiled, Execution::Interpreted] {
        let limits = Limits::default();
        assert_eq!(limits.max_stack_bytes, 4 << 20);
        let sandbox = Sandbox::load_as(
            &module(&Shape::default()),
            execution,
            limits,
            Grants::default(),
        )
        .unwrap();
        let outcome = std::thread::Builder::new()
            .stack_size(256 * 1024)
            .spawn(move || {
                let mut instance = sandbox.instantiate().unwrap();
                let outcome = instance.call("recurse", &[], soon());
                assert!(instance.is_poisoned());
                // The module is whole: the next instance answers, on the same thread.
                let mut next = sandbox.instantiate().unwrap();
                assert_eq!(
                    next.call("t", &[], soon()),
                    Ok(Reply::Json(b"true".to_vec()))
                );
                outcome
            })
            .unwrap()
            .join()
            .expect("the host thread survives");
        assert_eq!(outcome, Err(CallError::Stack), "{execution:?}");
    }
}

#[test]
fn a_trap_ends_the_call() {
    let mut instance = sandbox(Limits::default()).instantiate().unwrap();
    let error = instance.call("x", &[], soon()).unwrap_err();
    assert!(matches!(error, CallError::Trap(_)), "{error:?}");
    assert!(instance.is_poisoned());
}

#[test]
fn a_result_larger_than_the_limit_is_refused_unread() {
    let mut instance = sandbox(Limits::default()).instantiate().unwrap();
    assert_eq!(
        instance.call("big", &[], soon()),
        Err(CallError::OutputTooLarge)
    );
    assert!(instance.is_poisoned());
}

/// A caller with a budget gives a call what is left of it: the same result that fits
/// the module's bound is refused under the call's own.
#[test]
fn a_call_is_held_to_its_own_bound_on_the_result() {
    let sandbox = sandbox(Limits::default());
    let mut instance = sandbox.instantiate().unwrap();
    // `jtrue` is five bytes; the host reads a result of up to the bound and one more.
    assert_eq!(
        instance.call_within("t", &[], soon(), 4),
        Ok(Reply::Json(b"true".to_vec()))
    );
    assert_eq!(
        instance.call_within("t", &[], soon(), 3),
        Err(CallError::OutputTooLarge)
    );
    // The call's bound never raises the module's.
    let small = Sandbox::load(
        &module(&Shape::default()),
        Limits {
            max_output_bytes: 3,
            ..Limits::default()
        },
        Grants::default(),
    )
    .unwrap();
    assert_eq!(
        small
            .instantiate()
            .unwrap()
            .call_within("t", &[], soon(), u32::MAX),
        Err(CallError::OutputTooLarge)
    );
}

/// A hand-off that counts how often it was given the rest of a call, and runs it.
fn counting_hand_off() -> (HandOff, Arc<AtomicU32>) {
    let count = Arc::new(AtomicU32::new(0));
    let theirs = Arc::clone(&count);
    let hand_off: HandOff = Arc::new(move |rest: &mut dyn FnMut()| {
        theirs.fetch_add(1, Ordering::SeqCst);
        rest();
    });
    (hand_off, count)
}

/// ADR 0086 D6: a call that is still running after its first slice is suspended once,
/// and what is left of it runs inside the module's hand-off, to the same end it would
/// have had. A call that is done within the slice never meets the hand-off, and neither
/// does one whose deadline comes before the slice ends.
#[test]
fn a_call_that_outlasts_its_first_slice_goes_on_inside_the_hand_off() {
    let (hand_off, handed) = counting_hand_off();
    let limits = Limits {
        first_slice: Duration::from_millis(5),
        ..Limits::default()
    };
    let sandbox = sandbox(limits).with_hand_off(hand_off);
    let mut instance = sandbox.instantiate().unwrap();
    assert_eq!(
        instance.call("t", &[], soon()),
        Ok(Reply::Json(b"true".to_vec()))
    );
    assert_eq!(
        handed.load(Ordering::SeqCst),
        0,
        "a quick call was handed off"
    );

    // The endless loop: suspended after five milliseconds, stopped at its deadline.
    let started = Instant::now();
    let outcome = instance.call("loop", &[], Some(started + Duration::from_millis(80)));
    let took = started.elapsed();
    assert_eq!(outcome, Err(CallError::Timeout));
    assert_eq!(handed.load(Ordering::SeqCst), 1, "handed off once");
    assert!(
        took >= Duration::from_millis(80) && took < Duration::from_secs(5),
        "stopped after {took:?}"
    );

    // A deadline inside the first slice: the call is over before it could be handed on.
    let mut instance = sandbox.instantiate().unwrap();
    let started = Instant::now();
    let outcome = instance.call("loop", &[], Some(started + Duration::from_millis(2)));
    assert_eq!(outcome, Err(CallError::Timeout));
    assert_eq!(handed.load(Ordering::SeqCst), 1);

    // No deadline at all: handed off like any other, and still running until the
    // memory cap stops it (`g` grows its memory for ever).
    let slow = Limits {
        first_slice: Duration::ZERO,
        ..Limits::default()
    };
    let (hand_off, handed) = counting_hand_off();
    let mut instance = self::sandbox(slow)
        .with_hand_off(hand_off)
        .instantiate()
        .unwrap();
    assert_eq!(instance.call("grow", &[], None), Err(CallError::Memory));
    assert!(handed.load(Ordering::SeqCst) <= 1);
}

/// A hand-off that does not run what it is given, or runs it twice, changes nothing:
/// the call still runs to its end, once.
#[test]
fn a_hand_off_that_misbehaves_does_not_lose_or_repeat_the_call() {
    let limits = || Limits {
        first_slice: Duration::from_millis(1),
        ..Limits::default()
    };
    let lazy: HandOff = Arc::new(|_rest: &mut dyn FnMut()| {});
    let eager: HandOff = Arc::new(|rest: &mut dyn FnMut()| {
        rest();
        rest();
    });
    for hand_off in [lazy, eager] {
        let mut instance = sandbox(limits())
            .with_hand_off(hand_off)
            .instantiate()
            .unwrap();
        let started = Instant::now();
        let outcome = instance.call("loop", &[], Some(started + Duration::from_millis(30)));
        assert_eq!(outcome, Err(CallError::Timeout));
        assert!(started.elapsed() >= Duration::from_millis(30));
    }
}

/// The interpreter suspends and resumes a call as compiled code does.
#[test]
fn an_interpreted_call_is_handed_off_too() {
    let (hand_off, handed) = counting_hand_off();
    let limits = Limits {
        first_slice: Duration::from_millis(5),
        ..Limits::default()
    };
    let sandbox = Sandbox::load_as(
        &module(&Shape::default()),
        Execution::Interpreted,
        limits,
        Grants::default(),
    )
    .expect("the test module loads")
    .with_hand_off(hand_off);
    let mut instance = sandbox.instantiate().unwrap();
    assert_eq!(
        instance.call("t", &[], soon()),
        Ok(Reply::Json(b"true".to_vec()))
    );
    assert_eq!(handed.load(Ordering::SeqCst), 0);
    let started = Instant::now();
    let outcome = instance.call("loop", &[], Some(started + Duration::from_millis(60)));
    assert_eq!(outcome, Err(CallError::Timeout));
    assert_eq!(handed.load(Ordering::SeqCst), 1);
    assert!(started.elapsed() >= Duration::from_millis(60));
}

/// While one call is in its hand-off, calls on other threads are not suspended by its
/// ticks: each is handed off at the end of its own first slice, and not before.
#[test]
fn every_call_is_handed_off_at_the_end_of_its_own_first_slice() {
    let (hand_off, handed) = counting_hand_off();
    let limits = Limits {
        first_slice: Duration::from_millis(200),
        ..Limits::default()
    };
    let sandbox = sandbox(limits).with_hand_off(hand_off);
    std::thread::scope(|scope| {
        // Four loops, each stopped at 30 ms: well inside its first slice.
        for _ in 0..4 {
            let sandbox = &sandbox;
            scope.spawn(move || {
                let mut instance = sandbox.instantiate().unwrap();
                let deadline = Some(Instant::now() + Duration::from_millis(30));
                assert_eq!(
                    instance.call("loop", &[], deadline),
                    Err(CallError::Timeout)
                );
            });
        }
    });
    assert_eq!(handed.load(Ordering::SeqCst), 0);
}

#[test]
fn instances_share_no_state() {
    let sandbox = sandbox(Limits::default());
    let next_address = |instance: &mut crate::Instance| match instance.call("p", &[], soon()) {
        Err(CallError::Module { status, .. }) => status,
        other => panic!("{other:?}"),
    };
    let mut first = sandbox.instantiate().unwrap();
    let fresh = next_address(&mut first);
    for _ in 0..10 {
        first.call("t", &[Arg::Binary(&[0; 1000])], soon()).unwrap();
    }
    assert!(next_address(&mut first) > fresh + 10_000);
    // A second instance starts where the first one started, whatever the first did.
    let mut second = sandbox.instantiate().unwrap();
    assert_eq!(next_address(&mut second), fresh);
}

#[test]
fn an_import_outside_the_list_is_refused_at_load() {
    for (from, what) in [("env", "system"), ("wasi_snapshot_preview1", "sock_send")] {
        let shape = Shape {
            import: Some((from, what)),
            ..Shape::default()
        };
        let error = Sandbox::load(&module(&shape), Limits::default(), Grants::default());
        match error {
            Err(LoadError::Import { module, name }) => {
                assert_eq!((module.as_str(), name.as_str()), (from, what));
            }
            other => panic!("{other:?}"),
        }
    }
}

#[test]
fn an_import_on_the_list_is_provided() {
    let shape = Shape {
        import: Some(("wasi_snapshot_preview1", "fd_close")),
        ..Shape::default()
    };
    let sandbox = Sandbox::load(&module(&shape), Limits::default(), Grants::default()).unwrap();
    let mut instance = sandbox.instantiate().unwrap();
    assert_eq!(
        instance.call("t", &[], soon()),
        Ok(Reply::Json(b"true".to_vec()))
    );
    assert_eq!(instance.call("recurse", &[], soon()), Err(CallError::Stack));
}

#[test]
fn another_abi_version_is_refused_at_load() {
    let shape = Shape {
        version: ABI_VERSION + 1,
        ..Shape::default()
    };
    let error = Sandbox::load(&module(&shape), Limits::default(), Grants::default());
    assert!(
        matches!(error, Err(LoadError::AbiVersion(v)) if v == ABI_VERSION + 1),
        "{error:?}"
    );
}

#[test]
fn a_module_without_the_abi_is_refused_at_load() {
    let shape = Shape {
        without_call: true,
        ..Shape::default()
    };
    let error = Sandbox::load(&module(&shape), Limits::default(), Grants::default());
    assert!(matches!(error, Err(LoadError::Export(_))), "{error:?}");
}

#[test]
fn bytes_that_are_not_a_module_are_refused_at_load() {
    let mut cut = module(&Shape::default());
    cut.truncate(cut.len() - 7);
    for bytes in [b"not wasm at all".as_slice(), &cut] {
        let error = Sandbox::load(bytes, Limits::default(), Grants::default());
        assert!(matches!(error, Err(LoadError::Invalid(_))), "{error:?}");
    }
}

#[test]
fn a_call_runs_the_same_interpreted() {
    let sandbox = Sandbox::load_as(
        &module(&Shape::default()),
        Execution::Interpreted,
        Limits::default(),
        Grants::default(),
    )
    .unwrap();
    let mut instance = sandbox.instantiate().unwrap();
    assert_eq!(
        instance.call("t", &[Arg::Json("1")], soon()),
        Ok(Reply::Json(b"true".to_vec()))
    );
    assert_eq!(
        instance.call("error", &[], soon()),
        Err(CallError::Module {
            status: 7,
            message: b"nope".to_vec()
        })
    );
    let started = Instant::now();
    let stopped = instance.call("loop", &[], Some(started + Duration::from_millis(30)));
    assert_eq!(stopped, Err(CallError::Timeout));
    assert!(started.elapsed() >= Duration::from_millis(30));
    let mut grower = sandbox.instantiate().unwrap();
    assert_eq!(grower.call("grow", &[], soon()), Err(CallError::Memory));
}

#[test]
fn this_host_runs_both_kinds_of_code() {
    // The test hosts allow executable memory. One that does not gets
    // `LoadError::Engine` here for the first, which `examples/probe.rs` checks under
    // systemd's MemoryDenyWriteExecute in CI.
    Execution::Compiled.probe().expect("compiled code runs");
    Execution::Interpreted
        .probe()
        .expect("the interpreter runs");
}

#[test]
fn every_call_stops_at_its_own_deadline_and_a_call_without_one_is_left_alone() {
    let sandbox = sandbox(Limits::default());
    std::thread::scope(|scope| {
        let quiet = scope.spawn(|| {
            // No deadline: the other calls' ticks pass this one by, and it runs on.
            // (A new instance now and then: the test module's allocator never frees.)
            let until = Instant::now() + Duration::from_millis(200);
            let mut calls = 0u32;
            while Instant::now() < until {
                let mut instance = sandbox.instantiate().unwrap();
                for _ in 0..500 {
                    assert_eq!(
                        instance.call("t", &[], None),
                        Ok(Reply::Json(b"true".to_vec()))
                    );
                    calls += 1;
                }
            }
            calls
        });
        let loops: Vec<_> = [20u64, 60, 100, 140, 180]
            .into_iter()
            .map(|ms| {
                let sandbox = &sandbox;
                scope.spawn(move || {
                    let mut instance = sandbox.instantiate().unwrap();
                    let started = Instant::now();
                    let timeout = Duration::from_millis(ms);
                    let outcome = instance.call("loop", &[], Some(started + timeout));
                    (outcome, started.elapsed(), timeout)
                })
            })
            .collect();
        for looping in loops {
            let (outcome, took, timeout) = looping.join().unwrap();
            assert_eq!(outcome, Err(CallError::Timeout));
            assert!(took >= timeout, "{timeout:?} stopped early, after {took:?}");
            assert!(
                took < timeout + Duration::from_secs(5),
                "{timeout:?} stopped late, after {took:?}"
            );
        }
        assert!(quiet.join().unwrap() > 0);
    });
}

/// A small generator of the same numbers every run.
struct Numbers(u64);

impl Numbers {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }
}

/// Whatever bytes are handed to the loader, it answers: a module or a reason. And a
/// module it accepts can be instantiated and called under the limits, whatever its
/// code now does, without the host panicking or hanging.
#[test]
fn damaged_modules_are_refused_or_run_under_the_limits() {
    let whole = module(&Shape::default());
    let limits = Limits {
        memory_bytes: 1 << 20,
        max_stack_bytes: 256 * 1024,
        init_timeout: Duration::from_millis(200),
        ..Limits::default()
    };
    let mut numbers = Numbers(0x9e37_79b9_7f4a_7c15);
    let mut damaged: Vec<Vec<u8>> = (0..whole.len()).map(|cut| whole[..cut].to_vec()).collect();
    for _ in 0..600 {
        let mut bytes = whole.clone();
        for _ in 0..=numbers.below(3) {
            let at = numbers.below(bytes.len());
            match numbers.below(4) {
                0 => bytes[at] ^= 1 << numbers.below(8),
                1 => bytes[at] = u8::try_from(numbers.below(256)).unwrap(),
                2 => {
                    bytes.remove(at);
                }
                _ => bytes.insert(at, u8::try_from(numbers.below(256)).unwrap()),
            }
        }
        damaged.push(bytes);
    }
    for _ in 0..50 {
        let len = numbers.below(200);
        let mut bytes = b"\0asm\x01\0\0\0".to_vec();
        bytes.extend((0..len).map(|_| u8::try_from(numbers.below(256)).unwrap()));
        damaged.push(bytes);
    }
    let (mut loaded, mut refused) = (0u32, 0u32);
    for bytes in &damaged {
        match Sandbox::load(bytes, limits.clone(), Grants::default()) {
            Err(_) => refused += 1,
            Ok(sandbox) => {
                loaded += 1;
                let Ok(mut instance) = sandbox.instantiate() else {
                    continue;
                };
                for name in ["t", "error", "n", "big", "x", "grow", "recurse", "loop"] {
                    let deadline = Some(Instant::now() + Duration::from_millis(5));
                    let outcome = instance.call(name, &[Arg::Json("1")], deadline);
                    // A failure that is not the function's own ends the instance.
                    let own = matches!(outcome, Ok(_) | Err(CallError::Module { .. }));
                    assert_eq!(instance.is_poisoned(), !own, "{name}: {outcome:?}");
                    if !own {
                        break;
                    }
                }
                assert!(instance.memory_bytes() <= 1 << 20);
            }
        }
    }
    // Every truncation is refused; among the edits both outcomes occur.
    assert!(refused as usize >= whole.len(), "{refused} refused");
    assert!(loaded >= 5, "{loaded} loaded");
}

/// The module `jq::Pool` is given here is the hand-written one: its one function
/// answers `true` to any name, `jq` included, which is all the pool needs.
#[test]
fn a_pool_compiles_on_first_use_and_shares_warm_instances() {
    let pool = Pool::new(
        module(&Shape::default()),
        Execution::Compiled,
        Limits::default(),
        Grants::default(),
        2,
    );
    assert!(!pool.is_loaded());
    assert_eq!(pool.idle(), 0);
    assert_eq!(pool.eval(b".", Arg::Json("1"), None), Ok(b"true".to_vec()));
    assert!(pool.is_loaded());
    assert_eq!(pool.idle(), 1);
    // Eight callers at once: each gets an instance, and two are kept afterwards.
    std::thread::scope(|scope| {
        for _ in 0..8 {
            scope.spawn(|| {
                for _ in 0..50 {
                    assert_eq!(pool.eval(b".", Arg::Json("1"), None), Ok(b"true".to_vec()));
                }
            });
        }
    });
    assert!((1..=2).contains(&pool.idle()), "{} idle", pool.idle());
    assert!(pool.load().is_ok());
    assert!(!pool.refused_by_host());
    // What a broker exports: the instances kept and their memory (one page each for
    // this module), every instance ever made, none given up.
    let stats = pool.stats();
    let kept = u64::try_from(pool.idle()).unwrap();
    assert_eq!(
        (stats.instances, stats.memory_bytes, stats.replaced),
        (kept, kept * 65_536, 0),
        "{stats:?}"
    );
    assert!(stats.made >= kept, "{stats:?}");
}

/// A pool counts the instances its calls cost: one stopped at its deadline is given
/// up, and the next call is served by a new one. Its hand-off reaches every instance.
#[test]
fn a_pool_replaces_an_instance_a_call_stopped_and_counts_it() {
    let (hand_off, handed) = counting_hand_off();
    let limits = Limits {
        first_slice: Duration::from_millis(1),
        ..Limits::default()
    };
    let pool = Pool::new(
        module(&Shape::default()),
        Execution::Compiled,
        limits,
        Grants::default(),
        1,
    )
    .with_hand_off(hand_off);
    assert_eq!(pool.stats(), PoolStats::default());
    // The hand-written module's function loops for ever when called `l…`; the pool
    // calls it `jq`, so the endless call is made on an instance directly.
    let mut instance = pool.load().unwrap().instantiate().unwrap();
    let deadline = Some(Instant::now() + Duration::from_millis(20));
    assert_eq!(
        instance.call("loop", &[], deadline),
        Err(CallError::Timeout)
    );
    assert_eq!(handed.load(Ordering::SeqCst), 1);

    assert_eq!(pool.eval(b".", Arg::Json("1"), None), Ok(b"true".to_vec()));
    assert_eq!(pool.compile(b"."), Ok(()));
    assert_eq!(pool.warm(&[b".", b".a"]), Ok(()));
    let stats = pool.stats();
    assert_eq!(
        (stats.instances, stats.made, stats.replaced),
        (1, 1, 0),
        "{stats:?}"
    );
    // The result bound of one call, through the pool.
    let error = pool.eval_within(b".", Arg::Json("1"), None, 2).unwrap_err();
    assert_eq!((error.tag, error.kind), (Tag::Limit, "output"));
    let stats = pool.stats();
    assert_eq!(
        (
            stats.instances,
            stats.made,
            stats.replaced,
            stats.memory_bytes
        ),
        (0, 1, 1, 0),
        "the stopped instance is gone, and counted: {stats:?}"
    );
    assert_eq!(pool.eval(b".", Arg::Json("1"), None), Ok(b"true".to_vec()));
    assert_eq!(pool.stats().made, 2);
}

#[test]
fn a_pool_whose_module_cannot_load_says_why_on_every_call() {
    let pool = Pool::new(
        b"not a module".to_vec(),
        Execution::Compiled,
        Limits::default(),
        Grants::default(),
        2,
    );
    for _ in 0..2 {
        let error = pool.eval(b".", Arg::Json("1"), None).unwrap_err();
        assert_eq!(error.tag, Tag::System);
        let text = error.to_string();
        assert!(
            text.starts_with("jq_err_system: jq is not available: not a usable WebAssembly module"),
            "{text}"
        );
    }
    assert!(!pool.is_loaded());
    assert!(pool.load().is_err());
    assert_eq!(pool.idle(), 0);
    // The module's fault, not this host's: no operator setting would load it.
    assert!(!pool.refused_by_host());
    assert!(pool.warm(&[b"."]).is_err());
    assert_eq!(pool.stats(), PoolStats::default());
}

fn plugin_pool(max_idle: usize, fresh_instance: bool) -> crate::ModulePool {
    let wasm = crate::test_module::plugin("1", b"j41");
    let sandbox = Sandbox::load(&wasm, Limits::default(), Grants::default()).unwrap();
    crate::ModulePool::new(sandbox, max_idle, fresh_instance)
}

const A_SECOND: Duration = Duration::from_secs(1);

#[test]
fn the_test_plugin_describes_its_functions_and_returns_an_argument_as_it_came() {
    let pool = plugin_pool(2, false);
    let description = String::from_utf8(pool.sandbox().description().to_vec()).unwrap();
    assert!(
        description.starts_with("{\"module\":\"test\",\"version\":\"1\",\"functions\":["),
        "{description}"
    );
    for function in crate::test_module::FUNCTIONS {
        assert!(description.contains(&format!("\"name\":\"{function}\"")));
    }
    assert_eq!(
        pool.call("which", &[], A_SECOND, 64),
        Ok(Reply::Json(b"41".to_vec()))
    );
    assert_eq!(
        pool.call("spin", &[], A_SECOND, 64),
        Ok(Reply::Json(b"41".to_vec()))
    );
    let bytes = [0u8, 0xff, b'x'];
    assert_eq!(
        pool.call("ident", &[Arg::Binary(&bytes)], A_SECOND, 64),
        Ok(Reply::Binary(bytes.to_vec()))
    );
    assert_eq!(
        pool.call("ident", &[Arg::Json("{\"a\":[1,2.50]}")], A_SECOND, 64),
        Ok(Reply::Json(b"{\"a\":[1,2.50]}".to_vec()))
    );
    // The bound on the result is this call's own.
    assert_eq!(
        pool.call("ident", &[Arg::Binary(&[b'x'; 100])], A_SECOND, 64),
        Err(CallError::OutputTooLarge)
    );
}

#[test]
fn a_module_pool_keeps_warm_instances_and_replaces_one_a_call_stopped() {
    let pool = plugin_pool(2, false);
    assert_eq!(pool.stats(), crate::ModuleStats::default());
    for _ in 0..3 {
        pool.call("which", &[], A_SECOND, 64).unwrap();
    }
    let warm = pool.stats();
    assert_eq!((warm.instances, warm.in_call, warm.made), (1, 0, 1));
    assert_eq!(
        warm.memory_bytes, 65_536,
        "one page, as the module declares"
    );
    // The module's own refusal leaves the instance usable.
    assert!(matches!(
        pool.call("err", &[], A_SECOND, 64),
        Err(CallError::Module { status: 7, .. })
    ));
    assert_eq!(pool.stats().replaced, 0);
    // A call stopped at its deadline costs the pool that instance, and nothing else.
    assert_eq!(
        pool.call("loop_forever", &[], Duration::from_millis(5), 64),
        Err(CallError::Timeout)
    );
    let after = pool.stats();
    assert_eq!(
        (after.instances, after.replaced, after.memory_bytes),
        (0, 1, 0)
    );
    assert_eq!(
        pool.call("which", &[], A_SECOND, 64),
        Ok(Reply::Json(b"41".to_vec()))
    );
    assert_eq!(pool.stats().made, 2);
}

#[test]
fn a_module_pool_told_to_keeps_no_instance_between_calls() {
    let pool = plugin_pool(4, true);
    // `p` answers with the allocator's next address, which only moves up: on a kept
    // instance the second call would see a higher one.
    let address = |pool: &crate::ModulePool| match pool.call("p", &[], A_SECOND, 64) {
        Err(CallError::Module { status, .. }) => status,
        other => panic!("{other:?}"),
    };
    let first = address(&pool);
    assert_eq!(address(&pool), first);
    let stats = pool.stats();
    assert_eq!((stats.instances, stats.made, stats.replaced), (0, 2, 0));
    let kept = plugin_pool(4, false);
    let first = address(&kept);
    assert!(address(&kept) > first);
}
