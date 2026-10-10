//! The sandbox against a module written out by hand here, byte by byte: these tests
//! need no Wasm toolchain and no jq. What the module's one function does is chosen by
//! the first letter of the name it is called by (see [`call_body`]).

use std::time::{Duration, Instant};

use crate::{Arg, CallError, Grants, Limits, LoadError, Reply, Sandbox, ABI_VERSION};

fn uleb(mut n: u32, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(n & 0x7f).unwrap();
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn sleb(mut n: i32, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(n & 0x7f).unwrap();
        n >>= 7;
        let done = (n == 0 && byte & 0x40 == 0) || (n == -1 && byte & 0x40 != 0);
        if done {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn section(id: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(id);
    uleb(u32::try_from(body.len()).unwrap(), out);
    out.extend_from_slice(body);
}

fn name(text: &str, out: &mut Vec<u8>) {
    uleb(u32::try_from(text.len()).unwrap(), out);
    out.extend_from_slice(text.as_bytes());
}

fn konst(n: i32, out: &mut Vec<u8>) {
    out.push(0x41);
    sleb(n, out);
}

/// `ret[0] = at; ret[1] = len` for the result pair `ret` (local 5).
fn set_result(at: i32, len: i32, out: &mut Vec<u8>) {
    for (offset, value) in [(0u8, at), (4, len)] {
        out.extend_from_slice(&[0x20, 5]);
        konst(value, out);
        out.extend_from_slice(&[0x36, 2, offset]);
    }
}

const DESCRIPTION: (i32, &[u8]) = (16, b"{}");
const TRUE: (i32, &[u8]) = (32, b"jtrue");
const NOPE: (i32, &[u8]) = (48, b"nope");

/// The body of `mqttd_call(name, name_len, args, args_len, max_out, ret)`, with one
/// extra local (6) holding the first byte of the name:
///
/// * `l` loops for ever; `g` grows its memory for ever; `r` recurses for ever;
///   `x` runs `unreachable`;
/// * `e` fails with status 7 and the message `nope`;
/// * `n` fails with the number of arguments as its status, `p` with its allocator's
///   next address;
/// * `b` claims a result of two gigabytes;
/// * anything else returns the JSON value `true`.
fn call_body() -> Vec<u8> {
    let mut b = vec![1, 1, 0x7f]; // one extra local, an i32
    b.extend_from_slice(&[0x20, 0, 0x2d, 0, 0, 0x21, 6]); // local 6 = name[0]
    let mut when = |letter: u8, then: &[u8]| {
        b.extend_from_slice(&[0x20, 6]);
        konst(i32::from(letter), &mut b);
        b.extend_from_slice(&[0x46, 0x04, 0x40]); // i32.eq; if
        b.extend_from_slice(then);
        b.push(0x0b);
    };
    when(b'l', &[0x03, 0x40, 0x0c, 0, 0x0b]); // loop br 0 end
    when(b'g', &[0x03, 0x40, 0x41, 16, 0x40, 0, 0x1a, 0x0c, 0, 0x0b]); // loop grow 16 pages
    when(b'r', &[0x10, 5, 0x1a]); // call the recursing function
    when(b'x', &[0x00]);
    let failing = |status: &[u8]| {
        let mut code = Vec::new();
        set_result(NOPE.0, 4, &mut code);
        code.extend_from_slice(status);
        code.push(0x0f); // return
        code
    };
    when(b'e', &failing(&[0x41, 7]));
    when(b'n', &failing(&[0x20, 2, 0x28, 2, 0])); // i32.load(args): the count
    when(b'p', &failing(&[0x23, 0])); // global.get 0
    let mut huge = Vec::new();
    set_result(64, i32::MAX, &mut huge);
    huge.extend_from_slice(&[0x41, 0, 0x0f]);
    when(b'b', &huge);
    set_result(TRUE.0, 5, &mut b);
    b.extend_from_slice(&[0x41, 0, 0x0b]);
    b
}

/// What [`module`] varies.
#[derive(Clone, Copy)]
struct Shape {
    version: u32,
    /// An import to declare, as (module, name).
    import: Option<(&'static str, &'static str)>,
    /// Leave `mqttd_call` out of the exports.
    without_call: bool,
}

const PLAIN: Shape = Shape {
    version: ABI_VERSION,
    import: None,
    without_call: false,
};

/// A module speaking the ABI: a bump allocator, a description, one function.
fn module(shape: Shape) -> Vec<u8> {
    let mut out = b"\0asm\x01\0\0\0".to_vec();
    // Types: 0 () -> i32, 1 (i32) -> i32, 2 (i32) -> (), 3 (i32 x 6) -> i32.
    section(
        1,
        &[
            4, 0x60, 0, 1, 0x7f, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 1, 0x7f, 0, 0x60, 6, 0x7f, 0x7f,
            0x7f, 0x7f, 0x7f, 0x7f, 1, 0x7f,
        ],
        &mut out,
    );
    let imported = u8::from(shape.import.is_some());
    if let Some((from, what)) = shape.import {
        let mut body = vec![1];
        name(from, &mut body);
        name(what, &mut body);
        body.extend_from_slice(&[0, 1]); // a function of type 1
        section(2, &body, &mut out);
    }
    // Functions: version, alloc, free, describe, call, recurse.
    section(3, &[6, 0, 1, 2, 2, 3, 0], &mut out);
    section(5, &[1, 0, 1], &mut out); // one memory, one page, no maximum
    section(6, &[1, 0x7f, 1, 0x41, 0x80, 0x08, 0x0b], &mut out); // global 0: mut i32 = 1024
    let mut exports = Vec::new();
    let mut count = 0u8;
    let mut export = |text: &str, kind: u8, index: u8| {
        name(text, &mut exports);
        exports.extend_from_slice(&[kind, index]);
        count += 1;
    };
    export("memory", 2, 0);
    export("mqttd_abi_version", 0, imported);
    export("mqttd_alloc", 0, imported + 1);
    export("mqttd_free", 0, imported + 2);
    export("mqttd_describe", 0, imported + 3);
    if !shape.without_call {
        export("mqttd_call", 0, imported + 4);
    }
    let mut body = vec![count];
    body.extend_from_slice(&exports);
    section(7, &body, &mut out);

    let mut version = vec![0];
    konst(i32::try_from(shape.version).unwrap(), &mut version);
    version.push(0x0b);
    // The old top of the heap is the answer; the top moves up by the length.
    let alloc = vec![0, 0x23, 0, 0x23, 0, 0x20, 0, 0x6a, 0x24, 0, 0x0b];
    let free = vec![0, 0x0b];
    let mut describe = vec![0];
    for (offset, value) in [(0u8, DESCRIPTION.0), (4, 2)] {
        describe.extend_from_slice(&[0x20, 0]);
        konst(value, &mut describe);
        describe.extend_from_slice(&[0x36, 2, offset]);
    }
    describe.push(0x0b);
    let mut call = call_body();
    // Inside `call`, function 5 is the recursing one; an import shifts every index.
    if imported == 1 {
        let at = call.windows(3).position(|w| w == [0x10, 5, 0x1a]).unwrap();
        call[at + 1] = 6;
    }
    let recurse = vec![0, 0x10, imported + 5, 0x0b];
    let mut code = vec![6];
    for function in [version, alloc, free, describe, call, recurse] {
        uleb(u32::try_from(function.len()).unwrap(), &mut code);
        code.extend_from_slice(&function);
    }
    section(10, &code, &mut out);

    let mut data = vec![3];
    for (at, bytes) in [DESCRIPTION, TRUE, NOPE] {
        data.push(0);
        konst(at, &mut data);
        data.push(0x0b);
        name(std::str::from_utf8(bytes).unwrap(), &mut data);
    }
    section(11, &data, &mut out);
    out
}

fn sandbox(limits: Limits) -> Sandbox {
    Sandbox::load(&module(PLAIN), limits, Grants::default()).expect("the test module loads")
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
    assert!(instance.fuel_used() > 0);
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
    let sandbox = sandbox(limits);
    let burnt: Vec<u64> = (0..2)
        .map(|_| {
            let mut instance = sandbox.instantiate().unwrap();
            assert_eq!(instance.call("loop", &[], None), Err(CallError::Fuel));
            instance.fuel_used()
        })
        .collect();
    assert_eq!(burnt[0], burnt[1]);
    assert!(burnt[0] >= 1_000_000);
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
    // The call runs on a thread with a small stack: the module's calls are frames on
    // the interpreter's own heap-allocated stack, a hundred thousand deep here, and
    // not one of them is a frame on this one.
    let mut instance = sandbox(Limits::default()).instantiate().unwrap();
    assert_eq!(Limits::default().max_call_depth, 100_000);
    let outcome = std::thread::Builder::new()
        .stack_size(256 * 1024)
        .spawn(move || instance.call("recurse", &[], soon()))
        .unwrap()
        .join()
        .expect("the host thread survives");
    assert_eq!(outcome, Err(CallError::Stack));
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
            ..PLAIN
        };
        let error = Sandbox::load(&module(shape), Limits::default(), Grants::default());
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
        ..PLAIN
    };
    let sandbox = Sandbox::load(&module(shape), Limits::default(), Grants::default()).unwrap();
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
        ..PLAIN
    };
    let error = Sandbox::load(&module(shape), Limits::default(), Grants::default());
    assert!(
        matches!(error, Err(LoadError::AbiVersion(v)) if v == ABI_VERSION + 1),
        "{error:?}"
    );
}

#[test]
fn a_module_without_the_abi_is_refused_at_load() {
    let shape = Shape {
        without_call: true,
        ..PLAIN
    };
    let error = Sandbox::load(&module(shape), Limits::default(), Grants::default());
    assert!(matches!(error, Err(LoadError::Export(_))), "{error:?}");
}

#[test]
fn bytes_that_are_not_a_module_are_refused_at_load() {
    let mut cut = module(PLAIN);
    cut.truncate(cut.len() - 7);
    for bytes in [b"not wasm at all".as_slice(), &cut] {
        let error = Sandbox::load(bytes, Limits::default(), Grants::default());
        assert!(matches!(error, Err(LoadError::Invalid(_))), "{error:?}");
    }
}
