#![no_main]
//! Fuzz target: whatever bytes the sandbox is given as a module, it answers with a
//! module or a reason, and a module it accepts runs under its limits (ADR 0086).
//!
//! A module file is operator input (the jq module a release embeds, later a plugin
//! named in the rules file). A damaged or hostile one must be refused at load, or
//! load and then be held to its limits: no panic, no hang, no memory past the cap, and
//! an instance that says it is poisoned exactly when a call ended in anything but the
//! function's own answer.
//!
//! The first byte of an input chooses how the rest is used:
//!
//! * bit 0: the module is interpreted (Pulley) instead of compiled (Cranelift);
//! * bit 1: the rest is the body of `mqttd_call` inside a module that is otherwise
//!   whole ([`around`]: the ABI's exports, a bump allocator, seven of the host's
//!   imports to call). Without it the rest is the module file itself. The first form
//!   gets the fuzzer past the section framing and into code that runs; the second
//!   reaches everything the first cannot say;
//! * bit 2: calls are bounded by their deadline alone; without it also by fuel;
//! * bit 3: the module is granted the wall clock;
//! * bit 4: the module has a hand-off (ADR 0086 D6): a call still running after its
//!   first slice, 200 µs here, is suspended once and finished inside it.
//!
//! Every bound is small, so that no input can make a run slow or large: 8 KiB of
//! input, 1 MiB of linear memory, 256 KiB of stack, 4 KiB of output, 2 ms a call (and
//! 20,000 units of fuel), 20 ms to start.

use std::sync::Arc;
use std::time::{Duration, Instant};

use libfuzzer_sys::fuzz_target;
use mqtt_wasm_sandbox::{
    Arg, CallError, Execution, Grants, HandOff, Limits, LoadError, Reply, Sandbox,
};

const MAX_INPUT: usize = 8 * 1024;
const MEMORY: usize = 1 << 20;
const MAX_OUTPUT: u32 = 4096;

fn uleb(mut n: usize, out: &mut Vec<u8>) {
    loop {
        let byte = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn section(id: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(id);
    uleb(body.len(), out);
    out.extend_from_slice(body);
}

fn name(text: &str, out: &mut Vec<u8>) {
    uleb(text.len(), out);
    out.extend_from_slice(text.as_bytes());
}

/// A module speaking the ABI whose `mqttd_call` has `body` as its code (after two
/// extra `i32` locals, 6 and 7, and before the closing `end`).
///
/// Functions 0 to 6 are imports: `clock_time_get`, `environ_sizes_get`,
/// `environ_get`, `fd_fdstat_get`, `fd_read`, `fd_write`, `proc_exit`. Function 11 is
/// `mqttd_call` itself. Memory holds `{}` at 16 and `jtrue` at 32; global 0 is the
/// allocator's next address. The cancel slot (function 12, `mqttd_cancel_slot`) is at
/// address 0, which the body may fill with anything: the host follows whatever it
/// holds at a deadline, and must stay inside the module's memory doing it.
fn around(body: &[u8]) -> Vec<u8> {
    const I32: u8 = 0x7f;
    const I64: u8 = 0x7e;
    let mut out = b"\0asm\x01\0\0\0".to_vec();
    let types: [(&[u8], &[u8]); 7] = [
        (&[], &[I32]),
        (&[I32], &[I32]),
        (&[I32], &[]),
        (&[I32; 6], &[I32]),
        (&[I32, I64, I32], &[I32]),
        (&[I32, I32], &[I32]),
        (&[I32; 4], &[I32]),
    ];
    let mut s = vec![types.len() as u8];
    for (params, results) in types {
        s.push(0x60);
        uleb(params.len(), &mut s);
        s.extend_from_slice(params);
        uleb(results.len(), &mut s);
        s.extend_from_slice(results);
    }
    section(1, &s, &mut out);

    let imports: [(&str, u8); 7] = [
        ("clock_time_get", 4),
        ("environ_sizes_get", 5),
        ("environ_get", 5),
        ("fd_fdstat_get", 5),
        ("fd_read", 6),
        ("fd_write", 6),
        ("proc_exit", 2),
    ];
    let mut s = vec![imports.len() as u8];
    for (import, ty) in imports {
        name("wasi_snapshot_preview1", &mut s);
        name(import, &mut s);
        s.extend_from_slice(&[0, ty]);
    }
    section(2, &s, &mut out);

    // version, alloc, free, describe, call, cancel slot
    section(3, &[6, 0, 1, 2, 2, 3, 0], &mut out);
    section(5, &[1, 0, 1], &mut out); // one memory, one page, no maximum
    section(6, &[1, I32, 1, 0x41, 0x80, 0x08, 0x0b], &mut out); // global 0: mut i32 = 1024

    let exports: [(&str, u8, u8); 7] = [
        ("memory", 2, 0),
        ("mqttd_abi_version", 0, 7),
        ("mqttd_alloc", 0, 8),
        ("mqttd_free", 0, 9),
        ("mqttd_describe", 0, 10),
        ("mqttd_call", 0, 11),
        ("mqttd_cancel_slot", 0, 12),
    ];
    let mut s = vec![exports.len() as u8];
    for (export, kind, index) in exports {
        name(export, &mut s);
        s.extend_from_slice(&[kind, index]);
    }
    section(7, &s, &mut out);

    let mut call = vec![1, 2, I32];
    call.extend_from_slice(body);
    call.push(0x0b);
    let functions: [&[u8]; 6] = [
        &[0, 0x41, 1, 0x0b], // the ABI version: 1
        // The old top of the heap is the answer; the top moves up by the length.
        &[0, 0x23, 0, 0x23, 0, 0x20, 0, 0x6a, 0x24, 0, 0x0b],
        &[0, 0x0b],
        // ret[0] = 16; ret[1] = 2: the description
        &[
            0, 0x20, 0, 0x41, 16, 0x36, 2, 0, 0x20, 0, 0x41, 2, 0x36, 2, 4, 0x0b,
        ],
        &call,
        &[0, 0x41, 0, 0x0b], // the cancel slot: address 0
    ];
    let mut s = vec![functions.len() as u8];
    for function in functions {
        uleb(function.len(), &mut s);
        s.extend_from_slice(function);
    }
    section(10, &s, &mut out);

    let mut s = vec![2];
    for (at, bytes) in [(16u8, "{}"), (32, "jtrue")] {
        s.extend_from_slice(&[0, 0x41, at, 0x0b]);
        name(bytes, &mut s);
    }
    section(11, &s, &mut out);
    out
}

fuzz_target!(|data: &[u8]| {
    let Some((&how, rest)) = data.split_first() else {
        return;
    };
    if rest.len() > MAX_INPUT {
        return;
    }
    let execution = if how & 1 == 0 {
        Execution::Compiled
    } else {
        Execution::Interpreted
    };
    let built;
    let wasm = if how & 2 == 0 {
        rest
    } else {
        built = around(rest);
        &built
    };
    let limits = Limits {
        memory_bytes: MEMORY,
        max_output_bytes: MAX_OUTPUT,
        max_stack_bytes: 256 * 1024,
        min_run: Duration::from_micros(100),
        cancel_grace: Duration::from_millis(1),
        init_timeout: Duration::from_millis(20),
        max_fuel: (how & 4 == 0).then_some(20_000),
        first_slice: Duration::from_micros(200),
        max_call: None,
    };
    let grants = Grants {
        clock: how & 8 != 0,
        environment: vec![b"A=1".to_vec(), b"EMPTY=".to_vec(), vec![b'x'; 300]],
    };

    let sandbox = match Sandbox::load_as(wasm, execution, limits, grants) {
        Ok(sandbox) => sandbox,
        Err(error) => {
            // The reason is printable, and it is the module's: this host can run
            // compiled code, so nothing here may be blamed on the host.
            let _ = error.to_string();
            assert!(
                !matches!(error, LoadError::Engine(_)),
                "a module's fault reported as the host's: {error}"
            );
            return;
        }
    };
    assert!(sandbox.description().len() <= MAX_OUTPUT as usize + 1);
    let sandbox = if how & 16 == 0 {
        sandbox
    } else {
        let hand_off: HandOff = Arc::new(|rest: &mut dyn FnMut()| rest());
        sandbox.with_hand_off(hand_off)
    };
    let Ok(mut instance) = sandbox.instantiate() else {
        return;
    };
    let binary = &rest[..rest.len().min(64)];
    let calls: [(&str, &[Arg<'_>]); 4] = [
        ("jq", &[Arg::Json("1"), Arg::Binary(binary)]),
        ("", &[]),
        ("f", &[Arg::Binary(&[]), Arg::Json("{\"a\":[1,2]}")]),
        ("jq", &[Arg::Json("null")]),
    ];
    for (function, args) in calls {
        let deadline = Some(Instant::now() + Duration::from_millis(2));
        let outcome = instance.call(function, args, deadline);
        match &outcome {
            Ok(Reply::Json(bytes) | Reply::Binary(bytes)) => {
                assert!(bytes.len() <= MAX_OUTPUT as usize);
            }
            Err(error) => {
                let _ = error.to_string();
            }
        }
        // A failure that is not the function's own ends the instance, and only that.
        let own = matches!(outcome, Ok(_) | Err(CallError::Module { .. }));
        assert_eq!(instance.is_poisoned(), !own, "{function}: {outcome:?}");
        assert!(instance.memory_bytes() <= MEMORY);
        if !own {
            assert_eq!(
                instance.call(function, args, deadline),
                Err(CallError::Poisoned)
            );
            break;
        }
    }
});
