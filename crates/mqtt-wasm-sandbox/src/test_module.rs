//! A module speaking the function ABI, written out byte by byte: what this crate's
//! tests, and the tests of the crates that load operator modules, run without a Wasm
//! toolchain. Not part of a broker build (the `test-module` feature).
//!
//! The module has one `mqttd_call`, and what it does is chosen by the first letter of
//! the name it is called by ([`FUNCTIONS`] has one name per behaviour):
//!
//! * `l` loops for ever; `g` grows its memory for ever; `r` recurses for ever;
//!   `x` runs `unreachable`;
//! * `e` fails with status 7 and the message `nope`;
//! * `n` fails with the number of arguments as its status, `p` with its allocator's
//!   next address;
//! * `b` claims a result of two gigabytes;
//! * `i` returns its first argument as it came, tag and bytes;
//! * `s` counts down from [`Shape::spin`], thirty-two steps to the count, and then
//!   returns the answer;
//! * `c` clears the flag at [`CANCEL_FLAG_AT`], waits for it to be set, and then fails
//!   with status 7 and the message `nope`: a call that ends by itself when it is asked
//!   to stop ([`Shape::cancel`]);
//! * anything else returns the answer ([`Shape::answer`]: a tagged value).

use crate::ABI_VERSION;

fn uleb(mut n: u32, out: &mut Vec<u8>) {
    loop {
        let byte = u8::try_from(n & 0x7f).unwrap_or_default();
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
        let byte = u8::try_from(n & 0x7f).unwrap_or_default();
        n >>= 7;
        let done = (n == 0 && byte & 0x40 == 0) || (n == -1 && byte & 0x40 != 0);
        if done {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn len_of(bytes: &[u8]) -> u32 {
    u32::try_from(bytes.len()).unwrap_or(u32::MAX)
}

fn section(id: u8, body: &[u8], out: &mut Vec<u8>) {
    out.push(id);
    uleb(len_of(body), out);
    out.extend_from_slice(body);
}

fn name(text: &str, out: &mut Vec<u8>) {
    uleb(len_of(text.as_bytes()), out);
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

/// Where the answer, the error message and the description lie in the module's
/// memory, below its allocator's first address (1024).
const ANSWER_AT: i32 = 32;
const NOPE: (i32, &[u8]) = (48, b"nope");
const DESCRIPTION_AT: i32 = 64;

/// Where [`Shape::cancel`]'s slot lies when the module points `mqttd_cancel_slot` at
/// it, and the flag `c` waits on.
pub const CANCEL_SLOT_AT: u32 = 8;
/// The flag `c` clears and then waits on.
pub const CANCEL_FLAG_AT: u32 = 12;

/// The most bytes of [`Shape::answer`].
pub const MAX_ANSWER: usize = 16;
/// The most bytes of [`Shape::description`].
pub const MAX_DESCRIPTION: usize = 900;

/// One name per behaviour of the module, as [`plugin`] describes them.
pub const FUNCTIONS: [&str; 10] = [
    "which",
    "ident",
    "spin",
    "loop_forever",
    "grow_memory",
    "recurse",
    "xtrap",
    "err",
    "big",
    "nargs",
];

/// What [`module`] varies.
#[derive(Debug, Clone)]
pub struct Shape {
    /// What `mqttd_abi_version` answers.
    pub version: u32,
    /// An import to declare, as (module, name).
    pub import: Option<(&'static str, &'static str)>,
    /// Leave `mqttd_call` out of the exports.
    pub without_call: bool,
    /// What `mqttd_describe` answers, at most [`MAX_DESCRIPTION`] bytes.
    pub description: Vec<u8>,
    /// The tagged value most calls return (`jtrue`, `j1`, `bbytes`), at most
    /// [`MAX_ANSWER`] bytes.
    pub answer: Vec<u8>,
    /// How far `s` counts down before it answers.
    pub spin: u32,
    /// How far `mqttd_alloc` counts down before it allocates (0: not at all): an
    /// allocator that takes time, in an entry of its own before the call's.
    pub slow_alloc: u32,
    /// How far a start function counts down while an instance is made (0: the module
    /// has no start function).
    pub slow_start: u32,
    /// `Some((slot, value))`: the module exports `mqttd_cancel_slot`, which answers
    /// `slot`, and holds `value` at [`CANCEL_SLOT_AT`] from the start. A module that
    /// can be asked properly answers `CANCEL_SLOT_AT` and holds `CANCEL_FLAG_AT`.
    pub cancel: Option<(u32, u32)>,
}

impl Default for Shape {
    fn default() -> Self {
        Self {
            version: ABI_VERSION,
            import: None,
            without_call: false,
            description: b"{}".to_vec(),
            answer: b"jtrue".to_vec(),
            spin: 1,
            slow_alloc: 0,
            slow_start: 0,
            cancel: None,
        }
    }
}

/// Counts down from `n` (at least 1), thirty-two steps to the count, in the locals
/// `outer` and `inner`: some ten nanoseconds per count.
fn countdown(n: u32, outer: u8, inner: u8) -> Vec<u8> {
    let mut code = Vec::new();
    konst(n.max(1).cast_signed(), &mut code);
    // outer = n; loop { inner = 32; loop { inner -= 1; br_if (inner != 0) }
    // outer -= 1; br_if (outer != 0) }
    code.extend_from_slice(&[
        0x21, outer, 0x03, 0x40, // outer = n; loop
        0x41, 32, 0x21, inner, 0x03, 0x40, 0x20, inner, 0x41, 1, 0x6b, 0x22, inner, 0x0d, 0, 0x0b,
        0x20, outer, 0x41, 1, 0x6b, 0x22, outer, 0x0d, 0, 0x0b,
    ]);
    code
}

/// The body of `mqttd_call(name, name_len, args, args_len, max_out, ret)`, with three
/// extra locals: 6 holds the first byte of the name, 7 and 8 are `s`'s counters.
fn call_body(shape: &Shape) -> Vec<u8> {
    let mut b = vec![1, 3, 0x7f]; // three extra locals, all i32
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
    // The frame is the count (4 bytes), then per argument its tag, its length (4
    // bytes) and its bytes. The first argument's tag is copied over the last byte of
    // its length, so tag and bytes lie together: that is the result.
    let ident = [
        0x20, 5, 0x20, 2, 0x28, 0, 5, 0x41, 1, 0x6a, 0x36, 2, 4, // ret[1] = len + 1
        0x20, 2, 0x20, 2, 0x2d, 0, 4, 0x3a, 0, 8, // args[8] = args[4]
        0x20, 5, 0x20, 2, 0x41, 8, 0x6a, 0x36, 2, 0, // ret[0] = args + 8
        0x41, 0, 0x0f, // return 0
    ];
    when(b'i', &ident);
    when(b's', &countdown(shape.spin, 7, 8));
    // flag = 0; loop { br_if (flag == 0) }; fail with 7
    let flag = CANCEL_FLAG_AT.cast_signed();
    let mut waiting = Vec::new();
    konst(flag, &mut waiting);
    waiting.extend_from_slice(&[0x41, 0, 0x36, 2, 0, 0x03, 0x40]);
    konst(flag, &mut waiting);
    waiting.extend_from_slice(&[0x28, 2, 0, 0x45, 0x0d, 0, 0x0b]);
    waiting.extend_from_slice(&failing(&[0x41, 7]));
    when(b'c', &waiting);
    set_result(ANSWER_AT, len_of(&shape.answer).cast_signed(), &mut b);
    b.extend_from_slice(&[0x41, 0, 0x0b]);
    b
}

/// A module speaking the ABI: a bump allocator, a description, one function.
///
/// # Panics
///
/// When the answer or the description is longer than its room.
#[must_use]
#[allow(clippy::too_many_lines)] // one section of the module after another
pub fn module(shape: &Shape) -> Vec<u8> {
    assert!(shape.answer.len() <= MAX_ANSWER, "the answer is too long");
    assert!(
        shape.description.len() <= MAX_DESCRIPTION,
        "the description is too long"
    );
    let mut out = b"\0asm\x01\0\0\0".to_vec();
    // Types: 0 () -> i32, 1 (i32) -> i32, 2 (i32) -> (), 3 (i32 x 6) -> i32, and for a
    // start function 4 () -> ().
    let starting = shape.slow_start > 0;
    let mut types = vec![4 + u8::from(starting)];
    types.extend_from_slice(&[
        0x60, 0, 1, 0x7f, 0x60, 1, 0x7f, 1, 0x7f, 0x60, 1, 0x7f, 0, 0x60, 6, 0x7f, 0x7f, 0x7f,
        0x7f, 0x7f, 0x7f, 1, 0x7f,
    ]);
    if starting {
        types.extend_from_slice(&[0x60, 0, 0]);
    }
    section(1, &types, &mut out);
    let imported = u8::from(shape.import.is_some());
    if let Some((from, what)) = shape.import {
        let mut body = vec![1];
        name(from, &mut body);
        name(what, &mut body);
        body.extend_from_slice(&[0, 1]); // a function of type 1
        section(2, &body, &mut out);
    }
    // Functions: version, alloc, free, describe, call, recurse, a start function, and
    // `mqttd_cancel_slot`.
    let cancelling = shape.cancel.is_some();
    let mut functions = vec![
        6 + u8::from(starting) + u8::from(cancelling),
        0,
        1,
        2,
        2,
        3,
        0,
    ];
    if starting {
        functions.push(4);
    }
    if cancelling {
        functions.push(0);
    }
    section(3, &functions, &mut out);
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
    if cancelling {
        export("mqttd_cancel_slot", 0, imported + 6 + u8::from(starting));
    }
    let mut body = vec![count];
    body.extend_from_slice(&exports);
    section(7, &body, &mut out);
    if starting {
        section(8, &[imported + 6], &mut out);
    }

    let mut version = vec![0];
    konst(shape.version.cast_signed(), &mut version);
    version.push(0x0b);
    // The old top of the heap is the answer; the top moves up by the length, and the
    // memory grows a page at a time until it holds the new top.
    let mut alloc = if shape.slow_alloc > 0 {
        let mut slow = vec![1, 2, 0x7f]; // two extra locals, both i32
        slow.extend_from_slice(&countdown(shape.slow_alloc, 1, 2));
        slow
    } else {
        vec![0]
    };
    alloc.extend_from_slice(&[
        0x23, 0, 0x23, 0, 0x20, 0, 0x6a, 0x24, 0, // old top; top += len
        // The loop is entered only when the memory must grow: a loop is where the
        // runtime looks at the clock, and an allocation that fits has nothing to wait for.
        0x3f, 0, 0x41, 16, 0x74, 0x23, 0, 0x49, 0x04, 0x40, // if (pages << 16) < top
        0x03, 0x40, 0x41, 1, 0x40, 0, 0x1a, // loop: grow one page
        0x3f, 0, 0x41, 16, 0x74, 0x23, 0, 0x49, 0x0d, 0, // again while still short
        0x0b, 0x0b, 0x0b,
    ]);
    let free = vec![0, 0x0b];
    let mut describe = vec![0];
    let described = len_of(&shape.description).cast_signed();
    for (offset, value) in [(0u8, DESCRIPTION_AT), (4, described)] {
        describe.extend_from_slice(&[0x20, 0]);
        konst(value, &mut describe);
        describe.extend_from_slice(&[0x36, 2, offset]);
    }
    describe.push(0x0b);
    let mut call = call_body(shape);
    // Inside `call`, function 5 is the recursing one; an import shifts every index.
    if imported == 1 {
        if let Some(at) = call.windows(3).position(|w| w == [0x10, 5, 0x1a]) {
            call[at + 1] = 6;
        }
    }
    let recurse = vec![0, 0x10, imported + 5, 0x0b];
    let mut bodies = vec![version, alloc, free, describe, call, recurse];
    if starting {
        let mut start = vec![1, 2, 0x7f]; // two locals, both i32
        start.extend_from_slice(&countdown(shape.slow_start, 0, 1));
        start.push(0x0b);
        bodies.push(start);
    }
    if let Some((slot, _)) = shape.cancel {
        let mut answer = vec![0];
        konst(slot.cast_signed(), &mut answer);
        answer.push(0x0b);
        bodies.push(answer);
    }
    let mut code = vec![u8::try_from(bodies.len()).unwrap_or_default()];
    for function in bodies {
        uleb(len_of(&function), &mut code);
        code.extend_from_slice(&function);
    }
    section(10, &code, &mut out);

    let held = shape.cancel.map(|(_, value)| value.to_le_bytes());
    let mut segments = vec![
        (ANSWER_AT, shape.answer.as_slice()),
        NOPE,
        (DESCRIPTION_AT, shape.description.as_slice()),
    ];
    if let Some(held) = &held {
        segments.push((CANCEL_SLOT_AT.cast_signed(), held.as_slice()));
    }
    let mut data = vec![u8::try_from(segments.len()).unwrap_or_default()];
    for (at, bytes) in segments {
        data.push(0);
        konst(at, &mut data);
        data.push(0x0b);
        uleb(len_of(bytes), &mut data);
        data.extend_from_slice(bytes);
    }
    section(11, &data, &mut out);
    out
}

/// The description [`plugin`] gives: the module `test`, version `version`, and every
/// name of [`FUNCTIONS`] taking zero to eight arguments.
#[must_use]
pub fn description(version: &str) -> Vec<u8> {
    let functions: Vec<String> = FUNCTIONS
        .iter()
        .map(|f| format!("{{\"name\":\"{f}\",\"min_args\":0,\"max_args\":8}}"))
        .collect();
    format!(
        "{{\"module\":\"test\",\"version\":\"{version}\",\"functions\":[{}]}}",
        functions.join(",")
    )
    .into_bytes()
}

/// A module an operator could declare: it describes [`FUNCTIONS`], says it is
/// `version`, and answers `answer`. Two versions are two files with two hashes.
#[must_use]
pub fn plugin(version: &str, answer: &[u8]) -> Vec<u8> {
    plugin_spinning(version, answer, 1)
}

/// [`plugin`], whose `spin` counts down from `spin` before it answers: a call that
/// takes as long as the test needs it to (some ten nanoseconds per count).
#[must_use]
pub fn plugin_spinning(version: &str, answer: &[u8], spin: u32) -> Vec<u8> {
    module(&Shape {
        description: description(version),
        answer: answer.to_vec(),
        spin,
        ..Shape::default()
    })
}
