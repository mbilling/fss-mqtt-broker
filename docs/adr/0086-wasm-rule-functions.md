# ADR 0086 — Rule functions in a WebAssembly sandbox: the real jq first, operator plugins on the same mechanism

- **Status:** Proposed
- **Date:** 2026-10-10 (runtime decision and re-measurement: 2026-10-11)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0086-wasm-rule-functions.md](../delivery/0086-wasm-rule-functions.md) — plan, progress, and changelog
- **Revisits:** [ADR 0083](0083-rule-engine.md) (functions: `jq` is refused at load). This
  record proposes how `jq` is provided, with EMQX's results.
- **Related:** [ADR 0084](0084-watching-and-editing-rules-live.md) (live editing, the admin
  rules endpoints and the trace this extends), [ADR 0085](0085-rule-state-store.md) (the
  other proposed extension of the function set), [ADR 0045](0045-release-engineering-and-distribution.md)
  (reproducible builds, which the module's build joins).

> This record states the decision proposed and the measurements it rests on. The
> prototype is `crates/mqtt-wasm-sandbox`; nothing in the broker depends on it yet.
>
> **Decided so far (2026-10-11), with the record still Proposed as a whole:** the runtime
> is **wasmtime, chosen for speed** over the wasmi interpreter the spike first recommended
> (§3, §8); `$ENV` and `env` inside jq are **empty unless the operator lists variables**
> (D8); the jq module is **built in the pipeline from pinned sources** and compared with a
> recorded hash, not committed (D11); the last-digit differences of libm are **documented,
> not chased** (§7).

## Context

EMQX's rule engine has a `jq` function: `jq(Program, Input)` and `jq(Program, Input,
TimeoutMs)` run a [jq](https://jqlang.org) program over a value and return the list of
its outputs. mqttd's rule engine promises EMQX's results (ADR 0083, amended 2026-10-10)
and refuses `jq` at load, because none of the ways to provide it fit:

- EMQX links the C library (the `emqx/jq` NIF, v0.4.1, which builds `emqx/jqc` at
  `4c60b10`: jq 1.8.1 with its bundled oniguruma and decNumber, plus `jq_cancel`). The
  workspace forbids `unsafe`, and C linked into the broker runs with the broker's memory
  and the broker's rights. A fault in jq is then a fault in the broker: the spike found a
  nine-token program that takes an EMQX 6.3.1 node down (below).
- A Rust reimplementation (jaq) is not jq: different number printing, different error
  texts, different builtins. "Close" is what the rule engine's contract rules out.

A third way: compile the real jq to WebAssembly and run it in a WebAssembly runtime written
in Rust. The C code is then a guest of that runtime. It has one linear memory and no way to
name anything outside it, so memory safety of the broker no longer depends on jq (it
depends on the runtime instead, D11), and the results are jq's own.

If that works for jq, the same sandbox carries any function: an operator could supply a
function as a `.wasm` file, and it would run under the same limits. This record covers
both: what the spike measured for jq, and the design proposed for the general mechanism.

## The spike: what was built and what it measured

Everything here is reproducible from the tree: `crates/mqtt-wasm-sandbox/jq-module/build.sh`
builds the module, `cargo test --release -p mqtt-wasm-sandbox --features jq-module` runs
the comparison with EMQX and the limit tests, and the `measure`, `oracle` and `probe`
examples print the numbers. Times are from an Apple M1 Pro, release build, single thread,
unless a row names another host. The spike ran in two rounds: on wasmi (2026-10-10), and
after the runtime decision on wasmtime (2026-10-11). Both sets of numbers are here.

### 1. Build: jq compiles to WebAssembly unmodified

| | |
|---|---|
| Sources | `emqx/jqc@4c60b10c` (jq 1.8.1 = `jqlang/jq@4467af70`, plus 31 lines in `execute.c` and `jq.h`: `jq_cancel`, `jq_reset_cancel_state`, `jq_canceled`), `kkos/oniguruma@4ef89209` (the commit jq 1.8.1's submodule names), decNumber as vendored in jq. Fetched as tarballs, checked against sha256 in the script |
| Toolchain | zig 0.16.0 only: `zig cc -target wasm32-wasi`, with the wasi-libc it bundles. No wasi-sdk, no emscripten, no autotools |
| Flags | `-mcpu=mvp+bulk_memory+sign_ext+nontrapping_fptoint+mutable_globals -O2 -DNDEBUG -fno-ident -ffile-prefix-map=…`, linked `-mexec-model=reactor -Wl,--strip-all`, 1 MiB stack, 2 MiB initial memory |
| What needed attention | `-D_WASI_EMULATED_SIGNAL` and `-lwasi-emulated-signal` (decNumber includes `<signal.h>`); `gamma()`, glibc's alias of `lgamma`, in a five-line `compat.c`; the list of `HAVE_*` macros jq's `configure` finds on Linux, written out; the three files jq's Makefile generates (`builtin.inc`, `version.h`, oniguruma's `config.h`) |
| What did not | No `setjmp`/`longjmp` (libjq has none). No threads: wasi-libc's single-threaded pthread functions satisfy jq's `pthread_once` and keys. No patch to any jq, oniguruma or decNumber source file |
| Result | `jq.wasm`, **984,650 bytes** (607 kB code, 377 kB data); 311,264 bytes gzipped. `-Os` gives 882,372 bytes and runs about 20% slower. `wasm-opt` was not available and was not measured |
| Imports | 16 functions of `wasi_snapshot_preview1` (clock, environment, the file-descriptor and path calls libc's stdio links against, `proc_exit`). Nothing else |
| Exports | `memory`, `_initialize`, and the function ABI (D2) from a 360-line `shim.c` that follows EMQX's NIF (`c_src/port_nif_common.c`) call by call |

### 2. Reproducibility: the same bytes on three hosts

`build.sh` produced a file with sha256
`728129fa97bb9da2eaee05b74877413e8b1ec9928f100dac07c3d4859bec8c57` on macOS arm64
(Homebrew's zig 0.16.0), and in `debian:stable-slim` containers on Linux arm64 and Linux
amd64 (the official zig 0.16.0 tarballs), each with a different working directory. The pins
that make it so: the zig version (the script refuses another), the source tarball hashes,
the feature list spelled out (not `generic`, which moves with the compiler),
`-ffile-prefix-map` and `-fno-ident`. No `SOURCE_DATE_EPOCH` is needed: nothing in the
output carries a time.

The module is not checked in. `jq-module/jq.wasm.sha256` records the hash the tree must
build, and a test compares the file with it.

Since 2026-10-11 CI does this on every change that touches the crate (the `wasm sandbox`
job of `ci.yml`): `jq-module/fetch-zig.sh` downloads zig 0.16.0 and refuses an archive whose
sha256 is not the recorded one (one hash per platform, from ziglang.org's index),
`build.sh` builds from the two source tarballs it checks the same way, and
`sha256sum --check` compares the result with `jq.wasm.sha256`.

### 3. Runtime: wasmtime (decided 2026-10-11), with wasmi measured as the alternative

The first round of the spike ran the module in wasmi, an interpreter, and recommended it
for its small trust base. The maintainer's decision on seeing the speed table was
**wasmtime, chosen 2026-10-11 for speed**: Cranelift compiles the module to machine code
and the processor runs it. The prototype was moved to wasmtime 49.0.2 and everything below
was measured again on it. wasmi's numbers are kept beside wasmtime's: they are the
alternative considered, and the price of the choice in both directions.

| | wasmtime 49.0.2 (chosen) | wasmi 2.0.0 |
|---|---|---|
| Kind | compiler (Cranelift) to machine code; also an interpreter (Pulley), used here as the fallback of §8 | interpreter, pure Rust |
| jq speed against native (§6) | **1.9–3.1× slower** | 15–26× slower |
| Module load | **0.56 s** on an Apple M1 Pro, 0.78 s as a static musl binary on Linux arm64 (§8): the whole module is compiled, once per process | 11–12 ms (validation and translation) |
| Instantiate | 52 µs | 130–190 µs |
| Compile one jq program | 1.0–1.1 ms | 6.5–7.6 ms |
| License | Apache-2.0 WITH LLVM-exception: **added to `deny.toml`'s allow list** (29 crates carry it; a permissive license, and the exception only removes obligations) | MIT OR Apache-2.0: passes as it is |
| MSRV | 1.96 (the toolchain is pinned at 1.97.0; the workspace declares 1.88, see §8) | 1.86 |
| `Cargo.lock` | **54 entries** added; the crate's own build graph is 84 crates | 10 entries |
| Size of a static release binary linking it (§8) | 6.4 MB for `aarch64-unknown-linux-musl`, 9.7 MB for `x86_64-unknown-linux-musl` (an empty program: 0.4 MB) | 2.1 MB for a minimal runner |
| Machine code generated at run time | yes: executable pages, traps caught by signal handlers (`SIGSEGV`, `SIGBUS`, `SIGILL`, `SIGFPE`; Mach exception ports on macOS) | none |
| Native code linked into the binary | one C function of three lines (a weak-symbol test for the system unwinder, `wasmtime/src/runtime/vm/helpers.c`), built by `cc` at compile time; hand-written assembly for the fiber switch | none |
| `unsafe` | none in our code; **1,594** `unsafe` blocks, functions and impls in the `wasmtime` crate's 118,668 lines, 137 in its fiber crate, 156 in its core crate, 193 in Pulley, 40 in the unwinder, and the machine code Cranelift emits, which no Rust check sees | none in our code; 289 in wasmi's 42,840 lines (41 more in its three sibling crates) |
| Works where executable memory is forbidden | **no** for compiled code; yes with Pulley, 25× slower (§8) | yes |

What the decision buys: a `jq` call on a 1 KB payload takes 27–102 µs instead of
247–1,185 µs, eight to eleven times faster, and within 1.9–3.1× of what EMQX's native NIF
does. What it costs: a compiler and the code it generates join the broker's trust base;
the first load takes half a second; and compiled code cannot run under the
`MemoryDenyWriteExecute=true` of the systemd unit this repository ships (§8).

The features taken from wasmtime are `runtime`, `cranelift`, `async`, `pulley` and `std`:
no component model, no WASI crate (the sixteen imports are 240 lines of our own, which
is what keeps the grant model of D8 small), no garbage collector, no threads, no on-disk
code cache, no profiling or debugger hooks, no text format, no `rayon`, no backtraces.
`async` is there for one thing: a call runs on a stack of its own (§4, Stack).

Only the safe API is used. `Module::deserialize` and `deserialize_file`, which load
machine code from bytes or a file, are `unsafe` and are not called: the module is compiled
with `Module::new` in the process that runs it, every time.

`cargo deny check` and `cargo audit` pass with the allow-list entry: no advisory, no
banned crate, no new source. Five crates are in the lockfile twice because Cranelift uses
the 0.10 line of `sha2` (`sha2`, `digest`, `block-buffer`, `crypto-common`, `cpufeatures`);
duplicates are warnings in this repository's policy. Every other new crate is under a
license already allowed (MIT, Apache-2.0, or a choice including one of them).

### 4. Limits

Measured on wasmtime; wasmi's figures from the first round in the last column.

| Limit | Mechanism | Measured | wasmi |
|---|---|---|---|
| Time (`jq/3`'s timeout) | Epoch interruption: compiled code looks at a counter in every loop and at every function entry. One timer thread per loaded module sleeps until the earliest deadline among the calls now running and then advances the counter; while no call with a deadline runs it waits on a condition variable and does not tick. At its deadline a call's module is asked to stop (`mqttd_cancel`, which is `jq_cancel`: the program ends at its next step) and given 2 ms; a module still running after that is stopped by a trap and its instance dropped. A call always gets 1 ms, so a timeout of 0 still runs what is quick, as in EMQX | Never early. `def f: f; f` at 100 ms: stopped after 100.1–100.8 ms on macOS, 101.0–102.0 on Linux arm64. `repeat(.)` at 100 ms: 100.1–100.9 and 100.3–100.8. `last(range(1e12))` at 150 ms: 151.3 and 150.2–150.9. `[repeat(.)]` at 50 ms: 52.4 and 53.2–54.2 (it first frees what it built). At 10 ms: 10.9 and 10.7–11.0; at 1 ms: 1.3 and 1.8–2.1. The lateness is the timer thread waking up: a fraction of a millisecond to two. The next call on the same caller: 0.01–0.02 ms, same instance, compiled programs intact | 100.02, 100.02, 150.02, 10.01, 1.01 ms (the clock was read every 200,000 instructions) |
| Time, inside one C call | The 2 ms run out and the call is trapped. EMQX cannot stop such a program at all: its cancel flag is only looked at between jq's steps | `"a" * 8000000 \| test("a+b")` with a 30 ms timeout: EMQX's timeout error after 32.5–32.9 ms (the deadline, the 2 ms, one tick); the next call is served by a fresh instance (52 µs to make, 1 ms to compile the program again) | within the test's 2 s bound |
| Time to compile a jq program | A deadline of its own, 5 s (compiling is outside `jq/3`'s timeout, as in EMQX). A fixed amount of fuel is available instead (`Limits::max_fuel`) where a bound must not depend on the machine: it costs 27% in speed and is off for jq | one compilation takes 1.0–1.1 ms. With fuel on, the same loop stops at the same count on every run (test) | 20,000,000,000 fuel; one compilation burnt 73,000,000 |
| Memory | 64 MiB of linear memory per instance; the `memory.grow` that would pass it traps (a `ResourceLimiter`) | `[range(1e9)]`: stopped after 100–120 ms. `"x" * 1e9`: 0.1 ms. `"a" * 8000000 \| explode`: stopped. Next call unaffected | 955 ms, 0.2 ms |
| Output | The module stops producing once the outputs pass the caller's bound (1 MiB in the prototype; D7 ties it to the message's budget), and the host refuses to read a larger result | `[range(300000)]`, `range(1e7)`, `"x" * 2e6`: refused; `"x" * 1000000`: passes | the same |
| Stack | A call runs on **a stack of its own**: a fiber of 4 MiB plus 256 KiB for the host's frames, with a guard page below it, whatever thread calls and however little stack that thread has left. Compiled code checks its depth against the 4 MiB at every function entry (`Config::max_wasm_stack`). The module's C stack is 1 MiB at the bottom of its linear memory, so overflowing that is an out-of-bounds trap | A module recursing for ever ends in a stack error when called from a thread with a 256 KiB stack, compiled and interpreted (test). jq's own nesting limit (10,000 deep, `tojson`) needs 1.0–1.25 MiB of it; `reduce range(20000) as $i (null; [.])`, which takes an EMQX node down, 2–3 MiB, and returns `[1]`; at 300,000 the call fails and the next one is served | the interpreter's heap stack: 100,000 calls, 8 MiB |
| Host access | A module importing anything outside the 16 allowed functions is refused at load. None of the 16 reaches the operating system: no files (every path and descriptor is refused), no sockets, no randomness; standard output and error are swallowed | `include "/etc/passwd"; .`: jq's "module not found", as in EMQX. `env.sock_send` and `env.system` imports: refused at load (test) | the same |
| Clock and environment | Granted or not, per module (D8) | Without the grants `now` is 0 and `$ENV` is `{}`; with them `now` is the wall clock and `$ENV` holds exactly the listed entries (test) | the same |

The stack is why the `async` feature is on. Without it wasmtime runs a module on the
calling thread's stack and measures the limit from wherever that thread happens to be: a
caller with less stack left than the limit (a tokio worker has 2 MiB in all) would run off
its end, and the crate could not check that without `unsafe`. On a fiber the bound holds
by construction. The cost was measured: none on a call (27.2 µs before and after), 6 µs on
an instantiation, and the fiber's touched pages stay with the warm instance.

Two properties wasmi gave that wasmtime does not: a call cannot be paused and resumed by
the host between two slices of fuel (D6 is rewritten for that), and "the same work stops at
the same instruction count on every machine" is available only with fuel compiled in.

Proof that guest misbehaviour ends in an error and nothing else: a test damages the
hand-written test module 1,000 ways (every truncation, 600 random edits, 50 random bodies)
and loads each; whatever loads is instantiated and called under the limits. A second test
runs 400 jq programs and inputs drawn at random from bytes and jq fragments: each ends in
outputs or one of EMQX's error kinds, and the caller's next call is served.

### 5. State and isolation

- **A module is loaded once** (§8: half a second) and shared; making an **instance** costs
  **52 µs** (its 2 MiB of initial memory is mapped copy-on-write from an image, not copied;
  wasmi: 130–190 µs). No pre-initialised snapshot is needed for that.
- The cost worth caching is **compiling a jq program: 1.0–1.1 ms** (0.6 ms natively; wasmi
  6.5–7.6 ms), because jq parses its own 243-line builtin library for every program. The
  module keeps the 32 most recently used compiled programs (EMQX's NIF keeps 500 per
  thread), at about 15 KiB each.
- **Memory.** An instance's linear memory is 3.4 MiB with one program and 3.8 MiB with 32,
  as under wasmi (the module is the same). Of that the operating system has given a warm
  instance about 1.1 MiB on Linux and 1.5 MiB on macOS (untouched pages of the image and of the fiber's stack are
  not resident). Loading the module adds 7 MiB on Linux arm64 with musl's allocator (20 MiB under amd64 emulation, 42 MiB on macOS, whose allocator keeps more of what the compiler freed) to the process once: the machine
  code and what the allocator keeps of the compiler's working memory.
- A warm instance keeps compiled programs and nothing else. The test runs fourteen calls
  (`halt`, `first(range(10))`, `limit`, errors, `input`) on one instance and each on an
  instance of its own, and the answers are the same; the EMQX oracle includes the same
  pairs in sequence.
- An instance never returns memory. One that has grown past 16 MiB is dropped after its
  call (a 100 KB payload leaves it at 5.4–6.3 MiB).
- Instances are megabytes each, so they must not be per connection: D5. The prototype has
  the pool (`jq::Pool`): callers on any thread take a warm instance for one call and put
  it back; a test runs eight threads through four.

### 6. Speed: 1.9 to 3.1 times slower than native jq (wasmi: 15 to 26 times)

Median time of one call on a warm instance (program compiled), the same libjq sources
built natively with the same compiler for comparison. Apple M1 Pro, one thread.

| Filter | 1 KB: native | wasmtime | | wasmi | | 100 KB: native | wasmtime | | wasmi | |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| `.` | 25 µs | **55 µs** | 2.2× | 446 µs | 18× | 2.6 ms | **5.4 ms** | 2.1× | 42.3 ms | 17× |
| `.a.b` | 14 µs | **27 µs** | 1.9× | 247 µs | 18× | 1.5 ms | **2.6 ms** | 1.8× | 22.0 ms | 15× |
| `.readings \| map(select(.temp > 50)) \| length` | 19 µs | **41 µs** | 2.1× | 379 µs | 20× | 2.0 ms | **4.2 ms** | 2.1× | 34.3 ms | 17× |
| `.readings \| group_by(.id) \| map({id: .[0].id, n: length})` | 30 µs | **77 µs** | 2.6× | 639 µs | 21× | 3.3 ms | **9.8 ms** | 3.0× | 76.4 ms | 23× |
| `.readings \| map(select(.id \| test("^dev-[0-4]$"))) \| length` | 45 µs | **101 µs** | 2.3× | 1,185 µs | 26× | 5.0 ms | **10.8 ms** | 2.2× | 129.1 ms | 26× |
| `[.readings[] \| select(.ok) \| .temp] \| add / length` | 20 µs | **44 µs** | 2.2× | 404 µs | 20× | 2.1 ms | **4.5 ms** | 2.2× | 37.0 ms | 18× |

Where one call's time goes:

| Part | wasmtime | wasmi | When |
|---|---|---|---|
| Load the module | 0.56 s (§8) | 11–12 ms | once per process, when rules that call `jq` are first loaded |
| Instantiate | 52 µs | 130–190 µs | once per pooled instance, and after a stopped call |
| Compile the program | 1.0–1.1 ms | 6.5–7.6 ms | once per program per instance |
| Run (parse the input, run, print) | the table above; at 1 KB most of it is jq parsing the input | the table above | every call |
| Host side: write the input value as JSON, when it is not already a binary | 7 µs at 1 KB, 760 µs at 100 KB | the same | every call with a non-binary input |
| Host side: read the outputs with the rule engine's reader | 0.1–7 µs for these outputs; 770 µs to read back 100 KB | the same | every call |

In broker terms: a rule that calls `jq` on a 1 KB payload costs its publisher's connection
task 27–102 µs per message, so one core evaluates ten to thirty-five thousand such
messages a second (under wasmi: one to four thousand). At 1 KB the host's own share (7 µs
to write a non-binary input as JSON) is now a quarter of the fastest call rather than
noise, which is the argument for handing payloads over as the binaries they already are.
Rule SQL's own functions still do the same selections in microseconds; `jq` is for what
only jq expresses, and for rules arriving from EMQX unchanged.

What the other ways of compiling the module would give, measured on the same machine
(`.a.b` on 1 KB; the other filters move in proportion):

| Configuration | Module load | Call | |
|---|---:|---:|---|
| **Cranelift, optimising, traps by signal (chosen)** | 562 ms | 27 µs | wasmtime's default and so its most tested and most fuzzed configuration |
| Cranelift, `OptLevel::None` | 435 ms | 26 µs | the module was already optimised by the C compiler; not chosen, to stay on the default path |
| Cranelift, single-pass register allocation | 371 ms (242 ms with `OptLevel::None`) | 64–66 µs | 2.4× slower code for a third of the load time |
| Winch (wasmtime's baseline compiler) | 68 ms | 48 µs | 1.7× slower code; a second compiler in the build |
| Cranelift with explicit bounds checks instead of signal handlers | 1,200 ms | 36 µs | no fault handler, but 1.3× slower and twice the load time |
| Cranelift with fuel accounting | 728 ms | 35 µs | 1.27× slower; the deterministic bound of §4 |
| Pulley, wasmtime's interpreter (`Execution::Interpreted`) | 663 ms | 689 µs | 25× slower than compiled, 2.8× slower than wasmi: the fallback of §8 |
| wasmi 2.0.0 | 11–12 ms | 247 µs | the alternative considered |

### 7. Exactness: 278 of 285 cases identical to EMQX, byte for byte

`jq-module/emqx-oracle.py` ran 285 program-and-input cases through
`jq:process_json(Program, Input, 10000)` on EMQX 6.3.1 (the call `emqx_rule_funcs:jq/2`
makes) and recorded jq's raw output texts, or the error tag and message, in
`tests/emqx-jq-oracle.txt`. The test runs each through the module and compares bytes.

| | Cases |
|---|---:|
| Identical: outputs as jq printed them, or tag and message | 278 |
| Different | 6 |
| EMQX has no answer: the call took the node down | 1 |

The 278 include every error tag (`jq_err_compile`, `jq_err_parse`, `jq_err_process`,
`timeout`) with its text; integer literals of any size; `1e1000` printed `1E+1000`; `nan`,
`-0`, `1.10`; 17-digit doubles from arithmetic; duplicate keys; lone surrogates; a NUL in
the program or the input; invalid UTF-8; `$ENV`, `env`, `now`, `input`, `halt`, `debug`,
`include`, `import`, `$__loc__`; oniguruma's regex flags and Unicode classes; nesting 10,000
deep (jq's own limit) and its error at 10,001; and the two programs that run into EMQX's
ten-second timeout.

The six that differ, all from the C library under jq, not from jq:

| Cases | Difference | Cause |
|---:|---|---|
| 4 | 18 of 135 values of transcendental functions differ in the last one or two printed digits: `tgamma` (5), `cbrt` (4), `exp10` (3), `j0` (2), and one each of `lgamma`, `y0`, `log10` and `tan` | wasi-libc's libm is musl's; EMQX's is the glibc of its build host. `pow`, `sqrt`, `sin`, `cos`, `exp`, `log`, `atan`, `asinh`, `erf`, the rounding functions and all arithmetic are identical on the tested values |
| 1 | `strptime` with `%z` fails | wasi-libc's `strptime` has no `%z` |
| 1 | `strftime("%Z")` on a UTC broken-down time gives `UTC`, EMQX `GMT` | musl against glibc |

The one EMQX cannot answer: `jq("reduce range(20000) as $i (null; [.]) | 1", 1)` ends the
EMQX node (the NIF overflows the C stack freeing a 20,000-deep array). In the sandbox it
returns `[1]`; at 300,000 deep it fails the call.

A further 45 cases check the marshalling on rule values, against the pinned transcript of
`emqx_rule_funcs:jq/2`: a binary is handed to jq as text; any other value is written with
the rule engine's JSON writer first; the outputs are read with the rule engine's reader. All
45 match, including `123456789012345678901234567890` staying exact, `. + 1` on it giving
`123456789012345680000000000000`, `1.0` staying a float and `-0` an integer, and `1e1000`
failing the rule (EMQX's decoder refuses `1E+1000`; so does ours).

Not exact by construction, and stated in D7: the memory, output and stack limits. EMQX has
none; a program that would consume 10 GiB there fails here.

**Under wasmtime the results are the same as under wasmi, case for case**: the module is
the same file, and the 285 oracle cases (278 identical, the same six differing, the same
one answered where EMQX goes down) and the 45 marshalling cases give the same bytes. They
also give them as static musl binaries on Linux arm64 and amd64, and in the interpreter:
a test runs every oracle case EMQX answered within its timeout through compiled code and
through Pulley and compares the two, byte for byte. One thing moved and was put back: with
wasmtime's default stack the 10,000-deep case and the 20,000-deep one ended in a stack
error; the limit is 4 MiB for that reason (§4).

**libm: documented, not chased** (decided 2026-10-11). The 18 last-digit differences and
the two time formats stay as they are and go into RULES.md's differences table with T4;
`strptime %z` and `strftime %Z` may still be closed in the module's shim (T2).


### 8. What the compiler costs and needs

**Start-up.** `Module::new` validates the 985 kB module and compiles all of its 958
functions with Cranelift on one thread:

| Host | Module load |
|---|---:|
| Apple M1 Pro, macOS | 562 ms |
| The same machine, Linux arm64 container, static musl binary | 777–807 ms |
| The same machine, Linux amd64 container under emulation, static musl binary | 1,054 ms (emulated: an upper bound, not a measurement of an amd64 server) |
| GitHub's `ubuntu-latest` runner (amd64), glibc | printed by the last step of the `wasm sandbox` job |

The broker pays this **once per process, and only if its rules call `jq`**: the module is
compiled when a rules file that names `jq` is first loaded (boot, reload, the admin API,
`--check-rules`), on the thread doing the load, never on a publisher's connection task and
never at the first message. A broker without `jq` rules never constructs the engine, starts
no timer thread, installs no signal handler and maps nothing executable (`jq::Pool` in the
prototype: making one costs nothing, `load()` compiles). During the load the process
allocates about 7 MiB on Linux arm64 with musl's allocator (20 MiB under amd64 emulation, 42 MiB on macOS, whose allocator keeps more of what the compiler freed) that it keeps.

The ways to shorten it, and why none is taken:

- **wasmtime's compilation cache** (`Config::cache`): machine code written to a directory
  and loaded back on the next start. Not taken. The shipped container has a read-only root
  file system and nowhere to write it; a cache directory is a place where whoever can write
  a file gets code executed in the broker; it brings `zstd` (a C library) into the build;
  and loading it is `Module::deserialize` inside wasmtime, the very call this crate may not
  make itself.
- **Precompiling at build time** and embedding the machine code: needs
  `Module::deserialize`, which is `unsafe` because the bytes are trusted to be what the
  compiler wrote. Ruled out by the workspace's `unsafe_code = "forbid"`.
- **Winch**, wasmtime's baseline compiler: 68 ms instead of 562 ms, and code 1.7× slower on
  every call for the life of the process. Half a second once is the better trade for a
  broker, and Winch is a second compiler to trust and to keep building.
- **Parallel compilation** (`rayon`): divides the half second by the core count and adds a
  thread pool and its crates to the broker. Not worth it for a one-time cost.
- **`OptLevel::None`**: 23% off the load with no measurable loss on these filters. Not taken
  only to stay on the configuration wasmtime tests and fuzzes most; cheap to revisit.

**Executable memory.** Compiled code needs pages the process may make executable
(`mmap` writable, then `mprotect` to read-and-execute). Where that is forbidden:

| Environment | Compiled | Interpreted (Pulley) |
|---|---|---|
| The release image (`distroless/static:nonroot`), run as `deploy/compose` does: read-only root file system, all capabilities dropped, `no-new-privileges`, uid 65532 | runs: the probe, a jq call and the measurements (module load 784–807 ms), on Linux arm64 | runs |
| Kubernetes with the chart's `securityContext` (non-root, no privilege escalation, read-only root, all capabilities dropped; the chart sets no seccomp profile, and the container runtimes' default profile does not restrict `mprotect`) | runs: the same settings as the row above | runs |
| **`deploy/systemd/mqttd.service`, which sets `MemoryDenyWriteExecute=true`** | **refused**: "unable to make memory executable … Permission denied" under `prctl(PR_SET_MDWE)`, "Operation not permitted" under a seccomp filter on `mprotect(PROT_EXEC)` (both in a container), and under the unit's own properties with `systemd-run` in CI | runs (the same three ways) |
| A seccomp profile denying `mprotect(PROT_EXEC)`, or SELinux with `execmem` denied to the broker's domain | refused the same way | runs |
| A `noexec` mount of `/tmp` or of the data directory | runs: nothing is mapped from a file | runs |

So the hardened unit this repository ships and jq at full speed exclude each other, and
that is the main thing to weigh against wasmtime. The failure mode is designed, not
left to chance (D13): the refusal is an error value from `Sandbox::load`
(`LoadError::Engine`, "the WebAssembly compiler cannot run on this host (it needs memory it
may map executable): …"), never a panic and never a signal; `Execution::probe()` answers
the question without a module; and CI runs `examples/probe` under `systemd-run` with the
unit's own hardening properties and requires exactly "compiled refused, interpreted runs".

For those hosts the crate has a second way to run the same module:
`Execution::Interpreted` compiles it to bytecode for Pulley, wasmtime's own portable
interpreter. No executable memory, no signal handlers, the same limits, and the same
answers byte for byte (a test runs every oracle case both ways). It is 25× slower than
compiled code and 2.8× slower than wasmi: `.a.b` on 1 KB takes 689 µs, the 100 KB filters
55–370 ms, compiling a jq program 19 ms.

**Keeping wasmi as the fallback instead** was considered and is not recommended. It would
be 2.8× faster than Pulley on the hosts that need a fallback. It would also be a second
runtime with its own `unsafe`, its own limits code (fuel slices instead of epochs), its own
ten crates, and a second column in every test: two sandboxes to keep sound instead of
one. Pulley comes with wasmtime (the runtime links it regardless), shares every line of the
host code, and its slowness is confined to operators who chose both jq and
`MemoryDenyWriteExecute`.

**Signals.** In compiled mode wasmtime installs process-wide handlers for `SIGSEGV`,
`SIGBUS`, `SIGILL` and `SIGFPE` when the engine is first made (out-of-bounds accesses and
`unreachable` are caught as faults; a fault that is not in compiled module code is passed
on to the previous handler). The broker installs none of these itself today. Each linear
memory reserves 4 GiB and a guard region of address space (not memory), which is what lets
compiled code skip bounds checks; a process limited by `ulimit -v` would have to allow for
it. Explicit bounds checks instead (`signals_based_traps(false)`) were measured at 1.3×
slower and twice the load time and are used only by the interpreter.

**Static musl.** With the release recipe's flags (`scripts/release/build-repro.sh`:
`+crt-static`, stripped, remapped paths, `musl-gcc`) the crate's tests and examples build
for `aarch64-unknown-linux-musl` and `x86_64-unknown-linux-musl`, and all 36
tests pass on both as static binaries, the 285 oracle cases included. wasmtime needs
nothing from the C library that musl lacks.

**Size.** A static, stripped `aarch64-unknown-linux-musl` release binary that links the
sandbox with compiler and interpreter is 6.4 MB, and 9.7 MB for
`x86_64-unknown-linux-musl` (an empty program built the same way: 0.4 MB): about 6.0 MB on
aarch64 and 9.2 MB on x86_64 added to a broker, plus the 985 kB module.
The 1.1.0 image is 16.8 MB. Under wasmi the same figure was about 2.5 MB.

**Reproducible build.** The release pipeline builds twice and compares sha256 (ADR 0045).
The examples were built twice in the container with the release recipe, with `cargo clean`
between: byte-identical, all four binaries (`aarch64-unknown-linux-musl`). Cranelift is a compiler that is itself compiled, and nothing in its
build depends on time or host. The machine code it generates at run time is not part of
the binary and not covered by that gate.

**MSRV.** wasmtime 49 needs Rust 1.96. The toolchain is pinned at 1.97.0, so every build of
this repository is unaffected; but the workspace manifest declares `rust-version = "1.88"`,
and the crate declares 1.96 on its own. When `mqttd` depends on the crate (T2), the
workspace's declared minimum has to move to 1.96. wasmtime raises its minimum with every
monthly release (its policy is the current stable compiler and the two before it), so security updates of the
runtime will keep pulling the toolchain pin forward: a standing cost of this choice.

**The FIPS variant** (`cargo check -p mqttd --features fips`) is untouched: `mqttd` does
not depend on the crate yet. When it does, the runtime must be checked for a second
cryptographic provider: there is none in this feature set (`sha2` 0.10 is compiled in for
the keys of Cranelift's incremental-compilation cache, which is not enabled; nothing the
FIPS boundary covers).

## Decision

### D1. Functions may be provided as WebAssembly modules, run in a sandbox by wasmtime

A new crate, `mqtt-wasm-sandbox`, loads modules and runs calls into them with wasmtime. It
has no `unsafe`, uses only wasmtime's safe API, and does no I/O. `mqtt-rules` depends on it
and nothing else changes shape: a sandboxed function is an entry in the function table like
any other, taking and returning `Value`s. The rule engine still holds no broker state.

The runtime is wasmtime with the features `runtime`, `cranelift`, `async`, `pulley` and
`std` (§3). A module is validated and compiled when it is loaded; the WebAssembly features
it may use are the ones a C module built for `wasm32-wasi` needs (bulk memory, multi-value,
sign extension, non-trapping conversions, mutable globals) and no others: no SIMD, no
threads, no tail calls, no 64-bit memories, no garbage collector, no component model. NaN
results are canonicalised, so a module's answers do not depend on the processor.

### D2. One small, versioned ABI

A module exports its memory and:

```text
mqttd_abi_version() -> u32                      1
mqttd_alloc(len: u32) -> ptr                    memory the host writes arguments into
mqttd_free(ptr)
mqttd_describe(ret: ptr)                        JSON: module name, version, functions with min and max arity
mqttd_call(name, name_len, args, args_len, max_out, ret: ptr) -> status
mqttd_cancel()                                  optional: ask the running call to stop
```

`ret` receives an address and a length; the host copies the bytes and frees them.
`status` 0 is a value; any other status fails the rule, with the bytes as the message.
Reserved by the host: nothing — a module's statuses are its own (the jq module uses the
NIF's numbers). A module that speaks another version, lacks an export, has another
signature, or imports a function that is not provided, is refused when the rules are loaded.

`mqttd_call` takes the function's name, so one module may provide several functions.
`_initialize`, when exported, is run once per instance (C and Rust reactors have it).

### D3. Values cross as the rule engine has them: binaries as bytes, everything else as JSON text

The argument frame is a count, then per argument a tag byte, a length and the bytes:
`b` for a binary (a string or a payload), its bytes as they are, UTF-8 or not; `j` for any
other value, as the JSON text the rule engine's writer produces. A result is one tagged
value in the same form, read with the rule engine's reader.

This is EMQX's own rule for `jq` (a binary goes to jq as text, any other term is encoded
first), so the jq module needs no special case, and it keeps what a compact binary encoding
would have to re-specify: integers of any size, floats that stay floats, key order. A
binary encoding (CBOR, MessagePack) would be faster to read on both sides, but the measured
cost of the JSON on the host side is 7 µs per KB against hundreds for the call. Recommended:
JSON, and no second encoding until a function exists whose cost it dominates.

### D4. `jq` is the first module, built in

The jq module is embedded in the broker binary (985 kB, and 6.0 MB on aarch64 and 9.2 MB on x86_64 of runtime, added
to a 16.8 MB image; §8) and provides `jq/2` and `jq/3` with EMQX's semantics:

- the program must be a binary; the input is handed over as D3 says; the result is the list
  of all outputs; outputs before an error are discarded; program and input end at the first
  NUL;
- errors are EMQX's tags and texts and fail the rule;
- `jq/2` runs under `rules.jq_default_timeout_ms`, default 10000 as EMQX's
  `rule_engine.jq_function_default_timeout`; `jq/3`'s third argument is validated as EMQX
  validates it (a non-negative integer or `infinity`, else `bad_timeout_val`);
- compiling a program is outside the timeout, as it is in EMQX (the cancel flag is only
  looked at while a program runs), and bounded by a deadline of its own instead (5 s; one
  compilation takes a millisecond).

A cargo feature leaves the module and the runtime out of a build that does not want them;
`jq` is then refused at load as today.

### D5. The module is compiled when rules first name `jq`; instances are pooled per node

Compiling the module takes half a second (§8). It happens when a rules file that calls
`jq` is loaded (boot, reload, the admin API, `--check-rules`), on the thread doing that
load, and its failure is that load's failure with the reason. A broker whose rules do not
call `jq` pays nothing: no engine, no thread, no handler, no executable page.

An instance is megabytes. The broker keeps a pool of instances per module, as many as it
has worker threads; an evaluation takes one for the duration of one call and puts it back
(`jq::Pool` in the prototype). Instances are made on first use and when one was stopped
(52 µs). When rules are loaded, the literal jq programs in them are compiled into every
pooled instance off the publish path, so the millisecond is not paid by a publisher. A
program that comes from a payload (`jq(payload.prog, payload)`, which EMQX allows) is
compiled on its first use per instance, under the compile deadline, and takes a cache slot
like any other.

### D6. A long call must not hold a worker

Rule SQL is evaluated synchronously on the publisher's connection task (ADR 0083). A jq
call may legitimately take milliseconds and is allowed ten seconds by EMQX's default.
Under wasmi the call was resumable between slices of fuel. Under wasmtime it is a
future on a fiber (§4, Stack), and the deadline callback can suspend it
(`UpdateDeadline::Yield`): the host polls the call once, which runs it for a first budget
(proposed: 1 ms); if it has not returned, the timer's tick suspends it and the host goes
on polling it under `tokio::task::block_in_place`, so the worker's other connections are
moved to other workers. The hub loop is never involved. As before, this is a proposal the
prototype does not exercise (its calls never yield); it is task T2's to prove, with a test
that a ten-second jq on one connection does not delay another connection's publish. At
27–102 µs for the usual call, the hand-off is for the exception.

### D7. Limits per call, charged to the message's budget

| Limit | Value | On reaching it |
|---|---|---|
| Time | the function's timeout (`jq/3`), or the module's declared default | EMQX's `timeout` error for jq; a `timeout` error for a plugin. The rule fails |
| Compiling a jq program | 5 s per program | the rule fails; the instance is dropped |
| Memory | 64 MiB per instance (plugins: declared, at most the broker's ceiling) | the rule fails; the instance is dropped |
| Output | what is left of the message's 1 MiB build budget (`MAX_BUILT_BYTES`), charged when the call returns | the rule fails |
| Stack | 4 MiB of call stack on the call's own fiber, 1 MiB of C stack in the module's memory | the rule fails; the instance is dropped |

A limit is an evaluation error like any other: the rule fails for this message, counted by
kind, never the message or the connection (ADR 0083 §8). It is never "no result": a rule
must not silently pass a `WHERE` because a function was cut off. For `jq` the memory, output
and stack limits are a documented difference from EMQX, which has none.

Sandboxed functions may be called anywhere an expression may, including `WHERE`. The cost
is the author's to weigh; the rule trace shows it (D10).

### D8. A module gets nothing it is not granted

| Grant | jq (built in) | Operator plugin |
|---|---|---|
| Wall clock | yes: `now`, `localtime` and `mktime` work as in EMQX (time zone: UTC; wasi-libc has no zone database) | off unless `clock = true`; without it every clock reads 0 and a function's result depends on its arguments alone |
| Environment | **empty unless listed** (`rules.jq_env = ["SITE", …]`); decided 2026-10-11 | empty unless listed |
| Randomness | none (jq has no use for it) | none in ABI 1 |
| Files, network | never | never |

The environment is the one place this record leaves EMQX knowingly, and the decision is
taken (2026-10-11): **`$ENV` and `env` are empty unless the operator lists variables.** In
EMQX they read the broker's whole environment. A rule may take its program from a payload,
and what jq returns can be republished, so that is a way for a publisher to read `MQTTD_*`
secrets passed by environment. Exposing nothing by default costs the departures table one
row, which T4 writes: a rule ported from EMQX that reads `$ENV.X` sees `null` until `X` is
listed.

### D9. Operator plugins: declared in the rules file, pinned by hash

```toml
[functions.geo]
wasm = "plugins/geo.wasm"       # relative to the rules file
sha256 = "9f2c…"                # the load fails on any other content
functions = ["geo_distance", "geo_within"]   # must be among those the module describes
timeout_ms = 50
memory_mib = 16
clock = false
env = []
fresh_instance = false          # true: no state survives a call, at 52 µs each
```

- The file is read, hashed and validated when the rules are loaded (boot, reload, the admin
  API, `--check-rules`). Any failure is a load error with the reason; the running set stays.
- Function names must not collide with a built-in or with any EMQX function name, so a
  plugin can never change what an EMQX rule means. A rule calling an undeclared function is
  refused at load, as today.
- Rules are per-node files (ADR 0084), and so are plugins: the file must exist on every
  node, and the hash in the rules file is what makes "the same plugin everywhere" checkable.
  The rules digest covers the `[functions]` table and therefore the hashes.
- Live editing (ADR 0084) reloads plugins with the rules: a changed hash loads the new
  module, in-flight calls finish on the old one.
- The admin API lists, per module: name, version, sha256, functions, limits, grants,
  instances and their memory. It never serves the module's bytes.
- ABI 1 has no host functions beyond D8: a plugin cannot read broker state, publish, or call
  another function. Each of those would be a new decision.

### D10. Metrics and trace

Per function: calls, errors by kind (`timeout`, `memory`, `output`, `stack`, `trap`, the
module's own), time (`mqttd_rule_function_seconds_total{function}` beside
`mqttd_rule_eval_seconds_total`); per module: instances, instance memory, instances
replaced, and how it runs (compiled or interpreted). The rule trace records a failed call's kind, and for jq EMQX's message.

### D11. Trust model

- A plugin is operator-supplied code, with the rules file's trust for what it *computes*: it
  can return wrong values, and the operator chose it. It is not trusted with anything else.
- What a malicious or broken module can do: burn its time and memory limits on every call
  it gets, and return any value its functions are typed to return.
- What it cannot do: read or write memory outside its own; reach files, sockets, the clock
  or the environment without the grant; keep the broker from stopping it; survive its own
  trap; see another module's or another instance's memory. It can keep state between calls
  inside one instance (jq's program cache does), so two messages evaluated on the same
  instance are not isolated from each other *by the sandbox*: a plugin that must not
  correlate messages has to be written not to, or run with `fresh_instance = true`, which
  costs an instantiation (52 µs) per call.
- **These hold as long as wasmtime is sound, and with compiled code that means its
  compiler too.** The module's code is translated to machine code and run by the
  processor; what keeps it inside its linear memory is the bounds and guard pages Cranelift
  arranges, the runtime's signal handling, and 1,594 `unsafe` sites in the runtime (§3). A
  miscompilation is a sandbox escape, and Cranelift has had such bugs (they are published as
  security advisories against `wasmtime` and `cranelift-codegen`, which `cargo audit` gates
  here). What stands on the other side: the configuration used is wasmtime's default and
  most fuzzed one; the WebAssembly features a module may use are cut to the few a C module
  needs (D1), which removes whole areas of the compiler from reach; and the bytes compiled
  are, for jq, a module built from pinned sources, and for a plugin, a file the operator
  pinned by hash. A payload reaches the compiled code only as *data* to jq, never as
  WebAssembly: no publisher can hand the compiler input. The interpreter (D13) removes the
  compiler's output from the trust base, not the runtime.
- Operator plugins therefore remain "code the operator vouches for", and the built-in jq
  module is built from pinned sources.
- **New threat-model rows for T4** (THREAT-MODEL.md is not changed by this record): a
  compiler bug reachable from a hostile *module* (mitigated by the hash pin and the feature
  cut, residual); executable pages in the broker's address space (W^X is kept: pages are
  written, then made read-execute, never both); process-wide fault handlers; the timer
  thread; a start-up cost an operator can trigger by loading rules.
- **Supply chain of the jq module** (decided 2026-10-11: built in the pipeline, not
  committed). Built from source tarballs pinned by sha256, by a compiler pinned by version
  and by its archive's sha256 (`jq-module/fetch-zig.sh`), with a byte-reproducible result
  whose hash is in the tree. CI builds it and fails on a different hash (T1). The
  release build will embed the file it built (T2, when the broker links the crate), so the
  existing build-twice gate (`release.yml`) covers it; the release notes carry the module's
  hash and the source commits. The alternative, committing the 985 kB file with a reproduce
  script, would have removed zig from the release pipeline at the cost of a binary in the
  repository that reviewers cannot read.

### D12. An mqttd extension

EMQX has no WebAssembly plugins (it has `jq` as a NIF, and Erlang plugins and external
functions). `[functions.*]` is mqttd's own and is listed under "Differences from EMQX";
a rules file that uses it does not port back. `jq` itself is EMQX's function with EMQX's
results, within the limits of D7 and the differences of spike §7.

### D13. Compiled by default; interpreted by the operator's choice; never a crash

`rules.jq_engine` (and `engine` on a plugin) is `"compiled"` (default) or `"interpreted"`.

- **Compiled** is Cranelift's machine code. On a host that forbids executable memory the
  module cannot be loaded, and that is reported where the operator is looking: the rules
  file that calls `jq` **fails to load**, at boot, on reload, through the admin API and in
  `--check-rules`, with "jq is not available: the WebAssembly compiler cannot run on this
  host (it needs memory it may map executable): …", and the message names
  `rules.jq_engine = "interpreted"` and `MemoryDenyWriteExecute` as the two ways out. The
  running rule set stays as it was (ADR 0084). Rules that do not call `jq` are untouched.
  Nothing panics and no signal is raised: wasmtime returns the failed `mprotect` as an
  error, which CI checks under the shipped unit's hardening (§8).
- **Interpreted** is the same module in Pulley: 25× slower, no executable memory, no
  signal handlers. It is never chosen silently. A broker that fell back by itself would
  turn "my rules are 25× slower than on the other node" into something to be discovered
  in production.
- `mqttd --check-config` reports which of the two the host allows (`Execution::probe`).

For the operator of the shipped systemd unit the choice is explicit: keep
`MemoryDenyWriteExecute=true` and run jq interpreted, or drop that one line in a drop-in
and run it compiled. `deploy/systemd/README.md` and HARDENING.md say so when the function
ships (T2, T4); until then `jq` is refused at load and the unit is right as it is.

## Phasing

1. **T1 — the sandbox crate and the jq module's build.** Delivered on 2026-10-11: the
   prototype on wasmtime; the loader fed a thousand damaged modules and jq four hundred
   programs and inputs drawn at random (tests); the module built in CI from sources and a
   compiler pinned by hash and compared with the recorded hash; the oracle and limit tests
   in CI; the failure mode on a host without executable memory checked in CI. Still to do:
   a coverage-guided fuzz target for the loader in the nightly tier (the damage tests are
   fixed inputs, not a fuzzer).
2. **T2 — `jq/2` and `jq/3` in the rule engine**, behind the ABI: the pool (D5), the
   worker hand-off (D6), the budget (D7), the grants (D8), metrics and trace (D10), the
   engine choice and its load error (D13), the module embedded by the release build,
   `--rule-test` support, the workspace's declared Rust version (§8), and the remaining
   differences closed where they can be (`strptime %z`, `%Z`).
3. **T3 — operator plugins** (D9), with a worked example in Rust and one in C.
4. **T4 — documentation**: RULES.md (the function, its limits, its cost), the departures
   table, THREAT-MODEL.md and HARDENING.md rows, the cookbook.

T3 does not have to follow T2 immediately. The ABI is the same either way, and T2 alone
delivers `jq`.

## Consequences

- `jq` works, with jq's own results, and a jq fault is an evaluation error instead of a
  broker fault.
- A `jq` call costs two to three times what native jq costs (which is what EMQX runs):
  27–102 µs per 1 KB message. Documented where the function is.
- The broker gains a compiler. Its binary grows by about 6.0 MB on aarch64 and 9.2 MB on x86_64 and the module's
  985 kB, the lockfile by 54 entries, the license allow list by one expression, and the
  release pipeline by one pinned compiler (zig). The first load of rules that call `jq`
  takes about half a second.
- The broker's memory safety now also rests on wasmtime and on the machine code Cranelift
  generates (D11). Its security advisories become ours to follow, and its minimum Rust
  version moves ours (§8).
- Compiled `jq` and the shipped systemd unit's `MemoryDenyWriteExecute=true` exclude each
  other; the operator chooses (D13).
- The broker gains a general, bounded extension point. It also gains the obligation to keep
  an ABI stable.
- Transcendental functions in jq can differ from EMQX in the last digit, and two time
  formats differ, until closed.
- The same module gives the same answers on every platform mqttd runs on. EMQX's depend on
  the C library of each build.

## Alternatives considered

- **Link libjq natively behind a safe wrapper crate.** Exact and fast. Rejected: C in the
  broker's address space on payload-driven input, against the workspace's `unsafe` rule, and
  the spike reproduced a crash of exactly that arrangement in EMQX.
- **A `jq` subprocess.** Exact and isolated by the operating system. A viable fallback: it
  means shipping and supervising a second binary, a pipe round trip per call, and process
  limits that differ per platform. The sandbox does the same isolation in-process, with a
  deterministic bound.
- **jaq.** Pure Rust and fast, and not jq: its results differ, which the rule engine's
  contract does not allow.
- **wasmi** (an interpreter in pure Rust; the spike's first recommendation). No generated
  machine code, no executable memory, 10 crates, an allowed license, 11 ms to load, and it
  runs under `MemoryDenyWriteExecute`. 15–26× slower than native jq where wasmtime is
  1.9–3.1×. Not chosen: the maintainer decided for speed (2026-10-11). Its measurements
  stay in §3–§6 as the cost of that decision, and it remains the fallback if the compiler
  in the trust base is ever judged the wrong trade.
- **wasmtime with wasmi as the fallback for hardened hosts.** Two runtimes to keep sound;
  Pulley does the job inside one (§8).
- **Keep refusing `jq`.** The status quo, and what a build without the feature still does.
  It leaves every EMQX rule that uses `jq` unportable.

## Open questions

Decided on 2026-10-11 and no longer open: the runtime (wasmtime), `$ENV` (empty unless
listed), the module's place in a release (built in the pipeline), libm (documented).

1. **The shipped systemd unit and compiled jq** (D13). Proposed: the unit keeps
   `MemoryDenyWriteExecute=true`, and an operator who wants jq compiled removes it in a
   drop-in. The alternative is to ship the unit without it once `jq` exists, which weakens
   the default for every installation to speed up the ones that use `jq`.
2. **The compiler in the trust base** (D11). Accepted with the runtime decision; recorded
   here because it is the part of that decision that cannot be measured.
3. **The workspace's Rust version** has to go from 1.88 to 1.96 when the broker links the
   crate, and will follow wasmtime's from then on (§8).
4. **The worker hand-off (D6)** is designed, not measured, and now rests on wasmtime's
   fibers.
5. **`OptLevel::None`** would take a quarter off the half-second load at no measured cost
   (§6). Left at the default.
6. **Program cache size.** 32 per instance in the prototype against EMQX's 500 per thread;
   at 15 KiB per program 500 would cost about 7 MiB per instance.
7. **Operator plugins now or later** (T3), and whether ABI 1 should already carry a way to
   declare argument types so that arity and type errors are load errors.
8. The pinned source tarballs come from GitHub's archive endpoint, whose bytes are stable
   in practice but not by contract; a mirror of the tarballs as a release asset would
   remove that. (CI caches the built module, so an outage there fails only a run that has
   to rebuild.)
