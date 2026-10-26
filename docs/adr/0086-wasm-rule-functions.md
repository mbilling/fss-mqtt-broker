# ADR 0086 — Rule functions in a WebAssembly sandbox: the real jq first, operator plugins on the same mechanism

- **Status:** Accepted
- **Date:** 2026-10-10 (runtime decision, re-measurement and acceptance: 2026-10-11)
- **Deciders:** project maintainers
- **Delivery:** [docs/delivery/0086-wasm-rule-functions.md](../delivery/0086-wasm-rule-functions.md) — plan, progress, and changelog
- **Revisits:** [ADR 0083](0083-rule-engine.md) (functions: `jq` is refused at load). This
  record decides how `jq` is provided, with EMQX's results.
- **Related:** [ADR 0084](0084-watching-and-editing-rules-live.md) (live editing, the admin
  rules endpoints and the trace this extends), [ADR 0085](0085-rule-state-store.md) (the
  other extension of the function set, accepted 2026-10-11; its D14 says how a state
  call fits this record's evaluation model, and that a module cannot call state), [ADR 0045](0045-release-engineering-and-distribution.md)
  (reproducible builds, which the module's build joins).

> This record states the decision and the measurements it rests on. The sandbox is
> `crates/mqtt-wasm-sandbox`; since T2 the rule engine calls `jq` through it, and since T3
> the modules a rules file declares. What building T2 and T3 settled, measured and
> corrected, and what T4 checked while documenting it, is in [Amendments](#amendments)
> at the end, with the decision of 2026-10-11 that no call runs longer than a
> broker-wide ceiling (`rules.function_max_ms`).
>
> **Accepted on 2026-10-11.** Decided with it: the runtime
> is **wasmtime, chosen for speed** over the wasmi interpreter the spike first recommended
> (§3, §8); `$ENV` and `env` inside jq are **empty unless the operator lists variables**
> (D8); the jq module is **built in the pipeline from pinned sources** and compared with a
> recorded hash, not committed (D11); the last-digit differences of libm are **documented,
> not chased** (§7); `jq` is **off unless the operator switches it on** (`rules.jq`), and
> the shipped systemd unit keeps `MemoryDenyWriteExecute=true`, which the operator who
> switches `jq` on sets to `false` (D13).

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
both: what the spike measured for jq, and the design of the general mechanism.

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
`sha256sum --check` compares the result with `jq.wasm.sha256`. The first run built
`728129fa…c8c57` on GitHub's amd64 runner: the fourth host to give those bytes.

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
| Module load | **0.56 s** on an Apple M1 Pro, 0.78 s on Linux, arm64 and amd64 (§8): the whole module is compiled, once per process | 11–12 ms (validation and translation) |
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
| Time (`jq/3`'s timeout) | Epoch interruption: compiled code looks at a counter in every loop and at every function entry. One timer thread per loaded module sleeps until the earliest deadline among the calls now running and then advances the counter; while no call with a deadline runs it waits on a condition variable and does not tick. At its deadline a call's module is asked to stop (`mqttd_cancel`, which is `jq_cancel`: the program ends at its next step) and given 2 ms; a module still running after that is stopped by a trap and its instance dropped. A call always gets 1 ms, so a timeout of 0 still runs what is quick, as in EMQX | Never early. On Linux amd64 (GitHub's runner): `def f: f; f` at 100 ms stopped after 100.11 ms, `repeat(.)` at 100 ms after 100.09, `last(range(1e12))` at 150 ms after 150.10, at 10 ms after 10.08, at 1 ms after 1.08; `[repeat(.)]` at 50 ms after 51.96 (it first frees what it built). On macOS and in a Linux VM on it the timer thread wakes later: 100.1–102.0, 150.2–151.3, 10.7–11.0, 1.3–2.1 ms. The next call on the same caller: 0.01–0.02 ms, same instance, compiled programs intact | 100.02, 100.02, 150.02, 10.01, 1.01 ms (the clock was read every 200,000 instructions) |
| Time, inside one C call | The 2 ms run out and the call is trapped. EMQX cannot stop such a program at all: its cancel flag is only looked at between jq's steps | `"a" * 8000000 \| test("a+b")` with a 30 ms timeout: EMQX's timeout error after 32.5–32.9 ms (the deadline, the 2 ms, one tick; 32.6 on the runner); the next call is served by a fresh instance (52 µs to make, 1 ms to compile the program again) | within the test's 2 s bound |
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
  not resident). Loading the module adds 7 MiB on Linux arm64 with musl's allocator (28 MiB with glibc on GitHub's amd64 runner, 42 MiB on macOS: allocators that keep more of what the compiler freed) to the process once: the machine
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
| GitHub's `ubuntu-latest` runner (amd64), glibc | 779–782 ms |

The broker pays this **once per process, and only if its rules call `jq`**: the module is
compiled when a rules file that names `jq` is first loaded (boot, reload, the admin API,
`--check-rules`), on the thread doing the load, never on a publisher's connection task and
never at the first message. A broker without `jq` rules never constructs the engine, starts
no timer thread, installs no signal handler and maps nothing executable (`jq::Pool` in the
prototype: making one costs nothing, `load()` compiles). During the load the process
allocates about 7 MiB on Linux arm64 with musl's allocator (28 MiB with glibc on GitHub's amd64 runner, 42 MiB on macOS: allocators that keep more of what the compiler freed) that it keeps.

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
host code. (D13 in the end offers operators neither: `jq` is a switch, and switching it
on means compiled code.)

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
mqttd_cancel_slot() -> ptr                      optional: where the running call's stop flag is
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

`jq` is off unless the operator switches it on (`rules.jq = true`, D13); while it is off,
`jq` is refused at load as today. A cargo feature leaves the module and the runtime out of
a build that does not want them at all; the switch then has nothing to turn on, and says
so.

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
| Time | the function's timeout (`jq/3`), or the module's declared default; never more than the broker's ceiling on one call (`rules.function_max_ms`, [amendment of 2026-10-11](#2026-10-11--a-ceiling-on-how-long-any-one-call-may-run-rulesfunction_max_ms)) | EMQX's `timeout` error for jq; a `timeout` error for a plugin. The rule fails |
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
replaced. The rule trace records a failed call's kind, and for jq EMQX's message.

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

### D13. Off by default; compiled when switched on; never a crash

`rules.jq` is `false` by default (decided 2026-10-11).

- **Off.** A rules file that calls `jq` fails to load, as it does today, and the message
  names the switch: "jq is switched off: set `rules.jq = true` to use it (see RULES.md for
  what it needs from the host)". Nothing of the runtime is created: no engine, no thread,
  no handler, no executable page. An installation that does not use `jq` is exactly as
  hardened as it is today, and the shipped systemd unit keeps
  `MemoryDenyWriteExecute=true`.
- **On.** `jq` runs as Cranelift's machine code, which needs memory the process may map
  executable. The operator who switches `jq` on under the shipped unit therefore also sets
  `MemoryDenyWriteExecute=false`, in a drop-in
  (`systemctl edit mqttd` → `[Service]` / `MemoryDenyWriteExecute=false`); the unit file
  carries a comment saying so next to the line. Two settings, on purpose: the broker cannot
  lift a restriction its service manager imposed, and the operator should see that they
  are trading that restriction for `jq`.
- **On, on a host that still forbids executable memory.** The module cannot be loaded, and
  that is reported where the operator is looking: the rules file that calls `jq` **fails
  to load**, at boot, on reload, through the admin API and in `--check-rules`, with "jq is
  not available: the WebAssembly compiler cannot run on this host (it needs memory it may
  map executable): …", and the message names `MemoryDenyWriteExecute=false` as the way
  out. The running rule set stays as it was (ADR 0084). Rules that do not call `jq` are
  untouched. Nothing panics and no signal is raised: wasmtime returns the failed
  `mprotect` as an error, which CI checks under the shipped unit's hardening (§8).
- `mqttd --check-config` reports, when the switch is on, whether the host allows compiled
  code (`Execution::probe`).

The switch is per node, like the rest of the configuration. A cluster whose nodes disagree
has rules that load on some nodes and not on others; the rules-file load error says which
node refused and why, and RULES.md says to set it on all of them.

**The interpreter is not offered to operators.** The prototype can run the same module in
wasmtime's Pulley interpreter (`Execution::Interpreted`: no executable memory, 25× slower,
§8), and an earlier draft of this decision exposed it as `rules.jq_engine`. With the
switch, the operator who wants `jq` under `MemoryDenyWriteExecute=true` has a clear answer
(they cannot have both), and one mode is one thing to test and to explain. The code path
stays in the crate, where the tests use it to show the answers do not depend on the
compiler; exposing it later is additive. A plugin (D9) follows the same rule: it loads
only where `rules.jq`'s requirement is met, under its own `[functions.*]` declaration.

## Phasing

1. **T1 — the sandbox crate and the jq module's build.** Delivered on 2026-10-11: the
   prototype on wasmtime; the loader fed a thousand damaged modules and jq four hundred
   programs and inputs drawn at random (tests); the module built in CI from sources and a
   compiler pinned by hash and compared with the recorded hash; the oracle and limit tests
   in CI; the failure mode on a host without executable memory checked in CI; a
   coverage-guided fuzz target for the loader (`module_load`) in the nightly tier, fed
   module files and function bodies inside a module that is otherwise whole.
2. **T2 — `jq/2` and `jq/3` in the rule engine**, behind the ABI: the pool (D5), the
   worker hand-off (D6), the budget (D7), the grants (D8), metrics and trace (D10), the
   switch and its load errors (D13), the comment in the systemd unit, the module embedded by the release build,
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
- `jq` and the shipped systemd unit's `MemoryDenyWriteExecute=true` exclude each other.
  `jq` is off by default and the unit stays as it is; the operator who switches `jq` on
  gives up that one restriction, knowingly (D13).
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
listed), the module's place in a release (built in the pipeline), libm (documented), and the
shipped systemd unit (`jq` is off by default, the unit keeps `MemoryDenyWriteExecute=true`,
and the operator who switches `jq` on sets it to `false`; D13).

1. **The compiler in the trust base** (D11). Accepted with the runtime decision; recorded
   here because it is the part of that decision that cannot be measured.
2. **The workspace's Rust version** has to go from 1.88 to 1.96 when the broker links the
   crate, and will follow wasmtime's from then on (§8). *Done with T2, 2026-10-11.*
3. **The worker hand-off (D6)** is designed, not measured, and now rests on wasmtime's
   fibers. *Closed 2026-10-11: built and measured, with one correction (a Will); see
   [Amendments](#amendments).*
4. **`OptLevel::None`** would take a quarter off the half-second load at no measured cost
   (§6). Left at the default.
5. **Program cache size.** 32 per instance in the prototype against EMQX's 500 per thread;
   at 15 KiB per program 500 would cost about 7 MiB per instance.
6. **Operator plugins now or later** (T3), and whether ABI 1 should already carry a way to
   declare argument types so that arity and type errors are load errors. *Plugins built
   with T3, 2026-10-11 ([Amendments](#amendments)); arity is a load error, argument types
   are still not declared.*
8. The pinned source tarballs come from GitHub's archive endpoint, whose bytes are stable
   in practice but not by contract; a mirror of the tarballs as a release asset would
   remove that. (CI caches the built module, so an outage there fails only a run that has
   to rebuild.)

## Amendments

### 2026-10-11 — T2: what building `jq` into the rule engine settled, and the one place the design was wrong

T2 built D3 to D8, D10 and D13 as they are written above. This section records what was
decided where the text left a choice, what was measured where it said "to prove", and one
correction.

**D6 holds, with one correction: the hub does evaluate rules, for a Will.** D6 says "the
hub loop is never involved". ADR 0083 (decision 1) has one hub-side evaluation: the hub
publishes a Will and runs its rules itself, because Wills are rare and their rules were
microseconds. With `jq` a Will's rule may run for ten seconds, and everything waits for
the hub. The smallest change that keeps both records true: **while the loaded rules call
a sandboxed function, the hub hands a Will's rules to a task of its own** and goes on;
what they derive is posted back to the hub exactly as before (`HubCommand::RuleDerived`,
already asynchronous). Rules that do not call `jq` are evaluated where they always were.
A test publishes through the hub while a Will's jq runs for three seconds and requires
every round trip to stay under a second.

The hand-off itself is as designed, with the suspension spelled out: a call is suspended
**once**, when its first millisecond is used up (`Limits::first_slice`,
`UpdateDeadline::Yield`), and the rest of it is polled inside
`tokio::task::block_in_place`, on the same thread. One timer registration per call, as
before: of the end of the slice first, of the deadline after. Off a worker of the
multi-threaded runtime (a blocking thread, a command-line check, a single-threaded test
runtime) there is nothing to hand over and the call simply goes on. The sandbox crate
knows nothing of tokio: the broker gives the pool a closure.

Measured (Apple M1 Pro, debug build of the broker, **one runtime worker thread**,
`TOKIO_WORKER_THREADS=1`): while one connection's `jq('def f: f; f', payload)` ran into
the ten-second default timeout, a second connection completed 6,974 QoS 1 publishes, each
delivered to a subscriber: median round trip 152 µs, 99th percentile 339 µs, slowest
6.2 ms. With the hand-off taken out, the second connection's first publish was still
unanswered after two seconds, the test's bound: the only worker was inside jq. That test
is in the suite (`a_ten_second_jq_on_one_connection_does_not_delay_another_connections_publish`).

Two limits of the hand-off, recorded rather than solved:

- **Concurrent long calls are no longer bounded by the worker count.** Each connection
  whose call has passed its first millisecond runs on a thread of its own, up to the
  runtime's blocking-thread limit, and each may hold an instance of up to 64 MiB. Short
  calls, the usual case, never leave their worker. A cap on instances in a call (queueing
  or failing the rule past it) would be a new decision.
- **Message events for clients without a connection** (`$events/message/dropped` for an
  offline session, and the like) are evaluated by one task (ADR 0083). A long jq in such
  a rule delays the events queued behind it, not the hub and not any connection.

**D7: a call's output is charged whole.** The built-in functions charge what they build
*beyond their inputs*; `jq`'s output is charged in full, as D7's table says ("what is left
of the message's 1 MiB build budget"), because the module is told the bound before it
runs and cannot know what part of its output repeats its input. `jq('.', payload)` on a
600 KB payload therefore passes once per message and fails the second time. In the
departures table.

**D4: `jq/3`'s timeout, as probed on EMQX 6.3.1.** An integer of zero or more is the
timeout. Anything else fails the rule with EMQX's `bad_timeout_val` and its text
(`Needs positive integer or infinity but got …`, the value printed as Erlang's `~p`
prints it), checked before the program and the input are looked at. The atom `infinity`
cannot be written in rule SQL: the string `'infinity'` is a binary, and EMQX answers
`bad_timeout_val` for it; so does this engine. An integer of 2^64 or more makes
`timer:apply_after/4` fail and the NIF's match on it with it (`{badmatch,{error,badarg}}`);
below that, any timeout no clock can hold is none. An undefined input is the text
`undefined`, as `emqx_utils_json:encode/1` writes the atom. A program that is not a
binary has no clause in EMQX (`bad_sql_function_argument`); here it fails the rule too,
in this engine's words. A wrong number of arguments fails the load here and each message
there, as for every function.

**D5: what is compiled when.** The module is compiled by the first load, in the process,
of rules that call `jq`, on the thread doing that load, and the broker logs it once with
the time it took. The pool keeps as many warm instances as the runtime has workers.
The programs a rules file spells out are compiled into every warm instance at load; one
that does not compile is a load-time warning ("every call fails the rule, as in EMQX"),
as for a literal regular expression.

**D8, D13: the switch and its companions are read at startup.** `rules.jq`,
`rules.jq_default_timeout_ms` and `rules.jq_env` (`MQTTD_RULES_JQ`,
`MQTTD_RULES_JQ_DEFAULT_TIMEOUT_MS`, `MQTTD_RULES_JQ_ENV`) are not live: a reload that
changes them reports `rules.jq` as needing a restart, and the running broker keeps what
it started with. `jq_env` lists names; the values are the ones the broker's environment
had when it started. `mqttd --check-rules` and `mqttd --rule-test` read `[rules]` from the
effective configuration without validating the rest of it, so a file or a statement is
checked as the broker would load it on machines where no broker could start.
`mqttd --check-config` adds one line while the switch is on (this host allows compiled
code, or a warning that it does not and what to set) and still exits 0: the config is
valid either way and the check often runs where the broker will not. With `--preflight`
the rules file is loaded, and a file that calls `jq` on a host that refuses fails it.

**D10: the series.** `mqttd_rule_function_calls_total{function}`,
`mqttd_rule_function_errors_total{function,kind}` and
`mqttd_rule_function_seconds_total{function}`; per module
`mqttd_rule_module_instances{module}`, `mqttd_rule_module_memory_bytes{module}` and
`mqttd_rule_module_instances_replaced_total{module}`, as of the module's last call. The
kinds for `jq`: `compile`, `parse`, `process`, `timeout`, `system` (EMQX's tags);
`memory`, `output`, `stack`, `compile_timeout` (the sandbox's limits); `trap`; `argument`;
`decode`. A failed call is its rule's failure: the error text (`jq(): <tag>: <EMQX's
message>`, or `jq(): sandbox_limit (<kind>): …`) is what the log, the rule's last error
and the trace record carry.

**D4, D11: a build without the module.** The module is not in the tree, and a plain
`cargo build` must work without zig. So the sandbox crate's build script looks for the
module (`crates/mqtt-wasm-sandbox/jq-module/jq.wasm`, or the file `MQTTD_JQ_WASM` names):

- found: its sha256 is compared with `jq.wasm.sha256` (a mismatch fails the build), and
  it is embedded;
- not found: the build is whole without it. `rules.jq = true` then answers "this mqttd
  was built without the jq module", in the startup log, in `--check-config` and as the
  reason a rules file that calls `jq` does not load;
- `MQTTD_JQ_WASM_REQUIRED=1` makes "not found" a build error. The release recipe sets it,
  and so do CI's `test` and `wasm sandbox` jobs, so a release cannot ship, and the suite
  cannot pass, without the module by accident.

The cargo feature of D4 is `jq` (on by default, on `mqttd` and `mqtt-rules`): without it
the runtime is not linked at all. The tests of `jq` through the broker skip on a laptop
whose build has no module and fail under CI, where the module is built first.

`scripts/release/build-repro.sh` builds the module itself on every run, under `target/`
(the compiler fetched and checked by `fetch-zig.sh`), so the pipeline's clean second build
builds it a second time and the byte comparison of the two brokers covers it. The release
notes state the module's sha256 and the source commits.

**§8: what it cost.** The workspace declares Rust 1.96 (`mqttd-operator`, which links
none of this, still declares and is checked on 1.89). A release `mqttd` for
`aarch64-apple-darwin` grew from 19.9 MB to 28.4 MB (+8.5 MB; stripped:
16.5 MB to 23.5 MB), the module's 985 kB included. In dev and test builds
the Cranelift crates are optimised (`[profile.dev.package.*]`): unoptimised, the compiler
takes 6.5 s over the module where the optimised one takes 0.6 s, in every test process
that loads rules calling `jq`.

**§7: the two time formats stay.** `strptime` with `%z` and `strftime("%Z")` are recorded
in RULES.md's table with the libm digits, not closed: closing them means carrying a
time-zone parser and glibc's zone abbreviations in the module's shim, for two formats of
a function whose results otherwise match.

**Open questions 2 and 3 are closed** by the above. Left for T4: the threat-model and
hardening rows, the cookbook, and RULES.md beyond the section T2 needed.

### 2026-10-11 — T3: operator modules, and what D9 left to decide

T3 built D9 on the mechanism T2 put in the rule engine: `[functions.<name>]` in the rules
file, the load-time checks, the reload, the admin view, and two worked examples. This
section records what was decided where the text above left a choice.

**Declaring a module is the switch (D13).** D13's last sentence says a plugin "loads only
where `rules.jq`'s requirement is met, under its own `[functions.*]` declaration". Read
as written: the requirement is the host's (memory the process may map executable), not
the key. A rules file that declares a module loads it whether or not `rules.jq` is set,
and no key in the broker's configuration switches modules on or off: the default is that
none is declared, and then nothing of the runtime exists. On a host that forbids
executable memory the file fails to load with the compiler's reason and
`MemoryDenyWriteExecute=false` as the way out, as for `jq`. A build without the runtime
(the `jq` cargo feature off) refuses a declaration and says so.

**Who may point the broker at a file (D9, D11).** D9 says "relative to the rules file".
Rules text does not only come from the operator's disk: a rules writer of the admin API
(ADR 0084) replaces the file, and any operator may have rules text checked or dry-run.
A `[functions.*]` table in that text names a file and a hash, both chosen by the caller,
so the hash alone protects nothing against the caller. What does:

- **A module is read only from under the directory of the configured rules file**
  (`rules.file`; for `mqttd --check-rules <file>`, that file's directory): a relative path
  of plain names (no root, no `.`, no `..`) ending in `.wasm`, which must still lie under
  that directory once every symlink in it is resolved, and be a regular file of at most
  16 MiB. Text loaded with no rules file configured declares no module.
- **Every file there is the operator's.** The broker writes one kind of file into that
  directory, the rules file (and its `.prev` copy and a temporary file, ADR 0084), and
  writes it only after it has loaded as a rules file: it is TOML, which is never a
  WebAssembly module. So no caller of the API can put a module there.
- **The pin is a secret the caller must already hold.** The error for another hash names
  the hash that was asked for and never the file's own. Whoever declares a module has to
  know its content; the API does not tell them what lies in the directory beyond whether a
  name ending in `.wasm` exists.
- **The file is hashed on every load**, not only the first: a file replaced under an
  unchanged rules file fails the next reload, and the module already loaded keeps running.

What remains, stated: a rules writer, and an operator through a dry run, can load any
module the operator placed in that directory whose hash they know, under any of the names
it describes, with any limits up to the ceilings below. That is within the rules file's
trust (ADR 0083, 0084). The check-then-open of the path is not atomic: someone who can
write the directory can race it, and that someone is the operator. RULES.md says to keep
module files unwritable by the broker's user.

**The environment grant has a ceiling in the broker's configuration (D8).** D9's table has
`env = []` and D8 says "empty unless listed". Listed by whom was open, and the rules file
alone is the wrong answer for the reason above: a rules writer could list
`MQTTD_…` variables for a module that returns what it reads. So there is one new key,
`rules.functions_env` (`MQTTD_RULES_FUNCTIONS_ENV`), read at startup like `rules.jq_env`:
the variables any module may be shown. A table's `env` may name only those (another name
fails the load), and a listed variable that is not set is not given. `clock` needs no
such ceiling.

**The description (D2).** `mqttd_describe` answers
`{"module": …, "version": …, "functions": [{"name": …, "min_args": …, "max_args": …}]}`,
the shape the jq module already had; unknown keys are ignored. `functions` in the table is
required and not empty, so every name a file adds to rule SQL is written in the file.

**Names (D9).** A module's name and its functions' names are a lower-case letter followed
by up to 63 lower-case letters, digits or `_`. A function may not be called what a built-in
function is, nor what any function of EMQX's rule engine is: the exports of
`emqx_rule_funcs` in EMQX 6.3.1 (read from its `module_info(exports)`), the names it
resolves without exporting (the schema registry's and Sparkplug's, `maptab_lookup`), and
two names earlier releases exported (`byteszie`, `str_utf16_le_decode`); nor a word of rule
SQL; nor what another module of the file provides. A test holds the list against the
exports. The module name `jq` is the built-in module's.

**Limits (D7).** `timeout_ms` is 100 by default and at most 10000 (what `jq/2` gets);
`memory_mib` is 16 by default and at most 64 (the ceiling: a `jq` instance's); a file
declares at most 16 modules. Output and stack are D7's. A module's own non-zero status
fails the rule with its message and is counted as `module`; the sandbox's limits as
`timeout`, `memory`, `output`, `stack`; anything else that ends a call as `trap`. A module
keeps as many warm instances as the runtime has workers, like `jq`, and with
`fresh_instance = true` none.

**The reload (D9).** A rule set holds its modules, and an evaluation holds its rule set, so
"in-flight calls finish on the old one" needs no mechanism of its own: the old module is
dropped when the last rule set that declared it is. What the host adds is that a
declaration that did not change (same name, path, hash, functions, limits and grants)
keeps its loaded module and warm instances across a reload; a changed one is loaded anew
beside the old. Two tests hold a call in flight across a swap: one in the rule engine,
suspended in the hand-off of D6, and one through the binary and the admin API, where a
two-second call returns the old module's answer after the new module has answered a
later message.

**The cluster.** Nothing crosses the peer bus and its protocol is unchanged. Rules and
modules are per-node files; the rules digest covers the `[functions]` table, and a node
whose file has another hash does not load the rules.

**The admin view (D9).** `GET /admin/v1/rules` has `modules[]`: the table's name, the
module's own name and version, sha256, functions with their arities, `timeout_ms` and the
memory limit, the grants, and the instances (how many, how many in a call, their linear
memory, how many were replaced). A viewer's answer leaves out the path and the names of
the granted variables. `mqttd --admin rules` prints them as a second table.

**`--rule-test`.** A statement on its own may call the functions of the modules the
configured rules file declares; the file's modules are loaded for it.

**The examples and `unsafe`.** `examples/rule-plugins` has `geo` in Rust and `telemetry` in
C. The Rust one is a crate outside the broker's workspace, on purpose: a guest cannot
implement the ABI without `unsafe` (it is handed addresses in its own linear memory and
must read what lies there, and its exports need fixed names), and the workspace forbids
`unsafe` for the broker's code. That code runs inside the sandbox; a mistake in it is a
trap, and the rule fails. It is three marked blocks and the five exports, in one module of the example. CI
builds both examples (cached) and the broker's tests call them through the shipped
binary.

**Left open.** The cap on concurrent long calls that T2 recorded is still not decided,
and modules make it matter more: each connection whose call has passed its first
millisecond occupies a blocking thread and an instance of up to `memory_mib`. ABI 1 still
declares no argument types (open question 6). The threat-model and hardening chapters are
T4's; T3 adds the one row its path decision needs.

### 2026-10-11 — T4: what documenting it checked, and two sentences made exact

T4 wrote the documentation (RULES.md, the cookbook, THREAT-MODEL.md, HARDENING.md, the
deployment guides) and changed no behaviour. Three things belong in this record.

**§8's table, checked again with the broker itself.** The rows "the release image, run as
`deploy/compose` does" and "Kubernetes with the chart's `securityContext`" were measured
with the sandbox's examples. With a static `aarch64-unknown-linux-musl` build of `mqttd`
in the release image's base image: under Compose's settings `mqttd --check-config`
reports that the host allows compiled code and a `jq` statement evaluates; and on a kind
cluster (Kubernetes 1.36, containerd 2.3) the same holds for pods with the chart's pod
and container security context, with and without `seccompProfile: RuntimeDefault`. A
managed cluster was not tested. D13's instruction for the shipped unit (the drop-in that
sets `MemoryDenyWriteExecute=false`) is now checked where the refusal is: CI runs the
broker under the unit's hardening properties with that one set to `false` and requires
rules that call `jq`, and a rules file that declares a module, to load and run. That
static binary is 24.3 MB with the `jq` cargo feature and 16.9 MB without: 7.4 MB for
the runtime and the module on aarch64, where §8 estimated 6.0 MB and 985 kB from the
sandbox alone.

**The hand-off is per entry into the module, not per call of a function** (D6 and the T2
amendment say "a call is suspended once" and "one timer registration per call"). The host
enters a module several times to make one call of a rule function: to allocate the
argument frame, to call, and to free what it read. Each entry is armed on its own, with
its own first slice, and is suspended at most once. In practice only the call itself
outlasts a millisecond, so a long call is handed off once; an allocation is handed off
too only if its thread loses the processor for a millisecond in the middle of it, and
then the cost is one `block_in_place` around a few microseconds of work. The test of the
hand-off allows two for a call that fails in its second entry
(`a_call_that_outlasts_its_first_slice_goes_on_inside_the_hand_off`: with a first slice
of zero the timer's tick may already be there when the allocation starts, which CI's
runners showed and a laptop does not). That bound is the mechanism's, not a tolerance.

**The cap on concurrent long calls is still not decided.** T2 and T3 left it "to T3 or
T4"; T4 documents the behaviour as it is, as an accepted risk in THREAT-MODEL.md with
the operator's mitigations, and as H-7.14 in HARDENING.md. Deciding it (a bound on
instances in a call, with queueing or a failed rule past it) remains open.
*Decided later the same day, the other way: a ceiling on how long a call may run, not on
how many run; see the last amendment below.*

### 2026-10-11 — `rules.functions_env` confirmed

T3 added `rules.functions_env` (`MQTTD_RULES_FUNCTIONS_ENV`), the ceiling in the
broker's configuration on the environment variables any module may be shown, without
having been asked for it: D8 and D9 had left open who lists them (the T3 amendment
above). The user confirmed it on 2026-10-11. It stays as built: read at startup, a
module's `env` may name only variables it lists, and a rules file cannot widen it.

### 2026-10-11 — A ceiling on how long any one call may run (`rules.function_max_ms`)

T2, T3 and T4 each left one thing open: nothing bounded what long calls could hold. Two
things made it matter. A call that outlasts its first millisecond holds a thread and an
instance of up to its memory limit, and the number of such calls is bounded only by the
runtime's limit on blocking threads. And `jq/3`'s third argument is a value like any
other: `jq(payload.program, payload.data, payload.ms)` is legal rule SQL, as in EMQX, so
a publisher could name the timeout, and an integer below 2^64 was a timeout that never
came.

**Decided by the maintainer on 2026-10-11: a ceiling on time, per call, broker-wide.** No
call of `jq` or of a module's function runs longer than `rules.function_max_ms`
milliseconds, whatever the rule, `jq/3`'s third argument or a payload asks for. **Not a
limit on how many calls run at once**: that was the alternative (a bound on instances in a
call, with queueing or a failed rule past it), it was not chosen, and it is not built.
What a count limit would have bounded, the peak, is still bounded only as before; what
is new is that every one of those calls ends.

**The key.** `rules.function_max_ms` (`MQTTD_RULES_FUNCTION_MAX_MS`), default **10000**,
which is EMQX's default timeout for `jq/2` and was already the most a module's
`timeout_ms` could ask for; from 1 to 600000. Read at startup, like `rules.jq`,
`rules.jq_default_timeout_ms` and `rules.functions_env`: the instance pools are made with
it, and a reload reports a change as needing a restart. Set it on every node.

**What each call runs under.**

| Call | Before | Now |
|---|---|---|
| `jq/2` | `rules.jq_default_timeout_ms` | the same, which may not exceed the ceiling |
| `jq/3`, a timeout at or below the ceiling | its third argument | the same: nothing changes |
| `jq/3`, a timeout above the ceiling, or one that never comes (2^63 ms and more) | its third argument | **the ceiling**: the call runs, and is stopped there if it is still running |
| A module's function | the table's `timeout_ms`, at most 10000 | the table's `timeout_ms`, at most the ceiling |

**`jq/3` is clamped, not refused.** A call whose timeout is above the ceiling runs with
the ceiling as its limit. A program that finishes within the ceiling gives the result it
always gave, so a rule ported from EMQX with a generous literal timeout keeps working.
One that is cut at the ceiling fails its rule with the error any jq timeout fails with,
EMQX's tag and text, naming what the call ran under: `timeout: jq program canceled as it
took too long time to execute (timeout set to 10000 ms)`, where the rule may have said
`3600000`. It is counted as `kind="timeout"` like any other. What EMQX answers for a bad
third argument is untouched, because the argument is still judged first and as before:
`bad_timeout_val` for anything that is not an integer of zero or more, the
`{badmatch,{error,badarg}}` of 2^64 and above, both before the program is looked at. The
difference from EMQX is confined to calls that would have run longer than the ceiling,
and is in RULES.md's table of differences.

**A configured bound above the ceiling is an error, not a clamp.** Two bounds are written
down by someone who can be told: `rules.jq_default_timeout_ms` in the broker's
configuration and a module's `timeout_ms` in the rules file. Above the ceiling, the first
is refused by `Config::validate` (startup, `--check-config`, a reload's precheck) and the
second fails the load of the rules file (startup, a reload, an admin API write,
`--check-rules`), each naming both keys. This is how the codebase already treats a bound
a file sets against the broker's ceiling for it: a `[state.<name>]` table's `max_bytes`
above `rules.state_max_bytes` is a load error that names the key (ADR 0085), and a
module's `env` naming a variable outside `rules.functions_env` is one too. A clamp with
a warning would leave a file that says `timeout_ms = 60000` running under 10000, which
the next reader of the file cannot see. `jq/3`'s argument is the opposite case: it is a
value computed per message, possibly by a publisher, with nobody to tell and EMQX's
behaviour to keep for every call that ends in time; so it is clamped. Two details: the
default timeout is checked only where `rules.jq` is on (a broker that runs modules alone
lowers the ceiling without having to configure a function it does not have); and a
table that does not say `timeout_ms` gets the default of 100 ms or the ceiling,
whichever is lower, since a table that asks for nothing cannot be in conflict.

**The ceiling is per call of a rule function, not per entry into the module.** T4
recorded that one call is several entries (allocate the argument frame, call, free), each
armed with the timer on its own. The deadline they are armed with was already one
instant, computed once when the call starts; that is now where the ceiling is applied,
and it is extended to the two things a call could do outside it:

- **Compiling a jq program.** The timeout does not cover compiling, as in EMQX; compiling
  has its own five seconds (`COMPILE_TIMEOUT`). A program taken from a payload could
  therefore spend five seconds in the compiler on a broker whose ceiling is 100 ms. In a
  call, compiling is now stopped at the five seconds or at the call's ceiling, whichever
  comes first, and says which: `sandbox_limit (compile_timeout): jq stopped: compiling
  the program ran into the 100 ms one call may take`. When the rules are loaded
  (literal programs are compiled then, off the publish path) there is no call and the
  five seconds stand alone.
- **Making an instance.** A call that finds no warm instance makes one, and a module's
  start (data segments, a start function, constructors) had `init_timeout`, five
  seconds, whatever the call's own limit: with `fresh_instance = true`, on every call.
  Made for a call, an instance's start is now stopped at `init_timeout` or at the
  call's ceiling, whichever comes first. Loading a module with the rules is not a call
  and keeps `init_timeout`.

Neither deadline is lowered where it cannot defeat the ceiling: with the default of ten
seconds both are below it, and nothing changes.

The promise has a margin, which is the mechanism's and is stated rather than hidden. An
entry that starts when the deadline has already passed still gets one millisecond
(`min_run`: what lets a timeout of zero run what is quick, as in EMQX), and a module that
can be asked to stop gets two more to end by itself (`cancel_grace`). An entry that is
stopped ends the call, so only entries that each return in time can follow one another:
a module's call has four, a `jq` call that had to compile its program twelve, and an
instance made for the call two more. **A call therefore ends within some tens of
milliseconds of its ceiling: under 50 ms at the very most, a few in practice**, plus
what the operating system adds to a timer. The test for it gives a module an allocator that takes time, so that
the function starts with half the ceiling used up, and requires the call to end at the
ceiling and not an allocation later
(`one_ceiling_holds_a_slow_allocation_and_the_call_after_it_together`).

**The worst case, now.** At any moment long calls hold at most

    calls in flight × memory per call

and each of them for at most the ceiling. Calls in flight are bounded as before: one per
connection publishing to such a rule, and the runtime's limit on blocking threads
(tokio's default, 512). Memory per call is 64 MiB for `jq` and a module's `memory_mib`
(16 by default). With the defaults that is 512 × 64 MiB = 32 GiB at the worst moment,
unchanged by this decision; what changed is the third factor. Before, a publisher who
could choose `jq/3`'s timeout held a thread and an instance for as long as it liked with
one message. Now one message buys at most ten seconds, by default, and at
`function_max_ms = 100` a tenth of a second: to keep a thread and an instance occupied a
publisher has to send ten messages a second to that rule, for as long as it wants them
held, and everything is released within the ceiling of its stopping. The operator's
levers are the ceiling itself, `memory_mib`, the ACL on the topics such rules select, the
number of connections, and a memory limit on the process, which is still the only bound
on the product.

**What the operator sees.** `mqttd --check-config` prints the ceiling in force wherever
a sandboxed call can exist (`rules.jq` on, or a rules file configured, which may declare
modules): `rules.function_max_ms is 10000: no jq or module call runs longer, whatever a
rule or a payload asks for`. `GET /admin/v1/rules` has `function_max_ms` beside
`modules[]`, whose `limits.timeout_ms` are each at most that; it is the value the node
started with. The startup line for `rules.jq` carries it. **The metric does not tell a
clamp from a rule's own timeout**: both are `mqttd_rule_function_errors_total{kind="timeout"}`.
A separate kind would have been cheap to produce and not cheap to own (every alert and
dashboard written on `kind="timeout"` would silently stop counting the calls a ceiling
cut). The error's text does tell them apart, in the log, the rule's last error and the
trace: it names the milliseconds the call ran under, which is the ceiling's value and
not the rule's.

**Cost.** A sandboxed call computes one more instant when it starts and takes the
earlier of two; nothing else is on its path, and its time was not measured again. Rules
that call no sandboxed function do not reach any of it: `cargo bench -p mqtt-rules`,
four runs alternated with the build of the commit before (Apple M1 Pro, a machine busy
with other work), has the fastest run of each of its five cases within 5% either way
(`no_match` 90 ns against 95, `filter_reject` 799 against 836, `filter_republish` 1543
against 1514, `select_star_republish` 3696 against 3719, `foreach_10` 7113 against
7029), which is that machine's noise.

**What this closes and what it leaves.** Closed: the timeout a payload may name, which
was an accepted risk in THREAT-MODEL.md and is now a mitigated row with its test
(`a_publisher_cannot_make_a_jq_or_module_call_outlast_the_brokers_ceiling`). Left, on
purpose: the number of concurrent long calls, still bounded only by the blocking-thread
limit and the number of publishing connections. It stays an accepted risk, now bounded
in time per call. A limit on the count remains possible later and would sit beside the
ceiling, not replace it.

### 2026-10-12 — Compiling a jq program may take thirty seconds, not five

`COMPILE_TIMEOUT`, the bound on compiling one jq program outside a call (the literal
programs of a rules file, at load), goes from 5 s to 30 s. Five seconds is thousands of
times what a program needs on an idle host, and still a bound a loaded or emulated host
can run into; a load-time bound that a healthy host can miss fails rules for no fault of
theirs. Inside a call nothing changes: compiling ends at the call's ceiling
(`rules.function_max_ms`, 10 s unless set) first, as the amendment above has it. Rows that
say "5 s" in §4 and D7 above are of their date.

The test that showed the ceiling stopping the compiler asserted its timing against a
ceiling of one millisecond, and made its next call under the same millisecond, which a
one-character program does not always compile in. It failed on a busy CI runner. No
assertion in it depends on the machine's speed now.

### 2026-10-15 — A call is asked to stop by a flag the host sets, not by a call into the module

wasmtime 49.0.3 (RUSTSEC-2026-0353 to 0364, taken in #1029) refuses a blocking call into
a module from inside a call that is running on its own stack: "cannot do blocking `call`
from an async context" (RUSTSEC-2026-0362). Such a call can let an epoch interruption
suspend the outer call and hand data that must stay on one thread to another, and nests
stacks without bound. Every call here runs on its own stack (D6), and the deadline
callback asked a module to stop exactly that way: it called `mqttd_cancel`. With 49.0.3
that call was refused, the refusal ended the call, and a `jq` program that times out cost
its instance and its compiled programs instead of ending at its next step. The two tests
that pin that behaviour failed, which is how it was found.

The ABI's optional `mqttd_cancel()` is replaced by `mqttd_cancel_slot() -> ptr`, asked
once per instance after `_initialize`. Its answer is the address of a little-endian `u32`
in the module's memory, the slot, which holds the address of the module's stop flag, or 0.
At a call's deadline the host reads the slot and, when it holds an address, stores the
little-endian `i32` 1 there; none of the module's code runs for it. The module looks at
its flag between steps, as it would at a signal. The grace and its end are unchanged (2 ms,
then a trap and the instance dropped). A slot, or a flag address in it, that does not lie
wholly inside the module's memory ends the call at its deadline; a slot holding 0 leaves the
call its grace. No release has shipped `mqttd_cancel`, so nothing is kept for it.

In the `jq` module the flag is jq's own: `jq_state.canceled`, which `jq_cancel` sets and
the interpreter checks at every step. The shim stores the running program's flag in the
slot around `jq_next` and clears it after; `jq_reset_cancel_state` clears the flag before
each run, as before. jqc's `src/execute.c` is compiled as it is, through
`jq-module/execute_cancel.c`, which includes it and adds the one function that returns
that address; the pinned sources and their hashes do not change. The module does
(`jq.wasm.sha256`), and its build is still byte-reproducible: two clean builds give the
same file, and the previous revision's build reproduces the hash it recorded.

The §4 row on time, which says `mqttd_cancel`, is of its date. What it measured holds:
the tests that showed a timed-out program ending at its next step with its instance and
its compiled programs whole pass again unchanged, and the sandbox's own tests now show
the flag being set, a slot pointing outside memory ending the call at its deadline, and a
slot holding 0 keeping the grace.

### 2026-10-26 — A call past its deadline is ended there; the stop flag is no longer set

wasmtime 50 (#1124) hands the deadline callback a `StoreHookState` instead of a
`StoreContextMut`: the host's own data may be changed there, and nothing the module can
see (bytecodealliance/wasmtime#14386, "Scope the 'power' of epoch/call hooks", closing
#14048). The callback runs between two of the module's instructions, and "it is a semantic
specification violation for any guest-visible state in the store to get mutated between
wasm instructions"; upstream names such writes as a contributing factor in security
vulnerabilities, and the change fixed two soundness bugs found that way (a collection and
a thrown exception from inside a hook). The amendment of 2026-10-15 did exactly that: it
stored 1 at the module's stop flag from the callback.

No sanctioned way to keep it exists without changing the module. A raw pointer into the
memory, kept from before the call, would do through `unsafe` what the runtime now forbids
by type. The code that polls the call cannot write either: while the call is suspended its
store is lent to it, and the module is just as much between two instructions. Memory the
host may write at any moment is a shared memory, which needs the threads proposal, and
with it `memory.atomic.wait`, a blocking wait inside an asynchronous call — the concern of
RUSTSEC-2026-0362 again — and growth the store's memory cap does not see. The sanctioned
place to change the module's memory mid-call is an import the module calls; jq's
interpreter reads its flag at every step and calls no import, so that would mean changing
jqc's `execute.c` and the module's hash.

So at a call's deadline it is now ended by a trap and its instance dropped, as a module
that could not be asked always was. `Limits::cancel_grace` is gone, and with it the 2 ms
in the margin of the 2026-10-11 amendment. `mqttd_cancel_slot` stays a legal export, which
the host neither calls nor follows, so `jq.wasm` and its hash do not change; its
`execute_cancel.c` and the shim's slot are now dead and can go with the module's next
change. The time bound is unchanged or tighter: a call ends at its deadline, not up to
2 ms after. What a timed-out `jq` program costs is no longer the same: its instance and
the programs compiled in it (the next call makes an instance and compiles its program
again, about a millisecond together on the measurements in §4), counted in
`mqttd_rule_module_instances_replaced_total{module="jq"}`. The error a rule sees is
EMQX's timeout text, as before. The §4 rows on time, and the amendment of 2026-10-15, are
of their date.
