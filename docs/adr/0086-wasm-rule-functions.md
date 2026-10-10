# ADR 0086 — Rule functions in a WebAssembly sandbox: the real jq first, operator plugins on the same mechanism

- **Status:** Proposed
- **Date:** 2026-10-10
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

A third way: compile the real jq to WebAssembly and run it in an interpreter written in
Rust. The C code is then data to the interpreter. It has one linear memory and no way to
name anything outside it, so memory safety of the broker no longer depends on jq, and the
results are jq's own.

If that works for jq, the same sandbox carries any function: an operator could supply a
function as a `.wasm` file, and it would run under the same limits. This record covers
both: what the spike measured for jq, and the design proposed for the general mechanism.

## The spike: what was built and what it measured

Everything here is reproducible from the tree: `crates/mqtt-wasm-sandbox/jq-module/build.sh`
builds the module, `cargo test --release -p mqtt-wasm-sandbox --features jq-module` runs
the comparison with EMQX and the limit tests, and the `measure` and `oracle` examples print
the numbers. Times are from an Apple M1 Pro, release build, single thread.

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

### 3. Runtime: wasmi

| | wasmi 2.0.0 | wasmtime 48.0.5 |
|---|---|---|
| Kind | interpreter, pure Rust | Cranelift JIT (and an interpreter, Pulley) |
| License | MIT OR Apache-2.0: passes `deny.toml` as it is | Apache-2.0 WITH LLVM-exception: not on the allow list |
| MSRV | 1.86 (workspace: 1.88) | 1.95 |
| Crates added to `Cargo.lock` | 10 (`wasmi`, `wasmi_core`, `wasmi_ir`, `wasmi_collections`, `wasmparser`, `spin`, `libm`, `foldhash`, `hashbrown` 0.15, `string-interner`) | 106 in a scratch build with `cranelift`, `runtime`, `pulley` |
| Binary size of a minimal runner | 2.1 MB | 8.3 MB |
| Machine code generated at run time | none | yes: executable pages, trap handling by signals |
| Module load | 11–12 ms (validation and translation of the whole module, once per process) | 650 ms to compile (can be done ahead of time) |
| `unsafe` | none in our code; 289 `unsafe` blocks, functions and impls in wasmi's 42,840 lines (41 more in its three sibling crates) | 1,592 in the `wasmtime` crate's 118,514 lines alone, plus the code it generates |
| jq speed against native | 15–26× slower | 2.0–3.7× slower (JIT, fuel on); Pulley 47–82× slower |

**Recommended: wasmi.** It is the only one of the two that keeps the property the idea is
for: the module's code is never executed by the processor, there is no executable memory
to get wrong, and the dependency and license footprint is small enough to audit. wasmtime's
JIT is about eight times faster and is the wrong trade for a broker whose build forbids `unsafe`:
it would bring a compiler into the hot path's trust base. The sandbox's soundness does rest
on wasmi's own `unsafe`; that is a smaller and better-reviewed surface than jq, oniguruma
and decNumber in C, and it is stated as such in the trust model (D11).

`cargo deny check` and `cargo audit` pass with the prototype in the workspace; no allow-list
entry was needed.

Two things found on the way, both in the prototype:

- wasmi's `StoreLimits` with `trap_on_grow_failure` also turns a `memory.grow` that merely
  has to wait for fuel into a trap. The prototype implements the limiter itself (40 lines).
- wasmi translates functions lazily by default and charges fuel for it; a call that starts
  short of that fuel fails instead of pausing. The prototype translates eagerly at load,
  which also makes a call's fuel the same every time.

### 4. Limits

| Limit | Mechanism | Measured |
|---|---|---|
| Time (`jq/3`'s timeout) | Fuel in slices of 200,000 (15–30 µs of work); the clock is read between slices. At the deadline the host calls the module's `mqttd_cancel` (which is `jq_cancel`: the program ends at its next step) and grants a grace of 2,000,000 fuel; a module that has not returned by then is stopped and its instance dropped | `def f: f; f` at 100 ms: stopped after 100.02 ms. `repeat(.)` at 100 ms: 100.02. `last(range(1e12))` at 150 ms: 150.02. `[repeat(.)]` at 200 ms: 200.52. At 10 ms: 10.01; at 1 ms: 1.01. The next call on the same caller: 0.02–0.04 ms, same instance, compiled programs intact |
| Time, inside one C call | The grace runs out and the instance is stopped. EMQX cannot stop such a program at all: its cancel flag is only looked at between jq's steps | `"a" * 8000000 \| test("a+b")` with a 30 ms timeout: EMQX's timeout error within the test's 2 s bound; the next call is served by a fresh instance (0.13–0.19 ms to make, 6.5 ms to compile the program again) |
| Fuel | Optionally a fixed amount per call, whatever the clock says. Used for compiling a jq program (20,000,000,000; one compilation burns about 73,000,000) | The same loop stops at the same fuel count on every run (test) |
| Memory | 64 MiB of linear memory per instance; the `memory.grow` that would pass it traps | `[range(1e9)]`: stopped after 955 ms. `"x" * 1e9`: 0.2 ms. `"a" * 8000000 \| explode`: stopped. Next call unaffected |
| Output | The module stops producing once the outputs pass the caller's bound (1 MiB in the prototype; D7 ties it to the message's budget), and the host refuses to read a larger result | `[range(300000)]`, `range(1e7)`, `"x" * 2e6`: refused; `"x" * 1000000`: passes |
| Stack | The module's calls are frames on the interpreter's heap-allocated stack (100,000 calls, 8 MiB), never on the host thread's; the C stack is 1 MiB at the bottom of linear memory, so overflowing it is an out-of-bounds trap | A module recursing for ever ends in a stack error on a host thread with a 256 KiB stack (test). `reduce range(300000) as $i (null; [.])` (jq frees it recursively): the call fails, the next one is served |
| Host access | A module importing anything outside the 16 allowed functions is refused at load. None of the 16 reaches the operating system: no files (every path and descriptor is refused), no sockets, no randomness; standard output and error are swallowed | `include "/etc/passwd"; .`: jq's "module not found", as in EMQX. `env.sock_send` and `env.system` imports: refused at load (test) |
| Clock and environment | Granted or not, per module (D8) | Without the grants `now` is 0 and `$ENV` is `{}`; with them `now` is the wall clock and `$ENV` holds exactly the listed entries (test) |

### 5. State and isolation

- **A module is loaded once** (11–12 ms) and shared; making an **instance** costs
  **130–190 µs** (it is a copy of 2 MiB of initial memory and libc's start-up). No
  pre-initialised snapshot is needed for that.
- The cost worth caching is **compiling a jq program: 6.5–7.6 ms** (0.6 ms natively), because
  jq parses its own 243-line builtin library for every program. The module keeps the 32
  most recently used compiled programs (EMQX's NIF keeps 500 per thread), at about
  15 KiB each: an instance holds 3.4 MiB with one program and 3.8 MiB with 32.
- A warm instance keeps compiled programs and nothing else. The test runs fourteen calls
  (`halt`, `first(range(10))`, `limit`, errors, `input`) on one instance and each on an
  instance of its own, and the answers are the same; the EMQX oracle includes the same
  pairs in sequence.
- An instance never returns memory. One that has grown past 16 MiB is dropped after its
  call (a 100 KB payload leaves it at 5.4–6.3 MiB).
- Instances are 3.4 MiB and more each, so they must not be per connection: D5.

### 6. Speed: 15 to 26 times slower than native jq

Median time of one call on a warm instance (program compiled), the same libjq sources
built natively with the same compiler for comparison:

| Filter | 1 KB payload: native | wasmi | | 100 KB payload: native | wasmi | |
|---|---:|---:|---:|---:|---:|---:|
| `.` | 25 µs | 446 µs | 18× | 2.5 ms | 42.3 ms | 17× |
| `.a.b` | 14 µs | 247 µs | 18× | 1.5 ms | 22.0 ms | 15× |
| `.readings \| map(select(.temp > 50)) \| length` | 19 µs | 379 µs | 20× | 2.0 ms | 34.3 ms | 17× |
| `.readings \| group_by(.id) \| map({id: .[0].id, n: length})` | 30 µs | 639 µs | 21× | 3.3 ms | 76.4 ms | 23× |
| `.readings \| map(select(.id \| test("^dev-[0-4]$"))) \| length` | 46 µs | 1,185 µs | 26× | 5.0 ms | 129.1 ms | 26× |
| `[.readings[] \| select(.ok) \| .temp] \| add / length` | 20 µs | 404 µs | 20× | 2.1 ms | 37.0 ms | 18× |

Where one call's time goes:

| Part | Cost | When |
|---|---|---|
| Load the module | 11–12 ms | once per process |
| Instantiate | 0.13–0.19 ms | once per pooled instance, and after a stopped call |
| Compile the program | 6.5–7.6 ms | once per program per instance |
| Run (parse the input, run, print) | the table above; at 1 KB most of it is jq parsing the input (`.a.b`: 247 µs) | every call |
| Host side: write the input value as JSON, when it is not already a binary | 7 µs at 1 KB, 760 µs at 100 KB | every call with a non-binary input |
| Host side: read the outputs with the rule engine's reader | 0.1–7 µs for these outputs; 770 µs to read back 100 KB | every call |

In broker terms: a rule that calls `jq` on a 1 KB payload costs its publisher's connection
task a quarter to one millisecond per message, so one core evaluates one to four thousand
such messages a second. Rule SQL's own functions do the same selections in microseconds.
`jq` in the sandbox is for what only jq expresses, and for rules arriving from EMQX
unchanged; it is not a fast path, and D5 and D6 are written so that it cannot starve others.

Under wasmtime's JIT the same module runs at 2.0–3.7× native (61 µs, 30 µs, 44 µs, 84 µs,
113 µs, 48 µs at 1 KB). That is the price of the interpreter, stated once.

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

## Decision

### D1. Functions may be provided as WebAssembly modules, run by an interpreter

A new crate, `mqtt-wasm-sandbox`, loads modules and runs calls into them with wasmi. It has
no `unsafe` and does no I/O. `mqtt-rules` depends on it and nothing else changes shape: a
sandboxed function is an entry in the function table like any other, taking and returning
`Value`s. The rule engine still holds no broker state.

The interpreter is wasmi with `validate`, without `wat` and `memory64`, translating eagerly.
A module's code is never run by the processor.

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

The jq module is embedded in the broker binary (985 kB; with wasmi an estimated 2.5 MB
added to a 16.8 MB image) and provides `jq/2` and `jq/3` with EMQX's semantics:

- the program must be a binary; the input is handed over as D3 says; the result is the list
  of all outputs; outputs before an error are discarded; program and input end at the first
  NUL;
- errors are EMQX's tags and texts and fail the rule;
- `jq/2` runs under `rules.jq_default_timeout_ms`, default 10000 as EMQX's
  `rule_engine.jq_function_default_timeout`; `jq/3`'s third argument is validated as EMQX
  validates it (a non-negative integer or `infinity`, else `bad_timeout_val`);
- compiling a program is outside the timeout, as it is in EMQX (the cancel flag is only
  looked at while a program runs), and bounded by fuel instead.

A cargo feature leaves the module and the interpreter out of a build that does not want
them; `jq` is then refused at load as today.

### D5. Instances are pooled per node, never per connection

An instance is 3.4 MiB and more. The broker keeps a pool of instances per module, as many
as it has worker threads; an evaluation takes one for the duration of one call and puts it
back. Instances are made on first use and when one was stopped. When rules are loaded, the
literal jq programs in them are compiled into every pooled instance off the publish path,
so the 6.5 ms is not paid by a publisher. A program that comes from a payload
(`jq(payload.prog, payload)`, which EMQX allows) is compiled on its first use per instance,
under the fuel bound, and takes a cache slot like any other.

### D6. A long call must not hold a worker

Rule SQL is evaluated synchronously on the publisher's connection task (ADR 0083). A jq
call may legitimately take milliseconds and is allowed ten seconds by EMQX's default. The
call is resumable between fuel slices, which gives the host a choice native code would not:
a call runs inline for a first budget (proposed: 1 ms), and if it has not returned it is
continued under `tokio::task::block_in_place`, so the worker's other connections are moved
to other workers. The hub loop is never involved. This is a proposal the prototype does not
exercise; it is task T2's to prove, with a test that a ten-second jq on one connection does
not delay another connection's publish.

### D7. Limits per call, charged to the message's budget

| Limit | Value | On reaching it |
|---|---|---|
| Time | the function's timeout (`jq/3`), or the module's declared default | EMQX's `timeout` error for jq; a `timeout` error for a plugin. The rule fails |
| Compile fuel (jq) | 20,000,000,000 per program | the rule fails |
| Memory | 64 MiB per instance (plugins: declared, at most the broker's ceiling) | the rule fails; the instance is dropped |
| Output | what is left of the message's 1 MiB build budget (`MAX_BUILT_BYTES`), charged when the call returns | the rule fails |
| Stack | 100,000 calls, 8 MiB of interpreter stack, 1 MiB of C stack | the rule fails; the instance is dropped |

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
| Environment | **proposed: empty unless listed** (`rules.jq_env = ["SITE", …]`) | empty unless listed |
| Randomness | none (jq has no use for it) | none in ABI 1 |
| Files, network | never | never |

The environment is the one place this record proposes to leave EMQX knowingly. In EMQX,
`$ENV` and `env` read the broker's whole environment. A rule may take its program from a
payload, and what jq returns can be republished, so that is a way for a publisher to read
`MQTTD_*` secrets passed by environment. Exposing nothing by default and letting the
operator list variables costs the departures table one row. The alternative, EMQX's
behaviour behind `rules.jq_env = "all"`, is available if exactness matters more here.

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
fresh_instance = false          # true: no state survives a call, at 0.13–0.19 ms each
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
`mqttd_rule_eval_seconds_total`), fuel; per module: instances, instance memory, instances
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
  costs an instantiation (0.13–0.19 ms) per call.
- These hold as long as wasmi is sound. wasmi contains `unsafe` (D1's table); a memory-safety
  bug in it could be reachable from a crafted module. Operator plugins therefore remain
  "code the operator vouches for", and the built-in jq module is built from pinned sources.
- **Supply chain of the jq module.** Built from source tarballs pinned by sha256, by a
  compiler pinned by version (and by its tarball's sha256 in CI), with a byte-reproducible
  result whose hash is in the tree. Proposed: CI builds it and fails on a different hash;
  the release build embeds the file it built, so the existing build-twice gate
  (`release.yml`) covers it; the release notes carry the module's hash and the three source
  commits. The alternative, committing the 985 kB file with a reproduce script, removes
  zig from the release pipeline at the cost of a binary in the repository that reviewers
  cannot read.

### D12. An mqttd extension

EMQX has no WebAssembly plugins (it has `jq` as a NIF, and Erlang plugins and external
functions). `[functions.*]` is mqttd's own and is listed under "Differences from EMQX";
a rules file that uses it does not port back. `jq` itself is EMQX's function with EMQX's
results, within the limits of D7 and the differences of spike §7.

## Phasing

1. **T1 — the sandbox crate and the jq module's build.** The prototype hardened: fuzzing of
   the loader with arbitrary modules, the module built in CI from pinned sources and compared
   with the recorded hash, the oracle test in CI.
2. **T2 — `jq/2` and `jq/3` in the rule engine**, behind the ABI: the pool (D5), the
   worker hand-off (D6), the budget (D7), the grants (D8), metrics and trace (D10),
   `--rule-test` support, and the remaining differences closed where they can be
   (`strptime %z`, `%Z`).
3. **T3 — operator plugins** (D9), with a worked example in Rust and one in C.
4. **T4 — documentation**: RULES.md (the function, its limits, its cost), the departures
   table, THREAT-MODEL.md and HARDENING.md rows, the cookbook.

T3 does not have to follow T2 immediately. The ABI is the same either way, and T2 alone
delivers `jq`.

## Consequences

- `jq` works, with jq's own results, and a jq fault is an evaluation error instead of a
  broker fault.
- A `jq` call costs 15–26 times what native jq costs (which is what EMQX runs): a quarter
  to one millisecond per 1 KB message. Documented where the function is.
- The broker binary grows by about 2.5 MB, the lockfile by 10 crates, and the release
  pipeline by one pinned compiler (zig), unless the module is committed.
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
- **wasmtime.** About eight times faster. Rejected for the trust base (§3).
- **Keep refusing `jq`.** The status quo, and what a build without the feature still does.
  It leaves every EMQX rule that uses `jq` unportable.

## Open questions

1. **Is the speed acceptable?** This is the decision the measurements are for. If a quarter
   millisecond per 1 KB message is too slow for the intended use, the alternatives are the
   subprocess (similar cost per call, different shape) or leaving `jq` refused.
2. **`$ENV`: empty by default (proposed) or EMQX's whole environment?** (D8)
3. **The module in the release: built in the pipeline (proposed) or committed?** (D11) The
   pinned tarballs come from GitHub's archive endpoint, whose bytes are stable in practice
   but not by contract; a mirror of the three tarballs as a release asset would remove that.
4. **libm.** Matching glibc's last digit would mean compiling glibc's libm into the module
   (LGPL, and EMQX's own results vary with its build host's glibc). Proposed: document, do
   not chase.
5. **The worker hand-off (D6)** is designed, not measured.
6. **Program cache size.** 32 per instance in the prototype against EMQX's 500 per thread;
   at 15 KiB per program 500 would cost about 7 MiB per instance.
7. **Operator plugins now or later** (T3), and whether ABI 1 should already carry a way to
   declare argument types so that arity and type errors are load errors.
