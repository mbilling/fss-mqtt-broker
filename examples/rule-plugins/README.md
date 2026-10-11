# Rule-function modules: two worked examples

A rules file can add functions of its own to mqttd's rule SQL, supplied as WebAssembly
modules and run in the broker's sandbox
([RULES.md](../../docs/RULES.md#your-own-functions-functionsname),
[ADR 0086](../../docs/adr/0086-wasm-rule-functions.md) D9). These are two complete
modules to copy from, one in Rust and one in C.

| | Functions | Shows |
|---|---|---|
| [`rust/`](rust/src/lib.rs) → `geo.wasm` (target `wasm32-unknown-unknown`, no dependencies) | `geo_distance(lat1, lon1, lat2, lon2)`: metres between two points. `geo_within(lat, lon, lat0, lon0, radius_m)`: a boolean | Numbers in and out as JSON; a function's own refusal (a latitude of 91) failing the rule with its message |
| [`c/`](c/telemetry.c) → `telemetry.wasm` (target `wasm32-wasi`, wasi-libc) | `crc16_modbus(Payload)`: an integer. `site_label(Text)`: `<SITE>/<Text>`. `clock_ms()`: milliseconds since the epoch | A payload's bytes as they are; the two grants a module can be given, an environment variable and the wall clock, and what it reads without them (`/<Text>` and `0`) |

## Build

```sh
rustup target add wasm32-unknown-unknown      # once, for the repository's pinned toolchain
examples/rule-plugins/build.sh                # out/geo.wasm, out/telemetry.wasm, each with its sha256
```

`build.sh` needs zig 0.16.0, the compiler the broker's own jq module is built with (it
is the whole C toolchain: its clang and its bundled wasi-libc).
`crates/mqtt-wasm-sandbox/jq-module/fetch-zig.sh DIR` downloads it and checks the
archive's sha256; `ZIG=DIR/zig/zig examples/rule-plugins/build.sh` uses it. The script
holds the compiler flags for both, the WebAssembly features a module may use among
them: build a module of your own the way it does.

## Use

Put the files under the directory of your rules file, and declare them there with the
hashes `build.sh` printed:

```toml
[functions.geo]
wasm = "plugins/geo.wasm"
sha256 = "<the sha256 build.sh printed>"
functions = ["geo_distance", "geo_within"]

[functions.telemetry]
wasm = "plugins/telemetry.wasm"
sha256 = "<the sha256 build.sh printed>"
functions = ["crc16_modbus", "site_label", "clock_ms"]
clock = true
env = ["SITE"]            # and `functions_env = ["SITE"]` under [rules] in the broker's configuration

[rules.near_the_depot]
sql = '''
SELECT clientid, geo_distance(payload.lat, payload.lon, 55.6761, 12.5683) AS metres
FROM "trackers/+/position"
WHERE geo_within(payload.lat, payload.lon, 55.6761, 12.5683, 5000)
'''
actions = [{ function = "republish", args = { topic = "depot/near/${clientid}" } }]
```

`mqttd --check-rules <the rules file>` loads the modules and says what it loaded, or why
it did not.

## About `unsafe` in the Rust example

mqttd's own code has no `unsafe`: its workspace forbids it. **The Rust example does, and
is not part of that workspace** (it has its own `Cargo.toml` with an empty `[workspace]`,
and the repository's `Cargo.toml` excludes it). It is guest code: compiled to
WebAssembly, it runs inside the broker's sandbox, where its memory is one linear memory
that reaches nothing of the host.

A guest cannot implement the function ABI in Rust without it. The broker hands the module
addresses in the module's own memory, and reading what lies at an address is `unsafe` in
Rust; exporting a function under a fixed name (`#[no_mangle]`) counts as `unsafe` too.
The example keeps that to one module, `abi`, of `src/lib.rs`: three `unsafe` blocks, each
with the reason it holds, and the five exported functions. The plugin itself, `functions`,
is safe Rust over values. A mistake in the `unsafe` part cannot reach the broker: it is a
trap inside the sandbox, the rule fails for that message, and the instance is replaced.

The C example has no such line to draw; it is C, and it runs under the same sandbox.

## Tested

CI builds both modules (`.github/actions/rule-plugins`) and the broker's test suite
declares them in a rules file and calls them through the shipped binary
(`the_worked_example_modules_run_in_the_broker_with_their_grants` in
`crates/mqttd/tests/rules_binary.rs`). Locally that test is skipped until `out/` holds the
two files.
