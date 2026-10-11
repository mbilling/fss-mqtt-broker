//! `geo`: a worked example of a rule-function module for mqttd, in Rust
//! (ADR 0086 D9; `docs/RULES.md`, "Your own functions").
//!
//! It provides two functions to rule SQL:
//!
//! * `geo_distance(lat1, lon1, lat2, lon2)`: the great-circle distance between two
//!   points, in metres (a float);
//! * `geo_within(lat, lon, lat0, lon0, radius_m)`: whether the first point lies within
//!   `radius_m` metres of the second (a boolean).
//!
//! The file has two parts. [`functions`] is the plugin: ordinary safe Rust over values.
//! [`abi`] is the function ABI, version 1: six exported functions and the argument
//! frame. A module of your own keeps `abi` as it is and replaces `functions`.
//!
//! **About `unsafe`.** mqttd's own code has none (its workspace forbids it). This crate
//! is not part of that workspace: it is guest code, compiled to WebAssembly and run
//! inside the broker's sandbox, where its memory is one linear memory that reaches
//! nothing of the host. A guest cannot implement the ABI without `unsafe`: the host
//! hands over addresses in that memory, and reading what lies at an address is `unsafe`
//! in Rust, as is exporting a function under a fixed name. Every such place is in
//! [`abi`], marked, and says why it holds. Nothing here can affect the broker's memory:
//! a mistake in it is a trap in the sandbox, and the rule fails.

#![deny(unsafe_op_in_unsafe_fn)]

/// What the module tells the broker about itself (`mqttd_describe`): its name, its
/// version, and each function with the fewest and the most arguments it takes. A rule
/// that calls one with another number of arguments does not load.
const DESCRIPTION: &str = concat!(
    r#"{"module":"geo","version":""#,
    env!("CARGO_PKG_VERSION"),
    r#"","functions":["#,
    r#"{"name":"geo_distance","min_args":4,"max_args":4},"#,
    r#"{"name":"geo_within","min_args":5,"max_args":5}]}"#
);

/// One argument, as the broker hands it over (ADR 0086 D3).
pub enum Arg<'a> {
    /// A string or a payload: its bytes as they are, UTF-8 or not.
    Binary(&'a [u8]),
    /// Any other value (a number, a boolean, `null`, an array, an object) as JSON text.
    Json(&'a str),
}

/// What a function returns.
pub enum Value {
    /// A string, as bytes.
    Binary(Vec<u8>),
    /// Any other value, as JSON text.
    Json(String),
}

/// The plugin itself.
mod functions {
    use super::{Arg, Value};

    /// The mean radius of the earth, in metres (IUGG).
    const EARTH_RADIUS_M: f64 = 6_371_008.8;

    /// Calls the function `name`. An `Err` fails the rule with that text.
    pub fn call(name: &str, args: &[Arg<'_>]) -> Result<Value, String> {
        match (name, args) {
            ("geo_distance", [lat1, lon1, lat2, lon2]) => {
                let metres = distance(point(lat1, lon1)?, point(lat2, lon2)?);
                // `{:?}` keeps a float a float (`12.0`, never `12`).
                Ok(Value::Json(format!("{metres:?}")))
            }
            ("geo_within", [lat, lon, lat0, lon0, radius]) => {
                let radius = number(radius, "the radius")?;
                if radius.is_nan() || radius < 0.0 {
                    return Err(format!("the radius must be zero or more, got {radius}"));
                }
                let metres = distance(point(lat, lon)?, point(lat0, lon0)?);
                Ok(Value::Json((metres <= radius).to_string()))
            }
            _ => Err(format!("no function {name}/{}", args.len())),
        }
    }

    /// A JSON number as a float.
    fn number(arg: &Arg<'_>, what: &str) -> Result<f64, String> {
        match arg {
            Arg::Json(text) => text
                .trim()
                .parse::<f64>()
                .ok()
                .filter(|n| n.is_finite())
                .ok_or_else(|| format!("{what} must be a number, got {text}")),
            Arg::Binary(_) => Err(format!("{what} must be a number, got a string")),
        }
    }

    /// A latitude and a longitude in degrees, as radians.
    fn point(lat: &Arg<'_>, lon: &Arg<'_>) -> Result<(f64, f64), String> {
        let (lat, lon) = (number(lat, "a latitude")?, number(lon, "a longitude")?);
        if !(-90.0..=90.0).contains(&lat) {
            return Err(format!("a latitude is from -90 to 90, got {lat}"));
        }
        if !(-180.0..=180.0).contains(&lon) {
            return Err(format!("a longitude is from -180 to 180, got {lon}"));
        }
        Ok((lat.to_radians(), lon.to_radians()))
    }

    /// The haversine distance between two points given in radians, in metres.
    fn distance((lat1, lon1): (f64, f64), (lat2, lon2): (f64, f64)) -> f64 {
        let a = ((lat2 - lat1) / 2.0).sin().powi(2)
            + lat1.cos() * lat2.cos() * ((lon2 - lon1) / 2.0).sin().powi(2);
        2.0 * EARTH_RADIUS_M * a.sqrt().min(1.0).asin()
    }
}

/// The function ABI, version 1: what every module exports.
///
/// ```text
/// mqttd_abi_version() -> u32            1
/// mqttd_alloc(len) -> address           memory the broker writes into
/// mqttd_free(address)
/// mqttd_describe(ret)                   the description above
/// mqttd_call(name, name_len, args, args_len, max_out, ret) -> status
/// ```
///
/// `ret` points at two `u32`: the address and the length of the result, which the
/// broker reads and then frees with `mqttd_free`. Status 0 is a value (one tag byte,
/// `b` or `j`, then the bytes); any other status fails the rule, with the bytes as the
/// message. `args` is a frame: a `u32` count, then per argument a tag byte, a `u32`
/// length and the bytes. All integers are little-endian.
mod abi {
    use super::{functions, Arg, Value, DESCRIPTION};

    /// Bytes in front of every block: its whole length, so `mqttd_free` knows it.
    const HEADER: usize = 8;

    /// The module's own failure statuses (any number but 0; they are the module's).
    const FAILED: i32 = 1;
    const TOO_LARGE: i32 = 2;
    const BAD_FRAME: i32 = 3;

    /// A new block holding `bytes`, owned by whoever frees its address.
    fn lend(bytes: &[u8]) -> *mut u8 {
        let total = HEADER + bytes.len();
        let mut block = Vec::with_capacity(total);
        block.extend_from_slice(&(total as u64).to_le_bytes());
        block.extend_from_slice(bytes);
        let block: *mut [u8] = Box::into_raw(block.into_boxed_slice());
        block.cast::<u8>().wrapping_add(HEADER)
    }

    /// Writes the address and length of a new block holding `bytes` at `ret`.
    fn answer(ret: *mut u32, bytes: &[u8]) {
        let at = lend(bytes);
        // SAFETY: `ret` is the address of eight bytes the broker allocated with
        // `mqttd_alloc` for exactly this pair. Linear memory has no alignment
        // requirement the broker promises, hence `write_unaligned`.
        unsafe {
            ret.write_unaligned(at as u32);
            ret.add(1).write_unaligned(bytes.len() as u32);
        }
    }

    #[no_mangle]
    pub extern "C" fn mqttd_abi_version() -> u32 {
        1
    }

    #[no_mangle]
    pub extern "C" fn mqttd_alloc(len: u32) -> *mut u8 {
        lend(&vec![0; len as usize])
    }

    #[no_mangle]
    pub extern "C" fn mqttd_free(at: *mut u8) {
        if at.is_null() {
            return;
        }
        let block = at.wrapping_sub(HEADER);
        // SAFETY: the broker frees only addresses this module gave it, each once;
        // every one is `HEADER` bytes into a boxed slice made by `lend`, whose first
        // eight bytes are its length.
        unsafe {
            let total = u64::from_le_bytes(block.cast::<[u8; HEADER]>().read()) as usize;
            drop(Box::from_raw(core::ptr::slice_from_raw_parts_mut(
                block, total,
            )));
        }
    }

    #[no_mangle]
    pub extern "C" fn mqttd_describe(ret: *mut u32) {
        answer(ret, DESCRIPTION.as_bytes());
    }

    #[no_mangle]
    pub extern "C" fn mqttd_call(
        name: *const u8,
        name_len: u32,
        args: *const u8,
        args_len: u32,
        max_out: u32,
        ret: *mut u32,
    ) -> i32 {
        // SAFETY: the broker wrote the name and the frame into a block it got from
        // `mqttd_alloc` and passes their addresses and lengths; it does not touch the
        // block again until this call returns.
        let (name, frame) = unsafe {
            (
                core::slice::from_raw_parts(name, name_len as usize),
                core::slice::from_raw_parts(args, args_len as usize),
            )
        };
        let (status, bytes) = match (core::str::from_utf8(name), arguments(frame)) {
            (Ok(name), Some(args)) => match functions::call(name, &args) {
                Ok(Value::Binary(bytes)) => (0, [b"b".as_slice(), &bytes].concat()),
                Ok(Value::Json(text)) => (0, [b"j".as_slice(), text.as_bytes()].concat()),
                Err(message) => (FAILED, message.into_bytes()),
            },
            _ => (BAD_FRAME, b"the arguments are not a frame".to_vec()),
        };
        // The broker says how much it will read; a larger result is refused there, so
        // it is refused here first, with a message.
        if status == 0 && bytes.len() > max_out as usize {
            answer(ret, b"the result is larger than the caller allows");
            return TOO_LARGE;
        }
        answer(ret, &bytes);
        status
    }

    /// The arguments in `frame`; `None` when it is not a frame.
    fn arguments(frame: &[u8]) -> Option<Vec<Arg<'_>>> {
        let u32_at = |at: usize| {
            let bytes = frame.get(at..at.checked_add(4)?)?;
            Some(u32::from_le_bytes(bytes.try_into().ok()?) as usize)
        };
        let count = u32_at(0)?;
        let mut at = 4;
        let mut args = Vec::new();
        for _ in 0..count {
            let tag = *frame.get(at)?;
            let len = u32_at(at + 1)?;
            let bytes = frame.get(at + 5..(at + 5).checked_add(len)?)?;
            args.push(match tag {
                b'b' => Arg::Binary(bytes),
                b'j' => Arg::Json(core::str::from_utf8(bytes).ok()?),
                _ => return None,
            });
            at += 5 + len;
        }
        Some(args)
    }
}
