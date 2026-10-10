//! The host side of the module's imports: the sixteen `wasi_snapshot_preview1`
//! functions a wasi-libc reactor links against, and nothing else.
//!
//! None of them reaches the operating system. There is no file system (every path
//! and every descriptor above 2 is refused), no network, no randomness. Standard
//! output and error are swallowed. Two things can be granted ([`Grants`]): the wall
//! clock, and a list of environment entries. A module importing anything outside
//! [`ALLOWED`] does not load.

use wasmi::{Caller, Error, Extern, Linker, Memory};

use crate::sandbox::Host;

/// The import module every allowed function lives in.
pub(crate) const MODULE: &str = "wasi_snapshot_preview1";

/// The functions a module may import. Each is defined in [`define`].
pub(crate) const ALLOWED: [&str; 16] = [
    "clock_time_get",
    "environ_get",
    "environ_sizes_get",
    "fd_close",
    "fd_fdstat_get",
    "fd_fdstat_set_flags",
    "fd_filestat_get",
    "fd_prestat_get",
    "fd_prestat_dir_name",
    "fd_read",
    "fd_seek",
    "fd_write",
    "path_filestat_get",
    "path_open",
    "path_readlink",
    "proc_exit",
];

/// What a module may see of the world outside its memory.
#[derive(Debug, Clone, Default)]
pub struct Grants {
    /// The wall clock. Without it every clock reads zero, and a module's result
    /// depends on its arguments alone.
    pub clock: bool,
    /// The environment the module sees, as `NAME=value` entries. Empty unless given.
    pub environment: Vec<Vec<u8>>,
}

const OK: i32 = 0;
const EBADF: i32 = 8;
const EFAULT: i32 = 21;
const EINVAL: i32 = 28;
const ENOENT: i32 = 44;
const ESPIPE: i32 = 70;

fn memory(caller: &Caller<'_, Host>) -> Option<Memory> {
    caller.get_export("memory").and_then(Extern::into_memory)
}

/// Writes `bytes` at `at` in the module's memory; an address outside it is `EFAULT`.
fn put(caller: &mut Caller<'_, Host>, at: i32, bytes: &[u8]) -> i32 {
    let Some(mem) = memory(caller) else {
        return EFAULT;
    };
    match mem.write(caller, at.cast_unsigned() as usize, bytes) {
        Ok(()) => OK,
        Err(_) => EFAULT,
    }
}

fn get_u32(caller: &Caller<'_, Host>, at: u32) -> Option<u32> {
    let mut b = [0u8; 4];
    memory(caller)?.read(caller, at as usize, &mut b).ok()?;
    Some(u32::from_le_bytes(b))
}

fn std_stream(fd: i32) -> bool {
    (0..=2).contains(&fd)
}

/// Defines every function of [`ALLOWED`] on `linker`.
#[allow(clippy::too_many_lines)] // one short closure per import
pub(crate) fn define(linker: &mut Linker<Host>) -> Result<(), Error> {
    linker.func_wrap(
        MODULE,
        "clock_time_get",
        |mut caller: Caller<'_, Host>, _id: i32, _precision: i64, out: i32| -> i32 {
            let nanos = if caller.data().grants.clock {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
            } else {
                0
            };
            put(&mut caller, out, &nanos.to_le_bytes())
        },
    )?;
    linker.func_wrap(
        MODULE,
        "environ_sizes_get",
        |mut caller: Caller<'_, Host>, count: i32, size: i32| -> i32 {
            let env = &caller.data().grants.environment;
            let n = u32::try_from(env.len()).unwrap_or(u32::MAX);
            let bytes =
                u32::try_from(env.iter().map(|e| e.len() + 1).sum::<usize>()).unwrap_or(u32::MAX);
            match put(&mut caller, count, &n.to_le_bytes()) {
                OK => put(&mut caller, size, &bytes.to_le_bytes()),
                e => e,
            }
        },
    )?;
    linker.func_wrap(
        MODULE,
        "environ_get",
        |mut caller: Caller<'_, Host>, pointers: i32, buf: i32| -> i32 {
            let env = caller.data().grants.environment.clone();
            let mut pointer = pointers.cast_unsigned();
            let mut at = buf.cast_unsigned();
            for entry in env {
                let mut text = entry;
                text.push(0);
                let Ok(len) = u32::try_from(text.len()) else {
                    return EINVAL;
                };
                if put(&mut caller, pointer.cast_signed(), &at.to_le_bytes()) != OK
                    || put(&mut caller, at.cast_signed(), &text) != OK
                {
                    return EFAULT;
                }
                pointer = pointer.wrapping_add(4);
                at = at.wrapping_add(len);
            }
            OK
        },
    )?;
    linker.func_wrap(MODULE, "fd_close", |fd: i32| -> i32 {
        if std_stream(fd) {
            OK
        } else {
            EBADF
        }
    })?;
    linker.func_wrap(
        MODULE,
        "fd_fdstat_get",
        |mut caller: Caller<'_, Host>, fd: i32, out: i32| -> i32 {
            if !std_stream(fd) {
                return EBADF;
            }
            // A character device with no rights: libc buffers it by line.
            let mut stat = [0u8; 24];
            stat[0] = 2;
            put(&mut caller, out, &stat)
        },
    )?;
    linker.func_wrap(
        MODULE,
        "fd_fdstat_set_flags",
        |_fd: i32, _flags: i32| -> i32 { EBADF },
    )?;
    linker.func_wrap(MODULE, "fd_filestat_get", |_fd: i32, _out: i32| -> i32 {
        EBADF
    })?;
    // No preopened directories: the module's libc finds no file system at all.
    linker.func_wrap(MODULE, "fd_prestat_get", |_fd: i32, _out: i32| -> i32 {
        EBADF
    })?;
    linker.func_wrap(
        MODULE,
        "fd_prestat_dir_name",
        |_fd: i32, _path: i32, _len: i32| -> i32 { EBADF },
    )?;
    linker.func_wrap(
        MODULE,
        "fd_read",
        |mut caller: Caller<'_, Host>, fd: i32, _iovs: i32, _n: i32, nread: i32| -> i32 {
            if fd != 0 {
                return EBADF;
            }
            // Standard input is empty.
            put(&mut caller, nread, &0u32.to_le_bytes())
        },
    )?;
    linker.func_wrap(
        MODULE,
        "fd_seek",
        |fd: i32, _offset: i64, _whence: i32, _out: i32| -> i32 {
            if std_stream(fd) {
                ESPIPE
            } else {
                EBADF
            }
        },
    )?;
    linker.func_wrap(
        MODULE,
        "fd_write",
        |mut caller: Caller<'_, Host>, fd: i32, iovs: i32, n: i32, nwritten: i32| -> i32 {
            if fd != 1 && fd != 2 {
                return EBADF;
            }
            // Swallowed; the lengths are added up so the writer sees it all taken.
            let mut total = 0u32;
            let mut at = iovs.cast_unsigned();
            for _ in 0..n.cast_unsigned().min(1024) {
                let Some(len) = get_u32(&caller, at.wrapping_add(4)) else {
                    return EFAULT;
                };
                total = total.saturating_add(len);
                at = at.wrapping_add(8);
            }
            let host = caller.data_mut();
            host.discarded_output = host.discarded_output.saturating_add(u64::from(total));
            put(&mut caller, nwritten, &total.to_le_bytes())
        },
    )?;
    linker.func_wrap(
        MODULE,
        "path_filestat_get",
        |_fd: i32, _flags: i32, _path: i32, _len: i32, _out: i32| -> i32 { ENOENT },
    )?;
    linker.func_wrap(
        MODULE,
        "path_open",
        |_fd: i32,
         _dirflags: i32,
         _path: i32,
         _len: i32,
         _oflags: i32,
         _rights: i64,
         _inheriting: i64,
         _fdflags: i32,
         _out: i32|
         -> i32 { ENOENT },
    )?;
    linker.func_wrap(
        MODULE,
        "path_readlink",
        |_fd: i32, _path: i32, _len: i32, _buf: i32, _buf_len: i32, _used: i32| -> i32 { ENOENT },
    )?;
    // `exit` and `abort` end the call and the instance, never the broker.
    linker.func_wrap(MODULE, "proc_exit", |code: i32| -> Result<(), Error> {
        Err(Error::i32_exit(code))
    })?;
    Ok(())
}
