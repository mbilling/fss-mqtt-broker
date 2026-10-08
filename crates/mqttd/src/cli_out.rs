//! Standard output for the command-line modes (`--help`, `--version`, `--check-rules`,
//! `--admin …` and the rest), whose output may go to a pipe that closes early:
//! `mqttd --check-rules rules.toml | head -1`.
//!
//! `print!` panics when the reader is gone, and the process exits 101. [`out!`] and
//! [`outln!`] do not: a closed pipe silently ends the output and nothing else. The command
//! finishes and exits with the status it would have had. A failed `--check-tls` still exits
//! 1, and `--decommission` still waits for the drain. SIGPIPE stays ignored, as a server's
//! must. The server writes its log to stdout through `tracing`, not through these.
//!
//! Any other write error (a full disk under `--print-config > file`) is reported on stderr
//! and ends the process with status 1, because the output was what the command was for.

use std::fmt;
use std::io::{self, Write as _};

/// Writes `args` to stdout, as [`out!`] does.
pub fn write(args: fmt::Arguments<'_>) {
    if let Err(e) = io::stdout().write_fmt(args) {
        if e.kind() != io::ErrorKind::BrokenPipe {
            eprintln!("error: cannot write to stdout: {e}");
            std::process::exit(1);
        }
    }
}

/// `print!` for the command-line modes: a closed stdout ends the output, not the process.
/// See [`cli_out`](crate::cli_out).
#[macro_export]
macro_rules! out {
    ($($arg:tt)*) => {
        $crate::cli_out::write(::std::format_args!($($arg)*))
    };
}

/// [`out!`] with a newline, for `println!`.
#[macro_export]
macro_rules! outln {
    () => {
        $crate::cli_out::write(::std::format_args!("\n"))
    };
    ($($arg:tt)*) => {{
        $crate::cli_out::write(::std::format_args!($($arg)*));
        $crate::cli_out::write(::std::format_args!("\n"));
    }};
}
