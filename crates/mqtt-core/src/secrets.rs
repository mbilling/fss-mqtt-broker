//! Reading secret material mounted as files (ADR 0046 T5, secrets by reference).
//!
//! Every file-mounted secret in the workspace — HS256 shared secrets, the SWIM
//! gossip key, bridge passwords — goes through [`read_secret_file`] so they all
//! share ONE trimming rule: **ASCII whitespace is stripped from both ends**.
//! Trailing newlines are the ubiquitous `echo secret > file` / editor artifact;
//! leading/trailing spaces are copy-paste artifacts. Interior whitespace is
//! preserved (a password may legitimately contain spaces).
//!
//! Before this helper existed each site had its own rule (trailing-`\n` only,
//! `str::trim`, …), so the same secret file could authenticate through one
//! component and fail through another.

use std::io;
use std::path::Path;

/// Strip ASCII whitespace (space, `\t`, `\n`, `\r`, …) from both ends of raw
/// secret bytes. Interior bytes — including interior whitespace — are untouched.
#[must_use]
pub fn trim_secret(bytes: &[u8]) -> &[u8] {
    let start = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(start, |i| i + 1);
    &bytes[start..end]
}

/// Read a file-mounted secret and trim ASCII whitespace from both ends, so the
/// on-disk file and the literal value an operator typed agree regardless of how
/// the file was produced.
///
/// # Errors
/// Propagates the underlying [`std::fs::read`] error.
pub fn read_secret_file(path: impl AsRef<Path>) -> io::Result<Vec<u8>> {
    let raw = std::fs::read(path)?;
    Ok(trim_secret(&raw).to_vec())
}

#[cfg(test)]
mod tests {
    use super::trim_secret;

    #[test]
    fn trims_trailing_newline_variants() {
        assert_eq!(trim_secret(b"pw\n"), b"pw");
        assert_eq!(trim_secret(b"pw\r\n"), b"pw");
        assert_eq!(trim_secret(b"pw\n\n"), b"pw");
    }

    #[test]
    fn trims_both_ends_but_not_interior() {
        assert_eq!(trim_secret(b"  p w\t\n"), b"p w");
        assert_eq!(trim_secret(b"\tkey material "), b"key material");
    }

    #[test]
    fn passes_clean_and_empty_input_through() {
        assert_eq!(trim_secret(b"pw"), b"pw");
        assert_eq!(trim_secret(b""), b"");
        assert_eq!(trim_secret(b" \n\t"), b"");
    }
}
