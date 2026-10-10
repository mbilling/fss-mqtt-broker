//! The compression functions, as EMQX calls Erlang's `zlib` and its LZ4 NIF.
//!
//! - `zip_compress`/`zip_uncompress` are `zlib:compress/1`/`uncompress/1` (zlib
//!   wrapper), `zip`/`unzip` are `zlib:zip/1`/`unzip/1` (raw deflate), and
//!   `gzip`/`gunzip` are `zlib:gzip/1`/`gunzip/1`. Compression is C zlib's level 6
//!   (Erlang's `default`), window 15, memory level 8, so the bytes equal EMQX's; the gzip
//!   header is the one zlib writes on Linux (no name or time, OS byte 3).
//! - Decompressing stops at the end of the stream and ignores what follows, except
//!   `gunzip`, which (with zlib's `reset`) decodes concatenated members and fails on
//!   anything else after one. A truncated or corrupt stream fails.
//! - `lz4_compress`/`lz4_uncompress` are liblz4's `LZ4F_compressFrame` (level 0, 64 KiB
//!   blocks, independent when the input fits one, no checksums) and frame decompression
//!   of the first frame.
//!
//! What a decompression may produce is bounded by the message's growth budget
//! ([`super::MAX_BUILT_BYTES`]): a few hundred bytes of payload can inflate to
//! gigabytes, so output is produced in chunks and refused past the budget.

use std::io::{Read as _, Write as _};

use flate2::{Compression, Decompress, FlushDecompress, Status};

use super::{bounded_growth, FnCtx, MAX_BUILT_BYTES};
use crate::value::Value;
use crate::EvalError;

/// The deflate container.
#[derive(Clone, Copy)]
pub(crate) enum Wrap {
    /// zlib header and Adler-32 trailer.
    Zlib,
    /// No header or trailer.
    Raw,
    /// gzip header and CRC-32 trailer.
    Gzip,
}

/// Compress `input`.
pub(crate) fn deflate(
    cx: &FnCtx,
    name: &str,
    input: &[u8],
    wrap: Wrap,
) -> Result<Value, EvalError> {
    let level = Compression::new(6);
    let out = Vec::with_capacity(input.len() / 2 + 64);
    let write = |res: std::io::Result<Vec<u8>>| res.map_err(|e| EvalError::new(e.to_string()));
    let out = match wrap {
        Wrap::Zlib => {
            let mut e = flate2::write::ZlibEncoder::new(out, level);
            write(e.write_all(input).and_then(|()| e.finish()))?
        }
        Wrap::Raw => {
            let mut e = flate2::write::DeflateEncoder::new(out, level);
            write(e.write_all(input).and_then(|()| e.finish()))?
        }
        Wrap::Gzip => {
            // zlib's own gzip header on Linux: no mtime, no name, OS 3 (Unix).
            let mut e = flate2::GzBuilder::new()
                .operating_system(3)
                .write(out, level);
            write(e.write_all(input).and_then(|()| e.finish()))?
        }
    };
    bounded_growth(cx, name, input.len(), Some(out.len()))?;
    Ok(Value::from_bytes(&out.into()))
}

/// The most `name` may produce from `input`: the input plus what is left of the budget.
fn output_limit(cx: &FnCtx, input: &[u8]) -> usize {
    input
        .len()
        .saturating_add(MAX_BUILT_BYTES.saturating_sub(cx.ctx.built.get()))
}

/// Refused mid-stream, before the rest is inflated.
fn too_large() -> EvalError {
    EvalError::new(format!(
        "decompression stopped: the output would take the message past its \
         {MAX_BUILT_BYTES}-byte growth budget (the budget is per message, across every \
         function call)"
    ))
}

/// Decompress `input`.
pub(crate) fn inflate(
    cx: &FnCtx,
    name: &str,
    input: &[u8],
    wrap: Wrap,
) -> Result<Value, EvalError> {
    let kind = match wrap {
        Wrap::Zlib => "zlib",
        Wrap::Raw => "deflate",
        Wrap::Gzip => "gzip",
    };
    let data_error = || EvalError::new(format!("data_error: not a complete {kind} stream"));
    let fresh = || match wrap {
        Wrap::Zlib => Decompress::new(true),
        Wrap::Raw => Decompress::new(false),
        Wrap::Gzip => Decompress::new_gzip(15),
    };
    let limit = output_limit(cx, input);
    let mut out: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 32 * 1024];
    let mut d = fresh();
    let mut at = 0usize;
    loop {
        let (in0, out0) = (d.total_in(), d.total_out());
        let status = d
            .decompress(&input[at..], &mut buf, FlushDecompress::None)
            .map_err(|_| data_error())?;
        let used = usize::try_from(d.total_in() - in0).map_err(|_| data_error())?;
        let made = usize::try_from(d.total_out() - out0).map_err(|_| data_error())?;
        at += used;
        out.extend_from_slice(&buf[..made]);
        if out.len() > limit {
            return Err(too_large());
        }
        match status {
            Status::StreamEnd => {
                // gunzip resets on a member's end and goes on with what follows.
                if matches!(wrap, Wrap::Gzip) && at < input.len() {
                    d = fresh();
                    continue;
                }
                break;
            }
            Status::Ok | Status::BufError => {
                if used == 0 && made == 0 {
                    // Neither input taken nor output made: the stream is cut short.
                    return Err(data_error());
                }
            }
        }
    }
    bounded_growth(cx, name, input.len(), Some(out.len()))?;
    Ok(Value::from_bytes(&out.into()))
}

/// The largest input `LZ4F_compressFrame` writes as one block, and so with independent
/// blocks.
const LZ4_BLOCK: usize = 64 * 1024;

/// `lz4b_nif:dirty_compress_frame(Data, 0)`.
pub(crate) fn lz4_compress(cx: &FnCtx, input: &[u8]) -> Result<Value, EvalError> {
    let fail = |e: std::io::Error| EvalError::new(e.to_string());
    let mut e = lz4::EncoderBuilder::new()
        .level(0)
        .block_size(lz4::BlockSize::Max64KB)
        .block_mode(if input.len() <= LZ4_BLOCK {
            lz4::BlockMode::Independent
        } else {
            lz4::BlockMode::Linked
        })
        .checksum(lz4::ContentChecksum::NoChecksum)
        .block_checksum(lz4::liblz4::BlockChecksum::NoBlockChecksum)
        .build(Vec::with_capacity(input.len() + 32))
        .map_err(fail)?;
    e.write_all(input).map_err(fail)?;
    let (out, res) = e.finish();
    res.map_err(fail)?;
    bounded_growth(cx, "lz4_compress", input.len(), Some(out.len()))?;
    Ok(Value::from_bytes(&out.into()))
}

/// `lz4b_nif:dirty_decompress_frame(Data, 0)`: the first frame; what follows it is
/// ignored.
pub(crate) fn lz4_uncompress(cx: &FnCtx, input: &[u8]) -> Result<Value, EvalError> {
    let fail = |e: std::io::Error| EvalError::new(e.to_string());
    let limit = output_limit(cx, input);
    let mut d = lz4::Decoder::new(input).map_err(fail)?;
    let mut out: Vec<u8> = Vec::new();
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        let n = d.read(&mut buf).map_err(fail)?;
        if n == 0 {
            break;
        }
        out.extend_from_slice(&buf[..n]);
        if out.len() > limit {
            return Err(too_large());
        }
    }
    let (_, res) = d.finish();
    res.map_err(|_| EvalError::new("incomplete_frame: the LZ4 frame is cut short"))?;
    bounded_growth(cx, "lz4_uncompress", input.len(), Some(out.len()))?;
    Ok(Value::from_bytes(&out.into()))
}
