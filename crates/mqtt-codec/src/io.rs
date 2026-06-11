//! Bounds-checked readers and writers for MQTT primitive wire types.
//!
//! Every read is bounds-checked and returns [`CodecError::MalformedPacket`] on
//! underflow rather than panicking. Reads of byte slices are zero-copy: they
//! split from the underlying [`Bytes`] without allocating, which keeps PUBLISH
//! payloads cheap to route.

use crate::CodecError;
use bytes::{Buf, Bytes};

/// A cursor over a packet body that reads MQTT primitive types with bounds checks.
///
/// A `Reader` is constructed over the *body* of a single packet whose full extent
/// is already known (the fixed header's remaining-length has been satisfied), so
/// any underflow indicates a structurally malformed packet, never a short read.
#[derive(Debug)]
pub struct Reader {
    buf: Bytes,
}

impl Reader {
    /// Create a reader over `buf`.
    #[must_use]
    pub fn new(buf: Bytes) -> Self {
        Self { buf }
    }

    /// Bytes remaining to be read.
    #[must_use]
    pub fn remaining(&self) -> usize {
        self.buf.len()
    }

    /// Whether all bytes have been consumed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    fn ensure(&self, n: usize) -> Result<(), CodecError> {
        if self.buf.len() < n {
            Err(CodecError::MalformedPacket("unexpected end of packet body"))
        } else {
            Ok(())
        }
    }

    /// Read a single byte.
    pub fn read_u8(&mut self) -> Result<u8, CodecError> {
        self.ensure(1)?;
        Ok(self.buf.get_u8())
    }

    /// Read a big-endian two-byte integer.
    pub fn read_u16(&mut self) -> Result<u16, CodecError> {
        self.ensure(2)?;
        Ok(self.buf.get_u16())
    }

    /// Read a big-endian four-byte integer.
    pub fn read_u32(&mut self) -> Result<u32, CodecError> {
        self.ensure(4)?;
        Ok(self.buf.get_u32())
    }

    /// Read a variable byte integer, returning its value.
    pub fn read_varint(&mut self) -> Result<u32, CodecError> {
        let (value, consumed) = crate::varint::decode(&self.buf)?;
        self.buf.advance(consumed);
        Ok(value)
    }

    /// Read exactly `n` bytes, zero-copy.
    pub fn read_bytes(&mut self, n: usize) -> Result<Bytes, CodecError> {
        self.ensure(n)?;
        Ok(self.buf.split_to(n))
    }

    /// Read a length-prefixed binary blob (`u16` length + bytes), zero-copy.
    pub fn read_binary(&mut self) -> Result<Bytes, CodecError> {
        let len = self.read_u16()? as usize;
        self.read_bytes(len)
    }

    /// Read a length-prefixed UTF-8 string (`u16` length + bytes).
    ///
    /// # Errors
    /// Returns [`CodecError::InvalidUtf8`] if the bytes are not valid UTF-8.
    pub fn read_string(&mut self) -> Result<String, CodecError> {
        let bytes = self.read_binary()?;
        String::from_utf8(bytes.to_vec()).map_err(|_| CodecError::InvalidUtf8)
    }

    /// Take all remaining bytes, zero-copy. Useful for PUBLISH payloads.
    #[must_use]
    pub fn read_remaining(&mut self) -> Bytes {
        self.buf.split_off(0)
    }
}

/// Append a single byte.
pub fn put_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}

/// Append a big-endian two-byte integer.
pub fn put_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Append a big-endian four-byte integer.
pub fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_be_bytes());
}

/// Append a length-prefixed binary blob (`u16` length + bytes).
///
/// # Errors
/// Returns [`CodecError::ValueOutOfRange`] if `data` exceeds 65,535 bytes.
pub fn put_binary(out: &mut Vec<u8>, data: &[u8]) -> Result<(), CodecError> {
    let len =
        u16::try_from(data.len()).map_err(|_| CodecError::ValueOutOfRange("binary data length"))?;
    put_u16(out, len);
    out.extend_from_slice(data);
    Ok(())
}

/// Append a length-prefixed UTF-8 string (`u16` length + bytes).
///
/// # Errors
/// Returns [`CodecError::ValueOutOfRange`] if `s` exceeds 65,535 bytes.
pub fn put_string(out: &mut Vec<u8>, s: &str) -> Result<(), CodecError> {
    put_binary(out, s.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::{put_binary, put_string, put_u16, put_u32, put_u8, Reader};
    use crate::CodecError;
    use bytes::Bytes;

    #[test]
    fn read_integers() {
        let mut r = Reader::new(Bytes::from_static(&[
            0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
        ]));
        assert_eq!(r.read_u8().unwrap(), 1);
        assert_eq!(r.read_u16().unwrap(), 2);
        assert_eq!(r.read_u32().unwrap(), 3);
        assert!(r.is_empty());
    }

    #[test]
    fn read_string_and_binary() {
        let mut out = Vec::new();
        put_string(&mut out, "hello").unwrap();
        put_binary(&mut out, &[0xDE, 0xAD]).unwrap();
        let mut r = Reader::new(Bytes::from(out));
        assert_eq!(r.read_string().unwrap(), "hello");
        assert_eq!(&r.read_binary().unwrap()[..], &[0xDE, 0xAD]);
    }

    #[test]
    fn underflow_is_malformed_not_panic() {
        let mut r = Reader::new(Bytes::from_static(&[0x00]));
        assert!(matches!(r.read_u16(), Err(CodecError::MalformedPacket(_))));
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut out = Vec::new();
        put_u16(&mut out, 2);
        out.extend_from_slice(&[0xFF, 0xFE]); // not valid UTF-8
        let mut r = Reader::new(Bytes::from(out));
        assert!(matches!(r.read_string(), Err(CodecError::InvalidUtf8)));
    }

    #[test]
    fn string_length_overflow_rejected() {
        let mut out = Vec::new();
        let huge = "a".repeat(70_000);
        assert!(matches!(
            put_string(&mut out, &huge),
            Err(CodecError::ValueOutOfRange(_))
        ));
    }

    #[test]
    fn put_helpers_roundtrip_u32() {
        let mut out = Vec::new();
        put_u8(&mut out, 0xAB);
        put_u32(&mut out, 0x1122_3344);
        let mut r = Reader::new(Bytes::from(out));
        assert_eq!(r.read_u8().unwrap(), 0xAB);
        assert_eq!(r.read_u32().unwrap(), 0x1122_3344);
    }
}
