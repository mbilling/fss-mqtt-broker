//! Variable byte integer codec.
//!
//! MQTT encodes the remaining-length field (and several MQTT 5 fields) as a
//! "variable byte integer": 1-4 bytes, 7 bits of payload each, the high bit
//! signalling continuation. The maximum encodable value is 268,435,455.

use crate::CodecError;

/// The largest value representable as a 4-byte variable byte integer.
pub const MAX: u32 = 268_435_455;

/// Decode a variable byte integer from the front of `buf`.
///
/// Returns the decoded value and the number of bytes consumed.
///
/// # Errors
/// - [`CodecError::Incomplete`] if `buf` ends mid-integer (continuation bit set
///   on the last available byte, fewer than 4 bytes present).
/// - [`CodecError::MalformedVarInt`] if a 4-byte sequence still sets the
///   continuation bit (the value would exceed [`MAX`]).
pub fn decode(buf: &[u8]) -> Result<(u32, usize), CodecError> {
    let mut value: u32 = 0;
    let mut multiplier: u32 = 1;
    for (i, &byte) in buf.iter().take(4).enumerate() {
        value += u32::from(byte & 0x7F) * multiplier;
        if byte & 0x80 == 0 {
            return Ok((value, i + 1));
        }
        multiplier *= 128;
    }
    if buf.len() < 4 {
        Err(CodecError::Incomplete)
    } else {
        Err(CodecError::MalformedVarInt)
    }
}

/// Append the variable byte integer encoding of `value` to `out`.
///
/// # Errors
/// Returns [`CodecError::ValueOutOfRange`] if `value` exceeds [`MAX`].
pub fn encode(mut value: u32, out: &mut Vec<u8>) -> Result<(), CodecError> {
    if value > MAX {
        return Err(CodecError::ValueOutOfRange("variable byte integer"));
    }
    loop {
        let mut byte = u8::try_from(value % 128).unwrap_or(0);
        value /= 128;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    Ok(())
}

/// The number of bytes the variable byte integer encoding of `value` occupies.
#[must_use]
pub fn encoded_len(value: u32) -> usize {
    match value {
        0..=127 => 1,
        128..=16_383 => 2,
        16_384..=2_097_151 => 3,
        _ => 4,
    }
}

#[cfg(test)]
mod tests {
    use super::{decode, encode, encoded_len, MAX};
    use crate::CodecError;

    #[test]
    fn decode_single_byte() {
        assert_eq!(decode(&[0x00]).unwrap(), (0, 1));
        assert_eq!(decode(&[0x7F]).unwrap(), (127, 1));
    }

    #[test]
    fn decode_multi_byte() {
        assert_eq!(decode(&[0x80, 0x01]).unwrap(), (128, 2));
        assert_eq!(decode(&[0xFF, 0xFF, 0xFF, 0x7F]).unwrap(), (MAX, 4));
    }

    #[test]
    fn decode_incomplete() {
        assert!(matches!(decode(&[0x80]), Err(CodecError::Incomplete)));
    }

    #[test]
    fn decode_malformed() {
        assert!(matches!(
            decode(&[0xFF, 0xFF, 0xFF, 0xFF]),
            Err(CodecError::MalformedVarInt)
        ));
    }

    #[test]
    fn encode_rejects_too_large() {
        let mut out = Vec::new();
        assert!(encode(MAX + 1, &mut out).is_err());
    }

    #[test]
    fn roundtrip_boundaries() {
        for v in [0, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, MAX] {
            let mut out = Vec::new();
            encode(v, &mut out).unwrap();
            assert_eq!(out.len(), encoded_len(v), "len mismatch for {v}");
            let (decoded, consumed) = decode(&out).unwrap();
            assert_eq!(decoded, v);
            assert_eq!(consumed, out.len());
        }
    }
}
