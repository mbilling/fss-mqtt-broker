//! The admin listener's HTTP/1.1: one request per connection, bounded, no dependencies.
//!
//! The same shape as the health server's (`crate::health`): read the whole request,
//! answer, close. Unlike health, admin requests carry a method, a query string and, for
//! actions, a small body, so this parses all three — and nothing else. Every size and the
//! time to send the request are capped, so a slow or oversized client costs a bounded
//! amount before it is dropped.

use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Most bytes of request head (request line and headers) accepted.
const MAX_HEAD: usize = 16 * 1024;
/// Most bytes of request body accepted. Admin bodies are a few fields of JSON.
pub const MAX_BODY: usize = 64 * 1024;
/// How long a client has to send its whole request after the TLS handshake.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// One parsed request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `GET`, `POST`, …
    pub method: String,
    /// The path, percent-decoded, without the query string.
    pub path: String,
    /// The query parameters in order, percent-decoded.
    pub query: Vec<(String, String)>,
    /// The body (empty without `Content-Length`).
    pub body: Vec<u8>,
}

impl Request {
    /// The first value of query parameter `name`.
    #[must_use]
    pub fn param(&self, name: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Why a request could not be read. Each maps to one status and reason code.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadError {
    /// Not HTTP/1.x, or a malformed line or header.
    Malformed,
    /// Head or body over its cap.
    TooLarge,
    /// The peer closed before the request was complete.
    Closed,
}

/// Read one request from `stream`.
///
/// # Errors
/// [`ReadError`] for a malformed, oversized or truncated request; I/O errors are
/// reported as [`ReadError::Closed`].
pub async fn read_request<S: AsyncRead + Unpin>(stream: &mut S) -> Result<Request, ReadError> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 2048];
    let head_end = loop {
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        if buf.len() > MAX_HEAD {
            return Err(ReadError::TooLarge);
        }
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|_| ReadError::Closed)?;
        if n == 0 {
            return Err(ReadError::Closed);
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = std::str::from_utf8(&buf[..head_end]).map_err(|_| ReadError::Malformed)?;
    let mut lines = head.split("\r\n");
    let (method, target) = parse_request_line(lines.next().unwrap_or_default())?;
    let mut content_length = 0usize;
    for line in lines {
        let (name, value) = line.split_once(':').ok_or(ReadError::Malformed)?;
        if name.trim().eq_ignore_ascii_case("content-length") {
            content_length = value.trim().parse().map_err(|_| ReadError::Malformed)?;
        } else if name.trim().eq_ignore_ascii_case("transfer-encoding") {
            // Chunked bodies are not needed by any admin client; refusing them keeps the
            // one framing rule (Content-Length) and closes off smuggling-shaped inputs.
            return Err(ReadError::Malformed);
        }
    }
    if content_length > MAX_BODY {
        return Err(ReadError::TooLarge);
    }
    let mut body = buf[head_end + 4..].to_vec();
    if body.len() > content_length {
        return Err(ReadError::Malformed);
    }
    while body.len() < content_length {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|_| ReadError::Closed)?;
        if n == 0 {
            return Err(ReadError::Closed);
        }
        body.extend_from_slice(&chunk[..n]);
        if body.len() > content_length {
            return Err(ReadError::Malformed);
        }
    }
    let (path, query) = split_target(target)?;
    Ok(Request {
        method: method.to_string(),
        path,
        query,
        body,
    })
}

fn parse_request_line(line: &str) -> Result<(&str, &str), ReadError> {
    let mut parts = line.split(' ');
    let (Some(method), Some(target), Some(version), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(ReadError::Malformed);
    };
    if !version.starts_with("HTTP/1.") || !target.starts_with('/') || method.is_empty() {
        return Err(ReadError::Malformed);
    }
    Ok((method, target))
}

/// Split `/path?a=1&b=2` into the decoded path and decoded parameters.
fn split_target(target: &str) -> Result<(String, Vec<(String, String)>), ReadError> {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let path = percent_decode(path, false)?;
    let mut params = Vec::new();
    for pair in query.split('&').filter(|p| !p.is_empty()) {
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        params.push((percent_decode(k, true)?, percent_decode(v, true)?));
    }
    Ok((path, params))
}

/// Decode `%XX` escapes (and `+` as space in a query component).
///
/// # Errors
/// [`ReadError::Malformed`] for a bad escape or a result that is not UTF-8.
pub fn percent_decode(s: &str, plus_is_space: bool) -> Result<String, ReadError> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = bytes.get(i + 1..i + 3).ok_or(ReadError::Malformed)?;
                let hex = std::str::from_utf8(hex).map_err(|_| ReadError::Malformed)?;
                out.push(u8::from_str_radix(hex, 16).map_err(|_| ReadError::Malformed)?);
                i += 3;
            }
            b'+' if plus_is_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| ReadError::Malformed)
}

/// Percent-encode one query component (everything but unreserved characters).
#[must_use]
pub fn percent_encode(s: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// Write a JSON response and close.
///
/// # Errors
/// The underlying write error.
pub async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    status: u16,
    body: &str,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        413 => "Payload Too Large",
        503 => "Service Unavailable",
        _ => "",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    stream.shutdown().await
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn parse(raw: &[u8]) -> Result<Request, ReadError> {
        let mut input = raw;
        read_request(&mut input).await
    }

    #[tokio::test]
    async fn a_get_with_a_query_is_decoded() {
        let req = parse(b"GET /admin/v1/clients?user=a%20b&prefix=x+y HTTP/1.1\r\nHost: h\r\n\r\n")
            .await
            .unwrap();
        assert_eq!(req.method, "GET");
        assert_eq!(req.path, "/admin/v1/clients");
        assert_eq!(req.param("user"), Some("a b"));
        assert_eq!(req.param("prefix"), Some("x y"));
        assert!(req.body.is_empty());
    }

    #[tokio::test]
    async fn a_post_body_is_read_to_its_length() {
        let req = parse(b"POST /x HTTP/1.1\r\nContent-Length: 7\r\n\r\n{\"a\":1}")
            .await
            .unwrap();
        assert_eq!(req.body, b"{\"a\":1}");
    }

    #[tokio::test]
    async fn oversized_and_malformed_requests_are_refused() {
        let big = format!(
            "POST /x HTTP/1.1\r\nContent-Length: {}\r\n\r\n",
            MAX_BODY + 1
        );
        assert_eq!(parse(big.as_bytes()).await, Err(ReadError::TooLarge));
        assert_eq!(
            parse(b"POST /x HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").await,
            Err(ReadError::Malformed)
        );
        assert_eq!(
            parse(b"GET x HTTP/1.1\r\n\r\n").await,
            Err(ReadError::Malformed)
        );
        assert_eq!(
            parse(b"GET /x SPDY\r\n\r\n").await,
            Err(ReadError::Malformed)
        );
        assert_eq!(parse(b"GET /x HTTP/1.1\r\n").await, Err(ReadError::Closed));
        assert_eq!(
            parse(b"GET /%zz HTTP/1.1\r\n\r\n").await,
            Err(ReadError::Malformed)
        );
    }

    #[test]
    fn encode_and_decode_round_trip() {
        for s in ["a b", "sensors/+/temp", "#", "ü/ß", "x=y&z"] {
            assert_eq!(percent_decode(&percent_encode(s), true).unwrap(), s);
        }
    }
}
