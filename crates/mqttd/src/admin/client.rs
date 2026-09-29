//! A minimal HTTPS client for the admin API: `mqttd --admin` uses it, and so does a node
//! asking its peers for their state (ADR 0081 §2).
//!
//! One request per connection, `Connection: close`, the response read to EOF — the
//! server's own framing. mTLS always: the client presents a certificate and verifies the
//! server against a CA bundle.

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

/// Most bytes of response accepted. The largest admin answer is one page of a list.
const MAX_RESPONSE: usize = 16 * 1024 * 1024;

/// Where and how to reach one admin listener.
#[derive(Clone)]
pub struct Target {
    /// `host:port` to connect to.
    pub addr: String,
    /// The name the server certificate must carry (defaults to the host of `addr`).
    pub server_name: String,
    /// The mTLS connector (our certificate, and the CA to verify the server with).
    pub connector: TlsConnector,
    /// Connect + request + response deadline.
    pub timeout: Duration,
}

impl std::fmt::Debug for Target {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Target")
            .field("addr", &self.addr)
            .field("server_name", &self.server_name)
            .finish_non_exhaustive()
    }
}

/// Parse `https://host:port` (a trailing `/` allowed) into `host:port`.
///
/// # Errors
/// A message when the URL is not `https://` or has no port.
pub fn parse_url(url: &str) -> Result<String, String> {
    let rest = url
        .strip_prefix("https://")
        .ok_or_else(|| format!("admin URL must start with https:// (got {url:?})"))?;
    let authority = rest.trim_end_matches('/');
    if authority.contains('/') || authority.is_empty() {
        return Err(format!("admin URL must be https://host:port (got {url:?})"));
    }
    let has_port = authority.rsplit_once(':').is_some_and(|(host, port)| {
        !host.is_empty() && port.parse::<u16>().is_ok() && !port.is_empty()
    });
    if !has_port {
        return Err(format!("admin URL needs an explicit port (got {url:?})"));
    }
    Ok(authority.to_string())
}

/// The host part of `host:port` / `[v6]:port`.
#[must_use]
pub fn host_of(addr: &str) -> String {
    let host = addr.rsplit_once(':').map_or(addr, |(h, _)| h);
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .to_string()
}

/// Send one request; return `(status, body)`.
///
/// # Errors
/// A message for a connection, TLS, timeout or framing failure. An HTTP error status is
/// not an error here: the caller reads the body's error code.
pub async fn call(
    target: &Target,
    method: &str,
    path_and_query: &str,
    body: Option<&str>,
) -> Result<(u16, String), String> {
    tokio::time::timeout(
        target.timeout,
        call_inner(target, method, path_and_query, body),
    )
    .await
    .map_err(|_| format!("{}: timed out after {:?}", target.addr, target.timeout))?
}

async fn call_inner(
    target: &Target,
    method: &str,
    path_and_query: &str,
    body: Option<&str>,
) -> Result<(u16, String), String> {
    let addr = &target.addr;
    let server_name = mqtt_net::tls::server_name(&target.server_name).map_err(|e| e.to_string())?;
    let tcp = TcpStream::connect(addr)
        .await
        .map_err(|e| format!("{addr}: connect failed: {e}"))?;
    let mut tls = target
        .connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| format!("{addr}: TLS handshake failed: {e}"))?;
    let body = body.unwrap_or("");
    let request = format!(
        "{method} {path_and_query} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\n\
         Content-Length: {len}\r\nConnection: close\r\n\r\n{body}",
        host = target.server_name,
        len = body.len(),
    );
    tls.write_all(request.as_bytes())
        .await
        .map_err(|e| format!("{addr}: write failed: {e}"))?;
    tls.flush()
        .await
        .map_err(|e| format!("{addr}: write failed: {e}"))?;
    let mut raw = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match tls.read(&mut chunk).await {
            Ok(0) => break,
            Ok(n) => {
                raw.extend_from_slice(&chunk[..n]);
                if raw.len() > MAX_RESPONSE {
                    return Err(format!("{addr}: response larger than {MAX_RESPONSE} bytes"));
                }
            }
            // A server that closes without TLS close_notify after a complete response is
            // still a complete response; anything short of the head is a real failure.
            Err(e) if raw.windows(4).any(|w| w == b"\r\n\r\n") => {
                tracing::debug!(error = %e, "admin response ended without close_notify");
                break;
            }
            Err(e) => return Err(format!("{addr}: read failed: {e}")),
        }
    }
    parse_response(&raw).map_err(|e| format!("{addr}: {e}"))
}

/// Split a raw `HTTP/1.1` response into status and body.
fn parse_response(raw: &[u8]) -> Result<(u16, String), String> {
    let end = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("malformed response (no header terminator)")?;
    let head = std::str::from_utf8(&raw[..end]).map_err(|_| "malformed response head")?;
    let status = head
        .split("\r\n")
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .and_then(|s| s.parse::<u16>().ok())
        .ok_or("malformed status line")?;
    let body =
        String::from_utf8(raw[end + 4..].to_vec()).map_err(|_| "response body is not UTF-8")?;
    Ok((status, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_need_https_and_a_port() {
        assert_eq!(
            parse_url("https://127.0.0.1:9443").unwrap(),
            "127.0.0.1:9443"
        );
        assert_eq!(
            parse_url("https://node-1.mqttd:9443/").unwrap(),
            "node-1.mqttd:9443"
        );
        assert_eq!(parse_url("https://[::1]:9443").unwrap(), "[::1]:9443");
        assert!(parse_url("http://127.0.0.1:9443").is_err());
        assert!(parse_url("https://127.0.0.1").is_err());
        assert!(parse_url("https://127.0.0.1:9443/admin").is_err());
        assert_eq!(host_of("[::1]:9443"), "::1");
        assert_eq!(host_of("node-1:9443"), "node-1");
    }

    #[test]
    fn responses_split_into_status_and_body() {
        let raw = b"HTTP/1.1 403 Forbidden\r\nContent-Length: 2\r\n\r\n{}";
        assert_eq!(parse_response(raw).unwrap(), (403, "{}".to_string()));
        assert!(parse_response(b"HTTP/1.1 200 OK\r\n").is_err());
    }
}
