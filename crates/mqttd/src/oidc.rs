//! The OIDC JWKS fetch loop (ADR 0050) — the networked half.
//!
//! [`mqtt_auth::oidc::OidcAuthenticator`] is pure and synchronous; this task owns every
//! byte that crosses the network: discovery (`<issuer>/.well-known/openid-configuration`
//! → `jwks_uri`) and the JWKS fetches that follow rotation. Keys are pushed into the
//! authenticator with validate-before-swap (`install_jwks`), so a garbled or hostile
//! response can never evict the last-known-good set. The loop refetches on:
//!
//! - a TTL tick (`jwks_refresh`, default 5 min),
//! - a **refresh hint** — the authenticator saw an unknown `kid` (a rotation landing)
//!   or observed staleness. Hints arrive over a bounded(1) channel (the debounce) and
//!   are additionally rate-limited here, so a hostile client spamming unknown `kid`s
//!   cannot turn the broker into a request cannon against its own `IdP`.
//!
//! Discovery is checked before its `jwks_uri` is trusted: the document must name the
//! configured issuer exactly (OIDC Discovery 1.0 §4.3), and the `jwks_uri` must
//! be https unless the loud test override is set.
//!
//! Failure never panics and never fails open: fetch errors leave the installed set
//! untouched; the authenticator's own staleness window decides when last-known-good
//! stops being good (fail closed, ADR 0050 §3).

use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Duration;

use mqtt_auth::oidc::OidcAuthenticator;
use tracing::{info, warn};

/// Floor between two network fetches, whatever the hint pressure (anti-stampede).
const MIN_FETCH_GAP: Duration = Duration::from_secs(5);
/// Backoff ceiling for discovery/fetch retries.
const RETRY_MAX: Duration = Duration::from_mins(1);
/// Per-request timeout.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// The discovery document — the two fields we consume.
#[derive(serde::Deserialize)]
struct Discovery {
    issuer: String,
    jwks_uri: String,
}

/// Run the fetch loop until shutdown. `hints` is the authenticator's refresh-hint
/// channel (std sync — bridged here); `issuer` has already passed the https gate in
/// `main`.
///
/// # Panics
/// Panics only if the OS refuses to spawn the hint-bridge thread — an unrecoverable
/// resource exhaustion at startup, treated like any other spawn failure in the tree.
pub async fn run_fetch_loop(
    auth: Arc<OidcAuthenticator>,
    issuer: String,
    allow_http: bool,
    refresh: Duration,
    hints: Receiver<()>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    // Bridge the authenticator's std-sync hint channel into async: a blocking
    // forwarder thread parks on `recv` and nudges a tokio channel. (The authenticator
    // is sync by design — mqtt-auth stays runtime-free.)
    let (hint_tx, mut hint_rx) = tokio::sync::mpsc::channel::<()>(1);
    std::thread::Builder::new()
        .name("oidc-hint-bridge".into())
        .spawn(move || {
            while hints.recv().is_ok() {
                if hint_tx.blocking_send(()).is_err() {
                    return; // loop gone: shutdown
                }
            }
        })
        .expect("spawn oidc hint bridge");

    let client = match reqwest::Client::builder().timeout(HTTP_TIMEOUT).build() {
        Ok(c) => c,
        Err(e) => {
            // Never fails open: without a client there are no keys, and the
            // authenticator rejects (fail closed) until the operator intervenes.
            warn!(error = %e, "OIDC: HTTP client could not be built; token auth stays fail-closed");
            return;
        }
    };

    // Discovery with backoff: the IdP may come up after the broker (compose, k8s).
    let discovery_url = format!(
        "{}/.well-known/openid-configuration",
        issuer.trim_end_matches('/')
    );
    let mut backoff = mqtt_core::Backoff::new(Duration::from_secs(1), RETRY_MAX);
    let jwks_uri = loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            res = fetch_discovery(&client, &discovery_url, &issuer, allow_http) => match res {
                Ok(uri) => break uri,
                Err(e) => {
                    warn!(url = %discovery_url, error = %e,
                        "OIDC discovery failed; token auth fail-closed until it succeeds");
                    tokio::time::sleep(backoff.next_delay()).await;
                }
            }
        }
    };
    info!(%jwks_uri, "OIDC discovery complete");

    // Fetch loop: TTL tick or hint, floored by MIN_FETCH_GAP.
    let mut last_fetch: Option<tokio::time::Instant> = None;
    loop {
        // First iteration fetches immediately (last_fetch None).
        if let Some(prev) = last_fetch {
            let next_ttl = prev + refresh;
            tokio::select! {
                () = shutdown.cancelled() => return,
                () = tokio::time::sleep_until(next_ttl) => {}
                _ = hint_rx.recv() => {
                    // Rate-limit hint-driven fetches (unknown-kid spam is attacker
                    // reachable; the IdP is not our DoS amplifier).
                    let since = prev.elapsed();
                    if since < MIN_FETCH_GAP {
                        tokio::time::sleep(MIN_FETCH_GAP.saturating_sub(since)).await;
                    }
                }
            }
        }
        last_fetch = Some(tokio::time::Instant::now());
        match fetch_jwks(&client, &jwks_uri).await {
            Ok(bytes) => match auth.install_jwks(&bytes) {
                Ok(n) => info!(keys = n, "OIDC JWKS refreshed"),
                Err(e) => warn!(error = %e, "OIDC JWKS rejected; keeping last-known-good keys"),
            },
            Err(e) => {
                warn!(%jwks_uri, error = %e,
                    "OIDC JWKS fetch failed; keeping last-known-good keys (staleness window gates fail-closed)");
            }
        }
    }
}

async fn fetch_discovery(
    client: &reqwest::Client,
    url: &str,
    issuer: &str,
    allow_http: bool,
) -> Result<String, String> {
    let resp = client.get(url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    jwks_uri_from_discovery(
        &resp.bytes().await.map_err(|e| e.to_string())?,
        issuer,
        allow_http,
    )
}

/// The `jwks_uri` of a discovery document, once the document has passed its checks.
fn jwks_uri_from_discovery(body: &[u8], issuer: &str, allow_http: bool) -> Result<String, String> {
    let doc: Discovery = serde_json::from_slice(body)
        .map_err(|e| format!("discovery document does not parse: {e}"))?;
    // OpenID Connect Discovery 1.0 §4.3: the document names the issuer it was fetched for,
    // exactly. A document for another issuer (a misrouted proxy, a mixed-up tenant) would
    // otherwise hand over that issuer's keys.
    if doc.issuer != issuer {
        return Err(format!(
            "discovery document names issuer {:?}, not the configured {issuer:?} \
             (MQTTD_OIDC_ISSUER must match it exactly)",
            doc.issuer
        ));
    }
    // The jwks_uri inherits the issuer's transport bar: https unless explicitly
    // downgraded for tests — a compromised discovery document must not quietly
    // redirect key fetching onto plaintext.
    if !allow_http && !doc.jwks_uri.starts_with("https://") {
        return Err(format!(
            "jwks_uri is not https ({}) — refused without MQTTD_OIDC_ALLOW_HTTP",
            doc.jwks_uri
        ));
    }
    Ok(doc.jwks_uri)
}

async fn fetch_jwks(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, String> {
    let resp = client.get(url).send().await.map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    Ok(resp.bytes().await.map_err(|e| e.to_string())?.to_vec())
}

#[cfg(test)]
mod tests {
    use super::jwks_uri_from_discovery;

    const ISSUER: &str = "https://idp.test/realms/iot";

    fn doc(issuer: &str, jwks_uri: &str) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "issuer": issuer,
            "jwks_uri": jwks_uri,
            "token_endpoint": format!("{issuer}/protocol/openid-connect/token"),
        }))
        .unwrap()
    }

    /// A document that names the configured issuer yields its `jwks_uri`.
    #[test]
    fn a_document_for_the_configured_issuer_yields_its_jwks_uri() {
        let jwks = format!("{ISSUER}/protocol/openid-connect/certs");
        assert_eq!(
            jwks_uri_from_discovery(&doc(ISSUER, &jwks), ISSUER, false),
            Ok(jwks)
        );
    }

    /// OIDC Discovery 1.0 §4.3: a document that names any other issuer is
    /// refused, a trailing-slash difference included, and so is one without an issuer.
    #[test]
    fn a_document_for_another_issuer_is_refused() {
        let jwks = format!("{ISSUER}/protocol/openid-connect/certs");
        for other in [
            "https://idp.test/realms/other",
            "https://idp.test/realms/iot/",
        ] {
            let err = jwks_uri_from_discovery(&doc(other, &jwks), ISSUER, false).unwrap_err();
            assert!(err.contains(other) && err.contains(ISSUER), "{err}");
        }
        let no_issuer = serde_json::to_vec(&serde_json::json!({ "jwks_uri": jwks })).unwrap();
        assert!(jwks_uri_from_discovery(&no_issuer, ISSUER, false).is_err());
    }

    /// The `jwks_uri` must be https unless the loud test override is set.
    #[test]
    fn a_plaintext_jwks_uri_needs_the_test_override() {
        let plain = doc(ISSUER, "http://idp.test/certs");
        assert!(jwks_uri_from_discovery(&plain, ISSUER, false).is_err());
        assert_eq!(
            jwks_uri_from_discovery(&plain, ISSUER, true),
            Ok("http://idp.test/certs".to_string())
        );
    }
}
