//! The effective config and reload (ADR 0081 T6).
//!
//! `GET /admin/v1/config` shows what the node is running: the merged config (defaults <
//! file < env) with every secret replaced by a fingerprint, and the config file's checksum
//! — the value a deployment pipeline compares with the committed file.
//!
//! `POST /admin/v1/reload` runs the same reload `SIGHUP` does (ADR 0032, 0046 T4) and
//! returns its outcome. The file is still the only input: nothing in the request changes
//! the config (ADR 0081 §5).

use super::routes::{error, Answer};
use super::AdminState;
use crate::reload::{ConfigStamp, Reloader};
use serde_json::json;
use std::sync::Arc;

/// What the config endpoints need beside the live config.
#[derive(Clone)]
pub struct ReloadAccess {
    /// The reloader `SIGHUP` and the file watcher drive.
    pub reloader: Arc<Reloader>,
    /// The applied config file's checksum and load generation (ADR 0054 T3).
    pub stamp: Arc<ConfigStamp>,
}

impl std::fmt::Debug for ReloadAccess {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReloadAccess").finish_non_exhaustive()
    }
}

/// `GET /admin/v1/config`: the committed config, never a reload's unvalidated candidate.
pub async fn config(state: &AdminState) -> Answer {
    let committed = match state.reload.as_ref() {
        // Waits out a reload in flight (off the async workers), then reads config and
        // stamp together.
        Some(access) => {
            let reloader = access.reloader.clone();
            match tokio::task::spawn_blocking(move || reloader.committed_config()).await {
                Ok(c) => c,
                Err(e) => {
                    return error(
                        503,
                        "unavailable",
                        &format!("reading the config failed: {e}"),
                    )
                }
            }
        }
        None => None,
    };
    let (live, (checksum, generation)) = committed.unwrap_or_else(|| {
        let live = state
            .live_config
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        (live, (String::new(), 0))
    });
    let redacted = crate::config_view::redacted(&live);
    let body = match serde_json::to_value(&redacted) {
        Ok(v) => v,
        Err(e) => {
            return error(
                503,
                "unavailable",
                &format!("could not encode the config: {e}"),
            )
        }
    };
    (
        200,
        json!({
            "config": body,
            "file_checksum": (!checksum.is_empty()).then_some(checksum),
            "generation": generation,
            "redaction": "secret values are replaced by sha256 fingerprints; paths are shown as \
                          configured",
        })
        .to_string(),
    )
}

/// `POST /admin/v1/reload`: `200` with the outcome when it applied, `409 reload-rejected`
/// (with the outcome) when the new config or policy was refused and the running one kept.
pub async fn reload(state: &AdminState) -> Answer {
    let Some(access) = state.reload.as_ref() else {
        return error(503, "unavailable", "reload is not wired on this node");
    };
    let reloader = access.reloader.clone();
    // The reload reads files and rebuilds TLS material: off the async workers.
    let outcome =
        match tokio::task::spawn_blocking(move || reloader.reload_with_outcome("admin")).await {
            Ok(o) => o,
            Err(e) => return error(503, "unavailable", &format!("the reload task failed: {e}")),
        };
    if outcome.applied {
        (200, json!(outcome).to_string())
    } else {
        (
            409,
            json!({
                "error": {
                    "code": "reload-rejected",
                    "message": outcome.error.clone().unwrap_or_default(),
                },
                "outcome": outcome,
            })
            .to_string(),
        )
    }
}
