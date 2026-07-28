//! `/config` — the web-UI equivalent of `falcon config get/set`. This is the
//! ONLY configuration path besides the CLI; Falcon never reads env vars. A
//! `POST` here persists to the same profile file the CLI writes, so a change
//! made in the UI is durable and visible to `falcon config list`.
//!
//! Changes take effect on the next `falcon serve` (the running process keeps
//! its loaded config), matching the CLI's semantics — the UI surfaces this.

use crate::state::AppState;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use falcon_core::Profile;
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
pub struct ConfigEntry {
    pub key: String,
    pub value: String,
}

#[derive(Serialize)]
pub struct ConfigResponse {
    pub profile_path: String,
    pub entries: Vec<ConfigEntry>,
}

/// Read the current profile's settable keys and values, for the UI's config
/// panel. Falls back to defaults if no profile exists yet.
pub async fn get_config(State(state): State<AppState>) -> impl IntoResponse {
    let path = state.profile_path.as_ref();
    let profile = Profile::load_or_default(path).unwrap_or_default();
    let entries = profile
        .entries()
        .into_iter()
        .map(|(k, v)| ConfigEntry {
            key: k.to_string(),
            // Never echo secrets back to the browser in full.
            value: if k == "api-key" && !v.is_empty() {
                "••••••".to_string()
            } else {
                v
            },
        })
        .collect();
    Json(ConfigResponse {
        profile_path: path.display().to_string(),
        entries,
    })
}

#[derive(Deserialize)]
pub struct SetConfigRequest {
    pub key: String,
    pub value: String,
}

/// Persist one config key to the profile file (auth-gated, unlike GET).
pub async fn set_config(
    State(state): State<AppState>,
    Json(req): Json<SetConfigRequest>,
) -> impl IntoResponse {
    let path = state.profile_path.as_ref();
    let mut profile = match Profile::load_or_default(path) {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    if let Err(e) = profile.set(&req.key, &req.value) {
        return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
    }
    if let Err(e) = profile.save(path) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "ok": true,
            "key": req.key,
            "note": "saved to profile — restart `falcon serve` for it to take effect"
        })),
    )
        .into_response()
}
