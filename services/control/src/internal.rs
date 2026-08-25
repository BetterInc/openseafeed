//! Internal service-to-service API under `/v1/internal`.
//!
//! Guarded by a shared secret in the `X-Internal-Token` header rather than a
//! user session: these endpoints are called by other OpenSeaFeed services
//! (ingest, fan-out) on the private network, not by browsers.

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use crate::{db, json_error, AppState};

const INTERNAL_HEADER: &str = "x-internal-token";

/// Check the shared secret. Returns `Some(response)` to send back when the
/// token is missing or wrong, or `None` when the caller is authorized.
fn authorize(state: &AppState, headers: &HeaderMap) -> Option<Response> {
    let provided = headers
        .get(INTERNAL_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .as_bytes();
    let expected = state.cfg.internal_token.as_bytes();
    // Length-checked, non-short-circuiting byte compare so the check does not
    // leak the secret's length or a prefix match through timing.
    let equal = provided.len() == expected.len()
        && provided
            .iter()
            .zip(expected)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0;
    if equal {
        None
    } else {
        Some(json_error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing X-Internal-Token",
        ))
    }
}

/// `GET /v1/internal/keys/validate?key=...`
///
/// Resolves a key to its kind, owner, tier, and (for station keys) station
/// id. Unknown or revoked keys report `{"valid": false}` with a `200` so the
/// caller can distinguish "auth failed" (`401`) from "key is not valid".
#[derive(Deserialize)]
pub struct ValidateQuery {
    key: String,
}

pub async fn validate_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ValidateQuery>,
) -> Response {
    if let Some(resp) = authorize(&state, &headers) {
        return resp;
    }
    let conn = state.db.lock().await;

    let key = match db::key_by_id(&conn, &q.key) {
        Ok(k) => k,
        Err(err) => {
            tracing::error!(%err, "validate_key db error");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    let key = match key {
        Some(k) if k.revoked_at.is_none() => k,
        _ => return Json(serde_json::json!({ "valid": false })).into_response(),
    };

    // Feed/station keys presenting themselves IS contribution - record it so
    // the contributor tier follows automatically from feeding the network.
    // Throttle: skip if touched within the last 5 minutes.
    if key.kind != "live" {
        let stale = key
            .last_used_at
            .as_deref()
            .and_then(|t| chrono::DateTime::parse_from_rfc3339(t).ok())
            .map(|t| {
                chrono::Utc::now() - t.with_timezone(&chrono::Utc) > chrono::Duration::minutes(5)
            })
            .unwrap_or(true);
        if stale {
            if let Err(err) = db::touch_key(&conn, &key.key) {
                tracing::warn!(%err, "touch_key failed");
            }
        }
    }

    let tier = match db::tier_for_user(&conn, &key.user_id) {
        Ok(t) => t,
        Err(err) => {
            tracing::error!(%err, "tier lookup failed");
            return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    let station_id = if key.kind == "station" {
        db::station_by_key(&conn, &key.key)
            .ok()
            .flatten()
            .map(|s| s.id)
    } else {
        None
    };

    Json(serde_json::json!({
        "valid": true,
        "kind": key.kind,
        "tier": tier,
        "owner_id": key.user_id,
        "station_id": station_id,
    }))
    .into_response()
}

/// `POST /v1/internal/stations/heartbeat`
///
/// Called by the ingest path to record that a station is alive and how many
/// messages it has forwarded since the last beat.
#[derive(Deserialize)]
pub struct HeartbeatRequest {
    station_key: String,
    #[serde(default)]
    msgs: i64,
}

pub async fn heartbeat(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<HeartbeatRequest>,
) -> Response {
    if let Some(resp) = authorize(&state, &headers) {
        return resp;
    }
    let conn = state.db.lock().await;
    match db::record_heartbeat(&conn, &req.station_key, req.msgs) {
        Ok(0) => json_error(StatusCode::NOT_FOUND, "unknown station key"),
        Ok(_) => Json(serde_json::json!({ "ok": true })).into_response(),
        Err(err) => {
            tracing::error!(%err, "heartbeat db error");
            json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        }
    }
}
