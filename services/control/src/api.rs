//! Session-cookie authenticated user API under `/v1`.
//!
//! Every handler here resolves the caller from the `osf_session` cookie and
//! operates only on that user's own resources.

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::{db, json_error, AppState};

/// Resolve the caller or produce a `401` response.
async fn require_user(state: &AppState, headers: &HeaderMap) -> Result<db::User, Response> {
    state
        .current_user(headers)
        .await
        .ok_or_else(|| json_error(StatusCode::UNAUTHORIZED, "authentication required"))
}

fn internal_error(err: impl std::fmt::Display) -> Response {
    tracing::error!(%err, "database error");
    json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
}

/// `GET /v1/me` — the caller's profile, keys, stations, and current tier.
#[derive(Serialize)]
struct MeResponse {
    user: db::User,
    keys: Vec<db::ApiKey>,
    stations: Vec<db::Station>,
    tier: &'static str,
}

pub async fn me(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let user = match require_user(&state, &headers).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    let conn = state.db.lock().await;
    let body = (|| {
        Ok::<_, anyhow::Error>(MeResponse {
            keys: db::keys_for_user(&conn, &user.id)?,
            stations: db::stations_for_user(&conn, &user.id)?,
            tier: db::tier_for_user(&conn, &user.id)?,
            user,
        })
    })();
    match body {
        Ok(body) => Json(body).into_response(),
        Err(err) => internal_error(err),
    }
}

/// `POST /v1/keys` — issue a `live` or `feed` key. Station keys are minted
/// only through station registration, so they are rejected here.
#[derive(Deserialize)]
pub struct CreateKeyRequest {
    kind: String,
    #[serde(default)]
    label: Option<String>,
}

pub async fn create_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateKeyRequest>,
) -> Response {
    let user = match require_user(&state, &headers).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    if req.kind != "live" && req.kind != "feed" {
        return json_error(
            StatusCode::BAD_REQUEST,
            "kind must be 'live' or 'feed'; station keys come from station registration",
        );
    }
    let conn = state.db.lock().await;
    match db::create_key(&conn, &user.id, &req.kind, req.label.as_deref()) {
        Ok(key) => (StatusCode::CREATED, Json(key)).into_response(),
        Err(err) => internal_error(err),
    }
}

/// `DELETE /v1/keys/{key}` — revoke one of the caller's keys.
pub async fn revoke_key(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path(key): Path<String>,
) -> Response {
    let user = match require_user(&state, &headers).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    let conn = state.db.lock().await;
    match db::revoke_key(&conn, &user.id, &key) {
        Ok(true) => (StatusCode::OK, Json(serde_json::json!({ "revoked": true }))).into_response(),
        Ok(false) => json_error(StatusCode::NOT_FOUND, "no such live key for this account"),
        Err(err) => internal_error(err),
    }
}

/// `POST /v1/stations` — register a station and mint its `osf_stn_` key.
#[derive(Deserialize)]
pub struct CreateStationRequest {
    name: String,
    #[serde(default)]
    lat: Option<f64>,
    #[serde(default)]
    lon: Option<f64>,
}

#[derive(Serialize)]
struct CreateStationResponse {
    station: db::Station,
    key: db::ApiKey,
}

pub async fn create_station(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<CreateStationRequest>,
) -> Response {
    let user = match require_user(&state, &headers).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    if req.name.trim().is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "station name is required");
    }
    let mut conn = state.db.lock().await;
    match db::create_station(&mut conn, &user.id, req.name.trim(), req.lat, req.lon) {
        Ok((station, key)) => {
            (StatusCode::CREATED, Json(CreateStationResponse { station, key })).into_response()
        }
        Err(err) => internal_error(err),
    }
}

/// `GET /v1/stations` — the caller's stations with rolling stats.
pub async fn list_stations(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let user = match require_user(&state, &headers).await {
        Ok(u) => u,
        Err(resp) => return resp,
    };
    let conn = state.db.lock().await;
    match db::stations_for_user(&conn, &user.id) {
        Ok(stations) => Json(serde_json::json!({ "stations": stations })).into_response(),
        Err(err) => internal_error(err),
    }
}
