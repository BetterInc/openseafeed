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
        Ok((station, key)) => (
            StatusCode::CREATED,
            Json(CreateStationResponse { station, key }),
        )
            .into_response(),
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

// --- vessel photos ----------------------------------------------------------

/// `GET /v1/photos/{mmsi}` — free-licensed photo for a vessel, looked
/// up on Wikidata (MMSI property P587 -> Commons image P18) and cached in
/// sqlite. Public and CORS-open: the live map calls it cross-origin from the
/// stream host. Coverage is honest-but-thin: famous ships, ferries, navy and
/// big cargo have Commons photos; small craft essentially never do.
#[derive(Deserialize)]
pub struct PhotoQuery {
    /// IMO number when the caller knows it (from the stream's type 5
    /// statics). Wikidata keys ships by IMO ~2.5x more often than by MMSI,
    /// so this dramatically improves hit rate.
    imo: Option<u32>,
}

pub async fn vessel_photo(
    State(state): State<AppState>,
    Path(mmsi): Path<u32>,
    axum::extract::Query(q): axum::extract::Query<PhotoQuery>,
) -> Response {
    const POSITIVE_TTL_DAYS: i64 = 30;
    const NEGATIVE_TTL_DAYS: i64 = 7;

    {
        let conn = state.db.lock().await;
        if let Ok(Some(p)) = db::photo_get(&conn, mmsi) {
            let ttl = if p.image_url.is_some() {
                POSITIVE_TTL_DAYS
            } else {
                NEGATIVE_TTL_DAYS
            };
            let fresh = chrono::DateTime::parse_from_rfc3339(&p.fetched_at)
                .map(|t| {
                    chrono::Utc::now() - t.with_timezone(&chrono::Utc) < chrono::Duration::days(ttl)
                })
                .unwrap_or(false);
            // A cached miss that ran without the IMO we now have is not an
            // answer - retry with the stronger identity.
            let identity_ok = p.image_url.is_some() || q.imo.is_none() || p.imo == q.imo;
            if fresh && identity_ok {
                return photo_response(p.image_url, p.page_url);
            }
        }
    }

    // IMO first (the stable hull identity, far better covered on Wikidata),
    // MMSI as the fallback.
    let mut found = None;
    if let Some(imo) = q.imo {
        match wikidata_image(&state, &format!(r#"wdt:P458 "{imo}""#)).await {
            Ok(v) => found = v,
            Err(err) => return wikidata_unavailable(err),
        }
    }
    if found.is_none() {
        match wikidata_image(&state, &format!(r#"wdt:P587 "{mmsi}""#)).await {
            Ok(v) => found = v,
            Err(err) => return wikidata_unavailable(err),
        }
    }

    // P18 arrives as a Special:FilePath URL; ?width= makes Commons serve a
    // thumbnail, and swapping in File: yields the attribution page.
    let (image_url, page_url) = match found {
        Some(raw) => {
            let https = raw.replacen("http://", "https://", 1);
            (
                Some(format!("{https}?width=640")),
                Some(https.replacen("Special:FilePath/", "File:", 1)),
            )
        }
        None => (None, None),
    };

    {
        let conn = state.db.lock().await;
        if let Err(err) = db::photo_put(
            &conn,
            mmsi,
            q.imo,
            image_url.as_deref(),
            page_url.as_deref(),
        ) {
            tracing::warn!(%err, mmsi, "caching vessel photo failed");
        }
    }
    photo_response(image_url, page_url)
}

/// One Wikidata lookup: item matched by `predicate` (e.g. `wdt:P458 "123"`)
/// with a Commons image. `Ok(None)` = looked, nothing there.
async fn wikidata_image(state: &AppState, predicate: &str) -> Result<Option<String>, String> {
    let query =
        format!(r#"SELECT ?img WHERE {{ ?item {predicate} . ?item wdt:P18 ?img }} LIMIT 1"#);
    let resp = state
        .http
        .get("https://query.wikidata.org/sparql")
        .query(&[("format", "json"), ("query", query.as_str())])
        .timeout(std::time::Duration::from_secs(8))
        .send()
        .await
        .and_then(|r| r.error_for_status())
        .map_err(|e| e.to_string())?;
    let body: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    Ok(body["results"]["bindings"][0]["img"]["value"]
        .as_str()
        .map(str::to_string))
}

/// Wikidata being down must not cache a false "no photo": report and move on
/// without writing the negative result.
fn wikidata_unavailable(err: impl std::fmt::Display) -> Response {
    tracing::warn!(%err, "wikidata lookup failed");
    photo_response(None, None)
}

fn photo_response(image_url: Option<String>, page_url: Option<String>) -> Response {
    (
        [(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
        Json(serde_json::json!({
            "found": image_url.is_some(),
            "image_url": image_url,
            "page_url": page_url,
            "source": "wikimedia-commons",
        })),
    )
        .into_response()
}
