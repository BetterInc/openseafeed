//! History query API. Reads go through the same table whether rows sit on
//! the hot local volume or the cold S3 tier — cold ranges are just slower.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use openseafeed_keys::Validator;
use serde::Deserialize;

use crate::ch::ClickHouse;

const DEFAULT_LIMIT: u32 = 5_000;
const MAX_LIMIT: u32 = 50_000;

pub struct HistoryState {
    pub ch: Arc<ClickHouse>,
    pub validator: Arc<Validator>,
}

pub fn router(state: Arc<HistoryState>) -> Router {
    Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/history/{mmsi}", get(get_history))
        .with_state(state)
}

#[derive(Deserialize)]
struct Params {
    key: Option<String>,
    /// RFC 3339; default: `to` minus 24 hours.
    from: Option<DateTime<Utc>>,
    /// RFC 3339; default: now.
    to: Option<DateTime<Utc>>,
    limit: Option<u32>,
}

async fn get_history(
    State(state): State<Arc<HistoryState>>,
    Path(mmsi): Path<u32>,
    Query(p): Query<Params>,
) -> impl IntoResponse {
    let Some(key) = p.key.as_deref() else {
        return (StatusCode::UNAUTHORIZED, "missing ?key=").into_response();
    };
    if state.validator.validate(key).await.is_none() {
        return (StatusCode::FORBIDDEN, "invalid key").into_response();
    }

    let to = p.to.unwrap_or_else(Utc::now);
    let from = p.from.unwrap_or(to - ChronoDuration::hours(24));
    let limit = p.limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    // All interpolated values are numerics or chrono-formatted timestamps —
    // no free-text reaches the SQL.
    let sql = format!(
        "SELECT ts, msg_type, lat, lon, sog, cog, heading, nav_status \
         FROM {db}.positions \
         WHERE mmsi = {mmsi} \
           AND ts >= toDateTime64('{from}', 3) \
           AND ts <= toDateTime64('{to}', 3) \
         ORDER BY ts \
         LIMIT {limit} \
         FORMAT JSONEachRow",
        db = state.ch.db,
        from = from.format("%Y-%m-%d %H:%M:%S%.3f"),
        to = to.format("%Y-%m-%d %H:%M:%S%.3f"),
    );

    match state.ch.exec(&sql, None).await {
        Ok(text) => {
            let points: Vec<serde_json::Value> = text
                .lines()
                .filter_map(|l| serde_json::from_str(l).ok())
                .collect();
            Json(serde_json::json!({
                "mmsi": mmsi,
                "from": from.to_rfc3339(),
                "to": to.to_rfc3339(),
                "count": points.len(),
                "points": points,
            }))
            .into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "history query failed");
            (StatusCode::BAD_GATEWAY, "history query failed").into_response()
        }
    }
}
