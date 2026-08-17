//! Snapshotter: maintains the live fleet state from `ais.decoded.>` and
//! serves pre-generated, gzip-compressed full-fleet snapshots over HTTP.
//!
//! The full-fleet query ("give me all ~150k vessels") deliberately does NOT
//! go over WebSocket: snapshots are generated once per interval and served
//! as cacheable static bytes. Tier decides freshness: contributors get the
//! newest snapshot, the free tier gets one at least 10 minutes old.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use flate2::write::GzEncoder;
use flate2::Compression;
use futures::StreamExt;
use openseafeed_feed::StreamMessage;
use openseafeed_keys::Validator;
use serde::Serialize;
use tokio::sync::RwLock;

const SNAPSHOT_RING: usize = 15;
const FREE_TIER_AGE: Duration = Duration::from_secs(600);
const VESSEL_TTL: Duration = Duration::from_secs(24 * 3600);

#[derive(Serialize, Clone, Default)]
struct Vessel {
    mmsi: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    lat: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lon: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sog: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cog: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hdg: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    nav: Option<u8>,
    #[serde(skip_serializing_if = "String::is_empty")]
    name: String,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    ship_type: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    imo: Option<u32>,
    #[serde(skip_serializing_if = "String::is_empty")]
    callsign: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    dest: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    draught: Option<f64>,
    /// Length / beam in metres, from the Dimension block.
    #[serde(skip_serializing_if = "Option::is_none")]
    len: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    beam: Option<u16>,
    /// Raw AIS ETA block, `{Month, Day, Hour, Minute}`.
    #[serde(skip_serializing_if = "Option::is_none")]
    eta: Option<serde_json::Value>,
    /// Unix ms of last update.
    ts: u64,
}

struct Snapshot {
    generated_at_ms: u64,
    gzipped: Vec<u8>,
    count: usize,
}

#[derive(Default)]
struct SharedState {
    vessels: HashMap<u32, Vessel>,
    snapshots: Vec<Arc<Snapshot>>, // newest last
}

struct App {
    state: RwLock<SharedState>,
    validator: Arc<Validator>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let nats_url = std::env::var("OSF_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
    let nats = async_nats::connect(&nats_url).await?;
    tracing::info!(nats_url, "snapshotter connected to nats");

    let app = Arc::new(App {
        state: RwLock::new(SharedState::default()),
        validator: Validator::from_env(),
    });

    tokio::spawn(consume(nats, app.clone()));
    // Rebuild the fleet from stored history so a restart (every deploy) does
    // not blank the map and 404 shared vessel links until ships re-transmit.
    tokio::spawn(warm_from_history(app.clone()));
    let interval: u64 = std::env::var("OSF_SNAPSHOT_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);
    tokio::spawn(snapshot_loop(app.clone(), Duration::from_secs(interval)));

    let http_addr = std::env::var("OSF_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8082".into());
    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/snapshot", get(get_snapshot))
        .route("/v1/vessels/{mmsi}", get(get_vessel))
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    tracing::info!(http_addr, "snapshotter listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

/// One ClickHouse HTTP query, NDJSON out. Same env contract as the archiver.
async fn ch_query(sql: String) -> anyhow::Result<String> {
    let url =
        std::env::var("OSF_CLICKHOUSE_URL").unwrap_or_else(|_| "http://clickhouse:8123".into());
    let mut req = reqwest::Client::new().post(&url).body(sql);
    if let Ok(u) = std::env::var("OSF_CLICKHOUSE_USER") {
        req = req.header("X-ClickHouse-User", u);
    }
    if let Ok(p) = std::env::var("OSF_CLICKHOUSE_PASSWORD") {
        req = req.header("X-ClickHouse-Key", p);
    }
    let resp = req
        .timeout(Duration::from_secs(60))
        .send()
        .await?
        .error_for_status()?;
    Ok(resp.text().await?)
}

#[derive(serde::Deserialize)]
struct WarmPos {
    mmsi: u32,
    lat: f64,
    lon: f64,
    sog: Option<f64>,
    cog: Option<f64>,
    hdg: Option<u16>,
    nav: Option<u8>,
    ts: u64,
}

#[derive(serde::Deserialize)]
struct WarmStatic {
    mmsi: u32,
    name: String,
    callsign: String,
    imo: u32,
    ship_type: u8,
    dest: String,
    draught: f64,
    a: u16,
    b: u16,
    c: u16,
    d: u16,
}

/// Rebuild fleet state from the archiver's ClickHouse history: last 24h of
/// positions plus the per-vessel statics. Live messages that arrived while
/// the queries ran win over the warm data.
async fn warm_from_history(app: Arc<App>) {
    let db = std::env::var("OSF_CLICKHOUSE_DB").unwrap_or_else(|_| "osf".into());

    let positions = ch_query(format!(
        "SELECT mmsi, argMax(lat, ts) AS lat, argMax(lon, ts) AS lon, \
                argMax(sog, ts) AS sog, argMax(cog, ts) AS cog, \
                argMax(heading, ts) AS hdg, argMax(nav_status, ts) AS nav, \
                toUnixTimestamp64Milli(max(ts)) AS ts \
         FROM {db}.positions WHERE ts > now() - INTERVAL 24 HOUR \
         GROUP BY mmsi FORMAT JSONEachRow \
         SETTINGS output_format_json_quote_64bit_integers = 0"
    ))
    .await;
    let statics = ch_query(format!(
        "SELECT mmsi, argMax(name, ts) AS name, argMax(call_sign, ts) AS callsign, \
                argMax(imo, ts) AS imo, argMax(ship_type, ts) AS ship_type, \
                argMax(destination, ts) AS dest, argMax(draught, ts) AS draught, \
                argMax(dim_a, ts) AS a, argMax(dim_b, ts) AS b, \
                argMax(dim_c, ts) AS c, argMax(dim_d, ts) AS d \
         FROM {db}.statics GROUP BY mmsi FORMAT JSONEachRow"
    ))
    .await;

    let (mut warmed_pos, mut warmed_stat) = (0usize, 0usize);
    match positions {
        Ok(text) => {
            let mut st = app.state.write().await;
            for line in text.lines() {
                let Ok(w) = serde_json::from_str::<WarmPos>(line) else {
                    continue;
                };
                if !(-90.0..=90.0).contains(&w.lat) || !(-180.0..=180.0).contains(&w.lon) {
                    continue;
                }
                let v = st.vessels.entry(w.mmsi).or_default();
                if v.ts >= w.ts {
                    continue; // live consume already has fresher data
                }
                v.mmsi = w.mmsi;
                v.lat = Some(w.lat);
                v.lon = Some(w.lon);
                v.sog = w.sog.filter(|s| *s < 102.3);
                v.cog = w.cog.filter(|c| *c < 360.0);
                v.hdg = w.hdg.filter(|h| *h < 511);
                v.nav = w.nav;
                v.ts = w.ts;
                warmed_pos += 1;
            }
        }
        Err(e) => tracing::warn!(error = %e, "position warm-up unavailable"),
    }
    match statics {
        Ok(text) => {
            let mut st = app.state.write().await;
            for line in text.lines() {
                let Ok(w) = serde_json::from_str::<WarmStatic>(line) else {
                    continue;
                };
                let Some(v) = st.vessels.get_mut(&w.mmsi) else {
                    continue; // statics only enrich ships with a position
                };
                if v.name.is_empty() && !w.name.is_empty() {
                    v.name = w.name;
                }
                if v.callsign.is_empty() {
                    v.callsign = w.callsign;
                }
                if v.dest.is_empty() {
                    v.dest = w.dest;
                }
                if v.ship_type.is_none() && w.ship_type > 0 {
                    v.ship_type = Some(w.ship_type);
                }
                if v.imo.is_none() && w.imo > 0 {
                    v.imo = Some(w.imo);
                }
                if v.draught.is_none() && w.draught > 0.0 {
                    v.draught = Some(w.draught);
                }
                if v.len.is_none() && w.a + w.b > 0 {
                    v.len = Some(w.a + w.b);
                    v.beam = Some(w.c + w.d);
                }
                warmed_stat += 1;
            }
        }
        Err(e) => tracing::warn!(error = %e, "statics warm-up unavailable"),
    }
    tracing::info!(warmed_pos, warmed_stat, "fleet warmed from history");
}

async fn consume(nats: async_nats::Client, app: Arc<App>) {
    let mut sub = match nats.subscribe("ais.decoded.>").await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, "subscribe failed");
            return;
        }
    };
    while let Some(msg) = sub.next().await {
        let Ok(sm) = serde_json::from_slice::<StreamMessage>(&msg.payload) else {
            continue;
        };
        let mut st = app.state.write().await;
        let v = st.vessels.entry(sm.metadata.mmsi).or_default();
        v.mmsi = sm.metadata.mmsi;
        v.ts = now_ms();
        if !sm.metadata.ship_name.is_empty() {
            v.name = sm.metadata.ship_name.clone();
        }
        if sm.metadata.ship_type.is_some() {
            v.ship_type = sm.metadata.ship_type;
        }
        if sm.metadata.imo.is_some() {
            v.imo = sm.metadata.imo;
        }
        // The packet payload is externally tagged: {"PositionReport": {...}}.
        if let Some(p) = sm.message.get(&sm.message_type) {
            update_from_packet(v, &sm.message_type, p);
        }
    }
}

fn update_from_packet(v: &mut Vessel, message_type: &str, p: &serde_json::Value) {
    let f = |k: &str| p.get(k).and_then(|x| x.as_f64());
    let u = |k: &str| p.get(k).and_then(|x| x.as_u64());
    match message_type {
        "PositionReport"
        | "StandardClassBPositionReport"
        | "ExtendedClassBPositionReport"
        | "LongRangeAisBroadcastMessage"
        | "StandardSearchAndRescueAircraftReport"
        | "AidsToNavigationReport"
        | "BaseStationReport" => {
            // Not-available sentinels stay out of the snapshot.
            if let (Some(lat), Some(lon)) = (f("Latitude"), f("Longitude")) {
                if (-90.0..=90.0).contains(&lat) && (-180.0..=180.0).contains(&lon) {
                    v.lat = Some(lat);
                    v.lon = Some(lon);
                }
            }
            if let Some(sog) = f("Sog") {
                if sog < 102.3 {
                    v.sog = Some(sog);
                }
            }
            if let Some(cog) = f("Cog") {
                if cog < 360.0 {
                    v.cog = Some(cog);
                }
            }
            if let Some(h) = u("TrueHeading") {
                if h < 511 {
                    v.hdg = Some(h as u16);
                }
            }
            if let Some(n) = u("NavigationalStatus") {
                v.nav = Some(n as u8);
            }
        }
        "ShipStaticData" | "StaticDataReport" => {
            if let Some(name) = p.get("Name").and_then(|x| x.as_str()) {
                if !name.is_empty() {
                    v.name = name.to_string();
                }
            }
            if let Some(t) = u("Type").or_else(|| u("ShipType")) {
                if t > 0 {
                    v.ship_type = Some(t as u8);
                }
            }
            if let Some(cs) = p.get("CallSign").and_then(|x| x.as_str()) {
                if !cs.is_empty() {
                    v.callsign = cs.to_string();
                }
            }
            if let Some(d) = p.get("Destination").and_then(|x| x.as_str()) {
                if !d.is_empty() {
                    v.dest = d.to_string();
                }
            }
            if let Some(dr) = f("MaximumStaticDraught") {
                if dr > 0.0 {
                    v.draught = Some(dr);
                }
            }
            if let Some(dim) = p.get("Dimension") {
                let side = |k: &str| dim.get(k).and_then(|x| x.as_u64()).unwrap_or(0) as u16;
                let (len, beam) = (side("A") + side("B"), side("C") + side("D"));
                if len > 0 {
                    v.len = Some(len);
                    v.beam = Some(beam);
                }
            }
            if let Some(eta) = p.get("Eta") {
                if eta.get("Month").and_then(|x| x.as_u64()).unwrap_or(0) > 0 {
                    v.eta = Some(eta.clone());
                }
            }
        }
        _ => {}
    }
}

async fn snapshot_loop(app: Arc<App>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        let now = now_ms();
        let mut st = app.state.write().await;
        st.vessels
            .retain(|_, v| now.saturating_sub(v.ts) < VESSEL_TTL.as_millis() as u64);

        #[derive(Serialize)]
        struct Body<'a> {
            generated_at: u64,
            count: usize,
            vessels: Vec<&'a Vessel>,
        }
        let vessels: Vec<&Vessel> = st.vessels.values().collect();
        let count = vessels.len();
        let body = Body {
            generated_at: now,
            count,
            vessels,
        };
        let json = match serde_json::to_vec(&body) {
            Ok(j) => j,
            Err(e) => {
                tracing::error!(error = %e, "snapshot serialize failed");
                continue;
            }
        };
        let mut enc = GzEncoder::new(Vec::new(), Compression::fast());
        let gzipped = enc
            .write_all(&json)
            .and_then(|_| enc.finish())
            .unwrap_or_default();
        tracing::info!(
            vessels = count,
            raw_bytes = json.len(),
            gz_bytes = gzipped.len(),
            "snapshot generated"
        );
        st.snapshots.push(Arc::new(Snapshot {
            generated_at_ms: now,
            gzipped,
            count,
        }));
        // Ring must span at least the free-tier age; 15 x 60s does.
        let len = st.snapshots.len();
        if len > SNAPSHOT_RING {
            st.snapshots.drain(..len - SNAPSHOT_RING);
        }
    }
}

#[derive(serde::Deserialize)]
struct KeyParam {
    key: Option<String>,
}

async fn authed_tier(
    app: &App,
    key: &Option<String>,
) -> Result<String, (StatusCode, &'static str)> {
    // No key = anonymous (the public live map): free tier. A key that IS
    // presented must still be valid.
    let Some(key) = key.as_deref() else {
        return Ok("free".to_string());
    };
    let info = app
        .validator
        .validate(key)
        .await
        .ok_or((StatusCode::FORBIDDEN, "invalid key"))?;
    Ok(info.tier)
}

async fn get_snapshot(State(app): State<Arc<App>>, Query(q): Query<KeyParam>) -> impl IntoResponse {
    let tier = match authed_tier(&app, &q.key).await {
        Ok(t) => t,
        Err(e) => return e.into_response(),
    };
    let st = app.state.read().await;
    let now = now_ms();
    let pick = if tier == "contributor" {
        st.snapshots.last().cloned()
    } else {
        // Newest snapshot that is at least FREE_TIER_AGE old; if the service
        // is younger than that, fall back to the oldest we have.
        st.snapshots
            .iter()
            .rev()
            .find(|s| now.saturating_sub(s.generated_at_ms) >= FREE_TIER_AGE.as_millis() as u64)
            .cloned()
            .or_else(|| st.snapshots.first().cloned())
    };
    let Some(snap) = pick else {
        return (StatusCode::SERVICE_UNAVAILABLE, "no snapshot yet").into_response();
    };
    (
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CONTENT_ENCODING, "gzip"),
            (header::CACHE_CONTROL, "public, max-age=60"),
        ],
        [
            ("x-osf-generated-at", snap.generated_at_ms.to_string()),
            ("x-osf-vessels", snap.count.to_string()),
        ],
        snap.gzipped.clone(),
    )
        .into_response()
}

async fn get_vessel(
    State(app): State<Arc<App>>,
    Path(mmsi): Path<u32>,
    Query(q): Query<KeyParam>,
) -> impl IntoResponse {
    if let Err(e) = authed_tier(&app, &q.key).await {
        return e.into_response();
    }
    let st = app.state.read().await;
    match st.vessels.get(&mmsi) {
        // CORS-open: the live map's detail panel calls this cross-origin
        // from the stream host.
        Some(v) => (
            [(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
            axum::Json(v.clone()),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            [(header::ACCESS_CONTROL_ALLOW_ORIGIN, "*")],
            "unknown mmsi",
        )
            .into_response(),
    }
}
