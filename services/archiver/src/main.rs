//! Archiver: consumes `ais.decoded.>` and batch-inserts vessel history into
//! ClickHouse (positions + latest statics). Inserts go over the HTTP
//! interface as JSONEachRow; batches flush on size or age.

mod schema;

use std::time::Duration;

use chrono::DateTime;
use futures::StreamExt;
use openseafeed_feed::{headers, StreamMessage};
use serde::Serialize;

const FLUSH_ROWS: usize = 5_000;
const FLUSH_AGE: Duration = Duration::from_secs(5);
/// Upper bound on rows buffered while ClickHouse is unreachable.
const MAX_BUFFER: usize = 200_000;

#[derive(Serialize, Clone)]
struct PositionRow {
    ts: String,
    mmsi: u32,
    msg_type: String,
    lat: f64,
    lon: f64,
    sog: Option<f32>,
    cog: Option<f32>,
    heading: Option<u16>,
    nav_status: Option<u8>,
    station: String,
}

#[derive(Serialize, Clone)]
struct StaticRow {
    ts: String,
    mmsi: u32,
    name: String,
    call_sign: String,
    imo: u32,
    ship_type: u8,
    destination: String,
    draught: f32,
    dim_a: u16,
    dim_b: u16,
    dim_c: u8,
    dim_d: u8,
}

struct ClickHouse {
    http: reqwest::Client,
    url: String,
    db: String,
    user: Option<String>,
    password: Option<String>,
}

impl ClickHouse {
    fn from_env() -> Self {
        Self {
            http: reqwest::Client::new(),
            url: std::env::var("OSF_CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://localhost:8123".into()),
            db: std::env::var("OSF_CLICKHOUSE_DB").unwrap_or_else(|_| "osf".into()),
            user: std::env::var("OSF_CLICKHOUSE_USER").ok(),
            password: std::env::var("OSF_CLICKHOUSE_PASSWORD").ok(),
        }
    }

    async fn exec(&self, sql: &str, body: Option<Vec<u8>>) -> anyhow::Result<()> {
        let mut req = self.http.post(&self.url);
        if let Some(u) = &self.user {
            req = req.header("X-ClickHouse-User", u);
        }
        if let Some(p) = &self.password {
            req = req.header("X-ClickHouse-Key", p);
        }
        let req = match body {
            // SQL goes in the query string, data in the body.
            Some(data) => req.query(&[("query", sql)]).body(data),
            None => req.body(sql.to_string()),
        };
        let resp = req.timeout(Duration::from_secs(30)).send().await?;
        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            anyhow::bail!("clickhouse {status}: {text}");
        }
        Ok(())
    }

    async fn migrate(&self) -> anyhow::Result<()> {
        let hot_days: u32 = env_num("OSF_HOT_DAYS", 14);
        let retain_days: u32 = env_num("OSF_RETAIN_DAYS", 365);
        let tiered = std::env::var("OSF_CLICKHOUSE_TIERED").map(|v| v == "1").unwrap_or(false);
        for sql in schema::migrations(&self.db, hot_days, retain_days, tiered) {
            self.exec(&sql, None).await?;
        }
        tracing::info!(db = self.db, hot_days, retain_days, tiered, "schema ready");
        Ok(())
    }

    async fn insert<T: Serialize>(&self, table: &str, rows: &[T]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(rows.len() * 128);
        for r in rows {
            serde_json::to_writer(&mut body, r)?;
            body.push(b'\n');
        }
        let sql = format!("INSERT INTO {}.{} FORMAT JSONEachRow", self.db, table);
        self.exec(&sql, Some(body)).await
    }
}

fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let ch = ClickHouse::from_env();
    // Retry migrations until ClickHouse is up; the data plane must not
    // crash-loop just because history storage lags behind.
    loop {
        match ch.migrate().await {
            Ok(()) => break,
            Err(e) => {
                tracing::warn!(error = %e, "clickhouse not ready, retrying in 5s");
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        }
    }

    let nats_url =
        std::env::var("OSF_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
    let nats = async_nats::connect(&nats_url).await?;
    let mut sub = nats
        .queue_subscribe("ais.decoded.>", "archiver".into())
        .await?;
    tracing::info!(nats_url, "archiver consuming");

    // Health endpoint.
    let http_addr = std::env::var("OSF_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8084".into());
    tokio::spawn(async move {
        let router = axum::Router::new().route("/healthz", axum::routing::get(|| async { "ok" }));
        if let Ok(l) = tokio::net::TcpListener::bind(&http_addr).await {
            let _ = axum::serve(l, router).await;
        }
    });

    let mut positions: Vec<PositionRow> = Vec::new();
    let mut statics: Vec<StaticRow> = Vec::new();
    let mut flush_tick = tokio::time::interval(FLUSH_AGE);
    let mut written: u64 = 0;
    let mut dropped: u64 = 0;

    loop {
        tokio::select! {
            msg = sub.next() => {
                let Some(msg) = msg else { break };
                let hdrs = msg.headers.clone().unwrap_or_default();
                let station = hdrs
                    .get(headers::STATION)
                    .map(|v| v.to_string())
                    .unwrap_or_default();
                let Ok(sm) = serde_json::from_slice::<StreamMessage>(&msg.payload) else {
                    continue;
                };
                collect(&sm, &station, &mut positions, &mut statics);
                if positions.len() + statics.len() >= FLUSH_ROWS {
                    flush(&ch, &mut positions, &mut statics, &mut written, &mut dropped).await;
                }
            }
            _ = flush_tick.tick() => {
                flush(&ch, &mut positions, &mut statics, &mut written, &mut dropped).await;
                if written > 0 {
                    tracing::info!(written, dropped, "archiver stats");
                }
            }
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    flush(&ch, &mut positions, &mut statics, &mut written, &mut dropped).await;
    tracing::info!(written, dropped, "archiver shutting down");
    Ok(())
}

fn collect(
    sm: &StreamMessage,
    station: &str,
    positions: &mut Vec<PositionRow>,
    statics: &mut Vec<StaticRow>,
) {
    let ts = DateTime::parse_from_rfc3339(&sm.metadata.time_utc)
        .map(|t| t.format("%Y-%m-%d %H:%M:%S%.3f").to_string())
        .unwrap_or_else(|_| chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string());
    let p = sm.message.get(&sm.message_type);

    let f = |k: &str| p.and_then(|v| v.get(k)).and_then(|x| x.as_f64());
    let u = |k: &str| p.and_then(|v| v.get(k)).and_then(|x| x.as_u64());
    let s = |k: &str| {
        p.and_then(|v| v.get(k))
            .and_then(|x| x.as_str())
            .unwrap_or_default()
            .to_string()
    };

    match sm.message_type.as_str() {
        "PositionReport"
        | "StandardClassBPositionReport"
        | "ExtendedClassBPositionReport"
        | "LongRangeAisBroadcastMessage"
        | "StandardSearchAndRescueAircraftReport"
        | "AidsToNavigationReport"
        | "BaseStationReport" => {
            let (Some(lat), Some(lon)) = (f("Latitude"), f("Longitude")) else {
                return;
            };
            if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) {
                return;
            }
            positions.push(PositionRow {
                ts,
                mmsi: sm.metadata.mmsi,
                msg_type: sm.message_type.clone(),
                lat,
                lon,
                sog: f("Sog").filter(|v| *v < 102.3).map(|v| v as f32),
                cog: f("Cog").filter(|v| *v < 360.0).map(|v| v as f32),
                heading: u("TrueHeading").filter(|v| *v < 511).map(|v| v as u16),
                nav_status: u("NavigationalStatus").map(|v| v as u8),
                station: station.to_string(),
            });
        }
        "ShipStaticData" | "StaticDataReport" => {
            let dim = p.and_then(|v| v.get("Dimension"));
            let d = |k: &str| {
                dim.and_then(|v| v.get(k))
                    .and_then(|x| x.as_u64())
                    .unwrap_or(0)
            };
            statics.push(StaticRow {
                ts,
                mmsi: sm.metadata.mmsi,
                name: s("Name"),
                call_sign: s("CallSign"),
                imo: u("ImoNumber").unwrap_or(0) as u32,
                ship_type: u("Type").or_else(|| u("ShipType")).unwrap_or(0) as u8,
                destination: s("Destination"),
                draught: f("MaximumStaticDraught").unwrap_or(0.0) as f32,
                dim_a: d("A") as u16,
                dim_b: d("B") as u16,
                dim_c: d("C") as u8,
                dim_d: d("D") as u8,
            });
        }
        _ => {}
    }
}

async fn flush(
    ch: &ClickHouse,
    positions: &mut Vec<PositionRow>,
    statics: &mut Vec<StaticRow>,
    written: &mut u64,
    dropped: &mut u64,
) {
    for _ in 0..2 {
        let pos = ch.insert("positions", positions).await;
        let stat = ch.insert("statics", statics).await;
        match (pos, stat) {
            (Ok(()), Ok(())) => {
                *written += (positions.len() + statics.len()) as u64;
                positions.clear();
                statics.clear();
                return;
            }
            (p, s) => {
                if let Err(e) = p.and(s) {
                    tracing::warn!(error = %e, "insert failed, will retry");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    }
    // Keep buffering across flush failures, but bounded.
    let total = positions.len() + statics.len();
    if total > MAX_BUFFER {
        let excess = total - MAX_BUFFER;
        let cut = excess.min(positions.len());
        positions.drain(..cut);
        let rest = excess - cut;
        statics.drain(..rest.min(statics.len()));
        *dropped += excess as u64;
        tracing::error!(excess, "buffer cap hit, dropped oldest rows");
    }
}
