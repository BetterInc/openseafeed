//! Pipeline: consumes raw envelopes from `ais.raw.>`, decodes, dedupes,
//! enriches, and publishes aisstream.io-compatible JSON on
//! `ais.decoded.<c>.<c>.<c>.<c>` subjects (one token per geohash char).
//!
//! Horizontally scalable via the NATS queue group; note that dedupe state is
//! per-instance, so scaling out can let a small fraction of cross-station
//! duplicates through — acceptable for the live stream, and the snapshotter
//! keys by MMSI anyway.

mod dedupe;

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::{TimeZone, Utc};
use futures::StreamExt;
use openseafeed_feed::{headers, subjects, MetaData, RawEnvelope, StreamMessage};

const GEOHASH_PRECISION: usize = 4;
/// Bound both enrichment caches; ~400k active MMSIs globally.
const CACHE_CAP: usize = 1_000_000;

/// JetStream KV bucket persisting MMSI -> name across restarts. Without it
/// every deploy wiped the cache and the map went "unnamed" until each ship
/// re-broadcast its statics (type 5 / 24A, a ~6-minute cycle per vessel).
const NAMES_BUCKET: &str = "vessel-names";

#[derive(Default)]
struct Stats {
    received: u64,
    decoded: u64,
    duplicates: u64,
    decode_errors: u64,
    published: u64,
}

struct Enrichment {
    /// MMSI -> vessel name, from static messages.
    names: HashMap<u32, String>,
    /// MMSI -> last known geohash cell + position, for routing messages that
    /// carry no position (static data) to the cell where subscribers of that
    /// area will see them.
    last_pos: HashMap<u32, (String, f64, f64)>,
}

impl Enrichment {
    fn trim(&mut self) {
        // Blunt but effective bound; a real LRU is not worth it at this rate.
        if self.names.len() > CACHE_CAP {
            self.names.clear();
        }
        if self.last_pos.len() > CACHE_CAP {
            self.last_pos.clear();
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let nats_url = std::env::var("OSF_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
    let client = async_nats::connect(&nats_url).await?;
    tracing::info!(nats_url, "pipeline connected");

    let mut sub = client
        .queue_subscribe(subjects::RAW_ALL, "pipeline".into())
        .await?;

    let js = async_nats::jetstream::new(client.clone());
    let names_kv = match js.get_key_value(NAMES_BUCKET).await {
        Ok(kv) => kv,
        Err(_) => {
            js.create_key_value(async_nats::jetstream::kv::Config {
                bucket: NAMES_BUCKET.to_string(),
                description: "MMSI -> last seen vessel name".to_string(),
                history: 1,
                ..Default::default()
            })
            .await?
        }
    };

    let mut window = dedupe::Window::new(Duration::from_secs(10));
    let mut enrich = Enrichment {
        names: HashMap::new(),
        last_pos: HashMap::new(),
    };
    warm_names(&names_kv, &mut enrich.names).await;
    tracing::info!(names = enrich.names.len(), "name cache warmed from KV");
    let mut stats = Stats::default();
    let mut last_report = Instant::now();

    loop {
        let msg = tokio::select! {
            m = sub.next() => match m { Some(m) => m, None => break },
            _ = tokio::signal::ctrl_c() => break,
        };
        stats.received += 1;

        if let Err(e) = handle(
            &client,
            &names_kv,
            &msg.payload,
            &mut window,
            &mut enrich,
            &mut stats,
        )
        .await
        {
            stats.decode_errors += 1;
            tracing::debug!(error = %e, "message dropped");
        }

        if last_report.elapsed() > Duration::from_secs(30) {
            tracing::info!(
                received = stats.received,
                decoded = stats.decoded,
                duplicates = stats.duplicates,
                decode_errors = stats.decode_errors,
                published = stats.published,
                vessels_tracked = enrich.last_pos.len(),
                "pipeline stats"
            );
            enrich.trim();
            last_report = Instant::now();
        }
    }
    tracing::info!("pipeline shutting down");
    Ok(())
}

/// Load every current entry from the names bucket into the in-memory cache.
/// Entries stream oldest-first and `delta` is the distance to the newest, so
/// 0 means caught up. An empty bucket yields nothing, hence the timeout.
async fn warm_names(kv: &async_nats::jetstream::kv::Store, names: &mut HashMap<u32, String>) {
    let mut watch = match kv.watch_all_from_revision(1).await {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(error = %e, "name cache warm-up unavailable");
            return;
        }
    };
    let load = async {
        while let Some(entry) = watch.next().await {
            let Ok(entry) = entry else { break };
            if entry.operation == async_nats::jetstream::kv::Operation::Put {
                if let (Ok(mmsi), Ok(name)) =
                    (entry.key.parse::<u32>(), std::str::from_utf8(&entry.value))
                {
                    names.insert(mmsi, name.to_string());
                }
            }
            if entry.delta == 0 {
                break;
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(10), load)
        .await
        .is_err()
    {
        tracing::warn!("name cache warm-up timed out; continuing with what loaded");
    }
}

async fn handle(
    client: &async_nats::Client,
    names_kv: &async_nats::jetstream::kv::Store,
    payload: &[u8],
    window: &mut dedupe::Window,
    enrich: &mut Enrichment,
    stats: &mut Stats,
) -> anyhow::Result<()> {
    let raw: RawEnvelope = serde_json::from_slice(payload)?;

    if window.seen(&raw.payload, Instant::now()) {
        stats.duplicates += 1;
        return Ok(());
    }

    let msg = openseafeed_ais::decode(&raw.payload, raw.fill_bits)?;
    if msg.mmsi == 0 {
        anyhow::bail!("mmsi 0");
    }
    stats.decoded += 1;

    // Update enrichment caches; persist new/changed names so they survive
    // restarts. A lost write self-heals on the vessel's next static broadcast.
    if let Some(name) = &msg.name {
        if enrich.names.get(&msg.mmsi) != Some(name) {
            enrich.names.insert(msg.mmsi, name.clone());
            if let Err(e) = names_kv
                .put(msg.mmsi.to_string(), name.clone().into_bytes().into())
                .await
            {
                tracing::debug!(error = %e, mmsi = msg.mmsi, "name persist failed");
            }
        }
    }
    let cell_and_pos = match msg.position {
        Some((lat, lon)) => {
            let cell = openseafeed_geo::encode(lat, lon, GEOHASH_PRECISION);
            enrich.last_pos.insert(msg.mmsi, (cell.clone(), lat, lon));
            Some((cell, lat, lon))
        }
        // No position on this message: route to the vessel's last known cell.
        None => enrich.last_pos.get(&msg.mmsi).cloned(),
    };

    let time_utc = Utc
        .timestamp_millis_opt(raw.received_at_ms as i64)
        .single()
        .unwrap_or_else(Utc::now)
        .to_rfc3339();
    let (meta_lat, meta_lon) = cell_and_pos
        .as_ref()
        .map(|(_, lat, lon)| (*lat, *lon))
        .unwrap_or((0.0, 0.0));

    let out = StreamMessage {
        message: serde_json::to_value(&msg.packet)?,
        message_type: msg.type_name().to_string(),
        metadata: MetaData {
            mmsi: msg.mmsi,
            mmsi_string: msg.mmsi,
            ship_name: enrich.names.get(&msg.mmsi).cloned().unwrap_or_default(),
            latitude: meta_lat,
            longitude: meta_lon,
            time_utc,
        },
    };

    let subject = match &cell_and_pos {
        Some((cell, _, _)) => subjects::decoded(cell),
        None => subjects::NO_POSITION.to_string(),
    };

    let mut hdrs = async_nats::HeaderMap::new();
    hdrs.insert(headers::MMSI, msg.mmsi.to_string().as_str());
    hdrs.insert(headers::MSG_TYPE, msg.type_name());
    hdrs.insert(headers::STATION, raw.station_id.as_str());
    if let Some((_, lat, lon)) = &cell_and_pos {
        hdrs.insert(headers::LAT, format!("{lat:.6}").as_str());
        hdrs.insert(headers::LON, format!("{lon:.6}").as_str());
    }

    client
        .publish_with_headers(subject, hdrs, serde_json::to_vec(&out)?.into())
        .await?;
    stats.published += 1;
    Ok(())
}
