//! Warm start: the fleet this proxy already holds, replayed to a client the
//! moment it subscribes.
//!
//! OpenSeaFeed keeps one uplink per upstream feed and fans it out to every
//! consumer; the snapshotter is the cache of that uplink's current state.
//! Without a seed a fresh socket sees only what happens to transmit next, so
//! a world-wide subscriber needs the better part of ten minutes before its
//! map looks like the world — the reason our own live map looked emptier
//! than clients holding their own upstream connection. The seed turns "wait
//! for the next broadcast" into "here is the fleet, now the deltas".
//!
//! The frames are ordinary aisstream.io v0 `PositionReport`s carrying the
//! vessel's last known position and its `time_utc`, so a client needs no new
//! parsing: an old timestamp is the only thing that marks them as replayed.

use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use openseafeed_ais::{Header, PositionReport};
use openseafeed_feed::{MetaData, StreamMessage};
use serde::Deserialize;
use tokio::sync::RwLock;

/// The message type every seeded frame is sent as.
pub const SEED_MESSAGE_TYPE: &str = "PositionReport";

/// One vessel, pre-rendered into the frame a client receives. Built once per
/// refresh so a connecting client costs a filter pass and a send, not a
/// serialization of the whole fleet.
pub struct SeedVessel {
    pub mmsi: u32,
    pub lat: f64,
    pub lon: f64,
    pub frame: Arc<str>,
}

/// A vessel as the snapshotter serializes it. Every optional field is
/// omitted when unknown, so the whole struct defaults.
#[derive(Deserialize, Default)]
#[serde(default)]
struct SnapVessel {
    mmsi: u32,
    lat: Option<f64>,
    lon: Option<f64>,
    sog: Option<f64>,
    cog: Option<f64>,
    hdg: Option<u16>,
    nav: Option<u8>,
    name: String,
    #[serde(rename = "type")]
    ship_type: Option<u8>,
    imo: Option<u32>,
    /// Unix ms of the vessel's last update.
    ts: u64,
}

#[derive(Deserialize, Default)]
#[serde(default)]
struct SnapBody {
    generated_at: u64,
    vessels: Vec<SnapVessel>,
}

/// Render one snapshot vessel as an aisstream.io v0 stream frame. `None` for
/// a vessel we have no position for — there is nothing to draw.
fn frame_for(v: &SnapVessel) -> Option<String> {
    let (lat, lon) = (v.lat?, v.lon?);
    // AIS "not available" sentinels stand in for anything the vessel has not
    // told us, exactly as they would in a live message.
    let report = PositionReport {
        header: Header {
            message_id: 1,
            repeat_indicator: 0,
            user_id: v.mmsi,
            valid: true,
        },
        navigational_status: v.nav.unwrap_or(15),
        rate_of_turn: -128,
        sog: v.sog.unwrap_or(102.3),
        position_accuracy: false,
        longitude: lon,
        latitude: lat,
        cog: v.cog.unwrap_or(360.0),
        true_heading: v.hdg.unwrap_or(511),
        timestamp: 60,
        special_manoeuvre_indicator: 0,
        spare: 0,
        raim: false,
        communication_state: 0,
    };
    let msg = StreamMessage {
        message: serde_json::json!({ SEED_MESSAGE_TYPE: report }),
        message_type: SEED_MESSAGE_TYPE.to_string(),
        metadata: MetaData {
            mmsi: v.mmsi,
            mmsi_string: v.mmsi,
            ship_name: v.name.clone(),
            ship_type: v.ship_type,
            imo: v.imo,
            latitude: lat,
            longitude: lon,
            time_utc: Utc
                .timestamp_millis_opt(v.ts as i64)
                .single()
                .unwrap_or_else(Utc::now)
                .to_rfc3339(),
        },
    };
    serde_json::to_string(&msg).ok()
}

/// The rendered fleet, refreshed from the snapshotter in the background.
pub struct SeedCache {
    vessels: RwLock<Arc<Vec<SeedVessel>>>,
    url: String,
    token: String,
    client: reqwest::Client,
}

impl SeedCache {
    /// `OSF_SNAPSHOT_URL` points at the snapshotter's internal endpoint.
    /// Setting it empty disables seeding altogether.
    pub fn from_env() -> Arc<Self> {
        let url = std::env::var("OSF_SNAPSHOT_URL")
            .unwrap_or_else(|_| "http://snapshotter:8082/v1/internal/snapshot".into());
        let token =
            std::env::var("OSF_INTERNAL_TOKEN").unwrap_or_else(|_| "dev-internal-token".into());
        Arc::new(Self {
            vessels: RwLock::new(Arc::new(Vec::new())),
            url,
            token,
            client: reqwest::Client::new(),
        })
    }

    pub fn enabled(&self) -> bool {
        !self.url.is_empty()
    }

    pub async fn current(&self) -> Arc<Vec<SeedVessel>> {
        self.vessels.read().await.clone()
    }

    /// Refresh forever. A failed fetch keeps the previous fleet: a stale seed
    /// beats an empty map, and live messages correct it within minutes.
    pub async fn refresh_loop(self: Arc<Self>, every: Duration) {
        if !self.enabled() {
            tracing::info!("OSF_SNAPSHOT_URL empty: serving no initial state");
            return;
        }
        loop {
            match self.fetch().await {
                Ok(vessels) => {
                    tracing::info!(vessels = vessels.len(), "seed fleet refreshed");
                    *self.vessels.write().await = Arc::new(vessels);
                }
                Err(e) => tracing::warn!(error = %e, url = %self.url, "seed refresh failed"),
            }
            tokio::time::sleep(every).await;
        }
    }

    async fn fetch(&self) -> anyhow::Result<Vec<SeedVessel>> {
        let body: SnapBody = self
            .client
            .get(&self.url)
            .header("X-Internal-Token", &self.token)
            .timeout(Duration::from_secs(30))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        tracing::debug!(generated_at = body.generated_at, "seed snapshot fetched");
        Ok(body
            .vessels
            .iter()
            .filter_map(|v| {
                Some(SeedVessel {
                    mmsi: v.mmsi,
                    lat: v.lat?,
                    lon: v.lon?,
                    frame: Arc::from(frame_for(v)?.as_str()),
                })
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_an_aisstream_frame_a_client_can_parse() {
        let v = SnapVessel {
            mmsi: 257_123_000,
            lat: Some(60.1699),
            lon: Some(24.9384),
            sog: Some(10.5),
            cog: Some(123.4),
            hdg: Some(124),
            nav: Some(0),
            name: "TEST VESSEL".into(),
            ship_type: Some(70),
            imo: Some(9_074_729),
            ts: 1_753_358_400_000,
        };
        let json: serde_json::Value = serde_json::from_str(&frame_for(&v).unwrap()).unwrap();
        assert_eq!(json["MessageType"], "PositionReport");
        assert_eq!(json["MetaData"]["MMSI"], 257_123_000);
        assert_eq!(json["MetaData"]["ShipName"], "TEST VESSEL");
        assert_eq!(json["MetaData"]["ShipType"], 70);
        // The vessel's own last-heard time, not now: clients age it correctly.
        assert!(json["MetaData"]["time_utc"]
            .as_str()
            .unwrap()
            .starts_with("2025-07-24"));
        let report = &json["Message"]["PositionReport"];
        assert_eq!(report["UserID"], 257_123_000);
        assert_eq!(report["Sog"], 10.5);
        assert_eq!(report["TrueHeading"], 124);
    }

    #[test]
    fn unknown_fields_become_ais_not_available_sentinels() {
        let v = SnapVessel {
            mmsi: 1,
            lat: Some(0.0),
            lon: Some(1.0),
            ..Default::default()
        };
        let json: serde_json::Value = serde_json::from_str(&frame_for(&v).unwrap()).unwrap();
        let report = &json["Message"]["PositionReport"];
        assert_eq!(report["Sog"], 102.3);
        assert_eq!(report["Cog"], 360.0);
        assert_eq!(report["TrueHeading"], 511);
        assert_eq!(report["NavigationalStatus"], 15);
    }

    #[test]
    fn a_vessel_without_a_position_is_skipped() {
        let v = SnapVessel {
            mmsi: 1,
            name: "NO FIX".into(),
            ..Default::default()
        };
        assert!(frame_for(&v).is_none());
    }
}
