//! Finland upstream: Digitraffic marine AIS over MQTT-in-WebSocket.
//!
//! Digitraffic publishes *decoded* vessel data as JSON. We re-encode it to
//! standard AIVDM so it flows through the same validation, dedupe and
//! provenance path as every other feed.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Result};
use openseafeed_ais::{
    encode_position_report, encode_ship_static_data, Dimension, Eta, Header, PositionReport,
    ShipStaticData,
};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};
use serde::Deserialize;

use crate::backoff::Backoff;
use crate::queue::LineQueue;
use crate::stats::Stats;
use crate::upstream::ingest_line;

const BROKER_URL: &str = "wss://meri.digitraffic.fi:443/mqtt";
const TOPIC_LOCATION: &str = "vessels-v2/+/location";
const TOPIC_METADATA: &str = "vessels-v2/+/metadata";
const TOPIC_STATUS: &str = "vessels-v2/status";

/// vessels-v2 `location` message (subset we map to a type 1 position report).
#[derive(Debug, Deserialize)]
struct Location {
    #[serde(default)]
    time: i64,
    #[serde(default = "sog_na")]
    sog: f64,
    #[serde(default = "cog_na")]
    cog: f64,
    #[serde(rename = "navStat", default = "navstat_na")]
    nav_stat: u8,
    #[serde(default = "rot_na")]
    rot: i32,
    #[serde(rename = "posAcc", default)]
    pos_acc: bool,
    #[serde(default)]
    raim: bool,
    #[serde(default = "heading_na")]
    heading: u16,
    #[serde(default)]
    lon: f64,
    #[serde(default)]
    lat: f64,
}

fn sog_na() -> f64 {
    102.3
}
fn cog_na() -> f64 {
    360.0
}
fn navstat_na() -> u8 {
    15
}
fn heading_na() -> u16 {
    511
}
fn rot_na() -> i32 {
    -128
}

/// vessels-v2 `metadata` message (subset we map to a type 5 static report).
#[derive(Debug, Deserialize)]
struct Metadata {
    #[serde(default)]
    name: String,
    #[serde(default)]
    destination: String,
    /// Decimeters; divide by 10 for meters.
    #[serde(default)]
    draught: i64,
    /// Raw 20-bit AIS ETA field: month(4) day(5) hour(5) minute(6), MSB first.
    #[serde(default)]
    eta: u32,
    #[serde(rename = "posType", default)]
    pos_type: u8,
    #[serde(rename = "refA", default)]
    ref_a: u16,
    #[serde(rename = "refB", default)]
    ref_b: u16,
    #[serde(rename = "refC", default)]
    ref_c: u8,
    #[serde(rename = "refD", default)]
    ref_d: u8,
    #[serde(rename = "callSign", default)]
    call_sign: String,
    #[serde(default)]
    imo: u32,
    #[serde(rename = "type", default)]
    ship_type: u8,
}

/// Unpack Digitraffic's packed ETA into the AIS calendar fields. The value is
/// the raw 20-bit AIS field: month(4) | day(5) | hour(5) | minute(6), MSB
/// first, so month occupies the top four bits of the 20-bit word.
fn unpack_eta(raw: u32) -> Eta {
    Eta {
        month: ((raw >> 16) & 0xf) as u8,
        day: ((raw >> 11) & 0x1f) as u8,
        hour: ((raw >> 6) & 0x1f) as u8,
        minute: (raw & 0x3f) as u8,
    }
}

fn location_to_sentences(mmsi: u32, loc: &Location, seq: u8) -> Vec<String> {
    let report = PositionReport {
        header: Header {
            message_id: 1,
            repeat_indicator: 0,
            user_id: mmsi,
            valid: true,
        },
        navigational_status: loc.nav_stat,
        rate_of_turn: loc.rot.clamp(-128, 127) as i16,
        sog: loc.sog,
        position_accuracy: loc.pos_acc,
        longitude: loc.lon,
        latitude: loc.lat,
        cog: loc.cog,
        true_heading: loc.heading,
        timestamp: loc.time.rem_euclid(60) as u8,
        special_manoeuvre_indicator: 0,
        spare: 0,
        raim: loc.raim,
        communication_state: 0,
    };
    let (payload, fill) = encode_position_report(&report);
    openseafeed_nmea::to_sentences(&payload, fill, "A", seq)
}

fn metadata_to_sentences(mmsi: u32, md: &Metadata, seq: u8) -> Vec<String> {
    let data = ShipStaticData {
        header: Header {
            message_id: 5,
            repeat_indicator: 0,
            user_id: mmsi,
            valid: true,
        },
        ais_version: 0,
        imo_number: md.imo,
        call_sign: md.call_sign.clone(),
        name: md.name.clone(),
        ship_type: md.ship_type,
        dimension: Dimension {
            a: md.ref_a,
            b: md.ref_b,
            c: md.ref_c,
            d: md.ref_d,
        },
        fix_type: md.pos_type,
        eta: unpack_eta(md.eta),
        maximum_static_draught: md.draught as f64 / 10.0,
        destination: md.destination.clone(),
        dte: false,
        spare: false,
    };
    let (payload, fill) = encode_ship_static_data(&data);
    openseafeed_nmea::to_sentences(&payload, fill, "A", seq)
}

/// Run one MQTT connection lifecycle: connect, subscribe, and pump publishes
/// into the queue until the connection errors (then the caller reconnects).
pub async fn read(queue: &LineQueue, stats: &Stats, backoff: &mut Backoff) -> Result<()> {
    let mut opts = MqttOptions::new(client_id(), BROKER_URL, 443);
    opts.set_transport(Transport::wss_with_default_config());
    opts.set_keep_alive(Duration::from_secs(30));

    let (client, mut eventloop) = AsyncClient::new(opts, 128);
    client.subscribe(TOPIC_LOCATION, QoS::AtMostOnce).await?;
    client.subscribe(TOPIC_METADATA, QoS::AtMostOnce).await?;
    client.subscribe(TOPIC_STATUS, QoS::AtMostOnce).await?;

    let mut seq: u8 = 0;
    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                backoff.reset();
                tracing::info!("connected to Digitraffic MQTT");
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                handle_publish(&p.topic, p.payload.as_ref(), queue, stats, &mut seq);
            }
            Ok(_) => {}
            Err(e) => return Err(anyhow!("digitraffic mqtt: {e}")),
        }
    }
}

fn handle_publish(topic: &str, payload: &[u8], queue: &LineQueue, stats: &Stats, seq: &mut u8) {
    if topic == TOPIC_STATUS {
        tracing::debug!(status = %String::from_utf8_lossy(payload), "digitraffic status");
        return;
    }
    let segs: Vec<&str> = topic.split('/').collect();
    // vessels-v2 / <mmsi> / <kind>
    let (Some(mmsi), Some(kind)) = (
        segs.get(1).and_then(|m| m.parse::<u32>().ok()),
        segs.get(2).copied(),
    ) else {
        return;
    };

    let lines = match kind {
        "location" => match serde_json::from_slice::<Location>(payload) {
            Ok(loc) => location_to_sentences(mmsi, &loc, *seq),
            Err(_) => {
                Stats::incr(&stats.invalid);
                return;
            }
        },
        "metadata" => match serde_json::from_slice::<Metadata>(payload) {
            Ok(md) => metadata_to_sentences(mmsi, &md, *seq),
            Err(_) => {
                Stats::incr(&stats.invalid);
                return;
            }
        },
        _ => return,
    };
    *seq = seq.wrapping_add(1);
    for line in &lines {
        ingest_line(line, queue, stats);
    }
}

/// Per Digitraffic ToS, identify the client. MQTT has no UA header, so a
/// recognizable client id serves the purpose; the random suffix avoids
/// collisions between concurrent workers.
fn client_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("openseafeed-worker-{:x}-{:x}", std::process::id(), nanos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openseafeed_ais::{decode, Packet as AisPacket};

    fn decode_line(line: &str) -> openseafeed_ais::Message {
        let s = openseafeed_nmea::parse(line).unwrap();
        decode(&s.payload, s.fill_bits).unwrap()
    }

    #[test]
    fn location_json_round_trips_to_position_report() {
        let json = br#"{"time":1690000030,"sog":12.3,"cog":87.5,"navStat":0,
            "rot":0,"posAcc":true,"raim":false,"heading":88,
            "lon":24.9384,"lat":60.1699}"#;
        let loc: Location = serde_json::from_slice(json).unwrap();
        let lines = location_to_sentences(230_123_456, &loc, 0);
        assert_eq!(lines.len(), 1);
        let m = decode_line(&lines[0]);
        assert_eq!(m.mmsi, 230_123_456);
        let AisPacket::PositionReport(p) = m.packet else {
            panic!("expected position report");
        };
        assert!((p.sog - 12.3).abs() < 0.05);
        assert!((p.longitude - 24.9384).abs() < 1e-4);
        assert!((p.latitude - 60.1699).abs() < 1e-4);
        assert!((p.cog - 87.5).abs() < 0.05);
        assert_eq!(p.true_heading, 88);
        assert_eq!(p.navigational_status, 0);
    }

    #[test]
    fn metadata_json_round_trips_to_ship_static_data() {
        // eta packed for month=7 day=25 hour=6 minute=30.
        let eta = (7u32 << 16) | (25 << 11) | (6 << 6) | 30;
        let json = format!(
            r#"{{"name":"SILJA SERENADE","destination":"HELSINKI","draught":71,
               "eta":{eta},"posType":1,"refA":100,"refB":103,"refC":15,"refD":17,
               "callSign":"OJABC","imo":9074729,"type":60}}"#
        );
        let md: Metadata = serde_json::from_slice(json.as_bytes()).unwrap();
        let lines = metadata_to_sentences(230_987_000, &md, 3);
        assert_eq!(lines.len(), 2, "type 5 payload spans two sentences");
        // Reassemble both fragments, then decode the full payload.
        let a = openseafeed_nmea::parse(&lines[0]).unwrap();
        let b = openseafeed_nmea::parse(&lines[1]).unwrap();
        let full = format!("{}{}", a.payload, b.payload);
        let msg = decode(&full, b.fill_bits).unwrap();
        assert_eq!(msg.name.as_deref(), Some("SILJA SERENADE"));
        let AisPacket::ShipStaticData(s) = msg.packet else {
            panic!("expected ship static data");
        };
        assert_eq!(s.call_sign, "OJABC");
        assert_eq!(s.destination, "HELSINKI");
        assert_eq!(s.imo_number, 9_074_729);
        assert_eq!(s.ship_type, 60);
        assert_eq!(s.dimension, Dimension { a: 100, b: 103, c: 15, d: 17 });
        assert!((s.maximum_static_draught - 7.1).abs() < 0.05);
        assert_eq!(s.eta, Eta { month: 7, day: 25, hour: 6, minute: 30 });
    }

    #[test]
    fn eta_unpacks_msb_first() {
        let packed = (12u32 << 16) | (31 << 11) | (23 << 6) | 59;
        assert_eq!(
            unpack_eta(packed),
            Eta {
                month: 12,
                day: 31,
                hour: 23,
                minute: 59
            }
        );
        assert_eq!(
            unpack_eta(0),
            Eta {
                month: 0,
                day: 0,
                hour: 0,
                minute: 0
            }
        );
    }

    #[test]
    fn missing_fields_fall_back_to_not_available() {
        let loc: Location = serde_json::from_slice(br#"{"lat":60.0,"lon":25.0}"#).unwrap();
        assert_eq!(loc.heading, 511);
        assert_eq!(loc.nav_stat, 15);
        assert_eq!(loc.rot, -128);
        assert!((loc.sog - 102.3).abs() < 1e-9);
    }
}
