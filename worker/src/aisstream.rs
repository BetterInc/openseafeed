//! aisstream upstream: consume aisstream.io's own v0 stream.
//!
//! aisstream.io's wire format is the same StreamMessage shape this project
//! emits, so we deserialize the inner packet straight into `openseafeed_ais`
//! structs and re-encode to AIVDM, keeping one ingest format across sources.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use openseafeed_ais::{
    encode_position_report, encode_ship_static_data, encode_standard_class_b, PositionReport,
    ShipStaticData, StandardClassBPositionReport,
};
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::Message;

use crate::backoff::Backoff;
use crate::queue::LineQueue;
use crate::stats::Stats;
use crate::upstream::ingest_line;

const STREAM_URL: &str = "wss://stream.aisstream.io/v0/stream";
pub const AISSTREAM_KEY_ENV: &str = "OSF_AISSTREAM_KEY";
pub const AISSTREAM_HELP: &str = "aisstream: set OSF_AISSTREAM_KEY to your aisstream.io API key \
(environment only — never commit it), then rerun. Get a key at https://aisstream.io.";

#[derive(Debug, Deserialize)]
struct Frame {
    #[serde(rename = "MessageType")]
    message_type: String,
    #[serde(rename = "Message")]
    message: FrameMessage,
}

/// The `Message` object is keyed by the message-type name; only the field
/// matching this frame's type is populated.
#[derive(Debug, Default, Deserialize)]
struct FrameMessage {
    #[serde(rename = "PositionReport")]
    position_report: Option<PositionReport>,
    #[serde(rename = "ShipStaticData")]
    ship_static_data: Option<ShipStaticData>,
    #[serde(rename = "StandardClassBPositionReport")]
    standard_class_b: Option<StandardClassBPositionReport>,
}

enum Convert {
    Lines(Vec<String>),
    /// A message type we do not re-encode.
    Skip(String),
    /// Parse failure, or a known type whose payload didn't deserialize.
    Invalid,
}

fn convert(text: &str, seq: u8) -> Convert {
    let frame: Frame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(_) => return Convert::Invalid,
    };
    let encoded = match frame.message_type.as_str() {
        "PositionReport" => frame
            .message
            .position_report
            .map(|p| encode_position_report(&p)),
        "ShipStaticData" => frame
            .message
            .ship_static_data
            .map(|p| encode_ship_static_data(&p)),
        "StandardClassBPositionReport" => frame
            .message
            .standard_class_b
            .map(|p| encode_standard_class_b(&p)),
        other => return Convert::Skip(other.to_string()),
    };
    match encoded {
        Some((payload, fill)) => {
            Convert::Lines(openseafeed_nmea::to_sentences(&payload, fill, "A", seq))
        }
        None => Convert::Invalid,
    }
}

#[derive(Default)]
struct SkipCounts {
    total: u64,
    by_type: BTreeMap<String, u64>,
}

fn handle_text(
    text: &str,
    queue: &LineQueue,
    stats: &Stats,
    seq: &mut u8,
    skips: &mut SkipCounts,
) {
    match convert(text, *seq) {
        Convert::Lines(lines) => {
            *seq = seq.wrapping_add(1);
            for line in &lines {
                ingest_line(line, queue, stats);
            }
        }
        Convert::Skip(kind) => {
            skips.total += 1;
            *skips.by_type.entry(kind).or_default() += 1;
        }
        Convert::Invalid => Stats::incr(&stats.invalid),
    }
}

/// Run one connection lifecycle: connect, subscribe within the server's 3s
/// window, and pump frames until the socket closes (caller reconnects).
pub async fn read(queue: &LineQueue, stats: &Stats, backoff: &mut Backoff) -> Result<()> {
    let key = std::env::var(AISSTREAM_KEY_ENV).map_err(|_| anyhow!("{AISSTREAM_HELP}"))?;

    let (ws, _resp) = tokio_tungstenite::connect_async(STREAM_URL).await?;
    let (mut sink, mut stream) = ws.split();

    // Subscribe immediately; the server drops the socket if it sees no
    // subscription within 3s. The key lives only in this frame — never logged.
    let subscription = json!({
        "APIKey": key,
        "BoundingBoxes": [[[-90.0, -180.0], [90.0, 180.0]]],
    });
    sink.send(Message::Text(subscription.to_string().into()))
        .await?;
    backoff.reset();
    tracing::info!("connected to aisstream.io");

    let mut seq: u8 = 0;
    let mut skips = SkipCounts::default();
    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Text(text) => handle_text(text.as_str(), queue, stats, &mut seq, &mut skips),
            Message::Binary(bytes) => {
                if let Ok(text) = std::str::from_utf8(&bytes) {
                    handle_text(text, queue, stats, &mut seq, &mut skips);
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    if skips.total > 0 {
        tracing::debug!(skipped = skips.total, breakdown = ?skips.by_type, "aisstream skipped unconverted message types");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openseafeed_ais::{decode, Packet};

    // A realistic aisstream.io PositionReport frame. Field names match the
    // aisstream v0 wire format (which this project's types mirror).
    const POSITION_FRAME: &str = r#"{
      "MessageType": "PositionReport",
      "MetaData": {"MMSI": 257123000, "ShipName": "TEST VESSEL",
                   "latitude": 60.1699, "longitude": 24.9384,
                   "time_utc": "2026-07-24 12:00:00 +0000 UTC"},
      "Message": {"PositionReport": {
        "MessageID": 1, "RepeatIndicator": 0, "UserID": 257123000, "Valid": true,
        "NavigationalStatus": 0, "RateOfTurn": 0, "Sog": 10.5, "PositionAccuracy": true,
        "Longitude": 24.9384, "Latitude": 60.1699, "Cog": 123.4, "TrueHeading": 124,
        "Timestamp": 30, "SpecialManoeuvreIndicator": 0, "Spare": 0,
        "Raim": false, "CommunicationState": 0
      }}
    }"#;

    #[test]
    fn position_frame_converts_and_decodes_back() {
        let Convert::Lines(lines) = convert(POSITION_FRAME, 0) else {
            panic!("expected conversion to lines");
        };
        assert_eq!(lines.len(), 1);
        let s = openseafeed_nmea::parse(&lines[0]).unwrap();
        let m = decode(&s.payload, s.fill_bits).unwrap();
        assert_eq!(m.mmsi, 257_123_000);
        let Packet::PositionReport(p) = m.packet else {
            panic!("expected position report");
        };
        assert!((p.latitude - 60.1699).abs() < 1e-4);
        assert!((p.longitude - 24.9384).abs() < 1e-4);
        assert!((p.sog - 10.5).abs() < 0.05);
        assert_eq!(p.true_heading, 124);
    }

    #[test]
    fn unknown_message_type_is_skipped() {
        let frame = r#"{"MessageType":"AidsToNavigationReport",
            "Message":{"AidsToNavigationReport":{"UserID":1}}}"#;
        match convert(frame, 0) {
            Convert::Skip(kind) => assert_eq!(kind, "AidsToNavigationReport"),
            _ => panic!("expected skip"),
        }
    }

    #[test]
    fn malformed_json_is_invalid() {
        assert!(matches!(convert("not json", 0), Convert::Invalid));
    }
}
