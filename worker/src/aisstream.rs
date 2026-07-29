//! aisstream upstream: consume aisstream.io's own v0 stream.
//!
//! aisstream.io's wire format is the same StreamMessage shape this project
//! emits, so the inner `Message` object deserializes directly into our
//! `Packet` (serde external tagging) and re-encodes to AIVDM — covering every
//! message type the crate can encode, not just a hand-picked few.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use futures::{SinkExt, StreamExt};
use openseafeed_ais::Packet;
use serde::Deserialize;
use serde_json::{json, Value};
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
    /// Externally-tagged packet, e.g. `{"PositionReport": {...}}`.
    #[serde(rename = "Message")]
    message: Value,
}

enum Convert {
    Lines { kind: String, lines: Vec<String> },
    /// Bad JSON, an unrecognized message type, or a type we can't encode
    /// (`Unknown`). Expected to be near zero against a live feed.
    Failed,
}

fn convert(text: &str, seq: u8) -> Convert {
    let frame: Frame = match serde_json::from_str(text) {
        Ok(f) => f,
        Err(_) => return Convert::Failed,
    };
    let packet: Packet = match serde_json::from_value(frame.message) {
        Ok(p) => p,
        Err(_) => return Convert::Failed,
    };
    match packet.encode() {
        Some((payload, fill)) => Convert::Lines {
            kind: frame.message_type,
            lines: openseafeed_nmea::to_sentences(&payload, fill, "A", seq),
        },
        None => Convert::Failed,
    }
}

#[derive(Default)]
struct Counts {
    converted: BTreeMap<String, u64>,
    failed: u64,
}

fn handle_text(text: &str, queue: &LineQueue, stats: &Stats, seq: &mut u8, counts: &mut Counts) {
    match convert(text, *seq) {
        Convert::Lines { kind, lines } => {
            *seq = seq.wrapping_add(1);
            *counts.converted.entry(kind).or_default() += 1;
            for line in &lines {
                ingest_line(line, queue, stats);
            }
        }
        Convert::Failed => {
            counts.failed += 1;
            Stats::incr(&stats.invalid);
        }
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
    let mut counts = Counts::default();
    // Staleness watchdog: the world feed streams continuously, so a silent
    // socket is a dead socket. Without this, a half-open TCP connection
    // blocks the read forever and the reconnect logic never fires (seen in
    // production: counters frozen for hours with reconnects=0).
    let mut last_frame = tokio::time::Instant::now();
    let mut ping_tick = tokio::time::interval(std::time::Duration::from_secs(30));
    ping_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            msg = stream.next() => {
                let Some(msg) = msg else { break };
                last_frame = tokio::time::Instant::now();
                match msg? {
                    Message::Text(text) => {
                        handle_text(text.as_str(), queue, stats, &mut seq, &mut counts)
                    }
                    Message::Binary(bytes) => {
                        if let Ok(text) = std::str::from_utf8(&bytes) {
                            handle_text(text, queue, stats, &mut seq, &mut counts);
                        }
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
            _ = ping_tick.tick() => {
                if last_frame.elapsed() > std::time::Duration::from_secs(90) {
                    anyhow::bail!("no frames for 90s, treating connection as dead");
                }
                // Client-initiated ping: a broken path fails the send (or
                // stays silent and trips the 90s bail above).
                sink.send(Message::Ping(Vec::new().into())).await?;
            }
        }
    }

    if !counts.converted.is_empty() || counts.failed > 0 {
        tracing::debug!(
            converted = ?counts.converted,
            failed = counts.failed,
            "aisstream conversion breakdown"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openseafeed_ais::{decode, Header, LongRangeAisBroadcastMessage, Packet};

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
        let Convert::Lines { kind, lines } = convert(POSITION_FRAME, 0) else {
            panic!("expected conversion to lines");
        };
        assert_eq!(kind, "PositionReport");
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
    fn long_range_type27_frame_converts_and_decodes_back() {
        // Build the frame from our own serialization so the field names are
        // guaranteed to match what deserialize expects.
        let original = LongRangeAisBroadcastMessage {
            header: Header {
                message_id: 27,
                repeat_indicator: 0,
                user_id: 219_000_001,
                valid: true,
            },
            position_accuracy: false,
            raim: false,
            navigational_status: 5,
            longitude: 12.5,
            latitude: 55.7,
            sog: 12.0,
            cog: 180.0,
            position_latency: false,
            spare: false,
        };
        let message =
            serde_json::to_value(Packet::LongRangeAisBroadcastMessage(original.clone())).unwrap();
        let frame = json!({
            "MessageType": "LongRangeAisBroadcastMessage",
            "Message": message,
        });

        let Convert::Lines { kind, lines } = convert(&frame.to_string(), 0) else {
            panic!("expected conversion to lines");
        };
        assert_eq!(kind, "LongRangeAisBroadcastMessage");
        let s = openseafeed_nmea::parse(&lines[0]).unwrap();
        let m = decode(&s.payload, s.fill_bits).unwrap();
        assert_eq!(m.mmsi, 219_000_001);
        assert!(matches!(m.packet, Packet::LongRangeAisBroadcastMessage(_)));
    }

    #[test]
    fn unknown_message_type_fails() {
        let frame = r#"{"MessageType":"SomeFutureType",
            "Message":{"SomeFutureType":{"UserID":1}}}"#;
        assert!(matches!(convert(frame, 0), Convert::Failed));
    }

    #[test]
    fn malformed_json_fails() {
        assert!(matches!(convert("not json", 0), Convert::Failed));
    }
}
