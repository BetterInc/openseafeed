//! Envelopes carried on the NATS bus and the aisstream.io-compatible wire
//! format sent to streaming clients.

use serde::{Deserialize, Serialize};

/// A raw (still armored) AIS message group as published by the ingest
/// gateway on `ais.raw.<station>`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawEnvelope {
    /// Original NMEA lines, in fragment order.
    #[serde(rename = "s")]
    pub sentences: Vec<String>,
    /// Concatenated armored payload.
    #[serde(rename = "p")]
    pub payload: String,
    #[serde(rename = "f")]
    pub fill_bits: u8,
    #[serde(rename = "c", default, skip_serializing_if = "String::is_empty")]
    pub channel: String,
    /// Registered station id.
    #[serde(rename = "st")]
    pub station_id: String,
    /// Producer class / provenance, e.g. "rf", "connect:norway", "partner".
    #[serde(rename = "src", default, skip_serializing_if = "String::is_empty")]
    pub source: String,
    /// Unix milliseconds at ingest.
    #[serde(rename = "t")]
    pub received_at_ms: u64,
}

/// The aisstream.io v0 `MetaData` object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MetaData {
    #[serde(rename = "MMSI")]
    pub mmsi: u32,
    #[serde(rename = "MMSI_String")]
    pub mmsi_string: u32,
    #[serde(rename = "ShipName")]
    pub ship_name: String,
    /// ITU ship-type code (70-79 cargo, 80-89 tanker, ...) remembered from
    /// the vessel's static messages. Extension over aisstream.io's MetaData,
    /// omitted from the JSON when unknown.
    #[serde(rename = "ShipType", default, skip_serializing_if = "Option::is_none")]
    pub ship_type: Option<u8>,
    pub latitude: f64,
    pub longitude: f64,
    /// RFC 3339 UTC timestamp string.
    pub time_utc: String,
}

/// The aisstream.io v0 client wire format: what fan-out delivers over the
/// WebSocket, and what the pipeline publishes on `ais.decoded.*`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StreamMessage {
    /// `{"PositionReport": {...}}` — externally tagged packet.
    #[serde(rename = "Message")]
    pub message: serde_json::Value,
    #[serde(rename = "MessageType")]
    pub message_type: String,
    #[serde(rename = "MetaData")]
    pub metadata: MetaData,
}

/// NATS subject conventions.
pub mod subjects {
    /// Raw envelopes from one station.
    pub fn raw(station_id: &str) -> String {
        format!("ais.raw.{}", sanitize_token(station_id))
    }

    /// Wildcard for all raw envelopes.
    pub const RAW_ALL: &str = "ais.raw.>";

    /// Decoded messages, one token per geohash character:
    /// `ais.decoded.u.1.h.3`. Messages without a position go to
    /// `ais.decoded.none`.
    pub fn decoded(geohash4: &str) -> String {
        if geohash4.is_empty() {
            return NO_POSITION.to_string();
        }
        let mut s = String::from("ais.decoded");
        for c in geohash4.chars() {
            s.push('.');
            s.push(c);
        }
        s
    }

    /// Subscription pattern for a geohash prefix (may be shorter than 4
    /// chars). An empty prefix subscribes to everything positioned.
    pub fn decoded_pattern(prefix: &str) -> String {
        if prefix.is_empty() {
            return "ais.decoded.>".to_string();
        }
        let mut s = String::from("ais.decoded");
        for c in prefix.chars() {
            s.push('.');
            s.push(c);
        }
        if prefix.len() < 4 {
            s.push_str(".>");
        }
        s
    }

    pub const NO_POSITION: &str = "ais.decoded.none";

    /// NATS subject tokens must not contain '.', '*', '>', whitespace.
    pub fn sanitize_token(raw: &str) -> String {
        let mut out: String = raw
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        if out.is_empty() {
            out.push('_');
        }
        out
    }
}

/// NATS message headers set by the pipeline so the fan-out can filter
/// without parsing JSON bodies.
pub mod headers {
    pub const MMSI: &str = "Osf-Mmsi";
    pub const MSG_TYPE: &str = "Osf-Type";
    pub const LAT: &str = "Osf-Lat";
    pub const LON: &str = "Osf-Lon";
    pub const STATION: &str = "Osf-Station";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subject_shapes() {
        assert_eq!(subjects::raw("stn-1"), "ais.raw.stn-1");
        assert_eq!(subjects::raw("weird station!"), "ais.raw.weird_station_");
        assert_eq!(subjects::decoded("u1h3"), "ais.decoded.u.1.h.3");
        assert_eq!(subjects::decoded(""), "ais.decoded.none");
        assert_eq!(subjects::decoded_pattern("u1"), "ais.decoded.u.1.>");
        assert_eq!(subjects::decoded_pattern("u1h3"), "ais.decoded.u.1.h.3");
        assert_eq!(subjects::decoded_pattern(""), "ais.decoded.>");
    }

    #[test]
    fn stream_message_wire_format() {
        let m = openseafeed_ais::decode("177KQJ5000G?tO`K>RA1wUbN0TKH", 0).unwrap();
        let (lat, lon) = m.position.unwrap();
        let sm = StreamMessage {
            message: serde_json::to_value(&m.packet).unwrap(),
            message_type: m.type_name().to_string(),
            metadata: MetaData {
                mmsi: m.mmsi,
                mmsi_string: m.mmsi,
                ship_name: String::new(),
                ship_type: None,
                latitude: lat,
                longitude: lon,
                time_utc: "2026-07-24T00:00:00Z".into(),
            },
        };
        let v = serde_json::to_value(&sm).unwrap();
        assert_eq!(v["MessageType"], "PositionReport");
        assert_eq!(v["MetaData"]["MMSI"], 477_553_000);
        assert!(!v["Message"]["PositionReport"]["Latitude"].is_null());
    }
}
