//! AIS message encoding — the inverse of `decode`.
//!
//! Needed by connectors that consume sources publishing *decoded* vessel
//! data (e.g. Finland's Digitraffic JSON): re-encoding to standard AIVDM
//! keeps a single ingest format, so external feeds get the same dedupe,
//! routing and provenance treatment as RF stations.

use crate::types::*;

/// Bit accumulator that armors into an AIVDM payload.
pub struct BitsMut {
    b: Vec<u8>,
    n: usize,
}

impl Default for BitsMut {
    fn default() -> Self {
        Self::new()
    }
}

impl BitsMut {
    pub fn new() -> Self {
        Self {
            b: Vec::with_capacity(64),
            n: 0,
        }
    }

    pub fn push_uint(&mut self, v: u64, len: usize) {
        debug_assert!(len <= 64);
        for i in (0..len).rev() {
            let bit = (v >> i) & 1 == 1;
            if self.n.is_multiple_of(8) {
                self.b.push(0);
            }
            if bit {
                self.b[self.n / 8] |= 1 << (7 - self.n % 8);
            }
            self.n += 1;
        }
    }

    pub fn push_int(&mut self, v: i64, len: usize) {
        self.push_uint(v as u64 & ((1u64 << len) - 1), len);
    }

    pub fn push_bool(&mut self, v: bool) {
        self.push_uint(v as u64, 1);
    }

    /// Push a 6-bit-ASCII string field of exactly `len` bits, padding with
    /// '@' (0) which decodes back to end-of-string.
    pub fn push_str(&mut self, s: &str, len: usize) {
        debug_assert!(len.is_multiple_of(6));
        let chars = len / 6;
        let bytes = s.as_bytes();
        for i in 0..chars {
            let c = if i < bytes.len() {
                let mut c = bytes[i].to_ascii_uppercase();
                if c >= 64 {
                    c -= 64; // '@'..'_' -> 0..31
                }
                if c > 63 {
                    c = 0;
                }
                c
            } else {
                0
            };
            self.push_uint(c as u64, 6);
        }
    }

    /// Armor into (payload, fill_bits).
    pub fn to_payload(&self) -> (String, u8) {
        let fill = (6 - self.n % 6) % 6;
        let total = self.n + fill;
        let mut out = String::with_capacity(total / 6);
        for i in (0..total).step_by(6) {
            let mut v: u8 = 0;
            for j in 0..6 {
                v <<= 1;
                let pos = i + j;
                if pos < self.n && self.b[pos / 8] & (1 << (7 - pos % 8)) != 0 {
                    v |= 1;
                }
            }
            out.push(if v < 40 {
                (v + 48) as char
            } else {
                (v + 56) as char
            });
        }
        (out, fill as u8)
    }
}

fn deg_to_minutes(deg: f64, scale: f64) -> i64 {
    (deg * 60.0 * scale).round() as i64
}

fn push_header(bits: &mut BitsMut, h: &Header) {
    bits.push_uint(h.message_id as u64, 6);
    bits.push_uint(h.repeat_indicator as u64, 2);
    bits.push_uint(h.user_id as u64, 30);
}

/// Encode a class A position report (type 1/2/3) — 168 bits.
pub fn encode_position_report(p: &PositionReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(p.navigational_status as u64, 4);
    b.push_int(p.rate_of_turn as i64, 8);
    b.push_uint((p.sog * 10.0).round() as u64, 10);
    b.push_bool(p.position_accuracy);
    b.push_int(deg_to_minutes(p.longitude, 10_000.0), 28);
    b.push_int(deg_to_minutes(p.latitude, 10_000.0), 27);
    b.push_uint((p.cog * 10.0).round() as u64, 12);
    b.push_uint(p.true_heading as u64, 9);
    b.push_uint(p.timestamp as u64, 6);
    b.push_uint(p.special_manoeuvre_indicator as u64, 2);
    b.push_uint(p.spare as u64, 3);
    b.push_bool(p.raim);
    b.push_uint(p.communication_state as u64, 19);
    b.to_payload()
}

/// Encode class A static and voyage data (type 5) — 424 bits.
pub fn encode_ship_static_data(p: &ShipStaticData) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(p.ais_version as u64, 2);
    b.push_uint(p.imo_number as u64, 30);
    b.push_str(&p.call_sign, 42);
    b.push_str(&p.name, 120);
    b.push_uint(p.ship_type as u64, 8);
    b.push_uint(p.dimension.a as u64, 9);
    b.push_uint(p.dimension.b as u64, 9);
    b.push_uint(p.dimension.c as u64, 6);
    b.push_uint(p.dimension.d as u64, 6);
    b.push_uint(p.fix_type as u64, 4);
    b.push_uint(p.eta.month as u64, 4);
    b.push_uint(p.eta.day as u64, 5);
    b.push_uint(p.eta.hour as u64, 5);
    b.push_uint(p.eta.minute as u64, 6);
    b.push_uint((p.maximum_static_draught * 10.0).round() as u64, 8);
    b.push_str(&p.destination, 120);
    b.push_bool(p.dte);
    b.push_bool(p.spare);
    b.to_payload()
}

/// Encode a standard class B position report (type 18) — 168 bits.
pub fn encode_standard_class_b(p: &StandardClassBPositionReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(0, 8); // reserved
    b.push_uint((p.sog * 10.0).round() as u64, 10);
    b.push_bool(p.position_accuracy);
    b.push_int(deg_to_minutes(p.longitude, 10_000.0), 28);
    b.push_int(deg_to_minutes(p.latitude, 10_000.0), 27);
    b.push_uint((p.cog * 10.0).round() as u64, 12);
    b.push_uint(p.true_heading as u64, 9);
    b.push_uint(p.timestamp as u64, 6);
    b.push_uint(0, 2); // regional reserved
    b.push_bool(p.class_b_unit);
    b.push_bool(p.class_b_display);
    b.push_bool(p.class_b_dsc);
    b.push_bool(p.class_b_band);
    b.push_bool(p.class_b_msg22);
    b.push_bool(p.assigned_mode);
    b.push_bool(p.raim);
    b.push_bool(p.communication_state_is_itdma);
    b.push_uint(p.communication_state as u64, 19);
    b.to_payload()
}

/// Encode a base station report (type 4/11) — 168 bits.
pub fn encode_base_station_report(p: &BaseStationReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(p.utc_year as u64, 14);
    b.push_uint(p.utc_month as u64, 4);
    b.push_uint(p.utc_day as u64, 5);
    b.push_uint(p.utc_hour as u64, 5);
    b.push_uint(p.utc_minute as u64, 6);
    b.push_uint(p.utc_second as u64, 6);
    b.push_bool(p.position_accuracy);
    b.push_int(deg_to_minutes(p.longitude, 10_000.0), 28);
    b.push_int(deg_to_minutes(p.latitude, 10_000.0), 27);
    b.push_uint(p.fix_type as u64, 4);
    b.push_uint(p.spare as u64, 10);
    b.push_bool(p.raim);
    b.push_uint(p.communication_state as u64, 19);
    b.to_payload()
}

/// Encode a SAR aircraft position report (type 9) — 168 bits.
pub fn encode_sar_aircraft(p: &SarAircraftPositionReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(p.altitude as u64, 12);
    b.push_uint(p.sog as u64, 10);
    b.push_bool(p.position_accuracy);
    b.push_int(deg_to_minutes(p.longitude, 10_000.0), 28);
    b.push_int(deg_to_minutes(p.latitude, 10_000.0), 27);
    b.push_uint((p.cog * 10.0).round() as u64, 12);
    b.push_uint(p.timestamp as u64, 6);
    b.push_bool(p.alt_from_baro);
    b.push_uint(0, 7); // regional reserved
    b.push_bool(p.dte);
    b.push_uint(0, 3); // spare
    b.push_bool(p.assigned_mode);
    b.push_bool(p.raim);
    b.push_uint(p.communication_state as u64, 20);
    b.to_payload()
}

/// Encode an extended class B position report (type 19) — 312 bits.
pub fn encode_extended_class_b(p: &ExtendedClassBPositionReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(0, 8); // reserved
    b.push_uint((p.sog * 10.0).round() as u64, 10);
    b.push_bool(p.position_accuracy);
    b.push_int(deg_to_minutes(p.longitude, 10_000.0), 28);
    b.push_int(deg_to_minutes(p.latitude, 10_000.0), 27);
    b.push_uint((p.cog * 10.0).round() as u64, 12);
    b.push_uint(p.true_heading as u64, 9);
    b.push_uint(p.timestamp as u64, 6);
    b.push_uint(0, 4); // regional reserved
    b.push_str(&p.name, 120);
    b.push_uint(p.ship_type as u64, 8);
    push_dimension(&mut b, &p.dimension);
    b.push_uint(p.fix_type as u64, 4);
    b.push_bool(p.raim);
    b.push_bool(p.dte);
    b.push_bool(p.assigned_mode);
    b.push_uint(0, 4); // spare
    b.to_payload()
}

/// Encode an aids-to-navigation report (type 21) — 272 bits (name
/// extension omitted; names beyond 20 chars are truncated).
pub fn encode_aids_to_navigation(p: &AidsToNavigationReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(p.aid_type as u64, 5);
    b.push_str(&p.name, 120);
    b.push_bool(p.position_accuracy);
    b.push_int(deg_to_minutes(p.longitude, 10_000.0), 28);
    b.push_int(deg_to_minutes(p.latitude, 10_000.0), 27);
    push_dimension(&mut b, &p.dimension);
    b.push_uint(p.fix_type as u64, 4);
    b.push_uint(p.timestamp as u64, 6);
    b.push_bool(p.off_position);
    b.push_uint(p.aton_status as u64, 8);
    b.push_bool(p.raim);
    b.push_bool(p.virtual_aton);
    b.push_bool(p.assigned_mode);
    b.push_bool(false); // spare
    b.to_payload()
}

/// Encode a static data report (type 24, part A or B per `part_number`) —
/// 160/168 bits.
pub fn encode_static_data_report(p: &StaticDataReport) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_uint(p.part_number as u64, 2);
    if p.part_number == 0 {
        b.push_str(&p.name, 120);
    } else {
        b.push_uint(p.ship_type as u64, 8);
        b.push_str(&p.vendor_id_name, 18);
        b.push_uint(p.vender_id_model as u64, 4);
        b.push_uint(p.vender_id_serial as u64, 20);
        b.push_str(&p.call_sign, 42);
        push_dimension(&mut b, &p.dimension);
        b.push_uint(p.spare as u64, 6);
    }
    b.to_payload()
}

/// Encode a long-range broadcast (type 27) — 96 bits.
pub fn encode_long_range(p: &LongRangeAisBroadcastMessage) -> (String, u8) {
    let mut b = BitsMut::new();
    push_header(&mut b, &p.header);
    b.push_bool(p.position_accuracy);
    b.push_bool(p.raim);
    b.push_uint(p.navigational_status as u64, 4);
    b.push_int(deg_to_minutes(p.longitude, 10.0), 18);
    b.push_int(deg_to_minutes(p.latitude, 10.0), 17);
    b.push_uint(p.sog.round() as u64, 6);
    b.push_uint(p.cog.round() as u64, 9);
    b.push_bool(p.position_latency);
    b.push_bool(p.spare);
    b.to_payload()
}

fn push_dimension(b: &mut BitsMut, d: &Dimension) {
    b.push_uint(d.a as u64, 9);
    b.push_uint(d.b as u64, 9);
    b.push_uint(d.c as u64, 6);
    b.push_uint(d.d as u64, 6);
}

impl Packet {
    /// Encode any packet back to an armored payload. `None` for `Unknown`.
    pub fn encode(&self) -> Option<(String, u8)> {
        Some(match self {
            Packet::PositionReport(p) => encode_position_report(p),
            Packet::BaseStationReport(p) => encode_base_station_report(p),
            Packet::ShipStaticData(p) => encode_ship_static_data(p),
            Packet::StandardSearchAndRescueAircraftReport(p) => encode_sar_aircraft(p),
            Packet::StandardClassBPositionReport(p) => encode_standard_class_b(p),
            Packet::ExtendedClassBPositionReport(p) => encode_extended_class_b(p),
            Packet::AidsToNavigationReport(p) => encode_aids_to_navigation(p),
            Packet::StaticDataReport(p) => encode_static_data_report(p),
            Packet::LongRangeAisBroadcastMessage(p) => encode_long_range(p),
            Packet::Unknown(_) => return None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{decode, Packet};

    fn header(id: u8, mmsi: u32) -> Header {
        Header {
            message_id: id,
            repeat_indicator: 0,
            user_id: mmsi,
            valid: true,
        }
    }

    #[test]
    fn position_report_round_trip() {
        let p = PositionReport {
            header: header(1, 230_123_456),
            navigational_status: 0,
            rate_of_turn: -128,
            sog: 12.3,
            position_accuracy: true,
            // Values representable at 1/10000-minute resolution.
            longitude: 24.9384,
            latitude: 60.1699,
            cog: 87.5,
            true_heading: 88,
            timestamp: 42,
            special_manoeuvre_indicator: 0,
            spare: 0,
            raim: false,
            communication_state: 0,
        };
        let (payload, fill) = encode_position_report(&p);
        assert_eq!(payload.len(), 28);
        assert_eq!(fill, 0);
        let m = decode(&payload, fill).unwrap();
        assert_eq!(m.mmsi, 230_123_456);
        let Packet::PositionReport(q) = m.packet else {
            panic!()
        };
        assert_eq!(q.navigational_status, 0);
        assert_eq!(q.rate_of_turn, -128);
        assert!((q.sog - 12.3).abs() < 0.05);
        assert!((q.longitude - 24.9384).abs() < 1e-4);
        assert!((q.latitude - 60.1699).abs() < 1e-4);
        assert!((q.cog - 87.5).abs() < 0.05);
        assert_eq!(q.true_heading, 88);
        assert_eq!(q.timestamp, 42);
        assert!(q.position_accuracy);
    }

    #[test]
    fn static_data_round_trip() {
        let p = ShipStaticData {
            header: header(5, 230_987_000),
            ais_version: 2,
            imo_number: 9_074_729,
            call_sign: "OJABC".into(),
            name: "SILJA SERENADE".into(),
            ship_type: 60,
            dimension: Dimension {
                a: 100,
                b: 103,
                c: 15,
                d: 17,
            },
            fix_type: 1,
            eta: Eta {
                month: 7,
                day: 25,
                hour: 6,
                minute: 30,
            },
            maximum_static_draught: 7.1,
            destination: "HELSINKI".into(),
            dte: false,
            spare: false,
        };
        let (payload, fill) = encode_ship_static_data(&p);
        assert_eq!(payload.len(), 71);
        assert_eq!(fill, 2);
        let m = decode(&payload, fill).unwrap();
        assert_eq!(m.name.as_deref(), Some("SILJA SERENADE"));
        let Packet::ShipStaticData(q) = m.packet else {
            panic!()
        };
        assert_eq!(q.imo_number, 9_074_729);
        assert_eq!(q.call_sign, "OJABC");
        assert_eq!(q.ship_type, 60);
        assert_eq!(q.dimension, p.dimension);
        assert_eq!(q.eta, p.eta);
        assert!((q.maximum_static_draught - 7.1).abs() < 0.05);
        assert_eq!(q.destination, "HELSINKI");
    }

    #[test]
    fn class_b_round_trip() {
        let p = StandardClassBPositionReport {
            header: header(18, 265_547_250),
            sog: 4.2,
            position_accuracy: false,
            longitude: -0.1278,
            latitude: 51.5074,
            cog: 200.0,
            true_heading: 511,
            timestamp: 60,
            class_b_unit: true,
            class_b_display: false,
            class_b_dsc: true,
            class_b_band: true,
            class_b_msg22: true,
            assigned_mode: false,
            raim: true,
            communication_state_is_itdma: true,
            communication_state: 393_222,
        };
        let (payload, fill) = encode_standard_class_b(&p);
        let m = decode(&payload, fill).unwrap();
        let Packet::StandardClassBPositionReport(q) = m.packet else {
            panic!()
        };
        assert_eq!(q.header.user_id, 265_547_250);
        assert!((q.latitude - 51.5074).abs() < 1e-4);
        assert!((q.longitude - -0.1278).abs() < 1e-4);
        assert_eq!(q.true_heading, 511);
        assert!(q.communication_state_is_itdma);
        assert_eq!(q.communication_state, 393_222);
    }

    #[test]
    fn all_golden_vectors_survive_decode_encode_decode() {
        // Every payload from the pyais-validated golden set: decoding the
        // re-encoded payload must yield an identical packet.
        let vectors: &[(&str, u8)] = &[
            ("177KQJ5000G?tO`K>RA1wUbN0TKH", 0), // 1
            ("35Ml=50Oh@o?vlHDS6`AS0rR0000", 0), // 3
            ("402;rFiv@k;tmK`GJDTIS?vN20S:", 0), // 4
            (
                "55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E531@0000000000000",
                2,
            ), // 5
            ("91b55wi;hbOS@OdQAC062Ch2089h", 0), // 9
            ("B52K>;h00Fc>jpUlNV@ikwpUoP06", 0), // 18
            ("C5N3SRgPEnJGEBT>NhWAwwo862PaLELTBJ:V00000000S0D:R220", 0), // 19
            ("E>k`sO70VQ97aRh1T0W72V@611@=FVj<;V5d@00003v010", 4), // 21
            ("H52KNe@Pm>0Htt0000000000000", 2),  // 24A
            ("H3pro:4q3?=1B0000000000P7220", 0), // 24B
            ("KC5E2b@U19PFdLbMuc5=ROv62<7m", 0), // 27
        ];
        for (payload, fill) in vectors {
            let m1 = decode(payload, *fill).unwrap();
            let (p2, f2) = m1.packet.encode().unwrap();
            let m2 = decode(&p2, f2).unwrap();
            assert_eq!(m1.packet, m2.packet, "round trip failed for {payload}");
        }
    }

    #[test]
    fn lowercase_and_long_strings_are_normalized() {
        let mut p = ShipStaticData {
            header: header(5, 1),
            ais_version: 0,
            imo_number: 0,
            call_sign: "abc".into(),
            name: "a name that is much longer than twenty characters".into(),
            ship_type: 0,
            dimension: Dimension::default(),
            fix_type: 0,
            eta: Eta::default(),
            maximum_static_draught: 0.0,
            destination: String::new(),
            dte: false,
            spare: false,
        };
        p.name = p.name.to_uppercase();
        let (payload, fill) = encode_ship_static_data(&p);
        let m = decode(&payload, fill).unwrap();
        let Packet::ShipStaticData(q) = m.packet else {
            panic!()
        };
        assert_eq!(q.call_sign, "ABC");
        // Truncated to the 120-bit (20-char) field, trailing space trimmed.
        assert_eq!(q.name, "A NAME THAT IS MUCH");
    }
}
