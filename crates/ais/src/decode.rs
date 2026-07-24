use crate::bits::Bits;
use crate::types::*;
use crate::DecodeError;

const LON_NA: f64 = 181.0;
const LAT_NA: f64 = 91.0;

/// De-armor a reassembled AIVDM payload and decode it into a typed message.
pub fn decode(payload: &str, fill_bits: u8) -> Result<Message, DecodeError> {
    decode_bits(&Bits::from_payload(payload, fill_bits)?)
}

/// Decode an AIS bitstream.
pub fn decode_bits(bs: &Bits) -> Result<Message, DecodeError> {
    if bs.len() < 38 {
        return Err(DecodeError::TooShort {
            bits: bs.len(),
            need: 38,
        });
    }
    let header = Header {
        message_id: bs.uint(0, 6) as u8,
        repeat_indicator: bs.uint(6, 2) as u8,
        user_id: bs.uint(8, 30) as u32,
        valid: true,
    };
    let id = header.message_id;
    let mmsi = header.user_id;
    let need = |n: usize| -> Result<(), DecodeError> {
        if bs.len() < n {
            Err(DecodeError::TooShort {
                bits: bs.len(),
                need: n,
            })
        } else {
            Ok(())
        }
    };

    let mut position = None;
    let mut name = None;

    let packet = match id {
        1..=3 => {
            need(168)?;
            let p = PositionReport {
                header,
                navigational_status: bs.uint(38, 4) as u8,
                rate_of_turn: bs.int(42, 8) as i16,
                sog: bs.uint(50, 10) as f64 / 10.0,
                position_accuracy: bs.bit(60),
                longitude: minutes_to_deg(bs.int(61, 28), 10_000.0),
                latitude: minutes_to_deg(bs.int(89, 27), 10_000.0),
                cog: bs.uint(116, 12) as f64 / 10.0,
                true_heading: bs.uint(128, 9) as u16,
                timestamp: bs.uint(137, 6) as u8,
                special_manoeuvre_indicator: bs.uint(143, 2) as u8,
                spare: bs.uint(145, 3) as u8,
                raim: bs.bit(148),
                communication_state: bs.uint(149, 19) as u32,
            };
            position = valid_position(p.latitude, p.longitude);
            Packet::PositionReport(p)
        }
        4 | 11 => {
            need(168)?;
            let p = BaseStationReport {
                header,
                utc_year: bs.uint(38, 14) as u16,
                utc_month: bs.uint(52, 4) as u8,
                utc_day: bs.uint(56, 5) as u8,
                utc_hour: bs.uint(61, 5) as u8,
                utc_minute: bs.uint(66, 6) as u8,
                utc_second: bs.uint(72, 6) as u8,
                position_accuracy: bs.bit(78),
                longitude: minutes_to_deg(bs.int(79, 28), 10_000.0),
                latitude: minutes_to_deg(bs.int(107, 27), 10_000.0),
                fix_type: bs.uint(134, 4) as u8,
                spare: bs.uint(138, 10) as u16,
                raim: bs.bit(148),
                communication_state: bs.uint(149, 19) as u32,
            };
            position = valid_position(p.latitude, p.longitude);
            Packet::BaseStationReport(p)
        }
        5 => {
            // Some encoders truncate the two final spare/DTE bits, so accept
            // 420 bits and read the tail defensively (uint() returns 0 past
            // the end).
            need(420)?;
            let p = ShipStaticData {
                header,
                ais_version: bs.uint(38, 2) as u8,
                imo_number: bs.uint(40, 30) as u32,
                call_sign: bs.string(70, 42),
                name: bs.string(112, 120),
                ship_type: bs.uint(232, 8) as u8,
                dimension: dimension(bs, 240),
                fix_type: bs.uint(270, 4) as u8,
                eta: Eta {
                    month: bs.uint(274, 4) as u8,
                    day: bs.uint(278, 5) as u8,
                    hour: bs.uint(283, 5) as u8,
                    minute: bs.uint(288, 6) as u8,
                },
                maximum_static_draught: bs.uint(294, 8) as f64 / 10.0,
                destination: bs.string(302, 120),
                dte: bs.bit(422),
                spare: bs.bit(423),
            };
            name = non_empty(&p.name);
            Packet::ShipStaticData(p)
        }
        9 => {
            need(168)?;
            let p = SarAircraftPositionReport {
                header,
                altitude: bs.uint(38, 12) as u16,
                sog: bs.uint(50, 10) as u16,
                position_accuracy: bs.bit(60),
                longitude: minutes_to_deg(bs.int(61, 28), 10_000.0),
                latitude: minutes_to_deg(bs.int(89, 27), 10_000.0),
                cog: bs.uint(116, 12) as f64 / 10.0,
                timestamp: bs.uint(128, 6) as u8,
                alt_from_baro: bs.bit(134),
                dte: bs.bit(142),
                assigned_mode: bs.bit(146),
                raim: bs.bit(147),
                communication_state: bs.uint(148, 20) as u32,
            };
            position = valid_position(p.latitude, p.longitude);
            Packet::StandardSearchAndRescueAircraftReport(p)
        }
        18 => {
            need(168)?;
            let p = StandardClassBPositionReport {
                header,
                sog: bs.uint(46, 10) as f64 / 10.0,
                position_accuracy: bs.bit(56),
                longitude: minutes_to_deg(bs.int(57, 28), 10_000.0),
                latitude: minutes_to_deg(bs.int(85, 27), 10_000.0),
                cog: bs.uint(112, 12) as f64 / 10.0,
                true_heading: bs.uint(124, 9) as u16,
                timestamp: bs.uint(133, 6) as u8,
                class_b_unit: bs.bit(141),
                class_b_display: bs.bit(142),
                class_b_dsc: bs.bit(143),
                class_b_band: bs.bit(144),
                class_b_msg22: bs.bit(145),
                assigned_mode: bs.bit(146),
                raim: bs.bit(147),
                communication_state_is_itdma: bs.bit(148),
                communication_state: bs.uint(149, 19) as u32,
            };
            position = valid_position(p.latitude, p.longitude);
            Packet::StandardClassBPositionReport(p)
        }
        19 => {
            need(312)?;
            let p = ExtendedClassBPositionReport {
                header,
                sog: bs.uint(46, 10) as f64 / 10.0,
                position_accuracy: bs.bit(56),
                longitude: minutes_to_deg(bs.int(57, 28), 10_000.0),
                latitude: minutes_to_deg(bs.int(85, 27), 10_000.0),
                cog: bs.uint(112, 12) as f64 / 10.0,
                true_heading: bs.uint(124, 9) as u16,
                timestamp: bs.uint(133, 6) as u8,
                name: bs.string(143, 120),
                ship_type: bs.uint(263, 8) as u8,
                dimension: dimension(bs, 271),
                fix_type: bs.uint(301, 4) as u8,
                raim: bs.bit(305),
                dte: bs.bit(306),
                assigned_mode: bs.bit(307),
            };
            position = valid_position(p.latitude, p.longitude);
            name = non_empty(&p.name);
            Packet::ExtendedClassBPositionReport(p)
        }
        21 => {
            need(272)?;
            let p = AidsToNavigationReport {
                header,
                aid_type: bs.uint(38, 5) as u8,
                name: bs.string(43, 120),
                position_accuracy: bs.bit(163),
                longitude: minutes_to_deg(bs.int(164, 28), 10_000.0),
                latitude: minutes_to_deg(bs.int(192, 27), 10_000.0),
                dimension: dimension(bs, 219),
                fix_type: bs.uint(249, 4) as u8,
                timestamp: bs.uint(253, 6) as u8,
                off_position: bs.bit(259),
                aton_status: bs.uint(260, 8) as u8,
                raim: bs.bit(268),
                virtual_aton: bs.bit(269),
                assigned_mode: bs.bit(270),
                name_extension: if bs.len() > 272 {
                    bs.string(272, bs.len() - 272)
                } else {
                    String::new()
                },
            };
            position = valid_position(p.latitude, p.longitude);
            name = non_empty(&p.name);
            Packet::AidsToNavigationReport(p)
        }
        24 => {
            need(160)?;
            let part_number = bs.uint(38, 2) as u8;
            let mut p = StaticDataReport {
                header,
                part_number,
                ..Default::default()
            };
            match part_number {
                0 => {
                    p.name = bs.string(40, 120);
                    name = non_empty(&p.name);
                }
                1 => {
                    need(168)?;
                    p.ship_type = bs.uint(40, 8) as u8;
                    p.vendor_id_name = bs.string(48, 18);
                    p.vender_id_model = bs.uint(66, 4) as u8;
                    p.vender_id_serial = bs.uint(70, 20) as u32;
                    p.call_sign = bs.string(90, 42);
                    p.dimension = dimension(bs, 132);
                    p.spare = bs.uint(162, 6) as u8;
                }
                _ => {}
            }
            Packet::StaticDataReport(p)
        }
        27 => {
            need(96)?;
            let p = LongRangeAisBroadcastMessage {
                header,
                position_accuracy: bs.bit(38),
                raim: bs.bit(39),
                navigational_status: bs.uint(40, 4) as u8,
                longitude: minutes_to_deg(bs.int(44, 18), 10.0),
                latitude: minutes_to_deg(bs.int(62, 17), 10.0),
                sog: bs.uint(79, 6) as f64,
                cog: bs.uint(85, 9) as f64,
                position_latency: bs.bit(94),
                spare: bs.bit(95),
            };
            position = valid_position(p.latitude, p.longitude);
            Packet::LongRangeAisBroadcastMessage(p)
        }
        _ => Packet::Unknown(Unknown {
            header,
            num_bits: bs.len(),
        }),
    };

    Ok(Message {
        id,
        mmsi,
        packet,
        position,
        name,
    })
}

/// Convert a signed lat/lon field expressed in 1/scale minutes to degrees.
fn minutes_to_deg(raw: i64, scale: f64) -> f64 {
    raw as f64 / (60.0 * scale)
}

fn dimension(bs: &Bits, start: usize) -> Dimension {
    Dimension {
        a: bs.uint(start, 9) as u16,
        b: bs.uint(start + 9, 9) as u16,
        c: bs.uint(start + 18, 6) as u8,
        d: bs.uint(start + 24, 6) as u8,
    }
}

fn valid_position(lat: f64, lon: f64) -> Option<(f64, f64)> {
    if lat == LAT_NA || lon == LON_NA || !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon)
    {
        None
    } else {
        Some((lat, lon))
    }
}

fn non_empty(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}
