//! Golden decode tests. Expected values were generated independently with
//! pyais 3.1.0 (see docs/testing.md) so the decoder is checked against a
//! second implementation, not against itself.

use openseafeed_ais::{decode, Packet};

fn close(a: f64, b: f64) {
    assert!((a - b).abs() < 1e-5, "{a} != {b}");
}

#[test]
fn type1_position_report() {
    let m = decode("177KQJ5000G?tO`K>RA1wUbN0TKH", 0).unwrap();
    assert_eq!(m.id, 1);
    assert_eq!(m.mmsi, 477_553_000);
    let (lat, lon) = m.position.unwrap();
    close(lat, 47.582833);
    close(lon, -122.345833);
    let Packet::PositionReport(p) = &m.packet else {
        panic!("wrong packet: {:?}", m.packet)
    };
    assert_eq!(p.navigational_status, 5);
    assert_eq!(p.rate_of_turn, 0);
    close(p.sog, 0.0);
    assert!(!p.position_accuracy);
    close(p.cog, 51.0);
    assert_eq!(p.true_heading, 181);
    assert_eq!(p.timestamp, 15);
    assert_eq!(p.special_manoeuvre_indicator, 0);
    assert!(!p.raim);
    assert_eq!(p.communication_state, 149_208);
}

#[test]
fn type3_position_report() {
    let m = decode("35Ml=50Oh@o?vlHDS6`AS0rR0000", 0).unwrap();
    assert_eq!(m.mmsi, 366_808_340);
    let Packet::PositionReport(p) = &m.packet else {
        panic!()
    };
    assert_eq!(p.navigational_status, 0);
    assert_eq!(p.rate_of_turn, 127);
    close(p.sog, 1.6);
    assert!(p.position_accuracy);
    close(p.longitude, -122.3379);
    close(p.latitude, 35.911095);
    close(p.cog, 39.6);
    assert_eq!(p.true_heading, 29);
    assert_eq!(p.timestamp, 17);
    assert_eq!(p.communication_state, 0);
}

#[test]
fn type4_base_station() {
    let m = decode("402;rFiv@k;tmK`GJDTIS?vN20S:", 0).unwrap();
    assert_eq!(m.mmsi, 2_292_315);
    let Packet::BaseStationReport(p) = &m.packet else {
        panic!()
    };
    assert_eq!(
        (
            p.utc_year,
            p.utc_month,
            p.utc_day,
            p.utc_hour,
            p.utc_minute,
            p.utc_second
        ),
        (2020, 3, 6, 11, 60, 53)
    );
    close(p.longitude, -61.087023);
    close(p.latitude, 63.612265);
    // Raw EPFD bits are 14; pyais collapses values outside its enum to
    // Undefined (0). We keep the raw value.
    assert_eq!(p.fix_type, 14);
    assert!(p.raim);
    assert_eq!(p.communication_state, 2250);
}

#[test]
fn type5_static_voyage_multipart() {
    // Reassembled from a 2-fragment group; combined payload with 2 fill bits.
    let payload = concat!(
        "55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E53",
        "1@0000000000000"
    );
    let m = decode(payload, 2).unwrap();
    assert_eq!(m.mmsi, 369_190_000);
    assert_eq!(m.name.as_deref(), Some("MT.MITCHELL"));
    let Packet::ShipStaticData(p) = &m.packet else {
        panic!()
    };
    assert_eq!(p.ais_version, 0);
    assert_eq!(p.imo_number, 6_710_932);
    assert_eq!(p.call_sign, "WDA9674");
    assert_eq!(p.name, "MT.MITCHELL");
    assert_eq!(p.ship_type, 99);
    assert_eq!(
        (p.dimension.a, p.dimension.b, p.dimension.c, p.dimension.d),
        (90, 90, 10, 10)
    );
    assert_eq!(p.fix_type, 1);
    assert_eq!(
        (p.eta.month, p.eta.day, p.eta.hour, p.eta.minute),
        (1, 2, 8, 0)
    );
    close(p.maximum_static_draught, 6.0);
    assert_eq!(p.destination, "SEATTLE");
    assert!(!p.dte);
}

#[test]
fn type9_sar_aircraft() {
    let m = decode("91b55wi;hbOS@OdQAC062Ch2089h", 0).unwrap();
    assert_eq!(m.mmsi, 111_232_511);
    let Packet::StandardSearchAndRescueAircraftReport(p) = &m.packet else {
        panic!()
    };
    assert_eq!(p.altitude, 303);
    assert_eq!(p.sog, 42);
    close(p.longitude, -6.278843);
    close(p.latitude, 58.144);
    close(p.cog, 154.5);
    assert_eq!(p.timestamp, 15);
    assert!(p.dte);
    assert!(!p.assigned_mode);
    assert!(!p.raim);
    assert_eq!(p.communication_state, 33_392);
}

#[test]
fn type18_class_b() {
    let m = decode("B52K>;h00Fc>jpUlNV@ikwpUoP06", 0).unwrap();
    assert_eq!(m.mmsi, 338_087_471);
    let Packet::StandardClassBPositionReport(p) = &m.packet else {
        panic!()
    };
    close(p.sog, 0.1);
    assert!(!p.position_accuracy);
    close(p.longitude, -74.072132);
    close(p.latitude, 40.68454);
    close(p.cog, 79.6);
    assert_eq!(p.true_heading, 511);
    assert_eq!(p.timestamp, 49);
    assert!(p.class_b_unit);
    assert!(!p.class_b_display);
    assert!(p.class_b_dsc);
    assert!(p.class_b_band);
    assert!(p.class_b_msg22);
    assert!(!p.assigned_mode);
    assert!(p.raim);
    // pyais folds the selector bit into a 20-bit radio value: 917510 =
    // (1 << 19) + 393222.
    assert!(p.communication_state_is_itdma);
    assert_eq!(p.communication_state, 393_222);
}

#[test]
fn type19_extended_class_b() {
    let m = decode("C5N3SRgPEnJGEBT>NhWAwwo862PaLELTBJ:V00000000S0D:R220", 0).unwrap();
    assert_eq!(m.mmsi, 367_059_850);
    assert_eq!(m.name.as_deref(), Some("CAPT.J.RIMES"));
    let Packet::ExtendedClassBPositionReport(p) = &m.packet else {
        panic!()
    };
    close(p.sog, 8.7);
    close(p.longitude, -88.810392);
    close(p.latitude, 29.543695);
    close(p.cog, 335.9);
    assert_eq!(p.true_heading, 511);
    assert_eq!(p.timestamp, 46);
    assert_eq!(p.ship_type, 70);
    assert_eq!(
        (p.dimension.a, p.dimension.b, p.dimension.c, p.dimension.d),
        (5, 21, 4, 4)
    );
    assert_eq!(p.fix_type, 1);
    assert!(!p.raim);
    assert!(!p.dte);
}

#[test]
fn type21_aton() {
    let m = decode("E>k`sO70VQ97aRh1T0W72V@611@=FVj<;V5d@00003v010", 4).unwrap();
    assert_eq!(m.mmsi, 993_672_060);
    assert_eq!(m.name.as_deref(), Some("AMBROSE CHANNEL LBB"));
    let Packet::AidsToNavigationReport(p) = &m.packet else {
        panic!()
    };
    assert_eq!(p.aid_type, 14);
    close(p.longitude, -74.009367);
    close(p.latitude, 40.52795);
    assert_eq!(p.fix_type, 7);
    assert_eq!(p.timestamp, 60);
    assert!(!p.off_position);
    assert!(!p.raim);
    assert!(p.virtual_aton);
    assert_eq!(p.name_extension, "");
}

#[test]
fn type24_part_a() {
    let m = decode("H52KNe@Pm>0Htt0000000000000", 2).unwrap();
    assert_eq!(m.mmsi, 338_091_701);
    assert_eq!(m.name.as_deref(), Some("HMS FOO"));
    let Packet::StaticDataReport(p) = &m.packet else {
        panic!()
    };
    assert!(!p.part_number);
    assert!(p.report_a.valid);
    assert!(!p.report_b.valid);
    assert_eq!(p.report_a.name, "HMS FOO");
}

#[test]
fn type24_part_b() {
    let m = decode("H3pro:4q3?=1B0000000000P7220", 0).unwrap();
    assert_eq!(m.mmsi, 261_011_240);
    let Packet::StaticDataReport(p) = &m.packet else {
        panic!()
    };
    assert!(p.part_number);
    assert!(p.report_b.valid);
    assert!(!p.report_a.valid);
    let b = &p.report_b;
    assert_eq!(b.ship_type, 57);
    assert_eq!(b.vendor_id_name, "COM");
    assert_eq!(b.vender_id_model, 0);
    assert_eq!(b.vender_id_serial, 335_872);
    assert_eq!(b.call_sign, "");
    assert_eq!(
        (b.dimension.a, b.dimension.b, b.dimension.c, b.dimension.d),
        (4, 7, 2, 2)
    );
}

#[test]
fn type27_long_range() {
    let m = decode("KC5E2b@U19PFdLbMuc5=ROv62<7m", 0).unwrap();
    assert_eq!(m.mmsi, 206_914_217);
    let Packet::LongRangeAisBroadcastMessage(p) = &m.packet else {
        panic!()
    };
    assert_eq!(p.header.repeat_indicator, 1);
    assert!(!p.position_accuracy);
    assert!(!p.raim);
    assert_eq!(p.navigational_status, 2);
    close(p.longitude, 137.023333);
    close(p.latitude, 4.84);
    close(p.sog, 57.0);
    close(p.cog, 167.0);
    assert!(!p.position_latency);
}

#[test]
fn aisstream_compatible_json_shape() {
    let m = decode("177KQJ5000G?tO`K>RA1wUbN0TKH", 0).unwrap();
    assert_eq!(m.type_name(), "PositionReport");
    let v = serde_json::to_value(&m.packet).unwrap();
    let p = &v["PositionReport"];
    for key in [
        "MessageID",
        "RepeatIndicator",
        "UserID",
        "Valid",
        "NavigationalStatus",
        "RateOfTurn",
        "Sog",
        "PositionAccuracy",
        "Longitude",
        "Latitude",
        "Cog",
        "TrueHeading",
        "Timestamp",
        "SpecialManoeuvreIndicator",
        "Raim",
        "CommunicationState",
    ] {
        assert!(!p[key].is_null(), "missing field {key}: {v}");
    }
    assert_eq!(p["MessageID"], 1);
    assert_eq!(p["UserID"], 477_553_000);
}

#[test]
fn rejects_truncated_and_garbage() {
    assert!(decode("177KQJ", 0).is_err()); // 36 bits < header
    assert!(decode("1", 0).is_err());
    // Type says 5 but the payload is one char short of 420 bits.
    assert!(decode("55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E5", 2).is_err());
}
