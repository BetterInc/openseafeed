use serde::Serialize;

/// Header fields common to every message.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq)]
pub struct Header {
    #[serde(rename = "MessageID")]
    pub message_id: u8,
    #[serde(rename = "RepeatIndicator")]
    pub repeat_indicator: u8,
    #[serde(rename = "UserID")]
    pub user_id: u32,
    #[serde(rename = "Valid")]
    pub valid: bool,
}

/// Vessel dimensions relative to the reported position, meters.
#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct Dimension {
    #[serde(rename = "A")]
    pub a: u16, // to bow
    #[serde(rename = "B")]
    pub b: u16, // to stern
    #[serde(rename = "C")]
    pub c: u8, // to port
    #[serde(rename = "D")]
    pub d: u8, // to starboard
}

/// Estimated time of arrival from static/voyage data.
#[derive(Debug, Clone, Copy, Default, Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub struct Eta {
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
}

/// Message types 1, 2, 3 — class A position report.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct PositionReport {
    #[serde(flatten)]
    pub header: Header,
    pub navigational_status: u8,
    /// Raw ROT_AIS, -128 = not available.
    pub rate_of_turn: i16,
    /// Knots; 102.3 = not available.
    pub sog: f64,
    pub position_accuracy: bool,
    /// Degrees; 181 = not available.
    pub longitude: f64,
    /// Degrees; 91 = not available.
    pub latitude: f64,
    /// Degrees; 360 = not available.
    pub cog: f64,
    /// 511 = not available.
    pub true_heading: u16,
    pub timestamp: u8,
    pub special_manoeuvre_indicator: u8,
    pub spare: u8,
    pub raim: bool,
    pub communication_state: u32,
}

/// Message type 4 (and 11) — base station report.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct BaseStationReport {
    #[serde(flatten)]
    pub header: Header,
    pub utc_year: u16,
    pub utc_month: u8,
    pub utc_day: u8,
    pub utc_hour: u8,
    pub utc_minute: u8,
    pub utc_second: u8,
    pub position_accuracy: bool,
    pub longitude: f64,
    pub latitude: f64,
    pub fix_type: u8,
    pub spare: u16,
    pub raim: bool,
    pub communication_state: u32,
}

/// Message type 5 — class A static and voyage data.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct ShipStaticData {
    #[serde(flatten)]
    pub header: Header,
    pub ais_version: u8,
    pub imo_number: u32,
    pub call_sign: String,
    pub name: String,
    #[serde(rename = "Type")]
    pub ship_type: u8,
    pub dimension: Dimension,
    pub fix_type: u8,
    pub eta: Eta,
    /// Meters.
    pub maximum_static_draught: f64,
    pub destination: String,
    pub dte: bool,
    pub spare: bool,
}

/// Message type 9 — SAR aircraft position report.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct SarAircraftPositionReport {
    #[serde(flatten)]
    pub header: Header,
    /// Meters; 4095 = not available.
    pub altitude: u16,
    /// Knots; 1023 = not available.
    pub sog: u16,
    pub position_accuracy: bool,
    pub longitude: f64,
    pub latitude: f64,
    pub cog: f64,
    pub timestamp: u8,
    pub alt_from_baro: bool,
    pub dte: bool,
    pub assigned_mode: bool,
    pub raim: bool,
    pub communication_state: u32,
}

/// Message type 18 — standard class B position report.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct StandardClassBPositionReport {
    #[serde(flatten)]
    pub header: Header,
    pub sog: f64,
    pub position_accuracy: bool,
    pub longitude: f64,
    pub latitude: f64,
    pub cog: f64,
    pub true_heading: u16,
    pub timestamp: u8,
    pub class_b_unit: bool, // CS unit flag
    pub class_b_display: bool,
    pub class_b_dsc: bool,
    pub class_b_band: bool,
    pub class_b_msg22: bool,
    pub assigned_mode: bool,
    pub raim: bool,
    /// Selector flag at bit 148; the 19-bit state itself follows at 149.
    pub communication_state_is_itdma: bool,
    pub communication_state: u32,
}

/// Message type 19 — extended class B position report.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct ExtendedClassBPositionReport {
    #[serde(flatten)]
    pub header: Header,
    pub sog: f64,
    pub position_accuracy: bool,
    pub longitude: f64,
    pub latitude: f64,
    pub cog: f64,
    pub true_heading: u16,
    pub timestamp: u8,
    pub name: String,
    #[serde(rename = "Type")]
    pub ship_type: u8,
    pub dimension: Dimension,
    pub fix_type: u8,
    pub raim: bool,
    pub dte: bool,
    pub assigned_mode: bool,
}

/// Message type 21 — aids-to-navigation report.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct AidsToNavigationReport {
    #[serde(flatten)]
    pub header: Header,
    /// Aid type 0..=31.
    #[serde(rename = "Type")]
    pub aid_type: u8,
    pub name: String,
    pub position_accuracy: bool,
    pub longitude: f64,
    pub latitude: f64,
    pub dimension: Dimension,
    // aisstream.io spells this "Fixtype" on this message type.
    #[serde(rename = "Fixtype")]
    pub fix_type: u8,
    pub timestamp: u8,
    pub off_position: bool,
    /// Regional reserved bits.
    #[serde(rename = "AtoN")]
    pub aton_status: u8,
    pub raim: bool,
    #[serde(rename = "VirtualAtoN")]
    pub virtual_aton: bool,
    pub assigned_mode: bool,
    pub name_extension: String,
}

/// Message type 24 — static data report. Part A carries the name, part B
/// the rest.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct StaticDataReport {
    #[serde(flatten)]
    pub header: Header,
    pub part_number: u8,
    // Part A
    pub name: String,
    // Part B (field spellings follow aisstream.io, typos included)
    pub ship_type: u8,
    #[serde(rename = "VendorIDName")]
    pub vendor_id_name: String,
    #[serde(rename = "VenderIDModel")]
    pub vender_id_model: u8,
    #[serde(rename = "VenderIDSerial")]
    pub vender_id_serial: u32,
    pub call_sign: String,
    pub dimension: Dimension,
    pub spare: u8,
}

/// Message type 27 — long-range broadcast.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct LongRangeAisBroadcastMessage {
    #[serde(flatten)]
    pub header: Header,
    pub position_accuracy: bool,
    pub raim: bool,
    pub navigational_status: u8,
    /// Degrees at 1/10-minute resolution.
    pub longitude: f64,
    pub latitude: f64,
    /// Knots; 63 = not available.
    pub sog: f64,
    /// Degrees; 511 = not available.
    pub cog: f64,
    pub position_latency: bool,
    pub spare: bool,
}

/// Raw information about message types we do not decode yet.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
#[serde(rename_all = "PascalCase")]
pub struct Unknown {
    #[serde(flatten)]
    pub header: Header,
    pub num_bits: usize,
}

/// A decoded AIS packet. Serde's external tagging makes this serialize as
/// `{"PositionReport": {...}}` — exactly the shape of the aisstream.io
/// `Message` object.
#[derive(Debug, Clone, Serialize, serde::Deserialize, PartialEq)]
pub enum Packet {
    PositionReport(PositionReport),
    BaseStationReport(BaseStationReport),
    ShipStaticData(ShipStaticData),
    StandardSearchAndRescueAircraftReport(SarAircraftPositionReport),
    StandardClassBPositionReport(StandardClassBPositionReport),
    ExtendedClassBPositionReport(ExtendedClassBPositionReport),
    AidsToNavigationReport(AidsToNavigationReport),
    StaticDataReport(StaticDataReport),
    LongRangeAisBroadcastMessage(LongRangeAisBroadcastMessage),
    Unknown(Unknown),
}

impl Packet {
    /// The aisstream.io `MessageType` string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Packet::PositionReport(_) => "PositionReport",
            Packet::BaseStationReport(_) => "BaseStationReport",
            Packet::ShipStaticData(_) => "ShipStaticData",
            Packet::StandardSearchAndRescueAircraftReport(_) => {
                "StandardSearchAndRescueAircraftReport"
            }
            Packet::StandardClassBPositionReport(_) => "StandardClassBPositionReport",
            Packet::ExtendedClassBPositionReport(_) => "ExtendedClassBPositionReport",
            Packet::AidsToNavigationReport(_) => "AidsToNavigationReport",
            Packet::StaticDataReport(_) => "StaticDataReport",
            Packet::LongRangeAisBroadcastMessage(_) => "LongRangeAisBroadcastMessage",
            Packet::Unknown(_) => "Unknown",
        }
    }
}

/// A decoded AIS message plus the routing facts every service needs.
#[derive(Debug, Clone, PartialEq)]
pub struct Message {
    /// Raw ITU message id 1..=27.
    pub id: u8,
    pub mmsi: u32,
    pub packet: Packet,
    /// (latitude, longitude) in degrees, when the message carries a valid
    /// position.
    pub position: Option<(f64, f64)>,
    /// Vessel/aid name, when the message carries one (types 5, 19, 21, 24A).
    pub name: Option<String>,
    /// ITU ship-type code, when the message carries one (types 5, 19, 24B).
    pub ship_type: Option<u8>,
    /// IMO number, when the message carries a nonzero one (type 5 only).
    pub imo: Option<u32>,
}

impl Message {
    pub fn type_name(&self) -> &'static str {
        self.packet.type_name()
    }
}
