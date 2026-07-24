//! AIS (ITU-R M.1371) decoder. Struct and JSON field names are
//! wire-compatible with the aisstream.io v0 API so existing clients work
//! unchanged.

mod bits;
mod decode;
mod encode;
mod types;

pub use bits::Bits;
pub use decode::{decode, decode_bits};
pub use encode::{
    encode_aids_to_navigation, encode_base_station_report, encode_long_range,
    encode_position_report, encode_sar_aircraft, encode_ship_static_data,
    encode_standard_class_b, encode_static_data_report, BitsMut,
};
pub use types::*;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("invalid 6-bit armored payload")]
    Armoring,
    #[error("message shorter than its type requires: {bits} bits, need {need}")]
    TooShort { bits: usize, need: usize },
}
