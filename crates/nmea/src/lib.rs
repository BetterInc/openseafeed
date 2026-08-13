//! NMEA 0183 AIVDM/AIVDO sentence parsing and multi-fragment reassembly.

mod assembler;
mod encode;
pub use assembler::{Assembler, Group};
pub use encode::to_sentences;

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("not an AIVDM/AIVDO sentence")]
    NotAis,
    #[error("checksum mismatch: have {have:02X}, want {want:02X}")]
    BadChecksum { have: u8, want: u8 },
    #[error("malformed sentence")]
    Malformed,
    #[error("invalid fragment numbering")]
    BadFragment,
    #[error("payload contains invalid characters")]
    PayloadChars,
}

/// A single parsed AIVDM/AIVDO sentence (one fragment).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sentence {
    /// Original line, TAG block stripped.
    pub raw: String,
    /// TAG block content between backslashes, if any.
    pub tag_block: Option<String>,
    /// Talker id, e.g. "AI", "AB", "SA".
    pub talker: String,
    /// "VDM" or "VDO".
    pub kind: String,
    /// Total fragments in the group.
    pub fragments: u8,
    /// This fragment's index, 1-based.
    pub fragment: u8,
    /// Sequential group id; empty for single-fragment messages.
    pub seq_id: String,
    /// Radio channel: "A", "B", "1", "2" or empty.
    pub channel: String,
    /// Armored 6-bit payload.
    pub payload: String,
    /// 0..=5 trailing fill bits (meaningful on the last fragment).
    pub fill_bits: u8,
}

/// Parse one line. Leading/trailing whitespace and an optional NMEA 4.10
/// TAG block (`\...\`) are tolerated.
pub fn parse(line: &str) -> Result<Sentence, ParseError> {
    let mut line = line.trim();
    let mut tag_block = None;
    if let Some(rest) = line.strip_prefix('\\') {
        let end = rest.find('\\').ok_or(ParseError::Malformed)?;
        tag_block = Some(rest[..end].to_string());
        line = &rest[end + 1..];
    }
    let bytes = line.as_bytes();
    if bytes.len() < 10 || (bytes[0] != b'!' && bytes[0] != b'$') {
        return Err(ParseError::NotAis);
    }
    let star = line.rfind('*').ok_or(ParseError::Malformed)?;
    if star + 3 > line.len() {
        return Err(ParseError::Malformed);
    }
    let want =
        u8::from_str_radix(&line[star + 1..star + 3], 16).map_err(|_| ParseError::Malformed)?;
    let have = bytes[1..star].iter().fold(0u8, |acc, b| acc ^ b);
    if have != want {
        return Err(ParseError::BadChecksum { have, want });
    }

    let fields: Vec<&str> = line[1..star].split(',').collect();
    if fields.len() != 7 {
        return Err(ParseError::Malformed);
    }
    let head = fields[0]; // e.g. AIVDM
    if head.len() != 5 || !(head.ends_with("VDM") || head.ends_with("VDO")) {
        return Err(ParseError::NotAis);
    }
    let fragments: u8 = fields[1].parse().map_err(|_| ParseError::BadFragment)?;
    let fragment: u8 = fields[2].parse().map_err(|_| ParseError::BadFragment)?;
    if fragments < 1 || fragment < 1 || fragment > fragments {
        return Err(ParseError::BadFragment);
    }
    let fill_bits: u8 = if fields[6].is_empty() {
        0
    } else {
        fields[6].parse().map_err(|_| ParseError::Malformed)?
    };
    if fill_bits > 5 {
        return Err(ParseError::Malformed);
    }
    let payload = fields[5];
    if payload
        .bytes()
        .any(|c| !(48..=119).contains(&c) || (88..=95).contains(&c))
    {
        return Err(ParseError::PayloadChars);
    }

    Ok(Sentence {
        raw: line.to_string(),
        tag_block,
        talker: head[..2].to_string(),
        kind: head[2..].to_string(),
        fragments,
        fragment,
        seq_id: fields[3].to_string(),
        channel: fields[4].to_string(),
        payload: payload.to_string(),
        fill_bits,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_fragment() {
        let s = parse("!AIVDM,1,1,,B,177KQJ5000G?tO`K>RA1wUbN0TKH,0*5C").unwrap();
        assert_eq!(s.talker, "AI");
        assert_eq!(s.kind, "VDM");
        assert_eq!(s.fragments, 1);
        assert_eq!(s.fragment, 1);
        assert_eq!(s.channel, "B");
        assert_eq!(s.payload, "177KQJ5000G?tO`K>RA1wUbN0TKH");
        assert_eq!(s.fill_bits, 0);
    }

    #[test]
    fn parses_multipart_fields() {
        let s = parse("!AIVDM,2,2,3,B,1@0000000000000,2*55").unwrap();
        assert_eq!(s.fragments, 2);
        assert_eq!(s.fragment, 2);
        assert_eq!(s.seq_id, "3");
        assert_eq!(s.fill_bits, 2);
    }

    #[test]
    fn strips_tag_block() {
        // TAG block checksum content is not validated (some feeds get it wrong);
        // the sentence checksum is.
        let s =
            parse("\\s:2573135,c:1671620143*0B\\!AIVDM,1,1,,B,177KQJ5000G?tO`K>RA1wUbN0TKH,0*5C")
                .unwrap();
        assert_eq!(s.tag_block.as_deref(), Some("s:2573135,c:1671620143*0B"));
        assert_eq!(s.payload, "177KQJ5000G?tO`K>RA1wUbN0TKH");
    }

    #[test]
    fn rejects_bad_checksum() {
        assert!(matches!(
            parse("!AIVDM,1,1,,B,177KQJ5000G?tO`K>RA1wUbN0TKH,0*5D"),
            Err(ParseError::BadChecksum { .. })
        ));
    }

    #[test]
    fn rejects_garbage() {
        assert!(parse("GPGGA,nonsense").is_err());
        assert!(parse("").is_err());
        assert!(parse("!AIVDM,2,3,3,B,1@0000000000000,2*54").is_err()); // frag > total
    }
}
