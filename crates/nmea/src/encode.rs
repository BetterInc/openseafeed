/// Maximum armored payload chars per sentence, keeping the full line within
/// the traditional 82-char NMEA limit.
const MAX_PAYLOAD_CHARS: usize = 60;

/// Build AIVDM sentence(s) for an armored payload, splitting into a
/// multipart group when needed. `seq` is the sequential message id used to
/// correlate fragments (required semantics only for multipart; pass a
/// rotating 0..=9 counter).
pub fn to_sentences(payload: &str, fill_bits: u8, channel: &str, seq: u8) -> Vec<String> {
    let chunks: Vec<&str> = if payload.is_empty() {
        vec![""]
    } else {
        payload
            .as_bytes()
            .chunks(MAX_PAYLOAD_CHARS)
            .map(|c| std::str::from_utf8(c).expect("payload is ascii"))
            .collect()
    };
    let total = chunks.len();
    let seq_field = if total == 1 {
        String::new()
    } else {
        (seq % 10).to_string()
    };
    chunks
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            let fill = if i + 1 == total { fill_bits } else { 0 };
            let body = format!(
                "AIVDM,{},{},{},{},{},{}",
                total,
                i + 1,
                seq_field,
                channel,
                chunk,
                fill
            );
            let sum = body.bytes().fold(0u8, |a, b| a ^ b);
            format!("!{body}*{sum:02X}")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{parse, Assembler};
    use std::time::{Duration, Instant};

    #[test]
    fn single_sentence_round_trip() {
        let payload = "177KQJ5000G?tO`K>RA1wUbN0TKH";
        let lines = to_sentences(payload, 0, "A", 0);
        assert_eq!(lines.len(), 1);
        let s = parse(&lines[0]).unwrap();
        assert_eq!(s.payload, payload);
        assert_eq!(s.fill_bits, 0);
        assert_eq!(s.channel, "A");
        assert_eq!(s.fragments, 1);
    }

    #[test]
    fn multipart_round_trip_through_assembler() {
        // 71-char type 5 payload splits into 60 + 11.
        let payload = concat!(
            "55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E53",
            "1@0000000000000"
        );
        let lines = to_sentences(payload, 2, "B", 3);
        assert_eq!(lines.len(), 2);
        let mut asm = Assembler::new(Duration::from_secs(30));
        let now = Instant::now();
        assert!(asm.add(parse(&lines[0]).unwrap(), now).is_none());
        let group = asm.add(parse(&lines[1]).unwrap(), now).unwrap();
        assert_eq!(group.payload, payload);
        assert_eq!(group.fill_bits, 2);
    }
}
