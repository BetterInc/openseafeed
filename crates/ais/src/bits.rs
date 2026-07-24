use crate::DecodeError;

/// A bitstream decoded from an armored AIVDM payload.
pub struct Bits {
    b: Vec<u8>,
    n: usize, // number of valid bits
}

impl Bits {
    /// De-armor an AIVDM payload (each ASCII char carries six bits) and drop
    /// the trailing fill bits.
    pub fn from_payload(payload: &str, fill_bits: u8) -> Result<Self, DecodeError> {
        if fill_bits > 5 {
            return Err(DecodeError::Armoring);
        }
        let chars = payload.as_bytes();
        let mut b = vec![0u8; (chars.len() * 6 + 7) / 8];
        for (i, &c) in chars.iter().enumerate() {
            let mut v = c as i32 - 48;
            if v > 40 {
                v -= 8;
            }
            if !(0..=63).contains(&v) {
                return Err(DecodeError::Armoring);
            }
            let off = i * 6;
            for j in 0..6 {
                if v & (1 << (5 - j)) != 0 {
                    b[(off + j) / 8] |= 1 << (7 - (off + j) % 8);
                }
            }
        }
        let total = chars.len() * 6;
        if (fill_bits as usize) > total {
            return Err(DecodeError::Armoring);
        }
        Ok(Self {
            b,
            n: total - fill_bits as usize,
        })
    }

    /// Number of valid bits.
    pub fn len(&self) -> usize {
        self.n
    }

    pub fn is_empty(&self) -> bool {
        self.n == 0
    }

    /// Read an unsigned big-endian field. Reading past the end returns 0;
    /// callers validate message length up front.
    pub fn uint(&self, start: usize, length: usize) -> u64 {
        if length == 0 || length > 64 || start + length > self.n {
            return 0;
        }
        let mut v: u64 = 0;
        for i in start..start + length {
            v <<= 1;
            if self.b[i / 8] & (1 << (7 - i % 8)) != 0 {
                v |= 1;
            }
        }
        v
    }

    /// Read a two's-complement signed field.
    pub fn int(&self, start: usize, length: usize) -> i64 {
        let v = self.uint(start, length);
        if length > 0 && length < 64 && v & (1 << (length - 1)) != 0 {
            (v | (u64::MAX << length)) as i64
        } else {
            v as i64
        }
    }

    /// Read a single bit.
    pub fn bit(&self, pos: usize) -> bool {
        self.uint(pos, 1) == 1
    }

    /// Read a 6-bit-ASCII string field, stopping at '@' and trimming
    /// trailing spaces. `length` is in bits and is truncated to whole chars
    /// within the valid stream.
    pub fn string(&self, start: usize, mut length: usize) -> String {
        if start >= self.n {
            return String::new();
        }
        if start + length > self.n {
            length = self.n - start;
        }
        let mut s = String::with_capacity(length / 6);
        let mut off = start;
        while off + 6 <= start + length {
            let mut c = self.uint(off, 6) as u8;
            if c == 0 {
                break; // '@' terminates
            }
            if c < 32 {
                c += 64;
            }
            s.push(c as char);
            off += 6;
        }
        s.trim_end_matches(' ').to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dearmors_known_prefix() {
        // '1' => 1, '7' => 7: stream starts 000001 000111 ...
        let bs = Bits::from_payload("17", 0).unwrap();
        assert_eq!(bs.len(), 12);
        assert_eq!(bs.uint(0, 6), 1);
        assert_eq!(bs.uint(6, 6), 7);
    }

    #[test]
    fn signed_fields() {
        // 'w' => 63 (111111): -1 in 6-bit two's complement
        let bs = Bits::from_payload("w", 0).unwrap();
        assert_eq!(bs.int(0, 6), -1);
    }

    #[test]
    fn out_of_range_reads_zero() {
        let bs = Bits::from_payload("1", 0).unwrap();
        assert_eq!(bs.uint(4, 6), 0);
        assert_eq!(bs.uint(100, 6), 0);
    }

    #[test]
    fn rejects_bad_chars() {
        assert!(Bits::from_payload("\u{7f}", 0).is_err());
    }
}
