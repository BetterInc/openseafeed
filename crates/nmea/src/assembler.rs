use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::Sentence;

/// A fully reassembled AIS message: one or more sentences whose payloads
/// have been concatenated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Group {
    /// Original lines, in fragment order.
    pub sentences: Vec<String>,
    /// Concatenated armored payload.
    pub payload: String,
    /// Fill bits of the final fragment.
    pub fill_bits: u8,
    pub channel: String,
}

struct Pending {
    parts: Vec<Option<Sentence>>,
    have: usize,
    first_seen: Instant,
}

/// Reassembles multi-fragment AIVDM groups. Run one `Assembler` per
/// connection/source; it is not thread-safe by design.
pub struct Assembler {
    ttl: Duration,
    pending: HashMap<String, Pending>,
}

impl Assembler {
    /// Incomplete groups older than `ttl` are discarded (checked lazily on
    /// `add`).
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            pending: HashMap::new(),
        }
    }

    /// Feed one sentence; returns a completed `Group` or `None` if more
    /// fragments are needed.
    pub fn add(&mut self, s: Sentence, now: Instant) -> Option<Group> {
        if s.fragments == 1 {
            return Some(Group {
                sentences: vec![s.raw],
                payload: s.payload,
                fill_bits: s.fill_bits,
                channel: s.channel,
            });
        }
        self.evict(now);

        let key = format!("{}|{}|{}", s.talker, s.channel, s.seq_id);
        let total = s.fragments as usize;
        let p = self.pending.entry(key.clone()).or_insert_with(|| Pending {
            parts: vec![None; total],
            have: 0,
            first_seen: now,
        });
        if p.parts.len() != total {
            // Same key reused with a different fragment count: start over.
            *p = Pending {
                parts: vec![None; total],
                have: 0,
                first_seen: now,
            };
        }
        let idx = (s.fragment - 1) as usize;
        if p.parts[idx].is_none() {
            p.have += 1;
        }
        p.parts[idx] = Some(s);
        if p.have < total {
            return None;
        }
        let p = self.pending.remove(&key).unwrap();

        let mut g = Group {
            sentences: Vec::with_capacity(total),
            payload: String::new(),
            fill_bits: 0,
            channel: String::new(),
        };
        for part in p.parts.into_iter().flatten() {
            g.payload.push_str(&part.payload);
            g.fill_bits = part.fill_bits;
            g.channel = part.channel;
            g.sentences.push(part.raw);
        }
        Some(g)
    }

    fn evict(&mut self, now: Instant) {
        let ttl = self.ttl;
        self.pending
            .retain(|_, p| now.duration_since(p.first_seen) <= ttl);
    }
}

impl Default for Assembler {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse;

    #[test]
    fn single_fragment_passes_through() {
        let mut a = Assembler::default();
        let s = parse("!AIVDM,1,1,,B,177KQJ5000G?tO`K>RA1wUbN0TKH,0*5C").unwrap();
        let g = a.add(s, Instant::now()).unwrap();
        assert_eq!(g.payload, "177KQJ5000G?tO`K>RA1wUbN0TKH");
        assert_eq!(g.fill_bits, 0);
    }

    #[test]
    fn reassembles_two_fragments_any_order() {
        let f1 = "!AIVDM,2,1,3,B,55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E53,0*3E";
        let f2 = "!AIVDM,2,2,3,B,1@0000000000000,2*55";
        for order in [[f1, f2], [f2, f1]] {
            let mut a = Assembler::default();
            let now = Instant::now();
            assert!(a.add(parse(order[0]).unwrap(), now).is_none());
            let g = a.add(parse(order[1]).unwrap(), now).unwrap();
            assert_eq!(g.payload.len(), 56 + 15);
            assert_eq!(g.fill_bits, 2);
            assert_eq!(g.sentences, vec![f1.to_string(), f2.to_string()]);
        }
    }

    #[test]
    fn evicts_stale_partials() {
        let f1 = "!AIVDM,2,1,3,B,55P5TL01VIaAL@7WKO@mBplU@<PDhh000000001S;AJ::4A80?4i@E53,0*3E";
        let f2 = "!AIVDM,2,2,3,B,1@0000000000000,2*55";
        let mut a = Assembler::new(Duration::from_secs(5));
        let t0 = Instant::now();
        assert!(a.add(parse(f1).unwrap(), t0).is_none());
        // First fragment expires before its partner shows up.
        let t1 = t0 + Duration::from_secs(10);
        assert!(a.add(parse(f2).unwrap(), t1).is_none());
    }
}
