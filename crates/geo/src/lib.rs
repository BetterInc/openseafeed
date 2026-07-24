//! Geohash encoding and bounding-box covers.
//!
//! Decoded AIS messages are published on NATS subjects shaped
//! `ais.decoded.<c1>.<c2>.<c3>.<c4>` — one token per geohash character — so
//! a fan-out service can subscribe to exactly the cells its clients' bounding
//! boxes touch, using a token wildcard (`ais.decoded.u.>`) for coarse cells.

const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

/// Encode a position as a geohash of `precision` characters.
pub fn encode(lat: f64, lon: f64, precision: usize) -> String {
    let mut min_lat = -90.0f64;
    let mut max_lat = 90.0f64;
    let mut min_lon = -180.0f64;
    let mut max_lon = 180.0f64;
    let mut hash = String::with_capacity(precision);
    let mut even = true; // longitude bit first
    let mut bit = 0;
    let mut ch: usize = 0;
    while hash.len() < precision {
        if even {
            let mid = (min_lon + max_lon) / 2.0;
            if lon >= mid {
                ch = (ch << 1) | 1;
                min_lon = mid;
            } else {
                ch <<= 1;
                max_lon = mid;
            }
        } else {
            let mid = (min_lat + max_lat) / 2.0;
            if lat >= mid {
                ch = (ch << 1) | 1;
                min_lat = mid;
            } else {
                ch <<= 1;
                max_lat = mid;
            }
        }
        even = !even;
        bit += 1;
        if bit == 5 {
            hash.push(BASE32[ch] as char);
            bit = 0;
            ch = 0;
        }
    }
    hash
}

/// A latitude/longitude bounding box.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct BBox {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

impl BBox {
    /// Build from two opposite corners in any order, clamped to valid range.
    pub fn from_corners(a: (f64, f64), b: (f64, f64)) -> Self {
        Self {
            min_lat: a.0.min(b.0).clamp(-90.0, 90.0),
            max_lat: a.0.max(b.0).clamp(-90.0, 90.0),
            min_lon: a.1.min(b.1).clamp(-180.0, 180.0),
            max_lon: a.1.max(b.1).clamp(-180.0, 180.0),
        }
    }

    pub const WORLD: BBox = BBox {
        min_lat: -90.0,
        max_lat: 90.0,
        min_lon: -180.0,
        max_lon: 180.0,
    };

    pub fn contains(&self, lat: f64, lon: f64) -> bool {
        lat >= self.min_lat && lat <= self.max_lat && lon >= self.min_lon && lon <= self.max_lon
    }

    fn intersects(&self, o: &BBox) -> bool {
        self.min_lat <= o.max_lat
            && self.max_lat >= o.min_lat
            && self.min_lon <= o.max_lon
            && self.max_lon >= o.min_lon
    }

    fn inside(&self, o: &BBox) -> bool {
        self.min_lat >= o.min_lat
            && self.max_lat <= o.max_lat
            && self.min_lon >= o.min_lon
            && self.max_lon <= o.max_lon
    }

    /// Surface area in square degrees (rough tier-limit metric).
    pub fn area_deg2(&self) -> f64 {
        (self.max_lat - self.min_lat) * (self.max_lon - self.min_lon)
    }
}

#[derive(Debug, Clone)]
struct Cell {
    prefix: String,
    bounds: BBox,
    /// Whether the next geohash bit refines longitude.
    even: bool,
    /// Fully inside the target bbox — no need to refine further.
    done: bool,
}

impl Cell {
    fn world() -> Self {
        Cell {
            prefix: String::new(),
            bounds: BBox::WORLD,
            even: true,
            done: false,
        }
    }

    fn children(&self) -> Vec<Cell> {
        let mut out = Vec::with_capacity(32);
        for (i, &c) in BASE32.iter().enumerate() {
            let mut b = self.bounds;
            let mut even = self.even;
            for bit in (0..5).rev() {
                let set = i & (1 << bit) != 0;
                if even {
                    let mid = (b.min_lon + b.max_lon) / 2.0;
                    if set {
                        b.min_lon = mid;
                    } else {
                        b.max_lon = mid;
                    }
                } else {
                    let mid = (b.min_lat + b.max_lat) / 2.0;
                    if set {
                        b.min_lat = mid;
                    } else {
                        b.max_lat = mid;
                    }
                }
                even = !even;
            }
            let mut prefix = self.prefix.clone();
            prefix.push(c as char);
            out.push(Cell {
                prefix,
                bounds: b,
                even,
                done: false,
            });
        }
        out
    }
}

/// Compute a set of geohash prefixes covering `bbox`. Prefixes may have
/// mixed lengths up to `max_precision`; the result never exceeds
/// `max_cells` (falling back to coarser prefixes when it would). An empty
/// prefix in the result means "the whole world".
pub fn cover(bbox: &BBox, max_precision: usize, max_cells: usize) -> Vec<String> {
    let mut root = Cell::world();
    root.done = root.bounds.inside(bbox);
    let mut cells = vec![root];
    for _ in 0..max_precision {
        if cells.iter().all(|c| c.done) {
            break;
        }
        let mut next: Vec<Cell> = Vec::new();
        for cell in &cells {
            if cell.done {
                next.push(cell.clone());
                continue;
            }
            for mut child in cell.children() {
                if !child.bounds.intersects(bbox) {
                    continue;
                }
                child.done = child.bounds.inside(bbox);
                next.push(child);
            }
        }
        if next.len() > max_cells {
            // Refining further would exceed the budget; keep the coarser set.
            break;
        }
        cells = next;
    }
    cells.into_iter().map(|c| c.prefix).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_known_geohashes() {
        // Reference values from geohash.org.
        assert_eq!(encode(57.64911, 10.40744, 11), "u4pruydqqvj");
        assert_eq!(encode(42.6, -5.6, 5), "ezs42");
        assert_eq!(encode(0.0, 0.0, 4), "s000");
    }

    #[test]
    fn cover_world_is_empty_prefix() {
        let cells = cover(&BBox::WORLD, 4, 64);
        assert_eq!(cells, vec![String::new()]);
    }

    #[test]
    fn cover_contains_point_cell() {
        // A small box around Rotterdam must produce prefixes that cover the
        // port's geohash.
        let bbox = BBox::from_corners((51.8, 3.9), (52.1, 4.6));
        let cells = cover(&bbox, 4, 64);
        assert!(!cells.is_empty() && cells.len() <= 64);
        let port = encode(51.95, 4.1, 4);
        assert!(
            cells.iter().any(|p| port.starts_with(p.as_str())),
            "{port} not covered by {cells:?}"
        );
    }

    #[test]
    fn cover_respects_budget() {
        // A bbox spanning many level-4 cells must coarsen, not explode.
        let bbox = BBox::from_corners((30.0, -80.0), (60.0, 0.0));
        let cells = cover(&bbox, 4, 32);
        assert!(cells.len() <= 32, "{}", cells.len());
    }

    #[test]
    fn cover_excludes_far_away() {
        let bbox = BBox::from_corners((51.8, 3.9), (52.1, 4.6));
        let cells = cover(&bbox, 4, 64);
        let sydney = encode(-33.85, 151.2, 4);
        assert!(!cells.iter().any(|p| !p.is_empty() && sydney.starts_with(p.as_str())));
    }
}
