use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};

/// Sliding-window duplicate detector. The same AIS transmission is often
/// heard by several stations; the first copy wins and later copies are
/// counted (they confirm coverage overlap).
pub struct Window {
    ttl: Duration,
    seen: HashMap<u64, Instant>,
    last_sweep: Instant,
}

impl Window {
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            seen: HashMap::new(),
            last_sweep: Instant::now(),
        }
    }

    /// Returns true if this payload was already seen inside the window.
    pub fn seen(&mut self, payload: &str, now: Instant) -> bool {
        self.sweep(now);
        let mut h = DefaultHasher::new();
        payload.hash(&mut h);
        let key = h.finish();
        match self.seen.get(&key) {
            Some(&at) if now.duration_since(at) <= self.ttl => true,
            _ => {
                self.seen.insert(key, now);
                false
            }
        }
    }

    fn sweep(&mut self, now: Instant) {
        // Amortized cleanup: at most once per ttl.
        if now.duration_since(self.last_sweep) < self.ttl {
            return;
        }
        let ttl = self.ttl;
        self.seen.retain(|_, &mut at| now.duration_since(at) <= ttl);
        self.last_sweep = now;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedupes_within_window() {
        let mut w = Window::new(Duration::from_secs(10));
        let t0 = Instant::now();
        assert!(!w.seen("payload-a", t0));
        assert!(w.seen("payload-a", t0 + Duration::from_secs(5)));
        assert!(!w.seen("payload-b", t0));
    }

    #[test]
    fn expires_after_window() {
        let mut w = Window::new(Duration::from_secs(10));
        let t0 = Instant::now();
        assert!(!w.seen("payload-a", t0));
        assert!(!w.seen("payload-a", t0 + Duration::from_secs(11)));
    }
}
