use std::time::{Duration, SystemTime, UNIX_EPOCH};

const MIN: Duration = Duration::from_millis(250);
const MAX: Duration = Duration::from_secs(30);

/// Exponential backoff with full jitter, from 250ms up to a 30s cap.
#[derive(Debug)]
pub struct Backoff {
    current: Duration,
}

impl Backoff {
    pub fn new() -> Self {
        Self { current: MIN }
    }

    /// Reset after a successful connection so the next failure starts small.
    pub fn reset(&mut self) {
        self.current = MIN;
    }

    /// Return the next delay to wait and advance the schedule.
    pub fn next_delay(&mut self) -> Duration {
        // Full jitter: sleep a random amount in [0, current].
        let ceiling = self.current;
        let delay = jitter(ceiling);
        self.current = (self.current * 2).min(MAX);
        delay
    }
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new()
    }
}

/// Cheap uniform-ish jitter in [0, ceiling] without an rng dependency.
fn jitter(ceiling: Duration) -> Duration {
    let ceil_nanos = ceiling.as_nanos() as u64;
    if ceil_nanos == 0 {
        return Duration::ZERO;
    }
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // xorshift64 to spread the low-entropy clock bits.
    let mut x = seed | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    Duration::from_nanos(x % (ceil_nanos + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grows_and_caps() {
        let mut b = Backoff::new();
        for _ in 0..20 {
            let d = b.next_delay();
            assert!(d <= MAX);
        }
        // After many failures the ceiling is pinned at MAX.
        assert_eq!(b.current, MAX);
    }

    #[test]
    fn reset_returns_to_min() {
        let mut b = Backoff::new();
        for _ in 0..10 {
            b.next_delay();
        }
        b.reset();
        assert_eq!(b.current, MIN);
    }
}
