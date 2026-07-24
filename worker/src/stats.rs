use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Pipeline counters shared between the reader, writer and reporter tasks.
#[derive(Debug, Default)]
pub struct Stats {
    pub lines_in: AtomicU64,
    pub forwarded: AtomicU64,
    pub invalid: AtomicU64,
    pub dropped: AtomicU64,
    pub reconnects: AtomicU64,
}

impl Stats {
    pub fn incr(field: &AtomicU64) {
        field.fetch_add(1, Ordering::Relaxed);
    }

    pub fn add(field: &AtomicU64, n: u64) {
        field.fetch_add(n, Ordering::Relaxed);
    }
}

/// Log a stats line every 30s. Runs until aborted.
pub async fn report_loop(stats: Arc<Stats>) {
    let mut ticker = tokio::time::interval(Duration::from_secs(30));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Skip the immediate first tick so the first report reflects real traffic.
    ticker.tick().await;
    loop {
        ticker.tick().await;
        tracing::info!(
            lines_in = stats.lines_in.load(Ordering::Relaxed),
            forwarded = stats.forwarded.load(Ordering::Relaxed),
            invalid = stats.invalid.load(Ordering::Relaxed),
            dropped = stats.dropped.load(Ordering::Relaxed),
            reconnects = stats.reconnects.load(Ordering::Relaxed),
            "stats"
        );
    }
}
