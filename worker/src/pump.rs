use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use crate::cli::{Ingest, Upstream};
use crate::queue::LineQueue;
use crate::stats::{self, Stats};
use crate::{ingest, upstream};

const QUEUE_CAPACITY: usize = 10_000;

/// Where the pipeline gets its NMEA lines.
pub enum Source {
    Network(Upstream),
    ListenUdp(String),
    Replay {
        path: PathBuf,
        loop_forever: bool,
        rate: u32,
    },
}

/// Wire a reader, a writer and a stats reporter around a shared bounded queue,
/// then run until ctrl-c (or, for a finite replay, until the source drains).
pub async fn run(
    source: Source,
    target: Ingest,
    key: Option<String>,
    source_label: String,
) -> Result<()> {
    tracing::info!(source = %source_label, "worker starting");
    let stats = Arc::new(Stats::default());
    let queue = Arc::new(LineQueue::new(QUEUE_CAPACITY));

    let writer = tokio::spawn(ingest::run(target, key, queue.clone(), stats.clone()));
    let reporter = tokio::spawn(stats::report_loop(stats.clone()));

    let reader_queue = queue.clone();
    let reader_stats = stats.clone();
    let mut reader = tokio::spawn(async move {
        match source {
            Source::Network(up) => upstream::run_network(up, reader_queue, reader_stats).await,
            Source::ListenUdp(addr) => {
                upstream::run_listen_udp(addr, reader_queue, reader_stats).await
            }
            Source::Replay {
                path,
                loop_forever,
                rate,
            } => {
                if let Err(e) =
                    upstream::run_replay(path, loop_forever, rate, reader_queue, reader_stats).await
                {
                    tracing::error!(error = %e, "replay failed");
                }
            }
        }
    });

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received, shutting down");
            reader.abort();
        }
        _ = &mut reader => {
            tracing::info!("source exhausted, flushing buffer");
            drain(&queue).await;
        }
    }

    writer.abort();
    reporter.abort();
    log_final(&stats);
    Ok(())
}

/// Wait for the writer to empty the queue, with a hard timeout.
async fn drain(queue: &LineQueue) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !queue.is_empty() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // Small grace for the final batch already popped by the writer to land.
    tokio::time::sleep(Duration::from_millis(200)).await;
}

fn log_final(stats: &Stats) {
    tracing::info!(
        lines_in = stats.lines_in.load(Ordering::Relaxed),
        forwarded = stats.forwarded.load(Ordering::Relaxed),
        invalid = stats.invalid.load(Ordering::Relaxed),
        dropped = stats.dropped.load(Ordering::Relaxed),
        reconnects = stats.reconnects.load(Ordering::Relaxed),
        "final stats"
    );
}
