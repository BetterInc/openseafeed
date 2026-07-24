use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;

use crate::backoff::Backoff;
use crate::cli::Upstream;
use crate::queue::LineQueue;
use crate::stats::Stats;

/// Read from a network upstream forever, reconnecting with backoff. Each
/// received line is validated and, if valid, pushed to the queue.
pub async fn run_network(up: Upstream, queue: Arc<LineQueue>, stats: Arc<Stats>) {
    let mut backoff = Backoff::new();
    loop {
        let outcome = match &up {
            Upstream::Tcp { host, port } => {
                read_tcp(host, *port, &queue, &stats, &mut backoff).await
            }
            Upstream::Ws { url } => read_ws(url, &queue, &stats, &mut backoff).await,
            Upstream::Finland => crate::finland::read(&queue, &stats, &mut backoff).await,
        };
        match outcome {
            Ok(()) => tracing::warn!("upstream closed the connection, reconnecting"),
            Err(e) => tracing::warn!(error = %e, "upstream error, reconnecting"),
        }
        Stats::incr(&stats.reconnects);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn read_tcp(
    host: &str,
    port: u16,
    queue: &LineQueue,
    stats: &Stats,
    backoff: &mut Backoff,
) -> Result<()> {
    let stream = TcpStream::connect((host, port))
        .await
        .with_context(|| format!("connecting to tcp://{host}:{port}"))?;
    backoff.reset();
    tracing::info!(%host, port, "connected to upstream");
    let mut lines = BufReader::new(stream).lines();
    while let Some(line) = lines.next_line().await? {
        ingest_line(&line, queue, stats);
    }
    Ok(())
}

async fn read_ws(
    url: &str,
    queue: &LineQueue,
    stats: &Stats,
    backoff: &mut Backoff,
) -> Result<()> {
    let (ws, _resp) = tokio_tungstenite::connect_async(url)
        .await
        .with_context(|| format!("connecting to {url}"))?;
    backoff.reset();
    tracing::info!(%url, "connected to upstream");
    let (_sink, mut stream) = ws.split();
    while let Some(msg) = stream.next().await {
        match msg? {
            Message::Text(text) => ingest_frame(text.as_str(), queue, stats),
            Message::Binary(bytes) => {
                if let Ok(text) = std::str::from_utf8(&bytes) {
                    ingest_frame(text, queue, stats);
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }
    Ok(())
}

/// Read NMEA lines from a file. When `loop_forever` is false this returns
/// after one pass (letting the pipeline drain and exit).
pub async fn run_replay(
    path: PathBuf,
    loop_forever: bool,
    rate: u32,
    queue: Arc<LineQueue>,
    stats: Arc<Stats>,
) -> Result<()> {
    let rate = rate.max(1);
    let period = Duration::from_secs_f64(1.0 / rate as f64);
    loop {
        let file = tokio::fs::File::open(&path)
            .await
            .with_context(|| format!("opening replay file {}", path.display()))?;
        let mut lines = BufReader::new(file).lines();
        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while let Some(line) = lines.next_line().await? {
            if line.trim().is_empty() {
                continue;
            }
            ticker.tick().await;
            ingest_line(&line, &queue, &stats);
        }
        if !loop_forever {
            return Ok(());
        }
    }
}

/// Listen for local NMEA datagrams forever (e.g. AIS-catcher UDP output).
/// A rebind is attempted with backoff if the socket errors.
pub async fn run_listen_udp(addr: String, queue: Arc<LineQueue>, stats: Arc<Stats>) {
    let mut backoff = Backoff::new();
    loop {
        match listen_udp_once(&addr, &queue, &stats, &mut backoff).await {
            Ok(()) => {}
            Err(e) => tracing::warn!(error = %e, "udp listener error, rebinding"),
        }
        Stats::incr(&stats.reconnects);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn listen_udp_once(
    addr: &str,
    queue: &LineQueue,
    stats: &Stats,
    backoff: &mut Backoff,
) -> Result<()> {
    let socket = tokio::net::UdpSocket::bind(addr)
        .await
        .with_context(|| format!("binding udp {addr}"))?;
    backoff.reset();
    tracing::info!(%addr, "listening for NMEA datagrams");
    let mut buf = vec![0u8; 65536];
    loop {
        let (n, _from) = socket.recv_from(&mut buf).await?;
        if let Ok(text) = std::str::from_utf8(&buf[..n]) {
            ingest_frame(text, queue, stats);
        }
    }
}

/// A UDP frame or WS frame may carry several newline-separated lines.
fn ingest_frame(frame: &str, queue: &LineQueue, stats: &Stats) {
    for line in frame.split(['\n', '\r']) {
        if !line.trim().is_empty() {
            ingest_line(line, queue, stats);
        }
    }
}

/// Validate one line and enqueue the original text if it parses.
pub fn ingest_line(line: &str, queue: &LineQueue, stats: &Stats) {
    Stats::incr(&stats.lines_in);
    match openseafeed_nmea::parse(line) {
        Ok(_) => {
            if queue.push(line.to_string()) {
                Stats::incr(&stats.dropped);
            }
        }
        Err(_) => Stats::incr(&stats.invalid),
    }
}
