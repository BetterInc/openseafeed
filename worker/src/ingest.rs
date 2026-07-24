use std::sync::Arc;

use anyhow::{Context, Result};
use futures::SinkExt;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::MaybeTlsStream;
use tokio_tungstenite::WebSocketStream;

use crate::backoff::Backoff;
use crate::cli::{ws_ingest_url_with_key, Ingest};
use crate::queue::LineQueue;
use crate::stats::Stats;

/// Keep batches comfortably under a typical MTU to avoid UDP fragmentation.
const MAX_BATCH_LINES: usize = 32;
const MAX_BATCH_BYTES: usize = 1200;

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// A live connection to the ingest endpoint.
enum Conn {
    Udp { socket: UdpSocket, target: String },
    Tcp(TcpStream),
    Ws(Box<Ws>),
}

/// Consume validated lines from the queue and push them to ingest forever,
/// reconnecting with backoff and retrying the in-flight batch on failure.
pub async fn run(ingest: Ingest, key: Option<String>, queue: Arc<LineQueue>, stats: Arc<Stats>) {
    let mut backoff = Backoff::new();
    let mut pending: Option<Vec<String>> = None;

    loop {
        let mut conn = match connect(&ingest, key.as_deref()).await {
            Ok(c) => {
                backoff.reset();
                c
            }
            Err(e) => {
                tracing::warn!(error = %e, "ingest connect failed, retrying");
                Stats::incr(&stats.reconnects);
                tokio::time::sleep(backoff.next_delay()).await;
                continue;
            }
        };

        // Send batches until a write fails; hold the failed batch for retry.
        loop {
            let batch = match pending.take() {
                Some(b) => b,
                None => queue.recv_batch(MAX_BATCH_LINES, MAX_BATCH_BYTES).await,
            };
            match conn.send(&batch).await {
                Ok(()) => Stats::add(&stats.forwarded, batch.len() as u64),
                Err(e) => {
                    tracing::warn!(error = %e, "ingest write failed, reconnecting");
                    pending = Some(batch);
                    break;
                }
            }
        }

        Stats::incr(&stats.reconnects);
        tokio::time::sleep(backoff.next_delay()).await;
    }
}

async fn connect(ingest: &Ingest, key: Option<&str>) -> Result<Conn> {
    match ingest {
        Ingest::Udp { addr } => {
            let socket = UdpSocket::bind("0.0.0.0:0")
                .await
                .context("binding local udp socket")?;
            socket
                .connect(addr)
                .await
                .with_context(|| format!("resolving udp target {addr}"))?;
            tracing::info!(%addr, "ingest ready (udp)");
            Ok(Conn::Udp {
                socket,
                target: addr.clone(),
            })
        }
        Ingest::Tcp { addr } => {
            let mut stream = TcpStream::connect(addr)
                .await
                .with_context(|| format!("connecting tcp ingest {addr}"))?;
            let key = key.context("tcp ingest requires --key")?;
            stream
                .write_all(format!("AUTH {key}\n").as_bytes())
                .await
                .context("sending AUTH line")?;
            tracing::info!(%addr, "ingest ready (tcp, authenticated)");
            Ok(Conn::Tcp(stream))
        }
        Ingest::Ws { url } => {
            let key = key.context("ws ingest requires --key")?;
            let full = ws_ingest_url_with_key(url, key);
            let (ws, _resp) = tokio_tungstenite::connect_async(&full)
                .await
                .with_context(|| format!("connecting ws ingest {url}"))?;
            tracing::info!(%url, "ingest ready (ws)");
            Ok(Conn::Ws(Box::new(ws)))
        }
    }
}

impl Conn {
    async fn send(&mut self, batch: &[String]) -> Result<()> {
        let payload = batch.join("\n");
        match self {
            Conn::Udp { socket, target } => {
                socket
                    .send(payload.as_bytes())
                    .await
                    .with_context(|| format!("sending udp datagram to {target}"))?;
            }
            Conn::Tcp(stream) => {
                stream.write_all(payload.as_bytes()).await?;
                stream.write_all(b"\n").await?;
                stream.flush().await?;
            }
            Conn::Ws(ws) => {
                ws.send(Message::Text(payload.into())).await?;
            }
        }
        Ok(())
    }
}
