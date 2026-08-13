//! Ingest gateway: accepts NMEA AIS feeds over UDP, TCP and WebSocket,
//! reassembles multipart groups, and publishes raw envelopes to NATS
//! (`ais.raw.<station>`).
//!
//! Producers:
//! - UDP :10110 — raw NMEA datagrams (AIS-catcher style). Unauthenticated,
//!   enabled only when OSF_ALLOW_ANON_UDP=1 (dev/LAN use).
//! - TCP :10111 — first line `AUTH <key>`, then NMEA lines.
//! - WS  :8080 /v1/ingest?key=… — text frames of NMEA lines (also accepts
//!   `Authorization: Bearer <key>`).

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message as WsMessage, WebSocket};
use axum::extract::{Query, State, WebSocketUpgrade};
use axum::http::{HeaderMap as HttpHeaders, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use axum::Router;
use openseafeed_feed::{subjects, RawEnvelope};
use openseafeed_keys::{Kind, Validator};
use openseafeed_nmea::Assembler;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{TcpListener, UdpSocket};

#[derive(Default)]
struct Counters {
    lines: AtomicU64,
    invalid: AtomicU64,
    groups: AtomicU64,
    auth_failures: AtomicU64,
}

struct App {
    nats: async_nats::Client,
    validator: Arc<Validator>,
    counters: Counters,
}

impl App {
    /// Parse one NMEA line, feed the connection's assembler, publish any
    /// completed group.
    async fn handle_line(&self, asm: &mut Assembler, line: &str, station_id: &str, source: &str) {
        let line = line.trim();
        if line.is_empty() {
            return;
        }
        self.counters.lines.fetch_add(1, Ordering::Relaxed);
        let sentence = match openseafeed_nmea::parse(line) {
            Ok(s) => s,
            Err(_) => {
                self.counters.invalid.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        let Some(group) = asm.add(sentence, Instant::now()) else {
            return;
        };
        self.counters.groups.fetch_add(1, Ordering::Relaxed);
        let env = RawEnvelope {
            sentences: group.sentences,
            payload: group.payload,
            fill_bits: group.fill_bits,
            channel: group.channel,
            station_id: station_id.to_string(),
            source: source.to_string(),
            received_at_ms: now_ms(),
        };
        let subject = subjects::raw(station_id);
        match serde_json::to_vec(&env) {
            Ok(bytes) => {
                if let Err(e) = self.nats.publish(subject, bytes.into()).await {
                    tracing::error!(error = %e, "nats publish failed");
                }
            }
            Err(e) => tracing::error!(error = %e, "envelope serialize failed"),
        }
    }

    /// Validate a producer key (station or feed kind required).
    async fn producer_auth(&self, key: &str) -> Option<(String, String)> {
        let info = self.validator.validate(key).await?;
        if info.kind == Kind::Live {
            // Consumer keys cannot push data.
            return None;
        }
        let station = info
            .station_id
            .clone()
            .unwrap_or_else(|| format!("{}-{}", info.kind.as_str(), info.owner_id));
        let source = match info.kind {
            Kind::Feed => "partner".to_string(),
            _ => "rf".to_string(),
        };
        Some((station, source))
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let nats_url = std::env::var("OSF_NATS_URL").unwrap_or_else(|_| "nats://localhost:4222".into());
    let nats = async_nats::connect(&nats_url).await?;
    tracing::info!(nats_url, "ingest connected to nats");

    let app = Arc::new(App {
        nats,
        validator: Validator::from_env(),
        counters: Counters::default(),
    });

    let udp_addr = std::env::var("OSF_UDP_ADDR").unwrap_or_else(|_| "0.0.0.0:10110".into());
    let tcp_addr = std::env::var("OSF_TCP_ADDR").unwrap_or_else(|_| "0.0.0.0:10111".into());
    let http_addr = std::env::var("OSF_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8080".into());
    let allow_anon_udp = std::env::var("OSF_ALLOW_ANON_UDP")
        .map(|v| v == "1")
        .unwrap_or(true);

    if allow_anon_udp {
        tracing::warn!(udp_addr, "anonymous UDP ingest enabled (dev/LAN mode)");
        tokio::spawn(udp_listener(app.clone(), udp_addr));
    }
    tokio::spawn(tcp_listener(app.clone(), tcp_addr));
    tokio::spawn(stats_reporter(app.clone()));

    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/ingest", any(ws_ingest))
        .with_state(app.clone());
    let listener = TcpListener::bind(&http_addr).await?;
    tracing::info!(http_addr, "ws ingest listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn stats_reporter(app: Arc<App>) {
    loop {
        tokio::time::sleep(Duration::from_secs(30)).await;
        tracing::info!(
            lines = app.counters.lines.load(Ordering::Relaxed),
            invalid = app.counters.invalid.load(Ordering::Relaxed),
            groups = app.counters.groups.load(Ordering::Relaxed),
            auth_failures = app.counters.auth_failures.load(Ordering::Relaxed),
            "ingest stats"
        );
    }
}

// ---------------------------------------------------------------- UDP

async fn udp_listener(app: Arc<App>, addr: String) {
    let sock = match UdpSocket::bind(&addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(addr, error = %e, "udp bind failed");
            return;
        }
    };
    // Multipart fragments arrive in separate datagrams, so each remote IP
    // keeps its own assembler.
    let mut assemblers: HashMap<IpAddr, (Assembler, Instant)> = HashMap::new();
    let mut last_sweep = Instant::now();
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let (n, peer) = match sock.recv_from(&mut buf).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "udp recv error");
                continue;
            }
        };
        let text = String::from_utf8_lossy(&buf[..n]).into_owned();
        let station = format!("udp-{}", subjects::sanitize_token(&peer.ip().to_string()));
        let entry = assemblers
            .entry(peer.ip())
            .or_insert_with(|| (Assembler::default(), Instant::now()));
        entry.1 = Instant::now();
        for line in text.lines() {
            app.handle_line(&mut entry.0, line, &station, "udp-anon")
                .await;
        }
        if last_sweep.elapsed() > Duration::from_secs(60) {
            assemblers.retain(|_, (_, seen)| seen.elapsed() < Duration::from_secs(300));
            last_sweep = Instant::now();
        }
    }
}

// ---------------------------------------------------------------- TCP

async fn tcp_listener(app: Arc<App>, addr: String) {
    let listener = match TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr, error = %e, "tcp bind failed");
            return;
        }
    };
    tracing::info!(addr, "tcp ingest listening");
    loop {
        let (stream, peer) = match listener.accept().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, "tcp accept error");
                continue;
            }
        };
        let app = app.clone();
        tokio::spawn(async move {
            let mut lines = BufReader::new(stream).lines();
            // First line must be `AUTH <key>`.
            let auth = match tokio::time::timeout(Duration::from_secs(10), lines.next_line()).await
            {
                Ok(Ok(Some(l))) => l,
                _ => return,
            };
            let key = match auth.strip_prefix("AUTH ") {
                Some(k) => k.trim(),
                None => {
                    app.counters.auth_failures.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            let Some((station, source)) = app.producer_auth(key).await else {
                app.counters.auth_failures.fetch_add(1, Ordering::Relaxed);
                tracing::debug!(%peer, "tcp auth rejected");
                return;
            };
            tracing::info!(%peer, station, "tcp producer connected");
            let mut asm = Assembler::default();
            while let Ok(Some(line)) = lines.next_line().await {
                app.handle_line(&mut asm, &line, &station, &source).await;
            }
            tracing::info!(%peer, station, "tcp producer disconnected");
        });
    }
}

// ---------------------------------------------------------------- WebSocket

#[derive(serde::Deserialize)]
struct WsParams {
    key: Option<String>,
}

async fn ws_ingest(
    State(app): State<Arc<App>>,
    Query(params): Query<WsParams>,
    headers: HttpHeaders,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    let key = params.key.clone().or_else(|| {
        headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::to_string)
    });
    let Some(key) = key else {
        return (StatusCode::UNAUTHORIZED, "missing key").into_response();
    };
    let Some((station, source)) = app.producer_auth(&key).await else {
        app.counters.auth_failures.fetch_add(1, Ordering::Relaxed);
        return (StatusCode::FORBIDDEN, "invalid key").into_response();
    };
    ws.on_upgrade(move |socket| ws_pump(app, socket, station, source))
        .into_response()
}

async fn ws_pump(app: Arc<App>, mut socket: WebSocket, station: String, source: String) {
    tracing::info!(station, "ws producer connected");
    let mut asm = Assembler::default();
    while let Some(Ok(msg)) = socket.recv().await {
        match msg {
            WsMessage::Text(text) => {
                for line in text.lines() {
                    app.handle_line(&mut asm, line, &station, &source).await;
                }
            }
            WsMessage::Close(_) => break,
            _ => {}
        }
    }
    tracing::info!(station, "ws producer disconnected");
}
