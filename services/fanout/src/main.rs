//! Fan-out: streams decoded AIS messages to WebSocket clients using the
//! aisstream.io v0 wire protocol, so existing aisstream clients work by
//! changing only the URL.
//!
//! Protocol: the client connects to `/v1/stream` and must send a
//! subscription JSON within 3 seconds:
//! `{"APIKey": "...", "BoundingBoxes": [[[lat,lon],[lat,lon]], ...],
//!   "FiltersShipMMSI": ["..."], "FilterMessageTypes": ["PositionReport"]}`
//! Re-sending a subscription on the same socket swaps it in place.
//!
//! Routing: each client's bounding boxes are covered by geohash prefixes and
//! turned into NATS subscriptions on `ais.decoded.<c>.<c>...`; precise
//! filtering then uses the message headers only (no JSON parsing on the hot
//! path).

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as WsMessage, WebSocket};
use axum::extract::{State, WebSocketUpgrade};
use axum::response::IntoResponse;
use axum::routing::{any, get};
use axum::Router;
use futures::stream::{SelectAll, StreamExt};
use openseafeed_feed::{headers, subjects};
use openseafeed_geo::BBox;
use openseafeed_keys::Validator;
use serde::Deserialize;

/// Geohash precision used for subject covers: 3 chars ≈ 1.4 deg cells, a good
/// balance between subscription count and over-delivery filtered locally.
const COVER_PRECISION: usize = 3;
const MAX_CELLS_PER_CLIENT: usize = 48;
/// Free-tier ceiling on the summed bounding-box area, in square degrees.
const FREE_TIER_MAX_AREA: f64 = 30_000.0;

struct App {
    nats: async_nats::Client,
    validator: Arc<Validator>,
}

#[derive(Deserialize, Debug)]
struct Subscription {
    #[serde(rename = "APIKey", default)]
    api_key: String,
    #[serde(rename = "BoundingBoxes")]
    bounding_boxes: Vec<Vec<[f64; 2]>>,
    #[serde(rename = "FiltersShipMMSI", default)]
    filters_ship_mmsi: Vec<String>,
    #[serde(rename = "FilterMessageTypes", default)]
    filter_message_types: Vec<String>,
}

struct ClientFilter {
    boxes: Vec<BBox>,
    mmsi: HashSet<String>,
    types: HashSet<String>,
}

impl ClientFilter {
    fn from_subscription(sub: &Subscription) -> Result<Self, &'static str> {
        if sub.bounding_boxes.is_empty() {
            return Err("BoundingBoxes must not be empty");
        }
        let mut boxes = Vec::new();
        for bb in &sub.bounding_boxes {
            if bb.len() != 2 {
                return Err("each bounding box needs exactly two [lat, lon] corners");
            }
            // Corners are [lat, lon] pairs in any order (aisstream.io format).
            boxes.push(BBox::from_corners(
                (bb[0][0], bb[0][1]),
                (bb[1][0], bb[1][1]),
            ));
        }
        Ok(Self {
            boxes,
            mmsi: sub.filters_ship_mmsi.iter().cloned().collect(),
            types: sub.filter_message_types.iter().cloned().collect(),
        })
    }

    fn total_area(&self) -> f64 {
        self.boxes.iter().map(|b| b.area_deg2()).sum()
    }

    /// Geohash prefixes covering all boxes, deduped and shadow-free.
    fn cover(&self) -> Vec<String> {
        let mut prefixes: Vec<String> = Vec::new();
        for b in &self.boxes {
            for p in openseafeed_geo::cover(b, COVER_PRECISION, MAX_CELLS_PER_CLIENT) {
                prefixes.push(p);
            }
        }
        prefixes.sort();
        prefixes.dedup();
        // Drop prefixes already covered by a shorter one ("u1" shadows "u12").
        let mut out: Vec<String> = Vec::new();
        for p in prefixes {
            if !out.iter().any(|q| p.starts_with(q.as_str())) {
                out.push(p);
            }
        }
        out
    }

    fn matches(&self, hdrs: &async_nats::HeaderMap) -> bool {
        let Some(lat) = header_f64(hdrs, headers::LAT) else {
            return false;
        };
        let Some(lon) = header_f64(hdrs, headers::LON) else {
            return false;
        };
        if !self.boxes.iter().any(|b| b.contains(lat, lon)) {
            return false;
        }
        if !self.mmsi.is_empty() {
            match hdrs.get(headers::MMSI) {
                Some(v) if self.mmsi.contains(v.as_str()) => {}
                _ => return false,
            }
        }
        if !self.types.is_empty() {
            match hdrs.get(headers::MSG_TYPE) {
                Some(v) if self.types.contains(v.as_str()) => {}
                _ => return false,
            }
        }
        true
    }
}

fn header_f64(hdrs: &async_nats::HeaderMap, name: &str) -> Option<f64> {
    hdrs.get(name)?.as_str().parse().ok()
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
    tracing::info!(nats_url, "fanout connected to nats");

    let app = Arc::new(App {
        nats,
        validator: Validator::from_env(),
    });

    let http_addr = std::env::var("OSF_HTTP_ADDR").unwrap_or_else(|_| "0.0.0.0:8081".into());
    let router = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/stream", any(ws_stream))
        // Live map: watch the stream fill in from a browser. When
        // OSF_LIVE_HTML points at a readable file (dev: the bind-mounted
        // web/live.html) it is re-read on every request so edits only need a
        // browser refresh; otherwise the copy embedded at compile time is
        // served.
        .route(
            "/",
            get(|| async {
                let from_disk = std::env::var("OSF_LIVE_HTML")
                    .ok()
                    .and_then(|p| std::fs::read_to_string(p).ok());
                axum::response::Html(
                    from_disk.unwrap_or_else(|| include_str!("../../../web/live.html").to_string()),
                )
            }),
        )
        .with_state(app);
    let listener = tokio::net::TcpListener::bind(&http_addr).await?;
    tracing::info!(http_addr, "fanout listening");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

async fn ws_stream(State(app): State<Arc<App>>, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        if let Err(reason) = client_session(app, socket).await {
            tracing::debug!(reason, "client session ended");
        }
    })
}

/// Apply a subscription message: validate, filter-build, tier-check,
/// NATS-subscribe. Returns the filter and merged subscriber stream.
async fn apply_subscription(
    app: &App,
    text: &str,
) -> Result<(ClientFilter, SelectAll<async_nats::Subscriber>), String> {
    let sub: Subscription =
        serde_json::from_str(text).map_err(|e| format!("invalid subscription JSON: {e}"))?;
    // No key = anonymous viewer (the public live map): welcome, but capped to
    // the free-tier area. A key that IS presented must still be valid.
    let tier = if sub.api_key.is_empty() {
        "free".to_string()
    } else {
        app.validator
            .validate(&sub.api_key)
            .await
            .ok_or("invalid API key")?
            .tier
    };
    let filter = ClientFilter::from_subscription(&sub).map_err(str::to_string)?;
    if tier != "contributor" && filter.total_area() > FREE_TIER_MAX_AREA {
        return Err(format!(
            "free tier is limited to {FREE_TIER_MAX_AREA} square degrees of bounding-box area; \
             contribute a receiver or feed to unlock unlimited streaming"
        ));
    }
    let mut subs = SelectAll::new();
    for prefix in filter.cover() {
        let subject = subjects::decoded_pattern(&prefix);
        let s = app
            .nats
            .subscribe(subject)
            .await
            .map_err(|e| format!("subscribe failed: {e}"))?;
        subs.push(s);
    }
    Ok((filter, subs))
}

async fn client_session(app: Arc<App>, mut socket: WebSocket) -> Result<(), &'static str> {
    // aisstream.io semantics: subscription must arrive within 3 seconds.
    let first = tokio::time::timeout(Duration::from_secs(3), socket.recv())
        .await
        .map_err(|_| "no subscription within 3s")?
        .ok_or("closed before subscribing")?
        .map_err(|_| "socket error")?;
    let WsMessage::Text(text) = first else {
        return Err("first message must be a text subscription");
    };

    let (mut filter, mut subs) = match apply_subscription(&app, &text).await {
        Ok(x) => x,
        Err(e) => {
            let _ = socket
                .send(WsMessage::Text(format!("{{\"error\":\"{e}\"}}").into()))
                .await;
            return Err("subscription rejected");
        }
    };
    tracing::info!(cells = subs.len(), "client subscribed");

    let mut sent: u64 = 0;
    let mut dropped: u64 = 0;
    loop {
        tokio::select! {
            nats_msg = subs.next() => {
                let Some(msg) = nats_msg else { break };
                if !filter.matches(&msg.headers.clone().unwrap_or_default()) {
                    continue;
                }
                let body = String::from_utf8_lossy(&msg.payload).into_owned();
                // A client that can't drain within 10s is dead or hopelessly
                // slow; disconnect rather than buffer without bound.
                match tokio::time::timeout(
                    Duration::from_secs(10),
                    socket.send(WsMessage::Text(body.into())),
                ).await {
                    Ok(Ok(())) => sent += 1,
                    Ok(Err(_)) => break,
                    Err(_) => { dropped += 1; break; }
                }
            }
            ws_msg = socket.recv() => {
                match ws_msg {
                    Some(Ok(WsMessage::Text(text))) => {
                        // Swap-and-replace resubscription.
                        match apply_subscription(&app, &text).await {
                            Ok((f, s)) => {
                                filter = f;
                                subs = s;
                                tracing::info!(cells = subs.len(), "client resubscribed");
                            }
                            Err(e) => {
                                let _ = socket
                                    .send(WsMessage::Text(format!("{{\"error\":\"{e}\"}}").into()))
                                    .await;
                            }
                        }
                    }
                    Some(Ok(WsMessage::Close(_))) | None => break,
                    Some(Ok(_)) => {}
                    Some(Err(_)) => break,
                }
            }
        }
    }
    tracing::info!(sent, dropped, "client disconnected");
    Ok(())
}
