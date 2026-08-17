//! OpenSeaFeed control plane library.
//!
//! An axum HTTP service that owns accounts, API keys, tiers, and the station
//! registry, backed by a single SQLite file. Multi-provider sign-in (GitHub,
//! Google, and email magic links) is a hard requirement: the network this
//! replaces died when its sole login path went away, so every provider here
//! is optional and independent — a missing provider disables only its own
//! routes, never sign-in as a whole.
//!
//! The binary in `main.rs` is a thin wrapper around [`run`]; the router and
//! state are exposed so integration tests can drive the exact routes the
//! server serves.

pub mod api;
pub mod auth;
pub mod db;
pub mod internal;
pub mod pages;
pub mod session;

use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use rand::RngCore;
use rusqlite::Connection;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// OAuth credentials for one provider. Absent when either half is unset.
#[derive(Clone)]
pub struct OAuthProvider {
    pub client_id: String,
    pub client_secret: String,
}

/// Runtime configuration, resolved once from the environment at startup.
pub struct Config {
    pub addr: SocketAddr,
    pub db_path: String,
    pub public_url: String,
    pub session_secret: Vec<u8>,
    pub internal_token: String,
    pub github: Option<OAuthProvider>,
    pub google: Option<OAuthProvider>,
    pub smtp_url: Option<String>,
    pub smtp_from: String,
}

fn provider_from_env(id_var: &str, secret_var: &str) -> Option<OAuthProvider> {
    match (std::env::var(id_var).ok(), std::env::var(secret_var).ok()) {
        (Some(client_id), Some(client_secret))
            if !client_id.is_empty() && !client_secret.is_empty() =>
        {
            Some(OAuthProvider {
                client_id,
                client_secret,
            })
        }
        _ => None,
    }
}

impl Config {
    /// Resolve configuration from the process environment.
    pub fn from_env() -> Result<Self> {
        let addr = std::env::var("OSF_CONTROL_ADDR")
            .unwrap_or_else(|_| "0.0.0.0:8083".to_string())
            .parse()
            .context("OSF_CONTROL_ADDR must be host:port")?;

        let public_url =
            std::env::var("OSF_PUBLIC_URL").unwrap_or_else(|_| "http://localhost:8083".to_string());

        // A random per-boot secret is fine for dev (sessions just don't
        // survive a restart); production should pin OSF_SESSION_SECRET.
        let session_secret = match std::env::var("OSF_SESSION_SECRET") {
            Ok(s) if !s.is_empty() => s.into_bytes(),
            _ => {
                tracing::warn!(
                    "OSF_SESSION_SECRET unset; using a random secret (sessions reset on restart)"
                );
                let mut buf = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut buf);
                buf.to_vec()
            }
        };

        Ok(Config {
            addr,
            db_path: std::env::var("OSF_DB_PATH").unwrap_or_else(|_| "./control.db".to_string()),
            public_url,
            session_secret,
            internal_token: std::env::var("OSF_INTERNAL_TOKEN")
                .unwrap_or_else(|_| "dev-internal-token".to_string()),
            github: provider_from_env("OSF_GITHUB_CLIENT_ID", "OSF_GITHUB_CLIENT_SECRET"),
            google: provider_from_env("OSF_GOOGLE_CLIENT_ID", "OSF_GOOGLE_CLIENT_SECRET"),
            smtp_url: std::env::var("OSF_SMTP_URL").ok().filter(|s| !s.is_empty()),
            smtp_from: std::env::var("OSF_SMTP_FROM")
                .ok()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| "OpenSeaFeed <no-reply@openseafeed.com>".to_string()),
        })
    }

    /// Internal token used by the test configuration.
    pub const TEST_INTERNAL_TOKEN: &'static str = "test-internal-token";

    /// A fixed configuration for tests: no OAuth providers, no SMTP, and
    /// stable secrets so signed cookies verify across a test's requests.
    pub fn for_test() -> Self {
        Config {
            addr: "127.0.0.1:0".parse().unwrap(),
            db_path: ":memory:".to_string(),
            public_url: "http://localhost:8083".to_string(),
            session_secret: b"test-session-secret".to_vec(),
            internal_token: Self::TEST_INTERNAL_TOKEN.to_string(),
            github: None,
            google: None,
            smtp_url: None,
            smtp_from: "OpenSeaFeed <no-reply@openseafeed.com>".to_string(),
        }
    }
}

/// Shared handler state. The SQLite connection is single and mutex-guarded —
/// ample for the control plane's low write volume and what lets `:memory:`
/// databases work in tests (an in-memory DB lives only as long as its one
/// connection).
#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Mutex<Connection>>,
    pub cfg: Arc<Config>,
    pub http: reqwest::Client,
}

impl AppState {
    /// Build state around an already-open connection. Used by both `run` and
    /// the test harness.
    pub fn new(conn: Connection, cfg: Config) -> Self {
        let http = reqwest::Client::builder()
            .user_agent("openseafeed-control")
            .build()
            .expect("reqwest client builds");
        AppState {
            db: Arc::new(Mutex::new(conn)),
            cfg: Arc::new(cfg),
            http,
        }
    }

    /// The session-signing secret (exposed so tests can mint valid cookies).
    pub fn session_secret(&self) -> &[u8] {
        &self.cfg.session_secret
    }

    /// Resolve the signed-in user from request headers, if any.
    pub async fn current_user(&self, headers: &HeaderMap) -> Option<db::User> {
        let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
        let value = session::from_cookie_header(cookie)?;
        let user_id = session::verify(&self.cfg.session_secret, value)?;
        let conn = self.db.lock().await;
        db::user_by_id(&conn, &user_id).ok().flatten()
    }
}

/// Uniform JSON error body used across handlers.
pub fn json_error(status: StatusCode, message: &str) -> Response {
    (status, Json(serde_json::json!({ "error": message }))).into_response()
}

/// Assemble the full router. Kept public so integration tests exercise the
/// exact routes the server serves.
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(pages::landing))
        .route("/dashboard", get(pages::dashboard))
        .route("/healthz", get(healthz))
        // Auth
        .route("/auth/github", get(auth::github_start))
        .route("/auth/github/callback", get(auth::github_callback))
        .route("/auth/google", get(auth::google_start))
        .route("/auth/google/callback", get(auth::google_callback))
        .route("/auth/magic", post(auth::magic_request))
        .route("/auth/magic/verify", get(auth::magic_verify))
        .route("/auth/logout", post(auth::logout))
        // Session-authed API
        .route("/v1/me", get(api::me))
        .route("/v1/keys", post(api::create_key))
        .route("/v1/keys/{key}", delete(api::revoke_key))
        .route(
            "/v1/stations",
            post(api::create_station).get(api::list_stations),
        )
        // Public, CORS-open vessel enrichment used by the live map. NOT under
        // /v1/vessels: the api ingress routes that prefix to the snapshotter.
        .route("/v1/photos/{mmsi}", get(api::vessel_photo))
        // Internal (shared-secret) API
        .route("/v1/internal/keys/validate", get(internal::validate_key))
        .route("/v1/internal/stations/heartbeat", post(internal::heartbeat))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "ok"
}

/// Install tracing, open the database, and serve until ctrl-c.
pub async fn run() -> Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cfg = Config::from_env()?;
    let addr = cfg.addr;
    let conn = db::open(&cfg.db_path).context("opening control database")?;
    tracing::info!(db = %cfg.db_path, "control database ready");
    tracing::info!(
        github = cfg.github.is_some(),
        google = cfg.google.is_some(),
        magic_link = true,
        smtp = cfg.smtp_url.is_some(),
        "sign-in providers"
    );

    let state = AppState::new(conn, cfg);
    let app = router(state);
    let listener = TcpListener::bind(addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    tracing::info!(%addr, "control plane listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("server error")?;
    Ok(())
}

async fn shutdown_signal() {
    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("shutdown signal received"),
        Err(err) => tracing::error!(%err, "failed to install ctrl-c handler"),
    }
}
