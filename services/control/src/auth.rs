//! Sign-in flows: GitHub OAuth, Google OAuth, and email magic links.
//!
//! Each provider is independent. If a provider's credentials are not
//! configured its routes answer `503` with a JSON hint rather than 404, so
//! the front end can tell "not enabled here" apart from "wrong URL". A
//! successful sign-in sets the signed `osf_session` cookie and redirects to
//! the dashboard.
//!
//! OAuth state (CSRF protection) rides in a short-lived cookie: we mint a
//! random value, hand it to the provider, and require the callback to echo
//! the same value the browser still holds. No server-side state needed.

use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{AppendHeaders, IntoResponse, Redirect, Response};
use axum::Json;
use rand::RngCore;
use serde::Deserialize;

use crate::{db, json_error, session, AppState, OAuthProvider};

const STATE_COOKIE: &str = "osf_oauth_state";

fn random_state() -> String {
    let mut buf = [0u8; 16];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

fn provider_disabled(name: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({
            "error": format!("{name} sign-in is not configured on this server"),
            "hint": format!("set OSF_{}_CLIENT_ID and OSF_{}_CLIENT_SECRET",
                name.to_uppercase(), name.to_uppercase()),
        })),
    )
        .into_response()
}

fn redirect_uri(state: &AppState, provider: &str) -> String {
    format!(
        "{}/auth/{provider}/callback",
        state.cfg.public_url.trim_end_matches('/')
    )
}

/// Redirect to a provider's authorize endpoint, planting the CSRF state
/// cookie on the way out.
fn start_redirect(authorize_url: String, state_value: &str) -> Response {
    let cookie = format!(
        "{STATE_COOKIE}={state_value}; Path=/; HttpOnly; SameSite=Lax; Max-Age=600"
    );
    (
        [(header::SET_COOKIE, cookie)],
        Redirect::to(&authorize_url),
    )
        .into_response()
}

/// Read back the CSRF state cookie and confirm it matches what the provider
/// returned.
fn check_state(headers: &HeaderMap, returned: &str) -> bool {
    let Some(cookie) = headers.get(header::COOKIE).and_then(|v| v.to_str().ok()) else {
        return false;
    };
    let mut expected = None;
    for pair in cookie.split(';') {
        if let Some(rest) = pair.trim().strip_prefix(&format!("{STATE_COOKIE}=")) {
            expected = Some(rest);
        }
    }
    expected == Some(returned) && !returned.is_empty()
}

/// Complete sign-in: set the session cookie, clear the CSRF cookie, redirect
/// to the dashboard.
fn finish_login(state: &AppState, user_id: &str) -> Response {
    let session_cookie = session::set_cookie_header(&state.cfg.session_secret, user_id);
    let clear_state = format!("{STATE_COOKIE}=; Path=/; HttpOnly; Max-Age=0");
    // `AppendHeaders` (not a plain array) so both `Set-Cookie` lines survive;
    // an array would insert and the second would overwrite the first.
    (
        AppendHeaders([
            (header::SET_COOKIE, session_cookie),
            (header::SET_COOKIE, clear_state),
        ]),
        Redirect::to("/dashboard"),
    )
        .into_response()
}

// --- GitHub ----------------------------------------------------------------

pub async fn github_start(State(state): State<AppState>) -> Response {
    let Some(provider) = state.cfg.github.clone() else {
        return provider_disabled("github");
    };
    let csrf = random_state();
    let url = format!(
        "https://github.com/login/oauth/authorize?client_id={}&redirect_uri={}&scope=read:user%20user:email&state={}",
        urlencoding::encode(&provider.client_id),
        urlencoding::encode(&redirect_uri(&state, "github")),
        csrf,
    );
    start_redirect(url, &csrf)
}

#[derive(Deserialize)]
pub struct CallbackQuery {
    #[serde(default)]
    code: String,
    #[serde(default)]
    state: String,
}

pub async fn github_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let Some(provider) = state.cfg.github.clone() else {
        return provider_disabled("github");
    };
    if !check_state(&headers, &q.state) {
        return json_error(StatusCode::BAD_REQUEST, "invalid oauth state");
    }
    if q.code.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "missing authorization code");
    }

    match github_exchange(&state, &provider, &q.code).await {
        Ok(user) => finish_login(&state, &user.id),
        Err(err) => {
            tracing::error!(%err, "github oauth failed");
            json_error(StatusCode::BAD_GATEWAY, "github sign-in failed")
        }
    }
}

async fn github_exchange(
    state: &AppState,
    provider: &OAuthProvider,
    code: &str,
) -> anyhow::Result<db::User> {
    #[derive(Deserialize)]
    struct Token {
        access_token: String,
    }
    let token: Token = state
        .http
        .post("https://github.com/login/oauth/access_token")
        .header(header::ACCEPT, "application/json")
        .form(&[
            ("client_id", provider.client_id.as_str()),
            ("client_secret", provider.client_secret.as_str()),
            ("code", code),
            ("redirect_uri", &redirect_uri(state, "github")),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    #[derive(Deserialize)]
    struct GhUser {
        id: i64,
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
        login: Option<String>,
        #[serde(default)]
        email: Option<String>,
    }
    let gh: GhUser = state
        .http
        .get("https://api.github.com/user")
        .bearer_auth(&token.access_token)
        .header(header::ACCEPT, "application/vnd.github+json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    // Primary email is a separate call unless the profile email is public.
    let email = match gh.email {
        Some(e) => Some(e),
        None => github_primary_email(state, &token.access_token).await.ok().flatten(),
    };

    let display = gh.name.or(gh.login);
    let conn = state.db.lock().await;
    db::upsert_oauth_user(
        &conn,
        "github",
        &gh.id.to_string(),
        email.as_deref(),
        display.as_deref(),
    )
}

async fn github_primary_email(
    state: &AppState,
    access_token: &str,
) -> anyhow::Result<Option<String>> {
    #[derive(Deserialize)]
    struct Email {
        email: String,
        primary: bool,
        verified: bool,
    }
    let emails: Vec<Email> = state
        .http
        .get("https://api.github.com/user/emails")
        .bearer_auth(access_token)
        .header(header::ACCEPT, "application/vnd.github+json")
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    Ok(emails
        .into_iter()
        .find(|e| e.primary && e.verified)
        .map(|e| e.email))
}

// --- Google ----------------------------------------------------------------

pub async fn google_start(State(state): State<AppState>) -> Response {
    let Some(provider) = state.cfg.google.clone() else {
        return provider_disabled("google");
    };
    let csrf = random_state();
    let url = format!(
        "https://accounts.google.com/o/oauth2/v2/auth?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}",
        urlencoding::encode(&provider.client_id),
        urlencoding::encode(&redirect_uri(&state, "google")),
        urlencoding::encode("openid email profile"),
        csrf,
    );
    start_redirect(url, &csrf)
}

pub async fn google_callback(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<CallbackQuery>,
) -> Response {
    let Some(provider) = state.cfg.google.clone() else {
        return provider_disabled("google");
    };
    if !check_state(&headers, &q.state) {
        return json_error(StatusCode::BAD_REQUEST, "invalid oauth state");
    }
    if q.code.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "missing authorization code");
    }

    match google_exchange(&state, &provider, &q.code).await {
        Ok(user) => finish_login(&state, &user.id),
        Err(err) => {
            tracing::error!(%err, "google oauth failed");
            json_error(StatusCode::BAD_GATEWAY, "google sign-in failed")
        }
    }
}

async fn google_exchange(
    state: &AppState,
    provider: &OAuthProvider,
    code: &str,
) -> anyhow::Result<db::User> {
    #[derive(Deserialize)]
    struct Token {
        access_token: String,
    }
    let token: Token = state
        .http
        .post("https://oauth2.googleapis.com/token")
        .form(&[
            ("client_id", provider.client_id.as_str()),
            ("client_secret", provider.client_secret.as_str()),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", &redirect_uri(state, "google")),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    #[derive(Deserialize)]
    struct GoogleUser {
        id: String,
        #[serde(default)]
        email: Option<String>,
        #[serde(default)]
        name: Option<String>,
    }
    let gu: GoogleUser = state
        .http
        .get("https://www.googleapis.com/oauth2/v2/userinfo")
        .bearer_auth(&token.access_token)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let conn = state.db.lock().await;
    db::upsert_oauth_user(
        &conn,
        "google",
        &gu.id,
        gu.email.as_deref(),
        gu.name.as_deref(),
    )
}

// --- Magic link ------------------------------------------------------------

const MAGIC_TTL_MINUTES: i64 = 15;

#[derive(Deserialize)]
pub struct MagicRequest {
    email: String,
}

/// `POST /auth/magic` — issue a single-use sign-in link. Without an SMTP URL
/// configured (dev), the link is logged instead of mailed. The response is
/// deliberately identical whether or not the address is known, so it does not
/// leak which emails have accounts.
pub async fn magic_request(
    State(state): State<AppState>,
    Json(req): Json<MagicRequest>,
) -> Response {
    let email = req.email.trim().to_lowercase();
    if !email.contains('@') {
        return json_error(StatusCode::BAD_REQUEST, "a valid email is required");
    }

    let token = {
        let conn = state.db.lock().await;
        match db::create_magic_token(&conn, &email, MAGIC_TTL_MINUTES) {
            Ok(t) => t,
            Err(err) => {
                tracing::error!(%err, "creating magic token");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        }
    };

    let link = format!(
        "{}/auth/magic/verify?token={}",
        state.cfg.public_url.trim_end_matches('/'),
        token
    );

    match &state.cfg.smtp_url {
        Some(_smtp) => {
            // MVP: SMTP delivery is not wired up yet; log that we would send.
            tracing::info!(%email, "magic link requested (SMTP delivery not yet implemented)");
        }
        None => {
            tracing::info!(%email, %link, "magic link (dev mode: no SMTP configured)");
        }
    }

    Json(serde_json::json!({
        "ok": true,
        "message": "If that email has or can have an account, a sign-in link is on its way."
    }))
    .into_response()
}

#[derive(Deserialize)]
pub struct MagicVerifyQuery {
    #[serde(default)]
    token: String,
}

/// `GET /auth/magic/verify?token=...` — consume a token and start a session.
pub async fn magic_verify(
    State(state): State<AppState>,
    Query(q): Query<MagicVerifyQuery>,
) -> Response {
    let user = {
        let conn = state.db.lock().await;
        let email = match db::consume_magic_token(&conn, &q.token) {
            Ok(Some(email)) => email,
            Ok(None) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "this sign-in link is invalid or has expired",
                )
            }
            Err(err) => {
                tracing::error!(%err, "consuming magic token");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        };
        match db::upsert_email_user(&conn, &email) {
            Ok(u) => u,
            Err(err) => {
                tracing::error!(%err, "creating email user");
                return json_error(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
            }
        }
    };
    finish_login(&state, &user.id)
}

/// `POST /auth/logout` — clear the session cookie.
pub async fn logout() -> Response {
    (
        [(header::SET_COOKIE, session::clear_cookie_header())],
        Redirect::to("/"),
    )
        .into_response()
}
