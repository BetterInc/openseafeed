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
const NEXT_COOKIE: &str = "osf_next";

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    let cookie = headers.get(header::COOKIE)?.to_str().ok()?;
    for pair in cookie.split(';') {
        if let Some(rest) = pair.trim().strip_prefix(&format!("{name}=")) {
            return Some(rest.to_string());
        }
    }
    None
}

/// Validate a post-login return URL: relative paths, or absolute URLs on an
/// allowlisted first-party origin (the account page on openseafeed.com).
/// Everything else falls back to the built-in dashboard.
fn safe_next(state: &AppState, next: Option<&str>) -> Option<String> {
    let n = next?.trim();
    if n.starts_with('/') && !n.starts_with("//") {
        return Some(n.to_string());
    }
    state
        .cfg
        .cors_origins
        .iter()
        .any(|o| n == o || n.starts_with(&format!("{o}/")))
        .then(|| n.to_string())
}

#[derive(Deserialize)]
pub struct StartQuery {
    #[serde(default)]
    next: Option<String>,
}

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
/// cookie (and the validated post-login return URL) on the way out.
fn start_redirect(authorize_url: String, state_value: &str, next: Option<String>) -> Response {
    let state_cookie =
        format!("{STATE_COOKIE}={state_value}; Path=/; HttpOnly; SameSite=Lax; Max-Age=600");
    let next_cookie = match next {
        Some(n) => format!(
            "{NEXT_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age=600",
            urlencoding::encode(&n)
        ),
        None => format!("{NEXT_COOKIE}=; Path=/; HttpOnly; Max-Age=0"),
    };
    (
        AppendHeaders([
            (header::SET_COOKIE, state_cookie),
            (header::SET_COOKIE, next_cookie),
        ]),
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

/// Complete sign-in: set the session cookie, clear the flow cookies, and
/// redirect to the validated return URL (default: the built-in dashboard).
fn finish_login(state: &AppState, user_id: &str, next: Option<String>) -> Response {
    let session_cookie = session::set_cookie_header(&state.cfg.session_secret, user_id);
    let clear_state = format!("{STATE_COOKIE}=; Path=/; HttpOnly; Max-Age=0");
    let clear_next = format!("{NEXT_COOKIE}=; Path=/; HttpOnly; Max-Age=0");
    let target = next.unwrap_or_else(|| "/dashboard".to_string());
    // `AppendHeaders` (not a plain array) so all `Set-Cookie` lines survive;
    // an array would insert and later ones would overwrite earlier ones.
    (
        AppendHeaders([
            (header::SET_COOKIE, session_cookie),
            (header::SET_COOKIE, clear_state),
            (header::SET_COOKIE, clear_next),
        ]),
        Redirect::to(&target),
    )
        .into_response()
}

/// The validated return URL stashed by `start_redirect`, if any.
fn next_from_cookie(state: &AppState, headers: &HeaderMap) -> Option<String> {
    let raw = cookie_value(headers, NEXT_COOKIE)?;
    let decoded = urlencoding::decode(&raw).ok()?;
    safe_next(state, Some(&decoded))
}

// --- GitHub ----------------------------------------------------------------

pub async fn github_start(State(state): State<AppState>, Query(q): Query<StartQuery>) -> Response {
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
    let next = safe_next(&state, q.next.as_deref());
    start_redirect(url, &csrf, next)
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
        Ok(user) => {
            let next = next_from_cookie(&state, &headers);
            finish_login(&state, &user.id, next)
        }
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
        None => github_primary_email(state, &token.access_token)
            .await
            .ok()
            .flatten(),
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

pub async fn google_start(State(state): State<AppState>, Query(q): Query<StartQuery>) -> Response {
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
    let next = safe_next(&state, q.next.as_deref());
    start_redirect(url, &csrf, next)
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
        Ok(user) => {
            let next = next_from_cookie(&state, &headers);
            finish_login(&state, &user.id, next)
        }
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
    /// Optional post-login return URL, validated against the first-party
    /// allowlist before it is embedded in the sign-in link.
    #[serde(default)]
    next: Option<String>,
}

/// `POST /auth/magic` - issue a single-use sign-in link. Without an SMTP URL
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

    let mut link = format!(
        "{}/auth/magic/verify?token={}",
        state.cfg.public_url.trim_end_matches('/'),
        token
    );
    if let Some(next) = safe_next(&state, req.next.as_deref()) {
        link.push_str(&format!("&next={}", urlencoding::encode(&next)));
    }

    match &state.cfg.smtp_url {
        Some(smtp) => {
            // Send in the background: the response must not reveal whether
            // the address exists or how long delivery takes.
            let (smtp, from, to, link) = (
                smtp.clone(),
                state.cfg.smtp_from.clone(),
                email.clone(),
                link.clone(),
            );
            tokio::spawn(async move {
                match send_magic_email(&smtp, &from, &to, &link).await {
                    Ok(()) => tracing::info!(email = %to, "magic link emailed"),
                    Err(err) => tracing::error!(%err, email = %to, "magic link email failed"),
                }
            });
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
    #[serde(default)]
    next: Option<String>,
}

/// `GET /auth/magic/verify?token=...` - consume a token and start a session.
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
    let next = safe_next(&state, q.next.as_deref());
    finish_login(&state, &user.id, next)
}

/// `POST /auth/logout` - clear the session cookie.
pub async fn logout() -> Response {
    (
        [(header::SET_COOKIE, session::clear_cookie_header())],
        Redirect::to("/"),
    )
        .into_response()
}

/// Deliver one magic-link email over the configured SMTP relay
/// (`OSF_SMTP_URL`, e.g. `smtps://user:pass@mail.example.org:465`).
async fn send_magic_email(smtp_url: &str, from: &str, to: &str, link: &str) -> anyhow::Result<()> {
    use lettre::message::header::ContentType;
    use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};

    // Explicit Message-ID on the sender's domain: lettre does not add one on
    // its own, and a missing Message-ID is a classic spam signal (rspamd
    // scored it, and Gmail stamped SMTPIN_ADDED_MISSING on the first sends).
    let from_domain = from
        .rsplit('@')
        .next()
        .unwrap_or("openseafeed.com")
        .trim_end_matches('>');
    let msg = Message::builder()
        .from(from.parse()?)
        .to(to.parse()?)
        .message_id(Some(format!("<{}@{}>", random_state(), from_domain)))
        .subject("Your OpenSeaFeed sign-in link")
        .header(ContentType::TEXT_PLAIN)
        .body(format!(
            "Sign in to OpenSeaFeed:\n\n{link}\n\nThe link is single-use and expires shortly. \
             If you did not request this, ignore this email.\n"
        ))?;
    let mailer = AsyncSmtpTransport::<Tokio1Executor>::from_url(smtp_url)?.build();
    mailer.send(msg).await?;
    Ok(())
}
