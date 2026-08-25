//! Integration tests driving the real router over an in-memory SQLite
//! database via `tower::ServiceExt::oneshot`. No network is touched: OAuth
//! token exchange is never invoked, and everything else runs against the
//! `:memory:` connection created here.

use axum::body::Body;
use axum::http::{header, Request, StatusCode};
use http_body_util::BodyExt;
use openseafeed_control::{db, router, session, AppState, Config};
use serde_json::Value;
use tower::ServiceExt;

/// Build an `AppState` over an in-memory DB with fixed, test-known secrets.
fn test_state() -> AppState {
    let conn = db::open(":memory:").expect("open in-memory db");
    let cfg = Config::for_test();
    AppState::new(conn, cfg)
}

/// Seed one user directly and mint a valid session cookie for them.
async fn seed_user(state: &AppState) -> (String, String) {
    let user_id = {
        let conn = state.db.lock().await;
        db::create_user(
            &conn,
            Some("skipper@example.com"),
            None,
            None,
            Some("Skipper"),
        )
        .expect("create user")
        .id
    };
    let cookie = format!(
        "{}={}",
        session::COOKIE_NAME,
        session::issue(state.session_secret(), &user_id)
    );
    (user_id, cookie)
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn healthz_ok() {
    let app = router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn me_requires_session() {
    let app = router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/me")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn create_and_validate_live_key() {
    let state = test_state();
    let (owner_id, cookie) = seed_user(&state).await;
    let app = router(state.clone());

    // Create a live key via the session-authed API.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/keys")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"kind":"live","label":"laptop"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp).await;
    let key = created["key"].as_str().unwrap().to_string();
    assert!(key.starts_with("osf_live_"));

    // Validate it through the internal endpoint with the correct token.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/internal/keys/validate?key={key}"))
                .header("x-internal-token", Config::TEST_INTERNAL_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v = body_json(resp).await;
    assert_eq!(v["valid"], true);
    assert_eq!(v["kind"], "live");
    assert_eq!(v["tier"], "free");
    assert_eq!(v["owner_id"], owner_id);
    assert!(v["station_id"].is_null());
}

#[tokio::test]
async fn internal_endpoint_rejects_bad_token() {
    let state = test_state();
    let (_id, cookie) = seed_user(&state).await;
    let app = router(state);

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/keys")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"kind":"feed"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let key = body_json(created).await["key"]
        .as_str()
        .unwrap()
        .to_string();

    // Wrong token -> 401.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/internal/keys/validate?key={key}"))
                .header("x-internal-token", "wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // Missing token -> 401.
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/internal/keys/validate?key={key}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn revoked_key_is_invalid() {
    let state = test_state();
    let (_id, cookie) = seed_user(&state).await;
    let app = router(state);

    let created = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/keys")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"kind":"live"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    let key = body_json(created).await["key"]
        .as_str()
        .unwrap()
        .to_string();

    // Revoke it.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/v1/keys/{key}"))
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Now validation reports invalid.
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/internal/keys/validate?key={key}"))
                .header("x-internal-token", Config::TEST_INTERNAL_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v["valid"], false);
}

#[tokio::test]
async fn unknown_key_is_invalid() {
    let state = test_state();
    let app = router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/v1/internal/keys/validate?key=osf_live_does_not_exist")
                .header("x-internal-token", Config::TEST_INTERNAL_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["valid"], false);
}

#[tokio::test]
async fn station_registration_and_tier_flip() {
    let state = test_state();
    let (owner_id, cookie) = seed_user(&state).await;
    let app = router(state.clone());

    // Register a station; response carries both station and its stn key.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/stations")
                .header(header::COOKIE, &cookie)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"name":"Rotterdam Roof","lat":51.9,"lon":4.5}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED);
    let created = body_json(resp).await;
    let station_key = created["key"]["key"].as_str().unwrap().to_string();
    let station_id = created["station"]["id"].as_str().unwrap().to_string();
    assert!(station_key.starts_with("osf_stn_"));

    // Presenting the key to the data plane IS contributing: the first
    // validate touches the key and the tier flips in the same response.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/v1/internal/keys/validate?key={station_key}"))
                .header("x-internal-token", Config::TEST_INTERNAL_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let v = body_json(resp).await;
    assert_eq!(v["valid"], true);
    assert_eq!(v["kind"], "station");
    assert_eq!(v["tier"], "contributor");
    assert_eq!(v["station_id"], station_id);
    assert_eq!(v["owner_id"], owner_id);

    // A heartbeat records recent activity...
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/internal/stations/heartbeat")
                .header("x-internal-token", Config::TEST_INTERNAL_TOKEN)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(format!(
                    r#"{{"station_key":"{station_key}","msgs":42}}"#
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // ...and the tier stays contributor.
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/v1/internal/keys/validate?key={station_key}"))
                .header("x-internal-token", Config::TEST_INTERNAL_TOKEN)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(body_json(resp).await["tier"], "contributor");
}

#[tokio::test]
async fn disabled_oauth_provider_returns_503() {
    // Test config configures no OAuth providers.
    let app = router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/auth/github")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn magic_link_round_trip() {
    let state = test_state();
    let app = router(state.clone());

    // Request a link (dev mode logs it); token is stored in the db.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/magic")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"email":"newcomer@example.com"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Pull the token straight from the db (stands in for reading the email).
    let token = {
        let conn = state.db.lock().await;
        conn.query_row(
            "SELECT token FROM magic_tokens WHERE email = ?1",
            ["newcomer@example.com"],
            |r| r.get::<_, String>(0),
        )
        .unwrap()
    };

    // Verifying it starts a session (302 to /dashboard with Set-Cookie).
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/auth/magic/verify?token={token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SEE_OTHER);
    assert!(resp
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .any(|v| v.to_str().unwrap().contains(session::COOKIE_NAME)));
}
