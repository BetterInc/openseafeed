//! Stateless sessions carried in a signed `osf_session` cookie.
//!
//! The cookie value is `user_id|expiry_unix|hex_hmac`, where the HMAC-SHA256
//! is taken over `user_id|expiry_unix` using the server secret. Nothing is
//! stored server-side: a valid signature plus an unexpired timestamp is the
//! whole session. Tampering with either field invalidates the MAC, and the
//! comparison is constant-time via the `hmac` crate's verifier.

use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub const COOKIE_NAME: &str = "osf_session";

/// How long a freshly minted session stays valid.
const SESSION_DAYS: i64 = 30;

fn mac(secret: &[u8], msg: &str) -> String {
    let mut m = HmacSha256::new_from_slice(secret).expect("HMAC accepts any key length");
    m.update(msg.as_bytes());
    hex::encode(m.finalize().into_bytes())
}

/// Build a signed cookie value for `user_id` expiring `SESSION_DAYS` out.
pub fn issue(secret: &[u8], user_id: &str) -> String {
    let expiry = (Utc::now() + Duration::days(SESSION_DAYS)).timestamp();
    let payload = format!("{user_id}|{expiry}");
    let sig = mac(secret, &payload);
    format!("{payload}|{sig}")
}

/// Validate a cookie value, returning the user id if the signature checks out
/// and the session has not expired.
pub fn verify(secret: &[u8], value: &str) -> Option<String> {
    let mut parts = value.splitn(3, '|');
    let user_id = parts.next()?;
    let expiry_str = parts.next()?;
    let sig = parts.next()?;

    // Verify via the HMAC crate's constant-time comparison.
    let payload = format!("{user_id}|{expiry_str}");
    let mut m = HmacSha256::new_from_slice(secret).ok()?;
    m.update(payload.as_bytes());
    let sig_bytes = hex::decode(sig).ok()?;
    m.verify_slice(&sig_bytes).ok()?;

    let expiry: i64 = expiry_str.parse().ok()?;
    if Utc::now().timestamp() > expiry {
        return None;
    }
    Some(user_id.to_string())
}

/// A ready-to-send `Set-Cookie` header value for a freshly issued session.
pub fn set_cookie_header(secret: &[u8], user_id: &str) -> String {
    let value = issue(secret, user_id);
    let max_age = SESSION_DAYS * 24 * 60 * 60;
    format!("{COOKIE_NAME}={value}; Path=/; HttpOnly; SameSite=Lax; Max-Age={max_age}")
}

/// A `Set-Cookie` header value that clears the session (used on logout).
pub fn clear_cookie_header() -> String {
    format!("{COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0")
}

/// Pull the `osf_session` value out of a raw `Cookie` header, if present.
pub fn from_cookie_header(header: &str) -> Option<&str> {
    for pair in header.split(';') {
        let pair = pair.trim();
        if let Some(rest) = pair.strip_prefix(&format!("{COOKIE_NAME}=")) {
            return Some(rest);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let secret = b"test-secret";
        let cookie = issue(secret, "usr_abc");
        assert_eq!(verify(secret, &cookie).as_deref(), Some("usr_abc"));
    }

    #[test]
    fn rejects_tampering() {
        let secret = b"test-secret";
        let cookie = issue(secret, "usr_abc");
        let forged = cookie.replace("usr_abc", "usr_xyz");
        assert!(verify(secret, &forged).is_none());
    }

    #[test]
    fn rejects_wrong_secret() {
        let cookie = issue(b"secret-one", "usr_abc");
        assert!(verify(b"secret-two", &cookie).is_none());
    }

    #[test]
    fn parses_cookie_header() {
        assert_eq!(
            from_cookie_header("foo=1; osf_session=abc; bar=2"),
            Some("abc")
        );
        assert_eq!(from_cookie_header("foo=1"), None);
    }
}
