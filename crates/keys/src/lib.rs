//! Key parsing and validation shared by the data-plane services.
//!
//! Keys are long-lived bearer tokens so the feed keeps working even if every
//! OAuth provider is down (the failure mode that killed aisstream.io):
//! `osf_live_…` consumer API keys, `osf_stn_…` station sharing keys,
//! `osf_feed_…` partner/connector feed keys.
//!
//! Two validation modes, selected by `OSF_KEYS_MODE`:
//! - `dev` (default): any well-formed key is accepted, tier "contributor".
//! - `http`: ask the control plane at `OSF_CONTROL_URL`, authenticated with
//!   `OSF_INTERNAL_TOKEN`; results are cached for 60 s.

use serde::Deserialize;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Origins that serve OpenSeaFeed's own live map. A keyless connection from
/// one of these is a first-party viewer: it gets the whole world, where an
/// anonymous third-party client stays on the free tier's area cap and
/// snapshot delay. Override with `OSF_VIEWER_ORIGINS` (comma-separated).
///
/// `Origin` is set by the browser and page JavaScript cannot forge it, but a
/// non-browser client can send whatever it likes. This is a fair-use gate for
/// our own shop window, not an authentication boundary — nothing behind it is
/// private, it is the same public feed either way.
const DEFAULT_VIEWER_ORIGINS: &str = "https://stream.openseafeed.com,https://openseafeed.com,\
http://localhost:8081,http://127.0.0.1:8081";

/// The tier granted to a first-party viewer: unmetered area, freshest data.
pub const VIEWER_TIER: &str = "viewer";

fn viewer_origins() -> &'static [String] {
    static ORIGINS: OnceLock<Vec<String>> = OnceLock::new();
    ORIGINS.get_or_init(|| {
        std::env::var("OSF_VIEWER_ORIGINS")
            .unwrap_or_else(|_| DEFAULT_VIEWER_ORIGINS.to_string())
            .split(',')
            .map(normalize_origin)
            .filter(|s| !s.is_empty())
            .collect()
    })
}

fn normalize_origin(raw: &str) -> String {
    raw.trim().trim_end_matches('/').to_ascii_lowercase()
}

/// Whether an `Origin` header belongs to one of our own map hosts.
pub fn is_first_party_origin(origin: Option<&str>) -> bool {
    let Some(origin) = origin else { return false };
    let origin = normalize_origin(origin);
    !origin.is_empty() && viewer_origins().contains(&origin)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Live,
    Station,
    Feed,
}

impl Kind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Kind::Live => "live",
            Kind::Station => "station",
            Kind::Feed => "feed",
        }
    }
}

/// Determine a key's kind from its prefix, requiring at least 8 chars of
/// secret material.
pub fn kind_of(key: &str) -> Option<Kind> {
    let (kind, rest) = if let Some(r) = key.strip_prefix("osf_live_") {
        (Kind::Live, r)
    } else if let Some(r) = key.strip_prefix("osf_stn_") {
        (Kind::Station, r)
    } else {
        (Kind::Feed, key.strip_prefix("osf_feed_")?)
    };
    if rest.len() >= 8 && rest.chars().all(|c| c.is_ascii_alphanumeric()) {
        Some(kind)
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub struct KeyInfo {
    pub kind: Kind,
    /// "free" or "contributor".
    pub tier: String,
    pub owner_id: String,
    pub station_id: Option<String>,
}

#[derive(Deserialize)]
struct ValidateResponse {
    valid: bool,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    tier: String,
    #[serde(default)]
    owner_id: String,
    #[serde(default)]
    station_id: Option<String>,
}

enum Mode {
    Dev,
    Http {
        url: String,
        token: String,
        client: reqwest::Client,
        cache: RwLock<HashMap<String, (Option<KeyInfo>, Instant)>>,
    },
}

pub struct Validator {
    mode: Mode,
    cache_ttl: Duration,
}

impl Validator {
    /// Build from `OSF_KEYS_MODE` / `OSF_CONTROL_URL` / `OSF_INTERNAL_TOKEN`.
    pub fn from_env() -> Arc<Self> {
        let mode = std::env::var("OSF_KEYS_MODE").unwrap_or_else(|_| "dev".into());
        match mode.as_str() {
            "http" => {
                let url = std::env::var("OSF_CONTROL_URL")
                    .unwrap_or_else(|_| "http://localhost:8083".into());
                let token = std::env::var("OSF_INTERNAL_TOKEN")
                    .unwrap_or_else(|_| "dev-internal-token".into());
                tracing::info!(url, "key validation against control plane");
                Arc::new(Self {
                    mode: Mode::Http {
                        url,
                        token,
                        client: reqwest::Client::new(),
                        cache: RwLock::new(HashMap::new()),
                    },
                    cache_ttl: Duration::from_secs(60),
                })
            }
            _ => {
                tracing::warn!("OSF_KEYS_MODE=dev: accepting any well-formed key");
                Arc::new(Self {
                    mode: Mode::Dev,
                    cache_ttl: Duration::from_secs(60),
                })
            }
        }
    }

    pub fn dev() -> Arc<Self> {
        Arc::new(Self {
            mode: Mode::Dev,
            cache_ttl: Duration::from_secs(60),
        })
    }

    /// Validate a key. `None` means reject.
    pub async fn validate(&self, key: &str) -> Option<KeyInfo> {
        let kind = kind_of(key)?;
        match &self.mode {
            Mode::Dev => Some(KeyInfo {
                kind,
                tier: "contributor".into(),
                owner_id: "dev".into(),
                // In dev mode the station id is derived from the key so
                // distinct keys land on distinct NATS subjects.
                station_id: Some(format!("dev-{}", &key[key.len().saturating_sub(8)..])),
            }),
            Mode::Http {
                url,
                token,
                client,
                cache,
            } => {
                if let Some((info, at)) = cache.read().await.get(key) {
                    if at.elapsed() < self.cache_ttl {
                        return info.clone();
                    }
                }
                let info = self.fetch(client, url, token, key).await;
                cache
                    .write()
                    .await
                    .insert(key.to_string(), (info.clone(), Instant::now()));
                info
            }
        }
    }

    async fn fetch(
        &self,
        client: &reqwest::Client,
        url: &str,
        token: &str,
        key: &str,
    ) -> Option<KeyInfo> {
        let resp = client
            .get(format!("{url}/v1/internal/keys/validate"))
            .query(&[("key", key)])
            .header("X-Internal-Token", token)
            .timeout(Duration::from_secs(5))
            .send()
            .await
            .and_then(|r| r.error_for_status());
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                // Control plane being down must not kill the data plane:
                // reject unknown keys but log loudly.
                tracing::error!(error = %e, "key validation request failed");
                return None;
            }
        };
        let v: ValidateResponse = resp.json().await.ok()?;
        if !v.valid {
            return None;
        }
        let kind = match v.kind.as_str() {
            "live" => Kind::Live,
            "station" => Kind::Station,
            "feed" => Kind::Feed,
            _ => kind_of(key)?,
        };
        Some(KeyInfo {
            kind,
            tier: if v.tier.is_empty() {
                "free".into()
            } else {
                v.tier
            },
            owner_id: v.owner_id,
            station_id: v.station_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_kinds() {
        assert_eq!(kind_of("osf_live_abcdef1234"), Some(Kind::Live));
        assert_eq!(kind_of("osf_stn_abcdef1234"), Some(Kind::Station));
        assert_eq!(kind_of("osf_feed_abcdef1234"), Some(Kind::Feed));
        assert_eq!(kind_of("osf_live_short"), None);
        assert_eq!(kind_of("sk_live_abcdef1234"), None);
        assert_eq!(kind_of("osf_live_bad key!!"), None);
    }

    #[test]
    fn first_party_origins_are_exact_matches() {
        assert!(is_first_party_origin(Some(
            "https://stream.openseafeed.com"
        )));
        // Trailing slash and case are normalized away.
        assert!(is_first_party_origin(Some(
            "https://Stream.OpenSeaFeed.com/"
        )));
        // A look-alike host is not us.
        assert!(!is_first_party_origin(Some(
            "https://stream.openseafeed.com.evil.example"
        )));
        assert!(!is_first_party_origin(Some(
            "http://stream.openseafeed.com"
        )));
        assert!(!is_first_party_origin(Some("null")));
        assert!(!is_first_party_origin(None));
    }

    #[tokio::test]
    async fn dev_mode_accepts_well_formed() {
        let v = Validator::dev();
        let info = v.validate("osf_stn_abcdef1234").await.unwrap();
        assert_eq!(info.kind, Kind::Station);
        assert_eq!(info.tier, "contributor");
        assert!(info.station_id.unwrap().starts_with("dev-"));
        assert!(v.validate("garbage").await.is_none());
    }
}
