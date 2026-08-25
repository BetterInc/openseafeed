//! SQLite persistence: schema migrations and the small set of queries the
//! control plane needs for accounts, API keys, and the station registry.
//!
//! The whole control plane is single-node for the MVP, so a single
//! [`rusqlite::Connection`] behind a mutex is plenty. All timestamps are
//! stored as RFC 3339 strings in UTC; because the offset is fixed (`+00:00`)
//! lexical ordering matches chronological ordering, which lets the tier and
//! heartbeat queries compare against a cutoff with plain string comparison.

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use rand::RngCore;
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;

/// Number of random bytes behind a key/token; rendered as hex it becomes the
/// 32-character suffix the spec calls for.
const KEY_BYTES: usize = 16;

/// A registered account. Any of the three identity columns may be set; a user
/// can link more than one provider over time.
#[derive(Debug, Clone, Serialize)]
pub struct User {
    pub id: String,
    pub email: Option<String>,
    pub github_id: Option<String>,
    pub google_id: Option<String>,
    pub display_name: Option<String>,
    pub created_at: String,
    /// `user`, `moderator` or `admin`.
    pub role: String,
    /// Manual tier cap/boost set from the admin panel; `None` = computed.
    pub tier_override: Option<String>,
}

/// An issued API key. `kind` is one of `live`, `station`, `feed`.
#[derive(Debug, Clone, Serialize)]
pub struct ApiKey {
    pub key: String,
    pub user_id: String,
    pub kind: String,
    pub label: Option<String>,
    pub created_at: String,
    pub revoked_at: Option<String>,
    /// Last time ingest/fanout validated this key (throttled writes).
    pub last_used_at: Option<String>,
}

/// A contributing station and its rolling ingest counters.
#[derive(Debug, Clone, Serialize)]
pub struct Station {
    pub id: String,
    pub user_id: String,
    pub name: String,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub key: String,
    pub created_at: String,
    pub last_seen_at: Option<String>,
    pub msgs_total: i64,
}

/// Open the database at `path` (`:memory:` is accepted for tests) and apply
/// the schema. Safe to call repeatedly — every statement is `IF NOT EXISTS`.
pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    migrate(&conn)?;
    ensure_bootstrap_admin(&conn)?;
    Ok(conn)
}

/// If no admin exists yet, the oldest account becomes one. Deterministic
/// zero-config bootstrap: on a fresh deployment the operator signs in first
/// and is therefore the admin; roles can be handed out from the panel after.
pub fn ensure_bootstrap_admin(conn: &Connection) -> Result<()> {
    let admins: i64 =
        conn.query_row("SELECT COUNT(*) FROM users WHERE role = 'admin'", [], |r| {
            r.get(0)
        })?;
    if admins == 0 {
        conn.execute(
            "UPDATE users SET role = 'admin' WHERE id =
               (SELECT id FROM users ORDER BY created_at LIMIT 1)",
            [],
        )?;
    }
    Ok(())
}

/// Apply the schema. Split out so tests can migrate an in-memory connection.
pub fn migrate(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS users (
            id           TEXT PRIMARY KEY,
            email        TEXT UNIQUE,
            github_id    TEXT UNIQUE,
            google_id    TEXT UNIQUE,
            display_name TEXT,
            created_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS api_keys (
            key        TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL REFERENCES users(id),
            kind       TEXT NOT NULL CHECK(kind IN ('live','station','feed')),
            label      TEXT,
            created_at TEXT NOT NULL,
            revoked_at TEXT
        );

        CREATE TABLE IF NOT EXISTS stations (
            id           TEXT PRIMARY KEY,
            user_id      TEXT NOT NULL REFERENCES users(id),
            name         TEXT NOT NULL,
            lat          REAL,
            lon          REAL,
            key          TEXT NOT NULL REFERENCES api_keys(key),
            created_at   TEXT NOT NULL,
            last_seen_at TEXT,
            msgs_total   INTEGER NOT NULL DEFAULT 0
        );

        -- Single-use, time-limited email sign-in tokens.
        CREATE TABLE IF NOT EXISTS magic_tokens (
            token      TEXT PRIMARY KEY,
            email      TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            used_at    TEXT
        );

        -- Cached Wikimedia Commons photo lookups, keyed by MMSI. NULL
        -- image_url = "looked up, nothing found" (negative cache); imo is
        -- the identity the lookup ran with, so a later request that DOES
        -- know the IMO retries instead of trusting an MMSI-only miss.
        CREATE TABLE IF NOT EXISTS vessel_photos_v2 (
            mmsi       INTEGER PRIMARY KEY,
            imo        INTEGER,
            image_url  TEXT,
            page_url   TEXT,
            fetched_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);
        CREATE INDEX IF NOT EXISTS idx_stations_user ON stations(user_id);
        CREATE INDEX IF NOT EXISTS idx_stations_key  ON stations(key);
        "#,
    )?;
    // Additive columns on tables that predate them. SQLite has no
    // ADD COLUMN IF NOT EXISTS, so ignore the duplicate-column error.
    for sql in [
        "ALTER TABLE users ADD COLUMN role TEXT NOT NULL DEFAULT 'user'",
        "ALTER TABLE users ADD COLUMN tier_override TEXT",
        "ALTER TABLE api_keys ADD COLUMN last_used_at TEXT",
    ] {
        if let Err(err) = conn.execute(sql, []) {
            if !err.to_string().contains("duplicate column") {
                return Err(err.into());
            }
        }
    }
    Ok(())
}

/// Hex-encode `KEY_BYTES` of randomness.
fn random_hex() -> String {
    let mut buf = [0u8; KEY_BYTES];
    rand::thread_rng().fill_bytes(&mut buf);
    hex::encode(buf)
}

/// A fresh API key string with the kind-appropriate prefix.
pub fn new_key(kind: &str) -> String {
    let prefix = match kind {
        "live" => "osf_live_",
        "station" => "osf_stn_",
        "feed" => "osf_feed_",
        _ => "osf_",
    };
    format!("{prefix}{}", random_hex())
}

/// A short random identifier with the given prefix (e.g. `usr_`, `stn_`).
pub fn new_id(prefix: &str) -> String {
    let mut buf = [0u8; 8];
    rand::thread_rng().fill_bytes(&mut buf);
    format!("{prefix}{}", hex::encode(buf))
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339()
}

// --- users -----------------------------------------------------------------

fn row_to_user(row: &rusqlite::Row) -> rusqlite::Result<User> {
    Ok(User {
        id: row.get(0)?,
        email: row.get(1)?,
        github_id: row.get(2)?,
        google_id: row.get(3)?,
        display_name: row.get(4)?,
        created_at: row.get(5)?,
        role: row.get(6)?,
        tier_override: row.get(7)?,
    })
}

const USER_COLS: &str =
    "id, email, github_id, google_id, display_name, created_at, role, tier_override";

pub fn user_by_id(conn: &Connection, id: &str) -> Result<Option<User>> {
    let user = conn
        .query_row(
            &format!("SELECT {USER_COLS} FROM users WHERE id = ?1"),
            params![id],
            row_to_user,
        )
        .optional()?;
    Ok(user)
}

fn user_by_col(conn: &Connection, col: &str, value: &str) -> Result<Option<User>> {
    let user = conn
        .query_row(
            &format!("SELECT {USER_COLS} FROM users WHERE {col} = ?1"),
            params![value],
            row_to_user,
        )
        .optional()?;
    Ok(user)
}

/// Insert a user row. `id` and `created_at` are filled in here.
pub fn create_user(
    conn: &Connection,
    email: Option<&str>,
    github_id: Option<&str>,
    google_id: Option<&str>,
    display_name: Option<&str>,
) -> Result<User> {
    let id = new_id("usr_");
    let created_at = now_rfc3339();
    conn.execute(
        "INSERT INTO users (id, email, github_id, google_id, display_name, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![id, email, github_id, google_id, display_name, created_at],
    )?;
    Ok(User {
        id,
        email: email.map(str::to_string),
        github_id: github_id.map(str::to_string),
        google_id: google_id.map(str::to_string),
        display_name: display_name.map(str::to_string),
        created_at,
        role: "user".to_string(),
        tier_override: None,
    })
}

/// Find the user carrying `provider_id` for the given provider, else create a
/// new account. If a row already exists for the email it is reused and the
/// provider id linked onto it, so signing in with GitHub then Google (same
/// address) lands on one account rather than two.
pub fn upsert_oauth_user(
    conn: &Connection,
    provider: &str,
    provider_id: &str,
    email: Option<&str>,
    display_name: Option<&str>,
) -> Result<User> {
    let col = match provider {
        "github" => "github_id",
        "google" => "google_id",
        other => anyhow::bail!("unknown oauth provider: {other}"),
    };

    if let Some(user) = user_by_col(conn, col, provider_id)? {
        return Ok(user);
    }

    if let Some(email) = email {
        if let Some(existing) = user_by_col(conn, "email", email)? {
            conn.execute(
                &format!("UPDATE users SET {col} = ?1 WHERE id = ?2"),
                params![provider_id, existing.id],
            )?;
            return user_by_id(conn, &existing.id)?
                .ok_or_else(|| anyhow::anyhow!("user vanished after link"));
        }
    }

    let (github_id, google_id) = match provider {
        "github" => (Some(provider_id), None),
        _ => (None, Some(provider_id)),
    };
    create_user(conn, email, github_id, google_id, display_name)
}

/// Find (by email) or create a user for magic-link sign-in.
pub fn upsert_email_user(conn: &Connection, email: &str) -> Result<User> {
    if let Some(user) = user_by_col(conn, "email", email)? {
        return Ok(user);
    }
    create_user(conn, Some(email), None, None, None)
}

// --- api keys --------------------------------------------------------------

fn row_to_key(row: &rusqlite::Row) -> rusqlite::Result<ApiKey> {
    Ok(ApiKey {
        key: row.get(0)?,
        user_id: row.get(1)?,
        kind: row.get(2)?,
        label: row.get(3)?,
        created_at: row.get(4)?,
        revoked_at: row.get(5)?,
        last_used_at: row.get(6)?,
    })
}

const KEY_COLS: &str = "key, user_id, kind, label, created_at, revoked_at, last_used_at";

/// Mint and persist an API key of the given kind for `user_id`.
pub fn create_key(
    conn: &Connection,
    user_id: &str,
    kind: &str,
    label: Option<&str>,
) -> Result<ApiKey> {
    let key = new_key(kind);
    let created_at = now_rfc3339();
    conn.execute(
        "INSERT INTO api_keys (key, user_id, kind, label, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![key, user_id, kind, label, created_at],
    )?;
    Ok(ApiKey {
        key,
        user_id: user_id.to_string(),
        kind: kind.to_string(),
        label: label.map(str::to_string),
        created_at,
        revoked_at: None,
        last_used_at: None,
    })
}

pub fn key_by_id(conn: &Connection, key: &str) -> Result<Option<ApiKey>> {
    let row = conn
        .query_row(
            &format!("SELECT {KEY_COLS} FROM api_keys WHERE key = ?1"),
            params![key],
            row_to_key,
        )
        .optional()?;
    Ok(row)
}

pub fn keys_for_user(conn: &Connection, user_id: &str) -> Result<Vec<ApiKey>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {KEY_COLS} FROM api_keys WHERE user_id = ?1 ORDER BY created_at"
    ))?;
    let rows = stmt
        .query_map(params![user_id], row_to_key)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

/// Stamp `revoked_at`. Returns whether a matching, owned, still-live key was
/// found. Idempotent: re-revoking an already revoked key affects no rows.
pub fn revoke_key(conn: &Connection, user_id: &str, key: &str) -> Result<bool> {
    let now = now_rfc3339();
    let n = conn.execute(
        "UPDATE api_keys SET revoked_at = ?1
         WHERE key = ?2 AND user_id = ?3 AND revoked_at IS NULL",
        params![now, key, user_id],
    )?;
    Ok(n > 0)
}

// --- stations --------------------------------------------------------------

fn row_to_station(row: &rusqlite::Row) -> rusqlite::Result<Station> {
    Ok(Station {
        id: row.get(0)?,
        user_id: row.get(1)?,
        name: row.get(2)?,
        lat: row.get(3)?,
        lon: row.get(4)?,
        key: row.get(5)?,
        created_at: row.get(6)?,
        last_seen_at: row.get(7)?,
        msgs_total: row.get(8)?,
    })
}

const STATION_COLS: &str = "id, user_id, name, lat, lon, key, created_at, last_seen_at, msgs_total";

/// Register a station together with its own `osf_stn_` key, in one
/// transaction so a station never exists without a usable key.
pub fn create_station(
    conn: &mut Connection,
    user_id: &str,
    name: &str,
    lat: Option<f64>,
    lon: Option<f64>,
) -> Result<(Station, ApiKey)> {
    let tx = conn.transaction()?;
    let created_at = now_rfc3339();

    let key = new_key("station");
    tx.execute(
        "INSERT INTO api_keys (key, user_id, kind, label, created_at)
         VALUES (?1, ?2, 'station', ?3, ?4)",
        params![key, user_id, name, created_at],
    )?;

    let id = new_id("stn_");
    tx.execute(
        "INSERT INTO stations (id, user_id, name, lat, lon, key, created_at, msgs_total)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 0)",
        params![id, user_id, name, lat, lon, key, created_at],
    )?;
    tx.commit()?;

    let station = Station {
        id,
        user_id: user_id.to_string(),
        name: name.to_string(),
        lat,
        lon,
        key: key.clone(),
        created_at: created_at.clone(),
        last_seen_at: None,
        msgs_total: 0,
    };
    let api_key = ApiKey {
        key,
        user_id: user_id.to_string(),
        kind: "station".to_string(),
        label: Some(name.to_string()),
        created_at,
        revoked_at: None,
        last_used_at: None,
    };
    Ok((station, api_key))
}

pub fn stations_for_user(conn: &Connection, user_id: &str) -> Result<Vec<Station>> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {STATION_COLS} FROM stations WHERE user_id = ?1 ORDER BY created_at"
    ))?;
    let rows = stmt
        .query_map(params![user_id], row_to_station)?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(rows)
}

pub fn station_by_key(conn: &Connection, key: &str) -> Result<Option<Station>> {
    let row = conn
        .query_row(
            &format!("SELECT {STATION_COLS} FROM stations WHERE key = ?1"),
            params![key],
            row_to_station,
        )
        .optional()?;
    Ok(row)
}

/// Record a heartbeat for the station owning `station_key`: bump the message
/// counter and refresh `last_seen_at`. Returns rows affected (0 if unknown).
pub fn record_heartbeat(conn: &Connection, station_key: &str, msgs: i64) -> Result<usize> {
    let now = now_rfc3339();
    let n = conn.execute(
        "UPDATE stations SET last_seen_at = ?1, msgs_total = msgs_total + ?2
         WHERE key = ?3",
        params![now, msgs, station_key],
    )?;
    Ok(n)
}

/// A user is a `contributor` if they own at least one station seen within the
/// last `CONTRIBUTOR_WINDOW_DAYS`; otherwise `free`.
const CONTRIBUTOR_WINDOW_DAYS: i64 = 7;

/// Record that a key was just presented to the data plane. Cheap single-row
/// update; callers throttle (the validators cache lookups for 60s anyway).
pub fn touch_key(conn: &Connection, key: &str) -> Result<()> {
    conn.execute(
        "UPDATE api_keys SET last_used_at = ?1 WHERE key = ?2",
        params![now_rfc3339(), key],
    )?;
    Ok(())
}

pub fn tier_for_user(conn: &Connection, user_id: &str) -> Result<&'static str> {
    // A manual override from the admin panel wins over the computed tier.
    let overridden: Option<Option<String>> = conn
        .query_row(
            "SELECT tier_override FROM users WHERE id = ?1",
            params![user_id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(Some(t)) = overridden {
        return Ok(if t == "contributor" {
            "contributor"
        } else {
            "free"
        });
    }
    let cutoff = (Utc::now() - Duration::days(CONTRIBUTOR_WINDOW_DAYS)).to_rfc3339();
    // Contribution = a station seen recently OR a feed/station key actually
    // presented to ingest recently. Consuming (live keys) never counts.
    let count: i64 = conn.query_row(
        "SELECT
           (SELECT COUNT(*) FROM stations
             WHERE user_id = ?1 AND last_seen_at IS NOT NULL AND last_seen_at >= ?2)
         + (SELECT COUNT(*) FROM api_keys
             WHERE user_id = ?1 AND kind IN ('feed','station')
               AND revoked_at IS NULL
               AND last_used_at IS NOT NULL AND last_used_at >= ?2)",
        params![user_id, cutoff],
        |row| row.get(0),
    )?;
    Ok(if count > 0 { "contributor" } else { "free" })
}

// --- magic tokens ----------------------------------------------------------

/// Store a fresh single-use sign-in token valid for `ttl_minutes`, returning
/// the token string.
pub fn create_magic_token(conn: &Connection, email: &str, ttl_minutes: i64) -> Result<String> {
    let token = random_hex();
    let expires_at = (Utc::now() + Duration::minutes(ttl_minutes)).to_rfc3339();
    conn.execute(
        "INSERT INTO magic_tokens (token, email, expires_at) VALUES (?1, ?2, ?3)",
        params![token, email, expires_at],
    )?;
    Ok(token)
}

/// Consume a magic token, returning the email it was issued for if it exists,
/// has not been used, and has not expired. Marks it used so it cannot be
/// replayed.
pub fn consume_magic_token(conn: &Connection, token: &str) -> Result<Option<String>> {
    let row: Option<(String, String, Option<String>)> = conn
        .query_row(
            "SELECT email, expires_at, used_at FROM magic_tokens WHERE token = ?1",
            params![token],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;

    let (email, expires_at, used_at) = match row {
        Some(t) => t,
        None => return Ok(None),
    };
    if used_at.is_some() {
        return Ok(None);
    }
    let expires = DateTime::parse_from_rfc3339(&expires_at)?.with_timezone(&Utc);
    if Utc::now() > expires {
        return Ok(None);
    }
    conn.execute(
        "UPDATE magic_tokens SET used_at = ?1 WHERE token = ?2",
        params![now_rfc3339(), token],
    )?;
    Ok(Some(email))
}

// --- vessel photos ----------------------------------------------------------

/// One cached photo lookup; `image_url = None` means the lookup ran and found
/// nothing (negative cache). `imo` is the IMO the lookup was performed with.
pub struct VesselPhoto {
    pub imo: Option<u32>,
    pub image_url: Option<String>,
    pub page_url: Option<String>,
    pub fetched_at: String,
}

pub fn photo_get(conn: &Connection, mmsi: u32) -> Result<Option<VesselPhoto>> {
    let row = conn
        .query_row(
            "SELECT imo, image_url, page_url, fetched_at FROM vessel_photos_v2 WHERE mmsi = ?1",
            params![mmsi],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()?;
    Ok(
        row.map(|(imo, image_url, page_url, fetched_at)| VesselPhoto {
            imo,
            image_url,
            page_url,
            fetched_at,
        }),
    )
}

pub fn photo_put(
    conn: &Connection,
    mmsi: u32,
    imo: Option<u32>,
    image_url: Option<&str>,
    page_url: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO vessel_photos_v2 (mmsi, imo, image_url, page_url, fetched_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(mmsi) DO UPDATE SET imo = ?2, image_url = ?3, page_url = ?4, fetched_at = ?5",
        params![mmsi, imo, image_url, page_url, now_rfc3339()],
    )?;
    Ok(())
}

// --- admin -------------------------------------------------------------

/// One row of the admin panel's user table.
#[derive(Debug, Clone, Serialize)]
pub struct AdminUser {
    pub id: String,
    pub email: Option<String>,
    pub display_name: Option<String>,
    pub role: String,
    pub tier: String,
    pub tier_override: Option<String>,
    pub keys: i64,
    pub stations: i64,
    pub created_at: String,
}

pub fn list_users_admin(conn: &Connection) -> Result<Vec<AdminUser>> {
    let mut stmt = conn.prepare(
        "SELECT u.id, u.email, u.display_name, u.role, u.tier_override, u.created_at,
                (SELECT COUNT(*) FROM api_keys k WHERE k.user_id = u.id AND k.revoked_at IS NULL),
                (SELECT COUNT(*) FROM stations s WHERE s.user_id = u.id)
         FROM users u ORDER BY u.created_at",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, Option<String>>(4)?,
            r.get::<_, String>(5)?,
            r.get::<_, i64>(6)?,
            r.get::<_, i64>(7)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, email, display_name, role, tier_override, created_at, keys, stations) = row?;
        let tier = tier_for_user(conn, &id)?.to_string();
        out.push(AdminUser {
            id,
            email,
            display_name,
            role,
            tier,
            tier_override,
            keys,
            stations,
            created_at,
        });
    }
    Ok(out)
}

pub fn set_user_role(conn: &Connection, user_id: &str, role: &str) -> Result<()> {
    conn.execute(
        "UPDATE users SET role = ?1 WHERE id = ?2",
        params![role, user_id],
    )?;
    Ok(())
}

pub fn set_user_tier_override(conn: &Connection, user_id: &str, tier: Option<&str>) -> Result<()> {
    conn.execute(
        "UPDATE users SET tier_override = ?1 WHERE id = ?2",
        params![tier, user_id],
    )?;
    Ok(())
}
