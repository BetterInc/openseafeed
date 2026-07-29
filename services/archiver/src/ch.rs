use std::time::Duration;

use serde::Serialize;

use crate::schema;

/// Thin ClickHouse HTTP-interface client.
pub struct ClickHouse {
    http: reqwest::Client,
    url: String,
    pub db: String,
    user: Option<String>,
    password: Option<String>,
}

impl ClickHouse {
    pub fn from_env() -> Self {
        Self {
            http: reqwest::Client::new(),
            url: std::env::var("OSF_CLICKHOUSE_URL")
                .unwrap_or_else(|_| "http://localhost:8123".into()),
            db: std::env::var("OSF_CLICKHOUSE_DB").unwrap_or_else(|_| "osf".into()),
            user: std::env::var("OSF_CLICKHOUSE_USER").ok(),
            password: std::env::var("OSF_CLICKHOUSE_PASSWORD").ok(),
        }
    }

    pub async fn exec(&self, sql: &str, body: Option<Vec<u8>>) -> anyhow::Result<String> {
        let mut req = self.http.post(&self.url);
        if let Some(u) = &self.user {
            req = req.header("X-ClickHouse-User", u);
        }
        if let Some(p) = &self.password {
            req = req.header("X-ClickHouse-Key", p);
        }
        let req = match body {
            // SQL goes in the query string, data in the body.
            Some(data) => req.query(&[("query", sql)]).body(data),
            None => req.body(sql.to_string()),
        };
        let resp = req.timeout(Duration::from_secs(30)).send().await?;
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("clickhouse {status}: {text}");
        }
        Ok(text)
    }

    pub async fn migrate(&self) -> anyhow::Result<()> {
        // 7 hot days on local disk covers the queries people actually make
        // often; everything else reads from the Wasabi cold tier.
        let hot_days: u32 = env_num("OSF_HOT_DAYS", 7);
        // Five years cold. At current volume (~0.4 TB/yr) that's ~2 TB on
        // Wasabi ≈ $14/mo — history is the product, keep it deep.
        let retain_days: u32 = env_num("OSF_RETAIN_DAYS", 1825);
        let tiered = std::env::var("OSF_CLICKHOUSE_TIERED")
            .map(|v| v == "1")
            .unwrap_or(false);
        for sql in schema::migrations(&self.db, hot_days, retain_days, tiered) {
            self.exec(&sql, None).await?;
        }
        tracing::info!(db = self.db, hot_days, retain_days, tiered, "schema ready");
        Ok(())
    }

    pub async fn insert<T: Serialize>(&self, table: &str, rows: &[T]) -> anyhow::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut body = Vec::with_capacity(rows.len() * 128);
        for r in rows {
            serde_json::to_writer(&mut body, r)?;
            body.push(b'\n');
        }
        let sql = format!("INSERT INTO {}.{} FORMAT JSONEachRow", self.db, table);
        self.exec(&sql, Some(body)).await.map(|_| ())
    }
}

pub fn env_num<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}
