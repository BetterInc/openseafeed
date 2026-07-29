//! DMA history importer — a separate worker mode (`openseafeed-archiver
//! import-dma`) that watches the Danish Maritime Authority's daily dump
//! listing (aisdk-YYYY-MM-DD.zip, published a few days behind real time)
//! and backfills each new file into the ClickHouse archive.
//!
//! Historical data deliberately bypasses the live pipeline: day-old
//! positions on the live stream would corrupt the current picture. Rows go
//! straight into `positions`/`statics` with their original timestamps and
//! station label `import:dma`. Processed files are tracked in
//! `<db>.dma_imports`, so restarts and re-polls are idempotent.

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::time::Duration;

use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};
use futures::StreamExt;
use regex::Regex;

use crate::ch::{env_num, ClickHouse};
use crate::{PositionRow, StaticRow};

const INSERT_BATCH: usize = 10_000;

pub struct Config {
    /// Download base (the public website URL).
    pub url: String,
    /// S3 ListObjectsV2 endpoint behind the website (the site itself is a
    /// JS shell around this bucket).
    pub list_url: String,
    pub poll: Duration,
    /// How many days back to consider on an empty state (avoid importing
    /// years of history by accident).
    pub backfill_days: i64,
    pub rows_per_sec: u64,
    pub tmp_dir: std::path::PathBuf,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            url: std::env::var("OSF_DMA_URL").unwrap_or_else(|_| "http://aisdata.ais.dk/".into()),
            list_url: std::env::var("OSF_DMA_LIST_URL").unwrap_or_else(|_| {
                "http://aisdata.ais.dk.s3.eu-central-1.amazonaws.com/".into()
            }),
            poll: Duration::from_secs(env_num("OSF_DMA_POLL_SECS", 3600u64)),
            // DMA publishes each day's file ~3 days later, so the window
            // must comfortably exceed that lag.
            backfill_days: env_num("OSF_DMA_BACKFILL_DAYS", 7i64),
            rows_per_sec: env_num("OSF_DMA_ROWS_PER_SEC", 20_000u64),
            tmp_dir: std::env::var("OSF_DMA_TMP")
                .map(Into::into)
                .unwrap_or_else(|_| std::env::temp_dir()),
        }
    }
}

pub async fn run(ch: Arc<ClickHouse>, cfg: Config) -> anyhow::Result<()> {
    let http = reqwest::Client::builder()
        .user_agent("openseafeed-dma-import (+https://github.com/openseafeed/openseafeed)")
        // DMA's TLS certificate has been seen expired; the data is public
        // and integrity-checked only loosely (it's a bulk historical dump).
        .danger_accept_invalid_certs(true)
        .build()?;
    tracing::info!(url = cfg.url, poll_secs = cfg.poll.as_secs(), "dma importer starting");

    loop {
        if let Err(e) = poll_once(&ch, &http, &cfg).await {
            tracing::warn!(error = %e, "dma poll failed; will retry next interval");
        }
        tokio::select! {
            _ = tokio::time::sleep(cfg.poll) => {}
            _ = tokio::signal::ctrl_c() => break,
        }
    }
    Ok(())
}

async fn poll_once(ch: &ClickHouse, http: &reqwest::Client, cfg: &Config) -> anyhow::Result<()> {
    // S3 ListObjectsV2 with delimiter collapses the per-year subdirectories,
    // leaving the daily root files. Follow continuation tokens for the day
    // the root listing outgrows one page.
    let mut files = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut params = vec![("list-type", "2".to_string()), ("delimiter", "/".to_string())];
        if let Some(t) = &token {
            params.push(("continuation-token", t.clone()));
        }
        let page = http
            .get(&cfg.list_url)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        files.extend(parse_listing(&page));
        token = next_continuation_token(&page);
        if token.is_none() {
            break;
        }
    }
    let cutoff = Utc::now().date_naive() - chrono::Duration::days(cfg.backfill_days);
    let done = imported_files(ch).await?;

    for (date, name) in plan_imports(files, cutoff, &done) {
        tracing::info!(file = name, "new DMA dump found, importing");
        // Idempotency: a crash mid-import leaves rows without a dma_imports
        // record; clear that day's import rows before (re)importing.
        ch.exec(
            &format!(
                "DELETE FROM {}.positions WHERE station = 'import:dma' AND toDate(ts) = '{}'",
                ch.db, date
            ),
            None,
        )
        .await?;
        let rows = import_file(http, cfg, &name).await?;
        ch.exec(
            &format!(
                "INSERT INTO {}.dma_imports (file, rows, imported_at) VALUES ('{}', {}, now())",
                ch.db, name, rows
            ),
            None,
        )
        .await?;
        tracing::info!(file = name, rows, "DMA dump imported");
    }
    Ok(())
}

/// Extract (date, filename) for every `aisdk-YYYY-MM-DD.zip` referenced by
/// the listing page, tolerant of any directory-index HTML flavor.
pub fn parse_listing(html: &str) -> Vec<(NaiveDate, String)> {
    let re = Regex::new(r"aisdk-(\d{4})-(\d{2})-(\d{2})\.zip").unwrap();
    let mut out: Vec<(NaiveDate, String)> = re
        .captures_iter(html)
        .filter_map(|c| {
            let date = NaiveDate::from_ymd_opt(
                c[1].parse().ok()?,
                c[2].parse().ok()?,
                c[3].parse().ok()?,
            )?;
            Some((date, c[0].to_string()))
        })
        .collect();
    out.sort();
    out.dedup();
    out
}

/// Decide what to import and in which order: inside the backfill window,
/// not yet imported, NEWEST FIRST — the most recent day is the most queried,
/// so it must be available soonest; older backlog files follow.
fn plan_imports(
    mut files: Vec<(NaiveDate, String)>,
    cutoff: NaiveDate,
    done: &std::collections::HashSet<String>,
) -> Vec<(NaiveDate, String)> {
    files.sort();
    files.dedup();
    files.reverse();
    files
        .into_iter()
        .filter(|(date, name)| *date >= cutoff && !done.contains(name))
        .collect()
}

fn next_continuation_token(xml: &str) -> Option<String> {
    if !xml.contains("<IsTruncated>true</IsTruncated>") {
        return None;
    }
    let start = xml.find("<NextContinuationToken>")? + "<NextContinuationToken>".len();
    let end = xml[start..].find("</NextContinuationToken>")?;
    Some(xml[start..start + end].to_string())
}

async fn imported_files(ch: &ClickHouse) -> anyhow::Result<std::collections::HashSet<String>> {
    let text = ch
        .exec(
            &format!("SELECT DISTINCT file FROM {}.dma_imports FORMAT TSV", ch.db),
            None,
        )
        .await?;
    Ok(text.lines().map(str::to_string).collect())
}

async fn import_file(
    http: &reqwest::Client,
    cfg: &Config,
    name: &str,
) -> anyhow::Result<u64> {
    // 1. Download to a temp file (zips are GB-scale; the central directory
    //    sits at the end, so streaming decode isn't possible).
    let path = cfg.tmp_dir.join(name);
    let url = format!("{}/{name}", cfg.url.trim_end_matches('/'));
    let resp = http.get(&url).send().await?.error_for_status()?;
    let total = resp.content_length().unwrap_or(0);
    let mut stream = resp.bytes_stream();
    let mut file = tokio::fs::File::create(&path).await?;
    let mut got: u64 = 0;
    let mut last_log = std::time::Instant::now();
    use tokio::io::AsyncWriteExt;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        got += chunk.len() as u64;
        if last_log.elapsed() > Duration::from_secs(30) {
            tracing::info!(file = name, mb = got / 1_048_576, total_mb = total / 1_048_576, "downloading");
            last_log = std::time::Instant::now();
        }
    }
    file.flush().await?;
    drop(file);

    // 2. Parse + insert on a blocking thread (zip/csv are sync), throttled.
    let ch2 = ClickHouse::from_env();
    let rows_per_sec = cfg.rows_per_sec;
    let path2 = path.clone();
    let result = tokio::task::spawn_blocking(move || -> anyhow::Result<u64> {
        let rt = tokio::runtime::Handle::current();
        let f = std::fs::File::open(&path2)?;
        let mut zip = zip::ZipArchive::new(f)?;
        let idx = (0..zip.len())
            .find(|&i| zip.by_index(i).map(|e| e.name().ends_with(".csv")).unwrap_or(false))
            .ok_or_else(|| anyhow::anyhow!("no csv entry in zip"))?;
        let entry = zip.by_index(idx)?;
        import_csv(&rt, &ch2, entry, rows_per_sec)
    })
    .await?;

    let _ = tokio::fs::remove_file(&path).await;
    result
}

fn import_csv(
    rt: &tokio::runtime::Handle,
    ch: &ClickHouse,
    reader: impl Read,
    rows_per_sec: u64,
) -> anyhow::Result<u64> {
    let mut rdr = csv::ReaderBuilder::new().flexible(true).from_reader(reader);
    let headers = rdr.headers()?.clone();
    let col = |n: &str| headers.iter().position(|h| h.trim_start_matches("# ") == n);
    let (Some(c_ts), Some(c_mmsi), Some(c_lat), Some(c_lon)) = (
        col("Timestamp"),
        col("MMSI"),
        col("Latitude"),
        col("Longitude"),
    ) else {
        anyhow::bail!("unexpected CSV header: {headers:?}");
    };
    let c_nav = col("Navigational status");
    let c_sog = col("SOG");
    let c_cog = col("COG");
    let c_hdg = col("Heading");
    let c_imo = col("IMO");
    let c_call = col("Callsign");
    let c_name = col("Name");
    let c_draught = col("Draught");
    let c_dest = col("Destination");
    let (c_a, c_b, c_c, c_d) = (col("A"), col("B"), col("C"), col("D"));

    let mut positions: Vec<PositionRow> = Vec::with_capacity(INSERT_BATCH);
    let mut statics: HashMap<u32, StaticRow> = HashMap::new();
    let mut imported: u64 = 0;
    let window = std::time::Instant::now();

    for rec in rdr.records() {
        let Ok(rec) = rec else { continue };
        let get = |i: Option<usize>| i.and_then(|i| rec.get(i)).unwrap_or("").trim();
        let Some(ts) = parse_dma_ts(get(Some(c_ts))) else { continue };
        let Ok(mmsi) = get(Some(c_mmsi)).parse::<u32>() else { continue };
        let (Ok(lat), Ok(lon)) = (
            get(Some(c_lat)).parse::<f64>(),
            get(Some(c_lon)).parse::<f64>(),
        ) else {
            continue;
        };
        if !(-90.0..=90.0).contains(&lat) || !(-180.0..=180.0).contains(&lon) || mmsi == 0 {
            continue;
        }
        positions.push(PositionRow {
            ts: ts.clone(),
            mmsi,
            msg_type: "DmaImport".into(),
            lat,
            lon,
            sog: get(c_sog).parse().ok().filter(|v: &f32| *v < 102.3),
            cog: get(c_cog).parse().ok().filter(|v: &f32| *v < 360.0),
            heading: get(c_hdg).parse().ok().filter(|v: &u16| *v < 511),
            nav_status: nav_status_code(get(c_nav)),
            station: "import:dma".into(),
        });

        let name = get(c_name);
        if !name.is_empty() && name != "Unknown" {
            statics.entry(mmsi).or_insert_with(|| StaticRow {
                ts: ts.clone(),
                mmsi,
                name: name.into(),
                call_sign: get(c_call).to_string(),
                imo: get(c_imo).replace("Unknown", "").parse().unwrap_or(0),
                ship_type: 0, // DMA publishes coarse category strings, not AIS codes
                destination: get(c_dest).to_string(),
                draught: get(c_draught).parse().unwrap_or(0.0),
                dim_a: get(c_a).parse().unwrap_or(0),
                dim_b: get(c_b).parse().unwrap_or(0),
                dim_c: get(c_c).parse().unwrap_or(0),
                dim_d: get(c_d).parse().unwrap_or(0),
            });
        }

        if positions.len() >= INSERT_BATCH {
            rt.block_on(ch.insert("positions", &positions))?;
            imported += positions.len() as u64;
            positions.clear();
            // Throttle: stay at or below the configured import rate so a
            // multi-GB backfill never starves the live archiver.
            let target = Duration::from_secs_f64(imported as f64 / rows_per_sec as f64);
            if let Some(sleep) = target.checked_sub(window.elapsed()) {
                std::thread::sleep(sleep);
            }
        }
    }
    rt.block_on(ch.insert("positions", &positions))?;
    imported += positions.len() as u64;
    let static_rows: Vec<StaticRow> = statics.into_values().collect();
    rt.block_on(ch.insert("statics", &static_rows))?;
    Ok(imported)
}

/// DMA timestamps are `dd/mm/yyyy HH:MM:SS` local-agnostic; treat as UTC.
fn parse_dma_ts(s: &str) -> Option<String> {
    let dt = NaiveDateTime::parse_from_str(s, "%d/%m/%Y %H:%M:%S").ok()?;
    Some(
        Utc.from_utc_datetime(&dt)
            .format("%Y-%m-%d %H:%M:%S%.3f")
            .to_string(),
    )
}

/// Map DMA's navigational-status strings back to ITU codes.
fn nav_status_code(s: &str) -> Option<u8> {
    Some(match s {
        "Under way using engine" => 0,
        "At anchor" => 1,
        "Not under command" => 2,
        "Restricted maneuverability" => 3,
        "Constrained by her draught" => 4,
        "Moored" => 5,
        "Aground" => 6,
        "Engaged in fishing" => 7,
        "Under way sailing" => 8,
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_listing_html() {
        let html = r#"<html><a href="aisdk-2026-07-20.zip">aisdk-2026-07-20.zip</a>
            <a href="/sub/aisdk-2026-07-21.zip">aisdk-2026-07-21.zip</a>
            aisdk-2026-07-21.zip (duplicate) <a href="other.txt">x</a></html>"#;
        let files = parse_listing(html);
        assert_eq!(
            files.iter().map(|(_, n)| n.as_str()).collect::<Vec<_>>(),
            vec!["aisdk-2026-07-20.zip", "aisdk-2026-07-21.zip"]
        );
        assert_eq!(files[0].0, NaiveDate::from_ymd_opt(2026, 7, 20).unwrap());
    }

    #[test]
    fn parses_dma_timestamp() {
        assert_eq!(
            parse_dma_ts("23/07/2026 14:05:09").as_deref(),
            Some("2026-07-23 14:05:09.000")
        );
        assert!(parse_dma_ts("garbage").is_none());
    }

    #[test]
    fn plans_newest_first_within_window_skipping_done() {
        let today = Utc::now().date_naive();
        let day = |n: i64| today - chrono::Duration::days(n);
        let name = |d: NaiveDate| format!("aisdk-{d}.zip");

        // Listing: daily files from 9 to 3 days old (DMA's usual lag),
        // handed over unsorted like a raw listing could be.
        let mut files: Vec<(NaiveDate, String)> =
            (3..=9).map(|n| (day(n), name(day(n)))).collect();
        files.swap(0, 4);
        // One mid-window file already imported.
        let done: std::collections::HashSet<String> = [name(day(5))].into();
        // Same window rule as poll_once: today - backfill_days, inclusive.
        let cutoff = day(7);

        let plan = plan_imports(files, cutoff, &done);
        let got: Vec<String> = plan.into_iter().map(|(_, n)| n).collect();
        // Newest first; day 5 skipped (done); days 8-9 outside the window.
        let want: Vec<String> = [3i64, 4, 6, 7].iter().map(|&n| name(day(n))).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn maps_nav_status() {
        assert_eq!(nav_status_code("Moored"), Some(5));
        assert_eq!(nav_status_code("Unknown value"), None);
    }
}
