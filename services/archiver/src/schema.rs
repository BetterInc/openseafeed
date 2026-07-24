/// Schema migrations, applied idempotently at startup.
///
/// Retention model: rows live on the hot (local) volume for
/// `OSF_HOT_DAYS` (default 14), then — when `OSF_CLICKHOUSE_TIERED=1` and
/// the server has a storage policy named `tiered` with a `cold` (S3)
/// volume — parts are moved to object storage, where they remain queryable
/// through the same table at higher latency. Everything is deleted after
/// `OSF_RETAIN_DAYS` (default 365).
pub fn migrations(db: &str, hot_days: u32, retain_days: u32, tiered: bool) -> Vec<String> {
    let ttl = if tiered {
        format!(
            "TTL toDateTime(ts) + INTERVAL {hot_days} DAY TO VOLUME 'cold', \
                 toDateTime(ts) + INTERVAL {retain_days} DAY DELETE \
             SETTINGS storage_policy = 'tiered'"
        )
    } else {
        format!("TTL toDateTime(ts) + INTERVAL {retain_days} DAY DELETE")
    };
    vec![
        format!("CREATE DATABASE IF NOT EXISTS {db}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {db}.positions (
                ts         DateTime64(3, 'UTC'),
                mmsi       UInt32,
                msg_type   LowCardinality(String),
                lat        Float64,
                lon        Float64,
                sog        Nullable(Float32),
                cog        Nullable(Float32),
                heading    Nullable(UInt16),
                nav_status Nullable(UInt8),
                station    LowCardinality(String)
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMMDD(ts)
            ORDER BY (mmsi, ts)
            {ttl}"
        ),
        // Latest static/voyage data per vessel; ReplacingMergeTree keeps the
        // newest row per MMSI after merges.
        format!(
            "CREATE TABLE IF NOT EXISTS {db}.statics (
                ts          DateTime64(3, 'UTC'),
                mmsi        UInt32,
                name        String,
                call_sign   String,
                imo         UInt32,
                ship_type   UInt8,
                destination String,
                draught     Float32,
                dim_a       UInt16,
                dim_b       UInt16,
                dim_c       UInt8,
                dim_d       UInt8
            )
            ENGINE = ReplacingMergeTree(ts)
            ORDER BY mmsi"
        ),
    ]
}
