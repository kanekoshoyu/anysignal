use super::{PartitionKey, PartitionedSource, PartitionStats};
use crate::adapter::error::AdapterError;
use crate::database::{insert_market_state_1m, MarketStateRow, QuestDbClient};
use crate::error::{AnySignalError, AnySignalResult};
use chrono::NaiveDateTime;
use std::collections::HashMap;

const LIQUIDATION_LONG: &[&str] =
    &["Liquidated Isolated Long", "Liquidated Cross Long"];

const LIQUIDATION_SHORT: &[&str] =
    &["Liquidated Isolated Short", "Liquidated Cross Short"];

// ---------------------------------------------------------------------------
// Key type
// ---------------------------------------------------------------------------

pub struct MarketState1mHourKey {
    pub hour: NaiveDateTime,
}

impl std::fmt::Display for MarketState1mHourKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hour.format("%Y-%m-%dT%H:00:00"))
    }
}

impl PartitionKey for MarketState1mHourKey {}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Backfill source that computes `market_state_1m` from two existing QuestDB
/// tables:
///
/// - `hyperliquid_fill_1m_aggregate` — minute-level fill stats (trade volume,
///   trade count, liquidation volumes).
/// - `market_data` — per-minute price/market snapshots per coin (oracle/mark
///   price, open interest, funding rate, 24h volume).  Each minute bucket is
///   joined to the exact same-minute snapshot from `market_data`.
///
/// No S3 access is required — this is a pure DB-to-DB computation.
pub struct MarketState1mSource;

impl MarketState1mSource {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl PartitionedSource for MarketState1mSource {
    type Key = MarketState1mHourKey;

    /// Run only 2 partitions concurrently — each one fires 2 QuestDB SELECT
    /// queries plus an ILP write, so the default of 8 would issue 16
    /// simultaneous reads and overwhelm a small QuestDB instance.
    fn concurrency() -> usize {
        2
    }

    async fn partition_exists(
        db: &QuestDbClient,
        key: &MarketState1mHourKey,
    ) -> AnySignalResult<bool> {
        let hour_end = key.hour + chrono::Duration::hours(1);
        let sql = format!(
            "SELECT count() FROM market_state_1m \
             WHERE ts >= '{}Z' AND ts < '{}Z'",
            key.hour.format("%Y-%m-%dT%H:%M:%S"),
            hour_end.format("%Y-%m-%dT%H:%M:%S"),
        );
        Ok(db.count(&sql).await? > 0)
    }

    async fn ingest_partition(
        &self,
        db: &QuestDbClient,
        key: &MarketState1mHourKey,
    ) -> AnySignalResult<PartitionStats> {
        let hour_end = key.hour + chrono::Duration::hours(1);

        let t_fetch = std::time::Instant::now();

        // Fetch 1-minute fill aggregates for [H, H+1h).
        let fill_sql = format!(
            "SELECT ts, coin, category, buy_side, quantity, trade_count \
             FROM hyperliquid_fill_1m_aggregate \
             WHERE ts >= '{}Z' AND ts < '{}Z' \
             ORDER BY ts",
            key.hour.format("%Y-%m-%dT%H:%M:%S"),
            hour_end.format("%Y-%m-%dT%H:%M:%S"),
        );
        let fill_json = db.query_json(&fill_sql).await?;

        // Fetch per-minute market snapshots for [H, H+1h).
        // market_data stores minute-level snapshots so we scope to the exact
        // hour to avoid fetching millions of rows for the whole day.
        let market_sql = format!(
            "SELECT ts, ticker, category, value \
             FROM market_data \
             WHERE ts >= '{}Z' AND ts < '{}Z' \
             AND source = 'HYPERLIQUID_S3'",
            key.hour.format("%Y-%m-%dT%H:%M:%S"),
            hour_end.format("%Y-%m-%dT%H:%M:%S"),
        );
        let market_json = db.query_json(&market_sql).await?;

        let fetch_ms = t_fetch.elapsed().as_millis();

        // Build market snapshot map: coin → category → value
        let market_map = parse_market_data(&market_json)?;

        // Accumulate fill stats per (minute_ms, coin).
        let fill_rows = fill_json["dataset"]
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(&[]);

        let mut accum: HashMap<(i64, String), FillAccum> = HashMap::new();

        for row in fill_rows {
            let ts_str = row[0].as_str().unwrap_or("");
            let minute_ms = questdb_ts_to_ms(ts_str)?;
            let coin = row[1].as_str().unwrap_or("").to_string();
            let category = row[2].as_str().unwrap_or("");
            let buy_side = row[3].as_bool().unwrap_or(false);
            let quantity = row[4].as_f64().unwrap_or(0.0);
            let trade_count = row[5].as_i64().unwrap_or(0);

            let e = accum
                .entry((minute_ms, coin))
                .or_insert_with(FillAccum::default);

            // Count only buy side — buys/sells are paired so this avoids
            // double-counting the same notional volume.
            if buy_side {
                e.trade_volume += quantity;
                e.trade_count += trade_count;
            }

            if LIQUIDATION_LONG.contains(&category) {
                e.liq_long_volume += quantity;
                e.liq_long_count += trade_count;
            } else if LIQUIDATION_SHORT.contains(&category) {
                e.liq_short_volume += quantity;
                e.liq_short_count += trade_count;
            }
        }

        // Build output rows.
        let mut rows: Vec<MarketStateRow> = Vec::with_capacity(accum.len());

        for ((minute_ms, coin), a) in accum {
            let snap = market_map.get(&(minute_ms, coin.clone()));
            let price_oracle = snap_get(snap, "oracle_px");
            let price_mark = snap_get(snap, "mark_px");
            let price_mid = snap
                .and_then(|m| m.get("mid_px"))
                .copied()
                .unwrap_or_else(|| {
                    if price_oracle > 0.0 && price_mark > 0.0 {
                        (price_oracle + price_mark) / 2.0
                    } else {
                        0.0
                    }
                });

            rows.push(MarketStateRow {
                minute_ms,
                coin,
                price_oracle,
                price_mark,
                price_mid,
                open_interest: snap_get(snap, "open_interest"),
                funding_rate: snap_get(snap, "funding"),
                volume_24h_usd: snap_get(snap, "day_ntl_vlm"),
                trade_volume: a.trade_volume,
                trade_count: a.trade_count,
                liquidation_long_volume: a.liq_long_volume,
                liquidation_short_volume: a.liq_short_volume,
                liquidation_long_count: a.liq_long_count,
                liquidation_short_count: a.liq_short_count,
                predicted_funding_rate: None,
            });
        }

        // Sort so QuestDB receives rows in ascending timestamp order.
        rows.sort_unstable_by(|a, b| {
            a.minute_ms.cmp(&b.minute_ms).then(a.coin.cmp(&b.coin))
        });

        let t_insert = std::time::Instant::now();
        let row_count = tokio::task::block_in_place(|| {
            db.with_sender(|s| insert_market_state_1m(s, &rows))
        })? as u64;
        let insert_ms = t_insert.elapsed().as_millis();

        Ok(PartitionStats { rows: row_count, fetch_ms, insert_ms })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Per-(minute, coin) fill accumulator.
#[derive(Default)]
struct FillAccum {
    trade_volume: f64,
    trade_count: i64,
    liq_long_volume: f64,
    liq_long_count: i64,
    liq_short_volume: f64,
    liq_short_count: i64,
}

/// Build `(minute_ms, coin) → category → value` map from a `market_data`
/// query response (columns: ts, ticker, category, value).
///
/// market_data has per-minute snapshots so we key by both the minute
/// timestamp and the coin to allow exact-minute lookups.
fn parse_market_data(
    json: &serde_json::Value,
) -> AnySignalResult<HashMap<(i64, String), HashMap<String, f64>>> {
    let mut map: HashMap<(i64, String), HashMap<String, f64>> = HashMap::new();
    let rows = json["dataset"].as_array().map(Vec::as_slice).unwrap_or(&[]);
    for row in rows {
        let ts_str = row[0].as_str().unwrap_or("");
        let minute_ms = questdb_ts_to_ms(ts_str)?;
        let coin = row[1].as_str().unwrap_or("").to_string();
        let category = row[2].as_str().unwrap_or("").to_string();
        let value = row[3].as_f64().unwrap_or(0.0);
        map.entry((minute_ms, coin)).or_default().insert(category, value);
    }
    Ok(map)
}

/// Parse a QuestDB timestamp string (`"2025-06-01T12:00:00.000000Z"`) to Unix ms.
fn questdb_ts_to_ms(ts: &str) -> AnySignalResult<i64> {
    chrono::DateTime::parse_from_rfc3339(ts)
        .map(|dt| dt.timestamp_millis())
        .map_err(|e| {
            AnySignalError::Adapter(AdapterError::FetchError(format!(
                "invalid QuestDB timestamp '{ts}': {e}"
            )))
        })
}

/// Look up a value from an optional snapshot map, defaulting to `0.0`.
fn snap_get(snap: Option<&HashMap<String, f64>>, key: &str) -> f64 {
    snap.and_then(|m| m.get(key)).copied().unwrap_or(0.0)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn questdb_ts_to_ms_parses_correctly() {
        let ms = questdb_ts_to_ms("2025-06-01T12:00:00.000000Z").unwrap();
        // 2025-06-01T12:00:00 UTC
        assert_eq!(ms, 1748779200000);
    }

    #[test]
    fn questdb_ts_to_ms_rejects_invalid() {
        assert!(questdb_ts_to_ms("not-a-timestamp").is_err());
    }

    #[tokio::test]
    #[ignore = "requires live QuestDB with fill data"]
    async fn integration_ingest_20250601_h12() {
        dotenvy::dotenv().ok();
        use crate::config::Config;
        #[allow(unused_imports)]
        let config = Config::from_env();
        let db = crate::database::QuestDbClient::new(&config).expect("db");
        let source = MarketState1mSource::new();
        let hour = chrono::NaiveDate::from_ymd_opt(2025, 6, 1)
            .unwrap()
            .and_hms_opt(12, 0, 0)
            .unwrap();
        let key = MarketState1mHourKey { hour };
        let stats = source.ingest_partition(&db, &key).await.expect("ingest");
        eprintln!("rows={} fetch_ms={} insert_ms={}", stats.rows, stats.fetch_ms, stats.insert_ms);
        assert!(stats.rows > 0);
    }
}
