use super::{PartitionKey, PartitionedSource, PartitionStats};
use crate::adapter::hyperliquid_s3::node_fills_by_block::{NodeFillsByBlock, ParsedFill};
use crate::database::{insert_hyperliquid_fill_1m_aggregate, Fill1mAggregate, QuestDbClient};
use crate::error::AnySignalResult;
use chrono::{NaiveDateTime, Timelike};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Key type
// ---------------------------------------------------------------------------

/// Identifies one calendar hour of aggregated node fills in the
/// `hyperliquid_fill_1m_aggregate` table.
pub struct NodeFills1mAggregateHourKey {
    /// Start of the hour, e.g. `2025-08-01T03:00:00`.
    pub hour: NaiveDateTime,
}

impl std::fmt::Display for NodeFills1mAggregateHourKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hour.format("%Y-%m-%dT%H:00:00"))
    }
}

impl PartitionKey for NodeFills1mAggregateHourKey {}

// ---------------------------------------------------------------------------
// Aggregation logic
// ---------------------------------------------------------------------------

/// Aggregate a slice of raw fills into per-`(coin, category, buy_side, minute)`
/// buckets.
///
/// The minute bucket is **left-closed**: timestamp `12:00:00.000` through
/// `12:00:59.999` all map to bucket `12:00:00.000`; `12:01:00.000` maps to
/// bucket `12:01:00.000`.
///
/// Aggregation:
/// - `quantity`    → sum of fill quantities
/// - `trade_count` → number of individual fills
pub fn aggregate_fills(fills: &[ParsedFill]) -> Vec<Fill1mAggregate> {
    // Key: (minute_ms, coin, category, buy_side)
    type Key = (i64, String, String, bool);
    let mut map: HashMap<Key, (f64, i64)> = HashMap::new();

    for fill in fills {
        // Floor to the nearest minute boundary (left-closed).
        let minute_ms = fill.time_ms / 60_000 * 60_000;
        let buy_side = fill.side == "buy";
        let key: Key = (minute_ms, fill.coin.clone(), fill.category.clone(), buy_side);
        let entry = map.entry(key).or_insert((0.0, 0));
        entry.0 += fill.quantity;
        entry.1 += 1;
    }

    map.into_iter()
        .map(
            |((minute_ms, coin, category, buy_side), (quantity, trade_count))| Fill1mAggregate {
                minute_ms,
                coin,
                category,
                buy_side,
                quantity,
                trade_count,
            },
        )
        .collect()
}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Backfill source that fetches raw node fills for one hour, aggregates them
/// into 1-minute buckets, and writes to `hyperliquid_fill_1m_aggregate`.
pub struct NodeFills1mAggregateSource {
    fetcher: NodeFillsByBlock,
}

impl NodeFills1mAggregateSource {
    pub async fn new() -> AnySignalResult<Self> {
        Ok(Self {
            fetcher: NodeFillsByBlock::new().await?,
        })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for NodeFills1mAggregateSource {
    type Key = NodeFills1mAggregateHourKey;

    async fn partition_exists(
        db: &QuestDbClient,
        key: &NodeFills1mAggregateHourKey,
    ) -> AnySignalResult<bool> {
        let hour_end = key.hour + chrono::Duration::hours(1);
        let table = db.table_name("hyperliquid_fill_1m_aggregate");
        let sql = format!(
            "SELECT count() FROM {table} \
             WHERE ts >= '{}Z' \
             AND ts < '{}Z'",
            key.hour.format("%Y-%m-%dT%H:%M:%S"),
            hour_end.format("%Y-%m-%dT%H:%M:%S"),
        );
        Ok(db.count(&sql).await? > 0)
    }

    async fn ingest_partition(
        &self,
        db: &QuestDbClient,
        key: &NodeFills1mAggregateHourKey,
    ) -> AnySignalResult<PartitionStats> {
        // S3 files are keyed by *block processing time*, not fill timestamp.
        // A block committed just after the hour boundary (e.g. 13:00:02) may
        // contain fills whose timestamps belong to the previous hour (12:59:58).
        // To guarantee every minute bucket is complete we:
        //   1. Fetch the primary file for hour H.
        //   2. Peek at hour H+1 and pull any fills whose time_ms < hour_end_ms.
        //   3. Filter the combined set to [hour_start_ms, hour_end_ms) so the
        //      H+1 ingest later won't double-count those same fills.
        let hour_start_ms = key.hour.and_utc().timestamp_millis();
        let hour_end_ms = hour_start_ms + 3_600_000;

        let t_fetch = std::time::Instant::now();

        // Primary fetch.
        let mut fills = self
            .fetcher
            .fetch_and_parse(key.hour.date(), key.hour.hour() as u8)
            .await?;

        // S3 files are keyed by block *processing* time, not fill timestamp,
        // so fills for this hour can leak into adjacent files in both directions:
        //
        //   H-1 file — a block committed just before H may contain fills
        //               timestamped in H (exchange clock slightly ahead of the
        //               S3 commit clock).
        //   H+1 file — a block committed just after H ends may contain fills
        //               timestamped in H (common for the last ~seconds of H).
        //
        // We peek at both neighbours and merge any fills that belong to H.
        // The final `retain` keeps only [hour_start_ms, hour_end_ms) so the
        // H-1 / H+1 ingests won't double-count the same rows.
        let prev = key.hour - chrono::Duration::hours(1);
        let next = key.hour + chrono::Duration::hours(1);

        for (neighbour_date, neighbour_hour) in [
            (prev.date(), prev.hour() as u8),
            (next.date(), next.hour() as u8),
        ] {
            match self
                .fetcher
                .fetch_and_parse(neighbour_date, neighbour_hour)
                .await
            {
                Ok(neighbour_fills) => {
                    fills.extend(
                        neighbour_fills
                            .into_iter()
                            .filter(|f| f.time_ms >= hour_start_ms && f.time_ms < hour_end_ms),
                    );
                }
                Err(e) => {
                    tracing::debug!(
                        key = %key,
                        neighbour_hour,
                        error = %e,
                        "neighbour-hour lookahead unavailable; skipping"
                    );
                }
            }
        }

        // Final guard: discard anything outside [hour_start_ms, hour_end_ms).
        fills.retain(|f| f.time_ms >= hour_start_ms && f.time_ms < hour_end_ms);

        let fetch_ms = t_fetch.elapsed().as_millis();
        let raw_fills = fills.len();

        let mut aggregated = aggregate_fills(&fills);
        // Sort by minute_ms so QuestDB receives rows in ascending order,
        // avoiding the slow out-of-order commit path.
        aggregated.sort_unstable_by_key(|a| a.minute_ms);

        let t_insert = std::time::Instant::now();
        // Use block_in_place so the blocking mutex + synchronous HTTP flush
        // don't starve the Tokio thread pool.
        let table = db.table_name("hyperliquid_fill_1m_aggregate");
        let rows = tokio::task::block_in_place(|| {
            db.with_sender(|s| insert_hyperliquid_fill_1m_aggregate(s, &table, &aggregated))
        })? as u64;
        let insert_ms = t_insert.elapsed().as_millis();

        // Log the raw fill count here since it's specific to this source
        // and not part of the generic PartitionStats.
        tracing::debug!(key = %key, raw_fills, "aggregated from raw fills");

        Ok(PartitionStats { rows, fetch_ms, insert_ms })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_fill(coin: &str, category: &str, side: &str, time_ms: i64, quantity: f64) -> ParsedFill {
        ParsedFill {
            wallet: "0xtest".to_string(),
            coin: coin.to_string(),
            time_ms,
            side: side.to_string(),
            category: category.to_string(),
            is_taker: true,
            price: 100.0,
            quantity,
            position_before: 0.0,
            realized_pnl: 0.0,
        }
    }

    #[test]
    fn aggregate_groups_by_minute_coin_category_side() {
        // Two fills in the same minute bucket → combined; one in next minute → separate.
        let fills = vec![
            make_fill("BTC", "Buy", "buy", 60_000, 1.0),  // bucket 60_000 ms
            make_fill("BTC", "Buy", "buy", 90_000, 2.0),  // bucket 60_000 ms (same minute)
            make_fill("BTC", "Buy", "buy", 120_000, 0.5), // bucket 120_000 ms
        ];
        let mut aggs = aggregate_fills(&fills);
        aggs.sort_unstable_by_key(|a| a.minute_ms);

        assert_eq!(aggs.len(), 2);

        let first = &aggs[0];
        assert_eq!(first.minute_ms, 60_000);
        assert_eq!(first.coin, "BTC");
        assert_eq!(first.category, "Buy");
        assert!(first.buy_side);
        assert!((first.quantity - 3.0).abs() < 1e-9);
        assert_eq!(first.trade_count, 2);

        let second = &aggs[1];
        assert_eq!(second.minute_ms, 120_000);
        assert!((second.quantity - 0.5).abs() < 1e-9);
        assert_eq!(second.trade_count, 1);
    }

    #[test]
    fn aggregate_separates_buy_and_sell() {
        let fills = vec![
            make_fill("ETH", "Sell", "sell", 0, 5.0),
            make_fill("ETH", "Buy", "buy", 0, 3.0),
        ];
        let aggs = aggregate_fills(&fills);
        assert_eq!(aggs.len(), 2);
    }

    #[test]
    fn minute_bucket_is_left_closed() {
        // 12:00:00.000 → bucket 12:00; 12:00:59.999 → bucket 12:00; 12:01:00.000 → bucket 12:01
        let base_ms: i64 = 12 * 3600 * 1_000; // 12:00:00.000 in ms
        let fills = vec![
            make_fill("SOL", "Open Long", "buy", base_ms, 1.0),          // 12:00:00.000
            make_fill("SOL", "Open Long", "buy", base_ms + 59_999, 1.0), // 12:00:59.999
            make_fill("SOL", "Open Long", "buy", base_ms + 60_000, 1.0), // 12:01:00.000
        ];
        let mut aggs = aggregate_fills(&fills);
        aggs.sort_unstable_by_key(|a| a.minute_ms);

        assert_eq!(aggs.len(), 2);
        assert_eq!(aggs[0].minute_ms, base_ms);
        assert_eq!(aggs[0].trade_count, 2);
        assert_eq!(aggs[1].minute_ms, base_ms + 60_000);
        assert_eq!(aggs[1].trade_count, 1);
    }

    #[tokio::test]
    #[ignore = "requires real AWS credentials and outbound network access"]
    async fn integration_fetch_and_aggregate_20250801_h3() {
        dotenvy::dotenv().ok();
        let source = NodeFills1mAggregateSource::new()
            .await
            .expect("S3 client init failed");
        let hour =
            chrono::NaiveDate::from_ymd_opt(2025, 8, 1).unwrap().and_hms_opt(3, 0, 0).unwrap();
        let key = NodeFills1mAggregateHourKey { hour };
        let fills = source.fetcher.fetch_and_parse(key.hour.date(), 3).await.expect("fetch failed");
        let aggs = aggregate_fills(&fills);
        assert!(!aggs.is_empty(), "expected aggregated rows for 2025-08-01 h3");
        eprintln!("OK: {} minute-buckets from {} raw fills", aggs.len(), fills.len());
    }
}
