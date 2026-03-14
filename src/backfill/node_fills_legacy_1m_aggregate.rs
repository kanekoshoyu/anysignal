use super::{PartitionKey, PartitionedSource, PartitionStats};
use crate::adapter::hyperliquid_s3::node_fills::NodeFills;
use crate::backfill::node_fills_1m_aggregate::aggregate_fills;
use crate::database::{insert_hyperliquid_fill_1m_aggregate, QuestDbClient};
use crate::error::AnySignalResult;
use chrono::{NaiveDateTime, Timelike};

// ---------------------------------------------------------------------------
// Key type
// ---------------------------------------------------------------------------

/// Identifies one calendar hour of aggregated legacy node fills in the
/// `hyperliquid_fill_1m_aggregate` table.
///
/// Covers the `node_fills/hourly/` dataset (2025-05-25T14:00 – 2025-07-27T08:00).
pub struct NodeFillsLegacy1mAggregateHourKey {
    /// Start of the hour, e.g. `2025-06-01T12:00:00`.
    pub hour: NaiveDateTime,
}

impl std::fmt::Display for NodeFillsLegacy1mAggregateHourKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hour.format("%Y-%m-%dT%H:00:00"))
    }
}

impl PartitionKey for NodeFillsLegacy1mAggregateHourKey {}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Backfill source that fetches legacy node fills for one hour, aggregates them
/// into 1-minute buckets, and writes to `hyperliquid_fill_1m_aggregate`.
///
/// Unlike [`NodeFills1mAggregateSource`], this source does **not** fetch
/// neighbouring hours: the legacy archive (`node_fills/hourly/`) is keyed by
/// fill timestamp so S3 files align cleanly to hour boundaries without
/// boundary spillover.
///
/// [`NodeFills1mAggregateSource`]: crate::backfill::node_fills_1m_aggregate::NodeFills1mAggregateSource
pub struct NodeFillsLegacy1mAggregateSource {
    fetcher: NodeFills,
}

impl NodeFillsLegacy1mAggregateSource {
    pub async fn new() -> AnySignalResult<Self> {
        Ok(Self {
            fetcher: NodeFills::new().await?,
        })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for NodeFillsLegacy1mAggregateSource {
    type Key = NodeFillsLegacy1mAggregateHourKey;

    async fn partition_exists(
        db: &QuestDbClient,
        key: &NodeFillsLegacy1mAggregateHourKey,
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
        key: &NodeFillsLegacy1mAggregateHourKey,
    ) -> AnySignalResult<PartitionStats> {
        let date = key.hour.date();
        let hour = key.hour.hour() as u8;

        let t_fetch = std::time::Instant::now();
        let fills = self.fetcher.fetch_and_parse(date, hour).await?;
        let fetch_ms = t_fetch.elapsed().as_millis();
        let raw_fills = fills.len();

        let mut aggregated = aggregate_fills(&fills);
        aggregated.sort_unstable_by_key(|a| a.minute_ms);

        let t_insert = std::time::Instant::now();
        let table = db.table_name("hyperliquid_fill_1m_aggregate");
        let rows = tokio::task::block_in_place(|| {
            db.with_sender(|s| insert_hyperliquid_fill_1m_aggregate(s, &table, &aggregated))
        })? as u64;
        let insert_ms = t_insert.elapsed().as_millis();

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

    #[tokio::test]
    #[ignore = "requires real AWS credentials and outbound network access"]
    async fn integration_fetch_and_aggregate_20250601_h12() {
        dotenvy::dotenv().ok();
        let source = NodeFillsLegacy1mAggregateSource::new()
            .await
            .expect("S3 client init failed");
        let hour =
            chrono::NaiveDate::from_ymd_opt(2025, 6, 1).unwrap().and_hms_opt(12, 0, 0).unwrap();
        let key = NodeFillsLegacy1mAggregateHourKey { hour };
        let fills = source.fetcher.fetch_and_parse(key.hour.date(), 12).await.expect("fetch failed");
        let aggs = aggregate_fills(&fills);
        assert!(!aggs.is_empty(), "expected aggregated rows for 2025-06-01 h12");
        eprintln!("OK: {} minute-buckets from {} raw fills", aggs.len(), fills.len());
    }
}
