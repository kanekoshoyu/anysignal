use super::{PartitionKey, PartitionedSource, PartitionStats};
use crate::adapter::hyperliquid_s3::node_fills::NodeFills;
use crate::database::{insert_hyperliquid_fills, QuestDbClient};
use crate::error::AnySignalResult;
use chrono::{NaiveDateTime, Timelike};

// ---------------------------------------------------------------------------
// Key type
// ---------------------------------------------------------------------------

/// Identifies one hour of legacy node fills in the `hl-mainnet-node-data` archive.
///
/// Covers the `node_fills/hourly/` dataset (2025-05-25T14:00 – 2025-07-27T08:00).
pub struct NodeFillsLegacyHourKey {
    /// Start of the hour, e.g. `2025-06-01T12:00:00`.
    pub hour: NaiveDateTime,
}

impl std::fmt::Display for NodeFillsLegacyHourKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hour.format("%Y-%m-%dT%H:00:00"))
    }
}

impl PartitionKey for NodeFillsLegacyHourKey {}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Hyperliquid legacy node fills source — one partition = one calendar hour.
///
/// Reads from `s3://hl-mainnet-node-data/node_fills/hourly/{YYYYMMDD}/{H}.lz4`.
/// Data is inserted into `hyperliquid_fill` with `source = 'HYPERLIQUID_NODE'`,
/// the same value used by [`NodeFillsSource`] (node_fills_by_block).
///
/// The two archives are non-overlapping except for 2025-07-27 hour 8 which
/// appears in both.  The `partition_exists` check prevents double-ingestion
/// regardless of which source is backfilled first.
///
/// [`NodeFillsSource`]: crate::backfill::node_fills_by_block::NodeFillsSource
pub struct NodeFillsLegacySource {
    fetcher: NodeFills,
}

impl NodeFillsLegacySource {
    pub async fn new() -> AnySignalResult<Self> {
        Ok(Self {
            fetcher: NodeFills::new().await?,
        })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for NodeFillsLegacySource {
    type Key = NodeFillsLegacyHourKey;

    async fn partition_exists(
        db: &QuestDbClient,
        key: &NodeFillsLegacyHourKey,
    ) -> AnySignalResult<bool> {
        let hour_end = key.hour + chrono::Duration::hours(1);
        let table = db.table_name("hyperliquid_fill");
        let sql = format!(
            "SELECT count() FROM {table} \
             WHERE source = 'HYPERLIQUID_NODE' \
             AND ts >= '{}Z' \
             AND ts < '{}Z'",
            key.hour.format("%Y-%m-%dT%H:%M:%S"),
            hour_end.format("%Y-%m-%dT%H:%M:%S"),
        );
        Ok(db.count(&sql).await? > 0)
    }

    async fn ingest_partition(
        &self,
        db: &QuestDbClient,
        key: &NodeFillsLegacyHourKey,
    ) -> AnySignalResult<PartitionStats> {
        let date = key.hour.date();
        let hour = key.hour.hour() as u8;

        let t_fetch = std::time::Instant::now();
        let mut fills = self.fetcher.fetch_and_parse(date, hour).await?;
        let fetch_ms = t_fetch.elapsed().as_millis();

        // Sort by timestamp so database receives data in order,
        // avoiding the slow out-of-order commit path.
        fills.sort_unstable_by_key(|f| f.time_ms);

        let t_insert = std::time::Instant::now();
        // Use block_in_place so the blocking mutex + synchronous HTTP flush
        // don't starve the Tokio thread pool.
        let table = db.table_name("hyperliquid_fill");
        let rows = tokio::task::block_in_place(|| {
            db.with_sender(|s| insert_hyperliquid_fills(s, &table, &fills))
        })? as u64;
        let insert_ms = t_insert.elapsed().as_millis();

        Ok(PartitionStats { rows, fetch_ms, insert_ms })
    }
}
