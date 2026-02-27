use super::{PartitionKey, PartitionedSource, PartitionStats};
use crate::adapter::hyperliquid_s3::node_fills_by_block::NodeFillsByBlock;
use crate::database::{insert_hyperliquid_fills, QuestDbClient};
use crate::error::AnySignalResult;
use chrono::{NaiveDateTime, Timelike};

// ---------------------------------------------------------------------------
// Key type
// ---------------------------------------------------------------------------

/// Identifies one hour of node fills in the `hl-mainnet-node-data` archive.
pub struct NodeFillsHourKey {
    /// Start of the hour, e.g. `2025-08-01T03:00:00`.
    pub hour: NaiveDateTime,
}

impl std::fmt::Display for NodeFillsHourKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.hour.format("%Y-%m-%dT%H:00:00"))
    }
}

impl PartitionKey for NodeFillsHourKey {}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Hyperliquid node fills source — one partition = one calendar hour.
pub struct NodeFillsSource {
    fetcher: NodeFillsByBlock,
}

impl NodeFillsSource {
    pub async fn new() -> AnySignalResult<Self> {
        Ok(Self {
            fetcher: NodeFillsByBlock::new().await?,
        })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for NodeFillsSource {
    type Key = NodeFillsHourKey;

    async fn partition_exists(db: &QuestDbClient, key: &NodeFillsHourKey) -> AnySignalResult<bool> {
        let hour_end = key.hour + chrono::Duration::hours(1);
        let sql = format!(
            "SELECT count() FROM hyperliquid_fill \
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
        key: &NodeFillsHourKey,
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
        let rows = tokio::task::block_in_place(|| {
            db.with_sender(|s| insert_hyperliquid_fills(s, &fills))
        })? as u64;
        let insert_ms = t_insert.elapsed().as_millis();

        Ok(PartitionStats { rows, fetch_ms, insert_ms })
    }
}
