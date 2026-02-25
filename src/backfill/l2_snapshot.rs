use super::{PartitionKey, PartitionedSource};
use crate::adapter::hyperliquid_s3::market_data::MarketData;
use crate::database::{insert_l2_snapshots, QuestDbClient};
use crate::error::AnySignalResult;
use chrono::{NaiveDateTime, Timelike};

// ---------------------------------------------------------------------------
// Key type
// ---------------------------------------------------------------------------

/// Identifies one (hour, coin) partition in the L2 orderbook archive.
pub struct L2PartitionKey {
    pub hour: NaiveDateTime,
    pub coin: String,
}

impl std::fmt::Display for L2PartitionKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}+{}", self.hour.format("%Y-%m-%dT%H:00:00"), self.coin)
    }
}

impl PartitionKey for L2PartitionKey {}

// ---------------------------------------------------------------------------
// Source
// ---------------------------------------------------------------------------

/// Hyperliquid L2 orderbook source — one partition = one (hour, coin) pair.
pub struct L2SnapshotSource {
    fetcher: MarketData,
}

impl L2SnapshotSource {
    pub async fn new() -> AnySignalResult<Self> {
        Ok(Self {
            fetcher: MarketData::new().await?,
        })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for L2SnapshotSource {
    type Key = L2PartitionKey;

    async fn partition_exists(db: &QuestDbClient, key: &L2PartitionKey) -> AnySignalResult<bool> {
        let hour_end = key.hour + chrono::Duration::hours(1);
        let sql = format!(
            "SELECT count() FROM l2_snapshot \
             WHERE ticker = '{}' \
             AND ts >= '{}Z' \
             AND ts < '{}Z'",
            key.coin,
            key.hour.format("%Y-%m-%dT%H:%M:%S"),
            hour_end.format("%Y-%m-%dT%H:%M:%S"),
        );
        Ok(db.count(&sql).await? > 0)
    }

    async fn ingest_partition(
        &self,
        db: &QuestDbClient,
        key: &L2PartitionKey,
    ) -> AnySignalResult<u64> {
        let date = key.hour.date();
        let hour = key.hour.hour() as u8;
        let snapshots = self.fetcher.fetch_and_parse(date, hour, &key.coin).await?;
        let n = db.with_sender(|s| insert_l2_snapshots(s, &snapshots))?;
        Ok(n as u64)
    }
}
