use super::{PartitionKey, PartitionedSource};
use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxs;
use crate::config::Config;
use crate::database::{insert_asset_ctxs, QuestDbClient};
use crate::error::AnySignalResult;
use chrono::NaiveDate;

// `NaiveDate` already implements `Display` ("YYYY-MM-DD").
impl PartitionKey for NaiveDate {}

/// Hyperliquid asset_ctxs source — one partition = one calendar day.
pub struct AssetCtxsSource {
    fetcher: AssetCtxs,
}

impl AssetCtxsSource {
    pub async fn new(config: &Config) -> AnySignalResult<Self> {
        Ok(Self { fetcher: AssetCtxs::new(config).await? })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for AssetCtxsSource {
    type Key = NaiveDate;

    async fn partition_exists(db: &QuestDbClient, key: &NaiveDate) -> AnySignalResult<bool> {
        let next_day = key.succ_opt().unwrap_or(*key);
        let sql = format!(
            "SELECT count() FROM market_data \
             WHERE source = 'HYPERLIQUID_S3' \
             AND timestamp >= '{}T00:00:00Z' \
             AND timestamp < '{}T00:00:00Z'",
            key.format("%Y-%m-%d"),
            next_day.format("%Y-%m-%d"),
        );
        Ok(db.count(&sql).await? > 0)
    }

    async fn ingest_partition(&self, db: &QuestDbClient, key: &NaiveDate) -> AnySignalResult<u64> {
        let csv_text = self.fetcher.fetch_and_decompress(*key).await?;
        let rows = AssetCtxs::parse_csv(&csv_text)?;
        let n = rows.len() as u64;
        db.with_sender(|s| insert_asset_ctxs(s, &rows))?;
        Ok(n)
    }
}
