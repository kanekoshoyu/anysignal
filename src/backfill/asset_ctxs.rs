use super::{PartitionKey, PartitionedSource, PartitionStats};
use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxs;
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
    pub async fn new() -> AnySignalResult<Self> {
        Ok(Self {
            fetcher: AssetCtxs::new().await?,
        })
    }
}

#[async_trait::async_trait]
impl PartitionedSource for AssetCtxsSource {
    type Key = NaiveDate;

    async fn partition_exists(db: &QuestDbClient, key: &NaiveDate) -> AnySignalResult<bool> {
        let next_day = key.succ_opt().unwrap_or(*key);
        let table = db.table_name("market_data");
        let sql = format!(
            "SELECT count() FROM {table} \
             WHERE source = 'HYPERLIQUID_S3' \
             AND ts >= '{}T00:00:00Z' \
             AND ts < '{}T00:00:00Z'",
            key.format("%Y-%m-%d"),
            next_day.format("%Y-%m-%d"),
        );
        Ok(db.count(&sql).await? > 0)
    }

    async fn ingest_partition(
        &self,
        db: &QuestDbClient,
        key: &NaiveDate,
    ) -> AnySignalResult<PartitionStats> {
        let t_fetch = std::time::Instant::now();
        let csv_text = self.fetcher.fetch_and_decompress(*key).await?;
        let rows = AssetCtxs::parse_csv(&csv_text)?;
        let fetch_ms = t_fetch.elapsed().as_millis();

        let t_insert = std::time::Instant::now();
        let rows_n = rows.len() as u64;
        let table = db.table_name("market_data");
        db.with_sender(|s| insert_asset_ctxs(s, &table, &rows))?;
        let insert_ms = t_insert.elapsed().as_millis();

        Ok(PartitionStats { rows: rows_n, fetch_ms, insert_ms })
    }
}
