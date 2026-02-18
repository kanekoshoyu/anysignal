use super::prelude::*;
use crate::adapter::{DataSource, DataSourceType, HistoricDataSource, AdapterResult, AdapterError};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::AggregatedBytes;
use std::env;

pub struct AssetCtxs {
    pub client: Client,
    pub bucket: String,
}

impl AssetCtxs {
    pub async fn new() -> AnySignalResult<Self> {
        dotenvy::dotenv().ok();
        
        let bucket = env::var("HYPERLIQUID_S3_BUCKET").unwrap_or_else(|_| "hyperliquid-archive".to_string());

        let region_provider = RegionProviderChain::default_provider()
            .or_else("ap-northeast-1");

        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

        let client = Client::new(&config);

        Ok(Self { client, bucket })
    }

    pub async fn fetch_asset_ctxs(&self, date: &str) -> AnySignalResult<Vec<u8>> {
        let key = format!("asset_ctxs/{}.csv.lz4", date);

        let resp = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
            .map_err(|e| AnySignalError::Adapter(AdapterError::FetchError(format!("Failed to fetch from S3: {}", e))))?;

        let data: AggregatedBytes = resp.body.collect().await
            .map_err(|e| AnySignalError::Adapter(AdapterError::FetchError(format!("Failed to collect response body: {}", e))))?;
        
        Ok(data.into_bytes().to_vec())
    }

    pub async fn decompress_lz4(&self, compressed_data: Vec<u8>) -> AnySignalResult<String> {
        let decompressed = lz4::block::decompress(&compressed_data, None)
            .map_err(|e| AnySignalError::Adapter(AdapterError::Data))?;
        
        String::from_utf8(decompressed)
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))
    }

    pub async fn fetch_and_decompress(&self, date: &str) -> AnySignalResult<String> {
        let compressed_data = self.fetch_asset_ctxs(date).await?;
        self.decompress_lz4(compressed_data).await
    }
}

#[async_trait::async_trait]
impl DataSource for AssetCtxs {
    type DataType = String;

    fn id() -> String {
        "hyperliquid_s3_asset_ctxs".to_string()
    }

    fn data_source_type() -> DataSourceType {
        DataSourceType::Historic
    }
}

#[async_trait::async_trait]
impl HistoricDataSource for AssetCtxs {
    async fn fetch() -> AdapterResult<Self::DataType> {
        let fetcher = Self::new().await
            .map_err(|e| AdapterError::ConfigurationError(e.to_string()))?;
        
        let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
        fetcher.fetch_and_decompress(&today).await
            .map_err(|e| AdapterError::FetchError(e.to_string()))
    }
}