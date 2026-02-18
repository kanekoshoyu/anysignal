use super::prelude::*;
use crate::adapter::{AdapterError, AdapterResult, DataSource, DataSourceType, HistoricDataSource};
use serde::Deserialize;
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::AggregatedBytes;
use std::env;

/// One row from an `asset_ctxs/YYYY-MM-DD.csv.lz4` file.
///
/// Column names mirror Hyperliquid's camelCase JSON field names exactly so
/// that `csv` + `serde` can deserialise the header row without manual
/// mapping.  All price / rate fields that may be absent (e.g. `midPx` when
/// the market has no recent trades) are `Option<f64>`.
#[derive(Debug, Clone, Deserialize)]
pub struct AssetCtxRow {
    /// Unix timestamp in **milliseconds**.
    pub time: i64,
    pub coin: String,
    /// Hourly funding rate.
    pub funding: f64,
    #[serde(rename = "openInterest")]
    pub open_interest: f64,
    #[serde(rename = "prevDayPx")]
    pub prev_day_px: f64,
    /// Daily notional volume in USD.
    #[serde(rename = "dayNtlVlm")]
    pub day_ntl_vlm: f64,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub premium: Option<f64>,
    #[serde(rename = "oraclePx")]
    pub oracle_px: f64,
    #[serde(rename = "markPx")]
    pub mark_px: f64,
    #[serde(rename = "midPx", default, deserialize_with = "de_opt_f64")]
    pub mid_px: Option<f64>,
}

/// Deserialise an optional float from a CSV field that may be an empty
/// string or the literal string `"null"`.
fn de_opt_f64<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    let s: &str = Deserialize::deserialize(d)?;
    if s.is_empty() || s.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    s.parse::<f64>().map(Some).map_err(serde::de::Error::custom)
}

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

    /// Parse the decompressed CSV text into a list of [`AssetCtxRow`]s.
    pub fn parse_csv(csv_text: &str) -> AnySignalResult<Vec<AssetCtxRow>> {
        let mut reader = csv::Reader::from_reader(csv_text.as_bytes());
        let mut rows = Vec::new();
        for result in reader.deserialize::<AssetCtxRow>() {
            let row = result.map_err(|e| {
                AnySignalError::Adapter(AdapterError::FetchError(e.to_string()))
            })?;
            rows.push(row);
        }
        Ok(rows)
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