use super::prelude::*;
use crate::adapter::{AdapterError, DataSource, DataSourceType};
use crate::config::Config;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::types::RequestPayer;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::AggregatedBytes;
use serde::Deserialize;
use std::io::Read;

// ---------------------------------------------------------------------------
// Wire types
//
// Each line in `market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4`:
//   {"time":"2023-04-15T00:00:07.705751164","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"BTC","time":1681516807400,"levels":[[...],[...]]}}}
// ---------------------------------------------------------------------------

/// One price level within an L2 orderbook snapshot.
#[derive(Debug, Clone, Deserialize)]
pub struct L2Level {
    /// Price as a decimal string (e.g. `"30439.0"`).
    pub px: String,
    /// Size / quantity as a decimal string (e.g. `"0.08236"`).
    pub sz: String,
    /// Number of resting orders at this level.
    pub n: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct L2BookData {
    pub coin: String,
    /// Exchange timestamp in milliseconds.
    pub time: i64,
    /// `levels[0]` = bids (descending), `levels[1]` = asks (ascending).
    pub levels: [Vec<L2Level>; 2],
}

#[derive(Debug, Clone, Deserialize)]
struct L2BookRaw {
    pub data: L2BookData,
}

/// One L2 orderbook snapshot as stored in a Hyperliquid S3 NDJSON file.
#[derive(Debug, Clone, Deserialize)]
pub struct L2Snapshot {
    /// Wall-clock recording time (ISO 8601 nanosecond string).
    pub time: String,
    raw: L2BookRaw,
}

impl L2Snapshot {
    /// Coin ticker (e.g. `"BTC"`).
    pub fn coin(&self) -> &str {
        &self.raw.data.coin
    }

    /// Exchange timestamp in milliseconds.
    pub fn time_ms(&self) -> i64 {
        self.raw.data.time
    }

    /// Bid/ask levels: index 0 = bids, index 1 = asks.
    pub fn levels(&self) -> &[Vec<L2Level>; 2] {
        &self.raw.data.levels
    }
}

// ---------------------------------------------------------------------------
// S3 fetcher
// ---------------------------------------------------------------------------

pub struct MarketData {
    pub client: Client,
    pub bucket: String,
}

impl MarketData {
    pub async fn new(config: &Config) -> AnySignalResult<Self> {
        let bucket = config.s3_bucket.clone();
        let region = Region::new(config.aws_region.clone());

        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let client = Client::new(&aws_config);
        Ok(Self { client, bucket })
    }

    /// Fetch the raw LZ4-compressed bytes for one `(date, hour, coin)` slice.
    ///
    /// S3 key: `market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4`
    pub async fn fetch_l2_book(
        &self,
        date: chrono::NaiveDate,
        hour: u8,
        coin: &str,
    ) -> AnySignalResult<Vec<u8>> {
        let key = format!(
            "market_data/{}/{}/l2Book/{}.lz4",
            date.format("%Y%m%d"),
            hour,
            coin
        );

        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .request_payer(RequestPayer::Requester)
            .send()
            .await
            .map_err(|e| {
                AnySignalError::Adapter(AdapterError::FetchError(format!("S3 error: {e}")))
            })?;

        let data: AggregatedBytes = resp
            .body
            .collect()
            .await
            .map_err(|e| {
                AnySignalError::Adapter(AdapterError::FetchError(format!(
                    "Failed to collect S3 body: {e}"
                )))
            })?;

        Ok(data.into_bytes().to_vec())
    }

    /// Decompress an LZ4-framed byte slice and return the UTF-8 text.
    ///
    /// market_data files use LZ4 **frame** format (magic `0x184D2204`),
    /// distinct from the block format used by older asset_ctxs.
    pub fn decompress_lz4(compressed: Vec<u8>) -> AnySignalResult<String> {
        let mut decoder = lz4::Decoder::new(compressed.as_slice())
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))?;
        let mut text = String::new();
        decoder
            .read_to_string(&mut text)
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))?;
        Ok(text)
    }

    /// Parse newline-delimited JSON text into a list of [`L2Snapshot`]s.
    pub fn parse_ndjson(text: &str) -> AnySignalResult<Vec<L2Snapshot>> {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|line| {
                serde_json::from_str::<L2Snapshot>(line).map_err(|e| {
                    AnySignalError::Adapter(AdapterError::FetchError(e.to_string()))
                })
            })
            .collect()
    }

    /// Fetch, decompress, and parse one `(date, hour, coin)` slice.
    pub async fn fetch_and_parse(
        &self,
        date: chrono::NaiveDate,
        hour: u8,
        coin: &str,
    ) -> AnySignalResult<Vec<L2Snapshot>> {
        let compressed = self.fetch_l2_book(date, hour, coin).await?;
        let text = Self::decompress_lz4(compressed)?;
        Self::parse_ndjson(&text)
    }
}

#[async_trait::async_trait]
impl DataSource for MarketData {
    type DataType = Vec<L2Snapshot>;

    fn id() -> String {
        "hyperliquid_s3_market_data".to_string()
    }

    fn data_source_type() -> DataSourceType {
        DataSourceType::Historic
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use aws_sdk_s3::config::{BehaviorVersion, Builder as S3ConfigBuilder};

    fn dummy_fetcher() -> MarketData {
        let s3_config = S3ConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let client = Client::from_conf(s3_config);
        MarketData { client, bucket: "test-bucket".to_string() }
    }

    const NDJSON_FIXTURE: &str = concat!(
        r#"{"time":"2023-04-15T00:00:07.705751164","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"BTC","time":1681516807400,"levels":[[{"px":"30439.0","sz":"0.08236","n":1},{"px":"30433.0","sz":"0.10161","n":1}],[{"px":"30464.0","sz":"0.19674","n":1}]]}}}"#,
        "\n",
        r#"{"time":"2023-04-15T00:00:07.997632528","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"BTC","time":1681516807476,"levels":[[{"px":"49999.0","sz":"1.0","n":2}],[{"px":"50002.0","sz":"0.4","n":1},{"px":"50010.0","sz":"0.1","n":1}]]}}}"#,
        "\n",
    );

    #[test]
    fn test_parse_ndjson_basic() {
        let snapshots = MarketData::parse_ndjson(NDJSON_FIXTURE).unwrap();
        assert_eq!(snapshots.len(), 2);
        let s = &snapshots[0];
        assert_eq!(s.coin(), "BTC");
        assert_eq!(s.time_ms(), 1681516807400);
        assert_eq!(s.levels()[0].len(), 2);
        assert_eq!(s.levels()[1].len(), 1);
        assert_eq!(s.levels()[0][0].px, "30439.0");
        assert_eq!(s.levels()[0][0].sz, "0.08236");
        assert_eq!(s.levels()[0][0].n, 1);
        assert_eq!(s.levels()[1][0].px, "30464.0");
        let s2 = &snapshots[1];
        assert_eq!(s2.levels()[0].len(), 1);
        assert_eq!(s2.levels()[1].len(), 2);
    }

    #[test]
    fn test_parse_ndjson_empty_lines_ignored() {
        let text = "\n\n".to_string() + NDJSON_FIXTURE + "\n";
        assert_eq!(MarketData::parse_ndjson(&text).unwrap().len(), 2);
    }

    #[test]
    fn test_parse_ndjson_invalid_json_returns_error() {
        let bad = r#"{"time":"bad","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"BTC","time":"not_a_number","levels":[]}}}"#;
        assert!(MarketData::parse_ndjson(bad).is_err());
    }

    #[test]
    fn test_decompress_lz4_roundtrip() {
        let original = NDJSON_FIXTURE.as_bytes();
        let mut compressed: Vec<u8> = Vec::new();
        {
            let mut encoder = lz4::EncoderBuilder::new().build(&mut compressed).unwrap();
            std::io::copy(&mut std::io::Cursor::new(original), &mut encoder).unwrap();
            let (_w, r) = encoder.finish();
            r.unwrap();
        }
        let decompressed = MarketData::decompress_lz4(compressed).unwrap();
        assert_eq!(decompressed, NDJSON_FIXTURE);
    }

    #[test]
    fn test_decompress_lz4_invalid_returns_error() {
        let bad = vec![0x04, 0x22, 0x4D, 0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        assert!(MarketData::decompress_lz4(bad).is_err());
    }

    #[test]
    fn test_s3_key_format() {
        let date = chrono::NaiveDate::from_ymd_opt(2023, 4, 15).unwrap();
        let key = format!("market_data/{}/{}/l2Book/{}.lz4", date.format("%Y%m%d"), 0u8, "BTC");
        assert_eq!(key, "market_data/20230415/0/l2Book/BTC.lz4");
        let key2 = format!("market_data/{}/{}/l2Book/{}.lz4", date.format("%Y%m%d"), 12u8, "ETH");
        assert_eq!(key2, "market_data/20230415/12/l2Book/ETH.lz4");
    }

    #[tokio::test]
    #[ignore = "requires real AWS credentials and outbound network access"]
    async fn integration_fetch_btc_20230415_hour0() {
        dotenvy::dotenv().ok();
        let config = Config::from_env();
        let fetcher = MarketData::new(&config).await.expect("S3 client init failed");
        let date = chrono::NaiveDate::from_ymd_opt(2023, 4, 15).unwrap();
        let snapshots = fetcher.fetch_and_parse(date, 0, "BTC").await.expect("fetch failed");
        assert!(snapshots.len() > 20_000, "expected >20k snapshots, got {}", snapshots.len());
        let bid0 = snapshots[0].levels()[0][0].px.parse::<f64>().unwrap();
        let ask0 = snapshots[0].levels()[1][0].px.parse::<f64>().unwrap();
        assert!(ask0 > bid0, "best ask must be above best bid");
        eprintln!("OK: {} snapshots, BTC best bid={bid0} ask={ask0}", snapshots.len());
    }
}
