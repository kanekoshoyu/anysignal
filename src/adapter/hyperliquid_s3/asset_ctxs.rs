use super::prelude::*;
use crate::adapter::{AdapterError, AdapterResult, DataSource, DataSourceType, HistoricDataSource};
use crate::config::Config;
use serde::Deserialize;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::AggregatedBytes;

/// One row from an `asset_ctxs/YYYYMMDD.csv.lz4` file.
///
/// Column names match the snake_case headers in the archive CSV exactly.
/// Optional price fields may be absent (empty string) for illiquid markets.
#[derive(Debug, Clone, Deserialize)]
pub struct AssetCtxRow {
    /// ISO 8601 UTC timestamp (e.g. `"2025-01-01T00:00:00Z"`).
    pub time: chrono::DateTime<chrono::Utc>,
    pub coin: String,
    /// Hourly funding rate.
    pub funding: f64,
    pub open_interest: f64,
    pub prev_day_px: f64,
    /// Daily notional volume in USD.
    pub day_ntl_vlm: f64,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub premium: Option<f64>,
    pub oracle_px: f64,
    pub mark_px: f64,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub mid_px: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub impact_bid_px: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    pub impact_ask_px: Option<f64>,
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

    pub async fn fetch_asset_ctxs(&self, date: chrono::NaiveDate) -> AnySignalResult<Vec<u8>> {
        let key = format!("asset_ctxs/{}.csv.lz4", date.format("%Y%m%d"));

        let resp = self.client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .request_payer(aws_sdk_s3::types::RequestPayer::Requester)
            .send()
            .await
            .map_err(|e| AnySignalError::Adapter(AdapterError::FetchError(format!("Failed to fetch from S3: {e}"))))?;

        let data: AggregatedBytes = resp.body.collect().await
            .map_err(|e| AnySignalError::Adapter(AdapterError::FetchError(format!("Failed to collect response body: {}", e))))?;
        
        Ok(data.into_bytes().to_vec())
    }

    pub async fn decompress_lz4(&self, compressed_data: Vec<u8>) -> AnySignalResult<String> {
        let mut decoder = lz4::Decoder::new(compressed_data.as_slice())
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))?;
        let mut decompressed = Vec::new();
        std::io::Read::read_to_end(&mut decoder, &mut decompressed)
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))?;
        String::from_utf8(decompressed)
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))
    }

    pub async fn fetch_and_decompress(&self, date: chrono::NaiveDate) -> AnySignalResult<String> {
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
        let config = Config::from_env();
        let fetcher = Self::new(&config).await
            .map_err(|e| AdapterError::ConfigurationError(e.to_string()))?;

        let today = chrono::Utc::now().date_naive();
        fetcher.fetch_and_decompress(today).await
            .map_err(|e| AdapterError::FetchError(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::AdapterError;
    use crate::error::AnySignalError;
    use aws_sdk_s3::config::{BehaviorVersion, Builder as S3ConfigBuilder};

    /// Construct a dummy `AssetCtxs` that has no real credentials.
    /// Only safe to use with methods that do not touch `self.client`.
    fn dummy_fetcher() -> AssetCtxs {
        let s3_config = S3ConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-east-1"))
            .build();
        let client = Client::from_conf(s3_config);
        AssetCtxs {
            client,
            bucket: "test-bucket".to_string(),
        }
    }

    // -----------------------------------------------------------------------
    // parse_csv
    // -----------------------------------------------------------------------

    #[test]
    fn parse_csv_valid_row() {
        let csv = "\
time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px
2025-01-01T00:00:00Z,BTC,0.0001,100.0,42000.0,5000000.0,0.0001,42100.0,42050.0,42025.0,42020.0,42030.0
";
        let rows = AssetCtxs::parse_csv(csv).expect("should parse");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].coin, "BTC");
        assert_eq!(rows[0].time.timestamp(), 1735689600);
        assert!((rows[0].funding - 0.0001).abs() < f64::EPSILON);
    }

    #[test]
    fn parse_csv_null_optional_fields() {
        // `premium`, `mid_px`, `impact_bid_px`, `impact_ask_px` may be empty.
        let csv = "\
time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px
2025-01-01T00:00:00Z,ETH,0.00005,50.0,2200.0,1000000.0,,2210.0,2205.0,,,
2025-01-01T00:00:00Z,SOL,0.0002,10.0,100.0,500000.0,null,101.0,100.5,null,null,null
";
        let rows = AssetCtxs::parse_csv(csv).expect("should parse");
        assert_eq!(rows.len(), 2);
        assert!(rows[0].premium.is_none());
        assert!(rows[0].mid_px.is_none());
        assert!(rows[0].impact_bid_px.is_none());
        assert!(rows[1].premium.is_none());
        assert!(rows[1].mid_px.is_none());
    }

    #[test]
    fn parse_csv_missing_required_column() {
        // A CSV that is missing most required columns should produce a FetchError.
        let csv = "time,coin,funding\n2025-01-01T00:00:00Z,BTC,0.0001\n";
        let result = AssetCtxs::parse_csv(csv);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AnySignalError::Adapter(AdapterError::FetchError(_))
        ));
    }

    #[test]
    fn parse_csv_empty_body() {
        // An S3 object with only a header and no rows should return an empty vec.
        let csv = "time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px\n";
        let rows = AssetCtxs::parse_csv(csv).expect("should parse empty body");
        assert!(rows.is_empty());
    }

    // -----------------------------------------------------------------------
    // decompress_lz4
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn decompress_lz4_invalid_data_returns_data_error() {
        let fetcher = dummy_fetcher();
        // LZ4 frame magic (0x184D2204 LE) followed by a garbage frame descriptor
        // — valid magic ensures the decoder tries to parse the frame, then errors.
        let bad = vec![0x04, 0x22, 0x4D, 0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
        let result = fetcher.decompress_lz4(bad).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            AnySignalError::Adapter(AdapterError::Data)
        ));
    }

    #[tokio::test]
    async fn decompress_lz4_valid_roundtrip() {
        let original = b"time,coin,funding\n1704067200000,BTC,0.0001\n";
        let mut buf = Vec::new();
        let mut encoder = lz4::EncoderBuilder::new()
            .build(&mut buf)
            .expect("lz4 encoder should build");
        std::io::Write::write_all(&mut encoder, original).expect("write should succeed");
        let (_, result) = encoder.finish();
        result.expect("lz4 encoding should succeed");

        let fetcher = dummy_fetcher();
        let decompressed = fetcher
            .decompress_lz4(buf)
            .await
            .expect("decompression should succeed");

        assert_eq!(decompressed.as_bytes(), original);
    }

    // -----------------------------------------------------------------------
    // Error display format
    // -----------------------------------------------------------------------

    /// Verify that the error string produced by `AnySignalError::Adapter`
    /// matches the pattern seen in production logs:
    ///   "2025-01-01: adapter error: Fetch: Failed to fetch from S3: service error"
    #[test]
    fn fetch_error_display_matches_log_pattern() {
        let err = AnySignalError::Adapter(AdapterError::FetchError(
            "Failed to fetch from S3: service error".to_string(),
        ));
        let log_line = format!("{}: {}", "2025-01-01", err);
        assert_eq!(
            log_line,
            "2025-01-01: adapter error: Fetch: Failed to fetch from S3: service error"
        );
    }

    #[test]
    fn data_error_display() {
        let err = AnySignalError::Adapter(AdapterError::Data);
        assert_eq!(err.to_string(), "adapter error: Data");
    }

    // -----------------------------------------------------------------------
    // Screening / smoke test — requires real AWS credentials + network
    // Run with: cargo test -- --ignored
    // -----------------------------------------------------------------------

    /// Fetches `asset_ctxs/2025-01-01.csv.lz4` from the public Hyperliquid S3
    /// archive, decompresses it, parses the CSV, and validates structural
    /// invariants on every row.
    #[tokio::test]
    #[ignore = "requires real AWS credentials and outbound network access"]
    async fn screening_fetch_2025_01_01() {
        dotenvy::dotenv().ok();

        let date_2025_01_01 = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
            .expect("valid date")
            .with_timezone(&chrono::Utc);
        let one_day = chrono::Duration::days(1);

        let config = Config::from_env();
        let fetcher = AssetCtxs::new(&config)
            .await
            .expect("should initialise S3 client");

        let date = chrono::NaiveDate::from_ymd_opt(2025, 1, 1).expect("valid date");
        let csv_text = fetcher
            .fetch_and_decompress(date)
            .await
            .expect("should fetch and decompress asset_ctxs/20250101.csv.lz4");

        let rows = AssetCtxs::parse_csv(&csv_text).expect("should parse CSV");

        assert!(!rows.is_empty(), "expected at least one row for 2025-01-01");

        let mut coins: Vec<&str> = rows.iter().map(|r| r.coin.as_str()).collect();
        coins.sort_unstable();
        coins.dedup();

        eprintln!(
            "screening_fetch_2025_01_01: {} rows, {} unique coins",
            rows.len(),
            coins.len()
        );
        eprintln!("first row : {:?}", rows.first());
        eprintln!("last  row : {:?}", rows.last());

        for (i, row) in rows.iter().enumerate() {
            // coin must be a non-empty ASCII ticker
            assert!(!row.coin.is_empty(), "row {i}: coin is empty");
            assert!(
                row.coin.is_ascii(),
                "row {i}: coin contains non-ASCII: {}",
                row.coin
            );

            // timestamp must fall within ±1 day of 2025-01-01
            assert!(
                row.time >= date_2025_01_01 - one_day
                    && row.time <= date_2025_01_01 + one_day * 2,
                "row {i}: time {} is not near 2025-01-01",
                row.time
            );

            // prices must be strictly positive
            assert!(row.prev_day_px > 0.0, "row {i}: prev_day_px <= 0: {:?}", row);
            assert!(row.oracle_px > 0.0,   "row {i}: oracle_px  <= 0: {:?}", row);
            assert!(row.mark_px > 0.0,     "row {i}: mark_px    <= 0: {:?}", row);

            // optional prices, when present, must be non-negative
            // (illiquid coins with zero open interest report 0.0)
            if let Some(mid) = row.mid_px {
                assert!(mid >= 0.0, "row {i}: mid_px < 0: {:?}", row);
            }
            if let Some(bid) = row.impact_bid_px {
                assert!(bid >= 0.0, "row {i}: impact_bid_px < 0: {:?}", row);
            }
            if let Some(ask) = row.impact_ask_px {
                assert!(ask >= 0.0, "row {i}: impact_ask_px < 0: {:?}", row);
            }

            // volume and open interest must be non-negative
            assert!(row.day_ntl_vlm >= 0.0, "row {i}: day_ntl_vlm < 0: {:?}", row);
            assert!(row.open_interest >= 0.0, "row {i}: open_interest < 0: {:?}", row);

            // funding rate should be a small number (roughly ±5 % per hour is extreme)
            assert!(
                row.funding.abs() < 0.05,
                "row {i}: funding rate {} looks implausible: {:?}",
                row.funding,
                row
            );
        }
    }
}