use super::prelude::*;
use crate::adapter::{AdapterError, DataSource, DataSourceType};
use crate::adapter::hyperliquid_s3::node_fills_by_block::{FillEvent, ParsedFill};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::types::RequestPayer;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::AggregatedBytes;
use std::io::Read;

/// S3 bucket for Hyperliquid node data (same bucket as `node_fills_by_block`).
const BUCKET: &str = "hl-mainnet-node-data";
/// This bucket lives in ap-northeast-1 regardless of the app's default AWS region.
const BUCKET_REGION: &str = "ap-northeast-1";

// ---------------------------------------------------------------------------
// Wire format
//
// S3 key: `node_fills/hourly/{YYYYMMDD}/{H}.lz4`
//   where {H} is the unpadded hour (0–23).
//
// Format: NDJSON — one line per fill.
// Each line is a 2-element JSON array: `["<wallet>", fill_object]`.
//
// This is the older archive format covering 2025-05-25T14:00 – 2025-07-27T08:00.
// The fill_object fields are identical to those in `node_fills_by_block`.
//
// Example line:
//   ["0xabc...",{"coin":"BTC","px":"50000.0","sz":"0.001","side":"B",
//     "time":1234567890000,"startPosition":"0.0","dir":"Open Long",
//     "closedPnl":"0.0","hash":"0x...","oid":123,"crossed":true,
//     "fee":"0.05","tid":456,"feeToken":"USDC"}]
// ---------------------------------------------------------------------------

pub struct NodeFills {
    client: Client,
}

impl NodeFills {
    pub async fn new() -> AnySignalResult<Self> {
        let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        // This bucket is always in ap-northeast-1 regardless of any default region.
        let s3_config = aws_sdk_s3::config::Builder::from(&aws_config)
            .region(Region::new(BUCKET_REGION))
            .build();
        Ok(Self {
            client: Client::from_conf(s3_config),
        })
    }

    /// Fetch the LZ4-compressed NDJSON for one calendar hour.
    ///
    /// S3 key: `node_fills/hourly/{YYYYMMDD}/{H}.lz4`
    /// where `{H}` is the unpadded hour (0–23).
    pub async fn fetch_hour(&self, date: chrono::NaiveDate, hour: u8) -> AnySignalResult<Vec<u8>> {
        let key = format!(
            "node_fills/hourly/{}/{}.lz4",
            date.format("%Y%m%d"),
            hour
        );
        let resp = self
            .client
            .get_object()
            .bucket(BUCKET)
            .key(&key)
            .request_payer(RequestPayer::Requester)
            .send()
            .await
            .map_err(|e| classify_s3_error(&e, &key))?;

        let data: AggregatedBytes = resp.body.collect().await.map_err(|e| {
            AnySignalError::Adapter(AdapterError::FetchError(format!(
                "Failed to collect S3 body: {e}"
            )))
        })?;

        Ok(data.into_bytes().to_vec())
    }

    /// Decompress an LZ4-framed byte slice and return UTF-8 text.
    pub fn decompress_lz4(compressed: Vec<u8>) -> AnySignalResult<String> {
        let mut decoder = lz4::Decoder::new(compressed.as_slice())
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))?;
        let mut text = String::new();
        decoder
            .read_to_string(&mut text)
            .map_err(|_| AnySignalError::Adapter(AdapterError::Data))?;
        Ok(text)
    }

    /// Parse NDJSON text: each non-empty line is one `["wallet", fill_object]` entry.
    ///
    /// The fill_object fields are identical to [`FillEvent`] in `node_fills_by_block`.
    /// Unknown fields (e.g. `hash`, `oid`, `fee`, `tid`, `feeToken`) are ignored.
    pub fn parse_ndjson(text: &str) -> AnySignalResult<Vec<ParsedFill>> {
        let mut fills = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let (wallet, event): (String, FillEvent) = serde_json::from_str(line)
                .map_err(|e| AnySignalError::Adapter(AdapterError::FetchError(e.to_string())))?;
            let side = if event.side == "B" {
                "buy".to_string()
            } else {
                "sell".to_string()
            };
            fills.push(ParsedFill {
                wallet,
                coin: event.coin,
                time_ms: event.time,
                side,
                category: event.dir,
                is_taker: event.crossed,
                price: event.px.parse().unwrap_or(0.0),
                quantity: event.sz.parse().unwrap_or(0.0),
                position_before: event.start_position.parse().unwrap_or(0.0),
                realized_pnl: event.closed_pnl.parse().unwrap_or(0.0),
            });
        }
        Ok(fills)
    }

    /// Fetch, decompress, and parse one calendar hour.
    pub async fn fetch_and_parse(
        &self,
        date: chrono::NaiveDate,
        hour: u8,
    ) -> AnySignalResult<Vec<ParsedFill>> {
        let compressed = self.fetch_hour(date, hour).await?;
        let text = Self::decompress_lz4(compressed)?;
        Self::parse_ndjson(&text)
    }
}

#[async_trait::async_trait]
impl DataSource for NodeFills {
    type DataType = Vec<ParsedFill>;

    fn id() -> String {
        "hyperliquid_node_fills".to_string()
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

    // Two fills from the same trade (matching buy/sell), one Buy non-taker and
    // one Sell taker — the format used by the older node_fills archive.
    const NDJSON_FIXTURE: &str = concat!(
        r#"["0xdcac85ecae7148886029c20e661d848a4de99ce2",{"coin":"PURR/USDC","px":"0.19247","sz":"35.0","side":"B","time":1753574400031,"startPosition":"11544577.0668599997","dir":"Buy","closedPnl":"0.0","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","oid":121500417607,"crossed":false,"fee":"-0.00006736","tid":944113087409712,"feeToken":"USDC"}]"#,
        "\n",
        r#"["0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",{"coin":"PURR/USDC","px":"0.19247","sz":"35.0","side":"A","time":1753574400031,"startPosition":"35.6047","dir":"Sell","closedPnl":"-0.00034999","hash":"0x0000000000000000000000000000000000000000000000000000000000000000","oid":121500633108,"crossed":true,"fee":"0.00471551","tid":944113087409712,"feeToken":"USDC"}]"#,
        "\n",
    );

    #[test]
    fn parse_ndjson_basic() {
        let fills = NodeFills::parse_ndjson(NDJSON_FIXTURE).unwrap();
        assert_eq!(fills.len(), 2);

        let buy = &fills[0];
        assert_eq!(buy.wallet, "0xdcac85ecae7148886029c20e661d848a4de99ce2");
        assert_eq!(buy.coin, "PURR/USDC");
        assert_eq!(buy.time_ms, 1753574400031);
        assert_eq!(buy.side, "buy");
        assert_eq!(buy.category, "Buy");
        assert!(!buy.is_taker);
        assert!((buy.price - 0.19247).abs() < 1e-9);
        assert!((buy.quantity - 35.0).abs() < f64::EPSILON);
        assert!((buy.realized_pnl - 0.0).abs() < f64::EPSILON);

        let sell = &fills[1];
        assert_eq!(sell.side, "sell");
        assert_eq!(sell.category, "Sell");
        assert!(sell.is_taker);
        assert!((sell.realized_pnl - (-0.00034999)).abs() < 1e-9);
    }

    #[test]
    fn parse_ndjson_empty_lines_ignored() {
        let text = "\n\n".to_string() + NDJSON_FIXTURE + "\n";
        assert_eq!(NodeFills::parse_ndjson(&text).unwrap().len(), 2);
    }

    #[test]
    fn parse_ndjson_invalid_json_returns_error() {
        assert!(NodeFills::parse_ndjson("not json").is_err());
    }

    #[test]
    fn s3_key_format() {
        let date = chrono::NaiveDate::from_ymd_opt(2025, 5, 25).unwrap();
        assert_eq!(
            format!(
                "node_fills/hourly/{}/{}.lz4",
                date.format("%Y%m%d"),
                14u8
            ),
            "node_fills/hourly/20250525/14.lz4"
        );
    }

    #[tokio::test]
    #[ignore = "requires real AWS credentials and outbound network access"]
    async fn integration_fetch_20250727_h0() {
        dotenvy::dotenv().ok();
        let fetcher = NodeFills::new()
            .await
            .expect("S3 client init failed");
        let date = chrono::NaiveDate::from_ymd_opt(2025, 7, 27).unwrap();
        let fills = fetcher
            .fetch_and_parse(date, 0)
            .await
            .expect("fetch failed");
        assert!(!fills.is_empty(), "expected fills for 2025-07-27 hour 0");
        eprintln!(
            "OK: {} fills on 2025-07-27 h0, first={:?}",
            fills.len(),
            fills.first()
        );
    }
}
