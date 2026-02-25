use super::prelude::*;
use crate::adapter::{AdapterError, DataSource, DataSourceType};
use aws_sdk_s3::config::Region;
use aws_sdk_s3::types::RequestPayer;
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::AggregatedBytes;
use serde::Deserialize;
use std::io::Read;

/// S3 bucket for Hyperliquid node data (distinct from `hyperliquid-archive`).
const BUCKET: &str = "hl-mainnet-node-data";
/// This bucket lives in ap-northeast-1 regardless of the app's default AWS region.
const BUCKET_REGION: &str = "ap-northeast-1";

// ---------------------------------------------------------------------------
// Wire types
//
// S3 key: `node_fills_by_block/{YYYYMMDD}.lz4`
// Format: NDJSON — one line per block, each line is a `BlockFills` object.
//
// Each line:
//   {"events":[["<wallet>",{"coin":"...","px":"...","sz":"...","side":"B",
//               "time":<ms>,"startPosition":"...","dir":"Buy","closedPnl":"...",
//               "hash":"...","oid":<n>,"crossed":true,"fee":"...","tid":<n>,
//               "feeToken":"..."}], ...]}
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct FillEvent {
    pub coin: String,
    /// Price as a decimal string (e.g. `"115865.0"`).
    pub px: String,
    /// Quantity / size as a decimal string.
    pub sz: String,
    /// Trade direction: `"Buy"` or `"Sell"`.
    pub dir: String,
    /// Exchange timestamp in milliseconds.
    pub time: i64,
    /// Position size before this fill (decimal string).
    #[serde(rename = "startPosition")]
    pub start_position: String,
    /// Realised PnL for closing fills (decimal string, `"0.0"` for openers).
    #[serde(rename = "closedPnl")]
    pub closed_pnl: String,
    /// `true` when this fill is the taker (crossed the book).
    pub crossed: bool,
}

/// One block's worth of fill events.
#[derive(Debug, Clone, Deserialize)]
pub struct BlockFills {
    /// Each element is `(wallet_address, fill)`.
    pub events: Vec<(String, FillEvent)>,
}

// ---------------------------------------------------------------------------
// Flattened row ready for DB insertion
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct ParsedFill {
    pub wallet: String,
    pub coin: String,
    /// Exchange timestamp in milliseconds.
    pub time_ms: i64,
    pub trade_direction: String,
    pub is_taker: bool,
    pub price: f64,
    pub quantity: f64,
    pub position_before: f64,
    pub realized_pnl: f64,
}

// ---------------------------------------------------------------------------
// S3 fetcher
// ---------------------------------------------------------------------------

pub struct NodeFillsByBlock {
    client: Client,
}

impl NodeFillsByBlock {
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
    /// S3 key: `node_fills_by_block/hourly/{YYYYMMDD}/{H}.lz4`
    /// where `{H}` is the unpadded hour (0–23).
    pub async fn fetch_hour(&self, date: chrono::NaiveDate, hour: u8) -> AnySignalResult<Vec<u8>> {
        let key = format!(
            "node_fills_by_block/hourly/{}/{}.lz4",
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

    /// Parse NDJSON text: each non-empty line is one [`BlockFills`] object.
    ///
    /// Unknown fields (e.g. `hash`, `oid`, `fee`, `tid`, `feeToken`, `side`)
    /// are ignored — only the columns stored in `hyperliquid_fill` are kept.
    pub fn parse_ndjson(text: &str) -> AnySignalResult<Vec<ParsedFill>> {
        let mut fills = Vec::new();
        for line in text.lines().filter(|l| !l.trim().is_empty()) {
            let block: BlockFills = serde_json::from_str(line)
                .map_err(|e| AnySignalError::Adapter(AdapterError::FetchError(e.to_string())))?;
            for (wallet, event) in block.events {
                fills.push(ParsedFill {
                    wallet,
                    coin: event.coin,
                    time_ms: event.time,
                    trade_direction: event.dir,
                    is_taker: event.crossed,
                    price: event.px.parse().unwrap_or(0.0),
                    quantity: event.sz.parse().unwrap_or(0.0),
                    position_before: event.start_position.parse().unwrap_or(0.0),
                    realized_pnl: event.closed_pnl.parse().unwrap_or(0.0),
                });
            }
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
impl DataSource for NodeFillsByBlock {
    type DataType = Vec<ParsedFill>;

    fn id() -> String {
        "hyperliquid_node_fills_by_block".to_string()
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

    // Two events from the same block (same hash/tid), one Buy taker and one Sell maker.
    const NDJSON_FIXTURE: &str = concat!(
        r#"{"events":[["0x2e434472e65249f6b8eb3bb5d240f0682c1c7a9f",{"coin":"@142","px":"115865.0","sz":"0.0001","side":"B","time":1754017199857,"startPosition":"0.0906573601","dir":"Buy","closedPnl":"0.0","hash":"0xb2d9","oid":124639075240,"crossed":true,"fee":"0.0000000594","tid":343451763467020,"feeToken":"UBTC"}],["0x3c516f085e94177eaf0bdfb5b5eb1ab71052d064",{"coin":"@142","px":"115865.0","sz":"0.0001","side":"A","time":1754017199857,"startPosition":"0.0161384162","dir":"Sell","closedPnl":"-0.00701348","hash":"0xb2d9","oid":124638539336,"crossed":false,"fee":"0.00278076","tid":343451763467020,"feeToken":"USDC"}]]}"#,
        "\n",
    );

    #[test]
    fn parse_ndjson_basic() {
        let fills = NodeFillsByBlock::parse_ndjson(NDJSON_FIXTURE).unwrap();
        assert_eq!(fills.len(), 2);

        let buy = &fills[0];
        assert_eq!(buy.wallet, "0x2e434472e65249f6b8eb3bb5d240f0682c1c7a9f");
        assert_eq!(buy.coin, "@142");
        assert_eq!(buy.time_ms, 1754017199857);
        assert_eq!(buy.trade_direction, "Buy");
        assert!(buy.is_taker);
        assert!((buy.price - 115865.0).abs() < f64::EPSILON);
        assert!((buy.quantity - 0.0001).abs() < 1e-9);
        assert!((buy.realized_pnl - 0.0).abs() < f64::EPSILON);

        let sell = &fills[1];
        assert_eq!(sell.trade_direction, "Sell");
        assert!(!sell.is_taker);
        assert!((sell.realized_pnl - (-0.00701348)).abs() < 1e-9);
    }

    #[test]
    fn parse_ndjson_empty_lines_ignored() {
        let text = "\n\n".to_string() + NDJSON_FIXTURE + "\n";
        assert_eq!(NodeFillsByBlock::parse_ndjson(&text).unwrap().len(), 2);
    }

    #[test]
    fn parse_ndjson_invalid_json_returns_error() {
        assert!(NodeFillsByBlock::parse_ndjson("not json").is_err());
    }

    #[test]
    fn s3_key_format() {
        let date = chrono::NaiveDate::from_ymd_opt(2025, 8, 1).unwrap();
        assert_eq!(
            format!(
                "node_fills_by_block/hourly/{}/{}.lz4",
                date.format("%Y%m%d"),
                3u8
            ),
            "node_fills_by_block/hourly/20250801/3.lz4"
        );
    }

    #[tokio::test]
    #[ignore = "requires real AWS credentials and outbound network access"]
    async fn integration_fetch_20250801_h3() {
        dotenvy::dotenv().ok();
        let fetcher = NodeFillsByBlock::new()
            .await
            .expect("S3 client init failed");
        let date = chrono::NaiveDate::from_ymd_opt(2025, 8, 1).unwrap();
        let fills = fetcher
            .fetch_and_parse(date, 3)
            .await
            .expect("fetch failed");
        assert!(!fills.is_empty(), "expected fills for 2025-08-01 hour 3");
        eprintln!(
            "OK: {} fills on 2025-08-01 h3, first={:?}",
            fills.len(),
            fills.first()
        );
    }
}
