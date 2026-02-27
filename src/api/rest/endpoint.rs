use crate::backfill::asset_ctxs::AssetCtxsSource;
use crate::backfill::l2_orderbook::{L2PartitionKey, L2SnapshotSource};
use crate::backfill::node_fills_1m_aggregate::{NodeFills1mAggregateHourKey, NodeFills1mAggregateSource};
use crate::backfill::node_fills_by_block::{NodeFillsHourKey, NodeFillsSource};
use crate::backfill::run_backfill;
use crate::config::Config;
use crate::database::QuestDbClient;
use crate::metadata::cargo_package_version;
use chrono::{NaiveDate, NaiveDateTime, Timelike};
use poem_openapi::{
    param::Query,
    payload::{Json, PlainText},
    ApiResponse, Enum, Object, OpenApi,
};

/// Parse a datetime string in either `YYYY-MM-DDTHH:MM:SS` or `YYYY-MM-DD` format.
/// Date-only strings are treated as midnight (`T00:00:00`).
fn parse_flexible_datetime(s: &str) -> Result<NaiveDateTime, String> {
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(dt);
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Ok(d.and_hms_opt(0, 0, 0).unwrap_or_default());
    }
    Err(format!(
        "Invalid datetime '{s}': expected 'YYYY-MM-DDTHH:MM:SS' or 'YYYY-MM-DD'"
    ))
}

pub struct Endpoint {
    pub config: Config,
}

// ---------------------------------------------------------------------------
// Backfill types
// ---------------------------------------------------------------------------

/// Available historic data sources for a backfill job.
#[derive(Debug, Enum)]
enum BackfillSource {
    /// Hyperliquid daily asset-context snapshots from the public S3 archive.
    ///
    /// Fetches `s3://hyperliquid-archive/asset_ctxs/YYYYMMDD.csv.lz4` for
    /// every calendar day in the requested range.
    /// Only the date part of `from`/`to` is used.
    ///
    /// **Data available from:** `2023-05-20`
    HyperliquidAssetCtxs,

    /// Hyperliquid L2 orderbook snapshots from the public S3 archive.
    ///
    /// Fetches `s3://hyperliquid-archive/market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4`
    /// iterating **hour-by-hour** from `from` to `to`.  Requires `coins`.
    ///
    /// **Data available from:** `2023-04-15`
    HyperliquidL2Orderbook,

    /// Hyperliquid node fills batched by block, from `s3://hl-mainnet-node-data`.
    ///
    /// Fetches `node_fills_by_block/hourly/{YYYYMMDD}/{H}.lz4` for every hour in
    /// the requested range and inserts into `hyperliquid_fill`.
    ///
    /// **Data available from:** `2025-07-27`
    HyperliquidNodeFills,

    /// Hyperliquid node fills aggregated into 1-minute buckets.
    ///
    /// Fetches the same hourly files as `HyperliquidNodeFills` but aggregates
    /// each fill into `(coin, category, buy_side, minute)` buckets before
    /// writing to `hyperliquid_fill_1m_aggregate`.  The minute bucket is
    /// **left-closed**: `12:00:00` covers `[12:00, 12:01)`.
    ///
    /// **Data available from:** `2025-07-27`
    HyperliquidNodeFills1mAggregate,
}

/// Per-run summary returned by `GET /backfill`.
#[derive(Debug, Object)]
struct BackfillResult {
    /// Keys fetched successfully.
    keys_ok: Vec<String>,
    /// Keys that failed, formatted as `"<key>: <error>"`.
    keys_err: Vec<String>,
    /// Keys skipped because data was already present in QuestDB.
    keys_skipped: Vec<String>,
    /// Number of keys fetched successfully.
    total_ok: u64,
    /// Number of keys that produced an error.
    total_err: u64,
    /// Number of keys skipped due to existing data.
    total_skipped: u64,
    /// Total rows inserted into QuestDB.
    rows_inserted: u64,
    /// Wall-clock milliseconds from first key to last flush.
    elapsed_ms: u64,
}

#[derive(ApiResponse)]
enum BackfillApiResponse {
    /// Backfill finished — inspect `dates_err` for any per-period failures.
    #[oai(status = 200)]
    Ok(Json<BackfillResult>),
    /// Bad request — invalid range or missing required params.
    #[oai(status = 400)]
    BadRequest(PlainText<String>),
    /// Server-side error — e.g. AWS credentials not configured.
    #[oai(status = 500)]
    InternalError(PlainText<String>),
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

#[OpenApi]
impl Endpoint {
    /// Check whether the signal indexer server is running.
    #[oai(path = "/", method = "get")]
    async fn root(&self) -> PlainText<String> {
        PlainText("Server is running.".to_string())
    }

    /// Return the semantic version of the signal indexer server.
    #[oai(path = "/version", method = "get")]
    async fn version(&self) -> PlainText<String> {
        PlainText(cargo_package_version())
    }

    /// Backfill historic data for a datetime range.
    ///
    /// `from` and `to` accept either a full datetime (`2024-01-01T00:00:00`) or a
    /// date-only value (`2024-01-01`), which is treated as midnight (`T00:00:00`).
    ///
    /// By default, periods already present in QuestDB are skipped.
    /// Set `force=true` to bypass that check and always fetch+insert.
    ///
    /// | `source`                          | Steps  | Table                            | Available from | Extra params       |
    /// |-----------------------------------|--------|----------------------------------|----------------|--------------------|
    /// | `HyperliquidAssetCtxs`            | daily  | `market_data`                    | 2023-05-20     | —                  |
    /// | `HyperliquidL2Orderbook`          | hourly | `l2_orderbook`                   | 2023-04-15     | `coins` (required) |
    /// | `HyperliquidNodeFills`            | hourly | `hyperliquid_fill`               | 2025-07-27     | —                  |
    /// | `HyperliquidNodeFills1mAggregate` | hourly | `hyperliquid_fill_1m_aggregate`  | 2025-07-27     | —                  |
    ///
    /// **`HyperliquidNodeFills1mAggregate`** aggregates raw fills into
    /// `(coin, category, buy_side, minute)` buckets before writing.
    /// The minute bucket is left-closed: `12:00:00` covers `[12:00, 12:01)`.
    #[oai(path = "/backfill", method = "get")]
    async fn backfill(
        &self,
        /// Start of the range, **inclusive** (`2024-01-01T00:00:00` or `2024-01-01`).
        from: Query<String>,
        /// End of the range, **inclusive** (`2024-01-07T23:00:00` or `2024-01-07`).
        to: Query<String>,
        /// Which historic data source to pull from.
        source: Query<BackfillSource>,
        /// Skip the duplicate check and always fetch+insert.  Defaults to `false`.
        force: Query<Option<bool>>,
        /// Comma-separated coin tickers (required for `HyperliquidL2Orderbook`,
        /// e.g. `BTC,ETH`).
        coins: Query<Option<String>>,
    ) -> BackfillApiResponse {
        let from = match parse_flexible_datetime(&from.0) {
            Ok(dt) => dt,
            Err(e) => return BackfillApiResponse::BadRequest(PlainText(e)),
        };
        let to = match parse_flexible_datetime(&to.0) {
            Ok(dt) => dt,
            Err(e) => return BackfillApiResponse::BadRequest(PlainText(e)),
        };
        let (source, force) = (source.0, force.0.unwrap_or(false));

        if from > to {
            return BackfillApiResponse::BadRequest(PlainText(
                "'from' must be on or before 'to'.".to_string(),
            ));
        }

        let db = match QuestDbClient::new(&self.config) {
            Ok(c) => c,
            Err(e) => {
                return BackfillApiResponse::InternalError(PlainText(format!(
                    "Failed to connect to QuestDB: {e}"
                )))
            }
        };

        match source {
            BackfillSource::HyperliquidAssetCtxs => {
                let source = match AssetCtxsSource::new().await {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {e}"
                        )))
                    }
                };

                // Build day-by-day key iterator.
                let keys = {
                    let mut keys = Vec::new();
                    let mut current: NaiveDate = from.date();
                    let end = to.date();
                    while current <= end {
                        keys.push(current);
                        match current.succ_opt() {
                            Some(next) => current = next,
                            None => break,
                        }
                    }
                    keys
                };

                match run_backfill(&source, &db, keys, force).await {
                    Ok(stats) => BackfillApiResponse::Ok(Json(stats.into())),
                    Err(msg) => BackfillApiResponse::InternalError(PlainText(msg)),
                }
            }

            BackfillSource::HyperliquidL2Orderbook => {
                let coin_list: Vec<String> =
                    match coins.0 {
                        Some(s) if !s.trim().is_empty() => {
                            s.split(',').map(|c| c.trim().to_uppercase()).collect()
                        }
                        _ => return BackfillApiResponse::BadRequest(PlainText(
                            "'coins' is required for HyperliquidL2Orderbook (e.g. coins=BTC,ETH)."
                                .to_string(),
                        )),
                    };

                let source = match L2SnapshotSource::new().await {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {e}"
                        )))
                    }
                };

                // Build hour-by-hour × coin flat key iterator.
                let keys = {
                    let mut keys = Vec::new();
                    let mut current = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
                    while current <= to {
                        for coin in &coin_list {
                            keys.push(L2PartitionKey {
                                hour: current,
                                coin: coin.clone(),
                            });
                        }
                        current += chrono::Duration::hours(1);
                    }
                    keys
                };

                match run_backfill(&source, &db, keys, force).await {
                    Ok(stats) => BackfillApiResponse::Ok(Json(stats.into())),
                    Err(msg) => BackfillApiResponse::InternalError(PlainText(msg)),
                }
            }

            BackfillSource::HyperliquidNodeFills => {
                let source = match NodeFillsSource::new().await {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {e}"
                        )))
                    }
                };

                // Build hour-by-hour key iterator (same pattern as L2, no coin dimension).
                let keys = {
                    let mut keys = Vec::new();
                    let mut current = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
                    while current <= to {
                        keys.push(NodeFillsHourKey { hour: current });
                        current += chrono::Duration::hours(1);
                    }
                    keys
                };

                match run_backfill(&source, &db, keys, force).await {
                    Ok(stats) => BackfillApiResponse::Ok(Json(stats.into())),
                    Err(msg) => BackfillApiResponse::InternalError(PlainText(msg)),
                }
            }

            BackfillSource::HyperliquidNodeFills1mAggregate => {
                let source = match NodeFills1mAggregateSource::new().await {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {e}"
                        )))
                    }
                };

                // Build hour-by-hour key iterator — one partition = one hour of raw fills
                // which expands to up to 60 minute-buckets per (coin, category, buy_side).
                let keys = {
                    let mut keys = Vec::new();
                    let mut current = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
                    while current <= to {
                        keys.push(NodeFills1mAggregateHourKey { hour: current });
                        current += chrono::Duration::hours(1);
                    }
                    keys
                };

                match run_backfill(&source, &db, keys, force).await {
                    Ok(stats) => BackfillApiResponse::Ok(Json(stats.into())),
                    Err(msg) => BackfillApiResponse::InternalError(PlainText(msg)),
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// BackfillStats → BackfillResult
// ---------------------------------------------------------------------------

impl From<crate::backfill::BackfillStats> for BackfillResult {
    fn from(s: crate::backfill::BackfillStats) -> Self {
        let total_ok = s.keys_ok.len() as u64;
        let total_err = s.keys_err.len() as u64;
        let total_skipped = s.keys_skipped.len() as u64;
        BackfillResult {
            keys_ok: s.keys_ok,
            keys_err: s.keys_err,
            keys_skipped: s.keys_skipped,
            total_ok,
            total_err,
            total_skipped,
            rows_inserted: s.rows_inserted,
            elapsed_ms: s.elapsed_ms,
        }
    }
}
