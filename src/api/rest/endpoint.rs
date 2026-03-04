use crate::backfill::asset_ctxs::AssetCtxsSource;
use crate::backfill::l2_orderbook::{L2PartitionKey, L2SnapshotSource};
use crate::backfill::market_state_1m::{MarketState1mHourKey, MarketState1mSource};
use crate::backfill::node_fills::{NodeFillsLegacyHourKey, NodeFillsLegacySource};
use crate::backfill::node_fills_1m_aggregate::{NodeFills1mAggregateHourKey, NodeFills1mAggregateSource};
use crate::backfill::node_fills_by_block::{NodeFillsHourKey, NodeFillsSource};
use crate::backfill::node_fills_legacy_1m_aggregate::{NodeFillsLegacy1mAggregateHourKey, NodeFillsLegacy1mAggregateSource};
use crate::backfill::run_backfill;
use crate::backfill::tracker::{BackfillTracker, PostmortemSnapshot};
use crate::backfill::PartitionedSource;
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
    pub tracker: BackfillTracker,
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
    /// **Timestamp semantics:** `ts` in `market_data` is a **point-in-time snapshot** —
    /// `12:00:00` means the market state observed AT exactly `12:00:00`, not the
    /// start of any aggregation window.
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

    /// Hyperliquid legacy node fills from `s3://hl-mainnet-node-data`.
    ///
    /// Fetches `node_fills/hourly/{YYYYMMDD}/{H}.lz4` for every hour in the
    /// requested range and inserts into `hyperliquid_fill`.
    ///
    /// **Wire format:** one fill per line as `["wallet", fill_object]` — differs
    /// from `HyperliquidNodeFills` whose files group fills by block
    /// (`{"events":[...]}`).  The fill fields are otherwise identical.
    ///
    /// **Data available from:** `2025-05-25T14:00:00` **to** `2025-07-27T08:00:00`
    ///
    /// Note: `2025-07-27` hour 8 exists in both this dataset and
    /// `HyperliquidNodeFills`.  The `partition_exists` check prevents
    /// double-ingestion — whichever source is backfilled first wins.
    HyperliquidNodeFillsLegacy,

    /// Hyperliquid legacy node fills aggregated into 1-minute buckets.
    ///
    /// Fetches the same hourly files as `HyperliquidNodeFillsLegacy`
    /// (`node_fills/hourly/{YYYYMMDD}/{H}.lz4`) but aggregates each fill into
    /// `(coin, category, buy_side, minute)` buckets before writing to
    /// `hyperliquid_fill_1m_aggregate`.  The minute bucket is **left-closed**:
    /// `12:00:00` covers `[12:00, 12:01)`.
    ///
    /// Unlike `HyperliquidNodeFills1mAggregate`, neighbouring hours are **not**
    /// fetched: the legacy archive is keyed by fill timestamp so files align
    /// cleanly to hour boundaries without boundary spillover.
    ///
    /// **Data available from:** `2025-05-25T14:00:00` **to** `2025-07-27T08:00:00`
    HyperliquidNodeFillsLegacy1mAggregate,

    /// Compute `market_state_1m` by joining `hyperliquid_fill_1m_aggregate`
    /// (minute-level fill stats) with `market_data` (daily price/market
    /// snapshots).  No S3 access — pure DB-to-DB computation.
    ///
    /// Per minute bucket and coin the row contains:
    /// - `price_oracle`, `price_mark`, `price_mid` — from `market_data` daily snapshot
    /// - `open_interest`, `funding_rate`, `volume_24h_usd` — from `market_data` daily snapshot
    /// - `trade_volume`, `trade_count` — buy-side fills only (pairs cancel out)
    /// - `liquidation_long/short_volume/count` — fills with category in
    ///   (`Liquidated Isolated Long`, `Liquidated Cross Long`, …Short)
    ///
    /// Both source tables must be backfilled for the requested range first.
    MarketState1m,
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

/// Status of one active backfill job.
#[derive(Debug, Object)]
struct BackfillStatusEntry {
    /// Unique job ID assigned at registration.
    id: u64,
    /// Source name (e.g. `"HyperliquidNodeFills"`).
    source: String,
    /// All partition keys remaining to be processed (registered upfront, removed as each completes).
    ongoing: Vec<String>,
    /// RFC 3339 UTC timestamp of when the backfill request was received.
    started_at: String,
    /// Wall-clock milliseconds since the backfill request was received.
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
// Database types
// ---------------------------------------------------------------------------

/// Disk space and row count for a single QuestDB table.
#[derive(Debug, Object)]
struct TableStats {
    /// Table name.
    name: String,
    /// Total rows across all partitions.
    row_count: i64,
    /// Total disk usage in bytes across all partitions.
    disk_size_bytes: i64,
}

/// Response for `GET /database`.
#[derive(Debug, Object)]
struct DatabaseStats {
    tables: Vec<TableStats>,
    /// Sum of `disk_size_bytes` across all tables.
    total_disk_size_bytes: i64,
}

#[derive(ApiResponse)]
enum DatabaseApiResponse {
    #[oai(status = 200)]
    Ok(Json<DatabaseStats>),
    #[oai(status = 500)]
    InternalError(PlainText<String>),
}

// ---------------------------------------------------------------------------
// Coverage types
// ---------------------------------------------------------------------------

/// One contiguous range of missing partitions (both bounds inclusive).
#[derive(Debug, Object)]
struct CoverageGap {
    /// First missing partition key, e.g. `"2025-07-28T12:00:00"`.
    from: String,
    /// Last missing partition key, e.g. `"2025-07-28T14:00:00"`.
    to: String,
}

/// Response for `GET /coverage`.
#[derive(Debug, Object)]
struct CoverageResult {
    source: String,
    /// Requested range start.
    from: String,
    /// Requested range end.
    to: String,
    /// Total number of partition keys in the requested range.
    total_partitions: u64,
    /// Number of partitions present in QuestDB.
    present_count: u64,
    /// Number of partitions missing from QuestDB.
    missing_count: u64,
    /// Contiguous ranges of missing partitions (from/to both inclusive).
    gaps: Vec<CoverageGap>,
}

#[derive(ApiResponse)]
enum CoverageApiResponse {
    #[oai(status = 200)]
    Ok(Json<CoverageResult>),
    #[oai(status = 400)]
    BadRequest(PlainText<String>),
    #[oai(status = 500)]
    InternalError(PlainText<String>),
}

// ---------------------------------------------------------------------------
// Postmortem types
// ---------------------------------------------------------------------------

/// Summary of one completed backfill job.
#[derive(Debug, Object)]
struct PostmortemEntry {
    /// Source name (e.g. `"MarketState1m"`).
    source: String,
    /// RFC 3339 UTC timestamp when the job started.
    started_at: String,
    /// RFC 3339 UTC timestamp when the job completed.
    completed_at: String,
    /// Wall-clock milliseconds the job ran for.
    elapsed_ms: u64,
    /// Total rows written to QuestDB.
    rows_inserted: u64,
    /// Number of partitions ingested successfully.
    keys_ok_count: u64,
    /// Number of partitions skipped (already present or deduped).
    keys_skipped_count: u64,
    /// Per-partition errors, formatted as `"<key>: <error message>"`.
    errors: Vec<String>,
    /// Set when the job aborted early (e.g. AWS credential failure).
    fatal_error: Option<String>,
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

    /// List all currently running backfill jobs.
    ///
    /// Returns an empty array when no backfill is active.
    /// Each entry shows the source, the partition key currently being
    /// processed, progress counts, and elapsed time.
    #[oai(path = "/backfill/status", method = "get")]
    async fn backfill_status(&self) -> Json<Vec<BackfillStatusEntry>> {
        let entries = self
            .tracker
            .list()
            .into_iter()
            .map(|s| BackfillStatusEntry {
                id: s.id,
                source: s.source,
                ongoing: s.ongoing,
                started_at: s.started_at,
                elapsed_ms: s.elapsed_ms,
            })
            .collect();
        Json(entries)
    }

    /// Backfill historic data for a datetime range.
    ///
    /// `from` and `to` accept either a full datetime (`2024-01-01T00:00:00`) or a
    /// date-only value (`2024-01-01`), which is treated as midnight (`T00:00:00`).
    ///
    /// By default, periods already present in QuestDB are skipped.
    /// Set `force=true` to bypass that check and always fetch+insert.
    ///
    /// | `source`                                  | Steps  | Table                            | Available from         | Available to           | Extra params       |
    /// |-------------------------------------------|--------|----------------------------------|------------------------|------------------------|--------------------|
    /// | `HyperliquidAssetCtxs`                    | daily  | `market_data`                    | 2023-05-20             | present                | —                  |
    /// | `HyperliquidL2Orderbook`                  | hourly | `l2_orderbook`                   | 2023-04-15             | present                | `coins` (required) |
    /// | `HyperliquidNodeFillsLegacy`              | hourly | `hyperliquid_fill`               | 2025-05-25T14:00:00    | 2025-07-27T08:00:00    | —                  |
    /// | `HyperliquidNodeFillsLegacy1mAggregate`   | hourly | `hyperliquid_fill_1m_aggregate`  | 2025-05-25T14:00:00    | 2025-07-27T08:00:00    | —                  |
    /// | `HyperliquidNodeFills`                    | hourly | `hyperliquid_fill`               | 2025-07-27T08:00:00    | present                | —                  |
    /// | `HyperliquidNodeFills1mAggregate`         | hourly | `hyperliquid_fill_1m_aggregate`  | 2025-07-27             | present                | —                  |
    /// | `MarketState1m`                           | hourly | `market_state_1m`                | 2025-05-25T14:00:00    | present                | —                  |
    ///
    /// **Coverage gap in `hyperliquid_fill`:** `2025-03-22T10:00` – `2025-05-25T13:00` is covered
    /// by the `node_trades` S3 dataset (`node_trades/hourly/`), which uses a trade-level schema
    /// (one record per matched trade, both participants in `side_info`).  It is missing the
    /// `category` (Open Long / Close Short / …) and `realized_pnl` columns required by
    /// `hyperliquid_fill`, so it is not implemented as a backfill source.
    ///
    /// **`HyperliquidNodeFills1mAggregate`** aggregates raw fills into
    /// `(coin, category, buy_side, minute)` buckets before writing.
    /// The minute bucket is left-closed: `12:00:00` covers `[12:00, 12:01)`.
    ///
    /// **Timestamp semantics — joining `market_data` with `hyperliquid_fill_1m_aggregate`:**
    /// - `market_data.ts = 12:00:00` → snapshot of market state **at** `12:00:00`.
    /// - `hyperliquid_fill_1m_aggregate.ts = 12:00:00` → fills that occurred **during**
    ///   `[12:00:00, 12:01:00)`, i.e. up to `12:00:59.999`.
    ///
    /// These two tables share the same `ts` value for the same minute, but the
    /// semantics differ: the aggregate row's `ts` is the window **open**, not a
    /// point-in-time reading.  When correlating fill activity with price, the
    /// `market_data` snapshot at `ts` represents the price **at the start** of the
    /// minute in which those fills occurred.
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

                self.record_backfill_result("HyperliquidAssetCtxs", run_backfill(&source, &db, keys, force, Some((&self.tracker, "HyperliquidAssetCtxs"))).await)
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

                self.record_backfill_result("HyperliquidL2Orderbook", run_backfill(&source, &db, keys, force, Some((&self.tracker, "HyperliquidL2Orderbook"))).await)
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

                self.record_backfill_result("HyperliquidNodeFills", run_backfill(&source, &db, keys, force, Some((&self.tracker, "HyperliquidNodeFills"))).await)
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

                self.record_backfill_result("HyperliquidNodeFills1mAggregate", run_backfill(&source, &db, keys, force, Some((&self.tracker, "HyperliquidNodeFills1mAggregate"))).await)
            }

            BackfillSource::HyperliquidNodeFillsLegacy => {
                let source = match NodeFillsLegacySource::new().await {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {e}"
                        )))
                    }
                };

                // Build hour-by-hour key iterator.
                // Data is available 2025-05-25T14:00:00 – 2025-07-27T08:00:00.
                let keys = {
                    let mut keys = Vec::new();
                    let mut current = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
                    while current <= to {
                        keys.push(NodeFillsLegacyHourKey { hour: current });
                        current += chrono::Duration::hours(1);
                    }
                    keys
                };

                self.record_backfill_result("HyperliquidNodeFillsLegacy", run_backfill(&source, &db, keys, force, Some((&self.tracker, "HyperliquidNodeFillsLegacy"))).await)
            }

            BackfillSource::HyperliquidNodeFillsLegacy1mAggregate => {
                let source = match NodeFillsLegacy1mAggregateSource::new().await {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {e}"
                        )))
                    }
                };

                // Build hour-by-hour key iterator.
                // Data is available 2025-05-25T14:00:00 – 2025-07-27T08:00:00.
                let keys = {
                    let mut keys = Vec::new();
                    let mut current = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
                    while current <= to {
                        keys.push(NodeFillsLegacy1mAggregateHourKey { hour: current });
                        current += chrono::Duration::hours(1);
                    }
                    keys
                };

                self.record_backfill_result("HyperliquidNodeFillsLegacy1mAggregate", run_backfill(&source, &db, keys, force, Some((&self.tracker, "HyperliquidNodeFillsLegacy1mAggregate"))).await)
            }

            BackfillSource::MarketState1m => {
                let source = MarketState1mSource::new();

                // Build hour-by-hour key iterator.
                // Requires hyperliquid_fill_1m_aggregate and market_data to be
                // backfilled for the requested range first.
                let keys = {
                    let mut keys = Vec::new();
                    let mut current = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
                    while current <= to {
                        keys.push(MarketState1mHourKey { hour: current });
                        current += chrono::Duration::hours(1);
                    }
                    keys
                };

                self.record_backfill_result("MarketState1m", run_backfill(&source, &db, keys, force, Some((&self.tracker, "MarketState1m"))).await)
            }
        }
    }

    /// Return disk usage and row counts for every QuestDB table.
    ///
    /// Queries `tables()` to list all tables, then aggregates
    /// `diskSize` and `numRows` from `table_partitions()` for each one.
    #[oai(path = "/database", method = "get")]
    async fn database(&self) -> DatabaseApiResponse {
        let db = match QuestDbClient::new(&self.config) {
            Ok(c) => c,
            Err(e) => {
                return DatabaseApiResponse::InternalError(PlainText(format!(
                    "Failed to connect to QuestDB: {e}"
                )))
            }
        };

        let tables_json = match db
            .query_json("SELECT table_name FROM tables() ORDER BY table_name")
            .await
        {
            Ok(j) => j,
            Err(e) => {
                return DatabaseApiResponse::InternalError(PlainText(format!(
                    "Failed to list tables: {e}"
                )))
            }
        };

        let table_names: Vec<String> = tables_json["dataset"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|row| row[0].as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut tables = Vec::with_capacity(table_names.len());
        let mut total_disk_size_bytes: i64 = 0;

        for name in table_names {
            let sql = format!(
                "SELECT sum(diskSize), sum(numRows) FROM table_partitions('{name}')"
            );
            let json = match db.query_json(&sql).await {
                Ok(j) => j,
                Err(_) => continue,
            };
            let first_row = match json["dataset"].as_array().and_then(|d| d.first()) {
                Some(row) => row.clone(),
                None => continue,
            };
            let disk_size_bytes = first_row[0].as_i64().unwrap_or(0);
            let row_count = first_row[1].as_i64().unwrap_or(0);
            total_disk_size_bytes += disk_size_bytes;
            tables.push(TableStats {
                name,
                row_count,
                disk_size_bytes,
            });
        }

        DatabaseApiResponse::Ok(Json(DatabaseStats {
            tables,
            total_disk_size_bytes,
        }))
    }

    /// Return the postmortem log of recently completed backfill jobs.
    ///
    /// Lists the last 100 completed jobs, most recent first.
    /// Each entry includes per-partition errors and an optional `fatal_error`
    /// for jobs that aborted early (e.g. AWS credential failure).
    #[oai(path = "/backfill/postmortem", method = "get")]
    async fn backfill_postmortem(&self) -> Json<Vec<PostmortemEntry>> {
        let entries = self
            .tracker
            .list_postmortem()
            .into_iter()
            .map(PostmortemEntry::from)
            .collect();
        Json(entries)
    }

    /// Check which partitions are present or missing in QuestDB for a source and date range.
    ///
    /// Iterates the same partition keys as `GET /backfill` and tests each one against
    /// QuestDB.  Returns a list of contiguous **gaps** (missing ranges), plus summary
    /// counts.  The `from`/`to` in each gap are the first and last missing partition
    /// key labels (both inclusive), using the same format as the source's partition key
    /// (hourly: `"YYYY-MM-DDTHH:00:00"`, daily: `"YYYY-MM-DD"`).
    ///
    /// Useful for auditing coverage before or after a backfill run.
    ///
    /// The `source` parameter accepts the same values as `GET /backfill`.
    /// `coins` is required for `HyperliquidL2Orderbook`.
    #[oai(path = "/coverage", method = "get")]
    async fn coverage(
        &self,
        /// Start of the range to check, **inclusive**.
        from: Query<String>,
        /// End of the range to check, **inclusive**.
        to: Query<String>,
        /// Which data source to check coverage for.
        source: Query<BackfillSource>,
        /// Comma-separated coin tickers (required for `HyperliquidL2Orderbook`).
        coins: Query<Option<String>>,
    ) -> CoverageApiResponse {
        let from = match parse_flexible_datetime(&from.0) {
            Ok(dt) => dt,
            Err(e) => return CoverageApiResponse::BadRequest(PlainText(e)),
        };
        let to = match parse_flexible_datetime(&to.0) {
            Ok(dt) => dt,
            Err(e) => return CoverageApiResponse::BadRequest(PlainText(e)),
        };
        if from > to {
            return CoverageApiResponse::BadRequest(PlainText(
                "'from' must be on or before 'to'.".to_string(),
            ));
        }

        let db = match QuestDbClient::new(&self.config) {
            Ok(c) => c,
            Err(e) => {
                return CoverageApiResponse::InternalError(PlainText(format!(
                    "Failed to connect to QuestDB: {e}"
                )))
            }
        };

        let source_name = format!("{:?}", source.0);

        let checked: Vec<(String, bool)> = match source.0 {
            BackfillSource::HyperliquidAssetCtxs => {
                let keys = day_range(from.date(), to.date());
                check_coverage::<AssetCtxsSource, _>(&db, keys).await
            }

            BackfillSource::HyperliquidL2Orderbook => {
                let coin_list: Vec<String> = match coins.0 {
                    Some(s) if !s.trim().is_empty() => {
                        s.split(',').map(|c| c.trim().to_uppercase()).collect()
                    }
                    _ => {
                        return CoverageApiResponse::BadRequest(PlainText(
                            "'coins' is required for HyperliquidL2Orderbook.".to_string(),
                        ))
                    }
                };
                let keys: Vec<L2PartitionKey> = hour_range(from, to)
                    .into_iter()
                    .flat_map(|h| {
                        coin_list
                            .iter()
                            .map(move |c| L2PartitionKey { hour: h, coin: c.clone() })
                            .collect::<Vec<_>>()
                    })
                    .collect();
                check_coverage::<L2SnapshotSource, _>(&db, keys).await
            }

            BackfillSource::HyperliquidNodeFills => {
                let keys = hour_range(from, to)
                    .into_iter()
                    .map(|h| NodeFillsHourKey { hour: h })
                    .collect();
                check_coverage::<NodeFillsSource, _>(&db, keys).await
            }

            BackfillSource::HyperliquidNodeFills1mAggregate => {
                let keys = hour_range(from, to)
                    .into_iter()
                    .map(|h| NodeFills1mAggregateHourKey { hour: h })
                    .collect();
                check_coverage::<NodeFills1mAggregateSource, _>(&db, keys).await
            }

            BackfillSource::HyperliquidNodeFillsLegacy => {
                let keys = hour_range(from, to)
                    .into_iter()
                    .map(|h| NodeFillsLegacyHourKey { hour: h })
                    .collect();
                check_coverage::<NodeFillsLegacySource, _>(&db, keys).await
            }

            BackfillSource::HyperliquidNodeFillsLegacy1mAggregate => {
                let keys = hour_range(from, to)
                    .into_iter()
                    .map(|h| NodeFillsLegacy1mAggregateHourKey { hour: h })
                    .collect();
                check_coverage::<NodeFillsLegacy1mAggregateSource, _>(&db, keys).await
            }

            BackfillSource::MarketState1m => {
                let keys = hour_range(from, to)
                    .into_iter()
                    .map(|h| MarketState1mHourKey { hour: h })
                    .collect();
                check_coverage::<MarketState1mSource, _>(&db, keys).await
            }
        };

        // `checked` arrives in arbitrary order (concurrent checks) — sort by label.
        let mut checked = checked;
        checked.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        let total_partitions = checked.len() as u64;
        let present_count = checked.iter().filter(|(_, p)| *p).count() as u64;
        let missing_count = total_partitions - present_count;
        let gaps = merge_gaps(&checked);

        CoverageApiResponse::Ok(Json(CoverageResult {
            source: source_name,
            from: from.format("%Y-%m-%dT%H:%M:%S").to_string(),
            to: to.format("%Y-%m-%dT%H:%M:%S").to_string(),
            total_partitions,
            present_count,
            missing_count,
            gaps,
        }))
    }

    /// Record a completed backfill result in the postmortem log and convert it
    /// to the appropriate [`BackfillApiResponse`].
    fn record_backfill_result(
        &self,
        source_name: &str,
        result: Result<crate::backfill::BackfillStats, String>,
    ) -> BackfillApiResponse {
        self.tracker.record_completion(source_name, &result);
        match result {
            Ok(stats) => BackfillApiResponse::Ok(Json(stats.into())),
            Err(msg) => BackfillApiResponse::InternalError(PlainText(msg)),
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

impl From<PostmortemSnapshot> for PostmortemEntry {
    fn from(s: PostmortemSnapshot) -> Self {
        PostmortemEntry {
            source: s.source,
            started_at: s.started_at,
            completed_at: s.completed_at,
            elapsed_ms: s.elapsed_ms,
            rows_inserted: s.rows_inserted,
            keys_ok_count: s.keys_ok_count as u64,
            keys_skipped_count: s.keys_skipped_count as u64,
            errors: s.errors,
            fatal_error: s.fatal_error,
        }
    }
}

// ---------------------------------------------------------------------------
// Coverage helpers
// ---------------------------------------------------------------------------

/// Build a vec of every calendar day from `start` to `end` (both inclusive).
fn day_range(start: NaiveDate, end: NaiveDate) -> Vec<NaiveDate> {
    let mut days = Vec::new();
    let mut cur = start;
    while cur <= end {
        days.push(cur);
        match cur.succ_opt() {
            Some(next) => cur = next,
            None => break,
        }
    }
    days
}

/// Build a vec of every hour-aligned `NaiveDateTime` from `from` to `to` (both inclusive).
fn hour_range(from: NaiveDateTime, to: NaiveDateTime) -> Vec<NaiveDateTime> {
    let mut hours = Vec::new();
    let mut cur = from.date().and_hms_opt(from.hour(), 0, 0).unwrap_or(from);
    while cur <= to {
        hours.push(cur);
        cur += chrono::Duration::hours(1);
    }
    hours
}

/// Check existence of every key in `keys` against QuestDB concurrently (16 at a time).
/// Returns `(label, is_present)` pairs in **arbitrary** order.
async fn check_coverage<S, K>(db: &QuestDbClient, keys: Vec<K>) -> Vec<(String, bool)>
where
    S: PartitionedSource<Key = K>,
    K: crate::backfill::PartitionKey + Send + 'static,
{
    use futures::StreamExt as _;
    futures::stream::iter(keys)
        .map(|key| async move {
            let label = key.to_string();
            let present = S::partition_exists(db, &key).await.unwrap_or(false);
            (label, present)
        })
        .buffer_unordered(16)
        .collect()
        .await
}

/// Merge a sorted `(label, is_present)` slice into contiguous missing ranges.
fn merge_gaps(sorted: &[(String, bool)]) -> Vec<CoverageGap> {
    let mut gaps: Vec<CoverageGap> = Vec::new();
    let mut gap_start: Option<&str> = None;
    let mut gap_end: Option<&str> = None;

    for (label, present) in sorted {
        if !present {
            if gap_start.is_none() {
                gap_start = Some(label);
            }
            gap_end = Some(label);
        } else if let (Some(start), Some(end)) = (gap_start.take(), gap_end.take()) {
            gaps.push(CoverageGap { from: start.to_string(), to: end.to_string() });
            gap_end = None;
        }
    }
    // Close any open gap at the end of the range.
    if let (Some(start), Some(end)) = (gap_start, gap_end) {
        gaps.push(CoverageGap { from: start.to_string(), to: end.to_string() });
    }
    gaps
}
