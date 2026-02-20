use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxs;
use crate::adapter::hyperliquid_s3::market_data::MarketData;
use crate::config::Config;
use crate::database::{asset_ctxs_date_exists, insert_asset_ctxs, insert_l2_snapshots,
                       l2_snapshot_coin_hour_exists, questdb_sender};
use crate::metadata::cargo_package_version;
use chrono::{NaiveDate, NaiveDateTime, Timelike};
use poem_openapi::{
    param::Query,
    payload::{Json, PlainText},
    ApiResponse, Enum, Object, OpenApi,
};

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
    HyperliquidAssetCtxs,

    /// Hyperliquid L2 orderbook snapshots from the public S3 archive.
    ///
    /// Fetches `s3://hyperliquid-archive/market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4`
    /// iterating **hour-by-hour** from `from` to `to`.  Requires `coins`.
    HyperliquidL2Orderbook,
}

/// Per-run summary returned by `GET /backfill`.
#[derive(Debug, Object)]
struct BackfillResult {
    /// Dates (asset_ctxs) or datetimes (L2) fetched successfully.
    dates_ok: Vec<String>,
    /// Dates / datetimes that failed, formatted as `"<key>: <error>"`.
    dates_err: Vec<String>,
    /// Dates / datetimes skipped because data was already present in QuestDB.
    dates_skipped: Vec<String>,
    /// Number of periods fetched successfully.
    total_ok: u64,
    /// Number of periods that produced an error.
    total_err: u64,
    /// Number of periods skipped due to existing data.
    total_skipped: u64,
    /// Total rows inserted into QuestDB.
    rows_inserted: u64,
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
    /// `from` and `to` are **datetime** values at **hour precision**,
    /// e.g. `2024-01-01T00:00:00`.
    ///
    /// - `HyperliquidAssetCtxs` — steps day-by-day using only the date part.
    /// - `HyperliquidL2Orderbook` — steps **hour-by-hour** from `from` to `to`;
    ///   requires `coins` (comma-separated tickers, e.g. `BTC,ETH`).
    ///
    /// By default, periods already present in QuestDB are skipped.
    /// Set `force=true` to bypass that check and always fetch+insert.
    ///
    /// | `source`                  | Steps | Extra params |
    /// |---------------------------|-------|--------------|
    /// | `HyperliquidAssetCtxs`    | daily | — |
    /// | `HyperliquidL2Orderbook`  | hourly | `coins` (required) |
    #[oai(path = "/backfill", method = "get")]
    async fn backfill(
        &self,
        /// Start of the range, **inclusive** (e.g. `2024-01-01T00:00:00`).
        from: Query<NaiveDateTime>,
        /// End of the range, **inclusive** (e.g. `2024-01-07T23:00:00`).
        to: Query<NaiveDateTime>,
        /// Which historic data source to pull from.
        source: Query<BackfillSource>,
        /// Skip the duplicate check and always fetch+insert.  Defaults to `false`.
        force: Query<Option<bool>>,
        /// Comma-separated coin tickers (required for `HyperliquidL2Orderbook`,
        /// e.g. `BTC,ETH`).
        coins: Query<Option<String>>,
    ) -> BackfillApiResponse {
        let (from, to, source, force) = (from.0, to.0, source.0, force.0.unwrap_or(false));

        if from > to {
            return BackfillApiResponse::BadRequest(PlainText(
                "'from' must be on or before 'to'.".to_string(),
            ));
        }

        match source {
            BackfillSource::HyperliquidAssetCtxs => {
                let fetcher = match AssetCtxs::new(&self.config).await {
                    Ok(f) => f,
                    Err(e) => return BackfillApiResponse::InternalError(PlainText(format!(
                        "Failed to initialise S3 client: {e}"
                    ))),
                };
                let mut sender = match questdb_sender(&self.config) {
                    Ok(s) => s,
                    Err(e) => return BackfillApiResponse::InternalError(PlainText(format!(
                        "Failed to connect to QuestDB: {e}"
                    ))),
                };

                let mut dates_ok: Vec<String> = Vec::new();
                let mut dates_err: Vec<String> = Vec::new();
                let mut dates_skipped: Vec<String> = Vec::new();
                let mut rows_inserted: u64 = 0;

                // Step day-by-day over the date portion of from..=to.
                let mut current: NaiveDate = from.date();
                let end_date: NaiveDate = to.date();

                while current <= end_date {
                    let date_str = current.format("%Y-%m-%d").to_string();

                    if !force {
                        match asset_ctxs_date_exists(&self.config, current).await {
                            Ok(true) => {
                                dates_skipped.push(date_str);
                                match current.succ_opt() {
                                    Some(next) => { current = next; }
                                    None => break,
                                }
                                continue;
                            }
                            Ok(false) => {}
                            Err(e) => {
                                dates_err.push(format!("{date_str}: existence check failed: {e}"));
                                match current.succ_opt() {
                                    Some(next) => { current = next; }
                                    None => break,
                                }
                                continue;
                            }
                        }
                    }

                    let result = async {
                        let csv_text = fetcher.fetch_and_decompress(current).await?;
                        let rows = AssetCtxs::parse_csv(&csv_text)?;
                        let n = rows.len() as u64;
                        insert_asset_ctxs(&mut sender, &rows)?;
                        Ok::<u64, crate::error::AnySignalError>(n)
                    }
                    .await;

                    match result {
                        Ok(n) => { rows_inserted += n; dates_ok.push(date_str); }
                        Err(e) => dates_err.push(format!("{date_str}: {e}")),
                    }

                    match current.succ_opt() {
                        Some(next) => current = next,
                        None => break,
                    }
                }

                let total_ok = dates_ok.len() as u64;
                let total_err = dates_err.len() as u64;
                let total_skipped = dates_skipped.len() as u64;
                BackfillApiResponse::Ok(Json(BackfillResult {
                    dates_ok, dates_err, dates_skipped,
                    total_ok, total_err, total_skipped, rows_inserted,
                }))
            }

            BackfillSource::HyperliquidL2Orderbook => {
                // Parse coins (required).
                let coin_list: Vec<String> = match coins.0 {
                    Some(s) if !s.trim().is_empty() => {
                        s.split(',').map(|c| c.trim().to_uppercase()).collect()
                    }
                    _ => return BackfillApiResponse::BadRequest(PlainText(
                        "'coins' is required for HyperliquidL2Orderbook (e.g. coins=BTC,ETH).".to_string(),
                    )),
                };

                let fetcher = match MarketData::new(&self.config).await {
                    Ok(f) => f,
                    Err(e) => return BackfillApiResponse::InternalError(PlainText(format!(
                        "Failed to initialise S3 client: {e}"
                    ))),
                };
                let mut sender = match questdb_sender(&self.config) {
                    Ok(s) => s,
                    Err(e) => return BackfillApiResponse::InternalError(PlainText(format!(
                        "Failed to connect to QuestDB: {e}"
                    ))),
                };

                let mut dates_ok: Vec<String> = Vec::new();
                let mut dates_err: Vec<String> = Vec::new();
                let mut dates_skipped: Vec<String> = Vec::new();
                let mut rows_inserted: u64 = 0;

                // Step hour-by-hour from `from` to `to` (inclusive).
                // Truncate to whole hours so fractional minutes are ignored.
                let mut current = from
                    .date()
                    .and_hms_opt(from.hour(), 0, 0)
                    .unwrap_or(from);

                while current <= to {
                    let hour_str = current.format("%Y-%m-%dT%H:00:00").to_string();
                    let date = current.date();
                    let hour = current.hour() as u8;

                    let mut hour_rows: u64 = 0;
                    let mut hour_errors: Vec<String> = Vec::new();
                    let mut all_skipped = true;

                    for coin in &coin_list {
                        if !force {
                            match l2_snapshot_coin_hour_exists(&self.config, current, coin).await {
                                Ok(true) => continue, // already present
                                Ok(false) => {}
                                Err(e) => {
                                    hour_errors.push(format!("coin={coin}: existence check failed: {e}"));
                                    continue;
                                }
                            }
                        }
                        all_skipped = false;

                        let result = async {
                            let snapshots = fetcher.fetch_and_parse(date, hour, coin).await?;
                            let n = insert_l2_snapshots(&mut sender, &snapshots)?;
                            Ok::<u64, crate::error::AnySignalError>(n as u64)
                        }
                        .await;

                        match result {
                            Ok(n) => hour_rows += n,
                            Err(e) => hour_errors.push(format!("coin={coin}: {e}")),
                        }
                    }

                    if all_skipped && hour_errors.is_empty() {
                        dates_skipped.push(hour_str);
                    } else if hour_errors.is_empty() {
                        rows_inserted += hour_rows;
                        dates_ok.push(hour_str);
                    } else {
                        rows_inserted += hour_rows;
                        dates_err.push(format!(
                            "{hour_str}: {} error(s) — {}",
                            hour_errors.len(),
                            hour_errors.join("; ")
                        ));
                    }

                    current += chrono::Duration::hours(1);
                }

                let total_ok = dates_ok.len() as u64;
                let total_err = dates_err.len() as u64;
                let total_skipped = dates_skipped.len() as u64;
                BackfillApiResponse::Ok(Json(BackfillResult {
                    dates_ok, dates_err, dates_skipped,
                    total_ok, total_err, total_skipped, rows_inserted,
                }))
            }
        }
    }
}
