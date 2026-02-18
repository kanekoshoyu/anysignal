use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxs;
use crate::database::{insert_asset_ctxs, questdb_sender};
use crate::metadata::cargo_package_version;
use chrono::NaiveDate;
use poem_openapi::{
    payload::{Json, PlainText},
    ApiResponse, Enum, Object, OpenApi,
};

pub struct Endpoint;

// ---------------------------------------------------------------------------
// Backfill types
// ---------------------------------------------------------------------------

/// Available historic data sources for a backfill job.
#[derive(Debug, Enum)]
enum BackfillSource {
    /// Hyperliquid daily asset-context snapshots from the public S3 archive.
    ///
    /// Fetches `s3://hyperliquid-archive/asset_ctxs/YYYY-MM-DD.csv.lz4` for
    /// every calendar day in the requested range.
    HyperliquidAssetCtxs,
}

/// Request body for `POST /backfill`.
#[derive(Debug, Object)]
struct BackfillRequest {
    /// First date to fetch, **inclusive** (e.g. `2024-01-01`).
    from: NaiveDate,
    /// Last date to fetch, **inclusive** (e.g. `2024-01-31`).
    to: NaiveDate,
    /// Which historic data source to pull from.
    source: BackfillSource,
}

/// Per-run summary returned by `POST /backfill`.
#[derive(Debug, Object)]
struct BackfillResult {
    /// Calendar dates that were fetched successfully (`YYYY-MM-DD`).
    dates_ok: Vec<String>,
    /// Calendar dates that failed, formatted as `"YYYY-MM-DD: <error>"`.
    dates_err: Vec<String>,
    /// Number of dates fetched successfully.
    total_ok: u64,
    /// Number of dates that produced an error.
    total_err: u64,
    /// Total rows inserted into QuestDB across all successful dates.
    rows_inserted: u64,
}

#[derive(ApiResponse)]
enum BackfillApiResponse {
    /// Backfill finished — inspect `dates_err` for any per-date failures.
    #[oai(status = 200)]
    Ok(Json<BackfillResult>),
    /// Bad request — `from` > `to`.
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

    /// Backfill historic data for a date range.
    ///
    /// Provide human-readable dates (`from` / `to`) in **YYYY-MM-DD** format
    /// and select a data source.  The server fetches one file per calendar day
    /// and returns a per-date success / failure summary.
    ///
    /// Supported sources
    /// -----------------
    /// | `source`                | Description |
    /// |-------------------------|-------------|
    /// | `HyperliquidAssetCtxs`  | Daily asset-context snapshots from the Hyperliquid public S3 archive |
    #[oai(path = "/backfill", method = "post")]
    async fn backfill(&self, body: Json<BackfillRequest>) -> BackfillApiResponse {
        let BackfillRequest { from, to, source } = body.0;

        if from > to {
            return BackfillApiResponse::BadRequest(PlainText(
                "'from' must be on or before 'to'.".to_string(),
            ));
        }

        match source {
            BackfillSource::HyperliquidAssetCtxs => {
                let fetcher = match AssetCtxs::new().await {
                    Ok(f) => f,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to initialise S3 client: {}",
                            e
                        )));
                    }
                };

                let mut sender = match questdb_sender() {
                    Ok(s) => s,
                    Err(e) => {
                        return BackfillApiResponse::InternalError(PlainText(format!(
                            "Failed to connect to QuestDB: {}",
                            e
                        )));
                    }
                };

                let mut dates_ok: Vec<String> = Vec::new();
                let mut dates_err: Vec<String> = Vec::new();
                let mut rows_inserted: u64 = 0;
                let mut current = from;

                while current <= to {
                    let date_str = current.format("%Y-%m-%d").to_string();

                    let result = async {
                        let csv_text = fetcher.fetch_and_decompress(&date_str).await?;
                        let rows = AssetCtxs::parse_csv(&csv_text)?;
                        let n = rows.len() as u64;
                        insert_asset_ctxs(&mut sender, &rows)?;
                        Ok::<u64, crate::error::AnySignalError>(n)
                    }
                    .await;

                    match result {
                        Ok(n) => {
                            rows_inserted += n;
                            dates_ok.push(date_str);
                        }
                        Err(e) => dates_err.push(format!("{}: {}", date_str, e)),
                    }

                    match current.succ_opt() {
                        Some(next) => current = next,
                        None => break,
                    }
                }

                let total_ok = dates_ok.len() as u64;
                let total_err = dates_err.len() as u64;
                BackfillApiResponse::Ok(Json(BackfillResult {
                    dates_ok,
                    dates_err,
                    total_ok,
                    total_err,
                    rows_inserted,
                }))
            }
        }
    }
}
