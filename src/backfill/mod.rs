pub mod asset_ctxs;
pub mod l2_orderbook;
pub mod node_fills_1m_aggregate;
pub mod node_fills_by_block;
pub mod tracker;

use crate::adapter::error::AdapterError;
use crate::database::QuestDbClient;
use crate::error::{AnySignalError, AnySignalResult};
use tracker::BackfillTracker;

// ---------------------------------------------------------------------------
// Traits
// ---------------------------------------------------------------------------

/// Marker trait for a value that identifies a single partition (e.g. a date
/// or a date+coin pair).  Must be `Display` so it can appear in log output.
pub trait PartitionKey: std::fmt::Display + Send + Sync {}

/// Timing and row-count summary returned by [`PartitionedSource::ingest_partition`].
/// [`run_backfill`] uses this to emit a single structured log line per partition
/// so individual implementations do not need their own `tracing::info!` calls.
pub struct PartitionStats {
    /// Number of rows written to QuestDB.
    pub rows: u64,
    /// Wall-clock time spent fetching / parsing upstream data (ms).
    pub fetch_ms: u128,
    /// Wall-clock time spent writing to QuestDB (ms).
    pub insert_ms: u128,
}

/// A data source that can check for and ingest one partition at a time.
///
/// `partition_exists` only needs the DB handle; the adapter state (S3 client
/// etc.) lives on `self` and is used by `ingest_partition`.
#[async_trait::async_trait]
pub trait PartitionedSource: Send + Sync {
    type Key: PartitionKey;

    /// Return `true` if this partition is already present in QuestDB.
    /// Should return `Ok(false)` on any DB error so the backfill proceeds.
    async fn partition_exists(db: &QuestDbClient, key: &Self::Key) -> AnySignalResult<bool>;

    /// Fetch the partition from its upstream source and write it to QuestDB.
    /// Returns [`PartitionStats`] with row count and fetch/insert timings.
    /// Implementations should **not** emit their own log lines — [`run_backfill`]
    /// handles that using the returned stats.
    async fn ingest_partition(
        &self,
        db: &QuestDbClient,
        key: &Self::Key,
    ) -> AnySignalResult<PartitionStats>;
}

// ---------------------------------------------------------------------------
// Generic backfill loop
// ---------------------------------------------------------------------------

/// Summary of a completed (or partially completed) backfill run.
pub struct BackfillStats {
    pub keys_ok: Vec<String>,
    pub keys_err: Vec<String>,
    pub keys_skipped: Vec<String>,
    pub rows_inserted: u64,
    pub elapsed_ms: u64,
}

/// Drive a backfill loop over `keys`, honouring dedup and collecting results.
///
/// Returns `Err(msg)` immediately on an AWS credential error (all subsequent
/// fetches would fail anyway).  Otherwise accumulates per-key outcomes into
/// [`BackfillStats`].
///
/// When `tracker` is provided the job is registered on entry and automatically
/// unregistered when the function returns (via RAII guard), so the tracker is
/// always empty when no backfill is running.
///
/// Error classification per key:
/// - `Unauthorized`  → fatal, return `Err` immediately
/// - `NotFound`      → skipped (data absent from archive)
/// - anything else   → recorded in `dates_err`, loop continues
pub async fn run_backfill<S>(
    source: &S,
    db: &QuestDbClient,
    keys: Vec<S::Key>,
    force: bool,
    tracker: Option<(&BackfillTracker, &str)>,
) -> Result<BackfillStats, String>
where
    S: PartitionedSource,
{
    let started = std::time::Instant::now();

    // Register with the tracker; guard unregisters on any return path.
    let _guard = tracker.map(|(t, name)| t.register(name, keys.len()));

    let mut stats = BackfillStats {
        keys_ok: Vec::new(),
        keys_err: Vec::new(),
        keys_skipped: Vec::new(),
        rows_inserted: 0,
        elapsed_ms: 0,
    };

    for (idx, key) in keys.into_iter().enumerate() {
        let label = key.to_string();

        if let (Some((t, source_name)), Some(ref g)) = (tracker, &_guard) {
            if !t.try_claim_key(g.id, source_name, &label, idx) {
                stats
                    .keys_skipped
                    .push(format!("{label}: already being indexed"));
                continue;
            }
        }

        if !force {
            match S::partition_exists(db, &key).await {
                Ok(true) => {
                    stats.keys_skipped.push(label);
                    continue;
                }
                Ok(false) => {}
                Err(e) => {
                    stats
                        .keys_err
                        .push(format!("{label}: existence check failed: {e}"));
                    continue;
                }
            }
        }

        match source.ingest_partition(db, &key).await {
            Ok(ps) => {
                tracing::info!(
                    key = %label,
                    fetch_ms = ps.fetch_ms,
                    insert_ms = ps.insert_ms,
                    rows = ps.rows,
                    "partition ingested"
                );
                stats.rows_inserted += ps.rows;
                stats.keys_ok.push(label);
            }
            Err(AnySignalError::Adapter(AdapterError::Unauthorized(msg))) => {
                return Err(format!(
                    "AWS credential error — all further periods would fail: {msg}"
                ));
            }
            Err(AnySignalError::Adapter(AdapterError::NotFound(msg))) => {
                stats.keys_skipped.push(format!("{label}: {msg}"));
            }
            Err(e) => stats.keys_err.push(format!("{label}: {e}")),
        }
    }

    stats.elapsed_ms = started.elapsed().as_millis() as u64;
    Ok(stats)
}
