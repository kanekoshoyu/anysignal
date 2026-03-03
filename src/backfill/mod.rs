pub mod asset_ctxs;
pub mod l2_orderbook;
pub mod node_fills;
pub mod node_fills_1m_aggregate;
pub mod node_fills_by_block;
pub mod tracker;

use crate::adapter::error::AdapterError;
use crate::database::QuestDbClient;
use crate::error::{AnySignalError, AnySignalResult};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use tracker::BackfillTracker;

/// Maximum number of partition keys processed concurrently within one backfill job.
const BACKFILL_CONCURRENCY: usize = 8;

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

/// Drive a concurrent backfill over `keys`, honouring dedup and collecting results.
///
/// Up to [`BACKFILL_CONCURRENCY`] keys are processed simultaneously.
/// Returns `Err(msg)` if an AWS credential error is encountered (recorded after
/// all in-flight work drains).  Otherwise accumulates per-key outcomes into
/// [`BackfillStats`].
///
/// When `tracker` is provided the job is registered on entry and automatically
/// unregistered when the function returns (via RAII guard), so the tracker is
/// always empty when no backfill is running.
///
/// All keys that need ingestion are claimed in the tracker before Phase 2 starts,
/// so `/backfill/status` shows the full remaining work queue from the outset.
/// Keys are released from the tracker as each one completes.
///
/// Error classification per key:
/// - `Unauthorized`  → fatal; remaining keys are skipped after in-flight work drains
/// - `NotFound`      → skipped (data absent from archive)
/// - anything else   → recorded in `keys_err`, loop continues
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
    use futures::StreamExt as _;

    let started = std::time::Instant::now();

    // Register with the tracker; guard unregisters on any return path.
    let _guard = tracker.map(|(t, name)| t.register(name));
    let guard_id = _guard.as_ref().map(|g| g.id);

    // Shared accumulators written to by concurrent futures.
    // Mutexes are never held across `.await` points, so no async deadlocks.
    let keys_ok      = Arc::new(Mutex::new(Vec::<String>::new()));
    let keys_err     = Arc::new(Mutex::new(Vec::<String>::new()));
    let keys_skipped = Arc::new(Mutex::new(Vec::<String>::new()));
    let rows_total   = Arc::new(AtomicU64::new(0));
    let fatal_msg    = Arc::new(Mutex::new(None::<String>));

    // -----------------------------------------------------------------------
    // Phase 1: determine which keys actually need ingestion.
    //
    // Existence checks run concurrently so we know the full pending set before
    // any ingestion begins.  Results are encoded as:
    //   Ok((Some(key), label)) → missing, queue for ingestion
    //   Ok((None,      label)) → already in QuestDB → keys_skipped
    //   Err(msg)               → existence check failed → keys_err
    // -----------------------------------------------------------------------
    let check_results: Vec<Result<(Option<S::Key>, String), String>> = if force {
        keys.into_iter()
            .map(|k| { let l = k.to_string(); Ok((Some(k), l)) })
            .collect()
    } else {
        futures::stream::iter(keys)
            .map(|key| async move {
                let label = key.to_string();
                match S::partition_exists(db, &key).await {
                    Ok(true)  => Ok((None, label)),
                    Ok(false) => Ok((Some(key), label)),
                    Err(e)    => Err(format!("{label}: existence check failed: {e}")),
                }
            })
            .buffer_unordered(BACKFILL_CONCURRENCY)
            .collect::<Vec<_>>()
            .await
    };

    // Partition results; claim all pending keys in the tracker upfront so that
    // /backfill/status reflects the full remaining work queue immediately.
    let mut pending_keys: Vec<(S::Key, String)> = Vec::new();
    {
        let mut sk = keys_skipped.lock().unwrap_or_else(|p| p.into_inner());
        let mut ke = keys_err.lock().unwrap_or_else(|p| p.into_inner());
        for result in check_results {
            match result {
                Ok((None, label)) => sk.push(label), // already exists in QuestDB
                Ok((Some(key), label)) => {
                    // Dedup: if another concurrent job already owns this key, skip it.
                    let claimed = match (tracker, guard_id) {
                        (Some((t, sn)), Some(gid)) => t.try_claim_key(gid, sn, &label),
                        _ => true,
                    };
                    if claimed {
                        pending_keys.push((key, label));
                    } else {
                        sk.push(format!("{label}: already being indexed"));
                    }
                }
                Err(msg) => ke.push(msg),
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: ingest all pending keys concurrently.
    // Existence check is already done; just ingest and release on completion.
    // -----------------------------------------------------------------------
    {
        let keys_ok      = Arc::clone(&keys_ok);
        let keys_err     = Arc::clone(&keys_err);
        let keys_skipped = Arc::clone(&keys_skipped);
        let rows_total   = Arc::clone(&rows_total);
        let fatal_msg    = Arc::clone(&fatal_msg);

        futures::stream::iter(pending_keys)
            .map(|(key, label)| {
                let keys_ok      = Arc::clone(&keys_ok);
                let keys_err     = Arc::clone(&keys_err);
                let keys_skipped = Arc::clone(&keys_skipped);
                let rows_total   = Arc::clone(&rows_total);
                let fatal_msg    = Arc::clone(&fatal_msg);

                async move {
                    let release = || {
                        if let (Some((t, _)), Some(gid)) = (tracker, guard_id) {
                            t.release_key(gid, &label);
                        }
                    };

                    // Skip if a fatal credential error was already recorded.
                    {
                        let f = fatal_msg.lock().unwrap_or_else(|p| p.into_inner());
                        if f.is_some() {
                            release();
                            return;
                        }
                    }

                    // Ingest partition.
                    match source.ingest_partition(db, &key).await {
                        Ok(ps) => {
                            tracing::info!(
                                key       = %label,
                                fetch_ms  = ps.fetch_ms,
                                insert_ms = ps.insert_ms,
                                rows      = ps.rows,
                                "partition ingested"
                            );
                            rows_total.fetch_add(ps.rows, Ordering::Relaxed);
                            keys_ok
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push(label.clone());
                        }
                        Err(AnySignalError::Adapter(AdapterError::Unauthorized(msg))) => {
                            *fatal_msg.lock().unwrap_or_else(|p| p.into_inner()) =
                                Some(format!(
                                    "AWS credential error — all further periods would fail: {msg}"
                                ));
                            keys_err
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push(format!("{label}: {msg}"));
                        }
                        Err(AnySignalError::Adapter(AdapterError::NotFound(msg))) => {
                            keys_skipped
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push(format!("{label}: {msg}"));
                        }
                        Err(e) => {
                            keys_err
                                .lock()
                                .unwrap_or_else(|p| p.into_inner())
                                .push(format!("{label}: {e}"));
                        }
                    }

                    release();
                }
            })
            .buffer_unordered(BACKFILL_CONCURRENCY)
            .collect::<()>()
            .await;
    }

    let elapsed_ms = started.elapsed().as_millis() as u64;

    // Extract accumulated results (stream is done; no other holders of the Arcs).
    let fatal = std::mem::take(&mut *fatal_msg.lock().unwrap_or_else(|p| p.into_inner()));
    if let Some(msg) = fatal {
        return Err(msg);
    }

    let keys_ok      = std::mem::take(&mut *keys_ok.lock().unwrap_or_else(|p| p.into_inner()));
    let keys_err     = std::mem::take(&mut *keys_err.lock().unwrap_or_else(|p| p.into_inner()));
    let keys_skipped = std::mem::take(&mut *keys_skipped.lock().unwrap_or_else(|p| p.into_inner()));
    let rows_inserted = rows_total.load(Ordering::Relaxed);

    Ok(BackfillStats {
        keys_ok,
        keys_err,
        keys_skipped,
        rows_inserted,
        elapsed_ms,
    })
}
