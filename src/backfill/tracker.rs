use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

use super::BackfillStats;

/// Maximum number of completed jobs retained in the postmortem history.
const MAX_HISTORY: usize = 100;

struct Inner {
    next_id: u64,
    active: Vec<ActiveBackfillJob>,
    history: VecDeque<CompletedBackfillJob>,
}

#[derive(Clone)]
struct ActiveBackfillJob {
    id: u64,
    source: String,
    ongoing: Vec<String>,
    started_at: DateTime<Utc>,
}

struct CompletedBackfillJob {
    source: String,
    started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
    elapsed_ms: u64,
    rows_inserted: u64,
    keys_ok_count: usize,
    keys_skipped_count: usize,
    /// Per-key error strings, formatted as `"<key>: <error>"`.
    errors: Vec<String>,
    /// Set when the whole job aborted early (e.g. AWS credential failure).
    fatal_error: Option<String>,
}

/// Shared registry of in-progress backfill jobs.
///
/// Cheaply cloneable — backed by an [`Arc`] so all clones share the same state.
#[derive(Clone)]
pub struct BackfillTracker(Arc<Mutex<Inner>>);

/// A snapshot of one active backfill job, suitable for JSON serialisation.
#[derive(Debug, Clone)]
pub struct BackfillSnapshot {
    pub id: u64,
    pub source: String,
    /// All partition keys remaining to be processed (claimed but not yet released).
    pub ongoing: Vec<String>,
    pub started_at: String,
    pub elapsed_ms: u64,
}

/// A snapshot of one completed backfill job, suitable for JSON serialisation.
#[derive(Debug, Clone)]
pub struct PostmortemSnapshot {
    pub source: String,
    pub started_at: String,
    pub completed_at: String,
    pub elapsed_ms: u64,
    pub rows_inserted: u64,
    pub keys_ok_count: usize,
    pub keys_skipped_count: usize,
    /// Per-key errors, formatted as `"<key>: <error message>"`.
    pub errors: Vec<String>,
    /// Set when the whole job aborted (e.g. AWS credential error).
    pub fatal_error: Option<String>,
}

/// RAII guard that automatically unregisters a job when dropped.
/// Guarantees the tracker is always cleaned up, even on early returns.
pub struct BackfillGuard<'a> {
    tracker: &'a BackfillTracker,
    pub id: u64,
}

impl Drop for BackfillGuard<'_> {
    fn drop(&mut self) {
        self.tracker.unregister(self.id);
    }
}

impl BackfillTracker {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(Inner {
            next_id: 0,
            active: Vec::new(),
            history: VecDeque::new(),
        })))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        match self.0.lock() {
            Ok(g) => g,
            Err(p) => p.into_inner(),
        }
    }

    /// Register a new job and return a guard that unregisters it on drop.
    pub fn register<'a>(&'a self, source: impl Into<String>) -> BackfillGuard<'a> {
        let mut inner = self.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.active.push(ActiveBackfillJob {
            id,
            source: source.into(),
            ongoing: Vec::new(),
            started_at: Utc::now(),
        });
        BackfillGuard { tracker: self, id }
    }

    /// Atomically check whether `key` is already claimed by another job for the
    /// same source, and if not, add it to the ongoing set for job `id`.
    ///
    /// Returns `true` if the key was claimed (caller should proceed).
    /// Returns `false` if another job is already processing this key (caller
    /// should skip it).
    pub fn try_claim_key(&self, id: u64, source: &str, key: &str) -> bool {
        let mut inner = self.lock();
        let already_claimed = inner.active.iter().any(|j| {
            j.id != id
                && j.source == source
                && j.ongoing.iter().any(|k| k == key)
        });
        if already_claimed {
            return false;
        }
        if let Some(job) = inner.active.iter_mut().find(|j| j.id == id) {
            job.ongoing.push(key.to_owned());
        }
        true
    }

    /// Remove `key` from the ongoing set once processing is complete.
    pub fn release_key(&self, id: u64, key: &str) {
        let mut inner = self.lock();
        if let Some(job) = inner.active.iter_mut().find(|j| j.id == id) {
            job.ongoing.retain(|k| k != key);
        }
    }

    fn unregister(&self, id: u64) {
        self.lock().active.retain(|j| j.id != id);
    }

    /// Record the outcome of a completed backfill job in the postmortem history.
    ///
    /// `result` is the value returned by [`run_backfill`].  The history is
    /// bounded at [`MAX_HISTORY`] entries — oldest entries are evicted first.
    pub fn record_completion(&self, source: &str, result: &Result<BackfillStats, String>) {
        let completed_at = Utc::now();
        let (stats_ref, fatal_error) = match result {
            Ok(s) => (Some(s), None),
            Err(msg) => (None, Some(msg.clone())),
        };

        let elapsed_ms = stats_ref.map(|s| s.elapsed_ms).unwrap_or(0);
        let started_at = completed_at
            - chrono::Duration::milliseconds(elapsed_ms as i64);

        let entry = CompletedBackfillJob {
            source: source.to_owned(),
            started_at,
            completed_at,
            elapsed_ms,
            rows_inserted: stats_ref.map(|s| s.rows_inserted).unwrap_or(0),
            keys_ok_count: stats_ref.map(|s| s.keys_ok.len()).unwrap_or(0),
            keys_skipped_count: stats_ref.map(|s| s.keys_skipped.len()).unwrap_or(0),
            errors: stats_ref.map(|s| s.keys_err.clone()).unwrap_or_default(),
            fatal_error,
        };

        let mut inner = self.lock();
        if inner.history.len() >= MAX_HISTORY {
            inner.history.pop_front();
        }
        inner.history.push_back(entry);
    }

    /// Return completed job history, most recent first (up to [`MAX_HISTORY`] entries).
    pub fn list_postmortem(&self) -> Vec<PostmortemSnapshot> {
        self.lock()
            .history
            .iter()
            .rev()
            .map(|j| PostmortemSnapshot {
                source: j.source.clone(),
                started_at: j.started_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                completed_at: j.completed_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                elapsed_ms: j.elapsed_ms,
                rows_inserted: j.rows_inserted,
                keys_ok_count: j.keys_ok_count,
                keys_skipped_count: j.keys_skipped_count,
                errors: j.errors.clone(),
                fatal_error: j.fatal_error.clone(),
            })
            .collect()
    }

    /// Return a snapshot of all currently active jobs. Empty when idle.
    pub fn list(&self) -> Vec<BackfillSnapshot> {
        self.lock()
            .active
            .iter()
            .map(|j| {
                let now = Utc::now();
                let elapsed_ms = (now - j.started_at).num_milliseconds().max(0) as u64;
                BackfillSnapshot {
                    id: j.id,
                    source: j.source.clone(),
                    ongoing: j.ongoing.clone(),
                    started_at: j.started_at.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    elapsed_ms,
                }
            })
            .collect()
    }
}
