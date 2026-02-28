use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};

struct Inner {
    next_id: u64,
    active: Vec<ActiveBackfillJob>,
}

#[derive(Clone)]
struct ActiveBackfillJob {
    id: u64,
    source: String,
    ongoing: Vec<String>,
    started_at: DateTime<Utc>,
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
