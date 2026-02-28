use std::sync::{Arc, Mutex};
use std::time::Instant;

struct Inner {
    next_id: u64,
    active: Vec<ActiveBackfillJob>,
}

#[derive(Clone)]
struct ActiveBackfillJob {
    id: u64,
    source: String,
    current_key: Option<String>,
    keys_done: usize,
    keys_total: usize,
    started_at: Instant,
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
    pub current_key: Option<String>,
    pub keys_done: usize,
    pub keys_total: usize,
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
    pub fn register<'a>(&'a self, source: impl Into<String>, keys_total: usize) -> BackfillGuard<'a> {
        let mut inner = self.lock();
        let id = inner.next_id;
        inner.next_id += 1;
        inner.active.push(ActiveBackfillJob {
            id,
            source: source.into(),
            current_key: None,
            keys_done: 0,
            keys_total,
            started_at: Instant::now(),
        });
        BackfillGuard { tracker: self, id }
    }

    /// Atomically check whether `key` is already claimed by another job for the
    /// same source, and if not, set it as the current key for job `id`.
    ///
    /// Returns `true` if the key was claimed (caller should proceed).
    /// Returns `false` if another job is already processing this key (caller
    /// should skip it).
    ///
    /// The check and set happen under one lock acquisition to prevent races
    /// between concurrent backfill requests.
    pub fn try_claim_key(&self, id: u64, source: &str, key: &str, keys_done: usize) -> bool {
        let mut inner = self.lock();
        let already_claimed = inner.active.iter().any(|j| {
            j.id != id
                && j.source == source
                && j.current_key.as_deref() == Some(key)
        });
        if already_claimed {
            return false;
        }
        if let Some(job) = inner.active.iter_mut().find(|j| j.id == id) {
            job.current_key = Some(key.to_owned());
            job.keys_done = keys_done;
        }
        true
    }

    fn unregister(&self, id: u64) {
        self.lock().active.retain(|j| j.id != id);
    }

    /// Return a snapshot of all currently active jobs. Empty when idle.
    pub fn list(&self) -> Vec<BackfillSnapshot> {
        self.lock()
            .active
            .iter()
            .map(|j| BackfillSnapshot {
                id: j.id,
                source: j.source.clone(),
                current_key: j.current_key.clone(),
                keys_done: j.keys_done,
                keys_total: j.keys_total,
                elapsed_ms: j.started_at.elapsed().as_millis() as u64,
            })
            .collect()
    }
}
