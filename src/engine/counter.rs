use std::sync::atomic::{AtomicU64, Ordering};

use super::event::Event;

/// Per-variant event counters for the market engine.
///
/// All fields are [`AtomicU64`] so they can be incremented on the hot path
/// without locking. Values are approximate totals only (`Relaxed` ordering).
#[derive(Default)]
pub struct EventCounters {
    pub updates_processed: AtomicU64,
    pub window_reset: AtomicU64,
    pub snapshot: AtomicU64,
}

/// Plain serialisable snapshot of [`EventCounters`] values.
#[derive(serde::Serialize)]
pub struct EventCountSnapshot {
    pub updates_processed: u64,
    pub window_reset: u64,
    pub snapshot: u64,
}

impl EventCounters {
    /// Increment the `updates_processed` counter (called per [`super::Update`]).
    #[inline]
    pub fn increment_update(&self) {
        self.updates_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the counter for the given [`Event`] variant.
    #[inline]
    pub fn increment(&self, event: &Event) {
        let c = match event {
            Event::WindowReset { .. } => &self.window_reset,
            Event::Snapshot { .. } => &self.snapshot,
        };
        c.fetch_add(1, Ordering::Relaxed);
    }

    /// Read all counters atomically into a serialisable snapshot.
    pub fn snapshot(&self) -> EventCountSnapshot {
        EventCountSnapshot {
            updates_processed: self.updates_processed.load(Ordering::Relaxed),
            window_reset: self.window_reset.load(Ordering::Relaxed),
            snapshot: self.snapshot.load(Ordering::Relaxed),
        }
    }
}
