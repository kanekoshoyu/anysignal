use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::market_state::MarketState;

/// Events emitted by the [`super::MarketEngine`] via an unbounded MPSC channel.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Event {
    /// Emitted when the rolling window is reset. The scheduler uses this as
    /// the flush signal before zeroing per-window counters.
    WindowReset { timestamp: DateTime<Utc> },

    /// Periodic full snapshot of all tracked coin states.
    Snapshot {
        states: Vec<MarketState>,
        timestamp: DateTime<Utc>,
    },
}

/// Which side a liquidation occurred on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LiqSide {
    Long,
    Short,
}
