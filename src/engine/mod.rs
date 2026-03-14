pub mod counter;
pub mod event;
pub mod market_state;
pub mod scheduler;

pub use counter::{EventCounters, EventCountSnapshot};
pub use event::{Event, LiqSide};
pub use market_state::MarketState;
pub use scheduler::MarketStateScheduler;

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tracing::{info, warn};

// ---------------------------------------------------------------------------
// Updates fed INTO the MarketEngine
// ---------------------------------------------------------------------------

/// Raw market data update consumed by [`MarketEngine::process`].
#[derive(Debug, Clone)]
pub enum Update {
    /// Consolidated asset context snapshot from the data source.
    ///
    /// Sets snapshot fields (`price_oracle`, `price_mark`, `price_mid`,
    /// `open_interest`, `funding_rate`, `trading_volume_24h_usd`).
    /// `price_mid` falls back to `(oracle + mark) / 2` when `None`.
    AssetContext {
        coin: String,
        oracle_px: f64,
        mark_px: f64,
        /// Best-bid / best-ask midpoint from the exchange, if available.
        mid_px: Option<f64>,
        open_interest: f64,
        funding_rate: f64,
        volume_24h_usd: f64,
    },

    /// A completed buy-side trade (increments rolling window counters).
    Trade {
        coin: String,
        /// Fill quantity in base units.
        volume: f64,
        /// Number of fills represented by this update (typically 1).
        count: u64,
    },

    /// A liquidation event (increments rolling window liquidation counters).
    Liquidation {
        coin: String,
        side: LiqSide,
        volume: f64,
    },

    /// Predicted next funding rate, polled once per minute from the exchange.
    /// Overwrites the previous value; not reset on window boundaries.
    PredictedFundingRate { coin: String, rate: f64 },

    /// Sent by the bridge after it has finished seeding all coins. The
    /// scheduler skips flushes until this is received.
    EngineReady,
}

impl Update {
    #[allow(dead_code)]
    pub fn coin(&self) -> Option<&str> {
        match self {
            Update::AssetContext { coin, .. } => Some(coin),
            Update::Trade { coin, .. } => Some(coin),
            Update::Liquidation { coin, .. } => Some(coin),
            Update::PredictedFundingRate { coin, .. } => Some(coin),
            Update::EngineReady => None,
        }
    }
}

// ---------------------------------------------------------------------------
// MarketEngineConfig
// ---------------------------------------------------------------------------

pub struct MarketEngineConfig {
    /// How often rolling window counters are reset (typically 60 seconds).
    pub window_duration: std::time::Duration,
}

// ---------------------------------------------------------------------------
// MarketEngine
// ---------------------------------------------------------------------------

pub struct MarketEngine {
    states: HashMap<String, MarketState>,
    tx: UnboundedSender<Event>,
    counters: Arc<EventCounters>,
}

impl MarketEngine {
    /// Create a new engine and return the event receiver and shared counters.
    pub fn new(_cfg: MarketEngineConfig) -> (Self, UnboundedReceiver<Event>, Arc<EventCounters>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let counters = Arc::new(EventCounters::default());
        (
            Self {
                states: HashMap::new(),
                tx,
                counters: counters.clone(),
            },
            rx,
            counters,
        )
    }

    /// Process a single market data update, mutating state in-place.
    pub fn process(&mut self, update: Update) {
        self.counters.increment_update();

        match &update {
            Update::AssetContext {
                coin,
                oracle_px,
                mark_px,
                mid_px,
                open_interest,
                funding_rate,
                volume_24h_usd,
            } => {
                let state = self.get_or_create(coin);
                state.price_oracle = Some(*oracle_px);
                state.price_mark = Some(*mark_px);
                state.price_mid = Some(mid_px.unwrap_or((oracle_px + mark_px) / 2.0));
                state.open_interest = Some(*open_interest);
                state.funding_rate = Some(*funding_rate);
                state.trading_volume_24h_usd = *volume_24h_usd;
                state.last_updated = Some(Utc::now());
            }

            Update::Trade { coin, volume, count } => {
                let state = self.get_or_create(coin);
                state.trading_volume += volume;
                state.trade_count += count;
                state.last_updated = Some(Utc::now());
            }

            Update::Liquidation { coin, side, volume } => {
                let state = self.get_or_create(coin);
                match side {
                    LiqSide::Short => {
                        state.short_liquidation_volume += volume;
                        state.short_liquidation_count += 1;
                    }
                    LiqSide::Long => {
                        state.lng_liquidation_volume += volume;
                        state.lng_liquidation_count += 1;
                    }
                }
                state.last_updated = Some(Utc::now());
            }

            Update::PredictedFundingRate { coin, rate } => {
                let state = self.get_or_create(coin);
                state.predicted_funding_rate = Some(*rate);
                state.last_updated = Some(Utc::now());
            }

            Update::EngineReady => {}
        }
    }

    /// Emit a full snapshot of all current coin states.
    pub fn snapshot(&self) {
        self.emit(Event::Snapshot {
            states: self.states.values().cloned().collect(),
            timestamp: Utc::now(),
        });
    }

    /// Read-only view of a coin's current state.
    pub fn get(&self, coin: &str) -> Option<&MarketState> {
        self.states.get(coin)
    }

    /// Iterator over all tracked coin states.
    pub fn all(&self) -> impl Iterator<Item = &MarketState> {
        self.states.values()
    }

    pub fn symbols(&self) -> Vec<String> {
        self.states.keys().cloned().collect()
    }

    /// Zero all per-coin rolling window counters and emit [`Event::WindowReset`].
    pub fn reset_all_windows(&mut self) {
        for state in self.states.values_mut() {
            state.reset_window();
        }
        info!(coins = self.states.len(), "market window reset");
        self.emit(Event::WindowReset { timestamp: Utc::now() });
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn get_or_create(&mut self, coin: &str) -> &mut MarketState {
        self.states
            .entry(coin.to_string())
            .or_insert_with(|| MarketState::new(coin))
    }

    fn emit(&self, event: Event) {
        self.counters.increment(&event);
        if self.tx.send(event).is_err() {
            warn!("market engine event channel closed, dropping event");
        }
    }
}
