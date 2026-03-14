use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Aggregated market state for a single coin.
///
/// Snapshot fields (`price_*`, `open_interest`, `funding_rate`,
/// `trading_volume_24h_usd`) persist across window resets.
/// Rolling-window counters (`trading_volume`, `trade_count`,
/// liquidation fields) are zeroed by [`MarketState::reset_window`]
/// at the end of each 1-minute bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketState {
    pub coin: String,

    // --- Snapshots (carried forward, never reset) ---
    pub price_mid: Option<f64>,
    pub price_oracle: Option<f64>,
    pub price_mark: Option<f64>,
    pub open_interest: Option<f64>,
    pub funding_rate: Option<f64>,
    /// 24-hour USD trading volume from the data source snapshot.
    pub trading_volume_24h_usd: f64,
    /// Predicted next funding rate, polled every minute from the exchange.
    pub predicted_funding_rate: Option<f64>,

    // --- Rolling window counters (reset each minute) ---
    /// Sum of buy-side fill quantities in the current window.
    pub trading_volume: f64,
    /// Count of buy-side fills in the current window.
    pub trade_count: u64,
    pub short_liquidation_volume: f64,
    pub short_liquidation_count: u64,
    pub lng_liquidation_volume: f64,
    pub lng_liquidation_count: u64,

    pub last_updated: Option<DateTime<Utc>>,
}

impl MarketState {
    pub fn new(coin: impl Into<String>) -> Self {
        Self {
            coin: coin.into(),
            ..Default::default()
        }
    }

    /// Zero all rolling window counters. Snapshot fields are unchanged.
    pub fn reset_window(&mut self) {
        self.trading_volume = 0.0;
        self.trade_count = 0;
        self.short_liquidation_volume = 0.0;
        self.short_liquidation_count = 0;
        self.lng_liquidation_volume = 0.0;
        self.lng_liquidation_count = 0;
    }
}
