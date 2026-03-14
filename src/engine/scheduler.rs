use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::time::MissedTickBehavior;
use tracing::info;

use crate::database::{insert_market_state_rt_1m, MarketStateRt1mRow, QuestDbClient};
use crate::error::AnySignalResult;

use super::counter::EventCounters;
use super::event::Event;
use super::{MarketEngine, MarketEngineConfig, MarketState, Update};

/// Drives the [`MarketEngine`]: receives [`Update`]s from any data source,
/// flushes a completed `market_state_1m` row per coin to QuestDB every minute,
/// then resets the rolling window counters.
///
/// The scheduler is exchange-agnostic — it only knows about [`Update`] and
/// [`MarketState`]. All exchange-specific adapters (WebSocket bridges, REST
/// pollers) live in `src/adapter/` and send updates via [`Self::update_sender`].
///
/// # Timing
/// On [`Self::run`], the scheduler sleeps until the next wall-clock minute
/// boundary before starting the 60-second interval, keeping bucket timestamps
/// well-aligned regardless of when the process starts.
///
/// # Readiness
/// Flushes are skipped until [`Self::mark_ready`] is called. This prevents
/// writing partial rows before the bridge has finished seeding the engine.
///
/// # Feeding updates
/// Call [`Self::update_sender`] to obtain a cloneable [`UnboundedSender<Update>`]
/// that can be passed to any adapter task. The scheduler keeps one sender alive
/// internally so the channel stays open until the scheduler is dropped.
pub struct MarketStateScheduler {
    engine: MarketEngine,
    db: Arc<QuestDbClient>,
    update_rx: UnboundedReceiver<Update>,
    /// Kept alive so the channel is not closed until the scheduler is dropped.
    _update_tx: UnboundedSender<Update>,
    #[allow(dead_code)]
    counters: Arc<EventCounters>,
    /// Set by [`Self::mark_ready`] once the bridge has finished initialising.
    ready: bool,
}

impl MarketStateScheduler {
    /// Build a scheduler. Returns the scheduler and an event receiver for
    /// optional downstream consumers.
    pub fn new(db: Arc<QuestDbClient>) -> (Self, UnboundedReceiver<Event>) {
        use tokio::sync::mpsc;

        let (update_tx, update_rx) = mpsc::unbounded_channel::<Update>();
        let (engine, event_rx, counters) = MarketEngine::new(MarketEngineConfig {
            window_duration: Duration::from_secs(60),
        });

        let scheduler = Self {
            engine,
            db,
            update_rx,
            _update_tx: update_tx,
            counters,
            ready: false,
        };

        (scheduler, event_rx)
    }

    /// Clone the internal sender so an adapter task can push [`Update`]s into
    /// the engine.
    pub fn update_sender(&self) -> UnboundedSender<Update> {
        self._update_tx.clone()
    }

    /// Signal that the engine is fully seeded. Flushes will be skipped until
    /// this is called.
    pub fn mark_ready(&mut self) {
        self.ready = true;
    }

    /// Run the scheduler loop.
    ///
    /// - Sleeps until the next minute boundary (wall-clock alignment).
    /// - Then ticks every 60 s: flushes the current window to QuestDB and
    ///   resets rolling counters (skipped until [`Self::mark_ready`] is called).
    /// - Incoming [`Update`]s are processed immediately as they arrive.
    ///
    /// Runs until the update channel is closed (all senders dropped).
    pub async fn run(mut self) -> AnySignalResult<()> {
        // Align to the next wall-clock minute boundary.
        let now = Utc::now();
        let ms_into_minute = now.timestamp_millis() % 60_000;
        let ms_until_next = (60_000 - ms_into_minute) as u64;
        info!(ms_until_next, "market scheduler aligning to next minute boundary");
        tokio::time::sleep(Duration::from_millis(ms_until_next)).await;

        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                maybe_update = self.update_rx.recv() => {
                    match maybe_update {
                        Some(Update::EngineReady) => {
                            info!("market engine ready, flushes enabled");
                            self.mark_ready();
                        }
                        Some(update) => self.engine.process(update),
                        None => {
                            info!("market engine update channel closed, shutting down scheduler");
                            break;
                        }
                    }
                }
                _ = interval.tick() => {
                    if let Err(e) = self.flush_window() {
                        tracing::error!(error = ?e, "market_state_1m flush failed");
                    }
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// Snapshot the current window, write rows to QuestDB, then reset counters.
    /// No-op until [`Self::mark_ready`] has been called.
    fn flush_window(&mut self) -> AnySignalResult<()> {
        if !self.ready {
            info!("market engine not yet ready, skipping flush");
            return Ok(());
        }

        let now = Utc::now();
        // Bucket is the minute that just completed: floor(now) - 1 min.
        let bucket_ms = (now.timestamp_millis() / 60_000 - 1) * 60_000;

        let rows: Vec<MarketStateRt1mRow> = self
            .engine
            .all()
            .filter_map(|s| state_to_rt_row(s, bucket_ms))
            .collect();

        let count = rows.len();
        if !rows.is_empty() {
            self.db.with_sender(|s| insert_market_state_rt_1m(s, &rows))?;
        }

        self.engine.reset_all_windows();

        info!(bucket_ms, coins = count, "market_state_rt_1m flushed");
        Ok(())
    }
}

/// Convert a [`MarketState`] snapshot into a [`MarketStateRt1mRow`] for the given
/// minute bucket. Returns `None` when the state has no mark price yet (engine
/// not yet seeded for this coin).
fn state_to_rt_row(state: &MarketState, bucket_ms: i64) -> Option<MarketStateRt1mRow> {
    let price_mark = state.price_mark?;
    let price_mid = state.price_mid.unwrap_or(price_mark);

    Some(MarketStateRt1mRow {
        minute_ms: bucket_ms,
        coin: state.coin.clone(),
        price_oracle: state.price_oracle,
        price_mark,
        price_mid,
        open_interest: state.open_interest.unwrap_or(0.0),
        funding_rate: state.funding_rate.unwrap_or(0.0),
        volume_24h_usd: state.trading_volume_24h_usd,
        predicted_funding_rate: state.predicted_funding_rate,
        trade_volume: state.trading_volume,
        trade_count: state.trade_count as i64,
        liquidation_long_volume: state.lng_liquidation_volume,
        liquidation_short_volume: state.short_liquidation_volume,
        liquidation_long_count: state.lng_liquidation_count as i64,
        liquidation_short_count: state.short_liquidation_count as i64,
    })
}
