use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::SelectAll;
use guilder_abstraction::{AssetContext, BoxStream, Fill, GetMarketData, SubscribeMarketData};
use guilder_client_hyperliquid::HyperliquidClient;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::mpsc::UnboundedSender;
use tokio::time;
use tracing::warn;

use crate::engine::Update;

/// How often to re-fetch the symbol list to pick up newly listed coins.
const SYMBOL_REFETCH_INTERVAL: Duration = Duration::from_secs(60);

/// How often to poll for predicted funding rates.
const PREDICTED_FUNDING_INTERVAL: Duration = Duration::from_secs(60);

/// Only use the native Hyperliquid perp venue for predicted funding.
const HL_VENUE: &str = "HlPerp";

/// Market data event variants used inside the bridge run loop.
enum Ev {
    Fill(Fill),
    AssetCtx(AssetContext),
}

/// Bridges Hyperliquid WebSocket market data into the [`MarketStateScheduler`]
/// via an [`UnboundedSender<Update>`].
///
/// On [`run`], the bridge:
/// 1. Calls `get_all_asset_contexts()` to fetch all perp snapshots in one request.
/// 2. Seeds each coin in the engine from that snapshot.
/// 3. Opens `subscribe_fill` and `subscribe_asset_context` streams per coin.
/// 4. Dispatches incoming events as [`Update`]s to the engine channel.
/// 5. Polls predicted funding rates every 60 s.
/// 6. Re-fetches the symbol list every 60 s to subscribe to newly listed coins.
///
/// [`run`]: GuilderBridge::run
pub struct GuilderBridge {
    client: HyperliquidClient,
    tx: UnboundedSender<Update>,
}

impl GuilderBridge {
    pub fn new(tx: UnboundedSender<Update>) -> Self {
        Self {
            client: HyperliquidClient::new(),
            tx,
        }
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    fn send(&self, update: Update) -> bool {
        self.tx.send(update).is_ok()
    }

    fn d2f(d: rust_decimal::Decimal) -> f64 {
        d.to_f64().unwrap_or(0.0)
    }

    fn on_fill(&self, fill: Fill) {
        self.send(Update::Trade {
            coin: fill.symbol,
            volume: Self::d2f(fill.volume),
            count: 1,
        });
    }

    fn on_asset_context(&self, ctx: AssetContext) {
        let mark_px = Self::d2f(ctx.mark_price);
        self.send(Update::AssetContext {
            coin: ctx.symbol,
            oracle_px: ctx.oracle_price.map(Self::d2f).unwrap_or(mark_px),
            mark_px,
            mid_px: ctx.mid_price.map(Self::d2f),
            open_interest: Self::d2f(ctx.open_interest),
            funding_rate: Self::d2f(ctx.funding_rate),
            volume_24h_usd: Self::d2f(ctx.day_volume),
        });
    }

    async fn fetch_predicted_fundings(&self) {
        match self.client.get_predicted_fundings().await {
            Ok(fundings) => {
                let mut seen = std::collections::HashSet::new();
                let mut count = 0usize;
                for pf in fundings {
                    if pf.venue != HL_VENUE || !seen.insert(pf.symbol.clone()) {
                        continue;
                    }
                    let rate = match pf.funding_rate.to_string().parse::<f64>() {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(coin = %pf.symbol, "failed to parse predicted funding rate: {e}");
                            continue;
                        }
                    };
                    if !self.send(Update::PredictedFundingRate { coin: pf.symbol, rate }) {
                        return; // scheduler dropped
                    }
                    count += 1;
                }
                tracing::debug!(coins = count, "predicted funding rates refreshed");
            }
            Err(e) => warn!("get_predicted_fundings failed: {e}"),
        }
    }

    /// Fetch all symbols, seed the engine with REST snapshots, subscribe to
    /// streams for each coin, and return the list of seeded symbols.
    async fn initialize(&self, streams: &mut SelectAll<BoxStream<Ev>>) -> Vec<String> {
        let ctxs = match self.client.get_all_asset_contexts().await {
            Ok(c) => c,
            Err(e) => {
                warn!("get_all_asset_contexts failed: {e}");
                return Vec::new();
            }
        };
        tracing::info!(total = ctxs.len(), "guilder bridge: fetching initial snapshots");

        let mut seeded: Vec<String> = Vec::new();
        for ctx in ctxs {
            let symbol = ctx.symbol.clone();
            self.on_asset_context(ctx);
            seeded.push(symbol);
        }

        // Fetch predicted funding rates before opening streams so the engine
        // is fully seeded by the time the first minute flush fires.
        self.fetch_predicted_fundings().await;

        for symbol in &seeded {
            streams.push(Box::pin(
                self.client.subscribe_fill(symbol.clone()).map(Ev::Fill),
            ));
            streams.push(Box::pin(
                self.client
                    .subscribe_asset_context(symbol.clone())
                    .map(Ev::AssetCtx),
            ));
        }

        tracing::info!(symbols = seeded.len(), "guilder bridge initialised");
        seeded
    }

    /// Re-fetch the symbol list and subscribe to any coins not already tracked.
    async fn refetch_symbols(
        &self,
        known: &mut Vec<String>,
        streams: &mut SelectAll<BoxStream<Ev>>,
    ) {
        let all = match self.client.get_symbol().await {
            Ok(s) => s,
            Err(e) => {
                warn!("symbol refetch: get_symbol failed: {e}");
                return;
            }
        };

        let new_symbols: Vec<String> =
            all.into_iter().filter(|s| !known.contains(s)).collect();
        if new_symbols.is_empty() {
            return;
        }

        tracing::info!(count = new_symbols.len(), "symbol refetch: new coins detected");

        for symbol in &new_symbols {
            match self.client.get_asset_context(symbol.clone()).await {
                Ok(ctx) => self.on_asset_context(ctx),
                Err(e) => {
                    warn!(symbol = symbol.as_str(), "symbol refetch: get_asset_context failed: {e}");
                    continue;
                }
            }
            streams.push(Box::pin(
                self.client.subscribe_fill(symbol.clone()).map(Ev::Fill),
            ));
            streams.push(Box::pin(
                self.client
                    .subscribe_asset_context(symbol.clone())
                    .map(Ev::AssetCtx),
            ));
            known.push(symbol.clone());
        }
    }

    // -----------------------------------------------------------------------
    // Public entry point
    // -----------------------------------------------------------------------

    /// Run the bridge loop until all WS streams close or the engine channel drops.
    ///
    /// Exits when `streams.next()` returns `None` (all subscriptions ended).
    /// The caller is responsible for restarting if reconnect is desired.
    pub async fn run(self) {
        let mut streams: SelectAll<BoxStream<Ev>> = SelectAll::new();
        let mut known = self.initialize(&mut streams).await;
        // Signal to the scheduler that the engine is fully seeded.
        let _ = self.tx.send(Update::EngineReady);

        let mut symbol_tick = time::interval_at(
            time::Instant::now() + SYMBOL_REFETCH_INTERVAL,
            SYMBOL_REFETCH_INTERVAL,
        );
        let mut funding_tick = time::interval_at(
            time::Instant::now() + PREDICTED_FUNDING_INTERVAL,
            PREDICTED_FUNDING_INTERVAL,
        );

        loop {
            tokio::select! {
                biased;
                maybe_ev = streams.next() => {
                    let Some(ev) = maybe_ev else {
                        warn!("guilder bridge: all streams closed");
                        break;
                    };
                    if self.tx.is_closed() { break; }
                    match ev {
                        Ev::Fill(f)     => self.on_fill(f),
                        Ev::AssetCtx(c) => self.on_asset_context(c),
                    }
                }
                _ = symbol_tick.tick() => {
                    self.refetch_symbols(&mut known, &mut streams).await;
                }
                _ = funding_tick.tick() => {
                    self.fetch_predicted_fundings().await;
                }
            }
        }
    }
}
