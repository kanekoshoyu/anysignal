use std::time::Duration;

use guilder_abstraction::GetMarketData;
use guilder_client_hyperliquid::HyperliquidClient;
use tokio::sync::mpsc::UnboundedSender;
use tracing::warn;

use crate::engine::Update;

const HL_VENUE: &str = "HlPerp";

/// Poll predicted funding rates via `guilder-abstraction`'s [`GetMarketData`]
/// trait on a fixed interval and forward one [`Update::PredictedFundingRate`]
/// per coin (HlPerp venue only) to the market engine via `tx`.
///
/// Sends immediately on entry so the engine is seeded before the first flush.
/// Runs until `tx` is closed (scheduler dropped).
pub async fn run_predicted_funding_poller(tx: UnboundedSender<Update>, interval: Duration) {
    let client = HyperliquidClient::new();

    fetch_and_send(&client, &tx).await;

    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        ticker.tick().await;
        if tx.is_closed() {
            break;
        }
        fetch_and_send(&client, &tx).await;
    }
}

async fn fetch_and_send(client: &HyperliquidClient, tx: &UnboundedSender<Update>) {
    match client.get_predicted_fundings().await {
        Ok(fundings) => {
            let mut count = 0;
            for pf in fundings {
                if pf.venue != HL_VENUE {
                    continue;
                }
                // Decimal → f64 via string round-trip (avoids a direct rust_decimal dep).
                let rate = match pf.funding_rate.to_string().parse::<f64>() {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(coin = %pf.symbol, error = %e, "failed to parse predicted funding rate");
                        continue;
                    }
                };
                if tx
                    .send(Update::PredictedFundingRate {
                        coin: pf.symbol,
                        rate,
                    })
                    .is_err()
                {
                    return; // scheduler dropped
                }
                count += 1;
            }
            tracing::debug!(coins = count, "predicted funding rates refreshed");
        }
        Err(e) => {
            warn!(error = %e, "failed to fetch predicted funding rates");
        }
    }
}
