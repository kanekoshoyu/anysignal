use super::prelude::*;

/// free tier
pub async fn run_signal_new_listing() -> AnySignalResult<()> {
    // historic listing
    todo!("implement new listing");
    // let url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest";
    // Ok(())
}

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize)]
pub struct CryptoData {
    pub data: Vec<CryptoCurrency>,
    pub status: Status,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CryptoCurrency {
    pub id: u64,
    pub name: String,
    pub symbol: String,
    pub slug: String,
    pub cmc_rank: Option<u64>,
    pub num_market_pairs: Option<u64>,
    pub circulating_supply: Option<f64>,
    pub total_supply: Option<f64>,
    pub max_supply: Option<f64>,
    pub infinite_supply: Option<bool>,
    pub last_updated: Option<String>,
    pub date_added: Option<String>,
    pub tags: Option<Vec<String>>,
    pub platform: Option<Platform>,
    pub self_reported_circulating_supply: Option<f64>,
    pub self_reported_market_cap: Option<f64>,
    pub quote: HashMap<String, Quote>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Platform {
    pub id: u64,
    pub name: String,
    pub symbol: String,
    pub slug: String,
    pub token_address: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Quote {
    pub price: f64,
    pub volume_24h: Option<f64>,
    pub volume_change_24h: Option<f64>,
    pub percent_change_1h: Option<f64>,
    pub percent_change_24h: Option<f64>,
    pub percent_change_7d: Option<f64>,
    pub market_cap: Option<f64>,
    pub market_cap_dominance: Option<f64>,
    pub fully_diluted_market_cap: Option<f64>,
    pub last_updated: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Status {
    pub timestamp: String,
    pub error_code: u64,
    pub error_message: Option<String>,
    pub elapsed: u64,
    pub credit_count: u64,
}
