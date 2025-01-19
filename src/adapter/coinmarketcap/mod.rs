pub mod bitcoin_dominance;
pub mod fear_and_greed;
pub mod new_listing;

// pub use bitcoin_dominance::run_bitcoin_dominance;
pub use bitcoin_dominance::run_bitcoin_dominance;
pub use fear_and_greed::runner_store_signal_fear_and_greed_index;
pub use new_listing::run_signal_new_listing;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::Result;
    pub use crate::model::signal::SignalInfo;
    pub const KEY: &str = "X-CMC_PRO_API_KEY";
    pub const SOURCE: &str = "CoinMarketCap";
}
