/*
implementation of AWS S3 fetcher for hyperkiquid historic data
https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
*/

pub mod asset_ctxs;
pub mod explorer_blocks;
pub mod market_data;
pub mod node_fills_by_block;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::{AnySignalError, AnySignalResult};
    pub use crate::model::signal::*;
    pub const SOURCE: &str = "HYPERLIQUID_S3";
    pub const AGENT: &str = "Signals";
}
