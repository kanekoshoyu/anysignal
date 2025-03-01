pub mod live_closed_caption;

// pub use bitcoin_dominance::run_bitcoin_dominance;
pub use live_closed_caption::run_live_closed_caption_fetcher;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::{AnySignalError, AnySignalResult};
    pub use crate::model::signal::*;
    pub const SOURCE: &str = "YouTube";
    pub const AGENT: &str = "Signals";
}
