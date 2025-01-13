mod keyword;

// pub use bitcoin_dominance::run_bitcoin_dominance;
pub use keyword::run_news_fetcher;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::Result;
    pub use crate::model::signal::SignalInfo;
    pub const SOURCE: &str = "NewsAPI";
    pub const AGENT: &str = "Signals";
}
