pub mod btc_holding;

pub use btc_holding::run_polygonio_stock;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::AnySignalResult;
    pub const SOURCE: &str = "Microstrategy";
    pub const AGENT: &str = "Signals";
}
