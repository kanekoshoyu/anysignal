pub mod form4;

pub use form4::run_live_closed_caption_fetcher;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::{AnySignalError, AnySignalResult};
    pub use crate::model::signal::*;
    pub const SOURCE: &str = "SEC API";
    pub const AGENT: &str = "Signals";
}
