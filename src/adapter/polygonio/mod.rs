pub mod stock;

pub use stock::run_polygonio_stock;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::AnySignalResult;
    pub const SOURCE: &str = "Polygon";
    pub const AGENT: &str = "Signals";
}
