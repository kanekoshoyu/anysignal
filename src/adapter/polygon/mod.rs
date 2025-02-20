pub mod client;

pub use client::websocket_client;

pub mod prelude {
    pub use crate::config::Config;
    pub use crate::error::Result;
    pub const SOURCE: &str = "Polygon";
    pub const AGENT: &str = "Signals";
}
