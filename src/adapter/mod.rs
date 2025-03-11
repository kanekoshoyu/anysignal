// CoinMarketCap
pub mod coinmarketcap;
// DEX Screener
pub mod dexscreener;
// NewsAPI
pub mod newsapi;
// YouTube
pub mod youtube;
// MicroStrategy
pub mod microstrategy;
// adapter error
pub mod error;
// PolygonIO
pub mod polygonio;

pub use error::{AdapterError, AdapterResult};

#[derive(Clone, Debug)]
pub enum DataSourceType {
    Historic,
    Stream,
}

#[async_trait::async_trait]
pub trait DataSource {
    type DataType;
    /// arbitrary id that identified data source
    /// it can be generative but has to be replicable
    fn id() -> String;
    /// source type
    fn data_source_type() -> DataSourceType;
}

#[async_trait::async_trait]
pub trait HistoricDataSource: DataSource {
    async fn fetch() -> AdapterResult<Self::DataType>;
}
