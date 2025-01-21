pub mod bitcoin_dominance;
pub mod fear_and_greed;
pub mod new_listing;

// pub use bitcoin_dominance::run_bitcoin_dominance;
pub use bitcoin_dominance::run_bitcoin_dominance;
pub use new_listing::run_signal_new_listing;

pub mod prelude {
    pub use crate::config::Config;
    use crate::database::insert_signal_db;
    pub use crate::error::Result;
    pub use crate::model::signal::*;
    pub const KEY: &str = "X-CMC_PRO_API_KEY";
    pub const SOURCE: &str = "CoinMarketCap";
    use questdb::ingress::Sender;
    pub use serde::{Deserialize, Serialize};
    pub use std::collections::HashMap;
    pub use tokio::time::Duration;

    /// set up polling signal source trait
    pub trait PollingSignalSource {
        fn get_signal_info(&self) -> SignalInfo;

        fn poll_interval_duration(&self) -> Duration;

        fn get_signals(&self) -> impl std::future::Future<Output = Result<Vec<Signal>>> + Send;

        // this gets abtracted out as a PollingSignalSource
        fn run_loop(&self) -> impl std::future::Future<Output = Result<()>> + Send
        where
            Self: Sync,
        {
            async {
                let mut interval = tokio::time::interval(self.poll_interval_duration());
                let signal_info = self.get_signal_info();
                let mut sender = Sender::from_conf("http::addr=localhost:9000;").unwrap();
                loop {
                    let signals = self.get_signals().await?;
                    insert_signal_db(&mut sender, &signal_info, &signals)?;
                    interval.tick().await;
                }
            }
        }
    }
}
