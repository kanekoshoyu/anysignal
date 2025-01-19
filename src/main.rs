use futures::future::join_all;
use futures::TryFutureExt;
use signals::adapter::coinmarketcap::fear_and_greed::FearAndGreedSignalSource;
use signals::adapter::coinmarketcap::prelude::PollingSignalSource;
use signals::adapter::newsapi::run_news_fetcher;
use signals::api::host_rest_api_server;
use signals::config::Config;
use signals::error::{Result, SignalsError};
use tokio::task::JoinHandle;

/// load environment and manages runner at this level
#[tokio::main]
async fn main() -> Result<()> {
    // set up config to load API tokens
    let config: Config = Config::from_path("config.toml")?;
    println!("Config: {:?}", config);

    // each runner returns signals::error::Result<()>
    let mut runners: Vec<JoinHandle<Result<()>>> = Vec::new();

    if config.has_runner("api") {
        println!("Starting API server");
        let handle =
            tokio::spawn(async move { host_rest_api_server().await }.map_err(SignalsError::from));
        runners.push(handle);
    }

    if config.has_runner("coinmarketcap") {
        println!("Starting CoinMarketCap indexer");
        let poll_duration = tokio::time::Duration::from_secs(10);
        let config = config.clone();
        let source =
            FearAndGreedSignalSource::new(config.get_api_key("coinmarketcap")?, poll_duration);
        let handle = tokio::spawn(async move { source.run_loop().await });
        runners.push(handle);
    }

    if config.has_runner("newsapi") {
        println!("Starting NewsAPI indexer");
        let period = tokio::time::Duration::from_secs(10);
        let config = config.clone();

        let handle = tokio::spawn(async move { run_news_fetcher(config.clone(), period).await });
        runners.push(handle);
    }

    for result in join_all(runners).await {
        if let Err(e) = result {
            println!("error: {:?}", e);
        };
    }
    Ok(())
}
