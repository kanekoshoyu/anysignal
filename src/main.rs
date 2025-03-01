use anysignal::adapter::coinmarketcap::fear_and_greed::FearAndGreedSignalSource;
use anysignal::adapter::coinmarketcap::prelude::PollingSignalSource;
use anysignal::adapter::newsapi::run_news_fetcher;
use anysignal::adapter::polygonio::run_polygonio_stock;
use anysignal::api::host_rest_api_server;
use anysignal::config::Config;
use anysignal::error::{AnySignalError, AnySignalResult};
use futures::future::join_all;
use futures::TryFutureExt;
use tokio::task::JoinHandle;

/// load environment and manages runner at this level
#[tokio::main]
async fn main() -> AnySignalResult<()> {
    // set up config to load API tokens
    let config: Config = Config::from_path("config.toml")?;
    println!("Config: {:#?}", config);

    // each runner returns signals::error::Result<()>
    let mut runners: Vec<JoinHandle<AnySignalResult<()>>> = Vec::new();

    if config.has_runner("api") {
        println!("Starting API server");
        let handle =
            tokio::spawn(async move { host_rest_api_server().await }.map_err(AnySignalError::from));
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

    if config.has_runner("polygonio") {
        println!("Starting PolygonIO indexer");
        let config = config.clone();
        let api_key = config.get_api_key("polygonio")?;
        let handle = tokio::spawn(async move { run_polygonio_stock(api_key).await });
        runners.push(handle);
    }

    for result in join_all(runners).await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => println!("indexer error: {:?}", e),
            Err(e) => println!("join error: {:?}", e),
        }
    }
    println!("exit program gracefully");
    Ok(())
}
