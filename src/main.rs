use futures::future::join_all;
use futures::TryFutureExt;
use reqwest;
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
        let handle = tokio::spawn(
            async move { host_rest_api_server().await }.map_err(|i| SignalsError::from(i)),
        );
        runners.push(handle);
    }

    if config.has_runner("coinmarketcap") {
        println!("Starting CoinMarketCap indexer");
        let period = tokio::time::Duration::from_secs(10);
        let handle = tokio::spawn(async move { run_coinmarket_cap(config.clone(), period).await });
        runners.push(handle);
    }

    for result in join_all(runners).await {
        if let Err(e) = result {
            println!("error: {:?}", e);
        };
    }
    Ok(())
}

// TODO set up the global source to replace the config
async fn run_coinmarket_cap(config: Config, period: tokio::time::Duration) -> Result<()> {
    let mut interval = tokio::time::interval(period);

    // set up coinmarket cap fetchers
    let client = reqwest::Client::new();

    // historic listing
    let url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest";
    // fear and greed
    let url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical";
    // bitcoin dominance
    let url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest";

    let key_cmc = config.get_api_key("coinmarketcap")?;

    loop {
        interval.tick().await; // Wait for the next tick

        println!("Fetching coinmarket cap:");
        // dispatch request

        let response = client
            .get(url)
            .header("X-CMC_PRO_API_KEY", &key_cmc)
            .send()
            .await
            .unwrap();
        let text = response.text().await.unwrap();
        println!("{:?}", text);
    }
}
