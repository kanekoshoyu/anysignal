use eyre::eyre;
use reqwest::{self, Method, Url};
use std::str::FromStr;

use signals::config::Config;
use signals::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // optional config file that stores API keys

    // set up config to load API tokens
    let config: Config = Config::from_path("config.toml")?;
    println!("Config: {:?}", config);
    // set up coinmarket cap fetchers

    let key_cmc = config.get_key("coinmarketcap")?;
    // historic listing
    let url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest";

    // fear and greed
    let url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical";

    // bitcoin dominance
    let url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest";

    let client = reqwest::Client::new();

    loop {
        println!("Hello, world!");
        // dispatch request

        let response = client
            .get(url)
            .header("X-CMC_PRO_API_KEY", &key_cmc)
            .send()
            .await
            .unwrap();
        let text = response.text().await.unwrap();
        println!("{:?}", text);
        tokio::time::interval(std::time::Duration::from_secs(1))
            .tick()
            .await;
    }
}
