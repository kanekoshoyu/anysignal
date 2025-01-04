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
    // - fear and greed index
    // - memecoin price
    // - bitcoin index

    loop {
        println!("Hello, world!");
        // dispatch request
        let url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest";
        reqwest::Request::new(Method::GET, Url::from_str(url).map_err(|i| eyre!(i))?);

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
