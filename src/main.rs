use std::str::FromStr;
use reqwest::{self, Method, Url};
use signals::error::Result;

#[tokio::main]
async fn main() -> Result<> {
    // optional config file that stores API keys

    // set up config to load API tokens
    let config = config::Config::default();


    // set up coinmarket cap fetchers
    // - fear and greed index
    // - memecoin price
    // - bitcoin index

    loop {
        println!("Hello, world!");
        // dispatch request
        let url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest";
        reqwest::Request::new(Method::GET, Url::from_str(url)?);

        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
