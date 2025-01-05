use super::prelude::*;

// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal

pub async fn run_signal_fear_and_index(
    config: Config,
    period: tokio::time::Duration,
) -> Result<()> {
    let signal = SignalInfo {
        id: 0,
        source: SOURCE.to_string(),
        description: "Fear and Greed Index".to_string(),
        is_atomic: true,
    };
    let mut interval = tokio::time::interval(period);
    let client = reqwest::Client::new();
    let url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical";

    let key_cmc = config.get_api_key("coinmarketcap")?;
    loop {
        interval.tick().await; // Wait for the next tick

        println!("Fetching coinmarket cap:");
        // dispatch request

        let response = client.get(url).header(KEY, &key_cmc).send().await.unwrap();
        let text = response.text().await.unwrap();
        println!("{:#?}", text);
    }
    Ok(())
}

//TODO implement deserializer for the response
