use super::prelude::*;

fn signal_info() -> SignalInfo {
    SignalInfo {
        id: 2,
        signal_type: "btc_dominance".to_string(),
        data_type: SignalDataType::Scalar,
        source: SOURCE.to_string(),
        description: "Bitcoin Dominance Index".to_string(),
        is_atomic: true,
    }
}

// TODO set up the global source to replace the config
pub async fn run_bitcoin_dominance(
    config: Config,
    period: tokio::time::Duration,
) -> AnySignalResult<()> {
    let signal = signal_info();
    let mut interval = tokio::time::interval(period);
    let client = reqwest::Client::new();
    let url = "https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest";

    let key_cmc = config.get_api_key("coinmarketcap")?;

    loop {
        interval.tick().await; // Wait for the next tick

        let _response = client
            .get(url)
            .header(KEY, &key_cmc)
            .send()
            .await?
            .text()
            .await?;

        // write a parser for the response
        println!("{:?}", &signal.description);
    }
}
