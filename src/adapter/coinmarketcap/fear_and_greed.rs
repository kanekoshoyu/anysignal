use chrono::{DateTime, NaiveDateTime};
use serde::Deserialize;

use crate::model::signal::{Signal, SignalData};

use super::prelude::*;

// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal

pub async fn run_signal_fear_and_index(
    config: Config,
    period: tokio::time::Duration,
) -> Result<()> {
    let signal_info = SignalInfo {
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
        // Wait for the next tick
        interval.tick().await;

        println!("Fetching coinmarket cap:");

        // dispatch request
        let response = client.get(url).header(KEY, &key_cmc).send().await.unwrap();
        let response = response.text().await.unwrap();
        let response = serde_json::from_str::<FearAndGreedIndex>(&response).unwrap();

        let mut signals = Vec::new();
        for data in response.data {
            let signal = Signal {
                id: 0,
                timestamp: data.get_time().unwrap(),
                data: SignalData::Scalar(data.value),
                info_id: signal_info.id,
            };
            signals.push(signal);
        }
        signals.sort_by_key(|i| i.timestamp);
        println!("Signals: {:#?}", signals);
    }
}

// TODO implement deserializer for the response

#[derive(Debug, Clone, Deserialize)]
pub struct FearAndGreedIndex {
    data: Vec<FearAndGreedIndexData>,
}

// implement deserializer for the response
#[derive(Debug, Clone, Deserialize)]
pub struct FearAndGreedIndexData {
    // e.g. 1731888000
    timestamp: String,
    value: f64,
}
impl FearAndGreedIndexData {
    pub fn get_time(&self) -> Option<NaiveDateTime> {
        let time = self.timestamp.parse::<i64>().unwrap();
        Some(DateTime::from_timestamp(time, 0)?.naive_utc())
    }
}
