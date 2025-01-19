use crate::model::signal::{Signal, SignalData};
use questdb::{
    ingress::{Buffer, Sender, TimestampMicros},
    Result as QuestResult,
};
use rand::Rng;
use serde::Deserialize;

use super::prelude::*;

// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal

fn signal_info() -> SignalInfo {
    SignalInfo {
        id: 0,
        signal_type: "fear_and_greed".to_string(),
        source: SOURCE.to_string(),
        description: "Fear and Greed Index".to_string(),
        is_atomic: true,
    }
}

pub async fn runner_store_signal_fear_and_greed_index(
    config: Config,
    period: tokio::time::Duration,
) -> Result<()> {
    let signal_info = signal_info();
    let mut interval = tokio::time::interval(period);

    let key_cmc = config.get_api_key("coinmarketcap")?;
    loop {
        // Wait for the next tick
        interval.tick().await;

        println!("Fetching coinmarket cap:");
        let response = fetch_fear_and_greed_index(&key_cmc).await?;

        let mut signals = Vec::new();
        for data in response.data {
            let signal = Signal {
                id: 0,
                timestamp_us: data.timestamp_micros().unwrap(),
                data: SignalData::Scalar(data.value),
                info_id: signal_info.id,
            };
            signals.push(signal);
        }
        signals.sort_by_key(|i| i.timestamp_us);
        println!("Signals: {:#?}", signals);
    }
}

// Function to fetch the Fear and Greed Index
async fn fetch_fear_and_greed_index(key_cmc: &str) -> Result<FearAndGreedIndexResponse> {
    let url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical";
    let client = reqwest::Client::new();
    let response = client.get(url).header(KEY, key_cmc).send().await.unwrap();
    let response = response.text().await.unwrap();
    let response = serde_json::from_str::<FearAndGreedIndexResponse>(&response).unwrap();
    Ok(response)
}

#[derive(Debug, Clone, Deserialize)]
pub struct FearAndGreedIndexResponse {
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
    pub fn timestamp_micros(&self) -> Option<i64> {
        let timestamp_sec = self.timestamp.parse::<i64>().unwrap();
        Some(timestamp_sec * 1_000_000)
    }

    pub fn dummy() -> Self {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let value = rand::thread_rng().gen_range(0..=100) as f64; // Generate a number between 0 and 100 (inclusive)
        Self { timestamp, value }
    }
}

pub fn insert_quest_db(
    sender: &mut Sender,
    signal_info: &SignalInfo,
    data: &[FearAndGreedIndexData],
) -> QuestResult<()> {
    // do a query on existing data with the timestamp

    // batch store data into the buffer
    let mut buffer = Buffer::new();
    for data in data {
        let timestamp_us = data.timestamp_micros().unwrap();
        buffer
            .table("signal")?
            .symbol("id", &signal_info.source)?
            .column_f64("value", data.value)?
            .at(TimestampMicros::new(timestamp_us))?;
    }
    sender.flush(&mut buffer)?;
    Ok(())
}

// pub fn get_rows_quest_db(
//     sender: &mut Sender,
//     signal_info: &SignalInfo, // add condition here,
// ) -> QuestResult<()> {
//     let mut buffer = Buffer::new();
//     for data in data {
//         let timestamp_us = data.timestamp_micros().unwrap();

//         buffer
//             .table("signal")?
//             .symbol("id", &signal_info.source)?
//             .column_f64("value", data.value)?
//             .at(TimestampMicros::new(timestamp_us))?;
//     }
//     sender.flush(&mut buffer)?;
//     Ok(())
// }

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn test_store_questdb() {
        use super::*;
        use questdb::ingress::Sender;
        let mut sender = Sender::from_conf("http::addr=localhost:9000;").unwrap();

        let signal_info = signal_info();

        let data = [FearAndGreedIndexData::dummy()].to_vec();

        insert_quest_db(&mut sender, &signal_info, &data).unwrap();
    }

    #[tokio::test]
    async fn test_update_questdb() {
        // result: this seems to just store the both data
        use super::*;
        use questdb::ingress::Sender;
        let mut sender = Sender::from_conf("http::addr=localhost:9000;").unwrap();

        let signal_info = signal_info();

        let data = [
            FearAndGreedIndexData {
                timestamp: "1737275475".into(),
                value: 100.0,
            },
            FearAndGreedIndexData {
                timestamp: "1737275475".into(),
                value: 75.0,
            },
        ]
        .to_vec();

        insert_quest_db(&mut sender, &signal_info, &data).unwrap();
    }

    #[tokio::test]
    async fn test_store_index_questdb() {
        // result: this seems to just store the both data
        use super::*;
        use questdb::ingress::Sender;
        let config: Config = Config::from_path("config.toml").unwrap();
        let key_cmc = config.get_api_key("coinmarketcap").unwrap();
        let data = fetch_fear_and_greed_index(&key_cmc).await.unwrap();
        let signal_info = signal_info();
        let mut sender = Sender::from_conf("http::addr=localhost:9000;").unwrap();
        insert_quest_db(&mut sender, &signal_info, &data.data).unwrap();
    }
}
