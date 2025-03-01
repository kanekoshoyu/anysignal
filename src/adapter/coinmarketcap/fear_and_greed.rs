use super::prelude::*;
use crate::model::signal::{Signal, SignalData};
use rand::Rng;
use serde::Deserialize;
use tokio::time::Duration;

// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal

fn fear_and_greed_signal_info() -> SignalInfo {
    SignalInfo {
        id: 0,
        signal_type: "fear_and_greed".to_string(),
        data_type: SignalDataType::Scalar,
        source: SOURCE.to_string(),
        description: "Fear and Greed Index".to_string(),
        is_atomic: true,
    }
}

pub struct FearAndGreedSignalSource {
    api_key: String,
    poll_interval_duration: Duration,
}

impl FearAndGreedSignalSource {
    pub fn new(api_key: String, poll_interval_duration: Duration) -> Self {
        Self {
            api_key,
            poll_interval_duration,
        }
    }
}
/// set up polling signal source trait
impl PollingSignalSource for FearAndGreedSignalSource {
    fn get_signal_info(&self) -> SignalInfo {
        fear_and_greed_signal_info()
    }

    fn poll_interval_duration(&self) -> Duration {
        self.poll_interval_duration
    }

    async fn get_signals(&self) -> AnySignalResult<Vec<Signal>> {
        let response = fetch_fear_and_greed_index(&self.api_key).await?;

        let mut signals = Vec::new();
        for data in response.data {
            let signal = Signal {
                timestamp_us: data.timestamp_micros().unwrap_or_default(),
                data: SignalData::Scalar(data.value),
                info_id: self.get_signal_info().id,
            };
            signals.push(signal);
        }
        signals.sort_by_key(|i| i.timestamp_us);
        Ok(signals)
    }
}

// Function to fetch the Fear and Greed Index
async fn fetch_fear_and_greed_index(key_cmc: &str) -> AnySignalResult<FearAndGreedIndexResponse> {
    let url = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical";
    reqwest::Client::new()
        .get(url)
        .header(KEY, key_cmc)
        .send()
        .await?
        .json::<FearAndGreedIndexResponse>()
        .await
        .map_err(|e| e.into())
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
        let timestamp_sec = self.timestamp.parse::<i64>().ok()?;
        Some(timestamp_sec * 1_000_000)
    }

    pub fn dummy() -> Self {
        let timestamp = chrono::Utc::now().timestamp().to_string();
        let value = rand::thread_rng().gen_range(0..=100) as f64; // Generate a number between 0 and 100 (inclusive)
        Self { timestamp, value }
    }
}

// pub fn insert_quest_db_fear_and_greed(
//     sender: &mut Sender,
//     signal_info: &SignalInfo,
//     data: &[FearAndGreedIndexData],
// ) -> QuestResult<()> {
//     // do a query on existing data with the timestamp

//     // batch store data into the buffer
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
