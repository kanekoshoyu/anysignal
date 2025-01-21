use crate::adapter::AdapterError;

use super::prelude::*;
use chrono::{DateTime, NaiveDateTime};
use reqwest::header::USER_AGENT;
use serde::Deserialize;

// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal
fn signal_info() -> SignalInfo {
    SignalInfo {
        id: 4,
        signal_type: "news".to_string(),
        data_type: SignalDataType::Text,
        source: SOURCE.to_string(),
        description: "News Aritcle".to_string(),
        is_atomic: true,
    }
}

pub async fn run_news_fetcher(config: Config, period: tokio::time::Duration) -> Result<()> {
    let mut interval = tokio::time::interval(period);
    let api_key = config.get_api_key("newsapi")?;

    loop {
        // Wait for the next tick
        interval.tick().await;

        println!("Fetching NewsAPI:");
        let response = fetch(&api_key).await?;
        println!("artibles: {:?}", response.articles);
    }
}

async fn fetch(api_key: &str) -> Result<NewsApiResponse> {
    let client = reqwest::Client::new();
    let url = "https://newsapi.org/v2/everything?q=bitcoin&sortBy=publishedAt&apiKey=";

    // Dispatch request
    let response = client
        .get(format!("{}{}", url, api_key))
        .header(USER_AGENT, AGENT)
        .send()
        .await
        .unwrap();
    let response = response.text().await.unwrap();
    println!("response: {response:?}");
    serde_json::from_str::<NewsApiResponse>(&response).map_err(|_| AdapterError::Parser.into())
}

// TODO implement deserializer for the response

#[derive(Debug, Clone, Deserialize)]
pub struct NewsApiResponse {
    articles: Vec<NewsArticle>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NewsArticle {
    // e.g. "2024-01-01T12:34:56Z"
    published_at: String,
    title: String,
}

impl NewsArticle {
    pub fn get_time(&self) -> Option<NaiveDateTime> {
        DateTime::parse_from_rfc3339(&self.published_at)
            .ok()
            .map(|dt| dt.naive_utc())
    }
}

mod tests {
    #[tokio::test]
    async fn test_news_fetcher() {
        use super::*;
        let config: Config = Config::from_path("config.toml").unwrap();
        let api_key = config.get_api_key("newsapi").unwrap();
        let data = fetch(&api_key).await.unwrap();
        println!("{data:#?}");
    }
}
