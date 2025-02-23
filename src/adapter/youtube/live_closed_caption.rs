// polling HTTP live caption data

use crate::extension::reqwest::ResponseExt;

use super::prelude::*;
use chrono::{DateTime, NaiveDateTime};
use serde::Deserialize;
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator, ServiceAccountKey};

#[derive(Deserialize, Debug)]
struct CaptionListResponse {
    items: Vec<Caption>,
}

#[derive(Deserialize, Debug)]
struct Caption {
    id: String,
    snippet: CaptionSnippet,
}

#[derive(Deserialize, Debug)]
struct CaptionSnippet {
    language: String,
    // Add other fields you need
}

// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal
#[derive(Debug, Clone, Default)]
pub struct YouTubeClosedCaptionRequirement {
    url: String,
    language: Option<String>,
    polling_period: tokio::time::Duration,
}

fn signal_info(requirement: &YouTubeClosedCaptionRequirement) -> SignalInfo {
    SignalInfo {
        id: 4,
        signal_type: format!("youtube_closed_caption_{}", requirement.url),
        data_type: SignalDataType::Text,
        source: SOURCE.to_string(),
        description: "YouTube live closed caption".to_string(),
        is_atomic: true,
    }
}

pub async fn run_live_closed_caption_fetcher(
    config: Config,
    requirement: &YouTubeClosedCaptionRequirement,
) -> Result<()> {
    let mut interval = tokio::time::interval(requirement.polling_period);
    let api_key = config.get_api_key("youtube_data_v3")?;

    loop {
        // Wait for the next tick
        interval.tick().await;

        println!(
            "Fetching youtube live close caption for {}:",
            requirement.url
        );
        let response = fetch(&api_key, requirement).await?;
        println!("artibles: {:?}", response);
    }
}

fn extract_video_id(url: &str) -> Option<&str> {
    url.split("v=").nth(1).and_then(|v| v.split('&').next())
}

/// Fetches closed captions based on the provided requirements.
pub async fn fetch(api_key: &str, req: &YouTubeClosedCaptionRequirement) -> Result<String> {
    // Extract video ID from the URL
    let video_id = extract_video_id(&req.url).expect("Invalid YouTube URL");
    let path = "./config/oauth.json";
    let key = match read_service_account_key(path).await {
        Ok(key) => key,
        Err(e) => return Err(AnySignalError::from(format!("{}", e).as_str())),
    };

    // Create an authenticator
    let auth = ServiceAccountAuthenticator::builder(key)
        .build()
        .await
        .expect("Failed to create authenticator");

    // Get an access token
    let token = auth
        .token(&["https://www.googleapis.com/auth/youtube.force-ssl"])
        .await
        .expect("Failed to get access token");

    // List available captions for the video
    let url_base = "https://www.googleapis.com/youtube/v3";
    let url_caption_list = format!("{}/captions", url_base);

    let caption_list: CaptionListResponse = reqwest::Client::new()
        .get(url_caption_list)
        .query(&[("key", api_key), ("videoId", video_id), ("part", "snippet")])
        .send()
        .await?
        .parse_json()
        .await?;

    // Determine the desired language (default to "en" if not specified)
    let desired_language = req.language.to_owned().unwrap_or_else(|| "en".to_string());

    // Find and download captions in the desired language
    let caption_id = caption_list
        .items
        .iter()
        .find(|c| c.snippet.language == desired_language)
        .map(|c| c.id.to_owned())
        .ok_or(AnySignalError::from("no caption found"))?;

    let url_caption_download = format!("{}/captions/{}", url_base, caption_id);

    dbg!(&url_caption_download);

    // Use 'tfmt=vtt' for WebVTT format
    let caption_response = reqwest::Client::new()
        .get(url_caption_download)
        .bearer_auth(token.token().unwrap_or_default())
        .query(&[("key", api_key), ("tfmt", "srt")])
        .send()
        .await?;

    Ok(caption_response.text().await?)
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
    async fn test_live_closed_caption_fetcher() {
        use super::*;
        let config: Config = Config::from_path("config.toml").unwrap();
        let api_key = config.get_api_key("youtube_data_v3").unwrap();
        dbg!(&api_key);

        // Example usage
        let requirement = YouTubeClosedCaptionRequirement {
            url: "https://www.youtube.com/watch?v=UgPp6oTocAc".into(),
            // Fetch captions in English
            language: Some("en".to_string()),
            polling_period: std::time::Duration::from_secs(10),
        };

        match fetch(&api_key, &requirement).await {
            Ok(captions) => println!("Captions: {}", captions),
            Err(e) => eprintln!("Error fetching captions: {}", e),
        }
    }
}
