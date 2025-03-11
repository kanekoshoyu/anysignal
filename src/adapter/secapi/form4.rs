use std::collections::HashMap;

// polling HTTP live caption data
use super::prelude::*;
use crate::{
    adapter::{DataSource, DataSourceType},
    extension::reqwest::ResponseExt,
};
use chrono::{DateTime, FixedOffset, NaiveDate};
use serde::{Deserialize, Serialize};
use yup_oauth2::{read_service_account_key, ServiceAccountAuthenticator};

const QUERY_ANY_BUY_OR_SELL: &str = "nonDerivativeTable.transactions.coding.code:P OR derivativeTable.transactions.coding.code:P OR nonDerivativeTable.transactions.coding.code:S OR derivativeTable.transactions.coding.code:S";
pub struct Form4FilingAllRequirement {
    query: String,
    from: u32,
    size: u32,
    sort: Vec<HashMap<String, Sort>>,
}
impl Form4FilingAllRequirement {
    fn get_all_buy_sell_latest() -> Self {
        let mut hash = HashMap::new();
        hash.insert("filedAt".to_string(), Sort::asc());
        Self {
            query: QUERY_ANY_BUY_OR_SELL.to_string(),
            from: 0,
            size: 50,
            sort: vec![hash],
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub struct Sort {
    order: Order,
}
impl Sort {
    fn asc() -> Self {
        Self { order: Order::Asc }
    }
    fn desc() -> Self {
        Self { order: Order::Desc }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

pub struct Form4FilingAllFetcher {
    requirement: Form4FilingAllRequirement,
}

pub struct Form4FilingAllResponse {
    total: TotalData,
    transactions: Vec<SecFiling>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SecFiling {
    pub accession_no: String,
    pub filed_at: DateTime<FixedOffset>,
    pub document_type: String,
    pub period_of_report: NaiveDate,
    pub not_subject_to_section16: bool,
    pub issuer: Issuer,
    pub reporting_owner: ReportingOwner,
    pub non_derivative_table: TransactionTable,
    pub derivative_table: TransactionTable,
    pub footnotes: Vec<Footnote>,
    pub owner_signature_name: String,
    pub owner_signature_name_date: NaiveDate,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Issuer {
    pub cik: String,
    pub name: String,
    pub trading_symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReportingOwner {
    pub cik: String,
    pub name: String,
    pub address: serde_json::Value, // Replace with actual fields if known
    pub relationship: Relationship,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Relationship {
    pub is_director: bool,
    pub is_officer: bool,
    pub is_ten_percent_owner: bool,
    pub is_other: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionTable {
    pub transactions: Vec<Transaction>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Transaction {
    pub security_title: String,
    pub conversion_or_exercise_price: Option<f64>, // Only for derivative transactions
    pub transaction_date: NaiveDate,
    pub coding: TransactionCoding,
    pub exercise_date_footnote_id: Option<Vec<String>>, // Only for derivative transactions
    pub expiration_date: Option<NaiveDate>,             // Only for derivative transactions
    pub underlying_security: Option<UnderlyingSecurity>, // Only for derivative transactions
    pub amounts: TransactionAmounts,
    pub post_transaction_amounts: PostTransactionAmounts,
    pub ownership_nature: OwnershipNature,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionCoding {
    pub form_type: String,
    pub code: String,
    pub equity_swap_involved: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct UnderlyingSecurity {
    pub title: String,
    pub shares: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TransactionAmounts {
    pub shares: u32,
    pub price_per_share: f64,
    pub acquired_disposed_code: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PostTransactionAmounts {
    pub shares_owned_following_transaction: u32,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct OwnershipNature {
    pub direct_or_indirect_ownership: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Footnote {
    pub id: String,
    pub text: String,
}

impl DataSource for Form4FilingAllFetcher {
    type DataType = Vec<SecFiling>;

    fn id() -> String {
        "latest sec form 4 filing".to_string()
    }

    fn data_source_type() -> DataSourceType {
        DataSourceType::Historic
    }
}

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

pub struct TotalData {
    value: u32,
    relation: String,
}
// TODO generalize into a SignalFetcher Trait and implement the fetcher for each signal
#[derive(Debug, Clone, Default)]
pub struct YouTubeClosedCaptionRequirement {
    pub url: String,
    pub language: Option<String>,
    pub polling_period: tokio::time::Duration,
}

pub fn signal_info(requirement: &YouTubeClosedCaptionRequirement) -> SignalInfo {
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
) -> AnySignalResult<()> {
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
pub async fn fetch(
    api_key: &str,
    req: &YouTubeClosedCaptionRequirement,
) -> AnySignalResult<String> {
    // Extract video ID from the URL
    let video_id = extract_video_id(&req.url).ok_or("Invalid YouTube URL")?;
    let path = "./config/oauth.json";
    let key = match read_service_account_key(path).await {
        Ok(key) => key,
        Err(e) => return Err(AnySignalError::from(format!("{}", e).as_str())),
    };

    // Create an authenticator
    let auth = ServiceAccountAuthenticator::builder(key)
        .build()
        .await
        .map_err(|_| "Failed to create authenticator")?;

    // Get an access token
    let token = auth
        .token(&["https://www.googleapis.com/auth/youtube.force-ssl"])
        .await
        .map_err(|_| "Failed to get access token")?;

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

mod tests {
    #[tokio::test]
    async fn test_live_closed_caption_fetcher() {
        use form4::*;
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
