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
fn extract_video_id(url: &str) -> Option<&str> {
    url.split("v=").nth(1).and_then(|v| v.split('&').next())
}


