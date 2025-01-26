use crate::adapter::error::AdapterResult;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DexSearchPairsReponse {
    schema_version: String,
    pairs: Vec<DexTokenInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct DexTokenInfo {
    chain_id: String,
    dex_id: String,
    url: String,
    pair_address: String,
    labels: Option<Vec<String>>,
    base_token: TokenInfo,
    quote_token: TokenInfo,
    price_native: String,
    price_usd: String,
    txns: Txns,
    volume: Volume,
    price_change: PriceChange,
    liquidity: Liquidity,
    fdv: u64,
    market_cap: u64,
    pair_created_at: u64,
    info: Option<Info>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TokenInfo {
    address: String,
    name: String,
    symbol: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Txns {
    m5: TransactionStats,
    h1: TransactionStats,
    h6: TransactionStats,
    h24: TransactionStats,
}

#[derive(Serialize, Deserialize, Debug)]
struct TransactionStats {
    buys: u64,
    sells: u64,
}

#[derive(Serialize, Deserialize, Debug)]
struct Volume {
    h24: f64,
    h6: f64,
    h1: f64,
    m5: f64,
}

#[derive(Serialize, Deserialize, Debug)]
struct PriceChange {
    m5: Option<f64>,
    h1: Option<f64>,
    h6: Option<f64>,
    h24: Option<f64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Liquidity {
    usd: f64,
    base: f64,
    quote: f64,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct Info {
    image_url: String,
    header: String,
    open_graph: String,
    websites: Vec<Website>,
    socials: Vec<Social>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Website {
    label: String,
    url: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Social {
    r#type: String,
    url: String,
}

// fetch token info based on chain_id and token_id
async fn fetch_token_info(chain_id: &str, token_id: &str) -> AdapterResult<Vec<DexTokenInfo>> {
    let url = format!("https://api.dexscreener.com/tokens/v1/{chain_id}/{token_id}");
    let client = reqwest::Client::new();
    let response = client.get(url).send().await.unwrap();
    let response = response.text().await.unwrap();
    let response = serde_json::from_str::<Vec<DexTokenInfo>>(&response).unwrap();
    Ok(response)
}

// fetch relevant pairs based on a keyword
async fn search_pairs(keyword: &str) -> AdapterResult<DexSearchPairsReponse> {
    let url = format!("https://api.dexscreener.com/latest/dex/search");
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .query(&[("q", keyword)])
        .send()
        .await
        .unwrap();
    let response = response.text().await.unwrap();
    let response = serde_json::from_str::<DexSearchPairsReponse>(&response).unwrap();
    Ok(response)
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_fetch_token_info() {
        use super::*;
        let chain_id = "solana";
        let token_id = "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN";
        let result = fetch_token_info(&chain_id, &token_id).await.unwrap();
        assert!(!result.is_empty());
        println!("result: {:#?}", result.len())
    }

    #[tokio::test]
    async fn test_seach_pairs() {
        use super::*;
        let keyword = "TRUMP";
        let result = search_pairs(&keyword).await.unwrap();
        assert!(!result.pairs.is_empty());
        let result = result
            .pairs
            .into_iter()
            .map(|i| format!("{}-{}", i.base_token.symbol, i.quote_token.symbol))
            .collect::<Vec<_>>();
        println!("result: {:#?}", result)
    }
}
