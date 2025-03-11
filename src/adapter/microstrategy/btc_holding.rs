use crate::error::AnySignalResult;
use futures::SinkExt;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub async fn run_polygonio_stock(api_key: String) -> AnySignalResult<()> {
    let url = "wss://delayed.polygon.io/stocks";

    // Establish WebSocket connection
    let stream = connect_async(url).await?.0;
    let (mut tx, mut rx) = stream.split();
    println!("established connection");

    // auth
    {
        let message = format!("{{\"action\":\"auth\",\"params\":\"{}\"}}", api_key);
        let message = Message::Text(message.into());
        tx.send(message).await?;
    }
    println!("auth granted");

    // request
    {
        let message = "{\"action\":\"subscribe\",\"params\":\"AM.LPL,AM.MSFT,AM.TSLA\"}";
        let message = Message::Text(message.into());
        tx.send(message).await?;
    }
    println!("requested");

    // receive and print response
    loop {
        match rx.next().await {
            Some(Ok(msg)) => println!("Received: {:?}", msg),
            Some(Err(e)) => {
                eprintln!("Error receiving message: {:?}", e);
            }
            None => continue,
        };
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BtcPurchaseInfo {
    purchase_price: f64,
    count: f64,
    date_of_purchase: String,
}

mod tests {

    #[tokio::test]
    async fn test() {
        use super::*;
        use regex::Regex;
        use serde_json::from_value;
        use serde_json::Value;

        let url = "https://www.strategy.com/purchases";

        let html_text = reqwest::get(url).await.unwrap().text().await.unwrap();

        let re = Regex::new(
            r#"<script id="__NEXT_DATA__"[^>]*type="application/json"[^>]*>(.*?)</script>"#,
        )
        .unwrap();

        let Some(caps) = re.captures(&html_text) else {
            eprintln!("No match found for __NEXT_DATA__ script!");
            return;
        };
        let json_str = caps.get(1).unwrap().as_str();
        let parsed: Value = serde_json::from_str(json_str).unwrap();
        let bitcoin_data = &parsed["props"]["pageProps"]["bitcoinData"];

        let bitcoin_data: Vec<BtcPurchaseInfo> = from_value(bitcoin_data.clone()).unwrap();

        dbg!(bitcoin_data);
    }
}
