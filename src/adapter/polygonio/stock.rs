use crate::error::AnySignalResult;
use futures::SinkExt;
use futures::StreamExt;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

pub async fn run_polygonio_stock(api_key: String) -> AnySignalResult<()> {
    let url = "wss://delayed.polygon.io/stocks";

    // Establish WebSocket connection
    let stream = connect_async(url).await?.0;
    let (mut tx, mut rx) = stream.split();
    tracing::info!("established connection");

    // auth
    {
        let message = format!("{{\"action\":\"auth\",\"params\":\"{}\"}}", api_key);
        let message = Message::Text(message.into());
        tx.send(message).await?;
    }
    tracing::info!("auth granted");

    // request
    {
        let message = "{\"action\":\"subscribe\",\"params\":\"AM.LPL,AM.MSFT,AM.TSLA\"}";
        let message = Message::Text(message.into());
        tx.send(message).await?;
    }
    tracing::info!("requested");

    // receive and print response
    loop {
        match rx.next().await {
            Some(Ok(msg)) => tracing::debug!(msg = ?msg, "received"),
            Some(Err(e)) => {
                tracing::error!(error = ?e, "error receiving message");
            }
            None => continue,
        };
    }
}

mod tests {
    #[tokio::test]
    async fn test_stock() {
        //
        use super::*;
        let url = "wss://delayed.polygon.io/stocks";
        let result = connect_async(url).await;
        dbg!(&result);
        assert!(result.is_ok());
    }
}
