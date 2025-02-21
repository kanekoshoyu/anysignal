use tokio::sync::mpsc;
use config::Config;
use dotenv::dotenv;
use std::{env, future};

use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use futures::StreamExt;
use futures::SinkExt;
use url::Url;


pub async fn websocket_client() {
    // Define WebSocket URL (Echo test server)
    let url = Url::parse("wss://delayed.polygon.io/stocks").unwrap();

    println!("Connecting to WebSocket server...");

    // Establish WebSocket connection
    let (ws_stream, _) = connect_async(url).await.expect("Failed to connect");
    println!("Connected!");

    let (mut write, mut read) = ws_stream.split();

    // Send a message to the server
    dotenv().ok();
    let api_key = env::var("POLYGON_API_KEY").expect("POLYGON_API_KEY not set in .env file");
    let message = Message::Text(format!("{{\"action\":\"auth\",\"params\":\"{}\"}}", api_key));
    write.send(message).await.expect("Failed to send message");


    let message = Message::Text("{\"action\":\"subscribe\",\"params\":\"AM.LPL,AM.MSFT,AM.TSLA\"}".into());
    write.send(message).await.expect("Failed to send message");

    // Receive and print response
    while let Some(msg) = read.next().await {
        match msg {
            Ok(msg) => println!("Received: {:?}", msg),
            Err(e) => {
                eprintln!("Error receiving message: {:?}", e);
                break;
            }
        }
    }

    println!("Closing connection.");
}


