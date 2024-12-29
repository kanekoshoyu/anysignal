#[tokio::main]
async fn main() {
    loop {
        println!("Hello, world!");
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    }
}
