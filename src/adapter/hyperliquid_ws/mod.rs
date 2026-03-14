/// Guilder WebSocket bridge — subscribes to fills + asset contexts for all coins.
pub mod guilder_bridge;
/// Predicted funding rate poller (REST, polled every minute).
pub mod predicted_funding;

pub use guilder_bridge::GuilderBridge;
pub use predicted_funding::run_predicted_funding_poller;
