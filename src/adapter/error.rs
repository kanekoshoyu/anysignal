pub use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum AdapterError {
    #[error("Connection")]
    Connection,
    #[error("Parser")]
    Parser,
    #[error("Data")]
    Data,
}
