pub use thiserror::Error as ThisError;

#[derive(Debug, ThisError)]
pub enum AdapterError {
    #[error("Connection")]
    Connection,
    #[error("Parser")]
    Parser,
    #[error("Data")]
    Data,
    #[error("Configuration: {0}")]
    ConfigurationError(String),
    #[error("Fetch: {0}")]
    FetchError(String),
}

pub type AdapterResult<T> = Result<T, AdapterError>;
