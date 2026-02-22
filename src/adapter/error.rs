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
    /// The requested S3 object does not exist (HTTP 404 / NoSuchKey).
    #[error("NotFound: {0}")]
    NotFound(String),
    /// AWS credential or permission error (HTTP 401/403).
    #[error("Unauthorized: {0}")]
    Unauthorized(String),
}

pub type AdapterResult<T> = Result<T, AdapterError>;
