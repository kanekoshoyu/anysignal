use crate::{adapter::AdapterError, api::rest::ApiError};
pub use eyre::Error as EyreError;
pub use questdb::Error as QuestError;
pub use reqwest::Error as ReqwestError;
pub use thiserror::Error as ThisError;
pub use tokio_tungstenite::tungstenite::Error as TungError;

// project error
#[derive(Debug, ThisError)]
pub enum AnySignalError {
    #[error("generic error: {0}")]
    Generic(EyreError),
    #[error("api error: {0}")]
    Api(ApiError),
    #[error("adapter error: {0}")]
    Adapter(AdapterError),
    #[error("quest error: {0}")]
    Quest(QuestError),
}

pub type AnySignalResult<T> = std::result::Result<T, AnySignalError>;

impl From<&str> for AnySignalError {
    fn from(error: &str) -> Self {
        AnySignalError::Generic(eyre::eyre!("{error}"))
    }
}

impl From<EyreError> for AnySignalError {
    fn from(error: EyreError) -> Self {
        AnySignalError::Generic(error)
    }
}

impl From<serde_json::Error> for AnySignalError {
    fn from(error: serde_json::Error) -> Self {
        AnySignalError::Api(error.into())
    }
}

impl From<ApiError> for AnySignalError {
    fn from(error: ApiError) -> Self {
        AnySignalError::Api(error)
    }
}

impl From<AdapterError> for AnySignalError {
    fn from(error: AdapterError) -> Self {
        AnySignalError::Adapter(error)
    }
}

impl From<QuestError> for AnySignalError {
    fn from(error: QuestError) -> Self {
        AnySignalError::Quest(error)
    }
}

impl From<ReqwestError> for AnySignalError {
    fn from(error: ReqwestError) -> Self {
        AnySignalError::Api(error.into())
    }
}

impl From<TungError> for AnySignalError {
    fn from(_: TungError) -> Self {
        AnySignalError::Adapter(AdapterError::Connection)
    }
}
