use crate::{adapter::AdapterError, api::rest::ApiError};
pub use eyre::Error as EyreError;
pub use questdb::Error as QuestError;
pub use reqwest::Error as ReqwestError;
pub use std::io::Error as IoError;
pub use thiserror::Error as ThisError;
pub use toml::de::Error as TomlError;
#[derive(Debug, ThisError)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse TOML: {0}")]
    Toml(#[from] TomlError),
}

// project error
#[derive(Debug, ThisError)]
pub enum AnySignalError {
    #[error("generic error: {0}")]
    Generic(EyreError),
    #[error("conifg error: {0}")]
    Config(ConfigError),
    #[error("api error: {0}")]
    Api(ApiError),
    #[error("adapter error: {0}")]
    Adapter(AdapterError),
    #[error("quest error: {0}")]
    Quest(QuestError),
}

pub type Result<T> = std::result::Result<T, AnySignalError>;

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

impl From<&str> for AnySignalError {
    fn from(error: &str) -> Self {
        AnySignalError::Generic(eyre::eyre!("{error}"))
    }
}

impl From<ConfigError> for AnySignalError {
    fn from(error: ConfigError) -> Self {
        AnySignalError::Config(error)
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
