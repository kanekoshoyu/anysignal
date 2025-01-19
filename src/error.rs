use crate::{adapter::AdapterError, api::rest::ApiError};
pub use eyre::Error as EyreError;
pub use questdb::Error as QuestError;
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
pub enum SignalsError {
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

pub type Result<T> = std::result::Result<T, SignalsError>;

impl From<EyreError> for SignalsError {
    fn from(error: EyreError) -> Self {
        SignalsError::Generic(error)
    }
}

impl From<ConfigError> for SignalsError {
    fn from(error: ConfigError) -> Self {
        SignalsError::Config(error)
    }
}

impl From<ApiError> for SignalsError {
    fn from(error: ApiError) -> Self {
        SignalsError::Api(error)
    }
}

impl From<AdapterError> for SignalsError {
    fn from(error: AdapterError) -> Self {
        SignalsError::Adapter(error)
    }
}

impl From<QuestError> for SignalsError {
    fn from(error: QuestError) -> Self {
        SignalsError::Quest(error)
    }
}
