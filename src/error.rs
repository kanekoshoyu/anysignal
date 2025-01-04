pub use eyre::Error as EyreError;
pub use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Failed to read config file: {0}")]
    Io(#[from] std::io::Error),
    #[error("Failed to parse TOML: {0}")]
    Toml(#[from] toml::de::Error),
}

// TODO implement chaining errors
#[derive(Debug, Error)]
pub enum SignalsError {
    #[error("generic error: {0}")]
    Generic(EyreError),
    #[error("conifg error: {0}")]
    Config(ConfigError),
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
