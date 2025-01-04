use crate::error::ConfigError;
use serde::Deserialize;
use std::path::Path;

// config file parser
#[derive(Debug, Deserialize)]
pub struct Config {
    api_keys: Vec<ApiKey>, // Array of API keys
}

impl Config {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let file_path = path.as_ref();
        let config_content = std::fs::read_to_string(file_path)?;
        toml::from_str(&config_content).map_err(ConfigError::Toml)
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiKey {
    id: String,  // API key identifier
    key: String, // Actual API key
}
