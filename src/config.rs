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

    pub fn get_key(&self, id: &str) -> Result<String, eyre::Error> {
        self.api_keys
            .iter()
            .find(|key| key.id == id)
            .map(|key| key.key.clone())
            .ok_or_else(|| eyre::eyre!("key not found"))
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiKey {
    id: String,  // API key identifier
    key: String, // Actual API key
}
