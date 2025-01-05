use crate::error::ConfigError;
use serde::Deserialize;
use std::path::Path;

// config file parser
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    api_keys: Vec<ApiKey>, // Array of API keys
    runner: Vec<String>,   // runners
}

impl Config {
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let file_path = path.as_ref();
        let config_content = std::fs::read_to_string(file_path)?;
        toml::from_str(&config_content).map_err(ConfigError::Toml)
    }

    // get an API key by id
    pub fn get_api_key(&self, id: &str) -> Result<String, eyre::Error> {
        self.api_keys
            .iter()
            .find(|key| key.id == id)
            .map(|key| key.key.clone())
            .ok_or_else(|| eyre::eyre!("key not found"))
    }

    // check if a runner has been set
    pub fn has_runner(&self, id: &str) -> bool {
        self.runner.iter().any(|runner| runner == id)
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ApiKey {
    /// API key identifier
    id: String,
    /// Actual API key
    key: String,
}
