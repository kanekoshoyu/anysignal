use crate::error::ConfigError;
use serde::Deserialize;
use std::env;
use std::path::Path;

// config file parser
#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    api_keys: Vec<ApiKey>, // Array of API keys
    runner: Vec<String>,   // runners
}

impl Config {
    /// Load config from environment variables.
    ///
    /// - `RUNNERS` — comma-separated list of runners to enable, e.g. `api,coinmarketcap`
    /// - `API_KEY_<ID>` — API key for each runner, e.g. `API_KEY_COINMARKETCAP`
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        let runners = env::var("RUNNERS").unwrap_or_default();
        let runner = runners
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Self {
            runner,
            api_keys: Vec::new(), // keys are read on-demand via get_api_key
        }
    }

    /// Load config from a TOML file (used in local development and tests).
    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let file_path = path.as_ref();
        let config_content = std::fs::read_to_string(file_path)?;
        toml::from_str(&config_content).map_err(ConfigError::Toml)
    }

    /// Get an API key by runner id.
    ///
    /// When loaded via `from_env`, keys are resolved from `API_KEY_<ID>` env
    /// vars (e.g. `API_KEY_COINMARKETCAP`).  When loaded via `from_path` the
    /// inline `api_keys` table is used as a fallback.
    pub fn get_api_key(&self, id: &str) -> Result<String, eyre::Error> {
        let env_var = format!("API_KEY_{}", id.to_uppercase().replace('-', "_"));
        if let Ok(val) = env::var(&env_var) {
            return Ok(val);
        }
        self.api_keys
            .iter()
            .find(|key| key.id == id)
            .map(|key| key.key.clone())
            .ok_or_else(|| eyre::eyre!("API key not found: set {} env var", env_var))
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
