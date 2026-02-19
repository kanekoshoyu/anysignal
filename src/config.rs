use crate::adapter::AdapterError;
use crate::error::AnySignalResult;
use std::env;

/// Runtime configuration loaded from environment variables.
///
/// API keys are read from `API_KEY_<ID>` (uppercase), e.g. `API_KEY_NEWSAPI`.
/// Active runners are read from `RUNNERS` as a comma-separated list,
/// e.g. `RUNNERS=api,coinmarketcap,newsapi`.
#[derive(Debug, Clone)]
pub struct Config {
    runners: Vec<String>,
}

impl Config {
    pub fn from_env() -> Self {
        let runners = env::var("RUNNERS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();
        Self { runners }
    }

    pub fn has_runner(&self, name: &str) -> bool {
        self.runners.iter().any(|r| r == name)
    }

    /// Returns the API key for the given service id.
    /// Reads `API_KEY_<ID_UPPERCASE>` from the environment.
    pub fn get_api_key(&self, id: &str) -> AnySignalResult<String> {
        let var_name = format!("API_KEY_{}", id.to_uppercase());
        env::var(&var_name).map_err(|_| {
            AdapterError::ConfigurationError(format!("missing env var: {var_name}")).into()
        })
    }
}
