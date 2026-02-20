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
    pub questdb_addr: String,
    pub questdb_user: Option<String>,
    pub questdb_password: Option<String>,
    pub s3_bucket: String,
    pub aws_region: String,
}

impl Config {
    pub fn from_env() -> Self {
        let runners = env::var("RUNNERS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_lowercase())
            .filter(|s| !s.is_empty())
            .collect();

        let questdb_addr =
            env::var("QUESTDB_ADDR").unwrap_or_else(|_| "localhost:9000".to_string());
        let questdb_user = env::var("QUESTDB_USER").ok();
        let questdb_password = env::var("QUESTDB_PASSWORD").ok();
        let s3_bucket = env::var("HYPERLIQUID_S3_BUCKET")
            .unwrap_or_else(|_| "hyperliquid-archive".to_string());
        let aws_region =
            env::var("AWS_REGION").unwrap_or_else(|_| "ap-northeast-1".to_string());

        Self {
            runners,
            questdb_addr,
            questdb_user,
            questdb_password,
            s3_bucket,
            aws_region,
        }
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
