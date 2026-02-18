use std::env;

/// Runtime configuration loaded entirely from environment variables.
///
/// # Environment variables
///
/// ## Runners
/// `RUNNERS` — comma-separated list of subsystems to start.
///
/// | Value           | Description                                      |
/// |-----------------|--------------------------------------------------|
/// | `api`           | REST API server (port 3000)                      |
/// | `coinmarketcap` | Fear & Greed / BTC dominance poller              |
/// | `newsapi`       | News headline fetcher                            |
/// | `polygonio`     | Stock market orderbook indexer                   |
///
/// Example: `RUNNERS=api,coinmarketcap`
///
/// ## API keys
/// Each runner reads its key from `API_KEY_<ID>` (uppercase).
///
/// | Variable                  | Runner          |
/// |---------------------------|-----------------|
/// | `API_KEY_COINMARKETCAP`   | coinmarketcap   |
/// | `API_KEY_NEWSAPI`         | newsapi         |
/// | `API_KEY_YOUTUBE_DATA_V3` | youtube_data_v3 |
/// | `API_KEY_POLYGONIO`       | polygonio       |
/// | `API_KEY_ALPHAVANTAGE`    | alphavantage    |
/// | `API_KEY_USTREASURY`      | ustreasury      |
/// | `API_KEY_SECAPI`          | secapi          |
///
/// ## QuestDB
/// | Variable            | Description                                  | Default                       |
/// |---------------------|----------------------------------------------|-------------------------------|
/// | `QUESTDB_ADDR`      | `host:port` of the HTTP ingress              | `questdb.bounteer.com:9000`   |
/// | `QUESTDB_USER`      | Username (omit if auth disabled)             | —                             |
/// | `QUESTDB_PASSWORD`  | Password (omit if auth disabled)             | —                             |
///
/// ## AWS / Hyperliquid S3 (backfill endpoint)
/// | Variable                   | Description                        | Default              |
/// |----------------------------|------------------------------------|----------------------|
/// | `AWS_ACCESS_KEY_ID`        | AWS access key                     | —                    |
/// | `AWS_SECRET_ACCESS_KEY`    | AWS secret key                     | —                    |
/// | `AWS_REGION`               | AWS region                         | `ap-northeast-1`     |
/// | `HYPERLIQUID_S3_BUCKET`    | S3 bucket for historic data        | `hyperliquid-archive`|
#[derive(Debug, Clone)]
pub struct Config {
    runner: Vec<String>,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        let runners = env::var("RUNNERS").unwrap_or_default();
        let runner = runners
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        Self { runner }
    }

    /// Look up an API key for the given runner id.
    /// Reads `API_KEY_<ID>` from the environment (e.g. `API_KEY_COINMARKETCAP`).
    pub fn get_api_key(&self, id: &str) -> Result<String, eyre::Error> {
        let env_var = format!("API_KEY_{}", id.to_uppercase().replace('-', "_"));
        env::var(&env_var)
            .map_err(|_| eyre::eyre!("env var {} not set", env_var))
    }

    pub fn has_runner(&self, id: &str) -> bool {
        self.runner.iter().any(|r| r == id)
    }
}
