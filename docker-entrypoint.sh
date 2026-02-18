#!/bin/sh
# docker-entrypoint.sh
#
# Generates config.toml from environment variables at container startup,
# then exec's the main binary.
#
# All variables below can be set in Coolify's "Environment Variables" UI.
# The binary also reads AWS_* and QUESTDB_* vars directly from the environment
# (via dotenvy), so those do not need to appear in config.toml.
#
# Required / common variables
# ───────────────────────────
# RUNNERS                  Comma-separated list of runners to enable.
#                          Available: api, coinmarketcap, newsapi, polygonio
#                          Example: "api,coinmarketcap"
#                          Default: "api"
#
# API_KEY_COINMARKETCAP    CoinMarketCap API key
# API_KEY_NEWSAPI          NewsAPI key
# API_KEY_YOUTUBE_DATA_V3  YouTube Data v3 key
# API_KEY_POLYGONIO        Polygon.io key
# API_KEY_ALPHAVANTAGE     Alpha Vantage key
# API_KEY_USTREASURY       US Treasury key (leave blank if unused)
# API_KEY_SECAPI           SEC API key (leave blank if unused)
#
# AWS / Hyperliquid S3 (used by the backfill endpoint)
# ────────────────────────────────────────────────────
# AWS_ACCESS_KEY_ID
# AWS_SECRET_ACCESS_KEY
# AWS_REGION               Default: ap-northeast-1
# HYPERLIQUID_S3_BUCKET    Default: hyperliquid-archive
#
# QuestDB
# ───────
# QUESTDB_ADDR             host:port, e.g. questdb.example.com:9000
# QUESTDB_USER             Optional
# QUESTDB_PASSWORD         Optional

set -e

RUNNERS="${RUNNERS:-api}"

# Convert "api,coinmarketcap" → "api", "coinmarketcap"  (valid TOML inline array elements)
RUNNER_TOML=$(printf '%s' "$RUNNERS" | tr -d ' ' | sed 's/,/", "/g' | sed 's/^/"/' | sed 's/$/"/')

cat > /app/config.toml << TOML
runner = [${RUNNER_TOML}]

[[api_keys]]
id = "coinmarketcap"
key = "${API_KEY_COINMARKETCAP:-}"

[[api_keys]]
id = "newsapi"
key = "${API_KEY_NEWSAPI:-}"

[[api_keys]]
id = "youtube_data_v3"
key = "${API_KEY_YOUTUBE_DATA_V3:-}"

[[api_keys]]
id = "polygonio"
key = "${API_KEY_POLYGONIO:-}"

[[api_keys]]
id = "alphavantage"
key = "${API_KEY_ALPHAVANTAGE:-}"

[[api_keys]]
id = "ustreasury"
key = "${API_KEY_USTREASURY:-}"

[[api_keys]]
id = "secapi"
key = "${API_KEY_SECAPI:-}"
TOML

echo "Generated config.toml with runners: $RUNNERS"

exec /app/anysignal
