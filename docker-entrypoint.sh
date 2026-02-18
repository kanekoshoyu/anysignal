#!/bin/sh
# All configuration is read from environment variables at runtime.
# Set these in the Coolify "Environment Variables" UI:
#
#   RUNNERS                  Comma-separated runners: api,coinmarketcap,newsapi,polygonio
#   API_KEY_COINMARKETCAP
#   API_KEY_NEWSAPI
#   API_KEY_YOUTUBE_DATA_V3
#   API_KEY_POLYGONIO
#   API_KEY_ALPHAVANTAGE
#   API_KEY_USTREASURY
#   API_KEY_SECAPI
#   AWS_ACCESS_KEY_ID
#   AWS_SECRET_ACCESS_KEY
#   AWS_REGION               (default: ap-northeast-1)
#   HYPERLIQUID_S3_BUCKET    (default: hyperliquid-archive)
#   QUESTDB_ADDR             host:port  e.g. questdb.example.com:9000
#   QUESTDB_USER             (optional)
#   QUESTDB_PASSWORD         (optional)
set -e
exec /app/anysignal
