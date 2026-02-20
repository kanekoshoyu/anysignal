# [signal](./README.md) changelog
> [TODO](./src/README.md)

## [0.3.2]
### Added
- Hyperliquid L2 orderbook backfill (`HyperliquidL2Orderbook` source on `GET /backfill`)
  - `from`/`to` query params use `NaiveDateTime` (hour precision, e.g. `2024-01-01T06:00:00`)
  - Iterates hour-by-hour over the datetime range; dedup check per (coin × hour)
  - Fetches `s3://hyperliquid-archive/market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4`
  - Inserts into `l2_snapshot` table (ts, ticker, side, level, price, quantity)
  - `coins` query param required (comma-separated, e.g. `BTC,ETH`)
- `insert_l2_snapshots` DB writer with 64 MiB flush threshold
- `l2_snapshot_coin_hour_exists` dedup check (per coin × hour)
- Unit + integration tests verified against real S3 data (22 005 BTC snapshots)

## [0.3.1]
### Fixed
- QuestDB buffer overflow on large ingestions: flush every 64 MiB mid-loop
  so a full day of asset_ctxs (~192 MiB) no longer exceeds the 100 MiB cap

## [0.2.1]
### Added
- (WIP) polygon stock price indexer

## [0.2.0]
### Added
- questdb insert, select, insert_unique signals
- questdb row schema in table.rs

## [0.1.3]
### Added
- questdb sample insertion function

## [0.1.2]
### Added
- news fetcher

## [0.1.1]
### Added
- signal, isntance, strategy models
- adapter module

## [0.1.1]
### Added
- REST endpoint
- custom error
- TOML config
- runner management
- coinmarketcap runner

## [0.1.0]
### Added
- hello world
