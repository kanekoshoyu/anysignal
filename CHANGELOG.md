# [signal](./README.md) changelog
> [TODO](./src/README.md)

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