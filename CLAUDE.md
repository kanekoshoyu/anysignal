# AnySignal — Claude Code Notes

## Project overview
Rust async service that backfills and streams trading signals/market data into **QuestDB** (timeseries DB). Deployed via Docker on Coolify; all config is env-var only via `Config::from_env()`.

## Key architecture
- **`src/main.rs`** — spawns tokio tasks for each enabled runner (`RUNNERS=api,coinmarketcap,…`)
- **`src/config.rs`** — `Config` struct (`questdb_addr`, `s3_bucket`, `aws_region`, etc.); passed by reference everywhere — never read env vars directly in business logic
- **`src/adapter/`** — one sub-module per data source; implement `DataSource` / `HistoricDataSource` traits
- **`src/database/mod.rs`** — QuestDB ILP writers (batch via `Buffer`, flush via `Sender`)
- **`src/api/rest/endpoint.rs`** — Poem/OpenAPI REST; `GET /backfill` is the main entry point

## Adding a new backfill source — checklist
1. Create `src/adapter/hyperliquid_s3/<name>.rs`; `new(config: &Config)` pattern (see `asset_ctxs.rs` / `market_data.rs`)
2. Add dedup-check to `database/mod.rs`: `<name>_exists(config, …) -> AnySignalResult<bool>`
3. Add writer to `database/mod.rs`: `insert_<name>(sender, rows) -> QuestResult<usize>` with 64 MiB flush threshold
4. Add `BackfillSource` variant + match arm in `endpoint.rs` (datetime-based iteration, dedup + `force` flag)
5. Unit tests (no network) + `#[ignore]` integration test

## Hyperliquid S3 — critical details
| Dataset | Key pattern | LZ4 format | Date fmt | Hour fmt |
|---|---|---|---|---|
| asset_ctxs | `asset_ctxs/{YYYYMMDD}.csv.lz4` | **frame** (`lz4::Decoder`) | `YYYYMMDD` | — |
| L2 orderbook | `market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4` | **frame** (`lz4::Decoder`) | `YYYYMMDD` | unpadded `0`–`23` |

- Bucket: **`hyperliquid-archive`**, region: **`us-east-1`**
- Bucket is **requester-pays** — always pass `.request_payer(RequestPayer::Requester)`
- L2 wire format (NDJSON per line): `{"time":"…","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"…","time":<ms>,"levels":[[bids…],[asks…]]}}}`
- `asset_ctxs` CSV: snake_case headers; `time` is `chrono::DateTime<Utc>` (ISO 8601)

## Backfill API design
- `GET /backfill?from=2024-01-01T00:00:00&to=2024-01-07T23:00:00&source=…&force=false`
- `from`/`to` are **`NaiveDateTime`** (hour precision)
- `HyperliquidAssetCtxs` — steps **day-by-day** using only the date part of `from`/`to`
- `HyperliquidL2Orderbook` — steps **hour-by-hour**; requires `coins=BTC,ETH` query param
- Both sources check QuestDB before fetching (dedup); `force=true` bypasses the check

## QuestDB patterns
- Timestamps in **microseconds** (`ms * 1_000`, `chrono::DateTime::timestamp_micros()`)
- `SYMBOL` → `.symbol()`, `DOUBLE` → `.column_f64()`, `LONG` → `.column_i64()`
- Flush buffer every **64 MiB** (`BUFFER_FLUSH_THRESHOLD`) — daily data can exceed QuestDB's 100 MiB cap

## l2_snapshot table schema
```sql
CREATE TABLE l2_snapshot (
    ts       TIMESTAMP,
    ticker   SYMBOL,   -- coin ticker
    side     SYMBOL,   -- 'bid' or 'ask'
    level    INT,      -- 0-based price level index
    price    DOUBLE,
    quantity DOUBLE
) timestamp(ts) PARTITION BY DAY;
```

## Clippy lints
`unwrap_used`, `expect_used`, `panic` are **deny** — use `?` in production; `unwrap()` only inside `#[cfg(test)]`.

## Running integration tests
```bash
cargo test -- --ignored --nocapture   # requires .env with real AWS creds
```
