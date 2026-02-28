# AnySignal — Claude Code Notes

## Project overview
Rust async service that backfills and streams trading signals/market data into **QuestDB** (timeseries DB). Deployed via Docker on Coolify; all config is env-var only via `Config::from_env()`.

## Key architecture
- **`src/main.rs`** — spawns tokio tasks for each enabled runner (`RUNNERS=api,coinmarketcap,…`)
- **`src/config.rs`** — `Config` struct (`questdb_addr`, etc.); passed by reference everywhere — never read env vars directly in business logic
- **`src/adapter/`** — one sub-module per data source; implement `DataSource` / `HistoricDataSource` traits
- **`src/database/mod.rs`** — QuestDB ILP writers (batch via `Buffer`, flush via `Sender`)
- **`src/api/rest/endpoint.rs`** — Poem/OpenAPI REST; `GET /backfill` is the main entry point

## Adding a new backfill source — checklist
1. Create `src/adapter/hyperliquid_s3/<name>.rs`; `new()` async constructor (see `asset_ctxs.rs` / `node_fills_by_block.rs`)
2. Add row type + writer to `database/mod.rs`: `insert_<name>(sender, rows) -> QuestResult<usize>` with 64 MiB flush threshold
3. Create `src/backfill/<name>.rs`; implement `PartitionKey` + `PartitionedSource`
   - `partition_exists` — SQL count check; return `Ok(false)` on any DB error
   - `ingest_partition` — fetch → transform → insert; return `PartitionStats { rows, fetch_ms, insert_ms }`
   - Do **not** emit `tracing::info!` — `run_backfill` logs per-partition timing from `PartitionStats`
4. Register `pub mod <name>` in `src/backfill/mod.rs`
5. Add `BackfillSource` variant + match arm in `endpoint.rs` (hour-by-hour or day-by-day key iterator)
6. Unit tests (no network) + `#[ignore]` integration test

## Hyperliquid S3 — critical details

### `hyperliquid-archive` bucket (us-east-1, requester-pays)
| Dataset | Key pattern | LZ4 format | Date fmt | Hour fmt |
|---|---|---|---|---|
| asset_ctxs | `asset_ctxs/{YYYYMMDD}.csv.lz4` | frame (`lz4::Decoder`) | `YYYYMMDD` | — |
| L2 orderbook | `market_data/{YYYYMMDD}/{H}/l2Book/{coin}.lz4` | frame (`lz4::Decoder`) | `YYYYMMDD` | unpadded `0`–`23` |

- Always pass `.request_payer(RequestPayer::Requester)`
- L2 wire format (NDJSON per line): `{"time":"…","ver_num":1,"raw":{"channel":"l2Book","data":{"coin":"…","time":<ms>,"levels":[[bids…],[asks…]]}}}`
- `asset_ctxs` CSV: snake_case headers; `time` is `chrono::DateTime<Utc>` (ISO 8601)

### `hl-mainnet-node-data` bucket (ap-northeast-1, requester-pays)
| Dataset | Key pattern | LZ4 format | Hour fmt |
|---|---|---|---|
| node fills by block | `node_fills_by_block/hourly/{YYYYMMDD}/{H}.lz4` | frame (`lz4::Decoder`) | unpadded `0`–`23` |

- Wire format: NDJSON, one line per block — `{"events":[["<wallet>", {fill…}], …]}`
- `fill.side`: `"B"` → `"buy"`, `"A"` → `"sell"`; `fill.dir` stored as `category`
- **Hour boundary spillover:** S3 files are keyed by block *processing* time, not fill timestamp.
  When aggregating by minute, always fetch H-1 and H+1 and filter to `[hour_start_ms, hour_end_ms)`.

## REST endpoints

| Endpoint | Description |
|---|---|
| `GET /` | Health check |
| `GET /version` | Semver string |
| `GET /database` | Disk usage + row counts for all QuestDB tables |
| `GET /backfill/status` | List active backfill jobs (empty array when idle) |
| `GET /backfill` | Trigger historic backfill (see params below) |

## Backfill API design
- `GET /backfill?from=2024-01-01T00:00:00&to=2024-01-07T23:00:00&source=…&force=false`
- `GET /backfill/status` — list all active backfill jobs (empty array when idle)
- `from`/`to` are **`NaiveDateTime`**; date-only strings treated as `T00:00:00`
- All sources check QuestDB before fetching (dedup); `force=true` bypasses the check
- Concurrent requests for the same source+partition are deduplicated: the second request skips keys already being indexed (`"already being indexed"` in `keys_skipped`)

| Source | Steps | Table | Available from |
|---|---|---|---|
| `HyperliquidAssetCtxs` | daily | `market_data` | 2023-05-20 |
| `HyperliquidL2Orderbook` | hourly | `l2_orderbook` | 2023-04-15 |
| `HyperliquidNodeFills` | hourly | `hyperliquid_fill` | 2025-07-27 |
| `HyperliquidNodeFills1mAggregate` | hourly | `hyperliquid_fill_1m_aggregate` | 2025-07-27 |

### `/database` response fields
| Field | Description |
|---|---|
| `tables[].name` | Table name |
| `tables[].row_count` | Total rows across all partitions |
| `tables[].disk_size_bytes` | Total disk usage in bytes |
| `total_disk_size_bytes` | Sum of all tables' disk usage |

### `/backfill/status` response fields
| Field | Description |
|---|---|
| `id` | Unique job ID |
| `source` | Source name (e.g. `HyperliquidNodeFills`) |
| `ongoing` | Partition keys currently being processed concurrently |
| `started_at` | RFC 3339 UTC timestamp of when the backfill request was received |
| `elapsed_ms` | Wall-clock milliseconds since the backfill request was received |

## QuestDB patterns
- Timestamps in **microseconds** (`ms * 1_000`, `chrono::DateTime::timestamp_micros()`)
- `SYMBOL` → `.symbol()`, `DOUBLE` → `.column_f64()`, `LONG` → `.column_i64()`
- Flush buffer every **64 MiB** (`BUFFER_FLUSH_THRESHOLD`) — daily data can exceed QuestDB's 100 MiB cap

## Key table schemas

```sql
CREATE TABLE hyperliquid_fill (
    ts             TIMESTAMP,
    coin           SYMBOL,
    wallet         SYMBOL,
    side           SYMBOL,   -- 'buy' | 'sell'
    category       SYMBOL,   -- 'Buy' | 'Sell' | 'Open Long' | 'Close Long' | …
    source         SYMBOL,   -- 'HYPERLIQUID_NODE'
    is_taker       BOOLEAN,
    price          DOUBLE,
    quantity       DOUBLE,
    position_before DOUBLE,
    realized_pnl   DOUBLE
) timestamp(ts) PARTITION BY DAY;

CREATE TABLE hyperliquid_fill_1m_aggregate (
    ts          TIMESTAMP,  -- left-closed minute bucket: 12:00 covers [12:00, 12:01)
    coin        SYMBOL,
    category    SYMBOL,
    buy_side    BOOLEAN,
    quantity    DOUBLE,
    trade_count LONG
) timestamp(ts) PARTITION BY DAY;
```

## Version bumping
When asked to bump the version:
1. Update the version in `Cargo.toml`
2. Add a `CHANGELOG.md` entry (create if missing) — include date, new version, and bullet points summarising what changed
3. Audit `CLAUDE.md` for completeness — update any tables (sources, endpoints, schemas) to reflect the new functionality

## Clippy lints
`unwrap_used`, `expect_used`, `panic` are **deny** — use `?` in production; `unwrap()` only inside `#[cfg(test)]`.

## Running integration tests
```bash
cargo test -- --ignored --nocapture   # requires .env with real AWS creds
```
