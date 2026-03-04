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
| Dataset | Key pattern | LZ4 format | Hour fmt | Available | Implemented |
|---|---|---|---|---|---|
| node trades | `node_trades/hourly/{YYYYMMDD}/{H}.lz4` | frame (`lz4::Decoder`) | unpadded `0`–`23` | 2025-03-22T10:00 – 2025-05-25T13:00 | ❌ skipped |
| node fills (legacy) | `node_fills/hourly/{YYYYMMDD}/{H}.lz4` | frame (`lz4::Decoder`) | unpadded `0`–`23` | 2025-05-25T14:00 – 2025-07-27T08:00 | ✅ `HyperliquidNodeFillsLegacy` |
| node fills by block | `node_fills_by_block/hourly/{YYYYMMDD}/{H}.lz4` | frame (`lz4::Decoder`) | unpadded `0`–`23` | 2025-07-27T08:00 – present | ✅ `HyperliquidNodeFills` |

**`node_trades` wire format (not implemented):** NDJSON, one trade-level object per line.
Each record represents a single matched trade with both participants in `side_info[0/1]`.
Missing fields vs `hyperliquid_fill`: no `category` (Open Long / Close Short / etc.) and no `realized_pnl`.
This makes it incompatible with the fill-level schema — **coverage gap: 2025-03-22T10:00 – 2025-05-25T13:00**.

```json
{"coin":"AAVE","side":"B","time":"2025-05-25T13:59:59.974190721","px":"264.56","sz":"0.06",
 "hash":"0x84a0…","trade_dir_override":"Na",
 "side_info":[{"user":"0xdbe2…","start_pos":"-153.07","oid":97125228063,"twap_id":null,"cloid":"0xa92d…"},
              {"user":"0xf1d4…","start_pos":"-278.05","oid":97125145866,"twap_id":null,"cloid":"0xf800…"}]}
```

- `trade_dir_override`: `"Na"` | `"LiquidatedMarket"` | `"NetChildVaultPositions"`
- `side_info` always has exactly 2 entries (index 0 = taker, index 1 = maker)
- Timestamp is ISO 8601 with nanoseconds — **not** Unix milliseconds

**`node_fills` (legacy) wire format:** NDJSON, one line per fill — `["<wallet>", {fill…}]`

**`node_fills_by_block` wire format:** NDJSON, one line per block — `{"events":[["<wallet>", {fill…}], …]}`

- `fill.side`: `"B"` → `"buy"`, `"A"` → `"sell"`; `fill.dir` stored as `category`
- Both fill datasets write `source = 'HYPERLIQUID_NODE'`; the `partition_exists` dedup check covers both
- **Overlap:** 2025-07-27 hour 8 exists in both fill datasets — whichever is backfilled first wins
- **Hour boundary spillover (`node_fills_by_block` only):** S3 files are keyed by block *processing* time, not fill timestamp.
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

| Source | Steps | Table | Available from | Available to |
|---|---|---|---|---|
| `HyperliquidAssetCtxs` | daily | `market_data` | 2023-05-20 | present |
| `HyperliquidL2Orderbook` | hourly | `l2_orderbook` | 2023-04-15 | present |
| `HyperliquidNodeFillsLegacy` | hourly | `hyperliquid_fill` | 2025-05-25T14:00 | 2025-07-27T08:00 |
| `HyperliquidNodeFillsLegacy1mAggregate` | hourly | `hyperliquid_fill_1m_aggregate` | 2025-05-25T14:00 | 2025-07-27T08:00 |
| `HyperliquidNodeFills` | hourly | `hyperliquid_fill` | 2025-07-27T08:00 | present |
| `HyperliquidNodeFills1mAggregate` | hourly | `hyperliquid_fill_1m_aggregate` | 2025-07-27 | present |
| `MarketState1m` | hourly | `market_state_1m` | 2025-05-25T14:00 | present |

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
-- ts is a POINT-IN-TIME SNAPSHOT: 12:00:00 = market state observed AT 12:00:00.
-- Tall/normalised format: one row per (ts, ticker, category/metric).
CREATE TABLE market_data (
    ts       TIMESTAMP,  -- point-in-time snapshot, NOT a window start
    category SYMBOL,     -- metric name: 'funding', 'mark_px', 'oracle_px', …
    ticker   SYMBOL,
    source   SYMBOL,     -- 'HYPERLIQUID_S3'
    value    DOUBLE
) timestamp(ts) PARTITION BY DAY;

CREATE TABLE hyperliquid_fill (
    ts             TIMESTAMP,  -- exact fill timestamp (point-in-time)
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

-- ts is the LEFT-CLOSED WINDOW START: 12:00:00 covers fills in [12:00:00, 12:01:00).
-- NOTE: semantics differ from market_data.ts (snapshot) vs fill_1m_aggregate.ts (window open).
-- When joining: market_data.ts = fill_1m_agg.ts gives the price AT the start of the minute
-- in which those fills occurred. The fill data covers up to 12:00:59.999 for that ts.
CREATE TABLE hyperliquid_fill_1m_aggregate (
    ts          TIMESTAMP,  -- left-closed minute bucket: 12:00 covers [12:00, 12:01)
    coin        SYMBOL,
    category    SYMBOL,
    buy_side    BOOLEAN,
    quantity    DOUBLE,
    trade_count LONG
) timestamp(ts) PARTITION BY DAY;

-- ts is the LEFT-CLOSED WINDOW START (same as hyperliquid_fill_1m_aggregate).
-- Computed by MarketState1m backfill source from market_data + hyperliquid_fill_1m_aggregate.
-- Liquidation categories: 'Liquidated Isolated Long', 'Liquidated Cross Long',
--                         'Liquidated Isolated Short', 'Liquidated Cross Short'
-- trade_volume / trade_count count buy-side fills only (buy/sell pairs cancel out).
CREATE TABLE market_state_1m (
    ts                       TIMESTAMP,  -- left-closed minute bucket
    coin                     SYMBOL,
    price_oracle             DOUBLE,     -- from market_data.oracle_px (daily snapshot)
    price_mark               DOUBLE,     -- from market_data.mark_px (daily snapshot)
    price_mid                DOUBLE,     -- market_data.mid_px or (oracle+mark)/2
    open_interest            DOUBLE,     -- from market_data.open_interest
    funding_rate             DOUBLE,     -- from market_data.funding
    volume_24h_usd           DOUBLE,     -- from market_data.day_ntl_vlm
    trade_volume             DOUBLE,     -- sum of buy-side fill quantities
    trade_count              LONG,       -- count of buy-side fills
    liquidation_long_volume  DOUBLE,
    liquidation_short_volume DOUBLE,
    liquidation_long_count   LONG,
    liquidation_short_count  LONG
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
