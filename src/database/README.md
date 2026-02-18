# Database (QuestDB)

## Design philosophy

QuestDB is highly flexible and easy to maintain, but schema changes need to be tracked explicitly.
We version the schema in `schema_questdb.json` and avoid ORMs — direct ILP ingress and raw SQL queries keep things simple.

---

## Tables

### `market_data`

Generic time-series table for all numeric market metrics. One row = one (timestamp, metric, ticker) tuple.

| Column     | Type      | Notes                              |
|------------|-----------|------------------------------------|
| `ts`       | TIMESTAMP | Designated timestamp (microseconds)|
| `category` | SYMBOL    | Metric name (see categories below) |
| `ticker`   | SYMBOL    | Asset symbol, e.g. `SOL`, `BTC`    |
| `source`   | SYMBOL    | Data source identifier             |
| `value`    | DOUBLE    | Metric value                       |

**Known categories**

| Category        | Description                        | Source(s)         |
|-----------------|------------------------------------|-------------------|
| `open_interest` | Open interest (contracts)          | `HYPERLIQUID_S3`  |
| `funding`       | Hourly funding rate                | `HYPERLIQUID_S3`  |
| `mark_px`       | Mark price                         | `HYPERLIQUID_S3`  |
| `oracle_px`     | Oracle price                       | `HYPERLIQUID_S3`  |
| `prev_day_px`   | Previous day close price           | `HYPERLIQUID_S3`  |
| `day_ntl_vlm`   | Daily notional volume (USD)        | `HYPERLIQUID_S3`  |
| `mid_px`        | Mid price (omitted when absent)    | `HYPERLIQUID_S3`  |
| `premium`       | Premium (omitted when absent)      | `HYPERLIQUID_S3`  |

**Example query — SOL open interest over time:**
```sql
SELECT ts, value
FROM market_data
WHERE ticker = 'SOL'
  AND category = 'open_interest'
  AND source = 'HYPERLIQUID_S3'
ORDER BY ts;
```

---

### `market_event`

Boolean market events (e.g. price breakout up/down).

| Column      | Type      | Notes                              |
|-------------|-----------|------------------------------------|
| `ts`        | TIMESTAMP | Designated timestamp               |
| `category`  | SYMBOL    | Event type                         |
| `ticker`    | SYMBOL    | Asset symbol                       |
| `source`    | SYMBOL    | Data source                        |
| `is_rising` | BOOLEAN   | Direction of the event             |

---

### `market_signal`

Categorical/text signal outputs.

| Column     | Type      | Notes            |
|------------|-----------|------------------|
| `ts`       | TIMESTAMP | Designated timestamp |
| `category` | SYMBOL    |                  |
| `ticker`   | SYMBOL    |                  |
| `source`   | SYMBOL    |                  |
| `value`    | SYMBOL    | Signal label     |

---

## Hyperliquid S3 ingestion

Historic data is fetched from the public Hyperliquid S3 bucket (`hyperliquid-archive`) via `src/adapter/hyperliquid_s3/`.

### `asset_ctxs` → `market_data`

Files at `asset_ctxs/YYYY-MM-DD.csv.lz4` contain per-coin snapshots. Each row is fanned out into N `market_data` rows — one per metric — by `insert_asset_ctxs` in `src/database/mod.rs`.

| CSV field       | `market_data.category` | Notes                     |
|-----------------|------------------------|---------------------------|
| `openInterest`  | `open_interest`        |                           |
| `funding`       | `funding`              |                           |
| `markPx`        | `mark_px`              |                           |
| `oraclePx`      | `oracle_px`            |                           |
| `prevDayPx`     | `prev_day_px`          |                           |
| `dayNtlVlm`     | `day_ntl_vlm`          |                           |
| `midPx`         | `mid_px`               | Row omitted when absent   |
| `premium`       | `premium`              | Row omitted when absent   |

`ticker` = coin name from the `coin` CSV column.
`source` = `HYPERLIQUID_S3`.
`ts` = `time` field (Unix ms) converted to microseconds.
