# AnySignal — Claude Code Notes

## Hyperliquid S3 Archive

- **Bucket**: `hyperliquid-archive` in `us-east-1`
- **Requester-pays**: all requests must include `.request_payer(RequestPayer::Requester)`
- **Date format in keys**: `YYYYMMDD` (no dashes), e.g. `asset_ctxs/20250101.csv.lz4`
- **Compression**: LZ4 **frame** format — use `lz4::Decoder` (not `lz4::block::decompress`)
- **CSV schema** (snake_case headers, not camelCase):
  ```
  time,coin,funding,open_interest,prev_day_px,day_ntl_vlm,premium,oracle_px,mark_px,mid_px,impact_bid_px,impact_ask_px
  ```
- **`time` column**: ISO 8601 string (`2025-01-01T00:00:00Z`) — deserialise as `chrono::DateTime<chrono::Utc>`
- **Optional fields**: `premium`, `mid_px`, `impact_bid_px`, `impact_ask_px` — may be empty string or `"null"`; use `de_opt_f64`
- **Illiquid coins**: optional price fields may be `Some(0.0)` (zero open interest / volume is valid)
- **Credentials**: personal IAM credentials from `.env` — `dotenvy::dotenv().ok()` must be called before `Config::from_env()` in ignored tests

## Testing

- Unit tests: `cargo test`
- Integration tests (requires `.env` with real AWS credentials): `cargo test -- --ignored`
- Ignored tests use `dotenvy::dotenv().ok()` to load `.env` before `Config::from_env()`

## Development workflow

- **Push format/encoding decisions into the function, not the caller.** If a function accepts a `&str` date, every caller must know the right format — and they will drift. Accept a typed value (`NaiveDate`, `DateTime`, etc.) and format internally. One place to change, compiler enforces all callers.
  - Example: `fetch_and_decompress` originally took `&str` — tests passed `"20250101"` correctly but `endpoint.rs` passed `"2025-01-01"` (wrong). Changing the signature to `NaiveDate` made the format a single `format!` inside the function and removed the class of bug entirely.
- **When changing a data format or key name, grep for all call sites** — passing tests do not guarantee the production path is fixed if callers format independently. After any format/schema fix run: `grep -rn "old_format_string" src/`.

## AWS SDK patterns

- Build S3 client via `aws_config::defaults(BehaviorVersion::latest()).region(region).load().await`
- The `AWS_REGION` env var is loaded from `.env`; default falls back to `us-east-1`
- Cross-account S3 access is denied even for IAM users unless the bucket policy allows it — requester-pays was the issue here, not cross-account restrictions
