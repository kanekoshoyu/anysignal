/// direct response of the response
pub mod response;
/// representation of each table
pub mod table;

use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxRow;
use crate::adapter::hyperliquid_s3::market_data::L2Snapshot;
use crate::adapter::hyperliquid_s3::node_fills_by_block::ParsedFill;
use crate::config::Config;
use crate::database::table::*;
use crate::error::AnySignalResult;
use crate::model::signal::{Signal, SignalData, SignalDataType, SignalInfo};
use questdb::ingress::{Buffer, Sender, TimestampMicros};
use questdb::Result as QuestResult;
use response::QuestDbResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

// batch insert using questdb ingress
pub fn insert_signal_db(
    sender: &mut Sender,
    signal_info: &SignalInfo,
    signals: &[Signal],
) -> QuestResult<()> {
    // do a query on existing data with the timestamp

    // batch store data into the buffer
    let mut buffer = Buffer::new();
    for signal in signals {
        let timestamp_us = signal.timestamp_us;
        match &signal.data {
            SignalData::Simple => buffer
                .table("signal_simple")?
                .column_i64("info_id", signal_info.id)?
                .at(TimestampMicros::new(timestamp_us))?,
            SignalData::Binary(value) => buffer
                .table("signal_binary")?
                .column_i64("info_id", signal_info.id)?
                .column_bool("value", *value)?
                .at(TimestampMicros::new(timestamp_us))?,
            SignalData::Scalar(value) => buffer
                .table("signal_scalar")?
                .column_i64("info_id", signal_info.id)?
                .column_f64("value", *value)?
                .at(TimestampMicros::new(timestamp_us))?,
            SignalData::Text(value) => buffer
                .table("signal_text")?
                .column_i64("info_id", signal_info.id)?
                .column_str("value", value)?
                .at(TimestampMicros::new(timestamp_us))?,
        }
    }
    sender.flush(&mut buffer)?;
    Ok(())
}

// select via query on http
pub async fn select_signal_db(
    signal_data_type: SignalDataType,
    query_string: impl AsRef<str>,
) -> AnySignalResult<Vec<Signal>> {
    let client = reqwest::Client::new();
    let response = client
        .get("http://localhost:9000/exec")
        .query(&[("query", query_string.as_ref())])
        .send()
        .await?;
    let text = response.text().await?;

    // TODO select type based on the provided signal_data_type
    let signals = match signal_data_type {
        SignalDataType::Simple => {
            let result: QuestDbResponse<SignalSimpleRow> = serde_json::from_str(&text)?;
            result
                .dataset
                .into_iter()
                .map(|i| Signal {
                    info_id: i.info_id,
                    data: SignalData::Simple,
                    timestamp_us: i.timestamp.timestamp_micros(),
                })
                .collect()
        }
        SignalDataType::Binary => {
            let result: QuestDbResponse<SignalBooleanRow> = serde_json::from_str(&text)?;
            result
                .dataset
                .into_iter()
                .map(|i| Signal {
                    info_id: i.info_id,
                    data: SignalData::Binary(i.value),
                    timestamp_us: i.timestamp.timestamp_micros(),
                })
                .collect()
        }
        SignalDataType::Scalar => {
            let result: QuestDbResponse<SignalScalarRow> = serde_json::from_str(&text)?;
            result
                .dataset
                .into_iter()
                .map(|i| Signal {
                    info_id: i.info_id,
                    data: SignalData::Scalar(i.value),
                    timestamp_us: i.timestamp.timestamp_micros(),
                })
                .collect()
        }
        SignalDataType::Text => {
            let result: QuestDbResponse<SignalStringRow> = serde_json::from_str(&text)?;
            result
                .dataset
                .into_iter()
                .map(|i| Signal {
                    info_id: i.info_id,
                    data: SignalData::Text(i.value),
                    timestamp_us: i.timestamp.timestamp_micros(),
                })
                .collect()
        }
    };
    // TODO implement the reconstruction of signal from the questdb
    Ok(signals)
}

// syntatic sugar
// provide skip fn, so we insert only needed ones
pub async fn insert_unique_signal_db(
    sender: &mut Sender,
    signal_info: &SignalInfo,
    signals: &[Signal],
    skip_query_string: impl AsRef<str>,
) -> AnySignalResult<usize> {
    let signals_to_skip = select_signal_db(signal_info.data_type, skip_query_string).await?;
    let signals_to_skip: HashSet<Signal> = signals_to_skip.into_iter().collect();

    let mut signals = signals.to_vec();
    signals.retain(|i| signals_to_skip.contains(i));
    let len = signals.len();
    if !signals.is_empty() {
        insert_signal_db(sender, signal_info, &signals)?;
    }
    Ok(len)
}
// ---------------------------------------------------------------------------
// QuestDB connection factory
// ---------------------------------------------------------------------------

/// Build a [`Sender`] from environment variables.
///
/// Required env:
/// - `QUESTDB_ADDR` — `host:port` of the QuestDB HTTP ingress (port 9000)
///
/// Optional env (leave unset when auth is disabled):
/// - `QUESTDB_USER`
/// - `QUESTDB_PASSWORD`
pub fn questdb_sender(config: &Config) -> QuestResult<Sender> {
    let conf = match (&config.questdb_user, &config.questdb_password) {
        (Some(user), Some(pass)) => format!(
            "http::addr={};username={};password={};",
            config.questdb_addr, user, pass
        ),
        _ => format!("http::addr={};", config.questdb_addr),
    };

    Sender::from_conf(conf)
}

// ---------------------------------------------------------------------------
// QuestDbClient — combined HTTP query + ILP sender handle
// ---------------------------------------------------------------------------

/// Bundles the QuestDB HTTP query address and an ILP [`Sender`] into a single
/// handle that can be passed to [`PartitionedSource`] implementations.
pub struct QuestDbClient {
    pub addr: String,
    sender: std::sync::Mutex<Sender>,
}

impl QuestDbClient {
    /// Build a client from the application [`Config`].
    pub fn new(config: &Config) -> QuestResult<Self> {
        let sender = questdb_sender(config)?;
        Ok(Self {
            addr: config.questdb_addr.clone(),
            sender: std::sync::Mutex::new(sender),
        })
    }

    /// Execute a closure with exclusive access to the ILP [`Sender`].
    /// The closure must be synchronous — do not await inside it.
    pub fn with_sender<F, T>(&self, f: F) -> QuestResult<T>
    where
        F: FnOnce(&mut Sender) -> QuestResult<T>,
    {
        let mut guard = self.sender.lock().unwrap_or_else(|p| p.into_inner());
        f(&mut guard)
    }

    /// Run an arbitrary SQL query via the QuestDB HTTP `/exec` endpoint and
    /// return the raw JSON response body.
    pub async fn query_json(&self, sql: &str) -> AnySignalResult<serde_json::Value> {
        let url = format!("http://{}/exec", self.addr);
        let json: serde_json::Value = reqwest::Client::new()
            .get(&url)
            .query(&[("query", sql)])
            .send()
            .await?
            .json()
            .await?;
        Ok(json)
    }

    /// Run a `SELECT count()` SQL query via the QuestDB HTTP `/exec` endpoint
    /// and return the first cell as an `i64`.
    ///
    /// Returns `Ok(0)` on any QuestDB-level error (e.g. table not yet created)
    /// so callers can treat a missing table as "no data present".
    pub async fn count(&self, sql: &str) -> AnySignalResult<i64> {
        let url = format!("http://{}/exec", self.addr);
        let json: serde_json::Value = reqwest::Client::new()
            .get(&url)
            .query(&[("query", sql)])
            .send()
            .await?
            .json()
            .await?;
        if let Some(err) = json.get("error").and_then(|v| v.as_str()) {
            tracing::warn!(sql, questdb_error = err, "QuestDB count query failed; treating as 0");
            return Ok(0);
        }
        let count = json["dataset"][0][0].as_i64().unwrap_or(0);
        tracing::debug!(sql, count, "QuestDB count");
        Ok(count)
    }
}

// ---------------------------------------------------------------------------
// Hyperliquid asset_ctxs ingestion
// ---------------------------------------------------------------------------

/// Flush the buffer when it exceeds this size (64 MiB), well below QuestDB's
/// default 100 MiB cap.  A single day of asset_ctxs data is ~192 MiB
/// uncompressed, so without chunking the flush always fails.
const BUFFER_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024;

/// Batch-insert a slice of [`AssetCtxRow`]s into the `market_data` table.
///
/// Each source row is fanned out into one `market_data` row per metric so the
/// generic (ts, category, ticker, source, value) schema is preserved.
///
/// Metrics written per row:
///   open_interest, funding, mark_px, oracle_px, prev_day_px, day_ntl_vlm,
///   mid_px (skipped when None), premium (skipped when None).
///
/// The buffer is flushed automatically whenever it reaches
/// [`BUFFER_FLUSH_THRESHOLD`], so callers never need to worry about the
/// QuestDB maximum-buffer-size limit regardless of input size.
pub fn insert_asset_ctxs(sender: &mut Sender, rows: &[AssetCtxRow]) -> QuestResult<()> {
    let mut buffer = Buffer::new();

    for row in rows {
        let ts_us = TimestampMicros::new(row.time.timestamp_micros());

        let metrics: &[(&str, f64)] = &[
            ("open_interest", row.open_interest),
            ("funding", row.funding),
            ("mark_px", row.mark_px),
            ("oracle_px", row.oracle_px),
            ("prev_day_px", row.prev_day_px),
            ("day_ntl_vlm", row.day_ntl_vlm),
        ];

        for (category, value) in metrics {
            buffer
                .table("market_data")?
                .symbol("category", category)?
                .symbol("ticker", &row.coin)?
                .symbol("source", "HYPERLIQUID_S3")?
                .column_f64("value", *value)?
                .at(ts_us)?;
        }

        if let Some(v) = row.mid_px {
            buffer
                .table("market_data")?
                .symbol("category", "mid_px")?
                .symbol("ticker", &row.coin)?
                .symbol("source", "HYPERLIQUID_S3")?
                .column_f64("value", v)?
                .at(ts_us)?;
        }

        if let Some(v) = row.premium {
            buffer
                .table("market_data")?
                .symbol("category", "premium")?
                .symbol("ticker", &row.coin)?
                .symbol("source", "HYPERLIQUID_S3")?
                .column_f64("value", v)?
                .at(ts_us)?;
        }

        if buffer.len() >= BUFFER_FLUSH_THRESHOLD {
            sender.flush(&mut buffer)?;
        }
    }

    // Flush any rows that didn't fill a full chunk.
    if !buffer.is_empty() {
        sender.flush(&mut buffer)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Hyperliquid L2 orderbook ingestion
// ---------------------------------------------------------------------------

/// Flush the L2 buffer when it exceeds 64 MiB — well below QuestDB's 100 MiB cap.
const L2_BUFFER_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024;

/// Batch-insert L2 orderbook snapshots into the `l2_orderbook` table.
///
/// Each [`L2Snapshot`] expands to one row per price level per side:
///   - `levels[0]` → side `"bid"`, level index 0, 1, 2, …
///   - `levels[1]` → side `"ask"`, level index 0, 1, 2, …
///
/// The buffer is flushed automatically at [`L2_BUFFER_FLUSH_THRESHOLD`].
/// Returns the total number of rows written.
pub fn insert_l2_snapshots(sender: &mut Sender, snapshots: &[L2Snapshot]) -> QuestResult<usize> {
    let mut buffer = Buffer::new();
    let mut rows: usize = 0;

    for snapshot in snapshots {
        let ts_us = TimestampMicros::new(snapshot.time_ms() * 1_000); // ms → µs

        for (side_idx, side_str) in [(0usize, "bid"), (1usize, "ask")] {
            for (level_idx, level) in snapshot.levels()[side_idx].iter().enumerate() {
                let price: f64 = level.px.parse().unwrap_or(0.0);
                let quantity: f64 = level.sz.parse().unwrap_or(0.0);

                buffer
                    .table("l2_orderbook")?
                    .symbol("source", "HYPERLIQUID_S3")?
                    .symbol("ticker", snapshot.coin())?
                    .symbol("side", side_str)?
                    .column_i64("level", level_idx as i64)?
                    .column_f64("price", price)?
                    .column_f64("quantity", quantity)?
                    .at(ts_us)?;

                rows += 1;
            }
        }

        if buffer.len() >= L2_BUFFER_FLUSH_THRESHOLD {
            sender.flush(&mut buffer)?;
        }
    }

    if !buffer.is_empty() {
        sender.flush(&mut buffer)?;
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Hyperliquid 1-minute aggregate row
// ---------------------------------------------------------------------------

/// One row in `hyperliquid_fill_1m_aggregate`.
///
/// The timestamp (`minute_ms`) is the **left-closed** boundary of the
/// `[minute, minute+1)` bucket, stored as Unix milliseconds.
#[derive(Debug)]
pub struct Fill1mAggregate {
    /// Start of the minute bucket in Unix milliseconds
    /// (e.g. `12:00:00.000` for the `[12:00, 12:01)` bucket).
    pub minute_ms: i64,
    pub coin: String,
    /// Trade direction from the raw fill (e.g. `"Buy"`, `"Sell"`, `"Open Long"`).
    pub category: String,
    /// `true` when the fill side is `"buy"`.
    pub buy_side: bool,
    /// Sum of fill quantities within this bucket.
    pub quantity: f64,
    /// Number of individual fills in this bucket.
    pub trade_count: i64,
}

// ---------------------------------------------------------------------------
// Hyperliquid node fills ingestion
// ---------------------------------------------------------------------------

/// Batch-insert a slice of [`ParsedFill`]s into the `hyperliquid_fill` table.
///
/// The buffer is flushed automatically at [`BUFFER_FLUSH_THRESHOLD`].
/// Returns the total number of rows written.
pub fn insert_hyperliquid_fills(sender: &mut Sender, fills: &[ParsedFill]) -> QuestResult<usize> {
    let mut buffer = Buffer::new();
    let mut rows: usize = 0;

    for fill in fills {
        let ts_us = TimestampMicros::new(fill.time_ms * 1_000); // ms → µs

        buffer
            .table("hyperliquid_fill")?
            .symbol("coin", &fill.coin)?
            .symbol("wallet", &fill.wallet)?
            .symbol("side", &fill.side)?
            .symbol("category", &fill.category)?
            .symbol("source", "HYPERLIQUID_NODE")?
            .column_bool("is_taker", fill.is_taker)?
            .column_f64("price", fill.price)?
            .column_f64("quantity", fill.quantity)?
            .column_f64("position_before", fill.position_before)?
            .column_f64("realized_pnl", fill.realized_pnl)?
            .at(ts_us)?;

        rows += 1;

        if buffer.len() >= BUFFER_FLUSH_THRESHOLD {
            sender.flush(&mut buffer)?;
        }
    }

    if !buffer.is_empty() {
        sender.flush(&mut buffer)?;
    }

    Ok(rows)
}

// ---------------------------------------------------------------------------
// Market state 1-minute row
// ---------------------------------------------------------------------------

/// One row in `market_state_1m`.
///
/// All price / market fields come from `market_data` (daily snapshot carried
/// forward for all 1-minute buckets in that day).
/// Fill-derived fields come from `hyperliquid_fill_1m_aggregate`.
#[derive(Debug)]
pub struct MarketStateRow {
    /// Left-closed minute bucket start, Unix milliseconds.
    pub minute_ms: i64,
    pub coin: String,
    /// Oracle price from `market_data.oracle_px`.
    pub price_oracle: f64,
    /// Mark price from `market_data.mark_px`.
    pub price_mark: f64,
    /// Mid price — `market_data.mid_px` when present, else `(oracle + mark) / 2`.
    pub price_mid: f64,
    /// Open interest from `market_data.open_interest`.
    pub open_interest: f64,
    /// Funding rate from `market_data.funding`.
    pub funding_rate: f64,
    /// 24-hour notional volume from `market_data.day_ntl_vlm`.
    pub volume_24h_usd: f64,
    /// Sum of fill quantities (buy side only — buys/sells are paired).
    pub trade_volume: f64,
    /// Number of individual fills (buy side only).
    pub trade_count: i64,
    /// Sum of liquidated long fill quantities.
    pub liquidation_long_volume: f64,
    /// Sum of liquidated short fill quantities.
    pub liquidation_short_volume: f64,
    /// Number of liquidated long fills.
    pub liquidation_long_count: i64,
    /// Number of liquidated short fills.
    pub liquidation_short_count: i64,
}

// ---------------------------------------------------------------------------
// Market state 1-minute ingestion
// ---------------------------------------------------------------------------

/// Batch-insert [`MarketStateRow`]s into the `market_state_1m` table.
///
/// The buffer is flushed automatically at [`BUFFER_FLUSH_THRESHOLD`].
/// Returns the total number of rows written.
pub fn insert_market_state_1m(sender: &mut Sender, rows: &[MarketStateRow]) -> QuestResult<usize> {
    let mut buffer = Buffer::new();
    let mut count: usize = 0;

    for row in rows {
        let ts_us = TimestampMicros::new(row.minute_ms * 1_000); // ms → µs

        buffer
            .table("market_state_1m")?
            .symbol("coin", &row.coin)?
            .column_f64("price_oracle", row.price_oracle)?
            .column_f64("price_mark", row.price_mark)?
            .column_f64("price_mid", row.price_mid)?
            .column_f64("open_interest", row.open_interest)?
            .column_f64("funding_rate", row.funding_rate)?
            .column_f64("volume_24h_usd", row.volume_24h_usd)?
            .column_f64("trade_volume", row.trade_volume)?
            .column_i64("trade_count", row.trade_count)?
            .column_f64("liquidation_long_volume", row.liquidation_long_volume)?
            .column_f64("liquidation_short_volume", row.liquidation_short_volume)?
            .column_i64("liquidation_long_count", row.liquidation_long_count)?
            .column_i64("liquidation_short_count", row.liquidation_short_count)?
            .at(ts_us)?;

        count += 1;

        if buffer.len() >= BUFFER_FLUSH_THRESHOLD {
            sender.flush(&mut buffer)?;
        }
    }

    if !buffer.is_empty() {
        sender.flush(&mut buffer)?;
    }

    Ok(count)
}

// ---------------------------------------------------------------------------
// Hyperliquid 1-minute aggregate ingestion
// ---------------------------------------------------------------------------

/// Batch-insert a slice of [`Fill1mAggregate`]s into the
/// `hyperliquid_fill_1m_aggregate` table.
///
/// Each row's timestamp is the **left-closed** minute bucket boundary
/// (`minute_ms * 1_000` µs).
///
/// The buffer is flushed automatically at [`BUFFER_FLUSH_THRESHOLD`].
/// Returns the total number of rows written.
pub fn insert_hyperliquid_fill_1m_aggregate(
    sender: &mut Sender,
    rows: &[Fill1mAggregate],
) -> QuestResult<usize> {
    let mut buffer = Buffer::new();
    let mut count: usize = 0;

    for row in rows {
        let ts_us = TimestampMicros::new(row.minute_ms * 1_000); // ms → µs

        buffer
            .table("hyperliquid_fill_1m_aggregate")?
            .symbol("coin", &row.coin)?
            .symbol("category", &row.category)?
            .column_bool("buy_side", row.buy_side)?
            .column_f64("quantity", row.quantity)?
            .column_i64("trade_count", row.trade_count)?
            .at(ts_us)?;

        count += 1;

        if buffer.len() >= BUFFER_FLUSH_THRESHOLD {
            sender.flush(&mut buffer)?;
        }
    }

    if !buffer.is_empty() {
        sender.flush(&mut buffer)?;
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        database::insert_signal_db,
        model::signal::{Signal, SignalData, SignalInfo},
    };

    #[tokio::test]
    async fn test_insert_signal() {
        use questdb::ingress::Sender;
        let mut sender = Sender::from_conf("http::addr=localhost:9000;").unwrap();
        let signal_info = SignalInfo::dummy();

        let time = Utc::now().timestamp_micros();
        let signal_1 = Signal {
            data: SignalData::Scalar(1.0),
            timestamp_us: time.clone(),
            ..Default::default()
        };
        let mut signal_2 = signal_1.clone();
        signal_2.data = SignalData::Scalar(2.0);

        let data = [signal_1, signal_2].to_vec();

        insert_signal_db(&mut sender, &signal_info, &data).unwrap();
    }

    #[tokio::test]
    async fn test_select_signal() {
        use super::*;
        let query = "SELECT * FROM signal_scalar where info_id = 0;";
        let signals = select_signal_db(SignalDataType::Scalar, query)
            .await
            .unwrap();
        assert!(!signals.is_empty(), "is empty");
    }

    #[tokio::test]
    async fn test_insert_unique_signal() {
        use super::*;

        use questdb::ingress::Sender;
        let mut sender = Sender::from_conf("http::addr=localhost:9000;").unwrap();
        let skip_query = "SELECT * FROM signal_scalar where info_id = 0;";

        let signal_info = SignalInfo::dummy();

        let signal_1 = Signal {
            data: SignalData::Scalar(1.0),
            timestamp_us: 0,
            ..Default::default()
        };
        let mut signal_2 = signal_1.clone();
        signal_2.data = SignalData::Scalar(2.0);

        let data = [signal_1, signal_2].to_vec();

        insert_unique_signal_db(&mut sender, &signal_info, &data, skip_query)
            .await
            .unwrap();
    }
}
