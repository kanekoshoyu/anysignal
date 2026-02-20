/// direct response of the response
pub mod response;
/// representation of each table
pub mod table;

use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxRow;
use crate::adapter::hyperliquid_s3::market_data::L2Snapshot;
use crate::database::table::*;
use crate::error::AnySignalResult;
use crate::model::signal::{Signal, SignalData, SignalDataType, SignalInfo};
use questdb::ingress::{Buffer, Sender, TimestampMicros};
use questdb::Result as QuestResult;
use crate::config::Config;
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
// Hyperliquid asset_ctxs — duplicate check
// ---------------------------------------------------------------------------

/// Returns `true` if the `market_data` table already contains at least one row
/// for `source = 'HYPERLIQUID_S3'` on the given calendar day.
///
/// If the table does not yet exist or QuestDB returns any error, `Ok(false)` is
/// returned so that the caller proceeds with the fetch rather than skipping it.
pub async fn asset_ctxs_date_exists(
    config: &Config,
    date: chrono::NaiveDate,
) -> AnySignalResult<bool> {
    let next_day = date.succ_opt().unwrap_or(date);
    let query = format!(
        "SELECT count() FROM market_data \
         WHERE source = 'HYPERLIQUID_S3' \
         AND timestamp >= '{}T00:00:00Z' \
         AND timestamp < '{}T00:00:00Z'",
        date.format("%Y-%m-%d"),
        next_day.format("%Y-%m-%d"),
    );
    let url = format!("http://{}/exec", config.questdb_addr);
    let json: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .query(&[("query", &query)])
        .send()
        .await?
        .json()
        .await?;

    // If QuestDB returns an error field (e.g. table doesn't exist yet), treat as
    // no data so the backfill proceeds normally.
    if json.get("error").and_then(|v| v.as_str()).is_some() {
        return Ok(false);
    }

    let count = json["dataset"][0][0].as_i64().unwrap_or(0);
    Ok(count > 0)
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
// Hyperliquid L2 orderbook — duplicate check
// ---------------------------------------------------------------------------

/// Returns `true` if `l2_snapshot` already has data for the given coin during
/// the given hour (i.e. `ts >= hour_start AND ts < hour_start + 1 h`).
///
/// Returns `Ok(false)` on any QuestDB error so the caller proceeds with the
/// fetch rather than silently skipping it.
pub async fn l2_snapshot_coin_hour_exists(
    config: &Config,
    hour_start: chrono::NaiveDateTime,
    coin: &str,
) -> AnySignalResult<bool> {
    let hour_end = hour_start + chrono::Duration::hours(1);
    let query = format!(
        "SELECT count() FROM l2_snapshot \
         WHERE ticker = '{}' \
         AND ts >= '{}Z' \
         AND ts < '{}Z'",
        coin,
        hour_start.format("%Y-%m-%dT%H:%M:%S"),
        hour_end.format("%Y-%m-%dT%H:%M:%S"),
    );
    let url = format!("http://{}/exec", config.questdb_addr);
    let json: serde_json::Value = reqwest::Client::new()
        .get(&url)
        .query(&[("query", &query)])
        .send()
        .await?
        .json()
        .await?;

    if json.get("error").and_then(|v| v.as_str()).is_some() {
        return Ok(false);
    }

    let count = json["dataset"][0][0].as_i64().unwrap_or(0);
    Ok(count > 0)
}

// ---------------------------------------------------------------------------
// Hyperliquid L2 orderbook ingestion
// ---------------------------------------------------------------------------

/// Flush the L2 buffer when it exceeds 64 MiB — well below QuestDB's 100 MiB cap.
const L2_BUFFER_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024;

/// Batch-insert L2 orderbook snapshots into the `l2_snapshot` table.
///
/// Each [`L2Snapshot`] expands to one row per price level per side:
///   - `levels[0]` → side `"bid"`, level index 0, 1, 2, …
///   - `levels[1]` → side `"ask"`, level index 0, 1, 2, …
///
/// The buffer is flushed automatically at [`L2_BUFFER_FLUSH_THRESHOLD`].
/// Returns the total number of rows written.
pub fn insert_l2_snapshots(
    sender: &mut Sender,
    snapshots: &[L2Snapshot],
) -> QuestResult<usize> {
    let mut buffer = Buffer::new();
    let mut rows: usize = 0;

    for snapshot in snapshots {
        let ts_us = TimestampMicros::new(snapshot.time_ms() * 1_000); // ms → µs

        for (side_idx, side_str) in [(0usize, "bid"), (1usize, "ask")] {
            for (level_idx, level) in snapshot.levels()[side_idx].iter().enumerate() {
                let price: f64 = level.px.parse().unwrap_or(0.0);
                let quantity: f64 = level.sz.parse().unwrap_or(0.0);

                buffer
                    .table("l2_snapshot")?
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
