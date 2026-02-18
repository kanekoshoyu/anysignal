/// direct response of the response
pub mod response;
/// representation of each table
pub mod table;

use crate::adapter::hyperliquid_s3::asset_ctxs::AssetCtxRow;
use crate::database::table::*;
use crate::error::AnySignalResult;
use crate::model::signal::{Signal, SignalData, SignalDataType, SignalInfo};
use questdb::ingress::{Buffer, Sender, TimestampMicros};
use questdb::Result as QuestResult;
use response::QuestDbResponse;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::env;

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
pub fn questdb_sender() -> QuestResult<Sender> {
    dotenvy::dotenv().ok();
    let addr =
        env::var("QUESTDB_ADDR").unwrap_or_else(|_| "questdb.bounteer.com:9000".to_string());

    let conf = match (env::var("QUESTDB_USER"), env::var("QUESTDB_PASSWORD")) {
        (Ok(user), Ok(pass)) => {
            format!("http::addr={};username={};password={};", addr, user, pass)
        }
        _ => format!("http::addr={};", addr),
    };

    Sender::from_conf(conf)
}

// ---------------------------------------------------------------------------
// Hyperliquid asset_ctxs ingestion
// ---------------------------------------------------------------------------

/// Batch-insert a slice of [`AssetCtxRow`]s into the `market_data` table.
///
/// Each source row is fanned out into one `market_data` row per metric so the
/// generic (ts, category, ticker, source, value) schema is preserved.
///
/// Metrics written per row:
///   open_interest, funding, mark_px, oracle_px, prev_day_px, day_ntl_vlm,
///   mid_px (skipped when None), premium (skipped when None).
pub fn insert_asset_ctxs(sender: &mut Sender, rows: &[AssetCtxRow]) -> QuestResult<()> {
    let mut buffer = Buffer::new();

    for row in rows {
        let ts_us = TimestampMicros::new(row.time * 1_000); // ms → µs

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
    }

    sender.flush(&mut buffer)?;
    Ok(())
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
