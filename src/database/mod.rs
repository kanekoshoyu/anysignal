pub mod response;

use crate::model::signal::{Signal, SignalData, SignalInfo};
use questdb::ingress::{Buffer, Sender, TimestampMicros};
use questdb::Result as QuestResult;
use reqwest::Client;
use response::{QuestDBResponse, SignalDataRow};

// batch insert using questdb ingress
pub fn insert_signal_questdb(
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
                .symbol("id", &signal_info.source)?
                .at(TimestampMicros::new(timestamp_us))?,
            SignalData::Binary(value) => buffer
                .table("signal_binary")?
                .symbol("id", &signal_info.source)?
                .column_bool("value", *value)?
                .at(TimestampMicros::new(timestamp_us))?,
            SignalData::Scalar(value) => buffer
                .table("signal_scalar")?
                .symbol("id", &signal_info.source)?
                .column_f64("value", *value)?
                .at(TimestampMicros::new(timestamp_us))?,
            SignalData::Text(value) => buffer
                .table("signal")?
                .symbol("id", &signal_info.source)?
                .column_str("value", value)?
                .at(TimestampMicros::new(timestamp_us))?,
        }
    }
    sender.flush(&mut buffer)?;
    Ok(())
}

// select via query on http
pub async fn select_signal_questdb(signal_info_id: impl AsRef<str>) -> QuestResult<Vec<Signal>> {
    // TODO construct a query from the signal_info
    // TODO construct the query using AST
    let query_string = format!(
        "SELECT * FROM signal_scalar where id = '{}';",
        signal_info_id.as_ref()
    );
    println!("{query_string}");

    let client = reqwest::Client::new();
    let response = client
        .get("http://localhost:9000/exec")
        .query(&[("query", &query_string)])
        .send()
        .await
        .unwrap();
    let text = response.text().await.unwrap();
    let result: QuestDBResponse<SignalDataRow> = serde_json::from_str(&text).unwrap();
    let signals = result.dataset;
    println!("signals: {signals:?}");

    // TODO implement the reconstruction of signal from the questdb
    Ok(Vec::new())
}

#[cfg(test)]
mod tests {
    use chrono::Utc;

    use crate::{
        database::insert_signal_questdb,
        model::signal::{Signal, SignalData, SignalInfo},
    };

    #[tokio::test]
    async fn test_insert_questdb() {
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

        insert_signal_questdb(&mut sender, &signal_info, &data).unwrap();
    }

    #[tokio::test]
    async fn test_select_questdb() {
        use super::*;
        select_signal_questdb("DummySource").await.unwrap();
    }
}
