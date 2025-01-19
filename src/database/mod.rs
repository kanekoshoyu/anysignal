use crate::model::signal::{Signal, SignalData, SignalInfo};
use questdb::ingress::{Buffer, Sender, TimestampMicros};
use questdb::Result as QuestResult;

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
    println!("done buffer");
    sender.flush(&mut buffer)?;
    println!("done flush");
    Ok(())
}
