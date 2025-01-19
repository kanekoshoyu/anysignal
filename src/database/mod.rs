use questdb::ingress::{Buffer, Sender, TimestampNanos};
use questdb::Result as QuestResult;

pub fn run_quest_db() -> QuestResult<()> {
    let mut sender = Sender::from_conf("http::addr=localhost:9000;")?;
    let mut buffer = Buffer::new();
    buffer
        .table("sensors")?
        .symbol("id", "toronto1")?
        .column_f64("temperature", 20.0)?
        .column_i64("humidity", 50)?
        .at(TimestampNanos::now())?;
    sender.flush(&mut buffer)?;
    Ok(())
}
