// only store how the tables are being structured now per table name
// table name is snake_case
use super::*;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Deserialize)]
pub struct SignalSimpleRow {
    pub info_id: i64,             // INT type
    pub timestamp: DateTime<Utc>, // QuestDB's TIMESTAMP
}

#[derive(Debug, Clone, Deserialize)]
pub struct SignalBooleanRow {
    pub info_id: i64,             // INT type
    pub value: bool,              // BOOLEAN type
    pub timestamp: DateTime<Utc>, // QuestDB's TIMESTAMP
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SignalScalarRow {
    pub info_id: i64,             // INT type
    pub value: f64,               // DOUBLE type
    pub timestamp: DateTime<Utc>, // QuestDB's TIMESTAMP
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SignalStringRow {
    pub info_id: i64,             // INT type
    pub value: String,            // STRING type
    pub timestamp: DateTime<Utc>, // QuestDB's TIMESTAMP
}
