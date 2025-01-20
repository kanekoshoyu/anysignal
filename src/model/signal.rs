use chrono::Utc;
use serde::{Deserialize, Serialize};

/// the main signal struct
#[derive(Default, Debug, Clone)]
pub struct Signal {
    pub id: u64,
    pub timestamp_us: i64,
    pub data: SignalData,
    pub info_id: u64,
}
impl Signal {
    pub fn dummy_scalar(value: f64) -> Self {
        Self {
            id: 0,
            timestamp_us: Utc::now().timestamp_micros(),
            data: SignalData::Scalar(value),
            info_id: 0,
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub enum SignalData {
    #[default]
    Simple,
    Binary(bool),
    Scalar(f64),
    Text(String),
    // Json(Value), // JSON not supported by questdb
}

/// description of SignalData
#[derive(Default, Debug, Clone, Deserialize, Serialize)]
pub enum SignalDataType {
    #[default]
    Simple,
    Binary,
    Scalar,
    Text,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalInfo {
    pub id: u64,
    pub signal_type: String,
    pub source: String,
    pub description: String,
    /// atomic: is raw and indivisible
    pub is_atomic: bool,
}
impl SignalInfo {
    pub fn dummy() -> Self {
        Self {
            id: 0,
            signal_type: "dummy_index".to_string(),
            source: "DummySource".to_string(),
            description: "Dummy Index".to_string(),
            is_atomic: true,
        }
    }
}
