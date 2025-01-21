use std::hash::Hash;

use chrono::Utc;
use serde::{Deserialize, Serialize};

/// the main signal struct
#[derive(Default, Debug, Clone, PartialEq)]
pub struct Signal {
    pub info_id: i64,
    pub timestamp_us: i64,
    pub data: SignalData,
}
impl Hash for Signal {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.info_id.hash(state);
        self.timestamp_us.hash(state);
    }
}
impl Eq for Signal {}

impl Signal {
    pub fn dummy_scalar(value: f64) -> Self {
        Self {
            info_id: 0,
            timestamp_us: Utc::now().timestamp_micros(),
            data: SignalData::Scalar(value),
        }
    }
}

#[derive(Default, Debug, Clone, Deserialize, Serialize, PartialEq)]
pub enum SignalData {
    #[default]
    Simple,
    Binary(bool),
    Scalar(f64),
    Text(String),
    // Json(Value), // JSON not supported by questdb
}

/// description of SignalData
#[derive(Default, Debug, Clone, Copy, Deserialize, Serialize)]
pub enum SignalDataType {
    #[default]
    Simple,
    Binary,
    Scalar,
    Text,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalInfo {
    pub id: i64,
    pub signal_type: String,
    pub data_type: SignalDataType,
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
            data_type: SignalDataType::Scalar,
            source: "DummySource".to_string(),
            description: "Dummy Index".to_string(),
            is_atomic: true,
        }
    }
}
