use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

/// the main signal struct
#[derive(Debug, Clone)]
pub struct Signal {
    pub id: u64,
    pub timestamp: NaiveDateTime,
    pub data: SignalData,
    pub info_id: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum SignalData {
    Simple,
    Binary(bool),
    Scalar(f64),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalInfo {
    pub id: u64,
    pub source: String,
    pub description: String,
    /// atomic: is raw and indivisible
    pub is_atomic: bool,
}
