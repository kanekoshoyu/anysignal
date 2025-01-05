use chrono::NaiveDateTime;

/// the main signal struct
pub struct Signal {
    id: u64,
    timestamp: NaiveDateTime,
    data: SignalData,
    info_id: u64,
}

pub enum SignalData {
    Simple,
    Binary(bool),
    Scalar(f64),
}

pub struct SignalInfo {
    pub id: u64,
    pub source: String,
    pub description: String,
    /// atomic: is raw and indivisible
    pub is_atomic: bool,
}
