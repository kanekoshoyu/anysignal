use serde::Deserialize;
use std::fmt::Debug;

#[derive(Debug, Deserialize)]
pub struct QuestDbResponse<T: Debug + 'static> {
    // success/error
    pub query: String,
    // success
    pub columns: Vec<Column>,
    // success
    pub timestamp: usize,
    // success
    pub dataset: Vec<T>,
    // success
    pub count: usize,
    // error
    pub error: Option<String>,
    // success
    pub position: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

#[derive(Debug, Deserialize)]
pub struct SignalDataRow<VALUE: Debug + 'static>(
    pub String, // "id" (SYMBOL)
    pub VALUE,  // "value" (DOUBLE)
    pub String, // "timestamp" (TIMESTAMP as ISO 8601 string)
);

#[cfg(test)]
mod tests {
    use crate::database::table::SignalScalarRow;

    #[test]
    fn test_select_singal_scalar_ok() {
        use super::*;

        // Example JSON response from QuestDB
        let json_response = r#"{
            "query": "SELECT * FROM signal_scalar;",
            "columns": [
                {
                "name": "info_id",
                "type": "LONG"
                },
                {
                "name": "value",
                "type": "DOUBLE"
                },
                {
                "name": "timestamp",
                "type": "TIMESTAMP"
                }
            ],
            "timestamp": 2,
            "dataset": [
                [0, 1, "2025-01-21T05:54:08.189677Z"]
            ],
            "count": 1
        }    
        "#;

        // Deserialize JSON into Rust structs
        let response: QuestDbResponse<SignalScalarRow> =
            serde_json::from_str(json_response).unwrap();

        // Iterate through the dataset
        for row in &response.dataset {
            println!(
                "Row: info_id={}, value={}, timestamp={}",
                row.info_id, row.value, row.timestamp
            );
        }
    }
}
