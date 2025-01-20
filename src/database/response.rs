use serde::Deserialize;
use std::fmt::Debug;

#[derive(Debug, Deserialize)]
pub struct QuestDBResponse<T: Debug + 'static> {
    pub query: String,
    pub columns: Vec<Column>,
    pub timestamp: usize,
    pub dataset: Vec<T>,
    pub count: usize,
}

#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

#[derive(Debug, Deserialize)]
pub struct SignalDataRow(
    String, // "id" (SYMBOL)
    f64,    // "value" (DOUBLE)
    String, // "timestamp" (TIMESTAMP as ISO 8601 string)
);


#[cfg(test)]
mod tests {

    #[test]
    fn test_select_singal_scalar() {
        use super::*;

        // Example JSON response from QuestDB
        let json_response = r#"
            {
                "query": "SELECT * FROM signal_scalar;",
                "columns": [
                    { "name": "id", "type": "SYMBOL" },
                    { "name": "value", "type": "DOUBLE" },
                    { "name": "timestamp", "type": "TIMESTAMP" }
                ],
                "timestamp": 2,
                "dataset": [
                    ["DummySource", 1.0, "2025-01-20T08:08:41.221370Z"],
                    ["DummySource", 2.0, "2025-01-20T08:08:41.221370Z"],
                    ["DummySource", 1.0, "2025-01-20T08:08:41.221373Z"],
                    ["DummySource", 2.0, "2025-01-20T08:08:41.221373Z"],
                    ["DummySource", 1.0, "2025-01-20T08:16:09.237566Z"],
                    ["DummySource", 2.0, "2025-01-20T08:16:09.237566Z"],
                    ["DummySource", 1.0, "2025-01-20T08:16:09.237566Z"],
                    ["DummySource", 2.0, "2025-01-20T08:16:09.237566Z"]
                ],
                "count": 8
            }
        "#;

        // Deserialize JSON into Rust structs
        let response: QuestDBResponse<SignalDataRow> = serde_json::from_str(json_response).unwrap();

        // Iterate through the dataset
        for row in &response.dataset {
            println!("Row: id={}, value={}, timestamp={}", row.0, row.1, row.2);
        }
    }
}
