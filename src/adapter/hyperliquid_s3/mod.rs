/*
implementation of AWS S3 fetcher for hyperkiquid historic data
https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data
*/

pub mod asset_ctxs;
pub mod explorer_blocks;
pub mod market_data;
pub mod node_fills_by_block;

pub mod prelude {
    pub use crate::adapter::AdapterError;
    pub use crate::error::{AnySignalError, AnySignalResult};
    pub use crate::model::signal::*;
    pub const SOURCE: &str = "HYPERLIQUID_S3";
    pub const AGENT: &str = "Signals";

    /// Format an error and its full source chain into a single string.
    ///
    /// AWS SDK errors display as "service error" at the top level; the actual
    /// HTTP error code and message are in the source chain. Walking the chain
    /// produces e.g. `"service error: NoSuchKey: The specified key does not exist."`.
    /// Duplicate consecutive messages are suppressed.
    pub fn fmt_err_chain(e: &dyn std::error::Error) -> String {
        let mut msg = e.to_string();
        let mut cause = e.source();
        while let Some(s) = cause {
            let next = s.to_string();
            if !msg.contains(&next) {
                msg.push_str(": ");
                msg.push_str(&next);
            }
            cause = s.source();
        }
        msg
    }

    /// Classify an S3 `SdkError` into a typed [`AnySignalError`]:
    /// - `NoSuchKey` (404) → `AdapterError::NotFound`
    /// - HTTP 401/403     → `AdapterError::Unauthorized`
    /// - anything else    → `AdapterError::FetchError`
    pub fn classify_s3_error<E>(
        e: &aws_sdk_s3::error::SdkError<E>,
        key: &str,
    ) -> AnySignalError
    where
        E: std::error::Error + aws_sdk_s3::error::ProvideErrorMetadata + 'static,
    {
        // HTTP 401 / 403 → credential / permission problem
        if let Some(resp) = e.raw_response() {
            let status = resp.status().as_u16();
            if matches!(status, 401 | 403) {
                return AnySignalError::Adapter(AdapterError::Unauthorized(format!(
                    "HTTP {status} accessing s3://{key} — check AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY and that the requester-pays header is accepted: {}",
                    fmt_err_chain(e)
                )));
            }
        }

        // NoSuchKey (404) → data simply isn't in the archive
        if let Some(meta) = e.as_service_error() {
            if meta.code() == Some("NoSuchKey") {
                return AnySignalError::Adapter(AdapterError::NotFound(format!(
                    "s3://{key} does not exist in the archive"
                )));
            }
        }

        AnySignalError::Adapter(AdapterError::FetchError(format!(
            "Failed to fetch s3://{key}: {}",
            fmt_err_chain(e)
        )))
    }
}
