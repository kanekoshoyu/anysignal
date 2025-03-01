use crate::error::{AnySignalError, AnySignalResult};
use async_trait::async_trait;
use serde::de::DeserializeOwned;

#[async_trait]
pub trait ResponseExt {
    async fn parse_json<T: DeserializeOwned>(self) -> AnySignalResult<T>;
    fn verify(self) -> AnySignalResult<Self>
    where
        Self: Sized;
}

#[async_trait]
impl ResponseExt for reqwest::Response {
    async fn parse_json<T: DeserializeOwned>(self) -> AnySignalResult<T> {
        let text = self.text().await?;
        let json = serde_json::from_str::<T>(&text);
        if let Err(error) = &json {
            dbg!(text, error);
        }
        Ok(json?)
    }

    fn verify(self) -> AnySignalResult<Self> {
        if !self.status().is_success() {
            let msg = format!("response status({})", self.status());
            return Err(AnySignalError::from(msg.as_str()));
        }
        Ok(self)
    }
}
