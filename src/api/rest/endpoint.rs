use crate::metadata::cargo_package_version;
use poem_openapi::{payload::PlainText, OpenApi};

pub struct Endpoint;

#[OpenApi]
impl Endpoint {
    /// test if the points indexer server is working
    #[oai(path = "/", method = "get")]
    async fn root(&self) -> PlainText<String> {
        PlainText("Server is running.".to_string())
    }

    /// semantic version of points indexer server
    #[oai(path = "/version", method = "get")]
    async fn version(&self) -> PlainText<String> {
        PlainText(cargo_package_version())
    }
}
