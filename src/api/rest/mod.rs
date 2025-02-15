mod endpoint;
use crate::metadata::cargo_package_version;
use endpoint::Endpoint;
use poem::{listener::TcpListener, Route, Server};
use poem_openapi::{OpenApiService, ServerObject};
use serde_json::Error as SerdeJsonError;
use thiserror::Error as ThisError;
use tokio::task::JoinHandle;
#[derive(Debug, ThisError)]
pub enum ApiError {
    #[error("API Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("API Error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("API Error: {0}")]
    SerdeJsonError(#[from] SerdeJsonError),
}

// host a rest api server
pub async fn host_rest_api_server() -> Result<(), ApiError> {
    let url = "http://localhost:3000";

    let desciption = "signal indexer";

    let title = "signal indexer API";

    // stable
    let service_api_root = {
        let all_ep = Endpoint;
        OpenApiService::new(all_ep, title, cargo_package_version())
            .server(ServerObject::new(url))
            .description(desciption)
            .external_document("https://repoch-trading.com/yaml")
    };

    // openapi documentation endpoints
    let ep_swagger = service_api_root.swagger_ui();
    let ep_rapidoc = service_api_root.rapidoc();
    let ep_redoc = service_api_root.redoc();
    let ep_openapi = service_api_root.openapi_explorer();
    let ep_yaml = service_api_root.spec_endpoint_yaml();

    let route = Route::new()
        // original route for gradual integration purpose
        .nest("", service_api_root)
        .nest("/swagger", ep_swagger)
        .nest("/rapidoc", ep_rapidoc)
        .nest("/redoc", ep_redoc)
        .nest("/openapi", ep_openapi)
        .nest("/yaml", ep_yaml);

    Server::new(TcpListener::bind(("0.0.0.0", 3000)))
        .run(route)
        .await
        .map_err(|i| i.into())
}

pub async fn run_web() -> JoinHandle<Result<(), ApiError>> {
    tokio::spawn(async move { host_rest_api_server().await })
}
