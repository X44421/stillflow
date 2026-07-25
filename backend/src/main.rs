mod api;
mod error;
mod models;
mod pipeline;
mod storage;

use std::{env, net::SocketAddr, path::PathBuf, sync::Arc};

use api::AppState;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use storage::Storage;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

const MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "stillflow_backend=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let data_dir = env::var("STILLFLOW_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("data"));
    let address: SocketAddr = env::var("STILLFLOW_BIND")
        .unwrap_or_else(|_| "127.0.0.1:8787".to_owned())
        .parse()?;

    let state = AppState {
        storage: Arc::new(Storage::open(data_dir).await?),
    };

    let app = Router::new()
        .route("/api/health", get(api::health))
        .route(
            "/api/datasets",
            get(api::list_datasets).post(api::import_dataset),
        )
        .route("/api/datasets/import", post(api::import_dataset))
        .route("/api/datasets/:id/preview", get(api::preview_dataset))
        .route("/api/pipeline/run", post(api::run_pipeline))
        .route(
            "/api/exports/:id/download",
            get(api::download_export),
        )
        .layer(DefaultBodyLimit::max(MAX_UPLOAD_BYTES))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(address).await?;
    tracing::info!(%address, "StillFlow backend listening");
    axum::serve(listener, app).await?;
    Ok(())
}
