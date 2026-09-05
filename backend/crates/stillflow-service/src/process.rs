//! Process assembly and lifecycle (contract §4): stack composition, workspace
//! bootstrap, ready announcement, graceful drain on shutdown.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use tokio::net::TcpListener;
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use uuid::Uuid;

use stillflow_api::event_stream::EventStreamService;
use stillflow_api::{
    ApiLimits, ApiService, DaemonLifecycle, DeploymentError, LifecycleError, ServiceConfig,
};
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connector_object_store::ObjectStoreConnector;
use stillflow_connector_workbook::WorkbookConnector;
use stillflow_connectors::ConnectorRegistry;
use stillflow_engine::{ExecutionEngine, JobRuntime, JobRuntimeError};
use stillflow_storage::{SnapshotStore, StorageError, StorageLimits};

use crate::config::ProcessConfig;
use crate::resolver::DurableJobRequestResolver;
use crate::routes::{router, ServiceState};

#[derive(Debug, thiserror::Error)]
pub enum ProcessError {
    #[error("service configuration is invalid: {0}")]
    Config(String),
    #[error("storage failure: {0}")]
    Storage(#[from] StorageError),
    #[error("deployment contract failure: {0}")]
    Deployment(#[from] DeploymentError),
    #[error("lifecycle transition failure: {0}")]
    Lifecycle(#[from] LifecycleError),
    #[error("job runtime failure: {0}")]
    Runtime(#[from] JobRuntimeError),
    #[error("listener failure: {0}")]
    Listener(#[from] std::io::Error),
}

fn register_bounded_connectors(registry: &mut ConnectorRegistry) -> Result<(), ProcessError> {
    let register =
        |error: stillflow_connectors::ConnectorError| ProcessError::Config(error.to_string());
    registry
        .register(Arc::new(LocalTabularConnector))
        .map_err(register)?;
    registry
        .register(Arc::new(WorkbookConnector))
        .map_err(register)?;
    registry
        .register(Arc::new(ObjectStoreConnector::default()))
        .map_err(register)?;
    Ok(())
}

pub struct StartedService {
    pub addr: SocketAddr,
    pub workspace_id: Uuid,
    pub config: ServiceConfig,
    state: ServiceState,
    runtime: Arc<JobRuntime>,
    lifecycle: Mutex<DaemonLifecycle>,
    server: ServerTask,
    shutdown: tokio::sync::oneshot::Sender<()>,
}

type ServerTask = tokio::task::JoinHandle<Result<std::io::Result<()>, tokio::time::error::Elapsed>>;

pub async fn start_service(config: ProcessConfig) -> Result<StartedService, ProcessError> {
    config
        .service
        .validate()
        .map_err(|error| ProcessError::Config(error.to_string()))?;
    let managed_root = PathBuf::from(&config.service.managed_root);
    let snapshot_store = Arc::new(SnapshotStore::open(
        managed_root.join("store"),
        StorageLimits::default(),
    )?);
    let store = Arc::new(snapshot_store.control_plane());

    let workspace_id = config.workspace_id;
    if let Err(error) = store.create_workspace(workspace_id, chrono::Utc::now()) {
        // Adopt the existing row only when it is this exact workspace id; any
        // other storage failure is fail-closed (contract §4.3).
        if store.get_workspace(workspace_id).is_err() {
            return Err(ProcessError::Storage(error));
        }
    }

    let mut engine_registry = ConnectorRegistry::new();
    register_bounded_connectors(&mut engine_registry)?;
    let mut api_registry = ConnectorRegistry::new();
    register_bounded_connectors(&mut api_registry)?;
    let engine = Arc::new(ExecutionEngine::new(engine_registry));

    let resolver = Arc::new(DurableJobRequestResolver::new(Arc::clone(&store)));
    let runtime = Arc::new(JobRuntime::new_with_system_identity(
        workspace_id,
        Arc::clone(&store),
        Arc::clone(&snapshot_store),
        Arc::clone(&engine),
        resolver,
    )?);
    runtime.start().await?;

    let limits = ApiLimits::DEFAULT;
    let api = Arc::new(
        ApiService::new(Arc::clone(&store))
            .with_connectors(Arc::new(api_registry))
            .with_engine(Arc::clone(&engine))
            .with_runtime(Arc::clone(&runtime))
            .with_snapshot_store(Arc::clone(&snapshot_store))
            .with_limits(limits)
            .with_authorization_mode(config.authorization_mode.into()),
    );
    let events = Arc::new(EventStreamService::new(Arc::clone(&store)));
    let state = ServiceState { api, events };

    let listener =
        TcpListener::bind((config.service.bind_host.as_str(), config.service.bind_port)).await?;
    let addr = listener.local_addr()?;

    let app = router(state.clone())
        .layer(DefaultBodyLimit::max(limits.max_request_bytes))
        .layer(TimeoutLayer::with_status_code(
            axum::http::StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(limits.max_timeout_seconds),
        ))
        .layer(GlobalConcurrencyLimitLayer::new(
            limits.max_concurrent_requests,
        ));

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let grace = Duration::from_secs(u64::from(config.service.shutdown_grace_seconds));
    let server: ServerTask = tokio::spawn(async move {
        let serve = axum::serve(listener, app).with_graceful_shutdown(async {
            let _ = shutdown_rx.await;
        });
        // The grace cap is enforced inside the serve task: after the cap the
        // drain ends regardless of in-flight connections (contract §4.4).
        tokio::time::timeout(grace, serve).await
    });

    let mut lifecycle = DaemonLifecycle::new(config.service.max_recovery_attempts)?;
    lifecycle.start()?;
    lifecycle.mark_ready()?;

    Ok(StartedService {
        addr,
        workspace_id,
        config: config.service,
        state,
        runtime,
        lifecycle: Mutex::new(lifecycle),
        server,
        shutdown: shutdown_tx,
    })
}

impl StartedService {
    pub fn state(&self) -> &ServiceState {
        &self.state
    }

    pub fn ready_line(&self) -> String {
        serde_json::json!({
            "event": "ready",
            "pid": std::process::id(),
            "bindHost": self.config.bind_host,
            "port": self.addr.port(),
            "workspaceId": self.workspace_id,
            "apiVersion": 1,
            "transport": self.config.transport,
        })
        .to_string()
    }

    pub async fn shutdown(self) -> Result<(), ProcessError> {
        {
            let mut lifecycle = self
                .lifecycle
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            lifecycle.begin_shutdown()?;
        }
        let _ = self.shutdown.send(());
        match self.server.await {
            Ok(Ok(Ok(()))) => {}
            Ok(Ok(Err(error))) => return Err(ProcessError::Listener(error)),
            Ok(Err(_elapsed)) => {}
            Err(join_error) => return Err(ProcessError::Config(join_error.to_string())),
        }
        self.runtime.shutdown().await;
        self.lifecycle
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .complete_shutdown()?;
        Ok(())
    }
}
