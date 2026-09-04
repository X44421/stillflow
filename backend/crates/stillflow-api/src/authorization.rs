//! Centralized SEC-A1 workspace authorization gate.
//!
//! The gate is intentionally independent of transport. Request metadata
//! carries the authenticated principal identity, while this module resolves
//! the durable member/role state and caches only sanitized capability names.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use stillflow_core::WorkspaceState;
use stillflow_storage::{ControlPlaneStore, IdentityState, PrincipalKind};
use uuid::Uuid;

use crate::{ApiError, ApiResult, RequestPrincipal, RequestPrincipalKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizationMode {
    /// Desktop/local mode: the host process is the trust boundary when no
    /// principal is attached. An attached principal is still checked.
    LocalTrusted,
    /// Server mode: every workspace request must carry an active principal.
    Server,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Capability {
    WorkspaceRead,
    WorkspaceWrite,
    WorkspaceAdmin,
    IdentityManage,
    ConnectorRead,
    ConnectorWrite,
    ConnectorTest,
    DatasetRead,
    DatasetWrite,
    PlanRead,
    PlanWrite,
    JobRead,
    JobWrite,
    RunRead,
    EventRead,
    ArtifactRead,
    ArtifactDownload,
    ExportWrite,
    AutomationRead,
    AutomationCreate,
    AutomationUpdate,
    AutomationPause,
    AutomationResume,
    AutomationDelete,
    AutomationTrigger,
    CredentialManage,
    AuditRead,
    AuditExport,
}

impl Capability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkspaceRead => "workspace:read",
            Self::WorkspaceWrite => "workspace:write",
            Self::WorkspaceAdmin => "workspace:admin",
            Self::IdentityManage => "identity:manage",
            Self::ConnectorRead => "connector:read",
            Self::ConnectorWrite => "connector:write",
            Self::ConnectorTest => "connector:test",
            Self::DatasetRead => "dataset:read",
            Self::DatasetWrite => "dataset:write",
            Self::PlanRead => "plan:read",
            Self::PlanWrite => "plan:write",
            Self::JobRead => "job:read",
            Self::JobWrite => "job:write",
            Self::RunRead => "run:read",
            Self::EventRead => "event:read",
            Self::ArtifactRead => "artifact:read",
            Self::ArtifactDownload => "artifact:download",
            Self::ExportWrite => "export:write",
            Self::AutomationRead => "automation:read",
            Self::AutomationCreate => "automation:create",
            Self::AutomationUpdate => "automation:update",
            Self::AutomationPause => "automation:pause",
            Self::AutomationResume => "automation:resume",
            Self::AutomationDelete => "automation:delete",
            Self::AutomationTrigger => "automation:trigger",
            Self::CredentialManage => "credential:manage",
            Self::AuditRead => "audit:read",
            Self::AuditExport => "audit:export",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "workspace:read" => Self::WorkspaceRead,
            "workspace:write" => Self::WorkspaceWrite,
            "workspace:admin" => Self::WorkspaceAdmin,
            "identity:manage" => Self::IdentityManage,
            "connector:read" => Self::ConnectorRead,
            "connector:write" => Self::ConnectorWrite,
            "connector:test" => Self::ConnectorTest,
            "dataset:read" => Self::DatasetRead,
            "dataset:write" => Self::DatasetWrite,
            "plan:read" => Self::PlanRead,
            "plan:write" => Self::PlanWrite,
            "job:read" => Self::JobRead,
            "job:write" => Self::JobWrite,
            "run:read" => Self::RunRead,
            "event:read" => Self::EventRead,
            "artifact:read" => Self::ArtifactRead,
            "artifact:download" => Self::ArtifactDownload,
            "export:write" => Self::ExportWrite,
            "automation:read" => Self::AutomationRead,
            "automation:create" => Self::AutomationCreate,
            "automation:update" => Self::AutomationUpdate,
            "automation:pause" => Self::AutomationPause,
            "automation:resume" => Self::AutomationResume,
            "automation:delete" => Self::AutomationDelete,
            "automation:trigger" => Self::AutomationTrigger,
            "credential:manage" => Self::CredentialManage,
            "audit:read" => Self::AuditRead,
            "audit:export" => Self::AuditExport,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
struct PrincipalKey {
    workspace_id: Uuid,
    kind: RequestPrincipalKind,
    id: Uuid,
}

#[derive(Debug, Clone)]
struct CachedPrincipal {
    capabilities: BTreeSet<String>,
}

#[derive(Clone)]
pub(crate) struct AuthorizationGate {
    control_plane: Arc<ControlPlaneStore>,
    mode: AuthorizationMode,
    cache: Arc<RwLock<BTreeMap<PrincipalKey, CachedPrincipal>>>,
}

impl AuthorizationGate {
    pub(crate) fn new(control_plane: Arc<ControlPlaneStore>) -> Self {
        Self {
            control_plane,
            mode: AuthorizationMode::LocalTrusted,
            cache: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }

    pub(crate) fn with_mode(mut self, mode: AuthorizationMode) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn mode(&self) -> AuthorizationMode {
        self.mode
    }

    pub(crate) fn authorize(
        &self,
        workspace_id: Uuid,
        principal: Option<RequestPrincipal>,
        capability: Capability,
    ) -> ApiResult<()> {
        if self.mode == AuthorizationMode::LocalTrusted && principal.is_none() {
            return Ok(());
        }
        let principal = principal.ok_or_else(ApiError::unauthorized)?;
        let workspace = self
            .control_plane
            .get_workspace(workspace_id)
            .map_err(|_| ApiError::unauthorized())?;
        if workspace.state != WorkspaceState::Active {
            return Err(ApiError::unauthorized());
        }
        let key = PrincipalKey {
            workspace_id,
            kind: principal.kind,
            id: principal.id,
        };
        let cached = self
            .cache
            .read()
            .map_err(|_| ApiError::internal())?
            .get(&key)
            .cloned();
        let snapshot = match cached {
            Some(snapshot) => snapshot,
            None => {
                let snapshot = CachedPrincipal {
                    capabilities: self.load_capabilities(key)?,
                };
                self.cache
                    .write()
                    .map_err(|_| ApiError::internal())?
                    .insert(key, snapshot.clone());
                snapshot
            }
        };
        if snapshot.capabilities.contains(capability.as_str())
            || snapshot
                .capabilities
                .contains(Capability::WorkspaceAdmin.as_str())
        {
            Ok(())
        } else {
            Err(ApiError::unauthorized())
        }
    }

    pub(crate) fn invalidate_workspace(&self, workspace_id: Uuid) {
        if let Ok(mut cache) = self.cache.write() {
            cache.retain(|key, _| key.workspace_id != workspace_id);
        }
    }

    pub(crate) fn is_local_trusted(&self) -> bool {
        self.mode == AuthorizationMode::LocalTrusted
    }

    fn load_capabilities(&self, key: PrincipalKey) -> ApiResult<BTreeSet<String>> {
        let identity = self.control_plane.identity();
        match key.kind {
            RequestPrincipalKind::Member => {
                let member = identity
                    .get_member(key.workspace_id, key.id)
                    .map_err(|_| ApiError::unauthorized())?;
                if member.state != IdentityState::Active {
                    return Err(ApiError::unauthorized());
                }
                let mut capabilities = BTreeSet::new();
                for role_id in identity
                    .member_role_ids(key.workspace_id, key.id)
                    .map_err(|_| ApiError::unauthorized())?
                {
                    let role = identity
                        .get_role(key.workspace_id, role_id)
                        .map_err(|_| ApiError::unauthorized())?;
                    capabilities.extend(role.capabilities);
                }
                Ok(capabilities)
            }
            // SEC-S1 persists service-account lifecycle but deliberately does
            // not grant implicit privileges. A future role-assignment
            // contract must explicitly add them here.
            RequestPrincipalKind::ServiceAccount => {
                let account = identity
                    .get_service_account(key.workspace_id, key.id)
                    .map_err(|_| ApiError::unauthorized())?;
                if account.state != IdentityState::Active {
                    return Err(ApiError::unauthorized());
                }
                Ok(BTreeSet::new())
            }
        }
    }
}

impl From<RequestPrincipalKind> for PrincipalKind {
    fn from(value: RequestPrincipalKind) -> Self {
        match value {
            RequestPrincipalKind::Member => Self::Member,
            RequestPrincipalKind::ServiceAccount => Self::ServiceAccount,
        }
    }
}
