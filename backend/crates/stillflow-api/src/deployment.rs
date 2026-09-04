//! Transport-neutral service packaging and Desktop-daemon contracts.
//!
//! This module describes the boundary that an OS service, Desktop shell, or
//! remote transport may implement. It deliberately does not spawn processes or
//! own execution state: the API manifest, health views, scheduler, and job
//! runtime remain the canonical authorities.

use crate::{ApiVersion, HealthStatus, RouteManifest, BOOTSTRAP_MANIFEST, SUPPORTED_API_VERSIONS};
use serde::{Deserialize, Deserializer, Serialize};
use thiserror::Error;

const DEFAULT_MANAGED_ROOT: &str = ".stillflow";
const DEFAULT_BIND_HOST: &str = "127.0.0.1";
const DEFAULT_SHUTDOWN_GRACE_SECONDS: u16 = 30;
const DEFAULT_MAX_RECOVERY_ATTEMPTS: u8 = 3;
const MAX_SHUTDOWN_GRACE_SECONDS: u16 = 3_600;
const MAX_RECOVERY_ATTEMPTS: u8 = 10;
const CREDENTIAL_REFERENCE_PREFIX: &str = "credential://";

/// A transport adapter that may expose the same API contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TransportKind {
    /// A Desktop shell or local CLI talking to a local daemon.
    DesktopLocal,
    /// A Web or other remote client talking to the service boundary.
    WebRemote,
}

impl TransportKind {
    pub const fn is_local(self) -> bool {
        matches!(self, Self::DesktopLocal)
    }
}

/// The host platform selected by the service packaging layer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ServicePlatform {
    Windows,
    MacOs,
    Linux,
    Unknown,
}

/// A non-secret reference resolved by the platform credential provider.
///
/// The value is intentionally constrained to the `credential://` namespace so
/// plaintext passwords, tokens, and arbitrary environment values cannot be
/// mistaken for a persisted credential reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialReference(String);

impl CredentialReference {
    pub fn new(value: impl Into<String>) -> Result<Self, DeploymentError> {
        let value = value.into();
        if value.trim() != value
            || !value.starts_with(CREDENTIAL_REFERENCE_PREFIX)
            || value[CREDENTIAL_REFERENCE_PREFIX.len()..].is_empty()
            || value
                .chars()
                .any(|character| character.is_whitespace() || character.is_control())
            || value.contains('=')
        {
            return Err(DeploymentError::InvalidCredentialReference);
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for CredentialReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}

/// Bounded configuration consumed by an eventual OS service or Desktop shell.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceConfig {
    pub api_version: ApiVersion,
    pub platform: ServicePlatform,
    pub transport: TransportKind,
    pub managed_root: String,
    pub bind_host: String,
    pub bind_port: u16,
    pub allow_remote: bool,
    pub credential_reference: Option<CredentialReference>,
    pub shutdown_grace_seconds: u16,
    pub max_recovery_attempts: u8,
}

impl Default for ServiceConfig {
    fn default() -> Self {
        Self {
            api_version: ApiVersion::default(),
            platform: ServicePlatform::Unknown,
            transport: TransportKind::DesktopLocal,
            managed_root: DEFAULT_MANAGED_ROOT.to_owned(),
            bind_host: DEFAULT_BIND_HOST.to_owned(),
            bind_port: 0,
            allow_remote: false,
            credential_reference: None,
            shutdown_grace_seconds: DEFAULT_SHUTDOWN_GRACE_SECONDS,
            max_recovery_attempts: DEFAULT_MAX_RECOVERY_ATTEMPTS,
        }
    }
}

impl ServiceConfig {
    /// Validate the safety properties that an adapter must enforce before
    /// starting a service. Port zero means "let the adapter choose a port".
    pub fn validate(&self) -> Result<(), DeploymentError> {
        if !self.api_version.is_supported() {
            return Err(DeploymentError::UnsupportedApiVersion(
                self.api_version.value(),
            ));
        }
        if !valid_managed_root(&self.managed_root) {
            return Err(DeploymentError::InvalidManagedRoot);
        }
        if !valid_bind_host(&self.bind_host) {
            return Err(DeploymentError::InvalidBindHost);
        }
        if self.transport.is_local() && self.allow_remote {
            return Err(DeploymentError::LocalTransportCannotAllowRemote);
        }
        if self.allow_remote && is_loopback_host(&self.bind_host) {
            return Err(DeploymentError::RemoteBindingMustNotUseLoopback);
        }
        if self.shutdown_grace_seconds == 0
            || self.shutdown_grace_seconds > MAX_SHUTDOWN_GRACE_SECONDS
        {
            return Err(DeploymentError::InvalidShutdownGracePeriod);
        }
        if self.max_recovery_attempts > MAX_RECOVERY_ATTEMPTS {
            return Err(DeploymentError::InvalidRecoveryAttemptLimit);
        }
        Ok(())
    }

    pub fn transport_contract(&self) -> TransportContract {
        TransportContract::for_transport(self.transport)
    }
}

fn valid_managed_root(value: &str) -> bool {
    let trimmed = value.trim();
    let components = trimmed.split(['/', '\\']).collect::<Vec<_>>();
    !trimmed.is_empty()
        && trimmed != "."
        && trimmed != "/"
        && trimmed != "\\"
        && !trimmed.ends_with(':')
        && !trimmed.chars().any(char::is_control)
        && !components.iter().any(|component| *component == "..")
        && !(components.len() == 2 && components[0].ends_with(':') && components[1].is_empty())
}

fn valid_bind_host(value: &str) -> bool {
    !value.is_empty()
        && !value
            .chars()
            .any(|character| character.is_whitespace() || character.is_control())
        && !value.contains('/')
        && !value.contains('\\')
}

fn is_loopback_host(value: &str) -> bool {
    matches!(value, "localhost" | "127.0.0.1" | "::1" | "[::1]")
}

/// The version and route manifest that a transport adapter must expose.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransportContract {
    pub api_version: ApiVersion,
    pub transport: TransportKind,
    pub manifest: RouteManifest,
}

impl TransportContract {
    pub const fn for_transport(transport: TransportKind) -> Self {
        Self {
            api_version: ApiVersion::new(BOOTSTRAP_MANIFEST.api_version),
            transport,
            manifest: BOOTSTRAP_MANIFEST,
        }
    }

    pub fn is_protocol_equivalent_to(self, other: Self) -> bool {
        self.api_version == other.api_version && self.manifest == other.manifest
    }

    pub fn supported_versions(self) -> &'static [ApiVersion] {
        SUPPORTED_API_VERSIONS
    }
}

/// Lifecycle state for the process/service wrapper around the existing
/// scheduler and runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DaemonState {
    Stopped,
    Starting,
    Ready,
    Draining,
    Failed,
    Recovering,
}

/// Deterministic lifecycle state machine with bounded recovery attempts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DaemonLifecycle {
    state: DaemonState,
    recovery_attempts: u8,
    max_recovery_attempts: u8,
}

impl DaemonLifecycle {
    pub fn new(max_recovery_attempts: u8) -> Result<Self, DeploymentError> {
        if max_recovery_attempts > MAX_RECOVERY_ATTEMPTS {
            return Err(DeploymentError::InvalidRecoveryAttemptLimit);
        }
        Ok(Self {
            state: DaemonState::Stopped,
            recovery_attempts: 0,
            max_recovery_attempts,
        })
    }

    pub const fn state(&self) -> DaemonState {
        self.state
    }

    pub const fn recovery_attempts(&self) -> u8 {
        self.recovery_attempts
    }

    pub const fn is_accepting_requests(&self) -> bool {
        matches!(self.state, DaemonState::Ready)
    }

    pub const fn health_status(&self) -> HealthStatus {
        match self.state {
            DaemonState::Ready => HealthStatus::Healthy,
            DaemonState::Stopped => HealthStatus::Unavailable,
            DaemonState::Starting
            | DaemonState::Draining
            | DaemonState::Failed
            | DaemonState::Recovering => HealthStatus::Degraded,
        }
    }

    pub fn start(&mut self) -> Result<(), LifecycleError> {
        self.transition(DaemonState::Stopped, DaemonState::Starting, "start")
    }

    pub fn mark_ready(&mut self) -> Result<(), LifecycleError> {
        self.transition(DaemonState::Starting, DaemonState::Ready, "mark_ready")
    }

    pub fn begin_shutdown(&mut self) -> Result<(), LifecycleError> {
        match self.state {
            DaemonState::Starting | DaemonState::Ready => {
                self.state = DaemonState::Draining;
                Ok(())
            }
            state => Err(LifecycleError::InvalidTransition {
                state,
                operation: "begin_shutdown",
            }),
        }
    }

    pub fn complete_shutdown(&mut self) -> Result<(), LifecycleError> {
        self.transition(
            DaemonState::Draining,
            DaemonState::Stopped,
            "complete_shutdown",
        )
    }

    pub fn mark_failed(&mut self) -> Result<(), LifecycleError> {
        match self.state {
            DaemonState::Starting
            | DaemonState::Ready
            | DaemonState::Draining
            | DaemonState::Recovering => {
                self.state = DaemonState::Failed;
                Ok(())
            }
            state => Err(LifecycleError::InvalidTransition {
                state,
                operation: "mark_failed",
            }),
        }
    }

    pub fn begin_recovery(&mut self) -> Result<(), LifecycleError> {
        if self.state != DaemonState::Failed {
            return Err(LifecycleError::InvalidTransition {
                state: self.state,
                operation: "begin_recovery",
            });
        }
        if self.recovery_attempts >= self.max_recovery_attempts {
            return Err(LifecycleError::RecoveryExhausted {
                attempts: self.recovery_attempts,
            });
        }
        self.recovery_attempts += 1;
        self.state = DaemonState::Recovering;
        Ok(())
    }

    pub fn complete_recovery(&mut self) -> Result<(), LifecycleError> {
        self.transition(
            DaemonState::Recovering,
            DaemonState::Ready,
            "complete_recovery",
        )
    }

    pub fn reset_recovery_budget(&mut self) -> Result<(), LifecycleError> {
        if self.state != DaemonState::Stopped {
            return Err(LifecycleError::InvalidTransition {
                state: self.state,
                operation: "reset_recovery_budget",
            });
        }
        self.recovery_attempts = 0;
        Ok(())
    }

    fn transition(
        &mut self,
        expected: DaemonState,
        target: DaemonState,
        operation: &'static str,
    ) -> Result<(), LifecycleError> {
        if self.state != expected {
            return Err(LifecycleError::InvalidTransition {
                state: self.state,
                operation,
            });
        }
        self.state = target;
        Ok(())
    }
}

/// A versioned upgrade whose rollback target is explicit and validated.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpgradePlan {
    pub from: ApiVersion,
    pub to: ApiVersion,
    pub rollback_to: ApiVersion,
}

impl UpgradePlan {
    pub const fn new(from: ApiVersion, to: ApiVersion) -> Self {
        Self {
            from,
            to,
            rollback_to: from,
        }
    }

    pub const fn with_rollback_to(mut self, rollback_to: ApiVersion) -> Self {
        self.rollback_to = rollback_to;
        self
    }

    pub fn validate(self) -> Result<(), DeploymentError> {
        for version in [self.from, self.to, self.rollback_to] {
            if !version.is_supported() {
                return Err(DeploymentError::UnsupportedApiVersion(version.value()));
            }
        }
        if self.from == self.to {
            return Err(DeploymentError::UpgradeIsNoop);
        }
        if self.rollback_to != self.from {
            return Err(DeploymentError::RollbackTargetMustMatchSource);
        }
        Ok(())
    }

    pub const fn rollback_plan(self) -> RollbackPlan {
        RollbackPlan {
            from: self.to,
            to: self.rollback_to,
        }
    }
}

/// The reversible portion of an upgrade plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollbackPlan {
    pub from: ApiVersion,
    pub to: ApiVersion,
}

impl RollbackPlan {
    pub fn validate(self) -> Result<(), DeploymentError> {
        if !self.from.is_supported() {
            return Err(DeploymentError::UnsupportedApiVersion(self.from.value()));
        }
        if !self.to.is_supported() {
            return Err(DeploymentError::UnsupportedApiVersion(self.to.value()));
        }
        if self.from == self.to {
            return Err(DeploymentError::RollbackIsNoop);
        }
        Ok(())
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum DeploymentError {
    #[error("unsupported API version {0}")]
    UnsupportedApiVersion(u16),
    #[error("managed_root must be a non-root path without parent traversal")]
    InvalidManagedRoot,
    #[error("bind_host is not a valid host name")]
    InvalidBindHost,
    #[error("DesktopLocal transport cannot allow remote clients")]
    LocalTransportCannotAllowRemote,
    #[error("remote binding must not use a loopback host")]
    RemoteBindingMustNotUseLoopback,
    #[error("credential reference must use the credential:// namespace and contain no plaintext")]
    InvalidCredentialReference,
    #[error("shutdown grace period must be between one second and one hour")]
    InvalidShutdownGracePeriod,
    #[error("recovery attempt limit exceeds the bounded maximum")]
    InvalidRecoveryAttemptLimit,
    #[error("upgrade from and to versions must differ")]
    UpgradeIsNoop,
    #[error("rollback target must match the upgrade source version")]
    RollbackTargetMustMatchSource,
    #[error("rollback from and to versions must differ")]
    RollbackIsNoop,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum LifecycleError {
    #[error("invalid daemon lifecycle transition: {operation} from {state:?}")]
    InvalidTransition {
        state: DaemonState,
        operation: &'static str,
    },
    #[error("daemon recovery exhausted after {attempts} attempts")]
    RecoveryExhausted { attempts: u8 },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::API_V1;

    #[test]
    fn defaults_are_local_safe_and_bounded() {
        let config = ServiceConfig::default();
        assert_eq!(config.transport, TransportKind::DesktopLocal);
        assert_eq!(config.bind_host, DEFAULT_BIND_HOST);
        assert!(!config.allow_remote);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn remote_binding_requires_explicit_non_loopback_configuration() {
        let mut config = ServiceConfig {
            transport: TransportKind::WebRemote,
            allow_remote: true,
            ..ServiceConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(DeploymentError::RemoteBindingMustNotUseLoopback)
        );
        config.bind_host = "0.0.0.0".to_owned();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn local_transport_cannot_become_remote() {
        let config = ServiceConfig {
            allow_remote: true,
            ..ServiceConfig::default()
        };
        assert_eq!(
            config.validate(),
            Err(DeploymentError::LocalTransportCannotAllowRemote)
        );
    }

    #[test]
    fn credential_boundary_rejects_plaintext_and_accepts_reference() {
        assert_eq!(
            CredentialReference::new("password").unwrap_err(),
            DeploymentError::InvalidCredentialReference
        );
        let reference = CredentialReference::new("credential://desktop/keychain/api").unwrap();
        assert_eq!(reference.as_str(), "credential://desktop/keychain/api");
        let encoded = serde_json::to_string(&reference).expect("serialize reference");
        assert_eq!(encoded, "\"credential://desktop/keychain/api\"");
        assert!(serde_json::from_str::<CredentialReference>("\"token=secret\"").is_err());
    }

    #[test]
    fn managed_root_rejects_root_and_parent_traversal() {
        for root in ["", ".", "/", "C:\\", "../outside", "data/../../outside"] {
            let config = ServiceConfig {
                managed_root: root.to_owned(),
                ..ServiceConfig::default()
            };
            assert_eq!(
                config.validate(),
                Err(DeploymentError::InvalidManagedRoot),
                "root {root:?} should be rejected"
            );
        }
    }

    #[test]
    fn desktop_and_web_adapters_share_the_protocol_contract() {
        let desktop = TransportContract::for_transport(TransportKind::DesktopLocal);
        let web = TransportContract::for_transport(TransportKind::WebRemote);
        assert!(desktop.is_protocol_equivalent_to(web));
        assert_eq!(
            desktop.api_version,
            ApiVersion::new(BOOTSTRAP_MANIFEST.api_version)
        );
        assert_eq!(desktop.supported_versions(), SUPPORTED_API_VERSIONS);
    }

    #[test]
    fn lifecycle_accepts_graceful_shutdown() {
        let mut lifecycle = DaemonLifecycle::new(3).expect("bounded lifecycle");
        assert_eq!(lifecycle.health_status(), HealthStatus::Unavailable);
        lifecycle.start().expect("start");
        lifecycle.mark_ready().expect("ready");
        assert!(lifecycle.is_accepting_requests());
        lifecycle.begin_shutdown().expect("drain");
        assert_eq!(lifecycle.state(), DaemonState::Draining);
        lifecycle.complete_shutdown().expect("stopped");
        assert!(!lifecycle.is_accepting_requests());
    }

    #[test]
    fn lifecycle_recovery_is_bounded_and_invalid_transitions_are_rejected() {
        let mut lifecycle = DaemonLifecycle::new(1).expect("bounded lifecycle");
        assert_eq!(
            lifecycle.mark_ready().unwrap_err(),
            LifecycleError::InvalidTransition {
                state: DaemonState::Stopped,
                operation: "mark_ready"
            }
        );
        lifecycle.start().expect("start");
        lifecycle.mark_ready().expect("ready");
        lifecycle.mark_failed().expect("failure");
        lifecycle.begin_recovery().expect("one recovery");
        lifecycle.complete_recovery().expect("recovered");
        lifecycle.mark_failed().expect("second failure");
        assert_eq!(
            lifecycle.begin_recovery().unwrap_err(),
            LifecycleError::RecoveryExhausted { attempts: 1 }
        );
    }

    #[test]
    fn upgrade_plan_requires_a_supported_future_version_and_explicit_rollback() {
        let plan = UpgradePlan::new(API_V1, ApiVersion::new(2));
        assert_eq!(
            plan.validate(),
            Err(DeploymentError::UnsupportedApiVersion(2))
        );
        let noop = UpgradePlan::new(API_V1, API_V1);
        assert_eq!(noop.validate(), Err(DeploymentError::UpgradeIsNoop));
    }
}
