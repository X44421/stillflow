//! SEC-S1 identity and credential persistence.
//!
//! This module owns the storage/provider seam only. It deliberately does not
//! implement authorization middleware or transport endpoints. Secret material
//! is accepted only by provider adapters and never by the SQLite-facing APIs.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use stillflow_core::CredentialRef;
use thiserror::Error;
use uuid::Uuid;

use crate::{acquire_activity, format_timestamp, open_connection, ActivityKind, StorageError};
use crate::{StoreInner, STORAGE_SCHEMA_VERSION};

const MAX_IDENTITY_TEXT: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum IdentityState {
    Active,
    Revoked,
}

impl IdentityState {
    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "active" => Ok(Self::Active),
            "revoked" => Ok(Self::Revoked),
            _ => Err(StorageError::Identity("unknown identity state")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum PrincipalKind {
    Member,
    ServiceAccount,
}

impl PrincipalKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::ServiceAccount => "service_account",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "member" => Ok(Self::Member),
            "service_account" => Ok(Self::ServiceAccount),
            _ => Err(StorageError::Identity("unknown credential owner kind")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum CredentialState {
    Pending,
    Active,
    Rotating,
    Revoked,
    Expired,
    RecoveryRequired,
}

impl CredentialState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Active => "active",
            Self::Rotating => "rotating",
            Self::Revoked => "revoked",
            Self::Expired => "expired",
            Self::RecoveryRequired => "recovery_required",
        }
    }

    fn parse(value: &str) -> Result<Self, StorageError> {
        match value {
            "pending" => Ok(Self::Pending),
            "active" => Ok(Self::Active),
            "rotating" => Ok(Self::Rotating),
            "revoked" => Ok(Self::Revoked),
            "expired" => Ok(Self::Expired),
            "recovery_required" => Ok(Self::RecoveryRequired),
            _ => Err(StorageError::Identity("unknown credential state")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MemberRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub subject_ref: String,
    pub state: IdentityState,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoleRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub capabilities: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceAccountRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub name: String,
    pub state: IdentityState,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialOwner {
    pub kind: PrincipalKind,
    pub id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CredentialRefRecord {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub owner: CredentialOwner,
    pub provider_kind: String,
    pub credential_ref: CredentialRef,
    pub state: CredentialState,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialRefDraft {
    pub id: Uuid,
    pub workspace_id: Uuid,
    pub owner: CredentialOwner,
    pub provider_kind: String,
    pub credential_ref: CredentialRef,
    pub created_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum CredentialProviderError {
    #[error("credential is unavailable")]
    Unavailable,
    #[error("credential provider denied the operation")]
    Denied,
    #[error("credential provider does not support the operation")]
    Unsupported,
    #[error("credential reference is invalid")]
    InvalidReference,
    #[error("credential provider is not registered")]
    ProviderNotRegistered,
}

/// Secret material is intentionally non-serializable and redacted in Debug.
/// It is for the short-lived provider boundary only, never for persistence.
pub struct SecretMaterial(Vec<u8>);

impl SecretMaterial {
    pub fn new(value: impl Into<Vec<u8>>) -> Self {
        Self(value.into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretMaterial {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretMaterial")
            .field("value", &"[REDACTED]")
            .finish()
    }
}

impl Drop for SecretMaterial {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            *byte = 0;
        }
    }
}

/// Provider-neutral credential seam. Implementations must not put material in
/// domain records, logs, errors, or durable artifacts.
pub trait CredentialProvider: Send + Sync {
    fn kind(&self) -> &'static str;

    fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> Result<SecretMaterial, CredentialProviderError>;

    fn store(
        &self,
        _credential_ref: &CredentialRef,
        _material: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }

    fn rotate(
        &self,
        _old_ref: &CredentialRef,
        _new_ref: &CredentialRef,
        _replacement: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }

    fn revoke(&self, _credential_ref: &CredentialRef) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }

    fn recover(&self, _credential_ref: &CredentialRef) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }
}

#[derive(Default)]
pub struct CredentialProviderRegistry {
    providers: BTreeMap<String, Arc<dyn CredentialProvider>>,
}

impl CredentialProviderRegistry {
    pub fn register(
        &mut self,
        provider: Arc<dyn CredentialProvider>,
    ) -> Result<(), CredentialProviderError> {
        let kind = provider.kind();
        if kind.is_empty() || self.providers.contains_key(kind) {
            return Err(CredentialProviderError::InvalidReference);
        }
        self.providers.insert(kind.to_owned(), provider);
        Ok(())
    }

    pub fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> Result<SecretMaterial, CredentialProviderError> {
        let kind = provider_kind(credential_ref)?;
        self.providers
            .get(kind)
            .ok_or(CredentialProviderError::ProviderNotRegistered)?
            .resolve(credential_ref)
    }
}

#[derive(Debug, Clone)]
pub struct EnvironmentCredentialProvider {
    prefix: String,
}

impl EnvironmentCredentialProvider {
    pub fn new(prefix: impl Into<String>) -> Result<Self, CredentialProviderError> {
        let prefix = prefix.into();
        if prefix.chars().any(|character| character.is_control()) {
            return Err(CredentialProviderError::InvalidReference);
        }
        Ok(Self { prefix })
    }

    fn variable_name(
        &self,
        credential_ref: &CredentialRef,
    ) -> Result<String, CredentialProviderError> {
        let key = provider_key(credential_ref, "env")?;
        if key
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || character == '_'))
        {
            return Err(CredentialProviderError::InvalidReference);
        }
        Ok(format!("{}{}", self.prefix, key))
    }
}

impl CredentialProvider for EnvironmentCredentialProvider {
    fn kind(&self) -> &'static str {
        "env"
    }

    fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> Result<SecretMaterial, CredentialProviderError> {
        let variable = self.variable_name(credential_ref)?;
        std::env::var(variable)
            .map(SecretMaterial::new)
            .map_err(|_| CredentialProviderError::Unavailable)
    }
}

pub trait KeychainBackend: Send + Sync {
    fn get(&self, key: &str) -> Result<SecretMaterial, CredentialProviderError>;
    fn put(&self, key: &str, material: &SecretMaterial) -> Result<(), CredentialProviderError>;
    fn delete(&self, key: &str) -> Result<(), CredentialProviderError>;
    fn recover(&self, _key: &str) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }
}

pub struct OsKeychainProvider<B> {
    backend: Arc<B>,
}

impl<B> OsKeychainProvider<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

impl<B: KeychainBackend> CredentialProvider for OsKeychainProvider<B> {
    fn kind(&self) -> &'static str {
        "keychain"
    }

    fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> Result<SecretMaterial, CredentialProviderError> {
        self.backend.get(provider_key(credential_ref, "keychain")?)
    }

    fn store(
        &self,
        credential_ref: &CredentialRef,
        material: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        self.backend
            .put(provider_key(credential_ref, "keychain")?, material)
    }

    fn rotate(
        &self,
        old_ref: &CredentialRef,
        new_ref: &CredentialRef,
        replacement: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        self.store(new_ref, replacement)?;
        self.revoke(old_ref)
    }

    fn revoke(&self, credential_ref: &CredentialRef) -> Result<(), CredentialProviderError> {
        self.backend
            .delete(provider_key(credential_ref, "keychain")?)
    }

    fn recover(&self, credential_ref: &CredentialRef) -> Result<(), CredentialProviderError> {
        self.backend
            .recover(provider_key(credential_ref, "keychain")?)
    }
}

pub trait ExternalCredentialBackend: Send + Sync {
    fn resolve(&self, reference: &str) -> Result<SecretMaterial, CredentialProviderError>;
    fn store(
        &self,
        _reference: &str,
        _material: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }
    fn rotate(
        &self,
        _old_reference: &str,
        _new_reference: &str,
        _replacement: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }
    fn revoke(&self, _reference: &str) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }
    fn recover(&self, _reference: &str) -> Result<(), CredentialProviderError> {
        Err(CredentialProviderError::Unsupported)
    }
}

pub struct ExternalCredentialProvider<B> {
    backend: Arc<B>,
}

impl<B> ExternalCredentialProvider<B> {
    pub fn new(backend: Arc<B>) -> Self {
        Self { backend }
    }
}

impl<B: ExternalCredentialBackend> CredentialProvider for ExternalCredentialProvider<B> {
    fn kind(&self) -> &'static str {
        "external"
    }

    fn resolve(
        &self,
        credential_ref: &CredentialRef,
    ) -> Result<SecretMaterial, CredentialProviderError> {
        provider_key(credential_ref, "external")?;
        self.backend.resolve(credential_ref.as_str())
    }

    fn store(
        &self,
        credential_ref: &CredentialRef,
        material: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        provider_key(credential_ref, "external")?;
        self.backend.store(credential_ref.as_str(), material)
    }

    fn rotate(
        &self,
        old_ref: &CredentialRef,
        new_ref: &CredentialRef,
        replacement: &SecretMaterial,
    ) -> Result<(), CredentialProviderError> {
        provider_key(old_ref, "external")?;
        provider_key(new_ref, "external")?;
        self.backend
            .rotate(old_ref.as_str(), new_ref.as_str(), replacement)
    }

    fn revoke(&self, credential_ref: &CredentialRef) -> Result<(), CredentialProviderError> {
        provider_key(credential_ref, "external")?;
        self.backend.revoke(credential_ref.as_str())
    }

    fn recover(&self, credential_ref: &CredentialRef) -> Result<(), CredentialProviderError> {
        provider_key(credential_ref, "external")?;
        self.backend.recover(credential_ref.as_str())
    }
}

#[derive(Clone)]
pub struct IdentityStore {
    inner: Arc<StoreInner>,
}

impl fmt::Debug for IdentityStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IdentityStore")
            .field("storage_schema_version", &STORAGE_SCHEMA_VERSION)
            .finish_non_exhaustive()
    }
}

impl IdentityStore {
    pub(crate) fn from_inner(inner: Arc<StoreInner>) -> Self {
        Self { inner }
    }

    pub fn create_member(
        &self,
        workspace_id: Uuid,
        member_id: Uuid,
        subject_ref: &str,
        created_at: DateTime<Utc>,
    ) -> Result<MemberRecord, StorageError> {
        validate_id(member_id, "member")?;
        validate_text(subject_ref, "member subject")?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        ensure_active_workspace(&connection, workspace_id)?;
        connection
            .execute(
                "INSERT INTO sec_members
                 (id, workspace_id, subject_ref, state, created_at_utc, revoked_at_utc)
                 VALUES (?1, ?2, ?3, 'active', ?4, NULL)",
                params![
                    member_id.to_string(),
                    workspace_id.to_string(),
                    subject_ref,
                    format_timestamp(&created_at)
                ],
            )
            .map_err(map_identity_constraint)?;
        self.member_from_connection(&connection, workspace_id, member_id)
    }

    pub fn revoke_member(
        &self,
        workspace_id: Uuid,
        member_id: Uuid,
        revoked_at: DateTime<Utc>,
    ) -> Result<MemberRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        let current = self.member_from_connection(&connection, workspace_id, member_id)?;
        if revoked_at < current.created_at {
            return Err(StorageError::InvalidTimestampOrder("member revocation"));
        }
        connection
            .execute(
                "UPDATE sec_members SET state = 'revoked', revoked_at_utc = ?3
                 WHERE id = ?1 AND workspace_id = ?2",
                params![
                    member_id.to_string(),
                    workspace_id.to_string(),
                    format_timestamp(&revoked_at)
                ],
            )
            .map_err(|_| StorageError::database("revoke member"))?;
        self.member_from_connection(&connection, workspace_id, member_id)
    }

    pub fn get_member(
        &self,
        workspace_id: Uuid,
        member_id: Uuid,
    ) -> Result<MemberRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        self.member_from_connection(&connection, workspace_id, member_id)
    }

    pub fn create_role(
        &self,
        workspace_id: Uuid,
        role_id: Uuid,
        name: &str,
        created_at: DateTime<Utc>,
    ) -> Result<RoleRecord, StorageError> {
        validate_id(role_id, "role")?;
        validate_text(name, "role name")?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        ensure_active_workspace(&connection, workspace_id)?;
        connection
            .execute(
                "INSERT INTO sec_roles (id, workspace_id, name, created_at_utc)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    role_id.to_string(),
                    workspace_id.to_string(),
                    name,
                    format_timestamp(&created_at)
                ],
            )
            .map_err(map_identity_constraint)?;
        self.role_from_connection(&connection, workspace_id, role_id)
    }

    pub fn get_role(&self, workspace_id: Uuid, role_id: Uuid) -> Result<RoleRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        self.role_from_connection(&connection, workspace_id, role_id)
    }

    pub fn set_role_capabilities(
        &self,
        workspace_id: Uuid,
        role_id: Uuid,
        capabilities: &[&str],
    ) -> Result<RoleRecord, StorageError> {
        for capability in capabilities {
            validate_capability(capability)?;
        }
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin role capability update"))?;
        ensure_role_in_workspace(&transaction, workspace_id, role_id)?;
        transaction
            .execute(
                "DELETE FROM sec_role_capabilities WHERE role_id = ?1",
                params![role_id.to_string()],
            )
            .map_err(|_| StorageError::database("clear role capabilities"))?;
        for capability in capabilities {
            transaction
                .execute(
                    "INSERT INTO sec_role_capabilities (role_id, capability)
                     VALUES (?1, ?2)",
                    params![role_id.to_string(), capability],
                )
                .map_err(map_identity_constraint)?;
        }
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit role capability update"))?;
        self.role_from_connection(&connection, workspace_id, role_id)
    }

    pub fn assign_role(
        &self,
        workspace_id: Uuid,
        member_id: Uuid,
        role_id: Uuid,
    ) -> Result<(), StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin role assignment"))?;
        ensure_member_in_workspace(&transaction, workspace_id, member_id)?;
        ensure_role_in_workspace(&transaction, workspace_id, role_id)?;
        transaction
            .execute(
                "INSERT INTO sec_member_roles (workspace_id, member_id, role_id)
                 VALUES (?1, ?2, ?3)",
                params![
                    workspace_id.to_string(),
                    member_id.to_string(),
                    role_id.to_string()
                ],
            )
            .map_err(map_identity_constraint)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit role assignment"))
    }

    pub fn member_role_ids(
        &self,
        workspace_id: Uuid,
        member_id: Uuid,
    ) -> Result<Vec<Uuid>, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        ensure_member_in_workspace(&connection, workspace_id, member_id)?;
        let mut statement = connection
            .prepare(
                "SELECT role_id FROM sec_member_roles
                 WHERE workspace_id = ?1 AND member_id = ?2 ORDER BY role_id",
            )
            .map_err(|_| StorageError::database("prepare member roles"))?;
        let rows = statement
            .query_map(
                params![workspace_id.to_string(), member_id.to_string()],
                |row| {
                    let value: String = row.get(0)?;
                    Ok(value)
                },
            )
            .map_err(|_| StorageError::database("list member roles"))?;
        rows.map(|row| {
            let value = row.map_err(|_| StorageError::database("decode member role"))?;
            Uuid::parse_str(&value).map_err(|_| StorageError::Identity("invalid role identifier"))
        })
        .collect()
    }

    pub fn create_service_account(
        &self,
        workspace_id: Uuid,
        service_account_id: Uuid,
        name: &str,
        created_at: DateTime<Utc>,
    ) -> Result<ServiceAccountRecord, StorageError> {
        validate_id(service_account_id, "service account")?;
        validate_text(name, "service account name")?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        ensure_active_workspace(&connection, workspace_id)?;
        connection
            .execute(
                "INSERT INTO sec_service_accounts
                 (id, workspace_id, name, state, created_at_utc, revoked_at_utc)
                 VALUES (?1, ?2, ?3, 'active', ?4, NULL)",
                params![
                    service_account_id.to_string(),
                    workspace_id.to_string(),
                    name,
                    format_timestamp(&created_at)
                ],
            )
            .map_err(map_identity_constraint)?;
        self.service_account_from_connection(&connection, workspace_id, service_account_id)
    }

    pub fn revoke_service_account(
        &self,
        workspace_id: Uuid,
        service_account_id: Uuid,
        revoked_at: DateTime<Utc>,
    ) -> Result<ServiceAccountRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        let current =
            self.service_account_from_connection(&connection, workspace_id, service_account_id)?;
        if revoked_at < current.created_at {
            return Err(StorageError::InvalidTimestampOrder(
                "service account revocation",
            ));
        }
        connection
            .execute(
                "UPDATE sec_service_accounts SET state = 'revoked', revoked_at_utc = ?3
                 WHERE id = ?1 AND workspace_id = ?2",
                params![
                    service_account_id.to_string(),
                    workspace_id.to_string(),
                    format_timestamp(&revoked_at)
                ],
            )
            .map_err(|_| StorageError::database("revoke service account"))?;
        self.service_account_from_connection(&connection, workspace_id, service_account_id)
    }

    pub fn get_service_account(
        &self,
        workspace_id: Uuid,
        service_account_id: Uuid,
    ) -> Result<ServiceAccountRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        self.service_account_from_connection(&connection, workspace_id, service_account_id)
    }

    pub fn register_credential_reference(
        &self,
        draft: CredentialRefDraft,
    ) -> Result<CredentialRefRecord, StorageError> {
        validate_id(draft.id, "credential")?;
        validate_text(&draft.provider_kind, "credential provider kind")?;
        validate_provider_reference(&draft.provider_kind, &draft.credential_ref)?;
        validate_expiry(draft.created_at, draft.expires_at)?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        ensure_active_workspace(&connection, draft.workspace_id)?;
        ensure_owner_in_workspace(&connection, draft.workspace_id, draft.owner)?;
        connection
            .execute(
                "INSERT INTO sec_credentials
                 (id, workspace_id, owner_kind, owner_id, provider_kind, credential_ref,
                  state, created_at_utc, expires_at_utc, revoked_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?8, NULL)",
                params![
                    draft.id.to_string(),
                    draft.workspace_id.to_string(),
                    draft.owner.kind.as_str(),
                    draft.owner.id.to_string(),
                    draft.provider_kind,
                    draft.credential_ref.as_str(),
                    format_timestamp(&draft.created_at),
                    draft.expires_at.as_ref().map(format_timestamp),
                ],
            )
            .map_err(map_identity_constraint)?;
        self.credential_from_connection(&connection, draft.workspace_id, draft.id)
    }

    pub fn get_credential_reference(
        &self,
        workspace_id: Uuid,
        credential_id: Uuid,
    ) -> Result<CredentialRefRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Reader)?;
        let connection = open_connection(&self.inner)?;
        self.credential_from_connection(&connection, workspace_id, credential_id)
    }

    pub fn begin_credential_rotation(
        &self,
        workspace_id: Uuid,
        credential_id: Uuid,
        started_at: DateTime<Utc>,
    ) -> Result<CredentialRefRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        let current = self.credential_from_connection(&connection, workspace_id, credential_id)?;
        if !matches!(
            current.state,
            CredentialState::Active | CredentialState::Pending
        ) {
            return Err(StorageError::Identity("credential is not rotatable"));
        }
        if let Some(expires_at) = current.expires_at {
            if expires_at <= started_at {
                return Err(StorageError::Identity("credential is expired"));
            }
        }
        connection
            .execute(
                "UPDATE sec_credentials SET state = 'rotating'
                 WHERE id = ?1 AND workspace_id = ?2",
                params![credential_id.to_string(), workspace_id.to_string()],
            )
            .map_err(|_| StorageError::database("begin credential rotation"))?;
        self.credential_from_connection(&connection, workspace_id, credential_id)
    }

    pub fn complete_credential_rotation(
        &self,
        workspace_id: Uuid,
        old_credential_id: Uuid,
        replacement: CredentialRefDraft,
        rotated_at: DateTime<Utc>,
    ) -> Result<CredentialRefRecord, StorageError> {
        if replacement.workspace_id != workspace_id {
            return Err(StorageError::Identity("credential workspace mismatch"));
        }
        validate_provider_reference(&replacement.provider_kind, &replacement.credential_ref)?;
        validate_expiry(replacement.created_at, replacement.expires_at)?;
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let mut connection = open_connection(&self.inner)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| StorageError::database("begin credential rotation completion"))?;
        let old = raw_credential_in_connection(&transaction, workspace_id, old_credential_id)?;
        let old_state = CredentialState::parse(&old.state)?;
        if !matches!(
            old_state,
            CredentialState::Rotating | CredentialState::RecoveryRequired
        ) {
            return Err(StorageError::Identity(
                "credential rotation is not in progress",
            ));
        }
        let old_owner = CredentialOwner {
            kind: PrincipalKind::parse(&old.owner_kind)?,
            id: old.owner_id,
        };
        if old_owner != replacement.owner {
            return Err(StorageError::Identity("credential owner mismatch"));
        }
        let old_created_at =
            crate::parse_timestamp(&old.created_at, "credential creation timestamp")?;
        if rotated_at < old_created_at {
            return Err(StorageError::InvalidTimestampOrder("credential rotation"));
        }
        transaction
            .execute(
                "UPDATE sec_credentials SET state = 'revoked', revoked_at_utc = ?3
                 WHERE id = ?1 AND workspace_id = ?2",
                params![
                    old_credential_id.to_string(),
                    workspace_id.to_string(),
                    format_timestamp(&rotated_at)
                ],
            )
            .map_err(|_| StorageError::database("revoke rotated credential"))?;
        transaction
            .execute(
                "INSERT INTO sec_credentials
                 (id, workspace_id, owner_kind, owner_id, provider_kind, credential_ref,
                  state, created_at_utc, expires_at_utc, revoked_at_utc)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?8, NULL)",
                params![
                    replacement.id.to_string(),
                    workspace_id.to_string(),
                    replacement.owner.kind.as_str(),
                    replacement.owner.id.to_string(),
                    replacement.provider_kind,
                    replacement.credential_ref.as_str(),
                    format_timestamp(&replacement.created_at),
                    replacement.expires_at.as_ref().map(format_timestamp),
                ],
            )
            .map_err(map_identity_constraint)?;
        transaction
            .commit()
            .map_err(|_| StorageError::database("commit credential rotation completion"))?;
        self.credential_from_connection(&connection, workspace_id, replacement.id)
    }

    pub fn revoke_credential(
        &self,
        workspace_id: Uuid,
        credential_id: Uuid,
        revoked_at: DateTime<Utc>,
    ) -> Result<CredentialRefRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        let current = self.credential_from_connection(&connection, workspace_id, credential_id)?;
        if revoked_at < current.created_at {
            return Err(StorageError::InvalidTimestampOrder("credential revocation"));
        }
        connection
            .execute(
                "UPDATE sec_credentials SET state = 'revoked', revoked_at_utc = ?3
                 WHERE id = ?1 AND workspace_id = ?2 AND state <> 'revoked'",
                params![
                    credential_id.to_string(),
                    workspace_id.to_string(),
                    format_timestamp(&revoked_at)
                ],
            )
            .map_err(|_| StorageError::database("revoke credential"))?;
        self.credential_from_connection(&connection, workspace_id, credential_id)
    }

    /// Marks interrupted rotations as recovery-required. This is called during
    /// every managed-root open, making restart recovery explicit and fail-closed.
    pub(crate) fn recover_incomplete_rotations(&self) -> Result<u64, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        let changed = connection
            .execute(
                "UPDATE sec_credentials SET state = 'recovery_required'
                 WHERE state = 'rotating'",
                [],
            )
            .map_err(|_| StorageError::database("recover interrupted credential rotations"))?;
        Ok(changed as u64)
    }

    /// Explicit provider-verified recovery. No implicit reactivation happens
    /// during startup; callers must perform provider recovery first.
    pub fn recover_credential(
        &self,
        workspace_id: Uuid,
        credential_id: Uuid,
        recovered_at: DateTime<Utc>,
    ) -> Result<CredentialRefRecord, StorageError> {
        let _activity = acquire_activity(&self.inner, ActivityKind::Publisher)?;
        let connection = open_connection(&self.inner)?;
        let current = self.credential_from_connection(&connection, workspace_id, credential_id)?;
        if current.state != CredentialState::RecoveryRequired {
            return Err(StorageError::Identity(
                "credential does not require recovery",
            ));
        }
        let state = if current
            .expires_at
            .is_some_and(|expires_at| expires_at <= recovered_at)
        {
            CredentialState::Expired
        } else {
            CredentialState::Active
        };
        connection
            .execute(
                "UPDATE sec_credentials SET state = ?3
                 WHERE id = ?1 AND workspace_id = ?2",
                params![
                    credential_id.to_string(),
                    workspace_id.to_string(),
                    state.as_str()
                ],
            )
            .map_err(|_| StorageError::database("recover credential"))?;
        self.credential_from_connection(&connection, workspace_id, credential_id)
    }

    fn member_from_connection(
        &self,
        connection: &Connection,
        workspace_id: Uuid,
        member_id: Uuid,
    ) -> Result<MemberRecord, StorageError> {
        let raw = raw_member_in_connection(connection, workspace_id, member_id)?;
        Ok(MemberRecord {
            id: raw.id,
            workspace_id: raw.workspace_id,
            subject_ref: raw.subject_ref,
            state: IdentityState::parse(&raw.state)?,
            created_at: crate::parse_timestamp(&raw.created_at, "member creation timestamp")?,
            revoked_at: raw
                .revoked_at
                .as_deref()
                .map(|value| crate::parse_timestamp(value, "member revocation timestamp"))
                .transpose()?,
        })
    }

    fn role_from_connection(
        &self,
        connection: &Connection,
        workspace_id: Uuid,
        role_id: Uuid,
    ) -> Result<RoleRecord, StorageError> {
        let raw = raw_role_in_connection(connection, workspace_id, role_id)?;
        Ok(RoleRecord {
            id: raw.id,
            workspace_id: raw.workspace_id,
            name: raw.name,
            capabilities: raw.capabilities,
            created_at: crate::parse_timestamp(&raw.created_at, "role creation timestamp")?,
        })
    }

    fn service_account_from_connection(
        &self,
        connection: &Connection,
        workspace_id: Uuid,
        service_account_id: Uuid,
    ) -> Result<ServiceAccountRecord, StorageError> {
        let raw = raw_service_account_in_connection(connection, workspace_id, service_account_id)?;
        Ok(ServiceAccountRecord {
            id: raw.id,
            workspace_id: raw.workspace_id,
            name: raw.name,
            state: IdentityState::parse(&raw.state)?,
            created_at: crate::parse_timestamp(
                &raw.created_at,
                "service account creation timestamp",
            )?,
            revoked_at: raw
                .revoked_at
                .as_deref()
                .map(|value| crate::parse_timestamp(value, "service account revocation timestamp"))
                .transpose()?,
        })
    }

    fn credential_from_connection(
        &self,
        connection: &Connection,
        workspace_id: Uuid,
        credential_id: Uuid,
    ) -> Result<CredentialRefRecord, StorageError> {
        let raw = raw_credential_in_connection(connection, workspace_id, credential_id)?;
        Ok(CredentialRefRecord {
            id: raw.id,
            workspace_id: raw.workspace_id,
            owner: CredentialOwner {
                kind: PrincipalKind::parse(&raw.owner_kind)?,
                id: raw.owner_id,
            },
            provider_kind: raw.provider_kind,
            credential_ref: CredentialRef::new(raw.credential_ref)
                .map_err(|_| StorageError::Identity("stored credential reference is invalid"))?,
            state: CredentialState::parse(&raw.state)?,
            created_at: crate::parse_timestamp(&raw.created_at, "credential creation timestamp")?,
            expires_at: raw
                .expires_at
                .as_deref()
                .map(|value| crate::parse_timestamp(value, "credential expiry timestamp"))
                .transpose()?,
            revoked_at: raw
                .revoked_at
                .as_deref()
                .map(|value| crate::parse_timestamp(value, "credential revocation timestamp"))
                .transpose()?,
        })
    }
}

#[derive(Debug)]
struct RawMember {
    id: Uuid,
    workspace_id: Uuid,
    subject_ref: String,
    state: String,
    created_at: String,
    revoked_at: Option<String>,
}

#[derive(Debug)]
struct RawRole {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    capabilities: Vec<String>,
    created_at: String,
}

#[derive(Debug)]
struct RawServiceAccount {
    id: Uuid,
    workspace_id: Uuid,
    name: String,
    state: String,
    created_at: String,
    revoked_at: Option<String>,
}

#[derive(Debug)]
struct RawCredential {
    id: Uuid,
    workspace_id: Uuid,
    owner_kind: String,
    owner_id: Uuid,
    provider_kind: String,
    credential_ref: String,
    state: String,
    created_at: String,
    expires_at: Option<String>,
    revoked_at: Option<String>,
}

fn raw_member_in_connection(
    connection: &Connection,
    workspace_id: Uuid,
    member_id: Uuid,
) -> Result<RawMember, StorageError> {
    connection
        .query_row(
            "SELECT id, workspace_id, subject_ref, state, created_at_utc, revoked_at_utc
             FROM sec_members WHERE id = ?1 AND workspace_id = ?2",
            params![member_id.to_string(), workspace_id.to_string()],
            |row| {
                let id: String = row.get(0)?;
                let workspace: String = row.get(1)?;
                Ok(RawMember {
                    id: parse_uuid(id),
                    workspace_id: parse_uuid(workspace),
                    subject_ref: row.get(2)?,
                    state: row.get(3)?,
                    created_at: row.get(4)?,
                    revoked_at: row.get(5)?,
                })
            },
        )
        .map_err(|error| map_identity_lookup(error, "member"))
}

fn raw_role_in_connection(
    connection: &Connection,
    workspace_id: Uuid,
    role_id: Uuid,
) -> Result<RawRole, StorageError> {
    let (id, workspace, name, created_at): (String, String, String, String) = connection
        .query_row(
            "SELECT id, workspace_id, name, created_at_utc FROM sec_roles
             WHERE id = ?1 AND workspace_id = ?2",
            params![role_id.to_string(), workspace_id.to_string()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .map_err(|error| map_identity_lookup(error, "role"))?;
    let mut statement = connection
        .prepare(
            "SELECT capability FROM sec_role_capabilities
             WHERE role_id = ?1 ORDER BY capability",
        )
        .map_err(|_| StorageError::database("prepare role capabilities"))?;
    let capabilities = statement
        .query_map(params![role_id.to_string()], |row| row.get(0))
        .map_err(|_| StorageError::database("list role capabilities"))?
        .map(|row| row.map_err(|_| StorageError::database("decode role capability")))
        .collect::<Result<Vec<String>, StorageError>>()?;
    Ok(RawRole {
        id: parse_uuid(id),
        workspace_id: parse_uuid(workspace),
        name,
        capabilities,
        created_at,
    })
}

fn raw_service_account_in_connection(
    connection: &Connection,
    workspace_id: Uuid,
    service_account_id: Uuid,
) -> Result<RawServiceAccount, StorageError> {
    connection
        .query_row(
            "SELECT id, workspace_id, name, state, created_at_utc, revoked_at_utc
             FROM sec_service_accounts WHERE id = ?1 AND workspace_id = ?2",
            params![service_account_id.to_string(), workspace_id.to_string()],
            |row| {
                let id: String = row.get(0)?;
                let workspace: String = row.get(1)?;
                Ok(RawServiceAccount {
                    id: parse_uuid(id),
                    workspace_id: parse_uuid(workspace),
                    name: row.get(2)?,
                    state: row.get(3)?,
                    created_at: row.get(4)?,
                    revoked_at: row.get(5)?,
                })
            },
        )
        .map_err(|error| map_identity_lookup(error, "service account"))
}

fn raw_credential_in_connection(
    connection: &Connection,
    workspace_id: Uuid,
    credential_id: Uuid,
) -> Result<RawCredential, StorageError> {
    connection
        .query_row(
            "SELECT id, workspace_id, owner_kind, owner_id, provider_kind, credential_ref,
                    state, created_at_utc, expires_at_utc, revoked_at_utc
             FROM sec_credentials WHERE id = ?1 AND workspace_id = ?2",
            params![credential_id.to_string(), workspace_id.to_string()],
            |row| {
                let id: String = row.get(0)?;
                let workspace: String = row.get(1)?;
                let owner_kind: String = row.get(2)?;
                let owner_id: String = row.get(3)?;
                Ok(RawCredential {
                    id: parse_uuid(id),
                    workspace_id: parse_uuid(workspace),
                    owner_kind,
                    owner_id: parse_uuid(owner_id),
                    provider_kind: row.get(4)?,
                    credential_ref: row.get(5)?,
                    state: row.get(6)?,
                    created_at: row.get(7)?,
                    expires_at: row.get(8)?,
                    revoked_at: row.get(9)?,
                })
            },
        )
        .map_err(|error| map_identity_lookup(error, "credential"))
}

fn ensure_active_workspace(
    connection: &Connection,
    workspace_id: Uuid,
) -> Result<(), StorageError> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM cp_workspaces WHERE id = ?1 AND state = 'active'",
            params![workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check workspace for identity"))?;
    if exists.is_none() {
        return Err(StorageError::IdentityNotFound);
    }
    Ok(())
}

fn ensure_member_in_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    member_id: Uuid,
) -> Result<(), StorageError> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM sec_members WHERE id = ?1 AND workspace_id = ?2",
            params![member_id.to_string(), workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check member workspace"))?;
    if exists.is_none() {
        return Err(StorageError::IdentityNotFound);
    }
    Ok(())
}

fn ensure_role_in_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    role_id: Uuid,
) -> Result<(), StorageError> {
    let exists: Option<i64> = connection
        .query_row(
            "SELECT 1 FROM sec_roles WHERE id = ?1 AND workspace_id = ?2",
            params![role_id.to_string(), workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check role workspace"))?;
    if exists.is_none() {
        return Err(StorageError::IdentityNotFound);
    }
    Ok(())
}

fn ensure_owner_in_workspace(
    connection: &Connection,
    workspace_id: Uuid,
    owner: CredentialOwner,
) -> Result<(), StorageError> {
    let table = match owner.kind {
        PrincipalKind::Member => "sec_members",
        PrincipalKind::ServiceAccount => "sec_service_accounts",
    };
    let sql = format!("SELECT 1 FROM {table} WHERE id = ?1 AND workspace_id = ?2");
    let exists: Option<i64> = connection
        .query_row(
            &sql,
            params![owner.id.to_string(), workspace_id.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(|_| StorageError::database("check credential owner workspace"))?;
    if exists.is_none() {
        return Err(StorageError::IdentityNotFound);
    }
    Ok(())
}

fn validate_id(value: Uuid, label: &'static str) -> Result<(), StorageError> {
    if value == Uuid::nil() {
        return Err(StorageError::Identity(label));
    }
    Ok(())
}

fn validate_text(value: &str, label: &'static str) -> Result<(), StorageError> {
    if value.is_empty() || value.len() > MAX_IDENTITY_TEXT || value.chars().any(|c| c.is_control())
    {
        return Err(StorageError::Identity(label));
    }
    if value.to_ascii_lowercase().contains("password=")
        || value.to_ascii_lowercase().contains("token=")
        || value.to_ascii_lowercase().contains("private_key=")
    {
        return Err(StorageError::Identity("plaintext secret in identity text"));
    }
    Ok(())
}

fn validate_capability(value: &str) -> Result<(), StorageError> {
    validate_text(value, "capability")?;
    if value.chars().any(|character| {
        !(character.is_ascii_alphanumeric() || matches!(character, ':' | '_' | '-'))
    }) {
        return Err(StorageError::Identity(
            "capability contains unsafe characters",
        ));
    }
    Ok(())
}

fn validate_expiry(
    created_at: DateTime<Utc>,
    expires_at: Option<DateTime<Utc>>,
) -> Result<(), StorageError> {
    if expires_at.is_some_and(|value| value <= created_at) {
        return Err(StorageError::InvalidTimestampOrder("credential expiry"));
    }
    Ok(())
}

fn provider_kind(credential_ref: &CredentialRef) -> Result<&str, CredentialProviderError> {
    let rest = credential_ref
        .as_str()
        .strip_prefix("cred://")
        .ok_or(CredentialProviderError::InvalidReference)?;
    let (kind, key) = rest
        .split_once('/')
        .ok_or(CredentialProviderError::InvalidReference)?;
    if kind.is_empty()
        || key.is_empty()
        || kind
            .chars()
            .any(|character| !(character.is_ascii_alphanumeric() || character == '-'))
    {
        return Err(CredentialProviderError::InvalidReference);
    }
    Ok(kind)
}

fn provider_key<'a>(
    credential_ref: &'a CredentialRef,
    expected_kind: &str,
) -> Result<&'a str, CredentialProviderError> {
    if provider_kind(credential_ref)? != expected_kind {
        return Err(CredentialProviderError::InvalidReference);
    }
    credential_ref
        .as_str()
        .strip_prefix(&format!("cred://{expected_kind}/"))
        .filter(|key| !key.is_empty())
        .ok_or(CredentialProviderError::InvalidReference)
}

fn validate_provider_reference(
    provider_kind_value: &str,
    credential_ref: &CredentialRef,
) -> Result<(), StorageError> {
    let reference_kind = provider_kind(credential_ref)
        .map_err(|_| StorageError::Identity("invalid credential reference"))?;
    if reference_kind != provider_kind_value {
        return Err(StorageError::Identity(
            "credential provider/reference mismatch",
        ));
    }
    validate_text(provider_kind_value, "credential provider kind")
}

fn parse_uuid(value: String) -> Uuid {
    Uuid::parse_str(&value).unwrap_or_else(|_| Uuid::nil())
}

fn map_identity_constraint(error: rusqlite::Error) -> StorageError {
    if matches!(
        error,
        rusqlite::Error::SqliteFailure(ref failure, _)
            if failure.code == rusqlite::ffi::ErrorCode::ConstraintViolation
    ) {
        StorageError::IdentityAlreadyExists
    } else {
        StorageError::database("identity persistence")
    }
}

fn map_identity_lookup(error: rusqlite::Error, _label: &'static str) -> StorageError {
    match error {
        rusqlite::Error::QueryReturnedNoRows => StorageError::IdentityNotFound,
        _ => StorageError::database("identity lookup"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::Mutex;
    use tempfile::TempDir;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).expect("timestamp")
    }

    fn fixture() -> (TempDir, IdentityStore, Uuid, Uuid) {
        let temp = tempfile::tempdir().expect("temp root");
        let control = crate::ControlPlaneStore::open(temp.path()).expect("control store");
        let workspace_id = Uuid::from_u128(11);
        let member_id = Uuid::from_u128(12);
        control
            .create_workspace(workspace_id, at(1_700_000_000))
            .expect("workspace");
        let identity = control.identity();
        identity
            .create_member(workspace_id, member_id, "subject:alice", at(1_700_000_001))
            .expect("member");
        (temp, identity, workspace_id, member_id)
    }

    fn credential(
        workspace_id: Uuid,
        owner_id: Uuid,
        credential_id: Uuid,
        name: &str,
    ) -> CredentialRefDraft {
        CredentialRefDraft {
            id: credential_id,
            workspace_id,
            owner: CredentialOwner {
                kind: PrincipalKind::Member,
                id: owner_id,
            },
            provider_kind: "env".to_owned(),
            credential_ref: CredentialRef::new(format!("cred://env/{name}"))
                .expect("credential ref"),
            created_at: at(1_700_000_002),
            expires_at: Some(at(1_800_000_000)),
        }
    }

    #[test]
    fn persists_workspace_roles_capabilities_and_service_accounts() {
        let (_temp, identity, workspace_id, member_id) = fixture();
        let role_id = Uuid::from_u128(13);
        let service_id = Uuid::from_u128(14);
        identity
            .create_role(workspace_id, role_id, "operator", at(1_700_000_003))
            .expect("role");
        let role = identity
            .set_role_capabilities(workspace_id, role_id, &["dataset:read", "job:execute"])
            .expect("capabilities");
        assert_eq!(role.capabilities, vec!["dataset:read", "job:execute"]);
        identity
            .assign_role(workspace_id, member_id, role_id)
            .expect("assignment");
        assert_eq!(
            identity
                .member_role_ids(workspace_id, member_id)
                .expect("roles"),
            vec![role_id]
        );
        let account = identity
            .create_service_account(workspace_id, service_id, "automation", at(1_700_000_004))
            .expect("service account");
        assert_eq!(account.state, IdentityState::Active);
    }

    #[test]
    fn cross_workspace_owner_lookup_fails_closed() {
        let (_temp, identity, workspace_id, member_id) = fixture();
        let other_workspace = Uuid::from_u128(21);
        let control =
            crate::ControlPlaneStore::open(tempfile::tempdir().expect("other root").path());
        let _ = control;
        let result = identity.register_credential_reference(credential(
            other_workspace,
            member_id,
            Uuid::from_u128(22),
            "MISSING",
        ));
        assert!(matches!(result, Err(StorageError::IdentityNotFound)));
        assert!(identity.get_member(other_workspace, member_id).is_err());
        assert!(identity.get_member(workspace_id, member_id).is_ok());
    }

    #[test]
    fn credential_records_never_persist_plaintext_secret_sentinel() {
        let (temp, identity, workspace_id, member_id) = fixture();
        let record = identity
            .register_credential_reference(credential(
                workspace_id,
                member_id,
                Uuid::from_u128(31),
                "SENTINEL",
            ))
            .expect("credential");
        let debug = format!("{record:?}");
        assert!(!debug.contains("correct-horse-battery-staple"));
        let database = Connection::open(temp.path().join("metadata.sqlite3")).expect("database");
        let raw: String = database
            .query_row(
                "SELECT credential_ref FROM sec_credentials WHERE id = ?1",
                params![record.id.to_string()],
                |row| row.get(0),
            )
            .expect("reference");
        assert_eq!(raw, "cred://env/SENTINEL");
        let dump: String = database
            .query_row(
                "SELECT group_concat(id || ':' || credential_ref || ':' || state) FROM sec_credentials",
                [],
                |row| row.get(0),
            )
            .expect("dump");
        assert!(!dump.contains("correct-horse-battery-staple"));
    }

    #[test]
    fn rotation_revocation_and_restart_recovery_are_explicit() {
        let (temp, identity, workspace_id, member_id) = fixture();
        let old_id = Uuid::from_u128(41);
        let new_id = Uuid::from_u128(42);
        identity
            .register_credential_reference(credential(workspace_id, member_id, old_id, "OLD"))
            .expect("old credential");
        let rotating = identity
            .begin_credential_rotation(workspace_id, old_id, at(1_700_000_005))
            .expect("begin rotation");
        assert_eq!(rotating.state, CredentialState::Rotating);
        drop(identity);
        let reopened = crate::ControlPlaneStore::open(temp.path()).expect("restart");
        let recovered = reopened
            .identity()
            .get_credential_reference(workspace_id, old_id)
            .expect("recovered credential");
        assert_eq!(recovered.state, CredentialState::RecoveryRequired);
        let explicit = reopened
            .identity()
            .recover_credential(workspace_id, old_id, at(1_700_000_006))
            .expect("explicit recovery");
        assert_eq!(explicit.state, CredentialState::Active);
        reopened
            .identity()
            .begin_credential_rotation(workspace_id, old_id, at(1_700_000_006))
            .expect("retry rotation");
        let replacement = reopened
            .identity()
            .complete_credential_rotation(
                workspace_id,
                old_id,
                credential(workspace_id, member_id, new_id, "NEW"),
                at(1_700_000_007),
            )
            .expect("complete rotation");
        assert_eq!(replacement.state, CredentialState::Active);
        assert_eq!(
            reopened
                .identity()
                .revoke_credential(workspace_id, new_id, at(1_700_000_008))
                .expect("revoke")
                .state,
            CredentialState::Revoked
        );
        assert_eq!(
            reopened
                .identity()
                .revoke_credential(workspace_id, new_id, at(1_700_000_009))
                .expect("idempotent revoke")
                .state,
            CredentialState::Revoked
        );
    }

    #[test]
    fn providers_have_env_keychain_external_seams_and_redacted_debug() {
        let sentinel = "correct-horse-battery-staple";
        std::env::set_var("STILLFLOW_TEST_SENTINEL", sentinel);
        let env = EnvironmentCredentialProvider::new("STILLFLOW_").expect("env provider");
        let env_ref = CredentialRef::new("cred://env/TEST_SENTINEL").expect("env ref");
        assert_eq!(
            env.resolve(&env_ref).expect("env secret").as_bytes(),
            sentinel.as_bytes()
        );
        let material = SecretMaterial::new(sentinel.as_bytes().to_vec());
        assert!(!format!("{material:?}").contains(sentinel));
        std::env::remove_var("STILLFLOW_TEST_SENTINEL");

        #[derive(Default)]
        struct FakeKeychain(Mutex<BTreeMap<String, Vec<u8>>>);
        impl KeychainBackend for FakeKeychain {
            fn get(&self, key: &str) -> Result<SecretMaterial, CredentialProviderError> {
                self.0
                    .lock()
                    .expect("lock")
                    .get(key)
                    .cloned()
                    .map(SecretMaterial::new)
                    .ok_or(CredentialProviderError::Unavailable)
            }
            fn put(
                &self,
                key: &str,
                material: &SecretMaterial,
            ) -> Result<(), CredentialProviderError> {
                self.0
                    .lock()
                    .expect("lock")
                    .insert(key.to_owned(), material.as_bytes().to_vec());
                Ok(())
            }
            fn delete(&self, key: &str) -> Result<(), CredentialProviderError> {
                self.0.lock().expect("lock").remove(key);
                Ok(())
            }
        }
        let backend = Arc::new(FakeKeychain::default());
        let keychain = OsKeychainProvider::new(Arc::clone(&backend));
        let key_ref = CredentialRef::new("cred://keychain/unit").expect("key ref");
        keychain.store(&key_ref, &material).expect("store");
        assert_eq!(
            keychain.resolve(&key_ref).expect("resolve").as_bytes(),
            sentinel.as_bytes()
        );
        keychain.revoke(&key_ref).expect("delete");
        assert!(matches!(
            keychain.resolve(&key_ref),
            Err(CredentialProviderError::Unavailable)
        ));

        struct FakeExternal;
        impl ExternalCredentialBackend for FakeExternal {
            fn resolve(&self, _reference: &str) -> Result<SecretMaterial, CredentialProviderError> {
                Ok(SecretMaterial::new(
                    b"correct-horse-battery-staple".to_vec(),
                ))
            }
        }
        let external = ExternalCredentialProvider::new(Arc::new(FakeExternal));
        let external_ref =
            CredentialRef::new("cred://external/provider-key").expect("external ref");
        assert_eq!(
            external
                .resolve(&external_ref)
                .expect("external secret")
                .as_bytes(),
            sentinel.as_bytes()
        );
    }

    #[test]
    fn future_storage_version_is_rejected_before_identity_access() {
        let temp = tempfile::tempdir().expect("temp root");
        let control = crate::ControlPlaneStore::open(temp.path()).expect("store");
        drop(control);
        let database = Connection::open(temp.path().join("metadata.sqlite3")).expect("database");
        database
            .pragma_update(None, "user_version", 13_i64)
            .expect("future version");
        drop(database);
        assert!(matches!(
            crate::ControlPlaneStore::open(temp.path()),
            Err(StorageError::UnsupportedStorageVersion(13))
        ));
    }
}
