//! Production `JobRequestResolver`: every durable Job/Run is resolved back
//! through durable state only (PlanVersion → Scan asset → connection →
//! dataset). No process-local dispatch data crosses the boundary. This is the
//! production counterpart of the E5-G1 gate test's resolver, bounded per
//! contract §4.1.

use std::collections::BTreeSet;
use std::sync::Arc;

use uuid::Uuid;

use stillflow_core::{OperationDescriptorV1, RequestContext, SourceAsset, SourceConnection};
use stillflow_engine::{JobExecutionSpec, JobRequestResolver, JobResolution, JobRuntimeError};
use stillflow_plan::{LogicalPlan, PlanNodeKind};
use stillflow_storage::{
    ControlPlaneStore, JobRecord, RunRecord, SourceAssetRecord, SourceConnectionRecord,
};

pub const RESOLVER_DATASET_SCAN_LIMIT: usize = 1000;
const DEFAULT_BATCH_SIZE: usize = 1024;

pub struct DurableJobRequestResolver {
    store: Arc<ControlPlaneStore>,
}

impl DurableJobRequestResolver {
    pub fn new(store: Arc<ControlPlaneStore>) -> Self {
        Self { store }
    }

    fn scan_asset_id(plan: &LogicalPlan) -> Option<Uuid> {
        plan.nodes.values().find_map(|node| match &node.kind {
            PlanNodeKind::Scan {
                source_asset_id, ..
            } => Some(*source_asset_id),
            _ => None,
        })
    }

    fn domain_connection(
        record: &SourceConnectionRecord,
    ) -> Result<SourceConnection, JobRuntimeError> {
        serde_json::from_value(serde_json::json!({
            "id": record.id,
            "kind": record.kind,
            "name": record.name,
            "config": record.safe_config,
            "credentialRef": record.credential_ref,
            "createdAt": record.created_at,
            "updatedAt": record.updated_at,
        }))
        .map_err(|_| JobRuntimeError::Invalid("stored connection decodes"))
    }

    fn domain_asset(record: &SourceAssetRecord) -> Result<SourceAsset, JobRuntimeError> {
        let locator = serde_json::from_value(record.safe_locator.clone())
            .map_err(|_| JobRuntimeError::Invalid("stored locator decodes"))?;
        Ok(SourceAsset {
            id: record.id,
            connection_id: record.connection_id,
            kind: record.kind,
            name: record.name.clone(),
            locator,
            discovered_at: record.discovered_at,
        })
    }
}

impl JobRequestResolver for DurableJobRequestResolver {
    fn resolve(&self, job: JobRecord, _run: RunRecord, _context: RequestContext) -> JobResolution {
        let store = Arc::clone(&self.store);
        Box::pin(async move {
            let version = store
                .get_plan_version(job.plan_version_id)
                .map_err(JobRuntimeError::Storage)?;
            let plan: LogicalPlan = serde_json::from_value(version.logical_plan)
                .map_err(|_| JobRuntimeError::Invalid("durable plan decodes"))?;
            let asset_id = Self::scan_asset_id(&plan)
                .ok_or(JobRuntimeError::Invalid("durable plan has no Scan node"))?;
            let asset_record = store
                .get_source_asset(asset_id)
                .map_err(JobRuntimeError::Storage)?;
            let connection_record = store
                .get_source_connection(asset_record.connection_id)
                .map_err(JobRuntimeError::Storage)?;
            let datasets = store
                .list_datasets(job.workspace_id, RESOLVER_DATASET_SCAN_LIMIT)
                .map_err(JobRuntimeError::Storage)?;
            let dataset_id = datasets
                .iter()
                .find(|dataset| dataset.source_asset_id == asset_id)
                .map(|dataset| dataset.id)
                .ok_or(JobRuntimeError::Invalid(
                    "dataset bound to the plan asset is missing",
                ))?;
            let batch_size = job
                .operation
                .as_ref()
                .map(|operation| match &operation.descriptor {
                    OperationDescriptorV1::Materialize {
                        materialize_policy, ..
                    } => materialize_policy.batch_size,
                    OperationDescriptorV1::Verification {
                        verification_policy,
                        ..
                    } => verification_policy.batch_size,
                    _ => DEFAULT_BATCH_SIZE,
                })
                .unwrap_or(DEFAULT_BATCH_SIZE);
            Ok(JobExecutionSpec {
                plan,
                connection: Self::domain_connection(&connection_record)?,
                asset: Self::domain_asset(&asset_record)?,
                schema_override: None,
                snapshot_id: Uuid::new_v4(),
                dataset_id,
                lineage: BTreeSet::new(),
                quality_score: None,
                batch_size,
                bundle_ref: None,
            })
        })
    }
}
