//! Experimental E4 closed loop through the local tabular connector.
//!
//! Lives here so `stillflow-engine` does not depend on an adapter crate.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::DateTime;
use sha2::{Digest, Sha256};
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{ConnectorRegistry, SourceConnector, SourceConnectorRef};
use stillflow_core::{
    CredentialRef, DiscoverRequest, Expr, InputRef, InspectRequest, LogicalInputRef, SourceAsset,
    SourceConnection,
};
use stillflow_engine::{
    export_snapshot_to_csv, ExecutionEngine, VerificationIdentities, VerificationRequest,
    ENGINE_MAX_DEADLINE,
};
use stillflow_plan::{LogicalPlan, PlanNode, PlanNodeId, PlanNodeKind, Rule, ValidationSeverity};
use stillflow_storage::SnapshotStore;
use uuid::Uuid;

fn long_context() -> stillflow_core::RequestContext {
    stillflow_core::RequestContext::with_cancellation_and_deadline(
        stillflow_core::RequestContext::default()
            .cancellation()
            .clone(),
        tokio::time::Instant::now() + ENGINE_MAX_DEADLINE,
    )
}

fn plan_digest(plan: &LogicalPlan) -> [u8; 32] {
    Sha256::digest(plan.canonical_bytes().expect("canonical plan bytes")).into()
}

fn validate_dedup_plan(
    asset_id: Uuid,
    projection: Vec<stillflow_core::ColumnId>,
    key: stillflow_core::ColumnId,
) -> LogicalPlan {
    let scan = PlanNodeId::from_uuid(Uuid::from_u128(1));
    let rules = PlanNodeId::from_uuid(Uuid::from_u128(2));
    let materialize = PlanNodeId::from_uuid(Uuid::from_u128(3));
    let mut nodes = BTreeMap::new();
    nodes.insert(
        scan,
        PlanNode::new(
            PlanNodeKind::Scan {
                source_asset_id: asset_id,
                projection,
                predicate: None,
            },
            Vec::new(),
        ),
    );
    nodes.insert(
        rules,
        PlanNode::new(
            PlanNodeKind::ApplyRules {
                rules: vec![
                    Rule::Validate {
                        predicate: Expr::IsNull {
                            expression: Box::new(Expr::Column(key)),
                            negated: true,
                        },
                        severity: ValidationSeverity::Error,
                        message: "row must pass validation".to_owned(),
                    },
                    Rule::Deduplicate { keys: vec![key] },
                ],
            },
            vec![scan],
        ),
    );
    nodes.insert(
        materialize,
        PlanNode::new(
            PlanNodeKind::Materialize {
                output_label: "accepted".to_owned(),
            },
            vec![rules],
        ),
    );
    LogicalPlan::new(materialize, nodes).expect("plan")
}

fn identities_for(plan: &LogicalPlan, asset: &SourceAsset) -> VerificationIdentities {
    let at = DateTime::from_timestamp(1_700_000_000, 0).expect("timestamp");
    VerificationIdentities {
        run_id: Uuid::from_u128(300),
        bundle_id: Uuid::from_u128(301),
        bundle_artifact_id: Uuid::from_u128(302),
        snapshot_id: Uuid::from_u128(303),
        dataset_id: Uuid::from_u128(304),
        validation_report_artifact_id: Uuid::from_u128(305),
        rejected_rows_artifact_id: Some(Uuid::from_u128(306)),
        deduplication_report_artifact_id: Uuid::from_u128(307),
        session_id: Uuid::from_u128(308),
        logical_input: LogicalInputRef {
            input: InputRef::Asset { asset_id: asset.id },
            version_digest: [0x11; 32],
        },
        canonical_plan_digest: plan_digest(plan),
        created_at: at,
        started_at: at,
        committed_at: at,
        lineage: Default::default(),
        quality_score: None,
    }
}

#[tokio::test(flavor = "current_thread")]
async fn e4_csv_file_closes_the_loop() {
    let temp = tempfile::TempDir::new().expect("csv root");
    std::fs::write(
        temp.path().join("orders.csv"),
        b"id,label\n1,alpha\n2,beta\n3,gamma\n",
    )
    .expect("csv");
    let connection = SourceConnection::try_new(
        stillflow_core::ConnectorKind::LocalFile,
        "csv",
        serde_json::json!({
            "allowedRoots": [temp.path().to_str().expect("utf8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1048576 }
        }),
        CredentialRef::new("cred://local/csv").expect("cred"),
    )
    .expect("connection");
    let discovered = LocalTabularConnector
        .discover(
            &connection,
            DiscoverRequest {
                context: long_context(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    let source = discovered
        .into_iter()
        .find(|item| item.name == "orders.csv")
        .expect("orders.csv");
    let metadata = LocalTabularConnector
        .inspect(
            &connection,
            InspectRequest {
                context: long_context(),
                asset: source.clone(),
            },
        )
        .await
        .expect("inspect");
    let id = metadata
        .schema
        .fields
        .iter()
        .find(|field| field.name == "id")
        .expect("id column")
        .id;
    let projection: Vec<_> = metadata
        .schema
        .fields
        .iter()
        .map(|field| field.id)
        .collect();
    let plan = validate_dedup_plan(source.id, projection, id);
    let dir = tempfile::TempDir::new().expect("store");
    let store = SnapshotStore::open(dir.path(), stillflow_storage::StorageLimits::default())
        .expect("store");
    let identities = identities_for(&plan, &source);
    let mut registry = ConnectorRegistry::new();
    registry
        .register(Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register");
    let bundle = ExecutionEngine::new(registry)
        .materialize_verification(VerificationRequest {
            plan,
            connection,
            asset: source,
            schema_override: Some(metadata.schema),
            identities,
            context: long_context(),
            batch_size: 64,
            store: &store,
        })
        .await
        .expect("csv verification");
    assert_eq!(bundle.accepted.manifest.snapshot().stats().row_count(), 3);
    assert!(bundle.rejected_rows.is_none());
    let mut csv = Vec::new();
    export_snapshot_to_csv(&store, bundle.membership.accepted_snapshot_id, &mut csv)
        .unwrap_or_else(|error| panic!("export failed: {error}"));
    assert_eq!(
        String::from_utf8(csv).expect("utf8 csv"),
        "id,label\n1,alpha\n2,beta\n3,gamma\n"
    );
}
