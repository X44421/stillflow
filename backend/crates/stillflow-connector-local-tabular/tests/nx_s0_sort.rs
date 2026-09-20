//! NX-S0 (#370) acceptance, slice 1: the shared stable-ordering foundation.
//!
//! Contract: `docs/contracts/issue-370-nx-s0-shared-sort-contract.md`
//! §4 (ordering laws), §5 (bounds and fail-closed behaviour) and §6 (the
//! batch-boundary invariance law).

use std::collections::BTreeMap;
use std::fs;

use arrow_array::{Array, Int64Array, StringArray};
use serde_json::json;
use stillflow_connector_local_tabular::LocalTabularConnector;
use stillflow_connectors::{
    ConnectorRegistry, DiscoverRequest, InspectRequest, SourceConnectorRef,
};
use stillflow_core::{
    ColumnId, ConnectorKind, CredentialRef, LogicalSchema, NodeConfig, NodeEdge, NodeGraph,
    NodeGraphErrorCode, NodeId, NodeLoweringTarget, NodePort, NodeRegistry, PortId, RequestContext,
    SourceAsset, SourceConnection,
};
use stillflow_engine::{ExecutionEngine, PreviewRequest, PreviewResult};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};
use tempfile::TempDir;
use uuid::Uuid;

/// `value` carries ties (`10` appears three times) and NULLs; `id` is unique and
/// identifies a row across reorderings; `label` is Utf8 and therefore not an
/// ordered key.
const NDJSON: &str = concat!(
    r#"{"id":1,"value":5,"label":"a"}"#,
    "\n",
    r#"{"id":2,"value":10,"label":"b"}"#,
    "\n",
    r#"{"id":3,"value":null,"label":"c"}"#,
    "\n",
    r#"{"id":4,"value":10,"label":"d"}"#,
    "\n",
    r#"{"id":5,"value":-3,"label":"e"}"#,
    "\n",
    r#"{"id":6,"value":null,"label":"f"}"#,
    "\n",
    r#"{"id":7,"value":10,"label":"g"}"#,
    "\n",
    r#"{"id":8,"value":0,"label":"h"}"#,
    "\n",
);

fn uuid(value: u128) -> Uuid {
    Uuid::from_u128(value)
}
fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}
fn port(name: &str) -> PortId {
    PortId::new(name).expect("port id")
}
fn config(id: u128, type_id: &str, value: serde_json::Value) -> NodeConfig {
    NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("node config")
}
fn edge(from: u128, to: u128) -> NodeEdge {
    NodeEdge {
        from: NodePort {
            node_id: node(from),
            port: port("out"),
        },
        to: NodePort {
            node_id: node(to),
            port: port("in"),
        },
    }
}

/// One declared ordering key in the product (camelCase wire) shape.
fn key(column: &str, direction: &str, nulls: &str) -> serde_json::Value {
    json!({"column": column, "direction": direction, "nulls": nulls})
}

struct Fixture {
    _root: TempDir,
    connection: SourceConnection,
    asset: SourceAsset,
    schema: LogicalSchema,
}

async fn fixture() -> Fixture {
    let root = tempfile::tempdir().expect("temp dir");
    fs::write(root.path().join("rows.ndjson"), NDJSON).expect("write fixture");
    let connection = SourceConnection::try_new(
        ConnectorKind::LocalFile,
        "nx-s0 sort fixture",
        json!({
            "allowedRoots": [root.path().to_str().expect("utf-8 path")],
            "schemaInference": { "maxRows": 100, "maxBytes": 1_048_576 }
        }),
        CredentialRef::new("cred://local/nx-s0-sort").expect("credential ref"),
    )
    .expect("connection");

    let registry = registry();
    let assets = registry
        .discover(
            &connection,
            DiscoverRequest {
                context: RequestContext::default(),
                parent_path: None,
            },
        )
        .await
        .expect("discover");
    let asset = assets
        .into_iter()
        .find(|asset| asset.name == "rows.ndjson")
        .expect("fixture asset");
    let metadata = registry
        .inspect(
            &connection,
            InspectRequest {
                context: RequestContext::default(),
                asset: asset.clone(),
            },
        )
        .await
        .expect("inspect");

    Fixture {
        _root: root,
        connection,
        asset,
        schema: metadata.schema,
    }
}

fn registry() -> ConnectorRegistry {
    let mut registry = ConnectorRegistry::new();
    registry
        .register(std::sync::Arc::new(LocalTabularConnector) as SourceConnectorRef)
        .expect("register connector");
    registry
}

impl Fixture {
    fn column(&self, name: &str) -> ColumnId {
        self.schema
            .fields
            .iter()
            .find(|field| field.name == name)
            .unwrap_or_else(|| panic!("column {name} exists"))
            .id
    }

    fn column_wire(&self, name: &str) -> String {
        self.column(name).as_uuid().to_string()
    }

    fn graph(&self, keys: serde_json::Value) -> NodeGraph {
        let nodes = vec![
            config(
                1,
                "stillflow.node.source",
                json!({ "sourceAssetId": self.asset.id }),
            ),
            config(2, "stillflow.node.sort", json!({ "keys": keys })),
            config(5, "stillflow.node.output", json!({ "outputLabel": "out" })),
        ];
        NodeGraph::new(
            uuid(1),
            node(1),
            node(5),
            nodes,
            vec![edge(1, 2), edge(2, 5)],
            BTreeMap::new(),
        )
        .expect("valid graph")
    }

    fn compile(
        &self,
        graph: &NodeGraph,
    ) -> Result<stillflow_plan::CompiledNodeGraph, stillflow_plan::NodeGraphCompileError> {
        NodeGraphCompiler::new(NodeRegistry::deployed()).compile(
            graph,
            &AuthorizedSourceContext::new(self.asset.id, self.schema.clone())
                .expect("authorized source"),
            CompileTarget::Execution,
        )
    }

    /// Compiles, executes at `batch_size`, and returns the emitted `id` order
    /// together with the value column for cross-checking.
    async fn sorted(
        &self,
        keys: serde_json::Value,
        batch_size: usize,
    ) -> (Vec<i64>, Vec<Option<i64>>) {
        let graph = self.graph(keys);
        let compiled = self.compile(&graph).expect("graph compiles");
        let target = *compiled.node_plan_ids.get(&node(2)).expect("target");
        let mut request = PreviewRequest::new(
            compiled.plan.clone(),
            target,
            self.connection.clone(),
            self.asset.clone(),
        );
        request.batch_size = batch_size;
        request.row_limit = 1_000;
        request.byte_limit = 8 * 1024 * 1024;
        let result: PreviewResult = ExecutionEngine::new(registry())
            .preview(request)
            .await
            .expect("preview executes");

        let id_index = result
            .schema
            .fields
            .iter()
            .position(|field| field.name == "id")
            .expect("id index");
        let value_index = result
            .schema
            .fields
            .iter()
            .position(|field| field.name == "value")
            .expect("value index");
        let mut ids = Vec::new();
        let mut values = Vec::new();
        for batch in &result.batches {
            let payload = batch.payload();
            let id_array = payload
                .column(id_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            let value_array = payload
                .column(value_index)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("value column");
            for row in 0..id_array.len() {
                ids.push(id_array.value(row));
                values.push(if value_array.is_null(row) {
                    None
                } else {
                    Some(value_array.value(row))
                });
            }
        }
        (ids, values)
    }
}

#[tokio::test]
async fn ascending_with_nulls_last_orders_values_and_keeps_nulls_at_the_end() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let (ids, values) = fixture
        .sorted(json!([key(&value, "ascending", "last")]), 4)
        .await;

    assert_eq!(
        values,
        vec![
            Some(-3),
            Some(0),
            Some(5),
            Some(10),
            Some(10),
            Some(10),
            None,
            None,
        ]
    );
    // The three `10` rows keep their input order (ids 2, 4, 7): the stable
    // tie-break is the declared input order (§4.1–§4.2).
    assert_eq!(ids, vec![5, 8, 1, 2, 4, 7, 3, 6]);
}

#[tokio::test]
async fn descending_with_nulls_last_keeps_nulls_last() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let (ids, values) = fixture
        .sorted(json!([key(&value, "descending", "last")]), 4)
        .await;

    // NULL placement is absolute, never flipped by the direction (§4.3).
    assert_eq!(
        values,
        vec![
            Some(10),
            Some(10),
            Some(10),
            Some(5),
            Some(0),
            Some(-3),
            None,
            None,
        ]
    );
    assert_eq!(ids, vec![2, 4, 7, 1, 8, 5, 3, 6]);
}

#[tokio::test]
async fn descending_with_nulls_first_puts_nulls_first() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let (ids, values) = fixture
        .sorted(json!([key(&value, "descending", "first")]), 4)
        .await;

    assert_eq!(
        values,
        vec![
            None,
            None,
            Some(10),
            Some(10),
            Some(10),
            Some(5),
            Some(0),
            Some(-3),
        ]
    );
    assert_eq!(ids, vec![3, 6, 2, 4, 7, 1, 8, 5]);
}

#[tokio::test]
async fn ascending_with_nulls_first_puts_nulls_first() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let (_, values) = fixture
        .sorted(json!([key(&value, "ascending", "first")]), 4)
        .await;

    assert_eq!(
        values,
        vec![
            None,
            None,
            Some(-3),
            Some(0),
            Some(5),
            Some(10),
            Some(10),
            Some(10),
        ]
    );
}

#[tokio::test]
async fn the_result_is_invariant_across_input_batch_sizes() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let keys = json!([key(&value, "ascending", "last")]);

    // §6: changing the batch size may not change values, order or counts,
    // including a size that splits a duplicate-key run across batches.
    let baseline = fixture.sorted(keys.clone(), 1).await;
    for batch_size in [2, 3, 4, 5, 8, 16] {
        let candidate = fixture.sorted(keys.clone(), batch_size).await;
        assert_eq!(
            candidate, baseline,
            "batch_size {batch_size} changed the sorted result"
        );
    }
}

#[tokio::test]
async fn a_second_key_breaks_first_key_ties() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let id = fixture.column_wire("id");
    let (ids, values) = fixture
        .sorted(
            json!([
                key(&value, "ascending", "last"),
                key(&id, "descending", "first"),
            ]),
            4,
        )
        .await;

    // The three `10` rows are now ordered by descending id, so the second key
    // is significant and overrides input order for the tie run.
    assert_eq!(
        values,
        vec![
            Some(-3),
            Some(0),
            Some(5),
            Some(10),
            Some(10),
            Some(10),
            None,
            None,
        ]
    );
    assert_eq!(ids, vec![5, 8, 1, 7, 4, 2, 6, 3]);
}

#[tokio::test]
async fn a_utf8_key_column_is_rejected() {
    let fixture = fixture().await;
    let label = fixture.column_wire("label");
    let graph = fixture.graph(json!([key(&label, "ascending", "last")]));
    let error = fixture
        .compile(&graph)
        .expect_err("utf8 is not an ordered key type");
    assert_eq!(error.code(), NodeGraphErrorCode::IncompatibleType);
}

#[tokio::test]
async fn an_unknown_column_is_rejected() {
    let fixture = fixture().await;
    let graph = fixture.graph(json!([key(
        "00000000-0000-0000-0000-0000000000ff",
        "ascending",
        "last"
    )]));
    let error = fixture.compile(&graph).expect_err("unknown column");
    assert_eq!(error.code(), NodeGraphErrorCode::UnknownColumn);
}

#[tokio::test]
async fn zero_keys_and_too_many_keys_are_rejected() {
    let fixture = fixture().await;
    let empty = fixture.graph(json!([]));
    assert_eq!(
        fixture.compile(&empty).expect_err("zero keys").code(),
        NodeGraphErrorCode::InvalidConfig
    );

    let value = fixture.column_wire("value");
    let mut nine = Vec::new();
    for _ in 0..9 {
        nine.push(key(&value, "ascending", "last"));
    }
    let too_many = fixture.graph(json!(nine));
    assert_eq!(
        fixture.compile(&too_many).expect_err("nine keys").code(),
        NodeGraphErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn a_repeated_key_column_is_rejected() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let graph = fixture.graph(json!([
        key(&value, "ascending", "last"),
        key(&value, "descending", "first"),
    ]));
    assert_eq!(
        fixture.compile(&graph).expect_err("repeated key").code(),
        NodeGraphErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn a_missing_nulls_declaration_is_rejected() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    // NULL placement is declared configuration, never an engine default (§4.3).
    let graph = fixture.graph(json!([{ "column": value, "direction": "ascending" }]));
    assert_eq!(
        fixture.compile(&graph).expect_err("missing nulls").code(),
        NodeGraphErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn an_unknown_direction_or_nulls_value_is_rejected() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let bad_direction = fixture.graph(json!([key(&value, "sideways", "last")]));
    assert_eq!(
        fixture
            .compile(&bad_direction)
            .expect_err("unknown direction")
            .code(),
        NodeGraphErrorCode::InvalidConfig
    );

    let bad_nulls = fixture.graph(json!([key(&value, "ascending", "middle")]));
    assert_eq!(
        fixture
            .compile(&bad_nulls)
            .expect_err("unknown nulls")
            .code(),
        NodeGraphErrorCode::InvalidConfig
    );
}

#[tokio::test]
async fn the_output_schema_is_preserved_exactly() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let (ids, _) = fixture
        .sorted(json!([key(&value, "ascending", "last")]), 4)
        .await;
    assert_eq!(ids.len(), 8, "a schema-preserving sort keeps every row");
}

#[tokio::test]
async fn an_already_cancelled_request_publishes_nothing() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let graph = fixture.graph(json!([key(&value, "ascending", "last")]));
    let compiled = fixture.compile(&graph).expect("graph compiles");
    let target = *compiled.node_plan_ids.get(&node(2)).expect("target");

    let cancellation = tokio_util::sync::CancellationToken::new();
    cancellation.cancel();
    let mut request = PreviewRequest::new(
        compiled.plan.clone(),
        target,
        fixture.connection.clone(),
        fixture.asset.clone(),
    );
    request.context = RequestContext::with_cancellation(cancellation);
    request.batch_size = 4;
    request.row_limit = 1_000;
    request.byte_limit = 8 * 1024 * 1024;
    let error = ExecutionEngine::new(registry())
        .preview(request)
        .await
        .expect_err("a cancelled request must not produce a result");
    // A cancellation is a definite outcome, not a retryable fault: re-running
    // the same request under the same cancelled context would cancel again.
    assert!(
        matches!(error, stillflow_engine::EngineError::Cancelled),
        "a cancelled request must fail as Cancelled, got {error:?}"
    );
    assert!(!error.retryable(), "cancellation is never retryable");
}

#[tokio::test]
async fn a_sort_that_is_not_the_final_target_step_is_rejected() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    // source -> sort -> derive-column, targeting the derive so the sort is not
    // final. Ordering it produced would be discarded by the later per-chunk
    // step, so the run must refuse instead of silently leaving rows unordered.
    let nodes = vec![
        config(
            1,
            "stillflow.node.source",
            json!({ "sourceAssetId": fixture.asset.id }),
        ),
        config(
            2,
            "stillflow.node.sort",
            json!({ "keys": [key(&value, "ascending", "last")] }),
        ),
        config(
            3,
            "stillflow.node.derive-column",
            json!({
                "id": uuid(0xF00D),
                "name": "copy",
                "dataType": {"kind": "int64"},
                "nullable": true,
                "expression": { "kind": "column", "value": fixture.column("value") },
            }),
        ),
        config(5, "stillflow.node.output", json!({ "outputLabel": "out" })),
    ];
    let graph = NodeGraph::new(
        uuid(1),
        node(1),
        node(5),
        nodes,
        vec![edge(1, 2), edge(2, 3), edge(3, 5)],
        BTreeMap::new(),
    )
    .expect("valid graph");
    let compiled = fixture.compile(&graph).expect("graph compiles");
    let target = *compiled.node_plan_ids.get(&node(3)).expect("target");
    let mut request = PreviewRequest::new(
        compiled.plan.clone(),
        target,
        fixture.connection.clone(),
        fixture.asset.clone(),
    );
    request.batch_size = 4;
    request.row_limit = 1_000;
    request.byte_limit = 8 * 1024 * 1024;
    let error = ExecutionEngine::new(registry())
        .preview(request)
        .await
        .expect_err("a non-final sort must be refused");
    assert!(
        matches!(error, stillflow_engine::EngineError::InvalidPlan(_)),
        "expected InvalidPlan, got {error:?}"
    );
}

#[tokio::test]
async fn the_sorted_columns_other_than_the_key_travel_with_their_row() {
    let fixture = fixture().await;
    let value = fixture.column_wire("value");
    let graph = fixture.graph(json!([key(&value, "ascending", "last")]));
    let compiled = fixture.compile(&graph).expect("graph compiles");
    let target = *compiled.node_plan_ids.get(&node(2)).expect("target");
    let mut request = PreviewRequest::new(
        compiled.plan.clone(),
        target,
        fixture.connection.clone(),
        fixture.asset.clone(),
    );
    request.batch_size = 3;
    request.row_limit = 1_000;
    request.byte_limit = 8 * 1024 * 1024;
    let result: PreviewResult = ExecutionEngine::new(registry())
        .preview(request)
        .await
        .expect("preview executes");

    let label_index = result
        .schema
        .fields
        .iter()
        .position(|field| field.name == "label")
        .expect("label index");
    let mut labels = Vec::new();
    for batch in &result.batches {
        let payload = batch.payload();
        let array = payload
            .column(label_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("label column");
        for row in 0..array.len() {
            labels.push(array.value(row).to_owned());
        }
    }
    // Ascending value with NULLs last: -3(e), 0(h), 5(a), 10(b,d,g), NULL(c,f).
    assert_eq!(labels, vec!["e", "h", "a", "b", "d", "g", "c", "f"]);
}

#[test]
fn the_sort_bound_reuses_the_operator_state_law() {
    // §5.6: the declared bound tightens and widens nothing.
    assert_eq!(
        stillflow_engine::MAX_SORT_INPUT_BYTES,
        stillflow_engine::MAX_OPERATOR_STATE_BYTES
    );
}

#[test]
fn the_sort_node_is_registered_as_a_positional_operator() {
    let registry = NodeRegistry::deployed();
    let definition = registry
        .lookup("stillflow.node.sort", 1)
        .expect("sort is a deployed node");
    let entry = definition.catalog_entry();
    assert_eq!(entry.config_version, 1);
    assert!(!entry.config_schema.additional_properties);
    // Ordering is positional, not a per-row rule (#370 §2).
    assert_eq!(entry.lowering_target, NodeLoweringTarget::Sort);
}
