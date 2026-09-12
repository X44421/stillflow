//! NX-S1 (#336) version-1 compatibility corpus for the eleven built-in nodes.
//!
//! The corpus is captured at the frozen pre-change base, never regenerated
//! from a candidate head (NX-C0 contract §10.5). Capture:
//!
//! ```text
//! cargo test -p stillflow-plan --test nx_s1_semantics -- --ignored capture_v1_baseline
//! ```
//!
//! The verify test compares the current compiler against the captured v1
//! expectations. Any difference is a compatibility event: fix the candidate
//! or cite the contract clause that authorizes the change.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use serde_json::{json, Value};
use stillflow_core::{
    ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, NodeConfig, NodeEdge, NodeGraph,
    NodeGraphError, NodeId, BinaryOperator, ScalarValue, TimeUnit, UnaryOperator,
};
use stillflow_plan::{AuthorizedSourceContext, CompileTarget, NodeGraphCompiler};

const FIXTURE_DIR_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/nx-v1/cases"
);

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(FIXTURE_DIR_PATH).join(format!("{name}.json"))
}

fn uuid(value: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn column(value: u128) -> ColumnId {
    ColumnId::from_uuid(uuid(value))
}

/// Authorized source schema shared by the corpus. It carries one field of
/// every relevant kind, including the paused binary, date32, and
/// timestamp-second surfaces.
fn source_schema() -> LogicalSchema {
    LogicalSchema::new(vec![
        LogicalField::new(column(101), "name", LogicalType::Utf8, true).expect("field"),
        LogicalField::new(column(102), "age", LogicalType::Int64, true).expect("field"),
        LogicalField::new(column(103), "score", LogicalType::Float64, true).expect("field"),
        LogicalField::new(column(104), "active", LogicalType::Boolean, false).expect("field"),
        LogicalField::new(column(105), "blob", LogicalType::Binary, true).expect("field"),
        LogicalField::new(column(106), "created", LogicalType::Date32, true).expect("field"),
        LogicalField::new(
            column(107),
            "ts_sec",
            LogicalType::Timestamp {
                unit: TimeUnit::Second,
                timezone: None,
            },
            true,
        )
        .expect("field"),
    ])
    .expect("schema")
}

fn source(asset: u128) -> AuthorizedSourceContext {
    AuthorizedSourceContext::new(uuid(asset), source_schema()).expect("source")
}

fn config(id: u128, type_id: &str, value: Value) -> NodeConfig {
    NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("valid node config")
}

fn config_with_version(id: u128, type_id: &str, version: u16, value: Value) -> NodeConfig {
    NodeConfig::new(node(id), type_id, version, value, BTreeMap::new()).expect("valid node config")
}

fn serialized<T: serde::Serialize>(value: &T) -> Value {
    serde_json::to_value(value).expect("serializable test value")
}

fn edge(from: u128, to: u128) -> NodeEdge {
    NodeEdge {
        from: stillflow_core::NodePort {
            node_id: node(from),
            port: stillflow_core::PortId::new("out").expect("port"),
        },
        to: stillflow_core::NodePort {
            node_id: node(to),
            port: stillflow_core::PortId::new("in").expect("port"),
        },
    }
}

fn chain(nodes: Vec<NodeConfig>, edges: Vec<(u128, u128)>) -> Result<NodeGraph, NodeGraphError> {
    NodeGraph::new(
        uuid(900),
        node(1),
        node(5),
        nodes,
        edges.into_iter().map(|(from, to)| edge(from, to)).collect(),
        BTreeMap::new(),
    )
}

fn source_config(asset: u128) -> NodeConfig {
    config(
        1,
        "stillflow.node.source",
        json!({
            "sourceAssetId": uuid(asset),
            "projection": [
                column(101), column(102), column(103),
                column(104), column(105), column(106), column(107)
            ]
        }),
    )
}

fn output_config() -> NodeConfig {
    config(5, "stillflow.node.output", json!({"outputLabel": "cleaned"}))
}

fn select_config(id: u128, columns: &[u128]) -> NodeConfig {
    config(
        id,
        "stillflow.node.select",
        json!({"columns": columns.iter().map(|value| column(*value)).collect::<Vec<_>>()}),
    )
}

fn filter_config(id: u128, predicate: &Expr) -> NodeConfig {
    config(id, "stillflow.node.filter", json!({"predicate": serialized(predicate)}))
}

fn trim_config(id: u128, column_value: u128) -> NodeConfig {
    config(id, "stillflow.node.trim", json!({"column": column(column_value)}))
}

fn cast_config(id: u128, column_value: u128, data_type: &LogicalType, on_failure: &str) -> NodeConfig {
    config(
        id,
        "stillflow.node.cast",
        json!({
            "column": column(column_value),
            "dataType": serialized(data_type),
            "onFailure": on_failure
        }),
    )
}

fn rename_config(id: u128, column_value: u128, to: &str) -> NodeConfig {
    config(id, "stillflow.node.rename", json!({"column": column(column_value), "to": to}))
}

fn fill_null_config(id: u128, column_value: u128, value: &ScalarValue) -> NodeConfig {
    config(
        id,
        "stillflow.node.fill-null",
        json!({"column": column(column_value), "value": serialized(value)}),
    )
}

fn replace_config(id: u128, column_value: u128, from: &ScalarValue, to: &ScalarValue) -> NodeConfig {
    config(
        id,
        "stillflow.node.replace-literal",
        json!({"column": column(column_value), "from": serialized(from), "to": serialized(to)}),
    )
}

fn drop_config(id: u128, column_value: u128) -> NodeConfig {
    config(id, "stillflow.node.drop-column", json!({"column": column(column_value)}))
}

fn derive_config(
    id: u128,
    derived: u128,
    name: &str,
    data_type: &LogicalType,
    nullable: bool,
    expression: &Expr,
) -> NodeConfig {
    config(
        id,
        "stillflow.node.derive-column",
        json!({
            "id": column(derived),
            "name": name,
            "dataType": serialized(data_type),
            "nullable": nullable,
            "expression": serialized(expression)
        }),
    )
}

fn col(value: u128) -> Expr {
    Expr::Column(column(value))
}

fn lit_int(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(value))
}

fn lit_utf8(value: &str) -> Expr {
    Expr::Literal(ScalarValue::Utf8(value.to_owned()))
}

fn binary(operator: BinaryOperator, left: Expr, right: Expr) -> Expr {
    Expr::Binary {
        left: Box::new(left),
        operator,
        right: Box::new(right),
    }
}

fn is_null(expression: Expr) -> Expr {
    Expr::IsNull {
        expression: Box::new(expression),
        negated: false,
    }
}

/// One corpus case. `graph` may fail to construct, which is itself a frozen
/// product behavior (the API decodes graphs through the same structural
/// validation before compilation).
struct Case {
    name: &'static str,
    build: fn() -> CaseInput,
}

struct CaseInput {
    graph: Result<NodeGraph, NodeGraphError>,
    source: AuthorizedSourceContext,
    target: CompileTarget,
}

fn execution(graph: Result<NodeGraph, NodeGraphError>, asset: u128) -> CaseInput {
    CaseInput {
        graph,
        source: source(asset),
        target: CompileTarget::Execution,
    }
}

fn preview_of(
    graph: Result<NodeGraph, NodeGraphError>,
    asset: u128,
    target: u128,
) -> CaseInput {
    CaseInput {
        graph,
        source: source(asset),
        target: CompileTarget::Preview(node(target)),
    }
}

/// The full ten-transform chain used by the chain cases.
fn full_chain_nodes() -> Vec<NodeConfig> {
    vec![
        source_config(700),
        select_config(2, &[101, 102, 103, 104]),
        rename_config(3, 101, "label"),
        trim_config(4, 101),
        fill_null_config(6, 102, &ScalarValue::Int64(0)),
        derive_config(
            7,
            108,
            "score_seen",
            &LogicalType::Boolean,
            true,
            &is_null(col(103)),
        ),
        cast_config(8, 102, &LogicalType::Int32, "error"),
        filter_config(
            9,
            &binary(
                BinaryOperator::GreaterThanOrEqual,
                col(102),
                lit_int(-128),
            ),
        ),
        drop_config(10, 104),
        output_config(),
    ]
}

fn full_chain_edges() -> Vec<(u128, u128)> {
    vec![(1, 2), (2, 3), (3, 4), (4, 6), (6, 7), (7, 8), (8, 9), (9, 10), (10, 5)]
}

fn cases() -> Vec<Case> {
    vec![
        // ---- positive: one case per built-in transform and its lowering ----
        Case {
            name: "source_select_output",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), select_config(2, &[101, 102]), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "source_projection_subset",
            build: || {
                let subset = config(
                    1,
                    "stillflow.node.source",
                    json!({
                        "sourceAssetId": uuid(700),
                        "projection": [column(102), column(101)]
                    }),
                );
                execution(chain(vec![subset, output_config()], vec![(1, 5)]), 700)
            },
        },
        Case {
            name: "filter_boolean_predicate",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &binary(
                                    BinaryOperator::Equal,
                                    col(104),
                                    Expr::Literal(ScalarValue::Boolean(true)),
                                ),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "trim_utf8_column",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), trim_config(2, 101), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "rename_keeps_column_identity",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            rename_config(2, 101, "label"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_setnull_widens_nullability",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 102, &LogicalType::Float64, "setNull"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_error_keeps_nullability",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 102, &LogicalType::Int32, "error"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_identity_type",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 101, &LogicalType::Utf8, "error"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "replace_literal_to_null_widens_nullability",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            replace_config(
                                2,
                                101,
                                &ScalarValue::Utf8("x".to_owned()),
                                &ScalarValue::Null,
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "replace_literal_to_literal_keeps_schema",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            replace_config(
                                2,
                                101,
                                &ScalarValue::Utf8("x".to_owned()),
                                &ScalarValue::Utf8("y".to_owned()),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "replace_binary_null_to_null",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            replace_config(2, 105, &ScalarValue::Null, &ScalarValue::Null),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "fill_null_narrows_nullability",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            fill_null_config(2, 102, &ScalarValue::Int64(0)),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "drop_column_removes_identity",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), drop_config(2, 105), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_column_from_expression",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(
                                2,
                                108,
                                "age_plus",
                                &LogicalType::Int64,
                                true,
                                &binary(BinaryOperator::Add, col(102), lit_int(1)),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_column_null_literal_matches_any_type",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(
                                2,
                                108,
                                "nothing",
                                &LogicalType::Int64,
                                true,
                                &Expr::Literal(ScalarValue::Null),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_column_coalesce_arms",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(
                                2,
                                108,
                                "fallback",
                                &LogicalType::Int64,
                                true,
                                &Expr::Coalesce {
                                    expressions: vec![col(102), lit_int(0)],
                                },
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "full_chain_execution",
            build: || execution(chain(full_chain_nodes(), full_chain_edges()), 700),
        },
        Case {
            name: "full_chain_preview_midway",
            build: || {
                preview_of(chain(full_chain_nodes(), full_chain_edges()), 700, 7)
            },
        },
        Case {
            name: "array_order_permutation_same_bytes",
            build: || {
                let mut nodes = full_chain_nodes();
                nodes.reverse();
                let mut edges = full_chain_edges();
                edges.reverse();
                execution(chain(nodes, edges), 700)
            },
        },
        Case {
            name: "graph_and_node_metadata_present",
            build: || {
                let mut metadata = BTreeMap::new();
                metadata.insert("owner".to_owned(), "team".to_owned());
                let mut node_metadata = BTreeMap::new();
                node_metadata.insert("note".to_owned(), "value".to_owned());
                let mut trimmed = trim_config(2, 101);
                trimmed.metadata = node_metadata;
                let graph = NodeGraph::new(
                    uuid(900),
                    node(1),
                    node(5),
                    vec![source_config(700), trimmed, output_config()],
                    vec![edge(1, 2), edge(2, 5)],
                    metadata,
                );
                execution(graph, 700)
            },
        },
        Case {
            name: "trim_chain_plain",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), trim_config(2, 101), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        // ---- negative: paused capabilities and typed rejections ----
        Case {
            name: "unknown_column_select",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), select_config(2, &[999]), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_column_filter_predicate",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(2, &binary(BinaryOperator::Equal, col(999), lit_int(1))),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_column_rename",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), rename_config(2, 999, "x"), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_column_trim",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), trim_config(2, 999), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_column_cast",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 999, &LogicalType::Utf8, "error"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_column_drop",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), drop_config(2, 999), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_column_derive_expression",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(
                                2,
                                108,
                                "bad",
                                &LogicalType::Int64,
                                true,
                                &col(999),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "source_projection_unknown_column",
            build: || {
                let broken = config(
                    1,
                    "stillflow.node.source",
                    json!({"sourceAssetId": uuid(700), "projection": [column(999)]}),
                );
                execution(chain(vec![broken, output_config()], vec![(1, 5)]), 700)
            },
        },
        Case {
            name: "source_projection_duplicate_column",
            build: || {
                let broken = config(
                    1,
                    "stillflow.node.source",
                    json!({"sourceAssetId": uuid(700), "projection": [column(101), column(101)]}),
                );
                execution(chain(vec![broken, output_config()], vec![(1, 5)]), 700)
            },
        },
        Case {
            name: "select_empty_columns",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), select_config(2, &[]), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "trim_requires_utf8",
            build: || {
                execution(
                    chain(
                        vec![source_config(700), trim_config(2, 102), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_to_binary_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 102, &LogicalType::Binary, "error"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_from_binary_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 105, &LogicalType::Utf8, "error"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_date32_to_utf8_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            cast_config(2, 106, &LogicalType::Utf8, "error"),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "timestamp_second_reference_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(2, &is_null(col(107))),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "cast_inside_expression_from_binary_paused",
            build: || {
                let expression = Expr::Cast {
                    expression: Box::new(col(105)),
                    data_type: LogicalType::Utf8,
                };
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(2, 108, "from_blob", &LogicalType::Utf8, true, &expression),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "checked_arithmetic_in_predicate_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &binary(BinaryOperator::Equal, binary(BinaryOperator::Add, col(102), lit_int(1)), lit_int(2)),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "contains_operator_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &binary(
                                    BinaryOperator::Contains,
                                    col(101),
                                    lit_utf8("a"),
                                ),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unary_negate_paused",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &Expr::Unary {
                                    operator: UnaryOperator::Negate,
                                    expression: Box::new(col(102)),
                                },
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "filter_predicate_not_boolean",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(2, &col(102)),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "not_requires_boolean",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &Expr::Unary {
                                    operator: UnaryOperator::Not,
                                    expression: Box::new(col(102)),
                                },
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "ordered_comparison_rejects_utf8",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &binary(BinaryOperator::LessThan, col(101), col(101)),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "equality_rejects_incomparable_pair",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            filter_config(
                                2,
                                &binary(BinaryOperator::Equal, col(102), col(101)),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "coalesce_arms_incompatible",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(
                                2,
                                108,
                                "mixed",
                                &LogicalType::Int64,
                                true,
                                &Expr::Coalesce {
                                    expressions: vec![col(102), col(101)],
                                },
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "fill_null_value_must_not_be_null",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            fill_null_config(2, 102, &ScalarValue::Null),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "fill_null_not_authorized_on_binary",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            fill_null_config(2, 105, &ScalarValue::Utf8("x".to_owned())),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "fill_null_literal_type_mismatch",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            fill_null_config(2, 102, &ScalarValue::Utf8("x".to_owned())),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "replace_binary_rejects_non_null_pair",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            replace_config(
                                2,
                                105,
                                &ScalarValue::Utf8("a".to_owned()),
                                &ScalarValue::Utf8("b".to_owned()),
                            ),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "drop_cannot_remove_final_field",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            select_config(2, &[101]),
                            drop_config(3, 101),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 3), (3, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_nullability_narrower_than_expression",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(2, 108, "strict", &LogicalType::Utf8, false, &col(101)),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_type_mismatch",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(2, 108, "mismatch", &LogicalType::Utf8, true, &col(102)),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_duplicate_column_id",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(2, 102, "dupe", &LogicalType::Int64, true, &col(103)),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "derive_duplicate_column_name",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            derive_config(2, 108, "age", &LogicalType::Int64, true, &col(103)),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unknown_node_type",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            config(2, "stillflow.node.unknown", json!({})),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "unsupported_config_version",
            build: || {
                execution(
                    chain(
                        vec![
                            source_config(700),
                            config_with_version(2, "stillflow.node.trim", 2, json!({"column": column(101)})),
                            output_config(),
                        ],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "source_binding_mismatch",
            build: || {
                execution(
                    chain(
                        vec![source_config(701), output_config()],
                        vec![(1, 5)],
                    ),
                    700,
                )
            },
        },
        Case {
            name: "preview_target_is_output_node",
            build: || {
                preview_of(
                    chain(
                        vec![source_config(700), trim_config(2, 101), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                    5,
                )
            },
        },
        Case {
            name: "preview_target_not_in_graph",
            build: || {
                preview_of(
                    chain(
                        vec![source_config(700), trim_config(2, 101), output_config()],
                        vec![(1, 2), (2, 5)],
                    ),
                    700,
                    42,
                )
            },
        },
        // ---- negative: structural validation happens at graph decode ----
        Case {
            name: "branching_topology_rejected_at_decode",
            build: || {
                let graph = NodeGraph::new(
                    uuid(901),
                    node(1),
                    node(5),
                    vec![
                        source_config(700),
                        output_config(),
                        select_config(6, &[101]),
                    ],
                    vec![edge(1, 5), edge(1, 6)],
                    BTreeMap::new(),
                );
                CaseInput {
                    graph,
                    source: source(700),
                    target: CompileTarget::Execution,
                }
            },
        },
        Case {
            name: "disconnected_node_rejected_at_decode",
            build: || {
                let graph = NodeGraph::new(
                    uuid(902),
                    node(1),
                    node(5),
                    vec![
                        source_config(700),
                        output_config(),
                        select_config(6, &[101]),
                    ],
                    vec![edge(1, 5)],
                    BTreeMap::new(),
                );
                CaseInput {
                    graph,
                    source: source(700),
                    target: CompileTarget::Execution,
                }
            },
        },
        Case {
            name: "cycle_rejected_at_decode",
            build: || {
                let graph = NodeGraph::new(
                    uuid(903),
                    node(1),
                    node(5),
                    vec![
                        source_config(700),
                        trim_config(2, 101),
                        output_config(),
                    ],
                    vec![edge(1, 2), edge(2, 5), edge(5, 2)],
                    BTreeMap::new(),
                );
                CaseInput {
                    graph,
                    source: source(700),
                    target: CompileTarget::Execution,
                }
            },
        },
    ]
}

fn target_json(target: &CompileTarget) -> Value {
    match target {
        CompileTarget::Execution => json!("execution"),
        CompileTarget::Preview(target) => json!({ "preview": target.as_uuid() }),
    }
}

fn observe(input: &CaseInput) -> Value {
    let graph = match &input.graph {
        Ok(graph) => graph,
        Err(error) => {
            return json!({
                "stage": "graph",
                "error": { "code": error.code().as_str(), "nodeId": error.node_id().map(|id| id.as_uuid()) }
            });
        }
    };
    match NodeGraphCompiler::default().compile(graph, &input.source, input.target) {
        Ok(compiled) => {
            let canonical = compiled.canonical_bytes().expect("canonical bytes");
            let node_schemas: BTreeMap<String, Value> = compiled
                .node_schemas
                .iter()
                .map(|(id, schema)| (id.as_uuid().to_string(), serialized(schema)))
                .collect();
            json!({
                "stage": "compile",
                "ok": {
                    "fingerprint": compiled.fingerprint().expect("fingerprint").to_string(),
                    "canonicalBytesHex": hex(&canonical),
                    "nodePlanIds": compiled
                        .node_plan_ids
                        .iter()
                        .map(|(id, plan_id)| (id.as_uuid().to_string(), plan_id.as_uuid()))
                        .collect::<BTreeMap<_, _>>(),
                    "nodeSchemas": node_schemas,
                    "outputSchema": serialized(&compiled.output_schema),
                },
                "error": Value::Null,
            })
        }
        Err(error) => json!({
            "stage": "compile",
            "ok": Value::Null,
            "error": {
                "code": error.code().as_str(),
                "nodeId": error.node_id().map(|id| id.as_uuid())
            }
        }),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn case_json(name: &str, input: &CaseInput) -> Value {
    let graph_json = match &input.graph {
        Ok(graph) => serialized(graph),
        Err(_) => Value::Null,
    };
    json!({
        "name": name,
        "graph": graph_json,
        "sourceAssetId": input.source.source_asset_id,
        "sourceSchema": serialized(&input.source.schema),
        "target": target_json(&input.target),
        "expected": observe(input),
    })
}

fn load_cases() -> Vec<(String, Value)> {
    let mut entries: Vec<(String, Value)> = Vec::new();
    let dir = PathBuf::from(FIXTURE_DIR_PATH);
    let mut paths: Vec<_> = fs::read_dir(&dir)
        .expect("fixture directory exists")
        .map(|entry| entry.expect("readable entry").path())
        .filter(|path| path.extension().map(|ext| ext == "json").unwrap_or(false))
        .collect();
    paths.sort();
    for path in paths {
        let raw = fs::read_to_string(&path).expect("fixture readable");
        let value: Value = serde_json::from_str(&raw).expect("fixture parses");
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .expect("fixture names its case")
            .to_owned();
        entries.push((name, value));
    }
    entries
}

fn compare(expected: &Value, actual: &Value, path: &str) -> Vec<String> {
    if expected == actual {
        return Vec::new();
    }
    match (expected, actual) {
        (Value::Object(expected), Value::Object(actual)) => {
            let mut diffs = Vec::new();
            for (key, expected_value) in expected {
                match actual.get(key) {
                    Some(actual_value) => {
                        diffs.extend(compare(expected_value, actual_value, &format!("{path}.{key}")));
                    }
                    None => diffs.push(format!("{path}: missing key {key}")),
                }
            }
            diffs
        }
        _ => vec![format!(
            "{path}: expected {expected}, actual {actual}"
        )],
    }
}

#[test]
fn v1_baseline_is_reproduced() {
    let entries = load_cases();
    assert!(
        entries.len() >= 40,
        "the corpus must stay populated (found {} cases)",
        entries.len()
    );
    for (name, fixture) in &entries {
        let case = cases()
            .into_iter()
            .find(|case| case.name == name.as_str())
            .unwrap_or_else(|| panic!("fixture {name} has no corpus case"));
        let input = (case.build)();
        let normalized = case_json(&case.name, &input);
        let diffs = compare(
            fixture.get("expected").expect("fixture expectation"),
            normalized.get("expected").expect("fresh observation"),
            name,
        );
        assert!(
            diffs.is_empty(),
            "v1 baseline diverged for {name}:\n{}",
            diffs.join("\n")
        );
        // The normalized input must also stay byte-identical so the corpus
        // continues to describe the same logical graphs.
        assert_eq!(
            fixture.get("graph").expect("fixture graph"),
            normalized.get("graph").expect("fresh graph"),
            "normalized graph input changed for {name}"
        );
    }
}

/// NX-C0 §10.2: changing graph metadata or node metadata alone must not
/// change the plan bytes. The two cases differ only in metadata.
#[test]
fn v1_metadata_does_not_change_plan_bytes() {
    let entries = load_cases();
    let plain = entries
        .iter()
        .find(|(name, _)| name == "trim_chain_plain")
        .expect("plain chain fixture");
    let with_metadata = entries
        .iter()
        .find(|(name, _)| name == "graph_and_node_metadata_present")
        .expect("metadata fixture");
    let plain_hex = plain
        .1 .pointer("/expected/ok/canonicalBytesHex")
        .expect("plain bytes");
    let metadata_hex = with_metadata
        .1
        .pointer("/expected/ok/canonicalBytesHex")
        .expect("metadata bytes");
    assert_eq!(plain_hex, metadata_hex);
}

#[test]
#[ignore = "capture runs at the frozen pre-change base only; never regenerate from a candidate head"]
fn capture_v1_baseline() {
    let dir = PathBuf::from(FIXTURE_DIR_PATH);
    fs::create_dir_all(&dir).expect("fixture directory");
    for case in cases() {
        let input = (case.build)();
        let document = case_json(case.name, &input);
        let path = fixture_path(case.name);
        fs::write(&path, format!("{}\n", serde_json::to_string_pretty(&document).expect("serializable"))).expect("fixture written");
    }
}
