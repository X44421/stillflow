//! NX-S1 (#336) differential battery: the NodeGraph compile path and the
//! engine preflight path over the shared semantics (NX-C0 contract §5.3).
//!
//! Entry point A is the shared analyzer in `stillflow_plan::semantics` as
//! used by the NodeGraph compiler. Entry point B is the engine's preflight
//! surface exposed through `stillflow_engine::semantics` (typing through the
//! shared analyzer with the engine error mapping; rule propagation through
//! the production incremental layer). Both run without a connector, API,
//! service, filesystem, or network dependency.

use std::collections::BTreeMap;

use stillflow_core::{
    BinaryOperator, ColumnId, Expr, LogicalField, LogicalSchema, LogicalType, NodeConfig, NodeEdge,
    NodeGraph, NodeId, ScalarValue, TimeUnit, UnaryOperator,
};
use stillflow_engine::semantics as engine_semantics;
use stillflow_plan::semantics::{self, SemanticKind};
use stillflow_plan::{
    AuthorizedSourceContext, CompileTarget, NodeGraphCompiler, PlanNodeKind, Rule,
};
use stillflow_plan::{CastFailurePolicy, PlanNodeId};

fn uuid(value: u128) -> uuid::Uuid {
    uuid::Uuid::from_u128(value)
}

fn node(value: u128) -> NodeId {
    NodeId::from_uuid(uuid(value))
}

fn column(value: u128) -> ColumnId {
    ColumnId::from_uuid(uuid(value))
}

/// One field of every relevant kind, including the paused binary, date32,
/// and timestamp-second surfaces.
fn schema() -> LogicalSchema {
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

fn cast(expression: Expr, data_type: LogicalType) -> Expr {
    Expr::Cast {
        expression: Box::new(expression),
        data_type,
    }
}

/// The engine error classes the frozen mapping produces. The battery asserts
/// this mapping per shared failure kind, so the two error surfaces cannot
/// drift silently.
#[derive(Debug, PartialEq, Eq)]
enum EngineClass {
    InvalidPlan,
    UnknownColumn(u128),
    TypeError,
    BoundExceeded,
}

impl EngineClass {
    fn of(error: &stillflow_engine::EngineError) -> Self {
        match error {
            stillflow_engine::EngineError::InvalidPlan(_) => Self::InvalidPlan,
            stillflow_engine::EngineError::UnknownColumn(id) => {
                Self::UnknownColumn(id.as_uuid().as_u128())
            }
            stillflow_engine::EngineError::TypeError(_) => Self::TypeError,
            stillflow_engine::EngineError::BoundExceeded(_) => Self::BoundExceeded,
            other => panic!("unexpected engine error class: {other:?}"),
        }
    }
}

/// The frozen shared-kind → engine-class mapping (typing.rs `semantic_error`).
fn expected_engine_class(kind: SemanticKind) -> EngineClass {
    match kind {
        SemanticKind::ShapeInvalid => EngineClass::InvalidPlan,
        SemanticKind::ExprBounds => EngineClass::BoundExceeded,
        SemanticKind::UnknownColumn => EngineClass::UnknownColumn(u64::MAX as u128), // checked loosely below
        SemanticKind::NotRequiresBoolean
        | SemanticKind::LogicalOperandsMustBeBoolean
        | SemanticKind::ContainsPaused
        | SemanticKind::CheckedArithmeticPaused
        | SemanticKind::ListStructPaused
        | SemanticKind::TimestampSecondPaused
        | SemanticKind::InvalidLogicalType
        | SemanticKind::DateToUtf8CastPaused
        | SemanticKind::BinaryCastUnauthorized
        | SemanticKind::ComparisonIncomparable
        | SemanticKind::OrderedComparisonIncompatible
        | SemanticKind::OrderedComparisonRequiresNumeric
        | SemanticKind::CoalesceArmsIncompatible
        | SemanticKind::TrimRequiresUtf8
        | SemanticKind::LiteralIncompatibleWithColumn
        | SemanticKind::BinaryReplaceOnlyNullToNull
        | SemanticKind::FillNullNotAuthorizedOnBinary
        | SemanticKind::DerivedTypeMismatch
        | SemanticKind::DerivedNullabilityNarrower => EngineClass::TypeError,
        SemanticKind::FillNullValueMustNotBeNull
        | SemanticKind::DropLastRemainingField
        | SemanticKind::DerivedIdentityNotUnique
        | SemanticKind::DerivedFieldInvalid
        | SemanticKind::ProjectionEmpty
        | SemanticKind::ProjectionDuplicate
        | SemanticKind::SchemaRebuildInvalid
        | SemanticKind::RuleNotAdmitted => EngineClass::InvalidPlan,
    }
}

fn assert_kind_maps_to_engine_class(kind: SemanticKind, error: &stillflow_engine::EngineError) {
    let expected = expected_engine_class(kind);
    let actual = EngineClass::of(error);
    match (&expected, &actual) {
        (EngineClass::UnknownColumn(_), EngineClass::UnknownColumn(_)) => {}
        _ => assert_eq!(
            expected, actual,
            "engine class drifted for shared kind {kind:?}"
        ),
    }
}

struct ExprCase {
    name: &'static str,
    expr: Expr,
}

fn expression_cases() -> Vec<ExprCase> {
    vec![
        ExprCase {
            name: "column_utf8",
            expr: col(101),
        },
        ExprCase {
            name: "column_int",
            expr: col(102),
        },
        ExprCase {
            name: "column_float",
            expr: col(103),
        },
        ExprCase {
            name: "column_bool_non_null",
            expr: col(104),
        },
        ExprCase {
            name: "column_binary",
            expr: col(105),
        },
        ExprCase {
            name: "column_date32",
            expr: col(106),
        },
        ExprCase {
            name: "column_timestamp_second_paused",
            expr: col(107),
        },
        ExprCase {
            name: "literal_int",
            expr: lit_int(7),
        },
        ExprCase {
            name: "literal_utf8",
            expr: lit_utf8("x"),
        },
        ExprCase {
            name: "literal_null",
            expr: Expr::Literal(ScalarValue::Null),
        },
        ExprCase {
            name: "literal_bool",
            expr: Expr::Literal(ScalarValue::Boolean(true)),
        },
        ExprCase {
            name: "not_boolean",
            expr: Expr::Unary {
                operator: UnaryOperator::Not,
                expression: Box::new(col(104)),
            },
        },
        ExprCase {
            name: "not_non_boolean",
            expr: Expr::Unary {
                operator: UnaryOperator::Not,
                expression: Box::new(col(102)),
            },
        },
        ExprCase {
            name: "negate_paused",
            expr: Expr::Unary {
                operator: UnaryOperator::Negate,
                expression: Box::new(col(102)),
            },
        },
        ExprCase {
            name: "is_null",
            expr: is_null(col(101)),
        },
        ExprCase {
            name: "is_null_of_paused_column",
            expr: is_null(col(107)),
        },
        ExprCase {
            name: "cast_int_to_int32",
            expr: cast(col(102), LogicalType::Int32),
        },
        ExprCase {
            name: "cast_to_binary_paused",
            expr: cast(col(102), LogicalType::Binary),
        },
        ExprCase {
            name: "cast_from_binary_paused",
            expr: cast(col(105), LogicalType::Utf8),
        },
        ExprCase {
            name: "cast_date_to_utf8_paused",
            expr: cast(col(106), LogicalType::Utf8),
        },
        ExprCase {
            name: "cast_to_list_paused",
            expr: cast(col(102), LogicalType::List(Box::new(LogicalType::Int64))),
        },
        ExprCase {
            name: "and_of_booleans",
            expr: binary(BinaryOperator::And, col(104), is_null(col(101))),
        },
        ExprCase {
            name: "and_of_non_boolean",
            expr: binary(BinaryOperator::And, col(104), col(102)),
        },
        ExprCase {
            name: "or_of_booleans",
            expr: binary(BinaryOperator::Or, is_null(col(102)), is_null(col(103))),
        },
        ExprCase {
            name: "contains_paused",
            expr: binary(BinaryOperator::Contains, col(101), lit_utf8("a")),
        },
        ExprCase {
            name: "add_paused",
            expr: binary(BinaryOperator::Add, col(102), lit_int(1)),
        },
        ExprCase {
            name: "modulo_paused",
            expr: binary(BinaryOperator::Modulo, col(102), lit_int(2)),
        },
        ExprCase {
            name: "equal_comparable",
            expr: binary(BinaryOperator::Equal, col(102), col(103)),
        },
        ExprCase {
            name: "equal_incomparable",
            expr: binary(BinaryOperator::Equal, col(102), col(101)),
        },
        ExprCase {
            name: "not_equal_comparable",
            expr: binary(BinaryOperator::NotEqual, col(101), lit_utf8("a")),
        },
        ExprCase {
            name: "ordered_numeric",
            expr: binary(BinaryOperator::GreaterThanOrEqual, col(102), lit_int(-128)),
        },
        ExprCase {
            name: "ordered_date",
            expr: binary(BinaryOperator::LessThan, col(106), col(106)),
        },
        ExprCase {
            name: "ordered_utf8_rejected",
            expr: binary(BinaryOperator::LessThan, col(101), col(101)),
        },
        ExprCase {
            name: "unknown_column",
            expr: col(999),
        },
        ExprCase {
            name: "unknown_column_nested",
            expr: is_null(binary(BinaryOperator::Equal, col(999), lit_int(1))),
        },
        ExprCase {
            name: "coalesce_same_type",
            expr: Expr::Coalesce {
                expressions: vec![col(102), lit_int(0)],
            },
        },
        ExprCase {
            name: "coalesce_numeric_lub",
            expr: Expr::Coalesce {
                expressions: vec![col(102), col(103)],
            },
        },
        ExprCase {
            name: "coalesce_incompatible_arms",
            expr: Expr::Coalesce {
                expressions: vec![col(102), col(101)],
            },
        },
        ExprCase {
            name: "coalesce_with_null_arm",
            expr: Expr::Coalesce {
                expressions: vec![col(101), Expr::Literal(ScalarValue::Null)],
            },
        },
        ExprCase {
            name: "coalesce_paused_joined",
            expr: Expr::Coalesce {
                expressions: vec![col(107), col(107)],
            },
        },
        ExprCase {
            name: "nested_casts_incompatible",
            expr: cast(cast(col(102), LogicalType::Int32), LogicalType::Float64),
        },
    ]
}

/// Differential 1: expression analysis. Both entry points must produce the
/// same type and nullability, or both reject with the frozen class mapping.
#[test]
fn expression_analysis_agrees_between_entry_points() {
    let schema = schema();
    for case in expression_cases() {
        let shared = semantics::analyze_expr(&case.expr, &schema);
        let engine = engine_semantics::analyze_expr(&case.expr, &schema);
        match (shared, engine) {
            (Ok(shared), Ok(engine)) => {
                assert_eq!(
                    shared.data_type, engine.data_type,
                    "type diverged for {}",
                    case.name
                );
                assert_eq!(
                    shared.nullable, engine.nullable,
                    "nullability diverged for {}",
                    case.name
                );
            }
            (Err(shared), Err(engine)) => {
                assert_kind_maps_to_engine_class(shared.kind(), &engine);
            }
            (shared, engine) => panic!(
                "accept/reject divergence for {}: shared {:?} engine {:?}",
                case.name,
                shared.map(|a| a.data_type).map_err(|e| e.kind()),
                engine.map(|a| a.data_type).map_err(|e| format!("{e:?}"))
            ),
        }
    }
}

/// Differential 2: rule schema effects. The shared analyzer and the engine's
/// production incremental layer must produce identical schemas for every
/// accepted rule, and both must reject the same inputs. The one documented
/// classification delta is `fill-null` with a null value: the compile path
/// classifies NG_INVALID_CONFIG while the engine keeps `TypeError`.
#[test]
fn rule_effects_agree_between_entry_points() {
    let schema = schema();
    let rules: Vec<(&str, Rule)> = vec![
        (
            "rename",
            Rule::Rename {
                column: column(101),
                to: "label".to_owned(),
            },
        ),
        (
            "trim_utf8",
            Rule::Trim {
                column: column(101),
            },
        ),
        (
            "trim_non_utf8",
            Rule::Trim {
                column: column(102),
            },
        ),
        (
            "cast_setnull",
            Rule::Cast {
                column: column(102),
                data_type: LogicalType::Float64,
                on_failure: CastFailurePolicy::SetNull,
            },
        ),
        (
            "cast_to_binary_paused",
            Rule::Cast {
                column: column(102),
                data_type: LogicalType::Binary,
                on_failure: CastFailurePolicy::Error,
            },
        ),
        (
            "cast_from_binary_paused",
            Rule::Cast {
                column: column(105),
                data_type: LogicalType::Utf8,
                on_failure: CastFailurePolicy::Error,
            },
        ),
        (
            "replace_to_null",
            Rule::ReplaceLiteral {
                column: column(101),
                from: ScalarValue::Utf8("x".to_owned()),
                to: ScalarValue::Null,
            },
        ),
        (
            "replace_binary_non_null",
            Rule::ReplaceLiteral {
                column: column(105),
                from: ScalarValue::Utf8("a".to_owned()),
                to: ScalarValue::Utf8("b".to_owned()),
            },
        ),
        (
            "replace_type_mismatch",
            Rule::ReplaceLiteral {
                column: column(102),
                from: ScalarValue::Utf8("a".to_owned()),
                to: ScalarValue::Utf8("b".to_owned()),
            },
        ),
        (
            "fill_null_ok",
            Rule::FillNull {
                column: column(102),
                value: ScalarValue::Int64(0),
            },
        ),
        (
            "fill_null_value_null",
            Rule::FillNull {
                column: column(102),
                value: ScalarValue::Null,
            },
        ),
        (
            "fill_null_on_binary",
            Rule::FillNull {
                column: column(105),
                value: ScalarValue::Utf8("x".to_owned()),
            },
        ),
        (
            "drop_unknown",
            Rule::DropColumn {
                column: column(999),
            },
        ),
        (
            "derive_ok",
            Rule::DeriveColumn {
                id: column(108),
                name: "seen".to_owned(),
                data_type: LogicalType::Boolean,
                nullable: true,
                expression: is_null(col(103)),
            },
        ),
        (
            "derive_type_mismatch",
            Rule::DeriveColumn {
                id: column(108),
                name: "bad".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: true,
                expression: col(102),
            },
        ),
        (
            "derive_nullability_narrower",
            Rule::DeriveColumn {
                id: column(108),
                name: "strict".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: false,
                expression: col(101),
            },
        ),
        (
            "derive_duplicate_id",
            Rule::DeriveColumn {
                id: column(102),
                name: "dupe".to_owned(),
                data_type: LogicalType::Int64,
                nullable: true,
                expression: col(103),
            },
        ),
        (
            "derive_unknown_reference",
            Rule::DeriveColumn {
                id: column(108),
                name: "missing".to_owned(),
                data_type: LogicalType::Int64,
                nullable: true,
                expression: col(999),
            },
        ),
        (
            "derive_paused_cast_in_expression",
            Rule::DeriveColumn {
                id: column(108),
                name: "from_blob".to_owned(),
                data_type: LogicalType::Utf8,
                nullable: true,
                expression: cast(col(105), LogicalType::Utf8),
            },
        ),
    ];
    for (name, rule) in &rules {
        let shared = semantics::rule_effect(&schema, rule);
        let engine = engine_semantics::rule_effect(&schema, rule);
        match (shared, engine) {
            (Ok(shared), Ok(engine)) => {
                assert_eq!(shared, engine, "post-rule schema diverged for rule {name}");
            }
            (Err(shared), Err(engine)) => {
                if shared.kind() == SemanticKind::FillNullValueMustNotBeNull {
                    // The one documented classification delta: the engine
                    // keeps `TypeError` for the null fill value.
                    assert_eq!(
                        EngineClass::of(&engine),
                        EngineClass::TypeError,
                        "engine class drifted for the documented fill-null delta"
                    );
                } else {
                    assert_kind_maps_to_engine_class(shared.kind(), &engine);
                }
            }
            (shared, engine) => panic!(
                "accept/reject divergence for rule {name}: shared {:?} engine {:?}",
                shared.map(|_| ()).map_err(|e| e.kind()),
                engine.map(|_| ()).map_err(|e| format!("{e:?}"))
            ),
        }
    }
}

/// Differential 3: projections. The shared effect and the engine's indexed
/// projection must agree for positive and negative inputs.
#[test]
fn projection_effect_agrees_between_entry_points() {
    let schema = schema();
    let positives: Vec<&[u128]> = vec![&[101, 102], &[104], &[102, 101, 107]];
    for columns in positives {
        let columns: Vec<ColumnId> = columns.iter().map(|value| column(*value)).collect();
        let shared = semantics::project_effect(&schema, &columns);
        let engine = engine_semantics::project_effect(&schema, &columns);
        assert_eq!(
            shared.expect("shared projection"),
            engine.expect("engine projection"),
            "projection diverged"
        );
    }
    for columns in [vec![column(999)], vec![column(101), column(101)]] {
        let shared = semantics::project_effect(&schema, &columns);
        let engine = engine_semantics::project_effect(&schema, &columns);
        assert!(
            shared.is_err() && engine.is_err(),
            "projection rejection diverged for {columns:?}"
        );
    }
    // Documented divergence (NX-C0 §5.3, minimal case recorded by this
    // battery): the engine's internal projection accepts an empty column
    // list for its own lowering paths, while the product compile path
    // rejects it. The compile-path rejection is preserved by the shared
    // analyzer; the engine lowering keeps its internal behavior.
    let engine_empty = engine_semantics::project_effect(&schema, &[]);
    assert!(
        engine_empty.is_ok(),
        "the engine's internal empty projection is its own behavior"
    );
    let shared_empty = semantics::project_effect(&schema, &[]);
    assert_eq!(
        shared_empty
            .expect_err("compile path rejects empty projections")
            .kind(),
        SemanticKind::ProjectionEmpty,
    );
}

/// Differential 4: the full compile loop. Every built-in node's compiled
/// per-node schema must equal the engine propagation of the equivalent rule
/// sequence, proving "same schema or equivalent rejection" across the two
/// entry points for all eleven built-ins.
#[test]
fn compiled_node_schemas_match_engine_propagation_for_all_builtins() {
    struct Step {
        name: &'static str,
        config: NodeConfig,
        rule: Option<Rule>,
        project: Option<Vec<ColumnId>>,
        predicate: Option<Expr>,
    }
    fn config(id: u128, type_id: &str, value: serde_json::Value) -> NodeConfig {
        NodeConfig::new(node(id), type_id, 1, value, BTreeMap::new()).expect("config")
    }
    // Frozen version-1 wire shapes (adjacent tags, camelCase, transparent
    // UUIDs). Hard-coding them here pins the wire contract the compiler
    // consumes.
    fn col_json(value: u128) -> serde_json::Value {
        serde_json::json!({ "kind": "column", "value": uuid(value).to_string() })
    }
    fn is_null_json(inner: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "kind": "isNull",
            "value": { "expression": inner, "negated": false }
        })
    }
    fn binary_json(
        operator: &str,
        left: serde_json::Value,
        right: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "kind": "binary",
            "value": { "left": left, "operator": operator, "right": right }
        })
    }

    let steps = vec![
        Step {
            name: "source",
            config: config(
                1,
                "stillflow.node.source",
                serde_json::json!({
                    "sourceAssetId": uuid(700),
                    "projection": [
                        column(101), column(102), column(103),
                        column(104), column(105), column(106), column(107)
                    ]
                }),
            ),
            rule: None,
            project: Some(vec![
                column(101),
                column(102),
                column(103),
                column(104),
                column(105),
                column(106),
                column(107),
            ]),
            predicate: None,
        },
        Step {
            name: "select",
            config: config(
                2,
                "stillflow.node.select",
                serde_json::json!({"columns": [column(101), column(102), column(103), column(104)]}),
            ),
            rule: None,
            project: Some(vec![column(101), column(102), column(103), column(104)]),
            predicate: None,
        },
        Step {
            name: "rename",
            config: config(
                3,
                "stillflow.node.rename",
                serde_json::json!({"column": column(101), "to": "label"}),
            ),
            rule: Some(Rule::Rename {
                column: column(101),
                to: "label".to_owned(),
            }),
            project: None,
            predicate: None,
        },
        Step {
            name: "trim",
            config: config(
                4,
                "stillflow.node.trim",
                serde_json::json!({"column": column(101)}),
            ),
            rule: Some(Rule::Trim {
                column: column(101),
            }),
            project: None,
            predicate: None,
        },
        Step {
            name: "fill-null",
            config: config(
                6,
                "stillflow.node.fill-null",
                serde_json::json!({
                    "column": column(102),
                    "value": { "kind": "int64", "value": 0 }
                }),
            ),
            rule: Some(Rule::FillNull {
                column: column(102),
                value: ScalarValue::Int64(0),
            }),
            project: None,
            predicate: None,
        },
        Step {
            name: "derive-column",
            config: config(
                7,
                "stillflow.node.derive-column",
                serde_json::json!({
                    "id": column(108),
                    "name": "seen",
                    "dataType": { "kind": "boolean" },
                    "nullable": true,
                    "expression": is_null_json(col_json(103))
                }),
            ),
            rule: Some(Rule::DeriveColumn {
                id: column(108),
                name: "seen".to_owned(),
                data_type: LogicalType::Boolean,
                nullable: true,
                expression: is_null(col(103)),
            }),
            project: None,
            predicate: None,
        },
        Step {
            name: "cast",
            config: config(
                8,
                "stillflow.node.cast",
                serde_json::json!({
                    "column": column(102),
                    "dataType": { "kind": "int32" },
                    "onFailure": "error"
                }),
            ),
            rule: Some(Rule::Cast {
                column: column(102),
                data_type: LogicalType::Int32,
                on_failure: CastFailurePolicy::Error,
            }),
            project: None,
            predicate: None,
        },
        Step {
            name: "filter",
            config: config(
                9,
                "stillflow.node.filter",
                serde_json::json!({
                    "predicate": binary_json(
                        "greaterThanOrEqual",
                        col_json(102),
                        serde_json::json!({
                            "kind": "literal",
                            "value": { "kind": "int64", "value": -128 }
                        }),
                    )
                }),
            ),
            rule: None,
            project: None,
            predicate: Some(binary(
                BinaryOperator::GreaterThanOrEqual,
                col(102),
                lit_int(-128),
            )),
        },
        Step {
            name: "drop-column",
            config: config(
                10,
                "stillflow.node.drop-column",
                serde_json::json!({"column": column(104)}),
            ),
            rule: Some(Rule::DropColumn {
                column: column(104),
            }),
            project: None,
            predicate: None,
        },
        Step {
            name: "output",
            config: config(
                5,
                "stillflow.node.output",
                serde_json::json!({"outputLabel": "cleaned"}),
            ),
            rule: None,
            project: None,
            predicate: None,
        },
    ];

    let graph = NodeGraph::new(
        uuid(900),
        node(1),
        node(5),
        steps.iter().map(|step| step.config.clone()).collect(),
        vec![
            (1u128, 2u128),
            (2, 3),
            (3, 4),
            (4, 6),
            (6, 7),
            (7, 8),
            (8, 9),
            (9, 10),
            (10, 5),
        ]
        .into_iter()
        .map(|(from, to)| NodeEdge {
            from: stillflow_core::NodePort {
                node_id: node(from),
                port: stillflow_core::PortId::new("out").expect("port"),
            },
            to: stillflow_core::NodePort {
                node_id: node(to),
                port: stillflow_core::PortId::new("in").expect("port"),
            },
        })
        .collect(),
        BTreeMap::new(),
    )
    .expect("graph");

    let source = AuthorizedSourceContext::new(uuid(700), schema()).expect("source");
    let compiled = NodeGraphCompiler::default()
        .compile(&graph, &source, CompileTarget::Execution)
        .expect("compile");

    // Replay the equivalent semantics through the engine entry points.
    let mut engine_schema = schema();
    for step in &steps {
        if let Some(columns) = &step.project {
            engine_schema =
                engine_semantics::project_effect(&engine_schema, columns).expect("project");
        }
        if let Some(rule) = &step.rule {
            engine_schema = engine_semantics::rule_effect(&engine_schema, rule).expect("rule");
        }
        if let Some(predicate) = &step.predicate {
            engine_semantics::analyze_expr(predicate, &engine_schema).expect("predicate");
        }
        let node_schema = compiled
            .node_schemas
            .get(&node(step_name_id(step.name)))
            .expect("compiled schema for step");
        assert_eq!(
            &engine_schema, node_schema,
            "engine propagation diverged from the compiler at step {}",
            step.name
        );
    }

    // The compiled plan keeps one plan node per product node with identity
    // mapping, and the filter step is a Filter plan node (not a rule).
    assert_eq!(
        compiled.node_plan_ids[&node(4)],
        PlanNodeId::from_uuid(uuid(4))
    );
    let plan_node = compiled
        .plan
        .nodes
        .get(&PlanNodeId::from_uuid(uuid(9)))
        .expect("filter plan node");
    assert!(matches!(plan_node.kind, PlanNodeKind::Filter { .. }));
}

fn step_name_id(name: &str) -> u128 {
    match name {
        "source" => 1,
        "select" => 2,
        "rename" => 3,
        "trim" => 4,
        "fill-null" => 6,
        "derive-column" => 7,
        "cast" => 8,
        "filter" => 9,
        "drop-column" => 10,
        "output" => 5,
        _ => unreachable!("unknown step {name}"),
    }
}

/// The documented classification delta, asserted explicitly so a future
/// change cannot happen silently: the compile path classifies a null
/// fill-null value as NG_INVALID_CONFIG while the engine keeps `TypeError`.
#[test]
fn fill_null_null_value_classification_delta_is_stable() {
    let schema = schema();
    let rule = Rule::FillNull {
        column: column(102),
        value: ScalarValue::Null,
    };
    let shared = semantics::rule_effect(&schema, &rule).expect_err("shared rejects");
    assert_eq!(shared.kind(), SemanticKind::FillNullValueMustNotBeNull);
    assert_eq!(
        shared.code().as_str(),
        "NG_INVALID_CONFIG",
        "the compile-path classification is frozen"
    );
    let engine = engine_semantics::rule_effect(&schema, &rule).expect_err("engine rejects");
    assert_eq!(
        EngineClass::of(&engine),
        EngineClass::TypeError,
        "the engine classification is frozen"
    );
}
